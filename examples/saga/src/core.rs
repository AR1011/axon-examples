//! The saga and the services as state machines with no I/O. The actors drive them with the network and the
//! database; `tests/sim.rs` drives them with the seeded simulator.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const INVENTORY: &str = "inventory";
pub const PAYMENT: &str = "payment";
pub const SHIPPING: &str = "shipping";
pub const SERVICES: [&str; 3] = [INVENTORY, PAYMENT, SHIPPING];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Line {
    pub sku: String,
    pub qty: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub lines: Vec<Line>,
    pub account: String,
    pub amount: u64,
    pub region: String,
}

/// One forward step: hold `amount` of the pool `key` of `service` for the saga.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub service: &'static str,
    pub key: String,
    pub amount: u64,
}

impl Order {
    /// Reserve every line, charge the account, book a slot in the region. Compensation runs them backwards.
    pub fn steps(&self) -> Vec<Step> {
        let reserve = self.lines.iter().map(|l| Step { service: INVENTORY, key: l.sku.clone(), amount: l.qty });
        reserve
            .chain([
                Step { service: PAYMENT, key: self.account.clone(), amount: self.amount },
                Step { service: SHIPPING, key: self.region.clone(), amount: 1 },
            ])
            .collect()
    }

    /// A pool holds one amount per saga, so an order names each sku once.
    pub fn validate(&self) -> Result<(), String> {
        let mut skus: Vec<&str> = self.lines.iter().map(|l| l.sku.as_str()).collect();
        skus.sort_unstable();
        skus.dedup();
        match () {
            _ if self.lines.is_empty() => Err("an order needs a line".into()),
            _ if skus.len() != self.lines.len() => Err("a sku appears twice".into()),
            _ if self.lines.iter().any(|l| l.qty == 0) => Err("a line of zero".into()),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Doing this step; the steps before it are applied.
    Forward(usize),
    /// Undoing: the steps below `undo` may be applied, and step `undo - 1` is undone next.
    Backward {
        undo: usize,
        reason: String,
    },
    Done,
    Compensated {
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Do(usize),
    Undo(usize),
}

/// How an action ended. A call whose outcome is unknown is not an outcome: the caller repeats the action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    /// The service refused the step: it holds nothing for the saga.
    Refused(String),
    /// The caller stopped trying a step that may or may not have been applied.
    Abandoned(String),
}

impl Phase {
    pub fn terminal(&self) -> bool {
        matches!(self, Phase::Done | Phase::Compensated { .. })
    }

    pub fn action(&self) -> Option<Action> {
        match self {
            Phase::Forward(i) => Some(Action::Do(*i)),
            Phase::Backward { undo, .. } => Some(Action::Undo(undo - 1)),
            Phase::Done | Phase::Compensated { .. } => None,
        }
    }

    /// The phase after this one's action ended with `outcome`, for a saga of `steps` steps.
    pub fn advance(&self, steps: usize, outcome: Outcome) -> Phase {
        let back = |undo: usize, reason: String| match undo {
            0 => Phase::Compensated { reason },
            undo => Phase::Backward { undo, reason },
        };
        match (self, outcome) {
            (Phase::Forward(i), Outcome::Applied) if i + 1 == steps => Phase::Done,
            (Phase::Forward(i), Outcome::Applied) => Phase::Forward(i + 1),
            (Phase::Forward(i), Outcome::Refused(reason)) => back(*i, reason),
            (Phase::Forward(i), Outcome::Abandoned(reason)) => back(i + 1, reason),
            // An undo always succeeds; nothing else ends one.
            (Phase::Backward { undo, reason }, _) => back(undo - 1, reason.clone()),
            (terminal, _) => terminal.clone(),
        }
    }
}

/// What a service keeps per saga, so a repeated call gets the first call's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hold {
    Applied(u64),
    Refused,
    /// Released, or released before it arrived: a late hold for this saga is refused.
    Undone,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    Applied,
    Refused(String),
}

/// A quantity that sagas hold parts of: a sku's stock, an account's balance, a region's shipping slots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pool {
    pub available: u64,
    // ponytail: one entry per saga forever; expire entries of sagas older than their retention when it matters.
    pub holds: BTreeMap<String, Hold>,
}

impl Pool {
    /// The answer, and whether the pool changed. `refuse` is a failure injected for this saga.
    pub fn hold(&mut self, saga: &str, amount: u64, refuse: bool) -> (Answer, bool) {
        match self.holds.get(saga) {
            Some(Hold::Applied(_)) => (Answer::Applied, false),
            Some(Hold::Refused) => (Answer::Refused("refused before".into()), false),
            Some(Hold::Undone) => (Answer::Refused("compensated".into()), false),
            None if refuse => {
                self.holds.insert(saga.into(), Hold::Refused);
                (Answer::Refused("injected failure".into()), true)
            }
            None if self.available < amount => {
                self.holds.insert(saga.into(), Hold::Refused);
                (Answer::Refused(format!("{} available, {amount} asked", self.available)), true)
            }
            None => {
                self.available -= amount;
                self.holds.insert(saga.into(), Hold::Applied(amount));
                (Answer::Applied, true)
            }
        }
    }

    pub fn release(&mut self, saga: &str) -> (Answer, bool) {
        let changed = match self.holds.insert(saga.into(), Hold::Undone) {
            Some(Hold::Applied(amount)) => {
                self.available += amount;
                true
            }
            Some(Hold::Undone) => false,
            Some(Hold::Refused) | None => true,
        };
        (Answer::Applied, changed)
    }

    /// Held by sagas right now.
    pub fn held(&self) -> u64 {
        self.holds.values().map(|h| if let Hold::Applied(a) = h { *a } else { 0 }).sum()
    }
}

/// The failure injected into the shipping service: it refuses every saga whose id hashes to 0 modulo `every`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fault {
    pub shipping_every: u64,
}

impl Fault {
    pub fn refuses(&self, service: &str, saga: &str) -> bool {
        service == SHIPPING && self.shipping_every > 0 && fnv1a(saga).is_multiple_of(self.shipping_every)
    }
}

fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order() -> Order {
        let line = |sku: &str| Line { sku: sku.into(), qty: 2 };
        Order {
            id: "o1".into(),
            lines: vec![line("a"), line("b")],
            account: "x".into(),
            amount: 10,
            region: "eu".into(),
        }
    }

    #[test]
    fn a_refusal_undoes_the_steps_before_it_in_reverse() {
        let n = order().steps().len();
        let refused = Phase::Forward(2).advance(n, Outcome::Refused("declined".into()));
        assert_eq!(refused.action(), Some(Action::Undo(1)));
        let next = refused.advance(n, Outcome::Applied);
        assert_eq!(next.action(), Some(Action::Undo(0)));
        assert_eq!(next.advance(n, Outcome::Applied), Phase::Compensated { reason: "declined".into() });
        assert_eq!(
            Phase::Forward(0).advance(n, Outcome::Refused("r".into())),
            Phase::Compensated { reason: "r".into() }
        );
    }

    #[test]
    fn an_abandoned_step_is_undone_too() {
        let n = order().steps().len();
        assert_eq!(Phase::Forward(1).advance(n, Outcome::Abandoned("t".into())).action(), Some(Action::Undo(1)));
        assert_eq!(Phase::Forward(n - 1).advance(n, Outcome::Applied), Phase::Done);
    }

    #[test]
    fn a_pool_answers_a_repeat_as_the_first_time_and_refuses_a_hold_after_its_release() {
        let mut pool = Pool { available: 5, ..Default::default() };
        assert_eq!(pool.hold("s", 3, false), (Answer::Applied, true));
        assert_eq!(pool.hold("s", 3, false), (Answer::Applied, false));
        assert_eq!(pool.available, 2);
        assert_eq!(pool.release("s"), (Answer::Applied, true));
        assert_eq!(pool.release("s"), (Answer::Applied, false));
        assert_eq!(pool.available, 5);
        assert_eq!(pool.release("late"), (Answer::Applied, true));
        assert_eq!(pool.hold("late", 1, false).0, Answer::Refused("compensated".into()));
        assert!(matches!(pool.hold("big", 9, false).0, Answer::Refused(_)));
        assert_eq!(pool.available, 5);
    }

    #[test]
    fn an_order_names_each_sku_once() {
        let mut o = order();
        assert!(o.validate().is_ok());
        o.lines[1].sku = "a".into();
        assert!(o.validate().is_err());
    }
}
