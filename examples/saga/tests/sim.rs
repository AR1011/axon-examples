//! The saga and pool state machines over the seeded simulator: messages lost, duplicated and reordered, the saga
//! node and the service nodes crashed at random. A saga node keeps only each saga's stored phase across a crash,
//! as the actor does. Every seed must end with every saga done or compensated and every pool consistent, even
//! after the stale duplicates still in flight have landed.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon_testkit::{NodeId, SimConfig, Simulator, check_seeds};
use saga::core::{Action, Answer, Fault, INVENTORY, Order, Outcome, PAYMENT, Phase, Pool, SHIPPING};
use saga::{orders, verify};

#[derive(Clone, Debug)]
enum Msg {
    Call { saga: usize, attempt: u64, service: &'static str, key: String, hold: Option<u64> },
    Answer { saga: usize, attempt: u64, answer: Answer },
}

struct Saga {
    order: Order,
    /// What the saga's database holds; the only thing a crash keeps.
    stored: Phase,
    attempt: u64,
    tries: u32,
    /// When the saga gives up waiting for the answer to its attempt.
    deadline: Duration,
}

const SAGA_NODE: NodeId = 0;
/// A step tried this many times without an answer is abandoned, and compensated.
const MAX_TRIES: u32 = 4;
/// Shorter than the slowest one-way trip, so attempts overlap and stale copies land after the saga moved on.
const TIMEOUT: Duration = Duration::from_millis(6);

struct World {
    net: Simulator<Msg>,
    sagas: Vec<Saga>,
    pools: BTreeMap<(&'static str, String), (u64, Pool)>,
    fault: Fault,
    next_attempt: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let cfg = SimConfig {
            seed,
            jitter: Duration::from_millis(8),
            drop_every: 6,
            duplicate_every: 5,
            ordered: false,
            ..Default::default()
        };
        let mut net = Simulator::new(cfg);
        for name in ["sagas", "svc-a", "svc-b", "svc-c"] {
            net.add_node(name).expect("a node");
        }
        // Few units, so some orders run out of stock or money and compensate early.
        let mut pools = BTreeMap::new();
        for (service, key, amount) in saga::catalog() {
            let amount = match service {
                INVENTORY => 6,
                PAYMENT => 80,
                _ => amount,
            };
            pools.insert((service, key), (amount, Pool { available: amount, ..Default::default() }));
        }
        let sagas = orders(seed, 16)
            .into_iter()
            .map(|order| Saga { order, stored: Phase::Forward(0), attempt: 0, tries: 0, deadline: Duration::ZERO })
            .collect();
        World { net, sagas, pools, fault: Fault { shipping_every: 4 }, next_attempt: 1 }
    }

    fn service_node(key: &str) -> NodeId {
        1 + key.bytes().map(usize::from).sum::<usize>() % 3
    }

    /// Sends the saga's current action with a fresh attempt number.
    fn issue(&mut self, i: usize) {
        let saga = &mut self.sagas[i];
        let (index, hold) = match saga.stored.action() {
            None => return,
            Some(Action::Do(s)) => (s, true),
            Some(Action::Undo(s)) => (s, false),
        };
        let step = &saga.order.steps()[index];
        saga.attempt = self.next_attempt;
        saga.deadline = self.net.now() + TIMEOUT;
        self.next_attempt += 1;
        let msg = Msg::Call {
            saga: i,
            attempt: saga.attempt,
            service: step.service,
            key: step.key.clone(),
            hold: hold.then_some(step.amount),
        };
        self.net.send(SAGA_NODE, Self::service_node(&step.key), msg).expect("room in the network");
    }

    /// The saga stores its next phase, then acts on it.
    fn advance(&mut self, i: usize, outcome: Outcome) {
        let saga = &mut self.sagas[i];
        saga.stored = saga.stored.advance(saga.order.steps().len(), outcome);
        saga.tries = 0;
        self.issue(i);
    }

    fn deliver(&mut self, to: NodeId, msg: Msg) {
        match msg {
            Msg::Call { saga, attempt, service, key, hold } => {
                let id = self.sagas[saga].order.id.clone();
                let refuse = self.fault.refuses(service, &id);
                let pool = &mut self.pools.get_mut(&(service, key)).expect("a known pool").1;
                let (answer, _) = match hold {
                    Some(amount) => pool.hold(&id, amount, refuse),
                    None => pool.release(&id),
                };
                self.net.send(to, SAGA_NODE, Msg::Answer { saga, attempt, answer }).expect("room in the network");
            }
            Msg::Answer { saga, attempt, answer } if attempt == self.sagas[saga].attempt => {
                let outcome = match answer {
                    Answer::Applied => Outcome::Applied,
                    Answer::Refused(reason) => Outcome::Refused(reason),
                };
                self.sagas[saga].attempt = 0;
                self.advance(saga, outcome);
            }
            Msg::Answer { .. } => {} // a duplicate, or the answer to an attempt given up on
        }
    }

    /// A saga waited too long for an answer: it tries again, or abandons its step.
    fn time_out(&mut self) {
        let now = self.net.now();
        for i in 0..self.sagas.len() {
            let saga = &mut self.sagas[i];
            if saga.stored.terminal() || saga.deadline > now {
                continue;
            }
            saga.tries += 1;
            if saga.tries > MAX_TRIES && matches!(saga.stored, Phase::Forward(_)) {
                self.advance(i, Outcome::Abandoned("no answer".into()));
            } else {
                self.issue(i);
            }
        }
    }

    fn crash_saga_node(&mut self) {
        self.net.crash_node(SAGA_NODE).expect("the saga node");
        for i in 0..self.sagas.len() {
            (self.sagas[i].attempt, self.sagas[i].tries) = (0, 0);
            self.issue(i);
        }
    }

    fn run(mut self) -> (saga::Report, Vec<(Duration, u64)>) {
        for i in 0..self.sagas.len() {
            self.issue(i);
        }
        let mut crashes = Vec::new();
        for _ in 0..200_000 {
            if self.sagas.iter().all(|s| s.stored.terminal()) {
                break;
            }
            let next = self.sagas.iter().filter(|s| !s.stored.terminal()).map(|s| s.deadline).min();
            match (self.net.next_delivery_at(), next) {
                (Some(at), Some(deadline)) if at <= deadline => {
                    let delivery = self.net.step().expect("a delivery is due");
                    self.deliver(delivery.to, delivery.payload);
                }
                (_, Some(deadline)) => {
                    self.net.advance_idle_to(deadline.max(self.net.now())).expect("idle until the deadline");
                    self.time_out();
                }
                (_, None) => unreachable!("an unfinished saga has a deadline"),
            }
            if self.net.pick(0..97) == 0 {
                let node = 1 + self.net.pick(0..3) as usize;
                crashes.push((self.net.now(), self.net.crash_node(node).expect("a node")));
            } else if self.net.pick(0..89) == 0 {
                crashes.push((self.net.now(), 0));
                self.crash_saga_node();
            }
        }
        // Let the stale duplicates land: a late hold must not take anything back from a compensated saga.
        while let Some(delivery) = self.net.step() {
            if let Msg::Call { .. } = delivery.payload {
                self.deliver(delivery.to, delivery.payload);
            }
        }
        let sagas = self.sagas.iter().map(|s| (s.order.id.clone(), (s.order.clone(), s.stored.clone(), 0))).collect();
        (verify(&sagas, &self.pools, self.fault), crashes)
    }
}

#[test]
fn every_saga_ends_consistent_under_loss_duplicates_and_crashes() {
    let totals = [(); 4].map(|_| AtomicUsize::new(0));
    check_seeds(0..300, |seed| {
        let (report, crashes) = World::new(seed).run();
        assert!(report.violations.is_empty(), "seed {seed}: {:#?}", report.violations);
        assert_eq!(report.done + report.compensated, report.sagas, "seed {seed}");
        let abandoned = report.reasons.get("no answer").copied().unwrap_or(0);
        for (total, n) in totals.iter().zip([report.done, report.compensated, crashes.len(), abandoned]) {
            total.fetch_add(n, Relaxed);
        }
    })
    .unwrap();
    // The seeds cover both endings and crashes, or the test proves little.
    let [done, compensated, crashes, abandoned] = totals.map(AtomicUsize::into_inner);
    println!("{done} done, {compensated} compensated ({abandoned} abandoned), {crashes} crashes");
    assert!(
        done > 400 && compensated > 500 && crashes > 300 && abandoned > 100,
        "{done} {compensated} {crashes} {abandoned}"
    );
}

#[test]
fn a_seed_replays_the_same_run() {
    let run = |seed| {
        let (report, crashes) = World::new(seed).run();
        (report.done, report.compensated, report.reasons, crashes)
    };
    for seed in [3, 17, 42] {
        assert_eq!(run(seed), run(seed));
    }
}

#[test]
fn the_fault_refuses_a_quarter_of_shipments() {
    let fault = Fault { shipping_every: 4 };
    let refused = orders(9, 400).iter().filter(|o| fault.refuses(SHIPPING, &o.id)).count();
    assert!((60..140).contains(&refused), "{refused}");
}
