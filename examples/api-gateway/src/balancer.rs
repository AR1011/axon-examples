//! The gateway's routing core, without I/O: `now` is an argument so the simulator can drive it.
//!
//! - Pick: power of two choices on calls in flight, so a slow backend, whose calls pile up, gets
//!   fewer new ones.
//! - Eject: `eject_after` failures in a row (a timeout, a lost session) take a backend out for
//!   `eject_for`, doubling per ejection up to `eject_max`. After that it gets traffic again; one more
//!   failure ejects it at once, one success clears it. At most half the backends are out at once.
//! - Budget: each request deposits `retry_ratio` of a retry, a retry spends one, the balance is
//!   capped at `retry_cap`. So retries add at most `retry_ratio` to the load, however bad it gets.

use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct BalancerConfig {
    pub eject_after: u32,
    pub eject_for: Duration,
    pub eject_max: Duration,
    pub retry_ratio: f64,
    pub retry_cap: f64,
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            eject_after: 3,
            eject_for: Duration::from_millis(500),
            eject_max: Duration::from_secs(8),
            retry_ratio: 0.2,
            retry_cap: 50.0,
        }
    }
}

/// How an attempt ended, as far as the backend's health goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// It refused for load (`Full`, `Busy`): healthy, just full. No ejection.
    Overloaded,
    /// No answer in time, or the session failed: counts toward ejection.
    Failed,
}

struct Slot<T> {
    node: String,
    target: T,
    in_flight: u32,
    served: u64,
    failures: u32,
    ejections: u32,
    ejected_until: Duration,
}

pub struct Balancer<T> {
    cfg: BalancerConfig,
    slots: Vec<Slot<T>>,
    tokens: f64,
    rng: u64,
    pub stats: BalancerStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BalancerStats {
    pub requests: u64,
    pub retries: u64,
    pub retries_denied: u64,
    pub ejections: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendState {
    pub node: String,
    pub in_flight: u32,
    pub served: u64,
    pub ejected: bool,
}

impl<T: Clone> Balancer<T> {
    pub fn new(cfg: BalancerConfig, seed: u64) -> Self {
        Self { cfg, slots: Vec::new(), tokens: cfg.retry_cap.min(10.0), rng: seed | 1, stats: BalancerStats::default() }
    }

    /// Adds or replaces the backend on `node`.
    pub fn insert(&mut self, node: &str, target: T) {
        self.remove(node);
        let slot = Slot {
            node: node.into(),
            target,
            in_flight: 0,
            served: 0,
            failures: 0,
            ejections: 0,
            ejected_until: Duration::ZERO,
        };
        self.slots.push(slot);
    }

    pub fn remove(&mut self, node: &str) -> bool {
        let before = self.slots.len();
        self.slots.retain(|slot| slot.node != node);
        before != self.slots.len()
    }

    /// Keeps the backends `keep` answers true for.
    pub fn retain(&mut self, mut keep: impl FnMut(&str, &T) -> bool) {
        self.slots.retain(|slot| keep(&slot.node, &slot.target));
    }

    pub fn contains(&self, node: &str) -> bool {
        self.slots.iter().any(|slot| slot.node == node)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Whether a backend not in `tried` remains.
    pub fn has_other(&self, tried: &[String]) -> bool {
        self.slots.iter().any(|slot| !tried.contains(&slot.node))
    }

    pub fn backends(&self, now: Duration) -> Vec<BackendState> {
        let state = |s: &Slot<T>| BackendState {
            node: s.node.clone(),
            in_flight: s.in_flight,
            served: s.served,
            ejected: s.ejected_until > now,
        };
        self.slots.iter().map(state).collect()
    }

    /// Counts a new request toward the retry budget.
    pub fn admit(&mut self) {
        self.stats.requests += 1;
        self.tokens = (self.tokens + self.cfg.retry_ratio).min(self.cfg.retry_cap);
    }

    /// Spends one retry from the budget, if there is one.
    pub fn retry(&mut self) -> bool {
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.stats.retries += 1;
            true
        } else {
            self.stats.retries_denied += 1;
            false
        }
    }

    /// Picks a backend not in `tried` and counts the call in flight on it. Ejected backends are
    /// skipped while any other remains.
    pub fn pick(&mut self, now: Duration, tried: &[String]) -> Option<(String, T)> {
        let open: Vec<usize> = (0..self.slots.len()).filter(|&i| !tried.contains(&self.slots[i].node)).collect();
        let healthy: Vec<usize> = open.iter().copied().filter(|&i| self.slots[i].ejected_until <= now).collect();
        let pool = if healthy.is_empty() { &open } else { &healthy };
        let i = match pool.len() {
            0 => return None,
            1 => pool[0],
            n => {
                let first = self.next(n);
                let (a, b) = (pool[first], pool[(first + 1 + self.next(n - 1)) % n]);
                if self.slots[b].in_flight < self.slots[a].in_flight { b } else { a }
            }
        };
        let slot = &mut self.slots[i];
        slot.in_flight += 1;
        Some((slot.node.clone(), slot.target.clone()))
    }

    /// Ends a call `pick` started on `node`. A node removed meanwhile is ignored.
    pub fn finish(&mut self, node: &str, outcome: Outcome, now: Duration) {
        let cfg = self.cfg;
        let total = self.slots.len();
        let out = self.slots.iter().filter(|s| s.ejected_until > now).count();
        let Some(slot) = self.slots.iter_mut().find(|slot| slot.node == node) else { return };
        slot.in_flight = slot.in_flight.saturating_sub(1);
        match outcome {
            Outcome::Ok => (slot.served, slot.failures, slot.ejections) = (slot.served + 1, 0, 0),
            Outcome::Overloaded => {}
            Outcome::Failed => {
                slot.failures += 1;
                let out_already = slot.ejected_until > now;
                if slot.failures >= cfg.eject_after && !out_already && (out + 1) * 2 <= total.max(2) {
                    let pause = cfg.eject_for.saturating_mul(1 << slot.ejections.min(16)).min(cfg.eject_max);
                    slot.ejected_until = now + pause;
                    slot.ejections += 1;
                    self.stats.ejections += 1;
                }
            }
        }
    }

    fn next(&mut self, n: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng % n as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn prefers_the_less_loaded_backend() {
        let mut b = Balancer::new(BalancerConfig::default(), 7);
        b.insert("a", ());
        b.insert("b", ());
        for _ in 0..10 {
            b.pick(MS, &["b".into()]);
        }
        assert!(
            (0..10).all(|_| b.pick(MS, &[]).expect("a backend").0 == "b"),
            "b gets new calls until it holds as many as a"
        );
    }

    #[test]
    fn ejects_after_failures_and_readmits_after_the_pause() {
        let cfg = BalancerConfig::default();
        let mut b = Balancer::new(cfg, 1);
        b.insert("a", ());
        b.insert("b", ());
        for _ in 0..cfg.eject_after {
            b.pick(MS, &["b".into()]);
            b.finish("a", Outcome::Failed, MS);
        }
        assert!((0..50).all(|_| {
            let (node, ()) = b.pick(MS, &[]).expect("a backend");
            b.finish(&node, Outcome::Ok, MS);
            node == "b"
        }));
        let back = MS + cfg.eject_for;
        assert!((0..50).any(|_| {
            let (node, ()) = b.pick(back, &[]).expect("a backend");
            b.finish(&node, Outcome::Ok, back);
            node == "a"
        }));
        // Both failing: only one of two may be out.
        for node in ["a", "b"] {
            for _ in 0..cfg.eject_after {
                b.finish(node, Outcome::Failed, back);
            }
        }
        assert_eq!(b.backends(back).iter().filter(|s| s.ejected).count(), 1);
    }

    #[test]
    fn retries_stay_within_the_budget() {
        let cfg = BalancerConfig::default();
        let mut b: Balancer<()> = Balancer::new(cfg, 1);
        for _ in 0..1000 {
            b.admit();
            while b.retry() {}
        }
        assert!(b.stats.retries as f64 <= 10.0 + 1000.0 * cfg.retry_ratio);
        assert!(b.stats.retries >= 200);
    }
}
