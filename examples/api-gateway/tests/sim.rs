//! The gateway's balancer and retry policy on the seeded network simulator: a gateway sends a
//! request a millisecond to three backends while links are cut and healed. Virtual time, so a
//! run of eight seconds takes milliseconds, and a seed replays exactly.

use std::collections::BTreeMap;
use std::time::Duration;

use api_gateway::balancer::{Balancer, BalancerConfig, Outcome};
use axon_testkit::{NodeId, SimConfig, Simulator, check_seeds};

const MS: Duration = Duration::from_millis(1);
const DEADLINE: Duration = Duration::from_millis(300);
const TRY: Duration = Duration::from_millis(100);
const RUN: Duration = Duration::from_secs(8);
const CUT: Duration = Duration::from_secs(1);
const HEAL: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
enum Msg {
    Request(u64),
    Answer(u64),
}

struct Request {
    issued: Duration,
    tried: Vec<String>,
    /// When it ended, and whether it succeeded.
    done: Option<(Duration, bool)>,
}

#[derive(Debug, PartialEq)]
struct Report {
    requests: usize,
    ok: usize,
    slowest: Duration,
    /// Success share of requests issued and ended while cut (after the first attempts there timed out), and
    /// after the heal.
    ok_while_cut: f64,
    ok_after_heal: f64,
    /// Answers from each backend after the heal.
    served_after_heal: BTreeMap<String, u64>,
    retries: u64,
    retries_denied: u64,
    ejections: u64,
}

struct Run {
    net: Simulator<Msg>,
    gateway: NodeId,
    balancer: Balancer<NodeId>,
    requests: Vec<Request>,
    attempts: BTreeMap<u64, (usize, String)>,
    timeouts: BTreeMap<(Duration, u64), ()>,
    next_attempt: u64,
    served_after_heal: BTreeMap<String, u64>,
}

impl Run {
    fn dispatch(&mut self, request: usize, now: Duration) {
        let r = &mut self.requests[request];
        let Some((node, id)) = self.balancer.pick(now, &r.tried) else {
            r.done = Some((now, false));
            return;
        };
        r.tried.push(node.clone());
        let expires = (now + TRY).min(r.issued + DEADLINE);
        let attempt = self.next_attempt;
        self.next_attempt += 1;
        self.attempts.insert(attempt, (request, node));
        self.timeouts.insert((expires, attempt), ());
        self.net.send(self.gateway, id, Msg::Request(attempt)).unwrap();
    }

    fn timed_out(&mut self, attempt: u64, now: Duration) {
        let (request, node) = self.attempts.remove(&attempt).expect("a pending attempt");
        self.balancer.finish(&node, Outcome::Failed, now);
        let r = &self.requests[request];
        // Reads: a timeout may be retried elsewhere, within the deadline and the budget.
        if now < r.issued + DEADLINE && self.balancer.has_other(&r.tried) && self.balancer.retry() {
            self.dispatch(request, now);
        } else {
            self.requests[request].done = Some((now, false));
        }
    }

    fn answered(&mut self, attempt: u64, now: Duration) {
        let Some((request, node)) = self.attempts.remove(&attempt) else { return }; // already timed out
        self.timeouts.retain(|&(_, a), _| a != attempt);
        self.balancer.finish(&node, Outcome::Ok, now);
        self.requests[request].done = Some((now, true));
        if now > HEAL {
            *self.served_after_heal.entry(node).or_default() += 1;
        }
    }
}

fn simulate(seed: u64, cut: &[&str]) -> Report {
    let cfg = SimConfig { seed, base_delay: MS, jitter: Duration::from_millis(3), ..Default::default() };
    let mut net = Simulator::new(cfg);
    let gateway = net.add_node("gw").unwrap();
    let mut balancer = Balancer::new(BalancerConfig::default(), seed);
    let mut backends = Vec::new();
    for name in ["b1", "b2", "b3"] {
        let id = net.add_node(name).unwrap();
        balancer.insert(name, id);
        backends.push((name, id));
    }
    let cut: Vec<NodeId> = backends.iter().filter(|(n, _)| cut.contains(n)).map(|&(_, id)| id).collect();
    let mut run = Run {
        net,
        gateway,
        balancer,
        requests: Vec::new(),
        attempts: BTreeMap::new(),
        timeouts: BTreeMap::new(),
        next_attempt: 0,
        served_after_heal: BTreeMap::new(),
    };
    let mut next_issue = Duration::ZERO;
    let mut edges = vec![HEAL, CUT];
    loop {
        let edge = edges.last().copied();
        let delivery = run.net.next_delivery_at();
        let timeout = run.timeouts.first_key_value().map(|(&(at, _), _)| at);
        let issue = (next_issue < RUN).then_some(next_issue);
        let Some(now) = [edge, delivery, timeout, issue].into_iter().flatten().min() else { break };
        if edge == Some(now) {
            run.net.advance_idle_to(now).unwrap();
            match edges.pop() {
                Some(CUT) => run.net.partition(&[run.gateway], &cut).unwrap(),
                _ => run.net.heal(),
            }
        } else if timeout == Some(now) {
            run.net.advance_idle_to(now).unwrap();
            let ((_, attempt), ()) = run.timeouts.pop_first().unwrap();
            run.timed_out(attempt, now);
        } else if delivery == Some(now) {
            let d = run.net.step().unwrap();
            match d.payload {
                Msg::Request(attempt) => drop(run.net.send(d.to, d.from, Msg::Answer(attempt)).unwrap()),
                Msg::Answer(attempt) => run.answered(attempt, d.at),
            }
        } else {
            run.net.advance_idle_to(now).unwrap();
            run.balancer.admit();
            run.requests.push(Request { issued: now, tried: Vec::new(), done: None });
            run.dispatch(run.requests.len() - 1, now);
            next_issue += MS;
        }
    }
    let share = |from: Duration, to: Duration| {
        let window: Vec<_> = run.requests.iter().filter(|r| r.issued >= from && r.issued < to).collect();
        window.iter().filter(|r| r.done.is_some_and(|(_, ok)| ok)).count() as f64 / window.len().max(1) as f64
    };
    let s = run.balancer.stats;
    Report {
        requests: run.requests.len(),
        ok: run.requests.iter().filter(|r| r.done.is_some_and(|(_, ok)| ok)).count(),
        slowest: run.requests.iter().map(|r| r.done.expect("every request ends").0 - r.issued).max().unwrap(),
        ok_while_cut: share(CUT + DEADLINE, HEAL - DEADLINE),
        ok_after_heal: share(HEAL + DEADLINE, RUN),
        served_after_heal: run.served_after_heal,
        retries: s.retries,
        retries_denied: s.retries_denied,
        ejections: s.ejections,
    }
}

fn within_budget(r: &Report) -> bool {
    r.retries as f64 <= 10.0 + BalancerConfig::default().retry_ratio * r.requests as f64
}

#[test]
fn one_backend_cut_off_costs_almost_nothing_and_serves_again_after_the_heal() {
    check_seeds(0..100, |seed| {
        let r = simulate(seed, &["b2"]);
        assert_eq!(r.requests, 8000);
        assert!(r.slowest <= DEADLINE, "seed {seed}: a request took {:?}", r.slowest);
        assert!(r.ok_while_cut >= 0.99, "seed {seed}: {r:?}");
        assert!(r.ok_after_heal >= 0.999, "seed {seed}: {r:?}");
        assert!(r.served_after_heal.get("b2").is_some_and(|&n| n > 100), "seed {seed}: b2 back: {r:?}");
        assert!(r.ejections >= 1 && within_budget(&r), "seed {seed}: {r:?}");
    })
    .unwrap();
}

#[test]
fn every_backend_cut_off_fails_fast_within_the_budget_and_recovers() {
    check_seeds(0..100, |seed| {
        let r = simulate(seed, &["b1", "b2", "b3"]);
        assert!(r.slowest <= DEADLINE, "seed {seed}: a request took {:?}", r.slowest);
        assert!(r.ok_while_cut < 0.01, "seed {seed}: {r:?}");
        assert!(within_budget(&r) && r.retries_denied > 0, "seed {seed}: retries are capped: {r:?}");
        assert!(r.ok_after_heal >= 0.999, "seed {seed}: {r:?}");
    })
    .unwrap();
}

#[test]
fn a_seed_replays_exactly() {
    assert_eq!(simulate(7, &["b2"]), simulate(7, &["b2"]));
    assert_ne!(simulate(7, &["b2"]), simulate(8, &["b2"]));
}
