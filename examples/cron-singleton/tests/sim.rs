//! The scheduler's core and the run log over axon-testkit's seeded network: messages lost, duplicated and
//! reordered, the lease moving between three nodes with the deposed owner still acting for a while (a pause, a
//! partition), and each node's wall clock stepping back and forward. Every seed must fire each tick once.
//! One failing seed reruns with `SIM_SEED=n`; `SEEDS=n` runs seeds 0..n instead of 0..200.

use std::time::Duration;

use axon_testkit::{SimConfig, Simulator, check_seeds, seeds};
use cron_singleton::{Fire, Job, LogAnswer, LogCall, Outcome, Plan, RunLogActor, audit};

const BASE: u64 = 1_800_000_000_000;
const STEP: Duration = Duration::from_millis(50);
const TIMEOUT: Duration = Duration::from_millis(400);

#[derive(Clone)]
enum Wire {
    Call(u64, LogCall),
    Answer(u64, LogAnswer),
}

/// One scheduler node: the real `Plan`, with the one-request-at-a-time rule the actor follows.
struct Owner {
    epoch: u64,
    plan: Plan,
    in_flight: Option<(u64, Vec<Fire>, Duration)>,
    until: Option<Duration>, // deposed: acts until then, as a paused node would
}

struct Node {
    step_ms: i64,
    owner: Option<Owner>,
}

fn jobs() -> Vec<Job> {
    vec![
        Job { name: "beat".into(), every_ms: 1000, offset_ms: 0, max_runs: 1024 },
        Job { name: "digest".into(), every_ms: 3000, offset_ms: 500, max_runs: 1 },
        Job { name: "burst".into(), every_ms: 700, offset_ms: 100, max_runs: 4 },
    ]
}

fn run(seed: u64) -> Vec<(Fire, u64)> {
    let cfg = SimConfig {
        seed,
        jitter: Duration::from_millis(120),
        drop_every: 12,
        duplicate_every: 9,
        ordered: false,
        ..Default::default()
    };
    let mut net: Simulator<Wire> = Simulator::new(cfg);
    let log_id = net.add_node("runlog").unwrap();
    let ids: Vec<_> = (0..3).map(|i| net.add_node(&format!("s{i}")).unwrap()).collect();
    let mut nodes: Vec<Node> = ids.iter().map(|_| Node { step_ms: 0, owner: None }).collect();
    let mut log = RunLogActor::new(|_| {});
    let mut accepted: Vec<(Fire, u64)> = Vec::new();
    let (mut epoch, mut req, mut term_ends) = (1u64, 0u64, Duration::ZERO);
    let wall = |node: &Node, at: Duration| (BASE as i64 + at.as_millis() as i64 + node.step_ms) as u64;
    let faults_end = Duration::from_secs(120);
    let end = faults_end + Duration::from_secs(20);

    let mut t = Duration::ZERO;
    while t < end {
        t += STEP;
        while net.next_delivery_at().is_some_and(|at| at <= t) {
            let Some(d) = net.step() else { break };
            match d.payload {
                Wire::Call(id, call) => {
                    let (epoch, fires) = match &call {
                        LogCall::Fire { epoch, fires, .. } => (*epoch, fires.clone()),
                        LogCall::Claim { epoch, .. } => (*epoch, Vec::new()),
                    };
                    let answer = log.answer(call);
                    if let LogAnswer::Outcomes(outcomes) = &answer {
                        for (fire, outcome) in fires.into_iter().zip(outcomes) {
                            if let Outcome::Fired { from } = outcome {
                                accepted.push((Fire { from: *from, ..fire }, epoch));
                            }
                        }
                    }
                    net.send(log_id, d.from, Wire::Answer(id, answer)).unwrap();
                }
                Wire::Answer(id, answer) => {
                    let node = &mut nodes[ids.iter().position(|n| *n == d.to).unwrap()];
                    let now = wall(node, t);
                    let Some(owner) = node.owner.as_mut() else { continue };
                    let Some((_, fires, _)) = owner.in_flight.take_if(|(r, ..)| *r == id) else { continue };
                    let keep = match answer {
                        LogAnswer::Claimed(last) => {
                            owner.plan.claimed(last, now);
                            true
                        }
                        LogAnswer::Outcomes(outcomes) => match owner.plan.settle(&fires, &outcomes) {
                            Ok(()) => true,
                            Err(Outcome::Fenced) => false,
                            Err(other) => panic!("seed {seed}: the log refused {other:?} for {fires:?}"),
                        },
                        LogAnswer::Fenced(_) => false,
                    };
                    if !keep {
                        node.owner = None;
                    }
                }
            }
        }
        net.advance_idle_to(t).unwrap();

        if t < faults_end {
            if t >= term_ends {
                // The lease moves: a new owner under a larger epoch; the old one may not notice for up to 3 s.
                let next = net.pick(0..3) as usize;
                for (i, node) in nodes.iter_mut().enumerate() {
                    if let Some(owner) = node.owner.as_mut().filter(|o| o.until.is_none()) {
                        owner.until = Some(t + Duration::from_millis(net.pick(0..3000)));
                    }
                    if i == next {
                        epoch += 1;
                        node.owner = Some(Owner { epoch, plan: Plan::new(jobs()), in_flight: None, until: None });
                    }
                }
                term_ends = t + Duration::from_millis(1000 + net.pick(0..7000));
            }
            if net.pick(0..60) == 0 {
                // A clock step on one node: back up to 5 s, or forward up to 20 s.
                let node = &mut nodes[net.pick(0..3) as usize];
                node.step_ms += net.pick(0..25_000) as i64 - 5_000;
            }
        }

        for (i, node) in nodes.iter_mut().enumerate() {
            let now = wall(node, t);
            let Some(owner) = node.owner.as_mut() else { continue };
            if owner.until.is_some_and(|u| t >= u) {
                node.owner = None; // its lease deadline passed: the actor stops itself
                continue;
            }
            if owner.in_flight.as_ref().is_some_and(|(_, _, sent)| t >= *sent + TIMEOUT) {
                owner.in_flight = None; // no answer: ask again, the log answers a repeat with Duplicate
            }
            if owner.in_flight.is_some() {
                continue;
            }
            let call = if owner.plan.is_claimed() {
                let fires = owner.plan.owed(now);
                if fires.is_empty() {
                    continue;
                }
                owner.in_flight = Some((req, fires.clone(), t));
                LogCall::Fire { epoch: owner.epoch, node: format!("s{i}"), fires }
            } else {
                owner.in_flight = Some((req, Vec::new(), t));
                LogCall::Claim { epoch: owner.epoch, node: format!("s{i}") }
            };
            net.send(ids[i], log_id, Wire::Call(req, call)).unwrap();
            req += 1;
        }
    }

    let fires: Vec<Fire> = accepted.iter().map(|(f, _)| f.clone()).collect();
    let covered = audit(&fires).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    assert!(accepted.windows(2).all(|w| w[0].1 <= w[1].1), "seed {seed}: a deposed owner fired after its successor");
    let (i, last) = nodes.iter().enumerate().find_map(|(i, n)| n.owner.as_ref().map(|o| (i, o))).expect("an owner");
    for job in jobs() {
        // Up to 2 s behind: the last fires may still be retrying through the lossy network.
        let current = job.tick_at(wall(&nodes[i], t) - 2000).unwrap();
        let reached = covered.get(&job.name).map_or(0, |c| c.1);
        assert!(reached >= current, "seed {seed}: {} reached {reached}, the owner's clock is at {current}", job.name);
        // The owner may trail the log while answers are lost, never lead it.
        let next = last.plan.next(&job.name);
        assert!(
            next.is_some_and(|n| n <= reached + 1),
            "seed {seed}: {} owes from {next:?}, the log reached {reached}",
            job.name
        );
    }
    assert!(net.stats().dropped > 0 && net.stats().duplicated > 0);
    accepted
}

#[test]
fn every_tick_fires_once_under_loss_duplicates_handovers_and_clock_steps() {
    if let Err(failed) = check_seeds(seeds(0..200), |seed| drop(run(seed))) {
        panic!("seed {} failed: {}", failed.seed, failed.message);
    }
}

#[test]
fn a_seed_gives_the_same_run_twice() {
    // The rig only uses BTreeMaps and the seeded RNG, so the check above names seeds that fail again.
    assert_eq!(run(7), run(7));
}
