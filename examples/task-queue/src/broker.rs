//! Every queue's jobs, in one actor's memory. A worker's jobs stay its own while it keeps polling and lists them;
//! otherwise they go back to pending. Nothing here survives this node: see the README.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use axon::{Actor, Context};
use axon_cluster::Reply;

use crate::{Answer, Call, Counts, Delivery, JobView, Load, NewJob, Outcome, State, Status, Ticket};

#[derive(Clone, Debug)]
pub struct BrokerConfig {
    /// Jobs waiting (pending and delayed) before new ones are refused. Retries and taken-back jobs are never refused,
    /// so the count can pass it by at most the jobs running.
    pub capacity: usize,
    /// A worker that has not polled for this long loses its jobs to others.
    pub visibility: Duration,
    /// First retry after `backoff`, doubling per failure up to `max_backoff`, each times a jitter in 0.5..1.5.
    pub backoff: Duration,
    pub max_backoff: Duration,
    /// How often delayed jobs are promoted and silent workers are checked.
    pub tick: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            visibility: Duration::from_secs(5),
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            tick: Duration::from_millis(10),
        }
    }
}

pub enum Msg {
    Call(Call, Option<Reply<Answer>>),
    Tick,
}

struct Job {
    spec: NewJob,
    state: State,
    deliveries: u32,
    failures: u32,
    /// Failed runs left before the job is dead; a retry from the dead set refills it.
    left: u32,
    worker: Option<String>,
    last_error: Option<String>,
    /// Its place in line among jobs of its priority, kept when it is taken back or retried.
    seq: u64,
}

#[derive(Default)]
struct Worker {
    load: Load,
    holds: BTreeSet<u64>,
    seen: Option<Instant>,
}

pub struct Broker {
    cfg: BrokerConfig,
    boot: u64,
    // ponytail: done jobs are kept (without payload) for dedup and inspection; expire them after a TTL in production.
    jobs: HashMap<u64, Job>,
    ready: BinaryHeap<(u8, Reverse<u64>, u64)>,
    delayed: BinaryHeap<Reverse<(Instant, u64)>>,
    counts: BTreeMap<String, Counts>,
    workers: BTreeMap<String, Worker>,
    queued: usize,
    max_queued: usize,
    refused: u64,
    stale: u64,
}

impl Broker {
    pub fn new(cfg: BrokerConfig) -> Self {
        let boot = axon::trace::unix_micros();
        Self {
            cfg,
            boot,
            jobs: HashMap::new(),
            ready: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            counts: BTreeMap::new(),
            workers: BTreeMap::new(),
            queued: 0,
            max_queued: 0,
            refused: 0,
            stale: 0,
        }
    }

    /// Moves a job to `to`, keeping the counts, the workers' holds and the heaps in step.
    fn set(&mut self, id: u64, to: State, now: Instant) {
        let Some(job) = self.jobs.get_mut(&id) else { return };
        let from = std::mem::replace(&mut job.state, to);
        let worker = if from == State::Running { job.worker.take() } else { None };
        if let Some(w) = worker.and_then(|w| self.workers.get_mut(&w)) {
            w.holds.remove(&id);
            w.load.running = w.holds.len() as u64;
        }
        self.tally(id, from, -1);
        self.enter(id, now);
    }

    /// Counts a job in its current state and files it where that state is served from.
    fn enter(&mut self, id: u64, now: Instant) {
        let Some(job) = self.jobs.get_mut(&id) else { return };
        let state = job.state;
        match state {
            State::Pending => self.ready.push((job.spec.priority, Reverse(job.seq), id)),
            State::Delayed if job.deliveries == 0 => {
                self.delayed.push(Reverse((now + Duration::from_millis(job.spec.delay_ms), id)));
            }
            State::Delayed => {
                let wait = backoff(&self.cfg, id, job.failures);
                self.delayed.push(Reverse((now + wait, id)));
            }
            State::Done => job.spec.payload = String::new(),
            State::Running | State::Dead => {}
        }
        self.tally(id, state, 1);
    }

    fn tally(&mut self, id: u64, state: State, delta: i64) {
        let Some(job) = self.jobs.get(&id) else { return };
        let counts = self.counts.entry(job.spec.queue.clone()).or_default();
        let slot = match state {
            State::Pending => &mut counts.pending,
            State::Delayed => &mut counts.delayed,
            State::Running => &mut counts.running,
            State::Done => &mut counts.done,
            State::Dead => &mut counts.dead,
        };
        *slot = slot.saturating_add_signed(delta);
        if matches!(state, State::Pending | State::Delayed) {
            self.queued = self.queued.saturating_add_signed(delta as isize);
            self.max_queued = self.max_queued.max(self.queued);
        }
    }

    fn enqueue(&mut self, jobs: Vec<NewJob>, now: Instant) -> usize {
        let offered = jobs.len();
        let mut accepted = 0;
        for spec in jobs {
            if !self.jobs.contains_key(&spec.id) {
                if self.queued >= self.cfg.capacity {
                    break;
                }
                let (id, state) = (spec.id, if spec.delay_ms > 0 { State::Delayed } else { State::Pending });
                let (left, seq) = (spec.max_attempts.max(1), self.jobs.len() as u64);
                let job = Job { spec, state, deliveries: 0, failures: 0, left, worker: None, last_error: None, seq };
                self.jobs.insert(id, job);
                self.enter(id, now);
            }
            accepted += 1;
        }
        self.refused += (offered - accepted) as u64;
        accepted
    }

    fn fetch(&mut self, worker: String, want: u32, running: Vec<Ticket>, outcomes: Vec<Outcome>) -> Vec<Delivery> {
        let now = Instant::now();
        for outcome in outcomes {
            self.outcome(outcome, &worker, now);
        }
        let w = self.workers.entry(worker.clone()).or_default();
        w.seen = Some(now);
        // A delivery the broker gave this worker that the worker no longer runs was lost on the way: its answer
        // never arrived, or the worker restarted.
        let listed: BTreeSet<Ticket> = running.into_iter().collect();
        let boot = self.boot;
        let lost: Vec<u64> = w
            .holds
            .iter()
            .copied()
            .filter(|&id| {
                !self.jobs.get(&id).is_some_and(|j| listed.contains(&Ticket { boot, id, delivery: j.deliveries }))
            })
            .collect();
        self.take_back(&worker, lost, now);
        let mut out = Vec::new();
        while out.len() < want as usize {
            let Some((_, _, id)) = self.ready.pop() else { break };
            let Some(job) = self.jobs.get_mut(&id).filter(|j| j.state == State::Pending) else { continue };
            job.deliveries += 1;
            job.worker = Some(worker.clone());
            let d = Delivery {
                ticket: Ticket { boot, id, delivery: job.deliveries },
                queue: job.spec.queue.clone(),
                payload: job.spec.payload.clone(),
                failures: job.failures,
            };
            self.set(id, State::Running, now);
            let w = self.workers.entry(worker.clone()).or_default();
            w.holds.insert(id);
            w.load.running = w.holds.len() as u64;
            out.push(d);
        }
        out
    }

    fn outcome(&mut self, o: Outcome, worker: &str, now: Instant) {
        let Ticket { boot, id, delivery } = o.ticket;
        let current = boot == self.boot
            && self.jobs.get(&id).is_some_and(|j| {
                j.state == State::Running && j.deliveries == delivery && j.worker.as_deref() == Some(worker)
            });
        if !current {
            self.stale += 1;
            return;
        }
        let Some(job) = self.jobs.get_mut(&id) else { return };
        let load = &mut self.workers.entry(worker.to_owned()).or_default().load;
        let to = match o.error {
            None => {
                load.done += 1;
                State::Done
            }
            Some(error) => {
                load.failed += 1;
                job.failures += 1;
                job.left = job.left.saturating_sub(1);
                job.last_error = Some(error);
                if job.left == 0 { State::Dead } else { State::Delayed }
            }
        };
        self.set(id, to, now);
    }

    fn take_back(&mut self, worker: &str, ids: Vec<u64>, now: Instant) {
        if let Some(w) = self.workers.get_mut(worker) {
            w.load.lost += ids.len() as u64;
        }
        for id in ids {
            self.set(id, State::Pending, now);
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        while let Some(&Reverse((due, id))) = self.delayed.peek() {
            if due > now {
                break;
            }
            self.delayed.pop();
            if self.jobs.get(&id).is_some_and(|j| j.state == State::Delayed) {
                self.set(id, State::Pending, now);
            }
        }
        let silent: Vec<(String, Vec<u64>)> = self
            .workers
            .iter()
            .filter(|(_, w)| !w.holds.is_empty() && w.seen.is_some_and(|s| now - s > self.cfg.visibility))
            .map(|(name, w)| (name.clone(), w.holds.iter().copied().collect()))
            .collect();
        for (worker, ids) in silent {
            self.take_back(&worker, ids, now);
        }
    }

    fn view(&self, id: u64) -> Option<JobView> {
        self.jobs.get(&id).map(|j| JobView {
            id,
            queue: j.spec.queue.clone(),
            state: j.state,
            deliveries: j.deliveries,
            failures: j.failures,
            last_error: j.last_error.clone(),
        })
    }

    fn answer(&mut self, call: Call) -> Answer {
        let now = Instant::now();
        match call {
            Call::Enqueue(jobs) => Answer::Enqueued { accepted: self.enqueue(jobs, now) },
            Call::Fetch { worker, want, running, outcomes } => {
                Answer::Jobs(self.fetch(worker, want, running, outcomes))
            }
            Call::Status => Answer::Status(Box::new(Status {
                queues: self.counts.clone(),
                workers: self.workers.iter().map(|(n, w)| (n.clone(), w.load.clone())).collect(),
                capacity: self.cfg.capacity,
                max_queued: self.max_queued,
                refused: self.refused,
                stale: self.stale,
            })),
            Call::Inspect(ids) => Answer::Views(ids.into_iter().filter_map(|id| self.view(id)).collect()),
            Call::Dead { limit } => {
                let mut dead: Vec<u64> =
                    self.jobs.iter().filter(|(_, j)| j.state == State::Dead).map(|(id, _)| *id).collect();
                dead.sort_unstable();
                Answer::Views(dead.into_iter().take(limit).filter_map(|id| self.view(id)).collect())
            }
            Call::Retry(ids) => {
                let mut moved = 0;
                for id in ids {
                    if let Some(job) = self.jobs.get_mut(&id).filter(|j| j.state == State::Dead) {
                        job.left = job.spec.max_attempts.max(1);
                        self.set(id, State::Pending, now);
                        moved += 1;
                    }
                }
                Answer::Retried(moved)
            }
        }
    }
}

impl Actor for Broker {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        cx.schedule_every(self.cfg.tick, || Msg::Tick).map_err(|e| format!("{:?}", e.reason))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        match msg {
            Msg::Tick => self.tick(),
            Msg::Call(call, reply) => {
                let answer = self.answer(call);
                if let Some(reply) = reply {
                    // A caller that gave up, or an answer that does not encode, is not the broker's failure.
                    let _ = reply.send(answer);
                }
            }
        }
        Ok(())
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Tick => "tick",
            Msg::Call(Call::Enqueue(_), _) => "enqueue",
            Msg::Call(Call::Fetch { .. }, _) => "fetch",
            Msg::Call(_, _) => "admin",
        }
    }
}

fn backoff(cfg: &BrokerConfig, id: u64, failures: u32) -> Duration {
    let exp = cfg.backoff.saturating_mul(1 << failures.saturating_sub(1).min(20));
    exp.min(cfg.max_backoff).mul_f64(0.5 + unit(id ^ u64::from(failures) << 48))
}

/// A number in 0..1 from `x`, the same every time: the jitter needs spread, not secrecy.
fn unit(x: u64) -> f64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) >> 11) as f64 / (1u64 << 53) as f64
}
