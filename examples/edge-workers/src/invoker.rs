//! A runner's request path: the [`Invoker`] actor takes each request a gateway sends, runs it in the engine off its
//! own thread, answers, and publishes the run's record on the node's log stream.
//!
//! Nothing queues here. A worker has `per_worker` runs at once on a node and the node `in_flight` in all; a
//! request past either is answered `Busy` at once, and the gateway takes it to another runner. A worker nobody
//! has called for `idle` is dropped from the engine and loaded again from the node's disk by the next request,
//! which waits for it with at most `waiting` others.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axon::{
    Actor, ActorHandle, AtomicHistogram, Context, JobExecutor, MailboxConfig, MailboxLimits, RequestError, TraceContext,
};
use axon_cluster::Reply;
use axon_stream::{DeliveryPolicy, StreamError, StreamKey, StreamNode};
use tokio::time::Instant;

use crate::engine::{Engine, Failed, Failure, Host, Outcome, Request, Scope, Usage, guarded, submit};
use crate::follower::{self, Serving};
use crate::object::{Caller, Reach, UNTIMED};
use crate::wire::{
    Ask, HEAD_ROOM, Invocation, LogRecord, MAX_WORKERS, NodeStats, OUTCOMES, Ran, Told, WorkerStats, answer,
    headers_fit, logs, trace_id,
};

#[derive(Clone, Debug)]
pub struct InvokerConfig {
    /// Runs of one worker at once on this node.
    pub per_worker: usize,
    /// Runs of every worker at once on this node: the actor's jobs.
    pub in_flight: usize,
    /// Requests that wait, per worker, while it is loaded again after it went idle.
    pub waiting: usize,
    /// A worker not called for this long is dropped from the engine. Its bundle stays on disk.
    pub idle: Duration,
    /// Console output kept in a run's log record; the rest of what the engine kept is cut.
    pub log_bytes: usize,
    /// Where the engine runs each request, so the worker's CPU time is not taken from the threads that serve the
    /// cluster's sessions. `None` runs it where the actor's other jobs run, as a World needs.
    pub runtime: Option<tokio::runtime::Handle>,
}

impl Default for InvokerConfig {
    fn default() -> Self {
        Self {
            per_worker: 32,
            in_flight: 256,
            waiting: 32,
            idle: Duration::from_secs(300),
            log_bytes: 16 << 10,
            runtime: None,
        }
    }
}

#[derive(Default)]
struct Counters {
    outcomes: [AtomicU64; OUTCOMES.len()],
    busy: AtomicU64,
    cold_starts: AtomicU64,
    in_flight: AtomicU64,
    cpu: AtomicHistogram,
    wall: AtomicHistogram,
    /// A run ended since the invoker last swept. An object's run is not the invoker's, and this is how it sees
    /// that the worker is in use here.
    used: AtomicBool,
}

#[derive(Default)]
struct Shared {
    workers: RwLock<BTreeMap<String, Arc<Counters>>>,
    abandoned: AtomicU64,
    logs_published: AtomicU64,
    logs_dropped: AtomicU64,
}

/// A runner's counters since it started, per worker. A clone shares them; `Ask::Stats` reads them from afar.
#[derive(Clone, Default)]
pub struct Stats(Arc<Shared>);

impl Stats {
    fn of(&self, worker: &str) -> Arc<Counters> {
        let known = self.0.workers.read().unwrap_or_else(|e| e.into_inner()).get(worker).cloned();
        known.unwrap_or_else(|| {
            let mut workers = self.0.workers.write().unwrap_or_else(|e| e.into_inner());
            workers.entry(worker.to_owned()).or_default().clone()
        })
    }

    fn keep(&self, keep: impl Fn(&str) -> bool) {
        self.0.workers.write().unwrap_or_else(|e| e.into_inner()).retain(|worker, _| keep(worker));
    }

    pub fn snapshot(&self, node: &str) -> NodeStats {
        let workers = self.0.workers.read().unwrap_or_else(|e| e.into_inner());
        let one = |c: &Arc<Counters>| WorkerStats {
            outcomes: std::array::from_fn(|i| c.outcomes[i].load(Relaxed)),
            busy: c.busy.load(Relaxed),
            cold_starts: c.cold_starts.load(Relaxed),
            in_flight: c.in_flight.load(Relaxed),
            cpu: c.cpu.snapshot(),
            wall: c.wall.snapshot(),
        };
        NodeStats {
            node: node.to_owned(),
            workers: workers.iter().map(|(name, c)| (name.clone(), one(c))).collect(),
            serving: BTreeMap::new(),
            abandoned: self.0.abandoned.load(Relaxed),
            logs_published: self.0.logs_published.load(Relaxed),
            logs_dropped: self.0.logs_dropped.load(Relaxed),
        }
    }
}

pub enum Msg {
    Ask(Ask, Option<Reply<Told>>),
    Done(Box<Done>),
    /// An idle worker is back in the engine, or could not be brought back, or the wait for it ran out.
    Warmed(String, Result<bool, RequestError>),
    Sweep,
    /// The log stream is open, or could not be.
    Opened(Result<(), StreamError>),
}

pub struct Done {
    /// Which of `Invoker::replies` it answers.
    run: u64,
    worker: String,
    version: u32,
    method: String,
    url: String,
    result: Result<Outcome, Failed>,
}

struct Worker {
    running: usize,
    /// On the actor's clock.
    last_used: Duration,
    /// Until when the warm in flight waits, on tokio's clock as a request's deadline is.
    warming: Option<Instant>,
    waiting: VecDeque<(Invocation, Option<Reply<Told>>)>,
    counters: Arc<Counters>,
}

/// A run that ended, of a worker or of an object.
pub struct Run {
    pub worker: String,
    pub version: u32,
    pub method: String,
    pub url: String,
}

/// Where a runner's runs end: its counters, and its log stream. A clone shares them.
#[derive(Clone)]
pub struct Ending {
    node: String,
    streams: StreamNode,
    log: StreamKey,
    log_bytes: usize,
    stats: Stats,
}

impl Ending {
    /// Counts the run for its worker, publishes its record, and gives what its caller is told: the response, or
    /// the failure of `who` ("worker", "object") as one.
    pub fn of(&self, who: &str, run: Run, result: Result<Outcome, Failed>) -> Ran {
        let Run { worker, version, method, url } = run;
        let result = match result {
            // The engine held the body to what a reply carries. Headers that leave it no room fail the same way.
            Ok(outcome) if !headers_fit(&outcome.headers) => {
                Err(Failed { failure: Failure::ResponseTooLarge, usage: outcome.usage })
            }
            result => result,
        };
        let (failure, status, headers, body, usage) = match result {
            Ok(outcome) => (None, outcome.status, outcome.headers, outcome.body, outcome.usage),
            Err(failed) => {
                let text = format!("the {who} {}\n", failed.failure);
                (Some(failed.failure), 0, Vec::new(), text.into(), failed.usage)
            }
        };
        let outcome = outcome_of(failure.as_ref());
        let status = failure.as_ref().map_or(status, Failure::status);
        let (cpu_us, wall_us) = (usage.cpu.as_micros() as u64, usage.wall.as_micros() as u64);
        let counters = self.stats.of(&worker);
        counters.outcomes[outcome].fetch_add(1, Relaxed);
        counters.cpu.record(usage.cpu);
        counters.wall.record(usage.wall);
        counters.used.store(true, Relaxed);

        let mut kept = 0;
        let mut logs = usage.logs;
        let whole = logs.len();
        logs.retain(|line| {
            kept += line.len();
            kept <= self.log_bytes
        });
        let record = LogRecord {
            at_ms: axon::trace::unix_micros() / 1000,
            worker,
            version,
            node: self.node.clone(),
            trace: TraceContext::current().map(trace_id),
            method,
            url,
            status,
            outcome: OUTCOMES[outcome].to_owned(),
            cpu_us,
            wall_us,
            truncated: logs.len() < whole,
            logs,
        };
        // The ring gives up its oldest records for this one. A refusal, a stream that did not open, loses the
        // record and nothing else.
        let published = self.streams.publish(&self.log, vec![record.record()]).is_ok();
        let stats = &self.stats.0;
        if published { &stats.logs_published } else { &stats.logs_dropped }.fetch_add(1, Relaxed);
        let failure = failure.is_some().then(|| OUTCOMES[outcome].to_owned());
        Ran { status, headers, version, cpu_us, wall_us, failure, body }
    }
}

pub struct Invoker<E: Engine> {
    engine: E,
    serving: Serving<E::Version>,
    follower: ActorHandle<follower::Msg<E::Version>>,
    ending: Ending,
    cfg: InvokerConfig,
    workers: BTreeMap<String, Worker>,
    /// Runs in hand, of every worker.
    running: usize,
    /// Who each run in hand answers. Kept here and not in the run's job, so a job the actor could not start still
    /// has its caller to tell.
    replies: BTreeMap<u64, Option<Reply<Told>>>,
    next_run: u64,
    /// The node's way to the objects, for its workers' calls and for what it hosts of them.
    objects: Caller,
    /// Where the engine runs: `cfg.runtime`'s.
    jobs: Option<JobExecutor>,
}

fn outcome_of(failure: Option<&Failure>) -> usize {
    match failure {
        None => 0,
        Some(Failure::Exception(_)) => 1,
        Some(Failure::CpuLimit) => 2,
        Some(Failure::MemoryLimit) => 3,
        Some(Failure::WallTimeout) => 4,
        Some(Failure::Load(_) | Failure::Runtime(_)) => 5,
        Some(Failure::ResponseTooLarge) => 6,
        Some(Failure::Storage(_)) => 7,
    }
}

impl<E: Engine + Clone> Invoker<E> {
    pub fn new(
        node: &str,
        engine: E,
        serving: Serving<E::Version>,
        follower: ActorHandle<follower::Msg<E::Version>>,
        streams: StreamNode,
        objects: Caller,
        cfg: InvokerConfig,
    ) -> Self {
        let (log, log_bytes) = (logs(node), cfg.log_bytes);
        let ending = Ending { node: node.to_owned(), streams, log, log_bytes, stats: Stats::default() };
        Self {
            engine,
            serving,
            follower,
            ending,
            jobs: cfg.runtime.clone().map(JobExecutor::tokio),
            cfg,
            workers: BTreeMap::new(),
            running: 0,
            replies: BTreeMap::new(),
            next_run: 0,
            objects,
        }
    }

    pub fn stats(&self) -> Stats {
        self.ending.stats.clone()
    }

    /// For the objects this runner hosts: their runs end where the workers' do.
    pub fn ending(&self) -> Ending {
        self.ending.clone()
    }

    /// The mailbox this actor needs: a job per run, and a request weighed with its body.
    pub fn mailbox(cfg: &InvokerConfig) -> MailboxConfig<Msg> {
        // More jobs than runs: the wait for an idle worker's load is one too, one a worker at most.
        let limits = MailboxLimits { jobs: cfg.in_flight + MAX_WORKERS, ..MailboxLimits::default() };
        MailboxConfig::from(limits).with_weight(|msg: &Msg| {
            size_of::<Msg>()
                + match msg {
                    Msg::Ask(Ask::Run(invocation), _) => {
                        let headers = invocation.headers.iter().map(|(name, value)| name.len() + value.len());
                        invocation.body.len() + invocation.url.len() + headers.sum::<usize>()
                    }
                    Msg::Done(done) => done.result.as_ref().map_or(0, |outcome| outcome.body.len()),
                    _ => 0,
                }
        })
    }

    fn worker(&mut self, name: &str, now: Duration) -> &mut Worker {
        let stats = &self.ending.stats;
        self.workers.entry(name.to_owned()).or_insert_with(|| Worker {
            running: 0,
            last_used: now,
            warming: None,
            waiting: VecDeque::new(),
            counters: stats.of(name),
        })
    }

    /// Runs the request, or says at once why not. `may_wait` is false for one that already waited for a load.
    fn start(
        &mut self,
        invocation: Invocation,
        reply: Option<Reply<Told>>,
        may_wait: bool,
        cx: &mut Context<'_, Self>,
    ) {
        if reply.as_ref().is_some_and(Reply::is_abandoned) {
            self.ending.stats.0.abandoned.fetch_add(1, Relaxed);
            return;
        }
        let Some(current) = self.serving.get(&invocation.worker) else { return answer(reply, Told::NotServed) };
        let picked = current.pick(invocation.sample).map(|(version, prepared)| (version.clone(), prepared.clone()));
        let (per_worker, waiting, full) = (self.cfg.per_worker, self.cfg.waiting, self.running >= self.cfg.in_flight);
        let now = cx.now();
        let worker = self.worker(&invocation.worker, now);
        worker.last_used = now;
        let counters = worker.counters.clone();
        if full || worker.running >= per_worker {
            counters.busy.fetch_add(1, Relaxed);
            return answer(reply, Told::Busy);
        }
        let Some((version, prepared)) = picked else {
            if !may_wait || worker.waiting.len() >= waiting {
                counters.busy.fetch_add(1, Relaxed);
                return answer(reply, Told::Busy);
            }
            let name = invocation.worker.clone();
            worker.waiting.push_back((invocation, reply));
            if worker.warming.is_none() {
                counters.cold_starts.fetch_add(1, Relaxed);
                self.warm(name, cx);
            }
            return;
        };
        worker.running += 1;

        let mut limits = version.limits;
        // A run past what its caller waits for would be answered to no one.
        if let Some(left) = reply.as_ref().and_then(Reply::time_left) {
            limits.wall = limits.wall.min(left);
        }
        // A reply is one frame: the engine reads no more of a response than that carries.
        if let Some(reply) = &reply {
            limits.response_bytes = limits.response_bytes.min(reply.max_payload().saturating_sub(HEAD_ROOM));
        }
        let Invocation { worker: name, method, url, headers, body, .. } = invocation;
        let request = Request { method: method.clone(), url: url.clone(), headers, body };
        let (engine, run) = (self.engine.clone(), self.next_run);
        let (worker, caller) = (name.clone(), self.objects.clone());
        let job = async move {
            let deadline = tokio::time::Instant::now() + limits.wall;
            let reach: Arc<dyn Host> = Arc::new(Reach { caller, worker: worker.clone(), deadline, own: None });
            let scope = Scope { object: None, host: Some(reach) };
            // A run that panicked in the engine is that request's failure, not the actor's end.
            let ran = guarded(engine.invoke_in(&prepared, request, limits, scope)).await;
            let result = ran.unwrap_or_else(|failure| Err(Failed { failure, usage: Usage::default() }));
            Msg::Done(Box::new(Done { run, worker, version: version.id, method, url, result }))
        };
        // The actor's jobs are also its waits for idle workers, and the executor's queue has a bound of its
        // own: a run that cannot start was not run, and its caller may take it elsewhere.
        if submit(cx, self.jobs.as_ref(), job).is_err() {
            if let Some(worker) = self.workers.get_mut(&name) {
                worker.running -= 1;
            }
            counters.busy.fetch_add(1, Relaxed);
            return answer(reply, Told::Busy);
        }
        counters.in_flight.fetch_add(1, Relaxed);
        self.running += 1;
        self.next_run += 1;
        self.replies.insert(run, reply);
    }

    /// Asks for an idle worker back, for as long as the longest wait of the requests waiting for it.
    fn warm(&mut self, name: String, cx: &mut Context<'_, Self>) {
        let Some(worker) = self.workers.get_mut(&name) else { return };
        let waits =
            worker.waiting.iter().map(|(_, reply)| reply.as_ref().and_then(Reply::time_left).unwrap_or(UNTIMED));
        let left = waits.max().unwrap_or(UNTIMED);
        worker.warming = Some(Instant::now() + left);
        let (follower, asked) = (self.follower.clone(), name.clone());
        let warm = async move {
            let back = follower::warm(&follower, &asked, left).await;
            Msg::Warmed(asked, back.map(|current| current.is_some()))
        };
        if cx.submit_future(warm).is_err() {
            self.warmed(&name, Ok(false), cx);
        }
    }

    fn warmed(&mut self, name: &str, back: Result<bool, RequestError>, cx: &mut Context<'_, Self>) {
        let Some(worker) = self.workers.get_mut(name) else { return };
        let until = worker.warming.take();
        let waiting = std::mem::take(&mut worker.waiting);
        if matches!(back, Err(RequestError::Deadline { .. })) {
            // The load outlasted the wait asked for: a request that came later, with a later deadline, waits on.
            let (now, until) = (Instant::now(), until.unwrap_or_else(Instant::now));
            let later =
                |reply: &Reply<Told>| !reply.is_abandoned() && reply.time_left().is_some_and(|left| now + left > until);
            let (on, late): (VecDeque<_>, VecDeque<_>) =
                waiting.into_iter().partition(|(_, reply)| reply.as_ref().is_some_and(later));
            late.into_iter().for_each(|(_, reply)| answer(reply, Told::Busy));
            worker.waiting = on;
            if !worker.waiting.is_empty() {
                self.warm(name.to_owned(), cx);
            }
            return;
        }
        let back = matches!(back, Ok(true));
        for (invocation, reply) in waiting {
            match back {
                true => self.start(invocation, reply, false, cx),
                false => answer(reply, Told::Busy),
            }
        }
    }

    fn done(&mut self, done: Done, cx: &mut Context<'_, Self>) {
        let Done { run, worker, version, method, url, result } = done;
        self.running = self.running.saturating_sub(1);
        if let Some(worker) = self.workers.get_mut(&worker) {
            worker.running = worker.running.saturating_sub(1);
            worker.last_used = cx.now();
            worker.counters.in_flight.fetch_sub(1, Relaxed);
        }
        let ran = self.ending.of("worker", Run { worker, version, method, url }, result);
        answer(self.replies.remove(&run).flatten(), Told::Ran(ran));
    }

    /// Lets go of every worker nobody called for `idle`, and of what is known of one that is gone.
    fn sweep(&mut self, cx: &mut Context<'_, Self>) {
        let now = cx.now();
        let served = self.serving.all();
        for (name, current) in served.iter().filter(|(_, current)| current.is_warm()) {
            let idle = self.cfg.idle;
            let worker = self.worker(name, now);
            if worker.counters.used.swap(false, Relaxed) {
                worker.last_used = now;
            }
            let quiet = worker.running == 0 && worker.waiting.is_empty() && worker.warming.is_none();
            if quiet && now.saturating_sub(worker.last_used) >= idle {
                // A full mailbox there leaves it for the next sweep.
                drop(self.follower.tell(follower::Msg::Evict(current.manifest.name.clone())));
            }
        }
        self.workers
            .retain(|name, worker| served.contains_key(name) || worker.running > 0 || !worker.waiting.is_empty());
        // An object's run counts for a worker this actor may never have run itself.
        let workers = &self.workers;
        self.ending.stats.keep(|name| workers.contains_key(name) || served.contains_key(name));
    }
}

impl<E: Engine + Clone> Actor for Invoker<E> {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        // A start time is a generation that a restart raises.
        let generation = axon::trace::unix_micros() / 1000;
        let Ending { streams, log, .. } = &self.ending;
        let open = streams.open_stream(log, generation, DeliveryPolicy::ReplayRequired);
        let open = cx.submit_future(async move { Msg::Opened(open.await) }).map_err(|e| format!("{:?}", e.reason))?;
        // Part of the start: the log stream is open before a gateway can find this actor.
        cx.pause_until(&open).map_err(|e| format!("{e:?}"))?;
        cx.schedule_every((self.cfg.idle / 4).max(Duration::from_millis(1)), || Msg::Sweep)
            .map_err(|e| format!("{:?}", e.reason))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Ask(Ask::Run(invocation), reply) => self.start(invocation, reply, true, cx),
            Msg::Ask(Ask::Stats, reply) => {
                let mut stats = self.ending.stats.snapshot(&self.ending.node);
                stats.serving =
                    self.serving.all().iter().map(|(name, current)| (name.clone(), current.served())).collect();
                answer(reply, Told::Stats(stats));
            }
            Msg::Ask(Ask::Objects { prefix, from, limit }, reply) => {
                let (hosted, more) = self.objects.hosted(&prefix, from.as_deref(), (limit as usize).min(1000));
                answer(reply, Told::Objects(hosted, more));
            }
            Msg::Done(done) => self.done(*done, cx),
            Msg::Warmed(name, back) => self.warmed(&name, back, cx),
            Msg::Sweep => self.sweep(cx),
            Msg::Opened(opened) => opened.map_err(|e| format!("opening {:?}: {e:?}", self.ending.log))?,
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &axon::StopReason) {
        drop(self.ending.streams.close_stream(&self.ending.log));
    }
}
