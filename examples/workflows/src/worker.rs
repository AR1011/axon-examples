//! A worker: one actor per queue per node, polling each of the queue's partitions and running the attempts it gets
//! as jobs, at most `slots` at once. Each attempt says `Started` to its run first (a run that does not want it, or
//! has it on another worker, refuses, and it does not run), sends heartbeats when its step asks for them, and ends
//! with `Complete`. A drain stops the polls, gives back what it has not started (`Released`, which does not count as
//! an attempt) and lets what runs finish.
//!
//! [`WorkerWatch`] runs on each engine: when a worker node leaves the cluster, the runs with an attempt started there
//! fail that attempt, and try again.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, Context, JobExecutor, JobHandle};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, KeyedRefs, NodeChange, NodeRecord, Nodes, Permits, Placement, Quotas, Rejection,
    Reply, WireProtocol,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::journal::entry::{decode, encode};
use crate::node::RUN_ARGS;
use crate::queue::{QUEUE_ARGS, QueueAnswer, QueueApi, QueueCall, Task, partitions};
use crate::run::actor::OnWorkers;
use crate::run::{RunAnswer, RunApi, RunCall, call_idempotent};
use crate::workflow::{ActivityCx, App, Failure};

/// A poll waits this long in its queue for a task.
pub const POLL: Duration = Duration::from_secs(30);
/// A failed poll is made again after this.
const POLL_AGAIN: Duration = Duration::from_secs(1);
/// How long a worker tries to make `Started`, `Released` and `Complete` arrive.
const REPORT_WITHIN: Duration = Duration::from_secs(60);

pub fn worker_placement() -> Placement {
    Placement::default().require("worker", "yes")
}

/// The prefix of the label a worker node carries, `yes`, for each queue it serves.
pub const QUEUE_LABEL: &str = "queue.";
/// A worker node's slots, `queue:n,..`, in its metadata.
pub const SLOTS_KEY: &str = "wf.slots";
/// `yes` once a worker node drains before it stops.
pub const DRAINING_KEY: &str = "wf.draining";

/// The label a worker node carries for each queue it serves.
pub fn queue_label(queue: &str) -> String {
    format!("{QUEUE_LABEL}{queue}")
}

/// The name a worker node publishes its worker of `queue` under, for [`WorkerApi`].
pub fn worker_name(queue: &str) -> String {
    format!("worker-{queue}")
}

/// The queues a node's labels say it serves.
pub fn served_queues(record: &NodeRecord) -> impl Iterator<Item = &str> {
    record.metadata.iter().filter(|(_, v)| *v == "yes").filter_map(|(k, _)| k.strip_prefix(QUEUE_LABEL))
}

#[derive(Clone)]
pub struct WorkerConfig {
    pub queue: String,
    /// Attempts at once.
    pub slots: u32,
    /// Where the attempts run; `None`: as the actor's jobs on the system's runtime.
    pub executor: Option<JobExecutor>,
    /// Attempts at once across the cluster, keyed by the queue's name: one permit each, moved into its job.
    pub permits: Option<(Quotas, Permits)>,
}

impl WorkerConfig {
    pub fn new(queue: impl Into<String>, slots: u32) -> Self {
        WorkerConfig { queue: queue.into(), slots: slots.max(1), executor: None, permits: None }
    }
}

/// What a worker of one queue says of itself, for the console.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStats {
    pub slots: u32,
    pub running: u32,
    /// Attempts that ended in a failure in the last minute.
    pub failed_minute: u32,
    /// Since the last poll came back: 0 while one is out, `None` before the first.
    pub last_poll_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerCall {
    Stats,
}

/// A worker's stats over the network: each worker node publishes its worker of each queue as [`worker_name`].
pub struct WorkerApi;

impl WireProtocol for WorkerApi {
    type Message = WorkerCall;
    type Reply = WorkerStats;
    const ID: u64 = 0x5746_574b; // "WFWK"
    const SCHEMA: u16 = 1;

    fn encode(call: WorkerCall) -> Result<Bytes, CallError> {
        Ok(encode(&call))
    }

    fn decode_reply(body: Bytes) -> Result<WorkerStats, CallError> {
        decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for WorkerApi {
    type Mailbox = WorkerMsg;

    fn encode_reply(stats: WorkerStats) -> Result<Bytes, CallError> {
        Ok(encode(&stats))
    }

    fn decode(body: Bytes, reply: Option<Reply<WorkerStats>>) -> Result<WorkerMsg, Rejection> {
        let WorkerCall::Stats = decode(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(WorkerMsg::Stats(reply))
    }
}

pub enum WorkerMsg {
    Polled(usize, Result<QueueAnswer, String>),
    /// An attempt's job ended, and whether its activity failed.
    Ran(Task, bool),
    /// A `Released` or `Unpoll` job ended.
    Sent,
    PollAgain,
    /// Stop polling, give back what is not started, finish what runs: then the answer, and the actor stops.
    Drain(oneshot::Sender<()>),
    Stats(Option<Reply<WorkerStats>>),
}

pub struct Worker {
    cfg: WorkerConfig,
    app: Arc<App>,
    node: Arc<str>,
    /// How the queues know this worker's polls: `<node>/<queue>`.
    poller: String,
    runs: KeyedRefs<RunApi>,
    queues: KeyedRefs<QueueApi>,
    partitions: Vec<String>,
    /// The partitions with a poll out.
    polling: BTreeSet<usize>,
    running: BTreeMap<(String, u32, u32), JobHandle>,
    sending: usize,
    again: bool,
    drained: Option<oneshot::Sender<()>>,
    draining: bool,
    /// When a poll last came back, on the actor's clock.
    last_poll: Option<Duration>,
    /// Failed attempts per second of the actor's clock, the last minute's.
    failures: VecDeque<(u64, u32)>,
}

impl Worker {
    pub fn new(cfg: WorkerConfig, app: Arc<App>, cluster: &Cluster) -> Worker {
        let node = cluster.record().id;
        Worker {
            partitions: partitions(&cfg.queue),
            poller: format!("{node}/{}", cfg.queue),
            node: node.into(),
            cfg,
            app,
            runs: cluster.keyed_refs::<RunApi>(crate::run::RUN, &crate::node::run_placement(), 1_024),
            queues: cluster.keyed_refs::<QueueApi>(crate::queue::QUEUE, &crate::queue::queue_placement(), 64),
            polling: BTreeSet::new(),
            running: BTreeMap::new(),
            sending: 0,
            again: false,
            drained: None,
            draining: false,
            last_poll: None,
            failures: VecDeque::new(),
        }
    }

    /// Counts a failed attempt at `now`, and forgets those of more than a minute ago.
    fn failed(&mut self, now: Duration) {
        let second = now.as_secs();
        match self.failures.back_mut() {
            Some((at, n)) if *at == second => *n += 1,
            _ => self.failures.push_back((second, 1)),
        }
        while self.failures.front().is_some_and(|(at, _)| at + 60 <= second) {
            self.failures.pop_front();
        }
    }

    fn stats(&self, now: Duration) -> WorkerStats {
        let second = now.as_secs();
        WorkerStats {
            slots: self.cfg.slots,
            running: self.running.len() as u32,
            failed_minute: self.failures.iter().filter(|(at, _)| at + 60 > second).map(|(_, n)| n).sum(),
            last_poll_ms: match self.polling.is_empty() {
                true => self.last_poll.map(|at| now.saturating_sub(at).as_millis() as u64),
                false => Some(0),
            },
        }
    }

    fn free(&self) -> u32 {
        self.cfg.slots.saturating_sub(self.running.len() as u32)
    }

    /// A poll on each partition without one, while there are free slots.
    fn poll(&mut self, cx: &mut Context<'_, Self>) {
        if self.draining || self.free() == 0 {
            return;
        }
        // Each partition asks for a share of the free slots; tasks past them go back.
        let ask = self.free().div_ceil(self.partitions.len() as u32);
        for (p, key) in self.partitions.iter().enumerate() {
            if self.polling.contains(&p) {
                continue;
            }
            let queue = self.queues.get(key, QUEUE_ARGS);
            let worker = self.poller.clone();
            let work = async move {
                let answer = queue.request_within(QueueCall::Poll { worker, free: ask }, POLL).await;
                WorkerMsg::Polled(p, answer.map_err(|e| e.to_string()))
            };
            if cx.submit_future(work).is_ok() {
                self.polling.insert(p);
            }
        }
    }

    /// Runs `task`, or gives it back.
    fn take(&mut self, cx: &mut Context<'_, Self>, task: Task) {
        let key = (task.run.clone(), task.step, task.attempt);
        // The run offers an attempt again until it hears `Started`: one already running here is not run twice.
        if self.running.contains_key(&key) {
            return;
        }
        let permit = match (&self.cfg.permits, self.draining || self.free() == 0) {
            (_, true) => return self.release(cx, task),
            (None, false) => None,
            (Some((quotas, permits)), false) => match quotas.acquire(&self.cfg.queue, permits) {
                Ok(permit) => Some(permit),
                Err(denied) => {
                    // No polls until a permit may be free, or each task would go back and forth at once.
                    if !self.again {
                        let wait = denied.retry_after().max(POLL_AGAIN);
                        self.again = cx.schedule_after(wait, WorkerMsg::PollAgain).is_ok();
                    }
                    return self.release(cx, task);
                }
            },
        };
        let run = self.runs.get(&task.run, RUN_ARGS);
        let make = self.app.activity_future(
            &task.name,
            &task.input,
            ActivityCx::new(&task.run, task.step, task.attempt)
                .redone(task.generation)
                .by(Arc::from(task.owner.as_str()), task.epoch),
        );
        let job = attempt(run, task.clone(), self.node.to_string(), make, permit);
        let job = match &self.cfg.executor {
            Some(executor) => cx.submit_future_on(executor, job).map_err(|e| e.reason),
            None => cx.submit_future(job).map_err(|e| e.reason),
        };
        match job {
            Ok(job) => drop(self.running.insert(key, job)),
            // Given back, so the run offers it at once and not after its next wait.
            Err(_) => self.release(cx, task),
        }
    }

    fn release(&mut self, cx: &mut Context<'_, Self>, task: Task) {
        let run = self.runs.get(&task.run, RUN_ARGS);
        let call = RunCall::Released {
            step: task.step,
            attempt: task.attempt,
            round: task.round,
            worker: self.node.to_string(),
        };
        let work = async move {
            // A lost `Released` costs only time: the run offers again on its own.
            let _ = call_idempotent(&run, call, Instant::now() + REPORT_WITHIN).await;
            WorkerMsg::Sent
        };
        if cx.submit_future(work).is_ok() {
            self.sending += 1;
        }
    }

    fn drain(&mut self, cx: &mut Context<'_, Self>) {
        self.draining = true;
        // `RUN_MUTATION=drain_abandons`: a drain drops the attempts that run, as a crash would.
        if crate::mutation() == "drain_abandons" {
            for (_, job) in std::mem::take(&mut self.running) {
                job.cancel();
            }
        }
        for key in &self.partitions {
            let queue = self.queues.get(key, QUEUE_ARGS);
            let worker = self.poller.clone();
            let work = async move {
                let _ = queue.request_within(QueueCall::Unpoll { worker }, Duration::from_secs(5)).await;
                WorkerMsg::Sent
            };
            if cx.submit_future(work).is_ok() {
                self.sending += 1;
            }
        }
    }

    /// Drained: no poll out, nothing runs, nothing to send.
    fn settle(&mut self, cx: &mut Context<'_, Self>) {
        if self.draining && self.polling.is_empty() && self.running.is_empty() && self.sending == 0 {
            if let Some(done) = self.drained.take() {
                let _ = done.send(());
            }
            cx.stop();
        }
    }
}

/// One attempt: `Started`, the activity with its heartbeats, then `Complete`.
async fn attempt(
    run: axon_cluster::KeyedRef<RunApi>,
    task: Task,
    worker: String,
    activity: Option<futures_util::future::BoxFuture<'static, Result<Bytes, Failure>>>,
    permit: Option<axon_cluster::Permit>,
) -> WorkerMsg {
    let (step, attempt) = (task.step, task.attempt);
    let started = RunCall::Started { step, attempt, worker };
    match call_idempotent(&run, started, Instant::now() + REPORT_WITHIN).await {
        Ok(RunAnswer::Accepted) => {}
        // Not wanted, or on another worker; or the run cannot be reached, and offers again.
        _ => return WorkerMsg::Ran(task, false),
    }
    let result = match activity {
        None => Err(Failure::fatal(format!("activity {} is not in this build", task.name))),
        Some(activity) => {
            let activity = axon::catch_panic(activity);
            tokio::pin!(activity);
            let every = task.heartbeat_ms.map(|ms| Duration::from_millis(ms / 2).max(Duration::from_millis(10)));
            loop {
                let beat = async {
                    match every {
                        Some(d) => tokio::time::sleep(d).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    out = &mut activity => break out.unwrap_or_else(|panic| {
                        Err(Failure::retry(format!("panic: {}", panic.message().unwrap_or("no message"))))
                    }),
                    () = beat => {
                        let wait = every.unwrap_or(POLL);
                        // The run no longer wants it: drop it, and say nothing.
                        if let Ok(RunAnswer::Beat { cancel: true }) =
                            run.request_within(RunCall::Heartbeat { step, attempt }, wait).await
                        {
                            return WorkerMsg::Ran(task, false);
                        }
                    }
                }
            }
        }
    };
    drop(permit);
    let failed = result.is_err();
    let complete = RunCall::Complete { step, attempt, result };
    let _ = call_idempotent(&run, complete, Instant::now() + REPORT_WITHIN).await;
    WorkerMsg::Ran(task, failed)
}

impl Actor for Worker {
    type Message = WorkerMsg;
    type Error = std::convert::Infallible;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        self.poll(cx);
        Ok(())
    }

    fn on_message(&mut self, msg: WorkerMsg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            WorkerMsg::Polled(p, answer) => {
                self.polling.remove(&p);
                self.last_poll = Some(cx.now());
                match answer {
                    Ok(QueueAnswer::Tasks(tasks)) => {
                        for task in tasks {
                            self.take(cx, task);
                        }
                    }
                    _ if !self.again => {
                        self.again = cx.schedule_after(POLL_AGAIN, WorkerMsg::PollAgain).is_ok();
                    }
                    _ => {}
                }
            }
            WorkerMsg::Ran(task, failed) => {
                self.running.remove(&(task.run, task.step, task.attempt));
                if failed {
                    self.failed(cx.now());
                }
            }
            WorkerMsg::Stats(reply) => {
                if let Some(reply) = reply {
                    drop(reply.send(self.stats(cx.now())));
                }
            }
            WorkerMsg::Sent => self.sending -= 1,
            WorkerMsg::PollAgain => self.again = false,
            WorkerMsg::Drain(done) => {
                self.drained = Some(done);
                if !self.draining {
                    self.drain(cx);
                }
            }
        }
        if !self.again {
            self.poll(cx);
        }
        self.settle(cx);
        Ok(())
    }
}

pub enum WatchMsg {
    Change(Option<NodeChange>),
    Sweep,
    Told,
}

/// On an engine: tells the runs of this node with an attempt on a worker node that left.
pub struct WorkerWatch {
    workers: Nodes,
    on_workers: Arc<OnWorkers>,
    runs: KeyedRefs<RunApi>,
    /// Worker nodes seen in the view: one missing from it later has left, though no change said so. One that left
    /// stays while a run still has an attempt on it.
    seen: BTreeSet<String>,
}

/// The sweep of the runs' worker nodes against the view, for a change missed.
const SWEEP: Duration = Duration::from_secs(5);

impl WorkerWatch {
    pub fn new(cluster: &Cluster, on_workers: Arc<OnWorkers>, runs: KeyedRefs<RunApi>) -> WorkerWatch {
        WorkerWatch { workers: cluster.nodes(&worker_placement()), on_workers, runs, seen: BTreeSet::new() }
    }

    fn left(&mut self, cx: &mut Context<'_, Self>, node: &str) {
        for id in self.on_workers.runs(node) {
            let run = self.runs.get(&id, RUN_ARGS);
            let call = RunCall::WorkerLeft { node: node.to_owned() };
            let work = async move {
                let _ = call_idempotent(&run, call, Instant::now() + REPORT_WITHIN).await;
                WatchMsg::Told
            };
            // A run not told now is told at the next sweep: it still has the node.
            let _ = cx.submit_future(work);
        }
    }
}

impl Actor for WorkerWatch {
    type Message = WatchMsg;
    type Error = std::convert::Infallible;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let _ = cx.attach(self.workers.changes(), WatchMsg::Change);
        let _ = cx.schedule_every(SWEEP, || WatchMsg::Sweep);
        Ok(())
    }

    fn on_message(&mut self, msg: WatchMsg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            WatchMsg::Change(Some(NodeChange::Joined(id))) => {
                self.seen.insert(id.to_string());
            }
            WatchMsg::Change(Some(NodeChange::Left(id))) => self.left(cx, &id),
            WatchMsg::Change(None) | WatchMsg::Told => {}
            WatchMsg::Sweep => {
                let now: BTreeSet<String> = self.workers.ids().iter().map(|id| id.to_string()).collect();
                let gone: Vec<String> = (self.on_workers.nodes().into_iter())
                    .filter(|node| self.seen.contains(node) && !now.contains(node))
                    .collect();
                // Told once a sweep while the runs still have it: a call that failed, or a job the mailbox refused,
                // is made again.
                for node in gone {
                    self.left(cx, &node);
                }
                let held = self.on_workers.nodes();
                self.seen.retain(|node| now.contains(node) || held.contains(node));
                self.seen.extend(now);
            }
        }
        Ok(())
    }
}
