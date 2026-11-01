//! A cluster-wide cron. One `Scheduler` runs at a time, as the keyed actor `cron/singleton` under a lease; it fires
//! jobs on wall-clock ticks into the `RunLog`, which runs each tick once and refuses any owner older than the newest
//! that claimed it. `main.rs` runs the roles as processes; `core` holds the logic the tests drive without them.

pub mod core;

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, ActorSystem, Backoff, Context, StopReason, TimerId, TimerReject};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, Discovery, NamedRef, Owner, PaxosConfig,
    PaxosLeases, Placement, Rejection, Reply, Retry, SpawnContext, Unaddressed, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use crate::core::{Fire, Job, Outcome, Plan, RunLog, audit};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub const KIND: &str = "cron";
pub const KEY: &str = "singleton";
/// The name the run log is published under.
pub const LOG: &str = "runlog";
/// Most fires in one request, so a long catch-up stays well inside one frame. The rest follow once it is answered.
const MAX_BATCH: usize = 2048;
const MAX_JOBS: usize = 256;

/// `name:every_ms:offset_ms:max_runs`, comma separated, e.g. `report:60000:0:1,beat:1000:0:1024`.
pub fn parse_jobs(spec: &str) -> std::result::Result<Vec<Job>, String> {
    let jobs = spec
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let parts: Vec<&str> = s.trim().split(':').collect();
            let [name, every, offset, runs] = parts[..] else {
                return Err(format!("job {s:?}: want name:every_ms:offset_ms:max_runs"));
            };
            let num = |v: &str| v.parse::<u64>().map_err(|e| format!("job {s:?}: {e}"));
            let job = Job { name: name.into(), every_ms: num(every)?, offset_ms: num(offset)?, max_runs: num(runs)? };
            job.check().map(|()| job)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut names: Vec<&str> = jobs.iter().map(|j| j.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    if jobs.is_empty() || jobs.len() > MAX_JOBS || names.len() != jobs.len() {
        return Err(format!("1..={MAX_JOBS} jobs with distinct names"));
    }
    Ok(jobs)
}

// ---- the run log's contract ----

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogCall {
    Claim { epoch: u64, node: String },
    Fire { epoch: u64, node: String, fires: Vec<Fire> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogAnswer {
    /// The last tick fired per job.
    Claimed(BTreeMap<String, u64>),
    /// A newer owner, of this epoch, claimed the log first.
    Fenced(u64),
    Outcomes(Vec<Outcome>),
}

fn json<T: Serialize>(value: &T) -> std::result::Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

pub struct RunLogApi;

impl WireProtocol for RunLogApi {
    type Message = LogCall;
    type Reply = LogAnswer;
    const ID: u64 = 0xC4_0001;
    const SCHEMA: u16 = 1;
    fn encode(call: LogCall) -> std::result::Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> std::result::Result<LogAnswer, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for RunLogApi {
    type Mailbox = (LogCall, Option<Reply<LogAnswer>>);
    fn encode_reply(answer: LogAnswer) -> std::result::Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<LogAnswer>>) -> std::result::Result<Self::Mailbox, Rejection> {
        serde_json::from_slice(&body).map(|call| (call, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// The run log as an actor: one line per claim and per fire, `FIRE job from tick epoch node` for each side effect.
/// A real one is a table with a unique `(job, tick)` and the epoch check in the same transaction.
pub struct RunLogActor {
    log: RunLog,
    record: Box<dyn FnMut(String) + Send>,
}

impl RunLogActor {
    pub fn new(record: impl FnMut(String) + Send + 'static) -> Self {
        Self { log: RunLog::default(), record: Box::new(record) }
    }

    pub fn answer(&mut self, call: LogCall) -> LogAnswer {
        match call {
            LogCall::Claim { epoch, node } => match self.log.claim(epoch) {
                Ok(last) => {
                    (self.record)(format!("CLAIM {epoch} {node}"));
                    LogAnswer::Claimed(last)
                }
                Err(newer) => {
                    (self.record)(format!("REFUSE-CLAIM {epoch} {node} newer={newer}"));
                    LogAnswer::Fenced(newer)
                }
            },
            LogCall::Fire { epoch, node, fires } => LogAnswer::Outcomes(
                fires
                    .iter()
                    .map(|f| {
                        let outcome = self.log.fire(epoch, f);
                        let line = match outcome {
                            Outcome::Fired { from } => format!("FIRE {} {from} {}", f.job, f.tick),
                            refused => format!("{refused:?} {} {} {}", f.job, f.from, f.tick),
                        };
                        (self.record)(format!("{line} {epoch} {node}"));
                        outcome
                    })
                    .collect(),
            ),
        }
    }
}

impl Actor for RunLogActor {
    type Message = (LogCall, Option<Reply<LogAnswer>>);
    type Error = ();
    fn on_message(&mut self, (call, reply): Self::Message, _: &mut Context<'_, Self>) -> std::result::Result<(), ()> {
        let answer = self.answer(call);
        drop(reply.map(|r| r.send(answer)));
        Ok(())
    }
}

// ---- the scheduler ----

pub type Pending = Pin<Box<dyn Future<Output = std::result::Result<LogAnswer, ClusterError>> + Send>>;

/// How a scheduler reaches the run log: over the cluster in `main`, in memory in the tests.
pub trait Log: Send + 'static {
    fn call(&self, call: LogCall) -> Pending;
}

/// Wall time in ms since the Unix epoch. Ticks follow it; timers only wake the scheduler to look at it again.
pub type Wall = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_wall() -> Wall {
    Arc::new(|| axon::trace::unix_micros() / 1000)
}

pub enum Msg {
    Claim,
    Claimed(std::result::Result<LogAnswer, ClusterError>),
    Wake,
    Settled(Vec<Fire>, std::result::Result<LogAnswer, ClusterError>),
    Status(Option<Reply<String>>),
}

pub struct Scheduler<L> {
    plan: Plan,
    epoch: u64,
    node: String,
    wall: Wall,
    log: L,
    timer: Option<TimerId>,
    in_flight: bool,
    failures: u32,
}

const MAX_SLEEP: Duration = Duration::from_secs(1);
/// A claim or fire that may clear up goes again after this.
const RETRY: Backoff = Backoff { initial: Duration::from_millis(100), max: Duration::from_secs(5), jitter: 0.2 };
/// Its `max` is the longest a stopped singleton waits for a keeper to try it again, which bounds a failover after a
/// refused start.
const KEEP: Backoff = Backoff { max: Duration::from_secs(2), ..RETRY };

impl<L: Log> Scheduler<L> {
    pub fn new(jobs: Vec<Job>, epoch: u64, node: impl Into<String>, wall: Wall, log: L) -> Self {
        let plan = Plan::new(jobs);
        Self { plan, epoch, node: node.into(), wall, log, timer: None, in_flight: false, failures: 0 }
    }

    /// Claims the log before anything else is handled, so no fire of this owner can precede its claim.
    fn claim(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        let pending = self.log.call(LogCall::Claim { epoch: self.epoch, node: self.node.clone() });
        let job = cx.submit_future(async move { Msg::Claimed(pending.await) }).map_err(|e| format!("claim: {e:?}"))?;
        cx.pause_until(&job).map_err(|e| format!("claim: {e:?}"))
    }

    /// One wake-up at a time: a newer one replaces the older.
    fn arm(&mut self, cx: &mut Context<'_, Self>, after: Duration, msg: Msg) -> std::result::Result<(), String> {
        if let Some(old) = self.timer.take() {
            cx.cancel_timer(old);
        }
        match cx.schedule_after(after, msg) {
            Ok(id) => self.timer = Some(id),
            Err(e) if e.reason == TimerReject::Full => return Err("timer: Full".into()),
            Err(_) => {} // draining or closed: the actor is ending
        }
        Ok(())
    }

    /// Only for errors that may clear up; the caller has turned away the ones that will not.
    fn later(&mut self, cx: &mut Context<'_, Self>, msg: Msg) -> std::result::Result<(), String> {
        self.failures += 1;
        self.arm(cx, RETRY.delay(self.failures), msg)
    }

    fn wake(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        if self.in_flight || !self.plan.is_claimed() {
            return Ok(());
        }
        let now = (self.wall)();
        let mut fires = self.plan.owed(now);
        if fires.is_empty() {
            // Timers run on the monotonic clock. Looking at the wall clock at least every MAX_SLEEP bounds how late a
            // forward step of the wall clock makes a fire; nothing announces the step itself.
            let due = self.plan.next_due().unwrap_or(now + 1000);
            let after = Duration::from_millis(due.saturating_sub(now).max(1)).min(MAX_SLEEP);
            return self.arm(cx, after, Msg::Wake);
        }
        fires.truncate(MAX_BATCH);
        let pending = self.log.call(LogCall::Fire { epoch: self.epoch, node: self.node.clone(), fires: fires.clone() });
        cx.submit_future(async move { Msg::Settled(fires, pending.await) }).map_err(|e| format!("fire: {e:?}"))?;
        self.in_flight = true;
        Ok(())
    }

    fn fenced(&self, cx: &mut Context<'_, Self>, newer: Option<u64>) {
        let by = newer.map_or("a newer owner".into(), |e| format!("epoch {e}"));
        println!("{} epoch {} fenced by {by}", self.node, self.epoch);
        cx.stop();
    }
}

fn give_up(e: &ClusterError) -> bool {
    e.retry() == Retry::Never
}

impl<L: Log> Actor for Scheduler<L> {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        self.claim(cx)
    }

    fn on_stop(&mut self, reason: &StopReason) {
        println!("{} epoch {} stopped: {reason:?}", self.node, self.epoch);
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        match msg {
            Msg::Claim => self.claim(cx),
            Msg::Claimed(Ok(LogAnswer::Claimed(last))) => {
                println!("{} claimed the run log at epoch {}", self.node, self.epoch);
                self.failures = 0;
                self.plan.claimed(last, (self.wall)());
                self.wake(cx)
            }
            Msg::Claimed(Ok(LogAnswer::Fenced(newer))) | Msg::Settled(_, Ok(LogAnswer::Fenced(newer))) => {
                self.fenced(cx, Some(newer));
                Ok(())
            }
            Msg::Claimed(Err(e)) | Msg::Settled(_, Err(e)) if give_up(&e) => Err(format!("run log: {e}")),
            Msg::Claimed(Err(_)) => self.later(cx, Msg::Claim),
            Msg::Settled(_, Err(_)) => {
                // The fires may or may not have landed; sending them again is safe, the log answers Duplicate.
                self.in_flight = false;
                self.later(cx, Msg::Wake)
            }
            Msg::Settled(fires, Ok(LogAnswer::Outcomes(outcomes))) => {
                self.in_flight = false;
                self.failures = 0;
                match self.plan.settle(&fires, &outcomes) {
                    Ok(()) => self.wake(cx),
                    Err(Outcome::Fenced) => {
                        self.fenced(cx, None);
                        Ok(())
                    }
                    Err(refused) => Err(format!("run log refused a fire: {refused:?}")),
                }
            }
            Msg::Claimed(Ok(other)) | Msg::Settled(_, Ok(other)) => Err(format!("unexpected answer {other:?}")),
            Msg::Wake => {
                self.timer = None;
                self.wake(cx)
            }
            Msg::Status(reply) => {
                drop(reply.map(|r| r.send(format!("{} epoch {}", self.node, self.epoch))));
                Ok(())
            }
        }
    }
}

/// The scheduler's own contract: a request answers with where it runs and its epoch.
pub struct CronApi;

impl WireProtocol for CronApi {
    type Message = ();
    type Reply = String;
    const ID: u64 = 0xC4_0002;
    const SCHEMA: u16 = 1;
    fn encode(_: ()) -> std::result::Result<Bytes, CallError> {
        Ok(Bytes::new())
    }
    fn decode_reply(body: Bytes) -> std::result::Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for CronApi {
    type Mailbox = Msg;
    fn encode_reply(status: String) -> std::result::Result<Bytes, CallError> {
        Ok(Bytes::from(status))
    }
    fn decode(_: Bytes, reply: Option<Reply<String>>) -> std::result::Result<Msg, Rejection> {
        Ok(Msg::Status(reply))
    }
}

/// The run log over the cluster, resolved once and again after a call says the reference is stale.
impl Log for NamedRef<RunLogApi> {
    fn call(&self, call: LogCall) -> Pending {
        let log = self.clone();
        Box::pin(async move { log.request(call).await })
    }
}

/// Keeps the singleton running from every scheduler node: watches where it runs, and starts it when it stops.
pub async fn keep(cluster: Cluster, jobs: Bytes) {
    let placement = Placement::default();
    let mut watch = cluster.watch_key(KIND, KEY, &placement);
    let mut failures = 0;
    let mut next = watch.next().await;
    while let Some(owner) = next {
        next = match owner {
            Owner::Running { node, epoch } => {
                println!("singleton on {node} at epoch {}", epoch.unwrap_or_default());
                failures = 0;
                watch.next().await
            }
            Owner::Stopped => {
                match cluster.get_or_spawn::<Unaddressed>(KIND, KEY, jobs.clone(), &placement).await {
                    Ok(_) => {}
                    Err(e) if give_up(&e) => return eprintln!("keeper stops: {e}"),
                    Err(e) => println!("singleton refused: {e}"),
                }
                failures += 1;
                let wait = KEEP.delay(failures);
                // The watch shows `Stopped` once: neither a refused start nor one that ends before the watch sees it
                // running shows it again, so the start is tried again after the pause.
                tokio::select! {
                    next = watch.next() => next,
                    () = tokio::time::sleep(wait) => Some(Owner::Stopped),
                }
            }
        };
    }
    eprintln!("keeper stops: the watch on {KIND}/{KEY} ended");
}

/// What one node needs. `acceptors`, `lease` and `secret` must be the same on every node.
pub struct Settings {
    pub id: String,
    pub bind: SocketAddr,
    pub acceptors: Vec<String>,
    pub lease: Duration,
    pub secret: String,
    /// Where an acceptor logs who holds each key, in a directory named after the node.
    pub dir: PathBuf,
}

impl Settings {
    pub fn transport(&self, id: &str) -> Result<axon_net::Config> {
        let mut transport = axon_net::Config::new(id, SharedSecret::new(self.secret.as_bytes())?);
        transport.allow_plaintext = true; // the binary only with --plaintext; otherwise set ClusterConfig::tls
        Ok(transport)
    }

    pub async fn start(
        &self,
        transport: axon_net::Config,
        discovery: impl Discovery,
    ) -> Result<(ActorSystem, Cluster)> {
        let mut paxos = PaxosConfig::new(self.acceptors.iter().cloned());
        // The operator's lease, from LEASE_MS.
        paxos.lease = self.lease;
        // An acceptor logs who holds each key here; a node that is not one never reads it.
        let id = transport.identity.node_id.to_string();
        paxos.store = Some(Arc::new(axon::files::OsFiles::new(self.dir.join(id))));
        let mut cfg = ClusterConfig::new(self.bind, transport, discovery);
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
        // With seeds, as guide/cluster.md § Seed servers asks.
        (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
        let system = ActorSystem::builder().build()?;
        let cluster = Cluster::start(&system, cfg).await?;
        Ok((system, cluster))
    }
}

/// Lets this node host the scheduler. Every node must be given the same jobs.
pub async fn register_scheduler(cluster: &Cluster, wall: Wall) -> Result<()> {
    let (log, node) = (cluster.named::<RunLogApi>(LOG), cluster.identity().node_id.clone());
    let build = move |cx: &SpawnContext, args: Bytes| {
        let jobs: Vec<Job> = serde_json::from_slice(&args).map_err(|e| format!("jobs: {e}"))?;
        jobs.iter().try_for_each(Job::check)?;
        let epoch = cx.epoch.ok_or("the cron kind needs a lease authority in ClusterConfig::ownership")?;
        Ok(Scheduler::new(jobs, epoch, node.clone(), wall.clone(), log.clone()))
    };
    cluster.kind::<CronApi>(KIND).register(build).await?;
    Ok(())
}

pub fn jobs_args(jobs: &[Job]) -> Result<Bytes> {
    Ok(serde_json::to_vec(jobs).map(Bytes::from)?)
}
