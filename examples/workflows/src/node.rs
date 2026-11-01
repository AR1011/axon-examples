//! One node's parts, by its roles: a history node's replica and waker, an engine's `run` kind, and the references
//! the api and the waker call runs through. `dev`, the tests and (later) `node` all start here.

use std::sync::Arc;
use std::time::Duration;

use axon::files::Files;
use axon::{ActorHandle, ActorSystem, MailboxConfig, MailboxLimits};
use axon_cluster::{Cluster, KeyedRefs, Placement, SpawnContext, WeakCluster};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crate::journal::JournalClient;
use crate::journal::service::{self, JournalConfig, ToJournal, ToWaker, Wake, WakerConfig, start_waker};
use crate::metrics::EngineMetrics;
use crate::queue::{
    QUEUE, QUEUE_ARGS, Queue, QueueAnswer, QueueApi, QueueCall, QueueLimits, Task, partition, queue_placement,
};
use crate::run::actor::{OnWorkers, Peers};
use crate::run::{Env, RUN, Run, RunAnswer, RunApi, RunCall, Wall, call_idempotent, wire::valid_id};
use crate::worker::{DRAINING_KEY, SLOTS_KEY, Worker, WorkerApi, WorkerConfig, WorkerMsg, WorkerWatch, worker_name};
use crate::workflow::App;

/// Every caller of the `run` kind passes these args and this placement (axon's README, rule 10): a run's input goes
/// in its `Start`, never in the args.
pub const RUN_ARGS: Bytes = Bytes::new();

pub fn run_placement() -> Placement {
    Placement::default().require("engine", "yes")
}

pub fn history_placement() -> Placement {
    Placement::default().require("history", "yes")
}

/// Run references a node keeps.
const RUN_REFS: usize = 4_096;
/// Queue partition references a node keeps.
const QUEUE_REFS: usize = 256;
/// How long a run tries to make a call to another run arrive: a child's start, a parent's `ChildDone`.
const CALL_WITHIN: Duration = Duration::from_secs(30);
/// How long an offer to a queue may take.
const OFFER_WITHIN: Duration = Duration::from_secs(5);
/// A queue partition with no call for this long stops: it holds nothing a run does not offer again.
const QUEUE_IDLE: Duration = Duration::from_secs(300);
/// How long a waker tries to make a `Wake` arrive: through an owner's crash, its lease's lapse and a new open.
const WAKE_WITHIN: Duration = Duration::from_secs(30);

pub struct NodeConfig {
    pub app: App,
    pub wall: Wall,
    /// Its `replicas` are the history nodes, the same list in the same order on every node; the rest is a history
    /// node's.
    pub journal: JournalConfig,
    /// On a history node: its replica's log and its snapshot's directory.
    pub history: Option<(Arc<dyn Files>, Arc<dyn Files>)>,
    /// Hosts runs and queues.
    pub engine: bool,
    pub queue_limits: QueueLimits,
    /// A worker for each: on a node labelled `worker=yes` and `queue.<name>=yes` for each queue.
    pub workers: Vec<WorkerConfig>,
}

impl NodeConfig {
    pub fn new(app: App, wall: Wall, journal: JournalConfig) -> Self {
        NodeConfig {
            app,
            wall,
            journal,
            history: None,
            engine: false,
            queue_limits: QueueLimits::default(),
            workers: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub journal: Option<ActorHandle<ToJournal>>,
    pub waker: Option<ActorHandle<ToWaker>>,
    pub runs: KeyedRefs<RunApi>,
    pub queues: KeyedRefs<QueueApi>,
    /// The non-local queues the app's activities name.
    pub app_queues: std::collections::BTreeSet<String>,
    pub workers: Vec<ActorHandle<WorkerMsg>>,
    /// The history nodes, in the order every node lists them.
    pub replicas: Vec<Arc<str>>,
    /// The workflows and their versions in this build.
    pub app: Arc<App>,
    /// What this node's runs did, on an engine.
    pub metrics: Arc<EngineMetrics>,
    pub engine: bool,
    cluster: WeakCluster,
}

impl Node {
    /// Drains the workers: each stops polling, gives back what it has not started and finishes what runs, within
    /// `within`. Then `system.shutdown()`.
    pub async fn drain_workers(&self, within: Duration) {
        if let Some(cluster) = self.cluster.upgrade().filter(|_| !self.workers.is_empty()) {
            // The console shows the node draining; a node that cannot say so still drains.
            let _ = cluster.set_metadata(DRAINING_KEY, Some("yes")).await;
        }
        let mut waits = Vec::new();
        for worker in &self.workers {
            let (done, wait) = oneshot::channel();
            if worker.send(WorkerMsg::Drain(done)).await.is_ok() {
                waits.push(wait);
            }
        }
        let _ = tokio::time::timeout(within, futures_util::future::join_all(waits)).await;
    }
}

/// A run's calls to other runs and to queues, over the cluster.
struct ClusterPeers {
    runs: KeyedRefs<RunApi>,
    queues: KeyedRefs<QueueApi>,
}

impl Peers for ClusterPeers {
    fn run(&self, id: &str, call: RunCall) -> BoxFuture<'static, Result<RunAnswer, String>> {
        let run = self.runs.get(id, RUN_ARGS);
        Box::pin(async move {
            call_idempotent(&run, call, tokio::time::Instant::now() + CALL_WITHIN).await.map_err(|e| e.to_string())
        })
    }

    fn offer(&self, task: Task) -> BoxFuture<'static, Result<(), String>> {
        let queue = self.queues.get(&partition(&task.queue, &task.run), QUEUE_ARGS);
        Box::pin(async move {
            match queue.request_within(QueueCall::Offer(task), OFFER_WITHIN).await {
                Ok(QueueAnswer::Accepted) => Ok(()),
                Ok(other) => Err(format!("{other:?}")),
                Err(e) => Err(e.to_string()),
            }
        })
    }
}

pub async fn start(system: &ActorSystem, cluster: &Cluster, cfg: NodeConfig) -> Result<Node, String> {
    let runs = cluster.keyed_refs::<RunApi>(RUN, &run_placement(), RUN_REFS);
    let queues = cluster.keyed_refs::<QueueApi>(QUEUE, &queue_placement(), QUEUE_REFS);
    let app = Arc::new(cfg.app);
    let app_queues = app.queues().into_iter().map(str::to_owned).collect();
    // `RUN_MUTATION=no_eager` and `no_reconciler` drop what restarts a run nobody calls: `scripts/mutations.sh`.
    let mutation = crate::mutation();
    let (mut journal, mut waker) = (None, None);
    let metrics = Arc::new(EngineMetrics::default());
    let replicas = cfg.journal.replicas.clone();
    if let Some((files, snaps)) = cfg.history {
        let mut journal_cfg = cfg.journal.clone();
        journal_cfg.keep_workflows.extend(app.retentions().iter().map(|(w, d)| (w.clone(), *d)));
        let handle = service::start(system, cluster, files, snaps, journal_cfg).await?;
        let refs = runs.clone();
        let wake: Wake = Arc::new(move |run: Bytes| {
            let run = refs.get(&String::from_utf8_lossy(&run), RUN_ARGS);
            Box::pin(async move {
                let deadline = tokio::time::Instant::now() + WAKE_WITHIN;
                match call_idempotent(&run, RunCall::Wake, deadline).await {
                    Ok(RunAnswer::Accepted) => Ok(()),
                    Ok(other) => Err(format!("{other:?}")),
                    Err(e) => Err(e.to_string()),
                }
            })
        });
        let mut waker_cfg = WakerConfig::new(history_placement(), wake);
        if mutation == "no_reconciler" {
            waker_cfg.reconcile_every = Duration::from_secs(365 * 86_400);
        }
        waker = Some(start_waker(system, cluster, handle.clone(), waker_cfg)?);
        journal = Some(handle);
    }
    if cfg.engine {
        let mut env = Env::new(app.clone(), cfg.wall, cfg.journal.replicas.len());
        env.node = cluster.record().id.into();
        env.peers = Arc::new(ClusterPeers { runs: runs.clone(), queues: queues.clone() });
        env.metrics = metrics.clone();
        let on_workers: Arc<OnWorkers> = env.on_workers.clone();
        let env = Arc::new(env);
        let replicas = cfg.journal.replicas;
        let build = move |cx: &SpawnContext, _: Bytes| {
            let id = cx.key.clone().filter(|id| valid_id(id)).ok_or("a run id is 1 to 128 of [A-Za-z0-9._:/-]")?;
            let lease = cx.epoch.ok_or("runs need an ownership authority")?;
            // The cluster holds this factory, so the factory holds the cluster weakly; a run holds it while it lives.
            let cluster = cx.cluster.upgrade().ok_or("the cluster is shutting down")?;
            Ok(Run::new(env.clone(), Arc::new(JournalClient::new(cluster, replicas.clone())), id, lease))
        };
        // Five timers: the writer's tick, the next due time, the open's limit, the rest, the watch of attempts.
        let limits = MailboxLimits { timers: 5, jobs: 64, ..MailboxLimits::default() };
        let mut kind = cluster.kind::<RunApi>(RUN).mailbox(MailboxConfig::from(limits));
        // A gone owner's runs start again on the engines left, without a caller: each probes and opens if it goes on.
        if mutation != "no_eager" {
            kind = kind.eager(RUN_ARGS, run_placement());
        }
        kind.register(build).await.map_err(|e| e.to_string())?;
        let limits = cfg.queue_limits;
        let queue = cluster.kind::<QueueApi>(QUEUE).idle(QUEUE_IDLE);
        queue.register(move |_: &SpawnContext, _: Bytes| Ok(Queue::new(limits))).await.map_err(|e| e.to_string())?;
        let watch = WorkerWatch::new(cluster, on_workers, runs.clone());
        system.spawn(watch).name("worker-watch").start().map_err(|e| e.to_string())?;
    }
    let mut workers = Vec::new();
    let slots: Vec<String> = cfg.workers.iter().map(|w| format!("{}:{}", w.queue, w.slots)).collect();
    for wcfg in cfg.workers {
        // Polls, `Released` and `Unpoll` beside the attempts.
        let jobs = wcfg.slots as usize * 2 + 16;
        let mailbox = MailboxConfig::from(MailboxLimits { jobs, ..MailboxLimits::default() });
        let name = worker_name(&wcfg.queue);
        let worker = Worker::new(wcfg, app.clone(), cluster);
        let handle = system.spawn(worker).name(name.clone()).mailbox(mailbox).start().map_err(|e| e.to_string())?;
        // The console asks each worker how many slots it uses and how many attempts failed.
        cluster.publish::<WorkerApi>(&handle, &name).map_err(|e| e.to_string())?;
        workers.push(handle);
    }
    if !slots.is_empty() {
        cluster.set_metadata(SLOTS_KEY, Some(&slots.join(","))).await.map_err(|e| e.to_string())?;
    }
    let (engine, cluster) = (cfg.engine, cluster.downgrade());
    Ok(Node { journal, waker, runs, queues, app_queues, workers, replicas, app, metrics, engine, cluster })
}
