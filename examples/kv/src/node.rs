//! One kv node: an actor system, a cluster member under `PaxosLeases`, the eager `shard` kind, the RESP listener,
//! and axon's health and metrics.

use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{Files, MemoryFiles};
use axon::{ActorHandle, ActorSystem, MailboxConfig, MailboxLimits, RuntimeConfig, StartError, SystemError};
use axon_cluster::{
    AccessPolicy, Cluster, ClusterConfig, ClusterError, Discovery, OwnershipAuthority, PaxosConfig, PaxosLeases,
    Placement,
};
use axon_obs::{
    Dashboard, DashboardConfig, Health, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig, User,
};
use bytes::Bytes;
use tokio::net::TcpListener;

use crate::front::{Dispatch, Front, LeaseTimes, NodeInfo, Shards};
use crate::log::{LogActor, LogMsg, LogSettings};
use crate::repl::Ballot;
use crate::shard::{Shard, Storage};
use crate::slot::SHARDS;
use crate::wire::{LogApi, ShardApi, ShardMsg};
use crate::{Limits, Stats};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A shard's start, its recovery included, may take this long: three times the p99 of 85 shards recovering at once
/// on build-1, and at least axon's 5 s. Measured (`fault.sh restart-all`): a p99 of 46 ms at
/// most, so the floor.
pub const START_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Each shard's map in its owner's memory only: a shard that moves starts empty.
    Memory,
    /// Each write in the storage set's Logs before it is answered.
    Durable,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Memory => "memory",
            Mode::Durable => "durable",
        }
    }
}

/// How a durable shard answers a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reads {
    /// Once a majority of Logs has answered an Append sent after the read came: never stale.
    Quorum,
    /// At once, from the owner's map: stale only if a process is held past its lease with its clock stopped.
    Lease,
}

/// What one node needs. `nodes`, `lease`, `mode` and the secret must be the same on every node.
#[derive(Clone, Debug)]
pub struct Settings {
    pub id: String,
    /// Where the RESP listener binds; none in a World test, which has no sockets.
    pub resp: Option<SocketAddr>,
    pub bind: SocketAddr,
    /// The storage set, also the acceptor list: 1 or 3 node ids.
    pub nodes: Vec<String>,
    pub mode: Mode,
    pub reads: Reads,
    pub lease: Duration,
    /// How long a shard may take to start, its recovery included: the system's `start_timeout`. Set from the
    /// recovery of 85 shards at once measured on build-1.
    pub start_timeout: Duration,
    /// Committed entries a shard's owner keeps to send a Log that is behind, and of those only the newest within
    /// `keep_bytes`; one further behind gets the map.
    pub keep: u64,
    pub keep_bytes: u64,
    /// How long the front end tries a call before it answers with an error. Below the clients' read timeout.
    pub deadline: Duration,
    pub workers: Option<usize>,
    pub password: Option<String>,
    /// `/healthz`, `/readyz` and `/metrics`; with it the node runs `Telemetry`, which axon's console reads.
    pub health: Option<SocketAddr>,
    /// axon's console on this node, with a `MetricsService` that keeps its history in memory. It reads the
    /// `Telemetry` of every node, so the others need `health` set.
    pub console: Option<SocketAddr>,
    /// Who may sign in to the console; none leaves it open, which axon allows on loopback only.
    pub console_users: Vec<User>,
    pub limits: Limits,
    pub log: LogSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            id: "n1".into(),
            resp: Some(([127, 0, 0, 1], 6379).into()),
            bind: ([127, 0, 0, 1], 0).into(),
            nodes: vec!["n1".into()],
            mode: Mode::Memory,
            reads: Reads::Quorum,
            lease: Duration::from_secs(6),
            start_timeout: START_TIMEOUT,
            keep: 4096,
            keep_bytes: 64 << 20,
            deadline: Duration::from_secs(2),
            workers: None,
            password: None,
            health: None,
            console: None,
            console_users: Vec::new(),
            limits: Limits::default(),
            log: LogSettings::default(),
        }
    }
}

/// Where a node keeps what outlives it, and what it does when its Log ends.
#[derive(Clone)]
pub struct Store {
    /// The leases this node grants, when it is on `nodes`.
    pub leases: Arc<dyn Files>,
    /// Durable mode: the Log's WAL and its shards' snapshots.
    pub wal: Arc<dyn Files>,
    pub snaps: Arc<dyn Files>,
    /// Called once if the Log ends with a failure: the process exits, as a node whose Log is gone must not keep
    /// its leases. A test records it.
    pub halt: Arc<dyn Fn(String) + Send + Sync>,
}

impl Store {
    /// Everything in memory: for tests.
    pub fn memory() -> Self {
        Self {
            leases: Arc::new(MemoryFiles::new()),
            wal: Arc::new(MemoryFiles::new()),
            snaps: Arc::new(MemoryFiles::new()),
            halt: Arc::new(|why| panic!("the Log ended: {why}")),
        }
    }
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub front: Arc<Front>,
    pub stats: Arc<Stats>,
    /// Where the RESP listener is.
    pub resp: Option<SocketAddr>,
    /// Durable mode: the node's Log.
    pub log: Option<ActorHandle<LogMsg>>,
    pub health: Option<Health>,
    /// Where axon's console is, when `console` was set.
    pub console: Option<SocketAddr>,
    _telemetry: Option<Telemetry>,
    serving: Option<tokio::task::JoinHandle<()>>,
}

/// The shard kind's mailbox: every front end of the cluster may ask one shard at once. A cluster call comes as a
/// message, not an `ask`: `messages` bounds the calls that wait, `requests` none of them.
pub fn mailbox() -> MailboxConfig<ShardMsg> {
    let limits = MailboxLimits { messages: 1024, jobs: 64, bytes: 64 << 20, ..MailboxLimits::default() };
    let mut cfg = MailboxConfig::from(limits);
    cfg.weight = Some(Arc::new(|msg: &ShardMsg| match msg {
        ShardMsg::Call(call, _) => 64 + call.encoded_len(),
        ShardMsg::Uploaded(..) | ShardMsg::Sent | ShardMsg::Logged(..) | ShardMsg::Tick => 64,
    }));
    cfg
}

/// The Log's mailbox: each of the 256 shards may ask it at once, past the default of 128 local asks. The owners on the
/// other nodes call it through `kv.log`, a message each, so `messages` and `bytes` bound those, not `requests`.
pub fn log_mailbox() -> MailboxConfig<LogMsg> {
    MailboxConfig::from(MailboxLimits { requests: 1024, jobs: crate::log::JOBS, ..MailboxLimits::default() })
}

/// Starts a node. `transport` is the one `discovery` was built with.
pub async fn start(
    settings: &Settings,
    transport: axon_net::Config,
    discovery: impl Discovery,
    store: Store,
) -> Result<Node, Error> {
    let mut runtime = RuntimeConfig::default();
    if let Some(workers) = settings.workers {
        runtime.workers = workers;
    }
    start_on(settings, runtime, transport, discovery, store).await
}

/// `start` on a runtime of the caller's: a World host's.
pub async fn start_on(
    settings: &Settings,
    runtime: RuntimeConfig,
    transport: axon_net::Config,
    discovery: impl Discovery,
    store: Store,
) -> Result<Node, Error> {
    if !matches!(settings.nodes.len(), 1 | 3) {
        return Err(format!("--nodes names 1 or 3 nodes, not {}", settings.nodes.len()).into());
    }
    let place = settings.nodes.iter().position(|n| *n == settings.id);
    if settings.mode == Mode::Durable && place.is_none() {
        return Err("in durable mode each node is one that --nodes names: each keeps a Log".into());
    }
    // One Append holds one command whole, and up to 512 KiB of others after it, in one WAL record.
    if settings.mode == Mode::Durable && settings.limits.command + (1 << 20) > settings.log.wal.max_record {
        return Err("the WAL's max_record must hold the largest command and 1 MiB more".into());
    }
    let mut paxos = PaxosConfig::new(settings.nodes.iter().cloned());
    paxos.lease = settings.lease;
    if place.is_some() {
        paxos.store = Some(store.leases.clone());
    }
    let leases: Arc<dyn OwnershipAuthority> = Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?);
    // The cluster's own defaults (grace 7/8 of hold, renew 2/5 of grace), set here so INFO kv names what it runs with.
    let hold = leases.hold().unwrap_or(settings.lease);
    let lease = LeaseTimes { lease: settings.lease, hold, grace: hold * 7 / 8, renew: hold * 7 / 8 * 2 / 5 };
    let mut cfg = ClusterConfig::new(settings.bind, transport, discovery);
    cfg.ownership = Some(leases.clone());
    (cfg.lease_grace, cfg.lease_renew_interval) = (Some(lease.grace), Some(lease.renew));
    // What seed discovery asks for (guide/cluster.md § Seed servers); harmless with the others.
    (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
    let system = ActorSystem::builder().runtime(runtime).start_timeout(settings.start_timeout).build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    let stats = Arc::new(Stats::default());

    // With one Log no owner fetches from another, and no Log is behind: neither the Log nor the owners keep entries.
    let one_log = settings.nodes.len() == 1;
    // The Log reads the WAL before anything else: until then it could answer an owner from empty state.
    let log = match settings.mode {
        Mode::Memory => None,
        Mode::Durable => {
            let log_settings =
                LogSettings { keep: if one_log { 0 } else { settings.log.keep }, ..settings.log.clone() };
            let actor = LogActor::new(store.wal.clone(), store.snaps.clone(), log_settings, stats.clone());
            let log = system.spawn(actor).name("kv.log").mailbox(log_mailbox()).start()?;
            // A large WAL takes longer to read than a shard's start may: wait on.
            loop {
                match log.ready().await {
                    Err(SystemError::Start(StartError::TimedOut)) => {}
                    ready => break ready?,
                }
            }
            cluster.publish::<LogApi>(&log, "kv.log")?;
            let (watched, halt) = (log.clone(), store.halt.clone());
            tokio::spawn(async move {
                let exit = watched.join().await;
                if exit.reason.is_failure() {
                    halt(format!("{:?}", exit.reason));
                }
            });
            Some(log)
        }
    };

    let listener = match settings.resp {
        Some(bind) => Some(TcpListener::bind(bind).await?),
        None => None,
    };
    let resp = listener.as_ref().map(TcpListener::local_addr).transpose()?;
    if let Some(resp) = resp {
        cluster.set_metadata("kv.resp", Some(&resp.to_string())).await?;
    }
    cluster.set_metadata("kv.shards", Some(&SHARDS.to_string())).await?;
    cluster.set_metadata("kv.mode", Some(settings.mode.name())).await?;

    let storage = log.clone().map(|log| {
        Arc::new(Storage {
            local: place.expect("a durable node is on --nodes"),
            log,
            nodes: settings.nodes.clone(),
            cluster: cluster.downgrade(),
            limits: settings.limits.clone(),
            quorum_reads: settings.reads == Reads::Quorum,
            give_up: settings.start_timeout,
            keep: if one_log { 0 } else { settings.keep },
            keep_bytes: settings.keep_bytes,
            stats: stats.clone(),
            remotes: Mutex::default(),
        })
    });
    let (limits, starts) = (settings.limits.clone(), stats.clone());
    let (node, boot) = (place.unwrap_or(0) as u8, axon::trace::random_u64());
    cluster
        .kind::<ShardApi>("shard")
        .mailbox(mailbox())
        .eager(Bytes::new(), Placement::default())
        .register(move |cx, _| {
            let key = cx.key.as_deref().unwrap_or_default();
            let id = key.parse::<u16>().ok().filter(|id| *id < SHARDS).ok_or(format!("no shard {key:?}"))?;
            starts.shard_starts.fetch_add(1, Relaxed);
            Ok(match &storage {
                None => Shard::new(id, limits.clone()),
                Some(storage) => {
                    let ballot = Ballot { round: cx.epoch.unwrap_or(0), node, boot, start: cx.start };
                    Shard::durable(id, limits.clone(), ballot, storage.clone())
                }
            })
        })
        .await?;

    // Any node of the cluster may read it: the console's node, and nothing outside the shared secret.
    let telemetry = match settings.health.or(settings.console) {
        Some(_) => {
            let cfg = TelemetryConfig { access: AccessPolicy::Cluster, ..TelemetryConfig::default() };
            Some(Telemetry::start(&system, &cluster, cfg).await?)
        }
        None => None,
    };
    let health = match (settings.health, &telemetry) {
        (Some(bind), Some(telemetry)) => {
            let health = Health::start(&system, &cluster, bind).await?;
            health.serve_metrics(telemetry);
            Some(health)
        }
        _ => None,
    };
    let console = match settings.console {
        Some(bind) => {
            let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..MetricsConfig::default() };
            MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
            let cfg = DashboardConfig { bind, users: settings.console_users.clone(), ..DashboardConfig::default() };
            Some(Dashboard::start(&system, &cluster, cfg).await?.address())
        }
        None => None,
    };

    let shards = Shards::new(cluster.clone(), settings.deadline, settings.limits.clone(), stats.clone(), leases);
    let info = NodeInfo {
        id: settings.id.clone(),
        mode: settings.mode.name().into(),
        nodes: settings.nodes.clone(),
        resp: resp.unwrap_or(settings.bind),
        log: log.clone(),
        reads: match settings.reads {
            Reads::Quorum => "quorum",
            Reads::Lease => "lease",
        },
        lease,
    };
    let front = Arc::new(Front::new(
        Dispatch::Shards(shards),
        settings.limits.clone(),
        settings.password.clone(),
        stats.clone(),
        info,
    ));
    let serving = listener.map(|listener| tokio::spawn(front.clone().serve(listener)));
    Ok(Node { system, cluster, front, stats, resp, log, health, console, _telemetry: telemetry, serving })
}

impl Node {
    /// Members whose `kv.shards` or `kv.mode` differ from this node's: they would split the slots another way.
    pub fn mismatched(&self) -> Vec<String> {
        let ours = self.cluster.record();
        let view = self.cluster.view();
        view.members
            .iter()
            .filter(|m| {
                ["kv.shards", "kv.mode"].iter().any(|k| {
                    let theirs = m.record.metadata.get(*k);
                    theirs.is_some() && theirs != ours.metadata.get(*k)
                })
            })
            .map(|m| m.record.id.to_string())
            .collect()
    }

    /// Ends the node without a leave, as a killed process does: its peers find out from missed heartbeats and
    /// lapsed leases. Close its clients' connections first, as each holds the front end.
    pub fn crash(self) {
        if let Some(serving) = self.serving {
            serving.abort();
        }
    }

    /// Stops taking connections, then leaves the cluster.
    pub async fn shutdown(self) -> Result<(), Error> {
        if let Some(serving) = &self.serving {
            serving.abort();
        }
        self.system.shutdown().await?;
        Ok(())
    }
}

/// `kv stub`: the front end with no shards behind it.
pub async fn stub(resp: SocketAddr, limits: Limits) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), Error> {
    let listener = TcpListener::bind(resp).await?;
    let resp = listener.local_addr()?;
    let info = NodeInfo {
        id: "stub".into(),
        mode: "stub".into(),
        nodes: vec!["stub".into()],
        resp,
        log: None,
        reads: "",
        lease: LeaseTimes::default(),
    };
    let front = Arc::new(Front::new(Dispatch::Stub(stub_value()), limits, None, Arc::default(), info));
    Ok((resp, tokio::spawn(front.serve(listener))))
}

/// What go-ycsb's redis client reads back: ten fields of 100 bytes, as JSON with the bytes in base64.
pub fn stub_value() -> Bytes {
    let field = format!("{}==", "A".repeat(134));
    let fields: Vec<String> = (0..10).map(|i| format!("\"field{i}\":\"{field}\"")).collect();
    Bytes::from(format!("{{{}}}", fields.join(",")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stub_value_is_ycsb_json() {
        let value: serde_json::Value = serde_json::from_slice(&stub_value()).expect("json");
        let fields = value.as_object().expect("an object");
        assert_eq!(fields.len(), 10);
        // 100 bytes in base64: 33 groups of three and one byte left.
        assert!(fields.values().all(|f| f.as_str().is_some_and(|s| s.len() == 136)));
    }
}
