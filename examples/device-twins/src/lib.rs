//! Digital twins for IoT devices on an axon cluster.
//!
//! Twin nodes host one keyed `Twin` actor per device and take UDP reports from any device, sending each to the
//! device's twin wherever it lives. Twins publish alerts to `alerts/<node>`; the alerts node follows every twin
//! node's stream, logs and counts. Every node answers `GET /devices/<id>` and `GET /stats`.
pub mod demo;
pub mod load;
pub mod scale;
pub mod twin;
pub mod wire;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorSystem, AtomicHistogram, Context, Histogram, JobHandle, RuntimeConfig};
use axon_cluster::{
    Cluster, ClusterConfig, Discovery, KeyedRefs, NodeChange, Nodes, OwnershipAuthority, Placement, Rejection,
    WeakCluster,
};
use axon_net::SharedSecret;
use axon_stream::{
    Cursor, DeliveryPolicy, EndReason, ResetReason, StartPosition, StreamEvent, StreamNode, StreamNodeConfig,
    SubscribeOptions,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use twin::{Alert, Event, KIND, Op, PARKED, PARKED_PROTOCOL, Parking, Shared, Twin, TwinApi, TwinCounters, alerts_key};
use wire::{Report, ack, now_us};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Reports a node's ingest routes at once; past that it stops reading its socket and devices resend. Raising it to
/// 32 768 let a node take on more than it could finish, and it fell out of the cluster.
const IN_FLIGHT: usize = 2048;
/// Twins and keyed spawns a node may hold. Both caps default to 65 536.
const MAX_TWINS: usize = 1 << 21;

/// What one node needs. `secret` must be the same on every node.
#[derive(Clone)]
pub struct Settings {
    pub id: String,
    pub bind: SocketAddr,
    /// Where the others reach this node; needed when `bind` is a wildcard.
    pub advertise: Option<SocketAddr>,
    pub udp: SocketAddr,
    pub http: SocketAddr,
    /// A twin that hears nothing for this long is stopped and parked.
    pub idle: Duration,
    pub max_parked: usize,
    /// The ingest's twin references, the keys reported last kept.
    pub max_refs: usize,
    pub secret: String,
}

impl Settings {
    pub fn local(id: &str) -> Self {
        let any: SocketAddr = ([127, 0, 0, 1], 0).into();
        Self {
            id: id.into(),
            bind: any,
            advertise: None,
            udp: any,
            http: any,
            // A twin that hears nothing for 30 s stops and parks its state.
            idle: Duration::from_secs(30),
            max_parked: 4 << 20,
            max_refs: 4 << 20,
            secret: "device-twins-example-secret".into(),
        }
    }

    /// Each call is a new incarnation of the node: pass the same one to discovery and the cluster.
    pub fn transport(&self) -> Result<axon_net::Config> {
        let mut transport = axon_net::Config::new(&self.id, SharedSecret::new(self.secret.as_bytes())?);
        // For a private network only: across the internet, set `ClusterConfig::tls`.
        transport.allow_plaintext = true;
        Ok(transport)
    }

    async fn cluster(
        &self,
        role: &str,
        transport: axon_net::Config,
        discovery: impl Discovery,
        ownership: Arc<dyn OwnershipAuthority>,
    ) -> Result<(ActorSystem, Cluster)> {
        let mut cfg = ClusterConfig::new(self.bind, transport, discovery);
        cfg.advertise = self.advertise;
        cfg.labels = BTreeMap::from([("role".to_owned(), role.to_owned())]);
        cfg.ownership = Some(ownership);
        cfg.max_spawned = MAX_TWINS;
        // What seed discovery wants; harmless with the others.
        (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
        let runtime = RuntimeConfig { max_actors: MAX_TWINS, ..Default::default() };
        let system = ActorSystem::builder().runtime(runtime).build()?;
        let cluster = Cluster::start(&system, cfg).await?;
        Ok((system, cluster))
    }
}

pub fn now_ms() -> u64 {
    now_us() / 1000
}

/// Resident memory of this process, from `/proc` (0 elsewhere).
pub fn rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let line = status.lines().find(|l| l.starts_with("VmRSS:")).unwrap_or_default();
    line.split_whitespace().nth(1).and_then(|kb| kb.parse().ok()).unwrap_or(0)
}

/// How long a report is tried before it is left unacked, for the device to send again.
const DELIVER_WITHIN: Duration = Duration::from_secs(3);

/// Routes reports from the node's UDP socket to twins and acks them once the twin applied them.
pub struct Ingest {
    twins: KeyedRefs<TwinApi>,
    datagrams: AtomicU64,
    malformed: AtomicU64,
    acked: AtomicU64,
    unacked: AtomicU64,
}

impl Ingest {
    fn new(cluster: &Cluster, max_refs: usize) -> Self {
        Self {
            twins: cluster.keyed_refs(KIND, &Placement::default(), max_refs),
            datagrams: AtomicU64::default(),
            malformed: AtomicU64::default(),
            acked: AtomicU64::default(),
            unacked: AtomicU64::default(),
        }
    }

    /// `true` once the twin applied the report, or had already. A `KeyedRef` asks the owner once and again only
    /// after it hears the twin stopped, moved or its node restarted; `request_until` resends only what did not arrive.
    async fn deliver(&self, report: Report) -> bool {
        let twin = self.twins.get(&report.device.to_string(), Bytes::new());
        // One that may have arrived is not acked: the device sends it again and the twin drops it if it did.
        twin.request_until(Op::Report(report), tokio::time::Instant::now() + DELIVER_WITHIN).await.is_ok()
    }

    async fn receive(self: Arc<Self>, socket: Arc<UdpSocket>) {
        let permits = Arc::new(Semaphore::new(IN_FLIGHT));
        let mut buf = [0u8; 64];
        loop {
            let Ok((n, from)) = socket.recv_from(&mut buf).await else { continue };
            self.datagrams.fetch_add(1, Relaxed);
            let Some(report) = Report::decode(&buf[..n]) else {
                self.malformed.fetch_add(1, Relaxed);
                continue;
            };
            let Ok(permit) = permits.clone().acquire_owned().await else { return };
            let (ingest, socket) = (self.clone(), socket.clone());
            tokio::spawn(async move {
                if ingest.deliver(report).await {
                    ingest.acked.fetch_add(1, Relaxed);
                    drop(socket.send_to(&ack(report.device, report.seq), from).await);
                } else {
                    ingest.unacked.fetch_add(1, Relaxed);
                }
                drop(permit);
            });
        }
    }
}

/// What `GET /stats` answers.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeStats {
    pub node: String,
    pub at_ms: u64,
    pub members: usize,
    /// Keyed actors running here: live twins.
    pub twins: usize,
    pub parked: usize,
    pub parked_dropped: u64,
    pub twin: TwinCounters,
    /// Device send to twin handling.
    pub latency: Histogram,
    pub datagrams: u64,
    pub malformed: u64,
    pub acked: u64,
    pub unacked: u64,
    pub handoffs: u64,
    pub handoffs_failed: u64,
    pub spawn_refused: u64,
    pub keyed_failed: u64,
    /// Times this node stopped every keyed actor: out of touch for `heartbeat_grace`.
    pub isolations: u64,
    /// Twins stopped because their lease could not be renewed, one per key.
    pub leases_lost: u64,
    pub rss_kb: u64,
    /// The lease authority's own figures; an acceptor reports whether it is still warming up.
    pub leases: Vec<(String, String)>,
    pub alerts: Option<AlertCounts>,
}

fn cluster_stats(cluster: &Cluster) -> NodeStats {
    let c = cluster.stats();
    NodeStats {
        node: cluster.identity().node_id.to_string(),
        at_ms: now_ms(),
        members: c.members,
        twins: c.spawned,
        handoffs: c.handoffs,
        handoffs_failed: c.handoffs_failed,
        spawn_refused: c.spawn_refused,
        keyed_failed: c.keyed_failed,
        isolations: c.isolations,
        leases_lost: c.leases_lost,
        rss_kb: rss_kb(),
        leases: c.ownership.map(|o| o.stats).unwrap_or_default(),
        ..NodeStats::default()
    }
}

#[derive(Clone)]
struct Http {
    cluster: WeakCluster,
    stats: Arc<dyn Fn(&Cluster) -> NodeStats + Send + Sync>,
}

async fn get_device(State(http): State<Http>, Path(device): Path<u64>) -> Response {
    let Some(cluster) = http.cluster.upgrade() else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    let twin = cluster.keyed::<TwinApi>(KIND, &device.to_string(), Bytes::new(), &Placement::default());
    match twin.request(Op::Get).await {
        Ok(Some(view)) if view.reports > 0 => axum::Json(view).into_response(),
        Ok(_) => (StatusCode::NOT_FOUND, "no reports from this device\n").into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, format!("{e:?}\n")).into_response(),
    }
}

async fn get_stats(State(http): State<Http>) -> Response {
    match http.cluster.upgrade() {
        Some(cluster) => axum::Json((http.stats)(&cluster)).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn serve_http(bind: SocketAddr, http: Http) -> Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;
    let app = axum::Router::new()
        .route("/devices/{id}", axum::routing::get(get_device))
        .route("/stats", axum::routing::get(get_stats))
        .with_state(http);
    Ok((addr, tokio::spawn(async move { drop(axum::serve(listener, app).await) })))
}

pub struct TwinNode {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
    pub shared: Arc<Shared>,
    pub ingest: Arc<Ingest>,
    pub udp: SocketAddr,
    pub http: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl TwinNode {
    pub async fn start(
        settings: &Settings,
        transport: axon_net::Config,
        discovery: impl Discovery,
        ownership: Arc<dyn OwnershipAuthority>,
    ) -> Result<Self> {
        let (system, cluster) = settings.cluster("twin", transport, discovery, ownership).await?;
        let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await?;
        let alerts = alerts_key(&settings.id);
        streams.open_stream(&alerts, now_ms(), DeliveryPolicy::ReplayRequired).await?;
        let shared = Arc::new(Shared {
            node: settings.id.clone(),
            streams: streams.clone(),
            alerts: alerts.clone(),
            parking: Parking::new(settings.max_parked),
            counters: Default::default(),
            latency: AtomicHistogram::default(),
        });

        let lot = shared.clone();
        cluster.serve(PARKED, PARKED_PROTOCOL, 1, move |_, body, reply| {
            let device = <[u8; 8]>::try_from(&body[..]).map_err(|_| Rejection::InvalidMessage)?;
            let state = lot.parking.take(u64::from_be_bytes(device));
            let body = state.and_then(|s| serde_json::to_vec(&s).ok()).unwrap_or_default();
            drop(reply.map(|r| r.send(Bytes::from(body))));
            Ok(())
        })?;

        let factory = shared.clone();
        cluster
            .kind::<TwinApi>(KIND)
            .idle(settings.idle)
            .register(move |cx, _args| {
                let device = cx.key.as_deref().and_then(|k| k.parse().ok()).ok_or("a twin's key is its device id")?;
                Ok(Twin::new(device, cx.epoch.unwrap_or_default(), factory.clone(), cx.cluster.clone()))
            })
            .await?;

        let ingest = Arc::new(Ingest::new(&cluster, settings.max_refs));
        let socket = Arc::new(UdpSocket::bind(settings.udp).await?);
        let udp = socket.local_addr()?;
        let receive = tokio::spawn(ingest.clone().receive(socket));
        let (twins, lot) = (shared.clone(), ingest.clone());
        let stats = Arc::new(move |cluster: &Cluster| twin_stats(cluster, &twins, &lot));
        let (http, serve) = serve_http(settings.http, Http { cluster: cluster.downgrade(), stats }).await?;
        Ok(Self { system, cluster, streams, shared, ingest, udp, http, tasks: vec![receive, serve] })
    }

    pub fn stats(&self) -> NodeStats {
        twin_stats(&self.cluster, &self.shared, &self.ingest)
    }

    /// A clean leave: twins drain and park, their keys move to the other nodes.
    pub async fn shutdown(self) -> Result<()> {
        self.tasks.iter().for_each(JoinHandle::abort);
        self.system.shutdown().await?;
        Ok(())
    }
}

fn twin_stats(cluster: &Cluster, shared: &Shared, ingest: &Ingest) -> NodeStats {
    NodeStats {
        parked: shared.parking.len(),
        parked_dropped: shared.parking.dropped.load(Relaxed),
        twin: shared.counters.read(),
        latency: Histogram::from(&shared.latency.snapshot()),
        datagrams: ingest.datagrams.load(Relaxed),
        malformed: ingest.malformed.load(Relaxed),
        acked: ingest.acked.load(Relaxed),
        unacked: ingest.unacked.load(Relaxed),
        ..cluster_stats(cluster)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AlertCounts {
    pub raised: u64,
    pub cleared: u64,
    pub resets: u64,
    pub malformed: u64,
    /// Twin nodes whose alert stream this node follows.
    pub following: Vec<String>,
}

/// Each device's alerts, as (seq, event).
pub type History = HashMap<u64, Vec<(u32, Event)>>;

/// What the alerts node has seen.
#[derive(Default)]
pub struct AlertLog {
    raised: AtomicU64,
    cleared: AtomicU64,
    resets: AtomicU64,
    malformed: AtomicU64,
    following: Mutex<BTreeSet<String>>,
    /// Every event per device, for the demo's checks; off in the scale run.
    history: Option<Mutex<History>>,
    verbose: bool,
}

impl AlertLog {
    pub fn counts(&self) -> AlertCounts {
        let following = self.following.lock().map(|f| f.iter().cloned().collect()).unwrap_or_default();
        AlertCounts {
            raised: self.raised.load(Relaxed),
            cleared: self.cleared.load(Relaxed),
            resets: self.resets.load(Relaxed),
            malformed: self.malformed.load(Relaxed),
            following,
        }
    }

    pub fn history(&self) -> History {
        self.history.as_ref().and_then(|h| h.lock().ok().map(|h| h.clone())).unwrap_or_default()
    }

    fn record(&self, alert: Alert) {
        let counter = if alert.event == Event::Raised { &self.raised } else { &self.cleared };
        counter.fetch_add(1, Relaxed);
        if self.verbose {
            println!(
                "alert  {:?} device {} at #{} ({:.1}) on {}",
                alert.event, alert.device, alert.seq, alert.temperature, alert.node
            );
        }
        if let Some(Ok(mut history)) = self.history.as_ref().map(Mutex::lock) {
            history.entry(alert.device).or_default().push((alert.seq, alert.event));
        }
    }
}

enum Listen {
    Twins(Option<NodeChange>),
    Alerts(String, Option<StreamEvent>),
}

/// Follows `alerts/<node>` for every twin node, from the stream's first alert.
struct Listener {
    twins: Nodes,
    streams: StreamNode,
    log: Arc<AlertLog>,
    /// Each twin node's subscription, and the last alert handled from it.
    following: HashMap<String, (JobHandle, Option<Cursor>)>,
}

impl Listener {
    fn follow(
        &mut self,
        node: String,
        start: StartPosition,
        cx: &mut Context<'_, Self>,
    ) -> std::result::Result<(), String> {
        let options = SubscribeOptions { start, ..SubscribeOptions::default() };
        let subscription = self.streams.subscribe(&alerts_key(&node), options).map_err(|e| format!("{e:?}"))?;
        let tag = node.clone();
        let job = cx.attach(subscription, move |event| Listen::Alerts(tag.clone(), event));
        let cursor = if let StartPosition::After(cursor) = start { Some(cursor) } else { None };
        self.following.insert(node, (job.map_err(|e| format!("{e:?}"))?, cursor));
        Ok(())
    }
}

impl Actor for Listener {
    type Message = Listen;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        cx.attach(self.twins.changes(), Listen::Twins).map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Listen, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        match msg {
            Listen::Twins(Some(NodeChange::Joined(node))) if !self.following.contains_key(&*node) => {
                self.follow(node.to_string(), StartPosition::First, cx)?
            }
            // A twin node that left keeps its subscription, which follows the stream if it comes back.
            Listen::Twins(_) => {}
            Listen::Alerts(node, Some(StreamEvent::Batch(batch))) => {
                for record in batch.records() {
                    match serde_json::from_slice(record) {
                        Ok(alert) => self.log.record(alert),
                        Err(_) => drop(self.log.malformed.fetch_add(1, Relaxed)),
                    }
                }
                if let Some((_, cursor)) = self.following.get_mut(&node) {
                    *cursor = Some(batch.cursor());
                }
            }
            Listen::Alerts(node, Some(StreamEvent::Ready { .. })) => {
                if let Ok(mut following) = self.log.following.lock() {
                    following.insert(node);
                }
            }
            // Fell behind: pick up after the last alert handled. After a restart or a close the old position means
            // nothing, so follow what comes next; the reset count says alerts may be missing.
            Listen::Alerts(node, Some(event)) if event.is_terminal() => {
                eprintln!("alerts: alerts/{node} ended ({event:?}); following it again");
                self.log.resets.fetch_add(1, Relaxed);
                let behind = matches!(
                    event,
                    StreamEvent::ResetRequired(ResetReason::SlowConsumer) | StreamEvent::End(EndReason::SlowConsumer)
                );
                if let Some((_, cursor)) = self.following.get_mut(&node).filter(|_| !behind) {
                    *cursor = None;
                }
            }
            // The job is over, after its terminal event or not: follow again, once.
            Listen::Alerts(node, None) => {
                let cursor = self.following.remove(&node).and_then(|(_, cursor)| cursor);
                self.follow(node, cursor.map_or(StartPosition::Live, StartPosition::After), cx)?;
            }
            Listen::Alerts(..) => {}
        }
        Ok(())
    }
}

pub struct AlertsNode {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
    pub log: Arc<AlertLog>,
    pub http: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl AlertsNode {
    pub async fn start(
        settings: &Settings,
        transport: axon_net::Config,
        discovery: impl Discovery,
        ownership: Arc<dyn OwnershipAuthority>,
        keep_history: bool,
        verbose: bool,
    ) -> Result<Self> {
        let (system, cluster) = settings.cluster("alerts", transport, discovery, ownership).await?;
        let twins = BTreeMap::from([("role".to_owned(), "twin".to_owned())]);
        let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![twins])).await?;
        let history = keep_history.then(Mutex::default);
        let log = Arc::new(AlertLog { history, verbose, ..AlertLog::default() });
        let twin_nodes = cluster.nodes(&Placement::default().require("role", "twin"));
        let listener =
            Listener { twins: twin_nodes, streams: streams.clone(), log: log.clone(), following: HashMap::new() };
        system.spawn(listener).name("alerts").start()?;
        let counts = log.clone();
        let stats =
            Arc::new(move |cluster: &Cluster| NodeStats { alerts: Some(counts.counts()), ..cluster_stats(cluster) });
        let (http, serve) = serve_http(settings.http, Http { cluster: cluster.downgrade(), stats }).await?;
        Ok(Self { system, cluster, streams, log, http, tasks: vec![serve] })
    }

    pub async fn shutdown(self) -> Result<()> {
        self.tasks.iter().for_each(JoinHandle::abort);
        self.system.shutdown().await?;
        Ok(())
    }
}

/// `GET path` over plain HTTP/1.0: status and body. Enough for the demo and the scale run's polling.
pub async fn http_get(addr: SocketAddr, path: &str) -> Result<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or("no HTTP header")?;
    let status = head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or("no HTTP status")?;
    Ok((status, body.to_owned()))
}
