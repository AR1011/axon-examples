//! One chat node: a cluster member that hosts rooms, relays their streams and serves websockets. Every node is
//! the same; which rooms it owns is rendezvous hashing's choice, and it pulls every other room it needs from the
//! nodes labelled `app=chat`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axon::{ActorSystem, RuntimeConfig};
use axon_cluster::{AccessPolicy, Cluster, ClusterConfig, Discovery};
use axon_net::SharedSecret;
use axon_obs::{Dashboard, DashboardConfig, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig};
use axon_stream::{StreamNode, StreamNodeConfig};
use axum::Router;
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Deserialize;
use tokio::task::JoinHandle;

use crate::proto::RoomApi;
use crate::room::{self, Room};
use crate::session::Session;

pub struct NodeConfig {
    pub id: String,
    pub secret: String,
    pub cluster: SocketAddr,
    pub http: SocketAddr,
    /// Runs a metrics collector and the console here.
    pub console: Option<SocketAddr>,
    pub workers: usize,
    /// With seed discovery: 500 ms and 1 s, as the cluster guide asks.
    pub heartbeat: Option<(Duration, Duration)>,
    /// A room nobody posted to for this long closes; the next post opens it again.
    pub room_idle: Duration,
}

impl NodeConfig {
    pub fn local(id: &str) -> Self {
        Self {
            id: id.into(),
            secret: "chat-example-secret-16+".into(),
            cluster: ([127, 0, 0, 1], 0).into(),
            http: ([127, 0, 0, 1], 0).into(),
            console: None,
            workers: 2,
            heartbeat: None,
            room_idle: Duration::from_secs(600),
        }
    }
}

pub fn labels() -> BTreeMap<String, String> {
    BTreeMap::from([("app".to_string(), "chat".to_string())])
}

#[derive(Clone)]
struct Shared {
    system: ActorSystem,
    cluster: Cluster,
    streams: StreamNode,
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
    pub http: SocketAddr,
    pub console: Option<SocketAddr>,
    server: JoinHandle<()>,
}

impl Node {
    pub async fn start(cfg: NodeConfig, discovery: impl Discovery) -> Result<Self, String> {
        let transport = Self::transport(&cfg.id, &cfg.secret)?;
        Self::start_with(cfg, transport, discovery).await
    }

    /// Seed discovery needs the same transport value as the cluster: it carries this boot's identity.
    pub fn transport(id: &str, secret: &str) -> Result<axon_net::Config, String> {
        let secret = SharedSecret::new(secret).map_err(|e| e.to_string())?;
        let mut transport = axon_net::Config::new(id, secret);
        transport.allow_plaintext = true; // tests on loopback, `chat seed|node` only with --plaintext
        Ok(transport)
    }

    pub async fn start_with(
        cfg: NodeConfig,
        transport: axon_net::Config,
        discovery: impl Discovery,
    ) -> Result<Self, String> {
        let err = |e: &dyn std::fmt::Display| e.to_string();
        let mut ccfg = ClusterConfig::new(cfg.cluster, transport, discovery);
        ccfg.labels = labels();
        ccfg.labels.insert("role".into(), "chat".into());
        if let Some((interval, grace)) = cfg.heartbeat {
            // The operator's; with seeds the guide asks for 500 ms and 1 s.
            (ccfg.heartbeat_interval, ccfg.heartbeat_grace) = (interval, grace);
        }
        let runtime = RuntimeConfig { workers: cfg.workers, ..Default::default() };
        let system = ActorSystem::builder().runtime(runtime).timing_sample_every(64).build().map_err(|e| err(&e))?;
        let cluster = Cluster::start(&system, ccfg).await.map_err(|e| err(&e))?;
        // Every node is an origin for the rooms it owns and a relay for the rest.
        let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![labels()])).await.map_err(|e| err(&e))?;

        let hosted = streams.clone();
        cluster
            .kind::<RoomApi>(room::KIND)
            .idle(cfg.room_idle)
            .register(move |cx, _args| Ok(Room::new(cx.key.as_deref().ok_or("a room needs a name")?, hosted.clone())))
            .await
            .map_err(|e| err(&e))?;

        let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
        Telemetry::start(&system, &cluster, telemetry).await?.watch_streams(&streams);
        let mut console = None;
        if let Some(bind) = cfg.console {
            let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
            MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
            let dashboard = Dashboard::start(&system, &cluster, DashboardConfig { bind, ..Default::default() }).await?;
            console = Some(dashboard.address());
        }

        let shared = Shared { system: system.clone(), cluster: cluster.clone(), streams: streams.clone() };
        let app = Router::new().route("/ws", get(upgrade)).with_state(shared);
        let listener = tokio::net::TcpListener::bind(cfg.http).await.map_err(|e| err(&e))?;
        let http = listener.local_addr().map_err(|e| err(&e))?;
        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("http server: {e}");
            }
        });
        Ok(Self { system, cluster, streams, http, console, server })
    }

    /// Ends the node as a crash would, for tests: no leave, so its rooms stop with it and restart elsewhere only
    /// when asked for.
    pub fn crash(self) {
        self.server.abort();
    }

    /// Leaves the cluster cleanly: rooms close their streams and move, sessions end.
    pub async fn shutdown(self) {
        self.server.abort();
        if let Err(e) = self.system.shutdown().await {
            eprintln!("shutdown: {e:?}");
        }
    }
}

#[derive(Deserialize)]
struct Who {
    user: String,
}

async fn upgrade(ws: WebSocketUpgrade, Query(who): Query<Who>, State(shared): State<Shared>) -> Response {
    if who.user.is_empty() || who.user.len() > 64 {
        return (axum::http::StatusCode::BAD_REQUEST, "user must be 1 to 64 bytes").into_response();
    }
    ws.max_message_size(16 << 10).on_upgrade(move |socket| async move {
        let session = Session::new(who.user, socket, shared.cluster.clone(), shared.streams.clone());
        // Refused only at the runtime's actor limit; the socket closes when the session is dropped.
        if let Err(e) = shared.system.spawn(session).start() {
            eprintln!("session refused: {e}");
        }
    })
}
