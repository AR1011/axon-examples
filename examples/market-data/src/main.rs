//! Binance futures trades → on-demand OHLCV candles → WebSocket clients, over a three-role axon cluster.
//!
//! usage: market-data consumer | processor | ws [port=9000] | console [port=8080] | load [sessions=10] [url]
//! env:   CONSUL (http://127.0.0.1:18500), CLICKHOUSE (http://127.0.0.1:18123), CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
//!        NODE_ID (defaults to the role; set it to run two nodes of one role)
mod consumer;
mod load;
mod processor;
mod ws;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{AccessPolicy, Cluster, ClusterConfig, ConsulConfig, ConsulDiscovery, ConsulLeases};
use axon_net::SharedSecret;
use axon_obs::{
    ClickHouseConfig, ClickHouseProvider, Dashboard, DashboardConfig, MetricsConfig, MetricsService, Telemetry,
    TelemetryConfig,
};
use axon_stream::{StreamKey, StreamNode, StreamNodeConfig};
use serde::{Deserialize, Serialize};

pub const SYMBOLS: [&str; 5] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"];

/// One record of the `trades/<symbol>` stream, as published by the consumer and forwarded as-is to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub s: String,
    pub t: u64,
    pub p: f64,
    pub q: f64,
    pub b: bool,
}

pub fn trades_key(symbol: &str) -> StreamKey {
    StreamKey::new("trades", symbol)
}

pub fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

#[derive(Clone)]
pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn labels(role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("role".to_owned(), role.to_owned())])
}

/// A cluster node: Consul for discovery and key ownership, telemetry for the console, one stream node
/// that pulls what it does not originate from the roles in `streams`.
async fn node(role: &str, streams: StreamNodeConfig) -> Result<Node, Box<dyn Error>> {
    let id = env("NODE_ID", role);
    let consul = ConsulConfig::new(env("CONSUL", "http://127.0.0.1:18500"), "market-data");
    let mut transport = axon_net::Config::new(&id, SharedSecret::new(*b"market-data-example-secret")?);
    transport.allow_plaintext = true; // ponytail: loopback only; pass `cfg.tls` across machines
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse()?, transport, ConsulDiscovery::new(consul.clone())?);
    cfg.ownership = Some(Arc::new(ConsulLeases::new(consul, &id)?));
    cfg.labels = labels(role);
    let system = ActorSystem::builder().timing_sample_every(64).tracing(4096, 1000).build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, telemetry).await?;
    let streams = StreamNode::start(&cluster, streams).await?;
    telemetry.watch_streams(&streams);
    println!("{id}: cluster node on {}", cluster.local_addr());
    Ok(Node { system, cluster, streams })
}

/// The collector and the console. History goes to ClickHouse.
async fn console(node: &Node, port: u16) -> Result<(), Box<dyn Error>> {
    let store = ClickHouseProvider::connect(ClickHouseConfig {
        url: env("CLICKHOUSE", "http://127.0.0.1:18123"),
        username: env("CLICKHOUSE_USER", "axon"),
        password: env("CLICKHOUSE_PASSWORD", "axon-playground"),
        ..Default::default()
    })
    .await?;
    let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
    MetricsService::start(&node.system, &node.cluster, Arc::new(store), metrics).await?;
    let cfg = DashboardConfig { bind: ([127, 0, 0, 1], port).into(), ..Default::default() };
    let dashboard = Dashboard::start(&node.system, &node.cluster, cfg).await?;
    println!("console: http://{}", dashboard.address());
    Ok(())
}

/// Ctrl-C or SIGTERM, then a clean leave: keyed actors hand off, leases and the Consul record are released.
async fn wait(node: Node) -> Result<(), Box<dyn Error>> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tokio::time::timeout(Duration::from_secs(10), node.system.shutdown()).await??;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let role = args.next().unwrap_or_default();
    let mut arg = |default: &str| args.next().unwrap_or_else(|| default.to_owned());
    match role.as_str() {
        "consumer" => {
            let node = node("consumer", StreamNodeConfig::new(vec![])).await?;
            consumer::run(&node).await?;
            wait(node).await
        }
        "processor" => {
            let node = node("processor", StreamNodeConfig::new(vec![labels("consumer")])).await?;
            processor::run(&node).await?;
            wait(node).await
        }
        "ws" => {
            let port = arg("9000").parse()?;
            // Trades come from the consumer, candles from whichever processor samples them.
            let streams =
                StreamNodeConfig::new(vec![labels("consumer")]).upstream_for("candles", vec![labels("processor")]);
            let node = node("ws", streams).await?;
            ws::run(&node, port).await?;
            wait(node).await
        }
        "console" => {
            let port = arg("8080").parse()?;
            let node = node("console", StreamNodeConfig::new(vec![])).await?;
            console(&node, port).await?;
            wait(node).await
        }
        "load" => {
            let sessions = arg("10").parse()?;
            load::run(&arg("ws://127.0.0.1:9000"), sessions).await
        }
        _ => {
            eprintln!("usage: market-data consumer | processor | ws [port] | console [port] | load [sessions] [url]");
            std::process::exit(2)
        }
    }
}
