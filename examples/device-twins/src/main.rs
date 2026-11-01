//! usage: device-twins demo [devices=20000] | scale [devices=200000] [reports=30] [period-ms=2000] | alerts | twin
//! `alerts` and `twin` are the processes `scale` starts; they print `READY <json>` once they serve, the alerts node
//! once its lease acceptor is warm. Their env:
//!        NODE_ID     this node's id (default: the role); the alerts node must be `alerts`, the only acceptor
//!        SEEDS       the alerts node's seed server, for a twin node
//!        IDLE_SECS   30   how long a twin waits for a report before it stops
//!        LEASES      4194304   leases the acceptor keeps, one per live twin
//!        LEASE_DIR   where the acceptor logs who holds each key (default: a directory of this process under the
//!                    system's temporary one, so a run starts with no leases)
use std::time::Duration;

use axon_cluster::{PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use device_twins::demo::{DemoConfig, demo};
use device_twins::scale::{Ready, Scale, scale};
use device_twins::{AlertsNode, Result, Settings, TwinNode};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn arg<T: std::str::FromStr>(n: usize, default: T) -> Result<T> {
    std::env::args().nth(n).map_or(Ok(default), |s| s.parse().map_err(|_| format!("argument {n}: {s}").into()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let role = std::env::args().nth(1).unwrap_or_default();
    match role.as_str() {
        "demo" => demo(DemoConfig { devices: arg(2, 20_000)?, ..DemoConfig::default() }).await,
        "scale" => {
            let period = Duration::from_millis(arg(4, 2000)?);
            scale(Scale { devices: arg(2, 200_000)?, reports: arg(3, 30)?, period }).await
        }
        "alerts" | "twin" => node(&role).await,
        _ => Err("usage: device-twins demo [devices] | scale [devices] [reports] [period-ms] | alerts | twin".into()),
    }
}

async fn node(role: &str) -> Result<()> {
    let settings = Settings {
        // IDLE_SECS, 30 s unless set: how long a silent twin stays up.
        idle: Duration::from_secs(env("IDLE_SECS", "30").parse()?),
        ..Settings::local(&env("NODE_ID", role))
    };
    let mut paxos = PaxosConfig::new(["alerts"]);
    paxos.max_leases = env("LEASES", "4194304").parse()?;
    // The directory this process made for its leases goes when it ends.
    let mut made = None;
    if role == "alerts" {
        let dir = std::env::var("LEASE_DIR").map_or_else(
            |_| made.insert(std::env::temp_dir().join(format!("device-twins-leases-{}", std::process::id()))).clone(),
            std::path::PathBuf::from,
        );
        paxos.store = Some(std::sync::Arc::new(axon::files::OsFiles::new(dir)));
    }
    let leases = std::sync::Arc::new(PaxosLeases::new(paxos).map_err(|e| format!("{e:?}"))?);
    let transport = settings.transport()?;
    let seed = match role {
        "alerts" => {
            let seed = Settings { id: format!("{}-seed", settings.id), ..settings.clone() };
            Some(SeedServer::bind(SeedServerConfig::new(settings.bind, &seed.transport()?)).await?)
        }
        _ => None,
    };
    let seeds = seed.as_ref().map_or_else(|| env("SEEDS", ""), |s| s.local_addr().to_string());
    let discovery = SeedDiscovery::start(SeedConfig::new([seeds], &transport)).await?;
    let (http, udp, shutdown) = if role == "alerts" {
        let node = AlertsNode::start(&settings, transport, discovery, leases.clone(), false, false).await?;
        // A started acceptor refuses every lease for one lease time; twins would start only after it.
        while leases.warming() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (node.http, None, Box::pin(node.shutdown()) as std::pin::Pin<Box<dyn Future<Output = Result<()>>>>)
    } else {
        let node = TwinNode::start(&settings, transport, discovery, leases).await?;
        (node.http, Some(node.udp), Box::pin(node.shutdown()) as _)
    };
    let ready = Ready { node: settings.id.clone(), seed: seed.as_ref().map(SeedServer::local_addr), udp, http };
    println!("READY {}", serde_json::to_string(&ready)?);
    tokio::signal::ctrl_c().await?;
    let ended = shutdown.await;
    if let Some(dir) = made {
        let _ = std::fs::remove_dir_all(dir);
    }
    ended
}
