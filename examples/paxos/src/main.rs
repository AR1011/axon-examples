//! usage: paxos acceptor --plaintext | app --plaintext | demo [lease-secs=6]
//!        no TLS between nodes: an acceptor or an app starts only with --plaintext, on loopback or a private network
//! env:   NODE_ID     this node's id; an acceptor's must be on ACCEPTORS (default: the role)
//!        ACCEPTORS   lease-0,lease-1,lease-2   the same on every node
//!        SEEDS       127.0.0.1:7001            every acceptor's seed server
//!        HOST        127.0.0.1                 where the others reach this machine
//!        PORT        7400                      this node's cluster port; two nodes on one machine need two
//!        SEED_PORT   7001                      an acceptor's seed server
//!        LEASE_SECS  6, SECRET (the same on every node), KEYS 8 (the keys an app asks for)
//!        LEASE_DIR   where an acceptor logs who holds each key (default: a directory of this process under the
//!                    system's temporary one, so a start reads no earlier leases)
use std::collections::BTreeMap;
use std::time::Duration;

use axon_cluster::{Placement, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use bytes::Bytes;
use paxos::{KIND, OwnerApi, Result, Settings};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn list(name: &str, default: &str) -> Vec<String> {
    env(name, default).split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let role = std::env::args().nth(1).unwrap_or_default();
    if role == "demo" {
        let secs = std::env::args().nth(2).map_or(Ok(6), |s| s.parse())?;
        return paxos::demo(Duration::from_secs(secs)).await;
    }
    if role != "acceptor" && role != "app" {
        return Err("usage: paxos acceptor --plaintext | app --plaintext | demo [lease-secs]".into());
    }
    if !std::env::args().any(|a| a == "--plaintext") {
        return Err(
            "paxos has no TLS between nodes: add --plaintext to run on loopback or a private network only".into()
        );
    }
    let (host, port) = (env("HOST", "127.0.0.1"), env("PORT", "7400"));
    let id = env("NODE_ID", &role);
    let acceptors = list("ACCEPTORS", "lease-0,lease-1,lease-2");
    if role == "acceptor" && !acceptors.contains(&id) {
        return Err(format!("{id} is not on ACCEPTORS {acceptors:?}, so it would never vote").into());
    }
    let settings = Settings {
        id,
        bind: format!("0.0.0.0:{port}").parse()?,
        advertise: Some(format!("{host}:{port}").parse()?),
        acceptors,
        // PaxosConfig::new's 6 s unless LEASE_SECS says otherwise.
        lease: Duration::from_secs(env("LEASE_SECS", "6").parse()?),
        secret: env("SECRET", "paxos-example-secret"),
        dir: std::env::var("LEASE_DIR").map_or_else(
            |_| std::env::temp_dir().join(format!("paxos-leases-{}", std::process::id())),
            std::path::PathBuf::from,
        ),
    };

    // An acceptor machine also runs a seed server, so the cluster needs nothing but these processes.
    let _seed = match role.as_str() {
        "acceptor" => {
            let bind = format!("0.0.0.0:{}", env("SEED_PORT", "7001")).parse()?;
            let transport = settings.transport(&format!("{}-seed", settings.id))?;
            Some(SeedServer::bind(SeedServerConfig::new(bind, &transport)).await?)
        }
        _ => None,
    };
    let transport = settings.transport(&settings.id)?;
    let discovery = SeedDiscovery::start(SeedConfig::new(list("SEEDS", "127.0.0.1:7001"), &transport)).await?;
    let node = match role.as_str() {
        "acceptor" => settings.start(transport, discovery).await?,
        _ => settings.start_app(transport, discovery).await?,
    };
    println!("{} ({role}) on {}", settings.id, node.cluster.local_addr());

    // Prints what changed once a second: an acceptor its lease counters, an app where each key runs.
    let keys: Vec<String> = (0..env("KEYS", "8").parse::<usize>()?).map(|k| format!("k{k}")).collect();
    let (mut seen, anywhere) = (BTreeMap::new(), Placement::default());
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let pass = async {
            tick.tick().await;
            match role.as_str() {
                "acceptor" => node.cluster.stats().ownership.map(|o| o.stats).unwrap_or_default(),
                _ => {
                    let mut now = Vec::new();
                    for key in &keys {
                        let owner = node.cluster.get_or_spawn::<OwnerApi>(KIND, key, Bytes::new(), &anywhere).await;
                        let where_ = match owner {
                            Ok(owner) => format!("on {} at epoch {}", owner.node(), owner.epoch().unwrap_or_default()),
                            Err(error) => format!("refused: {error}"),
                        };
                        now.push((key.clone(), where_));
                    }
                    now
                }
            }
        };
        let now: Vec<(String, String)> = tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            now = pass => now,
        };
        for (what, value) in now {
            if seen.get(&what) != Some(&value) {
                println!("{what} {value}");
                seen.insert(what, value);
            }
        }
    }
    node.system.shutdown().await?;
    Ok(())
}
