//! `chat seed | node | load`: see the README. A seed or a node starts only with `--plaintext`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use chat::load::{self, LoadConfig};
use chat::{Node, NodeConfig};

fn flags(args: &[String]) -> Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let name = flag.strip_prefix("--").ok_or_else(|| format!("expected --flag, got {flag}"))?;
        out.insert(name.to_string(), it.next().ok_or_else(|| format!("--{name} needs a value"))?.clone());
    }
    Ok(out)
}

fn get<T: std::str::FromStr>(f: &HashMap<String, String>, name: &str, default: T) -> Result<T, String> {
    f.get(name).map_or(Ok(default), |v| v.parse().map_err(|_| format!("bad --{name} {v}")))
}

fn secret(f: &HashMap<String, String>) -> String {
    f.get("secret").cloned().unwrap_or_else(|| NodeConfig::local("x").secret)
}

async fn until_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    // No TLS between nodes: a node talks unencrypted only where `--plaintext` says it may.
    let (plaintext, args): (Vec<String>, Vec<String>) = std::env::args().skip(1).partition(|a| a == "--plaintext");
    let (command, rest) = args.split_first().ok_or("usage: chat seed|node [--plaintext] | load [--flag value]..")?;
    let f = flags(rest)?;
    match command.as_str() {
        "seed" | "node" if plaintext.is_empty() => {
            return Err(
                "chat has no TLS between nodes: add --plaintext to run on loopback or a private network only".into()
            );
        }
        "seed" => {
            let id = f.get("id").cloned().unwrap_or_else(|| "seed-1".into());
            let transport = Node::transport(&id, &secret(&f))?;
            let bind: SocketAddr = get(&f, "bind", ([127, 0, 0, 1], 0).into())?;
            let server = SeedServer::bind(SeedServerConfig::new(bind, &transport)).await.map_err(|e| e.to_string())?;
            println!("{}", serde_json::json!({ "seed": server.local_addr().to_string() }));
            until_signal().await;
        }
        "node" => {
            let id = f.get("id").cloned().ok_or("--id is required")?;
            let seeds: Vec<String> =
                f.get("seeds").ok_or("--seeds is required")?.split(',').map(String::from).collect();
            let mut cfg = NodeConfig::local(&id);
            cfg.secret = secret(&f);
            cfg.http = get(&f, "http", cfg.http)?;
            cfg.workers = get(&f, "workers", cfg.workers)?;
            cfg.console = f.get("console").map(|c| c.parse()).transpose().map_err(|_| "bad --console")?;
            // The cluster guide's settings for seed discovery.
            cfg.heartbeat = Some((Duration::from_millis(500), Duration::from_secs(1)));
            let transport = Node::transport(&cfg.id, &cfg.secret)?;
            let discovery =
                SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await.map_err(|e| e.to_string())?;
            let node = Node::start_with(cfg, transport, discovery).await?;
            let console = node.console.map(|c| c.to_string());
            println!("{}", serde_json::json!({ "node": id, "http": node.http.to_string(), "console": console }));
            until_signal().await;
            node.shutdown().await;
        }
        "load" => {
            let nodes = f.get("nodes").ok_or("--nodes a,b,c is required")?;
            let nodes =
                nodes.split(',').map(|n| n.parse().map_err(|_| format!("bad node {n}"))).collect::<Result<_, _>>()?;
            let cfg = LoadConfig {
                nodes,
                sessions: get(&f, "sessions", 1000)?,
                rooms: get(&f, "rooms", 20)?,
                per_session: get(&f, "per-session", 3)?,
                posters: get(&f, "posters", 50)?,
                rate: get(&f, "rate", 200.0)?,
                duration: Duration::from_secs_f64(get(&f, "secs", 20.0)?),
                settle: Duration::from_secs_f64(get(&f, "settle", 10.0)?),
                slow: get(&f, "slow", 0)?,
            };
            let report = load::run(cfg).await?;
            println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        }
        other => return Err(format!("unknown command {other}")),
    }
    Ok(())
}
