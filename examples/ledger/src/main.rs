//! usage: ledger demo [accounts=300] [commands=20000] [lease-secs=2] [dir=ledger-data]   (empties dir first)
//!        ledger host --plaintext | load --plaintext | audit   (no TLS between nodes: loopback or a private network)
//! env:   NODE_ID     a host's id, one of HOSTS (default ledger-a)
//!        HOSTS       ledger-a,ledger-b,ledger-c   the same everywhere: each one an acceptor and a seed server
//!        SEEDS       127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003   every host's seed server
//!        HOST        127.0.0.1, PORT 7400 (a host's; the load takes any), SEED_PORT 7001
//!        DIR         ledger-data, the same directory for every process
//!        LEASE_SECS  2, SECRET (the same everywhere), ACCOUNTS 300 and COMMANDS 20000 (the load's)
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use ledger::demo::{Load, open, settle};
use ledger::{KIND, Result, Settings, account, store};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn list(name: &str, default: &str) -> Vec<String> {
    env(name, default).split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize, default: &str| args.get(i).cloned().unwrap_or_else(|| default.to_owned());
    let role = arg(0, "");
    let dir = std::path::PathBuf::from(env("DIR", "ledger-data"));
    match role.as_str() {
        "demo" => {
            let dir = std::path::PathBuf::from(arg(4, "ledger-data"));
            let _ = std::fs::remove_dir_all(&dir);
            let lease = Duration::from_secs_f64(arg(3, "2").parse()?);
            let mut report = ledger::demo(&dir, arg(1, "300").parse()?, arg(2, "20000").parse()?, lease).await?;
            report.audit.balance.clear();
            println!("{report:#?}");
            return Ok(());
        }
        "audit" => {
            println!("{:?}", ledger::audit::audit(&store::cut(&dir)?)?);
            return Ok(());
        }
        "host" | "load" if !args.iter().any(|a| a == "--plaintext") => {
            return Err(
                "ledger has no TLS between nodes: add --plaintext to run on loopback or a private network only".into(),
            );
        }
        "host" | "load" => {}
        _ => return Err("usage: ledger demo [accounts] [commands] [lease-secs] [dir] | host | load | audit".into()),
    }
    let hosts = list("HOSTS", &ledger::demo::HOSTS.join(","));
    let id = if role == "host" { env("NODE_ID", &hosts[0]) } else { env("NODE_ID", "load") };
    if role == "host" && !hosts.contains(&id) {
        return Err(format!("{id} is not on HOSTS {hosts:?}").into());
    }
    let host = env("HOST", "127.0.0.1");
    let port = if role == "host" { env("PORT", "7400") } else { env("PORT", "0") };
    let settings = Settings {
        bind: format!("{host}:{port}").parse()?,
        secret: env("SECRET", "ledger-example-secret"),
        dir: dir.clone(),
        ..Settings::new(hosts.clone(), Duration::from_secs_f64(env("LEASE_SECS", "2").parse()?))
    };
    let _seed = match role.as_str() {
        "host" => {
            let bind = format!("{host}:{}", env("SEED_PORT", "7001")).parse()?;
            let transport = settings.transport(&format!("{id}-seed"))?;
            Some(SeedServer::bind(SeedServerConfig::new(bind, &transport)).await?)
        }
        _ => None,
    };
    let transport = settings.transport(&id)?;
    let seeds = list("SEEDS", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003");
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    let node = settings.start(transport, discovery).await?;
    println!("{id} ({role}) on {}", node.cluster.local_addr());

    if role == "host" {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut seen = String::new();
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tick.tick() => {}
            }
            let [live, fenced, waiting] = [&account::LIVE, &account::FENCED, &store::WAITING].map(|n| n.load(Relaxed));
            let now = format!("{id}: {live} accounts, {fenced} fenced, {waiting} waiting for an audit");
            if now != seen {
                println!("{now}");
                seen = now;
            }
        }
        node.system.shutdown().await?;
        return Ok(());
    }

    let (accounts, commands) = (env("ACCOUNTS", "300").parse()?, env("COMMANDS", "20000").parse()?);
    let patience = settings.lease * 5 + Duration::from_secs(10);
    let hosted =
        node.cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind(KIND)).count() >= hosts.len());
    tokio::time::timeout(patience, hosted)
        .await
        .map_err(|_| format!("{} hosts not seen in {patience:?}", hosts.len()))??;
    open(&node.cluster, accounts, patience).await?;
    println!("{accounts} accounts open");
    let (load, mut workers) = Load::start(&node.cluster, accounts, commands, patience);
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    loop {
        tokio::select! {
            done = workers.join_next() => match done {
                Some(done) => done??,
                None => break,
            },
            _ = tick.tick() => println!("progress {} commands", load.done.load(Relaxed)),
        }
    }
    let elapsed = started.elapsed();
    println!("{commands} commands in {elapsed:.1?}: {:.0}/s", commands as f64 / elapsed.as_secs_f64());
    let balances = settle(&node.cluster, accounts, patience).await?;
    node.system.shutdown().await?;
    let audit = load.check(&dir, accounts, &balances)?;
    println!(
        "audit passed: {} records, {} owners, {} bytes fenced, {} settled, {} refunded",
        audit.records, audit.owners, audit.fenced_bytes, audit.settled, audit.refunded
    );
    Ok(())
}
