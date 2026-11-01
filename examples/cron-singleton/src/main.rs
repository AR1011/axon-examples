//! usage: cron-singleton runlog --plaintext | sched --plaintext | audit < runlog.out
//!        no TLS between nodes: a node starts only with --plaintext, for loopback or a private network only
//! env:   NODE_ID     this node's id (default: the role)
//!        ACCEPTORS   runlog,s1,s2          lease acceptors, the same on every node
//!        SEEDS       127.0.0.1:7001        the run log's seed server (sched)
//!        HOST, PORT  127.0.0.1, 0          where this node listens; 0 picks a free port
//!        SEED_PORT   7001                  the run log's seed server port; 0 picks one and prints it
//!        LEASE_MS    6000, SECRET          the same on every node
//!        LEASE_DIR   where an acceptor logs who holds each key (default: a directory of this process under the
//!                    system's temporary one, so a start reads no earlier leases)
//!        JOBS        name:every_ms:offset_ms:max_runs,...   the same on every sched node
use std::collections::BTreeMap;
use std::time::Duration;

use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use cron_singleton::{
    Fire, LOG, Result, RunLogActor, Settings, audit, jobs_args, keep, parse_jobs, register_scheduler, system_wall,
};

const JOBS: &str = "beat:1000:0:1024,digest:5000:0:1";

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn list(name: &str, default: &str) -> Vec<String> {
    env(name, default).split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let role = std::env::args().nth(1).unwrap_or_default();
    if role == "audit" {
        return audit_stdin();
    }
    if role != "runlog" && role != "sched" {
        return Err("usage: cron-singleton runlog --plaintext | sched --plaintext | audit < runlog.out".into());
    }
    if !std::env::args().any(|a| a == "--plaintext") {
        return Err(
            "cron-singleton has no TLS between nodes: add --plaintext to run on loopback or a private network only"
                .into(),
        );
    }
    let host = env("HOST", "127.0.0.1");
    let settings = Settings {
        id: env("NODE_ID", &role),
        bind: format!("{host}:{}", env("PORT", "0")).parse()?,
        acceptors: list("ACCEPTORS", "runlog,s1,s2"),
        // PaxosConfig::new's 6 s unless LEASE_MS says otherwise.
        lease: Duration::from_millis(env("LEASE_MS", "6000").parse()?),
        secret: env("SECRET", "cron-singleton-example-secret"),
        dir: std::env::var("LEASE_DIR").map_or_else(
            |_| std::env::temp_dir().join(format!("cron-singleton-leases-{}", std::process::id())),
            std::path::PathBuf::from,
        ),
    };

    // The run log's machine also runs the seed server: it is the one machine the cron cannot do without.
    let (seed, seeds) = match role.as_str() {
        "runlog" => {
            let bind = format!("{host}:{}", env("SEED_PORT", "7001")).parse()?;
            let server = SeedServer::bind(SeedServerConfig::new(bind, &settings.transport("runlog-seed")?)).await?;
            let seeds = vec![server.local_addr().to_string()];
            println!("seed {}", seeds[0]);
            (Some(server), seeds)
        }
        _ => (None, list("SEEDS", "127.0.0.1:7001")),
    };
    let transport = settings.transport(&settings.id)?;
    let discovery = SeedDiscovery::start(SeedConfig::new(&seeds, &transport)).await?;
    let (system, cluster) = settings.start(transport, discovery).await?;
    println!("{} ({role}) on {}", settings.id, cluster.local_addr());

    let keeper = match role.as_str() {
        "runlog" => {
            // ponytail: stdout is the log's record; a real run log writes a table, off the handler's thread.
            let wall = system_wall();
            let log = system.spawn(RunLogActor::new(move |line| println!("{line} {}", wall()))).name(LOG).start()?;
            cluster.publish::<cron_singleton::RunLogApi>(&log, LOG)?;
            None
        }
        _ => {
            let jobs = parse_jobs(&env("JOBS", JOBS))?;
            register_scheduler(&cluster, system_wall()).await?;
            Some(tokio::spawn(keep(cluster.clone(), jobs_args(&jobs)?)))
        }
    };
    tokio::signal::ctrl_c().await?;
    keeper.inspect(|k| k.abort());
    system.shutdown().await?;
    drop(seed);
    Ok(())
}

/// Reads a run log's output and checks it: no tick fired twice, none skipped, and no fire from an owner after a newer
/// one claimed (the epochs of accepted fires never go down).
fn audit_stdin() -> Result<()> {
    let jobs = parse_jobs(&env("JOBS", JOBS))?;
    let (mut fires, mut epochs, mut refused, mut claims) = (Vec::new(), Vec::new(), 0usize, Vec::new());
    let mut late: BTreeMap<u64, (usize, u64)> = BTreeMap::new(); // per epoch: fires, the latest one's delay in ms
    for line in std::io::stdin().lines() {
        let line = line?;
        let words: Vec<&str> = line.split_whitespace().collect();
        match words[..] {
            ["FIRE", job, from, tick, epoch, _node, at] => {
                let (tick, epoch, at) = (tick.parse()?, epoch.parse::<u64>()?, at.parse::<u64>()?);
                let due = jobs.iter().find(|j| j.name == job).ok_or(format!("unknown job {job}"))?.starts_at(tick);
                let entry = late.entry(epoch).or_default();
                *entry = (entry.0 + 1, entry.1.max(at.saturating_sub(due)));
                fires.push(Fire { job: job.into(), from: from.parse()?, tick });
                epochs.push(epoch);
            }
            ["Fenced" | "Duplicate" | "Gap", ..] => refused += 1,
            ["CLAIM", epoch, node, _at] => claims.push(format!("{node}@{epoch}")),
            _ => {}
        }
    }
    let covered = audit(&fires)?;
    if let Some(w) = epochs.windows(2).find(|w| w[1] < w[0]) {
        return Err(format!("epoch {} fired after epoch {} had", w[1], w[0]).into());
    }
    println!("ok: {} fires, {} refused, claims {}", fires.len(), refused, claims.join(" "));
    for (job, (from, to)) in covered {
        println!("  {job}: ticks {from}..={to}, each once");
    }
    for (epoch, (n, ms)) in late {
        println!("  epoch {epoch}: {n} fires, the latest {ms} ms after its tick");
    }
    Ok(())
}
