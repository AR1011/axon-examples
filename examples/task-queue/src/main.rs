//! usage: task-queue queue | worker | produce [n=1000] [sleep-ms] | status | dead | retry | demo
//!        each but demo joins the cluster with no TLS, so it starts only with --plaintext, for loopback or a
//!        private network
//! env:   NODE_ID      this node's id (default: the role, with the pid for clients)
//!        SEEDS        127.0.0.1:7101          the queue node's seed server
//!        HOST         127.0.0.1               where the others reach this machine
//!        PORT         0                       this node's cluster port, on every interface (0: any free one)
//!        SEED_PORT    7101                    the queue node's seed server
//!        SECRET       the same on every node
//!        CAPACITY 10000, VISIBILITY_MS 5000   the queue node's limits
//!        CONCURRENCY 8                        a worker's jobs at once
use std::time::{Duration, Instant};

use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use task_queue::broker::{Broker, BrokerConfig};
use task_queue::demo::{self, Params, Work};
use task_queue::worker::{WorkerConfig, start_worker};
use task_queue::{Answer, BROKER, BrokerApi, Call, Delivery, Result, unexpected};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    std::env::var(name).map_or(Ok(default), |v| Ok(v.parse()?))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (plaintext, args): (Vec<String>, Vec<String>) = std::env::args().skip(1).partition(|a| a == "--plaintext");
    let role = args.first().map(String::as_str).unwrap_or_default();
    if role == "demo" {
        demo::demo(Params::default()).await?;
        return Ok(());
    }
    if !["queue", "worker", "produce", "status", "dead", "retry"].contains(&role) {
        return Err("usage: task-queue queue | worker | produce [n] [sleep-ms] | status | dead | retry | demo".into());
    }
    if plaintext.is_empty() {
        return Err(
            "task-queue has no TLS between nodes: add --plaintext to run on loopback or a private network only".into(),
        );
    }
    let id = match role {
        "queue" | "worker" => env("NODE_ID", role),
        _ => env("NODE_ID", &format!("{role}-{}", std::process::id())),
    };
    let (host, port) = (env("HOST", "127.0.0.1"), env("PORT", "0"));
    let secret = env("SECRET", "task-queue-example-secret");
    let seed_port = env("SEED_PORT", "7101");

    // The queue node also runs the seed server, so the cluster needs nothing but these processes.
    let _seed = match role {
        "queue" => {
            let bind = format!("0.0.0.0:{seed_port}").parse()?;
            let transport = task_queue::transport(&format!("{id}-seed"), &secret)?;
            Some(SeedServer::bind(SeedServerConfig::new(bind, &transport)).await?)
        }
        _ => None,
    };
    let transport = task_queue::transport(&id, &secret)?;
    let seeds: Vec<String> = env("SEEDS", &format!("127.0.0.1:{seed_port}")).split(',').map(str::to_owned).collect();
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    // A wildcard bind needs an address to advertise; its port 0 takes the port bound.
    let (bind, advertise) = (format!("0.0.0.0:{port}").parse()?, Some(format!("{host}:{port}").parse()?));
    let node = task_queue::start_node(transport, bind, advertise, role, discovery).await?;

    match role {
        "queue" => {
            let cfg = BrokerConfig {
                capacity: number("CAPACITY", 10_000)?,
                visibility: Duration::from_millis(number("VISIBILITY_MS", 5000)?),
                ..BrokerConfig::default()
            };
            let broker = node.system.spawn(Broker::new(cfg)).name("broker").start()?;
            node.cluster.publish::<BrokerApi>(&broker, BROKER)?;
            println!("{id} (queue) on {}", node.cluster.local_addr());
            tokio::signal::ctrl_c().await?;
        }
        "worker" => {
            let cfg = WorkerConfig { concurrency: number("CONCURRENCY", 8)?, ..WorkerConfig::default() };
            let run = |d: Delivery| async move {
                let work: Work = serde_json::from_str(&d.payload).map_err(|e| e.to_string())?;
                work.run(d.failures).await
            };
            let worker = start_worker(&node, cfg, run)?;
            println!("{id} (worker) on {}", node.cluster.local_addr());
            tokio::select! {
                signal = tokio::signal::ctrl_c() => signal?,
                exit = worker.join() => return Err(format!("the worker stopped: {:?}", exit.reason).into()),
            }
        }
        _ => {
            let broker = task_queue::find_broker(&node.cluster, Duration::from_secs(30)).await?;
            let answer = match role {
                "produce" => {
                    let n: u64 = args.get(1).map_or(Ok(1000), |s| s.parse())?;
                    let base = axon::trace::unix_micros() / 1000 * 10_000;
                    // With a sleep given, every job just sleeps that long: for measuring the queue, not the jobs.
                    let plain = match args.get(2) {
                        Some(ms) => Some(serde_json::to_string(&Work {
                            sleep_ms: ms.parse()?,
                            spin_us: 0,
                            fail_first: 0,
                            panic: false,
                        })?),
                        None => None,
                    };
                    let jobs = (0..n).map(|i| demo::job(i, 3).0).map(|j| match &plain {
                        Some(payload) => {
                            task_queue::NewJob { id: base + j.id, delay_ms: 0, payload: payload.clone(), ..j }
                        }
                        None => task_queue::NewJob { id: base + j.id, ..j },
                    });
                    let started = Instant::now();
                    let (_, held) = demo::produce(broker, jobs.collect()).await?;
                    let secs = started.elapsed().as_secs_f64();
                    println!("enqueued {n} in {secs:.2}s, held back {held} times by a full queue");
                    None
                }
                "status" => Some(broker.request(Call::Status).await?),
                "dead" => Some(broker.request(Call::Dead { limit: 1000 }).await?),
                _ => match broker.request(Call::Dead { limit: 1000 }).await? {
                    Answer::Views(dead) => {
                        Some(broker.request(Call::Retry(dead.iter().map(|v| v.id).collect())).await?)
                    }
                    other => return Err(unexpected(other).into()),
                },
            };
            if let Some(answer) = answer {
                println!("{}", serde_json::to_string_pretty(&answer)?);
            }
        }
    }
    node.system.shutdown().await?;
    Ok(())
}
