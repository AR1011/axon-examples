use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axon::ActorSystem;
use axon_cluster::{AccessPolicy, SeedServer, SeedServerConfig};
use axon_obs::{Dashboard, DashboardConfig, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig};
use saga::core::{Fault, Phase};
use saga::crash::{Plan, client, ended, run, submit};
use saga::store::Store;
use saga::{app, catalog, check, orders, start_node, transport};
use tokio::task::JoinSet;

const USAGE: &str = "usage:
  saga seed                                    a seed server on a free port; prints `seed <addr>`
  saga node --id n1 --seeds <addr>[,<addr>] --acceptors n1,n2,n3 --data <dir> [--fail-shipping-every 10]
            [--console 127.0.0.1:0]            also runs the metrics collector and the console
  saga load --seeds <addr> [--sagas 1500] [--seed 2]   submits orders as a client and waits for their ends
  saga init --data <dir>                       creates the databases with the catalog
  saga check --data <dir> [--fail-shipping-every 10]
  saga crash [--sagas 1500] [--kill-after 1000] [--data <dir>] [--fail-shipping-every 10] [--seed 1]";

type Error = Box<dyn std::error::Error>;

fn flags(args: &[String]) -> Result<BTreeMap<String, String>, Error> {
    let mut out = BTreeMap::new();
    for pair in args.chunks(2) {
        match pair {
            [flag, value] if flag.starts_with("--") => drop(out.insert(flag[2..].to_owned(), value.clone())),
            _ => return Err(format!("expected --flag value pairs\n{USAGE}").into()),
        }
    }
    Ok(out)
}

fn list(value: &str) -> Vec<String> {
    value.split(',').map(str::to_owned).collect()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (command, rest) = args.split_first().ok_or(USAGE)?;
    let flags = flags(rest)?;
    let get = |name: &str| flags.get(name).cloned().ok_or_else(|| format!("--{name} is required\n{USAGE}"));
    let number = |name: &str, default: u64| flags.get(name).map_or(Ok(default), |v| v.parse::<u64>());
    let fault = Fault { shipping_every: number("fail-shipping-every", 10)? };
    match command.as_str() {
        "seed" => {
            let server = SeedServer::bind(SeedServerConfig::new("127.0.0.1:0".parse()?, &transport("seed-1"))).await?;
            println!("seed {}", server.local_addr());
            tokio::signal::ctrl_c().await?;
        }
        "node" => {
            let id = get("id")?;
            let store = Store::open(get("data")?);
            let system = ActorSystem::builder().timing_sample_every(64).tracing(4096, 1000).build()?;
            let cluster =
                start_node(&system, &id, &list(&get("seeds")?), &list(&get("acceptors")?), store.dir()).await?;
            let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
            Telemetry::start(&system, &cluster, telemetry).await?;
            let mut console = String::new();
            if let Some(bind) = flags.get("console") {
                let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
                MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
                let cfg = DashboardConfig { bind: bind.parse()?, ..Default::default() };
                console = format!(" console http://{}/", Dashboard::start(&system, &cluster, cfg).await?.address());
            }
            app::serve(&cluster, store, fault).await?;
            println!("ready {id} {}{console}", cluster.local_addr());
            tokio::signal::ctrl_c().await?;
            drop(cluster);
            system.shutdown().await?;
        }
        "load" => {
            let orders = orders(number("seed", 2)?, number("sagas", 1500)? as usize);
            let (system, cluster) = client(&format!("load-{}", std::process::id()), &get("seeds")?, 1).await?;
            let started = Instant::now();
            let mut flight = JoinSet::new();
            for order in &orders {
                if flight.len() == 32 {
                    flight.join_next().await.expect("a submit in flight")??;
                }
                let (cluster, order) = (cluster.clone(), order.clone());
                flight.spawn(async move { submit(&cluster, &order).await });
            }
            while let Some(done) = flight.join_next().await {
                done??;
            }
            let submitted = started.elapsed();
            let mut ends = BTreeMap::<&str, usize>::new();
            for order in &orders {
                *ends
                    .entry(if ended(&cluster, order).await? == Phase::Done { "done" } else { "compensated" })
                    .or_default() += 1;
            }
            println!("{} submitted in {submitted:?}, all ended after {:?}: {ends:?}", orders.len(), started.elapsed());
            drop(cluster);
            system.shutdown().await?;
        }
        "init" => Store::open(get("data")?).create(&catalog())?,
        "check" => {
            let report = check(&Store::open(get("data")?), fault)?;
            println!("{report:#?}");
            if !report.violations.is_empty() {
                return Err("inconsistent".into());
            }
        }
        "crash" => {
            let data = match flags.get("data") {
                Some(dir) => PathBuf::from(dir),
                None => std::env::temp_dir().join(format!("saga-crash-{}", std::process::id())),
            };
            let plan = Plan {
                exe: std::env::current_exe()?,
                data,
                sagas: number("sagas", 1500)? as usize,
                kill_after: number("kill-after", 1000)? as usize,
                fault,
                seed: number("seed", 1)?,
            };
            let outcome = run(plan).await?;
            println!("{outcome:#?}");
            if !outcome.report.violations.is_empty() || outcome.resumed != outcome.orphaned {
                return Err("inconsistent".into());
            }
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}
