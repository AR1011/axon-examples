use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crawler::crawler::{NodeConfig, start_node};
use crawler::harness::{CrawlConfig, crawl};
use crawler::site::SiteConfig;
use tokio::io::AsyncReadExt;

type Error = Box<dyn std::error::Error>;

const USAGE: &str = "usage:
  crawler demo [--nodes 3] [--domains 8] [--pages 100] [--rate 20] [--kill-at 0.4] [--data DIR]
              [--console 127.0.0.1:8080 [--hold 60]]
  crawler node --id ID --seeds ADDR --site ADDR --data DIR [--rate 20] [--per-domain 2] [--node-fetches 32]";

fn flags(args: &[String]) -> Result<HashMap<&str, &str>, Error> {
    let mut out = HashMap::new();
    for pair in args.chunks(2) {
        match pair {
            [k, v] if k.starts_with("--") => drop(out.insert(&k[2..], v.as_str())),
            _ => return Err(format!("bad arguments\n{USAGE}").into()),
        }
    }
    Ok(out)
}

fn get<T: std::str::FromStr>(flags: &HashMap<&str, &str>, name: &str, default: Option<T>) -> Result<T, Error> {
    match flags.get(name) {
        Some(v) => v.parse().map_err(|_| format!("--{name}: cannot parse {v}").into()),
        None => default.ok_or_else(|| format!("--{name} is required\n{USAGE}").into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else { return Err(USAGE.into()) };
    let f = flags(rest)?;
    match command.as_str() {
        "node" => {
            let node = start_node(NodeConfig {
                id: get(&f, "id", None)?,
                seeds: get::<String>(&f, "seeds", None)?.split(',').map(str::to_owned).collect(),
                site: get(&f, "site", None)?,
                data: get(&f, "data", None)?,
                rate: get(&f, "rate", Some(20.0))?,
                per_domain: get(&f, "per-domain", Some(2))?,
                node_fetches: get(&f, "node-fetches", Some(32))?,
                crawl: true,
                console: None,
            })
            .await?;
            // Runs until stdin closes, then leaves the cluster cleanly.
            let mut sink = [0u8; 64];
            let mut stdin = tokio::io::stdin();
            while stdin.read(&mut sink).await.is_ok_and(|n| n > 0) {}
            node.system.shutdown().await?;
        }
        "demo" => {
            let data = match f.get("data") {
                Some(d) => PathBuf::from(d),
                None => std::env::temp_dir().join(format!("crawler-demo-{}", std::process::id())),
            };
            let cfg = CrawlConfig {
                site: SiteConfig {
                    domains: get(&f, "domains", Some(8))?,
                    pages: get(&f, "pages", Some(100))?,
                    ..SiteConfig::default()
                },
                nodes: get(&f, "nodes", Some(3))?,
                rate: get(&f, "rate", Some(20.0))?,
                per_domain: 2,
                node_fetches: 32,
                kill_at: f.get("kill-at").map(|v| v.parse()).transpose()?,
                data,
                exe: std::env::current_exe()?,
                deadline: Duration::from_secs(120),
                console: match f.get("console") {
                    Some(bind) => Some((bind.parse()?, Duration::from_secs(get(&f, "hold", Some(60))?))),
                    None => None,
                },
            };
            let report = crawl(&cfg).await?;
            println!("{report:#?}");
            let verdict = report.check(&cfg);
            println!("{}", report.summary());
            verdict?;
            println!("check passed");
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}
