//! `kv node|stub|check|warm|status|console|hash-password [--flag value]..`: a node, the front end alone, the history
//! checker, two looks at a running cluster, its web console, and the hash for axon's console. See the README.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axon::files::OsFiles;
use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use axon_net::SharedSecret;
use axon_obs::{User, hash_password};
use kv::check::{self, CheckConfig};
use kv::client::Client;
use kv::node::{self, Error, Mode, Reads, Settings};
use kv::resp::Value;
use kv::slot::{SHARDS, key_slot, shard_of_slot};
use serde_json::json;

const USAGE: &str = "usage: kv node|stub|check|warm|status|console|hash-password [--flag value].. (see the README)";

struct Args(BTreeMap<String, String>);

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Error> {
        let mut map = BTreeMap::new();
        let mut args = args.peekable();
        while let Some(flag) = args.next() {
            let name = flag.strip_prefix("--").ok_or_else(|| format!("{flag:?} is not a --flag; {USAGE}"))?;
            // A flag with no value is a switch.
            let value = match args.peek() {
                Some(next) if !next.starts_with("--") => args.next().unwrap_or_default(),
                _ => "true".into(),
            };
            map.insert(name.to_owned(), value);
        }
        Ok(Self(map))
    }
    fn or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.0.get(key).map_or(default, String::as_str)
    }
    fn list(&self, key: &str, default: &str) -> Vec<String> {
        self.or(key, default).split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
    }
    fn addrs(&self, default: &str) -> Result<Vec<SocketAddr>, Error> {
        self.list("addrs", default).iter().map(|a| a.parse().map_err(|e| format!("--addrs {a}: {e}").into())).collect()
    }
    fn duration(&self, key: &str, default: &str) -> Result<Duration, Error> {
        duration(self.or(key, default)).ok_or_else(|| format!("--{key}: a duration such as 500ms, 2s or 1m").into())
    }
    fn number(&self, key: &str, default: usize) -> Result<usize, Error> {
        self.0.get(key).map_or(Ok(default), |n| n.parse().map_err(|e| format!("--{key}: {e}").into()))
    }
}

fn duration(text: &str) -> Option<Duration> {
    let (n, unit) = text.split_at(text.find(|c: char| !c.is_ascii_digit())?);
    let n: u64 = n.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(60 * n)),
        _ => None,
    }
}

fn transport(id: &str) -> Result<axon_net::Config, Error> {
    let secret = std::env::var("KV_SECRET").map_err(|_| "KV_SECRET is not set: run.sh sets it")?;
    let mut transport = axon_net::Config::new(id, SharedSecret::new(secret)?);
    transport.allow_plaintext = true; // only with --plaintext, which run_node checks
    Ok(transport)
}

async fn terminated() {
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

async fn run_node(args: Args) -> Result<(), Error> {
    let id = args.or("id", "n1").to_owned();
    let settings = Settings {
        id: id.clone(),
        resp: Some(args.or("resp", "127.0.0.1:6379").parse()?),
        bind: args.or("bind", "127.0.0.1:7379").parse()?,
        nodes: args.list("nodes", &id),
        mode: match args.or("mode", "memory") {
            "memory" => Mode::Memory,
            "durable" => Mode::Durable,
            other => return Err(format!("--mode {other}: memory or durable").into()),
        },
        reads: match args.or("reads", "quorum") {
            "quorum" => Reads::Quorum,
            "lease" => Reads::Lease,
            other => return Err(format!("--reads {other}: quorum or lease").into()),
        },
        lease: args.duration("lease", "6s")?,
        start_timeout: match args.0.get("start-timeout") {
            Some(_) => args.duration("start-timeout", "")?,
            None => node::START_TIMEOUT,
        },
        keep: args.number("keep", 4096)? as u64,
        keep_bytes: args.number("keep-bytes", 64 << 20)? as u64,
        deadline: args.duration("deadline", "2s")?,
        workers: args.0.get("workers").map(|w| w.parse()).transpose()?,
        password: args.0.get("password").cloned(),
        health: args.0.get("health").map(|h| h.parse()).transpose()?,
        console: args.0.get("console").map(|c| c.parse()).transpose()?,
        console_users: match args.0.get("console-user") {
            Some(name) => vec![User {
                name: name.clone(),
                hash: std::env::var("KV_CONSOLE_HASH")
                    .map_err(|_| "--console-user needs KV_CONSOLE_HASH: see `kv hash-password`")?,
            }],
            None => Vec::new(),
        },
        limits: kv::Limits::default(),
        log: kv::log::LogSettings::default(),
    };
    // kv has no TLS between nodes: its cluster traffic goes unencrypted only where `--plaintext` says it may, on
    // loopback or a private network. The scripts pass it, and bind every port to 127.0.0.1.
    if args.0.get("plaintext").map(String::as_str) != Some("true") {
        return Err("kv has no TLS between nodes: add --plaintext to run on loopback or a private network only".into());
    }
    let seed_bind: SocketAddr = args.or("seed-bind", "127.0.0.1:8379").parse()?;
    let seed = SeedServer::bind(SeedServerConfig::new(seed_bind, &transport(&format!("{id}-seed"))?)).await?;
    let own_seed = format!("{id}-seed@{}", seed.local_addr());
    let seeds = args.list("seeds", &own_seed);
    let transport = transport(&id)?;
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    let dir = PathBuf::from(args.or("dir", &format!("./kv-data/{id}")));
    let store = node::Store {
        leases: Arc::new(OsFiles::new(dir.join("leases"))),
        wal: Arc::new(OsFiles::new(dir.join("wal"))),
        snaps: Arc::new(OsFiles::new(dir.join("snap"))),
        halt: Arc::new(|why| {
            eprintln!("kv: the Log ended ({why}): stopping, as a node without its Log must not keep its leases");
            std::process::exit(70);
        }),
    };
    let node = node::start(&settings, transport, discovery, store).await?;
    println!(
        "{}",
        json!({
            "node": id, "resp": node.resp.map(|r| r.to_string()), "cluster": node.cluster.local_addr().to_string(),
            "seed": seed.local_addr().to_string(), "mode": settings.mode.name(), "nodes": settings.nodes,
            "health": node.health.as_ref().map(|h| h.address().to_string()),
            "console": node.console.map(|c| format!("http://{c}/")), "pid": std::process::id(),
        })
    );
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    // Made once: a signal that came while a new one was being made would be lost.
    let stop = terminated();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = tick.tick() => {
                let odd = node.mismatched();
                if !odd.is_empty() {
                    eprintln!("kv: nodes {odd:?} have another kv.shards or kv.mode: they split the slots another way");
                }
            }
        }
    }
    node.shutdown().await
}

async fn run_stub(args: Args) -> Result<(), Error> {
    let (resp, _serving) = node::stub(args.or("resp", "127.0.0.1:6379").parse()?, kv::Limits::default()).await?;
    println!("{}", json!({ "stub": resp.to_string(), "pid": std::process::id() }));
    terminated().await;
    Ok(())
}

async fn run_check(args: Args) -> Result<(), Error> {
    let cfg = CheckConfig {
        addrs: args.addrs("127.0.0.1:6379")?,
        writers: args.number("writers", 64)?,
        readers: args.number("readers", 16)?,
        keys_per_writer: args.number("keys", 16)?,
        probe: args.duration("probe", "10ms")?,
        run: Duration::from_secs(args.number("secs", 40)? as u64),
        op_timeout: args.duration("timeout", "3s")?,
        out: args.0.get("out").map(PathBuf::from),
        expect_loss: args.0.contains_key("expect-loss"),
        password: args.0.get("password").cloned(),
        shards: args
            .list("shards", "")
            .iter()
            .map(|s| s.parse().map_err(|e| format!("--shards {s}: {e}")))
            .collect::<Result<_, _>>()?,
        prefix: args.or("prefix", "kvc").to_owned(),
        save: args.0.get("save").map(PathBuf::from),
    };
    let verdict = match args.0.get("verify") {
        Some(saved) => check::verify(cfg, saved.as_ref()).await?,
        None => check::run(cfg).await?,
    };
    println!("{}", verdict.summary);
    if !verdict.pass {
        std::process::exit(1);
    }
    Ok(())
}

/// Asks every shard once, through the given nodes, until each answers.
async fn run_warm(args: Args) -> Result<(), Error> {
    let took = kv::client::warm(&args.addrs("127.0.0.1:6379")?, args.duration("within", "60s")?).await?;
    println!("{}", json!({ "warm": SHARDS, "ms": took.as_millis() as u64 }));
    Ok(())
}

/// One node's `INFO <section>`, within 3 s.
async fn info(addr: SocketAddr, section: &str) -> Result<String, String> {
    let info = async {
        let mut client = Client::connect(addr).await?;
        client.call(&[b"INFO", section.as_bytes()]).await
    };
    match tokio::time::timeout(Duration::from_secs(3), info).await {
        Ok(Ok(Value::Bulk(Some(text)))) => Ok(String::from_utf8_lossy(&text).into_owned()),
        Ok(Ok(other)) => Err(format!("{other:?}")),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("no answer within 3 s".into()),
    }
}

/// `kv status --digest`: each shard's committed index and map hash on every node's Log, asked again until they
/// agree or `--within` passes. A Log learns the last commit from the next Append, up to a second after a write.
async fn run_digest(args: Args) -> Result<(), Error> {
    let addrs = args.addrs("127.0.0.1:6379")?;
    let until = tokio::time::Instant::now() + args.duration("within", "10s")?;
    loop {
        let mut logs = Vec::new();
        for addr in &addrs {
            let text = info(*addr, "kvdigest").await.map_err(|e| format!("{addr}: {e}"))?;
            logs.push(text.lines().map(|l| l.split_once(':').map_or("", |(_, d)| d).to_owned()).collect::<Vec<_>>());
        }
        let differ: Vec<usize> =
            (0..usize::from(SHARDS)).filter(|&s| logs.iter().any(|l| l.get(s) != logs[0].get(s))).collect();
        if differ.is_empty() || tokio::time::Instant::now() >= until {
            let shown: Vec<_> = differ
                .iter()
                .take(8)
                .map(|&s| json!({ "shard": s, "logs": logs.iter().map(|l| l.get(s)).collect::<Vec<_>>() }))
                .collect();
            println!(
                "{}",
                json!({ "digest": { "logs": addrs.len(), "shards": SHARDS, "differ": differ.len(), "first": shown } })
            );
            if !differ.is_empty() {
                std::process::exit(1);
            }
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// `kv status --owner KEY`: the node whose shard holds KEY, from `CLUSTER SLOTS` of the first node that answers.
async fn run_owner(args: Args, key: &str) -> Result<(), Error> {
    let slot = key_slot(key.as_bytes());
    for addr in args.addrs("127.0.0.1:6379")? {
        let slots = async { Client::connect(addr).await?.call(&[b"CLUSTER", b"SLOTS"]).await };
        let Ok(Ok(Value::Array(Some(ranges)))) = tokio::time::timeout(Duration::from_secs(3), slots).await else {
            continue;
        };
        for range in ranges {
            let Value::Array(Some(range)) = range else { continue };
            let [Value::Int(first), Value::Int(last), Value::Array(Some(node)), ..] = &range[..] else { continue };
            if (*first..=*last).contains(&i64::from(slot))
                && let Some(Value::Bulk(Some(id))) = node.get(2)
            {
                let owner = String::from_utf8_lossy(id);
                println!("{}", json!({ "key": key, "shard": shard_of_slot(slot), "owner": owner }));
                return Ok(());
            }
        }
    }
    Err(format!("no node names the owner of {key}").into())
}

/// `kv status --hosts`: where each shard runs, `n1@3` (node and lease epoch), from `INFO kvhosted` of every node; ""
/// where none runs it, and two joined by a space where two do. Every node must answer.
async fn run_hosts(args: Args) -> Result<(), Error> {
    let mut hosts = vec![String::new(); usize::from(SHARDS)];
    for addr in args.addrs("127.0.0.1:6379")? {
        for line in info(addr, "kvhosted").await.map_err(|e| format!("{addr}: {e}"))?.lines() {
            let Some((shard, host)) = line.strip_prefix('s').and_then(|l| l.split_once(':')) else { continue };
            let Some(here) = shard.parse::<usize>().ok().and_then(|s| hosts.get_mut(s)) else { continue };
            if !here.is_empty() {
                here.push(' ');
            }
            here.push_str(host);
        }
    }
    println!("{}", json!({ "hosts": hosts }));
    Ok(())
}

/// `INFO kv` of each node, as JSON.
async fn run_status(args: Args) -> Result<(), Error> {
    if args.0.contains_key("digest") {
        return run_digest(args).await;
    }
    if let Some(key) = args.0.get("owner").cloned() {
        return run_owner(args, &key).await;
    }
    if args.0.contains_key("hosts") {
        return run_hosts(args).await;
    }
    let mut nodes = Vec::new();
    for addr in args.addrs("127.0.0.1:6379")? {
        let fields = match info(addr, "kv").await {
            Ok(text) => text
                .lines()
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.to_owned(), v.parse::<u64>().map_or(json!(v), |n| json!(n))))
                .collect::<serde_json::Map<_, _>>()
                .into(),
            Err(e) => json!({ "error": e }),
        };
        nodes.push(json!({ "addr": addr.to_string(), "info": fields }));
    }
    println!("{}", json!({ "nodes": nodes }));
    Ok(())
}

/// `kv console`: the web page of the nodes at `--addrs` and of the runs in `--results`, until SIGTERM.
async fn run_console(args: Args) -> Result<(), Error> {
    let bind: SocketAddr = args.or("bind", "127.0.0.1:7380").parse()?;
    // The page has no sign-in: anyone who reaches it sees the cluster's counters and the runs.
    if !bind.ip().is_loopback() && !args.0.contains_key("public") {
        return Err(format!("--bind {bind} is not loopback, and the console has no sign-in: add --public to bind it anyway, or reach it with ssh -L").into());
    }
    let console = kv::console::start(kv::console::ConsoleConfig {
        addrs: args.addrs("127.0.0.1:6379")?,
        bind,
        results: PathBuf::from(args.or("results", concat!(env!("CARGO_MANIFEST_DIR"), "/results"))),
        password: args.0.get("password").cloned(),
        every: args.duration("every", "1s")?,
    })
    .await?;
    println!("{}", json!({ "console": format!("http://{}/", console.addr), "pid": std::process::id() }));
    terminated().await;
    console.stop();
    Ok(())
}

/// `kv hash-password`: the argon2id hash of `KV_CONSOLE_PASSWORD`, or of a line of stdin, for `KV_CONSOLE_HASH`.
fn run_hash_password() -> Result<(), Error> {
    let password = match std::env::var("KV_CONSOLE_PASSWORD") {
        Ok(password) => password,
        Err(_) => {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            line.trim_end_matches(['\r', '\n']).to_owned()
        }
    };
    if password.is_empty() {
        return Err("no password: set KV_CONSOLE_PASSWORD or give one line on stdin".into());
    }
    println!("{}", hash_password(&password)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut argv = std::env::args().skip(1);
    let command = argv.next().unwrap_or_default();
    let args = Args::parse(argv)?;
    match command.as_str() {
        "node" => run_node(args).await,
        "stub" => run_stub(args).await,
        "check" => run_check(args).await,
        "warm" => run_warm(args).await,
        "status" => run_status(args).await,
        "console" => run_console(args).await,
        "hash-password" => run_hash_password(),
        _ => Err(USAGE.into()),
    }
}
