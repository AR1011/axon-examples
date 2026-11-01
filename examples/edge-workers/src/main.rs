//! `edge-workers seed|node|status|stats --plaintext key=value..`: the processes of a local cluster, and two looks at
//! it from a member with no role. See the README. The operator's command line is the other binary, `edge`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::OsFiles;
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery, SeedServer,
    SeedServerConfig,
};
use axon_net::SharedSecret;
use edge_workers::admin::{AdminConfig, Totals, canary_json};
use edge_workers::engine::starling::{Starling, StarlingConfig};
use edge_workers::follower::FollowerConfig;
use edge_workers::gateway;
use edge_workers::node::{self, Client, Roles, Settings, Stores};
use serde_json::json;

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Args(BTreeMap<String, String>);

impl Args {
    fn get(&self, key: &str) -> Result<&str, Error> {
        self.0.get(key).map(String::as_str).ok_or_else(|| format!("missing {key}=").into())
    }
    fn or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.0.get(key).map_or(default, String::as_str)
    }
    fn list(&self, key: &str) -> Vec<String> {
        self.or(key, "").split(',').filter(|s| !s.is_empty()).map(String::from).collect()
    }
}

fn transport(id: &str) -> Result<axon_net::Config, Error> {
    let secret = std::env::var("EDGE_SECRET").unwrap_or_else(|_| "edge-workers-dev-secret".into());
    let mut transport = axon_net::Config::new(id, SharedSecret::new(secret)?);
    transport.allow_plaintext = true; // only with --plaintext, which main checks
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

/// A cluster member with the roles' labels.
async fn member(args: &Args, roles: Roles) -> Result<(ActorSystem, Cluster), Error> {
    let id = args.get("id")?;
    let transport = transport(id)?;
    let seeds = args.list("seeds");
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    let mut cfg = ClusterConfig::new(args.or("bind", "127.0.0.1:0").parse()?, transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    cfg.labels = roles.labels();
    // Objects are keyed actors under leases: the same acceptors on every node, and on each of them a directory of
    // its own for the leases it grants.
    let acceptors = args.list("acceptors");
    if !acceptors.is_empty() {
        let mut paxos = PaxosConfig::new(acceptors.clone());
        if acceptors.iter().any(|acceptor| acceptor == id) {
            paxos.store = Some(Arc::new(OsFiles::new(PathBuf::from(args.get("dir")?).join("leases"))));
        }
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
    }
    let system = ActorSystem::builder().build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    Ok((system, cluster))
}

async fn seed(args: Args) -> Result<(), Error> {
    let bind = args.or("bind", "127.0.0.1:0").parse()?;
    let server = SeedServer::bind(SeedServerConfig::new(bind, &transport(args.or("id", "seed-1"))?)).await?;
    println!("{}", json!({ "seed": server.local_addr().to_string(), "pid": std::process::id() }));
    terminated().await;
    Ok(())
}

/// The threads the workers run on. A tokio runtime may not be dropped where async code runs, and `run` is that.
struct Js(Option<tokio::runtime::Runtime>);

impl Js {
    fn start(threads: usize) -> Result<Self, Error> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        Ok(Self(Some(runtime.worker_threads(threads).thread_name("js").enable_all().build()?)))
    }
}

impl Drop for Js {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// The control node's admin API: its password hash from `EDGE_ADMIN_HASH` (`edge hash-password` makes one), and a
/// signing key kept in the node's directory, so a restart ends no session.
fn admin(dir: &std::path::Path) -> Result<AdminConfig, Error> {
    let hash =
        std::env::var("EDGE_ADMIN_HASH").map_err(|_| "admin= needs EDGE_ADMIN_HASH: see `edge hash-password`")?;
    let file = dir.join("admin.key");
    let key = match std::fs::read(&file) {
        Ok(key) => key,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            use std::os::unix::fs::OpenOptionsExt;
            let key = edge_workers::auth::random_key()?;
            std::fs::create_dir_all(dir)?;
            let mut options = std::fs::OpenOptions::new();
            std::io::Write::write_all(&mut options.write(true).create_new(true).mode(0o600).open(&file)?, &key)?;
            key
        }
        Err(e) => return Err(e.into()),
    };
    Ok(AdminConfig { key: Some(key), ..AdminConfig::new(hash) })
}

/// `node id= roles= dir= seeds= [acceptors=id,..] [engine=] [http=addr] [public=url] [domain=] [js=threads]
/// [idle=seconds] [object_idle=seconds] [admin=addr] [base=/path/]`: `http` is where a gateway listens, `public` the URL
/// clients reach it at (`http://` and the `http` address unless given), which the API shows as each worker's URL,
/// `domain` makes `<name>.<domain>` a worker's host, `js` and `idle` are a runner's threads for the workers and how long one stays loaded with no
/// request, `admin` is where the control node serves the operator's API and the console, `base` the path a proxy in front
/// puts the console at. `acceptors` are the nodes that grant the
/// objects' leases, the same list on every node; without it no object runs. `object_idle` is how long an object
/// stays on its runner with no request.
async fn run(args: Args) -> Result<(), Error> {
    let roles = Roles::parse(args.get("roles")?)?;
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    // Outlives the system, whose actors drain what runs on it.
    let js = Js::start(args.or("js", &cores.to_string()).parse()?)?;
    let (system, cluster) = member(&args, roles).await?;
    // Whatever stops the node, the cluster hears that it left.
    let ran = serve(&args, roles, &js, &system, &cluster).await;
    system.shutdown().await?;
    ran
}

async fn serve(args: &Args, roles: Roles, js: &Js, system: &ActorSystem, cluster: &Cluster) -> Result<(), Error> {
    let dir = PathBuf::from(args.get("dir")?);
    let mut settings = Settings::default();
    settings.invoker.runtime = js.0.as_ref().map(|runtime| runtime.handle().clone());
    settings.invoker.idle = Duration::from_secs(args.or("idle", "300").parse()?);
    settings.objects.idle = Duration::from_secs(args.or("object_idle", "60").parse()?);
    settings.gateway.domain = args.0.get("domain").cloned();
    if roles.control && args.0.contains_key("admin") {
        let mut admin = admin(&dir)?;
        if let Some(base) = args.0.get("base") {
            admin.base_path = base.clone();
        }
        settings.admin = Some(admin);
    }
    let mut engine = StarlingConfig::new(args.or("engine", "examples/edge-workers/engine"));
    engine.cache_dir = Some(dir.join("compiled"));
    // Starting the engine compiles its pooling setup: once, before the node takes any work.
    let engine = tokio::task::spawn_blocking(move || Starling::new(engine)).await??;
    let stores = Stores {
        streams: Arc::new(OsFiles::new(dir.join("streams"))),
        registry: Arc::new(OsFiles::new(dir.join("registry"))),
        objects: Arc::new(OsFiles::new(dir.join("objects"))),
        cache: FollowerConfig::new(Arc::new(OsFiles::new(dir.join("cache")))),
    };
    let node = node::start(system, cluster, roles, stores, engine, settings).await?;
    let id = cluster.identity().node_id.clone();
    let (mut http, mut url) = (None, None);
    if let (Some(gateway), Some(bind)) = (&node.gateway, args.0.get("http")) {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        let listens = listener.local_addr()?;
        http = Some(listens.to_string());
        // The cluster hears where this gateway is reached, so the control node's API can say each worker's URL.
        let public = gateway::public_url(args.0.get("public").map(String::as_str), listens)?;
        gateway::advertise(cluster, &public).await?;
        url = Some(public);
        let router = gateway.router();
        tokio::spawn(async move { axum::serve(listener, router).await });
    }
    let mut api = None;
    if let (Some(admin), Some(bind)) = (&node.admin, args.0.get("admin")) {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        api = Some(listener.local_addr()?.to_string());
        let router = admin.router();
        tokio::spawn(async move { axum::serve(listener, router).await });
    }
    println!(
        "{}",
        json!({ "node": id, "roles": args.get("roles")?, "pid": std::process::id(), "addr": cluster.local_addr().to_string(), "http": http, "url": url, "admin": api })
    );
    let mut every = tokio::time::interval(Duration::from_secs(5));
    let mut last = None;
    loop {
        tokio::select! {
            _ = terminated() => return Ok(()),
            // The node is its actors: without one it serves stale or nothing, so it stops and is started again.
            (actor, exit) = node.ended() => return Err(format!("{id}: the {actor} ended: {:?}", exit.reason).into()),
            _ = every.tick() => {
                let Some(serving) = &node.serving else { continue };
                let table: BTreeMap<_, _> = serving.all().iter().map(|(name, current)| {
                    let served = current.served();
                    (name.clone(), json!({ "seq": served.seq, "stable": served.stable, "canary": canary_json(served.canary) }))
                }).collect();
                let line = json!({ "node": id, "serving": table }).to_string();
                if last.as_ref() != Some(&line) {
                    println!("{line}");
                    last = Some(line);
                }
            }
        }
    }
}

async fn status(args: &Args, registry: &Client) -> Result<serde_json::Value, Error> {
    let workers = match args.0.get("name") {
        Some(name) => vec![registry.get(name).await?.0],
        None => registry.list().await?,
    };
    let workers: Vec<_> = workers
        .iter()
        .map(|f| {
            let d = f.deployment.as_ref();
            json!({ "name": f.name, "latest": f.latest, "seq": d.map(|d| d.seq), "stable": d.map(|d| d.stable), "canary": canary_json(d.and_then(|d| d.canary)) })
        })
        .collect();
    Ok(json!({ "workers": workers }))
}

/// `stats`: every runner's counters per worker, with the CPU and wall time of its runs.
async fn stats(cluster: &Cluster) -> Result<serde_json::Value, Error> {
    let runners = |v: &axon_cluster::View| v.members.iter().any(|m| m.record.label("runner").is_some());
    tokio::time::timeout(Duration::from_secs(10), cluster.wait_view(runners)).await??;
    let (answered, silent) = node::stats(cluster).await;
    let runners: Vec<_> = answered
        .iter()
        .map(|runner| {
            let workers: BTreeMap<_, _> = runner
                .workers
                .iter()
                .map(|(name, stats)| (name, Totals::of([stats]).json()))
                .collect();
            json!({ "node": runner.node, "workers": workers, "logs_published": runner.logs_published, "logs_dropped": runner.logs_dropped })
        })
        .collect();
    Ok(json!({ "runners": runners, "silent": silent }))
}

/// A command joins as a member with no role, asks, and leaves.
async fn command(name: &str, args: Args) -> Result<(), Error> {
    let (system, cluster) = member(&args, Roles::default()).await?;
    let asked = async {
        let has_control = |v: &axon_cluster::View| node::having(v, "control").next().is_some();
        tokio::time::timeout(Duration::from_secs(30), cluster.wait_view(has_control)).await??;
        match name {
            "stats" => stats(&cluster).await,
            _ => status(&args, &Client::new(&cluster)).await,
        }
    };
    let out = asked.await;
    system.shutdown().await?;
    println!("{}", out?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // No TLS between nodes: a node talks unencrypted only where `--plaintext` says it may.
    let plaintext = std::env::args().any(|a| a == "--plaintext");
    let mut argv = std::env::args().skip(1);
    let name = argv.next().unwrap_or_default();
    let args = Args(argv.filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect());
    match name.as_str() {
        "seed" | "node" | "status" | "stats" if !plaintext => {
            Err("edge-workers has no TLS between nodes: add --plaintext to run on loopback or a private network only"
                .into())
        }
        "seed" => seed(args).await,
        "node" => run(args).await,
        "status" | "stats" => command(&name, args).await,
        _ => Err("usage: edge-workers seed|node|status|stats --plaintext key=value..".into()),
    }
}
