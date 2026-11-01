//! `workflows seed|node|dev|token key=value..`: the processes of a cluster, a one-node cluster to try it, and a new
//! API token. Each process prints one JSON line once it is up, and on SIGTERM or Ctrl-C drains its workers and stops.
//! See the README; `scripts/run.sh` starts the seven processes of the local cluster. There is no TLS between nodes, so
//! a seed or a node starts only with `--plaintext`.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::OsFiles;
use axon::{ActorSystem, JobExecutor};
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, MemoryDiscovery, PaxosConfig, PaxosLeases, Permits, QuotaConfig, Quotas,
    Rate, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig,
};
use axon_net::SharedSecret;
use serde_json::json;
use workflows::api::{Api, ApiConfig, parse_duration};
use workflows::auth::{hash_password, new_token, random_key};
use workflows::check::Witness;
use workflows::journal::service::JournalConfig;
use workflows::node::{self, Node, NodeConfig};
use workflows::samples;
use workflows::worker::{WorkerConfig, queue_label};
use workflows::workflow::App;

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
    fn yes(&self, key: &str) -> bool {
        self.or(key, "no") == "yes"
    }
    fn duration(&self, key: &str, default: &str) -> Result<Duration, Error> {
        let text = self.or(key, default);
        parse_duration(text).ok_or_else(|| format!("{key}={text}: a duration such as 30s, 5m, 2h or 7d").into())
    }
}

async fn terminated() {
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

fn transport(id: &str) -> Result<axon_net::Config, Error> {
    let secret = std::env::var("WF_SECRET").unwrap_or_else(|_| "workflows-dev-secret".into());
    let mut transport = axon_net::Config::new(id, SharedSecret::new(secret)?);
    transport.allow_plaintext = true; // dev on loopback, or a seed or node with --plaintext
    Ok(transport)
}

/// Attempts at once per queue, unless `slots=`.
const SLOTS: &str = "16";
/// Attempts of one queue at once across its workers, keyed by the queue: `queue_tasks=` its maximum.
const QUEUE_TASKS: Permits = Permits::new("wf_queue_tasks", 1);

const DEV: &str = "dev";

/// A runtime of its own for the activities, so a slow one does not hold up the node's. A tokio runtime may not be
/// dropped where async code runs: this one shuts down in the background.
struct Activities(Option<tokio::runtime::Runtime>);

impl Activities {
    fn start() -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
        Ok(Self(Some(runtime)))
    }

    fn executor(&self) -> Option<JobExecutor> {
        self.0.as_ref().map(|runtime| JobExecutor::tokio(runtime.handle().clone()))
    }
}

impl Drop for Activities {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// The samples, and with `witness=FILE` a line in it for each attempt as it starts.
fn app(args: &Args) -> Result<App, Error> {
    let app = samples::app();
    let Some(file) = args.0.get("witness") else { return Ok(app) };
    let file = Mutex::new(std::fs::OpenOptions::new().create(true).append(true).open(file)?);
    Ok(app.witness(move |cx| {
        let witness = Witness {
            run: cx.run_id().to_owned(),
            step: cx.step(),
            attempt: cx.attempt(),
            node: cx.node().to_owned(),
            epoch: cx.epoch(),
            t: axon::trace::unix_micros(),
        };
        // One write a line, in append mode: whole in the file before the attempt runs, whatever kills the process.
        let line = serde_json::to_string(&witness).unwrap_or_default() + "\n";
        if let Ok(mut file) = file.lock()
            && let Err(e) = file.write_all(line.as_bytes())
        {
            eprintln!("witness: {e}");
        }
    }))
}

/// `dev dir=.. http=..`: one node that is history, engine, worker and api at once, with one replica (R=1), and a
/// password for this start.
async fn dev(args: Args) -> Result<(), Error> {
    let dir = PathBuf::from(args.or("dir", "target/wf"));
    // A cluster of one: its port stays on loopback, so it runs without --plaintext.
    let mut cfg = ClusterConfig::new(([127, 0, 0, 1], 0).into(), transport(DEV)?, MemoryDiscovery::new());
    cfg.labels = ["history", "engine", "api", "worker"].map(|role| (role.to_owned(), "yes".to_owned())).into();
    for queue in samples::app().queues() {
        cfg.labels.insert(queue_label(queue), "yes".to_owned());
    }
    let mut paxos = PaxosConfig::new([DEV]);
    paxos.store = Some(Arc::new(OsFiles::new(dir.join("leases"))));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
    let activities = Activities::start()?;
    let system = ActorSystem::builder().build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    // Whatever stops the node, its actors stop first.
    let served = serve_dev(&args, &dir, &activities, &system, &cluster).await;
    system.shutdown().await?;
    served
}

async fn serve_dev(
    args: &Args,
    dir: &Path,
    activities: &Activities,
    system: &ActorSystem,
    cluster: &Cluster,
) -> Result<(), Error> {
    let app = app(args)?;
    let queues: Vec<_> = app.queues().into_iter().map(str::to_owned).collect();
    let mut cfg = NodeConfig::new(app, Arc::new(axon::trace::unix_micros), JournalConfig::new(vec![DEV.into()]));
    cfg.history = Some((Arc::new(OsFiles::new(dir.join("journal"))), Arc::new(OsFiles::new(dir.join("journal-snap")))));
    cfg.engine = true;
    for queue in queues {
        let mut worker = WorkerConfig::new(queue, SLOTS.parse()?);
        worker.executor = activities.executor();
        cfg.workers.push(worker);
    }
    let node = node::start(system, cluster, cfg).await?;
    // A lone acceptor grants nothing for a little over a lease after it starts.
    while cluster.warming() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let password = format!("{:016x}", axon::trace::random_u64());
    let hash = {
        let password = password.clone();
        tokio::task::spawn_blocking(move || hash_password(&password)).await??
    };
    let mut cfg = ApiConfig::new(hash);
    cfg.base_path = args.or("base", "/").into();
    let url = listen(&Api::new(system, &node, cluster, cfg)?, args).await?;
    println!("{}", json!({ "node": DEV, "url": url, "password": password, "pid": std::process::id() }));
    terminated().await;
    node.drain_workers(args.duration("drain", "60s")?).await;
    Ok(())
}

/// `seed bind=addr`: a seed server alone. A history node can serve one itself, with `seed=addr`.
async fn seed(args: Args) -> Result<(), Error> {
    let bind = args.or("bind", "127.0.0.1:0").parse()?;
    let server = SeedServer::bind(SeedServerConfig::new(bind, &transport(args.or("id", "seed"))?)).await?;
    println!("{}", json!({ "seed": server.local_addr().to_string(), "pid": std::process::id() }));
    terminated().await;
    Ok(())
}

/// The api's session key, kept in `file` (made the first time), the same file for every api node.
fn session_key(file: &Path) -> Result<Vec<u8>, Error> {
    match std::fs::read(file) {
        Ok(key) => Ok(key),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            use std::os::unix::fs::OpenOptionsExt;
            let key = random_key()?;
            if let Some(dir) = file.parent() {
                std::fs::create_dir_all(dir)?;
            }
            // Written whole under another name and linked in: api nodes that start together take one key.
            let mut written = file.as_os_str().to_owned();
            written.push(format!(".{}.{:x}", std::process::id(), axon::trace::random_u64()));
            let written = PathBuf::from(written);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true).mode(0o600).open(&written)?.write_all(&key)?;
            let linked = std::fs::hard_link(&written, file);
            std::fs::remove_file(&written)?;
            match linked {
                Ok(()) => Ok(key),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(std::fs::read(file)?),
                Err(e) => Err(e.into()),
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// `node id= dir= seeds=a,b history_nodes=h1,h2,h3 [acceptors=..] [history=yes] [engine=yes] [api=yes] [worker=yes]
/// [queues=a,b] [slots=16] [queue_tasks=0] [seed=addr] [http=addr] [retention=7d] [failed_retention=30d] [drain=60s]
/// [tokens=FILE] [key=FILE] [start_rate=2000] [token_rate=0] [witness=FILE]`: `seeds` are the seed servers'
/// addresses, `seed` where this node serves one, `acceptors` the nodes that grant the runs' leases (the history nodes
/// unless given, the same list on every node), `queues` a worker's queues (the samples' unless given), `queue_tasks`
/// the attempts of one queue at once across the workers (the same on every worker node; 0: no limit), `http` where
/// an api node listens (its password hash in `WF_ADMIN_HASH`), `key` the sessions' signing key, the same file for
/// every api node (`dir/api.key` unless given), `start_rate` the starts of one workflow a second across the cluster,
/// `token_rate` those of one token (`admin` for a password session; 0: no limit).
async fn run_node(args: Args) -> Result<(), Error> {
    let id = args.get("id")?.to_owned();
    let dir = PathBuf::from(args.get("dir")?);
    let history_nodes = args.list("history_nodes");
    if history_nodes.is_empty() {
        return Err("history_nodes=h1,h2,h3: the same list, in the same order, on every node".into());
    }
    let acceptors = match args.list("acceptors") {
        list if list.is_empty() => history_nodes.clone(),
        list => list,
    };
    let transport = transport(&id)?;
    let _seed = match args.0.get("seed") {
        Some(addr) => Some(SeedServer::bind(SeedServerConfig::new(addr.parse()?, &transport)).await?),
        None => None,
    };
    let app = app(&args)?;
    let queues = match args.list("queues") {
        list if list.is_empty() => app.queues().into_iter().map(str::to_owned).collect(),
        list => list,
    };
    let discovery = SeedDiscovery::start(SeedConfig::new(args.list("seeds"), &transport)).await?;
    let mut cfg = ClusterConfig::new(args.or("bind", "127.0.0.1:0").parse()?, transport, discovery);
    // With seeds, as axon's guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    for role in ["history", "engine", "api", "worker"].into_iter().filter(|role| args.yes(role)) {
        cfg.labels.insert(role.to_owned(), "yes".to_owned());
    }
    if args.yes("worker") {
        for queue in &queues {
            cfg.labels.insert(queue_label(queue), "yes".to_owned());
        }
    }
    let mut paxos = PaxosConfig::new(acceptors.clone());
    if acceptors.contains(&id) {
        paxos.store = Some(Arc::new(OsFiles::new(dir.join("leases"))));
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
    let activities = Activities::start()?;
    let system = ActorSystem::builder().build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    // Whatever stops the node, the cluster hears that it left.
    let served = serve_node(&args, app, &queues, &history_nodes, &activities, &system, &cluster).await;
    system.shutdown().await?;
    served
}

async fn serve_node(
    args: &Args,
    app: App,
    queues: &[String],
    history_nodes: &[String],
    activities: &Activities,
    system: &ActorSystem,
    cluster: &Cluster,
) -> Result<(), Error> {
    let dir = PathBuf::from(args.get("dir")?);
    let mut journal = JournalConfig::new(history_nodes.iter().map(|h| Arc::from(h.as_str())).collect());
    journal.keep = args.duration("retention", "7d")?;
    journal.keep_failed = args.duration("failed_retention", "30d")?;
    let mut cfg = NodeConfig::new(app, Arc::new(axon::trace::unix_micros), journal);
    if args.yes("history") {
        let at = |what: &str| Arc::new(OsFiles::new(dir.join(what))) as Arc<dyn axon::files::Files>;
        cfg.history = Some((at("journal"), at("journal-snap")));
    }
    cfg.engine = args.yes("engine");
    let (start_rate, token_rate): (u64, u64) =
        (args.or("start_rate", "2000").parse()?, args.or("token_rate", "0").parse()?);
    let queue_tasks: u64 = args.or("queue_tasks", "0").parse()?;
    let (is_api, is_worker) = (args.yes("api"), args.yes("worker"));
    // One for the node: the api's starts and the workers' attempts.
    let quotas = match (is_api && (start_rate > 0 || token_rate > 0)) || (is_worker && queue_tasks > 0) {
        true => Some(Quotas::start(cluster, QuotaConfig::default()).await?),
        false => None,
    };
    if is_worker {
        for queue in queues {
            let mut worker = WorkerConfig::new(queue.clone(), args.or("slots", SLOTS).parse()?);
            worker.executor = activities.executor();
            worker.permits = quotas.clone().filter(|_| queue_tasks > 0).map(|q| (q, QUEUE_TASKS.with_max(queue_tasks)));
            cfg.workers.push(worker);
        }
    }
    let node = node::start(system, cluster, cfg).await?;
    let url = match is_api {
        true => {
            let per_second = |name, max| (max > 0).then(|| Rate::new(name, max, Duration::from_secs(1)));
            let starts = (per_second("wf_starts", start_rate), per_second("wf_token_starts", token_rate));
            Some(serve_api(args, &dir, &node, system, cluster, quotas, starts).await?)
        }
        false => None,
    };
    println!("{}", json!({ "node": cluster.record().id.to_string(), "url": url, "pid": std::process::id() }));
    terminated().await;
    node.drain_workers(args.duration("drain", "60s")?).await;
    Ok(())
}

async fn serve_api(
    args: &Args,
    dir: &Path,
    node: &Node,
    system: &ActorSystem,
    cluster: &Cluster,
    quotas: Option<Quotas>,
    (workflow_starts, token_starts): (Option<Rate>, Option<Rate>),
) -> Result<String, Error> {
    let hash = std::env::var("WF_ADMIN_HASH").map_err(|_| "api=yes needs WF_ADMIN_HASH: see `wf hash-password`")?;
    let mut cfg = ApiConfig::new(hash);
    cfg.key = Some(session_key(&args.0.get("key").map_or_else(|| dir.join("api.key"), PathBuf::from))?);
    cfg.tokens = args.0.get("tokens").map(PathBuf::from);
    cfg.base_path = args.or("base", "/").into();
    cfg.quotas = quotas;
    cfg.workflow_starts = workflow_starts;
    cfg.token_starts = token_starts;
    listen(&Api::new(system, node, cluster, cfg)?, args).await
}

/// Serves the api on `http=`: its URL.
async fn listen(api: &Api, args: &Args) -> Result<String, Error> {
    let listener = tokio::net::TcpListener::bind(args.or("http", "127.0.0.1:0")).await?;
    let url = format!("http://{}", listener.local_addr()?);
    let router = api.router();
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(url)
}

/// `token name= scope=`: a new token, and the entry that lists it in `tokens.toml`.
fn token(args: &Args) -> Result<(), Error> {
    let (token, entry) = new_token(args.get("name")?, args.get("scope")?)?;
    eprintln!("add this to tokens.toml on every api node; the token is shown once:\n\n{entry}");
    println!("{token}");
    Ok(())
}

const USAGE: &str = "usage: workflows dev [dir=target/wf] [http=127.0.0.1:0]
       workflows node --plaintext id= dir= seeds=a,b history_nodes=h1,h2,h3 [history=yes] [engine=yes] [api=yes]
                      [worker=yes] ..
       workflows seed --plaintext [bind=127.0.0.1:0]
       workflows token name= scope=read|signal|start|admin";

#[tokio::main]
async fn main() -> Result<(), Error> {
    // No TLS between nodes: a node talks unencrypted only where `--plaintext` says it may.
    let plaintext = std::env::args().any(|a| a == "--plaintext");
    let mut argv = std::env::args().skip(1);
    let name = argv.next().unwrap_or_default();
    let args = Args(argv.filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect());
    match name.as_str() {
        "seed" | "node" if !plaintext => {
            Err("workflows has no TLS between nodes: add --plaintext to run on loopback or a private network only"
                .into())
        }
        "dev" => dev(args).await,
        "node" => run_node(args).await,
        "seed" => seed(args).await,
        "token" => token(&args),
        _ => Err(USAGE.into()),
    }
}

#[cfg(test)]
mod tests {
    /// Api nodes that start at once with one key file all take the key that landed.
    #[test]
    fn nodes_that_start_together_take_one_session_key() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        for round in 0..20 {
            let file = dir.path().join(format!("api-{round}.key"));
            let keys: Vec<Vec<u8>> = std::thread::scope(|s| {
                let starts: Vec<_> = (0..4).map(|_| s.spawn(|| super::session_key(&file).expect("a key"))).collect();
                starts.into_iter().map(|t| t.join().expect("no panic")).collect()
            });
            assert!(keys.iter().all(|k| *k == keys[0] && !k.is_empty()), "round {round}");
            assert_eq!(std::fs::read(&file).expect("the file"), keys[0]);
        }
    }
}
