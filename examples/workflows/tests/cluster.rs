//! Real nodes over loopback, on real time: three history nodes (journal replicas, wakers, lease acceptors), two
//! engines with the API on the first, and a worker for queue `q` that joins later. Runs whose activity is on `q`
//! wait in its backlog, which `/api/queues` shows, until the worker comes; then they finish, and the backlog is
//! empty. Attempts that run show as slots in use, and one that fails as a failed attempt of the last minute. The
//! worker drains and stops polling. Starts past the rate per workflow are refused `limited`. Two workers whose queue
//! has a `Permits` quota run no more of its attempts at once than the quota, between them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use axon::ActorSystem;
use axon::files::OsFiles;
use axon_cluster::{
    Cluster, ClusterConfig, MemoryDiscovery, PaxosConfig, PaxosLeases, Permits, QuotaConfig, Quotas, Rate,
};
use axon_net::SharedSecret;
use axon_testkit::eventually_within;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use workflows::api::{Api, ApiConfig};
use workflows::auth::hash_password;
use workflows::client::{Client, StartRun};
use workflows::journal::service::JournalConfig;
use workflows::node::{self, NodeConfig};
use workflows::worker::{WorkerConfig, queue_label};
use workflows::workflow::{Activity, ActivityCx, App, Ctx, Failure};

const HISTORY: [&str; 3] = ["h1", "h2", "h3"];
const WAIT: Duration = Duration::from_secs(60);
/// Starts of one workflow per minute, across the cluster.
const STARTS: Rate = Rate::new("wf_starts", 10, Duration::from_secs(60));

#[derive(Serialize, Deserialize)]
struct Far {
    n: u32,
}

impl Activity for Far {
    const NAME: &'static str = "far";
    const QUEUE: &'static str = "q";
    type Output = u32;
    async fn run(self, _: ActivityCx) -> Result<u32, Failure> {
        Ok(self.n * 2)
    }
}

async fn remote(ctx: Ctx, n: u32) -> Result<u32, Failure> {
    ctx.activity(Far { n }).await
}

/// Lets the `Held` activities end.
static OPEN: AtomicBool = AtomicBool::new(false);

/// Holds its worker's slot until `OPEN`.
#[derive(Serialize, Deserialize)]
struct Held {}

impl Activity for Held {
    const NAME: &'static str = "held";
    const QUEUE: &'static str = "q";
    type Output = u32;
    async fn run(self, _: ActivityCx) -> Result<u32, Failure> {
        while !OPEN.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(0)
    }
}

#[derive(Serialize, Deserialize)]
struct Declined {}

impl Activity for Declined {
    const NAME: &'static str = "declined";
    const QUEUE: &'static str = "q";
    type Output = u32;
    async fn run(self, _: ActivityCx) -> Result<u32, Failure> {
        Err(Failure::fatal("card_declined"))
    }
}

/// Lets the `Metered` activities end.
static METERED_OPEN: AtomicBool = AtomicBool::new(false);
/// `Metered` activities that run now, on any worker of this process, and the most at once.
static METERED: AtomicU32 = AtomicU32::new(0);
static METERED_MOST: AtomicU32 = AtomicU32::new(0);

/// On queue `p`, whose attempts hold a permit: runs until `METERED_OPEN`, counted while it runs.
#[derive(Serialize, Deserialize)]
struct Metered {}

impl Activity for Metered {
    const NAME: &'static str = "metered";
    const QUEUE: &'static str = "p";
    type Output = u32;
    async fn run(self, _: ActivityCx) -> Result<u32, Failure> {
        let now = METERED.fetch_add(1, Ordering::SeqCst) + 1;
        METERED_MOST.fetch_max(now, Ordering::SeqCst);
        while !METERED_OPEN.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        METERED.fetch_sub(1, Ordering::SeqCst);
        Ok(now)
    }
}

async fn metered(ctx: Ctx, _: Value) -> Result<u32, Failure> {
    ctx.activity(Metered {}).await
}

async fn held(ctx: Ctx, _: Value) -> Result<u32, Failure> {
    ctx.activity(Held {}).await
}

async fn declined(ctx: Ctx, _: Value) -> Result<u32, Failure> {
    ctx.activity(Declined {}).await
}

async fn quick(_: Ctx, _: Value) -> Result<Value, Failure> {
    Ok(json!("ok"))
}

fn app() -> App {
    (App::new().activity::<Far>().activity::<Held>().activity::<Declined>().activity::<Metered>())
        .workflow("remote", 1, remote)
        .workflow("metered", 1, metered)
        .workflow("quick", 1, quick)
        .workflow("held", 1, held)
        .workflow("declined", 1, declined)
}

struct Up {
    system: ActorSystem,
    cluster: Cluster,
    node: node::Node,
    quotas: Option<Quotas>,
}

async fn boot(name: &str, discovery: &MemoryDiscovery, dir: &std::path::Path, roles: &[&str]) -> Up {
    boot_with(name, discovery, dir, roles, None).await
}

/// A worker of `p` (with the label) holds one of `permits` for each attempt it runs.
async fn boot_with(
    name: &str,
    discovery: &MemoryDiscovery,
    dir: &std::path::Path,
    roles: &[&str],
    permits: Option<Permits>,
) -> Up {
    let mut transport = axon_net::Config::new(name, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true; // loopback only
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    for role in roles {
        cfg.labels.insert((*role).into(), "yes".into());
    }
    let mut paxos = PaxosConfig::new(HISTORY);
    // A short lease: the acceptors' wait after their start is a little over one, and this test waits for it.
    paxos.lease = Duration::from_secs(2);
    let history = roles.contains(&"history");
    if history {
        paxos.store = Some(Arc::new(OsFiles::new(dir.join(name).join("leases"))));
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let mut ncfg =
        NodeConfig::new(app(), Arc::new(axon::trace::unix_micros), JournalConfig::new(HISTORY.map(Arc::from).to_vec()));
    if history {
        let at = |what: &str| Arc::new(OsFiles::new(dir.join(name).join(what))) as Arc<dyn axon::files::Files>;
        ncfg.history = Some((at("journal"), at("journal-snap")));
    }
    ncfg.engine = roles.contains(&"engine");
    let quotas = match roles.contains(&"engine") || permits.is_some() {
        true => Some(Quotas::start(&cluster, QuotaConfig::default()).await.unwrap()),
        false => None,
    };
    if roles.contains(&"worker") {
        ncfg.workers.push(WorkerConfig::new("q", 4));
    }
    if roles.contains(&queue_label("p").as_str()) {
        let mut p = WorkerConfig::new("p", 4);
        p.permits = permits.zip(quotas.clone()).map(|(permits, quotas)| (quotas, permits));
        ncfg.workers.push(p);
    }
    let node = node::start(&system, &cluster, ncfg).await.unwrap();
    Up { system, cluster, node, quotas }
}

/// A key new to a node admits only the fallback, `ceil(max / nodes)`, until the replicas grant it more: a start
/// refused `limited` goes again after its wait.
async fn start_run(client: &Client, start: &StartRun) {
    for _ in 0..WAIT.as_secs() {
        match client.start(start).await {
            Ok(_) => return,
            Err(e) if e.contains("limited") => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(e) => panic!("{e}"),
        }
    }
    panic!("{} still limited after {WAIT:?}", start.workflow);
}

async fn queue_q(client: &Client) -> Value {
    let said = client.queues().await.unwrap();
    said["queues"].as_array().and_then(|qs| qs.iter().find(|q| q["name"] == "q").cloned()).unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_waits_for_its_worker_shows_in_the_api_and_drains() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for h in HISTORY {
        nodes.push(boot(h, &discovery, dir.path(), &["history"]).await);
    }
    nodes.push(boot("e1", &discovery, dir.path(), &["engine", "api"]).await);
    nodes.push(boot("e2", &discovery, dir.path(), &["engine"]).await);
    eventually_within("the acceptors past their wait", WAIT, async || {
        nodes.iter().all(|n| !n.cluster.warming() && n.cluster.view().members.len() == 5)
    })
    .await;

    let hash = hash_password("pw").unwrap();
    let mut cfg = ApiConfig::new(hash);
    cfg.quotas = nodes[3].quotas.clone();
    cfg.workflow_starts = Some(STARTS);
    let api = Api::new(&nodes[3].system, &nodes[3].node, &nodes[3].cluster, cfg).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    tokio::spawn(async move { axum::serve(listener, router).await });
    let (token, _) = Client::new(&url, None).login("pw").await.unwrap();
    let client = Client::new(&url, Some(token.clone()));

    // No worker: the runs' attempts wait in the backlog.
    for n in 0..6 {
        let start =
            StartRun { workflow: "remote".into(), id: Some(format!("r{n}")), input: json!(n), ..StartRun::default() };
        start_run(&client, &start).await;
    }
    eventually_within("the backlog holds the six attempts", WAIT, async || {
        let q = queue_q(&client).await;
        q["backlog"] == 6 && q["workers"] == json!([])
    })
    .await;
    let q = queue_q(&client).await;
    eprintln!("GET /api/queues, queue q, before a worker: {q}");
    assert_eq!(q["pollers"], 0, "{q}");
    assert!(q["oldest_ms"].as_u64().is_some(), "{q}");

    let mut labels = vec!["worker"];
    let label = queue_label("q");
    labels.push(&label);
    nodes.push(boot("w1", &discovery, dir.path(), &labels).await);
    for n in 0..6u64 {
        let id = format!("r{n}");
        eventually_within("each run finished by the worker", WAIT, async || {
            client.get(&id).await.is_ok_and(|run| run["status"] == "completed" && run["output"] == json!(n * 2))
        })
        .await;
    }
    eventually_within("the backlog empty and the worker polling", WAIT, async || {
        let q = queue_q(&client).await;
        q["backlog"] == 0 && q["pollers"].as_u64() > Some(0) && q["workers"] == json!(["w1"])
    })
    .await;

    // Three attempts that run hold three of the worker's four slots; one that fails counts for the last minute.
    for n in 0..3 {
        let start = StartRun { workflow: "held".into(), id: Some(format!("held-{n}")), ..StartRun::default() };
        start_run(&client, &start).await;
    }
    let start = StartRun { workflow: "declined".into(), id: Some("declined-0".into()), ..StartRun::default() };
    start_run(&client, &start).await;
    eventually_within("three slots of four in use and a failed attempt", WAIT, async || {
        let q = queue_q(&client).await;
        q["slots"] == 4 && q["running"] == 3 && q["failed_minute"] == 1
    })
    .await;
    let workers = client.page("workers").await.unwrap();
    eprintln!("GET /api/queues, queue q, with attempts that run: {}", queue_q(&client).await);
    eprintln!("GET /api/workers: {workers}");
    let w1 = &workers["workers"][0];
    assert_eq!((&w1["node"], &w1["slots"]["q"], &w1["running"]["q"]), (&json!("w1"), &json!(4), &json!(3)), "{w1}");
    assert!(w1["last_poll_ms"]["q"].as_u64().is_some(), "{w1}");
    OPEN.store(true, Ordering::Relaxed);
    eventually_within("the slots free again", WAIT, async || queue_q(&client).await["running"] == 0).await;

    // A drained worker polls no more.
    nodes[5].node.drain_workers(Duration::from_secs(10)).await;
    eventually_within("no pollers after the drain", WAIT, async || queue_q(&client).await["pollers"] == 0).await;

    // Past the rate of starts, a start is refused with when to try again.
    let http = reqwest::Client::new();
    let mut said = Vec::new();
    for n in 0..12 {
        let response = http
            .post(format!("{url}/api/runs"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(json!({ "workflow": "quick", "id": format!("quick-{n}") }).to_string())
            .send()
            .await
            .unwrap();
        let after = response.headers().get("retry-after").map(|v| v.to_str().unwrap().to_owned());
        said.push((response.status().as_u16(), after));
    }
    assert!(said.iter().any(|(status, _)| *status == 201), "{said:?}");
    let limited: Vec<_> = said.iter().filter(|(status, _)| *status == 429).collect();
    assert!(!limited.is_empty(), "{said:?}");
    assert!(limited.iter().all(|(_, after)| after.as_deref().is_some_and(|s| s.parse::<u64>().is_ok_and(|s| s >= 1))));

    for up in nodes.into_iter().rev() {
        drop(up.quotas);
        drop(up.node);
        drop(up.cluster);
        up.system.shutdown().await.unwrap();
    }
}

/// Two workers of `p` with 4 slots each, and a quota of 2 permits on `p`: of 6 attempts, 2 run at once between them,
/// never more, until they end; then the next ones, until all have run. Without the quota all 6 run at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_quota_bounds_its_attempts_across_the_workers() {
    const TASKS: Permits = Permits::new("wf_queue_tasks", 2);
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for h in HISTORY {
        nodes.push(boot(h, &discovery, dir.path(), &["history"]).await);
    }
    nodes.push(boot("e1", &discovery, dir.path(), &["engine", "api"]).await);
    let labels = ["worker".to_owned(), queue_label("q"), queue_label("p")];
    let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
    for w in ["w1", "w2"] {
        nodes.push(boot_with(w, &discovery, dir.path(), &labels, Some(TASKS)).await);
    }
    eventually_within("the acceptors past their wait and the quotas started", WAIT, async || {
        nodes
            .iter()
            .all(|n| !n.cluster.warming() && !n.cluster.quotas_starting() && n.cluster.view().members.len() == 6)
    })
    .await;
    let api =
        Api::new(&nodes[3].system, &nodes[3].node, &nodes[3].cluster, ApiConfig::new(hash_password("pw").unwrap()))
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    tokio::spawn(async move { axum::serve(listener, router).await });
    let (token, _) = Client::new(&url, None).login("pw").await.unwrap();
    let client = Client::new(&url, Some(token));

    for n in 0..6 {
        let start = StartRun { workflow: "metered".into(), id: Some(format!("m{n}")), ..StartRun::default() };
        client.start(&start).await.unwrap();
    }
    let queue_p = async || {
        let said = client.queues().await.unwrap();
        said["queues"].as_array().and_then(|qs| qs.iter().find(|q| q["name"] == "p").cloned()).unwrap_or(Value::Null)
    };
    eventually_within("two attempts of p run", WAIT, async || METERED.load(Ordering::SeqCst) >= 2).await;
    // The workers poll again after each refusal: none takes a third meanwhile.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(METERED_MOST.load(Ordering::SeqCst), 2, "the most attempts of p at once");
    eventually_within("the api says two of p's eight slots in use", WAIT, async || {
        let q = queue_p().await;
        q["slots"] == 8 && q["running"] == 2
    })
    .await;
    eprintln!("GET /api/queues, queue p, at its quota: {}", queue_p().await);
    METERED_OPEN.store(true, Ordering::Relaxed);
    for n in 0..6 {
        let id = format!("m{n}");
        eventually_within("each metered run completed", WAIT, async || {
            client.get(&id).await.is_ok_and(|run| run["status"] == "completed")
        })
        .await;
    }
    assert_eq!(METERED_MOST.load(Ordering::SeqCst), 2, "the most attempts of p at once");

    for up in nodes.into_iter().rev() {
        drop(up.quotas);
        drop(up.node);
        drop(up.cluster);
        up.system.shutdown().await.unwrap();
    }
}

/// A directory whose listing takes as long as a big journal's read on a restart.
struct Slow(axon::files::MemoryFiles);

impl axon::files::Files for Slow {
    fn list(&self) -> axon::files::FileFuture<Vec<(String, u64)>> {
        let list = self.0.list();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            list.await
        })
    }
    fn read(&self, name: &str) -> axon::files::FileFuture<Vec<u8>> {
        self.0.read(name)
    }
    fn apply(&self, ops: Vec<axon::files::FileOp>) -> axon::files::FileFuture<()> {
        self.0.apply(ops)
    }
}

/// A history node's journal that takes longer than its 1 s tick and the system's start timeout to read its log
/// opens, and serves: the soak's history node, restarted with 800 MB of log, failed its start after 5 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_journal_slow_to_open_opens() {
    let mut transport = axon_net::Config::new("h1", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true; // loopback only
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    // Shorter than the open below, as the default 5 s is shorter than a big log's read.
    let system = ActorSystem::builder().start_timeout(Duration::from_secs(1)).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let files = Arc::new(Slow(axon::files::MemoryFiles::new()));
    let snaps = Arc::new(axon::files::MemoryFiles::new());
    let journal = JournalConfig::new(HISTORY.map(Arc::from).to_vec());
    let handle = workflows::journal::service::start(&system, &cluster, files, snaps, journal).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let stats = handle.ask(workflows::journal::service::ToJournal::Stats).await;
    assert!(stats.is_ok(), "the journal ended: {:?}", stats.err());
    drop(cluster);
    system.shutdown().await.unwrap();
}
