//! A node and its roles (design §5.1): a home keeps its documents, their streams and its catalog on its disk; the
//! accounts node keeps the users; a web node serves HTTP and the websockets, with a gate that ends revoked sessions.
//! `mdpad serve` is one node with every role, home `h1`, on `MemoryDiscovery`; `mdpad node` is one of a cluster that
//! finds itself through seed servers.
//!
//! **The boot check.** No lease decides who runs a home's documents: the one node labelled `home=<name>` does. So a
//! home waits for the provider to answer and two heartbeats to pass, then refuses to start while another record
//! carries its label at another address, and `docs/home.id` must name it. This is a check against operator error,
//! not consensus: two that start at the same moment, or on the two sides of a partition, both pass (README).
//!
//! Every node runs axon's telemetry; with `health=` it serves `/healthz`, `/readyz` and `/metrics`, and with
//! `console=` it also collects the cluster's metrics and serves axon's console.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{FileOp, Files, LogConfig};
use axon::{ActorHandle, ActorSystem, Execution, MailboxLimits, PoolConfig, RuntimeConfig};
use axon_cluster::{
    AccessPolicy, Cluster, ClusterConfig, Discovery, Member, MemoryDiscovery, Placement, View, WeakCluster,
};
use axon_net::SharedSecret;
use axon_obs::{
    Dashboard, DashboardConfig, Health, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig,
};
use axon_stream::{CompactLimits, RingLimits, StreamNode, StreamNodeConfig};
use bytes::Bytes;
use futures_util::future::join_all;
use tokio::task::JoinHandle;

use crate::accounts::{self, AccountsKeep};
use crate::auth::Auth;
use crate::catalog::{self, CatalogKeep, Ids};
use crate::core::accounts::Event as AccountsEvent;
use crate::core::catalog::{RowState, Tab};
use crate::doc::{self, Doc, Home, KIND};
use crate::gate::Gate;
use crate::journal::{self, Journal, ToJournal};
use crate::keeper::{self, Keeper};
use crate::remote::{
    ACCOUNTS, AccountsAnswer, AccountsP, AccountsReq, CATALOG, CatalogAnswer, CatalogP, CatalogReq, Front, GATE, GateP,
    GateReq, HomeStats, OpenDoc, Serve,
};
use crate::web::{self, Web};
use crate::wire::DocP;

/// How long a web node waits for its view to list every home before it serves anyway.
const VIEW_WAIT: Duration = Duration::from_secs(10);

/// What a node does. A home's node id is its home name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Roles {
    pub home: Option<String>,
    pub web: bool,
    pub accounts: bool,
}

impl Roles {
    pub fn all(home: &str) -> Self {
        Self { home: Some(home.to_string()), web: true, accounts: true }
    }

    /// `web,home,accounts` or any of them, for the node `id`.
    pub fn parse(id: &str, list: &str) -> Result<Self, String> {
        let mut roles = Self::default();
        for role in list.split(',').filter(|r| !r.is_empty()) {
            match role {
                "home" => roles.home = Some(id.to_string()),
                "web" => roles.web = true,
                "accounts" => roles.accounts = true,
                _ => return Err(format!("{role} is not a role: web, home or accounts")),
            }
        }
        Ok(roles)
    }

    pub fn labels(&self) -> BTreeMap<String, String> {
        let one = |role: &str| (role.to_string(), "1".to_string());
        let mut labels = BTreeMap::new();
        if let Some(home) = &self.home {
            labels.extend([one("homes"), ("home".into(), home.clone())]);
        }
        labels.extend(self.web.then(|| one("web")));
        labels.extend(self.accounts.then(|| one("accounts")));
        // What axon's console shows as the node's role.
        let shown: Vec<&str> = ["home", "web", "accounts"].into_iter().filter(|r| labels.contains_key(*r)).collect();
        labels.insert("role".into(), shown.join(","));
        labels
    }
}

pub struct NodeConfig {
    pub id: String,
    pub roles: Roles,
    /// Every home of the cluster, the same list on every node.
    pub homes: Vec<String>,
    /// A home's directory.
    pub docs: Option<Arc<dyn Files>>,
    /// The accounts node's directory.
    pub accounts: Option<Arc<dyn Files>>,
    /// Signs sign-ins, links and guests: 32 bytes or more, the same on every web node and kept across restarts.
    pub key: Vec<u8>,
    /// Where a web node serves HTTP. `None` serves none: the World tests open sessions in process.
    pub http: Option<SocketAddr>,
    pub workers: usize,
    /// A document nobody calls for this long stops; the next call loads it again.
    pub doc_idle: Duration,
    /// The document streams' limits: `doc::limits()`, but for a test that makes a publish fail.
    pub doc_limits: CompactLimits,
    /// The documents' log: `journal::docs_config()`. Its segments are made full length, so a full disk shows only when
    /// the next is made; the fault runs turn that off to see a full disk at once.
    pub docs_log: LogConfig,
    /// How long a home waits after the provider first answers before its boot check: two heartbeats with seeds. A
    /// `MemoryDiscovery` that the nodes of one process share is settled at once. A web node waits up to `VIEW_WAIT`
    /// for its view to list every home serving documents before it serves, or one home where this is zero.
    pub settle: Duration,
    /// Where `/healthz`, `/readyz` and `/metrics` are served, if anywhere.
    pub health: Option<SocketAddr>,
    /// Where axon's console is served, with the cluster's metrics collected here.
    pub console: Option<SocketAddr>,
    /// Samples this node for the console and `/metrics`. Its sampler runs on tokio's blocking pool, which the World
    /// refuses, so a World node leaves it off; `health=` and `console=` turn it on.
    pub telemetry: bool,
}

impl NodeConfig {
    /// One node with every role, home `h1`.
    pub fn new(docs: Arc<dyn Files>, accounts: Arc<dyn Files>, key: Vec<u8>) -> Self {
        Self { docs: Some(docs), accounts: Some(accounts), ..Self::with_key(key) }
    }

    /// Every role, home `h1`, and no directories yet.
    pub fn with_key(key: Vec<u8>) -> Self {
        Self {
            id: "h1".into(),
            roles: Roles::all("h1"),
            homes: vec!["h1".into()],
            docs: None,
            accounts: None,
            key,
            http: Some(([127, 0, 0, 1], 0).into()),
            workers: 2,
            doc_idle: Duration::from_secs(15 * 60),
            doc_limits: doc::limits(),
            docs_log: journal::docs_config(),
            settle: Duration::ZERO,
            health: None,
            console: None,
            telemetry: false,
        }
    }

    /// The cluster's settings for this node: its labels, and the heartbeats the design asks of members of seeds.
    pub fn cluster(&self, bind: SocketAddr, transport: axon_net::Config, discovery: impl Discovery) -> ClusterConfig {
        let mut cfg = ClusterConfig::new(bind, transport, discovery);
        cfg.labels = self.roles.labels();
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
        cfg
    }

    /// `runtime` with this node's workers and the pool the documents run on.
    pub fn runtime(&self, mut runtime: RuntimeConfig) -> RuntimeConfig {
        runtime.workers = self.workers;
        runtime.pools.push(PoolConfig { name: "doc".into(), workers: self.workers, ..Default::default() });
        runtime
    }
}

/// The transport between the nodes of this example: a shared secret, in plaintext.
pub fn transport(id: &str) -> Result<axon_net::Config, String> {
    let secret = SharedSecret::new("mdpad-cluster-secret").map_err(err)?;
    let mut transport = axon_net::Config::new(id, secret);
    transport.allow_plaintext = true; // `serve` on loopback; a seed or node only with --plaintext, which main checks
    Ok(transport)
}

/// What a home runs.
pub struct HomeParts {
    pub ids: Ids,
    pub journal: ActorHandle<ToJournal>,
    pub catalog: catalog::Handle,
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
    pub home: Option<HomeParts>,
    pub accounts: Option<accounts::Handle>,
    pub web: Option<Web>,
    http: Option<SocketAddr>,
    pub health: Option<SocketAddr>,
    pub console: Option<SocketAddr>,
    server: Option<JoinHandle<()>>,
}

/// A home's boot check: refused while another record carries `home=<home>` at an address other than `bind`'s (a
/// record of this node's own last start stays a while after a crash, at the same address).
pub async fn boot_check(
    discovery: &dyn Discovery,
    home: &str,
    bind: SocketAddr,
    settle: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while let Err(e) = discovery.snapshot().await {
        if tokio::time::Instant::now() > deadline {
            return Err(format!("discovery does not answer: {e}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(settle).await;
    let snapshot = discovery.snapshot().await.map_err(|e| format!("discovery: {e}"))?;
    let other =
        snapshot.nodes.iter().find(|r| r.label("home") == Some(home) && (bind.port() == 0 || r.address != bind));
    match other {
        Some(other) => Err(format!("home {home} runs on {} at {}: refused", other.id, other.address)),
        None => Ok(()),
    }
}

/// `home.id` in a home's directory names its home: a directory of another home is refused, and a new one is marked.
async fn home_id(docs: &dyn Files, home: &str) -> Result<(), String> {
    let names = docs.list().await.map_err(|e| format!("docs: {e}"))?;
    if names.iter().any(|(name, _)| name == "home.id") {
        let held = docs.read("home.id").await.map_err(|e| format!("docs/home.id: {e}"))?;
        let held = String::from_utf8_lossy(&held);
        return match held.trim() == home {
            true => Ok(()),
            false => Err(format!("docs/home.id names {}, not {home}", held.trim())),
        };
    }
    let mark = vec![FileOp::Replace("home.id".into(), Bytes::from(format!("{home}\n")))];
    docs.apply(mark).await.map_err(|e| format!("docs/home.id: {e}"))
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl Node {
    /// One node in this process on its own `MemoryDiscovery`, on loopback: `mdpad serve` and the tests.
    pub async fn start(cfg: NodeConfig) -> Result<Self, String> {
        let ccfg = cfg.cluster(([127, 0, 0, 1], 0).into(), transport(&cfg.id)?, MemoryDiscovery::new());
        let runtime = cfg.runtime(RuntimeConfig::default());
        Self::boot(cfg, ccfg, runtime).await
    }

    /// The boot check for a home, then the cluster and the roles.
    pub async fn boot(cfg: NodeConfig, ccfg: ClusterConfig, runtime: RuntimeConfig) -> Result<Self, String> {
        if let Some(home) = &cfg.roles.home {
            if *home != cfg.id {
                return Err(format!("a home's node id is its name: {home}, not {}", cfg.id));
            }
            boot_check(ccfg.discovery.as_ref(), home, ccfg.bind, cfg.settle).await?;
            home_id(cfg.docs.as_deref().ok_or("a home needs its docs directory")?, home).await?;
        }
        let system = ActorSystem::builder().runtime(runtime).timing_sample_every(64).build().map_err(err)?;
        let cluster = Cluster::start(&system, ccfg).await.map_err(err)?;
        Self::on(system, cluster, cfg).await
    }

    /// The roles of `cfg` on a cluster that has started.
    pub async fn on(system: ActorSystem, cluster: Cluster, cfg: NodeConfig) -> Result<Self, String> {
        let mut scfg = StreamNodeConfig::new(vec![]);
        for home in &cfg.homes {
            for (namespace, max_records, max_bytes) in
                [(doc::namespace(home), 2048, 4 << 20), (doc::presence_namespace(home), 512, 512 << 10)]
            {
                scfg.core.rings.push(RingLimits { namespace: namespace.clone(), max_records, max_bytes });
                if cfg.roles.home.as_ref() != Some(home) {
                    let origin = BTreeMap::from([("home".to_string(), home.clone())]);
                    scfg = scfg.upstream_for(namespace, vec![origin]);
                }
            }
        }
        scfg.core.hub.max_streams = 16_384;
        scfg.core.store.max_streams = 16_384;
        if cfg.roles.web {
            // A web node feeds every viewer of a hot document from its relay.
            scfg.core.hub.max_queued_bytes = 1 << 30;
        }
        let streams = StreamNode::start(&cluster, scfg).await.map_err(err)?;
        let (health, console) = observe(&system, &cluster, &streams, &cfg).await?;

        let home = match &cfg.roles.home {
            Some(name) => Some(start_home(&system, &cluster, &streams, &cfg, name).await?),
            None => None,
        };
        let accounts = match cfg.roles.accounts {
            true => Some(start_accounts(&system, &cluster, &cfg).await?),
            false => None,
        };
        let (mut web, mut http, mut server) = (None, None, None);
        if cfg.roles.web {
            let started = start_web(&system, &cluster, &streams, &cfg)?;
            // A session opened before the view lists its document's home, with the kind registered, would tell its
            // client the home is out of reach. In one process the homes start in turn: one is enough.
            let serves = |view: &View, h: &String| {
                let hosts = |m: &&Member| m.available() && m.record.has_kind(KIND);
                view.members.iter().filter(hosts).any(|m| m.record.label("home") == Some(h))
            };
            let homes = |view: &View| match cfg.settle.is_zero() {
                true => cfg.homes.iter().any(|h| serves(view, h)),
                false => cfg.homes.iter().all(|h| serves(view, h)),
            };
            let _ = tokio::time::timeout(VIEW_WAIT, cluster.wait_view(homes)).await;
            if let Some(bind) = cfg.http {
                let (addr, task) = serve_http(bind, &started).await?;
                (http, server) = (Some(addr), Some(task));
            }
            web = Some(started);
        }
        Ok(Self { system, cluster, streams, home, accounts, web, http, health, console, server })
    }

    /// The HTTP address of a web node.
    pub fn http(&self) -> SocketAddr {
        self.http.expect("a web node that serves HTTP")
    }

    pub fn web(&self) -> &Web {
        self.web.as_ref().expect("a web node")
    }

    pub fn ids(&self) -> &Ids {
        &self.home.as_ref().expect("a home").ids
    }

    pub fn journal(&self) -> &ActorHandle<ToJournal> {
        &self.home.as_ref().expect("a home").journal
    }

    pub fn catalog(&self) -> &catalog::Handle {
        &self.home.as_ref().expect("a home").catalog
    }

    pub fn accounts(&self) -> &accounts::Handle {
        self.accounts.as_ref().expect("the accounts node")
    }

    /// Ends the node as a crash would, for tests: the server stops at once and nothing more is written.
    pub fn crash(self) {
        if let Some(server) = self.server {
            server.abort();
        }
    }

    pub async fn shutdown(self) {
        if let Some(server) = self.server {
            server.abort();
        }
        if let Err(e) = self.system.shutdown().await {
            eprintln!("mdpad: shutdown: {e:?}");
        }
    }
}

/// Telemetry where asked, the probes and `/metrics` where asked, and the console where asked.
async fn observe(
    system: &ActorSystem,
    cluster: &Cluster,
    streams: &StreamNode,
    cfg: &NodeConfig,
) -> Result<(Option<SocketAddr>, Option<SocketAddr>), String> {
    if !(cfg.telemetry || cfg.health.is_some() || cfg.console.is_some()) {
        return Ok((None, None));
    }
    let telemetry =
        Telemetry::start(system, cluster, TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() })
            .await?;
    telemetry.watch_streams(streams);
    let health = match cfg.health {
        Some(bind) => {
            let health = Health::start(system, cluster, bind).await?;
            health.serve_metrics(&telemetry);
            Some(health.address())
        }
        None => None,
    };
    let console = match cfg.console {
        Some(bind) => {
            let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
            MetricsService::start(system, cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
            Some(Dashboard::start(system, cluster, DashboardConfig { bind, ..Default::default() }).await?.address())
        }
        None => None,
    };
    Ok((health, console))
}

async fn start_home(
    system: &ActorSystem,
    cluster: &Cluster,
    streams: &StreamNode,
    cfg: &NodeConfig,
    name: &str,
) -> Result<HomeParts, String> {
    let docs = cfg.docs.clone().ok_or("a home needs its docs directory")?;
    let journal = Journal::new(docs, cfg.docs_log.clone());
    let failing = journal.failing();
    // A restart starts every document its clients had open at once, and each asks for its records: the default 128
    // asks at once failed a crowd's starts, and each failure asked again.
    let crowd = MailboxLimits { messages: 16_384, requests: 16_384, ..Default::default() };
    let journal = system.spawn(journal).name("journal").mailbox(crowd).start().map_err(err)?;
    journal.ready().await.map_err(|e| format!("the docs journal: {e}"))?;

    let ids: Ids = Arc::new(Mutex::new(BTreeMap::new()));
    let (home, keep_ids, catalog_journal) = (name.to_string(), ids.clone(), journal.clone());
    let catalog = system
        .supervise(move || {
            let keep = CatalogKeep { home: home.clone(), ids: keep_ids.clone() };
            Keeper::new(keep, catalog_journal.clone())
        })
        .policy(keeper::policy())
        .start_supervised()
        .map_err(err)?;
    // The catalog has filled the id set before the first document starts.
    catalog
        .ask(|reply| {
            let (user, tab, after, limit) = (String::new(), Tab::Mine, None, 1);
            keeper::Msg::Request(catalog::Request::List { user, tab, after, limit, reply })
        })
        .await
        .map_err(|e| format!("the catalog: {e:?}"))?;
    let front = system
        .spawn(Front::<CatalogP>::new(serve_catalog(
            catalog.clone(),
            journal.clone(),
            cluster.downgrade(),
            streams.clone(),
        )))
        .mailbox(Front::<CatalogP>::limits())
        .name("catalog-front")
        .start()
        .map_err(err)?;
    cluster.publish::<CatalogP>(&front, CATALOG).map_err(err)?;

    let home = Home {
        journal: journal.clone(),
        catalog: catalog.clone(),
        streams: streams.clone(),
        limits: cfg.doc_limits,
        failing,
    };
    let factory_ids = ids.clone();
    // Sessions call a document through its KeyedRef, a message each: the default `messages` and `bytes` bound them
    // (`Rejected(Full)` past either), and `ClusterConfig::max_inbound_replies` a node's. `requests` counts local asks.
    cluster
        .kind::<DocP>(KIND)
        .execution(Execution::Pool("doc"))
        .idle(cfg.doc_idle)
        .policy(doc::policy())
        .register(move |cx, _args| {
            let id = cx.key.clone().ok_or("a document needs an id")?;
            // No I/O here: the catalog's id set says whether the id was ever made.
            let state = factory_ids.lock().expect("the id set").get(&id).copied();
            let state = state.ok_or_else(|| format!("{} {id}", doc::UNKNOWN))?;
            Ok(Doc::new(id, state == RowState::Ready, home.clone(), cx.cluster.clone()))
        })
        .await
        .map_err(err)?;
    Ok(HomeParts { ids, journal, catalog })
}

fn serve_catalog(
    catalog: catalog::Handle,
    journal: ActorHandle<ToJournal>,
    cluster: WeakCluster,
    streams: StreamNode,
) -> Serve<CatalogReq, CatalogAnswer> {
    Arc::new(move |req| {
        let (catalog, journal, cluster, streams) = (catalog.clone(), journal.clone(), cluster.clone(), streams.clone());
        Box::pin(async move {
            let unavailable = |e: &dyn std::fmt::Debug| CatalogAnswer::Unavailable { why: format!("{e:?}") };
            match req {
                CatalogReq::List { user, tab, after, limit } => catalog
                    .ask(|reply| {
                        let (user, after) = (user.clone(), after.clone());
                        keeper::Msg::Request(catalog::Request::List { user, tab, after, limit, reply })
                    })
                    .await
                    .map_or_else(|e| unavailable(&e), |rows| CatalogAnswer::Rows { rows }),
                CatalogReq::Create { owner } => catalog
                    .ask(|reply| keeper::Msg::Request(catalog::Request::Create { owner: owner.clone(), reply }))
                    .await
                    .map_or_else(|e| unavailable(&e), |id| CatalogAnswer::Created { id }),
                CatalogReq::Hide { id, user } => catalog
                    .ask(|reply| {
                        let (id, user) = (id.clone(), user.clone());
                        keeper::Msg::Request(catalog::Request::Hide { id, user, reply })
                    })
                    .await
                    .map_or_else(|e| unavailable(&e), |hidden| CatalogAnswer::Hidden { hidden }),
                CatalogReq::Hosted { from, limit } => {
                    let Some(cluster) = cluster.upgrade() else { return unavailable(&"stopping") };
                    // `hosted_keys` reads this node only: the reason a home answers this and not a web node.
                    let (keys, more) = cluster.hosted_keys(KIND, "", from.as_deref(), limit.min(1000));
                    let open = cluster.hosted(0).kinds.iter().find(|k| k.kind == KIND).map_or(0, |k| k.count);
                    let ids: Vec<String> = keys.into_iter().map(|k| k.key).collect();
                    let rows =
                        catalog.ask(|reply| keeper::Msg::Request(catalog::Request::Rows { ids: ids.clone(), reply }));
                    let (rows, log) = futures_util::future::join(rows, journal.ask(ToJournal::Stats)).await;
                    let (Ok((rows, stored)), Ok(log)) = (rows, log) else {
                        return unavailable(&"the catalog or the journal");
                    };
                    let docs = rows.into_iter().map(|(id, row)| OpenDoc { id, title: row.title, owner: row.owner });
                    CatalogAnswer::Hosted(HomeStats {
                        open,
                        docs: docs.collect(),
                        more,
                        stored,
                        saved: doc::SAVED.load(std::sync::atomic::Ordering::Relaxed),
                        log_bytes: log.bytes,
                        log_live_bytes: log.live_bytes,
                        log_segments: log.segments,
                        failed_writes: log.failed_writes,
                        streams: streams.stats().hub.streams,
                    })
                }
            }
        })
    })
}

async fn start_accounts(system: &ActorSystem, cluster: &Cluster, cfg: &NodeConfig) -> Result<accounts::Handle, String> {
    let files = cfg.accounts.clone().ok_or("the accounts node needs its accounts directory")?;
    let journal = system
        .spawn(Journal::new(files, journal::small_config(accounts::KEY)))
        .name("accounts-journal")
        .start()
        .map_err(err)?;
    journal.ready().await.map_err(|e| format!("the accounts journal: {e}"))?;
    let accounts = system
        .supervise(move || Keeper::new(AccountsKeep, journal.clone()))
        .policy(keeper::policy())
        .start_supervised()
        .map_err(err)?;
    let front = system
        .spawn(Front::<AccountsP>::new(serve_accounts(accounts.clone(), cluster.downgrade())))
        .mailbox(Front::<AccountsP>::limits())
        .name("accounts-front")
        .start()
        .map_err(err)?;
    cluster.publish::<AccountsP>(&front, ACCOUNTS).map_err(err)?;
    Ok(accounts)
}

/// The accounts' calls. A new password or a disable, once durable, is told to every web node's gate.
fn serve_accounts(accounts: accounts::Handle, cluster: WeakCluster) -> Serve<AccountsReq, AccountsAnswer> {
    Arc::new(move |req| {
        let (accounts, cluster) = (accounts.clone(), cluster.clone());
        Box::pin(async move {
            let unavailable = |e: &dyn std::fmt::Debug| AccountsAnswer::Unavailable { why: format!("{e:?}") };
            let get = |name: String| {
                let accounts = accounts.clone();
                async move {
                    accounts
                        .ask(|reply| keeper::Msg::Request(accounts::Request::Get { name: name.clone(), reply }))
                        .await
                }
            };
            let event = match req {
                AccountsReq::Get { name } => {
                    return get(name).await.map_or_else(|e| unavailable(&e), |user| AccountsAnswer::User { user });
                }
                AccountsReq::List => {
                    return accounts
                        .ask(|reply| keeper::Msg::Request(accounts::Request::List { reply }))
                        .await
                        .map_or_else(|e| unavailable(&e), |users| AccountsAnswer::Users { users });
                }
                AccountsReq::Record { event } => event,
            };
            let ends = match &event {
                AccountsEvent::Password { name, .. } | AccountsEvent::Disable { name } => Some(name.clone()),
                AccountsEvent::Add { .. } | AccountsEvent::Enable { .. } | AccountsEvent::Operator { .. } => None,
            };
            let recorded = accounts
                .ask(|reply| keeper::Msg::Request(accounts::Request::Record { event: event.clone(), reply }))
                .await;
            let result = match recorded {
                Ok(result) => result,
                Err(e) => return unavailable(&e),
            };
            if result.is_ok()
                && let Some(name) = ends
                && let Ok(Some(user)) = get(name.clone()).await
                && let Some(cluster) = cluster.upgrade()
            {
                let (gates, _) = cluster.resolve_all_among::<GateP>(GATE, &doc::web()).await;
                let revoke = GateReq::Revoke { user: name, version: user.version };
                join_all(gates.iter().map(|gate| gate.tell(revoke.clone()))).await;
            }
            AccountsAnswer::Recorded { result }
        })
    })
}

fn start_web(system: &ActorSystem, cluster: &Cluster, streams: &StreamNode, cfg: &NodeConfig) -> Result<Web, String> {
    let accounts = cluster.named::<AccountsP>(ACCOUNTS).among(&Placement::default().require("accounts", "1"));
    let catalogs =
        cfg.homes.iter().map(|h| (h.clone(), cluster.named::<CatalogP>(CATALOG).among(&doc::home(h)))).collect();
    let gate = system
        .spawn(Gate::new(accounts.clone(), streams.clone()))
        // A watch of each session counts against the sources.
        .mailbox(MailboxLimits { sources: 65_536, ..Default::default() })
        .name("gate")
        .start()
        .map_err(err)?;
    cluster.publish::<GateP>(&gate, GATE).map_err(err)?;
    Ok(Web {
        system: system.clone(),
        cluster: cluster.clone(),
        streams: streams.clone(),
        auth: Arc::new(Auth::new(&cfg.key)?),
        accounts,
        catalogs: Arc::new(catalogs),
        gate,
    })
}

async fn serve_http(bind: SocketAddr, web: &Web) -> Result<(SocketAddr, JoinHandle<()>), String> {
    use axum::serve::ListenerExt;
    let listener = tokio::net::TcpListener::bind(bind).await.map_err(err)?;
    let http = listener.local_addr().map_err(err)?;
    // Nagle's algorithm would hold a small frame sent behind another until the peer's delayed ACK.
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    let app = web::router(web.clone());
    let server = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("mdpad: http server: {e}");
        }
    });
    Ok((http, server))
}
