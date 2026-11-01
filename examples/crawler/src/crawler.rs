//! The crawler: one keyed actor per domain owns that domain's frontier, its politeness timer and its log. Fetches
//! run as jobs. A page's links to other domains are handed to their actors, wherever they run, before the page
//! counts as finished, so a crash loses no link: the page is fetched again instead.

use std::collections::BTreeMap;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, ActorSystem, Context, MailboxConfig, MailboxLimits, SubmitError, TaskAdmissionError, TimerReject};
use axon_cluster::{
    AccessPolicy, ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, KeyedRef, KeyedRefs, MemberChange,
    MembershipEvent, Placement, Rejection, Reply, Retry, SeedConfig, SeedDiscovery, SpawnContext, Unaddressed,
    WeakCluster, WireProtocol,
};
use axon_net::SharedSecret;
use axon_obs::{Dashboard, DashboardConfig, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::frontier::{Counts, Frontier, Next, Pending};
use crate::http::{self, Fetch};
use crate::store::{self, Record};

pub const KIND: &str = "domain";
pub const SECRET: &str = "crawler example shared secret";
/// How long a page's links may take to reach their domains before they count as lost.
const FORWARD_DEADLINE: Duration = Duration::from_secs(30);
const MAX_PEERS: usize = 4096;

pub enum Call {
    Add(Vec<String>),
    Status,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub node: String,
    pub counts: Counts,
    /// Records waiting for, or in, a log write.
    pub logging: u64,
    /// Links this domain could not hand over within `FORWARD_DEADLINE`.
    pub lost_links: u64,
}

impl Status {
    pub fn idle(&self) -> bool {
        self.counts.queued == 0 && self.counts.in_flight == 0 && self.logging == 0
    }
}

pub enum Answer {
    Added,
    Status(Status),
}

pub struct DomainApi;

fn codec(what: &str) -> CallError {
    CallError::Codec(what.into())
}

impl WireProtocol for DomainApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0x0C4A_71E4;
    const SCHEMA: u16 = 1;

    fn encode(call: Call) -> Result<Bytes, CallError> {
        let mut out = BytesMut::new();
        match call {
            Call::Add(urls) => {
                out.put_u8(0);
                out.put_slice(urls.join("\n").as_bytes());
            }
            Call::Status => out.put_u8(1),
        }
        Ok(out.freeze())
    }

    fn decode_reply(mut body: Bytes) -> Result<Answer, CallError> {
        match body.try_get_u8().map_err(|_| codec("empty answer"))? {
            0 => Ok(Answer::Added),
            1 => {
                let mut n = [0u64; 7];
                for v in &mut n {
                    *v = body.try_get_u64().map_err(|_| codec("short status"))?;
                }
                let node = String::from_utf8(body.to_vec()).map_err(|_| codec("node id"))?;
                let counts = Counts { seen: n[0], queued: n[1], in_flight: n[2], finished: n[3], over_cap: n[4] };
                Ok(Answer::Status(Status { node, counts, logging: n[5], lost_links: n[6] }))
            }
            _ => Err(codec("unknown answer")),
        }
    }
}

impl ActorProtocol for DomainApi {
    type Mailbox = Msg;

    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        let mut out = BytesMut::new();
        match answer {
            Answer::Added => out.put_u8(0),
            Answer::Status(s) => {
                out.put_u8(1);
                let c = s.counts;
                for v in [c.seen, c.queued, c.in_flight, c.finished, c.over_cap, s.logging, s.lost_links] {
                    out.put_u64(v);
                }
                out.put_slice(s.node.as_bytes());
            }
        }
        Ok(out.freeze())
    }

    fn decode(mut body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        match body.try_get_u8() {
            Ok(0) => {
                let text = std::str::from_utf8(&body).map_err(|_| Rejection::InvalidMessage)?;
                Ok(Msg::Add(text.split('\n').filter(|u| !u.is_empty()).map(str::to_owned).collect(), reply))
            }
            Ok(1) => Ok(Msg::Status(reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

/// What every domain actor on a node shares.
pub struct Shared {
    pub node: String,
    pub site: SocketAddr,
    pub data: PathBuf,
    pub interval: Duration,
    pub per_domain: usize,
    pub max_urls: usize,
    pub fetch_timeout: Duration,
    /// Fetches in flight on this node, across its domains.
    pub permits: Arc<Semaphore>,
    /// The domains this node has linked to, so a hand-over asks for the owner once, not per page.
    peers: KeyedRefs<DomainApi>,
}

pub enum Msg {
    Add(Vec<String>, Option<Reply<Answer>>),
    Status(Option<Reply<Answer>>),
    Loaded(std::io::Result<store::Loaded>),
    Tick,
    Fetched(Pending, Outcome),
    Logged(std::io::Result<()>, Batch),
}

pub struct Outcome {
    transient: bool,
    same_domain: Vec<String>,
    lost_links: u64,
}

/// Records for one log write, and what to do once they are in.
#[derive(Default)]
pub struct Batch {
    records: Vec<Record>,
    discovered: Vec<String>,
    finished: u64,
    acks: Vec<Reply<Answer>>,
}

pub struct Domain {
    name: String,
    shared: Arc<Shared>,
    frontier: Frontier,
    file: Option<Arc<File>>,
    batch: Batch,
    writing: bool,
    armed: bool,
    lost_links: u64,
}

impl Domain {
    pub fn new(name: String, shared: Arc<Shared>) -> Self {
        let frontier = Frontier::new(shared.interval, shared.per_domain, shared.max_urls);
        Self {
            name,
            shared,
            frontier,
            file: None,
            batch: Batch::default(),
            writing: false,
            armed: false,
            lost_links: 0,
        }
    }

    fn status(&self) -> Status {
        Status {
            node: self.shared.node.clone(),
            counts: self.frontier.counts(),
            logging: (self.batch.records.len() + usize::from(self.writing)) as u64,
            lost_links: self.lost_links,
        }
    }

    /// New URLs join the next log write; `ack` is answered once everything this domain has seen is logged.
    fn add(&mut self, urls: Vec<String>, ack: Option<Reply<Answer>>) {
        let new = self.frontier.discover(urls);
        self.batch.records.extend(new.iter().cloned().map(Record::Discovered));
        self.batch.discovered.extend(new);
        self.batch.acks.extend(ack);
    }

    fn flush(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.writing {
            return Ok(());
        }
        let Some(file) = self.file.clone() else { return Ok(()) };
        if self.batch.records.is_empty() {
            for ack in self.batch.acks.drain(..) {
                let _ = ack.send(Answer::Added); // a caller that gave up asks again
            }
            return Ok(());
        }
        let batch = std::mem::take(&mut self.batch);
        match cx.submit_io(move || Msg::Logged(store::append(&file, &batch.records), batch)) {
            Ok(_) => self.writing = true,
            // Draining: the unanswered callers send again to the next owner, and unlogged ends are fetched again.
            Err(SubmitError { reason: TaskAdmissionError::Draining | TaskAdmissionError::Closed, .. }) => {}
            Err(e) => return Err(format!("log write refused: {e:?}")),
        }
        Ok(())
    }

    fn pump(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        loop {
            match self.frontier.next(cx.now()) {
                Next::Idle => return Ok(()),
                Next::At(at) => return self.arm(cx, at),
                Next::Fetch(pending) => {
                    let Ok(permit) = self.shared.permits.clone().try_acquire_owned() else {
                        // ponytail: no event says a node-wide permit came free; look again one interval on.
                        self.frontier.unstarted(pending);
                        return self.arm(cx, cx.now() + self.shared.interval.max(Duration::from_millis(5)));
                    };
                    let job = fetch(self.shared.clone(), self.name.clone(), pending.url.clone(), permit);
                    let back = pending.clone();
                    match cx.submit_future(async move { Msg::Fetched(pending, job.await) }) {
                        Ok(_) => {}
                        // Draining: the URL stays in the log unfinished, so the next owner fetches it.
                        Err(SubmitError {
                            reason: TaskAdmissionError::Draining | TaskAdmissionError::Closed, ..
                        }) => {
                            self.frontier.unstarted(back);
                            return Ok(());
                        }
                        Err(e) => return Err(format!("fetch refused: {e:?}")),
                    }
                }
            }
        }
    }

    fn arm(&mut self, cx: &mut Context<'_, Self>, at: Duration) -> Result<(), String> {
        if !self.armed {
            match cx.schedule_at(at, Msg::Tick) {
                Ok(_) => self.armed = true,
                Err(e) if e.reason == TimerReject::Draining || e.reason == TimerReject::Closed => {}
                Err(e) => return Err(format!("timer refused: {:?}", e.reason)),
            }
        }
        Ok(())
    }
}

impl Actor for Domain {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (dir, name) = (self.shared.data.clone(), self.name.clone());
        let load = cx.submit_io(move || Msg::Loaded(store::open(&dir, &name))).map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&load).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let loaded = loaded.map_err(|e| format!("log of {}: {e}", self.name))?;
                self.frontier.restore(loaded.discovered, &loaded.finished, cx.now());
                self.file = Some(Arc::new(loaded.file));
            }
            Msg::Add(urls, ack) => self.add(urls, ack),
            Msg::Status(reply) => drop(reply.map(|r| r.send(Answer::Status(self.status())))),
            Msg::Tick => self.armed = false,
            Msg::Fetched(pending, outcome) => {
                self.lost_links += outcome.lost_links;
                if self.frontier.returned(pending.clone(), outcome.transient) {
                    self.add(outcome.same_domain, None);
                    self.batch.records.push(Record::Finished(pending.url));
                    self.batch.finished += 1;
                }
            }
            Msg::Logged(written, batch) => {
                written.map_err(|e| format!("log of {}: {e}", self.name))?;
                self.writing = false;
                self.frontier.logged(batch.discovered);
                for _ in 0..batch.finished {
                    self.frontier.finished();
                }
                for ack in batch.acks {
                    let _ = ack.send(Answer::Added); // a caller that gave up asks again
                }
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.flush(cx)?;
        self.pump(cx)
    }
}

/// Fetches one page, then hands its links on other domains to their owners. Links on this domain come back
/// in the outcome.
async fn fetch(shared: Arc<Shared>, domain: String, url: String, permit: OwnedSemaphorePermit) -> Outcome {
    let fetched = http::get(shared.site, &url, shared.fetch_timeout).await;
    drop(permit);
    let links = match fetched {
        Fetch::Page(links) => links,
        Fetch::Gone(_) => return Outcome { transient: false, same_domain: Vec::new(), lost_links: 0 },
        Fetch::Transient(_) => return Outcome { transient: true, same_domain: Vec::new(), lost_links: 0 },
    };
    let mut by_domain: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for link in links {
        if let Some((host, _)) = http::split(&link) {
            by_domain.entry(host.to_owned()).or_default().push(link);
        }
    }
    let same_domain = by_domain.remove(&domain).unwrap_or_default();
    let mut lost_links = 0;
    for (host, urls) in by_domain {
        let n = urls.len() as u64;
        let peer = shared.peers.get(&host, Bytes::new());
        if add(&peer, urls, FORWARD_DEADLINE).await.is_err() {
            lost_links += n;
        }
    }
    Outcome { transient: false, same_domain, lost_links }
}

/// Hands `urls` to their domain's actor and waits until it has logged them. `Add` is idempotent, so every failure
/// but one that would repeat itself is worth another try, until `within` has passed.
pub async fn add(peer: &KeyedRef<DomainApi>, urls: Vec<String>, within: Duration) -> Result<(), ClusterError> {
    let deadline = tokio::time::Instant::now() + within;
    let mut pause = Duration::from_millis(10);
    loop {
        let err = match peer.request(Call::Add(urls.clone())).await {
            Ok(_) => return Ok(()),
            Err(e) => e,
        };
        if err.retry() == Retry::Never || tokio::time::Instant::now() + pause > deadline {
            eprintln!("hand-over to {peer:?} failed: {err}");
            return Err(err);
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(Duration::from_millis(500));
    }
}

pub struct NodeConfig {
    pub id: String,
    pub seeds: Vec<String>,
    pub site: SocketAddr,
    pub data: PathBuf,
    /// Requests per second to one domain.
    pub rate: f64,
    pub per_domain: usize,
    pub node_fetches: usize,
    /// Host domain actors. The control node that seeds a crawl and watches it does not.
    pub crawl: bool,
    /// Serve the axon console here, with the cluster's history in memory.
    pub console: Option<SocketAddr>,
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub console: Option<SocketAddr>,
}

pub fn transport(id: &str) -> axon_net::Config {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(SECRET).expect("secret is long enough"));
    transport.allow_plaintext = true; // loopback only
    transport
}

pub async fn start_node(cfg: NodeConfig) -> Result<Node, Box<dyn std::error::Error>> {
    let transport = transport(&cfg.id);
    let discovery = SeedDiscovery::start(SeedConfig::new(cfg.seeds.clone(), &transport)).await?;
    let mut ccfg = ClusterConfig::new("127.0.0.1:0".parse()?, transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    ccfg.heartbeat_interval = Duration::from_millis(500);
    ccfg.heartbeat_grace = Duration::from_secs(1);
    ccfg.labels.insert("role".into(), if cfg.crawl { "crawler" } else { "control" }.into());
    let system = ActorSystem::builder().timing_sample_every(64).build()?;
    let cluster = Cluster::start(&system, ccfg).await?;
    let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    Telemetry::start(&system, &cluster, telemetry).await?;
    let mut console = None;
    if let Some(bind) = cfg.console {
        let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
        MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
        let dashboard = Dashboard::start(&system, &cluster, DashboardConfig { bind, ..Default::default() }).await?;
        console = Some(dashboard.address());
    }
    if cfg.crawl {
        let shared = Arc::new(Shared {
            node: cfg.id.clone(),
            site: cfg.site,
            data: cfg.data,
            interval: Duration::from_secs_f64(1.0 / cfg.rate),
            per_domain: cfg.per_domain,
            max_urls: 100_000,
            fetch_timeout: Duration::from_secs(5),
            permits: Arc::new(Semaphore::new(cfg.node_fetches)),
            peers: cluster.keyed_refs(KIND, &Placement::default(), MAX_PEERS),
        });
        tokio::spawn(resume(cluster.downgrade(), shared.data.clone(), cfg.id.clone()));
        // URLs are heap data: weigh them, so the mailbox's byte bound holds what waits for a domain.
        let weight = |m: &Msg| 64 + if let Msg::Add(urls, _) = m { urls.iter().map(String::len).sum() } else { 0 };
        let build = move |cx: &SpawnContext, _args: Bytes| {
            Ok(Domain::new(cx.key.clone().ok_or("a domain actor needs a key")?, shared.clone()))
        };
        let limits = MailboxConfig::from(MailboxLimits::default()).with_weight(weight);
        cluster.kind::<DomainApi>(KIND).mailbox(limits).register(build).await?;
    }
    Ok(Node { system, cluster, console })
}

/// A key whose node crashed starts again only when something calls it, and a domain whose links all came from
/// itself never is. So when a node goes, every survivor starts the logged domains it owns now.
async fn resume(cluster: WeakCluster, data: PathBuf, me: String) {
    let Some(mut events) = cluster.upgrade().map(|c| c.subscribe()) else { return };
    while let Some(event) = events.next().await {
        let gone = |c: &MemberChange| {
            matches!(c, MemberChange::Left(_) | MemberChange::Unreachable(_) | MemberChange::Replaced { .. })
        };
        let lost = match &event {
            MembershipEvent::Changes { changes, .. } => changes.iter().any(gone),
            MembershipEvent::Resync(_) => true,
            _ => false,
        };
        let Some(cluster) = cluster.upgrade().filter(|_| lost) else { continue };
        let dir = data.clone();
        let domains = match tokio::task::spawn_blocking(move || store::domains(&dir)).await {
            Ok(Ok(domains)) => domains,
            other => {
                eprintln!("cannot list the logs to resume domains: {other:?}");
                continue;
            }
        };
        let anywhere = Placement::default();
        for domain in domains {
            if cluster.owner(KIND, &domain, &anywhere).is_some_and(|owner| *owner == *me)
                && let Err(e) = cluster.get_or_spawn::<Unaddressed>(KIND, &domain, Bytes::new(), &anywhere).await
            {
                eprintln!("cannot resume {domain}: {e}");
            }
        }
    }
}
