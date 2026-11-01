//! The cluster of the World stories (design §8): a seed `s`, homes `h1` (with the accounts), `h2` and `h3`, web nodes
//! `w1` and `w2`, and a document on each home. The World carries axon's network only, so its bots open no
//! websockets: each runs [`ClientCore`] on a web node and opens a [`Session`](mdpad::session) through channels, as the
//! websocket would. A bot's state lives outside the World, so after its web node crashes it joins through the other.
#![allow(dead_code, reason = "each story uses part of it")]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::Files;
use axon_cluster::{Retry, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use axon_net::SharedSecret;
use axon_testkit::world::{Host, Task, World, WorldConfig};
use futures_util::{sink, stream};
use mdpad::core::acl::Principal;
use mdpad::crdt;
use mdpad::follow::{ClientAct, ClientCore};
use mdpad::node::{NodeConfig, Roles};
use mdpad::remote::{CatalogAnswer, CatalogReq};
use mdpad::session::{In, Out, Reader, Writer};
use mdpad::wire::{DocAnswer, DocReq, ServerFrame};
use tokio::sync::mpsc;
use tokio::time::Instant;

pub const HOMES: [&str; 3] = ["h1", "h2", "h3"];
pub const WEBS: [&str; 2] = ["w1", "w2"];
const SEED_PORT: u16 = 7000;
/// The user every bot is: the documents' owner.
pub const USER: &str = "ann";

#[derive(Clone)]
pub struct Node {
    pub node: Option<Arc<mdpad::node::Node>>,
    _seed: Option<Arc<SeedServer>>,
}

impl Node {
    pub fn get(&self) -> &mdpad::node::Node {
        self.node.as_deref().expect("a member")
    }
}

pub async fn boot(host: Host) -> Node {
    let transport = host.transport(SharedSecret::new("mdpad-world-secret").unwrap());
    let seed = SocketAddr::new(host.ip_of("s"), SEED_PORT);
    if host.name() == "s" {
        let server = SeedServer::bind(SeedServerConfig::new(seed, &transport)).await.unwrap();
        return Node { node: None, _seed: Some(Arc::new(server)) };
    }
    let name = host.name().to_string();
    let home = HOMES.contains(&name.as_str()).then(|| name.clone());
    let roles = Roles { home: home.clone(), web: WEBS.contains(&name.as_str()), accounts: name == "h1" };
    let disk = host.disk();
    let dir = |d: &str| Arc::new(disk.dir(d)) as Arc<dyn Files>;
    let mut cfg = NodeConfig {
        id: name,
        homes: HOMES.map(str::to_string).to_vec(),
        docs: home.map(|_| dir("docs")),
        accounts: roles.accounts.then(|| dir("accounts")),
        http: None,
        workers: 1,
        // As `mdpad node`: two heartbeats after the seeds answer.
        settle: Duration::from_secs(1),
        roles,
        ..NodeConfig::with_key(vec![7; 32])
    };
    // Each write grows its file, so a full disk refuses the next (`disk_full`).
    cfg.docs_log.preallocate = false;
    let discovery = SeedDiscovery::start(SeedConfig::new([seed.to_string()], &transport)).await.unwrap();
    let ccfg = cfg.cluster(host.addr(), transport, discovery);
    let runtime = cfg.runtime(host.runtime());
    let node = mdpad::node::Node::boot(cfg, ccfg, runtime).await.expect("the node boots");
    Node { node: Some(Arc::new(node)), _seed: None }
}

/// A bot: one client of one document, and what it saw.
pub struct Bot {
    pub name: String,
    pub doc: String,
    pub editor: bool,
    pub core: ClientCore,
    pub tokens: Vec<String>,
    pub saved: BTreeSet<u64>,
    /// Tokens seen in the text, read each half second.
    pub seen: BTreeSet<String>,
    pub joins: u32,
    pub closes: BTreeMap<u16, u32>,
    /// The web node it joins through.
    pub web: String,
    /// It stops reading its socket until then: a client that does not read.
    pub stall_until: Option<Instant>,
    /// It leaves and stays away while set: nothing it holds reaches the server meanwhile.
    pub hold: bool,
}

pub type Shared = Arc<Mutex<Bot>>;

impl Bot {
    pub fn acked(&self) -> impl Iterator<Item = &String> {
        self.saved.iter().map(|mark| &self.tokens[*mark as usize])
    }

    pub fn text(&self) -> String {
        crdt::text(&self.core.doc)
    }

    pub fn notices(&self, kind: &str) -> u64 {
        self.core.notices.get(kind).copied().unwrap_or_default()
    }

    /// Acts on a server frame, and gives what to send back. `None` to join again.
    fn frame(&mut self, bytes: &[u8], retry: &mut Option<Instant>) -> Option<Vec<Vec<u8>>> {
        let frame = ServerFrame::decode(bytes).ok()?;
        let mut send = Vec::new();
        for act in self.core.frame(frame) {
            match act {
                ClientAct::Send(frame) => send.push(frame.encode()),
                ClientAct::Rejoin => return None,
                ClientAct::RetryAfter(ms) => *retry = Some(Instant::now() + Duration::from_millis(ms)),
                ClientAct::Saved(marks) => self.saved.extend(marks),
            }
        }
        Some(send)
    }
}

fn is_token(word: &str) -> bool {
    word.starts_with('b') && word.contains('k')
}

/// A session of the bot's document on `node`, through channels.
fn open(node: &Node, bot: &Shared) -> Option<(mpsc::Sender<In>, mpsc::Receiver<Out>)> {
    let (to_server, from_client) = mpsc::channel::<In>(256);
    let (to_client, from_server) = mpsc::channel::<Out>(64);
    let reader: Reader =
        Box::pin(stream::unfold(from_client, |mut rx| async move { rx.recv().await.map(|m| (m, rx)) }));
    let writer: Writer = Box::pin(sink::unfold(to_client, |tx, out: Out| async move {
        tx.send(out).await.map(|()| tx).map_err(|e| e.to_string())
    }));
    let doc = bot.lock().unwrap().doc.clone();
    let web = node.get().web();
    web.open_session(&doc, Principal::User(USER.into()), None, (reader, writer)).ok()?;
    Some((to_server, from_server))
}

async fn send(tx: &mpsc::Sender<In>, frames: Vec<Vec<u8>>) -> bool {
    for frame in frames {
        if tx.send(In::Frame(frame.into())).await.is_err() {
            return false;
        }
    }
    true
}

/// What ended a connection.
enum End {
    Rejoin,
    Deadline,
}

/// One connection: typing every `period` for an editor, until `deadline` or `done`.
async fn connection(node: &Node, bot: &Shared, period: Option<Duration>, deadline: Instant) -> End {
    let Some((tx, mut rx)) = open(node, bot) else { return End::Rejoin };
    let hello = {
        let mut b = bot.lock().unwrap();
        b.joins += 1;
        b.core.connect().encode()
    };
    if !send(&tx, vec![hello]).await {
        return End::Rejoin;
    }
    let mut retry: Option<Instant> = None;
    let (mut next_key, mut next_aware, mut next_scan) = (Instant::now(), Instant::now(), Instant::now());
    loop {
        if bot.lock().unwrap().hold {
            return End::Rejoin;
        }
        let stall = bot.lock().unwrap().stall_until.filter(|at| *at > Instant::now());
        // Typing and cursors wait for the welcome: until then they are not due.
        let typing = period.is_some() && bot.lock().unwrap().core.welcome.is_some();
        let (key_at, aware_at) = (typing.then_some(next_key), typing.then_some(next_aware));
        let wake = [Some(deadline), key_at, aware_at, Some(next_scan), retry, stall]
            .into_iter()
            .flatten()
            .min()
            .expect("a deadline");
        let out = if stall.is_some() {
            tokio::time::sleep_until(wake).await;
            None
        } else {
            tokio::select! {
                out = rx.recv() => Some(out),
                () = tokio::time::sleep_until(wake) => None,
            }
        };
        let now = Instant::now();
        let mut frames = Vec::new();
        match out {
            Some(Some(Out::Frame(bytes))) => match bot.lock().unwrap().frame(&bytes, &mut retry) {
                Some(more) => frames = more,
                None => return End::Rejoin,
            },
            Some(Some(Out::Close(code))) => {
                *bot.lock().unwrap().closes.entry(code).or_default() += 1;
                return End::Rejoin;
            }
            Some(None) => return End::Rejoin,
            None => {}
        }
        if now >= deadline {
            return End::Deadline;
        }
        {
            let mut b = bot.lock().unwrap();
            if retry.is_some_and(|at| at <= now) {
                retry = None;
                frames.extend(b.core.retry().map(|f| f.encode()));
            }
            if let Some(period) = period
                && now >= next_key
                && b.core.welcome.is_some()
            {
                let mark = b.tokens.len() as u64;
                let token = format!("{}k{mark}", b.name);
                let end = crdt::len(&b.core.doc);
                let update = crdt::insert(&b.core.doc, end, &format!(" {token} "));
                b.tokens.push(token);
                frames.extend(b.core.local(update, mark).map(|f| f.encode()));
                next_key = now + period;
            }
            if period.is_some() && now >= next_aware && b.core.welcome.is_some() {
                let cursor = serde_json::json!({ "cursor": { "anchor": crdt::len(&b.core.doc) } });
                frames.push(b.core.aware(Some(cursor)).encode());
                next_aware = now + Duration::from_secs(1);
            }
            if now >= next_scan {
                let text = b.text();
                b.seen.extend(text.split_whitespace().filter(|w| is_token(w)).map(str::to_string));
                next_scan = now + Duration::from_millis(500);
            }
        }
        if !send(&tx, frames).await {
            return End::Rejoin;
        }
    }
}

/// Runs the bot on `node` for `d`, joining again after each end.
pub async fn drive(node: Node, bot: Shared, period: Option<Duration>, d: Duration) {
    let deadline = Instant::now() + d;
    while Instant::now() < deadline {
        if bot.lock().unwrap().hold {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if let End::Deadline = connection(&node, &bot, period, deadline).await {
            break;
        }
        bot.lock().unwrap().core.disconnected();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bot.lock().unwrap().core.disconnected();
}

/// Joins until nothing is unsaved and the state is the server's, then leaves cleanly. `false` past `within`.
pub async fn settle(node: Node, bot: Shared, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    let synced = |b: &Bot| {
        b.core.welcome.is_some() && b.core.position.is_some() && b.core.unsaved() == 0 && b.core.is_connected()
    };
    while Instant::now() < deadline {
        let Some((tx, mut rx)) = open(&node, &bot) else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        bot.lock().unwrap().core.disconnected();
        let mut retry = None;
        let hello = bot.lock().unwrap().core.connect().encode();
        if !send(&tx, vec![hello]).await {
            continue;
        }
        loop {
            if synced(&bot.lock().unwrap()) {
                // What is live a little longer, then a clean leave.
                let until = Instant::now() + Duration::from_millis(500);
                while let Ok(Some(Out::Frame(bytes))) = tokio::time::timeout_at(until, rx.recv()).await {
                    let more = bot.lock().unwrap().frame(&bytes, &mut retry).unwrap_or_default();
                    send(&tx, more).await;
                }
                let leave = bot.lock().unwrap().core.aware(None).encode();
                send(&tx, vec![leave]).await;
                bot.lock().unwrap().core.disconnected();
                return true;
            }
            let wake = retry.unwrap_or(deadline).min(deadline);
            match tokio::time::timeout_at(wake, rx.recv()).await {
                Ok(Some(Out::Frame(bytes))) => {
                    let more = bot.lock().unwrap().frame(&bytes, &mut retry);
                    let Some(more) = more else { break };
                    if !send(&tx, more).await {
                        break;
                    }
                }
                Ok(_) => break,
                Err(_) if Instant::now() >= deadline => return false,
                Err(_) => {
                    retry = None;
                    let again = bot.lock().unwrap().core.retry().map(|f| f.encode());
                    send(&tx, again.into_iter().collect()).await;
                }
            }
        }
        bot.lock().unwrap().core.disconnected();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// The World of a story: booted, with a document on each home and four bots of each (three editors, a viewer),
/// spread over the web nodes.
pub struct Story {
    pub w: World<Node>,
    pub docs: Vec<String>,
    pub bots: Vec<Shared>,
    tasks: Vec<Task<()>>,
}

pub const PERIOD: Duration = Duration::from_millis(200);

impl Story {
    pub fn new(seed: u64) -> Self {
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        for name in ["s", "h1", "h2", "h3", "w1", "w2"] {
            w.add(name, boot);
        }
        w.run_until("every node booted and sees the others", Duration::from_secs(60), |w| {
            w.nodes().count() == 6
                && w.nodes().all(|(_, n)| n.node.as_ref().is_none_or(|n| n.cluster.view().members.len() == 5))
        });
        let docs = w.block_on("w1", create_docs).expect("w1 is up");
        let mut bots = Vec::new();
        for doc in &docs {
            for b in 0..4 {
                let name = format!("b{}{b}", bots.len() / 4);
                bots.push(Arc::new(Mutex::new(Bot {
                    name,
                    doc: doc.clone(),
                    editor: b < 3,
                    core: ClientCore::new(w.pick(1..1 << 30) * 2),
                    tokens: Vec::new(),
                    saved: BTreeSet::new(),
                    seen: BTreeSet::new(),
                    joins: 0,
                    closes: BTreeMap::new(),
                    web: WEBS[b % 2].to_string(),
                    stall_until: None,
                    hold: false,
                })));
            }
        }
        Self { w, docs, bots, tasks: Vec::new() }
    }

    /// Starts every bot on its web node for `d`.
    pub fn run_bots(&mut self, d: Duration) {
        for bot in self.bots.clone() {
            self.run_bot(bot, d);
        }
    }

    pub fn run_bot(&mut self, bot: Shared, d: Duration) {
        let (web, editor) = {
            let b = bot.lock().unwrap();
            (b.web.clone(), b.editor)
        };
        let task = self.w.spawn_on(&web, move |n| drive(n, bot, editor.then_some(PERIOD), d));
        self.tasks.push(task);
    }

    /// Waits for the bots' runs to end; one whose web node crashed ended with it.
    pub fn join_bots(&mut self) {
        for task in std::mem::take(&mut self.tasks) {
            let _ = self.w.join(task);
        }
    }

    pub fn bots_on(&self, web: &str) -> Vec<Shared> {
        self.bots.iter().filter(|b| b.lock().unwrap().web == web).cloned().collect()
    }

    /// Every bot settles through its web node, or the other when that one is down.
    pub fn settle(&mut self) {
        let up: Vec<String> = self.w.nodes().map(|(n, _)| n.to_string()).collect();
        let mut tasks = Vec::new();
        for bot in &self.bots {
            let web = bot.lock().unwrap().web.clone();
            let web = if up.contains(&web) {
                web
            } else {
                WEBS.iter().find(|w| up.contains(&w.to_string())).unwrap().to_string()
            };
            let bot = bot.clone();
            tasks.push((web.clone(), self.w.spawn_on(&web, move |n| settle(n, bot, Duration::from_secs(60)))));
        }
        for (web, task) in tasks {
            let settled = self.w.join(task).unwrap_or_else(|_| panic!("{web} crashed while settling"));
            assert!(settled, "seed: a bot did not settle through {web}\n{}", self.w.trace_tail(40));
        }
    }

    /// Holds the bots away from the server, or lets them join again.
    pub fn hold(&self, bots: &[Shared], hold: bool) {
        bots.iter().for_each(|b| b.lock().unwrap().hold = hold);
    }

    /// The tokens the bots were told are saved, and those they saw from others: each must be in the text a reload
    /// gives, before any client sends it again.
    pub fn kept(&self, bots: &[Shared]) -> BTreeSet<String> {
        let mut kept = BTreeSet::new();
        for bot in bots {
            let b = bot.lock().unwrap();
            let own = format!("{}k", b.name);
            kept.extend(b.acked().cloned());
            kept.extend(b.seen.iter().filter(|t| !t.starts_with(&own)).cloned());
        }
        kept
    }

    /// The server's text of one document, asked until its home answers.
    pub fn text(&mut self, id: &str) -> String {
        let id = id.to_string();
        self.w
            .block_on("w2", move |n| async move {
                let cluster = n.get().cluster.clone();
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let ask = DocReq::Text { who: Principal::User(USER.into()) };
                    match mdpad::doc::keyed(&cluster, &id).request_within(ask, Duration::from_secs(2)).await {
                        Ok(DocAnswer::Text(text)) => return text,
                        other => assert!(Instant::now() < deadline, "the text of {id}: {other:?}"),
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .expect("w2 is up")
    }

    /// One call to a document through `w1`, asked again only where it did not run: refused unsent, or busy.
    pub fn ask(&mut self, id: &str, req: DocReq) -> DocAnswer {
        let id = id.to_string();
        self.w
            .block_on("w1", move |n| async move {
                let doc = mdpad::doc::keyed(&n.get().cluster, &id);
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    assert!(Instant::now() < deadline, "{req:?} to {id}: no answer");
                    match doc.request_within(req.clone(), Duration::from_secs(10)).await {
                        Ok(DocAnswer::Busy { .. }) => {}
                        Ok(answer) => return answer,
                        Err(e) if matches!(e.retry(), Retry::Later | Retry::Resolve) => {}
                        Err(e) => panic!("{req:?} to {id}: {e:?}"),
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .expect("w1 is up")
    }

    /// The server's text of each document.
    pub fn texts(&mut self) -> BTreeMap<String, String> {
        self.docs.clone().into_iter().map(|id| (id.clone(), self.text(&id))).collect()
    }

    /// After `settle`: every client holds the server's text, which holds every token acked and every token seen
    /// (I1, I2, I3).
    pub fn check(&mut self) {
        let texts = self.texts();
        for bot in &self.bots {
            let b = bot.lock().unwrap();
            let text = &texts[&b.doc];
            let held: BTreeSet<&str> = text.split_whitespace().collect();
            for token in b.acked() {
                assert!(held.contains(token.as_str()), "{} acked {token}, lost from {}", b.name, b.doc);
            }
            for token in &b.seen {
                assert!(held.contains(token.as_str()), "{} saw {token}, lost from {}", b.name, b.doc);
            }
            assert_eq!(&b.text(), text, "{} differs from the server", b.name);
        }
        let (mut typed, mut acked, mut notices) = (0, 0, BTreeMap::<String, u64>::new());
        for bot in &self.bots {
            let b = bot.lock().unwrap();
            (typed, acked) = (typed + b.tokens.len(), acked + b.saved.len());
            for (kind, n) in &b.core.notices {
                *notices.entry(kind.clone()).or_default() += n;
            }
        }
        assert!(acked > 0, "nothing was acked");
        println!("{acked} of {typed} tokens acked, notices {notices:?}");
    }

    /// The presence a bot sees: client ids with a state.
    pub fn shown(&self, bot: &Shared) -> BTreeSet<u64> {
        bot.lock().unwrap().core.presence.keys().copied().collect()
    }

    pub fn client(&self, bot: &Shared) -> u64 {
        bot.lock().unwrap().core.client_id
    }

    pub fn home_bots(&self, home: &str) -> Vec<Shared> {
        self.bots.iter().filter(|b| mdpad::doc::home_of(&b.lock().unwrap().doc) == home).cloned().collect()
    }
}

/// A document on each home, owned by `USER`. Tries again while the cluster warms.
async fn create_docs(n: Node) -> Vec<String> {
    let node = n.get();
    let web = node.web();
    let mut docs = Vec::new();
    for home in HOMES {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(Instant::now() < deadline, "no document on {home}");
            let made = async {
                let create = CatalogReq::Create { owner: USER.into() };
                let Ok(CatalogAnswer::Created { id }) =
                    web.catalogs[home].request_within(create, Duration::from_secs(2)).await
                else {
                    return None;
                };
                let create = DocReq::Create { owner: USER.into(), markdown: format!("# {home}\n") };
                let doc = mdpad::doc::keyed(&node.cluster, &id);
                for _ in 0..50 {
                    if let Ok(DocAnswer::Done) = doc.request_within(create.clone(), Duration::from_secs(2)).await {
                        return Some(id);
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                None
            };
            if let Some(id) = made.await {
                docs.push(id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    docs
}
