//! The engine on a real in-process cluster: three engine nodes that are also the Paxos acceptors, and a client node.

use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, PaxosConfig, PaxosLeases};
use axon_net::SharedSecret;
use axon_stream::{StreamEvent, StreamNode, StreamNodeConfig, SubscribeOptions};
use axon_testkit::{eventually, eventually_within};
use matching_engine::book::{Book, Command, Flow, Outcome, Reason};
use matching_engine::client::{self, Refused};
use matching_engine::engine::{self, EngineConfig, EngineStats};
use matching_engine::md::{self, MarketDataView};
use matching_engine::wire::{Answer, MarketData};
use matching_engine::{journal, replay};
use tokio::task::JoinSet;

const LEASE: Duration = Duration::from_secs(1);
const ENGINES: [&str; 3] = ["e-0", "e-1", "e-2"];
const HOT: &str = "HOT";

struct Node {
    system: ActorSystem,
    cluster: Cluster,
    streams: StreamNode,
    stats: Option<Arc<EngineStats>>,
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("matching-engine-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn node(id: &str, discovery: &MemoryDiscovery, engine: Option<EngineConfig>) -> Node {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("matching-engine-test-secret").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let role = if engine.is_some() { "engine" } else { "client" };
    cfg.labels.insert("role".into(), role.into());
    let mut paxos = PaxosConfig::new(ENGINES);
    // The test's own lease, so an engine's keys move within its wait.
    paxos.lease = LEASE;
    // An acceptor's log of who holds each key; in memory, so tests that run at once share no directory.
    paxos.store = Some(Arc::new(axon::files::MemoryFiles::new()));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = if engine.is_some() { vec![] } else { vec![[("role".into(), "engine".into())].into()] };
    let streams = StreamNode::start(&cluster, StreamNodeConfig::new(upstream)).await.unwrap();
    let stats = match engine {
        Some(cfg) => Some(engine::serve(&cluster, streams.clone(), cfg).await.unwrap()),
        None => None,
    };
    Node { system, cluster, streams, stats }
}

async fn world(cfg: EngineConfig) -> (MemoryDiscovery, Vec<Node>, Node) {
    let discovery = MemoryDiscovery::new();
    let mut engines = Vec::new();
    for id in ENGINES {
        engines.push(node(id, &discovery, Some(cfg.clone())).await);
    }
    let client = node("client", &discovery, None).await;
    client.cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind("book")).count() == 3).await.unwrap();
    (discovery, engines, client)
}

/// Waits out the acceptors' warm-up after they all started: a lease and a bit.
async fn first_answer(
    book: &axon_cluster::KeyedRef<matching_engine::wire::BookApi>,
) -> matching_engine::wire::Snapshot {
    client::snapshot(book, 0, LEASE * 10).await.expect("the book never started")
}

#[derive(Default, Debug)]
struct Seen {
    readies: u32,
    trades: u64,
    levels: u64,
    last_seq: u64,
    events: Vec<String>,
}

/// Counts what arrives on `md/<symbol>` at the client node.
fn follow(streams: &StreamNode, symbol: &str) -> Arc<Mutex<Seen>> {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut subscription = streams.subscribe(&engine::market_data(symbol), SubscribeOptions::default()).unwrap();
    let counts = seen.clone();
    tokio::spawn(async move {
        while let Some(event) = subscription.next().await {
            let mut seen = counts.lock().unwrap();
            let line = match &event {
                StreamEvent::Batch(b) => format!("batch g{} {}..{}", b.generation(), b.first(), b.last()),
                other => format!("{other:?}"),
            };
            if !line.starts_with("batch") || seen.events.len() < 6 {
                seen.events.push(line);
            } else if let Some(last) = seen.events.last_mut() {
                *last = line;
            }
            match event {
                StreamEvent::Ready { .. } => seen.readies += 1,
                StreamEvent::Batch(batch) => {
                    for record in batch.records() {
                        match serde_json::from_slice(record).unwrap() {
                            MarketData::Trade { seq, .. } => {
                                seen.trades += 1;
                                seen.last_seq = seq;
                            }
                            MarketData::Level { seq, .. } => {
                                seen.levels += 1;
                                seen.last_seq = seq;
                            }
                        }
                    }
                }
                other => assert!(!other.is_terminal(), "the market data ended: {other:?}"),
            }
        }
    });
    seen
}

async fn down(nodes: impl IntoIterator<Item = Node>) {
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

/// The actor answers exactly what the pure book answers for the same seeded flow, and the journal replays to it.
#[tokio::test(flavor = "multi_thread")]
async fn the_actor_matches_as_the_pure_book_does_and_publishes_every_trade() {
    let dir = scratch("same");
    let cfg = EngineConfig::new(&dir);
    let (_discovery, engines, client) = world(cfg.clone()).await;
    let book = client::book(&client.cluster, HOT);
    first_answer(&book).await;
    let seen = follow(&client.streams, HOT);
    eventually("subscribed", async || seen.lock().unwrap().readies > 0).await;

    let (mut flow, mut pure, mut levels) = (Flow::new(42, 1), Book::new(cfg.max_resting, cfg.max_seen), Vec::new());
    let mut fills = 0;
    for _ in 0..2000 {
        let command = flow.next_command();
        let expected = pure.apply(command, &mut levels);
        if let Outcome::Accepted { fills: f, .. } = &expected {
            fills += f.len() as u64;
        }
        match client::submit(&book, command).await.unwrap() {
            Answer::Done { outcome, .. } => assert_eq!(outcome, expected, "{command:?}"),
            other => panic!("{other:?}"),
        }
    }
    let snapshot = client::snapshot(&book, 5, client.cluster.request_timeout()).await.unwrap();
    assert_eq!(snapshot.digest, pure.digest());
    let (replayed, accepted) = replay(&journal::read_all(&dir, HOT).unwrap(), cfg.max_resting, cfg.max_seen);
    assert_eq!((replayed.digest(), accepted), (snapshot.digest, snapshot.seq));
    eventually("every trade is on the stream", async || seen.lock().unwrap().trades == fills).await;
    assert_eq!(seen.lock().unwrap().last_seq, snapshot.seq);
    down(engines.into_iter().chain([client])).await;
}

/// A book that cannot keep up refuses at its mailbox. Every order gets an answer or a refusal, none is lost,
/// and the refused ones never reached the book.
#[tokio::test(flavor = "multi_thread")]
async fn a_book_that_cannot_keep_up_refuses_orders_instead_of_queueing_them() {
    let dir = scratch("busy");
    let cfg = EngineConfig { mailbox: 16, max_unwritten: 32, ..EngineConfig::new(&dir) };
    let (_discovery, engines, client) = world(cfg.clone()).await;
    let book = client::book(&client.cluster, HOT);
    first_answer(&book).await;

    let mut burst = JoinSet::new();
    for id in 1..=3000u64 {
        let book = book.clone();
        let side = if id % 2 == 0 { matching_engine::book::Side::Buy } else { matching_engine::book::Side::Sell };
        let command = Command::Limit { id, side, price: 1000 + id % 7, qty: 1 };
        burst.spawn(async move { (id, client::submit(&book, command).await) });
    }
    let results = tokio::time::timeout(Duration::from_secs(30), burst.join_all()).await.expect("nothing hangs");
    let accepted: BTreeSet<u64> = results
        .iter()
        .filter_map(|(id, r)| matches!(r, Ok(Answer::Done { outcome: Outcome::Accepted { .. }, .. })).then_some(*id))
        .collect();
    let busy = results.iter().filter(|(_, r)| *r == Err(Refused::Busy)).count();
    let other: Vec<_> = results.iter().filter(|(id, r)| !accepted.contains(id) && *r != Err(Refused::Busy)).collect();
    assert!(other.is_empty(), "neither answered nor refused: {:?}", &other[..other.len().min(5)]);
    assert!(busy > 0 && !accepted.is_empty(), "{busy} busy, {} accepted", accepted.len());

    let snapshot = client::snapshot(&book, 0, client.cluster.request_timeout()).await.unwrap();
    let journal = journal::read_all(&dir, HOT).unwrap();
    let journaled: BTreeSet<u64> = journal
        .iter()
        .filter_map(|c| match c {
            Command::Limit { id, .. } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(journaled, accepted, "the book holds exactly what it acked");
    assert_eq!(snapshot.seq, accepted.len() as u64);
    let stats = engines
        .iter()
        .filter_map(|e| e.stats.as_ref())
        .map(|s| s.commands.load(std::sync::atomic::Ordering::Relaxed))
        .sum::<u64>();
    assert_eq!(stats, accepted.len() as u64, "a refused order never reached a book");
    println!("{busy} of 3000 refused busy, {} accepted", accepted.len());
    down(engines.into_iter().chain([client])).await;
}

/// The owner of a hot book crashes under load. The book comes back on another node, rebuilt from its journal:
/// every order acked before the crash is in it, orders in flight are resent under their ids until known, and
/// the market data follows the new owner.
#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_owner_book_comes_back_whole_on_another_node() {
    let dir = scratch("crash");
    let cfg = EngineConfig::new(&dir);
    let (_discovery, mut engines, client) = world(cfg.clone()).await;
    let book = client::book(&client.cluster, HOT);
    let before = first_answer(&book).await;
    let view = client.system.spawn(MarketDataView::new(HOT, client.streams.clone(), book.clone())).start().unwrap();

    let acked = Arc::new(Mutex::new(HashSet::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut traders = JoinSet::new();
    for trader in 0..8u64 {
        let (book, acked, stop) = (book.clone(), acked.clone(), stop.clone());
        traders.spawn(async move {
            let mut flow = Flow::new(trader, trader << 40 | 1);
            let mut resent = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let command = flow.next_command();
                let (answer, tries) = client::submit_known(&book, command, Duration::from_secs(20)).await;
                resent += tries - 1;
                match answer {
                    Ok(Answer::Done { outcome: Outcome::Accepted { .. } | Outcome::Cancelled { .. }, .. }) => {
                        acked.lock().unwrap().insert(command);
                    }
                    // Accepted by the owner that crashed before it could answer.
                    Ok(Answer::Done { outcome: Outcome::Rejected(Reason::Duplicate), .. }) if tries > 1 => {
                        acked.lock().unwrap().insert(command);
                    }
                    Ok(_) => {}
                    Err(Refused::Busy) => {}
                    Err(e) => panic!("{command:?} never answered: {e:?}"),
                }
            }
            resent
        });
    }
    eventually("orders flow", async || acked.lock().unwrap().len() > 500).await;
    let owner = engines.iter().position(|e| e.cluster.identity().node_id == before.node).unwrap();
    let acked_before_crash = acked.lock().unwrap().clone();
    let crashed = Instant::now();
    drop(engines.remove(owner));

    let moved = acked.lock().unwrap().len();
    eventually_within("orders flow again", Duration::from_secs(20), async || acked.lock().unwrap().len() > moved + 500)
        .await;
    let back_after = crashed.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let resent: u32 = traders.join_all().await.iter().sum();

    let after = client::snapshot(&book, 0, client.cluster.request_timeout()).await.unwrap();
    assert_ne!(after.node, before.node);
    assert!(after.epoch > before.epoch);
    let journal = journal::read_all(&dir, HOT).unwrap();
    let (replayed, accepted) = replay(&journal, cfg.max_resting, cfg.max_seen);
    assert_eq!((replayed.digest(), accepted), (after.digest, after.seq));
    let journaled: HashSet<_> = journal.iter().collect();
    let acked = acked.lock().unwrap().clone();
    assert!(acked.iter().all(|c| journaled.contains(c)), "an acked order was lost");
    assert!(acked_before_crash.iter().all(|c| journaled.contains(c)));
    let full = client::snapshot(&book, usize::MAX, client.cluster.request_timeout()).await.unwrap();
    let mut seen = None;
    eventually_within(&format!("the market data follows {}", after.node), Duration::from_secs(30), async || {
        let now = view.ask(md::Msg::Get).await.unwrap();
        let followed = now.synced && now.seq == full.seq;
        seen = Some(now);
        followed
    })
    .await;
    let seen = seen.expect("asked at least once");
    assert_eq!((seen.bids, seen.asks), (full.bids, full.asks), "the market-data view is the book");
    assert!(seen.snapshots >= 2, "one snapshot per owner: {}", seen.snapshots);
    println!(
        "book back on {} after {back_after:?}; {resent} resends; {} acked orders all journaled",
        after.node,
        acked.len()
    );
    down(engines.into_iter().chain([client])).await;
}
