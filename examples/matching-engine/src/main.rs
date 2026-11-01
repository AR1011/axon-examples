//! `matching-engine seed|engine|load --plaintext key=value..`: the processes of a local cluster. See the README.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig,
};
use axon_net::SharedSecret;
use axon_stream::{StreamNode, StreamNodeConfig};
use matching_engine::book::{Command, Flow, Outcome, Reason};
use matching_engine::client::{self, Refused};
use matching_engine::engine::{self, EngineConfig};
use matching_engine::md::{self, MarketDataView};
use matching_engine::wire::Answer;
use matching_engine::{Samples, journal, percentiles, replay};
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
    fn list(&self, key: &str) -> Result<Vec<String>, Error> {
        Ok(self.get(key)?.split(',').filter(|s| !s.is_empty()).map(String::from).collect())
    }
}

fn transport(id: &str) -> Result<axon_net::Config, Error> {
    let secret = std::env::var("ME_SECRET").unwrap_or_else(|_| "matching-engine-dev-secret".into());
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

async fn node(args: &Args, role: &str, acceptors: Option<Vec<String>>) -> Result<(ActorSystem, Cluster), Error> {
    let id = args.get("id")?;
    let transport = transport(id)?;
    let discovery = SeedDiscovery::start(SeedConfig::new(args.list("seeds")?, &transport)).await?;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse()?, transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    cfg.labels.insert("role".into(), role.into());
    if let Some(acceptors) = acceptors {
        let mut paxos = PaxosConfig::new(acceptors);
        // An acceptor logs who holds each key here; a node that is not one never reads it.
        paxos.store =
            Some(Arc::new(axon::files::OsFiles::new(PathBuf::from(args.get("dir")?).join("leases").join(id))));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos)?));
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

async fn engine(args: Args) -> Result<(), Error> {
    let (system, cluster) = node(&args, "engine", Some(args.list("acceptors")?)).await?;
    let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await?;
    let mut cfg = EngineConfig::new(PathBuf::from(args.get("dir")?));
    cfg.mailbox = args.or("mailbox", "1024").parse()?;
    let stats = engine::serve(&cluster, streams, cfg).await?;
    let id = cluster.identity().node_id.clone();
    println!("{}", json!({ "engine": id, "pid": std::process::id(), "addr": cluster.local_addr().to_string() }));
    let mut every = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = terminated() => break,
            _ = every.tick() => {
                let s = cluster.stats();
                println!("{}", json!({ "engine": id, "spawned": s.spawned, "commands": stats.commands.load(Relaxed),
                    "writes": stats.writes.load(Relaxed), "pauses": stats.pauses.load(Relaxed), "md_refused": stats.md_refused.load(Relaxed) }));
            }
        }
    }
    system.shutdown().await?;
    Ok(())
}

#[derive(Default)]
struct Counts {
    accepted: AtomicU64,
    /// Refused by the book's rules: an unknown order to cancel, a full book, an invalid order.
    rejected: AtomicU64,
    book_full: AtomicU64,
    busy: AtomicU64,
    resent: AtomicU64,
    /// Resent after a lost answer and found accepted: `Duplicate` on the resend.
    found: AtomicU64,
    unresolved: AtomicU64,
}

/// Closed-loop traders: `window` on the hot symbol, one on each other. Prints a line a second, then checks every
/// book against its journal.
async fn load(args: Args) -> Result<(), Error> {
    let (system, cluster) = node(&args, "client", None).await?;
    let symbols = args.list("symbols")?;
    let hot = args.or("hot", &symbols[0]).to_string();
    let secs: u64 = args.or("secs", "30").parse()?;
    let window: u64 = args.or("window", "32").parse()?;
    let dir = PathBuf::from(args.get("dir")?);
    let tiers = vec![[("role".to_string(), "engine".to_string())].into()];
    let streams = StreamNode::start(&cluster, StreamNodeConfig::new(tiers)).await?;
    tokio::time::timeout(
        Duration::from_secs(30),
        cluster.wait_view(|v| v.members.iter().any(|m| m.record.has_kind(matching_engine::wire::KIND))),
    )
    .await??;

    let counts = Arc::new(Counts::default());
    let stop = Arc::new(AtomicBool::new(false));
    let acked: Arc<Mutex<HashMap<String, HashSet<Command>>>> = Arc::default();
    let latencies = Arc::new(Mutex::new(Samples::new(1 << 20)));
    let books: HashMap<_, _> = symbols.iter().map(|s| (s.clone(), client::book(&cluster, s))).collect();
    // Wait out the acceptors' warm-up: the first snapshot of each book is its first start.
    for (symbol, book) in &books {
        client::snapshot(book, 0, Duration::from_secs(30))
            .await
            .map_err(|e| format!("{symbol} never started: {e:?}"))?;
    }
    let view = system.spawn(MarketDataView::new(&hot, streams.clone(), books[&hot].clone())).start()?;

    let accepted_by: BTreeMap<String, Arc<AtomicU64>> = symbols.iter().map(|s| (s.clone(), Arc::default())).collect();
    // When HOT last accepted an order, and the longest it went without.
    let hot_last = Arc::new(Mutex::new((Instant::now(), Duration::ZERO)));
    let mut traders = tokio::task::JoinSet::new();
    for (s, symbol) in symbols.iter().enumerate() {
        for t in 0..if *symbol == hot { window } else { 1 } {
            let (book, symbol) = (books[symbol].clone(), symbol.clone());
            let (counts, stop, acked, latencies) = (counts.clone(), stop.clone(), acked.clone(), latencies.clone());
            let (per_symbol, hot_last, hot_symbol) = (accepted_by[&symbol].clone(), hot_last.clone(), hot.clone());
            let trader = (s as u64) << 16 | t;
            traders.spawn(async move {
                let mut flow = Flow::new(trader, trader << 32 | 1);
                while !stop.load(Relaxed) {
                    let command = flow.next_command();
                    let sent = Instant::now();
                    let (answer, tries) = client::submit_known(&book, command, Duration::from_secs(20)).await;
                    counts.resent.fetch_add(u64::from(tries - 1), Relaxed);
                    let known = match answer {
                        Ok(Answer::Done { outcome: Outcome::Rejected(Reason::Duplicate), .. }) if tries > 1 => {
                            counts.found.fetch_add(1, Relaxed);
                            true
                        }
                        Ok(Answer::Done { outcome: Outcome::Rejected(reason), .. }) => {
                            counts.rejected.fetch_add(1, Relaxed);
                            if reason == Reason::BookFull {
                                counts.book_full.fetch_add(1, Relaxed);
                            }
                            false
                        }
                        Ok(_) => {
                            counts.accepted.fetch_add(1, Relaxed);
                            per_symbol.fetch_add(1, Relaxed);
                            if symbol == hot_symbol {
                                let mut last = hot_last.lock().expect("hot_last");
                                let gap = last.0.elapsed();
                                if gap > last.1 {
                                    last.1 = gap;
                                }
                                last.0 = Instant::now();
                            }
                            if tries == 1 {
                                let micros = sent.elapsed().as_micros().min(u32::MAX as u128) as u32;
                                latencies.lock().expect("latencies").add(micros);
                            }
                            true
                        }
                        Err(Refused::Busy) => {
                            counts.busy.fetch_add(1, Relaxed);
                            false
                        }
                        Err(_) => {
                            counts.unresolved.fetch_add(1, Relaxed);
                            false
                        }
                    };
                    if known {
                        // ponytail: every acked command is kept for the final check; a digest per symbol for long runs.
                        acked.lock().expect("acked").entry(symbol.clone()).or_default().insert(command);
                    }
                }
            });
        }
    }

    let started = Instant::now();
    let mut last = (0, 0);
    for second in 1..=secs {
        tokio::time::sleep_until((started + Duration::from_secs(second)).into()).await;
        // Bounded, so a book that is moving shows as a second without an answer rather than a late line.
        let owner = match client::snapshot(&books[&hot], 0, Duration::from_millis(500)).await {
            Ok(s) => json!({ "node": s.node, "epoch": s.epoch, "seq": s.seq }),
            Err(e) => json!(format!("{e:?}")),
        };
        let by: BTreeMap<_, _> = accepted_by.iter().map(|(s, n)| (s.clone(), n.swap(0, Relaxed))).collect();
        let md =
            view.ask(md::Msg::Get).await.map(|v| json!({ "seq": v.seq, "snapshots": v.snapshots, "synced": v.synced }));
        let (accepted, busy) = (counts.accepted.load(Relaxed), counts.busy.load(Relaxed));
        println!(
            "{}",
            json!({ "t": second, "accepted": accepted - last.0, "by": by, "busy": busy - last.1, "resent": counts.resent.load(Relaxed),
                "found": counts.found.load(Relaxed), "unresolved": counts.unresolved.load(Relaxed), "hot": owner,
                "md": md.unwrap_or_default() })
        );
        last = (accepted, busy);
    }
    stop.store(true, Relaxed);
    traders.join_all().await;

    let mut samples =
        Arc::try_unwrap(latencies).map_err(|_| "traders still hold the samples")?.into_inner()?.into_vec();
    let (p50, p99, max) = percentiles(&mut samples);
    let mut verified = true;
    let mut books_out = Vec::new();
    for (symbol, book) in &books {
        let snapshot = client::snapshot(book, usize::MAX, cluster.request_timeout())
            .await
            .map_err(|e| format!("{symbol}: {e:?}"))?;
        let journal = journal::read_all(&dir, symbol)?;
        let (replayed, accepted) = replay(&journal, 100_000, 100_000);
        let journaled: HashSet<_> = journal.iter().collect();
        let lost =
            acked.lock().expect("acked").get(symbol).map_or(0, |a| a.iter().filter(|c| !journaled.contains(c)).count());
        let same = replayed.digest() == snapshot.digest && accepted == snapshot.seq;
        verified &= same && lost == 0;
        books_out.push(json!({ "symbol": symbol, "node": snapshot.node, "epoch": snapshot.epoch, "seq": snapshot.seq,
            "replay_matches": same, "acked_not_journaled": lost }));
    }
    let hot_view = view.ask(md::Msg::Get).await?;
    let full =
        client::snapshot(&books[&hot], usize::MAX, cluster.request_timeout()).await.map_err(|e| format!("{e:?}"))?;
    let md_matches = hot_view.seq == full.seq && hot_view.bids == full.bids && hot_view.asks == full.asks;
    verified &= md_matches;
    println!(
        "{}",
        json!({ "verified": verified, "p50_us": p50, "p99_us": p99, "max_us": max, "samples": samples.len(),
            "accepted": counts.accepted.load(Relaxed), "busy": counts.busy.load(Relaxed), "rejected": counts.rejected.load(Relaxed),
            "book_full": counts.book_full.load(Relaxed), "hot_longest_without_an_accept_ms": hot_last.lock().expect("hot_last").1.as_millis() as u64,
            "resent": counts.resent.load(Relaxed), "found": counts.found.load(Relaxed), "unresolved": counts.unresolved.load(Relaxed),
            "md_view_matches_book": md_matches, "md_snapshots": hot_view.snapshots, "books": books_out })
    );
    system.shutdown().await?;
    if verified { Ok(()) } else { Err("verification failed".into()) }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // No TLS between nodes: a node talks unencrypted only where `--plaintext` says it may.
    let plaintext = std::env::args().any(|a| a == "--plaintext");
    let mut argv = std::env::args().skip(1);
    let command = argv.next().unwrap_or_default();
    let args = Args(argv.filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect());
    match command.as_str() {
        "seed" | "engine" | "load" if !plaintext => Err(
            "matching-engine has no TLS between nodes: add --plaintext to run on loopback or a private network only"
                .into(),
        ),
        "seed" => seed(args).await,
        "engine" => engine(args).await,
        "load" => load(args).await,
        _ => Err("usage: matching-engine seed|engine|load --plaintext key=value..".into()),
    }
}
