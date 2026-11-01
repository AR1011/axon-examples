//! A document's compacted stream at the design's limits (README § Streams): `base` and `u/*` records filled to the
//! 448 KiB state of `doc.hN`, its namespace's own tiers to its home, and 500 `SnapshotThenLive` subscribers on a web
//! node, which relays it. They get the whole state, then a compaction live, then a new generation all at once (the
//! storm after a home restarts), with no `LimitExceeded`, no slow-consumer reset and no `GlobalCapacity` reset. A
//! second home's document goes through the same web node in its own namespace.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::{
    CompactLimits, Cursor, DeliveryPolicy, RESET_REASONS, ResetReason, RingLimits, StartPosition, StreamError,
    StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions, keyed, split_keyed,
};
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::time::Instant;

const STATE: usize = 448 << 10;
const SUBSCRIBERS: usize = 500;
const HOMES: [&str; 2] = ["h1", "h2"];

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

async fn node(id: &str, pairs: &[(&str, &str)], discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("mdpad-spike-secret-16").unwrap());
    transport.allow_plaintext = true; // loopback only
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels = labels(pairs);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

/// The limits every node gets: a ring per document namespace, and room for two streams per open document.
fn config(upstream: bool) -> StreamNodeConfig {
    let mut cfg = StreamNodeConfig::new(vec![]);
    for home in HOMES {
        let namespace = format!("doc.{home}");
        cfg.core.rings.push(RingLimits { namespace: namespace.clone(), max_records: 2048, max_bytes: 4 << 20 });
        if upstream {
            cfg = cfg.upstream_for(namespace, vec![labels(&[("home", home)])]);
        }
    }
    cfg.core.hub.max_streams = 16_384;
    cfg.core.store.max_streams = 16_384;
    cfg
}

fn compacted() -> DeliveryPolicy {
    DeliveryPolicy::Compacted(CompactLimits { max_keys: 1024, max_bytes: STATE })
}

type Table = BTreeMap<Bytes, Bytes>;

fn apply(table: &mut Table, records: &[Bytes]) {
    for record in records {
        let (key, value) = split_keyed(record).expect("keyed");
        if value.is_empty() {
            table.remove(&key);
        } else {
            table.insert(key, value);
        }
    }
}

/// The origin's side: what it published, as a subscriber should hold it.
struct Doc {
    node: StreamNode,
    key: StreamKey,
    table: Table,
    at: Option<Cursor>,
    next_update: u64,
}

impl Doc {
    fn publish(&mut self, records: Vec<Bytes>) -> Result<Cursor, StreamError> {
        let cursor = self.node.publish(&self.key, records.clone()).map_err(|e| e.error)?;
        apply(&mut self.table, &records);
        self.at = Some(cursor);
        Ok(cursor)
    }

    /// `base`, the ACL and 1 KiB updates up to the state's limit, in one batch as a load after a restart publishes
    /// it; then one more update is refused. Returns the state's bytes.
    fn fill(&mut self, generation: u64) -> usize {
        let mut batch = vec![base(generation), keyed(b"acl", &[7; 200])];
        let mut bytes: usize = batch.iter().map(Bytes::len).sum();
        loop {
            let update = keyed(format!("u/{generation}.{}", self.next_update + 1).as_bytes(), &[1; 1024]);
            if bytes + update.len() > STATE {
                break;
            }
            self.next_update += 1;
            bytes += update.len();
            batch.push(update);
        }
        self.publish(batch).expect("the whole state in one batch");
        let past = keyed(b"u/past", &[1; 1024]);
        assert_eq!(self.publish(vec![past]).unwrap_err(), StreamError::StateFull, "at the limit");
        bytes
    }

    /// A new `base` and an empty value for every `u/*`, in one batch.
    fn compact(&mut self, generation: u64) -> Cursor {
        let mut batch = vec![base(generation + 100)];
        batch.extend(self.table.keys().filter(|k| k.starts_with(b"u/")).map(|k| keyed(k, b"")));
        self.publish(batch).expect("a compaction fits the batch and the state")
    }
}

fn base(generation: u64) -> Bytes {
    keyed(b"base", &vec![u8::try_from(generation % 251).unwrap(); 250 << 10])
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

#[derive(Default)]
struct Seen {
    at: Option<Cursor>,
    table: Table,
    snapshots: u64,
    generation_changes: u64,
    /// Any other end or reset.
    bad: Vec<String>,
}

/// A session's subscription: the state, then live; a new generation, or any end, subscribes again.
async fn subscriber(node: StreamNode, key: StreamKey, seen: Arc<Mutex<Seen>>) {
    loop {
        let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
        let mut events = node.subscribe(&key, options).expect("subscribe");
        while let Some(event) = events.next().await {
            let mut seen = seen.lock().await;
            match event {
                StreamEvent::Snapshot(state) => {
                    seen.table.clear();
                    apply(&mut seen.table, state.records());
                    seen.snapshots += 1;
                    seen.at = Some(state.through());
                }
                StreamEvent::Batch(batch) => {
                    apply(&mut seen.table, batch.records());
                    seen.at = Some(Cursor { generation: batch.generation(), sequence: batch.last() });
                }
                StreamEvent::ResetRequired(ResetReason::GenerationChanged) => {
                    seen.generation_changes += 1;
                    break;
                }
                end if end.is_terminal() => {
                    seen.bad.push(format!("{end:?}"));
                    break;
                }
                _ => {}
            }
        }
    }
}

/// Waits until every subscriber holds what `doc` published, and returns how long that took.
async fn converged(seen: &[Arc<Mutex<Seen>>], doc: &Doc, what: &str) -> Duration {
    let started = Instant::now();
    loop {
        let mut behind = 0;
        for s in seen {
            let s = s.lock().await;
            assert!(s.bad.is_empty(), "{what}: {:?}", s.bad);
            if s.at != doc.at || s.table != doc.table {
                behind += 1;
            }
        }
        if behind == 0 {
            return started.elapsed();
        }
        assert!(started.elapsed() < Duration::from_secs(120), "{what}: {behind} subscribers behind");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_document_state_reaches_500_subscribers_through_a_relay() {
    let discovery = MemoryDiscovery::new();
    let mut systems = Vec::new();
    let mut homes = Vec::new();
    for home in HOMES {
        let (system, cluster) = node(home, &[("homes", "1"), ("home", home)], &discovery).await;
        homes.push(StreamNode::start(&cluster, config(false)).await.unwrap());
        systems.push((system, cluster));
    }
    let (system, cluster) = node("w1", &[("web", "1")], &discovery).await;
    cluster.wait_view(|v| v.members.len() == 3).await.unwrap();
    let web = StreamNode::start(&cluster, config(true)).await.unwrap();
    systems.push((system, cluster));

    let mut docs = Vec::new();
    for (home, origin) in HOMES.iter().zip(&homes) {
        let key = StreamKey::new(format!("doc.{home}"), format!("{home}-spike"));
        origin.open_stream(&key, 1, compacted()).await.unwrap();
        docs.push(Doc { node: origin.clone(), key, table: Table::new(), at: None, next_update: 0 });
    }
    let bytes: Vec<usize> = docs.iter_mut().map(|d| d.fill(1)).collect();
    println!("state at the limit: {bytes:?} bytes of {STATE}");
    assert!(bytes.iter().all(|&b| b > STATE - 1100 && b <= STATE));

    // 500 viewers of the first home's document, 5 of the second's, all subscribing at once.
    let mut seen = Vec::new();
    for (i, doc) in docs.iter().enumerate() {
        let n = if i == 0 { SUBSCRIBERS } else { 5 };
        let all: Vec<_> = (0..n).map(|_| Arc::new(Mutex::new(Seen::default()))).collect();
        for s in &all {
            tokio::spawn(subscriber(web.clone(), doc.key.clone(), s.clone()));
        }
        seen.push(all);
    }
    let peak = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (web, peak) = (web.clone(), peak.clone());
        tokio::spawn(async move {
            loop {
                peak.fetch_max(web.stats().hub.queued_bytes, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    let first = converged(&seen[0], &docs[0], "first state").await;
    converged(&seen[1], &docs[1], "the second home's state").await;
    println!("500 subscribers hold the 448 KiB state {first:?} after subscribing");

    // Live: a compaction (a new base, every u/* removed), then updates after it.
    docs[0].compact(1);
    for i in 0..20 {
        docs[0].publish(vec![keyed(format!("u/1.after.{i}").as_bytes(), &[3; 1024])]).unwrap();
    }
    let live = converged(&seen[0], &docs[0], "compaction").await;
    println!("the compaction and 20 updates after it reached all 500 in {live:?}");

    // The storm: the home restarts the document in a new generation and fills it again.
    let home_before = homes[0].stats();
    web_stats("before the storm", &web);
    homes[0].restart_stream(&docs[0].key, 2).unwrap();
    docs[0].table.clear();
    docs[0].fill(2);
    let storm = converged(&seen[0], &docs[0], "new generation").await;
    let home_after = homes[0].stats();
    let pushed = home_after.push_bytes_out - home_before.push_bytes_out;
    let pushes = home_after.pushes_out - home_before.pushes_out;
    println!(
        "storm: 500 subscribers hold generation 2 {storm:?} after it opened; the home pushed {:.1} MiB in {pushes} pushes",
        mib(pushed)
    );
    sampler.abort();
    println!("peak queued on the web node: {:.1} MiB", mib(peak.load(Ordering::Relaxed) as u64));
    web_stats("after the storm", &web);

    for s in &seen[0] {
        let s = s.lock().await;
        assert!(s.bad.is_empty(), "{:?}", s.bad);
        assert_eq!(s.generation_changes, 1);
        assert_eq!(s.snapshots, 2, "the first state and the new generation's");
    }
    let stats = web.stats();
    for reason in ["slow_consumer", "global_capacity"] {
        let i = RESET_REASONS.iter().position(|r| *r == reason).unwrap();
        assert_eq!(stats.hub.resets[i], 0, "{reason} resets on the web node");
    }
    assert_eq!(stats.local_resets, 1, "the generation change only: {stats:?}");
    for (system, _cluster) in systems.into_iter().rev() {
        system.shutdown().await.unwrap();
    }
}

fn web_stats(when: &str, web: &StreamNode) {
    let s = web.stats();
    println!(
        "web {when}: subscriptions {}, queued {} B, resets {:?}, ends {:?}, pushes in {} ({} B), refusals {:?}",
        s.hub.subscriptions, s.hub.queued_bytes, s.hub.resets, s.hub.ends, s.pushes_in, s.push_bytes_in, s.refusals
    );
}
