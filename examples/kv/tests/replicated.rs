//! durable-3 on real time: three nodes in one process on loopback, each with a Log. Values of 5 and 32 MiB go to
//! the other Logs in parts, the owner of their shard crashes, the others serve every acknowledged write, and after
//! its restart the three Logs hold the same maps. A Log sent more uploads at once than it has jobs keeps writing.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::Histogram;
use axon_cluster::{CallError, MemoryDiscovery, Placement};
use axon_net::SharedSecret;
use bytes::Bytes;
use kv::client::{Client, warm};
use kv::node::{self, Mode, Node, Settings, Store};
use kv::resp::Value;
use kv::slot::{SHARDS, shard_key, tag_for};
use kv::wire::{LogApi, LogCall};

const IDS: [&str; 3] = ["n1", "n2", "n3"];

fn store(halts: &Arc<Mutex<Vec<String>>>) -> Store {
    let seen = halts.clone();
    Store { halt: Arc::new(move |why| seen.lock().expect("not poisoned").push(why)), ..Store::memory() }
}

async fn start(id: &str, discovery: &MemoryDiscovery, store: &Store) -> Node {
    let settings = Settings {
        id: id.into(),
        resp: Some(([127, 0, 0, 1], 0).into()),
        nodes: IDS.iter().map(|n| n.to_string()).collect(),
        mode: Mode::Durable,
        // A short lease so the crashed node's shards move within the test; the recorded runs use the shipped 6 s.
        lease: Duration::from_secs(1),
        deadline: Duration::from_secs(5),
        ..Settings::default()
    };
    let mut transport = axon_net::Config::new(id, SharedSecret::new("kv-replicated-test-secret").unwrap());
    transport.allow_plaintext = true; // loopback
    node::start(&settings, transport, discovery.clone(), store.clone()).await.expect("the node starts")
}

fn resp(node: &Node) -> SocketAddr {
    node.resp.expect("a RESP listener")
}

/// GET until the shard answers: a moved shard says TRYAGAIN or CLUSTERDOWN until its new owner serves.
async fn get(c: &mut Client, key: &[u8]) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match c.call(&[b"GET", key]).await.expect("an answer") {
            Value::Error(e) if tokio::time::Instant::now() < deadline => {
                assert!(e.starts_with("TRYAGAIN") || e.starts_with("CLUSTERDOWN"), "{e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            got => return got,
        }
    }
}

async fn digest(addr: SocketAddr) -> String {
    let mut c = Client::connect(addr).await.expect("connect");
    let Value::Bulk(Some(d)) = c.call(&[b"INFO", b"kvdigest"]).await.expect("an answer") else { panic!("digest") };
    String::from_utf8_lossy(&d).into_owned()
}

fn recovery(node: &Node) -> String {
    let h = Histogram::from(&node.stats.recovery.snapshot());
    let ms = |p| h.percentile(p).map_or(0, |d| d.as_millis());
    format!("p50 {} ms, p99 {} ms, max {} ms", ms(0.5), ms(0.99), node.stats.recovery_max_ms.load(Relaxed))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acknowledged_write_survives_the_owner_crash_and_the_logs_agree_after_its_restart() {
    let halts = Arc::new(Mutex::new(Vec::new()));
    let stores: Vec<Store> = IDS.iter().map(|_| store(&halts)).collect();
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for (id, store) in IDS.iter().zip(&stores) {
        nodes.push(start(id, &discovery, store).await);
    }
    let addrs: Vec<SocketAddr> = nodes.iter().map(resp).collect();
    warm(&addrs, Duration::from_secs(60)).await.expect("every shard answers");

    let owner = |nodes: &[Node], shard: u16| {
        let owner = nodes[0].cluster.owner("shard", &shard_key(shard), &Placement::default());
        owner.expect("every shard has an owner in the view").to_string()
    };
    let on_n3 = (0..SHARDS).find(|s| owner(&nodes, *s) == "n3").expect("n3 owns a shard");
    let mut c = Client::connect(addrs[0]).await.expect("connect");
    let mut want = BTreeMap::new();
    for j in 0..600 {
        let (key, value) = (format!("key:{j}"), format!("value {j}"));
        assert_eq!(c.call(&[b"SET", key.as_bytes(), value.as_bytes()]).await.unwrap(), Value::ok());
        want.insert(key.into_bytes(), Bytes::from(value));
    }
    for (name, len) in [("five", 5 << 20), ("thirty-two", 32 << 20)] {
        let key = format!("{name}{{{}}}", tag_for(on_n3));
        let value: Bytes = (0..len).map(|i| (i % 251) as u8).collect();
        assert_eq!(c.call(&[b"SET", key.as_bytes(), &value]).await.unwrap(), Value::ok(), "{name}");
        want.insert(key.into_bytes(), value);
    }
    assert!(nodes[2].stats.log_uploads.load(Relaxed) >= 4, "n3 sent the large Appends to n1 and n2 in parts");

    nodes.pop().expect("n3").crash();
    for (key, value) in &want {
        let got = get(&mut c, key).await;
        assert!(got == Value::Bulk(Some(value.clone())), "{} after n3 crashed", String::from_utf8_lossy(key));
    }
    println!("n1 recovery {}; n2 recovery {}", recovery(&nodes[0]), recovery(&nodes[1]));
    assert!(nodes.iter().map(|n| n.stats.recovery.snapshot().count()).sum::<u64>() > 0, "the survivors recovered");

    nodes.push(start("n3", &discovery, &stores[2]).await);
    let addrs: Vec<SocketAddr> = nodes.iter().map(resp).collect();
    warm(&addrs, Duration::from_secs(60)).await.expect("every shard answers");
    let mut c3 = Client::connect(addrs[2]).await.expect("connect");
    for (key, value) in &want {
        let got = get(&mut c3, key).await;
        assert!(got == Value::Bulk(Some(value.clone())), "{} after n3 restarted", String::from_utf8_lossy(key));
    }
    // The owners bring the restarted Log up to date on their heartbeat.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let d = [digest(addrs[0]).await, digest(addrs[1]).await, digest(addrs[2]).await];
        if d[0] == d[1] && d[1] == d[2] {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the Logs differ after 30 s");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop((c, c3));
    for node in nodes {
        node.shutdown().await.unwrap();
    }
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
}

/// In a soak a restarted node's Log was sent more Installs at once than it had jobs: its next write was refused, and
/// the node stopped itself. Here 120 bodies that never end go to n2's Log: those past its share are not answered at
/// once, and its writes go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crowd_of_uploads_leaves_the_log_its_writes() {
    let halts = Arc::new(Mutex::new(Vec::new()));
    let stores: Vec<Store> = IDS.iter().map(|_| store(&halts)).collect();
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for (id, store) in IDS.iter().zip(&stores) {
        nodes.push(start(id, &discovery, store).await);
    }
    let addrs: Vec<SocketAddr> = nodes.iter().map(resp).collect();
    warm(&addrs, Duration::from_secs(60)).await.expect("every shard answers");

    let mut crowd = Vec::new();
    for from in [&nodes[0], &nodes[2]] {
        let log = from.cluster.resolve_on::<LogApi>("n2", "kv.log").await.expect("n2's Log");
        for _ in 0..60 {
            let log = log.clone();
            let body = futures_util::stream::pending::<Result<Bytes, CallError>>();
            crowd.push(tokio::spawn(async move { log.upload(LogCall::Upload, body).await }));
        }
    }
    // Those past the Log's share are cut at once, inside the 5 s `request_timeout` that ends the held ones.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while crowd.iter().filter(|t| t.is_finished()).count() < 120 - 64 {
        assert!(tokio::time::Instant::now() < deadline, "n2's Log took every upload");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cut = crowd.iter().filter(|t| t.is_finished()).count();

    let before = nodes[1].stats.wal.writes.load(Relaxed);
    let mut c = Client::connect(addrs[0]).await.expect("connect");
    for j in 0..600 {
        let key = format!("key:{j}");
        assert_eq!(c.call(&[b"SET", key.as_bytes(), b"v"]).await.unwrap(), Value::ok());
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while nodes[1].stats.wal.writes.load(Relaxed) == before {
        assert!(tokio::time::Instant::now() < deadline, "n2's Log wrote nothing: {:?}", halts.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
    assert!(cut >= 120 - 32, "{cut} uploads cut at once");
    crowd.iter().for_each(|t| t.abort());
    drop(c);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
}
