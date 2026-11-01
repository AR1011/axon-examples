//! durable-1 on real time: one node whose Log keeps every acknowledged write through a restart, values of 5 and
//! 32 MiB included, its 256 shards opening at once, and a WAL that stays bounded as checkpoints let it be trimmed.

use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{FileFuture, FileOp, Files, LogConfig, MemoryFiles};
use axon_cluster::MemoryDiscovery;
use axon_net::SharedSecret;
use bytes::Bytes;
use kv::client::{Client, warm};
use kv::log::{LogMsg, LogSettings, LogStatus};
use kv::node::{self, Mode, Node, Settings, Store};
use kv::resp::Value;
use kv::slot::{SHARDS, tag_for};

/// The store a restart finds again, and the reasons any Log ended with.
fn store() -> (Store, Arc<Mutex<Vec<String>>>) {
    let halts = Arc::new(Mutex::new(Vec::new()));
    let seen = halts.clone();
    let store = Store { halt: Arc::new(move |why| seen.lock().expect("not poisoned").push(why)), ..Store::memory() };
    (store, halts)
}

fn settings() -> Settings {
    Settings {
        resp: Some(([127, 0, 0, 1], 0).into()),
        mode: Mode::Durable,
        // The acceptor warms for a little over a lease after each start: a short one keeps the restarts quick.
        lease: Duration::from_secs(1),
        deadline: Duration::from_secs(5),
        ..Settings::default()
    }
}

async fn start(settings: &Settings, store: &Store) -> Node {
    let mut transport = axon_net::Config::new("n1", SharedSecret::new("kv-durable-test-secret").unwrap());
    transport.allow_plaintext = true; // loopback
    node::start(settings, transport, MemoryDiscovery::new(), store.clone()).await.expect("the node starts")
}

fn resp(node: &Node) -> SocketAddr {
    node.resp.expect("a RESP listener")
}

async fn client(node: &Node) -> Client {
    Client::connect(resp(node)).await.expect("connect")
}

async fn status(node: &Node) -> LogStatus {
    let log = node.log.as_ref().expect("a durable node has a Log");
    log.ask(LogMsg::Status).await.expect("the Log answers")
}

fn bulk(v: &[u8]) -> Value {
    Value::Bulk(Some(Bytes::copy_from_slice(v)))
}

/// Every shard asked at once, one connection each: 256 Opens reach the Log together, past the 128 asks a mailbox
/// takes by default.
async fn touch_every_shard_at_once(node: &Node) {
    let addr = resp(node);
    let mut tasks = Vec::new();
    for shard in 0..SHARDS {
        tasks.push(tokio::spawn(async move {
            let key = format!("t:{{{}}}", tag_for(shard));
            let mut c = Client::connect(addr).await.expect("connect");
            let answer = c.call(&[b"EXISTS", key.as_bytes()]).await.expect("an answer");
            assert_eq!(answer, Value::Int(0), "shard {shard}");
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

/// Memory whose writes take as long as a sync of a fast disk.
struct Slow(MemoryFiles);

impl Files for Slow {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.0.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.0.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let inner = self.0.apply(ops);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            inner.await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_256_shards_of_one_node_open_at_once_and_again_after_a_restart() {
    let (store, halts) = store();
    let store = Store { wal: Arc::new(Slow(MemoryFiles::new())), ..store };
    let settings = settings();
    for run in 0..2 {
        let node = start(&settings, &store).await;
        touch_every_shard_at_once(&node).await;
        assert_eq!(node.stats.shard_starts.load(Relaxed), u64::from(SHARDS), "run {run}");
        let st = status(&node).await;
        // Each start's promise, Begin and commit are records, and one write of 2 ms takes many of them.
        assert!(st.wal.writes * 4 < st.wal.durable.0, "run {run}: {st:?}");
        node.shutdown().await.unwrap();
    }
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_node_keeps_every_acknowledged_write_through_a_restart() {
    let (store, halts) = store();
    let settings = settings();
    let node = start(&settings, &store).await;
    warm(&[resp(&node)], Duration::from_secs(60)).await.expect("every shard answers");
    let mut c = client(&node).await;
    for j in 0..500 {
        let (key, value) = (format!("key:{j}"), format!("value {j}"));
        assert_eq!(c.call(&[b"SET", key.as_bytes(), value.as_bytes()]).await.unwrap(), Value::ok());
    }
    for j in (0..500).step_by(5) {
        assert_eq!(c.call(&[b"DEL", format!("key:{j}").as_bytes()]).await.unwrap(), Value::Int(1));
    }
    let five: Vec<u8> = (0..5 << 20).map(|i| (i % 251) as u8).collect();
    let thirty_two: Vec<u8> = (0..32 << 20).map(|i| (i % 253) as u8).collect();
    assert_eq!(c.call(&[b"SET", b"big:5", &five]).await.unwrap(), Value::ok());
    assert_eq!(c.call(&[b"SET", b"big:32", &thirty_two]).await.unwrap(), Value::ok());
    let st = status(&node).await;
    assert!(st.entries > 0 && st.waiting == 0 && st.backlog == 0, "{st:?}");
    let Value::Bulk(Some(info)) = c.call(&[b"INFO", b"kv"]).await.unwrap() else { panic!("INFO kv") };
    let info = String::from_utf8_lossy(&info);
    assert!(info.contains(&format!("wal_entries:{}\r\n", st.entries)), "{info}");
    assert!(info.contains("entries_per_write:") && info.contains("snapshots:"), "{info}");
    drop(c);
    node.shutdown().await.unwrap();

    let node = start(&settings, &store).await;
    warm(&[resp(&node)], Duration::from_secs(60)).await.expect("every shard answers");
    let mut c = client(&node).await;
    for j in 0..500 {
        let got = c.call(&[b"GET", format!("key:{j}").as_bytes()]).await.unwrap();
        let want = if j % 5 == 0 { Value::Bulk(None) } else { bulk(format!("value {j}").as_bytes()) };
        assert_eq!(got, want, "key:{j}");
    }
    assert_eq!(c.call(&[b"GET", b"big:5"]).await.unwrap(), bulk(&five));
    assert_eq!(c.call(&[b"GET", b"big:32"]).await.unwrap(), bulk(&thirty_two));
    assert_eq!(c.call(&[b"DBSIZE"]).await.unwrap(), Value::Int(402));
    drop(c);
    node.shutdown().await.unwrap();
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
}

const ROUNDS: usize = 5000;

/// Small segments and a small target, and 40 MiB written over 2000 keys: snapshots let the WAL be trimmed and its
/// segments removed, so it stays near its target, and a restart reads every value back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_wal_stays_bounded_as_snapshots_let_it_be_trimmed() {
    let (store, halts) = store();
    let mut settings = settings();
    settings.limits.value = 8 << 10;
    settings.limits.command = 16 << 10;
    settings.log = LogSettings {
        wal: LogConfig {
            segment_bytes: 4 << 20,
            max_record: 2 << 20,
            max_write_bytes: 4 << 20,
            max_unwritten_bytes: 16 << 20,
            ..kv::log::wal_config()
        },
        wal_floor: 8 << 20,
        ..LogSettings::default()
    };
    let node = start(&settings, &store).await;
    warm(&[resp(&node)], Duration::from_secs(60)).await.expect("every shard answers");
    let addr = resp(&node);
    let log = node.log.clone().expect("a Log");
    let sampler = tokio::spawn(async move {
        let mut most = 0;
        loop {
            let Ok(st) = log.ask(LogMsg::Status).await else { return most };
            most = most.max(st.wal.bytes);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let mut writers = Vec::new();
    for w in 0..8 {
        writers.push(tokio::spawn(async move {
            let mut c = Client::connect(addr).await.expect("connect");
            for round in 0..ROUNDS {
                let value = format!("{round:06}-{}", "x".repeat(1000));
                let key = format!("w{w}:{}", round % 250);
                assert_eq!(c.call(&[b"SET", key.as_bytes(), value.as_bytes()]).await.unwrap(), Value::ok());
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    let st = status(&node).await;
    assert!(st.snaps > 0, "{st:?}");
    node.shutdown().await.unwrap();
    let most = sampler.await.unwrap();
    // The target, the snapshots' share of it, and a segment being filled: well under the 40 MiB written.
    assert!(most <= 16 << 20, "the WAL stays bounded: {st:?}, most {most}");

    let node = start(&settings, &store).await;
    warm(&[resp(&node)], Duration::from_secs(60)).await.expect("every shard answers");
    let mut c = client(&node).await;
    for w in 0..8 {
        for k in 0..250 {
            let got = c.call(&[b"GET", format!("w{w}:{k}").as_bytes()]).await.unwrap();
            let Value::Bulk(Some(v)) = got else { panic!("w{w}:{k} is gone") };
            assert_eq!(&v[..6], format!("{:06}", ROUNDS - 250 + k).as_bytes(), "w{w}:{k}");
        }
    }
    drop(c);
    node.shutdown().await.unwrap();
    assert!(halts.lock().unwrap().is_empty(), "{:?}", halts.lock().unwrap());
}
