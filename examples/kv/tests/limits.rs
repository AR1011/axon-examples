//! A node's bounds, each reached on one memory node on real time: the 1 KiB key, a shard's queue of 1,024 requests
//! and of 64 MiB, and its 64 uploads at once. Each refusal is said at once or at the deadline, nothing waits past
//! it, and every call the shard took is answered and kept.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use axon::{Actor, ActorHandle, Context};
use axon_cluster::{CallError, ClusterError, KeyedRef, MemoryDiscovery, Placement, Rejection};
use axon_net::SharedSecret;
use bytes::Bytes;
use kv::client::{Client, warm};
use kv::node::{self, Node, Settings, Store};
use kv::resp::Value;
use kv::slot::{shard_key, shard_of, tag_for};
use kv::wire::{Answer, Call, ShardApi};
use tokio::time::Instant;

/// The shard the tests fill.
const SHARD: u16 = 7;

async fn start() -> Node {
    let settings = Settings {
        resp: Some(([127, 0, 0, 1], 0).into()),
        // One worker, so that `Hold` holds every actor of the node, the shards with it.
        workers: Some(1),
        // Shorter than a hold: a call to a full shard gets its answer while the shard is still full.
        deadline: Duration::from_millis(300),
        ..Settings::default()
    };
    let mut transport = axon_net::Config::new("n1", SharedSecret::new("kv-limits-test-secret").unwrap());
    transport.allow_plaintext = true; // loopback
    let node =
        node::start(&settings, transport, MemoryDiscovery::new(), Store::memory()).await.expect("the node starts");
    warm(&[resp(&node)], Duration::from_secs(60)).await.expect("every shard answers");
    node
}

fn resp(node: &Node) -> SocketAddr {
    node.resp.expect("a RESP listener")
}

fn key(i: usize) -> Bytes {
    let key = Bytes::from(format!("q{i:04}{{{}}}", tag_for(SHARD)));
    assert_eq!(shard_of(&key), SHARD);
    key
}

fn shard(node: &Node) -> Arc<KeyedRef<ShardApi>> {
    Arc::new(node.cluster.keyed::<ShardApi>("shard", &shard_key(SHARD), Bytes::new(), &Placement::default()))
}

/// An actor whose one message blocks its worker: on a node of one worker, no shard runs a turn until it returns,
/// and calls wait in the shards' queues.
struct Hold(Arc<AtomicBool>);

impl Actor for Hold {
    type Message = Duration;
    type Error = String;

    fn on_message(&mut self, d: Duration, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.0.store(true, SeqCst);
        std::thread::sleep(d);
        Ok(())
    }
}

/// Holds the node's worker for `d`, from when this returns.
async fn hold(node: &Node, d: Duration) -> ActorHandle<Duration> {
    let held = Arc::new(AtomicBool::new(false));
    let handle = node.system.spawn(Hold(held.clone())).start().expect("spawn");
    handle.ready().await.expect("started");
    handle.tell(d).expect("told");
    while !held.load(SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    handle
}

fn full(e: &ClusterError) -> bool {
    matches!(e, ClusterError::Call(CallError::Rejected(Rejection::Full) | CallError::Busy))
}

/// Sends `values.len()` SETs at once while the shard is held for `held`. Returns, by call, whether the shard took
/// it. The ones it refused must be told so before the hold ends; the ones it took are all answered after it.
async fn fill(node: &Node, values: Vec<Bytes>, held: Duration) -> Vec<bool> {
    let target = shard(node);
    // Asked once before the hold, so the reference is kept and no call waits for the owner's answer.
    assert_eq!(target.request(Call::Len).await.unwrap(), Answer::Count(0));
    // The largest value: one the queue's bytes could still take would wait in it.
    let probe = values.iter().max_by_key(|v| v.len()).expect("values").clone();
    let n = values.len();
    let until = Instant::now() + held;
    let _hold = hold(node, held).await;
    let refused = Arc::new(AtomicUsize::new(0));
    let calls: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            let (target, refused) = (target.clone(), refused.clone());
            tokio::spawn(async move {
                let result = target.request(Call::Set(key(i), v)).await;
                if result.as_ref().is_err_and(full) {
                    refused.fetch_add(1, SeqCst);
                }
                (result, Instant::now())
            })
        })
        .collect();

    // The first refusal says the queue is full, and the held shard takes nothing out of it. A call through the front
    // end is then tried until the deadline, and is BUSY.
    while refused.load(SeqCst) == 0 {
        assert!(Instant::now() < until, "the queue filled while the shard was held");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let mut c = Client::connect(resp(node)).await.unwrap();
    let busy = c.call(&[b"SET", &key(n), &probe]).await.unwrap();
    assert_eq!(busy, Value::err(format!("BUSY shard {SHARD} is full")));
    assert!(Instant::now() < until, "the front end answered while the shard was held");

    let mut took = Vec::new();
    for call in calls {
        match call.await.unwrap() {
            (Ok(answer), _) => {
                assert_eq!(answer, Answer::Ok);
                took.push(true);
            }
            (Err(e), at) => {
                assert!(full(&e), "{e:?}");
                assert!(at < until, "a refusal came after the hold ended");
                took.push(false);
            }
        }
    }

    // What the shard took is there, what it refused is not, and it answers at once again.
    for (i, took) in took.iter().enumerate() {
        let got = c.call(&[b"EXISTS", &key(i)]).await.unwrap();
        assert_eq!(got, Value::Int(i64::from(*took)), "key {i}");
    }
    assert_eq!(c.call(&[b"EXISTS", &key(n)]).await.unwrap(), Value::Int(0), "the BUSY SET is not kept");
    assert_eq!(c.call(&[b"SET", &key(n), b"v"]).await.unwrap(), Value::ok());
    took
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_past_1_kib_is_refused_and_nothing_of_it_is_kept() {
    let node = start().await;
    let mut c = Client::connect(resp(&node)).await.unwrap();
    let (edge, over) = (vec![b'k'; 1024], vec![b'k'; 1025]);
    assert_eq!(c.call(&[b"SET", &edge, b"v"]).await.unwrap(), Value::ok());
    let too_large = Value::err("ERR key too large");
    let refused: [&[&[u8]]; 6] = [
        &[b"SET", &over, b"v"],
        &[b"GET", &over],
        &[b"EXISTS", &over],
        &[b"MGET", &edge, &over],
        &[b"DEL", &edge, &over],
        &[b"MSET", &over, b"1"],
    ];
    for args in refused {
        assert_eq!(c.call(args).await.unwrap(), too_large, "{}", String::from_utf8_lossy(args[0]));
    }
    assert_eq!(c.call(&[b"GET", &edge]).await.unwrap(), Value::bulk("v"), "the DEL with a long key removed nothing");
    assert_eq!(c.call(&[b"DBSIZE"]).await.unwrap(), Value::Int(1));
    drop(c);
    node.shutdown().await.unwrap();
}

/// 1,100 SETs at a shard that runs no turn: its queue takes 1,024 requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_queue_of_1024_requests_is_busy_past_it_and_keeps_what_it_took() {
    let node = start().await;
    let took = fill(&node, vec![Bytes::from_static(b"v"); 1100], Duration::from_secs(3)).await;
    assert_eq!(took.iter().filter(|t| **t).count(), 1024);
    node.shutdown().await.unwrap();
}

/// 100 SETs of 1,000 KiB, each one frame, at a shard that runs no turn: its queue takes 64 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_queue_of_64_mib_is_busy_past_it_and_keeps_what_it_took() {
    let node = start().await;
    let value = Bytes::from(vec![7u8; 1000 << 10]);
    // As `node::mailbox` weighs a call.
    let weight = 64 + Call::Set(key(0), value.clone()).encoded_len();
    let took = fill(&node, vec![value; 100], Duration::from_secs(3)).await;
    assert_eq!(took.iter().filter(|t| **t).count(), (64 << 20) / weight);
    node.shutdown().await.unwrap();
}

/// 80 uploads whose bodies never end: the shard reads 64 at once and refuses the rest at once. Once their callers
/// go, a large SET goes through again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_reads_64_uploads_at_once_and_refuses_more_at_once() {
    let node = start().await;
    let target = shard(&node);
    assert_eq!(target.request(Call::Len).await.unwrap(), Answer::Count(0));
    let uploads: Vec<_> = (0..80)
        .map(|_| {
            let target = target.clone();
            tokio::spawn(async move {
                let first = futures_util::stream::iter([Ok(Bytes::from_static(b"x"))]);
                target
                    .upload(Call::Upload, futures_util::StreamExt::chain(first, futures_util::stream::pending()))
                    .await
            })
        })
        .collect();
    let too_many = Answer::Refused(format!("BUSY shard {SHARD} has too many uploads"));
    let deadline = Instant::now() + Duration::from_secs(2);
    while uploads.iter().filter(|u| u.is_finished()).count() < 16 {
        assert!(Instant::now() < deadline, "16 refusals within 2 s");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (done, waiting): (Vec<_>, Vec<_>) = uploads.into_iter().partition(|u| u.is_finished());
    assert_eq!((done.len(), waiting.len()), (16, 64));
    for upload in done {
        assert_eq!(upload.await.unwrap().unwrap(), too_many);
    }

    // The callers go: the shard's reads end, and a value larger than a frame is taken again.
    for upload in &waiting {
        upload.abort();
    }
    let mut c = Client::connect(resp(&node)).await.unwrap();
    let large = vec![9u8; 2 << 20];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match c.call(&[b"SET", &key(0), &large]).await.unwrap() {
            Value::Simple(ok) if ok == "OK" => break,
            Value::Error(e) if e.starts_with("BUSY") && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(c.call(&[b"GET", &key(0)]).await.unwrap(), Value::Bulk(Some(large.into())));
    drop(c);
    node.shutdown().await.unwrap();
}
