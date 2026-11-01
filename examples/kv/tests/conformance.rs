//! kv's replies, byte for byte, against Redis 7.4's for the commands both serve. The expected bytes are in
//! `tests/conformance.golden`, recorded from the pinned Redis in cluster mode with every slot on one node:
//!
//!   KV_GOLDEN_REDIS=127.0.0.1:5399 cargo test -p kv --test conformance   (bench.sh golden runs it)
//!
//! Commands kv answers otherwise on purpose (SET's options, SELECT, HELLO, INFO, CONFIG, COMMAND, CLUSTER SLOTS)
//! are not here. In `CLUSTER SHARDS` a node's id, address and replication offset are
//! its own on any server (a Redis node alone has not learnt its address, and gives ""): they are compared as `*`, the
//! rest byte for byte.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use axon_cluster::MemoryDiscovery;
use axon_net::SharedSecret;
use bytes::BytesMut;
use kv::node::{self, Settings, Store};
use kv::resp::{Out, Value, parse_reply};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/conformance.golden");

/// The fields of a node, in `CLUSTER SHARDS`, that are its own on any server.
const OWN: [&str; 5] = ["id", "port", "ip", "endpoint", "replication-offset"];

/// The reply with the values of [`OWN`] fields as `*`, wherever an array holds field and value pairs. Only for
/// `CLUSTER SHARDS`: in any other case's reply a value such as `port` is data, compared byte for byte.
fn redact(value: Value) -> Value {
    let Value::Array(Some(items)) = value else { return value };
    let pairs = items.len() % 2 == 0 && items.iter().step_by(2).all(|k| matches!(k, Value::Bulk(Some(_))));
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let own = pairs
            && out.len() % 2 == 1
            && matches!(out.last(), Some(Value::Bulk(Some(k))) if OWN.iter().any(|o| k[..] == *o.as_bytes()));
        out.push(if own { Value::bulk("*") } else { redact(item) });
    }
    Value::Array(Some(out))
}

fn multibulk(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend(format!("${}\r\n{a}\r\n", a.len()).into_bytes());
    }
    out
}

fn commands(list: &[&[&str]]) -> (Vec<u8>, usize) {
    (list.iter().flat_map(|args| multibulk(args)).collect(), list.len())
}

/// Each case: its name, the bytes sent on a new connection, and the replies to read. They run in order, from an
/// empty store.
fn cases() -> Vec<(&'static str, Vec<u8>, usize)> {
    let mut cases = Vec::new();
    let mut add = |name, (bytes, n): (Vec<u8>, usize)| cases.push((name, bytes, n));
    add("flushall", commands(&[&["FLUSHALL"]]));
    add("ping", commands(&[&["PING"], &["PING", "hello"], &["ECHO", "a b"]]));
    add(
        "set_get",
        commands(&[&["SET", "a", "1"], &["GET", "a"], &["GET", "nothing"], &["SET", "e", ""], &["GET", "e"]]),
    );
    add("overwrite", commands(&[&["SET", "a", "2"], &["GET", "a"]]));
    add("inline", (b"SET b \"x y\"\r\nGET b\r\nPING\r\n".to_vec(), 3));
    add("inline_empty_lines", (b"\r\n\r\nPING\r\n".to_vec(), 1));
    add(
        "del_exists",
        commands(&[
            &["SET", "{u}a", "1"],
            &["SET", "{u}b", "2"],
            &["EXISTS", "{u}a", "{u}b", "{u}c", "{u}a"],
            &["DEL", "{u}a", "{u}c"],
            &["EXISTS", "{u}a"],
        ]),
    );
    add("mset_mget", commands(&[&["MSET", "{m}a", "1", "{m}b", "2"], &["MGET", "{m}a", "{m}b", "{m}c"]]));
    add(
        "crossslot",
        commands(&[&["MSET", "a", "1", "b", "2"], &["MGET", "a", "b"], &["DEL", "a", "b"], &["EXISTS", "a", "b"]]),
    );
    add("dbsize", commands(&[&["DBSIZE"]]));
    add("unknown", commands(&[&["FOO", "bar", "baz"], &["FOO"]]));
    add("arity", commands(&[&["GET"], &["SET", "a"], &["ECHO"], &["MSET", "a"], &["MSET", "a", "1", "b"], &["DEL"]]));
    add("keyslot", commands(&[&["CLUSTER", "KEYSLOT", "foo"], &["CLUSTER", "KEYSLOT", "{user1000}.following"]]));
    add("cluster_shards", commands(&[&["CLUSTER", "SHARDS"], &["CLUSTER", "SHARDS", "x"]]));
    add("client", commands(&[&["CLIENT", "SETNAME", "ycsb"]]));
    add("flushdb", commands(&[&["FLUSHDB"], &["DBSIZE"]]));
    add("multibulk_length", (b"*x\r\n".to_vec(), 1));
    add("expected_dollar", (b"*1\r\nPING\r\n".to_vec(), 1));
    add("bulk_length", (b"*1\r\n$x\r\n".to_vec(), 1));
    add("unbalanced_quotes", (b"SET a \"b\r\n".to_vec(), 1));
    add("inline_too_big", (vec![b'a'; 70_000], 1));
    add("quit", commands(&[&["QUIT"], &["PING"]]));
    cases
}

/// What one case gets back: the bytes of its replies (with [`OWN`] fields as `*` where `own`), and whether the server
/// closed the connection after them.
async fn exchange(addr: SocketAddr, send: &[u8], replies: usize, own: bool) -> (String, bool) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(send).await.unwrap();
    let (mut buf, mut got, mut parsed) = (BytesMut::new(), Vec::new(), 0);
    let mut closed = false;
    while parsed < replies {
        let mut probe = buf.clone();
        if let Ok(Some(value)) = parse_reply(&mut probe) {
            buf = probe;
            let mut out = Out::default();
            out.push(&if own { redact(value) } else { value });
            out.take().iter().for_each(|b| got.extend_from_slice(b));
            parsed += 1;
            continue;
        }
        match tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {
                closed = true;
                break;
            }
            Ok(Ok(_)) => {}
            Err(_) => panic!("no reply within 5 s to {:?}", String::from_utf8_lossy(&send[..send.len().min(60)])),
        }
    }
    if !closed {
        let mut rest = [0u8; 64];
        closed = matches!(
            tokio::time::timeout(Duration::from_millis(300), stream.read(&mut rest)).await,
            Ok(Ok(0) | Err(_))
        );
    }
    (String::from_utf8(got).expect("ASCII replies"), closed)
}

async fn run(addr: SocketAddr) -> BTreeMap<String, serde_json::Value> {
    let mut out = BTreeMap::new();
    for (name, send, replies) in cases() {
        let (got, closed) = exchange(addr, &send, replies, name == "cluster_shards").await;
        out.insert(name.to_owned(), serde_json::json!({ "replies": got, "closed": closed }));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_match_redis_byte_for_byte() {
    if let Ok(redis) = std::env::var("KV_GOLDEN_REDIS") {
        let recorded = run(redis.parse().expect("KV_GOLDEN_REDIS is host:port")).await;
        let lines: Vec<String> =
            recorded.iter().map(|(name, v)| serde_json::json!({ "case": name, "redis": v }).to_string()).collect();
        std::fs::write(GOLDEN, lines.join("\n") + "\n").unwrap();
        return;
    }
    let golden = std::fs::read_to_string(GOLDEN).expect("tests/conformance.golden: record it with KV_GOLDEN_REDIS");
    let golden: BTreeMap<String, serde_json::Value> = golden
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            (v["case"].as_str().unwrap().to_owned(), v["redis"].clone())
        })
        .collect();

    let settings = Settings { resp: Some(([127, 0, 0, 1], 0).into()), ..Settings::default() };
    let mut transport = axon_net::Config::new("n1", SharedSecret::new("kv-conformance-secret").unwrap());
    transport.allow_plaintext = true; // loopback
    let node = node::start(&settings, transport, MemoryDiscovery::new(), Store::memory()).await.unwrap();
    let resp = node.resp.expect("a RESP listener");
    kv::client::warm(&[resp], Duration::from_secs(60)).await.unwrap();
    let ours = run(resp).await;
    let mut differ = Vec::new();
    for (name, _, _) in cases() {
        let redis = golden.get(name).unwrap_or_else(|| panic!("no golden for {name}: record again"));
        if &ours[name] != redis {
            differ.push(format!("{name}:\n  redis {redis}\n  kv    {}", ours[name]));
        }
    }
    node.shutdown().await.unwrap();
    assert!(differ.is_empty(), "kv answers otherwise than Redis:\n{}", differ.join("\n"));
}
