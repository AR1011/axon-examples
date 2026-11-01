//! Three kv nodes in one process on loopback, on real time: routing from any node, pipelining, the slot map, values
//! past a frame in parts, and memory mode's loss when a shard moves (I7).

use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon_cluster::{MemoryDiscovery, Placement};
use axon_net::SharedSecret;
use kv::client::{Client, warm};
use kv::node::{self, Node, Settings, Store};
use kv::resp::Value;
use kv::slot::{SHARDS, SLOTS_PER_SHARD, shard_key, shard_of, tag_for};

async fn start(n: usize) -> Vec<Node> {
    let discovery = MemoryDiscovery::new();
    let ids: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    let mut nodes = Vec::new();
    for id in &ids {
        let settings = Settings {
            id: id.clone(),
            resp: Some(([127, 0, 0, 1], 0).into()),
            nodes: ids.clone(),
            // A short lease so a crashed node's shards move within the test; the recorded runs use the shipped 6 s.
            lease: Duration::from_secs(1),
            deadline: Duration::from_secs(3),
            ..Settings::default()
        };
        let mut transport = axon_net::Config::new(id.as_str(), SharedSecret::new("kv-cluster-test-secret").unwrap());
        transport.allow_plaintext = true; // loopback
        let node = node::start(&settings, transport, discovery.clone(), Store::memory()).await;
        nodes.push(node.expect("the node starts"));
    }
    let addrs: Vec<SocketAddr> = nodes.iter().map(resp).collect();
    warm(&addrs, Duration::from_secs(60)).await.expect("every shard answers");
    nodes
}

fn resp(node: &Node) -> SocketAddr {
    node.resp.expect("a RESP listener")
}

async fn client(node: &Node) -> Client {
    Client::connect(resp(node)).await.expect("connect")
}

fn bulk(s: &str) -> Value {
    Value::Bulk(Some(s.as_bytes().to_vec().into()))
}

fn owner(nodes: &[Node], shard: u16) -> String {
    let owner = nodes[0].cluster.owner("shard", &shard_key(shard), &Placement::default());
    owner.expect("every shard has an owner in the view").to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_reach_the_owner_from_any_node() {
    let nodes = start(3).await;
    let mut clients = Vec::new();
    for node in &nodes {
        clients.push(client(node).await);
    }
    for j in 0..300 {
        let (key, value) = (format!("key:{j}"), format!("value {j}"));
        assert_eq!(clients[j % 3].call(&[b"SET", key.as_bytes(), value.as_bytes()]).await.unwrap(), Value::ok());
    }
    for j in 0..300 {
        let key = format!("key:{j}");
        let got = clients[(j + 1) % 3].call(&[b"GET", key.as_bytes()]).await.unwrap();
        assert_eq!(got, bulk(&format!("value {j}")), "{key} read through another node");
    }
    for c in &mut clients {
        assert_eq!(c.call(&[b"DBSIZE"]).await.unwrap(), Value::Int(300));
    }

    let c = &mut clients[1];
    assert_eq!(c.call(&[b"MSET", b"{t}a", b"1", b"{t}b", b"2"]).await.unwrap(), Value::ok());
    let got = c.call(&[b"MGET", b"{t}a", b"{t}b", b"{t}c"]).await.unwrap();
    assert_eq!(got, Value::Array(Some(vec![bulk("1"), bulk("2"), Value::Bulk(None)])));
    let crossslot = Value::err("CROSSSLOT Keys in request don't hash to the same slot");
    assert_eq!(c.call(&[b"MSET", b"a", b"1", b"b", b"2"]).await.unwrap(), crossslot);
    assert_eq!(c.call(&[b"DEL", b"a", b"b"]).await.unwrap(), crossslot);
    assert_eq!(c.call(&[b"EXISTS", b"{t}a", b"{t}a", b"{t}c"]).await.unwrap(), Value::Int(2));
    assert_eq!(c.call(&[b"DEL", b"{t}a", b"{t}c"]).await.unwrap(), Value::Int(1));

    // Pipelined: every command written before any reply is read, the replies in the order sent.
    for i in 0..100 {
        let key = format!("p:{i}");
        c.send(&[b"SET", key.as_bytes(), i.to_string().as_bytes()]).await.unwrap();
        c.send(&[b"GET", key.as_bytes()]).await.unwrap();
    }
    for i in 0..100 {
        assert_eq!(c.read().await.unwrap(), Value::ok());
        assert_eq!(c.read().await.unwrap(), bulk(&i.to_string()));
    }

    assert_eq!(c.call(&[b"CLUSTER", b"KEYSLOT", b"foo"]).await.unwrap(), Value::Int(12182));
    let Value::Array(Some(ranges)) = c.call(&[b"CLUSTER", b"SLOTS"]).await.unwrap() else { panic!("an array") };
    let mut next = 0;
    for range in ranges {
        let Value::Array(Some(range)) = range else { panic!("a range") };
        let (Value::Int(first), Value::Int(last)) = (&range[0], &range[1]) else { panic!("slots") };
        assert_eq!(*first, next, "the ranges cover every slot in order");
        let Value::Array(Some(node)) = &range[2] else { panic!("a node") };
        let (Value::Int(port), Value::Bulk(Some(id))) = (&node[1], &node[2]) else { panic!("port and id") };
        let id = String::from_utf8(id.to_vec()).unwrap();
        for shard in (*first as u16 / SLOTS_PER_SHARD)..=(*last as u16 / SLOTS_PER_SHARD) {
            assert_eq!(owner(&nodes, shard), id, "shard {shard}");
        }
        let resp = nodes.iter().find(|n| n.cluster.identity().node_id.as_ref() == id).map(resp).unwrap();
        assert_eq!(*port, i64::from(resp.port()));
        next = last + 1;
    }
    assert_eq!(next, 16384);

    // CLUSTER SHARDS: one entry per node that owns slots, its ranges flat, and the node's fields as Redis 7.4 has them.
    let Value::Array(Some(entries)) = c.call(&[b"CLUSTER", b"SHARDS"]).await.unwrap() else { panic!("an array") };
    assert_eq!(entries.len(), 3, "a shard per node");
    let mut covered = vec![false; 16384];
    for entry in entries {
        let Value::Array(Some(entry)) = entry else { panic!("an entry") };
        assert_eq!((&entry[0], &entry[2]), (&bulk("slots"), &bulk("nodes")));
        let (Value::Array(Some(slots)), Value::Array(Some(members))) = (&entry[1], &entry[3]) else { panic!("arrays") };
        let [Value::Array(Some(fields))] = &members[..] else { panic!("one node: no replicas") };
        let field = |name: &str| fields.chunks(2).find(|f| f[0] == bulk(name)).map(|f| f[1].clone());
        let Some(Value::Bulk(Some(id))) = field("id") else { panic!("an id") };
        let id = String::from_utf8(id.to_vec()).unwrap();
        let resp = nodes.iter().find(|n| n.cluster.identity().node_id.as_ref() == id).map(resp).unwrap();
        assert_eq!(field("port"), Some(Value::Int(i64::from(resp.port()))));
        assert_eq!(field("ip"), Some(bulk("127.0.0.1")));
        assert_eq!((field("role"), field("health")), (Some(bulk("master")), Some(bulk("online"))));
        for range in slots.chunks(2) {
            let [Value::Int(first), Value::Int(last)] = range else { panic!("a range") };
            for slot in *first..=*last {
                assert!(!std::mem::replace(&mut covered[slot as usize], true), "slot {slot} twice");
                assert_eq!(owner(&nodes, slot as u16 / SLOTS_PER_SHARD), id, "slot {slot}");
            }
        }
    }
    assert!(covered.iter().all(|c| *c), "every slot in one shard");

    let Value::Bulk(Some(info)) = c.call(&[b"INFO", b"commandstats"]).await.unwrap() else { panic!("info") };
    assert!(String::from_utf8_lossy(&info).contains("cmdstat_get:calls="));
    assert_eq!(c.call(&[b"FLUSHALL"]).await.unwrap(), Value::ok());
    assert_eq!(c.call(&[b"DBSIZE"]).await.unwrap(), Value::Int(0));
    drop(clients);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// A key in a shard that `node` owns, or one that it does not.
fn key_where(nodes: &[Node], node: usize, local: bool, name: &str) -> String {
    let id = nodes[node].cluster.identity().node_id.to_string();
    let shard = (0..SHARDS).find(|s| (owner(nodes, *s) == id) == local).expect("such a shard");
    let key = format!("{name}{{{}}}", tag_for(shard));
    assert_eq!(shard_of(key.as_bytes()), shard);
    key
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_past_a_frame_go_in_parts_to_a_local_and_a_remote_shard() {
    let nodes = start(3).await;
    let mut c = client(&nodes[0]).await;
    let limits = kv::Limits::default();
    for local in [true, false] {
        let key = key_where(&nodes, 0, local, "big");
        // At the frame's edge: the largest SET in one frame (9 bytes of call and the key), and one byte more.
        for len in [limits.frame_body - 9 - key.len(), limits.frame_body - 8 - key.len(), 5 << 20, 32 << 20] {
            let value: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            assert_eq!(c.call(&[b"SET", key.as_bytes(), &value]).await.unwrap(), Value::ok(), "{len} local {local}");
            let got = c.call(&[b"GET", key.as_bytes()]).await.unwrap();
            assert!(got == Value::Bulk(Some(value.into())), "{len} bytes read back whole, local {local}");
        }
        let over = vec![1u8; (32 << 20) + 1];
        assert_eq!(c.call(&[b"SET", key.as_bytes(), &over]).await.unwrap(), Value::err("ERR value too large"));

        let (a, b) = (format!("a{key}"), format!("b{key}"));
        let five = vec![5u8; 5 << 20];
        assert_eq!(c.call(&[b"MSET", a.as_bytes(), &five, b.as_bytes(), &five]).await.unwrap(), Value::ok());
        let both = c.call(&[b"MGET", a.as_bytes(), b.as_bytes()]).await.unwrap();
        let five = Value::Bulk(Some(five.into()));
        assert!(both == Value::Array(Some(vec![five.clone(), five])), "MGET of two 5 MiB values, local {local}");
        let twenty = vec![2u8; 20 << 20];
        // One at a time: both in one MSET would pass the 33 MiB command limit.
        for k in [&a, &b] {
            assert_eq!(c.call(&[b"SET", k.as_bytes(), &twenty]).await.unwrap(), Value::ok());
        }
        let refused = c.call(&[b"MGET", a.as_bytes(), b.as_bytes()]).await.unwrap();
        assert_eq!(refused, Value::err("ERR reply too large"), "40 MiB is past the 33 MiB reply limit");
    }
    // Per shard: four SETs past a frame and an MSET went as uploads (an upload tried again counts again), and the
    // GETs of 5 and 32 MiB and the MGET of 10 MiB came back in parts.
    assert!(nodes[0].stats.uploads.load(Relaxed) >= 12);
    assert_eq!(nodes[0].stats.parts_replies.load(Relaxed), 6);
    // The connection still works after the refusals.
    assert_eq!(c.call(&[b"PING"]).await.unwrap(), Value::Simple("PONG".into()));
    drop(c);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// I7: in memory mode a shard that moves starts empty, and the node says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_loses_a_moved_shard_and_says_so() {
    let mut nodes = start(3).await;
    let key = key_where(&nodes, 2, true, "lost");
    let mut c = client(&nodes[0]).await;
    assert_eq!(c.call(&[b"SET", key.as_bytes(), b"here"]).await.unwrap(), Value::ok());
    assert_eq!(c.call(&[b"GET", key.as_bytes()]).await.unwrap(), bulk("here"));
    let before: u64 = nodes[..2].iter().map(|n| n.stats.shard_starts.load(Relaxed)).sum();

    nodes.pop().expect("n3").crash();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let got = loop {
        match c.call(&[b"GET", key.as_bytes()]).await.unwrap() {
            Value::Error(e) if tokio::time::Instant::now() < deadline => {
                assert!(e.starts_with("TRYAGAIN") || e.starts_with("CLUSTERDOWN"), "{e}");
            }
            got => break got,
        }
    };
    assert_eq!(got, Value::Bulk(None), "the moved shard started empty");
    let after: u64 = nodes.iter().map(|n| n.stats.shard_starts.load(Relaxed)).sum();
    assert!(after > before, "the survivors count the shards they started: {before} then {after}");
    let Value::Bulk(Some(info)) = c.call(&[b"INFO", b"kv"]).await.unwrap() else { panic!("info") };
    assert!(String::from_utf8_lossy(&info).contains("mode:memory"));
    drop(c);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
