//! `kv console` against three kv nodes in one process, on real time: where each shard runs, the load each node takes,
//! a node that ends, and the bench runs it reads from the results directory. And axon's own console (`kv node
//! --console`) on the first node: where each shard runs, as every node's Telemetry tells it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axon_cluster::MemoryDiscovery;
use axon_net::SharedSecret;
use kv::client::{Client, warm};
use kv::console::{self, ConsoleConfig};
use kv::node::{self, Node, Settings, Store};
use kv::resp::Value;
use serde_json::Value as Json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn start(n: usize) -> Vec<Node> {
    start_with(n, false).await
}

/// `axon_console`: axon's console on n1, and every node's Telemetry and health port, as `run.sh` starts them.
async fn start_with(n: usize, axon_console: bool) -> Vec<Node> {
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
            health: axon_console.then(|| ([127, 0, 0, 1], 0).into()),
            console: (axon_console && id == "n1").then(|| ([127, 0, 0, 1], 0).into()),
            ..Settings::default()
        };
        let mut transport = axon_net::Config::new(id.as_str(), SharedSecret::new("kv-console-test-secret").unwrap());
        transport.allow_plaintext = true; // loopback
        nodes.push(
            node::start(&settings, transport, discovery.clone(), Store::memory()).await.expect("the node starts"),
        );
    }
    let addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.resp.expect("a RESP listener")).collect();
    warm(&addrs, Duration::from_secs(60)).await.expect("every shard answers");
    nodes
}

fn results() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/runs")
}

async fn console(addrs: Vec<SocketAddr>) -> console::Console {
    console::start(ConsoleConfig {
        addrs,
        bind: ([127, 0, 0, 1], 0).into(),
        results: results(),
        password: None,
        // Five samples a second, so the test sees rates within a second; `kv console` asks once a second.
        every: Duration::from_millis(200),
    })
    .await
    .expect("the console starts")
}

/// One HTTP/1.1 GET: the status, the content type and the body.
async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).await.unwrap();
    let answer = String::from_utf8_lossy(&answer).into_owned();
    let (head, body) = answer.split_once("\r\n\r\n").expect("a head and a body");
    let status = head.split(' ').nth(1).and_then(|s| s.parse().ok()).expect("a status");
    let kind = head
        .lines()
        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-type: ").map(str::to_owned))
        .unwrap_or_default();
    (status, kind, body.to_owned())
}

async fn json(addr: SocketAddr, path: &str) -> Json {
    let (status, kind, body) = get(addr, path).await;
    assert_eq!(status, 200, "{path}: {body}");
    assert!(kind.starts_with("application/json"), "{path}: {kind}");
    serde_json::from_str(&body).expect("JSON")
}

/// Asks `path` until `ok` holds of it, for up to `within`.
async fn until(addr: SocketAddr, path: &str, within: Duration, ok: impl Fn(&Json) -> bool) -> Json {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let got = json(addr, path).await;
        if ok(&got) {
            return got;
        }
        assert!(tokio::time::Instant::now() < deadline, "{path} not as expected within {within:?}: {got:#}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The node each shard runs on, from `/api/cluster`: "" for none, the nodes joined by a space for two.
fn hosts(cluster: &Json) -> Vec<String> {
    let shards = cluster["shards"].as_array().expect("shards");
    shards
        .iter()
        .map(|s| {
            let hosts = s["hosts"].as_array().expect("hosts");
            hosts.iter().map(|h| h["node"].as_str().unwrap().to_owned()).collect::<Vec<_>>().join(" ")
        })
        .collect()
}

/// The rows in summary.md's list of rows with errors.
fn summary_rows_with_errors(run: &str) -> usize {
    let summary = std::fs::read_to_string(results().join(run).join("summary.md")).unwrap();
    let (_, listed) = summary.split_once("## Rows with errors").expect("a list of rows with errors");
    let table = listed.lines().skip_while(|l| !l.starts_with('|')).take_while(|l| l.starts_with('|'));
    table.count() - 2
}

fn all_placed_on(cluster: &Json, on: &[&str]) -> bool {
    let hosts = hosts(cluster);
    hosts.len() == 256 && hosts.iter().all(|h| on.contains(&h.as_str()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_shows_each_shards_node_the_load_and_a_node_that_ends() {
    let mut nodes = start(3).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.resp.unwrap()).collect();
    let console = console(addrs.clone()).await;
    let at = console.addr;

    let cluster = until(at, "/api/cluster", Duration::from_secs(10), |c| all_placed_on(c, &["n1", "n2", "n3"])).await;
    let listed: Vec<&str> = cluster["nodes"].as_array().unwrap().iter().map(|n| n["id"].as_str().unwrap()).collect();
    assert_eq!(listed, ["n1", "n2", "n3"]);
    let here: u64 = cluster["nodes"].as_array().unwrap().iter().map(|n| n["shards_here"].as_u64().unwrap()).sum();
    assert_eq!(here, 256, "each node's count adds up to every shard once");
    for (node, addr) in cluster["nodes"].as_array().unwrap().iter().zip(&addrs) {
        assert_eq!(node["addr"], addr.to_string());
        assert_eq!(node["up"], true);
        assert_eq!(node["mode"], "memory");
        assert_eq!(node["info"]["storage_set"], "n1,n2,n3");
    }
    assert_eq!(cluster["slots_per_shard"], 64);

    // Load through n1: the rates and the history show reads and writes, and the console's own INFO is not counted.
    let load = tokio::spawn(async move {
        let mut c = Client::connect(addrs[0]).await.unwrap();
        for j in 0..2000u32 {
            let key = format!("load:{j}");
            assert_eq!(c.call(&[b"SET", key.as_bytes(), b"v"]).await.unwrap(), Value::ok());
            c.call(&[b"GET", key.as_bytes()]).await.unwrap();
        }
    });
    let busy = |c: &Json| {
        let rate = |what: &str| -> f64 {
            c["nodes"].as_array().unwrap().iter().map(|n| n["rate"][what].as_f64().unwrap_or(0.0)).sum()
        };
        rate("writes") > 10.0 && rate("reads") > 10.0
    };
    until(at, "/api/cluster", Duration::from_secs(10), busy).await;
    load.await.unwrap();
    let ops = json(at, "/api/ops?minutes=1").await;
    assert_eq!(ops["every_ms"], 200, "a minute in samples as they came");
    let written: f64 = ops["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|n| n["points"].as_array().unwrap().iter().filter_map(|p| p[2].as_f64()))
        .sum();
    assert!(written > 10.0, "the history holds the writes: {ops:#}");
    // At most 360 points a node: five minutes in buckets of 1 s, an hour in buckets of 10 s.
    assert_eq!(json(at, "/api/ops?minutes=5").await["every_ms"], 1000);
    assert_eq!(json(at, "/api/ops?minutes=60").await["every_ms"], 10_000);
    // At rest every rate is 0, while the console asks each node INFO five times a second.
    until(at, "/api/cluster", Duration::from_secs(10), |c| {
        let rates = c["nodes"].as_array().unwrap().iter().map(|n| &n["rate"]);
        rates.flat_map(|r| ["reads", "writes", "failed"].map(|k| r[k].as_f64())).all(|v| v == Some(0.0))
    })
    .await;

    // A node that ends: it shows as not answering, and its shards on the others once they move.
    nodes.pop().expect("n3").crash();
    let moved = until(at, "/api/cluster", Duration::from_secs(30), |c| {
        c["nodes"][2]["up"] == false && all_placed_on(c, &["n1", "n2"])
    })
    .await;
    let gone = &moved["nodes"][2];
    assert_eq!(gone["id"], "n3", "a node that does not answer keeps the name it had");
    assert!(gone["error"].as_str().is_some_and(|e| !e.is_empty()), "{gone:#}");
    assert!(gone["down_ms"].as_u64().is_some_and(|d| d >= gone["seen_ms"].as_u64().unwrap()), "{gone:#}");
    assert_eq!(gone["shards_here"], Json::Null, "nothing is claimed for a node that does not answer");

    console.stop();
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn axons_console_on_the_first_node_shows_where_each_shard_runs() {
    let nodes = start_with(3, true).await;
    let at = nodes[0].console.expect("n1 serves axon's console");
    assert!(nodes[1..].iter().all(|n| n.console.is_none()), "only the node given --console serves it");
    // The console's members, and the keys of the kind `shard` that run on each, asked of every node's Telemetry.
    until(at, "/api/overview", Duration::from_secs(20), |o| {
        o["nodes"].as_array().is_some_and(|m| m.len() == 3 && m.iter().all(|n| n["reachable"] == true))
    })
    .await;
    // Each shard where the view places it: a shard that started on n1 before n2 and n3 joined may still be moving.
    let placed = |key: &Json| {
        let shard: u16 = key["key"].as_str().and_then(|k| k.parse().ok()).expect("a shard's key is its number");
        let owner = nodes[0].cluster.owner("shard", &kv::slot::shard_key(shard), &Default::default());
        owner.is_some_and(|o| key["node"] == o.as_ref())
    };
    let keys = until(at, "/api/keys?kind=shard&limit=1000", Duration::from_secs(20), |k| {
        k["keys"].as_array().is_some_and(|k| k.len() == 256 && k.iter().all(&placed))
    })
    .await;
    assert_eq!(keys["errors"], serde_json::json!({}), "every node answers: {keys:#}");
    for key in keys["keys"].as_array().unwrap() {
        assert!(key["epoch"].as_u64().is_some(), "under PaxosLeases each key has its lease epoch: {key}");
    }
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_console_reads_the_runs_and_serves_its_pages() {
    // Nothing listens here: the node shows as not answering, and the runs do not need it.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let console = console(vec![closed]).await;
    let at = console.addr;

    let runs = json(at, "/api/runs").await;
    let list = runs["runs"].as_array().expect("runs");
    let names: Vec<&str> = list.iter().map(|r| r["name"].as_str().unwrap()).collect();
    let mut sorted = names.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(names, sorted, "newest first");
    let run = list.iter().find(|r| r["name"] == "2026-10-11-epyc-4484px-full").expect("the committed full run");
    assert_eq!(run["host"], "epyc-4484px");
    assert_eq!(run["kind"], "full");
    assert_eq!(run["rounds"], 2);
    assert_eq!(run["summary"], true);
    let csv = std::fs::read_to_string(results().join("2026-10-11-epyc-4484px-full/results.csv")).unwrap();
    assert_eq!(run["rows"], csv.lines().count() - 1);
    let listed_apart = summary_rows_with_errors("2026-10-11-epyc-4484px-full");
    assert_eq!(run["error_rows"], listed_apart, "the rows with errors are those summary.md lists apart");
    assert!(run["faults"]["phases"].as_u64().unwrap() >= 7, "{run:#}");
    assert!(run["targets"].as_array().unwrap().iter().any(|t| t == "kv-durable-3"));

    let one = json(at, "/api/runs/2026-10-11-epyc-4484px-full").await;
    assert!(one["summary"].as_str().unwrap().starts_with("# kv bench"));
    assert!(one["env"].as_str().unwrap().contains("host: epyc-4484px"));
    let columns: Vec<&str> = one["columns"].as_array().unwrap().iter().map(|c| c.as_str().unwrap()).collect();
    assert_eq!(columns[..5], ["date", "host", "run_kind", "round", "target"]);
    let rows = one["rows"].as_array().unwrap();
    assert_eq!(rows.len(), csv.lines().count() - 1);
    assert!(rows.iter().all(|r| r.as_array().unwrap().len() == columns.len()), "a quoted comma stays in its field");
    let bound = columns.iter().position(|c| *c == "bound").unwrap();
    assert!(rows.iter().any(|r| r[bound] == "stub-bound, not client-bound"));
    assert!(one["faults"].as_array().unwrap().iter().any(|f| f["phase"] == "kill" && f["pass"] == true));

    for path in ["/api/runs/nothing-here", "/api/runs/..%2Fsrc", "/api/runs/.", "/api/nothing"] {
        let (status, kind, body) = get(at, path).await;
        assert_eq!(status, 404, "{path}: {body}");
        assert!(kind.starts_with("application/json"), "{path}: {kind}");
        assert_eq!(serde_json::from_str::<Json>(&body).unwrap()["error"], "not_found");
    }

    let cluster = until(at, "/api/cluster", Duration::from_secs(5), |c| c["nodes"][0]["error"].is_string()).await;
    assert_eq!(cluster["nodes"][0]["up"], false);
    assert_eq!(cluster["nodes"][0]["id"], Json::Null, "a node never seen has no name");

    // A run's name may hold a dot: its page is still the page.
    for page in ["/", "/shards", "/runs/2026-10-11-epyc-4484px-full", "/runs/a-run.v2"] {
        let (status, kind, body) = get(at, page).await;
        assert_eq!((status, kind.as_str()), (200, "text/html; charset=utf-8"), "{page}");
        assert!(body.contains("<div id=\"root\">"), "{page}");
    }
    let (status, ..) = get(at, "/assets/nothing.js").await;
    assert_eq!(status, 404, "a missing file is not answered with the page");
    console.stop();
}
