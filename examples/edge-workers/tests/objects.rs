//! Durable objects on real nodes over loopback TCP, with the real engine and real leases: a control node and two
//! gateways that are the lease acceptors, and three runners. A worker's handler reaches its objects through
//! `env.COUNTER`; each test deploys through the registry and calls through the gateways or a runner's invoker.

#[path = "support/real.rs"]
mod real;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axon::files::OsFiles;
use axon_cluster::{MemoryDiscovery, PaxosConfig, PaxosLeases};
use axon_testkit::eventually_within;
use bytes::Bytes;
use edge_workers::engine::Request;
use edge_workers::engine::starling::Version;
use edge_workers::gateway::{Call, Response};
use edge_workers::node::{self, Client, Roles, Settings};
use edge_workers::object::Caller;
use edge_workers::wire::{
    Ask, INVOKE, Invocation, InvokeApi, MAX_VALUE, STORE, StoreApi, StoreAsk, StoreTold, Told, Upload,
};
use real::js;

const ACCEPTORS: [&str; 3] = ["c1", "g1", "g2"];

type Member = real::Member<Version>;

async fn member(
    id: &str,
    roles: Roles,
    discovery: &MemoryDiscovery,
    dir: &std::path::Path,
    settings: &Settings,
) -> Member {
    let dir = dir.join(id);
    let mut cfg = real::config(id, roles, discovery);
    let mut paxos = PaxosConfig::new(ACCEPTORS);
    if ACCEPTORS.contains(&id) {
        paxos.store = Some(Arc::new(OsFiles::new(dir.join("leases"))));
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    real::member(cfg, roles, real::os_stores(&dir), real::engine(false), settings.clone()).await
}

struct Edge {
    control: Member,
    gateways: Vec<Member>,
    runners: Vec<Member>,
    _dir: tempfile::TempDir,
}

/// A counter's answer: the object's id and its count.
fn counted(response: &Response) -> (String, u64) {
    let said: serde_json::Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|e| panic!("{e}: {} {:?}", response.status, String::from_utf8_lossy(&response.body)));
    (said["id"].as_str().unwrap().to_owned(), said["n"].as_u64().unwrap())
}

impl Edge {
    async fn start(mut settings: Settings) -> Self {
        settings.invoker.runtime = Some(js());
        let dir = tempfile::tempdir().unwrap();
        let discovery = MemoryDiscovery::new();
        let roles = |control, gateway, runner| Roles { control, gateway, runner };
        let control = member("c1", roles(true, false, false), &discovery, dir.path(), &settings).await;
        let mut gateways = Vec::new();
        for id in ["g1", "g2"] {
            gateways.push(member(id, roles(false, true, false), &discovery, dir.path(), &settings).await);
        }
        let mut runners = Vec::new();
        for id in ["r1", "r2", "r3"] {
            runners.push(member(id, roles(false, false, true), &discovery, dir.path(), &settings).await);
        }
        for gateway in &gateways {
            let edge = gateway.node.gateway.clone().unwrap();
            tokio::time::timeout(Duration::from_secs(20), edge.wait_runners(|n| *n == 3)).await.unwrap();
        }
        // An acceptor that has just started grants nothing for a little over one lease.
        eventually_within("the acceptors grant leases", Duration::from_secs(30), async || {
            !control.cluster.warming() && gateways.iter().all(|gateway| !gateway.cluster.warming())
        })
        .await;
        Self { control, gateways, runners, _dir: dir }
    }

    async fn deploy(&self, upload: Upload) -> u32 {
        let name = upload.name.clone();
        let (version, seq) = Client::new(&self.control.cluster).upload(upload).await.unwrap();
        let seq = seq.unwrap();
        // A runner compiles the bundle before it serves it: seconds, with an unoptimised build around the compiler.
        let what = format!("{name} at {seq} on every gateway and runner");
        eventually_within(&what, Duration::from_secs(90), async || {
            let at = |member: &Member| member.node.serving.as_ref().unwrap().seq(&name) == Some(seq);
            self.gateways.iter().chain(&self.runners).all(at)
        })
        .await;
        version
    }

    async fn counter(&self, source: &str) -> u32 {
        let objects = Some([("COUNTER".to_owned(), "Counter".to_owned())].into());
        let upload =
            Upload { name: "counter".into(), source: Some(source.into()), objects, deploy: true, ..Upload::default() };
        self.deploy(upload).await
    }

    /// One request through a gateway.
    async fn call(&self, gateway: usize, method: &str, path: &str) -> Response {
        let request = Request { method: method.into(), url: path.into(), headers: Vec::new(), body: Bytes::new() };
        let call =
            Call { account: "test".into(), worker: "counter".into(), request, deadline: Some(Duration::from_secs(20)) };
        self.gateways[gateway].node.gateway.as_ref().unwrap().call(call).await
    }

    /// The objects of `counter` that run now: each key with its runner and the epoch of its lease.
    async fn running(&self) -> Vec<(String, String, u64)> {
        // A runner that was killed is silent until the view drops it.
        let (hosted, _) = node::objects(&self.control.cluster, Some("counter"), 100).await;
        hosted.into_iter().map(|(node, object)| (object.key, node, object.epoch.expect("a lease"))).collect()
    }

    async fn stored(&self, key: &str) -> Vec<(String, String)> {
        let store = node::on_control::<StoreApi>(&self.control.cluster, STORE);
        match store.request_within(StoreAsk::Read { object: key.into() }, Duration::from_secs(10)).await.unwrap() {
            StoreTold::Snapshot { entries, .. } => entries,
            other => panic!("{other:?}"),
        }
    }

    async fn stop(self) {
        for member in self.runners.into_iter().chain(self.gateways).chain([self.control]) {
            member.system.shutdown().await.unwrap();
        }
    }
}

/// A counter per room. `POST` adds one; any request answers the count. The worker's handler only passes the
/// request on, and turns what the stub throws into a 599.
const COUNTER: &str = r#"
export class Counter {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const storage = this.state.storage;
    const path = new URL(request.url).pathname;
    if (path === "/big") {
      try {
        await storage.put("big", "x".repeat(70000));
      } catch (e) {
        return new Response("refused: " + e.message, { status: 507 });
      }
      return new Response("stored");
    }
    if (path === "/throws") {
      await storage.put("n", -1);
      throw new Error("on purpose");
    }
    if (path === "/spins") {
      await storage.put("n", -2);
      for (;;) {}
    }
    if (path === "/self") {
      try {
        await this.env.COUNTER.get(this.env.COUNTER.idFromName(this.state.id.name)).fetch("http://object/");
      } catch (e) {
        return new Response("refused: " + e.message, { status: 508 });
      }
      return new Response("it called itself");
    }
    if (path === "/keys") {
      await storage.put("list:a", [1, "two"]);
      await storage.put("list:b", { three: 3 });
      await storage.put("other", null);
      await storage.delete("list:a");
      const listed = await storage.list({ prefix: "list:" });
      return new Response(JSON.stringify([...listed]));
    }
    let n = (await storage.get("n")) ?? 0;
    if (request.method === "POST") {
      n += STEP;
      await storage.put("n", n);
    }
    return new Response(JSON.stringify({ id: this.state.id.name, n }));
  }
}
export default {
  async fetch(request, env) {
    const room = new URL(request.url).searchParams.get("room") ?? "lobby";
    if (new URL(request.url).pathname === "/raw") {
      // Past the shim, as a script that knows the engine could.
      const get = { method: "POST", body: JSON.stringify({ op: "get", key: "n" }) };
      const answer = await fetch("http://edge.internal/storage", get);
      return new Response(await answer.text(), { status: answer.headers.has("x-edge-fault") ? 598 : 200 });
    }
    const stub = env.COUNTER.get(env.COUNTER.idFromName(room));
    try {
      return await stub.fetch(request);
    } catch (e) {
      return new Response(e.message, { status: 599 });
    }
  },
};
"#;

fn counter(step: u32) -> String {
    COUNTER.replace("STEP", &step.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_instance_counts_for_two_runners_and_both_gateways_and_two_ids_are_apart() {
    let edge = Edge::start(Settings::default()).await;
    edge.counter(&counter(1)).await;

    let first = edge.call(0, "POST", "/").await;
    assert_eq!((first.status, counted(&first)), (200, ("lobby".into(), 1)), "{first:?}");
    assert_eq!(counted(&edge.call(1, "POST", "/").await), ("lobby".into(), 2));

    // The same object from the handlers of two runners, asked directly: each runs the worker itself and calls
    // the object wherever it is.
    for (i, runner) in edge.runners.iter().take(2).enumerate() {
        let invoker = edge.control.cluster.resolve_on::<InvokeApi>(&runner.id, INVOKE).await.unwrap();
        let invocation = Invocation {
            worker: "counter".into(),
            method: "POST".into(),
            url: "http://counter.test/".into(),
            headers: Vec::new(),
            sample: 0,
            body: Bytes::new(),
        };
        let Told::Ran(ran) = invoker.request(Ask::Run(invocation)).await.unwrap() else { panic!("a run") };
        let said: serde_json::Value = serde_json::from_slice(&ran.body).unwrap();
        assert_eq!((ran.status, said["n"].as_u64()), (200, Some(3 + i as u64)), "from {}: {ran:?}", runner.id);
    }

    // Requests at once, through both gateways: one at a time in the object, so every count is given out once.
    let calls = (0..20).map(|i| edge.call(i % 2, "POST", "/"));
    let counts: BTreeSet<u64> = futures_util::future::join_all(calls).await.iter().map(|r| counted(r).1).collect();
    assert_eq!(counts, (5..=24).collect::<BTreeSet<u64>>());

    // Another id is another object, with a count of its own.
    assert_eq!(counted(&edge.call(0, "POST", "/?room=b").await), ("b".into(), 1));
    assert_eq!(counted(&edge.call(1, "GET", "/").await), ("lobby".into(), 24));

    // One instance each in the whole cluster, under a lease, and the store has what they wrote.
    let running = edge.running().await;
    let keys: Vec<&str> = running.iter().map(|(key, _, _)| key.as_str()).collect();
    assert_eq!(keys, ["counter/Counter/b", "counter/Counter/lobby"], "{running:?}");
    assert_eq!(edge.stored("counter/Counter/lobby").await, [("n".to_owned(), "24".to_owned())]);
    assert_eq!(edge.stored("counter/Counter/b").await, [("n".to_owned(), "1".to_owned())]);

    // Eight objects on three runners: one of them hosts three at least, and is asked for them one at a time.
    for room in 0..6 {
        assert_eq!(counted(&edge.call(room % 2, "POST", &format!("/?room=r{room}")).await).1, 1);
    }
    let (listed, silent) = node::objects(&edge.control.cluster, Some("counter"), 1).await;
    let keys: Vec<&str> = listed.iter().map(|(_, object)| object.key.as_str()).collect();
    let rooms = ["b", "lobby", "r0", "r1", "r2", "r3", "r4", "r5"].map(|room| format!("counter/Counter/{room}"));
    assert_eq!((keys, silent.len()), (rooms.iter().map(String::as_str).collect(), 0), "every page of every runner");
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_survives_the_object_going_idle_and_the_kill_of_its_runner() {
    let mut settings = Settings::default();
    // An object leaves its runner after a second without a request, so the test sees it go.
    settings.objects.idle = Duration::from_secs(1);
    let mut edge = Edge::start(settings).await;
    edge.counter(&counter(1)).await;
    for n in 1..=3 {
        assert_eq!(counted(&edge.call(0, "POST", "/").await).1, n);
    }
    let (_, _, first_epoch) = edge.running().await.pop().expect("it runs");

    eventually_within("the object went idle", Duration::from_secs(20), async || edge.running().await.is_empty()).await;
    assert_eq!(counted(&edge.call(1, "GET", "/").await).1, 3, "started again, it reads what it stored");
    let (_, host, epoch) = edge.running().await.pop().expect("it runs again");
    assert!(epoch > first_epoch, "a new lease: {epoch} after {first_epoch}");
    assert_eq!(counted(&edge.call(1, "POST", "/").await).1, 4);

    // Its runner is gone without a word: dropped, not shut down. The lease lapses, another runner takes the object,
    // and it has every write that was answered.
    let at = edge.runners.iter().position(|runner| runner.id == host).unwrap();
    drop(edge.runners.remove(at));
    eventually_within("the object answers from another runner", Duration::from_secs(60), async || {
        let response = edge.call(0, "GET", "/").await;
        response.status == 200 && counted(&response).1 == 4
    })
    .await;
    assert_eq!(counted(&edge.call(1, "POST", "/").await).1, 5);
    let (_, moved, after) = edge.running().await.pop().expect("it runs");
    assert!(moved != host && after > epoch, "on {moved} at {after}, after {host} at {epoch}");
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_past_the_limits_is_an_error_in_the_script_and_a_deploy_changes_the_class_and_keeps_the_data() {
    let edge = Edge::start(Settings::default()).await;
    let first = edge.counter(&counter(1)).await;
    assert_eq!(counted(&edge.call(0, "POST", "/").await).1, 1);

    // A value over the limit: `put` throws in the class, which answers as it likes; nothing is stored.
    let big = edge.call(0, "GET", "/big").await;
    let said = String::from_utf8_lossy(&big.body).into_owned();
    assert_eq!(big.status, 507, "{said}");
    assert!(said.contains(&format!("a value over {MAX_VALUE} bytes")), "{said}");
    // A class that throws fails its request: the caller's stub throws, and what the request wrote is not kept.
    let threw = edge.call(0, "GET", "/throws").await;
    let said = String::from_utf8_lossy(&threw.body).into_owned();
    assert!(threw.status == 599 && said.contains("the object failed") && said.contains("on purpose"), "{said}");
    assert_eq!(edge.stored("counter/Counter/lobby").await, [("n".to_owned(), "1".to_owned())]);
    // So does a class that ran past its CPU time after a put.
    let spun = edge.call(0, "GET", "/spins").await;
    let said = String::from_utf8_lossy(&spun.body).into_owned();
    assert!(spun.status == 599 && said.contains("the object exceeded its CPU time limit"), "{}: {said}", spun.status);
    assert_eq!(edge.stored("counter/Counter/lobby").await, [("n".to_owned(), "1".to_owned())]);
    // An object that calls itself would wait for its own answer: refused at once, as an error in the class.
    let own = edge.call(0, "GET", "/self").await;
    let said = String::from_utf8_lossy(&own.body).into_owned();
    assert!(own.status == 508 && said.contains("an object cannot call itself"), "{}: {said}", own.status);
    // An id is 1 to 128 bytes, and storage is an object's: a handler that asks for it past the shim is refused.
    for room in [String::new(), "x".repeat(129)] {
        let refused = edge.call(0, "GET", &format!("/?room={room}")).await;
        let said = String::from_utf8_lossy(&refused.body).into_owned();
        assert!(
            refused.status == 599 && said.contains("an object's id is 1 to 128 bytes"),
            "{}: {said}",
            refused.status
        );
    }
    assert_eq!(counted(&edge.call(0, "GET", &format!("/?room={}", "x".repeat(128))).await).1, 0);
    let raw = edge.call(0, "GET", "/raw").await;
    let said = String::from_utf8_lossy(&raw.body).into_owned();
    assert!(raw.status == 598 && said.contains("storage belongs to an object"), "{}: {said}", raw.status);
    // Values are JSON, a delete and a list by prefix see the request's own writes.
    let keys = edge.call(0, "GET", "/keys").await;
    assert_eq!(String::from_utf8_lossy(&keys.body), r#"[["list:b",{"three":3}]]"#);
    let stored: Vec<String> = edge.stored("counter/Counter/lobby").await.into_iter().map(|(key, _)| key).collect();
    assert_eq!(stored, ["list:b", "n", "other"]);

    // A new version of the class: the object runs it from its next request, on what the old one stored.
    let second = edge.counter(&counter(10)).await;
    assert!(second > first);
    let after = edge.call(1, "POST", "/").await;
    assert_eq!((counted(&after).1, after.version), (11, Some(second)));
    assert_eq!(counted(&edge.call(0, "GET", "/").await).1, 11);

    // A binding to a class the module does not export is refused at the upload.
    let objects = Some([("COUNTER".to_owned(), "Missing".to_owned())].into());
    let upload = Upload { name: "counter".into(), objects, deploy: true, ..Upload::default() };
    let refused = Client::new(&edge.control.cluster).upload(upload).await.unwrap_err();
    assert!(refused.to_string().contains("exports no class Missing"), "{refused}");

    // The worker is removed while its object runs: the object's next request is told so, and its actor ends.
    Client::new(&edge.control.cluster).delete("counter").await.unwrap();
    eventually_within("no runner serves the counter", Duration::from_secs(30), async || {
        edge.runners.iter().all(|runner| runner.node.serving.as_ref().unwrap().seq("counter").is_none())
    })
    .await;
    assert!(edge.running().await.iter().any(|(key, _, _)| key == "counter/Counter/lobby"), "it runs still");
    let invocation = Invocation {
        worker: "counter".into(),
        method: "GET".into(),
        url: "http://counter.test/".into(),
        headers: Vec::new(),
        sample: 0,
        body: Bytes::new(),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let told = Caller::new(&edge.gateways[0].cluster, 4).call("counter/Counter/lobby", invocation, deadline).await;
    assert_eq!(told.unwrap_err(), "the object's worker is not deployed");
    eventually_within("the object ended", Duration::from_secs(20), async || {
        edge.running().await.iter().all(|(key, _, _)| key != "counter/Counter/lobby")
    })
    .await;
    edge.stop().await;
}
