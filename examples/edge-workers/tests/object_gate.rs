//! An object's answer and the store under it, on one real node with the fake engine and in-process leases: the
//! answer waits for the store, a write the store did not take fails the request, an actor whose copy is not the
//! store's answers nobody from it, and an object gives up when its caller does.

#[path = "support/real.rs"]
mod real;
mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon_cluster::{ClusterConfig, MemoryDiscovery, MemoryLeases};
use axon_testkit::eventually;
use edge_workers::engine::fake::{FakeEngine, FakeVersion};
use edge_workers::engine::{Limits, Request};
use edge_workers::gateway::{Call, Response};
use edge_workers::node::{self, Client, Roles, Settings, Stores};
use edge_workers::object::Caller;
use edge_workers::wire::{Invocation, OUTCOMES, STORE, StoreApi, StoreAsk, StoreTold, Upload};
use real::{Faulty, Member, Mode};
use support::counter;

const KEY: &str = "counter/Counter/a";

/// One node of every role, serving the counter. `tune` changes the cluster's and the node's settings from their
/// defaults. The store's directory comes back beside it.
async fn start(tune: impl FnOnce(&mut ClusterConfig, &mut Settings)) -> (Member<FakeVersion>, Arc<Faulty>) {
    let roles = Roles { control: true, gateway: true, runner: true };
    let mut cfg = real::config("one", roles, &MemoryDiscovery::new());
    cfg.ownership = Some(Arc::new(MemoryLeases::new().authority()));
    let mut settings = Settings::default();
    tune(&mut cfg, &mut settings);
    let disk = Arc::new(Faulty::default());
    let stores = Stores { objects: disk.clone(), ..real::memory_stores() };
    let one = real::member(cfg, roles, stores, FakeEngine::new().scoped("counter", counter), settings).await;
    // The fake engine's compute is what a request's `cpu-ms` header says: room for seconds of it.
    let limits = Some(Limits { cpu: Duration::from_secs(10), ..Limits::default() });
    let source = Some("counter".into());
    let upload = Upload { name: "counter".into(), source, limits, deploy: true, ..Upload::default() };
    let (_, seq) = Client::new(&one.cluster).upload(upload).await.unwrap();
    let node = &one.node;
    eventually("the counter is served", async || node.serving.as_ref().unwrap().seq("counter") == seq).await;
    node.gateway.as_ref().unwrap().wait_runners(|runners| *runners == 1).await;
    (one, disk)
}

impl Member<FakeVersion> {
    /// One request for the object `a` through the gateway, to be answered within `deadline`.
    async fn ask(&self, request: Request, deadline: Duration) -> Response {
        let call = Call { account: "test".into(), worker: "counter".into(), request, deadline: Some(deadline) };
        self.node.gateway.as_ref().unwrap().call(call).await
    }

    async fn call(&self, method: &str) -> Response {
        self.ask(Request { method: method.into(), ..Request::get("/a") }, Duration::from_secs(5)).await
    }

    async fn store(&self, ask: StoreAsk) -> StoreTold {
        let store = node::on_control::<StoreApi>(&self.cluster, STORE);
        store.request_within(ask, Duration::from_secs(10)).await.unwrap()
    }

    async fn stored(&self) -> Vec<(String, String)> {
        match self.store(StoreAsk::Read { object: KEY.into() }).await {
            StoreTold::Snapshot { entries, .. } => entries,
            other => panic!("{other:?}"),
        }
    }
}

fn said(response: &Response) -> (u16, String) {
    (response.status, String::from_utf8_lossy(&response.body).into_owned())
}

fn count(n: u64) -> (u16, String) {
    (200, n.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_waits_for_the_store_and_a_write_that_fails_fails_the_request_at_once() {
    let (one, disk) = start(|_, _| {}).await;
    assert_eq!(said(&one.call("POST").await), count(1));
    assert_eq!(one.stored().await, [("n".to_owned(), "1".to_owned())]);

    // The store's disk refuses writes: the request that counted to 2 is not answered with it. The store said no,
    // so the object does not ask again, and the caller is told long before its 5 s are up.
    disk.set(Mode::Refuses);
    let began = std::time::Instant::now();
    let failed = one.call("POST").await;
    let (status, why) = said(&failed);
    assert!(status == 500 && why.contains("the store refused the writes"), "{status}: {why}");
    assert!(began.elapsed() < Duration::from_millis(2500), "told after {:?}", began.elapsed());
    assert_eq!(one.stored().await, [("n".to_owned(), "1".to_owned())]);

    // And the count it failed with is not kept by the object either: the next request counts from what is stored.
    disk.set(Mode::Takes);
    assert_eq!(said(&one.call("POST").await), count(2));
    assert_eq!(one.stored().await, [("n".to_owned(), "2".to_owned())]);
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_whose_storage_was_cleared_answers_no_request_from_its_copy_and_an_older_epoch_is_fenced() {
    let (one, _) = start(|_, _| {}).await;
    assert_eq!(said(&one.call("POST").await), count(1));
    assert_eq!(said(&one.call("POST").await), count(2));
    assert_eq!(said(&one.call("GET").await), count(2));

    // The operator removes what it stores. The actor holds a copy of it, and its next request only reads: the
    // store is asked all the same, and says the copy is of a version that is no more. The request fails.
    assert_eq!(one.store(StoreAsk::Clear { object: KEY.into() }).await, StoreTold::Written { version: 0 });
    let (status, why) = said(&one.call("GET").await);
    assert!(status == 500 && why.contains("storage is at version 0, and this actor read version 2"), "{status}: {why}");
    // The actor let go of its copy, and reads the store for the next: nothing, so it counts from there.
    assert_eq!(said(&one.call("GET").await), count(0));
    assert_eq!(one.stored().await, []);
    assert_eq!(said(&one.call("POST").await), count(1));

    // The same for a request that writes.
    one.store(StoreAsk::Clear { object: KEY.into() }).await;
    let (status, why) = said(&one.call("POST").await);
    assert!(status == 500 && why.contains("storage is at version 0, and this actor read version 1"), "{status}: {why}");
    assert_eq!(one.stored().await, []);
    assert_eq!(said(&one.call("POST").await), count(1));

    // A write under an epoch older than the store has seen is refused, whoever sends it.
    let StoreTold::Snapshot { epoch, version, .. } = one.store(StoreAsk::Read { object: KEY.into() }).await else {
        panic!("a snapshot")
    };
    let ops = vec![("n".into(), Some("9".into()))];
    let stale = StoreAsk::Write { object: KEY.into(), epoch: epoch - 1, base: version, ops };
    assert_eq!(one.store(stale).await, StoreTold::Fenced { seen: epoch });
    assert_eq!(one.stored().await, [("n".to_owned(), "1".to_owned())]);
    one.system.shutdown().await.unwrap();
}

fn post(headers: &[(&str, &str)]) -> Request {
    let headers = headers.iter().map(|(name, value)| (name.to_string(), value.to_string())).collect();
    Request { method: "POST".into(), headers, ..Request::get("/a") }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_call_lasts_a_request_timeout_at_most_and_the_object_gives_up_with_its_caller() {
    // A try of `KeyedRef::request_until` waits no longer than this, whatever the deadline. Short, so the test
    // meets it in a second and not in the shipped five.
    let (one, _) = start(|cfg, _| cfg.request_timeout = Duration::from_secs(1)).await;
    assert_eq!(said(&one.call("POST").await), count(1));

    // The object would compute for three seconds, and its caller's handler has five. The call ends at the timeout,
    // and so does the object's run: what it would have counted is not stored after its caller was told it failed.
    let slow = one.ask(post(&[("cpu-ms", "3000")]), Duration::from_secs(5)).await;
    assert_eq!(slow.status, 500, "{:?}", said(&slow));
    assert_eq!(said(&one.call("POST").await), count(2), "the next request waits for no run, and counts from 1");
    assert_eq!(one.stored().await, [("n".to_owned(), "2".to_owned())]);
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_of_objects_shares_the_first_request_s_deadline() {
    let (one, _) = start(|_, _| {}).await;
    // `a` calls `b`, which would compute for three seconds, and the request has half of one: `b` is given what is
    // left of that, not a time of its own, so neither of them counts.
    let chained = one.ask(post(&[("then", "b"), ("cpu-ms", "3000")]), Duration::from_millis(500)).await;
    assert!(chained.status >= 500, "{:?}", said(&chained));
    let b = Request { method: "POST".into(), ..Request::get("/b") };
    assert_eq!(said(&one.ask(b, Duration::from_secs(5)).await), count(1), "b ran on, and stored its count");
    assert_eq!(said(&one.call("POST").await), count(1));
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_s_runs_keep_its_worker_loaded_and_are_counted_with_the_worker_s() {
    // A worker nobody called for this long is dropped from the engine. Short, so the test outlasts it four times.
    let idle = Duration::from_millis(250);
    let (one, disk) = start(|_, settings| settings.invoker.idle = idle).await;
    // The object alone is called, as from a handler on another runner: this runner's invoker runs nothing.
    let caller = Caller::new(&one.cluster, 16);
    let serving = one.node.serving.as_ref().unwrap();
    let (began, mut calls, mut evicted) = (std::time::Instant::now(), 0, 0);
    while began.elapsed() < 4 * idle {
        let invocation = Invocation {
            worker: "counter".into(),
            method: "POST".into(),
            url: "/a".into(),
            headers: Vec::new(),
            sample: 0,
            body: Default::default(),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let ran = caller.call(KEY, invocation, deadline).await.unwrap();
        calls += 1;
        assert_eq!((ran.failure, &ran.body[..]), (None, calls.to_string().as_bytes()));
        if calls == 1 {
            // From the first run on: the node may have been idle for longer than `idle` before it.
            evicted = serving.stats.evicted.load(Relaxed);
        }
    }
    let dropped = serving.stats.evicted.load(Relaxed) - evicted;
    assert_eq!(dropped, 0, "dropped under an object that ran {calls} times in {:?}", began.elapsed());
    let stats = one.node.invoked.as_ref().unwrap().snapshot("one");
    let counted = &stats.workers["counter"];
    assert_eq!((counted.outcomes[0], stats.logs_published), (calls, calls), "{OUTCOMES:?}: {counted:?}");

    // A request the store did not take is one of the worker's failures.
    disk.set(Mode::Refuses);
    assert_eq!(one.call("POST").await.status, 500);
    let stats = one.node.invoked.as_ref().unwrap().snapshot("one");
    let storage = OUTCOMES.iter().position(|outcome| *outcome == "storage").unwrap();
    assert_eq!(stats.workers["counter"].outcomes[storage], 1, "{:?}", stats.workers["counter"]);
    one.system.shutdown().await.unwrap();
}
