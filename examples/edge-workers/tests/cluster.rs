//! Real nodes over loopback TCP. The first test runs the whole path with the real engine: an upload compiled on the
//! control node, its 12 MB bundle fetched in pieces by a runner, and a request in flight across a switch. The
//! second gives a runner a stream and a bundle store that misbehave.

#[path = "support/real.rs"]
mod real;

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::{FileOp, Files, MemoryFiles};
use axon_cluster::{Cluster, MemoryDiscovery};
use axon_stream::{DeliveryPolicy, StreamNode, StreamNodeConfig};
use axon_testkit::{eventually, eventually_within};
use bytes::Bytes;
use edge_workers::engine::fake::{FakeEngine, FakeRun};
use edge_workers::engine::starling::Starling;
use edge_workers::engine::{Engine, Env, Limits, Request};
use edge_workers::follower::FollowerConfig;
use edge_workers::node::{self, Client, Roles, Settings, Stores};
use edge_workers::registry::{Bundles, compact_limits};
use edge_workers::wire::{BUNDLES, BundleApi, Hash, Manifest, Upload, VersionRef, manifests};
use real::os_stores;

async fn member(id: &str, roles: Roles, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    real::cluster(real::config(id, roles, discovery)).await
}

const CONTROL: Roles = Roles { control: true, gateway: false, runner: false };
const RUNNER: Roles = Roles { control: false, gateway: false, runner: true };

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_deployed_on_control_runs_on_a_runner_and_a_request_in_flight_ends_on_its_version() {
    let mut cfg = real::starling();
    cfg.max_in_flight = 16;
    let engine = Starling::new(cfg).unwrap_or_else(|e| panic!("{e}"));
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let (control_system, control) = member("control", CONTROL, &discovery).await;
    let (runner_system, runner) = member("runner", RUNNER, &discovery).await;
    runner.wait_view(|view| view.members.len() == 2).await.unwrap();
    node::start(
        &control_system,
        &control,
        CONTROL,
        os_stores(&dir.path().join("control")),
        engine.clone(),
        Settings::default(),
    )
    .await
    .unwrap();
    let node = node::start(
        &runner_system,
        &runner,
        RUNNER,
        os_stores(&dir.path().join("runner")),
        engine.clone(),
        Settings::default(),
    )
    .await
    .unwrap();
    let serving = node.serving.unwrap();

    let source = |version: &str| {
        format!(
            r#"export default {{ async fetch(request, env) {{
                const wait = Number(new URL(request.url).searchParams.get("wait") ?? 0);
                await new Promise(done => setTimeout(done, wait));
                return new Response("{version} " + env.GREETING + " " + env.TOKEN);
            }} }}"#
        )
    };
    let registry = Client::new(&runner);
    let first = Upload {
        name: "hello".into(),
        source: Some(source("v1")),
        vars: Some(Env::from([("GREETING".into(), "hi".into())])),
        secrets: [("TOKEN".to_owned(), Some("s3cret".to_owned()))].into(),
        deploy: true,
        ..Upload::default()
    };
    let uploaded = std::time::Instant::now();
    assert_eq!(registry.upload(first).await.unwrap(), (1, Some(1)));
    // The runner compiles the bundle before it serves it: seconds, with an unoptimised build around the compiler.
    let minute = Duration::from_secs(60);
    eventually_within("the runner serving v1", minute, async || serving.seq("hello") == Some(1)).await;
    let compile = uploaded.elapsed();
    let v1 = serving.get("hello").unwrap();
    assert!(v1.manifest.stable.size > 1 << 20, "a bundle larger than a frame: it came in pieces");
    assert_eq!((serving.stats.fetched.load(Relaxed), serving.stats.from_cache.load(Relaxed)), (1, 0));
    let run = |current: Arc<edge_workers::follower::Current<_>>, url: String| {
        let engine = engine.clone();
        async move {
            let (version, loaded) = current.pick(0).expect("prepared on a runner");
            let outcome = engine.invoke(loaded, Request::get(&url), version.limits).await.unwrap();
            String::from_utf8(outcome.body.to_vec()).unwrap()
        }
    };
    assert_eq!(run(v1.clone(), "/".into()).await, "v1 hi s3cret");

    // A request that is still waiting when the runner switches ends on the version it started on.
    // It waits three times what the first version took from its upload to the runner, so the switch comes first
    // on a slow machine too, and less than the 30 s a request may take.
    let wait = (compile * 3 + Duration::from_secs(2)).min(Duration::from_secs(25)).as_millis();
    let in_flight = tokio::spawn(run(v1, format!("/?wait={wait}")));
    let second = Upload { name: "hello".into(), source: Some(source("v2")), deploy: true, ..Upload::default() };
    assert_eq!(registry.upload(second).await.unwrap(), (2, Some(2)));
    eventually_within("the runner serving v2", minute, async || serving.seq("hello") == Some(2)).await;
    assert!(!in_flight.is_finished(), "the switch came while the first request was running");
    assert_eq!(
        run(serving.get("hello").unwrap(), "/".into()).await,
        "v2 hi s3cret",
        "variables and secrets carried over"
    );
    assert_eq!(in_flight.await.unwrap(), "v1 hi s3cret");

    let detail = registry.version("hello", 2).await.unwrap();
    assert!(detail.source.contains("v2") && detail.vars.contains_key("GREETING"));
    assert_eq!(detail.secrets, vec!["TOKEN".to_owned()]);
    runner_system.shutdown().await.unwrap();
    control_system.shutdown().await.unwrap();
}

fn version(id: u32, artifact: &[u8]) -> VersionRef {
    VersionRef { id, bundle: Hash::of(artifact), size: artifact.len() as u64, limits: Limits::default() }
}

fn manifest(name: &str, seq: u64, stable: VersionRef) -> Bytes {
    Manifest { name: name.into(), seq, stable, canary: None }.record()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_runner_ignores_an_older_manifest_and_never_serves_a_bundle_that_is_not_its_hash() {
    let discovery = MemoryDiscovery::new();
    let (control_system, control) = member("control", CONTROL, &discovery).await;
    let (runner_system, runner) = member("runner", RUNNER, &discovery).await;
    runner.wait_view(|view| view.members.len() == 2).await.unwrap();

    // A control node by hand: a stream to say anything on, and a store whose `forged` bundle is not what its name says.
    let origin = StreamNode::start(&control, StreamNodeConfig::new(vec![])).await.unwrap();
    origin.open_stream(&manifests(), 1, DeliveryPolicy::Compacted(compact_limits())).await.unwrap();
    let blobs = MemoryFiles::new();
    let (good, forged) = (version(1, b"one"), version(1, b"two"));
    let blob = |v: &VersionRef, bytes: &'static [u8]| {
        FileOp::Append(format!("blob.{}", v.bundle.as_str()), Bytes::from_static(bytes))
    };
    blobs.apply(vec![blob(&good, b"one"), blob(&forged, b"one")]).await.unwrap();
    let bundles = control_system.spawn(Bundles::new(Arc::new(blobs))).start().unwrap();
    control.publish::<BundleApi>(&bundles, BUNDLES).unwrap();

    let engine = FakeEngine::new().handler("one", |_, _| FakeRun::ok("one")).handler("two", |_, _| FakeRun::ok("two"));
    let files: Arc<dyn Files> = Arc::new(MemoryFiles::new());
    let mut cache = FollowerConfig::new(files.clone());
    // A failed fetch is tried again at once, so the test sees more than one.
    cache.retry = Duration::from_millis(20);
    let stores = Stores { streams: files.clone(), registry: files.clone(), objects: files, cache };
    let serving = node::start(&runner_system, &runner, RUNNER, stores, engine, Settings::default())
        .await
        .unwrap()
        .serving
        .unwrap();

    origin.publish(&manifests(), vec![manifest("hello", 5, good.clone())]).unwrap();
    eventually("hello served", async || serving.seq("hello") == Some(5)).await;
    // An older manifest of it, as a control node that lost its log would say, then one of `forged`, then a marker
    // that shows the runner has read past both.
    let older = VersionRef { id: 9, ..good.clone() };
    origin.publish(&manifests(), vec![manifest("hello", 4, older), manifest("forged", 6, forged)]).unwrap();
    origin.publish(&manifests(), vec![manifest("marker", 7, good)]).unwrap();
    eventually("the marker served", async || serving.seq("marker") == Some(7)).await;
    eventually("the forged bundle fetched twice", async || serving.stats.failures.load(Relaxed) >= 2).await;
    let hello = serving.get("hello").unwrap();
    assert_eq!((hello.manifest.seq, hello.manifest.stable.id), (5, 1), "it did not go back");
    assert_eq!(serving.seq("forged"), None, "what came was not the bundle the manifest names");
    runner_system.shutdown().await.unwrap();
    control_system.shutdown().await.unwrap();
}
