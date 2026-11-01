//! The fake engine keeps the real one's contract where World tests lean on it.

use std::time::Duration;

use bytes::Bytes;
use edge_workers::engine::fake::{FakeEngine, FakeRun};
use edge_workers::engine::{Engine, Env, Failure, Limits, Request};

fn engine() -> FakeEngine {
    FakeEngine::new()
        .handler("echo", |request, env| FakeRun::ok(format!("{} {}", env["NAME"], request.url)))
        .handler("burn", |request, _| FakeRun::ok("done").cpu(Duration::from_millis(request.body.len() as u64)))
        .handler("throw", |_, _| FakeRun::fail(Failure::Exception("on demand".into())))
}

#[tokio::test(start_paused = true)]
async fn a_handler_runs_by_name_with_its_env_and_survives_the_artifact() {
    let engine = engine();
    let env = Env::from([("NAME".to_owned(), "a=b\nc".to_owned())]);
    let version = engine.deploy("echo", &env).await.unwrap();
    let loaded = engine.load(engine.artifact(&version)).await.unwrap();
    for version in [version, loaded] {
        let outcome = engine.invoke(&version, Request::get("/x"), Limits::default()).await.unwrap();
        assert_eq!((outcome.status, &outcome.body[..]), (200, &b"a=b\nc /x"[..]));
    }
    assert_eq!(engine.deploy("absent", &env).await.err(), Some(Failure::Load("no handler named \"absent\"".into())));
}

#[tokio::test(start_paused = true)]
async fn simulated_cpu_takes_time_and_is_cut_at_the_limit() {
    let engine = engine();
    let version = engine.deploy("burn", &Env::new()).await.unwrap();
    let burn = |ms: usize| Request { body: Bytes::from(vec![0; ms]), ..Request::get("/") };
    let began = tokio::time::Instant::now();
    let outcome = engine.invoke(&version, burn(30), Limits::default()).await.unwrap();
    assert_eq!((outcome.usage.cpu, began.elapsed()), (Duration::from_millis(30), Duration::from_millis(30)));

    let began = tokio::time::Instant::now();
    let failed = engine.invoke(&version, burn(500), Limits::default()).await.unwrap_err();
    assert_eq!(
        (failed.failure, failed.usage.cpu, began.elapsed()),
        (Failure::CpuLimit, Limits::default().cpu, Limits::default().cpu)
    );

    let limits = Limits { wall: Duration::from_millis(10), ..Limits::default() };
    let failed = engine.invoke(&version, burn(30), limits).await.unwrap_err();
    assert_eq!((failed.failure, failed.usage.wall), (Failure::WallTimeout, Duration::from_millis(10)));
}

#[tokio::test(start_paused = true)]
async fn a_handler_fails_on_demand() {
    let engine = engine();
    let version = engine.deploy("throw", &Env::new()).await.unwrap();
    let failed = engine.invoke(&version, Request::get("/"), Limits::default()).await.unwrap_err();
    assert_eq!(failed.failure, Failure::Exception("on demand".into()));
}
