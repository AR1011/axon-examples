//! The real engine: JavaScript on StarlingMonkey under wasmtime. Needs the engine binaries
//! (`scripts/fetch-engine.sh`) and fails, saying so, without them.

#[path = "support/real.rs"]
mod real;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use edge_workers::engine::starling::{Starling, StarlingConfig, Version};
use edge_workers::engine::{
    Engine, Env, Failed, Failure, Host, Limits, Objects, Outcome, Request, Response, Scope, StorageAnswer, StorageOp,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

fn config() -> StarlingConfig {
    let mut cfg = real::starling();
    cfg.max_in_flight = 64;
    cfg
}

/// One engine for the whole file: starting it compiles SpiderMonkey.
fn engine() -> &'static Starling {
    static ENGINE: OnceLock<Starling> = OnceLock::new();
    ENGINE.get_or_init(|| Starling::new(config()).unwrap_or_else(|e| panic!("{e}")))
}

async fn deploy(source: &str) -> Version {
    engine().deploy(source, &Env::new()).await.unwrap_or_else(|e| panic!("{e}"))
}

async fn get(version: &Version, url: &str) -> Result<Outcome, Failed> {
    engine().invoke(version, Request::get(url), Limits::default()).await
}

fn text(outcome: &Outcome) -> &str {
    std::str::from_utf8(&outcome.body).unwrap()
}

#[test]
fn an_absent_engine_is_an_error_that_says_how_to_fetch_it() {
    let empty = tempfile::tempdir().unwrap();
    let Err(Failure::Runtime(said)) = Starling::new(StarlingConfig::new(empty.path())).map(|_| ()) else {
        panic!("an engine started from an empty directory")
    };
    assert!(said.contains("starling-raw.wasm") && said.contains("scripts/fetch-engine.sh"), "{said}");
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_world() {
    let version =
        deploy(r#"export default { async fetch(request, env, ctx) { return new Response("hello world") } }"#).await;
    let outcome = get(&version, "/").await.unwrap();
    assert_eq!((outcome.status, text(&outcome)), (200, "hello world"));
    assert!(outcome.usage.logs.is_empty(), "{:?}", outcome.usage.logs);
    assert!(outcome.usage.cpu > Duration::ZERO && outcome.usage.cpu <= outcome.usage.wall, "{:?}", outcome.usage);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_request_reaches_the_handler_and_its_response_comes_back() {
    let version = deploy(
        r#"export default {
          async fetch(request) {
            const url = new URL(request.url);
            const seen = { method: request.method, host: url.host, path: url.pathname, query: url.searchParams.get("q"),
                           header: request.headers.get("x-ask"), body: await request.text() };
            return new Response(JSON.stringify(seen), { status: 201, headers: { "content-type": "application/json", "x-answer": "yes" } });
          }
        }"#,
    )
    .await;
    let request = Request {
        method: "POST".into(),
        url: "http://w.example/a/b?q=1".into(),
        headers: vec![("x-ask".into(), "please".into())],
        body: Bytes::from_static(b"the body"),
    };
    let outcome = engine().invoke(&version, request, Limits::default()).await.unwrap();
    assert_eq!(outcome.status, 201);
    assert_eq!(
        text(&outcome),
        r#"{"method":"POST","host":"w.example","path":"/a/b","query":"1","header":"please","body":"the body"}"#
    );
    let header = |name: &str| outcome.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
    assert_eq!((header("x-answer"), header("content-type")), (Some("yes"), Some("application/json")));
}

#[tokio::test(flavor = "multi_thread")]
async fn env_bindings_are_strings_the_handler_reads_and_cannot_change() {
    let source = r#"export default { fetch(request, env) {
        try { env.GREETING = "changed" } catch (e) {}
        return new Response(`${env.GREETING}, ${env["quote\"d"]}`) } }"#;
    let env = Env::from([("GREETING".to_owned(), "hi".to_owned()), ("quote\"d".to_owned(), "a\nb".to_owned())]);
    let version = engine().deploy(source, &env).await.unwrap();
    assert_eq!(text(&get(&version, "/").await.unwrap()), "hi, a\nb");
}

#[tokio::test(flavor = "multi_thread")]
async fn console_output_is_captured_in_order() {
    let version = deploy(
        r#"export default { fetch() {
            console.log("first", 1); console.error("second"); console.warn({ third: true });
            return new Response("ok") } }"#,
    )
    .await;
    let outcome = get(&version, "/").await.unwrap();
    assert_eq!(outcome.usage.logs, ["Log: first 1", "Error: second", "Warn: { third: true }"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn console_output_past_the_limit_is_dropped_and_the_request_still_answers() {
    let version = deploy(
        r#"export default { fetch() { for (let i = 0; i < 1000; i++) console.log("x".repeat(100)); return new Response("ok") } }"#,
    )
    .await;
    let limits = Limits { log_bytes: 1000, ..Limits::default() };
    let outcome = engine().invoke(&version, Request::get("/"), limits).await.unwrap();
    assert_eq!(text(&outcome), "ok");
    assert_eq!(
        outcome.usage.logs.iter().map(|l| l.len() + 1).sum::<usize>(),
        1000 + 1,
        "{:?}",
        outcome.usage.logs.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_infinite_loop_is_cut_at_the_cpu_limit() {
    let version = deploy("export default { fetch() { console.log('in'); while (true) {} } }").await;
    let failed = get(&version, "/").await.unwrap_err();
    assert_eq!(failed.failure, Failure::CpuLimit);
    assert_eq!(failed.failure.status(), 503);
    assert_eq!(failed.usage.logs, ["Log: in"]);
    let limit = Limits::default().cpu;
    // Cut at the first epoch tick past the limit: ticks are 1 ms apart, and late on a loaded machine.
    assert!(failed.usage.cpu >= limit && failed.usage.cpu < limit + Duration::from_millis(50), "{:?}", failed.usage);
}

#[tokio::test(flavor = "multi_thread")]
async fn time_spent_waiting_is_not_cpu_time() {
    let version = deploy(
        r#"export default { async fetch() { await new Promise((done) => setTimeout(done, 150)); return new Response("late") } }"#,
    )
    .await;
    let outcome = get(&version, "/").await.unwrap();
    assert_eq!(text(&outcome), "late");
    assert!(outcome.usage.wall >= Duration::from_millis(150), "{:?}", outcome.usage);
    assert!(outcome.usage.cpu < Limits::default().cpu, "{:?}", outcome.usage);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_waits_too_long_is_cut_at_the_wall_limit() {
    let version = deploy(
        r#"export default { async fetch() { console.log("waiting"); await new Promise((done) => setTimeout(done, 60000)); return new Response("never") } }"#,
    )
    .await;
    let limits = Limits { wall: Duration::from_millis(200), ..Limits::default() };
    let failed = engine().invoke(&version, Request::get("/"), limits).await.unwrap_err();
    assert_eq!((failed.failure.clone(), failed.failure.status()), (Failure::WallTimeout, 504));
    assert_eq!(failed.usage.logs, ["Log: waiting"]);
    assert!(failed.usage.wall < Duration::from_secs(5), "{:?}", failed.usage);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_hog_is_cut_at_the_memory_limit() {
    let version = deploy(
        r#"export default { fetch() { const kept = []; while (true) kept.push(new Uint8Array(1 << 20).fill(7)) } }"#,
    )
    .await;
    // CPU enough to reach the limit: the test is of memory.
    let limits = Limits { memory: 64 << 20, cpu: Duration::from_secs(20), ..Limits::default() };
    let failed = engine().invoke(&version, Request::get("/"), limits).await.unwrap_err();
    assert_eq!(failed.failure, Failure::MemoryLimit, "{:?}", failed.usage);
    // The same worker has room under a higher limit, until its CPU time ends.
    let limits = Limits { memory: 512 << 20, cpu: Duration::from_millis(20), ..Limits::default() };
    let failed = engine().invoke(&version, Request::get("/"), limits).await.unwrap_err();
    assert_eq!(failed.failure, Failure::CpuLimit, "{:?}", failed.usage);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_thrown_exception_is_a_500_with_the_message_in_the_logs() {
    let version = deploy(
        r#"export default { async fetch(request) {
            if (new URL(request.url).pathname === "/own") return new Response("mine", { status: 500 });
            throw new Error("boom at " + request.method) } }"#,
    )
    .await;
    let failed = get(&version, "/").await.unwrap_err();
    assert_eq!(failed.failure, Failure::Exception("boom at GET".into()));
    assert_eq!(failed.failure.status(), 500);
    let logs = failed.usage.logs.join("\n");
    assert!(logs.contains("boom at GET") && logs.contains("worker.js"), "{logs}");
    // A 500 the worker made itself is its response, not a failure.
    let outcome = get(&version, "/own").await.unwrap();
    assert_eq!((outcome.status, text(&outcome)), (500, "mine"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_returns_no_response_is_an_exception() {
    let version = deploy("export default { fetch() { return 'not a response' } }").await;
    let failed = get(&version, "/").await.unwrap_err();
    assert_eq!(failed.failure, Failure::Exception("fetch() did not return a Response".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_requests_share_no_globals() {
    let version = deploy(
        r#"let count = 0; globalThis.seen = [];
        export default { fetch(request) {
            count += 1; globalThis.seen.push(request.url);
            return new Response(`${count} ${globalThis.seen.length} ${Math.random()} ${crypto.randomUUID()}`) } }"#,
    )
    .await;
    let (a, b) = tokio::join!(get(&version, "/a"), get(&version, "/b"));
    let c = get(&version, "/c").await.unwrap();
    let (a, b) = (a.unwrap(), b.unwrap());
    let parts = |o: &Outcome| text(o).split(' ').map(str::to_owned).collect::<Vec<_>>();
    for outcome in [&a, &b, &c] {
        assert_eq!(parts(outcome)[..2], ["1", "1"], "a request saw another's writes");
    }
    // The snapshot holds no random state: each request draws its own.
    assert!(parts(&a)[2] != parts(&b)[2] && parts(&b)[2] != parts(&c)[2], "Math.random repeats: {a:?} {b:?} {c:?}");
    assert!(parts(&a)[3] != parts(&b)[3] && parts(&b)[3] != parts(&c)[3], "randomUUID repeats");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_top_level_runs_once_at_deploy() {
    let version = deploy(
        r#"const built = { at: Date.now(), table: Array.from({ length: 1000 }, (_, i) => i * i) };
        export default { fetch() { return new Response(`${built.table[999]} ${Date.now() - built.at >= 0}`) } }"#,
    )
    .await;
    assert_eq!(text(&get(&version, "/").await.unwrap()), "998001 true");
}

#[tokio::test(flavor = "multi_thread")]
async fn source_that_does_not_load_fails_the_deploy_with_the_reason() {
    for (source, reason) in [
        ("export default { fetch() { return new Response( } }", "SyntaxError"),
        ("export const fetch = () => new Response('named, not default')", "default"),
        ("export default { notFetch() {} }", "no fetch(request, env, ctx)"),
        ("throw new Error('top level says no'); export default { fetch() {} }", "top level says no"),
        ("while (true) {} export default { fetch() {} }", "startup limit"),
        ("import other from './other.js'; export default { fetch() { return other } }", "other.js"),
    ] {
        match engine().deploy(source, &Env::new()).await {
            Err(Failure::Load(said)) => assert!(said.contains(reason), "{source}: {said}"),
            Err(other) => panic!("{source}: {other:?}"),
            Ok(_) => panic!("{source} deployed"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_artifact_loads_as_the_same_version() {
    let env = Env::from([("NAME".to_owned(), "artifact".to_owned())]);
    let version =
        engine().deploy("export default { fetch(r, env) { return new Response(env.NAME) } }", &env).await.unwrap();
    let loaded = engine().load(engine().artifact(&version)).await.unwrap();
    assert_eq!(text(&get(&loaded, "/").await.unwrap()), "artifact");
    assert!(matches!(engine().load(Bytes::from_static(b"not wasm")).await, Err(Failure::Load(_))));
}

/// Answers every connection `hits` on a loopback port, and counts them.
async fn backend() -> (String, &'static AtomicUsize) {
    let hits: &'static AtomicUsize = Box::leak(Box::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await;
                let _ =
                    socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\nbackend").await;
            });
        }
    });
    (url, hits)
}

const FETCHER: &str = r#"export default { async fetch(request, env) {
    const results = [];
    for (let i = 0; i < Number(env.TIMES); i++) {
        try { const r = await fetch(env.BACKEND); results.push(`${r.status} ${await r.text()}`) }
        catch (e) { results.push("refused") }
    }
    return new Response(results.join(",")) } }"#;

#[tokio::test(flavor = "multi_thread")]
async fn fetch_to_a_loopback_address_is_refused() {
    let (url, hits) = backend().await;
    for backend in
        [url.clone(), url.replace("127.0.0.1", "localhost"), "http://10.0.0.1/".into(), "http://[::1]:9/".into()]
    {
        let env = Env::from([("BACKEND".to_owned(), backend), ("TIMES".to_owned(), "1".to_owned())]);
        let version = engine().deploy(FETCHER, &env).await.unwrap();
        let outcome = get(&version, "/").await.unwrap();
        assert_eq!((text(&outcome), outcome.usage.subrequests), ("refused", 1));
    }
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_reaches_a_backend_up_to_the_subrequest_limit() {
    let mut cfg = config();
    cfg.allow_private_fetch = true;
    let engine = Starling::new(cfg).unwrap();
    let (url, hits) = backend().await;
    let env = Env::from([("BACKEND".to_owned(), url), ("TIMES".to_owned(), "3".to_owned())]);
    let version = engine.deploy(FETCHER, &env).await.unwrap();
    let limits = Limits { subrequests: 2, ..Limits::default() };
    let outcome = engine.invoke(&version, Request::get("/"), limits).await.unwrap();
    assert_eq!(text(&outcome), "200 backend,200 backend,refused");
    assert_eq!((hits.load(Ordering::SeqCst), outcome.usage.subrequests), (2, 3));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_request_past_the_engine_in_flight_limit_is_refused_and_the_slots_come_back() {
    let mut cfg = config();
    cfg.max_in_flight = 2;
    let engine = Starling::new(cfg).unwrap();
    let source = r#"export default { async fetch() { await new Promise((done) => setTimeout(done, 300)); return new Response("ok") } }"#;
    let version = engine.deploy(source, &Env::new()).await.unwrap();
    let run = || engine.invoke(&version, Request::get("/"), Limits::default());
    let (a, b, c) = tokio::join!(run(), run(), run());
    let refused: Vec<_> = [a, b, c].into_iter().filter_map(Result::err).collect();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(matches!(refused[0].failure, Failure::Runtime(_)), "{refused:?}");
    let (a, b) = tokio::join!(run(), run());
    assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_response_past_the_limit_is_not_read_further_and_fails_by_name() {
    let version = deploy(
        r#"export default { async fetch(request) {
            return new Response("x".repeat(Number(new URL(request.url).searchParams.get("bytes"))));
        } }"#,
    )
    .await;
    // CPU time to spare: a slow machine must reach the size limit, not the 50 ms of CPU, with the megabytes.
    let limits = Limits { response_bytes: 1000, cpu: Duration::from_secs(20), ..Limits::default() };
    let fits = engine().invoke(&version, Request::get("/?bytes=1000"), limits).await.unwrap();
    assert_eq!(fits.body.len(), 1000);
    let failed = engine().invoke(&version, Request::get("/?bytes=1001"), limits).await.unwrap_err();
    assert_eq!((failed.failure.clone(), failed.failure.status()), (Failure::ResponseTooLarge, 500));
    // Far past it: the engine stops at the limit, it does not hold the megabytes to count them.
    let failed = engine().invoke(&version, Request::get("/?bytes=8000000"), limits).await.unwrap_err();
    assert_eq!(failed.failure, Failure::ResponseTooLarge);
}

/// A host in one process: an object's request runs in the same engine, and each object's storage is a map.
/// What each object stores, by class and id.
type Stored = BTreeMap<(String, String), BTreeMap<String, String>>;

#[derive(Clone)]
struct OneNode {
    version: Version,
    object: Option<(String, String)>,
    stored: Arc<Mutex<Stored>>,
    /// Of the object's runs.
    limits: Limits,
}

impl Host for OneNode {
    fn fetch(&self, class: String, id: String, request: Request) -> Boxed<Result<Response, String>> {
        let host = Self { object: Some((class, id)), ..self.clone() };
        Box::pin(async move {
            let scope = Scope { object: host.object.clone(), host: Some(Arc::new(host.clone())) };
            match engine().invoke_in(&host.version, request, host.limits, scope).await {
                Ok(outcome) => Ok(Response { status: outcome.status, headers: outcome.headers, body: outcome.body }),
                Err(failed) => Err(failed.failure.to_string()),
            }
        })
    }

    fn storage(&self, op: StorageOp) -> Boxed<Result<StorageAnswer, String>> {
        let Some(object) = self.object.clone() else {
            return Box::pin(async { Err("storage outside an object".to_owned()) });
        };
        let mut stored = self.stored.lock().unwrap();
        let data = stored.entry(object).or_default();
        let answer = match op {
            StorageOp::Get { key } => StorageAnswer::Value(data.get(&key).cloned()),
            StorageOp::Put { key, value } => {
                data.insert(key, value);
                StorageAnswer::Done
            }
            StorageOp::Delete { key } => StorageAnswer::Deleted(data.remove(&key).is_some()),
            StorageOp::List { prefix, limit } => {
                let found = data.iter().filter(|(key, _)| key.starts_with(&prefix)).take(limit.unwrap_or(usize::MAX));
                StorageAnswer::Entries(found.map(|(k, v)| (k.clone(), v.clone())).collect())
            }
        };
        Box::pin(async move { Ok(answer) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_calls_an_object_of_its_module_and_the_object_its_storage() {
    let source = r#"
      export class Counter {
        constructor(state, env) { this.state = state; this.greeting = env.GREETING; }
        async fetch(request) {
          const n = ((await this.state.storage.get("n")) ?? 0) + 1;
          await this.state.storage.put("n", n);
          await this.state.storage.put("seen:" + n, { url: request.url, said: await request.text() });
          if (n === 2) await this.state.storage.delete("seen:1");
          const seen = [...await this.state.storage.list({ prefix: "seen:" })];
          return new Response(JSON.stringify({ id: this.state.id.name, n, seen, greeting: this.greeting }));
        }
      }
      export default { async fetch(request, env) {
        const id = env.COUNTER.idFromName(new URL(request.url).searchParams.get("room"));
        if (request.method !== "GET") return env.COUNTER.get(id).fetch(request);
        return env.COUNTER.get(id).fetch("http://object/add", { method: "POST", body: "hi" });
      } }"#;
    let env = Env::from([("GREETING".to_owned(), "hello".to_owned())]);
    let objects = Objects::from([("COUNTER".to_owned(), "Counter".to_owned())]);
    let version = engine().deploy_bound(source, &env, &objects).await.unwrap_or_else(|e| panic!("{e}"));
    let stored = Arc::new(Mutex::new(BTreeMap::new()));
    let call = async |room: &str| {
        let host =
            OneNode { version: version.clone(), object: None, stored: stored.clone(), limits: Limits::default() };
        let scope = Scope { object: None, host: Some(Arc::new(host)) };
        let request = Request::get(&format!("/?room={room}"));
        let outcome = engine().invoke_in(&version, request, Limits::default(), scope).await.unwrap();
        assert_eq!(outcome.status, 200, "{}", text(&outcome));
        serde_json::from_slice::<serde_json::Value>(&outcome.body).unwrap()
    };
    let seen = |n: u32| serde_json::json!([format!("seen:{n}"), { "url": "http://object/add", "said": "hi" }]);
    let first = call("a%20b%2F%C3%A9").await;
    assert_eq!(first, serde_json::json!({ "id": "a b/é", "n": 1, "seen": [seen(1)], "greeting": "hello" }));
    assert_eq!(call("a%20b%2F%C3%A9").await["seen"], serde_json::json!([seen(2)]), "the delete and the list");
    assert_eq!(call("other").await["n"], 1, "another id is another object");

    // The handler's own request, passed on as it came.
    for (method, body) in [("POST", "sent"), ("POST", ""), ("DELETE", "")] {
        let host =
            OneNode { version: version.clone(), object: None, stored: stored.clone(), limits: Limits::default() };
        let scope = Scope { object: None, host: Some(Arc::new(host)) };
        let request = Request {
            method: method.into(),
            url: "http://counter.test/x?room=passed".into(),
            headers: vec![("x-mine".into(), "1".into())],
            body: Bytes::from(body),
        };
        let outcome = engine().invoke_in(&version, request, Limits::default(), scope).await.unwrap();
        assert_eq!(outcome.status, 200, "{method} {body:?}: {}", text(&outcome));
        let said: serde_json::Value = serde_json::from_slice(&outcome.body).unwrap();
        let last = said["seen"].as_array().unwrap().last().unwrap()[1].clone();
        assert_eq!(last, serde_json::json!({ "url": "http://counter.test/x?room=passed", "said": body }));
    }

    // A request from outside cannot say it is an object's: the header is taken off, and the handler runs.
    let host = OneNode { version: version.clone(), object: None, stored: stored.clone(), limits: Limits::default() };
    let scope = Scope { object: None, host: Some(Arc::new(host)) };
    let mut forged = Request::get("/?room=real");
    forged.headers.push(("X-Edge-Object".into(), "Counter/forged".into()));
    let outcome = engine().invoke_in(&version, forged, Limits::default(), scope).await.unwrap();
    let said: serde_json::Value = serde_json::from_slice(&outcome.body).unwrap_or_default();
    assert_eq!((outcome.status, &said["id"]), (200, &serde_json::json!("real")), "{}", text(&outcome));
    // A run without a host reaches no object: the stub throws.
    let failed = engine().invoke(&version, Request::get("/?room=x"), Limits::default()).await.unwrap_err();
    assert_eq!(failed.failure, Failure::Exception("objects cannot be reached from this run".into()));
    assert!(stored.lock().unwrap().keys().all(|(_, id)| id != "forged" && id != "x"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_of_an_object_is_a_subrequest_and_a_call_of_its_storage_is_not() {
    let source = r#"
      export class Counter {
        constructor(state) { this.state = state; }
        async fetch() {
          for (let i = 0; i < 5; i++) await this.state.storage.put("k" + i, i);
          return new Response(String((await this.state.storage.list()).size));
        }
      }
      export default { async fetch(request, env) {
        const said = [];
        for (let i = 0; i < Number(new URL(request.url).searchParams.get("calls")); i++) {
          try {
            said.push(await (await env.COUNTER.get(env.COUNTER.idFromName("a")).fetch("http://object/")).text());
          } catch (e) {
            said.push("refused");
          }
        }
        return new Response(said.join(","));
      } }"#;
    let objects = Objects::from([("COUNTER".to_owned(), "Counter".to_owned())]);
    let version = engine().deploy_bound(source, &Env::new(), &objects).await.unwrap_or_else(|e| panic!("{e}"));
    // Two subrequests a run, the handler's and the object's alike. The object's six storage calls are none.
    let limits = Limits { subrequests: 2, ..Limits::default() };
    let host = OneNode { version: version.clone(), object: None, stored: Arc::default(), limits };
    let scope = Scope { object: None, host: Some(Arc::new(host)) };
    let outcome = engine().invoke_in(&version, Request::get("/?calls=3"), limits, scope).await.unwrap();
    assert_eq!((text(&outcome), outcome.usage.subrequests), ("5,5,refused", 3));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_binding_of_a_class_the_module_does_not_export_fails_the_deploy() {
    let objects = Objects::from([("COUNTER".to_owned(), "Counter".to_owned())]);
    let source = "export default { fetch() { return new Response('ok') } }";
    let Err(Failure::Load(said)) = engine().deploy_bound(source, &Env::new(), &objects).await.map(|_| ()) else {
        panic!("it deployed")
    };
    assert!(said.contains("binding COUNTER: the module exports no class Counter"), "{said}");
}
