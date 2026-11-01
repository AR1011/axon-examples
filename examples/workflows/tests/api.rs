//! The HTTP API's contract, on real nodes over loopback: each route's answers and every code of the design's table,
//! the scopes of named tokens and their revocation, logout, delete, `/ready` and `/metrics`, and I17 (an action on an
//! unknown id writes nothing to the replicas). Three clusters: one that works, one with no majority of history
//! replicas (`unavailable`), and one whose engine takes few runs (`busy`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::OsFiles;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, PaxosConfig, PaxosLeases, QuotaConfig, Quotas, Rate};
use axon_net::SharedSecret;
use axon_testkit::eventually_within;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use workflows::api::{Api, ApiConfig};
use workflows::auth::{hash_password, new_token};
use workflows::journal::service::JournalConfig;
use workflows::node::{self, NodeConfig};
use workflows::worker::queue_label;
use workflows::workflow::{Activity, ActivityCx, App, Ctx, Failure};

const WAIT: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
struct Boom;

impl Activity for Boom {
    const NAME: &'static str = "boom";
    type Output = ();
    async fn run(self, _: ActivityCx) -> Result<(), Failure> {
        Err(Failure::fatal("boom"))
    }
}

/// On queue `q`, which no worker serves: its run stays open with the attempt out.
#[derive(Serialize, Deserialize)]
struct Far;

impl Activity for Far {
    const NAME: &'static str = "far";
    const QUEUE: &'static str = "q";
    type Output = ();
    async fn run(self, _: ActivityCx) -> Result<(), Failure> {
        Ok(())
    }
}

async fn quick(_: Ctx, input: Value) -> Result<Value, Failure> {
    Ok(input)
}

async fn hold(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.signal::<Value>("go").await
}

async fn boom(ctx: Ctx, _: Value) -> Result<(), Failure> {
    ctx.activity(Boom).await
}

async fn far(ctx: Ctx, _: Value) -> Result<(), Failure> {
    ctx.activity(Far).await
}

fn app() -> App {
    App::new()
        .activity::<Boom>()
        .activity::<Far>()
        .workflow("quick", 1, quick)
        .workflow("hold", 1, hold)
        .workflow("boom", 1, boom)
        .workflow("far", 1, far)
}

struct Up {
    system: ActorSystem,
    cluster: Cluster,
    node: node::Node,
    quotas: Option<Quotas>,
}

struct Boot<'a> {
    discovery: &'a MemoryDiscovery,
    dir: &'a Path,
    replicas: &'a [&'a str],
    acceptors: &'a [&'a str],
    max_spawned: Option<usize>,
}

async fn boot(b: &Boot<'_>, name: &str, roles: &[&str]) -> Up {
    let mut transport = axon_net::Config::new(name, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true; // loopback only
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, b.discovery.clone());
    for role in roles {
        cfg.labels.insert((*role).into(), "yes".into());
    }
    if let Some(max) = b.max_spawned {
        cfg.max_spawned = max;
    }
    let mut paxos = PaxosConfig::new(b.acceptors.iter().copied());
    // A short lease: the acceptors' wait after their start is a little over one, and each test waits for it.
    paxos.lease = Duration::from_secs(2);
    if b.acceptors.contains(&name) {
        paxos.store = Some(Arc::new(OsFiles::new(b.dir.join(name).join("leases"))));
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let replicas = b.replicas.iter().map(|r| Arc::from(*r)).collect();
    let mut ncfg = NodeConfig::new(app(), Arc::new(axon::trace::unix_micros), JournalConfig::new(replicas));
    if roles.contains(&"history") {
        let at = |what: &str| Arc::new(OsFiles::new(b.dir.join(name).join(what))) as Arc<dyn axon::files::Files>;
        ncfg.history = Some((at("journal"), at("journal-snap")));
    }
    ncfg.engine = roles.contains(&"engine");
    let node = node::start(&system, &cluster, ncfg).await.unwrap();
    let quotas = match roles.contains(&"engine") {
        true => Some(Quotas::start(&cluster, QuotaConfig::default()).await.unwrap()),
        false => None,
    };
    Up { system, cluster, node, quotas }
}

async fn settle(nodes: &[Up], members: usize) {
    eventually_within("the acceptors past their wait", WAIT, async || {
        nodes.iter().all(|n| !n.cluster.warming() && n.cluster.view().members.len() == members)
    })
    .await;
}

async fn down(nodes: Vec<Up>) {
    for up in nodes.into_iter().rev() {
        drop(up.quotas);
        drop(up.node);
        drop(up.cluster);
        up.system.shutdown().await.unwrap();
    }
}

async fn serve(api: &Api) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    tokio::spawn(async move { axum::serve(listener, router).await });
    url
}

/// An HTTP answer: its status, its `Retry-After` and `Set-Cookie`, and its body as JSON (or a string).
#[derive(Debug)]
struct Said {
    status: StatusCode,
    retry_after: Option<String>,
    cookie: Option<String>,
    body: Value,
}

impl Said {
    /// The refusal's code, after its status was checked.
    #[track_caller]
    fn refused(&self, status: u16, code: &str) {
        assert_eq!((self.status.as_u16(), self.body["error"].as_str()), (status, Some(code)), "{self:?}");
        assert!(self.body["message"].as_str().is_some_and(|m| !m.is_empty()), "{self:?}");
        if matches!(status, 429 | 503) {
            let after = self.retry_after.as_deref().and_then(|s| s.parse::<u64>().ok());
            assert!(after.is_some_and(|s| s >= 1), "{status} without Retry-After: {self:?}");
        }
    }

    #[track_caller]
    fn ok(&self, status: u16) -> &Value {
        assert_eq!(self.status.as_u16(), status, "{self:?}");
        &self.body
    }
}

#[derive(Clone)]
struct Http {
    client: reqwest::Client,
    url: String,
}

impl Http {
    fn new(url: &str) -> Self {
        Http { client: reqwest::Client::new(), url: url.to_owned() }
    }

    async fn call(&self, method: Method, path: &str, token: Option<&str>, body: Option<String>) -> Said {
        let mut request = self.client.request(method, format!("{}{path}", self.url));
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request = request.header("content-type", "application/json").body(body);
        }
        let response = request.send().await.unwrap();
        let header = |name: &str| response.headers().get(name).map(|v| v.to_str().unwrap().to_owned());
        let (status, retry_after, cookie) = (response.status(), header("retry-after"), header("set-cookie"));
        let bytes = response.bytes().await.unwrap();
        let body =
            serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
        Said { status, retry_after, cookie, body }
    }

    async fn get(&self, path: &str, token: &str) -> Said {
        self.call(Method::GET, path, Some(token), None).await
    }

    async fn post(&self, path: &str, token: &str, body: Value) -> Said {
        self.call(Method::POST, path, Some(token), Some(body.to_string())).await
    }

    async fn start(&self, token: &str, workflow: &str, id: &str, input: Value) -> Said {
        self.post("/api/runs", token, json!({ "workflow": workflow, "id": id, "input": input })).await
    }

    async fn status_of(&self, token: &str, id: &str) -> Option<String> {
        let said = self.get(&format!("/api/runs/{id}"), token).await;
        said.body["status"].as_str().map(str::to_owned)
    }
}

/// Every file under the replicas' journal directories, with its length and CRC: what a write would change.
fn journal_files(dir: &Path, replicas: &[&str]) -> Vec<(PathBuf, u64, u32)> {
    let mut files = Vec::new();
    for replica in replicas {
        let journal = dir.join(replica).join("journal");
        for entry in std::fs::read_dir(&journal).unwrap() {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            files.push((path, bytes.len() as u64, axon::files::crc32(&bytes)));
        }
    }
    files.sort();
    files
}

const HISTORY: [&str; 3] = ["h1", "h2", "h3"];

/// A token line of `tokens.toml` for each (name, scope), and each token.
fn tokens(file: &Path, which: &[(&str, &str)]) -> Vec<String> {
    let (mut text, mut tokens) = (String::new(), Vec::new());
    for (name, scope) in which {
        let (token, entry) = new_token(name, scope).unwrap();
        text.push_str(&entry);
        text.push('\n');
        tokens.push(token);
    }
    std::fs::write(file, text).unwrap();
    tokens
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_route_answers_its_codes() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let b = Boot { discovery: &discovery, dir: dir.path(), replicas: &HISTORY, acceptors: &HISTORY, max_spawned: None };
    let mut nodes = Vec::new();
    for h in HISTORY {
        nodes.push(boot(&b, h, &["history"]).await);
    }
    nodes.push(boot(&b, "e1", &["engine", "api"]).await);
    settle(&nodes, 4).await;

    let hash = hash_password("pw").unwrap();
    let file = dir.path().join("tokens.toml");
    let [reader, signaller, starter, admin] = <[String; 4]>::try_from(tokens(
        &file,
        &[("reader", "read"), ("carrier", "signal"), ("app", "start"), ("ops", "admin")],
    ))
    .unwrap();
    let mut cfg = ApiConfig::new(&hash);
    cfg.tokens = Some(file.clone());
    // Revocation is seen within a tenth of the shipped 10 s, so the test need not wait for it.
    cfg.tokens_every = Duration::from_millis(200);
    // A call gives up on a run sooner than the shipped 10 s, so the waits below are short.
    cfg.patience = Duration::from_secs(3);
    cfg.max_waiters = 1;
    let api = Api::new(&nodes[3].system, &nodes[3].node, &nodes[3].cluster, cfg).unwrap();
    let http = Http::new(&serve(&api).await);

    // `/ready` asks for no sign-in.
    let ready = http.call(Method::GET, "/ready", None, None).await;
    assert_eq!(
        ready.ok(200),
        &json!({ "journal": true, "warming": false, "isolated": false, "quotas_starting": false })
    );

    // I17, while no run exists: an action on an unknown id writes nothing to any replica.
    let before = journal_files(dir.path(), &HISTORY);
    for (method, path, body) in [
        (Method::GET, "/api/runs/ghost", None),
        (Method::GET, "/api/runs/ghost/history", None),
        (Method::GET, "/api/runs/ghost/wait?timeout=1s", None),
        (Method::POST, "/api/runs/ghost/signals/go", Some(json!({ "signal_id": "s1" }))),
        (Method::POST, "/api/runs/ghost/cancel", Some(json!({}))),
        (Method::POST, "/api/runs/ghost/terminate", Some(json!({}))),
        (Method::POST, "/api/runs/ghost/pause", Some(json!({}))),
        (Method::POST, "/api/runs/ghost/resume", Some(json!({}))),
        (Method::POST, "/api/runs/ghost/retry", Some(json!({}))),
        (Method::DELETE, "/api/runs/ghost", None),
    ] {
        let said = http.call(method.clone(), path, Some(&admin), body.map(|b| b.to_string())).await;
        assert_eq!(said.body["error"], "not_found", "{method} {path}: {said:?}");
        said.refused(404, "not_found");
    }
    assert_eq!(journal_files(dir.path(), &HISTORY), before, "I17: an unknown id wrote to a replica");

    // 401: no credentials, a wrong password, a token no file lists.
    http.call(Method::GET, "/api/runs", None, None).await.refused(401, "unauthorized");
    http.get("/api/runs", "wft_00000000000000000000000000000000").await.refused(401, "unauthorized");
    http.call(Method::POST, "/api/login", None, Some(json!({ "password": "no" }).to_string()))
        .await
        .refused(401, "unauthorized");
    http.call(Method::GET, "/metrics", None, None).await.refused(401, "unauthorized");

    // Login sets a session cookie; logout clears it.
    let login = http.call(Method::POST, "/api/login", None, Some(json!({ "password": "pw" }).to_string())).await;
    login.ok(200);
    let cookie = login.cookie.clone().expect("a session cookie");
    assert!(cookie.starts_with("wf_session=") && cookie.contains("HttpOnly"), "{cookie}");
    let session = cookie.trim_start_matches("wf_session=").split(';').next().unwrap().to_owned();
    let mut said =
        http.client.get(format!("{}/api/workflows", http.url)).header("cookie", format!("wf_session={session}"));
    assert_eq!(said.send().await.unwrap().status(), 200, "the cookie signs in");
    said = http.client.post(format!("{}/api/logout", http.url)).header("cookie", format!("wf_session={session}"));
    let out = said.send().await.unwrap();
    assert_eq!(out.status(), 200);
    let cleared = out.headers().get("set-cookie").unwrap().to_str().unwrap().to_owned();
    assert!(cleared.starts_with("wf_session=;") && cleared.contains("Max-Age=0"), "{cleared}");
    http.call(Method::POST, "/api/logout", None, None).await.refused(401, "unauthorized");

    // Scopes: read < signal < start < admin.
    http.get("/api/runs", &reader).await.ok(200);
    http.start(&reader, "quick", "q-0", json!(1)).await.refused(403, "forbidden");
    http.start(&signaller, "quick", "q-0", json!(1)).await.refused(403, "forbidden");
    http.get("/metrics", &reader).await.ok(200);

    // Start: 201, 200 for the same input, 409 for another, and each 400 and 413.
    let made = http.start(&starter, "quick", "q-1", json!({ "n": 1 })).await;
    assert_eq!(made.ok(201)["created"], true);
    assert_eq!(http.start(&starter, "quick", "q-1", json!({ "n": 1 })).await.ok(200)["created"], false);
    http.start(&starter, "quick", "q-1", json!({ "n": 2 })).await.refused(409, "exists");
    let v9 = json!({ "workflow": "quick", "id": "q-9", "version": 9, "input": 1 });
    http.post("/api/runs", &starter, v9).await.refused(400, "version_missing");
    http.start(&starter, "nothing", "q-n", json!(1)).await.refused(400, "invalid");
    http.start(&starter, "quick", "bad id!", json!(1)).await.refused(400, "invalid");
    let timeout = json!({ "workflow": "quick", "id": "q-t", "run_timeout": "soon" });
    http.post("/api/runs", &starter, timeout).await.refused(400, "invalid");
    http.call(Method::POST, "/api/runs", Some(&starter), Some("{not json".into())).await.refused(400, "invalid");
    http.call(Method::POST, "/api/runs", Some(&starter), Some("{}".into())).await.refused(400, "invalid");
    let big = "x".repeat(300 << 10);
    http.start(&starter, "quick", "q-big", json!(big)).await.refused(413, "too_large");
    let huge = "x".repeat(3 << 20);
    http.start(&starter, "quick", "q-huge", json!(huge)).await.refused(413, "too_large");

    // Read: a run, and its history.
    let run = http.get("/api/runs/q-1", &reader).await;
    assert_eq!(run.ok(200)["workflow"], "quick");
    let history = http.get("/api/runs/q-1/history?limit=1", &reader).await;
    let entries = history.ok(200)["entries"].as_array().unwrap().clone();
    assert_eq!((entries.len(), &history.body["next"]), (1, &json!(1)), "{history:?}");
    assert!(entries[0]["Own"].is_object() || entries[0]["Started"].is_object(), "{entries:?}");
    let rest = http.get(&format!("/api/runs/q-1/history?after={}", history.body["next"]), &reader).await;
    assert!(!rest.ok(200)["entries"].as_array().unwrap().is_empty(), "{rest:?}");
    http.get("/api/runs/q-1/history?limit=x", &reader).await.refused(400, "invalid");

    // Wait: 200 once ended, 202 at the deadline, 429 past the waiters this node takes.
    assert_eq!(http.get("/api/runs/q-1/wait?timeout=10s", &reader).await.ok(200)["status"], "completed");
    http.start(&starter, "hold", "h-1", json!(null)).await.ok(201);
    assert_eq!(http.get("/api/runs/h-1/wait?timeout=1s", &reader).await.ok(202)["status"], "running");
    http.get("/api/runs/h-1/wait?timeout=forever", &reader).await.refused(400, "invalid");
    // One wait held (the node takes one): the next is refused. The held one may lose the race to the probe, which
    // then holds the slot; it is started again until it holds it.
    let mut waiting = None;
    for _ in 0..20 {
        let (http2, reader2) = (http.clone(), reader.clone());
        let held = tokio::spawn(async move { http2.get("/api/runs/h-1/wait?timeout=30s", &reader2).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let probe = http.get("/api/runs/h-1/wait?timeout=1ms", &reader).await;
        if probe.status == 429 && !held.is_finished() {
            probe.refused(429, "waiters_full");
            waiting = Some(held);
            break;
        }
        held.abort();
    }
    let waiting = waiting.expect("a wait held while the next is refused");

    // Signal: 202, a duplicate, 403 for read, 413, 400.
    let sig = |token: &str, id: &str, signal_id: &str, payload: Value| {
        let (http, token) = (http.clone(), token.to_owned());
        let path = format!("/api/runs/{id}/signals/go");
        let body = json!({ "signal_id": signal_id, "payload": payload });
        async move { http.post(&path, &token, body).await }
    };
    sig(&reader, "h-1", "s-1", json!(1)).await.refused(403, "forbidden");
    sig(&signaller, "h-1", "", json!(1)).await.refused(400, "invalid");
    sig(&signaller, "h-1", "s-big", json!(big)).await.refused(413, "too_large");
    assert_eq!(sig(&signaller, "h-1", "s-1", json!(7)).await.ok(202)["duplicate"], false);
    assert_eq!(sig(&signaller, "h-1", "s-1", json!(7)).await.ok(202)["duplicate"], true);
    assert_eq!(waiting.await.unwrap().ok(200)["output"], 7, "the wait held ends with the run");
    sig(&signaller, "h-1", "s-2", json!(8)).await.refused(409, "finished");

    // Cancel and terminate: admin only, 409 once ended.
    http.start(&starter, "hold", "h-2", json!(null)).await.ok(201);
    http.start(&starter, "hold", "h-3", json!(null)).await.ok(201);
    http.post("/api/runs/h-2/cancel", &starter, json!({})).await.refused(403, "forbidden");
    http.post("/api/runs/h-2/cancel", &admin, json!({ "reason": "test" })).await.ok(202);
    http.post("/api/runs/h-3/terminate", &admin, json!({})).await.ok(202);
    eventually_within("both ended", WAIT, async || {
        http.status_of(&reader, "h-2").await.as_deref() == Some("cancelled")
            && http.status_of(&reader, "h-3").await.as_deref() == Some("terminated")
    })
    .await;
    http.post("/api/runs/h-2/cancel", &admin, json!({})).await.refused(409, "finished");
    http.post("/api/runs/h-3/terminate", &admin, json!({})).await.refused(409, "finished");

    // Pause and resume.
    http.start(&starter, "hold", "h-4", json!(null)).await.ok(201);
    http.post("/api/runs/h-4/resume", &admin, json!({})).await.refused(409, "not_resumable");
    http.post("/api/runs/h-4/pause", &admin, json!({})).await.ok(202);
    assert_eq!(http.status_of(&reader, "h-4").await.as_deref(), Some("paused"));
    http.post("/api/runs/h-4/pause", &admin, json!({})).await.refused(409, "paused");
    http.post("/api/runs/h-4/resume", &admin, json!({})).await.ok(202);
    http.post("/api/runs/h-2/pause", &admin, json!({})).await.refused(409, "finished");

    // Retry from the failed step.
    http.start(&starter, "boom", "b-1", json!(null)).await.ok(201);
    eventually_within("b-1 failed", WAIT, async || http.status_of(&reader, "b-1").await.as_deref() == Some("failed"))
        .await;
    http.post("/api/runs/q-1/retry", &admin, json!({})).await.refused(409, "not_failed");
    http.post("/api/runs/b-1/retry", &admin, json!({})).await.ok(202);

    // Delete: only an ended run; then it is not found, and its id stays taken.
    http.call(Method::DELETE, "/api/runs/h-4", Some(&admin), None).await.refused(409, "not_finished");
    http.call(Method::DELETE, "/api/runs/q-1", Some(&reader), None).await.refused(403, "forbidden");
    http.call(Method::DELETE, "/api/runs/q-1", Some(&admin), None).await.ok(202);
    http.get("/api/runs/q-1", &reader).await.refused(404, "not_found");
    http.start(&starter, "quick", "q-1", json!({ "n": 1 })).await.refused(409, "exists");

    // Lists: newest first, by filter, in pages.
    for n in 0..5 {
        let tags = if n % 2 == 0 { json!([["color", "red"]]) } else { json!([]) };
        let body = json!({ "workflow": "quick", "id": format!("l-{n}"), "input": n, "tags": tags });
        http.post("/api/runs", &starter, body).await.ok(201);
    }
    let ids = |said: &Said| -> Vec<String> {
        said.body["runs"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap().to_owned()).collect()
    };
    let first = http.get("/api/runs?prefix=l-&limit=3", &reader).await;
    assert_eq!(ids(&first), ["l-0", "l-1", "l-2"], "a prefix lists in id order: {first:?}");
    let cursor = first.body["next"].as_str().expect("a cursor").to_owned();
    let second = http.get(&format!("/api/runs?prefix=l-&limit=3&cursor={cursor}"), &reader).await;
    assert_eq!(ids(&second), ["l-3", "l-4"]);
    assert!(second.body["next"].is_null());
    eventually_within("the quick runs completed", WAIT, async || {
        let said = http.get("/api/runs?workflow=quick&status=completed", &reader).await;
        ids(&said).iter().filter(|id| id.starts_with("l-")).count() == 5
    })
    .await;
    let red = http.get("/api/runs?tag=color:red", &reader).await;
    assert_eq!(ids(&red).len(), 3, "{red:?}");
    let failed = http.get("/api/runs?status=failed", &reader).await;
    assert!(ids(&failed).iter().all(|id| id == "b-1"), "{failed:?}");
    let row = &red.body["runs"][0];
    for field in ["id", "workflow", "version", "status", "started_ms", "updated_ms", "tags"] {
        assert!(!row[field].is_null(), "{field} in {row}");
    }
    http.get("/api/runs?status=sleepy", &reader).await.refused(400, "invalid");
    http.get("/api/runs?limit=0", &reader).await.refused(400, "invalid");
    http.get("/api/runs?cursor=zz", &reader).await.refused(400, "invalid");
    http.get("/api/runs?tag=nocolon", &reader).await.refused(400, "invalid");

    // The console's pages.
    let workflows = http.get("/api/workflows", &reader).await;
    let quick = workflows.ok(200)["workflows"].as_array().unwrap().iter().find(|w| w["name"] == "quick").cloned();
    assert_eq!(quick.as_ref().map(|q| &q["versions"]), Some(&json!([1])), "{workflows:?}");
    assert!(http.get("/api/queues", &reader).await.ok(200)["queues"].is_array());
    assert!(http.get("/api/workers", &reader).await.ok(200)["workers"].is_array());
    let cluster = http.get("/api/cluster", &reader).await;
    assert_eq!(cluster.ok(200)["quorum"], true, "{cluster:?}");
    assert_eq!(cluster.body["replicas"].as_array().map(Vec::len), Some(3), "{cluster:?}");
    let metrics = http.get("/api/metrics", &reader).await;
    assert!(metrics.ok(200)["points"].is_array() && metrics.body["since_ms"].is_u64(), "{metrics:?}");
    eventually_within("the observer's counts", WAIT, async || {
        http.get("/api/metrics", &reader).await.body["counts"]["running"].is_u64()
    })
    .await;
    let text = http.get("/metrics", &reader).await;
    let text = text.body.as_str().unwrap().to_owned();
    for series in ["wf_step_seconds_bucket", "wf_runs_started_total{workflow=\"quick\"}", "wf_runs{state=\"running\"}"]
    {
        assert!(text.contains(series), "{series} not in:\n{text}");
    }

    // A token taken out of the file is refused once the file is read again.
    let [reader2] = <[String; 1]>::try_from(tokens(&file, &[("reader2", "read")])).unwrap();
    eventually_within("the old token refused", WAIT, async || http.get("/api/runs", &reader).await.status == 401).await;
    http.get("/api/runs", &reader2).await.ok(200);

    down(nodes).await;
}

/// Starts past a rate per workflow, and per token name, are refused `limited` with when to try again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn starts_past_a_rate_are_limited() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let b = Boot { discovery: &discovery, dir: dir.path(), replicas: &HISTORY, acceptors: &HISTORY, max_spawned: None };
    let mut nodes = Vec::new();
    for h in HISTORY {
        nodes.push(boot(&b, h, &["history"]).await);
    }
    nodes.push(boot(&b, "e1", &["engine", "api"]).await);
    settle(&nodes, 4).await;
    let file = dir.path().join("tokens.toml");
    let [one, two] = <[String; 2]>::try_from(tokens(&file, &[("one", "start"), ("two", "start")])).unwrap();
    let mut cfg = ApiConfig::new(hash_password("pw").unwrap());
    cfg.tokens = Some(file);
    cfg.quotas = nodes[3].quotas.clone();
    cfg.workflow_starts = Some(Rate::new("wf_starts", 6, Duration::from_secs(60)));
    cfg.token_starts = Some(Rate::new("wf_token_starts", 3, Duration::from_secs(60)));
    let api = Api::new(&nodes[3].system, &nodes[3].node, &nodes[3].cluster, cfg).unwrap();
    let http = Http::new(&serve(&api).await);

    let mut by_one = Vec::new();
    for n in 0..5 {
        by_one.push(http.start(&one, "quick", &format!("one-{n}"), json!(n)).await);
    }
    assert!(by_one[0].status == 201, "{by_one:?}");
    let limited = by_one.iter().find(|s| s.status == 429).expect("token one past its 3 a minute");
    limited.refused(429, "limited");
    assert!(limited.body["message"].as_str().unwrap().contains("one"), "{limited:?}");
    // Token two has its own rate; the workflow's is across tokens.
    let mut by_two = Vec::new();
    for n in 0..8 {
        by_two.push(http.start(&two, "quick", &format!("two-{n}"), json!(n)).await);
    }
    assert!(by_two.iter().any(|s| s.status == 201), "{by_two:?}");
    by_two.iter().find(|s| s.status == 429).expect("past a rate").refused(429, "limited");
    down(nodes).await;
}

/// Replicas h1 to h3, of which only h1 runs: a run cannot write, so a start answers `unavailable`, and `/ready` says
/// there is no journal quorum. The leases come from an acceptor on the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_majority_of_replicas_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let b = Boot { discovery: &discovery, dir: dir.path(), replicas: &HISTORY, acceptors: &["e1"], max_spawned: None };
    let nodes = vec![boot(&b, "h1", &["history"]).await, boot(&b, "e1", &["engine", "api"]).await];
    settle(&nodes, 2).await;
    let mut cfg = ApiConfig::new(hash_password("pw").unwrap());
    // A call gives up on a run sooner than the shipped 10 s.
    cfg.patience = Duration::from_secs(5);
    let api = Api::new(&nodes[1].system, &nodes[1].node, &nodes[1].cluster, cfg).unwrap();
    let http = Http::new(&serve(&api).await);
    let login =
        http.call(Method::POST, "/api/login", None, Some(json!({ "password": "pw", "token": true }).to_string()));
    let token = login.await.ok(200)["token"].as_str().unwrap().to_owned();

    let ready = http.call(Method::GET, "/ready", None, None).await;
    assert_eq!(ready.status, 503, "{ready:?}");
    assert_eq!(ready.body["journal"], false, "{ready:?}");
    http.start(&token, "quick", "u-1", json!(1)).await.refused(503, "unavailable");
    // A list reads one replica's index, which answers without a majority.
    http.get("/api/runs", &token).await.ok(200);
    down(nodes).await;
}

/// An engine that takes two keyed actors at most: past them a start answers `busy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_at_capacity_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let b =
        Boot { discovery: &discovery, dir: dir.path(), replicas: &HISTORY, acceptors: &HISTORY, max_spawned: Some(2) };
    let mut nodes = Vec::new();
    for h in HISTORY {
        nodes.push(boot(&b, h, &["history", "api"]).await);
    }
    nodes.push(boot(&b, "e1", &["engine", &queue_label("none")]).await);
    settle(&nodes, 4).await;
    let mut cfg = ApiConfig::new(hash_password("pw").unwrap());
    // A call gives up on a run sooner than the shipped 10 s.
    cfg.patience = Duration::from_secs(2);
    // The api is on h1, so each run is a spawn the engine counts.
    let api = Api::new(&nodes[0].system, &nodes[0].node, &nodes[0].cluster, cfg).unwrap();
    let http = Http::new(&serve(&api).await);
    let login =
        http.call(Method::POST, "/api/login", None, Some(json!({ "password": "pw", "token": true }).to_string()));
    let token = login.await.ok(200)["token"].as_str().unwrap().to_owned();

    let mut said = Vec::new();
    for n in 0..6 {
        // A lease round slower than the patience is `unavailable`, and a start the same again is fine.
        let mut start = http.start(&token, "far", &format!("f-{n}"), json!(null)).await;
        let tried = std::time::Instant::now();
        while start.body["error"] == "unavailable" && tried.elapsed() < WAIT {
            start = http.start(&token, "far", &format!("f-{n}"), json!(null)).await;
        }
        let busy = start.status == 503;
        said.push(start);
        if busy {
            break;
        }
    }
    let last = said.last().unwrap();
    assert!(said[0].status.is_success(), "{said:?}");
    last.refused(503, "busy");
    down(nodes).await;
}

/// The console beside the API, under a base path a proxy puts it at: its page names the base, its files are kept by
/// the browser, a page's path is the page, and nothing under `/api` is. `/api/session` says who is signed in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_is_served_beside_the_api_under_its_base_path() {
    let dir = tempfile::tempdir().unwrap();
    let discovery = MemoryDiscovery::new();
    let b = Boot { discovery: &discovery, dir: dir.path(), replicas: &HISTORY, acceptors: &["e1"], max_spawned: None };
    let nodes = vec![boot(&b, "e1", &["engine", "api"]).await];
    settle(&nodes, 1).await;
    let hash = hash_password("pw").unwrap();
    for bad in ["ops", "/ops", "ops/", "/o\"ps/"] {
        let cfg = ApiConfig { base_path: bad.into(), ..ApiConfig::new(&hash) };
        assert!(Api::new(&nodes[0].system, &nodes[0].node, &nodes[0].cluster, cfg).is_err(), "{bad}");
    }
    let cfg = ApiConfig { base_path: "/ops/wf/".into(), ..ApiConfig::new(&hash) };
    let api = Api::new(&nodes[0].system, &nodes[0].node, &nodes[0].cluster, cfg).unwrap();
    let url = serve(&api).await;
    let client = reqwest::Client::new();
    let open = async |method: Method, path: &str, cookie: &str| {
        client.request(method, format!("{url}{path}")).header("cookie", cookie).send().await.unwrap()
    };
    let kind = |response: &reqwest::Response| response.headers()["content-type"].to_str().unwrap().to_owned();

    let shell = open(Method::GET, "/ops/wf/", "").await;
    assert_eq!((shell.status().as_u16(), kind(&shell).as_str()), (200, "text/html; charset=utf-8"));
    assert_eq!(shell.headers()["cache-control"], "no-store");
    assert!(shell.headers()["content-security-policy"].to_str().unwrap().contains("frame-ancestors 'none'"));
    let index = shell.text().await.unwrap();
    assert!(index.contains(r#"<base href="/ops/wf/" />"#) && index.contains(r#"<div id="root">"#), "{index}");

    // Every file the page names is there, with its type, under the base or with it cut, and kept by the browser.
    let named: Vec<&str> = index.split('"').filter_map(|quoted| quoted.strip_prefix("./")).collect();
    assert!(named.iter().any(|file| file.ends_with(".js")) && named.iter().any(|file| file.ends_with(".css")));
    for file in named {
        for path in [format!("/ops/wf/{file}"), format!("/{file}")] {
            let asset = open(Method::GET, &path, "").await;
            let wanted = if file.ends_with(".js") { "text/javascript" } else { "text/css" };
            assert_eq!((asset.status().as_u16(), kind(&asset).as_str()), (200, wanted), "{path}");
            assert!(asset.headers()["cache-control"].to_str().unwrap().contains("immutable"), "{path}");
            assert_eq!(asset.headers()["x-content-type-options"], "nosniff");
        }
    }
    // A page's path is the browser's to read; a run id may hold a slash.
    for page in
        ["/ops/wf/login", "/ops/wf/runs/batch-7/3", "/ops/wf/runs/order.v2", "/cluster", "/", "/ops/wf/index.html"]
    {
        let fallback = open(Method::GET, page, "").await;
        assert_eq!(fallback.status(), 200, "{page}");
        assert_eq!(fallback.text().await.unwrap(), index, "{page}");
    }
    let missing = open(Method::GET, "/ops/wf/assets/none.js", "").await;
    assert_eq!(missing.status(), 404);
    assert_eq!(open(Method::POST, "/ops/wf/login", "").await.status(), 404, "only a GET reads a page");
    for path in ["/api", "/api/nothing", "/api/runs/x/nothing"] {
        let unknown = open(Method::GET, path, "").await;
        assert_eq!(unknown.status(), 404, "{path}");
        assert!(kind(&unknown).starts_with("application/json"), "{path}");
    }

    // The sign-in page asks who is signed in: nobody, then the admin until the session ends.
    let body = |response: reqwest::Response| async move {
        serde_json::from_slice::<Value>(&response.bytes().await.unwrap()).unwrap()
    };
    assert_eq!(
        body(open(Method::GET, "/api/session", "").await).await,
        json!({ "name": null, "scope": null, "expires_ms": null })
    );
    let login = client
        .post(format!("{url}/api/login"))
        .header("content-type", "application/json")
        .body(json!({ "password": "pw" }).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 200);
    let cookie = login.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap().to_owned();
    let expires = body(login).await["expires_ms"].as_u64().unwrap();
    let session = body(open(Method::GET, "/api/session", &cookie).await).await;
    assert_eq!(session, json!({ "name": "admin", "scope": "admin", "expires_ms": expires }));
    assert_eq!(open(Method::GET, "/api/cluster", &cookie).await.status(), 200);
    down(nodes).await;
}
