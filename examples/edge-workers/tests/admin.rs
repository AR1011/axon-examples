//! The operator's side on real nodes over loopback TCP, with the real engine: the control node's HTTP API, and the
//! `edge` command line against it. A control node, a gateway behind an HTTP listener, and runners.

#[path = "support/real.rs"]
mod real;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use argon2::password_hash::{PasswordHasher, SaltString};
use axon_cluster::{MemoryDiscovery, MemoryLeases};
use axon_testkit::{eventually, eventually_within};
use edge_workers::admin::AdminConfig;
use edge_workers::engine::starling::Version;
use edge_workers::node::{Roles, Settings};
use edge_workers::templates::TEMPLATES;
use real::js;
use reqwest::Method;
use serde_json::{Value, json};

const PASSWORD: &str = "correct horse";

/// The password's hash at argon2's lowest cost: an unoptimised build takes seconds over the default one.
fn password_hash() -> String {
    let params = argon2::Params::new(8, 1, 1, None).unwrap();
    let cheap = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    cheap.hash_password(PASSWORD.as_bytes(), &SaltString::encode_b64(b"0123456789abcdef").unwrap()).unwrap().to_string()
}

type Member = real::Member<Version>;

struct Edge {
    control: Member,
    gateway: Member,
    runners: Vec<Member>,
    /// The admin API's address, and the gateway's.
    api: String,
    http: String,
    client: reqwest::Client,
    token: String,
    dir: tempfile::TempDir,
}

async fn member(
    id: &str,
    roles: Roles,
    (discovery, leases): (&MemoryDiscovery, &MemoryLeases),
    dir: &Path,
    settings: &Settings,
) -> Member {
    let mut cfg = real::config(id, roles, discovery);
    // Objects run under a lease. One process, so the leases are in its memory.
    cfg.ownership = Some(Arc::new(leases.authority()));
    // The proxy template's upstream is a listener on loopback.
    let engine = real::engine(true);
    real::member(cfg, roles, real::os_stores(&dir.join(id)), engine, settings.clone()).await
}

async fn listen(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    at
}

impl Edge {
    async fn start(runners: usize) -> Self {
        Self::start_at(runners, "/").await
    }

    /// With the console at `base_path`.
    async fn start_at(runners: usize, base_path: &str) -> Self {
        let mut settings = Settings::default();
        settings.invoker.runtime = Some(js());
        let mut admin = AdminConfig { base_path: base_path.into(), ..AdminConfig::new(password_hash()) };
        // A point every 200 ms, so a test sees a worker's series without waiting 10 s for each.
        admin.observer.sample = Duration::from_millis(200);
        settings.admin = Some(admin);
        let dir = tempfile::tempdir().unwrap();
        let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
        let found = (&discovery, &leases);
        let roles = |control, gateway, runner| Roles { control, gateway, runner };
        let control = member("control", roles(true, false, false), found, dir.path(), &settings).await;
        let gateway = member("gw", roles(false, true, false), found, dir.path(), &settings).await;
        let mut started = Vec::new();
        for i in 1..=runners {
            started.push(member(&format!("r{i}"), roles(false, false, true), found, dir.path(), &settings).await);
        }
        let front = gateway.node.gateway.clone().unwrap();
        tokio::time::timeout(Duration::from_secs(20), front.wait_runners(|n| *n == runners)).await.unwrap();
        let api = listen(control.node.admin.as_ref().expect("the control node's admin").router()).await;
        let http = listen(front.router()).await;
        // As a gateway with http= does: the cluster hears where it is reached.
        edge_workers::gateway::advertise(&gateway.cluster, &http).await.unwrap();
        let hears = |view: &axon_cluster::View| !edge_workers::gateway::urls(view).is_empty();
        tokio::time::timeout(Duration::from_secs(20), control.cluster.wait_view(hears)).await.unwrap().unwrap();
        // A redirect is the redirect template's answer, not something to follow.
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        let mut edge = Self { control, gateway, runners: started, api, http, client, token: String::new(), dir };
        let (status, said) = edge.api(Method::POST, "/login", json!({ "password": PASSWORD, "token": true })).await;
        assert_eq!(status, 200, "{said}");
        edge.token = said["token"].as_str().unwrap().to_owned();
        edge
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.client.request(method, format!("{}/api{path}", self.api)).bearer(&self.token)
    }

    /// The API's status and answer. A `Null` body sends none.
    async fn api(&self, method: Method, path: &str, body: Value) -> (u16, Value) {
        let request = self.request(method, path);
        let request = match body {
            Value::Null => request,
            body => request.header("content-type", "application/json").body(body.to_string()),
        };
        let response = request.send().await.unwrap();
        let (status, text) = (response.status().as_u16(), response.text().await.unwrap());
        (status, serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn get(&self, path: &str) -> Value {
        let (status, said) = self.api(Method::GET, path, Value::Null).await;
        assert_eq!(status, 200, "GET {path}: {said}");
        said
    }

    /// A change that must be taken: its answer.
    async fn change(&self, method: Method, path: &str, body: Value) -> Value {
        let (status, said) = self.api(method.clone(), path, body).await;
        assert!(status == 200 || status == 201, "{method} {path}: {status} {said}");
        said
    }

    /// Waits until the gateway routes `name` at `seq` and every runner runs it.
    async fn serving(&self, name: &str, seq: &Value) {
        let seq = seq.as_u64().unwrap_or_else(|| panic!("a seq: {seq}"));
        // A runner compiles the bundle before it serves it: seconds, with an unoptimised build around the compiler.
        let what = format!("{name} at {seq} on the gateway and every runner");
        eventually_within(&what, Duration::from_secs(90), async || {
            let at = |member: &Member| member.node.serving.as_ref().unwrap().seq(name) == Some(seq);
            at(&self.gateway) && self.runners.iter().all(at)
        })
        .await;
    }

    /// A request through the gateway's listener.
    async fn call(&self, path: &str) -> reqwest::Response {
        self.client.get(format!("{}{path}", self.http)).send().await.unwrap()
    }

    /// One request through the admin API, asked again until the control node's own gateway has found the runners.
    async fn invoke(&self, name: &str, ask: Value) -> Value {
        let path = format!("/workers/{name}/invoke");
        let mut ran = Value::Null;
        eventually("a runner answers the control node", async || {
            ran = self.change(Method::POST, &path, ask.clone()).await;
            ran["error"] != "unavailable"
        })
        .await;
        ran
    }

    async fn stop(self) {
        for member in self.runners.into_iter().chain([self.gateway, self.control]) {
            member.system.shutdown().await.unwrap();
        }
    }
}

trait Bearer {
    fn bearer(self, token: &str) -> Self;
}

impl Bearer for reqwest::RequestBuilder {
    fn bearer(self, token: &str) -> Self {
        if token.is_empty() { self } else { self.header("authorization", format!("Bearer {token}")) }
    }
}

fn answer(text: &str) -> String {
    format!(r#"export default {{ async fetch(request) {{ return new Response("{text}"); }} }}"#)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_api_answers_only_a_session_from_the_password() {
    let edge = Edge::start(0).await;
    let open = |path: &str| edge.client.get(format!("{}/api{path}", edge.api));
    for path in ["/workers", "/cluster", "/tail", "/templates", "/session", "/workers/x/vars"] {
        let refused = open(path).send().await.unwrap();
        assert_eq!(refused.status(), 401, "{path}");
        assert_eq!(refused.json_value().await["error"], "unauthorized");
    }
    let login = |body: Value| {
        edge.client
            .post(format!("{}/api/login", edge.api))
            .header("content-type", "application/json")
            .body(body.to_string())
    };
    let wrong = login(json!({ "password": "correct horse " })).send().await.unwrap();
    assert_eq!(wrong.status(), 401);
    assert!(wrong.headers().get("set-cookie").is_none());

    // A browser: the session is a cookie no script can read and no other site can send.
    let signed_in = login(json!({ "password": PASSWORD })).send().await.unwrap();
    assert_eq!(signed_in.status(), 200);
    let cookie = signed_in.headers().get("set-cookie").unwrap().to_str().unwrap().to_owned();
    assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"), "{cookie}");
    assert!(signed_in.json_value().await["token"].is_null(), "a browser's session is not readable by its page");
    let session = cookie.split(';').next().unwrap();
    let with_cookie = open("/session").header("cookie", format!("theme=dark; {session}")).send().await.unwrap();
    assert_eq!(with_cookie.status(), 200);
    assert!(with_cookie.json_value().await["expires_ms"].as_u64().unwrap() > 0);

    // The command line: the same session as a bearer token.
    assert_eq!(edge.get("/templates").await["templates"].as_array().unwrap().len(), TEMPLATES.len());
    let mut forged = edge.token.clone();
    let last = if forged.pop() == Some('0') { '1' } else { '0' };
    forged.push(last);
    assert_eq!(open("/workers").bearer(&forged).send().await.unwrap().status(), 401);
    // A later expiry under the same signature.
    let (_, mac) = edge.token.split_once('.').unwrap();
    assert_eq!(open("/workers").bearer(&format!("99999999999999.{mac}")).send().await.unwrap().status(), 401);
    assert_eq!(open("/workers").header("cookie", format!("edge_session={forged}")).send().await.unwrap().status(), 401);

    let out = edge.client.post(format!("{}/api/logout", edge.api)).bearer(&edge.token).send().await.unwrap();
    assert!(out.headers().get("set-cookie").unwrap().to_str().unwrap().contains("Max-Age=0"));
    edge.stop().await;
}

trait JsonValue {
    async fn json_value(self) -> Value;
}

impl JsonValue for reqwest::Response {
    async fn json_value(self) -> Value {
        let text = self.text().await.unwrap();
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    }
}

const GREET: &str = r#"export default { async fetch(request) {
    const url = new URL(request.url);
    console.log("greet " + request.method + " " + url.pathname);
    return new Response("one:" + (await request.text()));
} }"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_is_made_deployed_called_rolled_back_and_removed_through_the_api() {
    let edge = Edge::start(2).await;
    let (status, made) = edge.api(Method::POST, "/workers", json!({ "name": "greet", "source": GREET })).await;
    assert_eq!((status, &made["version"]), (201, &json!(1)), "{made}");
    edge.serving("greet", &made["seq"]).await;
    let (status, again) = edge.api(Method::POST, "/workers", json!({ "name": "greet", "source": GREET })).await;
    assert_eq!((status, again["error"].as_str()), (409, Some("exists")));
    assert_eq!(edge.call("/w/greet/").await.text().await.unwrap(), "one:");

    // The API says where the gateway serves the worker: the URL the call above went to.
    let urls = json!([{ "gateway": "gw", "url": format!("{}/w/greet/", edge.http) }]);
    assert_eq!(edge.get("/workers/greet").await["urls"], urls);
    assert_eq!(edge.get("/workers").await["workers"][0]["urls"], urls);
    let cluster = edge.get("/cluster").await;
    let mut said: Vec<_> =
        cluster["nodes"].as_array().unwrap().iter().map(|n| (n["id"].as_str().unwrap(), n["url"].as_str())).collect();
    said.sort_unstable();
    assert_eq!(said, vec![("control", None), ("gw", Some(edge.http.as_str())), ("r1", None), ("r2", None)]);

    // A second version takes the traffic, and a rollback gives it back to the first.
    let second = json!({ "source": answer("two"), "message": "second" });
    let uploaded = edge.change(Method::POST, "/workers/greet/versions", second).await;
    assert_eq!(uploaded["version"], 2);
    edge.serving("greet", &uploaded["seq"]).await;
    assert_eq!(edge.call("/w/greet/").await.text().await.unwrap(), "two");
    let worker = edge.get("/workers/greet").await;
    assert_eq!((&worker["deployment"]["stable"], &worker["latest"]), (&json!(2), &json!(2)), "{worker}");
    let versions: Vec<_> = worker["versions"].as_array().unwrap().iter().map(|v| (&v["id"], &v["message"])).collect();
    assert_eq!(versions, vec![(&json!(2), &json!("second")), (&json!(1), &json!(""))], "newest first");
    let first = edge.get("/workers/greet/versions/1").await;
    assert_eq!((&first["source"], &first["source_bytes"]), (&json!(GREET), &json!(GREET.len())), "{first}");
    // What the engine built is apart from the size of the version, and larger than any source may be.
    assert!(first["build_bytes"].as_u64().unwrap() > 1 << 20 && worker["versions"][1]["source_bytes"] == GREET.len());

    let back = edge.change(Method::POST, "/workers/greet/rollback", Value::Null).await;
    assert_eq!((&back["version"], &back["from"]), (&json!(1), &json!(2)), "{back}");
    edge.serving("greet", &back["seq"]).await;
    assert_eq!(edge.call("/w/greet/").await.text().await.unwrap(), "one:");
    let (status, none) = edge.api(Method::POST, "/workers/greet/rollback", Value::Null).await;
    assert_eq!((status, none["error"].as_str()), (409, Some("nothing_to_roll_back")), "{none}");

    // One request through the API, on a runner, and what it left behind.
    let ask = json!({ "method": "POST", "path": "/from/admin?x=1", "body": "ping" });
    let ran = edge.invoke("greet", ask).await;
    assert_eq!(
        (&ran["status"], &ran["body"], &ran["version"], &ran["error"]),
        (&json!(200), &json!("one:ping"), &json!(1), &Value::Null)
    );
    let (node, trace) = (ran["node"].as_str().unwrap().to_owned(), ran["trace"].as_str().unwrap().to_owned());
    assert!(["r1", "r2"].contains(&node.as_str()) && ran["cpu_us"].as_u64().unwrap() > 0, "{ran}");

    let mut metrics = Value::Null;
    eventually("the worker's points add up to its requests", async || {
        metrics = edge.get("/workers/greet/metrics").await;
        let points = metrics["series"].as_array().unwrap().iter().map(|point| point["requests"].as_u64().unwrap());
        points.sum::<u64>() == 4
    })
    .await;
    assert_eq!((&metrics["total"]["requests"], &metrics["total"]["errors"]), (&json!(4), &json!(0)), "{metrics}");
    assert!(
        metrics["total"]["cpu_p50_us"].as_u64().unwrap() > 0 && metrics["total"]["wall_p99_us"].as_u64().unwrap() > 0
    );
    let by_node = metrics["by_node"].as_array().unwrap();
    assert_eq!(by_node.iter().map(|row| row["requests"].as_u64().unwrap()).sum::<u64>(), 4, "{by_node:?}");
    assert!(by_node.iter().any(|row| row["node"] == node.as_str()), "{by_node:?}");
    let listed = edge.get("/workers").await;
    assert_eq!(listed["workers"][0]["metrics"]["requests"], 4, "{listed}");

    let mut found = Value::Null;
    eventually("the invocation reached the control node", async || {
        let (status, said) = edge.api(Method::GET, &format!("/invocations/{trace}"), Value::Null).await;
        found = said;
        status == 200
    })
    .await;
    assert_eq!(
        (&found["node"], &found["status"], &found["url"]),
        (&json!(node), &json!(200), &json!("/from/admin?x=1"))
    );
    assert_eq!(found["logs"], json!(["Log: greet POST /from/admin"]));
    let listed = edge.get("/workers/greet/invocations?q=from/admin&status=2xx").await;
    assert_eq!(listed["invocations"].as_array().unwrap().len(), 1, "{listed}");
    eventually("all four invocations listed", async || {
        edge.get("/workers/greet/invocations").await["invocations"].as_array().unwrap().len() == 4
    })
    .await;
    assert_eq!(edge.get("/workers/greet/invocations?status=5xx").await["invocations"], json!([]));
    assert_eq!(edge.api(Method::GET, "/invocations/0000000000000000", Value::Null).await.0, 404);

    // A canary, and what each node serves.
    let split = json!({ "version": 1, "canary": { "version": 2, "percent": 25 } });
    let seq = edge.change(Method::POST, "/workers/greet/deploy", split).await["seq"].clone();
    edge.serving("greet", &seq).await;
    let cluster = edge.get("/cluster").await;
    let node = |id: &str| cluster["nodes"].as_array().unwrap().iter().find(|node| node["id"] == id).unwrap().clone();
    assert_eq!(
        (&cluster["node"], &node("control")["roles"], &node("gw")["roles"]),
        (&json!("control"), &json!(["control"]), &json!(["gateway"]))
    );
    for runner in ["r1", "r2"] {
        let served = &node(runner)["serving"]["greet"];
        assert_eq!(
            served,
            &json!({ "seq": seq, "stable": 1, "canary": { "version": 2, "percent": 25 }, "warm": true }),
            "{runner}"
        );
    }

    // Refusals say what was wrong.
    for (method, path, body, status, code) in [
        (Method::GET, "/workers/nope", Value::Null, 404, "not_found"),
        (Method::POST, "/workers/nope/invoke", Value::Null, 404, "not_found"),
        (Method::POST, "/workers/greet/deploy", json!({ "version": 9 }), 404, "not_found"),
        (Method::POST, "/workers/greet/versions", json!({ "source": "export default {" }), 422, "load"),
        (Method::POST, "/workers", json!({ "name": "Not A Name", "source": GREET }), 400, "invalid"),
        (Method::POST, "/workers", json!({ "name": "other", "template": "nope" }), 400, "invalid"),
    ] {
        let (got, said) = edge.api(method.clone(), path, body).await;
        assert_eq!((got, said["error"].as_str()), (status, Some(code)), "{method} {path}: {said}");
        assert!(said["message"].as_str().is_some_and(|message| !message.is_empty()), "{said}");
    }
    assert_eq!(edge.get("/workers/greet").await["latest"], 2, "a source that does not load makes no version");

    edge.change(Method::DELETE, "/workers/greet", Value::Null).await;
    assert_eq!(edge.api(Method::GET, "/workers/greet", Value::Null).await.0, 404);
    eventually("the gateway stops routing it", async || edge.call("/w/greet/").await.status() == 404).await;
    edge.stop().await;
}

const VAULT: &str = r#"export default { async fetch(request, env) {
    return new Response(String(env.API_KEY).length + ":" + env.GREETING);
} }"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_reaches_the_worker_and_no_call_returns_it() {
    const SECRET: &str = "s3cr3t-0f-tw3nty-ch4r";
    let edge = Edge::start(1).await;
    // A worker can be made with its secrets, and without taking traffic yet.
    let made = json!({ "name": "vault", "source": VAULT, "secrets": { "API_KEY": "first" }, "deploy": false });
    let made = edge.change(Method::POST, "/workers", made).await;
    assert_eq!((&made["version"], &made["seq"]), (&json!(1), &Value::Null), "{made}");
    assert_eq!(edge.get("/workers/vault/versions/1").await["secrets"], json!(["API_KEY"]));
    let plain = edge.change(Method::PUT, "/workers/vault/vars/GREETING", json!({ "value": "hello" })).await;
    assert_eq!(plain["version"], 2, "a change of a variable is a new version");
    let secret = json!({ "type": "secret", "value": SECRET });
    let set = edge.change(Method::PUT, "/workers/vault/vars/API_KEY", secret).await;
    assert_eq!(set["version"], 3);
    edge.serving("vault", &set["seq"]).await;
    assert_eq!(edge.invoke("vault", Value::Null).await["body"], format!("{}:hello", SECRET.len()));
    // Unsaved code runs with the variables and none of the secrets: whoever may preview could print them.
    let leak = "export default { async fetch(request, env) { return new Response(JSON.stringify(env)) } }";
    let draft = edge.change(Method::POST, "/workers/vault/preview", json!({ "source": leak })).await;
    assert_eq!(draft["body"], json!({ "GREETING": "hello" }).to_string(), "{draft}");

    let vars = edge.get("/workers/vault/vars").await;
    let expected =
        json!([{ "name": "GREETING", "type": "plain", "value": "hello" }, { "name": "API_KEY", "type": "secret" }]);
    assert_eq!((&vars["vars"], &vars["version"]), (&expected, &json!(3)));
    assert_eq!(edge.get("/workers/vault/versions/3").await["secrets"], json!(["API_KEY"]));
    for path in [
        "/workers",
        "/workers/vault",
        "/workers/vault/versions",
        "/workers/vault/versions/3",
        "/workers/vault/vars",
        "/workers/vault/metrics",
        "/workers/vault/invocations",
        "/invocations",
        "/cluster",
    ] {
        let text = edge.get(path).await.to_string();
        assert!(!text.contains(SECRET), "{path} returns the secret: {text}");
    }

    // A new source keeps the secret, and removing the secret is one more version.
    let longer = VAULT.replace("env.GREETING", "env.GREETING + \"!\"");
    let longer = json!({ "source": longer, "secrets": { "EXTRA": "more" } });
    let uploaded = edge.change(Method::POST, "/workers/vault/versions", longer).await;
    assert_eq!(edge.get("/workers/vault/versions/4").await["secrets"], json!(["API_KEY", "EXTRA"]));
    edge.serving("vault", &uploaded["seq"]).await;
    assert_eq!(edge.invoke("vault", Value::Null).await["body"], format!("{}:hello!", SECRET.len()));
    let removed = edge.change(Method::DELETE, "/workers/vault/vars/API_KEY", Value::Null).await;
    assert_eq!(removed["version"], 5);
    edge.serving("vault", &removed["seq"]).await;
    assert_eq!(edge.invoke("vault", Value::Null).await["body"], "9:hello!", "undefined, as a string");
    assert_eq!(
        edge.get("/workers/vault/vars").await["vars"],
        json!([{ "name": "GREETING", "type": "plain", "value": "hello" }, { "name": "EXTRA", "type": "secret" }])
    );
    assert_eq!(edge.api(Method::DELETE, "/workers/vault/vars/API_KEY", Value::Null).await.0, 404);
    edge.stop().await;
}

/// Reads server-sent events until one's data satisfies `wanted`.
/// The console is in the binary: its page and its files need no session, and what they show comes from the API,
/// which does.
#[tokio::test(flavor = "multi_thread")]
async fn the_console_s_page_and_files_are_served_beside_the_api() {
    let edge = Edge::start(0).await;
    let open = |path: &str| edge.client.get(format!("{}{path}", edge.api)).send();
    let kind = |response: &reqwest::Response| response.headers()["content-type"].to_str().unwrap().to_owned();

    let shell = open("/").await.unwrap();
    assert_eq!((shell.status().as_u16(), kind(&shell).as_str()), (200, "text/html; charset=utf-8"));
    assert_eq!(shell.headers()["cache-control"], "no-store");
    assert!(shell.headers()["content-security-policy"].to_str().unwrap().contains("frame-ancestors 'none'"));
    let index = shell.text().await.unwrap();
    assert!(index.contains(r#"<base href="/" />"#) && index.contains(r#"<div id="root">"#), "{index}");

    // Every file the page names is there, with its type, and is kept by the browser: its name is its hash.
    let named: Vec<&str> = index.split('"').filter_map(|quoted| quoted.strip_prefix("./")).collect();
    assert!(named.iter().any(|file| file.ends_with(".js")) && named.iter().any(|file| file.ends_with(".css")));
    for file in named {
        let asset = open(&format!("/{file}")).await.unwrap();
        let wanted = if file.ends_with(".js") { "text/javascript" } else { "text/css" };
        assert_eq!((asset.status().as_u16(), kind(&asset).as_str()), (200, wanted), "{file}");
        assert!(asset.headers()["cache-control"].to_str().unwrap().contains("immutable"), "{file}");
        assert_eq!(asset.headers()["x-content-type-options"], "nosniff");
        assert!(!asset.bytes().await.unwrap().is_empty(), "{file}");
    }

    // A page's path is the browser's to read: the same page.
    for page in ["/login", "/workers", "/workers/hello/logs", "/cluster", "/index.html"] {
        let fallback = open(page).await.unwrap();
        assert_eq!(fallback.status(), 200, "{page}");
        assert_eq!(fallback.text().await.unwrap(), index, "{page}");
    }
    // A file the console does not have is not a page.
    let missing = open("/assets/none.js").await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.json_value().await["error"], "not_found");

    // Under /api nothing is a page: no route is a JSON 404, with a session or without, and a route needs the session.
    for path in ["/api/nothing", "/api", "/api/workers/x/nothing"] {
        for token in ["", edge.token.as_str()] {
            let unknown = open_with(&edge, path, token).await;
            assert_eq!(unknown.status(), 404, "{path}");
            assert!(kind(&unknown).starts_with("application/json"), "{path}");
            assert_eq!(unknown.json_value().await["error"], "not_found", "{path}");
        }
    }
    assert_eq!(open("/api/workers").await.unwrap().status(), 401);
    let posted = edge.client.post(format!("{}/login", edge.api)).send().await.unwrap();
    assert_eq!(posted.status(), 404, "only a GET reads a page");
    edge.stop().await;
}

async fn open_with(edge: &Edge, path: &str, token: &str) -> reqwest::Response {
    edge.client.get(format!("{}{path}", edge.api)).bearer(token).send().await.unwrap()
}

/// Behind a proxy that puts the console at a path, the page says so; the proxy may pass the path whole or cut.
#[tokio::test(flavor = "multi_thread")]
async fn the_console_under_a_base_path_names_it_in_its_page() {
    let edge = Edge::start_at(0, "/ops/edge/").await;
    let text = async |path: &str| open_with(&edge, path, "").await.text().await.unwrap();
    let index = text("/ops/edge/").await;
    assert!(index.contains(r#"<base href="/ops/edge/" />"#), "{index}");
    for page in ["/ops/edge/workers/hello/code", "/cluster", "/"] {
        assert_eq!(text(page).await, index, "{page}");
    }
    let file = index.split('"').find_map(|quoted| quoted.strip_prefix("./")).expect("a file the page names");
    for path in [format!("/ops/edge/{file}"), format!("/{file}")] {
        assert_eq!(open_with(&edge, &path, "").await.status(), 200, "{path}");
    }
    // The API is not below the base path.
    assert_eq!(open_with(&edge, "/api/session", &edge.token).await.status(), 200);
    edge.stop().await;
}

async fn next_event(tail: &mut reqwest::Response, buffer: &mut Vec<u8>, wanted: impl Fn(&Value) -> bool) -> Value {
    let read = async {
        loop {
            while let Some(end) = buffer.windows(2).position(|pair| pair == b"\n\n") {
                let event: Vec<u8> = buffer.drain(..end + 2).collect();
                let event = String::from_utf8(event).unwrap();
                let data = event.lines().find_map(|line| line.strip_prefix("data:"));
                let record: Value = data.and_then(|data| serde_json::from_str(data).ok()).unwrap_or(Value::Null);
                if wanted(&record) {
                    assert!(event.contains("event: invocation"), "{event}");
                    return record;
                }
            }
            buffer.extend_from_slice(&tail.chunk().await.unwrap().expect("the tail goes on"));
        }
    };
    tokio::time::timeout(Duration::from_secs(20), read).await.expect("the invocation on the tail")
}

const TALK: &str = r#"export default { async fetch(request) {
    console.log("talk heard " + new URL(request.url).searchParams.get("word"));
    console.error("and warned");
    return new Response("saved");
} }"#;

const UNSAVED: &str = r#"export default { async fetch(request, env) {
    const url = new URL(request.url);
    console.log("preview " + request.method + " " + url.pathname + url.search);
    if (url.searchParams.has("throw")) throw new Error("not yet right");
    return new Response("unsaved " + env.MODE + " " + request.headers.get("x-a") + " " + (await request.text()),
        { status: 202, headers: { "x-preview": "yes" } });
} }"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_tail_receives_each_call_with_its_logs_and_a_preview_runs_unsaved_code_once() {
    let edge = Edge::start(2).await;
    let made = edge.change(Method::POST, "/workers", json!({ "name": "talk", "source": TALK })).await;
    let quiet = edge.change(Method::POST, "/workers", json!({ "name": "quiet", "source": answer("quiet") })).await;
    edge.serving("talk", &made["seq"]).await;
    edge.serving("quiet", &quiet["seq"]).await;

    assert_eq!(edge.client.get(format!("{}/api/tail", edge.api)).send().await.unwrap().status(), 401);
    let mut tail = edge.request(Method::GET, "/workers/talk/tail").send().await.unwrap();
    assert_eq!(tail.status(), 200);
    assert_eq!(tail.headers().get("content-type").unwrap(), "text/event-stream");
    let mut buffer = Vec::new();
    // The other worker's calls are not on this tail: the first event is the first call of `talk`.
    assert_eq!(edge.call("/w/quiet/").await.status(), 200);
    for word in ["first", "second"] {
        let called = edge.call(&format!("/w/talk/?word={word}")).await;
        let trace = called.headers().get("x-trace-id").unwrap().to_str().unwrap().to_owned();
        let node = called.headers().get("x-edge-node").unwrap().to_str().unwrap().to_owned();
        let event = next_event(&mut tail, &mut buffer, |record| !record.is_null()).await;
        assert_eq!((&event["trace"], &event["node"], &event["worker"]), (&json!(trace), &json!(node), &json!("talk")));
        assert_eq!(event["logs"], json!([format!("Log: talk heard {word}"), "Error: and warned"]), "{event}");
        assert_eq!((&event["status"], &event["outcome"], &event["version"]), (&json!(200), &json!("ok"), &json!(1)));
    }
    drop(tail);

    // Unsaved code runs once on the control node, with what a caller would see, and leaves nothing.
    let request = json!({ "method": "post", "path": "/p?x=1", "headers": { "x-a": "b" }, "body": "payload" });
    let preview = json!({ "source": UNSAVED, "env": { "MODE": "draft" }, "request": request });
    let ran = edge.change(Method::POST, "/workers/talk/preview", preview.clone()).await;
    assert_eq!(
        (&ran["status"], &ran["body"], &ran["error"]),
        (&json!(202), &json!("unsaved draft b payload"), &Value::Null),
        "{ran}"
    );
    assert!(ran["headers"].as_array().unwrap().contains(&json!(["x-preview", "yes"])), "{ran}");
    assert_eq!((&ran["logs"], &ran["node"]), (&json!(["Log: preview POST /p?x=1"]), &json!("control")));
    let (cpu, wall) = (ran["cpu_us"].as_u64().unwrap(), ran["wall_us"].as_u64().unwrap());
    assert!(cpu > 0 && wall >= cpu, "{ran}");

    let mut thrown = preview.clone();
    thrown["request"]["path"] = json!("/?throw");
    let ran = edge.change(Method::POST, "/workers/talk/preview", thrown).await;
    assert_eq!(ran["status"], 500);
    assert!(ran["error"].as_str().unwrap().contains("not yet right"), "{ran}");
    assert!(ran["logs"].as_array().unwrap().len() > 1, "the stack follows the console line: {ran}");
    let (status, bad) = edge.api(Method::POST, "/workers/talk/preview", json!({ "source": "export default 1" })).await;
    assert_eq!((status, bad["error"].as_str()), (422, Some("load")), "{bad}");
    // Of a worker that does not exist yet, too.
    let new = edge.change(Method::POST, "/workers/brand-new/preview", json!({ "source": answer("new") })).await;
    assert_eq!(new["body"], "new");

    assert_eq!(edge.api(Method::GET, "/workers/brand-new", Value::Null).await.0, 404);
    let worker = edge.get("/workers/talk").await;
    assert_eq!((&worker["latest"], worker["versions"].as_array().unwrap().len()), (&json!(1), 1), "{worker}");
    assert_eq!(edge.call("/w/talk/?word=x").await.text().await.unwrap(), "saved");
    let talk: u64 = edge.get("/workers/talk/metrics").await["total"]["requests"].as_u64().unwrap();
    assert_eq!(talk, 3, "no runner ran a preview");
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_template_deploys_and_does_what_it_says() {
    let edge = Edge::start(1).await;
    let upstream = axum::Router::new().fallback(|method: axum::http::Method, uri: axum::http::Uri| async move {
        ([("x-upstream", "yes")], format!("upstream {method} {uri}"))
    });
    let upstream = listen(upstream).await;
    let listed = edge.get("/templates").await;
    let names: Vec<&str> =
        listed["templates"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["hello", "json-api", "redirect", "proxy", "feature-flag", "counter"]);

    let vars = |name: &str| match name {
        "redirect" => json!({ "TARGET": "https://example.org" }),
        "proxy" => json!({ "UPSTREAM": upstream }),
        _ => json!({}),
    };
    for name in &names {
        let made =
            edge.change(Method::POST, "/workers", json!({ "name": name, "template": name, "vars": vars(name) })).await;
        edge.serving(name, &made["seq"]).await;
    }

    assert_eq!(edge.call("/w/hello/?name=you").await.text().await.unwrap(), "hello, you\n");

    let echo = edge.client.post(format!("{}/w/json-api/echo", edge.http)).body(r#"{"a":1}"#).send().await.unwrap();
    assert_eq!(echo.status(), 201);
    assert_eq!(echo.json_value().await["received"], json!({ "a": 1 }));
    let missing = edge.call("/w/json-api/nothing").await;
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.json_value().await["path"], "/nothing");

    let location =
        |response: &reqwest::Response| response.headers().get("location").unwrap().to_str().unwrap().to_owned();
    let docs = edge.call("/w/redirect/docs").await;
    assert_eq!((docs.status().as_u16(), location(&docs).as_str()), (302, "https://example.com/documentation"));
    let other = edge.call("/w/redirect/other?x=1").await;
    assert_eq!((other.status().as_u16(), location(&other).as_str()), (302, "https://example.org/other?x=1"));

    let proxied = edge.call("/w/proxy/hi?x=1").await;
    assert_eq!(proxied.status(), 200);
    let marks = |name: &str| proxied.headers().get(name).map(|value| value.to_str().unwrap().to_owned());
    assert_eq!((marks("x-proxied-by").as_deref(), marks("x-upstream").as_deref()), (Some("edge-workers"), Some("yes")));
    assert_eq!(proxied.text().await.unwrap(), "upstream GET /hi?x=1");

    // The flag is a variable: setting it is a deployment, and the worker answers by it.
    assert_eq!(edge.call("/w/feature-flag/").await.json_value().await["checkout"], "classic");
    let on = edge.change(Method::PUT, "/workers/feature-flag/vars/NEW_CHECKOUT", json!({ "value": "on" })).await;
    edge.serving("feature-flag", &on["seq"]).await;
    assert_eq!(edge.call("/w/feature-flag/").await.json_value().await["checkout"], "new");

    // The counter is made with the binding its source needs, and each name counts by itself.
    assert_eq!(edge.get("/workers/counter/versions/1").await["objects"], json!({ "COUNTER": "Counter" }));
    let count = async |method: Method, name: &str| {
        let said = edge.client.request(method, format!("{}/w/counter/{name}", edge.http)).send().await.unwrap();
        (said.status().as_u16(), said.json_value().await["count"].clone())
    };
    assert_eq!(count(Method::POST, "a").await, (200, json!(1)));
    assert_eq!(count(Method::POST, "a").await, (200, json!(2)));
    assert_eq!(count(Method::POST, "b").await, (200, json!(1)));
    assert_eq!(count(Method::GET, "a").await, (200, json!(2)));
    assert_eq!(count(Method::DELETE, "a").await, (200, json!(0)));
    assert_eq!(count(Method::POST, "a").await, (200, json!(1)));
    edge.stop().await;
}

struct Ran {
    ok: bool,
    out: String,
    err: String,
}

/// Runs the `edge` binary with its credentials in `config` and nothing of the caller's environment in its way.
const ROOMS: &str = r#"
export class Room {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const n = ((await this.state.storage.get("visits")) ?? 0) + 1;
    await this.state.storage.put("visits", n);
    await this.state.storage.put("last", { path: new URL(request.url).pathname });
    return new Response(String(n));
  }
}
export default { fetch(request, env) {
  const name = new URL(request.url).searchParams.get("room");
  return env.ROOMS.get(env.ROOMS.idFromName(name)).fetch(request);
} };
"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_s_objects_are_listed_read_and_deleted_through_the_api() {
    let edge = Edge::start(2).await;
    let wrong = json!({ "name": "rooms", "source": ROOMS, "objects": { "ROOMS": "Hall" } });
    let (status, refused) = edge.api(Method::POST, "/workers", wrong).await;
    assert!(status == 400 || status == 422, "{status} {refused}");
    assert!(refused["message"].as_str().unwrap().contains("the module exports no class Hall"), "{refused}");

    let made = edge
        .change(Method::POST, "/workers", json!({ "name": "rooms", "source": ROOMS, "objects": { "ROOMS": "Room" } }))
        .await;
    edge.serving("rooms", &made["seq"]).await;
    assert_eq!(edge.get("/workers/rooms/versions/1").await["objects"], json!({ "ROOMS": "Room" }));
    assert_eq!(edge.get("/workers/rooms/objects").await["objects"], json!([]));
    for (room, said) in [("a%2Fb", "1"), ("a%2Fb", "2"), ("c", "1")] {
        assert_eq!(edge.call(&format!("/w/rooms/in?room={room}")).await.text().await.unwrap(), said);
    }

    // Where each runs, and that it stores something. An id is one path segment, whatever is in it.
    let listed = edge.get("/workers/rooms/objects").await;
    let objects = listed["objects"].as_array().unwrap();
    let ids: Vec<_> = objects.iter().map(|o| (o["class"].as_str().unwrap(), o["id"].as_str().unwrap())).collect();
    assert_eq!(ids, [("Room", "a/b"), ("Room", "c")], "{listed}");
    for object in objects {
        assert!(object["stored"] == true && object["node"].as_str().unwrap().starts_with('r'), "{object}");
    }
    assert_eq!(listed["silent"], json!([]));
    let stored = edge.get("/workers/rooms/objects/Room/a%2Fb").await;
    let entries = json!([{ "key": "last", "value": { "path": "/in" } }, { "key": "visits", "value": 2 }]);
    assert_eq!((&stored["entries"], &stored["id"]), (&entries, &json!("a/b")), "{stored}");
    assert!(stored["bytes"].as_u64().unwrap() > 0);

    // Deleted while its actor runs: that actor holds a copy of what is gone, so the store refuses the one request
    // that finds it, and the next starts from nothing. The other object keeps its own.
    edge.change(Method::DELETE, "/workers/rooms/objects/Room/a%2Fb", Value::Null).await;
    assert_eq!(edge.get("/workers/rooms/objects/Room/a%2Fb").await["entries"], json!([]));
    let stale = edge.call("/w/rooms/in?room=a%2Fb").await;
    assert_eq!(stale.status(), 500);
    let why = stale.text().await.unwrap();
    assert!(why.contains("storage is at version 0, and this actor read version 2"), "{why}");
    assert_eq!(edge.call("/w/rooms/in?room=a%2Fb").await.text().await.unwrap(), "1");
    assert_eq!(edge.call("/w/rooms/in?room=c").await.text().await.unwrap(), "2");
    let (status, none) = edge.api(Method::GET, "/workers/absent/objects", Value::Null).await;
    assert_eq!((status, none["error"].as_str()), (404, Some("not_found")), "{none}");
    edge.stop().await;
}

fn edge_command(config: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_edge"));
    command.args(args).env("EDGE_CONFIG", config);
    for name in ["EDGE_URL", "EDGE_TOKEN", "EDGE_PASSWORD"] {
        command.env_remove(name);
    }
    command
}

async fn cli(config: &Path, args: &[&str], stdin: Option<&str>) -> Ran {
    let mut command = edge_command(config, args);
    let stdin = stdin.map(String::from);
    let run = move || {
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        // Closed after the line, or with nothing: no command waits for a terminal.
        let mut pipe = child.stdin.take().unwrap();
        stdin.iter().for_each(|text| pipe.write_all(text.as_bytes()).unwrap());
        drop(pipe);
        child.wait_with_output().unwrap()
    };
    let output = tokio::task::spawn_blocking(run).await.unwrap();
    let text = |bytes: Vec<u8>| String::from_utf8(bytes).unwrap();
    Ran { ok: output.status.success(), out: text(output.stdout), err: text(output.stderr) }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_command_line_signs_in_deploys_invokes_tails_rolls_back_and_loads() {
    let edge = Edge::start(1).await;
    let config = edge.dir.path().join("cli/credentials.json");
    let file = edge.dir.path().join("greet.js");
    let (config, file) = (config.as_path(), file.to_str().unwrap());
    let run = async |args: &[&str]| {
        let ran = cli(config, args, None).await;
        assert!(ran.ok, "edge {args:?}: {}", ran.err);
        ran
    };
    let fails = async |args: &[&str], why: &str| {
        let ran = cli(config, args, None).await;
        assert!(!ran.ok && ran.err.contains(why), "edge {args:?} fails with {why:?}: {:?} {:?}", ran.out, ran.err);
        assert!(ran.err.starts_with("edge: "), "{:?}", ran.err);
    };

    fails(&["list"], "no control node").await;
    fails(&["list", "--url", &edge.api], "not signed in").await;
    fails(&["login", "--url", &edge.api, "--password", "guess"], "wrong password").await;
    fails(&["login", "--url", "http://127.0.0.1:1", "--password", PASSWORD], "cannot reach").await;
    let signed_in = cli(config, &["login", "--url", &edge.api], Some(&format!("{PASSWORD}\n"))).await;
    assert!(signed_in.ok && signed_in.out.starts_with(&format!("signed in to {}", edge.api)), "{}", signed_in.err);
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(config).unwrap().permissions().mode() & 0o777,
            0o600,
            "the token is the user's alone"
        );
    }

    // From a template to a deployed worker.
    run(&["init", "hello", file]).await;
    fails(&["init", "hello", file], "exists").await;
    let deployed = run(&["deploy", file, "--message", "first", "--var", "GREETING=hi"]).await;
    assert_eq!(deployed.out, format!("deployed greet version 1 (seq 1)\n  {}/w/greet/\n", edge.http));
    edge.serving("greet", &json!(1)).await;
    let mut invoked = cli(config, &["invoke", "greet", "/?name=cli"], None).await;
    eventually("a runner answers the control node", async || {
        invoked = cli(config, &["invoke", "greet", "/?name=cli"], None).await;
        invoked.ok
    })
    .await;
    assert_eq!(invoked.out, "hi, cli\n");
    assert!(
        invoked.err.starts_with("HTTP 200 (cpu ") && invoked.err.contains("node r1, version 1, trace "),
        "{}",
        invoked.err
    );

    // A source that does not load is refused with the engine's reason, and nothing changes.
    let broken = edge.dir.path().join("broken.js");
    std::fs::write(&broken, "export default { fetch(request) { return new Response('x' }").unwrap();
    fails(&["deploy", broken.to_str().unwrap(), "--name", "greet"], "did not load").await;
    fails(&["invoke", "nope"], "no such worker").await;
    fails(&["rollback", "greet"], "no version older").await;
    fails(&["frobnicate"], "no command").await;

    // A second version, the list of them, and back.
    std::fs::write(file, std::fs::read_to_string(file).unwrap().replace("${name}\\n", "${name}!\\n")).unwrap();
    let louder = run(&["deploy", file, "--message", "louder"]).await.out;
    assert_eq!(louder, format!("deployed greet version 2 (seq 2)\n  {}/w/greet/\n", edge.http));
    edge.serving("greet", &json!(2)).await;
    assert_eq!(run(&["invoke", "greet", "/?name=cli"]).await.out, "hi, cli!\n", "the variables are kept");
    let versions = run(&["versions", "greet"]).await.out;
    let lines: Vec<&str> = versions.lines().collect();
    assert_eq!(lines[0], "greet is v2");
    assert_eq!(lines[1], format!("  {}/w/greet/", edge.http));
    assert!(lines[2].starts_with("  v2 ") && lines[2].ends_with("louder"), "{versions}");
    assert!(lines[3].starts_with("  v1 ") && lines[3].ends_with("first"), "{versions}");
    assert_eq!(run(&["rollback", "greet"]).await.out, "rolled greet back from version 2 to version 1 (seq 3)\n");
    edge.serving("greet", &json!(3)).await;
    assert_eq!(run(&["invoke", "greet", "/?name=cli"]).await.out, "hi, cli\n");

    // A secret from stdin, never shown again; a variable beside it.
    let put = cli(config, &["secret", "put", "greet", "API_KEY"], Some("hunter2-hunter2\n")).await;
    assert!(put.ok && put.out.contains("version 3 deployed"), "{} {}", put.out, put.err);
    let set = run(&["var", "set", "greet", "GREETING", "hey"]).await;
    assert!(set.out.contains("version 4 deployed (seq 5)"), "{}", set.out);
    edge.serving("greet", &json!(5)).await;
    assert_eq!(run(&["var", "list", "greet"]).await.out, "GREETING = hey\nAPI_KEY (secret)\n");
    assert!(!run(&["versions", "greet", "--json"]).await.out.contains("hunter2"));

    // A tail hears the next call, with its console line.
    let mut tail = edge_command(config, &["tail", "greet", "--count", "1"]);
    let mut tail = tail.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut stderr = BufReader::new(tail.stderr.take().unwrap());
    let listening = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        stderr.read_line(&mut line).unwrap();
        line
    });
    assert!(listening.await.unwrap().starts_with("tailing greet"));
    assert_eq!(run(&["invoke", "greet", "/tailed?name=tail", "-X", "PUT", "-d", "body"]).await.out, "hey, tail\n");
    let heard = tokio::task::spawn_blocking(move || tail.wait_with_output().unwrap()).await.unwrap();
    let heard = String::from_utf8(heard.stdout).unwrap();
    assert!(heard.contains(" 200 ok ") && heard.contains("PUT /tailed?name=tail  greet v4 on r1"), "{heard}");
    assert!(heard.lines().nth(1).is_some_and(|line| line == "    Log: greeting tail"), "{heard}");

    // Unsaved code, a steady load through the gateway, and what the cluster looks like after.
    let preview = run(&["preview", file, "/?name=draft"]).await;
    assert_eq!(preview.out, "hey, draft!\n", "the file as it is, not the deployed source, with the worker's variables");
    let load = async |seconds: &str| {
        let load =
            ["load", "--gateway", &edge.http, "--mix", "greet=1", "--rps", "50", "--duration", seconds, "--json"];
        serde_json::from_str::<Value>(&run(&load).await.out).unwrap()
    };
    // A gateway new to an account refuses it until the quota nodes have granted it a share.
    eventually("the gateway has a share of the account's rate", async || {
        load("0.2").await["statuses"] == json!({ "200": 10 })
    })
    .await;
    let load = load("1").await;
    assert_eq!(
        (&load["sent"], &load["statuses"], &load["skipped"]),
        (&json!(50), &json!({ "200": 50 }), &json!(0)),
        "{load}"
    );
    assert!(load["p50_us"].as_u64().unwrap() > 0 && load["p99_us"].as_u64() >= load["p50_us"].as_u64(), "{load}");
    fails(
        &["load", "--gateway", "http://127.0.0.1:1", "--mix", "greet", "--rps", "20", "--duration", "0.2"],
        "no request was answered",
    )
    .await;
    let metrics = run(&["metrics", "greet", "--json"]).await.out;
    let metrics: Value = serde_json::from_str(&metrics).unwrap();
    assert!(metrics["total"]["requests"].as_u64().unwrap() >= 64, "{metrics}");
    assert!(run(&["cluster"]).await.out.contains("    greet: v4, seq 5"));
    let listed = run(&["list"]).await.out;
    let row = |line: &str| {
        line.starts_with("greet ") && line.contains(" v4 ") && line.ends_with(&format!("  {}/w/greet/", edge.http))
    };
    assert!(listed.lines().nth(1).is_some_and(row), "{listed}");
    assert!(run(&["cluster"]).await.out.contains(&format!("gateway          up  {}\n", edge.http)));

    // A template with a Durable Object says how to bind it; then the object, where it runs and what it stores.
    let counter = edge.dir.path().join("counter.js");
    let counter = counter.to_str().unwrap();
    let wrote = run(&["init", "counter", counter]).await.out;
    assert!(wrote.ends_with(&format!("`edge deploy {counter} --object COUNTER=Counter`\n")), "{wrote}");
    let deployed = run(&["deploy", counter, "--object", "COUNTER=Counter"]).await.out;
    assert_eq!(deployed, format!("deployed counter version 1 (seq 6)\n  {}/w/counter/\n", edge.http));
    edge.serving("counter", &json!(6)).await;
    assert_eq!(run(&["invoke", "counter", "/a", "-X", "POST"]).await.out, "{\"name\":\"a\",\"count\":1}\n");
    let listed = run(&["objects", "counter"]).await.out;
    assert!(listed.starts_with("Counter a  on r1, epoch ") && listed.lines().count() == 1, "{listed}");
    let stored = run(&["object", "get", "counter", "Counter", "a"]).await.out;
    let lines: Vec<&str> = stored.lines().collect();
    assert!(lines[0].starts_with("Counter a of counter: 6 bytes, last written under epoch "), "{stored}");
    assert_eq!(lines[1..], ["count = 1"], "{stored}");
    let deleted = run(&["object", "delete", "counter", "Counter", "a"]).await.out;
    assert_eq!(deleted, "deleted what Counter a of counter stored\n");
    assert_eq!(run(&["object", "get", "counter", "Counter", "a"]).await.out.lines().count(), 1);
    fails(&["object", "put", "counter", "Counter", "a"], "get or delete").await;
    assert_eq!(run(&["delete", "counter"]).await.out, "deleted counter\n");

    assert_eq!(run(&["delete", "greet"]).await.out, "deleted greet\n");
    assert_eq!(serde_json::from_str::<Value>(&run(&["list", "--json"]).await.out).unwrap()["workers"], json!([]));
    edge.stop().await;
}
