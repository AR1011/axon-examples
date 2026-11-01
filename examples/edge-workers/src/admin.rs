//! The operator's HTTP API, on the control node: workers, versions, deployments, variables and secrets, metrics,
//! invocations, a live tail, a preview of unsaved code, and the cluster. Everything under `/api` but the login
//! needs a session: the cookie a browser got from `POST /api/login`, or the token the command line got from it.
//!
//! The registry stays the one writer: each change here is one call to it. What the runners did comes from the
//! [`Observer`], and a request for a deployed worker goes through a gateway of this node's own, under the same
//! quotas as any caller's.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axon::{ActorHandle, ActorSystem, Histogram};
use axon_cluster::{Cluster, NamedRef, View};
use axon_stream::StreamNode;
use axum::extract::{DefaultBodyLimit, Path, Query, Request as HttpRequest, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use include_dir::{Dir, include_dir};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Semaphore, broadcast};

use crate::auth::Auth;
use crate::engine::{Env, Limits, Objects, Request};
use crate::gateway::{self, Gateway};
use crate::node::{self, Client, on_control};
use crate::observer::{self, Filter, Observer, ObserverConfig};
use crate::templates::{TEMPLATES, Template, template};
use crate::wire::{
    Answer, Call, LogRecord, MAX_SOURCE, OUTCOMES, PreviewAsk, Refused, STORE, Served, StoreApi, StoreAsk, StoreTold,
    Upload, VersionDetail, VersionInfo, WorkerInfo, WorkerStats, object_key, split_object_key,
};

#[derive(Clone, Debug)]
pub struct AdminConfig {
    /// The admin password as an argon2id PHC string, from `auth::hash_password`.
    pub password_hash: String,
    /// Signs the sessions, 32 bytes or more. Without one every session ends with the process.
    pub key: Option<Vec<u8>>,
    /// How long a browser's session and a command line's token hold.
    pub session: Duration,
    pub token: Duration,
    pub observer: ObserverConfig,
    /// Where a browser finds the console: `/`, or the path a proxy in front puts it at, with the `/` at both ends
    /// (`/ops/edge/`). The API stays at `/api/`.
    pub base_path: String,
}

impl AdminConfig {
    pub fn new(password_hash: impl Into<String>) -> Self {
        Self {
            password_hash: password_hash.into(),
            key: None,
            session: Duration::from_secs(12 * 3600),
            token: Duration::from_secs(30 * 86_400),
            observer: ObserverConfig::default(),
            base_path: "/".into(),
        }
    }
}

/// The console, as `npm run build` in `ui/` left it (`build.rs` refuses a build without it).
static CONSOLE: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");
/// The console's page loads scripts, styles and fonts from this origin only, and no other site may frame it.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
                   font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; \
                   object-src 'none'";
const COOKIE: &str = "edge_session";
/// The account the admin's own requests for a worker are charged to.
const ACCOUNT: &str = "admin";

struct Inner {
    cfg: AdminConfig,
    cluster: Cluster,
    registry: Client,
    /// What the objects store, on this node.
    store: NamedRef<StoreApi>,
    gateway: Gateway,
    observer: ActorHandle<observer::Msg>,
    tail: broadcast::Sender<Arc<LogRecord>>,
    auth: Arc<Auth>,
    /// One password check at a time: each is tens of milliseconds of CPU, and a guesser gets no more of it.
    logins: Semaphore,
}

#[derive(Clone)]
pub struct Admin(Arc<Inner>);

/// A refusal as the API says it: a status, a code to act on, and a sentence to read.
struct Refusal(StatusCode, &'static str, String);

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1, "message": self.2 }))).into_response()
    }
}

impl From<node::Error> for Refusal {
    fn from(error: node::Error) -> Self {
        let text = error.to_string();
        match error {
            node::Error::Refused(Refused::NotFound | Refused::NoSuchVersion(_)) => {
                Refusal(StatusCode::NOT_FOUND, "not_found", text)
            }
            node::Error::Refused(Refused::Invalid(_)) => Refusal(StatusCode::BAD_REQUEST, "invalid", text),
            node::Error::Refused(Refused::Load(_)) => Refusal(StatusCode::UNPROCESSABLE_ENTITY, "load", text),
            node::Error::Refused(Refused::Busy) => Refusal(StatusCode::SERVICE_UNAVAILABLE, "busy", text),
            node::Error::Refused(Refused::Storage(_)) => Refusal(StatusCode::INTERNAL_SERVER_ERROR, "storage", text),
            node::Error::Cluster(_) => Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable", text),
            node::Error::Other(_) => Refusal(StatusCode::INTERNAL_SERVER_ERROR, "internal", text),
        }
    }
}

fn invalid(why: impl Into<String>) -> Refusal {
    Refusal(StatusCode::BAD_REQUEST, "invalid", why.into())
}

type Reply = Result<Response, Refusal>;

fn ok(body: Value) -> Reply {
    Ok(Json(body).into_response())
}

fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

impl Admin {
    /// Starts the observer. `gateway` is this node's way to the runners.
    pub fn start(
        system: &ActorSystem,
        cluster: &Cluster,
        streams: StreamNode,
        gateway: Gateway,
        cfg: AdminConfig,
    ) -> Result<Self, String> {
        if !cfg.base_path.starts_with('/') || !cfg.base_path.ends_with('/') || cfg.base_path.contains(['"', '<', '>']) {
            return Err(format!("the console's base path starts and ends with /: {:?}", cfg.base_path));
        }
        let auth = Arc::new(Auth::new(&cfg.password_hash, cfg.key.as_deref())?);
        let (observer, tail) = Observer::new(cluster, streams, cfg.observer.clone());
        let observer = system.spawn(observer).name("observer").start().map_err(|e| format!("{e:?}"))?;
        let (cluster, registry, logins) = (cluster.clone(), Client::new(cluster), Semaphore::new(1));
        let store = on_control(&cluster, STORE);
        Ok(Self(Arc::new(Inner { cfg, cluster, registry, store, gateway, observer, tail, auth, logins })))
    }

    /// Of the observer.
    pub fn lifecycle(&self) -> axon::LifecycleHandle {
        self.0.observer.lifecycle.clone()
    }

    /// The API under `/api`, and the console on every other path: its files, and its page for the rest. The page
    /// and its files hold nothing of the cluster, so they need no session; all they show comes from the API.
    pub fn router(&self) -> Router {
        let api = Router::new()
            .route("/session", get(session))
            .route("/logout", post(logout))
            .route("/templates", get(templates))
            .route("/workers", get(workers).post(create))
            .route("/workers/{name}", get(worker).delete(delete))
            .route("/workers/{name}/versions", get(worker).post(upload))
            .route("/workers/{name}/versions/{id}", get(version))
            .route("/workers/{name}/deploy", post(deploy))
            .route("/workers/{name}/rollback", post(rollback))
            .route("/workers/{name}/preview", post(preview))
            .route("/workers/{name}/invoke", post(invoke))
            .route("/workers/{name}/vars", get(vars))
            .route("/workers/{name}/vars/{var}", put(set_var).delete(delete_var))
            .route("/workers/{name}/metrics", get(metrics))
            .route("/workers/{name}/objects", get(objects))
            .route("/workers/{name}/objects/{class}/{id}", get(object).delete(clear_object))
            .route("/workers/{name}/invocations", get(invocations_of))
            .route("/workers/{name}/tail", get(tail_of))
            .route("/invocations", get(invocations))
            .route("/invocations/{trace}", get(invocation))
            .route("/tail", get(tail))
            .route("/cluster", get(cluster))
            .route_layer(axum::middleware::from_fn_with_state(self.clone(), guard))
            .route("/login", post(login))
            // A source in JSON is longer than the source.
            .layer(DefaultBodyLimit::max(2 * MAX_SOURCE + (64 << 10)))
            .with_state(self.clone());
        Router::new().nest("/api", api).fallback(console).with_state(self.clone())
    }

    /// The session a request carries, as a bearer token or as the cookie: when it ends.
    fn signed_in(&self, headers: &HeaderMap) -> Option<u64> {
        let text = |name| headers.get(name).and_then(|value: &HeaderValue| value.to_str().ok());
        let bearer = text(header::AUTHORIZATION).and_then(|value| value.strip_prefix("Bearer "));
        let cookie =
            || text(header::COOKIE)?.split(';').find_map(|pair| pair.trim().strip_prefix(COOKIE)?.strip_prefix('='));
        self.0.auth.check(bearer.or_else(cookie)?, now_ms())
    }

    /// The version a change of variables builds on, as the registry picks it.
    async fn base(&self, name: &str) -> Result<VersionDetail, Refusal> {
        let (info, _) = self.0.registry.get(name).await?;
        let id = info.deployment.as_ref().map_or(info.latest, |deployment| deployment.stable);
        Ok(self.0.registry.version(name, id).await?)
    }

    async fn ask<R: Send + 'static>(
        &self,
        build: impl FnOnce(axon::ReplyTo<R>) -> observer::Msg,
    ) -> Result<R, Refusal> {
        let unavailable = |e| Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable", format!("the observer: {e:?}"));
        self.0.observer.ask(build).await.map_err(unavailable)
    }
}

async fn guard(State(admin): State<Admin>, request: HttpRequest, next: Next) -> Response {
    match admin.signed_in(request.headers()) {
        Some(_) => next.run(request).await,
        None => {
            Refusal(StatusCode::UNAUTHORIZED, "unauthorized", "sign in first: POST /api/login".into()).into_response()
        }
    }
}

/// A file of the console, or its page: the browser's router reads the path. A path under `/api` that is no route
/// is the API's 404, and so is a file the console does not have, so a mistyped asset is not answered with a page.
async fn console(State(admin): State<Admin>, request: HttpRequest) -> Response {
    let base = admin.0.cfg.base_path.as_str();
    let whole = request.uri().path();
    // A proxy that mounts the console at the base path may pass the path whole or without the base.
    let path = whole.strip_prefix(base).unwrap_or(whole).trim_start_matches('/');
    let missing = || Refusal(StatusCode::NOT_FOUND, "not_found", format!("nothing at {whole}")).into_response();
    if whole == "/api"
        || whole.starts_with("/api/")
        || !matches!(*request.method(), http::Method::GET | http::Method::HEAD)
    {
        return missing();
    }
    let secured = |mut response: Response, cache: &'static str| {
        let headers = response.headers_mut();
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
        headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
        response
    };
    if let Some(file) = CONSOLE.get_file(path).filter(|_| path != "index.html") {
        let mime = match path.rsplit('.').next() {
            Some("js") => "text/javascript",
            Some("css") => "text/css",
            Some("svg") => "image/svg+xml",
            Some("woff2") => "font/woff2",
            Some("json") => "application/json",
            Some("png") => "image/png",
            _ => "application/octet-stream",
        };
        // A built file's name holds the hash of what is in it.
        let cache = if path.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "no-store" };
        return secured(([(header::CONTENT_TYPE, mime)], file.contents()).into_response(), cache);
    }
    let page = path == "index.html" || path.rsplit('/').next().is_some_and(|last| !last.contains('.'));
    match CONSOLE.get_file("index.html").and_then(|file| file.contents_utf8()).filter(|_| page) {
        Some(index) => {
            let based = index.replacen("<base href=\"/\" />", &format!("<base href=\"{base}\" />"), 1);
            secured(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], based).into_response(), "no-store")
        }
        None => missing(),
    }
}

#[derive(Deserialize)]
struct Login {
    password: String,
    /// For a command line: the session comes back as a token, and lasts as long as one.
    #[serde(default)]
    token: bool,
}

async fn login(State(admin): State<Admin>, Json(login): Json<Login>) -> Reply {
    let inner = &admin.0;
    let Ok(_slot) = inner.logins.try_acquire() else {
        return Err(Refusal(StatusCode::TOO_MANY_REQUESTS, "busy", "another sign-in is being checked".into()));
    };
    let (auth, password) = (inner.auth.clone(), login.password);
    if !tokio::task::spawn_blocking(move || auth.verify(&password)).await.unwrap_or(false) {
        return Err(Refusal(StatusCode::UNAUTHORIZED, "unauthorized", "wrong password".into()));
    }
    let lasts = if login.token { inner.cfg.token } else { inner.cfg.session };
    let expires_ms = now_ms() + lasts.as_millis() as u64;
    let sealed = inner.auth.seal(expires_ms);
    if login.token {
        return ok(json!({ "token": sealed, "expires_ms": expires_ms }));
    }
    // No `Secure`: the listener is plain HTTP, and what puts TLS in front of it adds that.
    let cookie = format!("{COOKIE}={sealed}; Path=/; Max-Age={}; HttpOnly; SameSite=Strict", lasts.as_secs());
    Ok(([(header::SET_COOKIE, cookie)], Json(json!({ "expires_ms": expires_ms }))).into_response())
}

async fn session(State(admin): State<Admin>, headers: HeaderMap) -> Reply {
    ok(json!({ "expires_ms": admin.signed_in(&headers) }))
}

/// Clears the cookie in this browser. The value itself holds until it expires: nothing here keeps a list of them.
async fn logout() -> Response {
    let cookie = format!("{COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict");
    ([(header::SET_COOKIE, cookie)], Json(json!({}))).into_response()
}

async fn templates() -> Reply {
    let all: Vec<Value> = TEMPLATES
        .iter()
        .map(|t| json!({ "name": t.name, "description": t.description, "source": t.source, "objects": bindings(t) }))
        .collect();
    ok(json!({ "templates": all }))
}

fn limits_json(limits: &Limits) -> Value {
    json!({
        "cpu_ms": limits.cpu.as_millis() as u64,
        "wall_ms": limits.wall.as_millis() as u64,
        "memory_mb": limits.memory >> 20,
        "subrequests": limits.subrequests,
    })
}

fn version_json(version: &VersionInfo) -> Value {
    json!({
        "id": version.id,
        "created_ms": version.created_ms,
        "message": version.message,
        "bundle": version.bundle.as_str(),
        "source_bytes": version.source_bytes,
        "build_bytes": version.size,
        "limits": limits_json(&version.limits),
    })
}

/// A canary as every answer says it: `{version, percent}`, or null.
pub fn canary_json(canary: Option<(u32, u8)>) -> Value {
    canary.map_or(Value::Null, |(version, percent)| json!({ "version": version, "percent": percent }))
}

/// `urls`: where each gateway of `view` serves the worker, `[{gateway, url}]`.
fn worker_json(info: &WorkerInfo, view: &View) -> Value {
    let deployment =
        info.deployment.as_ref().map(|d| json!({ "seq": d.seq, "stable": d.stable, "canary": canary_json(d.canary) }));
    let urls: Vec<Value> = gateway::urls(view)
        .into_iter()
        .map(|(node, base)| json!({ "gateway": node, "url": gateway::worker_url(base, &info.name) }))
        .collect();
    json!({ "name": info.name, "latest": info.latest, "deployment": deployment, "urls": urls })
}

/// A worker's counters on one runner, or added up over several.
#[derive(Default)]
pub struct Totals {
    outcomes: [u64; OUTCOMES.len()],
    busy: u64,
    cold_starts: u64,
    in_flight: u64,
    cpu: Histogram,
    wall: Histogram,
}

impl Totals {
    pub fn of<'a>(stats: impl IntoIterator<Item = &'a WorkerStats>) -> Self {
        let mut totals = Self::default();
        stats.into_iter().for_each(|stats| totals.add(stats));
        totals
    }

    pub fn add(&mut self, stats: &WorkerStats) {
        (0..OUTCOMES.len()).for_each(|i| self.outcomes[i] += stats.outcomes[i]);
        (self.busy, self.cold_starts) = (self.busy + stats.busy, self.cold_starts + stats.cold_starts);
        self.in_flight += stats.in_flight;
        self.cpu.merge(&Histogram::from(&stats.cpu));
        self.wall.merge(&Histogram::from(&stats.wall));
    }

    pub fn json(&self) -> Value {
        let requests: u64 = self.outcomes.iter().sum();
        let ((cpu_p50, cpu_p99), (wall_p50, wall_p99)) =
            (observer::p50_p99_us(&self.cpu), observer::p50_p99_us(&self.wall));
        let outcomes: BTreeMap<&str, u64> = OUTCOMES.iter().copied().zip(self.outcomes).collect();
        json!({
            "requests": requests,
            // Runs with no response of the worker's own. A 500 the worker returned is its answer, not one of these.
            "errors": requests - self.outcomes[0],
            "outcomes": outcomes,
            "busy": self.busy,
            "cold_starts": self.cold_starts,
            "in_flight": self.in_flight,
            "cpu_p50_us": cpu_p50,
            "cpu_p99_us": cpu_p99,
            "wall_p50_us": wall_p50,
            "wall_p99_us": wall_p99,
        })
    }
}

async fn workers(State(admin): State<Admin>) -> Reply {
    let listed = admin.0.registry.list().await?;
    let (runners, silent) = node::stats(&admin.0.cluster).await;
    let view = admin.0.cluster.view();
    let listed: Vec<Value> = listed
        .iter()
        .map(|info| {
            let totals = Totals::of(runners.iter().filter_map(|runner| runner.workers.get(&info.name)));
            let mut worker = worker_json(info, &view);
            worker["metrics"] = totals.json();
            worker
        })
        .collect();
    ok(json!({ "workers": listed, "silent": silent }))
}

#[derive(Deserialize)]
struct Create {
    name: String,
    template: Option<String>,
    source: Option<String>,
    #[serde(default)]
    message: String,
    vars: Option<Env>,
    /// Kept apart from the variables: no call returns their values.
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    /// Object bindings: the name in `env`, and the class the source exports.
    objects: Option<Objects>,
    /// Deployed at once at 100 % unless this is `false`.
    deploy: Option<bool>,
}

/// A template's object bindings, as an upload takes them.
fn bindings(template: &Template) -> Objects {
    template.objects.iter().map(|(name, class)| (name.to_string(), class.to_string())).collect()
}

async fn create(State(admin): State<Admin>, Json(create): Json<Create>) -> Reply {
    // A worker made from a template is bound to the classes the template exports, unless it says otherwise.
    let (source, objects) = match (create.source, create.template.as_deref()) {
        (Some(source), _) => (source, create.objects),
        (None, name) => {
            let name = name.unwrap_or("hello");
            let template = template(name).ok_or_else(|| invalid(format!("no template {name:?}")))?;
            let bound = (!template.objects.is_empty()).then(|| bindings(template));
            (template.source.to_owned(), create.objects.or(bound))
        }
    };
    match admin.0.registry.get(&create.name).await {
        Ok(_) => {
            let why =
                format!("{} exists: a new version of it is POST /api/workers/{}/versions", create.name, create.name);
            return Err(Refusal(StatusCode::CONFLICT, "exists", why));
        }
        Err(node::Error::Refused(Refused::NotFound)) => {}
        Err(other) => return Err(other.into()),
    }
    let name = create.name;
    let upload = Upload {
        name: name.clone(),
        source: Some(source),
        vars: create.vars,
        secrets: set_secrets(create.secrets),
        objects,
        message: create.message,
        deploy: create.deploy.unwrap_or(true),
        ..Upload::default()
    };
    let (version, seq) = admin.0.registry.upload(upload).await?;
    Ok((StatusCode::CREATED, Json(json!({ "name": name, "version": version, "seq": seq }))).into_response())
}

/// The worker with its versions, newest first.
async fn worker(State(admin): State<Admin>, Path(name): Path<String>) -> Reply {
    let (info, versions) = admin.0.registry.get(&name).await?;
    let mut worker = worker_json(&info, &admin.0.cluster.view());
    worker["versions"] = versions.iter().map(version_json).collect();
    ok(worker)
}

async fn delete(State(admin): State<Admin>, Path(name): Path<String>) -> Reply {
    admin.0.registry.delete(&name).await?;
    ok(json!({ "deleted": name }))
}

async fn version(State(admin): State<Admin>, Path((name, id)): Path<(String, u32)>) -> Reply {
    let detail = admin.0.registry.version(&name, id).await?;
    let mut version = version_json(&detail.info);
    version["source"] = json!(detail.source);
    version["vars"] = json!(detail.vars);
    // Their names. No call returns a secret's value.
    version["secrets"] = json!(detail.secrets);
    version["objects"] = json!(detail.objects);
    ok(version)
}

#[derive(Deserialize)]
struct NewVersion {
    /// Without one, the source of the version that takes the traffic.
    source: Option<String>,
    #[serde(default)]
    message: String,
    /// Replaces the variables whole; without it they are kept.
    vars: Option<Env>,
    /// Set on top of the secrets it has.
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    /// Replaces the object bindings whole; without it they are kept.
    objects: Option<Objects>,
    /// Deployed at once at 100 % unless this is `false`.
    deploy: Option<bool>,
    cpu_ms: Option<u64>,
    memory_mb: Option<usize>,
}

async fn upload(State(admin): State<Admin>, Path(name): Path<String>, Json(new): Json<NewVersion>) -> Reply {
    let mut limits = None;
    if new.cpu_ms.is_some() || new.memory_mb.is_some() {
        let mut base = match admin.base(&name).await {
            Ok(base) => base.info.limits,
            // A new worker: the defaults.
            Err(refusal) if refusal.0 == StatusCode::NOT_FOUND => Limits::default(),
            Err(other) => return Err(other),
        };
        base.cpu = new.cpu_ms.map_or(base.cpu, Duration::from_millis);
        base.memory = new.memory_mb.map_or(base.memory, |mb| mb.saturating_mul(1 << 20));
        limits = Some(base);
    }
    let upload = Upload {
        name,
        source: new.source,
        vars: new.vars,
        secrets: set_secrets(new.secrets),
        objects: new.objects,
        limits,
        message: new.message,
        deploy: new.deploy.unwrap_or(true),
        ..Upload::default()
    };
    let (version, seq) = admin.0.registry.upload(upload).await?;
    Ok((StatusCode::CREATED, Json(json!({ "version": version, "seq": seq }))).into_response())
}

#[derive(Deserialize)]
struct CanaryBody {
    version: u32,
    percent: u8,
}

#[derive(Deserialize)]
struct Deploy {
    version: u32,
    canary: Option<CanaryBody>,
}

async fn deploy(State(admin): State<Admin>, Path(name): Path<String>, Json(deploy): Json<Deploy>) -> Reply {
    let canary = deploy.canary.map(|canary| (canary.version, canary.percent));
    let seq = admin.0.registry.deploy(&name, deploy.version, canary).await?;
    ok(json!({ "seq": seq, "version": deploy.version }))
}

#[derive(Default, Deserialize)]
struct Rollback {
    version: Option<u32>,
}

/// Back to `version`, or to the newest version older than the one that takes the traffic. A deployment like any
/// other: the next `seq`, and no canary.
async fn rollback(State(admin): State<Admin>, Path(name): Path<String>, body: Option<Json<Rollback>>) -> Reply {
    let (info, versions) = admin.0.registry.get(&name).await?;
    let conflict = |why: &str| Refusal(StatusCode::CONFLICT, "nothing_to_roll_back", format!("{name} {why}"));
    let stable = info.deployment.map(|deployment| deployment.stable).ok_or_else(|| conflict("is not deployed"))?;
    let to = match body.and_then(|Json(rollback)| rollback.version) {
        Some(version) => version,
        None => {
            let older = versions.iter().map(|version| version.id).filter(|id| *id < stable).max();
            older.ok_or_else(|| conflict("has no version older than the one deployed"))?
        }
    };
    let seq = admin.0.registry.deploy(&name, to, None).await?;
    ok(json!({ "seq": seq, "version": to, "from": stable }))
}

/// One request, as a preview and an invocation take it.
#[derive(Default, Deserialize)]
struct Ask {
    method: Option<String>,
    path: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: String,
}

impl Ask {
    /// The method, and the path the worker sees: its host is `localhost`.
    fn target(&self) -> Result<(String, String), Refusal> {
        let path = self.path.clone().unwrap_or_else(|| "/".into());
        if !path.starts_with('/') {
            return Err(invalid("a path starts with /"));
        }
        Ok((self.method.clone().unwrap_or_else(|| "GET".into()).to_uppercase(), path))
    }
}

#[derive(Deserialize)]
struct Preview {
    source: String,
    #[serde(default)]
    env: Env,
    #[serde(default)]
    request: Ask,
}

/// Runs unsaved code once, on this node, with the worker's variables under `env` and none of its secrets: the
/// code is whatever the caller typed, and would hand them back. No version is made and no runner hears of it.
async fn preview(State(admin): State<Admin>, Path(name): Path<String>, Json(preview): Json<Preview>) -> Reply {
    let (method, url) = preview.request.target()?;
    let ask = PreviewAsk {
        name,
        source: preview.source,
        env: preview.env,
        method,
        url,
        headers: preview.request.headers.into_iter().collect(),
        body: preview.request.body,
    };
    match admin.0.registry.call(Call::Preview(ask)).await? {
        Answer::Previewed(ran) => {
            let mut out = serde_json::to_value(&ran).map_err(|e| invalid(e.to_string()))?;
            out["node"] = json!(admin.0.cluster.identity().node_id);
            ok(out)
        }
        other => Err(node::unexpected(other).into()),
    }
}

/// One request for the deployed worker, through the runners, as a gateway sends it.
async fn invoke(State(admin): State<Admin>, Path(name): Path<String>, ask: Option<Json<Ask>>) -> Reply {
    let Json(ask) = ask.unwrap_or_default();
    let (method, url) = ask.target()?;
    // A name nobody deployed is told from one no runner serves yet.
    admin.0.registry.get(&name).await?;
    let request = Request { method, url, headers: ask.headers.into_iter().collect(), body: ask.body.into() };
    let call = gateway::Call { account: ACCOUNT.into(), worker: name, request, deadline: None };
    let response = admin.0.gateway.call(call).await;
    ok(json!({
        "status": response.status,
        "headers": response.headers,
        "body": String::from_utf8_lossy(&response.body),
        "node": response.node,
        "version": response.version,
        "trace": response.trace,
        "cpu_us": response.cpu_us,
        "wall_us": response.wall_us,
        "error": response.error,
        "retry_after_ms": response.retry_after.map(|after| after.as_millis() as u64),
    }))
}

async fn vars(State(admin): State<Admin>, Path(name): Path<String>) -> Reply {
    let base = admin.base(&name).await?;
    let plain = base.vars.iter().map(|(name, value)| json!({ "name": name, "type": "plain", "value": value }));
    let secret = base.secrets.iter().map(|name| json!({ "name": name, "type": "secret" }));
    ok(json!({ "version": base.info.id, "vars": plain.chain(secret).collect::<Vec<_>>() }))
}

#[derive(Deserialize)]
struct SetVar {
    /// `plain` or `secret`.
    #[serde(rename = "type", default)]
    kind: Option<String>,
    value: String,
}

/// Secrets as an upload sets them: each to its value.
fn set_secrets(given: BTreeMap<String, String>) -> BTreeMap<String, Option<String>> {
    given.into_iter().map(|(name, value)| (name, Some(value))).collect()
}

/// A change of one variable or secret: a new version of the one that takes the traffic, deployed at once. Only the
/// change is sent, and the registry applies it to the version it builds on, so two changes at once both hold.
async fn change_var(admin: &Admin, name: String, var: &str, set: Option<SetVar>) -> Reply {
    let one = |value: Option<String>| BTreeMap::from([(var.to_owned(), value)]);
    let mut upload = Upload { name: name.clone(), deploy: true, ..Upload::default() };
    match set {
        Some(SetVar { kind, value }) => match kind.as_deref().unwrap_or("plain") {
            "plain" => (upload.set, upload.message) = (one(Some(value)), format!("set {var}")),
            "secret" => (upload.secrets, upload.message) = (one(Some(value)), format!("set secret {var}")),
            other => return Err(invalid(format!("a variable is plain or secret, not {other:?}"))),
        },
        None => {
            let base = admin.base(&name).await?;
            if !base.vars.contains_key(var) && !base.secrets.iter().any(|secret| secret == var) {
                return Err(Refusal(StatusCode::NOT_FOUND, "not_found", format!("{name} has no variable {var}")));
            }
            (upload.set, upload.secrets, upload.message) = (one(None), one(None), format!("removed {var}"));
        }
    }
    let (version, seq) = admin.0.registry.upload(upload).await?;
    ok(json!({ "version": version, "seq": seq }))
}

async fn set_var(
    State(admin): State<Admin>,
    Path((name, var)): Path<(String, String)>,
    Json(set): Json<SetVar>,
) -> Reply {
    change_var(&admin, name, &var, Some(set)).await
}

async fn delete_var(State(admin): State<Admin>, Path((name, var)): Path<(String, String)>) -> Reply {
    change_var(&admin, name, &var, None).await
}

/// What every runner counted of the worker since it started, and the worker's points over time.
async fn metrics(State(admin): State<Admin>, Path(name): Path<String>) -> Reply {
    admin.0.registry.get(&name).await?;
    let (runners, silent) = node::stats(&admin.0.cluster).await;
    let mut total = Totals::default();
    let mut by_node = Vec::new();
    for (runner, stats) in runners.iter().filter_map(|runner| Some((runner, runner.workers.get(&name)?))) {
        total.add(stats);
        let mut one = Totals::of([stats]).json();
        one["node"] = json!(runner.node);
        by_node.push(one);
    }
    let asked = name.clone();
    let series = admin.ask(|reply| observer::Msg::Series(asked, reply)).await?;
    let sample_ms = admin.0.cfg.observer.sample.as_millis() as u64;
    ok(
        json!({ "worker": name, "total": total.json(), "by_node": by_node, "silent": silent, "series": series, "sample_ms": sample_ms }),
    )
}

async fn recent(admin: &Admin, filter: Filter) -> Reply {
    let found = admin.ask(|reply| observer::Msg::Recent(filter, reply)).await?;
    ok(json!({ "invocations": found.iter().map(|record| &**record).collect::<Vec<_>>() }))
}

async fn invocations(State(admin): State<Admin>, Query(filter): Query<Filter>) -> Reply {
    recent(&admin, filter).await
}

async fn invocations_of(State(admin): State<Admin>, Path(name): Path<String>, Query(filter): Query<Filter>) -> Reply {
    recent(&admin, Filter { worker: Some(name), ..filter }).await
}

/// An invocation by its trace id, as the gateway's `x-trace-id` names it, while the list still holds it.
async fn invocation(State(admin): State<Admin>, Path(trace): Path<String>) -> Reply {
    let filter = Filter { trace: Some(trace.clone()), limit: Some(1), ..Filter::default() };
    let found = admin.ask(|reply| observer::Msg::Recent(filter, reply)).await?;
    match found.first() {
        Some(record) => ok(json!(&**record)),
        None => Err(Refusal(StatusCode::NOT_FOUND, "not_found", format!("no invocation {trace} is kept"))),
    }
}

/// Server-sent events: an `invocation` for each run that ends from now on, and `missed` with a count when the
/// reader fell behind by more than the tail holds.
fn follow(admin: &Admin, filter: Filter) -> Response {
    let events = futures_util::stream::unfold((admin.0.tail.subscribe(), filter), |(mut tail, filter)| async move {
        loop {
            let event = match tail.recv().await {
                Ok(record) if filter.admits(&record) => {
                    Event::default().event("invocation").data(serde_json::to_string(&*record).unwrap_or_default())
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    Event::default().event("missed").data(missed.to_string())
                }
                // The observer has stopped, with the node.
                Err(broadcast::error::RecvError::Closed) => return None,
            };
            return Some((Ok::<_, Infallible>(event), (tail, filter)));
        }
    });
    Sse::new(events).keep_alive(KeepAlive::default()).into_response()
}

async fn tail(State(admin): State<Admin>, Query(filter): Query<Filter>) -> Response {
    follow(&admin, filter)
}

async fn tail_of(State(admin): State<Admin>, Path(name): Path<String>, Query(filter): Query<Filter>) -> Response {
    follow(&admin, Filter { worker: Some(name), ..filter })
}

impl Admin {
    /// One question to the store, which is on this node.
    async fn store(&self, ask: StoreAsk) -> Result<StoreTold, Refusal> {
        let unavailable = |why: String| Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable", why);
        match self.0.store.request_within(ask, Duration::from_secs(10)).await {
            Ok(StoreTold::Refused(why)) => Err(Refusal(StatusCode::INTERNAL_SERVER_ERROR, "storage", why)),
            Ok(told) => Ok(told),
            Err(e) => Err(unavailable(format!("the objects' store: {e}"))),
        }
    }
}

/// How many objects a runner, and the store, are asked for at a time.
const OBJECTS_PAGE: u32 = 1000;
const ANOTHER_CALL: &str = "the store answered another call";

/// A worker's objects: those that run now, each with its runner and the epoch of its lease, and those that only
/// store something. An object that runs and has stored nothing yet is listed too.
async fn objects(State(admin): State<Admin>, Path(name): Path<String>) -> Reply {
    admin.0.registry.get(&name).await?;
    let (running, silent) = node::objects(&admin.0.cluster, Some(&name), OBJECTS_PAGE).await;
    let mut listed: BTreeMap<String, Value> = BTreeMap::new();
    loop {
        let after = listed.keys().next_back().cloned();
        let listing = StoreAsk::List { prefix: format!("{name}/"), after, limit: OBJECTS_PAGE };
        let StoreTold::Objects(stored) = admin.store(listing).await? else {
            return Err(Refusal(StatusCode::INTERNAL_SERVER_ERROR, "internal", ANOTHER_CALL.into()));
        };
        let last = stored.len() < OBJECTS_PAGE as usize;
        listed.extend(stored.into_iter().map(|key| (key, json!({ "stored": true, "node": null, "epoch": null }))));
        if last {
            break;
        }
    }
    for (node, hosted) in running {
        let stored = listed.contains_key(&hosted.key);
        listed.insert(hosted.key, json!({ "stored": stored, "node": node, "epoch": hosted.epoch }));
    }
    let objects: Vec<Value> = listed
        .into_iter()
        .filter_map(|(key, mut object)| {
            let (_, class, id) = split_object_key(&key)?;
            (object["class"], object["id"]) = (json!(class), json!(id));
            Some(object)
        })
        .collect();
    ok(json!({ "objects": objects, "silent": silent }))
}

/// What an object stores: its keys, each with its value.
async fn object(State(admin): State<Admin>, Path((name, class, id)): Path<(String, String, String)>) -> Reply {
    let StoreTold::Snapshot { epoch, entries, .. } =
        admin.store(StoreAsk::Read { object: object_key(&name, &class, &id) }).await?
    else {
        return Err(Refusal(StatusCode::INTERNAL_SERVER_ERROR, "internal", ANOTHER_CALL.into()));
    };
    let bytes: usize = entries.iter().map(|(key, value)| key.len() + value.len()).sum();
    let entries: Vec<Value> = entries
        .into_iter()
        .map(|(key, value)| json!({ "key": key, "value": serde_json::from_str::<Value>(&value).unwrap_or(Value::String(value)) }))
        .collect();
    ok(json!({ "class": class, "id": id, "epoch": epoch, "bytes": bytes, "entries": entries }))
}

/// Removes what an object stores. If it runs, its actor holds a copy of what is gone: the store refuses its next
/// request, which fails, and the one after starts from nothing.
async fn clear_object(State(admin): State<Admin>, Path((name, class, id)): Path<(String, String, String)>) -> Reply {
    admin.store(StoreAsk::Clear { object: object_key(&name, &class, &id) }).await?;
    ok(json!({ "cleared": { "class": class, "id": id } }))
}

/// What a runner serves, a canary as a deployment's is: `{version, percent}`.
fn serving_json(serving: &BTreeMap<String, Served>) -> Value {
    let served = |served: &Served| json!({ "seq": served.seq, "stable": served.stable, "canary": canary_json(served.canary), "warm": served.warm });
    serving.iter().map(|(name, at)| (name.clone(), served(at))).collect::<serde_json::Map<_, _>>().into()
}

/// The nodes this one sees, their roles, and what each runner serves.
async fn cluster(State(admin): State<Admin>) -> Reply {
    let view = admin.0.cluster.view();
    let (runners, silent) = node::stats(&admin.0.cluster).await;
    let nodes: Vec<Value> = view
        .members
        .iter()
        .map(|member| {
            let record = &member.record;
            let roles: Vec<&str> =
                ["control", "gateway", "runner"].into_iter().filter(|role| record.label(role).is_some()).collect();
            let stats = runners.iter().find(|runner| runner.node == record.id);
            let in_flight = stats.map(|runner| runner.workers.values().map(|f| f.in_flight).sum::<u64>());
            json!({
                "id": record.id,
                "address": record.address.to_string(),
                "roles": roles,
                // Where clients reach a gateway, as it said itself; null for the other nodes.
                "url": record.metadata.get(gateway::URL),
                "available": member.available(),
                "reachable": member.reachable,
                "serving": stats.map(|runner| serving_json(&runner.serving)),
                "in_flight": in_flight,
                "logs_dropped": stats.map(|runner| runner.logs_dropped),
            })
        })
        .collect();
    ok(json!({ "node": admin.0.cluster.identity().node_id, "nodes": nodes, "silent": silent }))
}
