//! The HTTP API under `/api`, and `/ready` and `/metrics` beside it. A call on a run goes to the run's keyed actor,
//! which answers once what it changed is on a majority of the history replicas; a list or a count comes from the
//! first history replica that answers, and can lag it by a write. Errors are `{error, message}`, with the codes of
//! the design's table; a 429 or a 503 says when to try again in `Retry-After`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use axon::{ActorHandle, ActorSystem};
use axon_cluster::{
    CallError, Cluster, ClusterError, KeyedRefs, Member, Quotas, Rate, Rejection, SpawnRefusal, WeakCluster,
};
use axon_net::SubmitReason;
use axum::extract::{FromRequest, MatchedPath, Path, Query, Request as HttpRequest, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use bytes::Bytes;
use futures_util::StreamExt as _;
use include_dir::{Dir, include_dir};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::auth::{ADMIN, Auth, Caller, Scope, Tokens, hex, unhex};
use crate::journal::index::{Counts, Indexed, Page, Query as ListQuery};
use crate::journal::service::ToJournal;
use crate::journal::wire::{IndexAnswer, IndexCall, MAX_LIST, ask_index};
use crate::journal::{Entry, Status};
use crate::metrics::{EngineMetrics, Text};
use crate::node::{Node, RUN_ARGS, run_placement};
use crate::observer::{self, Observer, ObserverConfig};
use crate::queue::{QUEUE_ARGS, QueueAnswer, QueueApi, QueueCall, QueueStats, partitions};
use crate::run::wire::valid_id;
use crate::run::{Code, RUN, RunAnswer, RunApi, RunCall, RunView, call_idempotent};
use crate::worker::{
    DRAINING_KEY, SLOTS_KEY, WorkerApi, WorkerCall, WorkerStats, queue_label, served_queues, worker_name,
};
use crate::workflow::App;

const COOKIE: &str = "wf_session";
/// The largest request body: a frame. A run refuses a payload past 256 KiB itself, `too_large`.
const MAX_BODY: usize = 1 << 20;
/// A body up to this is read to its end before it is refused `too_large`, so that the client gets the answer.
const DRAIN_BODY: usize = 8 * MAX_BODY;
/// How long a list or a count waits for one replica before it asks the next.
const ASK_WITHIN: Duration = Duration::from_secs(5);
/// The longest long poll.
const MAX_WAIT: Duration = Duration::from_secs(60);
/// How often a long poll looks at its run.
const WAIT_POLL: Duration = Duration::from_millis(250);

pub struct ApiConfig {
    /// The admin's password, an argon2id PHC string (`wf hash-password`).
    pub password_hash: String,
    /// Signs the sessions, the same on every api node; at least 32 bytes. `None`: a random key, and every session
    /// ends with the process.
    pub key: Option<Vec<u8>>,
    /// `tokens.toml`, read at start and again every `tokens_every`: a token taken out is refused from then on.
    pub tokens: Option<PathBuf>,
    pub tokens_every: Duration,
    pub quotas: Option<Quotas>,
    /// Starts of one workflow, across the cluster; needs `quotas`.
    pub workflow_starts: Option<Rate>,
    /// Starts by one token name, across the cluster (`admin` for password sessions); needs `quotas`.
    pub token_starts: Option<Rate>,
    /// How long a password session lasts.
    pub session: Duration,
    /// Long polls at once on this node; past them `waiters_full`.
    pub max_waiters: usize,
    /// How long a call waits for a run that moves, opens or is busy, at most.
    pub patience: Duration,
    pub observer: ObserverConfig,
    /// Where a browser finds the console: `/`, or the path a proxy in front puts it at, with a `/` at both ends.
    pub base_path: String,
}

impl ApiConfig {
    pub fn new(password_hash: impl Into<String>) -> Self {
        ApiConfig {
            password_hash: password_hash.into(),
            key: None,
            tokens: None,
            tokens_every: Duration::from_secs(10),
            quotas: None,
            workflow_starts: None,
            token_starts: None,
            session: Duration::from_secs(30 * 86_400),
            max_waiters: 1_024,
            patience: Duration::from_secs(10),
            observer: ObserverConfig::default(),
            base_path: "/".into(),
        }
    }
}

struct Inner {
    runs: KeyedRefs<RunApi>,
    queues: KeyedRefs<QueueApi>,
    /// The queues the build's activities name; the members' `queue.<name>` labels name the others.
    app_queues: BTreeSet<String>,
    app: Arc<App>,
    /// This node's replica, on a history node.
    journal: Option<ActorHandle<ToJournal>>,
    replicas: Vec<Arc<str>>,
    /// This node's runs, on an engine.
    metrics: Option<Arc<EngineMetrics>>,
    cluster: WeakCluster,
    quotas: Option<Quotas>,
    workflow_starts: Option<Rate>,
    token_starts: Option<Rate>,
    auth: Auth,
    tokens: RwLock<Arc<Tokens>>,
    /// One password check at a time: each is tens of milliseconds of CPU.
    logins: Semaphore,
    session: Duration,
    waiters: Semaphore,
    patience: Duration,
    sample: Duration,
    observer: ActorHandle<observer::Msg>,
    base_path: String,
}

#[derive(Clone)]
pub struct Api(Arc<Inner>);

/// A refusal as the API says it: a status, a code to act on, and a sentence to read.
#[derive(Debug)]
pub struct Refusal(StatusCode, &'static str, String);

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let mut response = (self.0, Json(json!({ "error": self.1, "message": self.2 }))).into_response();
        if matches!(self.0, StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS) {
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

fn invalid(why: impl Into<String>) -> Refusal {
    Refusal(StatusCode::BAD_REQUEST, "invalid", why.into())
}

fn refusal(code: Code, message: String) -> Refusal {
    let status = match code {
        Code::Invalid | Code::VersionMissing => StatusCode::BAD_REQUEST,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::Exists | Code::Finished | Code::NotFinished | Code::NotFailed | Code::NotResumable | Code::Paused => {
            StatusCode::CONFLICT
        }
        Code::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        Code::Limited => StatusCode::TOO_MANY_REQUESTS,
        Code::Busy | Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    Refusal(status, code.as_str(), message)
}

/// A run that did not answer: `busy` where a node or a mailbox is full, `unavailable` otherwise.
fn not_answered(error: &ClusterError) -> Refusal {
    let busy = matches!(
        error,
        ClusterError::Spawn(SpawnRefusal::Capacity, _)
            | ClusterError::Call(
                CallError::Busy | CallError::NotSent(SubmitReason::Full) | CallError::Rejected(Rejection::Full)
            )
    );
    let code = if busy { "busy" } else { "unavailable" };
    Refusal(StatusCode::SERVICE_UNAVAILABLE, code, format!("the run did not answer: {error}"))
}

fn unavailable(why: impl Into<String>) -> Refusal {
    Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable", why.into())
}

type Reply = Result<Response, Refusal>;

fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1_000
}

/// A JSON body, refused `invalid` (`too_large` past [`MAX_BODY`]) as the API's error; an empty body is `{}`.
struct Body<T>(T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = Refusal;

    async fn from_request(request: HttpRequest, _: &S) -> Result<Self, Refusal> {
        let too_large =
            || Refusal(StatusCode::PAYLOAD_TOO_LARGE, "too_large", format!("a body is {MAX_BODY} bytes at most"));
        let length = request.headers().get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
        if length.is_some_and(|n| n > DRAIN_BODY) {
            return Err(too_large());
        }
        // Past MAX_BODY the rest is read and dropped, up to DRAIN_BODY: closed with the body unread, the socket
        // resets, and a client still sending sees the reset, not the 413.
        let mut chunks = request.into_body().into_data_stream();
        let (mut bytes, mut seen) = (Vec::new(), 0);
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| invalid(format!("the body: {e}")))?;
            seen += chunk.len();
            if seen > DRAIN_BODY {
                return Err(too_large());
            }
            if seen <= MAX_BODY {
                bytes.extend_from_slice(&chunk);
            }
        }
        if seen > MAX_BODY {
            return Err(too_large());
        }
        let text: &[u8] = if bytes.iter().all(u8::is_ascii_whitespace) { b"{}" } else { &bytes };
        serde_json::from_slice(text).map(Body).map_err(|e| invalid(format!("the body: {e}")))
    }
}

/// The query string's parameters, each once.
type Params = Query<BTreeMap<String, String>>;

fn number<T: std::str::FromStr>(params: &BTreeMap<String, String>, key: &str) -> Result<Option<T>, Refusal> {
    let parse = |text: &String| text.parse().map_err(|_| invalid(format!("{key} is a number")));
    params.get(key).map(parse).transpose()
}

fn duration(params: &BTreeMap<String, String>, key: &str) -> Result<Option<Duration>, Refusal> {
    let parse = |text: &String| parse_duration(text).ok_or(invalid(format!("{key} is a duration: 30s, 5m, 2h, 7d")));
    params.get(key).map(parse).transpose()
}

impl Api {
    /// The API over `node`'s references, with its observer started on `system`; an api node serves
    /// [`Api::router`].
    pub fn new(system: &ActorSystem, node: &Node, cluster: &Cluster, cfg: ApiConfig) -> Result<Api, String> {
        if !cfg.base_path.starts_with('/') || !cfg.base_path.ends_with('/') || cfg.base_path.contains(['"', '<', '>']) {
            return Err(format!("the console's base path starts and ends with /: {:?}", cfg.base_path));
        }
        let auth = Auth::new(&cfg.password_hash, cfg.key.as_deref())?;
        let tokens = match &cfg.tokens {
            Some(file) => read_tokens(file)?,
            None => Tokens::default(),
        };
        let sample = cfg.observer.sample;
        let observer = Observer::new(cluster.downgrade(), node.replicas.clone(), cfg.observer);
        let observer = system.spawn(observer).name("observer").start().map_err(|e| e.to_string())?;
        let api = Api(Arc::new(Inner {
            runs: node.runs.clone(),
            queues: node.queues.clone(),
            app_queues: node.app_queues.clone(),
            app: node.app.clone(),
            journal: node.journal.clone(),
            replicas: node.replicas.clone(),
            metrics: node.engine.then(|| node.metrics.clone()),
            cluster: cluster.downgrade(),
            quotas: cfg.quotas,
            workflow_starts: cfg.workflow_starts,
            token_starts: cfg.token_starts,
            auth,
            tokens: RwLock::new(Arc::new(tokens)),
            logins: Semaphore::new(1),
            session: cfg.session,
            waiters: Semaphore::new(cfg.max_waiters),
            patience: cfg.patience,
            sample,
            observer,
            base_path: cfg.base_path,
        }));
        if let Some(file) = cfg.tokens {
            tokio::spawn(reread_tokens(Arc::downgrade(&api.0), file, cfg.tokens_every));
        }
        Ok(api)
    }

    pub fn router(&self) -> Router {
        let guarded = Router::new()
            .route("/api/logout", post(logout))
            .route("/api/runs", get(list).post(start))
            .route("/api/runs/{id}", get(read).delete(delete))
            .route("/api/runs/{id}/history", get(history))
            .route("/api/runs/{id}/wait", get(wait))
            .route("/api/runs/{id}/signals/{name}", post(signal))
            .route("/api/runs/{id}/cancel", post(cancel))
            .route("/api/runs/{id}/terminate", post(terminate))
            .route("/api/runs/{id}/pause", post(pause))
            .route("/api/runs/{id}/resume", post(resume))
            .route("/api/runs/{id}/retry", post(retry))
            .route("/api/workflows", get(workflows))
            .route("/api/queues", get(queues))
            .route("/api/workers", get(workers))
            .route("/api/metrics", get(metrics))
            .route("/api/cluster", get(cluster_page))
            .route("/metrics", get(prometheus))
            .route_layer(axum::middleware::from_fn_with_state(self.clone(), guard));
        Router::new()
            .merge(guarded)
            .route("/api/login", post(login))
            .route("/api/session", get(session))
            .route("/ready", get(ready))
            .fallback(console)
            .with_state(self.clone())
    }

    /// Who calls: a named token from the file, or a session this key signed whose name the file still lists (the
    /// admin's always).
    fn caller(&self, headers: &HeaderMap) -> Option<Caller> {
        let bearer = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()?.strip_prefix("Bearer "));
        let presented = bearer.or_else(|| session_cookie(headers))?;
        let tokens = self.0.tokens.read().map(|t| t.clone()).unwrap_or_default();
        if presented.starts_with("wft_") {
            return tokens.caller(presented).cloned();
        }
        let (caller, _) = self.0.auth.check(presented, now_ms())?;
        (caller.name == ADMIN || tokens.has(&caller.name)).then_some(caller)
    }

    /// One call to the run, asked again while it moves, opens or its node is busy, up to the patience.
    async fn call(&self, id: &str, call: RunCall) -> Result<RunAnswer, Refusal> {
        if !valid_id(id) {
            return Err(invalid("a run id is 1 to 128 of A-Z a-z 0-9 . _ : / -"));
        }
        let run = self.0.runs.get(id, RUN_ARGS);
        // The whole wait is the deadline, not the cluster's `request_timeout`: a run may answer only once it opens.
        match call_idempotent(&run, call, tokio::time::Instant::now() + self.0.patience).await {
            Ok(RunAnswer::Refused { code, message }) => Err(refusal(code, message)),
            Ok(answer) => Ok(answer),
            Err(error) => Err(not_answered(&error)),
        }
    }

    /// The first replica, in the order every node lists them, that answers: its name and answer.
    async fn index(&self, call: IndexCall) -> Result<(Arc<str>, IndexAnswer), Refusal> {
        let cluster = self.0.cluster.upgrade().ok_or_else(|| unavailable("the node is stopping"))?;
        ask_index(&cluster, &self.0.replicas, call, ASK_WITHIN)
            .await
            .map_err(|last| unavailable(format!("no history replica answered; the last: {last}")))
    }

    async fn counts(&self) -> Result<Counts, Refusal> {
        match self.index(IndexCall::Counts).await? {
            (_, IndexAnswer::Counts(counts)) => Ok(counts),
            (replica, other) => Err(odd(&format!("{replica} answered {other:?}"))),
        }
    }
}

fn read_tokens(file: &std::path::Path) -> Result<Tokens, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("{}: {e}", file.display()))?;
    Tokens::parse(&text).map_err(|e| format!("{}: {e}", file.display()))
}

/// Reads `tokens.toml` again every `every` while the api lives. A file that does not read keeps the tokens before it.
async fn reread_tokens(api: Weak<Inner>, file: PathBuf, every: Duration) {
    let mut last_error = None;
    loop {
        tokio::time::sleep(every).await;
        let Some(api) = api.upgrade() else { return };
        let path = file.clone();
        let read = tokio::task::spawn_blocking(move || read_tokens(&path)).await.unwrap_or_else(|e| Err(e.to_string()));
        match read {
            Ok(tokens) => {
                if let Ok(mut kept) = api.tokens.write() {
                    *kept = Arc::new(tokens);
                }
                last_error = None;
            }
            Err(e) if last_error.as_ref() != Some(&e) => {
                eprintln!("api: kept the tokens read before: {e}");
                last_error = Some(e);
            }
            Err(_) => {}
        }
    }
}

/// The scope a route needs: reads `read`, a signal `signal`, a start `start`, the rest `admin`; logout any.
fn needs(method: &Method, path: &str) -> Scope {
    match (method, path) {
        (_, "/api/logout") => Scope::Read,
        (&Method::GET, _) => Scope::Read,
        (&Method::POST, "/api/runs") => Scope::Start,
        (&Method::POST, path) if path.ends_with("/signals/{name}") => Scope::Signal,
        _ => Scope::Admin,
    }
}

async fn guard(State(api): State<Api>, matched: Option<MatchedPath>, mut request: HttpRequest, next: Next) -> Response {
    let Some(caller) = api.caller(request.headers()) else {
        return Refusal(StatusCode::UNAUTHORIZED, "unauthorized", "sign in first: POST /api/login, or a token".into())
            .into_response();
    };
    let need = needs(request.method(), matched.as_ref().map_or("", |m| m.as_str()));
    if caller.scope < need {
        let message = format!("{} has scope {}; this needs {need}", caller.name, caller.scope);
        return Refusal(StatusCode::FORBIDDEN, "forbidden", message).into_response();
    }
    request.extensions_mut().insert(caller);
    next.run(request).await
}

#[derive(Deserialize)]
struct Login {
    password: String,
    /// For a command line: the session comes back as a token.
    #[serde(default)]
    token: bool,
}

async fn login(State(api): State<Api>, Body(login): Body<Login>) -> Reply {
    let inner = api.0.clone();
    let Ok(_slot) = inner.logins.try_acquire() else {
        return Err(Refusal(StatusCode::TOO_MANY_REQUESTS, "limited", "another sign-in is being checked".into()));
    };
    let (check, password) = (inner.clone(), login.password);
    if !tokio::task::spawn_blocking(move || check.auth.verify(&password)).await.unwrap_or(false) {
        return Err(Refusal(StatusCode::UNAUTHORIZED, "unauthorized", "wrong password".into()));
    }
    let expires_ms = now_ms() + inner.session.as_millis() as u64;
    let sealed = inner.auth.seal(expires_ms, &Caller { name: ADMIN.into(), scope: Scope::Admin });
    if login.token {
        return Ok(Json(json!({ "token": sealed, "expires_ms": expires_ms })).into_response());
    }
    // No `Secure`: the listener is plain HTTP, and what puts TLS in front of it adds that.
    let cookie = format!("{COOKIE}={sealed}; Path=/; Max-Age={}; HttpOnly; SameSite=Strict", inner.session.as_secs());
    Ok(([(header::SET_COOKIE, cookie)], Json(json!({ "expires_ms": expires_ms }))).into_response())
}

/// The value of the session cookie the request carries.
fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    cookies.split(';').find_map(|pair| pair.trim().strip_prefix(COOKIE)?.strip_prefix('='))
}

/// Who the request's session or token is, and when it ends: nulls when it has none. A browser asks before it shows
/// the sign-in page.
async fn session(State(api): State<Api>, headers: HeaderMap) -> Response {
    let caller = api.caller(&headers);
    let expires_ms =
        caller.as_ref().and(session_cookie(&headers)).and_then(|c| api.0.auth.check(c, now_ms())).map(|(_, at)| at);
    let (name, scope) = caller.map_or((None, None), |c| (Some(c.name), Some(c.scope.to_string())));
    Json(json!({ "name": name, "scope": scope, "expires_ms": expires_ms })).into_response()
}

/// The console, as `npm run build` in `ui/` left it (`build.rs` refuses a build without it).
static CONSOLE: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");
/// The console's page loads scripts, styles and fonts from this origin only, and no other site may frame it.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
                   font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; \
                   object-src 'none'";

/// A file of the console, or its page: the browser's router reads the path. A path under `/api` that is no route
/// is the API's 404, and so is a file under `assets/` the console does not have, so a mistyped asset is not answered
/// with a page.
async fn console(State(api): State<Api>, request: HttpRequest) -> Response {
    let base = api.0.base_path.as_str();
    let whole = request.uri().path();
    // A proxy that mounts the console at the base path may pass the path whole or without the base.
    let path = whole.strip_prefix(base).unwrap_or(whole).trim_start_matches('/');
    let missing = || Refusal(StatusCode::NOT_FOUND, "not_found", format!("nothing at {whole}")).into_response();
    if whole == "/api" || whole.starts_with("/api/") || !matches!(*request.method(), Method::GET | Method::HEAD) {
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
    // Every built file is under `assets/`; any other path is a page, a run id with a dot too.
    let page = !path.starts_with("assets/");
    match CONSOLE.get_file("index.html").and_then(|file| file.contents_utf8()).filter(|_| page) {
        Some(index) => {
            let based = index.replacen("<base href=\"/\" />", &format!("<base href=\"{base}\" />"), 1);
            secured(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], based).into_response(), "no-store")
        }
        None => missing(),
    }
}

/// Clears the cookie. A session value kept elsewhere holds until it expires: sign every session out by giving the
/// api nodes a new key.
async fn logout() -> Response {
    let cookie = format!("{COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict");
    ([(header::SET_COOKIE, cookie)], Json(json!({}))).into_response()
}

#[derive(Deserialize)]
struct StartBody {
    workflow: String,
    version: Option<u32>,
    id: Option<String>,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    tags: Vec<(String, String)>,
    /// `30s`, `5m`, `2h` or `7d`.
    run_timeout: Option<String>,
}

/// `30s`, `5m`, `2h`, `7d`, or a number of milliseconds.
pub fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let (n, unit) = text.split_at(split);
    let n: u64 = n.parse().ok()?;
    let secs = match unit {
        "" | "ms" => return Some(Duration::from_millis(n)),
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(3_600)?,
        "d" => n.checked_mul(86_400)?,
        _ => return None,
    };
    Some(Duration::from_secs(secs))
}

fn limited(what: String, retry_after: Duration) -> Response {
    let after = retry_after.as_secs().max(1).to_string();
    let message = format!("{what}: try again in {after} s");
    let mut response = Refusal(StatusCode::TOO_MANY_REQUESTS, "limited", message).into_response();
    if let Ok(value) = HeaderValue::from_str(&after) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

async fn start(State(api): State<Api>, Extension(caller): Extension<Caller>, Body(body): Body<StartBody>) -> Reply {
    let run_timeout_ms = match body.run_timeout.as_deref().map(parse_duration) {
        None => None,
        Some(Some(d)) if !d.is_zero() => Some(d.as_millis() as u64),
        Some(_) => return Err(invalid("run_timeout is a duration such as 30s, 5m, 2h or 7d")),
    };
    let input = Bytes::from(serde_json::to_vec(&body.input).map_err(|e| invalid(e.to_string()))?);
    if let Some(quotas) = &api.0.quotas {
        if let Some(rate) = api.0.token_starts
            && let Err(denied) = quotas.take(format!("token:{}", caller.name), &[(rate, 1)])
        {
            return Ok(limited(format!("too many starts by {}", caller.name), denied.retry_after()));
        }
        // A name this build does not know shares one key, so made-up names cannot fill the quota's rows.
        let key = if api.0.app.latest(&body.workflow).is_some() { body.workflow.as_str() } else { "?" };
        if let Some(rate) = api.0.workflow_starts
            && let Err(denied) = quotas.take(key, &[(rate, 1)])
        {
            return Ok(limited(format!("too many starts of {}", body.workflow), denied.retry_after()));
        }
    }
    let id = body.id.unwrap_or_else(|| format!("run-{:016x}", axon::trace::random_u64()));
    let call = RunCall::Start {
        workflow: body.workflow,
        version: body.version,
        input,
        tags: body.tags,
        parent: None,
        run_timeout_ms,
    };
    match api.call(&id, call).await? {
        RunAnswer::Started { version, created } => {
            let status = if created { StatusCode::CREATED } else { StatusCode::OK };
            Ok((status, Json(json!({ "id": id, "version": version, "created": created }))).into_response())
        }
        other => Err(unexpected(other)),
    }
}

fn odd(what: &str) -> Refusal {
    Refusal(StatusCode::INTERNAL_SERVER_ERROR, "internal", what.to_owned())
}

/// A run's answer that is not the one its call asks for.
fn unexpected(answer: RunAnswer) -> Refusal {
    odd(&format!("the run answered {answer:?}"))
}

fn json_or_text(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
}

/// A run as JSON: its outputs as JSON values, not strings.
pub fn view_json(view: &RunView) -> Value {
    let steps: Vec<Value> = view
        .steps
        .iter()
        .map(|s| {
            json!({
                "step": s.step, "kind": s.kind, "name": s.name, "status": s.status, "attempts": s.attempts,
                "wake_at_us": s.wake_at_us, "error": s.error, "output": s.output.as_deref().map(json_or_text),
                "input": s.input.as_deref().map(json_or_text), "seq": s.seq, "started_us": s.started_us,
                "ended_us": s.ended_us,
            })
        })
        .collect();
    json!({
        "id": view.id, "workflow": view.workflow, "version": view.version, "status": view.status,
        "output": view.output.as_deref().map(json_or_text), "error": view.error, "parked": view.parked,
        "due_us": view.due_us, "custom_status": view.custom_status.as_deref().map(json_or_text), "tags": view.tags,
        "parent": view.parent, "owner": view.owner, "epoch": view.epoch, "started_us": view.started_us,
        "steps": steps, "events": view.events,
    })
}

async fn read(State(api): State<Api>, Path(id): Path<String>) -> Reply {
    match api.call(&id, RunCall::Get).await? {
        RunAnswer::Run(view) => Ok(Json(view_json(&view)).into_response()),
        other => Err(unexpected(other)),
    }
}

/// A journal entry as JSON, with each byte string that holds UTF-8 as that text, or as the JSON in it.
fn readable(value: Value) -> Value {
    match value {
        Value::Array(items) => {
            let bytes: Option<Vec<u8>> = items.iter().map(|v| v.as_u64().and_then(|b| u8::try_from(b).ok())).collect();
            match bytes.filter(|b| !b.is_empty()).and_then(|b| String::from_utf8(b).ok()) {
                Some(text) => json_or_text(&text),
                None => Value::Array(items.into_iter().map(readable).collect()),
            }
        }
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, readable(v))).collect()),
        other => other,
    }
}

/// A page of the run's journal, from entry `after` (from 0), at most `limit`; `next` where the next page starts.
async fn history(State(api): State<Api>, Path(id): Path<String>, Query(params): Params) -> Reply {
    let after = number(&params, "after")?.unwrap_or(0);
    let limit = number(&params, "limit")?.unwrap_or(100u32);
    match api.call(&id, RunCall::History { after, limit }).await? {
        RunAnswer::History { entries, next } => {
            let entries: Vec<Value> = entries
                .iter()
                .map(|body| match Entry::decode(body) {
                    Ok(entry) => readable(serde_json::to_value(&entry).unwrap_or(Value::Null)),
                    Err(e) => json!({ "unreadable": e.to_string() }),
                })
                .collect();
            Ok(Json(json!({ "entries": entries, "next": next })).into_response())
        }
        other => Err(unexpected(other)),
    }
}

/// Waits until the run ends, up to `timeout` (at most 60 s): 200 and the run, or 202 and the run at the deadline.
async fn wait(State(api): State<Api>, Path(id): Path<String>, Query(params): Params) -> Reply {
    let timeout = duration(&params, "timeout")?.unwrap_or(MAX_WAIT).min(MAX_WAIT);
    let Ok(_waiter) = api.0.waiters.try_acquire() else {
        let message = "this node holds as many waits as it takes: try again, or poll GET /api/runs/:id";
        return Err(Refusal(StatusCode::TOO_MANY_REQUESTS, "waiters_full", message.into()));
    };
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let view = match api.call(&id, RunCall::Get).await? {
            RunAnswer::Run(view) => view,
            other => return Err(unexpected(other)),
        };
        if view.status.is_finished() {
            return Ok(Json(view_json(&view)).into_response());
        }
        if tokio::time::Instant::now() + WAIT_POLL >= deadline {
            return Ok((StatusCode::ACCEPTED, Json(view_json(&view))).into_response());
        }
        tokio::time::sleep(WAIT_POLL).await;
    }
}

#[derive(Deserialize)]
struct SignalBody {
    signal_id: Option<String>,
    #[serde(default)]
    payload: Value,
}

async fn signal(
    State(api): State<Api>,
    Path((id, name)): Path<(String, String)>,
    Body(body): Body<SignalBody>,
) -> Reply {
    let signal_id = body.signal_id.unwrap_or_else(|| format!("sig-{:016x}", axon::trace::random_u64()));
    if signal_id.is_empty() || signal_id.len() > 128 || name.is_empty() || name.len() > 128 {
        return Err(invalid("a signal's name and id are 1 to 128 bytes"));
    }
    let payload = Bytes::from(serde_json::to_vec(&body.payload).map_err(|e| invalid(e.to_string()))?);
    match api.call(&id, RunCall::Signal { signal_id: signal_id.clone(), name, payload }).await? {
        RunAnswer::Signalled { duplicate } => {
            Ok((StatusCode::ACCEPTED, Json(json!({ "signal_id": signal_id, "duplicate": duplicate }))).into_response())
        }
        other => Err(unexpected(other)),
    }
}

#[derive(Deserialize, Default)]
struct Reason {
    #[serde(default)]
    reason: String,
}

fn accepted(answer: RunAnswer) -> Reply {
    match answer {
        RunAnswer::Accepted => Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response()),
        other => Err(unexpected(other)),
    }
}

async fn cancel(State(api): State<Api>, Path(id): Path<String>, Body(body): Body<Reason>) -> Reply {
    accepted(api.call(&id, RunCall::Cancel { reason: body.reason }).await?)
}

async fn terminate(State(api): State<Api>, Path(id): Path<String>, Body(body): Body<Reason>) -> Reply {
    accepted(api.call(&id, RunCall::Terminate { reason: body.reason }).await?)
}

async fn pause(State(api): State<Api>, Path(id): Path<String>) -> Reply {
    accepted(api.call(&id, RunCall::Pause).await?)
}

async fn resume(State(api): State<Api>, Path(id): Path<String>) -> Reply {
    accepted(api.call(&id, RunCall::Resume).await?)
}

async fn retry(State(api): State<Api>, Path(id): Path<String>) -> Reply {
    accepted(api.call(&id, RunCall::Retry).await?)
}

async fn delete(State(api): State<Api>, Path(id): Path<String>) -> Reply {
    accepted(api.call(&id, RunCall::Delete).await?)
}

/// A list's cursor: the last run's update and its id, `<µs>.<hex id>`.
fn cursor((updated_us, run): &(u64, Bytes)) -> String {
    format!("{updated_us}.{}", hex(run))
}

fn read_cursor(text: &str) -> Option<(u64, Bytes)> {
    let (updated, run) = text.split_once('.')?;
    Some((updated.parse().ok()?, Bytes::from(unhex(run).filter(|r| !r.is_empty())?)))
}

/// The filters of `GET /api/runs`, as the index reads them.
fn list_query(params: &BTreeMap<String, String>) -> Result<ListQuery, Refusal> {
    let known = ["workflow", "status", "tag", "parent", "prefix", "since", "since_ms", "cursor", "limit"];
    if let Some(key) = params.keys().find(|k| !known.contains(&k.as_str())) {
        return Err(invalid(format!("no filter {key}: {}", known.join(", "))));
    }
    let mut query = ListQuery { workflow: params.get("workflow").cloned(), ..ListQuery::default() };
    if let Some(status) = params.get("status") {
        let parsed = serde_json::from_value(Value::String(status.clone())).ok().filter(|s| *s != Status::Deleted);
        let why = "status is running, paused, parked, completed, failed, cancelled or terminated";
        query.status = Some(parsed.ok_or(invalid(why))?);
    }
    if let Some(tag) = params.get("tag") {
        let (k, v) = tag.split_once(':').ok_or(invalid("tag is key:value"))?;
        query.tag = Some((k.to_owned(), v.to_owned()));
    }
    query.prefix = match (params.get("prefix"), params.get("parent")) {
        (Some(_), Some(_)) => return Err(invalid("prefix or parent, not both")),
        (Some(prefix), None) => Some(prefix.clone()),
        (None, Some(parent)) => Some(format!("{parent}/")),
        (None, None) => None,
    };
    let ago = duration(params, "since")?.map(|d| now_ms().saturating_sub(d.as_millis() as u64));
    query.since_us = ago.or(number(params, "since_ms")?).map(|ms: u64| ms.saturating_mul(1_000));
    if let Some(text) = params.get("cursor") {
        query.after = Some(read_cursor(text).ok_or(invalid("cursor is the next of the page before"))?);
    }
    query.limit = number(params, "limit")?.unwrap_or(50);
    if !(1..=MAX_LIST).contains(&query.limit) {
        return Err(invalid(format!("limit is 1 to {MAX_LIST}")));
    }
    Ok(query)
}

fn row(api: &Api, run: &[u8], ix: &Indexed) -> Value {
    let s = &ix.summary;
    let id = String::from_utf8_lossy(run);
    let due_ms = s.timers.values().min().map(|us| us / 1_000);
    let waiting = match () {
        _ if s.status != Status::Running => None,
        _ if due_ms.is_some() => Some("timer"),
        _ if s.idle => Some("call"),
        _ => None,
    };
    let owner = (s.status == Status::Running && !s.idle)
        .then(|| api.0.cluster.upgrade().and_then(|c| c.owner(RUN, &id, &run_placement())))
        .flatten()
        .map(|node| node.to_string());
    json!({
        "id": id, "workflow": s.workflow, "version": s.version, "status": s.status, "tags": s.tags,
        "parent": s.parent, "started_ms": s.started_us / 1_000, "updated_ms": ix.updated_us / 1_000,
        "due_ms": due_ms, "waiting": waiting, "owner": owner, "parked_version": s.parked_version,
    })
}

/// Runs newest first, or in id order under a prefix or a parent, from the first replica that answers.
async fn list(State(api): State<Api>, Query(params): Params) -> Reply {
    let query = list_query(&params)?;
    let (replica, page) = match api.index(IndexCall::List(query)).await? {
        (replica, IndexAnswer::Page(page)) => (replica, page),
        (replica, other) => return Err(odd(&format!("{replica} answered {other:?}"))),
    };
    let Page { runs, next, cut } = page;
    let runs: Vec<Value> = runs
        .iter()
        .filter(|(_, ix)| ix.summary.status != Status::Deleted)
        .map(|(run, ix)| row(&api, run, ix))
        .collect();
    let next = next.as_ref().map(cursor);
    Ok(Json(json!({ "runs": runs, "next": next, "cut": cut, "replica": replica.as_ref() })).into_response())
}

fn status_name(status: Status) -> String {
    serde_json::to_value(status).ok().and_then(|v| v.as_str().map(str::to_owned)).unwrap_or_default()
}

/// Each workflow of the build, or that runs name: its versions here, its runs by status, the runs not on its
/// latest version, and those on a version this build lacks.
async fn workflows(State(api): State<Api>) -> Reply {
    let counts = api.counts().await.ok();
    let blank = |name: &str, versions: Vec<u32>| json!({ "name": name, "versions": versions, "runs": {}, "old_version": 0, "missing_version": 0 });
    let mut out: BTreeMap<String, Value> = BTreeMap::new();
    for (name, versions) in api.0.app.workflows() {
        out.insert(name.to_owned(), blank(name, versions));
    }
    let add = |value: &mut Value, n: u64| *value = json!(value.as_u64().unwrap_or(0) + n);
    for (workflow, version, status, n) in counts.iter().flat_map(|c| &c.runs) {
        let entry = out.entry(workflow.clone()).or_insert_with(|| blank(workflow, Vec::new()));
        add(&mut entry["runs"][status_name(*status)], *n);
        if status.is_finished() {
            continue;
        }
        if api.0.app.latest(workflow).is_some_and(|latest| *version < latest) {
            add(&mut entry["old_version"], *n);
        }
        if !api.0.app.has(workflow, *version) {
            add(&mut entry["missing_version"], *n);
        }
    }
    let counted = counts.is_some();
    Ok(Json(json!({ "workflows": out.into_values().collect::<Vec<_>>(), "counted": counted })).into_response())
}

/// How long the queues page waits for a partition's numbers.
const STATS_WITHIN: Duration = Duration::from_secs(2);

/// What each worker of each queue on the available worker nodes says, by node and queue; one that does not answer
/// within `STATS_WITHIN` is left out.
async fn worker_stats(api: &Api, members: &[Member]) -> BTreeMap<(String, String), WorkerStats> {
    let Some(cluster) = api.0.cluster.upgrade() else { return BTreeMap::new() };
    let asks = (members.iter().filter(|m| m.available() && m.record.label("worker") == Some("yes")))
        .flat_map(|m| served_queues(&m.record).map(move |queue| (m.record.id.clone(), queue.to_owned())))
        .map(|(node, queue)| {
            let cluster = cluster.clone();
            async move {
                let ask = async {
                    let worker = cluster.resolve_on::<WorkerApi>(&node, &worker_name(&queue)).await.ok()?;
                    worker.request_within(WorkerCall::Stats, STATS_WITHIN).await.ok()
                };
                let stats = tokio::time::timeout(STATS_WITHIN, ask).await.ok().flatten();
                stats.map(|stats| ((node, queue), stats))
            }
        });
    futures_util::future::join_all(asks).await.into_iter().flatten().collect()
}

/// Each queue's numbers over its partitions, the partitions that did not answer, and the nodes that serve it.
async fn queue_stats(api: &Api) -> Vec<(String, QueueStats, usize, Vec<String>)> {
    let view = api.0.cluster.upgrade().map(|cluster| cluster.view());
    let members = view.as_ref().map(|view| view.members.as_slice()).unwrap_or_default();
    let mut names = api.0.app_queues.clone();
    for member in members {
        names.extend(served_queues(&member.record).map(str::to_owned));
    }
    // Every partition of every queue at once: the page waits `STATS_WITHIN` at most.
    let asks = names.iter().map(|name| {
        let asks = partitions(name).into_iter().map(|key| {
            let queue = api.0.queues.get(&key, QUEUE_ARGS);
            async move { queue.request_within(QueueCall::Stats, STATS_WITHIN).await }
        });
        futures_util::future::join_all(asks)
    });
    let answers = futures_util::future::join_all(asks).await;
    let mut out = Vec::new();
    for (name, answers) in names.into_iter().zip(answers) {
        let (mut total, mut missing) = (QueueStats::default(), 0);
        for answer in answers {
            match answer {
                Ok(QueueAnswer::Stats(stats)) => total.add(&stats),
                _ => missing += 1,
            }
        }
        let label = queue_label(&name);
        let workers = (members.iter())
            .filter(|m| m.available() && m.record.label(&label) == Some("yes"))
            .map(|m| m.record.id.to_string())
            .collect();
        out.push((name, total, missing, workers));
    }
    out
}

/// Each queue: its backlog, the oldest task's wait and its pollers, over its partitions; its worker nodes, and their
/// slots, those in use and the attempts that failed in the last minute.
async fn queues(State(api): State<Api>) -> Reply {
    let view = api.0.cluster.upgrade().map(|cluster| cluster.view());
    let members = view.as_ref().map(|view| view.members.as_slice()).unwrap_or_default();
    let (queues, workers) = tokio::join!(queue_stats(&api), worker_stats(&api, members));
    let queues: Vec<Value> = queues
        .into_iter()
        .map(|(name, total, missing, nodes)| {
            let mine = workers.iter().filter(|((_, queue), _)| *queue == name).map(|(_, stats)| stats);
            let (slots, running, failed) =
                mine.fold((0, 0, 0), |(s, r, f), w| (s + w.slots, r + w.running, f + w.failed_minute));
            json!({
                "name": name, "backlog": total.backlog, "backlog_bytes": total.backlog_bytes,
                "oldest_ms": total.oldest_ms, "pollers": total.pollers, "refused": total.refused,
                "handed": total.handed, "workers": nodes, "partitions_missing": missing,
                "slots": slots, "running": running, "failed_minute": failed,
            })
        })
        .collect();
    Ok(Json(json!({ "queues": queues })).into_response())
}

/// Each worker node: its queues and slots as it announces them, whether it drains or left, and per queue what its
/// worker says: slots in use, attempts failed in the last minute, and the time since its last poll.
async fn workers(State(api): State<Api>) -> Reply {
    let view = api.0.cluster.upgrade().map(|cluster| cluster.view());
    let members = view.as_ref().map(|view| view.members.as_slice()).unwrap_or_default();
    let said = worker_stats(&api, members).await;
    let workers: Vec<Value> = members
        .iter()
        .filter(|m| m.record.label("worker") == Some("yes"))
        .map(|m| {
            let queues: Vec<&str> = served_queues(&m.record).collect();
            let slots: BTreeMap<&str, u64> = (m.record.label(SLOTS_KEY).unwrap_or_default().split(','))
                .filter_map(|pair| pair.split_once(':'))
                .filter_map(|(queue, n)| Some((queue, n.parse().ok()?)))
                .collect();
            let of = |queue: &&str| said.get(&(m.record.id.clone(), (*queue).to_owned()));
            let each = |f: fn(&WorkerStats) -> Option<u64>| -> BTreeMap<&str, u64> {
                queues.iter().filter_map(|q| Some((*q, f(of(q)?)?))).collect()
            };
            json!({
                "node": m.record.id.as_str(), "queues": queues, "slots": slots, "available": m.available(),
                "draining": m.record.label(DRAINING_KEY) == Some("yes") || !m.available(),
                "running": each(|w| Some(w.running.into())), "failed_minute": each(|w| Some(w.failed_minute.into())),
                "last_poll_ms": each(|w| w.last_poll_ms),
            })
        })
        .collect();
    Ok(Json(json!({ "workers": workers })).into_response())
}

/// The overview's counts, from a replica's index; the failed runs of the last hour from the observer's points.
fn count_json(counts: &Counts, points: &[observer::Point], sample: Duration) -> Value {
    let of = |status: Status| counts.runs.iter().filter(|r| r.2 == status).map(|r| r.3).sum::<u64>();
    let hour_ago = now_ms().saturating_sub(3_600_000);
    // Each point is a rate per minute over one sample.
    let per_point = sample.as_secs_f64() / 60.0;
    let failed: f64 = points.iter().filter(|p| p.at_ms >= hour_ago).map(|p| p.failed * per_point).sum();
    json!({
        "running": of(Status::Running).saturating_sub(counts.idle), "sleeping": counts.idle,
        "paused": of(Status::Paused), "parked": of(Status::Parked), "failed": of(Status::Failed),
        "failed_hour": failed.round() as u64, "old": counts.old, "stuck": counts.stuck,
    })
}

/// The overview: the observer's points from `from` to `to` (ms), and the counts now.
async fn metrics(State(api): State<Api>, Query(params): Params) -> Reply {
    let from = number(&params, "from")?.unwrap_or_else(|| now_ms().saturating_sub(3_600_000));
    let to = number(&params, "to")?.unwrap_or(u64::MAX);
    let asked = api.0.observer.ask(|reply| observer::Msg::Points(from, reply)).await;
    let (points, since_ms) = asked.map_err(|e| unavailable(format!("the observer did not answer: {e:?}")))?;
    let points: Vec<_> = points.into_iter().filter(|p| p.at_ms <= to).collect();
    let counts = api.counts().await.ok().map(|c| count_json(&c, &points, api.0.sample));
    Ok(Json(json!({ "points": points, "since_ms": since_ms, "counts": counts })).into_response())
}

/// The history replicas each up or not, as this node sees them, and whether a majority is.
fn quorum(api: &Api, cluster: &Cluster) -> (Vec<Value>, bool) {
    let view = cluster.view();
    let up = |id: &str| view.members.iter().any(|m| m.record.id.as_str() == id && m.available());
    let replicas: Vec<Value> = api.0.replicas.iter().map(|r| json!({ "node": r.as_ref(), "up": up(r) })).collect();
    let n = replicas.iter().filter(|r| r["up"] == true).count();
    (replicas, n > api.0.replicas.len() / 2)
}

async fn cluster_page(State(api): State<Api>) -> Reply {
    let cluster = api.0.cluster.upgrade().ok_or_else(|| unavailable("the node is stopping"))?;
    let (replicas, quorum) = quorum(&api, &cluster);
    let view = cluster.view();
    let members: Vec<Value> = (view.members.iter())
        .map(|m| {
            let roles: Vec<&str> = ["history", "engine", "api", "worker"]
                .into_iter()
                .filter(|role| m.record.label(role) == Some("yes"))
                .collect();
            json!({ "node": m.record.id.as_str(), "roles": roles, "available": m.available() })
        })
        .collect();
    Ok(Json(json!({
        "replicas": replicas, "quorum": quorum, "warming": cluster.warming(), "isolated": cluster.isolated(),
        "members": members,
    }))
    .into_response())
}

/// For a load balancer: 200 when a majority of the history replicas is up, this node can take leases and its quota
/// replica has the others' rows (a rolling restart waits for it, or starts pass the limits meanwhile).
async fn ready(State(api): State<Api>) -> Response {
    let Some(cluster) = api.0.cluster.upgrade() else {
        let said = json!({ "journal": false, "warming": false, "isolated": true, "quotas_starting": false });
        return (StatusCode::SERVICE_UNAVAILABLE, Json(said)).into_response();
    };
    let (_, journal) = quorum(&api, &cluster);
    let (warming, isolated, starting) = (cluster.warming(), cluster.isolated(), cluster.quotas_starting());
    let ok = journal && !warming && !isolated && !starting;
    let said = json!({ "journal": journal, "warming": warming, "isolated": isolated, "quotas_starting": starting });
    (if ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE }, Json(said)).into_response()
}

/// The engine's `wf_*` series in the Prometheus text format: this node's runs, a replica's counts, the queues.
async fn prometheus(State(api): State<Api>) -> Response {
    let mut text = Text::default();
    if let Some(metrics) = &api.0.metrics {
        metrics.write(&mut text);
    }
    if let Ok(counts) = api.counts().await {
        text.family(
            "wf_runs_started_total",
            "Runs started, as a history replica saw them since it started.",
            "counter",
        );
        for (workflow, n) in &counts.started {
            text.sample("wf_runs_started_total", &[("workflow", workflow)], *n as f64);
        }
        text.family("wf_runs_finished_total", "Runs ended, as a history replica saw them since it started.", "counter");
        for (workflow, status, n) in &counts.finished {
            let labels = [("workflow", workflow.as_str()), ("status", &status_name(*status))];
            text.sample("wf_runs_finished_total", &labels, *n as f64);
        }
        text.family("wf_runs", "Runs a history replica holds, by state.", "gauge");
        let states = count_json(&counts, &[], api.0.sample);
        for state in ["running", "sleeping", "paused", "parked", "failed"] {
            text.sample("wf_runs", &[("state", state)], states[state].as_u64().unwrap_or(0) as f64);
        }
        text.family("wf_stuck_runs", "Running runs with no record for 60 s that did not stop themselves.", "gauge");
        text.sample("wf_stuck_runs", &[], counts.stuck as f64);
    }
    if let Some(journal) = &api.0.journal
        && let Ok(stats) = journal.ask(ToJournal::Stats).await
    {
        let replica = [
            ("wf_journal_runs", "gauge", "Runs this history replica holds.", stats.runs as f64),
            (
                "wf_journal_repaired_total",
                "counter",
                "Records this replica copied from its peers.",
                stats.repaired as f64,
            ),
            (
                "wf_journal_expired_total",
                "counter",
                "Finished runs this replica removed past retention.",
                stats.expired as f64,
            ),
            (
                "wf_journal_failed_writes_total",
                "counter",
                "Writes of this replica's log that failed.",
                stats.failed_writes as f64,
            ),
            ("wf_journal_bytes", "gauge", "This replica's log on disk.", stats.log.bytes as f64),
            (
                "wf_journal_keys",
                "gauge",
                "Runs this replica's log counts against its max_keys, the removed ones whose segments remain too.",
                stats.log.keys as f64,
            ),
        ];
        for (name, kind, help, value) in replica {
            text.family(name, help, kind);
            text.sample(name, &[], value);
        }
    }
    let queues = queue_stats(&api).await;
    text.family("wf_queue_backlog", "Tasks waiting for a worker.", "gauge");
    for (name, stats, ..) in &queues {
        text.sample("wf_queue_backlog", &[("queue", name)], stats.backlog as f64);
    }
    text.family("wf_queue_oldest_seconds", "How long the oldest waiting task has waited.", "gauge");
    for (name, stats, ..) in &queues {
        text.sample("wf_queue_oldest_seconds", &[("queue", name)], stats.oldest_ms as f64 / 1e3);
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text.0).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_read_with_their_unit() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7_200)));
        assert_eq!(parse_duration("7d"), Some(Duration::from_secs(604_800)));
        assert_eq!(parse_duration("250"), Some(Duration::from_millis(250)));
        for bad in ["", "s", "5x", "-1s", "1.5s"] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn each_route_needs_its_scope() {
        assert_eq!(needs(&Method::GET, "/api/runs/{id}"), Scope::Read);
        assert_eq!(needs(&Method::GET, "/metrics"), Scope::Read);
        assert_eq!(needs(&Method::POST, "/api/logout"), Scope::Read);
        assert_eq!(needs(&Method::POST, "/api/runs/{id}/signals/{name}"), Scope::Signal);
        assert_eq!(needs(&Method::POST, "/api/runs"), Scope::Start);
        let admin = [(Method::POST, "/api/runs/{id}/cancel"), (Method::DELETE, "/api/runs/{id}"), (Method::POST, "")];
        for (method, path) in admin {
            assert_eq!(needs(&method, path), Scope::Admin, "{method} {path}");
        }
    }

    #[test]
    fn a_cursor_and_a_journal_entry_read_back() {
        let at = (1_234, Bytes::from_static(b"a/b"));
        assert_eq!(read_cursor(&cursor(&at)), Some(at));
        for bad in ["", "1", "x.00", "1.", "1.zz"] {
            assert_eq!(read_cursor(bad), None, "{bad}");
        }
        let entry = json!({ "Completed": { "output": [123, 34, 97, 34, 58, 49, 125] }, "seq": [1, 300] });
        assert_eq!(readable(entry), json!({ "Completed": { "output": { "a": 1 } }, "seq": [1, 300] }));
        assert_eq!(readable(json!([104, 105])), json!("hi"));
        assert_eq!(readable(json!([])), json!([]));
    }
}
