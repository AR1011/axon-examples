//! A gateway's request path: [`Gateway::call`] takes a request for a worker through the quotas to a runner and
//! back, and [`Gateway::router`] is the HTTP front of it.
//!
//! A request waits nowhere but on its runner's answer. It is refused at once past the gateway's own `max_in_flight`
//! (503), its account's rate or its worker's concurrency (429 with `Retry-After`). The runners are tried in the
//! worker's rendezvous order, the less loaded of the first two first, so a worker keeps to the same few
//! runners and stays warm there. A runner that says it is full, does not serve the worker yet, or cannot be
//! reached, did not run the request: the next one is asked, whatever the method. A runner that went quiet with the
//! request may have run it: a GET, HEAD or OPTIONS goes to one more runner, anything else is a 502 that says so.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axon::{Actor, ActorHandle, ActorSystem, Context, LifecycleHandle, TraceContext};
use axon_cluster::{
    CallError, Cluster, ClusterError, Denied, NodeChange, Nodes, Permits, Quotas, Rate, RemoteRef, Retry, View,
};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use bytes::Bytes;
use tokio::sync::{Semaphore, watch};

use crate::engine::Request;
use crate::wire::{Ask, INVOKE, Invocation, InvokeApi, Ran, Told, trace_id};

/// Requests a second, per account. The same on every node, as every quota is.
pub const REQUESTS: Rate = Rate::new("edge.requests", 1000, Duration::from_secs(1));
/// Requests in hand at once, per worker, over every gateway.
pub const CONCURRENCY: Permits = Permits::new("edge.concurrency", 256);

/// The metadata entry in which a gateway tells the cluster where clients reach it: `http://host:port`.
pub const URL: &str = "url";

/// Where clients reach a gateway: `public` (`http://host[:port]` or `https://..`, as a proxy or a DNS name in front
/// gives it), or `http://` and the address it listens on.
pub fn public_url(public: Option<&str>, listens: SocketAddr) -> Result<String, String> {
    let Some(public) = public else { return Ok(format!("http://{listens}")) };
    let url = public.trim_end_matches('/');
    let host = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://"));
    match host {
        Some(host) if !host.is_empty() && !host.contains(char::is_whitespace) => Ok(url.to_owned()),
        _ => Err(format!("public={public:?}: a URL as http://host:port or https://host")),
    }
}

/// Tells the cluster where this node's gateway is reached, for the control node's API to show.
pub async fn advertise(cluster: &Cluster, url: &str) -> Result<(), ClusterError> {
    cluster.set_metadata(URL, Some(url)).await
}

/// The gateways of `view` that can be sent to and say where they are reached: node id and URL, in id order.
pub fn urls(view: &View) -> Vec<(&str, &str)> {
    let mut urls: Vec<_> = crate::node::having(view, "gateway")
        .filter_map(|member| Some((member.record.id.as_str(), member.record.metadata.get(URL)?.as_str())))
        .collect();
    urls.sort_unstable();
    urls
}

/// The URL of a worker on the gateway at `base`.
pub fn worker_url(base: &str, name: &str) -> String {
    format!("{base}/w/{name}/")
}

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// A request's deadline unless it sends `x-deadline-ms`, and the most it may ask for.
    pub deadline: Duration,
    pub max_deadline: Duration,
    /// Requests in hand at once on this gateway; one past it is a 503 at once.
    pub max_in_flight: usize,
    /// A request's body. It goes to the runner in one frame, with its headers.
    pub max_body: usize,
    pub rate: Rate,
    pub concurrency: Permits,
    /// `<name>.<domain>` as a request's host names a worker, beside `/w/<name>/..` on any host.
    pub domain: Option<String>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(10),
            max_deadline: Duration::from_secs(30),
            max_in_flight: 4096,
            max_body: 512 << 10,
            rate: REQUESTS,
            concurrency: CONCURRENCY,
            domain: None,
        }
    }
}

/// A request for a worker, from HTTP or from a test.
#[derive(Clone, Debug)]
pub struct Call {
    /// Who is charged: the key of the rate limit. Authenticate before this.
    pub account: String,
    pub worker: String,
    pub request: Request,
    pub deadline: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    /// The id of the trace the gateway started for the request.
    pub trace: String,
    /// The runner that ran it, and the version it ran.
    pub node: Option<String>,
    pub version: Option<u32>,
    /// What the run took on its runner.
    pub cpu_us: Option<u64>,
    pub wall_us: Option<u64>,
    /// Why the response is not the worker's own: one of `RESULTS`, or the runner's word for a failed run.
    pub error: Option<String>,
    pub retry_after: Option<Duration>,
}

/// How a request ended at the gateway, for its counters. `ran` is every response a runner gave.
pub const RESULTS: [&str; 10] = [
    "ran",
    "not_found",
    "rate_limited",
    "concurrency_limited",
    "overloaded",
    "busy",
    "unavailable",
    "timeout",
    "outcome_unknown",
    "bad_gateway",
];

fn refuse(status: u16, code: &'static str, why: impl std::fmt::Display, retry_after: Option<Duration>) -> Response {
    let body = serde_json::json!({ "error": code, "message": why.to_string() }).to_string();
    Response {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: body.into(),
        error: Some(code.to_owned()),
        retry_after,
        ..Response::default()
    }
}

#[derive(Clone)]
struct Runner {
    node: String,
    remote: RemoteRef<InvokeApi>,
    /// This gateway's requests out to it.
    in_flight: Arc<AtomicU32>,
}

type Table = Arc<RwLock<Arc<Vec<Runner>>>>;

struct Holding<'a>(&'a AtomicU32);

impl Drop for Holding<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

struct Inner {
    cfg: GatewayConfig,
    quotas: Quotas,
    /// Whether this gateway's table names the worker.
    routes: Box<dyn Fn(&str) -> bool + Send + Sync>,
    /// The runners as the view has them, and the table of those this gateway can send to.
    nodes: Nodes,
    table: Table,
    runners: ActorHandle<RunnersMsg>,
    count: watch::Receiver<usize>,
    admission: Semaphore,
    results: [AtomicU64; RESULTS.len()],
    retries: AtomicU64,
}

#[derive(Clone)]
pub struct Gateway(Arc<Inner>);

impl Gateway {
    /// Starts following the cluster's runners. `routes` says whether a worker is deployed.
    pub fn start(
        system: &ActorSystem,
        cluster: &Cluster,
        quotas: Quotas,
        routes: impl Fn(&str) -> bool + Send + Sync + 'static,
        cfg: GatewayConfig,
    ) -> Result<Self, String> {
        let (table, nodes) = (Table::default(), cluster.nodes(&crate::node::runners()));
        let (ready, count) = watch::channel(0);
        let runners = Runners {
            cluster: cluster.clone(),
            nodes: nodes.clone(),
            table: table.clone(),
            looking: false,
            again: false,
            ready,
        };
        let runners = system.spawn(runners).name("runners").start().map_err(|e| format!("{e:?}"))?;
        let admission = Semaphore::new(cfg.max_in_flight);
        let (results, retries, routes) = (Default::default(), AtomicU64::new(0), Box::new(routes));
        let inner = Inner { cfg, quotas, routes, nodes, table, runners, count, admission, results, retries };
        Ok(Self(Arc::new(inner)))
    }

    /// Of the actor that keeps the table of runners.
    pub fn lifecycle(&self) -> LifecycleHandle {
        self.0.runners.lifecycle.clone()
    }

    /// Waits until the number of runners this gateway can send to satisfies `ready`.
    pub async fn wait_runners(&self, ready: impl FnMut(&usize) -> bool) {
        // The sender is the runners actor's: it goes only with the system.
        drop(self.0.count.clone().wait_for(ready).await);
    }

    /// The runners this gateway sends to now.
    pub fn runners(&self) -> usize {
        *self.0.count.borrow()
    }

    /// How many requests ended each way, by `RESULTS`, and how many were sent to a second runner after the first
    /// went quiet.
    pub fn stats(&self) -> ([u64; RESULTS.len()], u64) {
        (std::array::from_fn(|i| self.0.results[i].load(Relaxed)), self.0.retries.load(Relaxed))
    }

    /// The runners in the worker's rendezvous order.
    fn order(&self, worker: &str) -> Vec<Runner> {
        let table = self.0.table.read().unwrap_or_else(|e| e.into_inner()).clone();
        let order = self.0.nodes.order(worker).into_iter();
        order.filter_map(|node| table.iter().find(|runner| *runner.node == *node).cloned()).collect()
    }

    /// One request, in a trace of its own whose id the response carries.
    pub async fn call(&self, call: Call) -> Response {
        let trace = TraceContext::root();
        let mut response = trace.scope(self.route(call, trace.trace.get())).await;
        response.trace = trace_id(trace);
        let result = match &response.error {
            Some(code) => RESULTS.iter().position(|result| result == code).unwrap_or(0),
            None => 0,
        };
        self.0.results[result].fetch_add(1, Relaxed);
        response
    }

    async fn route(&self, call: Call, sample: u64) -> Response {
        let inner = &*self.0;
        let second = Some(Duration::from_secs(1));
        let Ok(_admitted) = inner.admission.try_acquire() else {
            return refuse(503, "overloaded", "this gateway has as many requests in hand as it takes", second);
        };
        if !(inner.routes)(&call.worker) {
            return refuse(404, "not_found", format_args!("no worker {:?}", call.worker), None);
        }
        let until =
            tokio::time::Instant::now() + call.deadline.unwrap_or(inner.cfg.deadline).min(inner.cfg.max_deadline);
        let denied = |code, denied: Denied| refuse(429, code, denied, Some(denied.retry_after()));
        if let Err(over) = inner.quotas.take(&call.account, &[(inner.cfg.rate, 1)]) {
            return denied("rate_limited", over);
        }
        // Held until the answer: the worker's requests in hand, over every gateway.
        let _permit = match inner.quotas.acquire(&call.worker, &inner.cfg.concurrency) {
            Ok(permit) => permit,
            Err(over) => return denied("concurrency_limited", over),
        };

        let safe = matches!(call.request.method.as_str(), "GET" | "HEAD" | "OPTIONS");
        let Request { method, url, headers, body } = call.request;
        let invocation = Invocation { worker: call.worker, method, url, headers, sample, body };
        let mut runners = self.order(&invocation.worker);
        let (mut busy, mut again) = (false, false);
        while !runners.is_empty() {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return refuse(504, "timeout", "the deadline passed before a runner answered", None);
            }
            let less_loaded =
                runners.len() > 1 && runners[1].in_flight.load(Relaxed) < runners[0].in_flight.load(Relaxed);
            let runner = runners.remove(usize::from(less_loaded));
            runner.in_flight.fetch_add(1, Relaxed);
            let holding = Holding(&runner.in_flight);
            let told = runner.remote.request_within(Ask::Run(invocation.clone()), left).await;
            drop(holding);
            let error = match told {
                Ok(Told::Ran(ran)) => return ran_response(ran, runner.node),
                Ok(Told::Busy) => {
                    busy = true;
                    continue;
                }
                Ok(Told::NotServed) => continue,
                Ok(Told::Stats(_) | Told::Objects(..)) => {
                    return refuse(502, "bad_gateway", "the runner answered another question", None);
                }
                Err(error) => error,
            };
            match error.retry() {
                Retry::Later => busy = true,
                // Its node went, or started again: the table is behind.
                Retry::Resolve => drop(inner.runners.tell(RunnersMsg::Stale(runner.node))),
                Retry::IfIdempotent if matches!(error, CallError::Timeout) => {
                    return refuse(504, "timeout", format_args!("{} did not answer in time", runner.node), None);
                }
                Retry::IfIdempotent if safe && !again => {
                    again = true;
                    inner.retries.fetch_add(1, Relaxed);
                }
                Retry::IfIdempotent => {
                    let why = format_args!(
                        "{} went quiet with the request ({error}): it may have run, and was not sent again",
                        runner.node
                    );
                    return refuse(502, "outcome_unknown", why, None);
                }
                Retry::Never => return refuse(502, "bad_gateway", format_args!("{}: {error}", runner.node), None),
            }
        }
        match busy {
            true => refuse(503, "busy", "every runner has as many runs of the worker as it takes", second),
            false => refuse(503, "unavailable", "no runner serves the worker now", second),
        }
    }

    pub fn router(&self) -> axum::Router {
        axum::Router::new().fallback(serve).layer(DefaultBodyLimit::max(self.0.cfg.max_body)).with_state(self.clone())
    }
}

fn ran_response(ran: Ran, node: String) -> Response {
    Response {
        status: ran.status,
        headers: ran.headers,
        body: ran.body,
        node: Some(node),
        version: Some(ran.version),
        cpu_us: Some(ran.cpu_us),
        wall_us: Some(ran.wall_us),
        error: ran.failure,
        ..Response::default()
    }
}

/// The worker a request names and the URL it sees: `/w/<name>/rest` on any host, or `<name>.<domain>`.
fn target(host: &str, uri: &Uri, domain: Option<&str>) -> Option<(String, String)> {
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    if let Some(rest) = uri.path().strip_prefix("/w/") {
        let (name, path) = rest.find('/').map_or((rest, "/"), |at| rest.split_at(at));
        return (!name.is_empty()).then(|| (name.to_owned(), format!("http://{host}{path}{query}")));
    }
    let name = host.split(':').next()?.strip_suffix(domain?)?.strip_suffix('.')?;
    (!name.is_empty()).then(|| (name.to_owned(), format!("http://{host}{}{query}", uri.path())))
}

/// What the gateway and the connection own, of a request and of a worker's response.
const OWN: [&str; 4] = ["host", "content-length", "connection", "transfer-encoding"];

async fn serve(
    State(gateway): State<Gateway>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let text = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let host = text("host").unwrap_or("localhost");
    let response = match target(host, &uri, gateway.0.cfg.domain.as_deref()) {
        None => refuse(404, "not_found", "name a worker: /w/<name>/..", None),
        Some((worker, url)) => {
            let kept = headers.iter().filter(|(name, _)| !OWN.contains(&name.as_str()) && *name != "x-deadline-ms");
            let headers = kept.filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())));
            let call = Call {
                // ponytail: the caller's word. A real front authenticates and puts the account here.
                account: text("x-account").unwrap_or("anonymous").to_owned(),
                worker,
                request: Request { method: method.to_string(), url, headers: headers.collect(), body },
                deadline: text("x-deadline-ms").and_then(|ms| ms.parse().ok()).map(Duration::from_millis),
            };
            gateway.call(call).await
        }
    };

    let mut out = axum::response::Response::new(axum::body::Body::from(response.body));
    *out.status_mut() = StatusCode::from_u16(response.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let named = response.headers.iter().filter_map(|(name, value)| {
        Some((HeaderName::try_from(name.as_str()).ok()?, HeaderValue::try_from(value.as_str()).ok()?))
    });
    for (name, value) in named.filter(|(name, _)| !OWN.contains(&name.as_str())) {
        out.headers_mut().append(name, value);
    }
    let retry_after = response.retry_after.map(|after| after.as_secs_f64().ceil().max(1.0).to_string());
    let version = response.version.map(|version| version.to_string());
    let ours = [
        ("x-trace-id", Some(response.trace)),
        ("x-edge-node", response.node),
        ("x-edge-version", version),
        ("x-edge-error", response.error),
        ("retry-after", retry_after),
    ];
    for (name, value) in ours {
        if let Some(value) = value.and_then(|value| HeaderValue::try_from(value).ok()) {
            out.headers_mut().insert(name, value);
        }
    }
    out
}

pub enum RunnersMsg {
    Changed(Option<NodeChange>),
    Found(Vec<(String, Option<RemoteRef<InvokeApi>>)>),
    /// A call found the runner gone or started again.
    Stale(String),
    Again,
}

/// Keeps the gateway's table equal to the view's available runners, each with a reference to its invoker.
/// Level-triggered: every change compares the whole set.
struct Runners {
    cluster: Cluster,
    nodes: Nodes,
    table: Table,
    looking: bool,
    again: bool,
    ready: watch::Sender<usize>,
}

/// How long one runner has to say where its invoker is, and the pause before one that did not is asked again.
const LOOKUP: Duration = Duration::from_secs(1);

impl Runners {
    /// Drops the runners that left, and with `look` asks the new ones for their invokers.
    fn refresh(&mut self, look: bool, cx: &mut Context<'_, Self>) {
        let wanted = self.nodes.ids();
        let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
        let kept: Vec<Runner> = table.iter().filter(|w| wanted.iter().any(|node| **node == *w.node)).cloned().collect();
        let missing: Vec<String> =
            wanted.iter().filter(|node| kept.iter().all(|w| *w.node != ***node)).map(|node| node.to_string()).collect();
        self.ready.send_replace(kept.len());
        *table = Arc::new(kept);
        drop(table);
        if missing.is_empty() || !look {
            return;
        }
        if self.looking {
            self.again = true;
            return;
        }
        let cluster = self.cluster.clone();
        let look = async move {
            let mut lookups = tokio::task::JoinSet::new();
            for node in missing {
                let cluster = cluster.clone();
                lookups.spawn(async move {
                    let found = tokio::time::timeout(LOOKUP, cluster.resolve_on::<InvokeApi>(&node, INVOKE)).await;
                    // One that did not answer, or does not publish its invoker yet, is asked again.
                    (node, found.ok().and_then(Result::ok))
                });
            }
            RunnersMsg::Found(lookups.join_all().await)
        };
        match cx.submit_future(look) {
            Ok(_) => self.looking = true,
            Err(_) => drop(cx.schedule_after(LOOKUP, RunnersMsg::Again)),
        }
    }
}

impl Actor for Runners {
    type Message = RunnersMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        // A runner that restarted under its id is no change here: a call to it says so, and `Stale` drops it.
        cx.attach(self.nodes.changes(), RunnersMsg::Changed).map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    fn on_message(&mut self, msg: RunnersMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let mut look = true;
        match msg {
            // The cluster shut down.
            RunnersMsg::Changed(None) => return Ok(()),
            RunnersMsg::Changed(Some(_)) | RunnersMsg::Again => {}
            RunnersMsg::Found(found) => {
                self.looking = false;
                let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
                let mut next = table.to_vec();
                let missed = found.iter().any(|(_, remote)| remote.is_none());
                for (node, remote) in found.into_iter().filter_map(|(node, remote)| Some((node, remote?))) {
                    next.retain(|runner| runner.node != node);
                    next.push(Runner { node, remote, in_flight: Arc::default() });
                }
                *table = Arc::new(next);
                drop(table);
                // Unless the view changed meanwhile, a runner that did not answer is asked again after a pause.
                if missed && !std::mem::take(&mut self.again) {
                    drop(cx.schedule_after(LOOKUP, RunnersMsg::Again));
                    look = false;
                }
            }
            RunnersMsg::Stale(node) => {
                let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
                let next: Vec<Runner> = table.iter().filter(|runner| runner.node != node).cloned().collect();
                *table = Arc::new(next);
            }
        }
        self.refresh(look, cx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_names_its_worker_by_path_or_by_host() {
        let at = |host: &str, uri: &str, domain| target(host, &uri.parse().unwrap(), domain);
        let named = |name: &str, url: &str| Some((name.to_owned(), url.to_owned()));
        assert_eq!(at("gw:80", "/w/hello/a/b?x=1", None), named("hello", "http://gw:80/a/b?x=1"));
        assert_eq!(at("gw", "/w/hello", None), named("hello", "http://gw/"));
        assert_eq!(at("gw", "/w/", None), None);
        assert_eq!(at("gw", "/other", None), None);
        let domain = Some("edge.test");
        assert_eq!(at("hello.edge.test:8080", "/a?b", domain), named("hello", "http://hello.edge.test:8080/a?b"));
        assert_eq!(at("edge.test", "/a", domain), None);
        assert_eq!(at("helloedge.test", "/a", domain), None);
        // The path wins on a worker's own host.
        assert_eq!(at("hello.edge.test", "/w/other/x", domain), named("other", "http://hello.edge.test/x"));
    }

    #[test]
    fn a_gateway_s_url_is_the_one_given_or_its_listen_address() {
        let listens = "127.0.0.1:7701".parse().unwrap();
        assert_eq!(public_url(None, listens).unwrap(), "http://127.0.0.1:7701");
        assert_eq!(public_url(Some("https://edge.example/"), listens).unwrap(), "https://edge.example");
        assert_eq!(public_url(Some("http://10.0.0.5:8080"), listens).unwrap(), "http://10.0.0.5:8080");
        for bad in ["edge.example", "ftp://edge.example", "http://", "http://a b"] {
            assert!(public_url(Some(bad), listens).is_err(), "{bad}");
        }
        assert_eq!(worker_url("https://edge.example", "hello"), "https://edge.example/w/hello/");
    }
}
