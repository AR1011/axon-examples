//! The HTTP front: admission, deadlines, retries by `err.retry()` within a budget, and the backend
//! list, kept from membership events.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axon_cluster::{CallError, Cluster, RemoteRef, Retry};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::sync::{Notify, Semaphore, watch};

use crate::balancer::{Balancer, BalancerConfig, Outcome};
use crate::service::{Answer, Op, QuoteApi};
use crate::{SERVES, SERVICE};

#[derive(Clone, Copy, Debug)]
pub struct GatewayConfig {
    /// A request's deadline unless it sends `x-deadline-ms`.
    pub deadline: Duration,
    pub max_deadline: Duration,
    /// One attempt's timeout, so a stalled backend leaves time to try another.
    pub try_timeout: Duration,
    /// Requests in progress at once; past it, 429 at once.
    pub max_in_flight: usize,
    pub balancer: BalancerConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            deadline: Duration::from_millis(500),
            max_deadline: Duration::from_secs(5),
            try_timeout: Duration::from_millis(150),
            max_in_flight: 2048,
            balancer: BalancerConfig::default(),
        }
    }
}

/// A finished request: its status and JSON body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcomes {
    pub status: u16,
    pub body: String,
}

impl Outcomes {
    fn error(status: u16, why: &str) -> Self {
        Self { status, body: format!("{{\"error\":\"{why}\"}}") }
    }
}

const STATUSES: [u16; 6] = [200, 201, 429, 502, 503, 504];

struct Inner {
    cfg: GatewayConfig,
    balancer: Mutex<Balancer<RemoteRef<QuoteApi>>>,
    admission: Semaphore,
    epoch: Instant,
    counts: [AtomicU64; STATUSES.len()],
    /// A call found a backend gone: look at the view again.
    stale: Notify,
    backends: watch::Sender<usize>,
}

#[derive(Clone)]
pub struct Gateway(Arc<Inner>);

impl Gateway {
    /// Starts following `cluster`'s membership for backends. The follower, and so the gateway's state, lives
    /// until the cluster shuts down.
    pub fn start(cluster: &Cluster, cfg: GatewayConfig) -> Self {
        let seed = cluster.identity().incarnation as u64;
        let inner = Arc::new(Inner {
            cfg,
            balancer: Mutex::new(Balancer::new(cfg.balancer, seed)),
            admission: Semaphore::new(cfg.max_in_flight),
            epoch: Instant::now(),
            counts: Default::default(),
            stale: Notify::new(),
            backends: watch::Sender::new(0),
        });
        let gateway = Self(inner);
        tokio::spawn(follow(gateway.clone(), cluster.clone()));
        gateway
    }

    /// Waits until the number of routable backends satisfies `ready`.
    pub async fn wait_backends(&self, ready: impl FnMut(&usize) -> bool) {
        let mut count = self.0.backends.subscribe();
        let _ = count.wait_for(ready).await; // cannot fail: the sender lives in `self`
    }

    fn balancer(&self) -> MutexGuard<'_, Balancer<RemoteRef<QuoteApi>>> {
        self.0.balancer.lock().expect("balancer lock") // ponytail: one lock per gateway, shard it if it shows in profiles
    }

    fn clock(&self) -> Duration {
        self.0.epoch.elapsed()
    }

    pub async fn call(&self, op: Op, deadline: Duration) -> Outcomes {
        let outcome = self.attempts(op, deadline).await;
        if let Some(i) = STATUSES.iter().position(|&s| s == outcome.status) {
            self.0.counts[i].fetch_add(1, Relaxed);
        }
        outcome
    }

    async fn attempts(&self, op: Op, deadline: Duration) -> Outcomes {
        let Ok(_permit) = self.0.admission.try_acquire() else {
            return Outcomes::error(429, "gateway at capacity");
        };
        let until = Instant::now() + deadline.min(self.0.cfg.max_deadline);
        let idempotent = matches!(op, Op::Quote(_));
        self.balancer().admit();
        let mut tried = Vec::new();
        loop {
            let Some(left) = until.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()) else {
                return Outcomes::error(504, "deadline");
            };
            let Some((node, target)) = self.balancer().pick(self.clock(), &tried) else {
                return Outcomes::error(503, "no backend");
            };
            let attempt = left.min(self.0.cfg.try_timeout);
            let result = target.request_within(op, attempt).await;
            let (health, next, outcome) = judge(result, idempotent, &node);
            let mut balancer = self.balancer();
            balancer.finish(&node, health, self.clock());
            if next == Next::Resolve && balancer.remove(&node) {
                self.0.stale.notify_one();
            }
            tried.push(node);
            if next == Next::Stop || !balancer.has_other(&tried) || !balancer.retry() {
                return outcome;
            }
        }
    }

    pub fn stats_json(&self) -> String {
        let balancer = self.balancer();
        let s = balancer.stats;
        let counts: Vec<String> =
            STATUSES.iter().zip(&self.0.counts).map(|(s, c)| format!("\"{s}\":{}", c.load(Relaxed))).collect();
        let backends: Vec<String> = balancer
            .backends(self.clock())
            .iter()
            .map(|b| {
                let (node, in_flight, served, ejected) = (&b.node, b.in_flight, b.served, b.ejected);
                format!("{{\"node\":\"{node}\",\"in_flight\":{in_flight},\"served\":{served},\"ejected\":{ejected}}}")
            })
            .collect();
        format!(
            "{{\"status\":{{{}}},\"requests\":{},\"retries\":{},\"retries_denied\":{},\"ejections\":{},\"backends\":[{}]}}",
            counts.join(","),
            s.requests,
            s.retries,
            s.retries_denied,
            s.ejections,
            backends.join(",")
        )
    }

    pub fn router(&self) -> axum::Router {
        axum::Router::new()
            .route("/v1/quote/{sku}", get(quote))
            .route("/v1/orders/{sku}", post(order))
            .route("/stats", get(|State(g): State<Gateway>| async move { g.stats_json() }))
            .with_state(self.clone())
    }

    fn deadline(&self, headers: &HeaderMap) -> Duration {
        let asked = headers.get("x-deadline-ms").and_then(|v| v.to_str().ok()?.parse().ok());
        asked.map_or(self.0.cfg.deadline, Duration::from_millis)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Next {
    Stop,
    /// Another backend may take it.
    Retry,
    /// Another backend may take it, and this one is gone from the view: drop it and look again.
    Resolve,
}

/// The backend's health, what to do next, and the answer if nothing else is tried.
fn judge(result: Result<Answer, CallError>, idempotent: bool, node: &str) -> (Outcome, Next, Outcomes) {
    let maybe = if idempotent { Next::Retry } else { Next::Stop };
    let ok = |status, field, n| Outcomes { status, body: format!("{{\"{field}\":{n},\"backend\":\"{node}\"}}") };
    match result {
        Ok(Answer::Price(n)) => (Outcome::Ok, Next::Stop, ok(200, "price", n)),
        Ok(Answer::Placed(n)) => (Outcome::Ok, Next::Stop, ok(201, "order", n)),
        // Refused before any work: another backend may take it, write or not.
        Ok(Answer::Busy) => (Outcome::Overloaded, Next::Retry, Outcomes::error(503, "backends busy")),
        Err(e) => match e.retry() {
            Retry::Later => (Outcome::Overloaded, Next::Retry, Outcomes::error(503, "backends busy")),
            Retry::Resolve => (Outcome::Failed, Next::Resolve, Outcomes::error(503, "backend gone")),
            Retry::IfIdempotent if matches!(e, CallError::Timeout) => {
                (Outcome::Failed, maybe, Outcomes::error(504, "backend timeout"))
            }
            Retry::IfIdempotent => (Outcome::Failed, maybe, Outcomes::error(502, "outcome unknown")),
            Retry::Never => (Outcome::Ok, Next::Stop, Outcomes::error(502, "backend refused")),
        },
    }
}

fn respond(outcome: Outcomes) -> Response {
    let status = StatusCode::from_u16(outcome.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, [(header::CONTENT_TYPE, "application/json")], outcome.body).into_response();
    if matches!(outcome.status, 429 | 503) {
        response.headers_mut().insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
    }
    response
}

async fn quote(State(g): State<Gateway>, Path(sku): Path<u64>, headers: HeaderMap) -> Response {
    respond(g.call(Op::Quote(sku), g.deadline(&headers)).await)
}

async fn order(State(g): State<Gateway>, Path(sku): Path<u64>, headers: HeaderMap) -> Response {
    respond(g.call(Op::Order(sku), g.deadline(&headers)).await)
}

/// Keeps the balancer's backends equal to the view's available members that serve the service.
/// Level-triggered: every event, and every stale call, compares the whole view.
async fn follow(gateway: Gateway, cluster: Cluster) {
    let mut events = cluster.subscribe();
    let mut again: Option<tokio::time::Instant> = None;
    loop {
        let pause = async {
            match again {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            event = events.next() => if event.is_none() { return },
            () = gateway.0.stale.notified() => {}
            () = pause => {}
        }
        // A lookup that failed (the node answers slowly, or has not published yet) is tried again in 1 s.
        again = (!reconcile(&gateway, &cluster).await).then(|| tokio::time::Instant::now() + Duration::from_secs(1));
    }
}

/// Returns false if some backend could not be looked up. Logs each change to stderr.
async fn reconcile(gateway: &Gateway, cluster: &Cluster) -> bool {
    let view = cluster.view();
    let wanted: Vec<(String, u128)> = view
        .members
        .iter()
        .filter(|m| m.available() && m.record.metadata.get(SERVES).is_some_and(|s| s == SERVICE))
        .map(|m| (m.record.id.clone(), m.record.incarnation))
        .collect();
    let t = gateway.clock().as_secs_f64();
    gateway.balancer().retain(|node, target| {
        let keep = wanted.iter().any(|(n, i)| n == node && *i == target.node_incarnation());
        if !keep {
            eprintln!("{t:.1}s backend {node} removed");
        }
        keep
    });
    let mut complete = true;
    for (node, _) in &wanted {
        if gateway.balancer().contains(node) {
            continue;
        }
        // ponytail: one lookup at a time, bounded by a timeout; run them side by side if backends number in hundreds.
        match tokio::time::timeout(Duration::from_secs(1), cluster.resolve_on::<QuoteApi>(node, SERVICE)).await {
            Ok(Ok(target)) => {
                gateway.balancer().insert(node, target);
                eprintln!("{t:.1}s backend {node} added");
            }
            Ok(Err(e)) => {
                eprintln!("{t:.1}s backend {node} lookup failed: {e}"); // retried by the caller
                complete = false;
            }
            Err(_) => {
                eprintln!("{t:.1}s backend {node} lookup timed out");
                complete = false;
            }
        }
    }
    let count = gateway.balancer().len();
    gateway.0.backends.send_replace(count);
    complete
}
