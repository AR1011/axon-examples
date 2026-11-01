//! A gateway and backends on one process, over real sessions, with `MemoryDiscovery`.

use std::time::{Duration, Instant};

use api_gateway::gateway::{Gateway, GatewayConfig, Outcomes};
use api_gateway::service::{Answer, Op, QuoteApi, Service, ServiceConfig};
use api_gateway::{SERVES, SERVICE, transport};
use axon::{ActorSystem, MailboxLimits};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};

async fn node(id: &str, discovery: &MemoryDiscovery, limits: MailboxLimits) -> (ActorSystem, Cluster) {
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport(id), discovery.clone());
    let system = ActorSystem::builder().mailbox(limits).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

async fn backend(id: &str, discovery: &MemoryDiscovery, cfg: ServiceConfig, limits: MailboxLimits) -> ActorSystem {
    let (system, cluster) = node(id, discovery, limits).await;
    let service = system.spawn(Service::new(cfg)).start().unwrap();
    cluster.publish::<QuoteApi>(&service, SERVICE).unwrap();
    cluster.set_metadata(SERVES, Some(SERVICE)).await.unwrap();
    system
}

async fn gateway(discovery: &MemoryDiscovery, cfg: GatewayConfig, backends: usize) -> (ActorSystem, Cluster, Gateway) {
    let (system, cluster) = node("gw", discovery, MailboxLimits::default()).await;
    let gateway = Gateway::start(&cluster, cfg);
    within(Duration::from_secs(10), gateway.wait_backends(|&n| n >= backends)).await;
    (system, cluster, gateway)
}

/// Nothing is left counted in flight on the gateway's node once its callers have returned or given up.
fn settled(cluster: &Cluster) {
    let e = cluster.stats().engine;
    eprintln!(
        "calls {} succeeded {} rejected {} timed_out {} unknown {} failed {} pending {}",
        e.calls, e.succeeded, e.rejected, e.timed_out, e.unknown_outcomes, e.failed, e.pending
    );
    assert_eq!(e.pending, 0, "calls given up on still count as pending");
}

async fn within<T>(limit: Duration, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(limit, f).await.expect("in time")
}

/// `n` calls, `lanes` at a time; each with how long it took.
async fn run(
    gateway: &Gateway,
    n: usize,
    lanes: usize,
    op: fn(u64) -> Op,
    deadline: Duration,
) -> Vec<(Outcomes, Duration)> {
    let mut lanes_set = tokio::task::JoinSet::new();
    for lane in 0..lanes {
        let gateway = gateway.clone();
        lanes_set.spawn(async move {
            let mut done = Vec::new();
            for i in (lane..n).step_by(lanes) {
                let started = Instant::now();
                let outcome = gateway.call(op(i as u64), deadline).await;
                done.push((outcome, started.elapsed()));
            }
            done
        });
    }
    let mut all = Vec::new();
    while let Some(done) = lanes_set.join_next().await {
        all.extend(done.unwrap());
    }
    all
}

fn count(results: &[(Outcomes, Duration)], status: u16) -> usize {
    results.iter().filter(|(o, _)| o.status == status).count()
}

fn slowest(results: &[(Outcomes, Duration)]) -> Duration {
    results.iter().map(|(_, took)| *took).max().unwrap_or_default()
}

fn stat(gateway: &Gateway, key: &str) -> u64 {
    let json = gateway.stats_json();
    let at = json.find(&format!("\"{key}\":")).expect("stat") + key.len() + 3;
    json[at..].split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
}

const SLACK: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_around_a_slow_backend() {
    let discovery = MemoryDiscovery::new();
    let fast = ServiceConfig::default();
    let slow = ServiceConfig { delay: Duration::from_millis(400), ..fast };
    let _b1 = backend("b1", &discovery, fast, MailboxLimits::default()).await;
    let _b2 = backend("b2", &discovery, slow, MailboxLimits::default()).await;
    let cfg = GatewayConfig { try_timeout: Duration::from_millis(100), ..Default::default() };
    let (_system, gw, gateway) = gateway(&discovery, cfg, 2).await;

    let deadline = Duration::from_millis(300);
    let results = run(&gateway, 400, 4, Op::Quote, deadline).await;
    assert!(slowest(&results) < deadline + SLACK, "slowest {:?}", slowest(&results));
    assert!(count(&results, 200) >= 390, "{} of 400 answered", count(&results, 200));
    assert!(stat(&gateway, "ejections") >= 1);
    let late = &results.iter().filter(|(o, _)| o.body.contains("\"b2\"")).count();
    assert_eq!(*late, 0, "a 400 ms backend never answers inside a 100 ms attempt");
    assert!(stat(&gateway, "retries") <= 10 + 400 / 5);
    settled(&gw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_max_in_flight_is_429_at_once() {
    let discovery = MemoryDiscovery::new();
    // Each admitted call holds its slot for 100 ms (inside the 150 ms attempt), so the burst finds them all taken.
    let slow = ServiceConfig { delay: Duration::from_millis(100), ..Default::default() };
    let _b1 = backend("b1", &discovery, slow, MailboxLimits::default()).await;
    let cfg = GatewayConfig { max_in_flight: 16, ..Default::default() };
    let (_system, gw, gateway) = gateway(&discovery, cfg, 1).await;

    let results = run(&gateway, 64, 64, Op::Quote, Duration::from_millis(500)).await;
    assert_eq!((count(&results, 200), count(&results, 429)), (16, 48));
    let refused = results.iter().filter(|(o, _)| o.status == 429).map(|(_, took)| *took).max().unwrap();
    assert!(refused < Duration::from_millis(50), "a 429 waits for nothing: {refused:?}");
    settled(&gw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_backend_mailbox_is_503_and_retries_stay_in_the_budget() {
    let discovery = MemoryDiscovery::new();
    // A few milliseconds of work per call in a debug build and room for four calls: most of a burst is refused
    // `Full`, and a full mailbox still drains well inside `try_timeout` on a loaded machine, so no attempt times out.
    let heavy = ServiceConfig { work: 20_000, ..Default::default() };
    let tiny = MailboxLimits { messages: 4, ..Default::default() };
    let _b1 = backend("b1", &discovery, heavy, tiny).await;
    let _b2 = backend("b2", &discovery, heavy, tiny).await;
    let (_system, gw, gateway) = gateway(&discovery, GatewayConfig::default(), 2).await;

    let deadline = Duration::from_millis(500);
    let results = run(&gateway, 512, 256, Op::Quote, deadline).await;
    assert!(slowest(&results) < deadline + SLACK, "slowest {:?}", slowest(&results));
    let (ok, full) = (count(&results, 200), count(&results, 503));
    let others: Vec<_> = results.iter().filter(|(o, _)| !matches!(o.status, 200 | 503 | 504)).collect();
    assert!(full > 0 && ok > 0, "{ok} answered, {full} refused");
    assert!(others.is_empty(), "only 200, 503, and 504 for an attempt stuck behind the work: {others:?}");
    let (requests, retries) = (stat(&gateway, "requests"), stat(&gateway, "retries"));
    assert!(retries as f64 <= 10.0 + 0.2 * requests as f64, "{retries} retries for {requests} requests");
    assert!(stat(&gateway, "retries_denied") > 0, "the budget ran out rather than retrying everything");

    // Nothing stayed counted in flight or ejected; the backends may still be working off calls their callers gave up on.
    let json = gateway.stats_json();
    assert_eq!(json.matches("\"in_flight\":0,").count(), 2, "{json}");
    assert_eq!(stat(&gateway, "ejections"), 0, "busy is not broken: {json}");
    settled(&gw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backend_that_leaves_is_dropped_and_one_that_joins_is_used() {
    let discovery = MemoryDiscovery::new();
    let cfg = ServiceConfig::default();
    let _b1 = backend("b1", &discovery, cfg, MailboxLimits::default()).await;
    let b2 = backend("b2", &discovery, cfg, MailboxLimits::default()).await;
    let (_system, _gw, gateway) = gateway(&discovery, GatewayConfig::default(), 2).await;

    let deadline = Duration::from_millis(500);
    let load = {
        let gateway = gateway.clone();
        tokio::spawn(async move { run(&gateway, 4000, 8, Op::Quote, deadline).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await; // let the load get going before b2 leaves
    b2.shutdown().await.unwrap();
    let results = within(Duration::from_secs(30), load).await.unwrap();
    assert!(slowest(&results) < deadline + SLACK);
    assert!(count(&results, 200) >= 3990, "{} of 4000", count(&results, 200));
    within(Duration::from_secs(10), gateway.wait_backends(|&n| n == 1)).await; // b2's Left event removed it

    let _b3 = backend("b3", &discovery, cfg, MailboxLimits::default()).await;
    within(Duration::from_secs(10), gateway.wait_backends(|&n| n == 2)).await;
    let results = run(&gateway, 200, 4, Op::Quote, deadline).await;
    assert_eq!(count(&results, 200), 200);
    assert!(results.iter().any(|(o, _)| o.body.contains("\"b3\"")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_are_not_retried_after_a_timeout_reads_are() {
    let discovery = MemoryDiscovery::new();
    let slow = ServiceConfig { delay: Duration::from_millis(300), ..Default::default() };
    let _b1 = backend("b1", &discovery, slow, MailboxLimits::default()).await;
    let _b2 = backend("b2", &discovery, slow, MailboxLimits::default()).await;
    let cfg = GatewayConfig { try_timeout: Duration::from_millis(100), ..Default::default() };
    let (_system, _gw, gateway) = gateway(&discovery, cfg, 2).await;

    let order = gateway.call(Op::Order(7), Duration::from_secs(1)).await;
    assert_eq!(order.status, 504, "{order:?}");
    assert_eq!(stat(&gateway, "retries"), 0, "a write that may have been placed is not sent again");

    let quote = gateway.call(Op::Quote(7), Duration::from_secs(1)).await;
    assert_eq!(quote.status, 504, "{quote:?}");
    assert_eq!(stat(&gateway, "retries"), 1, "a read is tried once on the other backend, then there is none");
}

/// A call that waited in the mailbox past its caller's deadline is dropped unworked: an order given up on is never
/// placed. The orders wait behind quotes that each hash for milliseconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_order_whose_caller_gave_up_in_the_mailbox_is_not_placed() {
    let discovery = MemoryDiscovery::new();
    let heavy = ServiceConfig { work: 500_000, ..Default::default() };
    let _b1 = backend("b1", &discovery, heavy, MailboxLimits::default()).await;
    let (_system, cluster) = node("client", &discovery, MailboxLimits::default()).await;
    let service = within(Duration::from_secs(10), async {
        loop {
            match cluster.resolve::<QuoteApi>(SERVICE).await {
                Ok(service) => break service,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;

    for sku in 0..40 {
        let service = service.clone();
        tokio::spawn(async move { service.request(Op::Quote(sku)).await });
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    for sku in 0..5 {
        let late = service.request_within(Op::Order(sku), Duration::from_millis(10)).await;
        assert!(late.is_err(), "an order answered in time behind the quotes: {late:?}");
    }
    // Behind the quotes and the orders in the mailbox: its number says how many orders were placed before it.
    let placed = service.request(Op::Order(9)).await.unwrap();
    assert_eq!(placed, Answer::Placed(1 << 20 | 9), "the orders given up on were placed too");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maps_outcomes_to_statuses() {
    let discovery = MemoryDiscovery::new();
    let (_system, _gw, gateway) = gateway(&discovery, GatewayConfig::default(), 0).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(axum::serve(listener, gateway.router()).into_future());
    let client = reqwest::Client::new();

    let none = client.get(format!("{url}/v1/quote/1")).send().await.unwrap();
    assert_eq!(none.status(), 503);
    assert_eq!(none.headers()["retry-after"], "1");

    let _b1 = backend("b1", &discovery, ServiceConfig::default(), MailboxLimits::default()).await;
    within(Duration::from_secs(10), gateway.wait_backends(|&n| n == 1)).await;
    let quote = client.get(format!("{url}/v1/quote/1")).header("x-deadline-ms", "200").send().await.unwrap();
    assert_eq!(quote.status(), 200);
    assert!(quote.text().await.unwrap().contains("\"backend\":\"b1\""));
    let order = client.post(format!("{url}/v1/orders/1")).send().await.unwrap();
    assert_eq!(order.status(), 201);
    let stats = client.get(format!("{url}/stats")).send().await.unwrap().text().await.unwrap();
    assert!(stats.contains("\"200\":1") && stats.contains("\"201\":1") && stats.contains("\"503\":1"), "{stats}");
}
