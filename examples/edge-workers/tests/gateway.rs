//! The request path on real nodes over loopback TCP, with the real engine: a control node, a gateway behind an
//! HTTP listener, and runners. Each worker is deployed through the registry and called through the gateway.

#[path = "support/real.rs"]
mod real;

use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use axon_cluster::MemoryDiscovery;
use axon_stream::{StartPosition, StreamEvent, SubscribeOptions};
use axon_testkit::{eventually, eventually_within};
use edge_workers::engine::starling::Version;
use edge_workers::gateway::{CONCURRENCY, REQUESTS};
use edge_workers::node::{self, Client, Roles, Settings};
use edge_workers::wire::{LogRecord, OUTCOMES, Upload, WorkerStats, logs};
use real::js;

type Member = real::Member<Version>;

struct Edge {
    control: Member,
    gateway: Member,
    runners: Vec<Member>,
    /// The gateway's HTTP address.
    http: String,
    client: reqwest::Client,
    /// What a member that starts later is given.
    discovery: MemoryDiscovery,
    settings: Settings,
    dir: tempfile::TempDir,
}

async fn member(
    id: &str,
    roles: Roles,
    discovery: &MemoryDiscovery,
    dir: &std::path::Path,
    settings: &Settings,
) -> Member {
    let stores = real::os_stores(&dir.join(id));
    real::member(real::config(id, roles, discovery), roles, stores, real::engine(false), settings.clone()).await
}

impl Edge {
    async fn start(runners: usize, mut settings: Settings) -> Self {
        settings.invoker.runtime = Some(js());
        let dir = tempfile::tempdir().unwrap();
        let discovery = MemoryDiscovery::new();
        let roles = |control, gateway, runner| Roles { control, gateway, runner };
        let control = member("control", roles(true, false, false), &discovery, dir.path(), &settings).await;
        let gateway = member("gw", roles(false, true, false), &discovery, dir.path(), &settings).await;
        let mut started = Vec::new();
        for i in 1..=runners {
            started.push(member(&format!("r{i}"), roles(false, false, true), &discovery, dir.path(), &settings).await);
        }
        let edge = gateway.node.gateway.clone().unwrap();
        tokio::time::timeout(Duration::from_secs(20), edge.wait_runners(|n| *n == runners)).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, edge.router()).await });
        let client = reqwest::Client::new();
        Self { control, gateway, runners: started, http, client, discovery, settings, dir }
    }

    /// Uploads and deploys `source` as `name`, and waits until the gateway routes it and every runner runs it.
    async fn deploy(&self, name: &str, source: &str) -> u32 {
        let upload = Upload { name: name.into(), source: Some(source.into()), deploy: true, ..Upload::default() };
        let (version, seq) = Client::new(&self.gateway.cluster).upload(upload).await.unwrap();
        self.serving(name, seq.unwrap()).await;
        version
    }

    async fn serving(&self, name: &str, seq: u64) {
        // A runner compiles the bundle before it serves it: seconds, with an unoptimised build around the compiler.
        let what = format!("{name} at {seq} on the gateway and every runner");
        eventually_within(&what, Duration::from_secs(90), async || {
            let at = |member: &Member| member.node.serving.as_ref().unwrap().seq(name) == Some(seq);
            at(&self.gateway) && self.runners.iter().all(at)
        })
        .await;
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.get(format!("{}{path}", self.http))
    }

    /// What every runner counted of `worker`, added up.
    fn counted(&self, worker: &str) -> WorkerStats {
        let mut all = WorkerStats::default();
        for runner in &self.runners {
            let stats = runner.node.invoked.as_ref().unwrap().snapshot(&runner.id);
            let Some(one) = stats.workers.get(worker) else { continue };
            (0..OUTCOMES.len()).for_each(|i| all.outcomes[i] += one.outcomes[i]);
            (all.busy, all.cold_starts) = (all.busy + one.busy, all.cold_starts + one.cold_starts);
            all.in_flight += one.in_flight;
        }
        all
    }

    async fn stop(self) {
        for member in self.runners.into_iter().chain([self.gateway, self.control]) {
            member.system.shutdown().await.unwrap();
        }
    }
}

fn header<'a>(response: &'a reqwest::Response, name: &str) -> &'a str {
    response.headers().get(name).map_or("", |value| value.to_str().unwrap())
}

const ECHO: &str = r#"export default { async fetch(request, env) {
    const url = new URL(request.url);
    console.log("echo " + request.method + " " + url.pathname);
    if (url.searchParams.has("throw")) throw new Error("boom");
    console.warn("second line");
    const body = await request.text();
    return new Response(JSON.stringify({ host: url.host, path: url.pathname + url.search, tag: request.headers.get("x-tag"), body }),
        { status: 201, headers: { "content-type": "application/json", "x-from": "echo" } });
} }"#;

/// Waits `wait` milliseconds, off the CPU, then answers `tag`.
fn slow(tag: &str) -> String {
    format!(
        r#"export default {{ async fetch(request) {{
            const wait = Number(new URL(request.url).searchParams.get("wait") ?? 0);
            await new Promise(done => setTimeout(done, wait));
            return new Response("{tag}");
        }} }}"#
    )
}

/// The record of the request with `trace` on `runner`'s log stream, read from the control node.
async fn record(edge: &Edge, runner: &str, trace: &str) -> LogRecord {
    let options = SubscribeOptions { start: StartPosition::First, ..Default::default() };
    let mut subscription = edge.control.node.streams.subscribe(&logs(runner), options).unwrap();
    let find = async {
        loop {
            match subscription.next().await.expect("the log stream goes on") {
                StreamEvent::Batch(batch) => {
                    let mut records = batch.records().iter().filter_map(|record| LogRecord::parse(record));
                    if let Some(found) = records.find(|record| record.trace.as_deref() == Some(trace)) {
                        return found;
                    }
                }
                event => assert!(!event.is_terminal(), "the log stream ended: {event:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), find).await.expect("the record of the request on the log stream")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deployed_worker_answers_through_the_gateway_and_each_call_is_on_its_runners_log() {
    let mut settings = Settings::default();
    settings.gateway.domain = Some("edge.test".into());
    let edge = Edge::start(2, settings).await;
    assert_eq!(edge.deploy("echo", ECHO).await, 1);

    let post = edge.client.post(format!("{}/w/echo/some/path?x=1", edge.http));
    let response = post.header("x-tag", "blue").header("x-account", "acme").body("ping").send().await.unwrap();
    assert_eq!(response.status(), 201, "{response:?}");
    assert_eq!((header(&response, "x-from"), header(&response, "content-type")), ("echo", "application/json"));
    let (trace, node) = (header(&response, "x-trace-id").to_owned(), header(&response, "x-edge-node").to_owned());
    assert!(trace.len() == 16 && trace.bytes().all(|b| b.is_ascii_hexdigit()), "a trace id: {trace:?}");
    assert!(["r1", "r2"].contains(&node.as_str()), "the runner that ran it: {node:?}");
    assert_eq!(header(&response, "x-edge-version"), "1");
    let host = edge.http.trim_start_matches("http://").to_owned();
    let said: serde_json::Value = response.json_or_text().await;
    let expected = serde_json::json!({ "host": host, "path": "/some/path?x=1", "tag": "blue", "body": "ping" });
    assert_eq!(said, expected);

    // The call's record, with its console lines, where the control node reads that runner's stream.
    let logged = record(&edge, &node, &trace).await;
    assert_eq!(
        (logged.worker.as_str(), logged.version, logged.status, logged.outcome.as_str()),
        ("echo", 1, 201, "ok")
    );
    assert_eq!((logged.node.as_str(), logged.method.as_str()), (node.as_str(), "POST"));
    assert!(logged.url.ends_with("/some/path?x=1"), "{}", logged.url);
    assert_eq!(logged.logs, vec!["Log: echo POST /some/path".to_owned(), "Warn: second line".to_owned()]);
    assert!(logged.cpu_us > 0 && logged.wall_us >= logged.cpu_us && !logged.truncated, "{logged:?}");

    // By host, the worker sees the whole path.
    let by_host = edge.get("/w-less/path").header("host", "echo.edge.test").send().await.unwrap();
    assert_eq!(by_host.status(), 201);
    let said: serde_json::Value = by_host.json_or_text().await;
    assert_eq!((said["host"].as_str(), said["path"].as_str()), (Some("echo.edge.test"), Some("/w-less/path")));

    // A worker that throws is a 500 that says so, and its stack is in the record.
    let thrown = edge.get("/w/echo/?throw").send().await.unwrap();
    assert_eq!((thrown.status().as_u16(), header(&thrown, "x-edge-error")), (500, "exception"));
    let (trace, node) = (header(&thrown, "x-trace-id").to_owned(), header(&thrown, "x-edge-node").to_owned());
    let logged = record(&edge, &node, &trace).await;
    assert_eq!((logged.status, logged.outcome.as_str()), (500, "exception"));
    assert!(logged.logs.iter().any(|line| line.contains("boom")), "{:?}", logged.logs);

    let missing = edge.get("/w/nothing/").send().await.unwrap();
    assert_eq!((missing.status().as_u16(), header(&missing, "x-edge-error")), (404, "not_found"));
    assert_eq!(edge.get("/elsewhere").send().await.unwrap().status(), 404);

    // The counters, read from another node through the runners' remote call.
    let (stats, silent) = node::stats(&edge.control.cluster).await;
    assert!(silent.is_empty() && stats.len() == 2, "{silent:?}");
    let echo: Vec<&WorkerStats> = stats.iter().filter_map(|runner| runner.workers.get("echo")).collect();
    let runs = |outcome: &str| -> u64 {
        echo.iter().map(|f| f.outcomes[OUTCOMES.iter().position(|o| *o == outcome).unwrap()]).sum()
    };
    assert_eq!((runs("ok"), runs("exception")), (2, 1));
    assert_eq!(echo.iter().map(|f| f.cpu.count()).sum::<u64>(), 3, "a CPU time per run");
    assert_eq!(stats.iter().map(|runner| runner.logs_published).sum::<u64>(), 3);
    let (results, retries) = edge.gateway.node.gateway.as_ref().unwrap().stats();
    assert_eq!((results[0], results[1], retries), (3, 1, 0), "three ran, one named no worker: {results:?}");
    edge.stop().await;
}

trait Said {
    async fn json_or_text(self) -> serde_json::Value;
}

impl Said for reqwest::Response {
    async fn json_or_text(self) -> serde_json::Value {
        let text = self.text().await.unwrap();
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_account_past_its_rate_is_refused_429_with_retry_after_and_another_account_is_not() {
    let mut settings = Settings::default();
    // Five a second, so the test meets the limit with a few requests.
    settings.gateway.rate = REQUESTS.with_max(5);
    let edge = Edge::start(1, settings).await;
    edge.deploy("fast", &slow("fast")).await;

    let started = Instant::now();
    let (mut admitted, mut refused) = (0, 0);
    for _ in 0..40 {
        let response = edge.get("/w/fast/").header("x-account", "acme").send().await.unwrap();
        match response.status().as_u16() {
            200 => admitted += 1,
            429 => {
                assert_eq!(header(&response, "x-edge-error"), "rate_limited");
                assert!(header(&response, "retry-after").parse::<u64>().unwrap() >= 1, "{response:?}");
                assert!(header(&response, "x-edge-node").is_empty(), "a refused request reaches no runner");
                refused += 1;
            }
            other => panic!("{other}: {response:?}"),
        }
    }
    // Five in each second the 40 requests took, the one they began in included.
    let seconds = started.elapsed().as_secs() + 1;
    assert!(admitted >= 1 && admitted <= 5 * (seconds + 1), "{admitted} admitted in {seconds} s");
    assert_eq!(admitted + refused, 40);
    assert_eq!(edge.counted("fast").outcomes[0], admitted, "only the admitted requests ran");

    // Another account has a rate of its own.
    assert_eq!(edge.get("/w/fast/").header("x-account", "other").send().await.unwrap().status(), 200);
    // And the first account is admitted again once its second has passed.
    eventually("the first account admitted again", async || {
        edge.get("/w/fast/").header("x-account", "acme").send().await.unwrap().status() == 200
    })
    .await;
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_account_and_worker_are_admitted_at_once_through_two_gateways() {
    let edge = Edge::start(2, Settings::default()).await;
    let roles = Roles { control: false, gateway: true, runner: false };
    let second = member("gw2", roles, &edge.discovery, edge.dir.path(), &edge.settings).await;
    let gateway = second.node.gateway.clone().unwrap();
    tokio::time::timeout(Duration::from_secs(20), gateway.wait_runners(|n| *n == 2)).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, gateway.router()).await });
    edge.deploy("fast", &slow("fast")).await;
    eventually("the second gateway routes it", async || second.node.serving.as_ref().unwrap().seq("fast").is_some())
        .await;

    // The quota nodes give each gateway its share of a key some time after it first asks, and none meanwhile: a
    // request waits for it in the gateway. A client that goes from one gateway to the other is not refused.
    for i in 0..12 {
        let base = if i % 2 == 0 { &edge.http } else { &http };
        let response = edge.client.get(format!("{base}/w/fast/")).header("x-account", "new").send().await.unwrap();
        assert_eq!(response.status(), 200, "request {i}: {response:?}");
        tokio::time::sleep(Duration::from_millis(150)).await; // a person's pause between two requests
    }
    second.system.shutdown().await.unwrap();
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_past_its_concurrency_is_refused_429_until_a_request_ends() {
    let mut settings = Settings::default();
    settings.gateway.concurrency = CONCURRENCY.with_max(2);
    let edge = Edge::start(1, settings).await;
    edge.deploy("slow", &slow("done")).await;
    let quotas = edge.gateway.node.quotas.clone().unwrap();

    // Two requests that hold their permits for a while. The worker's key is new to the gateway, which may hold a
    // share of one permit at first: refused for that, a client asks again after `retry-after`, as it would at the limit.
    let held = |edge: &Edge| {
        let request = edge.get("/w/slow/?wait=2500");
        async move {
            for _ in 0..5 {
                let response = request.try_clone().unwrap().send().await.unwrap();
                if response.status() != 429 {
                    return response.status().as_u16();
                }
                tokio::time::sleep(Duration::from_secs(header(&response, "retry-after").parse().unwrap())).await;
            }
            panic!("refused five times")
        }
    };
    let (first, second) = (tokio::spawn(held(&edge)), tokio::spawn(held(&edge)));
    eventually("both requests holding a permit", async || quotas.stats().permits == 2).await;
    for _ in 0..3 {
        let third = edge.get("/w/slow/").send().await.unwrap();
        assert_eq!((third.status().as_u16(), header(&third, "x-edge-error")), (429, "concurrency_limited"));
        assert!(header(&third, "retry-after").parse::<u64>().unwrap() >= 1, "{third:?}");
    }
    assert_eq!((first.await.unwrap(), second.await.unwrap()), (200, 200));
    eventually("the permits given back", async || quotas.stats().permits == 0).await;
    assert_eq!(edge.get("/w/slow/").send().await.unwrap().status(), 200);
    assert_eq!(edge.counted("slow").outcomes[0], 3, "the refused requests reached no runner");
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn when_every_runner_is_full_the_answer_is_503_with_retry_after_at_once() {
    let mut settings = Settings::default();
    // One run of a worker at a time on a runner, so two requests fill the two runners.
    settings.invoker.per_worker = 1;
    let edge = Edge::start(2, settings).await;
    edge.deploy("slow", &slow("done")).await;

    let long = |edge: &Edge| {
        let request = edge.get("/w/slow/?wait=3000");
        async move { request.send().await.unwrap() }
    };
    let (first, second) = (tokio::spawn(long(&edge)), tokio::spawn(long(&edge)));
    // The second goes to the less loaded runner, or finds the first full and is taken by the other.
    eventually("a run on each runner", async || {
        edge.runners.iter().all(|runner| {
            let stats = runner.node.invoked.as_ref().unwrap().snapshot(&runner.id);
            stats.workers.get("slow").is_some_and(|f| f.in_flight == 1)
        })
    })
    .await;

    let asked = Instant::now();
    let third = edge.client.post(format!("{}/w/slow/", edge.http)).send().await.unwrap();
    assert_eq!((third.status().as_u16(), header(&third, "x-edge-error")), (503, "busy"));
    assert_eq!(header(&third, "retry-after"), "1");
    assert!(asked.elapsed() < Duration::from_secs(1), "refused at once, not queued: {:?}", asked.elapsed());

    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!((first.status().as_u16(), second.status().as_u16()), (200, 200));
    assert_ne!(header(&first, "x-edge-node"), header(&second, "x-edge-node"), "one on each runner");
    eventually("the runners free again", async || edge.counted("slow").in_flight == 0).await;
    assert_eq!(edge.get("/w/slow/").send().await.unwrap().status(), 200);
    let counted = edge.counted("slow");
    assert_eq!(counted.outcomes[0], 3, "the refused request ran nowhere");
    assert!(counted.busy >= 2, "the third request was refused by both runners: {}", counted.busy);
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_past_its_deadline_is_a_504_and_its_run_is_cut_there() {
    let edge = Edge::start(1, Settings::default()).await;
    edge.deploy("slow", &slow("done")).await;
    // Once without a deadline of its own, so the next one's time is the worker's and not a first run's.
    assert_eq!(edge.get("/w/slow/").send().await.unwrap().status(), 200);

    let asked = Instant::now();
    let late = edge.get("/w/slow/?wait=20000").header("x-deadline-ms", "400").send().await.unwrap();
    let took = asked.elapsed();
    assert_eq!(late.status(), 504, "{late:?}");
    assert!(["timeout", "wall_timeout"].contains(&header(&late, "x-edge-error")), "{late:?}");
    assert!(took >= Duration::from_millis(400) && took < Duration::from_secs(3), "answered at the deadline: {took:?}");
    // The runner gave the run the deadline as its wall limit: it ended there, not 20 s later.
    let wall_timeout = OUTCOMES.iter().position(|outcome| *outcome == "wall_timeout").unwrap();
    eventually_within("the run cut at the deadline", Duration::from_secs(3), async || {
        let counted = edge.counted("slow");
        counted.in_flight == 0 && counted.outcomes[wall_timeout] == 1
    })
    .await;
    assert_eq!(edge.get("/w/slow/").send().await.unwrap().status(), 200);
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_canary_takes_about_its_share_of_the_requests() {
    let edge = Edge::start(2, Settings::default()).await;
    edge.deploy("split", &slow("one")).await;
    let registry = Client::new(&edge.gateway.cluster);
    let second = Upload { name: "split".into(), source: Some(slow("two")), ..Upload::default() };
    assert_eq!(registry.upload(second).await.unwrap(), (2, None));
    let seq = registry.deploy("split", 1, Some((2, 20))).await.unwrap();
    edge.serving("split", seq).await;

    let (mut canary, total) = (0, 400);
    for _ in 0..total / 16 {
        let batch: Vec<_> = (0..16).map(|_| tokio::spawn(edge.get("/w/split/").send())).collect();
        for response in batch {
            let response = response.await.unwrap().unwrap();
            assert_eq!(response.status(), 200, "{response:?}");
            let version = header(&response, "x-edge-version").to_owned();
            let said = response.text().await.unwrap();
            assert!(matches!((version.as_str(), said.as_str()), ("1", "one") | ("2", "two")), "{version} said {said}");
            canary += usize::from(version == "2");
        }
    }
    // 20 % of 400 is 80, and three standard deviations of that draw are 24.
    assert!((50..=110).contains(&canary), "{canary} of {total} on the canary");
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_worker_leaves_the_engine_and_the_next_request_brings_it_back() {
    let mut settings = Settings::default();
    // Idle after half a second, so the test sees a worker go and come back.
    settings.invoker.idle = Duration::from_millis(500);
    let edge = Edge::start(1, settings).await;
    edge.deploy("hello", &slow("hello")).await;
    let serving = edge.runners[0].node.serving.clone().unwrap();

    eventually("the worker dropped from the engine", async || {
        serving.stats.evicted.load(Relaxed) >= 1 && !serving.get("hello").unwrap().is_warm()
    })
    .await;
    assert_eq!(serving.seq("hello"), Some(1), "it is still deployed");
    // A load from disk on a starved CPU (two cores, as CI has) can outlast the default 10 s, so wait the most.
    let back = edge.get("/w/hello/").header("x-deadline-ms", "30000").send().await.unwrap();
    assert_eq!(back.status(), 200, "{back:?}");
    assert_eq!(back.text().await.unwrap(), "hello");
    assert!(serving.stats.warmed.load(Relaxed) >= 1 && edge.counted("hello").cold_starts >= 1);
    assert_eq!(serving.stats.fetched.load(Relaxed), 1, "loaded again from the node's disk, not fetched again");
    edge.stop().await;
}

/// Answers `bytes` of body under a header of `header` bytes.
const SIZED: &str = r#"export default { async fetch(request) {
    const ask = new URL(request.url).searchParams;
    const size = name => Number(ask.get(name) ?? 0);
    return new Response("x".repeat(size("bytes")), { headers: { "x-pad": "y".repeat(size("header")) } });
} }"#;

#[tokio::test(flavor = "multi_thread")]
async fn what_is_too_large_or_one_too_many_is_refused_by_name_and_the_rest_goes_on() {
    let mut settings = Settings::default();
    // Two requests in hand at once, so two slow ones fill the gateway.
    settings.gateway.max_in_flight = 2;
    let edge = Edge::start(1, settings).await;
    edge.deploy("sized", SIZED).await;
    edge.deploy("slow", &slow("done")).await;

    let fits = edge.get("/w/sized/?bytes=300000&header=1000").send().await.unwrap();
    assert_eq!(fits.status(), 200, "{fits:?}");
    assert_eq!((header(&fits, "x-pad").len(), fits.bytes().await.unwrap().len()), (1000, 300_000));
    // A response goes from the runner to the gateway in one frame: a body or headers that do not fit it are the
    // worker's failure, said by name, and the runner counts it.
    for too_large in ["/w/sized/?bytes=2000000", "/w/sized/?header=70000"] {
        let refused = edge.get(too_large).send().await.unwrap();
        let said = (refused.status().as_u16(), header(&refused, "x-edge-error"));
        assert_eq!(said, (500, "response_too_large"), "{too_large}: {refused:?}");
        assert_eq!(header(&refused, "x-edge-node"), "r1", "{too_large}: the runner answered");
    }
    let too_large = OUTCOMES.iter().position(|outcome| *outcome == "response_too_large").unwrap();
    let counted = edge.counted("sized");
    assert_eq!((counted.outcomes[0], counted.outcomes[too_large], counted.in_flight), (1, 2, 0));

    // A request body past the gateway's limit reaches no runner.
    let body = vec![b'a'; (512 << 10) + 1];
    let heavy = edge.client.post(format!("{}/w/sized/", edge.http)).body(body).send().await.unwrap();
    assert_eq!(heavy.status(), 413, "{heavy:?}");
    assert_eq!(edge.counted("sized").outcomes.iter().sum::<u64>(), 3);

    let long = |edge: &Edge| {
        let request = edge.get("/w/slow/?wait=2000");
        async move { request.send().await.unwrap().status().as_u16() }
    };
    let (first, second) = (tokio::spawn(long(&edge)), tokio::spawn(long(&edge)));
    eventually("both in hand", async || edge.counted("slow").in_flight == 2).await;
    let third = edge.get("/w/sized/").send().await.unwrap();
    assert_eq!((third.status().as_u16(), header(&third, "x-edge-error")), (503, "overloaded"), "{third:?}");
    assert_eq!(header(&third, "retry-after"), "1");
    assert_eq!((first.await.unwrap(), second.await.unwrap()), (200, 200));
    assert_eq!(edge.get("/w/sized/").send().await.unwrap().status(), 200);
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_runner_started_again_serves_from_its_disk_and_fetches_nothing() {
    let mut edge = Edge::start(1, Settings::default()).await;
    edge.deploy("hello", &slow("hello")).await;
    let before = edge.runners.pop().unwrap();
    let stats = before.node.serving.as_ref().unwrap().stats.clone();
    assert_eq!((stats.fetched.load(Relaxed), stats.from_cache.load(Relaxed)), (1, 0));
    eventually("the runner's table on its disk", async || stats.saved.load(Relaxed) == 1).await;
    before.system.shutdown().await.unwrap();
    drop((before, stats));

    let roles = Roles { control: false, gateway: false, runner: true };
    let again = member("r1", roles, &edge.discovery, edge.dir.path(), &edge.settings).await;
    edge.runners.push(again);
    edge.serving("hello", 1).await;
    let stats = &edge.runners[0].node.serving.as_ref().unwrap().stats;
    assert_eq!((stats.fetched.load(Relaxed), stats.from_cache.load(Relaxed)), (0, 1), "the bundle it kept, loaded");
    eventually("a request answered by the runner again", async || {
        let response = edge.get("/w/hello/").send().await.unwrap();
        response.status() == 200 && response.text().await.unwrap() == "hello"
    })
    .await;
    edge.stop().await;
}
