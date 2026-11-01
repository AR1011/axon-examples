//! One node's actors when something under them fails, on real nodes over loopback TCP with the fake engine: a
//! handler that panics, a run the actor cannot start, a cache that cannot be written or read, and a runner that
//! starts again under the control node's reader of its log.

#[path = "support/real.rs"]
mod real;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::files::{Files, MemoryFiles};
use axon::{MailboxConfig, MailboxLimits};
use axon_cluster::MemoryDiscovery;
use axon_testkit::eventually;
use edge_workers::engine::Request;
use edge_workers::engine::fake::{FakeEngine, FakeRun, FakeVersion};
use edge_workers::follower::{self, FollowerConfig};
use edge_workers::gateway::{Call, Response};
use edge_workers::invoker::{self, Invoker, InvokerConfig};
use edge_workers::node::{self, Client, Error, Roles, Settings};
use edge_workers::object::Caller;
use edge_workers::observer::{self, Filter, Observer, ObserverConfig};
use edge_workers::wire::{self, Ask, Invocation, PreviewAsk, Refused, Upload};
use real::{Faulty, Member, Mode};

const ALL: Roles = Roles { control: true, gateway: true, runner: true };

fn engine() -> FakeEngine {
    FakeEngine::new()
        .handler("fine", |_, _| FakeRun::ok("fine"))
        // Under the default CPU limit, and long enough that what is sent right after it finds it running.
        .handler("slow", |_, _| FakeRun::ok("done").cpu(Duration::from_millis(40)))
        .handler("boom", |_, _| panic!("on purpose"))
}

async fn member(
    id: &str,
    roles: Roles,
    discovery: &MemoryDiscovery,
    cache: Arc<dyn Files>,
    settings: Settings,
) -> Member<FakeVersion> {
    let mut stores = real::memory_stores();
    stores.cache = FollowerConfig::new(cache);
    // A failed write is tried again at once, so a test sees more than one.
    stores.cache.retry = Duration::from_millis(20);
    real::member(real::config(id, roles, discovery), roles, stores, engine(), settings).await
}

impl Member<FakeVersion> {
    async fn deploy(&self, name: &str, source: &str) -> u64 {
        let upload = Upload { name: name.into(), source: Some(source.into()), deploy: true, ..Upload::default() };
        let (_, seq) = Client::new(&self.cluster).upload(upload).await.unwrap();
        seq.expect("deployed")
    }

    fn seq(&self, name: &str) -> Option<u64> {
        self.node.serving.as_ref().unwrap().seq(name)
    }

    async fn call(&self, worker: &str) -> Response {
        let call = Call { account: "test".into(), worker: worker.into(), request: Request::get("/"), deadline: None };
        self.node.gateway.as_ref().unwrap().call(call).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_and_a_preview_that_panic_fail_their_own_request_and_the_node_goes_on() {
    // Where the engine's work runs is the actor's jobs, or a runtime of its own as in the binary: both hold a panic.
    for runtime in [None, Some(tokio::runtime::Handle::current())] {
        let mut settings = Settings::default();
        settings.invoker.runtime = runtime.clone();
        let one = member("one", ALL, &MemoryDiscovery::new(), Arc::new(MemoryFiles::new()), settings).await;
        let (fine, boom) = (one.deploy("fine", "fine").await, one.deploy("boom", "boom").await);
        eventually("both served", async || one.seq("fine") == Some(fine) && one.seq("boom") == Some(boom)).await;
        one.node.gateway.as_ref().unwrap().wait_runners(|runners| *runners == 1).await;

        let failed = one.call("boom").await;
        assert_eq!((failed.status, failed.error.as_deref()), (500, Some("engine")), "{runtime:?}: {failed:?}");
        let after = one.call("fine").await;
        assert_eq!((after.status, &after.body[..]), (200, &b"fine"[..]), "{runtime:?}: the runner still runs");

        let ask = PreviewAsk {
            name: "boom".into(),
            source: "boom".into(),
            env: Default::default(),
            method: "GET".into(),
            url: "http://preview/".into(),
            headers: Vec::new(),
            body: String::new(),
        };
        let registry = Client::new(&one.cluster);
        let previewed = registry.call(wire::Call::Preview(ask)).await;
        assert!(matches!(&previewed, Err(Error::Refused(Refused::Load(_)))), "{runtime:?}: {previewed:?}");
        assert_eq!(registry.list().await.unwrap().len(), 2, "{runtime:?}: the registry still answers");
        one.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_the_actor_cannot_start_is_busy_and_holds_nothing() {
    let one = member("one", ALL, &MemoryDiscovery::new(), Arc::new(MemoryFiles::new()), Settings::default()).await;
    one.deploy("slow", "slow").await;
    let seq = one.deploy("fine", "fine").await;
    eventually("both served", async || one.seq("fine") == Some(seq) && one.seq("slow").is_some()).await;

    // A second invoker over the same table, with room for one job: while one run holds it, no other can start.
    let follower = one.system.lookup::<follower::Msg<FakeVersion>>(node::FOLLOWER).expect("the node's follower");
    let cfg = InvokerConfig { per_worker: 2, ..InvokerConfig::default() };
    let serving = one.node.serving.clone().unwrap();
    let caller = Caller::new(&one.cluster, 16);
    let invoker = Invoker::new("probe", engine(), serving, follower, one.node.streams.clone(), caller, cfg);
    let stats = invoker.stats();
    let mailbox = MailboxConfig::from(MailboxLimits { jobs: 1, ..MailboxLimits::default() });
    let invoker = one.system.spawn(invoker).name("probe").mailbox(mailbox).start().unwrap();
    let run = |worker: &str| {
        let invocation = Invocation {
            worker: worker.into(),
            method: "GET".into(),
            url: "/".into(),
            headers: Vec::new(),
            sample: 0,
            body: Default::default(),
        };
        invoker.tell(invoker::Msg::Ask(Ask::Run(invocation), None)).unwrap();
    };
    let counted = |worker: &str| {
        let counted = stats.snapshot("probe").workers.get(worker).cloned().unwrap_or_default();
        (counted.outcomes[0], counted.busy, counted.in_flight)
    };
    // Each once alone, so neither's load is the job in the way below.
    for worker in ["slow", "fine"] {
        run(worker);
        eventually("it ran alone", async || counted(worker) == (1, 0, 0)).await;
    }

    run("slow");
    (0..2).for_each(|_| run("fine"));
    eventually("the slow one ran and the two behind it were refused", async || {
        counted("slow") == (2, 0, 0) && counted("fine") == (1, 2, 0)
    })
    .await;
    // The two refused runs count against nothing: with two at once allowed, the next one runs.
    run("fine");
    eventually("the next one ran", async || counted("fine") == (2, 2, 0)).await;
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_said_to_be_warm_again_is_in_the_table_by_then() {
    let one = member("one", ALL, &MemoryDiscovery::new(), Arc::new(MemoryFiles::new()), Settings::default()).await;
    let seq = one.deploy("fine", "fine").await;
    eventually("served", async || one.seq("fine") == Some(seq)).await;
    let follower = one.system.lookup::<follower::Msg<FakeVersion>>(node::FOLLOWER).expect("the node's follower");
    let serving = one.node.serving.clone().unwrap();
    // The invoker runs its waiting requests from the table as soon as the answer comes: the answer must not come
    // before the table has the worker. Many times, as the answer and the table's change race.
    const ROUNDS: u64 = 300;
    for round in 1..=ROUNDS {
        follower.tell(follower::Msg::Evict("fine".into())).unwrap();
        let back = follower::warm(&follower, "fine", Duration::from_secs(10)).await.unwrap();
        assert!(back.is_some_and(|current| current.is_warm()), "round {round}");
        assert!(serving.get("fine").is_some_and(|current| current.is_warm()), "round {round}: in the table by then");
    }
    assert_eq!((serving.stats.evicted.load(Relaxed), serving.stats.warmed.load(Relaxed)), (ROUNDS, ROUNDS));
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cold_start_slower_than_an_ask_s_timeout_runs_in_the_request_s_time() {
    let cache = Arc::new(Faulty::default());
    let one = member("one", ALL, &MemoryDiscovery::new(), cache.clone(), Settings::default()).await;
    let seq = one.deploy("fine", "fine").await;
    eventually("served", async || one.seq("fine") == Some(seq)).await;
    one.node.gateway.as_ref().unwrap().wait_runners(|runners| *runners == 1).await;
    let follower = one.system.lookup::<follower::Msg<FakeVersion>>(node::FOLLOWER).expect("the node's follower");
    let serving = one.node.serving.clone().unwrap();
    follower.tell(follower::Msg::Evict("fine".into())).unwrap();
    eventually("let go of", async || serving.get("fine").is_some_and(|current| !current.is_warm())).await;

    // The bundle's read back from disk outlasts an ask's 5 s, as a load on a starved CPU can; the request has 10 s.
    let slow = Duration::from_secs(6);
    cache.delay_reads(slow);
    let asked = std::time::Instant::now();
    let back = one.call("fine").await;
    assert_eq!((back.status, &back.body[..]), (200, &b"fine"[..]), "{back:?}");
    assert!(asked.elapsed() >= slow && serving.stats.warmed.load(Relaxed) == 1, "{:?}", asked.elapsed());
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_that_cannot_be_written_leaves_the_last_whole_one_on_disk() {
    let cache = Arc::new(Faulty::default());
    let one = member("one", ALL, &MemoryDiscovery::new(), cache.clone(), Settings::default()).await;
    let stats = one.node.serving.as_ref().unwrap().stats.clone();
    let table = async || cache.read("manifests").await.ok().filter(|table| !table.is_empty());
    let first = one.deploy("fine", "fine").await;
    eventually("the first table on disk", async || stats.saved.load(Relaxed) == 1).await;
    let whole = table().await;
    assert!(whole.is_some());

    cache.set(Mode::Full);
    let second = one.deploy("fine", "slow").await;
    eventually("the second deployment served", async || one.seq("fine") == Some(second)).await;
    let failures = stats.failures.load(Relaxed);
    eventually("the write tried three times more", async || stats.failures.load(Relaxed) >= failures + 3).await;
    assert_eq!(table().await, whole, "the table of deployment {first} is what a restart would find");

    cache.set(Mode::Takes);
    eventually("the second table on disk", async || stats.saved.load(Relaxed) == 2).await;
    assert!(table().await.is_some_and(|now| Some(now) != whole), "the table of deployment {second} replaced it");
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_actor_ended_says_which() {
    let cache = Arc::new(Faulty::unlisted());
    let one = member("one", ALL, &MemoryDiscovery::new(), cache, Settings::default()).await;
    let ended = tokio::time::timeout(Duration::from_secs(10), one.node.ended()).await;
    let (actor, exit) = ended.expect("the follower cannot read its cache, and ends");
    assert_eq!(actor, node::FOLLOWER);
    assert!(exit.reason.is_failure(), "{:?}", exit.reason);
    one.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_log_reader_follows_a_runner_that_starts_again() {
    let discovery = MemoryDiscovery::new();
    let memory = || -> Arc<dyn Files> { Arc::new(MemoryFiles::new()) };
    let front = Roles { control: true, gateway: true, runner: false };
    let runner = Roles { control: false, gateway: false, runner: true };
    let control = member("control", front, &discovery, memory(), Settings::default()).await;
    let cache = memory();
    let first = member("r", runner, &discovery, cache.clone(), Settings::default()).await;
    let seq = control.deploy("fine", "fine").await;
    eventually("served", async || control.seq("fine") == Some(seq) && first.seq("fine") == Some(seq)).await;

    let (observer, _tail) = Observer::new(&control.cluster, control.node.streams.clone(), ObserverConfig::default());
    let observer = control.system.spawn(observer).start().unwrap();
    let traces = async || -> Vec<String> {
        let recent = observer.ask(|reply| observer::Msg::Recent(Filter::default(), reply)).await.unwrap();
        recent.iter().filter_map(|record| record.trace.clone()).collect()
    };
    // Calls are asked again until a runner answers: the gateway finds a runner a moment after it starts.
    let answered = async |wanted: usize| -> BTreeSet<String> {
        let mut answered = BTreeSet::new();
        eventually("the calls answered", async || {
            let response = control.call("fine").await;
            if response.status == 200 {
                answered.insert(response.trace);
            }
            answered.len() == wanted
        })
        .await;
        answered
    };
    let mut expected = answered(3).await;
    eventually("the first runner's records read", async || traces().await.len() == 3).await;

    first.system.shutdown().await.unwrap();
    let second = member("r", runner, &discovery, cache, Settings::default()).await;
    eventually("served from its disk", async || second.seq("fine") == Some(seq)).await;
    expected.extend(answered(2).await);
    eventually("the records of both runs of the runner", async || traces().await.len() >= 5).await;
    let read = traces().await;
    assert_eq!(read.len(), 5, "each record once: {read:?}");
    assert_eq!(read.into_iter().collect::<BTreeSet<_>>(), expected);
    second.system.shutdown().await.unwrap();
    control.system.shutdown().await.unwrap();
}
