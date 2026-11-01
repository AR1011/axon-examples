//! The fault story on the World: a control node, a gateway and three runners as real nodes on virtual time, with the
//! fake engine, under callers that send a request, wait for its answer and send the next. A runner is killed, a
//! runner is cut off from the control node across a deploy, and the control node is killed and started again. A
//! seed picks who, when, how, and for how long.

#[path = "support/world.rs"]
mod on_world;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon_cluster::MemoryDiscovery;
use axon_testkit::world::{Crash, Host, Task, World};
use axon_testkit::{check_seeds, seeds};
use edge_workers::engine::fake::{FakeEngine, FakeRun, FakeVersion};
use edge_workers::engine::{Env, Limits, Request};
use edge_workers::gateway::{Call, Response};
use edge_workers::node::{self, Client, Error, Roles};
use edge_workers::wire::Upload;
use on_world::{Node, Seen, serves};

const CONTROL: &str = "control";
const GATEWAY: &str = "gw";
const RUNNERS: [&str; 3] = ["r-1", "r-2", "r-3"];
const WITHIN: Duration = Duration::from_secs(60);
/// A request's deadline, the gateway's default: twice what a session takes to give up on a machine gone silent.
const DEADLINE: Duration = Duration::from_secs(10);
/// The deadline of a request for `slow`, which runs four times as long.
const SHORT: Duration = Duration::from_millis(100);
/// A step of the World and a timer's tick, which an answer at its deadline may be late by.
const SLACK: Duration = Duration::from_millis(3);
/// A dead process's reset is at the gateway within this: the wire's latency and jitter, and a step each side.
const RESET: Duration = Duration::from_millis(5);
/// The gateway stops sending to a machine that went silent within this.
const GIVES_UP: Duration = Duration::from_secs(8);
/// After a heal, a runner serves what was deployed while it was cut off within this.
const CATCH_UP: Duration = Duration::from_secs(5);
/// A deploy while the control node is down is refused within this.
const REFUSED: Duration = Duration::from_secs(10);

/// `one`, `two` and `three` answer their name after 20 ms, so a crash finds some in hand; `slow` takes 400 ms.
fn engine() -> FakeEngine {
    let work = |name: &'static str| move |_: &Request, _: &Env| FakeRun::ok(name).cpu(Duration::from_millis(20));
    let slow = |_: &Request, _: &Env| FakeRun::ok("late").cpu(Duration::from_millis(400));
    FakeEngine::new()
        .handler("one", work("one"))
        .handler("two", work("two"))
        .handler("three", work("three"))
        .handler("slow", slow)
}

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let name = host.name();
    let roles = Roles { control: name == CONTROL, gateway: name == GATEWAY, runner: name.starts_with("r-") };
    on_world::boot(host, discovery, roles, engine(), |_, _, _| {}).await
}

fn upload(name: &str, source: &str) -> Upload {
    Upload { name: name.into(), source: Some(source.into()), deploy: true, ..Upload::default() }
}

fn deploy(w: &mut World<Node>, from: &str, upload: Upload) -> Result<(u32, Option<u64>), Error> {
    w.block_on(from, |n| async move { Client::new(&n.cluster).upload(upload).await }).expect("the caller is up")
}

fn node<'a>(w: &'a World<Node>, name: &str) -> &'a node::Node<FakeVersion> {
    &w.nodes().find(|(node, _)| *node == name).unwrap_or_else(|| panic!("{name} is up")).1.node
}

/// The runs of `worker` that ended well on `runner` since it started.
fn ran(w: &World<Node>, runner: &str, worker: &str) -> u64 {
    let stats = node(w, runner).invoked.as_ref().expect("a runner").snapshot(runner);
    stats.workers.get(worker).map_or(0, |f| f.outcomes[0])
}

fn gateway_sees(w: &mut World<Node>, runners: usize) {
    let seen = w.spawn_on(GATEWAY, move |n| async move {
        n.node.gateway.as_ref().unwrap().wait_runners(|ready| *ready == runners).await;
    });
    w.join(seen).expect("the gateway is up");
}

/// The five nodes, `work` deployed from source `one` and `slow` with room to run its 400 ms, both served everywhere.
fn world(seed: u64) -> World<Node> {
    let names: Vec<_> = [CONTROL, GATEWAY].into_iter().chain(RUNNERS).collect();
    let mut w = on_world::booted(seed, &names, boot);
    assert_eq!(deploy(&mut w, GATEWAY, upload("work", "one")).unwrap(), (1, Some(1)));
    let limits = Limits { cpu: Duration::from_secs(1), ..Limits::default() };
    let slow = Upload { limits: Some(limits), ..upload("slow", "slow") };
    assert_eq!(deploy(&mut w, GATEWAY, slow).unwrap(), (1, Some(2)));
    w.run_until("both workers served on the gateway and every runner", WITHIN, |w| {
        [GATEWAY].into_iter().chain(RUNNERS).all(|n| serves(w, n, "work") == Some(1) && serves(w, n, "slow") == Some(2))
    });
    gateway_sees(&mut w, 3);
    w
}

/// What the test and its callers share: the test moves `phase` on at each fault, the callers count what they send.
#[derive(Default)]
struct Load {
    stop: AtomicBool,
    phase: AtomicU64,
    sent: AtomicU64,
}

#[derive(Debug)]
struct Sample {
    /// Its place in the order the gateway took the requests in, and which caller's it is.
    order: u64,
    caller: usize,
    /// On the gateway's clock.
    began: tokio::time::Instant,
    worker: &'static str,
    get: bool,
    /// The phase it was sent in and the phase it was answered in.
    sent: u64,
    answered: u64,
    took: Duration,
    deadline: Duration,
    response: Response,
}

/// One request after the other until told to stop, 2 ms apart: of `work` a GET then a POST and so on, every other
/// caller the POST first; of `slow` GETs with a deadline it cannot meet.
async fn caller(node: Node, load: Arc<Load>, id: usize, worker: &'static str) -> Vec<Sample> {
    let gateway = node.node.gateway.expect("a gateway");
    let deadline = if worker == "slow" { SHORT } else { DEADLINE };
    let mut samples = Vec::new();
    while !load.stop.load(Relaxed) {
        let get = worker == "slow" || (samples.len() + id).is_multiple_of(2);
        let mut request = Request::get("/");
        if !get {
            request.method = "POST".into();
        }
        let account = format!("account-{id}");
        let call = Call { account, worker: worker.into(), request, deadline: Some(deadline) };
        let (order, sent) = (load.sent.fetch_add(1, Relaxed), load.phase.load(Relaxed));
        let began = tokio::time::Instant::now();
        let response = gateway.call(call).await;
        let took = began.elapsed();
        let answered = load.phase.load(Relaxed);
        samples.push(Sample { order, caller: id, began, worker, get, sent, answered, took, deadline, response });
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    samples
}

struct Running {
    load: Arc<Load>,
    callers: Vec<Task<Vec<Sample>>>,
}

fn start_load(w: &mut World<Node>, work: usize, slow: usize) -> Running {
    let load = Arc::new(Load::default());
    let workers = std::iter::repeat_n("work", work).chain(std::iter::repeat_n("slow", slow));
    let callers = workers.enumerate().map(|(id, worker)| {
        let load = load.clone();
        w.spawn_on(GATEWAY, move |n| caller(n, load, id, worker))
    });
    Running { callers: callers.collect(), load }
}

/// Stops the callers and gives what each request came to, in the order the gateway took them. Every request has
/// its answer: a caller that never got one would not end.
fn end_load(w: &mut World<Node>, running: Running) -> Vec<Sample> {
    running.load.stop.store(true, Relaxed);
    let mut samples = Vec::new();
    for caller in running.callers {
        samples.extend(w.join(caller).expect("the gateway is up"));
    }
    samples.sort_by_key(|sample| sample.order);
    samples
}

/// No answer came later than its request's deadline, and nothing is held for a request that has its answer: no
/// permit at the gateway, no run on a runner.
fn nothing_outlived_its_deadline_and_nothing_is_held(w: &mut World<Node>, seed: u64, samples: &[Sample]) {
    for sample in samples {
        assert!(sample.took <= sample.deadline + SLACK, "seed {seed}: past its deadline: {sample:?}");
    }
    assert_eq!(node(w, GATEWAY).quotas.as_ref().unwrap().stats().permits, 0, "seed {seed}: a permit is held");
    // A run the gateway gave up on at its deadline ends at that deadline on its runner, a moment after.
    w.run_until("no run left on a runner", Duration::from_millis(50), |w| {
        w.nodes()
            .filter_map(|(name, n)| Some(n.node.invoked.as_ref()?.snapshot(name)))
            .all(|stats| stats.workers.values().all(|worker| worker.in_flight == 0))
    });
}

fn ok(seed: u64, sample: &Sample, body: &str) {
    let response = &sample.response;
    assert_eq!((response.status, response.error.as_deref()), (200, None), "seed {seed}: {sample:?}");
    assert_eq!(&response.body[..], body.as_bytes(), "seed {seed}: {sample:?}");
}

fn a_runner_killed_under_load(seed: u64) {
    let mut w = world(seed);
    let running = start_load(&mut w, 8, 2);
    let warm = Duration::from_millis(200 + w.pick(0..300));
    w.run_for(warm);

    // One of the runners the worker's requests go to.
    let active: Vec<&str> = RUNNERS.into_iter().filter(|runner| ran(&w, runner, "work") > 0).collect();
    let victim = active[w.pick(0..active.len() as u64) as usize];
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    let crashed = w.read(GATEWAY, |_| tokio::time::Instant::now()).expect("the gateway is up");
    running.load.phase.store(1, Relaxed);
    w.log(format!("crash {victim} {how:?}"));
    w.crash(victim, how);
    let at = w.now();
    w.run_until("the gateway sending to the two survivors only", GIVES_UP, |w| {
        let gateway = node(w, GATEWAY).gateway.as_ref().unwrap();
        gateway.runners() == 2
    });
    let gave_up = w.now() - at;
    running.load.phase.store(2, Relaxed);
    let on = Duration::from_millis(500 + w.pick(0..1500));
    w.run_for(on);
    let samples = end_load(&mut w, running);

    let (mut unknown, mut cut, mut after) = (BTreeSet::new(), 0, BTreeSet::new());
    for sample in &samples {
        let response = &sample.response;
        match (sample.worker, response.status, response.error.as_deref()) {
            ("work", 200, None) => ok(seed, sample, "one"),
            // The crash took a POST with it: it may have run, so it was not sent again. A GET is never among them.
            ("work", 502, Some("outcome_unknown")) if !sample.get => {
                // A process that died resets its connections: only what it had in hand is lost. A machine that
                // went silent takes what is sent to it until the gateway gives up on it.
                match how {
                    Crash::Process => assert!(sample.began <= crashed + RESET, "seed {seed}: sent late: {sample:?}"),
                    Crash::Machine => assert!(sample.sent < 2, "seed {seed}: sent late: {sample:?}"),
                }
                assert!(unknown.insert(sample.caller), "seed {seed}: a caller lost two requests: {sample:?}");
            }
            ("slow", 504, Some("timeout" | "wall_timeout")) => cut += 1,
            _ => panic!("seed {seed}, {how:?}: {sample:?}"),
        }
        if sample.sent == 2 {
            after.extend(response.node.clone());
        }
    }
    assert!(cut > 0, "seed {seed}: no request of the slow worker");
    assert_eq!(after.len(), 2, "seed {seed}: the two survivors served after the crash: {after:?}");
    let (_, retries) = node(&w, GATEWAY).gateway.as_ref().unwrap().stats();
    nothing_outlived_its_deadline_and_nothing_is_held(&mut w, seed, &samples);
    // The slow worker's runs were cut at their callers' deadline: none ran its 400 ms out on a survivor.
    for runner in RUNNERS.into_iter().filter(|runner| *runner != victim) {
        assert_eq!(ran(&w, runner, "slow"), 0, "seed {seed}: {runner} ran a request past its deadline");
    }

    // The runner comes back with what it had on disk, and takes requests again.
    w.restart(victim);
    gateway_sees(&mut w, 3);
    let running = start_load(&mut w, 8, 0);
    w.run_until("the restarted runner runs requests again", WITHIN, |w| ran(w, victim, "work") > 0);
    let again = end_load(&mut w, running);
    again.iter().for_each(|sample| ok(seed, sample, "one"));
    nothing_outlived_its_deadline_and_nothing_is_held(&mut w, seed, &again);
    let slowest = samples.iter().filter(|s| s.worker == "work").map(|s| s.took).max().unwrap_or_default();
    println!(
        "world digest kill seed {seed}: {} {how:?} requests={} unknown_posts={} gets_sent_again={retries} \
         slowest_ms={} gave_up_ms={}",
        w.digest(),
        samples.len(),
        unknown.len(),
        slowest.as_millis(),
        gave_up.as_millis()
    );
}

fn a_deploy_while_a_runner_is_cut_off_from_control(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    let running = start_load(&mut w, 4, 0);
    let warm = Duration::from_millis(200 + w.pick(0..300));
    w.run_for(warm);

    // Cut off from the control node only: the gateway still sends it requests.
    let active: Vec<&str> = RUNNERS.into_iter().filter(|runner| ran(&w, runner, "work") > 0).collect();
    let cut = active[w.pick(0..active.len() as u64) as usize];
    let reached: Vec<&str> = [GATEWAY].into_iter().chain(RUNNERS).filter(|n| *n != cut).collect();
    w.partition(&[cut], &[CONTROL]);
    running.load.phase.store(1, Relaxed);
    let mut last = (2u32, 3u64, "two");
    assert_eq!(deploy(&mut w, GATEWAY, upload("work", "two")).unwrap(), (2, Some(3)));
    // In half the seeds a second deploy lands while it is away.
    if w.pick(0..2) == 0 {
        assert_eq!(deploy(&mut w, GATEWAY, upload("work", "three")).unwrap(), (3, Some(4)));
        last = (3, 4, "three");
    }
    w.run_until("the nodes that can hear of it serving the deploy", WITHIN, |w| {
        seen.look(w);
        reached.iter().all(|n| serves(w, n, "work") == Some(last.1))
    });
    running.load.phase.store(2, Relaxed);
    // Shorter than a session lasts without an answer in some seeds, longer in most.
    let apart = Duration::from_millis(500 + w.pick(0..12_000));
    w.run_for(apart);
    assert_eq!(serves(&w, cut, "work"), Some(1), "seed {seed}: {cut} is cut off");

    running.load.phase.store(3, Relaxed);
    w.heal();
    let healed = w.now();
    w.run_until("the runner that was cut off serving the deploy", CATCH_UP, |w| {
        seen.look(w);
        serves(w, cut, "work") == Some(last.1)
    });
    let caught_up = w.now() - healed;
    running.load.phase.store(4, Relaxed);
    w.run_for(Duration::from_millis(300));
    seen.look(&w);
    let samples = end_load(&mut w, running);

    let (mut on, mut stale) = (BTreeMap::new(), 0);
    for sample in &samples {
        let (node, version) = (sample.response.node.clone().unwrap_or_default(), sample.response.version);
        let version = version.unwrap_or_else(|| panic!("seed {seed}: no deploy or cut fails a request: {sample:?}"));
        ok(seed, sample, ["one", "two", "three"][version as usize - 1]);
        // In the order the gateway sent them, a runner never runs an older version after a newer one.
        let before = on.insert(node.clone(), version).unwrap_or(0);
        assert!(version >= before, "seed {seed}: {node} went back from {before} to {version}: {sample:?}");
        match (sample.sent, sample.answered) {
            // While it is cut off it runs what it has, and the others what was deployed.
            (2, 2) if node == cut => {
                assert_eq!(version, 1, "seed {seed}: {sample:?}");
                stale += 1;
            }
            (2, 2) | (4, 4) => assert_eq!(version, last.0, "seed {seed}: {sample:?}"),
            _ => {}
        }
    }
    assert!(stale > 0, "seed {seed}: {cut} served nothing while cut off");
    nothing_outlived_its_deadline_and_nothing_is_held(&mut w, seed, &samples);
    println!(
        "world digest cut seed {seed}: {} requests={} by_the_cut_runner={stale} apart_ms={} caught_up_ms={}",
        w.digest(),
        samples.len(),
        apart.as_millis(),
        caught_up.as_millis()
    );
}

fn the_control_node_killed_and_started_again(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    let read = |r: Client| async move { Ok::<_, Error>((r.list().await?, r.get("work").await?)) };
    let registry = move |w: &mut World<Node>| w.block_on(GATEWAY, move |n| read(Client::new(&n.cluster))).unwrap();
    let before = registry(&mut w).unwrap();
    let running = start_load(&mut w, 4, 0);
    let warm = Duration::from_millis(200 + w.pick(0..300));
    w.run_for(warm);

    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash(CONTROL, how);
    running.load.phase.store(1, Relaxed);
    let asked = w.now();
    let refused = deploy(&mut w, GATEWAY, upload("work", "two"));
    assert!(matches!(refused, Err(Error::Cluster(_))), "seed {seed}, {how:?}: {refused:?}");
    let refused_in = w.now() - asked;
    assert!(refused_in <= REFUSED, "seed {seed}, {how:?}: a deploy waited {refused_in:?} to be refused");
    let down = Duration::from_millis(300 + w.pick(0..5000));
    w.run_for(down);
    seen.look(&w);

    w.restart(CONTROL);
    running.load.phase.store(2, Relaxed);
    // Everything the registry answered before the crash, and nothing of the deploy that was refused.
    let end = w.now() + WITHIN;
    let back = loop {
        match registry(&mut w) {
            Ok(back) => break back,
            // Until the gateway sees the control node again.
            Err(Error::Cluster(_)) if w.now() < end => w.run_for(Duration::from_millis(50)),
            Err(e) => panic!("seed {seed}, {how:?}: the registry did not come back: {e:?}"),
        }
    };
    assert_eq!(back, before, "seed {seed}, {how:?}");
    w.run_for(Duration::from_millis(300));
    seen.look(&w);
    let samples = end_load(&mut w, running);
    samples.iter().for_each(|sample| ok(seed, sample, "one"));
    let while_down: BTreeSet<_> =
        samples.iter().filter(|s| s.sent == 1 && s.answered == 1).filter_map(|s| s.response.node.clone()).collect();
    assert!(while_down.len() >= 2, "seed {seed}: the runners served while it was down: {while_down:?}");
    nothing_outlived_its_deadline_and_nothing_is_held(&mut w, seed, &samples);

    // A runner that lost its disk has only the control node to ask, and nothing was deployed since it restarted:
    // what the runner gets is what the control node kept.
    let lost = RUNNERS[w.pick(0..3) as usize];
    w.crash(lost, Crash::Process);
    // Until the gateway has let go of it: what it sent on the old session would be reset by the new process.
    w.run_until("the gateway sending to the two runners left", WITHIN, |w| {
        node(w, GATEWAY).gateway.as_ref().unwrap().runners() == 2
    });
    for dir in ["streams", "registry", "objects", "cache"].map(|name| w.disk(lost).dir(name)) {
        dir.list().into_iter().for_each(|(file, _)| dir.remove(&file));
    }
    w.restart(lost);
    w.run_until("the runner without a disk serving what was deployed before the crash", WITHIN, |w| {
        serves(w, lost, "work") == Some(1) && serves(w, lost, "slow") == Some(2)
    });
    gateway_sees(&mut w, 3);

    // The next deploy goes on from the last seq, and every node takes it.
    assert_eq!(deploy(&mut w, GATEWAY, upload("work", "two")).unwrap(), (2, Some(3)), "seed {seed}, {how:?}");
    w.run_until("every node serving the deploy made after the restart", WITHIN, |w| {
        seen.look(w);
        [GATEWAY].into_iter().chain(RUNNERS).all(|n| serves(w, n, "work") == Some(3))
    });
    let running = start_load(&mut w, 4, 0);
    w.run_for(Duration::from_millis(200));
    let after = end_load(&mut w, running);
    assert!(!after.is_empty());
    after.iter().for_each(|sample| ok(seed, sample, "two"));
    println!(
        "world digest control seed {seed}: {} {how:?} requests={} while_down={} refused_ms={}",
        w.digest(),
        samples.len(),
        samples.iter().filter(|s| s.sent == 1 && s.answered == 1).count(),
        refused_in.as_millis()
    );
}

#[test]
fn a_runner_killed_under_load_loses_no_get_and_only_the_posts_it_had_and_holds_nothing_after() {
    check_seeds(seeds(0..64), a_runner_killed_under_load).unwrap();
}

#[test]
fn a_deploy_reaches_a_runner_cut_off_from_control_after_the_heal_and_no_runner_goes_back_a_version() {
    check_seeds(seeds(0..64), a_deploy_while_a_runner_is_cut_off_from_control).unwrap();
}

#[test]
fn with_the_control_node_down_requests_are_served_deploys_refused_and_it_restarts_with_every_manifest() {
    check_seeds(seeds(0..64), the_control_node_killed_and_started_again).unwrap();
}
