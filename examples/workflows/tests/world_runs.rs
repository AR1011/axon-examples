//! The engine in the cluster on the World, by seed: three history nodes (each a journal replica, a waker and a lease
//! acceptor), two engines that host the eager `run` kind and the task queues, a client node that calls runs as the
//! API does, through `call_idempotent`, and in some stories two workers for queue `q`. `PaxosLeases` and the cluster run at their shipped timings. Runs of a test workflow take
//! `ctx.now` and `ctx.random`, run activities that note each execution, sleep and wait for signals, while engines
//! crash, pause or leave, history nodes go down, the whole cluster goes down, and clocks drift and step.
//!
//! Each story ends the same way: every run ends within the story's bound (I12, read from the replicas' indexes),
//! then the checker reads every replica's disk against the client's acks and the activity's witness (I2-I7, I10,
//! I16), each replica's summaries against the journals (I14), and the values each execution of the code saw (I19).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::{Files, LogConfig};
use axon::trace::unix_micros;
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, DiscoveryError, PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery,
    SeedServer, SeedServerConfig,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, Task, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;
use workflows::check::{self, Ack, Report, Witness};
use workflows::journal::service::{JournalConfig, RunState, ToJournal};
use workflows::journal::{Entry, Status, ValueKind, replica_config};
use workflows::node::{self, NodeConfig, RUN_ARGS};
use workflows::queue::{QUEUE_ARGS, QueueAnswer, QueueCall, partitions};
use workflows::run::machine::input_hash;
use workflows::run::{Code, RUN, RunAnswer, RunCall, call_idempotent};
use workflows::worker::{WorkerConfig, queue_label};
use workflows::workflow::{Activity, ActivityCx, App, Ctx, Failure};

const HISTORY: [&str; 3] = ["h1", "h2", "h3"];
const ENGINES: [&str; 2] = ["e1", "e2"];
const CLIENT: &str = "c1";
const WORKERS: [&str; 2] = ["w1", "w2"];
const WITHIN: Duration = Duration::from_secs(60);
/// How long a client call tries, through the faults of a story.
const PATIENCE: Duration = Duration::from_secs(90);
/// `PaxosLeases`' shipped lease and the cluster's `rebalance_settle`.
const LEASE: Duration = Duration::from_secs(6);
const SETTLE: Duration = Duration::from_millis(500);
/// axon-net's shipped `handshake_timeout`: a dial to a suspended node fails after it.
const HANDSHAKE: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    node: node::Node,
    system: ActorSystem,
    _seed: Option<Arc<SeedServer>>,
}

/// The history nodes' seed servers listen here, beside the cluster's port.
const SEED_PORT: u16 = 7100;

type Acks = Rc<RefCell<Vec<Ack>>>;

thread_local! {
    /// Each execution of an activity attempt. The World runs a seed on one thread.
    static WITNESS: RefCell<Vec<Witness>> = const { RefCell::new(Vec::new()) };
    /// What each execution of a run's code saw of `ctx.now` and `ctx.random`, in order: its first run and every replay.
    static SEEN: RefCell<BTreeMap<String, Vec<(u64, u64)>>> = const { RefCell::new(BTreeMap::new()) };
    /// The workers of this story's World.
    static WORKING: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn witness(cx: &ActivityCx) {
    let witness = Witness {
        run: cx.run_id().to_owned(),
        step: cx.step(),
        attempt: cx.attempt(),
        node: cx.node().to_owned(),
        epoch: cx.epoch(),
        t: unix_micros(),
    };
    WITNESS.with(|w| w.borrow_mut().push(witness));
}

#[derive(Serialize, Deserialize)]
struct Act {
    ms: u64,
}

impl Activity for Act {
    const NAME: &'static str = "act";
    type Output = u64;
    async fn run(self, cx: ActivityCx) -> Result<u64, Failure> {
        witness(&cx);
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        Ok(self.ms)
    }
}

/// `Act` on queue `q`: run by a worker.
#[derive(Serialize, Deserialize)]
struct Far {
    ms: u64,
}

impl Activity for Far {
    const NAME: &'static str = "far";
    const QUEUE: &'static str = "q";
    type Output = u64;
    async fn run(self, cx: ActivityCx) -> Result<u64, Failure> {
        witness(&cx);
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        Ok(self.ms)
    }
}

/// What a run of `work` does: `steps` activities of `ms` each, a sleep of `sleep_ms` after each, then a wait for
/// `signals` signals named `go`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Plan {
    steps: u32,
    ms: u64,
    sleep_ms: u64,
    signals: u32,
}

async fn work(ctx: Ctx, plan: Plan) -> Result<Value, Failure> {
    let (now, random) = (ctx.now(), ctx.random());
    SEEN.with(|seen| seen.borrow_mut().entry(ctx.run_id()).or_default().push((now, random)));
    let mut total = 0;
    for _ in 0..plan.steps {
        total += ctx.activity(Act { ms: plan.ms }).await?;
        if plan.sleep_ms > 0 {
            ctx.sleep(Duration::from_millis(plan.sleep_ms)).await?;
        }
    }
    let mut got = Vec::new();
    for _ in 0..plan.signals {
        got.push(ctx.signal::<Value>("go").await?);
    }
    Ok(json!({ "now": now, "random": random, "total": total, "got": got }))
}

/// `work` with its activities on queue `q`.
async fn remote(ctx: Ctx, plan: Plan) -> Result<u64, Failure> {
    let mut total = 0;
    for _ in 0..plan.steps {
        total += ctx.activity(Far { ms: plan.ms }).await?;
    }
    Ok(total)
}

/// `n` children at once; the sum of what they return.
async fn fan(ctx: Ctx, n: u64) -> Result<u64, Failure> {
    let all = ctx.all((0..n).map(|i| ctx.child::<u64>("kid", i))).await?;
    Ok(all.into_iter().sum())
}

async fn kid(ctx: Ctx, i: u64) -> Result<u64, Failure> {
    ctx.now();
    Ok(i)
}

fn app() -> App {
    (App::new().activity::<Act>().activity::<Far>())
        .workflow("work", 1, work)
        .workflow("remote", 1, remote)
        .workflow("fan", 1, fan)
        .workflow("kid", 1, kid)
}

fn journal_config() -> JournalConfig {
    let mut journal = JournalConfig::new(HISTORY.map(Arc::from).to_vec());
    // Small segments, not preallocated: a story's journals are a few MiB on each machine's disk in memory.
    journal.log = LogConfig { segment_bytes: 2 << 20, preallocate: false, max_bytes: 64 << 20, ..replica_config() };
    // Repair within a story's quiet end.
    journal.repair_every = Duration::from_secs(2);
    journal
}

/// Each history node is a seed server, as the design boots them, so a node that crashes leaves every view within
/// the heartbeat grace.
async fn boot(host: Host) -> Node {
    let name = host.name().to_owned();
    let (history, engine) = (HISTORY.contains(&name.as_str()), ENGINES.contains(&name.as_str()));
    let worker = WORKERS.contains(&name.as_str());
    let transport = || host.transport(SharedSecret::new("at-least-16-bytes-long").unwrap());
    let seed = match history {
        true => Some(Arc::new(
            SeedServer::bind(SeedServerConfig::new(SocketAddr::new(host.ip(), SEED_PORT), &transport())).await.unwrap(),
        )),
        false => None,
    };
    let seeds: Vec<String> = HISTORY.iter().map(|h| SocketAddr::new(host.ip_of(h), SEED_PORT).to_string()).collect();
    let disk = host.disk();
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = loop {
        // Each try is a new incarnation, from the wall clock.
        let transport = transport();
        let discovery = SeedDiscovery::start(SeedConfig::new(seeds.clone(), &transport)).await.unwrap();
        let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
        labels(&mut cfg, &name, &disk);
        match Cluster::start(&system, cfg).await {
            Ok(cluster) => break cluster,
            // A wall clock stepped back makes an older incarnation than the seeds last saw: they refuse it until the
            // wall passes that one.
            Err(ClusterError::Discovery(DiscoveryError::Stale)) => tokio::time::sleep(Duration::from_secs(5)).await,
            Err(error) => panic!("{name} does not start: {error}"),
        }
    };
    let dir = |name: &str| Arc::new(disk.dir(name)) as Arc<dyn Files>;
    let mut cfg = NodeConfig::new(app(), Arc::new(unix_micros), journal_config());
    cfg.history = history.then(|| (dir("journal"), dir("journal-snap")));
    cfg.engine = engine;
    if worker {
        cfg.workers.push(WorkerConfig::new("q", 8));
    }
    let node = node::start(&system, &cluster, cfg).await.unwrap();
    Node { cluster, node, system, _seed: seed }
}

/// The roles of node `name`, and the acceptors.
fn labels(cfg: &mut ClusterConfig, name: &str, disk: &axon_testkit::world::Disk) {
    let (history, engine) = (HISTORY.contains(&name), ENGINES.contains(&name));
    let worker = WORKERS.contains(&name);
    let mut paxos = PaxosConfig::new(HISTORY);
    if history {
        cfg.labels.insert("history".into(), "yes".into());
        paxos.store = Some(Arc::new(disk.dir("leases")));
    }
    if engine {
        cfg.labels.insert("engine".into(), "yes".into());
    }
    if worker {
        cfg.labels.insert("worker".into(), "yes".into());
        cfg.labels.insert(queue_label("q"), "yes".into());
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
}

fn names() -> Vec<&'static str> {
    let workers = WORKING.with(|w| w.borrow().clone());
    HISTORY.into_iter().chain(ENGINES).chain([CLIENT]).chain(workers).collect()
}

fn up(w: &World<Node>, name: &str) -> bool {
    w.nodes().any(|(n, _)| n == name)
}

fn booted(w: &mut World<Node>) {
    let n = names().len();
    w.run_until("every node booted and sees the others", WITHIN, |w| {
        w.nodes().count() == n && w.nodes().all(|(_, node)| node.cluster.view().members.len() == n)
    });
    w.run_until("the acceptors past their wait", WITHIN, |w| {
        w.nodes().filter(|(name, _)| HISTORY.contains(name)).all(|(_, node)| !node.cluster.warming())
    });
}

fn world(seed: u64) -> World<Node> {
    world_with(seed, &[])
}

/// The World with `workers` too.
fn world_with(seed: u64, workers: &[&'static str]) -> World<Node> {
    WORKING.with(|w| *w.borrow_mut() = workers.to_vec());
    WITNESS.with(|w| w.borrow_mut().clear());
    SEEN.with(|s| s.borrow_mut().clear());
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in names() {
        w.add(name, boot);
    }
    booted(&mut w);
    w
}

/// Starts `run` from the client and notes the ack; the answer, or why there is none.
async fn start(n: Node, run: String, plan: Plan, run_timeout_ms: Option<u64>, acks: Acks) -> Result<(), String> {
    let input = Bytes::from(serde_json::to_vec(&plan).unwrap());
    match start_as(n, run.clone(), "work", input, run_timeout_ms, acks).await? {
        Ok(_) => Ok(()),
        Err(code) => Err(format!("{run}: {code:?}")),
    }
}

/// Starts `run` of `workflow` and notes the ack: whether it created the run, or the refusal's code.
async fn start_as(
    n: Node,
    run: String,
    workflow: &'static str,
    input: Bytes,
    run_timeout_ms: Option<u64>,
    acks: Acks,
) -> Result<Result<bool, Code>, String> {
    let call = RunCall::Start {
        workflow: workflow.into(),
        version: None,
        input: input.clone(),
        tags: Vec::new(),
        parent: None,
        run_timeout_ms,
    };
    let refs = n.node.runs.get(&run, RUN_ARGS);
    match call_idempotent(&refs, call, Instant::now() + PATIENCE).await {
        Ok(RunAnswer::Started { created, .. }) => {
            acks.borrow_mut().push(Ack::Started { input_hash: input_hash(workflow, &input), run, t: unix_micros() });
            Ok(Ok(created))
        }
        Ok(RunAnswer::Refused { code, .. }) => Ok(Err(code)),
        other => Err(format!("{run}: {other:?}")),
    }
}

/// Starts each of `runs` of `workflow` from the client, all at once, and waits for every ack.
fn start_all_of(w: &mut World<Node>, workflow: &'static str, runs: &[(String, Bytes)], acks: &Acks) {
    let tasks: Vec<_> = (runs.iter())
        .map(|(run, input)| {
            let (run, input, acks) = (run.clone(), input.clone(), acks.clone());
            w.spawn_on(CLIENT, move |n| start_as(n, run, workflow, input, None, acks))
        })
        .collect();
    for task in tasks {
        assert_eq!(w.join(task), Ok(Ok(Ok(true))));
    }
}

fn json_input(v: impl Serialize) -> Bytes {
    Bytes::from(serde_json::to_vec(&v).unwrap())
}

/// Sends one signal and notes the ack; whether the run took it before.
async fn signal(n: &Node, run: &str, signal_id: &str, acks: &Acks) -> Result<bool, String> {
    let call = RunCall::Signal { signal_id: signal_id.into(), name: "go".into(), payload: Bytes::from_static(b"1") };
    let refs = n.node.runs.get(run, RUN_ARGS);
    match call_idempotent(&refs, call, Instant::now() + PATIENCE).await {
        Ok(RunAnswer::Signalled { duplicate }) => {
            let ack = Ack::Signal { run: run.into(), signal_id: signal_id.into(), t: unix_micros() };
            acks.borrow_mut().push(ack);
            Ok(duplicate)
        }
        other => Err(format!("{run} {signal_id}: {other:?}")),
    }
}

/// Starts each run from the client, all at once, and waits for every ack.
fn start_all(w: &mut World<Node>, runs: &[(String, Plan, Option<u64>)], acks: &Acks) {
    let tasks: Vec<Task<Result<(), String>>> = runs
        .iter()
        .map(|(run, plan, timeout)| {
            let (run, plan, timeout, acks) = (run.clone(), *plan, *timeout, acks.clone());
            w.spawn_on(CLIENT, move |n| start(n, run, plan, timeout, acks))
        })
        .collect();
    for task in tasks {
        assert_eq!(w.join(task), Ok(Ok(())));
    }
}

/// `n` runs named `prefix-i`, each with the plan `plan` gives it.
fn runs(
    n: usize,
    prefix: &str,
    mut plan: impl FnMut(usize) -> (Plan, Option<u64>),
) -> Vec<(String, Plan, Option<u64>)> {
    (0..n)
        .map(|i| {
            let (p, timeout) = plan(i);
            (format!("{prefix}-{i}"), p, timeout)
        })
        .collect()
}

fn ids(runs: &[(String, Plan, Option<u64>)]) -> Vec<String> {
    runs.iter().map(|(run, _, _)| run.clone()).collect()
}

/// Each replica that is up: what it holds of each run.
fn states(w: &mut World<Node>) -> Vec<(&'static str, Vec<RunState>)> {
    let mut all = Vec::new();
    let live: Vec<&'static str> = HISTORY.into_iter().filter(|h| up(w, h)).collect();
    for h in live {
        let asked =
            w.block_on(h, |n| async move { n.node.journal.expect("a history node").ask(ToJournal::Runs).await });
        if let Ok(Ok(runs)) = asked {
            all.push((h, runs));
        }
    }
    all
}

/// Each run's status as most of the replicas that index it say.
fn statuses(w: &mut World<Node>) -> BTreeMap<String, Status> {
    let mut seen: BTreeMap<String, BTreeMap<Status, usize>> = BTreeMap::new();
    for (_, runs) in states(w) {
        for r in runs {
            let Some(indexed) = r.indexed else { continue };
            let run = String::from_utf8_lossy(&r.run).into_owned();
            *seen.entry(run).or_default().entry(indexed.summary.status).or_default() += 1;
        }
    }
    let ended = |counts: &BTreeMap<Status, usize>| {
        counts.iter().find(|(s, n)| s.is_finished() && **n >= 2).map_or(Status::Running, |(s, _)| *s)
    };
    seen.iter().map(|(run, counts)| (run.clone(), ended(counts))).collect()
}

/// I12: every run ends within `bound`, as two replicas index it. Returns each run's status.
fn all_end(w: &mut World<Node>, runs: &[String], bound: Duration) -> BTreeMap<String, Status> {
    let began = w.now();
    loop {
        let now = statuses(w);
        let open: Vec<&String> = runs.iter().filter(|r| !now.get(*r).is_some_and(|s| s.is_finished())).collect();
        if open.is_empty() {
            return now;
        }
        let waited = w.now() - began;
        assert!(waited < bound, "I12: {} runs not ended {waited:?} on, as {:?}: {open:?}", open.len(), now);
        w.run_for(Duration::from_millis(500));
    }
}

/// The newest segment of each run that any replica holds records of, by epoch.
fn newest(w: &mut World<Node>) -> BTreeMap<String, u64> {
    let mut newest = BTreeMap::new();
    for (_, runs) in states(w) {
        for r in runs {
            let Some(epoch) = r.segments.iter().filter(|(_, n)| *n > 0).map(|(e, _)| *e).max() else { continue };
            let at = newest.entry(String::from_utf8_lossy(&r.run).into_owned()).or_insert(0);
            *at = (*at).max(epoch);
        }
    }
    newest
}

/// The runs `engine` hosts, by the cluster's own count.
fn hosted(w: &World<Node>, engine: &str) -> Vec<String> {
    let keys = w.read(engine, |n| n.cluster.hosted_keys(RUN, "", None, 10_000).0).unwrap_or_default();
    keys.into_iter().map(|k| k.key).collect()
}

/// The engine that hosts the most runs, and those runs.
fn busiest(w: &World<Node>) -> (&'static str, Vec<String>) {
    ENGINES.map(|e| (e, hosted(w, e))).into_iter().max_by_key(|(_, runs)| runs.len()).expect("two engines")
}

/// Those of `runs` that have not ended.
fn going_on(w: &mut World<Node>, runs: Vec<String>) -> Vec<String> {
    let ended = statuses(w);
    runs.into_iter().filter(|r| !ended.get(r).is_some_and(|s| s.is_finished())).collect()
}

/// Runs the World until each of `orphans` has a segment newer than the one it had, its new owner's `Own`, or ended:
/// it may have ended just before its owner went. Returns the longest wait.
fn recovered(w: &mut World<Node>, orphans: &[String], before: &BTreeMap<String, u64>, within: Duration) -> Duration {
    let began = w.now();
    let mut left: BTreeSet<&String> = orphans.iter().collect();
    let mut longest = Duration::ZERO;
    while !left.is_empty() {
        let (now, ended) = (newest(w), statuses(w));
        left.retain(|run| now.get(*run) <= before.get(*run) && !ended.get(*run).is_some_and(|s| s.is_finished()));
        longest = w.now() - began;
        assert!(longest <= within, "{} runs not opened again {longest:?} on: {left:?}", left.len());
        w.run_for(Duration::from_millis(100));
    }
    longest
}

fn leave(w: &mut World<Node>, engine: &str) {
    let left = w.spawn_on(engine, |n| async move { n.system.shutdown().await });
    assert!(matches!(w.join(left), Ok(Ok(_))), "{engine} leaves");
    w.crash(engine, Crash::Process);
}

/// Waits for the replicas to hold the same newest segments, then checks them: I2-I7, I10 and I16 against the acks
/// and the witness, I14 on each replica, I19 over what the code saw.
fn quiesce(w: &mut World<Node>, acks: &Acks) -> Report {
    let mut tries = 0;
    loop {
        let all: Vec<BTreeMap<Bytes, Option<(u64, u64)>>> = states(w)
            .into_iter()
            .map(|(_, runs)| {
                runs.into_iter().map(|r| (r.run, r.segments.into_iter().filter(|(_, n)| *n > 0).max())).collect()
            })
            .collect();
        if all.len() == HISTORY.len() && all.iter().all(|s| *s == all[0]) {
            break;
        }
        tries += 1;
        assert!(tries < 60, "the replicas are not level after a minute of repair");
        w.run_for(Duration::from_secs(1));
    }
    let mut dumps = Vec::new();
    for h in HISTORY {
        let files = Arc::new(w.disk(h).dir("journal"));
        let cfg = journal_config().log;
        dumps.push(w.block_on(h, move |_| check::dump(files, cfg)).unwrap().expect("the replica reads back"));
    }
    let witness = WITNESS.with(|w| w.borrow().clone());
    let report = check::check(&dumps, &acks.borrow(), Some(&witness));
    report.assert_ok();
    for (h, runs) in states(w) {
        let summaries: Vec<_> = runs.into_iter().filter_map(|r| Some((r.run, r.indexed?.summary))).collect();
        let v = check::check_summaries(&report, h, &summaries);
        assert!(v.is_empty(), "I14 on {h}: {v:?}");
    }
    // An owner sees a value as it journals it, so one that crashes first saw a value no one else can: nothing it did
    // with it left the run (I2). Once the journal holds the values, every replay sees them.
    SEEN.with(|seen| {
        for (run, values) in seen.borrow().iter() {
            let held = |want: ValueKind| {
                journal(&report, run).into_iter().find_map(|e| match e {
                    Entry::Value { kind, value, .. } if kind == want => serde_json::from_slice::<u64>(&value).ok(),
                    _ => None,
                })
            };
            let journaled = (held(ValueKind::Now).expect("now journaled"), held(ValueKind::Random).expect("random"));
            let first = values.iter().position(|v| *v == journaled);
            let after = first.map(|i| &values[i..]);
            assert!(
                after.is_some_and(|after| after.iter().all(|v| *v == journaled)),
                "I19: run {run} journaled {journaled:?} and saw {values:?}"
            );
        }
    });
    report
}

/// The run's journal, decoded.
fn journal(report: &Report, run: &str) -> Vec<Entry> {
    let bodies = report.journals.get(run.as_bytes()).map_or(&[][..], Vec::as_slice);
    bodies.iter().map(|b| Entry::decode(b).expect("decodes")).collect()
}

fn story(test: impl Fn(u64) + Sync) {
    check_seeds(seeds(0..64), test).unwrap();
}

fn plan(steps: u32, ms: u64, sleep_ms: u64, signals: u32) -> Plan {
    Plan { steps, ms, sleep_ms, signals }
}

/// Reads `run` back to back for `d`, as a client polling it would; the first read that gets no answer.
async fn reader(n: Node, run: String, d: Duration) -> Result<(), String> {
    let (refs, end) = (n.node.runs.get(&run, RUN_ARGS), Instant::now() + d);
    while Instant::now() < end {
        match call_idempotent(&refs, RunCall::Get, Instant::now() + PATIENCE).await {
            Ok(RunAnswer::Run(_)) => {}
            other => return Err(format!("{run}: {other:?}")),
        }
    }
    Ok(())
}

/// (a) and (a'): an engine crashes in the middle of steps under load. Each run it hosted opens on the other within
/// a lease, `rebalance_settle` and 2 s (`Crash::Process`), or 10 s, settle and 2 s (`Crash::Machine`), with no caller:
/// eager starts it. A client that reads a few of them back to back through the crash gets an answer to each read: a
/// read lost with the engine is asked again. Every run then ends, each step done once from the journal's view (I4,
/// I5), and a replay sees the first run's `ctx.now` and `ctx.random` (I19).
fn engine_crash(seed: u64, how: Crash, bound: Duration) {
    let mut w = world(seed);
    let acks = Acks::default();
    let all = runs(200, "a", |i| (plan(3, 400 + 300 * (i as u64 % 4), 200 * (i as u64 % 3), 0), None));
    start_all(&mut w, &all, &acks);
    let ms = w.pick(500..2_500);
    w.run_for(Duration::from_millis(ms));
    let (victim, hosted) = busiest(&w);
    let orphans = going_on(&mut w, hosted);
    assert!(!orphans.is_empty(), "{victim} hosts runs that go on");
    let before = newest(&mut w);
    let reads: Vec<Task<Result<(), String>>> = orphans
        .iter()
        .take(10)
        .map(|run| {
            let run = run.clone();
            w.spawn_on(CLIENT, move |n| reader(n, run, Duration::from_secs(3)))
        })
        .collect();
    w.run_for(Duration::from_millis(50));
    w.crash(victim, how);
    let took = recovered(&mut w, &orphans, &before, bound);
    // Shown with `--nocapture`: the recovery bound's numbers.
    eprintln!("seed {seed}: {} runs of {victim} opened again within {took:?}", orphans.len());
    for read in reads {
        assert_eq!(w.join(read), Ok(Ok(())));
    }
    w.restart(victim);
    all_end(&mut w, &ids(&all), Duration::from_secs(90));
    let report = quiesce(&mut w, &acks);
    for run in ids(&all) {
        // A read of a run that ended opens it, and its owner writes an `Own` after the end.
        let last = journal(&report, &run).into_iter().rev().find(|e| !matches!(e, Entry::Own(_)));
        assert!(matches!(last, Some(Entry::Completed { .. })), "{run} completed");
    }
}

#[test]
fn a_an_engine_process_crashes_mid_step() {
    story(|seed| engine_crash(seed, Crash::Process, LEASE + SETTLE + Duration::from_secs(2)));
}

#[test]
fn a_prime_an_engine_machine_crashes_mid_step() {
    story(|seed| engine_crash(seed, Crash::Machine, Duration::from_secs(10) + SETTLE + Duration::from_secs(2)));
}

/// (b) An engine is suspended past its leases with runs in hand, and a client asks after them. Its dial to the engine
/// fails at the handshake timeout, and the runs open on the other engine within it, `rebalance_settle` and 2 s,
/// sealing their journals; when the first wakes, inside its leases, its writes at the old epochs reach no replica
/// that took the seal (I1), and every run ends once. With no caller, nothing notices the engine until it wakes: the
/// other nodes keep a session to a peer only while they call it.
#[test]
fn b_a_suspended_engine_writes_at_stale_epochs() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let all = runs(20, "b", |i| (plan(4, 600 + 200 * (i as u64 % 3), 0, 0), None));
        start_all(&mut w, &all, &acks);
        let ms = w.pick(300..1_500);
        w.run_for(Duration::from_millis(ms));
        let (victim, hosted) = busiest(&w);
        let orphans = going_on(&mut w, hosted);
        let before = newest(&mut w);
        w.suspend(victim, Duration::from_secs(20));
        let asks: Vec<Task<bool>> = orphans
            .iter()
            .map(|run| {
                let run = run.clone();
                w.spawn_on(CLIENT, move |n| async move {
                    let refs = n.node.runs.get(&run, RUN_ARGS);
                    call_idempotent(&refs, RunCall::Get, Instant::now() + PATIENCE).await.is_ok()
                })
            })
            .collect();
        let took = recovered(&mut w, &orphans, &before, HANDSHAKE + SETTLE + Duration::from_secs(2));
        eprintln!("seed {seed}: {} runs of {victim} opened again within {took:?}", orphans.len());
        for ask in asks {
            assert_eq!(w.join(ask), Ok(true), "the client's ask is answered");
        }
        // Each orphan's old segment and its length on the replicas whose fence passed it: I1 says none of them grows.
        let mut sealed = BTreeMap::new();
        for (h, runs) in states(&mut w) {
            for r in runs {
                let run = String::from_utf8_lossy(&r.run).into_owned();
                let Some(old) = before.get(&run).filter(|old| r.fence > **old && orphans.contains(&run)) else {
                    continue;
                };
                let len = r.segments.iter().find(|(e, _)| e == old).map_or(0, |(_, n)| *n);
                sealed.insert((h, run), (*old, len));
            }
        }
        assert!(!sealed.is_empty(), "the new owners sealed");
        w.run_for(Duration::from_secs(25));
        for (h, runs) in states(&mut w) {
            for r in runs {
                let key = (h, String::from_utf8_lossy(&r.run).into_owned());
                let Some((old, len)) = sealed.get(&key) else { continue };
                let now = r.segments.iter().find(|(e, _)| e == old).map_or(0, |(_, n)| *n);
                assert_eq!(now, *len, "I1: {h} took a write at the stale epoch {old} of {}", key.1);
            }
        }
        all_end(&mut w, &ids(&all), Duration::from_secs(90));
        quiesce(&mut w, &acks);
    });
}

/// (e) The whole cluster goes down at once, losing what it had not synced. The history nodes come back after 10 s and
/// the engines two minutes later: by then the acceptors, which list a lapsed lease for a minute, have forgotten the
/// engines' leases, so eager starts nothing. The waker fires the sleepers, and the reconciler opens the runs that
/// were in an activity, which no timer wakes. (Had the acceptors come back with the engines, they would list every
/// lease their logs hold again, and eager would start the runs.)
#[test]
fn e_the_whole_cluster_crashes_and_the_reconciler_finishes_the_runs() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let all = runs(20, "e", |i| match i % 2 {
            0 => (plan(2, 6_000, 0, 0), None),
            _ => (plan(1, 100, 30_000, 0), None),
        });
        start_all(&mut w, &all, &acks);
        let ms = w.pick(1_000..3_000);
        w.run_for(Duration::from_millis(ms));
        for name in names() {
            w.crash(name, Crash::Machine);
        }
        w.run_for(Duration::from_secs(10));
        for name in HISTORY.into_iter().chain([CLIENT]) {
            w.restart(name);
        }
        w.run_for(Duration::from_secs(110));
        for name in ENGINES {
            w.restart(name);
        }
        booted(&mut w);
        all_end(&mut w, &ids(&all), Duration::from_secs(120));
        quiesce(&mut w, &acks);
    });
}

/// (f) Clocks move under sleepers and run timeouts: a waker's wall runs fast, another's steps ahead, an acceptor's
/// steps back a little. No timer fires before its time on the clock of the node that fires it (I6), and a run past
/// its timeout ends `Failed { timeout }` and nothing else (I16).
#[test]
fn f_drifting_and_stepped_clocks_under_sleepers_and_run_timeouts() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        // Every third run times out in its long sleep.
        let all = runs(24, "f", |i| match i % 3 {
            0 => (plan(1, 50, 40_000, 0), Some(15_000)),
            1 => (plan(2, 50, 3_000 + 1_000 * (i as u64 % 5), 0), Some(60_000)),
            _ => (plan(3, 100, 2_000, 0), None),
        });
        let waker = HISTORY[w.pick(0..3) as usize];
        w.drift(waker, 50_000);
        start_all(&mut w, &all, &acks);
        let ahead = HISTORY.into_iter().find(|h| *h != waker).expect("another");
        let ms = w.pick(500..4_000);
        w.run_for(Duration::from_millis(ms));
        let by = w.pick(500..3_000) as i64;
        w.step_wall(ahead, by);
        let acceptor = HISTORY.into_iter().find(|h| *h != waker && *h != ahead).expect("a third");
        let back = w.pick(50..500) as i64;
        w.step_wall(acceptor, -back);
        let ended = all_end(&mut w, &ids(&all), Duration::from_secs(90));
        let report = quiesce(&mut w, &acks);
        for (i, run) in ids(&all).iter().enumerate() {
            let last = journal(&report, run).pop();
            match i % 3 {
                0 => assert!(matches!(last, Some(Entry::Failed { timeout: true, .. })), "I16: {run} ended {last:?}"),
                _ => assert_eq!(ended.get(run), Some(&Status::Completed), "{run}"),
            }
        }
    });
}

/// Signals one run as a client would, `gap` apart, each sent twice: the second is a duplicate. The last one is a
/// cancel instead when `cancel`.
async fn signaller(n: Node, run: String, gaps: Vec<u64>, cancel: bool, acks: Acks) -> Result<(), String> {
    let count = gaps.len();
    for (k, gap) in gaps.into_iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(gap)).await;
        if cancel && k + 1 == count {
            let refs = n.node.runs.get(&run, RUN_ARGS);
            let answer = call_idempotent(&refs, RunCall::Cancel { reason: "test".into() }, Instant::now() + PATIENCE);
            return match answer.await {
                Ok(RunAnswer::Accepted) => Ok(()),
                other => Err(format!("{run} cancel: {other:?}")),
            };
        }
        let id = format!("{run}-s{k}");
        if signal(&n, &run, &id, &acks).await? {
            return Err(format!("{id} was a duplicate the first time"));
        }
        if !signal(&n, &run, &id, &acks).await? {
            return Err(format!("{id} was new the second time"));
        }
    }
    Ok(())
}

/// (h) Signals and cancels arrive as runs that wait for them stop for having nothing to do. Each signal is taken
/// once, a duplicate is answered as one, and none is lost or taken out of order (I7); a cancelled run schedules no
/// activity after its cancel (I11). Calls to ids that were never started are answered `not_found`, and leave neither
/// a fence nor an entry on any replica (I17).
#[test]
fn h_signals_and_cancels_race_passivation() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let all = runs(16, "h", |_| (plan(1, 50, 0, 3), None));
        start_all(&mut w, &all, &acks);
        let mut tasks = Vec::new();
        for (i, run) in ids(&all).into_iter().enumerate() {
            // Around the second a run with nothing to do waits before it stops.
            let gaps: Vec<u64> = (0..3).map(|_| w.pick(600..1_600)).collect();
            let acks = acks.clone();
            tasks.push(w.spawn_on(CLIENT, move |n| signaller(n, run, gaps, i % 4 == 0, acks)));
        }
        let unknown: Vec<String> = (0..4).map(|i| format!("nobody-{i}")).collect();
        let calls = unknown.clone();
        let refused = w.spawn_on(CLIENT, move |n| async move {
            let mut codes = Vec::new();
            for (i, run) in calls.into_iter().enumerate() {
                let call = match i % 3 {
                    0 => RunCall::Get,
                    1 => RunCall::Cancel { reason: "x".into() },
                    _ => {
                        RunCall::Signal { signal_id: "s".into(), name: "go".into(), payload: Bytes::from_static(b"1") }
                    }
                };
                let refs = n.node.runs.get(&run, RUN_ARGS);
                codes.push(call_idempotent(&refs, call, Instant::now() + PATIENCE).await);
            }
            codes
        });
        for task in tasks {
            assert_eq!(w.join(task), Ok(Ok(())));
        }
        for answer in w.join(refused).expect("the client is up") {
            assert!(matches!(answer, Ok(RunAnswer::Refused { code: Code::NotFound, .. })), "{answer:?}");
        }
        let ended = all_end(&mut w, &ids(&all), Duration::from_secs(60));
        let report = quiesce(&mut w, &acks);
        for (i, run) in ids(&all).iter().enumerate() {
            let want = if i % 4 == 0 { Status::Cancelled } else { Status::Completed };
            assert_eq!(ended.get(run), Some(&want), "{run}");
            let entries = journal(&report, run);
            if let Some(cancel) = entries.iter().position(|e| matches!(e, Entry::CancelRequested { .. })) {
                let late = entries[cancel..].iter().find(|e| matches!(e, Entry::ActivityScheduled { .. }));
                assert!(late.is_none(), "I11: {run} scheduled {late:?} after its cancel");
            }
        }
        for (h, runs) in states(&mut w) {
            for r in runs {
                let id = String::from_utf8_lossy(&r.run).into_owned();
                assert!(!unknown.contains(&id), "I17: {h} holds {id}: fence {}, {:?}", r.fence, r.segments);
            }
        }
    });
}

/// (l) Two history nodes, a majority of the replicas and of the acceptors, are down for three minutes. No run moves
/// meanwhile; once they are back, each run goes on and ends.
#[test]
fn l_two_history_nodes_down_for_three_minutes() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let all = runs(20, "l", |i| (plan(3, 500 + 500 * (i as u64 % 3), 1_000, 0), None));
        start_all(&mut w, &all, &acks);
        let ms = w.pick(500..3_000);
        w.run_for(Duration::from_millis(ms));
        let kept = HISTORY[w.pick(0..3) as usize];
        let down: Vec<&str> = HISTORY.into_iter().filter(|h| *h != kept).collect();
        for h in &down {
            w.crash(h, Crash::Process);
        }
        w.run_for(Duration::from_secs(180));
        for h in &down {
            w.restart(h);
        }
        booted(&mut w);
        all_end(&mut w, &ids(&all), Duration::from_secs(150));
        quiesce(&mut w, &acks);
    });
}

/// (n) Both engines leave in turn, each started again before the other goes: a graceful leave hands each run to the
/// other engine and fails no attempt (I18).
#[test]
fn n_a_rolling_shutdown_of_both_engines() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let all = runs(20, "n", |i| (plan(4, 300 + 200 * (i as u64 % 3), 500, 0), None));
        start_all(&mut w, &all, &acks);
        for engine in ENGINES {
            let ms = w.pick(500..2_000);
            w.run_for(Duration::from_millis(ms));
            leave(&mut w, engine);
            w.restart(engine);
            booted(&mut w);
        }
        all_end(&mut w, &ids(&all), Duration::from_secs(90));
        let report = quiesce(&mut w, &acks);
        for run in ids(&all) {
            let failed = journal(&report, &run).into_iter().find(|e| matches!(e, Entry::ActivityFailed { .. }));
            assert!(failed.is_none(), "I18: {run} has {failed:?}");
        }
    });
}

/// (g) Each run is started twice at once, by the client and by a history node, as two api nodes would, while an
/// engine crashes. Both get the run (a start lost with the engine is asked again, and the second is answered
/// `created: false`), no id is created twice, and each journal starts once with the acked input (I8). A start of
/// another input under the id is refused `exists`.
#[test]
fn g_a_double_start_from_two_callers_through_an_engine_crash() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        let input = json_input(plan(2, 300, 0, 0));
        let ids: Vec<String> = (0..20).map(|i| format!("g-{i}")).collect();
        let mut tasks = Vec::new();
        for id in &ids {
            for from in [CLIENT, HISTORY[0]] {
                let (run, input, acks) = (id.clone(), input.clone(), acks.clone());
                tasks.push((id.clone(), w.spawn_on(from, move |n| start_as(n, run, "work", input, None, acks))));
            }
        }
        let ms = w.pick(0..200);
        w.run_for(Duration::from_millis(ms));
        let victim = ENGINES[w.pick(0..2) as usize];
        w.crash(victim, Crash::Process);
        let mut created: BTreeMap<String, usize> = BTreeMap::new();
        for (id, task) in tasks {
            match w.join(task) {
                Ok(Ok(Ok(true))) => *created.entry(id).or_default() += 1,
                Ok(Ok(Ok(false))) => {}
                other => panic!("{id}: {other:?}"),
            }
        }
        assert!(created.values().all(|n| *n == 1), "I8: an id created other than once: {created:?}");
        w.restart(victim);
        booted(&mut w);
        let other = json_input(plan(1, 1, 0, 0));
        for id in ids.iter().take(4) {
            let (run, other, acks) = (id.clone(), other.clone(), Acks::default());
            let again = w.spawn_on(CLIENT, move |n| start_as(n, run, "work", other, None, acks));
            assert_eq!(w.join(again), Ok(Ok(Err(Code::Exists))), "I8: {id} started with another input");
        }
        all_end(&mut w, &ids, Duration::from_secs(90));
        quiesce(&mut w, &acks);
    });
}

/// (i) A run starts 1,000 children at once and its engine crashes as they start. Each child starts once, ends, and
/// tells its parent, which takes each result once (I5) and ends with their sum. A run that asks for 1,001 at once
/// fails at once and starts none.
#[test]
fn i_a_fan_out_of_a_thousand_children_through_a_crash() {
    story(|seed| {
        let mut w = world(seed);
        let acks = Acks::default();
        start_all_of(
            &mut w,
            "fan",
            &[("fan-0".into(), json_input(1_000)), ("fan-big".into(), json_input(1_001))],
            &acks,
        );
        let ms = w.pick(100..1_500);
        w.run_for(Duration::from_millis(ms));
        let victim = ENGINES.into_iter().find(|e| hosted(&w, e).contains(&"fan-0".to_owned())).unwrap_or(ENGINES[0]);
        w.crash(victim, Crash::Process);
        w.run_for(Duration::from_secs(5));
        w.restart(victim);
        booted(&mut w);
        let children: Vec<String> = (1..=1_000).map(|step| format!("fan-0/{step}")).collect();
        let parents = ["fan-0".to_owned(), "fan-big".to_owned()];
        all_end(&mut w, &children, Duration::from_secs(180));
        let ended = all_end(&mut w, &parents, Duration::from_secs(60));
        assert_eq!(ended.get("fan-0"), Some(&Status::Completed));
        assert_eq!(ended.get("fan-big"), Some(&Status::Failed));
        // A child whose engine crashed after it ended, before it journaled that its parent heard, opens again by
        // eager or the waker, and tells it then.
        let mut tries = 0;
        while states(&mut w).iter().any(|(_, runs)| {
            runs.iter()
                .any(|r| r.run.starts_with(b"fan-0/") && r.indexed.as_ref().is_some_and(|i| i.summary.owes_parent))
        }) {
            tries += 1;
            assert!(tries < 60, "a child of fan-0 has not told its parent after a minute");
            w.run_for(Duration::from_secs(1));
        }
        let report = quiesce(&mut w, &acks);
        let parent = journal(&report, "fan-0");
        let count = |f: fn(&Entry) -> bool| parent.iter().filter(|e| f(e)).count();
        assert_eq!(count(|e| matches!(e, Entry::ChildStarted { .. })), 1_000);
        assert_eq!(count(|e| matches!(e, Entry::ChildDone { result: Ok(_), .. })), 1_000);
        let done = parent.iter().find_map(|e| match e {
            Entry::Completed { output } => Some(output.clone()),
            _ => None,
        });
        assert_eq!(done.as_deref(), Some(b"499500".as_slice()));
        for child in &children {
            let entries = journal(&report, child);
            assert!(entries.iter().any(|e| matches!(e, Entry::ParentNotified)), "{child} told its parent");
        }
        let big = journal(&report, "fan-big");
        assert!(!big.iter().any(|e| matches!(e, Entry::ChildStarted { .. })), "1,001 children: none starts");
        assert!(!report.journals.keys().any(|run| run.starts_with(b"fan-big/")));
    });
}

/// The backlog of queue `q` over its partitions, as the client asks it.
fn backlog(w: &mut World<Node>) -> u64 {
    let asked = w.block_on(CLIENT, |n| async move {
        let mut total = 0;
        for key in partitions("q") {
            match n.node.queues.get(&key, QUEUE_ARGS).request_within(QueueCall::Stats, Duration::from_secs(5)).await {
                Ok(QueueAnswer::Stats(stats)) => total += stats.backlog,
                other => panic!("{key}: {other:?}"),
            }
        }
        total
    });
    asked.expect("the client is up")
}

/// (k) Two workers run the activities of queue `q`. One crashes with attempts in hand: the engines' worker watch
/// fails them, and they run again on the other. Then the other drains and leaves: what it started finishes, what it
/// had not started is given back, and no attempt of those runs fails (I18). At the end the queue's backlog is empty
/// (I13).
#[test]
fn k_a_worker_crashes_then_a_worker_drains() {
    story(|seed| {
        let mut w = world_with(seed, &WORKERS);
        let acks = Acks::default();
        let crashed: Vec<(String, Bytes)> =
            (0..10).map(|i| (format!("kc-{i}"), json_input(plan(3, 1_000 + 500 * (i % 3), 0, 0)))).collect();
        start_all_of(&mut w, "remote", &crashed, &acks);
        let ms = w.pick(1_500..3_000);
        w.run_for(Duration::from_millis(ms));
        w.crash(WORKERS[0], Crash::Process);
        let crashed: Vec<String> = crashed.into_iter().map(|(run, _)| run).collect();
        all_end(&mut w, &crashed, Duration::from_secs(90));
        w.restart(WORKERS[0]);
        booted(&mut w);

        let drained: Vec<(String, Bytes)> =
            (0..10).map(|i| (format!("kd-{i}"), json_input(plan(3, 1_000 + 500 * (i % 3), 0, 0)))).collect();
        start_all_of(&mut w, "remote", &drained, &acks);
        let ms = w.pick(1_500..3_000);
        w.run_for(Duration::from_millis(ms));
        let leaving = w.spawn_on(WORKERS[1], |n| async move {
            n.node.drain_workers(Duration::from_secs(30)).await;
            n.system.shutdown().await
        });
        assert!(matches!(w.join(leaving), Ok(Ok(_))), "{} drains and leaves", WORKERS[1]);
        w.crash(WORKERS[1], Crash::Process);
        let drained: Vec<String> = drained.into_iter().map(|(run, _)| run).collect();
        let ended = all_end(&mut w, &drained, Duration::from_secs(90));
        let report = quiesce(&mut w, &acks);
        let mut left = 0;
        for run in &crashed {
            assert_eq!(statuses(&mut w).get(run), Some(&Status::Completed), "{run}");
            left += journal(&report, run).iter().filter(|e| matches!(e, Entry::ActivityFailed { .. })).count();
        }
        eprintln!("seed {seed}: {left} attempts failed when {} crashed", WORKERS[0]);
        for run in &drained {
            assert_eq!(ended.get(run), Some(&Status::Completed), "{run}");
            let failed = journal(&report, run).into_iter().find(|e| matches!(e, Entry::ActivityFailed { .. }));
            assert!(failed.is_none(), "I18: {run} has {failed:?}");
        }
        assert_eq!(backlog(&mut w), 0, "I13: the backlog of q is empty");
    });
}
