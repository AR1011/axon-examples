//! The journal service on the World, by seed: three history nodes, each with its `Journal` and `Waker` on a disk of
//! its own, and two engine nodes whose stub owners write runs through [`Owner`]. Each story ends quiet: the faults
//! heal, a last owner opens each run, repair brings every replica level, and then the checker reads the replicas'
//! disks against every ack an owner got (I2, I3), each replica's summaries against the journals (I14), and the story
//! asserts what it saw of the fence (I1).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{LogConfig, crc32};
use axon::trace::unix_micros;
use axon::{ActorHandle, ActorSystem, ReplyTo};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, Placement};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;
use workflows::check::{self, Ack, Report};
use workflows::journal::client::{Ended, JournalClient, Owner};
use workflows::journal::service::{self, JournalConfig, JournalStats, RunState, ToJournal, Wake, WakerConfig};
use workflows::journal::{Entry, Mutation, ValueKind, WriterConfig, journal_epoch, replica_config};

const HISTORY: [&str; 3] = ["h1", "h2", "h3"];
const ENGINES: [&str; 2] = ["e1", "e2"];
/// The last owner's lease, above every story's.
const LAST: u64 = 1_000;
const WITHIN: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    journal: Option<ActorHandle<ToJournal>>,
    client: JournalClient,
    _system: ActorSystem,
}

/// Each wake a waker fired: the node, the run and the node's wall clock.
type Wakes = Arc<Mutex<Vec<(String, Bytes, u64)>>>;
type Acks = Rc<RefCell<Vec<Ack>>>;

#[derive(Clone)]
struct Setup {
    journal: JournalConfig,
    wakes: Wakes,
    /// Each waker's wakes in flight at once.
    in_flight: usize,
}

impl Setup {
    fn new(segment_bytes: u64) -> Self {
        let mut journal = JournalConfig::new(HISTORY.map(Arc::from).to_vec());
        // Small, so that a story fills several segments, and the log copies records forward once past 2 MiB.
        let log = LogConfig {
            segment_bytes,
            preallocate: false,
            max_record: 32 << 10,
            max_bytes: 4 << 20,
            ..replica_config()
        };
        journal.log = log;
        // Repair and snapshots within a story's minute; nothing expires unless a story says so.
        journal.repair_every = Duration::from_secs(2);
        journal.snapshot_every = Duration::from_secs(3);
        journal.mutation = mutation();
        Self { journal, wakes: Wakes::default(), in_flight: 128 }
    }
}

fn history() -> Placement {
    Placement::default().require("history", "yes")
}

/// `JOURNAL_MUTATION=<name>` weakens one rule of the writers or of the replicas, and a story must fail.
fn mutation() -> Mutation {
    std::env::var("JOURNAL_MUTATION").map_or(Mutation::None, |m| Mutation::parse(&m).expect("a mutation"))
}

fn writer_config() -> WriterConfig {
    WriterConfig { mutation: mutation(), ..WriterConfig::new(HISTORY.len()) }
}

async fn boot(host: Host, discovery: MemoryDiscovery, setup: Setup) -> Node {
    let name = host.name().to_owned();
    let secret = SharedSecret::new("at-least-16-bytes-long").unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(secret), discovery);
    let is_history = HISTORY.contains(&name.as_str());
    if is_history {
        cfg.labels.insert("history".into(), "yes".into());
    }
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let mut journal = None;
    if is_history {
        // The journal and its snapshot in directories of their own, on the machine's one disk.
        let disk = host.disk();
        let (files, snaps) = (Arc::new(disk.dir("journal")), Arc::new(disk.dir("journal-snap")));
        let handle = service::start(&system, &cluster, files, snaps, setup.journal.clone()).await.unwrap();
        let wakes = setup.wakes.clone();
        let wake: Wake = Arc::new(move |run| {
            wakes.lock().unwrap().push((name.clone(), run, unix_micros()));
            Box::pin(async { Ok(()) })
        });
        let waker = WakerConfig { in_flight: setup.in_flight, ..WakerConfig::new(history(), wake) };
        service::start_waker(&system, &cluster, handle.clone(), waker).unwrap();
        journal = Some(handle);
    }
    let client = JournalClient::new(cluster.clone(), HISTORY.map(Arc::from).to_vec());
    Node { cluster, journal, client, _system: system }
}

fn world(seed: u64, setup: &Setup) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in HISTORY.iter().chain(&ENGINES) {
        let (discovery, setup) = (discovery.clone(), setup.clone());
        w.add(name, move |host| boot(host, discovery.clone(), setup.clone()));
    }
    w.run_until("every node booted and sees the others", Duration::from_secs(60), |w| {
        w.nodes().count() == 5 && w.nodes().all(|(_, n)| n.cluster.view().members.len() == 5)
    });
    w
}

fn started(run: &str) -> Bytes {
    let tags = vec![("story".into(), "world".into())];
    let input = Bytes::from(run.to_owned());
    Entry::Started {
        workflow: "stub".into(),
        version: 1,
        input_hash: u64::from(crc32(&input)),
        input,
        tags,
        parent: None,
        run_timeout_ms: None,
        at_us: 0,
    }
    .encode()
}

/// What a stub owner writes after its open.
#[derive(Clone, Copy)]
struct Plan {
    values: u32,
    bytes: usize,
    /// Ends the run with `Completed`.
    complete: bool,
    /// Between appends.
    pace: Duration,
}

impl Plan {
    const NOTHING: Plan = Plan { values: 0, bytes: 0, complete: false, pace: Duration::ZERO };

    fn values(values: u32) -> Plan {
        Plan { values, bytes: 64, complete: false, pace: Duration::from_millis(20) }
    }
}

/// A stub owner of `run` under `lease`: opens it, starts it when it is new, appends the plan's entries one at a time
/// and logs each ack. `opened` hears the epoch once the open is on a majority.
async fn own(
    node: Node,
    run: String,
    lease: u64,
    plan: Plan,
    acks: Acks,
    opened: Option<Rc<RefCell<u64>>>,
) -> Result<(), Ended> {
    let id = Bytes::from(run.clone());
    let (mut owner, journal) = Owner::open(node.client.clone(), id, lease, writer_config(), WITHIN).await?;
    if let Some(opened) = opened {
        *opened.borrow_mut() = owner.epoch();
    }
    let mut entries = Vec::new();
    if journal.is_empty() && plan.values > 0 {
        entries.push(started(&run));
    }
    // A step number no earlier owner used: each entry takes at least one index.
    let first = journal.len() as u32 + 1;
    for step in first..first + plan.values {
        let value = Bytes::from(vec![step as u8; plan.bytes]);
        entries.push(Entry::Value { step, kind: ValueKind::Random, value }.encode());
    }
    if plan.complete {
        entries.push(Entry::Completed { output: Bytes::new() }.encode());
    }
    for body in entries {
        let crc = crc32(&body);
        let index = owner.append(vec![body], WITHIN).await?[0];
        acks.borrow_mut().push(Ack::Entry { run: run.clone(), index, crc, t: unix_micros() });
        if !plan.pace.is_zero() {
            tokio::time::sleep(plan.pace).await;
        }
    }
    Ok(())
}

fn spawn_owner(
    w: &mut World<Node>,
    on: &str,
    run: &str,
    lease: u64,
    plan: Plan,
    acks: &Acks,
) -> axon_testkit::world::Task<Result<(), Ended>> {
    let (run, acks) = (run.to_owned(), acks.clone());
    w.spawn_on(on, move |n| own(n, run, lease, plan, acks, None))
}

fn ask<T: Send + 'static>(w: &mut World<Node>, replica: &str, f: fn(ReplyTo<T>) -> ToJournal) -> T {
    let answer = w.block_on(replica, move |n| async move { n.journal.expect("a history node").ask(f).await });
    answer.expect("the replica is up").expect("the journal answers")
}

fn runs_on(w: &mut World<Node>, replica: &str) -> Vec<RunState> {
    ask(w, replica, ToJournal::Runs)
}

fn stats_on(w: &mut World<Node>, replica: &str) -> JournalStats {
    ask(w, replica, ToJournal::Stats)
}

/// Each run's newest segment with records, and its length, on each replica. A segment below a replica's fence takes
/// no repair (`Log::append_at` refuses it): a replica may hold less of it than the others, and the carry and the
/// checker read it from them.
fn newest(w: &mut World<Node>) -> Vec<BTreeMap<Bytes, Option<(u64, u64)>>> {
    let newest = |r: RunState| (r.run, r.segments.into_iter().filter(|(_, n)| *n > 0).max());
    HISTORY.map(|h| runs_on(w, h).into_iter().map(newest).collect()).to_vec()
}

/// The length of `run`'s segment `epoch` on `replica`.
fn length(w: &mut World<Node>, replica: &str, run: &str, epoch: u64) -> u64 {
    let runs = runs_on(w, replica);
    let run = runs.iter().find(|r| r.run == run.as_bytes());
    run.and_then(|r| r.segments.iter().find(|(e, _)| *e == epoch)).map_or(0, |(_, n)| *n)
}

fn fence(w: &mut World<Node>, replica: &str, run: &str) -> u64 {
    runs_on(w, replica).into_iter().find(|r| r.run == run.as_bytes()).map_or(0, |r| r.fence)
}

/// The replicas whose fence reached `epoch`, each with the length of `run`'s first owner's segment: I1 says it
/// stays.
fn sealed_lengths(w: &mut World<Node>, run: &str, epoch: u64) -> Vec<(&'static str, u64)> {
    let stale = journal_epoch(1).unwrap();
    let mut sealed = Vec::new();
    for h in HISTORY {
        if fence(w, h, run) >= epoch {
            sealed.push((h, length(w, h, run, stale)));
        }
    }
    sealed
}

/// Heals every fault, opens each run as its last owner, waits until repair has brought every replica the newest
/// segment of each run whole, and checks them: I2 and I3 from their disks, I14 from their summaries.
fn quiesce(w: &mut World<Node>, runs: &[&str], acks: &Acks, log: &LogConfig) -> Report {
    w.heal();
    for run in runs {
        let owner = spawn_owner(w, "e2", run, LAST, Plan::NOTHING, acks);
        assert_eq!(w.join(owner), Ok(Ok(())), "the last owner of {run} opens it");
    }
    let mut tries = 0;
    loop {
        let all = newest(w);
        if all.iter().all(|s| *s == all[0]) {
            break;
        }
        tries += 1;
        assert!(tries < 60, "the replicas do not hold the newest segments after a minute of repair: {all:?}");
        w.run_for(Duration::from_secs(1));
    }
    let mut dumps = Vec::new();
    for h in HISTORY {
        let (files, cfg) = (Arc::new(w.disk(h).dir("journal")), log.clone());
        dumps.push(w.block_on(h, move |_| check::dump(files, cfg)).unwrap().expect("the replica reads back"));
    }
    let report = check::check(&dumps, &acks.borrow(), None);
    report.assert_ok();
    for h in HISTORY {
        let summaries: Vec<_> = runs_on(w, h).into_iter().filter_map(|r| Some((r.run, r.indexed?.summary))).collect();
        let v = check::check_summaries(&report, h, &summaries);
        assert!(v.is_empty(), "I14 on {h}: {v:?}");
    }
    report
}

/// Runs the World for `at_least` ms and up to `spread` more, as the seed picks.
fn wait(w: &mut World<Node>, at_least: u64, spread: u64) {
    let ms = at_least + w.pick(0..spread);
    w.run_for(Duration::from_millis(ms));
}

fn story(test: impl Fn(u64) + Sync) {
    check_seeds(seeds(0..64), test).unwrap();
}

/// (b) An owner suspended mid-run wakes to write at its old epoch, after a newer owner sealed the run: no replica
/// that took the seal grows the old segment (I1), and the old owner stops, fenced.
#[test]
fn a_suspended_owner_writes_at_a_stale_epoch() {
    story(|seed| {
        let setup = Setup::new(2 << 20);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let old = spawn_owner(&mut w, "e1", "b", 1, Plan::values(400), &acks);
        wait(&mut w, 500, 1_500);
        w.suspend("e1", Duration::from_secs(5));
        let opened = Rc::new(RefCell::new(0));
        let (a, o) = (acks.clone(), opened.clone());
        let new = w.spawn_on("e2", move |n| own(n, "b".into(), 2, Plan::values(20), a, Some(o)));
        w.run_until("the new owner opened", WITHIN, |_| *opened.borrow() > 0);
        let stale = journal_epoch(1).unwrap();
        let sealed = sealed_lengths(&mut w, "b", *opened.borrow());
        assert!(sealed.len() >= 2, "a majority took the seal: {sealed:?}");
        assert!(sealed.iter().any(|(_, n)| *n > 2), "the stale owner had written: {sealed:?}");
        assert_eq!(w.join(new), Ok(Ok(())));
        assert!(matches!(w.join(old), Ok(Err(Ended::Fenced(_)))), "the stale owner is fenced");
        for (h, len) in &sealed {
            assert_eq!(length(&mut w, h, "b", stale), *len, "I1: {h} took a stale write after the seal");
        }
        quiesce(&mut w, &["b"], &acks, &setup.journal.log);
    });
}

/// (c) A replica's machine crashes under writes, losing what it had not synced, and comes back to be repaired; then
/// another. Writes go on throughout, on the two that are up.
#[test]
fn replicas_crash_one_after_another_and_are_repaired() {
    story(|seed| {
        let setup = Setup::new(2 << 20);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let owners = [("e1", "c1"), ("e2", "c2"), ("e1", "c3")]
            .map(|(on, run)| spawn_owner(&mut w, on, run, 1, Plan { complete: true, ..Plan::values(600) }, &acks));
        let first = HISTORY[w.pick(0..3) as usize];
        let second = HISTORY.into_iter().filter(|h| *h != first).nth(w.pick(0..2) as usize).unwrap();
        for h in [first, second] {
            wait(&mut w, 500, 1_000);
            w.crash(h, Crash::Machine);
            wait(&mut w, 500, 2_000);
            w.restart(h);
            // Caught up, by the owners' resends or by repair, before the next goes down.
            w.run_for(Duration::from_secs(3));
        }
        for owner in owners {
            assert_eq!(w.join(owner), Ok(Ok(())), "every write is acked");
        }
        quiesce(&mut w, &["c1", "c2", "c3"], &acks, &setup.journal.log);
    });
}

/// (d) An engine is cut off from two replicas while it owns a run; another engine takes the run. Once healed, the
/// first owner's writes are fenced, and nothing it was told is acked goes missing.
#[test]
fn an_engine_partitioned_from_the_majority_is_fenced() {
    story(|seed| {
        let setup = Setup::new(2 << 20);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let old = spawn_owner(&mut w, "e1", "d", 1, Plan::values(400), &acks);
        wait(&mut w, 500, 1_500);
        w.partition(&["e1"], &["h1", "h2"]);
        let opened = Rc::new(RefCell::new(0));
        let (a, o) = (acks.clone(), opened.clone());
        let new = w.spawn_on("e2", move |n| own(n, "d".into(), 2, Plan::values(20), a, Some(o)));
        w.run_until("the new owner opened", WITHIN, |_| *opened.borrow() > 0);
        let stale = journal_epoch(1).unwrap();
        let sealed = sealed_lengths(&mut w, "d", *opened.borrow());
        wait(&mut w, 0, 3_000);
        w.heal();
        assert_eq!(w.join(new), Ok(Ok(())));
        assert!(matches!(w.join(old), Ok(Err(Ended::Fenced(_)))), "the cut-off owner is fenced");
        for (h, len) in &sealed {
            assert_eq!(length(&mut w, h, "d", stale), *len, "I1: {h} took a stale write after the seal");
        }
        quiesce(&mut w, &["d"], &acks, &setup.journal.log);
    });
}

/// (j) Runs finish and others go on, on small segments: retention removes the finished runs, the log copies live
/// records out of mostly dead segments and removes them, and snapshots are taken, while a replica's machine crashes
/// at a random moment of it. The finished runs go from every replica; the rest read back whole.
#[test]
fn retention_compaction_and_snapshots_survive_a_machine_crash() {
    story(|seed| {
        let mut setup = Setup::new(128 << 10);
        // Short, so that runs finished in the story go within it, and long against repair, so that a replica that
        // missed a run's end catches up first.
        setup.journal.keep = Duration::from_secs(12);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let names: Vec<String> = (0..16).map(|i| format!("j{i:02}")).collect();
        // Three runs in four finish at once; the fourth writes on through the story, so that the logs write after
        // retention and copy its records out of segments that are mostly dead.
        let mut finished = Vec::new();
        let mut live = Vec::new();
        for (i, run) in names.iter().enumerate() {
            let plan = match i % 4 {
                3 => Plan { values: 300, bytes: 2 << 10, complete: false, pace: Duration::from_millis(100) },
                _ => Plan { values: 12, bytes: 2 << 10, complete: true, pace: Duration::from_millis(50) },
            };
            let owner = spawn_owner(&mut w, ENGINES[i % 2], run, 1, plan, &acks);
            if i % 4 == 3 { live.push(owner) } else { finished.push(owner) }
        }
        for owner in finished {
            assert_eq!(w.join(owner), Ok(Ok(())));
        }
        let h = HISTORY[w.pick(0..3) as usize];
        wait(&mut w, 0, 16_000);
        w.crash(h, Crash::Machine);
        wait(&mut w, 500, 2_000);
        w.restart(h);
        // Retention after the restart, from the replica's own clock, then a repair round.
        w.run_for(setup.journal.keep + Duration::from_secs(6));
        for owner in live {
            assert_eq!(w.join(owner), Ok(Ok(())));
        }
        let live: Vec<&str> = names.iter().enumerate().filter(|(i, _)| i % 4 == 3).map(|(_, r)| r.as_str()).collect();
        for h in HISTORY {
            let held: Vec<Bytes> = runs_on(&mut w, h).into_iter().map(|r| r.run).collect();
            for (i, run) in names.iter().enumerate() {
                assert_eq!(held.contains(&Bytes::from(run.clone())), i % 4 == 3, "{h} holds {run} after retention");
            }
        }
        let stats: Vec<JournalStats> = HISTORY.map(|h| stats_on(&mut w, h)).to_vec();
        // The counters are this boot's: the crashed replica may have removed its runs before.
        assert!(stats.iter().all(|s| s.snapshots > 0), "a snapshot on each: {stats:?}");
        assert!(stats.iter().any(|s| s.log.copied_bytes > 0), "a log copied live records forward: {stats:?}");
        acks.borrow_mut().retain(|ack| matches!(ack, Ack::Entry { run, .. } if live.contains(&run.as_str())));
        quiesce(&mut w, &live, &acks, &setup.journal.log);
    });
}

/// (m) An engine's messages to one replica vanish, while its answers still come: writes go on with the other two,
/// and repair brings the cut replica level.
#[test]
fn a_one_way_cut_from_an_engine_to_a_replica() {
    story(|seed| {
        let setup = Setup::new(2 << 20);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let cut = HISTORY[w.pick(0..3) as usize];
        w.cut(&["e1"], &[cut]);
        let owners = ["m1", "m2"].map(|run| spawn_owner(&mut w, "e1", run, 1, Plan::values(200), &acks));
        for owner in owners {
            assert_eq!(w.join(owner), Ok(Ok(())), "writes go on without {cut}");
        }
        // The cut replica caught up by repair alone, before the heal.
        let mut tries = 0;
        while stats_on(&mut w, cut).repaired == 0 && tries < 30 {
            w.run_for(Duration::from_secs(1));
            tries += 1;
        }
        assert!(stats_on(&mut w, cut).repaired > 0, "{cut} is repaired");
        quiesce(&mut w, &["m1", "m2"], &acks, &setup.journal.log);
    });
}

/// Starts `run` with a timer due at `due`, through a stub owner on e1.
fn start_timer(w: &mut World<Node>, run: &str, due: u64, acks: &Acks) {
    let entries = [started(run), Entry::TimerStarted { step: 1, wake_at_us: due, at_us: 0 }.encode()];
    let (run, acks) = (run.to_owned(), acks.clone());
    let wrote = w.block_on("e1", move |n| async move {
        let (mut owner, _) =
            Owner::open(n.client.clone(), Bytes::from(run.clone()), 1, writer_config(), WITHIN).await?;
        for (index, body) in owner.append(entries.to_vec(), WITHIN).await?.into_iter().zip(&entries) {
            acks.borrow_mut().push(Ack::Entry { run: run.clone(), index, crc: crc32(body), t: unix_micros() });
        }
        Ok::<_, Ended>(())
    });
    assert_eq!(wrote, Ok(Ok(())));
}

fn first(w: &World<Node>, run: &str) -> String {
    w.read("e1", |n| n.cluster.nodes(&history()).order(run)[0].to_string()).unwrap()
}

/// A due timer is woken once by the waker first in the run's order, not before it is due; with that node's machine
/// gone, another wakes it, at most `late` after.
#[test]
fn a_due_timer_is_woken_and_a_dead_waker_is_covered() {
    story(|seed| {
        let setup = Setup::new(2 << 20);
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let due = w.read("e1", |_| unix_micros()).unwrap() + 3_000_000;
        for run in ["t1", "t2"] {
            start_timer(&mut w, run, due, &acks);
        }
        let (waker, gone) = (first(&w, "t1"), first(&w, "t2"));
        w.crash(&gone, Crash::Machine);
        let late = Duration::from_secs(5);
        w.run_for(Duration::from_secs(3) + late + Duration::from_secs(3));
        let wakes = setup.wakes.lock().unwrap().clone();
        let of = |run: &str| wakes.iter().filter(|(_, r, _)| r == run.as_bytes()).collect::<Vec<_>>();
        let t1 = of("t1");
        assert!(!t1.is_empty(), "t1 was woken");
        assert!(t1.iter().all(|(_, _, at)| *at >= due), "I6: no wake before it is due: {t1:?}");
        if gone != waker {
            assert_eq!(t1[0].0, waker, "the first in t1's order wakes it");
        }
        let t2 = of("t2");
        assert!(t2.iter().any(|(node, _, at)| *node != gone && *at >= due), "another node woke t2: {t2:?}");
        w.restart(&gone);
        quiesce(&mut w, &["t1", "t2"], &acks, &setup.journal.log);
    });
}

/// More timers due at once than one ask of a waker reads, the earliest of them other nodes': each node pages on
/// past them, and wakes each of its own before another node would (`late`).
#[test]
fn due_timers_past_the_first_page_are_woken() {
    story(|seed| {
        let mut setup = Setup::new(2 << 20);
        // An ask reads twice this: four of the nine.
        setup.in_flight = 2;
        let mut w = world(seed, &setup);
        let acks = Acks::default();
        let due = w.read("e1", |_| unix_micros()).unwrap() + 3_000_000;
        let runs = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"];
        for (i, run) in runs.iter().enumerate() {
            start_timer(&mut w, run, due + i as u64, &acks);
        }
        let woken = |run: &str| setup.wakes.lock().unwrap().iter().find(|(_, r, _)| r == run.as_bytes()).cloned();
        w.run_until("every run is woken", WITHIN, |_| runs.iter().all(|run| woken(run).is_some()));
        for run in runs {
            let (node, _, at) = woken(run).unwrap();
            assert_eq!(node, first(&w, run), "the first in {run}'s order wakes it");
            // Before `late`, when another node would.
            assert!((due..due + 5_000_000).contains(&at), "{run} woken at {at}, due at {due}");
        }
        quiesce(&mut w, &runs, &acks, &setup.journal.log);
    });
}
