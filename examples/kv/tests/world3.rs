//! durable-3 on the World: three kv nodes, each a Log of every shard and an acceptor, and a client node that writes
//! to the shards and reads them back while an owner crashes and comes back, sleeps past its lease, is cut off alone,
//! or while a follower crashes, falls far behind, fills its disk, panics in its writes or has its clock stepped back,
//! and while two of the three are down. Each story ends with every key answered again, no acknowledged write lost, no
//! read below an acknowledged write (quorum reads), every Log with nothing left to write, no Log ended, and the three
//! Logs holding the same entries for every shard.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::LogConfig;
use axon::{ActorHandle, ActorSystem};
use axon_cluster::{Cluster, ClusterConfig, KeyedRef, MemoryDiscovery, PaxosConfig, PaxosLeases, Placement};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;
use kv::log::{LogMsg, LogSettings, LogStatus};
use kv::node::{self, Mode, Node, Reads, Settings, Store};
use kv::slot::{shard_key, shard_of};
use kv::wire::{Answer, Call, ShardApi};

const SECRET: &str = "kv-world-test-secret";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
/// Keys written at once, each by one writer.
const KEYS: usize = 6;
/// Bytes of padding in each value: a story's writes roll the WAL's segments of a little over 1 MiB.
const PAD: usize = 16000;
/// A short lease so a shard moves, and moves back, within a story; the recorded runs use the shipped 6 s.
const LEASE: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct WNode {
    cluster: Cluster,
    log: Option<ActorHandle<LogMsg>>,
    _node: Option<Rc<Node>>,
    _system: Option<ActorSystem>,
}

type Halts = Arc<Mutex<Vec<String>>>;

/// How a story's nodes run.
#[derive(Clone, Copy)]
struct Opts {
    reads: Reads,
    /// Committed entries an owner keeps for a Log that is behind: a few, so one that was down gets an Install.
    keep: u64,
    /// Keys only in shards whose owner is not n3, so n3 is a follower of every one written.
    spare_n3: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self { reads: Reads::Quorum, keep: 4096, spare_n3: false }
    }
}

fn paxos() -> PaxosConfig {
    let mut paxos = PaxosConfig::new(NODES.map(String::from));
    paxos.lease = LEASE;
    paxos
}

/// A storage node, its WAL small enough that a story rolls segments and snapshots shards.
async fn boot_node(id: &'static str, host: Host, discovery: MemoryDiscovery, halts: Halts, opts: Opts) -> WNode {
    let mut settings = Settings {
        id: id.into(),
        resp: None,
        bind: host.addr(),
        nodes: NODES.map(String::from).to_vec(),
        mode: Mode::Durable,
        reads: opts.reads,
        keep: opts.keep,
        lease: LEASE,
        ..Settings::default()
    };
    (settings.limits.value, settings.limits.command) = (32 << 10, 64 << 10);
    settings.log = LogSettings {
        wal: LogConfig {
            // About the smallest the Log allows for a record of the largest command and 1 MiB more.
            segment_bytes: (1 << 20) + (256 << 10),
            max_record: (1 << 20) + (64 << 10),
            max_write_bytes: 2 << 20,
            max_unwritten_bytes: 4 << 20,
            ..kv::log::wal_config()
        },
        wal_floor: 1 << 20,
        ..LogSettings::default()
    };
    let disk = host.disk();
    let store = Store {
        leases: Arc::new(disk.dir("leases")),
        wal: Arc::new(disk.dir("wal")),
        snaps: Arc::new(disk.dir("snap")),
        halt: Arc::new(move |why| halts.lock().expect("not poisoned").push(format!("{id}: {why}"))),
    };
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let node = node::start_on(&settings, host.runtime(), transport, discovery, store).await.expect("the node starts");
    WNode { cluster: node.cluster.clone(), log: node.log.clone(), _node: Some(Rc::new(node)), _system: None }
}

/// A node that only calls: the same acceptor list and lease, no shards.
async fn boot_client(host: Host, discovery: MemoryDiscovery) -> WNode {
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(SharedSecret::new(SECRET).unwrap()), discovery);
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos()).expect("a lease config")));
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    WNode { cluster, log: None, _node: None, _system: Some(system) }
}

fn shard(cluster: &Cluster, key: &str) -> KeyedRef<ShardApi> {
    cluster.keyed::<ShardApi>("shard", &shard_key(shard_of(key.as_bytes())), Bytes::new(), &Placement::default())
}

async fn get(shard: &KeyedRef<ShardApi>, key: &str) -> Result<Option<u64>, String> {
    match shard.request_within(Call::Get(Bytes::copy_from_slice(key.as_bytes())), Duration::from_secs(2)).await {
        Ok(Answer::Value(v)) => Ok(v.map(|v| {
            let v = std::str::from_utf8(&v).expect("a value written here");
            v.split('-').next().and_then(|n| n.parse().ok()).expect("a value written here")
        })),
        other => Err(format!("{other:?}")),
    }
}

/// Per key: the highest value tried, each value acknowledged in order with the World's time, and reads that went
/// below an acknowledged one.
#[derive(Clone, Default)]
struct Calls {
    tried: u64,
    acked: Vec<(u64, Duration)>,
    stale: Vec<(u64, u64)>,
}

type Ledger = Rc<RefCell<BTreeMap<String, Calls>>>;

/// One writer a key: SET to the next number, then a GET, which must not see less than the last acknowledged.
fn writers(w: &mut World<WNode>, ledger: &Ledger, stop: &Rc<Cell<bool>>, clock: &Rc<Cell<Duration>>) {
    let keys: Vec<String> = ledger.borrow().keys().cloned().collect();
    for key in keys {
        let (ledger, stop, clock) = (ledger.clone(), stop.clone(), clock.clone());
        drop(w.spawn_on("client", move |n| async move {
            let shard = shard(&n.cluster, &key);
            let mut next = 0u64;
            while !stop.get() {
                next += 1;
                ledger.borrow_mut().get_mut(&key).expect("a key").tried = next;
                let set =
                    Call::Set(Bytes::copy_from_slice(key.as_bytes()), format!("{next}-{}", "x".repeat(PAD)).into());
                if let Ok(Answer::Ok) = shard.request_within(set, Duration::from_secs(2)).await {
                    ledger.borrow_mut().get_mut(&key).expect("a key").acked.push((next, clock.get()));
                }
                let acked = ledger.borrow()[&key].acked.last().map_or(0, |a| a.0);
                if let Ok(read) = get(&shard, &key).await
                    && read.unwrap_or(0) < acked
                {
                    ledger.borrow_mut().get_mut(&key).expect("a key").stale.push((acked, read.unwrap_or(0)));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }
}

/// One reader a key, which finds the shard's owner afresh for each GET, as a new client does: it may reach another
/// owner than the writer last did, such as one that wakes after its shard moved (I5). A GET must not see less than
/// the last write acknowledged before it was sent.
fn readers(w: &mut World<WNode>, ledger: &Ledger, stop: &Rc<Cell<bool>>) {
    let keys: Vec<String> = ledger.borrow().keys().cloned().collect();
    for key in keys {
        let (ledger, stop) = (ledger.clone(), stop.clone());
        drop(w.spawn_on("client", move |n| async move {
            while !stop.get() {
                let acked = ledger.borrow()[&key].acked.last().map_or(0, |a| a.0);
                if let Ok(read) = get(&shard(&n.cluster, &key), &key).await
                    && read.unwrap_or(0) < acked
                {
                    ledger.borrow_mut().get_mut(&key).expect("a key").stale.push((acked, read.unwrap_or(0)));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }
}

fn acked(ledger: &Ledger) -> Vec<usize> {
    ledger.borrow().values().map(|c| c.acked.len()).collect()
}

/// The node that owns `key`'s shard, as the client sees it.
fn owner(w: &World<WNode>, key: &str) -> String {
    let shard = shard_key(shard_of(key.as_bytes()));
    let owner = w.read("client", |n| n.cluster.owner("shard", &shard, &Placement::default()));
    owner.flatten().expect("the client sees an owner").to_string()
}

struct Story {
    w: World<WNode>,
    ledger: Ledger,
    stop: Rc<Cell<bool>>,
    /// The World's time, kept current for the writers, which record when each write was acknowledged.
    clock: Rc<Cell<Duration>>,
    halts: Halts,
    discovery: MemoryDiscovery,
    seed: u64,
}

impl Story {
    fn new(seed: u64, opts: Opts) -> Self {
        let discovery = MemoryDiscovery::new();
        let halts = Halts::default();
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        for id in NODES {
            let (d, h) = (discovery.clone(), halts.clone());
            w.add(id, move |host| boot_node(id, host, d.clone(), h.clone(), opts));
        }
        let c = discovery.clone();
        w.add("client", move |host| boot_client(host, c.clone()));
        w.run_until("the client sees three nodes with shards", Duration::from_secs(30), |w| {
            w.read("client", |n| n.cluster.view().members.iter().filter(|m| m.record.has_kind("shard")).count() == 3)
                .unwrap_or(false)
        });
        let keys: Vec<String> =
            (0..).map(|i| format!("k{i}")).filter(|k| !opts.spare_n3 || owner(&w, k) != "n3").take(KEYS).collect();
        let ledger: Ledger = Rc::new(RefCell::new(keys.into_iter().map(|k| (k, Calls::default())).collect()));
        let (stop, clock) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(Duration::ZERO)));
        writers(&mut w, &ledger, &stop, &clock);
        readers(&mut w, &ledger, &stop);
        let mut story = Story { w, ledger, stop, clock, halts, discovery, seed };
        story
            .until("every key acknowledged once", Duration::from_secs(30), |s| acked(&s.ledger).iter().all(|n| *n > 0));
        let before = Duration::from_millis(story.w.pick(0..400));
        story.run_for(before);
        story
    }

    fn run_for(&mut self, d: Duration) {
        let end = self.w.now() + d;
        while self.w.now() < end {
            self.w.run_for(Duration::from_millis(10));
            self.clock.set(self.w.now());
        }
    }

    fn until(&mut self, what: &str, within: Duration, done: impl Fn(&Self) -> bool) {
        let end = self.w.now() + within;
        while !done(self) {
            assert!(self.w.now() < end, "seed {}: not {what} within {within:?}\n{}", self.seed, self.w.trace_tail(40));
            self.run_for(Duration::from_millis(10));
        }
    }

    fn pick_ms(&mut self, range: std::ops::Range<u64>) -> Duration {
        Duration::from_millis(self.w.pick(range))
    }

    fn crash(&mut self, node: &str, how: Crash) {
        self.w.log(format_args!("{node} {how:?}"));
        self.w.crash(node, how);
    }

    fn status(&mut self, node: &str) -> LogStatus {
        let status = self.w.block_on(node, |n| async move { n.log.expect("a Log").ask(LogMsg::Status).await });
        status.expect("the node is up").expect("the Log answers")
    }

    fn digest(&mut self, node: &str) -> Vec<(u64, u64)> {
        let digest = self.w.block_on(node, |n| async move { n.log.expect("a Log").ask(LogMsg::Digest).await });
        digest.expect("the node is up").expect("the Log answers")
    }

    /// Acknowledgements of writes between `from` and `to` in the World's time.
    fn acked_between(&self, from: Duration, to: Duration) -> usize {
        self.ledger.borrow().values().map(|c| c.acked.iter().filter(|a| (from..to).contains(&a.1)).count()).sum()
    }

    /// Every key is acknowledged again after `what`, and writes go on a while. Then for each key: acknowledged
    /// values rise, no read went below the last acknowledged (unless `stale_ok`), the value held is between the last
    /// acknowledged and the last tried; every Log has nothing waiting, no Log ended, and the three Logs agree on
    /// every shard once the last commit has reached them.
    fn settle(mut self, what: &str, stale_ok: bool) -> World<WNode> {
        let seed = self.seed;
        let before = acked(&self.ledger);
        self.until(&format!("every key acknowledged after {what}"), Duration::from_secs(40), |s| {
            acked(&s.ledger).iter().zip(&before).all(|(now, before)| now > before)
        });
        let more = self.pick_ms(0..300);
        self.run_for(more);
        self.stop.set(true);
        self.run_for(Duration::from_secs(3));

        let ledger = self.ledger.borrow().clone();
        let mut stale = 0;
        for (key, calls) in ledger {
            let back = calls.acked.windows(2).find(|w| w[1].0 <= w[0].0);
            assert!(back.is_none(), "seed {seed}: {key} acknowledged {back:?} after {what}");
            stale += calls.stale.len();
            assert!(
                stale_ok || calls.stale.is_empty(),
                "seed {seed}: {key} read below an acknowledged write: {:?}",
                calls.stale
            );
            let k = key.clone();
            let held = self.w.block_on("client", move |n| async move { get(&shard(&n.cluster, &k), &k).await });
            let held = held.unwrap().unwrap_or_else(|e| panic!("seed {seed}: {key} read: {e}")).unwrap_or(0);
            let last = calls.acked.last().map_or(0, |a| a.0);
            assert!(
                (last..=calls.tried).contains(&held),
                "seed {seed}: {key} holds {held}, {last} was acknowledged last and {} tried, after {what}",
                calls.tried
            );
        }
        if stale_ok {
            println!("stale reads {what} seed {seed}: {stale}");
        }
        assert!(self.halts.lock().unwrap().is_empty(), "seed {seed}: a Log ended: {:?}", self.halts.lock().unwrap());
        // Every Log ends with nothing waiting, and the three agree. A Log learns the last commit from the owner's
        // next Append, a heartbeat, a second at most, and writes it as a record of its own.
        let mut tries = 0;
        loop {
            let busy: Vec<_> = NODES
                .iter()
                .map(|n| {
                    let st = self.status(n);
                    let left = (st.wal.unwritten_bytes, st.waiting, st.backlog, st.installing, st.held);
                    (n, left, self.w.disk(n).panics_left())
                })
                .filter(|(_, left, panics)| *left != (0, 0, 0, 0, 0) || *panics != 0)
                .collect();
            let digests: Vec<_> = NODES.iter().map(|n| self.digest(n)).collect();
            let differ: Vec<usize> =
                (0..digests[0].len()).filter(|&s| digests.iter().any(|d| d[s] != digests[0][s])).collect();
            if busy.is_empty() && differ.is_empty() {
                break;
            }
            tries += 1;
            assert!(
                tries < 30,
                "seed {seed}: after {what}, Logs busy: {busy:?}; Logs differ on shards {differ:?}: {:?}",
                differ.iter().take(3).map(|&s| digests.iter().map(|d| d[s]).collect::<Vec<_>>()).collect::<Vec<_>>()
            );
            self.run_for(Duration::from_millis(200));
        }
        println!("world digest kv3 {what} seed {seed}: {:016x}", self.w.digest());
        self.w
    }
}

/// The owner of a written key crashes, its process or its machine, and comes back at a random moment: at once,
/// before its shards could move (a restart within its own lease), or after they moved to the other two. Its shards
/// move back to it, onto its Log, which is behind.
fn owner_crash_and_rejoin(seed: u64) -> World<WNode> {
    let mut s = Story::new(seed, Opts::default());
    let node = owner(&s.w, "k0");
    let how = if s.w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    s.crash(&node, how);
    let down = s.pick_ms(0..4000);
    s.run_for(down);
    s.w.restart(&node);
    s.settle("an owner crash and rejoin", false)
}

#[test]
fn an_owner_that_crashes_and_rejoins_loses_no_acknowledged_write() {
    check_seeds(seeds(0..64), |seed| drop(owner_crash_and_rejoin(seed))).unwrap();
}

/// The owner of a written key stops for longer than its lease, then goes on as if it still held it: under
/// `suspend` its clocks stand still too, under `pause` they run on. The other two have moved its shards meanwhile.
fn zombie(seed: u64, reads: Reads, suspend: bool) -> World<WNode> {
    let mut s = Story::new(seed, Opts { reads, ..Opts::default() });
    let node = owner(&s.w, "k0");
    let away = s.pick_ms(1500..4000);
    if suspend {
        s.w.suspend(&node, away);
    } else {
        s.w.pause(&node, away);
    }
    s.run_for(away);
    // Lease reads may be stale only from a node whose clock stood still (I5): never with quorum reads.
    let stale_ok = reads == Reads::Lease && suspend;
    s.settle(if suspend { "a suspended owner" } else { "a paused owner" }, stale_ok)
}

#[test]
fn a_suspended_owner_acknowledges_nothing_a_newer_owner_loses_and_quorum_reads_stay_fresh() {
    check_seeds(seeds(0..64), |seed| drop(zombie(seed, Reads::Quorum, true))).unwrap();
}

#[test]
fn lease_reads_stay_fresh_when_the_paused_owners_clock_runs_on() {
    check_seeds(seeds(0..64), |seed| drop(zombie(seed, Reads::Lease, false))).unwrap();
}

#[test]
fn lease_reads_from_a_suspended_owner_lose_no_acknowledged_write() {
    check_seeds(seeds(0..64), |seed| drop(zombie(seed, Reads::Lease, true))).unwrap();
}

/// The owner of a written key is cut off from the other two and the client, then the cut heals.
#[test]
fn an_owner_alone_in_a_partition_loses_no_acknowledged_write() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let node = owner(&s.w, "k0");
        let rest: Vec<&str> = NODES.iter().copied().filter(|n| *n != node).chain(["client"]).collect();
        s.w.log(format_args!("partition {node}"));
        s.w.partition(&[node.as_str()], &rest);
        let cut = s.pick_ms(500..5000);
        s.run_for(cut);
        s.w.heal();
        drop(s.settle("a partition", false));
    })
    .unwrap();
}

/// I4: an owner crashes, the new owners promise on the two left, and one of those machines crashes soon after, so
/// it may lose all it had not synced. Then both come back. A promise answered was on disk.
#[test]
fn a_follower_machine_crash_right_after_a_promise_loses_nothing() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let node = owner(&s.w, "k0");
        s.crash(&node, Crash::Process);
        let promised = LEASE + s.pick_ms(0..1500);
        s.run_for(promised);
        let others: Vec<&str> = NODES.iter().copied().filter(|n| *n != node).collect();
        let follower = others[s.w.pick(0..2) as usize];
        s.crash(follower, Crash::Machine);
        let down = s.pick_ms(0..1000);
        s.run_for(down);
        s.w.restart(follower);
        s.w.restart(&node);
        drop(s.settle("a follower machine crash after a promise", false));
    })
    .unwrap();
}

/// The epoch of each written key's shard on its owner.
fn epochs(s: &Story) -> Vec<(String, Option<u64>)> {
    let keys: Vec<String> = s.ledger.borrow().keys().cloned().collect();
    keys.iter()
        .map(|k| {
            let (node, shard) = (owner(&s.w, k), shard_key(shard_of(k.as_bytes())));
            let hosted = s.w.read(&node, |n| n.cluster.hosted_keys("shard", &shard, None, 1).0).unwrap_or_default();
            (node, hosted.into_iter().find(|h| h.key == shard).and_then(|h| h.epoch))
        })
        .collect()
}

/// I12: a follower of every written shard crashes under writes and comes back. The other two commit meanwhile, and
/// no written shard moves.
#[test]
fn a_follower_restart_moves_no_written_shard() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts { spare_n3: true, ..Opts::default() });
        let before = epochs(&s);
        let how = if s.w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        s.crash("n3", how);
        let down = s.pick_ms(0..3000);
        s.run_for(down);
        let during = s.acked_between(s.w.now() - down, s.w.now());
        s.w.restart("n3");
        let back = s.pick_ms(1000..3000);
        s.run_for(back);
        assert_eq!(before, epochs(&s), "seed {seed}: a written shard moved");
        let w = s.settle("a follower restart", false);
        // Writes went on with two of the three Logs, unless n3 was down for too short a time to see any.
        assert!(during > 0 || down < Duration::from_millis(500), "seed {seed}: no write acknowledged in {down:?}");
        drop(w);
    })
    .unwrap();
}

/// A follower is down while the owners commit past the few entries they keep: on its return each sends it the map
/// (an Install, written as the shard's snapshot), then its machine crashes and it comes back once more. Its Log then
/// holds what the other two hold.
#[test]
fn a_follower_far_behind_installs_the_map_and_keeps_it_through_a_crash() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts { keep: 4, spare_n3: true, ..Opts::default() });
        s.crash("n3", Crash::Process);
        let before = acked(&s.ledger);
        s.until("each key acknowledged 10 times more", Duration::from_secs(30), |s| {
            acked(&s.ledger).iter().zip(&before).all(|(now, before)| *now >= before + 10)
        });
        s.w.restart("n3");
        let catch_up = s.pick_ms(1500..3000);
        s.run_for(catch_up);
        // Its Log replays and its acceptor warms before the owners reach it.
        let until = s.w.now() + Duration::from_secs(10);
        while s.status("n3").installs == 0 {
            assert!(s.w.now() < until, "seed {seed}: n3 installed no map\n{}", s.w.trace_tail(40));
            s.run_for(Duration::from_millis(100));
        }
        s.crash("n3", Crash::Machine);
        let down = s.pick_ms(0..500);
        s.run_for(down);
        s.w.restart("n3");
        drop(s.settle("a follower far behind", false));
    })
    .unwrap();
}

/// One node's disk fills under writes: its Log's writes fail until it has room. In half the seeds the machine
/// crashes meanwhile. The other two go on, none acknowledged is lost, and the Log writes again.
#[test]
fn a_full_disk_on_one_node_loses_nothing() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let node = NODES[s.w.pick(0..3) as usize];
        let disk = s.w.disk(node);
        let room = s.w.pick(0..4000);
        disk.set_capacity(Some(disk.list().iter().map(|(_, len)| len).sum::<u64>() + room));
        let full = s.pick_ms(1000..3000);
        s.run_for(full);
        let st = s.status(node);
        assert!(st.wal.failed_writes > 0, "seed {seed}: the disk never filled: {st:?}");
        if s.w.pick(0..2) == 0 {
            s.crash(node, Crash::Machine);
            s.w.restart(node);
        }
        let still = s.pick_ms(0..500);
        s.run_for(still);
        disk.set_capacity(None);
        drop(s.settle("a full disk", false));
    })
    .unwrap();
}

/// Some of one node's writes panic, its WAL's and its snapshots': each is written again.
#[test]
fn a_write_that_panics_on_one_node_is_written_again() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let node = NODES[s.w.pick(0..3) as usize];
        let panics = s.w.pick(1..5) as u32;
        s.w.disk(node).panic_writes(panics);
        s.until("every panic spent", Duration::from_secs(30), |s| s.w.disk(node).panics_left() == 0);
        drop(s.settle("panicking writes", false));
    })
    .unwrap();
}

/// Two of the three crash: no majority of Logs or acceptors, so nothing is acknowledged until one comes back. The
/// shards on the one left stop for want of a majority and start again on it, under the lease it still holds: a
/// start at the same epoch, which its ballot tells apart.
#[test]
fn with_two_logs_down_nothing_is_acknowledged_and_nothing_lost() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let left = NODES[s.w.pick(0..3) as usize];
        let down: Vec<&str> = NODES.iter().copied().filter(|n| *n != left).collect();
        for node in &down {
            let how = if s.w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
            s.crash(node, how);
        }
        let from = s.w.now();
        let outage = s.pick_ms(1000..8000);
        s.run_for(outage);
        // A write committed before the crash may be acknowledged just after it.
        let acked = s.acked_between(from + Duration::from_millis(500), s.w.now());
        assert_eq!(acked, 0, "seed {seed}: {acked} writes acknowledged with one Log of three");
        for node in down {
            s.w.restart(node);
        }
        drop(s.settle("two Logs down", false));
    })
    .unwrap();
}

/// One node's wall clock steps back an hour, then it restarts: its shards open above the promises the Logs made
/// before, and go on.
#[test]
fn a_clock_stepped_back_an_hour_before_a_restart_loses_nothing() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed, Opts::default());
        let node = NODES[s.w.pick(0..3) as usize];
        s.w.step_wall(node, -3_600_000);
        let after = s.pick_ms(0..300);
        s.run_for(after);
        let how = if s.w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        s.crash(node, how);
        // Its record names a later incarnation than the stepped clock gives, so discovery refuses the restart as
        // stale until it expires, as a seed's health check expires a silent node.
        s.discovery.expire(node);
        s.w.restart(node);
        drop(s.settle("a clock stepped back", false));
    })
    .unwrap();
}

#[test]
#[ignore = "a seed replays only in a build with RUSTFLAGS='--cfg tokio_unstable': run it there with --ignored"]
fn the_same_seed_gives_the_same_trace() {
    check_seeds(seeds(0..2), |seed| {
        let (one, two) = (owner_crash_and_rejoin(seed), owner_crash_and_rejoin(seed));
        if let Some(report) = one.divergence(&two) {
            panic!("{report}");
        }
    })
    .unwrap();
}
