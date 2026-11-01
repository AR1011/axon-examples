//! durable-1 on the World: one kv node keeps its shards in its Log on its disk, and a client node writes to them
//! while the node's machine crashes twice in a row, its disk fills, its writes panic, or its wall clock is stepped
//! back an hour before a restart. Each story ends with every key answered again, no acknowledged write lost, the
//! Log with nothing left to write, and no Log ended.

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
use kv::node::{self, Mode, Node, Settings, Store};
use kv::slot::{shard_key, shard_of};
use kv::wire::{Answer, Call, ShardApi};

const SECRET: &str = "kv-world-test-secret";
const STORE: &str = "n1";
/// Keys written at once, each by one writer.
const KEYS: usize = 6;
/// Bytes of padding in each value: a story's writes roll the WAL's segments of a little over 1 MiB, and fill a disk.
const PAD: usize = 16000;
/// A short lease so a restarted node owns its shards again within a story; the recorded runs use the shipped 6 s.
const LEASE: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct WNode {
    cluster: Cluster,
    log: Option<ActorHandle<LogMsg>>,
    _node: Option<Rc<Node>>,
    _system: Option<ActorSystem>,
}

type Halts = Arc<Mutex<Vec<String>>>;

fn paxos() -> PaxosConfig {
    let mut paxos = PaxosConfig::new([STORE.to_owned()]);
    paxos.lease = LEASE;
    paxos
}

/// The kv node, its WAL small enough that a story rolls segments and snapshots shards.
async fn boot_store(host: Host, discovery: MemoryDiscovery, halts: Halts) -> WNode {
    let mut settings = Settings {
        id: STORE.into(),
        resp: None,
        bind: host.addr(),
        nodes: vec![STORE.into()],
        mode: Mode::Durable,
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
        halt: Arc::new(move |why| halts.lock().expect("not poisoned").push(why)),
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

/// Per key: the highest value tried, each value acknowledged in order, and reads that went below an acknowledged
/// one.
#[derive(Clone, Default)]
struct Calls {
    tried: u64,
    acked: Vec<u64>,
    stale: Vec<(u64, u64)>,
}

type Ledger = Rc<RefCell<BTreeMap<String, Calls>>>;

/// One writer a key: SET to the next number, then a GET, which must not see less than the last acknowledged.
fn writers(w: &mut World<WNode>, ledger: &Ledger, stop: &Rc<Cell<bool>>) {
    let keys: Vec<String> = ledger.borrow().keys().cloned().collect();
    for key in keys {
        let (ledger, stop) = (ledger.clone(), stop.clone());
        drop(w.spawn_on("client", move |n| async move {
            let shard = shard(&n.cluster, &key);
            let mut next = 0u64;
            while !stop.get() {
                next += 1;
                ledger.borrow_mut().get_mut(&key).expect("a key").tried = next;
                let set =
                    Call::Set(Bytes::copy_from_slice(key.as_bytes()), format!("{next}-{}", "x".repeat(PAD)).into());
                if let Ok(Answer::Ok) = shard.request_within(set, Duration::from_secs(2)).await {
                    ledger.borrow_mut().get_mut(&key).expect("a key").acked.push(next);
                }
                let acked = ledger.borrow()[&key].acked.last().copied().unwrap_or(0);
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

fn acked(ledger: &Ledger) -> Vec<usize> {
    ledger.borrow().values().map(|c| c.acked.len()).collect()
}

fn up(w: &World<WNode>) -> bool {
    w.nodes().any(|(n, node)| n == "client" && node.cluster.view().members.iter().any(|m| m.record.has_kind("shard")))
}

fn world(seed: u64) -> (World<WNode>, Ledger, Rc<Cell<bool>>, Halts, MemoryDiscovery) {
    let discovery = MemoryDiscovery::new();
    let halts = Halts::default();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    let (d, h) = (discovery.clone(), halts.clone());
    w.add(STORE, move |host| boot_store(host, d.clone(), h.clone()));
    let c = discovery.clone();
    w.add("client", move |host| boot_client(host, c.clone()));
    w.run_until("the client sees the shard kind", Duration::from_secs(30), up);
    let ledger: Ledger = Rc::new(RefCell::new((0..KEYS).map(|i| (format!("k{i}"), Calls::default())).collect()));
    let stop = Rc::new(Cell::new(false));
    writers(&mut w, &ledger, &stop);
    w.run_until("every key acknowledged once", Duration::from_secs(30), |_| acked(&ledger).iter().all(|n| *n > 0));
    let before = Duration::from_millis(w.pick(0..400));
    w.run_for(before);
    (w, ledger, stop, halts, discovery)
}

fn status(w: &mut World<WNode>) -> LogStatus {
    let status = w.block_on(STORE, |n| async move { n.log.expect("a Log").ask(LogMsg::Status).await });
    status.expect("the store is up").expect("the Log answers")
}

/// Every key is acknowledged again after `what`, and writes go on a while. Then for each key: acknowledged values
/// rise, no read went below the last acknowledged, the value held is between the last acknowledged and the last
/// tried; the Log has nothing waiting, and no Log ended.
fn settle(
    mut w: World<WNode>,
    ledger: &Ledger,
    stop: &Rc<Cell<bool>>,
    halts: &Halts,
    seed: u64,
    what: &str,
) -> World<WNode> {
    let before = acked(ledger);
    w.run_until(&format!("every key acknowledged after {what}"), Duration::from_secs(30), |_| {
        acked(ledger).iter().zip(&before).all(|(now, before)| now > before)
    });
    let more = Duration::from_millis(w.pick(0..300));
    w.run_for(more);
    stop.set(true);
    w.run_for(Duration::from_secs(3));

    let ledger = ledger.borrow().clone();
    for (key, calls) in ledger {
        let back = calls.acked.windows(2).find(|w| w[1] <= w[0]);
        assert!(back.is_none(), "seed {seed}: {key} acknowledged {back:?} after {what}");
        assert!(calls.stale.is_empty(), "seed {seed}: {key} read below an acknowledged write: {:?}", calls.stale);
        let k = key.clone();
        let held = w.block_on("client", move |n| async move { get(&shard(&n.cluster, &k), &k).await }).unwrap();
        let held = held.unwrap_or_else(|e| panic!("seed {seed}: {key} read: {e}")).unwrap_or(0);
        let last = calls.acked.last().copied().unwrap_or(0);
        assert!(
            (last..=calls.tried).contains(&held),
            "seed {seed}: {key} holds {held}, {last} was acknowledged last and {} tried, after {what}",
            calls.tried
        );
    }
    let st = status(&mut w);
    assert_eq!(
        (st.wal.unwritten_bytes, st.waiting, st.backlog, w.disk(STORE).panics_left()),
        (0, 0, 0, 0),
        "seed {seed}: {st:?}"
    );
    assert!(halts.lock().unwrap().is_empty(), "seed {seed}: a Log ended: {:?}", halts.lock().unwrap());
    println!("log status {what} seed {seed}: {st:?}");
    println!("world digest kv {what} seed {seed}: {:016x}", w.digest());
    w
}

/// The machine crashes at a random moment under writes, and again soon after it restarts: while it boots, replays
/// or opens its shards. What the page cache held is gone each time, and a seeded part of the last unsynced write.
fn two_machine_crashes(seed: u64) -> World<WNode> {
    let (mut w, ledger, stop, halts, _) = world(seed);
    w.log(format_args!("{STORE} Machine"));
    w.crash(STORE, Crash::Machine);
    let down = Duration::from_millis(w.pick(0..300));
    w.run_for(down);
    w.restart(STORE);
    let booting = Duration::from_millis(w.pick(0..1500));
    w.run_for(booting);
    w.log(format_args!("{STORE} Machine again"));
    w.crash(STORE, Crash::Machine);
    w.restart(STORE);
    settle(w, &ledger, &stop, &halts, seed, "two machine crashes")
}

#[test]
fn an_acknowledged_write_survives_two_machine_crashes_back_to_back() {
    check_seeds(seeds(0..64), |seed| drop(two_machine_crashes(seed))).unwrap();
}

/// The disk fills under writes: a segment cannot be filled further, nor the next one or a snapshot made, and writes
/// fail until it has room.
/// In half the seeds the machine crashes meanwhile. Writes wait, none acknowledged is lost, and the Log writes again.
#[test]
fn a_full_disk_holds_writes_until_it_has_room_and_loses_none() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger, stop, halts, _) = world(seed);
        let disk = w.disk(STORE);
        let room = w.pick(0..4000);
        disk.set_capacity(Some(disk.list().iter().map(|(_, len)| len).sum::<u64>() + room));
        let full = Duration::from_millis(w.pick(1000..3000));
        w.run_for(full);
        let st = status(&mut w);
        assert!(st.wal.failed_writes > 0, "seed {seed}: the disk never filled: {st:?}");
        if w.pick(0..2) == 0 {
            w.crash(STORE, Crash::Machine);
            w.restart(STORE);
        }
        let still = Duration::from_millis(w.pick(0..500));
        w.run_for(still);
        disk.set_capacity(None);
        drop(settle(w, &ledger, &stop, &halts, seed, "a full disk"));
    })
    .unwrap();
}

/// Some of the Log's writes panic, its WAL's and its snapshots': each is written again, and the Log goes on.
#[test]
fn a_write_that_panics_is_written_again() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger, stop, halts, _) = world(seed);
        let panics = w.pick(1..5) as u32;
        w.disk(STORE).panic_writes(panics);
        let st = loop {
            w.run_for(Duration::from_millis(100));
            if w.disk(STORE).panics_left() == 0 {
                break status(&mut w);
            }
        };
        assert!(st.wal.failed_writes > 0 || st.snaps > 0, "seed {seed}: {st:?}");
        drop(settle(w, &ledger, &stop, &halts, seed, "panicking writes"));
    })
    .unwrap();
}

/// The wall clock steps back an hour, then the process or machine restarts: the shards open above the promises they
/// made before, and go on.
#[test]
fn a_clock_stepped_back_an_hour_before_a_restart_loses_nothing() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger, stop, halts, discovery) = world(seed);
        w.step_wall(STORE, -3_600_000);
        let after = Duration::from_millis(w.pick(0..300));
        w.run_for(after);
        let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        w.crash(STORE, how);
        // Its record names a later incarnation than the stepped clock gives, so discovery refuses the restart as
        // stale until it expires, as a seed's health check expires a silent node.
        discovery.expire(STORE);
        w.restart(STORE);
        drop(settle(w, &ledger, &stop, &halts, seed, "a clock stepped back"));
    })
    .unwrap();
}

#[test]
#[ignore = "a seed replays only in a build with RUSTFLAGS='--cfg tokio_unstable': run it there with --ignored"]
fn the_same_seed_gives_the_same_trace() {
    check_seeds(seeds(0..2), |seed| {
        let (one, two) = (two_machine_crashes(seed), two_machine_crashes(seed));
        if let Some(report) = one.divergence(&two) {
            panic!("{report}");
        }
    })
    .unwrap();
}
