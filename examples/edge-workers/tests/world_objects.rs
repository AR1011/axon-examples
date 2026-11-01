//! Durable objects under faults, on the World: a control node (the objects' store), two gateways and three runners
//! as real nodes on virtual time, with the fake engine and `PaxosLeases` at its shipped timings. The control node
//! and the gateways are the acceptors, as in `scripts/run.sh`. Callers add one to a counter object through the
//! gateways, each waiting for its answer before it sends the next, while the object's runner is killed, paused past
//! its lease or cut off from the acceptors, and while the store is cut off, full or killed. What the callers were
//! told is then held against the store's own file. A seed picks who, when, how, and for how long.

#[path = "support/world.rs"]
mod on_world;
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon_cluster::{MemoryDiscovery, PaxosConfig, PaxosLeases};
use axon_testkit::world::{Crash, Host, Task, World};
use axon_testkit::{check_seeds, seeds};
use edge_workers::engine::Request;
use edge_workers::engine::fake::FakeEngine;
use edge_workers::gateway::{Call, Response};
use edge_workers::node::{self, Client, Roles};
use edge_workers::store;
use edge_workers::wire::{OBJECTS, STORE, StoreApi, StoreAsk, StoreTold, Upload, object_key};
use on_world::Node;
use serde_json::Value;
use support::counter;

const CONTROL: &str = "control";
const GATEWAYS: [&str; 2] = ["gw-1", "gw-2"];
const RUNNERS: [&str; 3] = ["r-1", "r-2", "r-3"];
/// The nodes that grant the objects' leases: a majority of them stays when any one node goes.
const ACCEPTORS: [&str; 3] = [CONTROL, "gw-1", "gw-2"];
const WITHIN: Duration = Duration::from_secs(60);
/// A request's deadline where the story is about an owner that goes: longer than the longest pause, so a request
/// its owner had in hand when it stopped is still wanted when it wakes, and its write is the fence's to refuse.
const PATIENT: Duration = Duration::from_secs(15);
/// A request's deadline where the story is about the store going: each request that meets the outage takes this.
const SHORT: Duration = Duration::from_secs(2);
/// Between a caller's answer and its next request.
const THINK: Duration = Duration::from_millis(200);
/// A step of the World and a timer's tick, which an answer at its deadline may be late by.
const SLACK: Duration = Duration::from_millis(3);
/// The gateways stop sending to a machine that went silent within this.
const GIVES_UP: Duration = Duration::from_secs(8);
/// After its runner went, or the store came back, the object counts again within this: a lease (6 s) and what the
/// callers take to see the runner gone, or an acceptor's wait after its start, and a request's own time.
const RECOVERS: Duration = Duration::from_secs(30);

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let name = host.name().to_owned();
    let roles = Roles { control: name == CONTROL, gateway: name.starts_with("gw-"), runner: name.starts_with("r-") };
    let engine = FakeEngine::new().scoped("counter", counter);
    on_world::boot(host, discovery, roles, engine, |cfg, _, disk| {
        let mut paxos = PaxosConfig::new(ACCEPTORS);
        if ACCEPTORS.contains(&name.as_str()) {
            paxos.store = Some(Arc::new(disk.dir("leases")));
        }
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    })
    .await
}

fn node<'a>(w: &'a World<Node>, name: &str) -> &'a Node {
    w.nodes().find(|(node, _)| *node == name).unwrap_or_else(|| panic!("{name} is up")).1
}

fn gateways_see(w: &mut World<Node>, runners: usize) {
    for gateway in GATEWAYS {
        let seen = w.spawn_on(gateway, move |n| async move {
            n.node.gateway.as_ref().unwrap().wait_runners(|ready| *ready == runners).await;
        });
        w.join(seen).expect("the gateway is up");
    }
}

/// The six nodes, `counter` served everywhere, and the acceptors past the wait after their start.
fn world(seed: u64) -> World<Node> {
    let names: Vec<_> = [CONTROL].into_iter().chain(GATEWAYS).chain(RUNNERS).collect();
    let mut w = on_world::booted(seed, &names, boot);
    let upload = Upload { name: "counter".into(), source: Some("counter".into()), deploy: true, ..Upload::default() };
    let deployed = w.block_on(GATEWAYS[0], |n| async move { Client::new(&n.cluster).upload(upload).await });
    assert_eq!(deployed.expect("the gateway is up").unwrap(), (1, Some(1)));
    w.run_until("the counter served on every gateway and runner", WITHIN, |w| {
        let serves = |name: &str| node(w, name).node.serving.as_ref().and_then(|serving| serving.seq("counter"));
        GATEWAYS.into_iter().chain(RUNNERS).all(|name| serves(name) == Some(1))
    });
    gateways_see(&mut w, 3);
    w.run_until("the acceptors past their wait", WITHIN, |w| ACCEPTORS.iter().all(|a| !node(w, a).cluster.warming()));
    w
}

/// What the test and its callers share. The test moves `phase` on at each fault and each repair; `counted` is the
/// latest phase in which a request was sent and then answered with a count.
struct Load {
    stop: AtomicBool,
    phase: AtomicU64,
    counted: AtomicU64,
}

#[derive(Debug)]
struct Sample {
    object: &'static str,
    /// The phase it was sent in and the phase it was answered in.
    sent: u64,
    answered: u64,
    took: Duration,
    deadline: Duration,
    response: Response,
}

impl Sample {
    /// The count the caller was told the object reached with its request.
    fn count(&self) -> Option<u64> {
        (self.response.status == 200).then(|| {
            let said = std::str::from_utf8(&self.response.body).expect("a count");
            said.parse().unwrap_or_else(|_| panic!("a count: {self:?}"))
        })
    }
}

/// One POST for `object` after the other until told to stop, `THINK` apart.
async fn caller(node: Node, load: Arc<Load>, id: usize, object: &'static str, deadline: Duration) -> Vec<Sample> {
    let gateway = node.node.gateway.expect("a gateway");
    let mut samples = Vec::new();
    while !load.stop.load(Relaxed) {
        let request = Request { method: "POST".into(), ..Request::get(&format!("/{object}")) };
        let call =
            Call { account: format!("account-{id}"), worker: "counter".into(), request, deadline: Some(deadline) };
        let sent = load.phase.load(Relaxed);
        let began = tokio::time::Instant::now();
        let response = gateway.call(call).await;
        let took = began.elapsed();
        if response.status == 200 {
            load.counted.fetch_max(sent, Relaxed);
        }
        samples.push(Sample { object, sent, answered: load.phase.load(Relaxed), took, deadline, response });
        tokio::time::sleep(THINK).await;
    }
    samples
}

struct Running {
    load: Arc<Load>,
    callers: Vec<Task<Vec<Sample>>>,
}

/// `each` callers per object, in turn on the two gateways.
fn start_load(w: &mut World<Node>, objects: &[&'static str], each: usize, deadline: Duration) -> Running {
    let load = Arc::new(Load { stop: AtomicBool::new(false), phase: AtomicU64::new(1), counted: AtomicU64::new(0) });
    let all = objects.iter().flat_map(|object| std::iter::repeat_n(*object, each));
    let callers = all.enumerate().map(|(id, object)| {
        let load = load.clone();
        w.spawn_on(GATEWAYS[id % 2], move |n| caller(n, load, id, object, deadline))
    });
    Running { callers: callers.collect(), load }
}

/// Stops the callers and gives what each request came to. Every request has its answer: a caller that never got
/// one would not end.
fn end_load(w: &mut World<Node>, running: Running) -> Vec<Sample> {
    running.load.stop.store(true, Relaxed);
    let mut samples = Vec::new();
    for caller in running.callers {
        samples.extend(w.join(caller).expect("the gateway is up"));
    }
    samples
}

/// From here on the callers' requests are of a new phase.
fn next_phase(load: &Load) -> u64 {
    load.phase.fetch_add(1, Relaxed) + 1
}

/// Runs until a request sent in `phase` was answered with a count, and says how long that took.
fn counted_in(w: &mut World<Node>, load: &Arc<Load>, phase: u64, what: &str) -> Duration {
    let (at, load) = (w.now(), load.clone());
    w.run_until(what, RECOVERS, move |_| load.counted.load(Relaxed) >= phase);
    w.now() - at
}

/// Where the object runs, and the epoch of its lease: the newest, when a node that lost it has not noticed yet.
/// Two nodes never run it under one epoch: a lease is one node's.
fn owner(w: &World<Node>, object: &str) -> Option<(String, u64)> {
    let key = object_key("counter", "Counter", object);
    let hosts = w.nodes().filter_map(|(name, n)| {
        let (keys, _) = n.cluster.hosted_keys(OBJECTS, &key, None, 1);
        let hosted = keys.into_iter().find(|hosted| hosted.key == key)?;
        Some((name.to_owned(), hosted.epoch.expect("a lease")))
    });
    let hosts: Vec<(String, u64)> = hosts.collect();
    let epochs: BTreeSet<u64> = hosts.iter().map(|(_, epoch)| *epoch).collect();
    assert_eq!(epochs.len(), hosts.len(), "{object} runs on two nodes under one epoch: {hosts:?}");
    hosts.into_iter().max_by_key(|(_, epoch)| *epoch)
}

/// Where the object runs, once it does: between two requests it may be nowhere, as when the runner its key is
/// placed on is back and takes it from the one that ran it meanwhile.
fn runs_on(w: &mut World<Node>, object: &str) -> (String, u64) {
    w.run_until("the object running on a runner", RECOVERS, |w| owner(w, object).is_some());
    owner(w, object).expect("it runs")
}

/// The count the store answers for the object, asked from a gateway until the control node is there to answer.
fn stored(w: &mut World<Node>, object: &str) -> u64 {
    let key = object_key("counter", "Counter", object);
    let read = w.block_on(GATEWAYS[0], move |n| async move {
        let store = node::on_control::<StoreApi>(&n.cluster, STORE);
        store.request_until(StoreAsk::Read { object: key }, tokio::time::Instant::now() + WITHIN).await
    });
    match read.expect("the gateway is up") {
        Ok(StoreTold::Snapshot { entries, .. }) => count(&serde_json::json!(entries)).unwrap_or(0),
        other => panic!("the store did not answer for {object}: {other:?}"),
    }
}

/// The value of `n` among pairs of a key and a value, as a snapshot's entries and a write's ops are.
fn count(pairs: &Value) -> Option<u64> {
    let n = pairs.as_array()?.iter().find(|pair| pair[0] == "n")?;
    n[1].as_str()?.parse().ok()
}

/// The object's file on the control node's disk, record by record: the epoch each was written under, and the count
/// it left (`None` for an open, which writes nothing). The first is what the file started from: nothing, or the
/// object as it was when the store wrote the file anew.
fn logged(w: &World<Node>, object: &str) -> Vec<(u64, Option<u64>)> {
    let key = object_key("counter", "Counter", object);
    let disk = w.disk(CONTROL).dir("objects");
    let mut newest = None;
    for (name, _) in disk.list().into_iter().filter(|(name, _)| name.starts_with("obj.")) {
        let data = disk.read(&name).expect("listed");
        let records: Vec<Value> =
            store::frames(&data).0.into_iter().map(|record| serde_json::from_slice(record).expect("JSON")).collect();
        let Some(first) = records.first().map(|record| &record["Snapshot"]) else { continue };
        if first["object"] != key.as_str() {
            continue;
        }
        let generation: u64 = name.rsplit('.').next().and_then(|n| n.parse().ok()).expect("a generation");
        let mut log = vec![(first["epoch"].as_u64().expect("an epoch"), Some(count(&first["entries"]).unwrap_or(0)))];
        log.extend(records[1..].iter().map(|record| match record["Epoch"].as_u64() {
            Some(epoch) => (epoch, None),
            None => (record["Write"]["epoch"].as_u64().expect("a write"), count(&record["Write"]["ops"])),
        }));
        if newest.as_ref().is_none_or(|(other, _)| *other < generation) {
            newest = Some((generation, log));
        }
    }
    newest.map(|(_, log)| log).unwrap_or_default()
}

/// What must hold of an object whatever happened to its runner and to the store, from the callers' answers and the
/// store's file. Gives the counts the callers were told, and the count the store holds.
fn held(w: &mut World<Node>, seed: u64, object: &'static str, samples: &[Sample]) -> (BTreeSet<u64>, u64) {
    let mut told = BTreeSet::new();
    for sample in samples.iter().filter(|sample| sample.object == object) {
        assert!(sample.took <= sample.deadline + SLACK, "seed {seed}: past its deadline: {sample:?}");
        match sample.count() {
            // One request at a time, each on what the last one stored: no count is given out twice.
            Some(n) => assert!(told.insert(n), "seed {seed}: two callers of {object} were told {n}: {sample:?}"),
            None => {
                let response = &sample.response;
                let why = response.error.is_some() && !response.body.is_empty();
                assert!(response.status >= 429 && why, "seed {seed}: failed without a reason: {sample:?}");
            }
        }
    }

    // The store's file: one writer at a time. An epoch never goes back, and each write stores one more than the
    // write before it, whoever wrote that, or the same again under the same lease (a write that was asked twice
    // because its answer was lost).
    let log = logged(w, object);
    let (mut epoch, mut n) = log.first().map_or((0, 0), |(epoch, n)| (*epoch, n.unwrap_or(0)));
    let mut written = BTreeSet::new();
    for (at, (under, wrote)) in log.iter().enumerate().skip(1) {
        assert!(*under >= epoch, "seed {seed}: {object}'s file goes back from epoch {epoch} to {under}: {log:?}");
        if let Some(wrote) = *wrote {
            let again = wrote == n && *under == epoch;
            assert!(wrote == n + 1 || again, "seed {seed}: record {at} of {object} stores {wrote} over {n}: {log:?}");
            written.insert(wrote);
            n = wrote;
        }
        epoch = *under;
    }
    let holds = stored(w, object);
    assert_eq!(holds, n, "seed {seed}: the store answers what {object}'s file holds");
    // The output gate: a count a caller was told is one the store holds. `from` is where the file starts when the
    // store wrote it anew on the way: the writes before that are not in it.
    let from = log.first().and_then(|(_, n)| *n).unwrap_or(0);
    for n in &told {
        assert!(*n <= from || written.contains(n), "seed {seed}: a caller of {object} was told {n}, never stored");
        assert!(*n <= holds, "seed {seed}: a caller of {object} was told {n}, and the store holds {holds}");
    }
    (told, holds)
}

fn fenced(w: &World<Node>) -> u64 {
    node(w, CONTROL).node.stored.as_ref().expect("the store's counters").fenced.load(Relaxed)
}

/// Writes the store refused for an older epoch, by the fault that left the old owner behind: over every seed run.
static FENCED: [AtomicU64; 4] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
const FAULTS: [&str; 4] = ["killed", "paused", "suspended", "cut off from the acceptors"];

fn one_writer_while_its_runner_is_killed_paused_and_cut_off(seed: u64) {
    let mut w = world(seed);
    let running = start_load(&mut w, &["a"], 3, PATIENT);
    counted_in(&mut w, &running.load, 1, "the object counting");
    let warm = Duration::from_millis(300 + w.pick(0..500));
    w.run_for(warm);

    let (mut story, mut epochs) = (Vec::new(), Vec::new());
    for _ in 0..2 {
        let (victim, epoch) = runs_on(&mut w, "a");
        let (victim, before) = (victim.as_str(), fenced(&w));
        let fault = w.pick(0..4) as usize;
        let phase = next_phase(&running.load);
        let at = w.now();
        // Each longer than the lease, so the object is another runner's before the fault ends.
        let lasts = Duration::from_millis(7500 + w.pick(0..4000));
        let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        w.log(format!("{victim} {} for {lasts:?}", FAULTS[fault]));
        match fault {
            0 => w.crash(victim, how),
            1 => w.pause(victim, lasts),
            // Its clocks stand still: it wakes sure of a lease that ran out while it was away.
            2 => w.suspend(victim, lasts),
            _ => w.partition(&[victim], &ACCEPTORS),
        }
        // Its lease runs out, and the first caller after that starts it on another runner, from the store.
        let moved = counted_in(&mut w, &running.load, phase, "the object counting on another runner");
        let (now, later) = runs_on(&mut w, "a");
        assert!(now != victim && later > epoch, "seed {seed}: on {now} at {later} after {victim} at {epoch}");
        assert!(moved < lasts, "seed {seed}: {victim} {} for {lasts:?}, and it moved after {moved:?}", FAULTS[fault]);
        let left = (at + lasts).saturating_sub(w.now());
        w.run_for(left);
        match fault {
            0 => {
                // Until the gateways have let go of it: what they sent on the old session the new process resets.
                w.run_until("the gateways sending to the two runners left", GIVES_UP, |w| {
                    GATEWAYS.iter().all(|gateway| node(w, gateway).node.gateway.as_ref().unwrap().runners() == 2)
                });
                w.restart(victim);
                gateways_see(&mut w, 3);
            }
            1 | 2 => {}
            _ => w.heal(),
        }
        // The runner is back, with a request in hand in some seeds, under a lease it has lost. One that was paused
        // sees at once that the lease ran out, and axon ends the actor before its job goes on; one that was suspended
        // does not, sends its write, and only the store's fence stops that.
        let phase = next_phase(&running.load);
        counted_in(&mut w, &running.load, phase, "the object counting with its old runner back");
        let on = Duration::from_millis(500 + w.pick(0..1000));
        w.run_for(on);
        let refused = fenced(&w) - before;
        FENCED[fault].fetch_add(refused, Relaxed);
        story.push(format!("{}:{}ms:{refused}", ["kill", "pause", "freeze", "cut"][fault], moved.as_millis()));
        epochs.push(later);
    }
    let samples = end_load(&mut w, running);
    let (told, holds) = held(&mut w, seed, "a", &samples);
    assert!(told.len() > 20, "seed {seed}: the callers were told {} counts", told.len());
    println!(
        "world digest objects-writer seed {seed}: {} requests={} counted={} stored={holds} faults={}",
        w.digest(),
        samples.len(),
        told.len(),
        story.join(",")
    );
}

/// Requests sent and answered while the store was away, by how it went: over every seed run.
static MEANWHILE: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

fn no_count_is_told_that_the_store_does_not_hold(seed: u64) {
    let mut w = world(seed);
    let running = start_load(&mut w, &["a"], 3, SHORT);
    counted_in(&mut w, &running.load, 1, "the object counting");
    let warm = Duration::from_millis(300 + w.pick(0..500));
    w.run_for(warm);

    // The store goes while requests are in hand: cut off from the runners, its disk full, or its node killed.
    let fault = w.pick(0..3);
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    let lasts = Duration::from_millis(500 + w.pick(0..5000));
    let during = next_phase(&running.load);
    let what = match fault {
        0 => {
            w.partition(&[CONTROL], &RUNNERS);
            "cut".to_owned()
        }
        1 => {
            w.disk(CONTROL).set_capacity(Some(0));
            "full".to_owned()
        }
        _ => {
            w.crash(CONTROL, how);
            format!("{how:?}")
        }
    };
    w.log(format!("the store {what} for {lasts:?}"));
    w.run_for(lasts);
    let after = next_phase(&running.load);
    match fault {
        0 => w.heal(),
        1 => w.disk(CONTROL).set_capacity(None),
        _ => w.restart(CONTROL),
    }
    counted_in(&mut w, &running.load, after, "the object counting with the store back");
    w.run_for(Duration::from_millis(500));
    let samples = end_load(&mut w, running);

    let (mut failed, mut refused) = (0, 0);
    for sample in samples.iter().filter(|sample| sample.sent == during && sample.answered == during) {
        // Nothing the store could have acknowledged: no request sent and answered meanwhile has a count.
        assert_eq!(sample.count(), None, "seed {seed}, {what}: counted without the store: {sample:?}");
        failed += 1;
        refused += usize::from(String::from_utf8_lossy(&sample.response.body).contains("the store refused the writes"));
    }
    MEANWHILE[fault as usize].fetch_add(failed, Relaxed);
    if fault == 1 {
        // A full disk is refused at once, and the caller is told so; the other two are a wait for the deadline.
        assert!(refused > 0, "seed {seed}: no request met the full disk in {lasts:?}");
    }
    let (told, holds) = held(&mut w, seed, "a", &samples);
    println!(
        "world digest objects-gate seed {seed}: {} {what} requests={} counted={} stored={holds} failed_meanwhile={failed}",
        w.digest(),
        samples.len(),
        told.len(),
    );
}

fn the_control_node_restarts_with_what_the_objects_stored(seed: u64) {
    const OBJECTS: [&str; 3] = ["a", "b", "c"];
    let mut w = world(seed);
    let running = start_load(&mut w, &OBJECTS, 2, SHORT);
    let end = w.now() + RECOVERS;
    while OBJECTS.iter().any(|object| stored(&mut w, object) == 0) {
        assert!(w.now() < end, "seed {seed}: not every object counted within {RECOVERS:?}");
        w.run_for(Duration::from_millis(100));
    }
    let warm = Duration::from_millis(w.pick(0..1000));
    w.run_for(warm);
    let before = OBJECTS.map(|object| stored(&mut w, object));
    let ran = OBJECTS.map(|object| owner(&w, object));

    // Longer than a lease in most seeds: the objects keep theirs, which the two acceptors left renew.
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    let down = Duration::from_millis(3000 + w.pick(0..8000));
    w.crash(CONTROL, how);
    let during = next_phase(&running.load);
    w.run_for(down);
    let after = next_phase(&running.load);
    assert_eq!(OBJECTS.map(|object| owner(&w, object)), ran, "seed {seed}, {how:?}: an object moved or ended");
    w.restart(CONTROL);
    counted_in(&mut w, &running.load, after, "an object counting with the control node back");
    w.run_for(Duration::from_millis(1000));
    let samples = end_load(&mut w, running);

    let meanwhile: Vec<&Sample> = samples.iter().filter(|s| s.sent == during && s.answered == during).collect();
    assert!(!meanwhile.is_empty(), "seed {seed}, {how:?}: no request met the outage of {down:?}");
    for sample in meanwhile.iter() {
        assert_eq!(sample.count(), None, "seed {seed}, {how:?}: counted without the store: {sample:?}");
    }
    let mut counted = 0;
    for (object, before) in OBJECTS.into_iter().zip(before) {
        let (told, holds) = held(&mut w, seed, object, &samples);
        // Every write the store had answered is there after the restart, and the object went on from it.
        assert!(holds > before, "seed {seed}, {how:?}: {object} had {before}, and has {holds} after the restart");
        assert!(told.iter().any(|n| *n > before), "seed {seed}, {how:?}: {object} did not count again");
        counted += told.len();
    }
    println!(
        "world digest objects-control seed {seed}: {} {how:?} requests={} counted={counted} failed_meanwhile={} down_ms={}",
        w.digest(),
        samples.len(),
        meanwhile.len(),
        down.as_millis()
    );
}

#[test]
fn an_object_has_one_writer_while_its_runner_is_killed_paused_frozen_or_cut_off_from_the_acceptors() {
    let seeds = seeds(0..64);
    let all = seeds.end - seeds.start >= 64;
    check_seeds(seeds, one_writer_while_its_runner_is_killed_paused_and_cut_off).unwrap();
    let fenced = FENCED.each_ref().map(|n| n.load(Relaxed));
    println!("writes the store fenced, after the old owner was {FAULTS:?}: {fenced:?}");
    // Not only the lease: on some seed an owner woke with a request in hand and sent its write, and the store
    // refused it.
    assert!(!all || fenced[2] > 0, "no frozen owner's write met the fence in any seed");
}

#[test]
fn no_caller_is_told_a_count_the_store_does_not_hold_while_the_store_is_cut_off_full_or_killed() {
    let seeds = seeds(0..64);
    let all = seeds.end - seeds.start >= 64;
    check_seeds(seeds, no_count_is_told_that_the_store_does_not_hold).unwrap();
    let meanwhile = MEANWHILE.each_ref().map(|n| n.load(Relaxed));
    println!("requests sent and answered while the store was [cut off, full, killed]: {meanwhile:?}");
    // Not only seeds whose outage no request met: for each way the store went, some were refused a count.
    assert!(!all || meanwhile.iter().all(|n| *n > 0), "an outage no request met in any seed: {meanwhile:?}");
}

#[test]
fn the_control_node_restarts_with_every_object_s_data_and_requests_meanwhile_fail_by_their_deadline() {
    check_seeds(seeds(0..64), the_control_node_restarts_with_what_the_objects_stored).unwrap();
}
