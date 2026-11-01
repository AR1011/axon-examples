//! The journal under faults, by seed: three replicas and two engine nodes on the seeded simulator, with lost,
//! doubled and reordered messages, replica crashes inside a write (every byte of an append may end it), engine
//! crashes, partitions, new owners under new leases and restarts under the same lease, while stale owners keep
//! writing. Each seed ends with a last owner of each run opening it, and the checker reading the replicas.
//!
//! Oracles, online: a replica never acks past what it told a newer seal it held (I1); every owner opens with every
//! entry acked so far, in its place (I2); no owner fails to read its journal back. At the end: the checker finds no
//! violation, and its journal is the last owner's.
//!
//! `JOURNAL_MUTATION=<name>` weakens one rule (`Mutation::parse`), and `scripts/mutations.sh` expects a seed to fail.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use axon::files::{FileFuture, FileOp, Files, LogConfig, MemoryFiles, crc32};
use axon_testkit::{Link, NodeId, SimConfig, Simulator, check_seeds, crash_points, crashed, seeds};
use bytes::Bytes;
use workflows::check::{self, Ack};
use workflows::journal::{
    Answer, AppendError, Entry, Handled, Mutation, Out, Replica, Request, ValueKind, Writer, WriterConfig,
    replica_config,
};

const REPLICAS: usize = 3;
const ENGINES: [NodeId; 2] = [3, 4];
const RUNS: [&str; 2] = ["a", "b"];
const TICK: Duration = Duration::from_millis(10);
/// Faults and new entries stop here, and every replica comes back.
const FAULTS_END: Duration = Duration::from_secs(4);
/// The last owner of each run starts here.
const LAST_AT: Duration = Duration::from_secs(5);
const GIVE_UP: Duration = Duration::from_secs(30);
const DROP_ONE_IN: u64 = 40;
const DOUBLE_ONE_IN: u64 = 60;

#[derive(Clone, Debug)]
enum Msg {
    Req {
        w: usize,
        req: Request,
    },
    Ans {
        w: usize,
        ans: Answer,
    },
    /// The replica's write out lands.
    Disk {
        generation: u64,
    },
    Tick,
}

fn ready<T>(mut future: FileFuture<T>) -> std::io::Result<T> {
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("files in memory waited"),
    }
}

fn log_config(preallocate: bool) -> LogConfig {
    LogConfig {
        segment_bytes: 2 << 20,
        preallocate,
        max_write_bytes: 2 << 20,
        max_unwritten_bytes: 4 << 20,
        ..replica_config()
    }
}

/// A replica's disk whose writes land later: while `defer` is set, `apply` keeps the ops for the simulation to land
/// when its `Disk` message arrives, or to tear in a crash.
#[derive(Clone, Default)]
struct SimFiles {
    disk: MemoryFiles,
    defer: Arc<AtomicBool>,
    taken: Arc<Mutex<Vec<FileOp>>>,
}

impl Files for SimFiles {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.disk.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.disk.read(name)
    }

    fn read_range(&self, name: &str, offset: u64, len: usize) -> FileFuture<Vec<u8>> {
        self.disk.read_range(name, offset, len)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        if !self.defer.load(Ordering::Relaxed) {
            return self.disk.apply(ops);
        }
        self.taken.lock().expect("not poisoned").extend(ops);
        Box::pin(std::future::ready(Ok(())))
    }
}

struct ReplicaNode {
    files: SimFiles,
    replica: Replica<(usize, Bytes)>,
    writing: Option<Vec<FileOp>>,
    generation: u64,
    down_until: Option<Duration>,
    /// I1: each seal it answered, per run: the epoch, and the segments it held then.
    seals: BTreeMap<Bytes, Vec<Sealed>>,
}

/// A seal's epoch, and each segment's length then.
type Sealed = (u64, Vec<(u64, u64)>);

struct Owner {
    node: NodeId,
    run: usize,
    lease: u64,
    writer: Writer,
    alive: bool,
    killed: bool,
    /// Appended, not acked yet: index and body.
    pending: BTreeMap<u64, Bytes>,
    next_step: u32,
    opened: Option<Vec<Bytes>>,
}

struct Sim {
    net: Simulator<Msg>,
    replicas: Vec<ReplicaNode>,
    owners: Vec<Owner>,
    lasts: Vec<usize>,
    acks: Vec<Ack>,
    acked: [BTreeMap<u64, Bytes>; 2],
    lease: u64,
    mutation: Mutation,
    preallocate: bool,
    hash: DefaultHasher,
}

fn mutation() -> Mutation {
    std::env::var("JOURNAL_MUTATION").map_or(Mutation::None, |m| Mutation::parse(&m).expect("a known mutation"))
}

impl Sim {
    fn new(seed: u64, mutation: Mutation) -> Sim {
        let cfg = SimConfig {
            seed,
            jitter: Duration::from_millis(3),
            ordered: false,
            max_pending: 1 << 20,
            max_trace: 0,
            ..SimConfig::default()
        };
        let mut net = Simulator::new(cfg);
        for name in ["r0", "r1", "r2", "e0", "e1"] {
            net.add_node(name).expect("room for the nodes");
        }
        for e in ENGINES {
            net.set_link(e, e, Link { up: true, delay: Some(TICK) }).expect("a node");
        }
        let preallocate = seed.is_multiple_of(2);
        let mut sim = Sim {
            net,
            replicas: Vec::new(),
            owners: Vec::new(),
            lasts: Vec::new(),
            acks: Vec::new(),
            acked: Default::default(),
            lease: 0,
            mutation,
            preallocate,
            hash: DefaultHasher::new(),
        };
        for _ in 0..REPLICAS {
            let files = SimFiles::default();
            let replica = sim.open(&files);
            sim.replicas.push(ReplicaNode {
                files,
                replica,
                writing: None,
                generation: 0,
                down_until: None,
                seals: BTreeMap::new(),
            });
        }
        for (run, e) in [(0, ENGINES[0]), (1, ENGINES[1])] {
            sim.lease += 1;
            sim.start(e, run, sim.lease);
        }
        for e in ENGINES {
            sim.net.send(e, e, Msg::Tick).expect("room");
        }
        sim
    }

    fn open(&self, files: &SimFiles) -> Replica<(usize, Bytes)> {
        let mut replica = ready(Replica::open(Arc::new(files.clone()), log_config(self.preallocate), None, 0))
            .expect("the replica opens");
        replica.mutation = self.mutation;
        replica
    }

    fn now_ms(&self) -> u64 {
        self.net.now().as_millis() as u64
    }

    /// Between nodes, a message may be lost or come twice; a node's messages to itself always arrive.
    fn send(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        if from != to && self.net.pick(0..DROP_ONE_IN) == 0 {
            return;
        }
        if from != to && self.net.pick(0..DOUBLE_ONE_IN) == 0 {
            self.net.send(from, to, msg.clone()).expect("room");
        }
        self.net.send(from, to, msg).expect("room");
    }

    fn run(mut self) -> u64 {
        while let Some(d) = self.net.step() {
            assert!(d.at < GIVE_UP, "the last owners did not open: {:?}", self.lasts);
            (d.at, d.from, d.to, format!("{:?}", d.payload)).hash(&mut self.hash);
            match d.payload {
                Msg::Req { w, req } => self.request(d.to, w, req),
                Msg::Ans { w, ans } if self.owners[w].alive && self.owners[w].node == d.to => {
                    let now = self.now_ms();
                    self.owners[w].writer.answer(d.from, ans, now);
                    self.drain(w);
                }
                Msg::Ans { .. } => {}
                Msg::Disk { generation } if generation == self.replicas[d.to].generation => self.landed(d.to),
                Msg::Disk { .. } => {}
                Msg::Tick => self.tick(d.to),
            }
        }
        self.finish()
    }

    fn request(&mut self, r: usize, w: usize, req: Request) {
        let run = match &req {
            Request::Seal { run, .. }
            | Request::Append { run, .. }
            | Request::Read { run, .. }
            | Request::Probe { run } => run.clone(),
            Request::Pages { .. } => return,
        };
        match self.replicas[r].replica.handle(req, (w, run)) {
            Handled::Now(to, ans) => self.answer(r, to, ans),
            Handled::Queued => {}
            Handled::Read(to, read) => {
                let ans = ready(read).expect("a read in memory");
                self.answer(r, to, ans);
            }
        }
        self.write(r);
    }

    fn answer(&mut self, r: usize, (w, run): (usize, Bytes), ans: Answer) {
        let seals = self.replicas[r].seals.entry(run).or_default();
        match &ans {
            Answer::Sealed { epoch, segments } => seals.push((*epoch, segments.clone())),
            Answer::Acked { epoch, have } => {
                for (sealed, segments) in seals.iter().filter(|(sealed, _)| sealed > epoch) {
                    let held = segments.iter().find(|(e, _)| e == epoch).map_or(0, |(_, n)| *n);
                    assert!(
                        *have <= held,
                        "I1: replica {r} acked {have} records of segment {epoch} after a seal at {sealed} found {held}"
                    );
                }
            }
            _ => {}
        }
        self.send(r, self.owners[w].node, Msg::Ans { w, ans });
    }

    /// Starts the replica's next write, if none is out: its ops land when `Disk` arrives.
    fn write(&mut self, r: usize) {
        let now = self.now_ms();
        let node = &mut self.replicas[r];
        if node.writing.is_some() {
            return;
        }
        let Some(write) = node.replica.take_write(now) else { return };
        node.files.defer.store(true, Ordering::Relaxed);
        let result = ready(write);
        node.files.defer.store(false, Ordering::Relaxed);
        result.expect("a write in memory");
        node.writing = Some(std::mem::take(&mut *node.files.taken.lock().expect("not poisoned")));
        let generation = node.generation;
        self.send(r, r, Msg::Disk { generation });
    }

    fn landed(&mut self, r: usize) {
        let node = &mut self.replicas[r];
        let ops = node.writing.take().expect("a write out");
        ready(node.files.disk.apply(ops)).expect("ops in memory");
        for (to, ans) in node.replica.written(Ok(())) {
            self.answer(r, to, ans);
        }
        self.write(r);
    }

    /// The replica stops inside its write: what reached the disk is any prefix `Files` allows.
    fn crash_replica(&mut self, r: usize, down: Option<Duration>) {
        let at = self.net.pick(0..u64::MAX);
        let node = &mut self.replicas[r];
        if let Some(ops) = node.writing.take()
            && crash_points(&ops) > 0
        {
            ready(node.files.disk.apply(crashed(&ops, at))).expect("ops in memory");
        }
        node.generation += 1;
        node.files.taken.lock().expect("not poisoned").clear();
        let files = node.files.clone();
        self.replicas[r].replica = self.open(&files);
        self.net.crash_node(r).expect("a node");
        if let Some(down) = down {
            self.replicas[r].down_until = Some(self.net.now() + down);
            self.net.set_node_down(r, true).expect("a node");
        }
    }

    fn start(&mut self, node: NodeId, run: usize, lease: u64) -> usize {
        let mut cfg = WriterConfig::new(REPLICAS);
        cfg.max_batch_bytes = 8 << 10;
        cfg.max_unacked_bytes = 32 << 10;
        cfg.max_kept_bytes = 64 << 10;
        // Small, so that some opens wait for every replica instead of carrying.
        cfg.max_carry_bytes = 2 << 10;
        cfg.resend_ms = 100;
        cfg.mutation = self.mutation;
        let token = self.owners.len() as u64;
        let writer = Writer::open(Bytes::from_static(RUNS[run].as_bytes()), lease, token, cfg, self.now_ms());
        self.owners.push(Owner {
            node,
            run,
            lease,
            writer,
            alive: true,
            killed: false,
            pending: BTreeMap::new(),
            next_step: 1,
            opened: None,
        });
        let w = self.owners.len() - 1;
        self.drain(w);
        w
    }

    fn drain(&mut self, w: usize) {
        let now_us = self.net.now().as_micros() as u64;
        for out in self.owners[w].writer.take_out() {
            let owner = &mut self.owners[w];
            let run = owner.run;
            match out {
                Out::Send(r, req) => {
                    let node = owner.node;
                    self.send(node, r, Msg::Req { w, req });
                }
                Out::Opened(journal) => {
                    for (index, body) in &self.acked[run] {
                        assert_eq!(
                            journal.get(*index as usize),
                            Some(body),
                            "I2: owner {w} of run {run} opened without acked entry {index}"
                        );
                    }
                    let steps = journal.iter().filter_map(|b| Entry::decode(b).ok()?.outcome_of());
                    owner.next_step = steps.max().unwrap_or(0) + 1;
                    owner.opened = Some(journal);
                }
                Out::Acked(n) => {
                    let done: Vec<u64> = owner.pending.range(..n).map(|(i, _)| *i).collect();
                    for index in done {
                        let body = owner.pending.remove(&index).expect("pending");
                        let ack = Ack::Entry { run: RUNS[run].into(), index, crc: crc32(&body), t: now_us };
                        self.acks.push(ack);
                        if let Some(before) = self.acked[run].insert(index, body.clone()) {
                            assert_eq!(before, body, "I2: run {run} acked two bodies at entry {index}");
                        }
                    }
                }
                Out::Fenced(_) => owner.alive = false,
                Out::Failed(why) => panic!("owner {w} of run {run} under lease {}: {why}", owner.lease),
            }
        }
    }

    fn append(&mut self, w: usize) {
        let size = if self.net.pick(0..20) == 0 { self.net.pick(500..3000) } else { self.net.pick(0..64) };
        let now = self.now_ms();
        let owner = &mut self.owners[w];
        let value = Bytes::from(vec![owner.next_step as u8; size as usize]);
        let body = Entry::Value { step: owner.next_step, kind: ValueKind::Random, value }.encode();
        match owner.writer.append(body.clone(), now) {
            Ok(index) => {
                owner.pending.insert(index, body);
                owner.next_step += 1;
            }
            Err(AppendError::Behind | AppendError::NotOpen) => {}
            Err(AppendError::TooLarge) => unreachable!("the sim's entries are under the batch bound"),
        }
        self.drain(w);
    }

    fn tick(&mut self, e: NodeId) {
        let now = self.net.now();
        for r in 0..REPLICAS {
            if self.replicas[r].down_until.is_some_and(|until| until <= now || now >= FAULTS_END) {
                self.replicas[r].down_until = None;
                self.net.set_node_down(r, false).expect("a node");
            }
        }
        if now < FAULTS_END {
            self.faults(e);
        } else if e == ENGINES[0] {
            self.net.heal();
        }
        if now >= LAST_AT && self.lasts.is_empty() && e == ENGINES[0] {
            for run in 0..RUNS.len() {
                self.lease += 1;
                let w = self.start(e, run, self.lease);
                self.lasts.push(w);
            }
        }
        let mine: Vec<usize> =
            (0..self.owners.len()).filter(|w| self.owners[*w].alive && self.owners[*w].node == e).collect();
        for w in mine {
            self.owners[w].writer.tick(now.as_millis() as u64);
            self.drain(w);
            if now < FAULTS_END && self.owners[w].writer.is_open() {
                for _ in 0..self.net.pick(0..3) {
                    self.append(w);
                }
            }
        }
        let done = !self.lasts.is_empty() && self.lasts.iter().all(|w| self.owners[*w].writer.is_open());
        if !done {
            self.net.send(e, e, Msg::Tick).expect("room");
        }
    }

    fn faults(&mut self, e: NodeId) {
        if self.net.pick(0..200) == 0 {
            let r = self.net.pick(0..REPLICAS as u64) as usize;
            let down = (self.net.pick(0..2) == 0).then(|| Duration::from_millis(self.net.pick(10..300)));
            self.crash_replica(r, down);
        }
        if self.net.pick(0..600) == 0 {
            self.net.crash_node(e).expect("a node");
            for owner in self.owners.iter_mut().filter(|o| o.node == e && o.alive) {
                (owner.alive, owner.killed) = (false, true);
            }
        }
        match self.net.pick(0..300) {
            0 => {
                let r = self.net.pick(0..REPLICAS as u64) as usize;
                self.net.partition(&[e], &[r]).expect("nodes");
            }
            1 => self.net.heal(),
            _ => {}
        }
        if self.net.pick(0..120) == 0 {
            let run = self.net.pick(0..RUNS.len() as u64) as usize;
            let newest = self.owners.iter().filter(|o| o.run == run).max_by_key(|o| o.lease);
            // The node restarts the run under the lease it still holds: the journal takes the next epoch of it.
            let lease = match newest {
                Some(o) if o.node == e && o.killed && self.net.pick(0..2) == 0 => o.lease,
                _ => {
                    self.lease += 1;
                    self.lease
                }
            };
            self.start(e, run, lease);
        }
    }

    fn finish(mut self) -> u64 {
        for (r, node) in self.replicas.iter().enumerate() {
            assert!(node.writing.is_none() && node.replica.waiting() == 0, "replica {r} holds answers at the end");
        }
        let dumps: Vec<check::Dump> = self
            .replicas
            .iter()
            .map(|node| ready(check::dump(Arc::new(node.files.disk.clone()), log_config(self.preallocate))))
            .collect::<Result<_, _>>()
            .expect("the replicas read back");
        let report = check::check(&dumps, &self.acks, None);
        report.assert_ok();
        for &w in &self.lasts {
            let owner = &self.owners[w];
            let opened = owner.opened.as_ref().expect("the last owner opened");
            let journal = &report.journals[RUNS[owner.run].as_bytes()];
            assert_eq!(
                &journal[..journal.len() - 1],
                &opened[..],
                "run {}: the checker reads another journal",
                owner.run
            );
            assert!(matches!(Entry::decode(&journal[journal.len() - 1]), Ok(Entry::Own(_))));
        }
        if std::env::var_os("SIM_DEBUG").is_some() {
            let stats = self.net.stats();
            let lens: Vec<usize> = report.journals.values().map(Vec::len).collect();
            eprintln!(
                "{} delivered, {} owners, {} acks, journals {lens:?}, {:?} ms",
                stats.delivered,
                self.owners.len(),
                self.acks.len(),
                self.net.now().as_millis()
            );
        }
        (report.journals, self.acks.len()).hash(&mut self.hash);
        self.hash.finish()
    }
}

#[test]
fn every_seed_keeps_each_acked_entry_in_one_journal() {
    let mutation = mutation();
    if let Err(failed) = check_seeds(seeds(0..500), |seed| {
        Sim::new(seed, mutation).run();
    }) {
        panic!("seed {} failed: {}", failed.seed, failed.message);
    }
}

#[test]
fn a_seed_runs_the_same_twice() {
    let mutation = mutation();
    for seed in [1, 2] {
        assert_eq!(Sim::new(seed, mutation).run(), Sim::new(seed, mutation).run(), "seed {seed}");
    }
}
