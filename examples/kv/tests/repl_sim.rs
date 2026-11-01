//! The replication core (`src/repl/core.rs`) under a seeded simulation of three logs and the owners a lease oracle
//! starts. The oracle can give two starts one round, or a round below a log's promise. Messages are dropped,
//! duplicated, delayed and reordered; a node is cut off for a while; a crash loses what its log had not synced and
//! may tear the last record; logs checkpoint and drop the records before. After every step the simulation checks:
//!
//! - I1: an acknowledged write is never replaced at its index;
//! - I2: no two entries are committed at one index, by any log or owner;
//! - I3: no log takes an Append below a promise it showed;
//! - I5: a read gives at least the last write acknowledged before it was sent, and nothing not yet written.
//!
//! At the end the faults stop, one log misses the last owner's promise and comes back, and every log must catch up
//! with the owner and hold its map (I12, I2); then every key is read back (I1).
//!
//! `SEEDS=n` or `SEEDS=a..b` sets the seeds (8 by default), `SIM_SEED=n` one, `SIM_DEBUG=1` prints its trace.
//! Before a merge: `SEEDS=0..10000 cargo test -p kv --release --test repl_sim`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;
use kv::repl::{Ballot, Config, Entry, FromLog, Op, Out, Owner, Pos, Rec, Replica, Status, Stop, ToLog, Weak};

const LOGS: usize = 3;
const STEPS: u64 = 20_000;
const KEYS: usize = 4;
/// Steps until a call to a log ends unanswered.
const CALL_TIMEOUT: u64 = 30;
/// Owners alive at once, zombies among them.
const OWNERS: usize = 3;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

/// The lines a run weakens: the core's, and three the simulation plays itself (the Log actor's and the owner's
/// start in kv).
#[derive(Clone, Copy, Debug, Default)]
struct Lines {
    core: Weak,
    /// A log answers before its records are on disk.
    ack_before_sync: bool,
    /// A restarted log keeps a torn last record, so what it writes after is lost to the next replay.
    keep_torn_tail: bool,
    /// A ballot is its round alone: two starts given one round share it.
    round_only: bool,
}

enum Slot {
    Rec(Rec),
    Torn,
}

struct Log {
    up: bool,
    replica: Replica,
    disk: Vec<Slot>,
    unsynced: Vec<Rec>,
    /// Answers waiting for the records written before them: the owner, the answer. A sync lands every one.
    held: Vec<(usize, FromLog)>,
    /// The highest promise an answer that left showed: I3's measure, kept across crashes.
    shown: Ballot,
    boot: u64,
    starts: u64,
    down_until: u64,
    /// Committed indices compared with the record of what is committed.
    checked: u64,
}

enum Payload {
    To(ToLog),
    From(FromLog),
}

struct Packet {
    at: u64,
    owner: usize,
    log: usize,
    payload: Payload,
}

#[derive(Default)]
struct Key {
    sent: u64,
    acked: u64,
    waiting: bool,
}

enum Pending {
    Write { owner: usize, key: usize, seq: u64 },
    Read { owner: usize, key: usize, floor: u64 },
}

impl Pending {
    fn owner(&self) -> usize {
        match self {
            Pending::Write { owner, .. } | Pending::Read { owner, .. } => *owner,
        }
    }
}

/// What a run reached, summed over seeds to show the faults happened.
#[derive(Clone, Copy, Debug, Default)]
struct Stats {
    steps: u64,
    owners: u64,
    served: u64,
    reopened: u64,
    deposed: u64,
    acked: u64,
    unknown: u64,
    reads: u64,
    crashes: u64,
    torn: u64,
    cut_entries: u64,
    installs: u64,
    fetched_maps: u64,
    checkpoints: u64,
    same_round: u64,
    round_back: u64,
    partitions: u64,
    digest: u64,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.steps += o.steps;
        self.owners += o.owners;
        self.served += o.served;
        self.reopened += o.reopened;
        self.deposed += o.deposed;
        self.acked += o.acked;
        self.unknown += o.unknown;
        self.reads += o.reads;
        self.crashes += o.crashes;
        self.torn += o.torn;
        self.cut_entries += o.cut_entries;
        self.installs += o.installs;
        self.fetched_maps += o.fetched_maps;
        self.checkpoints += o.checkpoints;
        self.same_round += o.same_round;
        self.round_back += o.round_back;
        self.partitions += o.partitions;
        self.digest ^= o.digest;
    }
}

struct Sim {
    rng: Rng,
    lines: Lines,
    debug: bool,
    now: u64,
    calm: bool,
    logs: Vec<Log>,
    /// Owners by id, with their node; `None` once stopped or killed.
    owners: Vec<Option<(usize, Owner, bool)>>,
    epoch: u64,
    last_round: [Option<u64>; LOGS],
    cut: Option<(usize, u64)>,
    packets: Vec<Packet>,
    /// When each Append or Install's call ends unanswered, as axon's `request_timeout` ends one: never dropped, and
    /// a no-op where the answer came first. The owner sends again only after it.
    timeouts: Vec<(u64, usize, usize, u64)>,
    keys: Vec<Key>,
    pending: BTreeMap<u64, Pending>,
    next_token: u64,
    /// Every index known committed, with its entry and who said so.
    committed: BTreeMap<u64, (Entry, String)>,
    max_entries: usize,
    keep: u64,
    keep_bytes: u64,
    stats: Stats,
}

fn value(seq: u64) -> Bytes {
    Bytes::copy_from_slice(&seq.to_be_bytes())
}

fn key(k: usize) -> Bytes {
    Bytes::from(format!("k{k}"))
}

impl Sim {
    fn new(seed: u64, lines: Lines) -> Self {
        let mut rng = Rng(seed);
        let logs = (0..LOGS)
            .map(|_| Log {
                up: true,
                replica: Replica::new(lines.core),
                disk: Vec::new(),
                unsynced: Vec::new(),
                held: Vec::new(),
                shown: Ballot::default(),
                boot: rng.next(),
                starts: 0,
                down_until: 0,
                checked: 0,
            })
            .collect();
        let max_entries = 1 + rng.below(4) as usize;
        let keep = rng.below(12);
        // A Set here is about 10 bytes: half the seeds keep none to a few by bytes.
        let keep_bytes = if rng.one_in(2) { u64::MAX } else { rng.below(64) };
        Self {
            rng,
            lines,
            debug: std::env::var_os("SIM_DEBUG").is_some(),
            now: 0,
            calm: false,
            logs,
            owners: Vec::new(),
            epoch: 0,
            last_round: [None; LOGS],
            cut: None,
            packets: Vec::new(),
            timeouts: Vec::new(),
            keys: (0..KEYS).map(|_| Key::default()).collect(),
            pending: BTreeMap::new(),
            next_token: 0,
            committed: BTreeMap::new(),
            max_entries,
            keep,
            keep_bytes,
            stats: Stats::default(),
        }
    }

    fn note(&mut self, what: &str, a: u64, b: u64) {
        let mut h = self.stats.digest.rotate_left(7) ^ self.now;
        for x in what.bytes().map(u64::from).chain([a, b]) {
            h = (h ^ x).wrapping_mul(0x100_0000_01b3);
        }
        self.stats.digest = h;
        if self.debug {
            println!("{:>5} {what} {a} {b}", self.now);
        }
    }

    fn commit(&mut self, index: u64, entry: &Entry, who: String) -> Result<(), String> {
        match self.committed.get(&index) {
            Some((e, _)) if e == entry => Ok(()),
            Some((e, by)) => {
                let what = if by.starts_with("ack") { "I1: an acknowledged write was lost" } else { "I2" };
                Err(format!("{what}: index {index} is {e:?} ({by}), and {who} commits {entry:?}"))
            }
            None => {
                self.committed.insert(index, (entry.clone(), who));
                Ok(())
            }
        }
    }

    fn serving(&mut self) -> Option<usize> {
        let ids: Vec<usize> = (0..self.owners.len())
            .filter(|&i| self.owners[i].as_ref().is_some_and(|o| o.1.status() == Status::Serving))
            .collect();
        (!ids.is_empty()).then(|| ids[self.rng.below(ids.len() as u64) as usize])
    }

    fn start_owner(&mut self) {
        if self.owners.iter().flatten().count() >= OWNERS {
            return;
        }
        let up: Vec<usize> = (0..LOGS).filter(|&l| self.logs[l].up).collect();
        if up.is_empty() {
            return;
        }
        let node = up[self.rng.below(up.len() as u64) as usize];
        // Mostly a new epoch; sometimes the one this node had (a start while it still holds the lease), sometimes
        // an epoch below what the logs promised (an acceptor that forgot, a clock stepped back).
        let round = match (self.rng.below(10), self.last_round[node]) {
            (0, Some(r)) => {
                self.stats.same_round += 1;
                r
            }
            (1, _) if self.epoch > 0 => {
                self.stats.round_back += 1;
                self.epoch - self.rng.below(self.epoch.min(3))
            }
            _ => {
                self.epoch += 1;
                self.epoch
            }
        };
        self.start_at(node, round);
        // The lease moved: the owners it moved from stop, or go on as zombies until killed.
        for id in 0..self.owners.len() - 1 {
            if self.owners[id].is_some() && self.rng.one_in(2) {
                self.kill(id);
            }
        }
    }

    fn start_at(&mut self, node: usize, round: u64) {
        self.last_round[node] = Some(round);
        let log = &mut self.logs[node];
        log.starts += 1;
        let ballot = if self.lines.round_only {
            Ballot { round, ..Ballot::default() }
        } else {
            Ballot { round, node: node as u8, boot: log.boot, start: log.starts }
        };
        let mut cfg = Config::new(LOGS, Some(node));
        cfg.max_entries = self.max_entries;
        cfg.max_bytes = 64;
        (cfg.keep, cfg.keep_bytes) = (self.keep, self.keep_bytes);
        cfg.resend = 30;
        cfg.heartbeat = 5;
        cfg.give_up = 400;
        cfg.weak = self.lines.core;
        let id = self.owners.len();
        self.owners.push(Some((node, Owner::start(ballot, cfg, self.now), false)));
        self.stats.owners += 1;
        self.note("start", id as u64, round);
        self.drain(id).expect("a start answers nothing");
    }

    fn kill(&mut self, id: usize) {
        let Some((_, mut owner, _)) = self.owners[id].take() else { return };
        self.note("kill", id as u64, 0);
        // Its callers hear nothing more: a write may still have landed.
        drop(owner.take_out());
        let tokens: Vec<u64> = self.pending.iter().filter(|(_, p)| p.owner() == id).map(|(t, _)| *t).collect();
        for t in tokens {
            self.drop_token(t);
        }
    }

    fn drop_token(&mut self, token: u64) {
        match self.pending.remove(&token) {
            Some(Pending::Write { key, .. }) => {
                self.keys[key].waiting = false;
                self.stats.unknown += 1;
            }
            Some(Pending::Read { .. }) | None => {}
        }
    }

    fn token(&mut self) -> u64 {
        self.next_token += 1;
        self.next_token
    }

    fn drain(&mut self, id: usize) -> Result<(), String> {
        let Some((node, owner, served)) = self.owners[id].as_mut() else { return Ok(()) };
        let (node, ballot) = (*node, owner.ballot());
        if owner.status() == Status::Serving && !*served {
            *served = true;
            self.stats.served += 1;
            if ballot.round != self.last_round[node].unwrap_or(0) {
                self.stats.reopened += 1;
            }
        }
        let status = owner.status();
        for out in owner.take_out() {
            match out {
                Out::Send(log, msg) => {
                    if matches!(msg, ToLog::Install { .. }) {
                        self.stats.installs += 1;
                    }
                    if let Some(seq) = msg.seq() {
                        self.timeouts.push((self.now + CALL_TIMEOUT, id, log, seq));
                    }
                    let at = self.now + 1 + self.rng.below(4);
                    self.packets.push(Packet { at, owner: id, log, payload: Payload::To(msg) });
                }
                Out::Acked { token, index, .. } => {
                    let Some(Pending::Write { key: k, seq, .. }) = self.pending.remove(&token) else {
                        return Err(format!("an ack of token {token} that waits for nothing"));
                    };
                    let entry = Entry { ballot, op: Op::Set(vec![(key(k), value(seq))]) };
                    self.commit(index, &entry, format!("ack of {k}={seq} by owner {id}"))?;
                    self.keys[k].acked = self.keys[k].acked.max(seq);
                    self.keys[k].waiting = false;
                    self.stats.acked += 1;
                    self.note("acked", k as u64, seq);
                }
                Out::Read { token, value } => {
                    let Some(Pending::Read { key: k, floor, .. }) = self.pending.remove(&token) else {
                        return Err(format!("a read of token {token} that waits for nothing"));
                    };
                    let got = value.map_or(0, |v| u64::from_be_bytes(v[..].try_into().expect("8 bytes")));
                    if got < floor || got > self.keys[k].sent {
                        return Err(format!(
                            "I5: owner {id} read k{k}={got}; {floor} was acknowledged before the read, {} sent",
                            self.keys[k].sent
                        ));
                    }
                    self.stats.reads += 1;
                }
                Out::Dropped { token } => self.drop_token(token),
            }
        }
        if let Status::Stopped(why) = status {
            if matches!(why, Stop::Deposed(_)) {
                self.stats.deposed += 1;
            }
            self.owners[id] = None;
            self.note("stopped", id as u64, 0);
        }
        Ok(())
    }

    fn write(&mut self) -> Result<(), String> {
        let k = self.rng.below(KEYS as u64) as usize;
        if self.keys[k].waiting {
            return Ok(());
        }
        let Some(id) = self.serving() else { return Ok(()) };
        let seq = self.keys[k].sent + 1;
        let token = self.token();
        let owner = &mut self.owners[id].as_mut().expect("serving").1;
        owner.write(Op::Set(vec![(key(k), value(seq))]), token).expect("serving");
        owner.flush(self.now);
        self.keys[k].sent = seq;
        self.keys[k].waiting = true;
        self.pending.insert(token, Pending::Write { owner: id, key: k, seq });
        self.note("write", k as u64, seq);
        self.drain(id)
    }

    fn read(&mut self, k: usize, id: usize) -> Result<(), String> {
        let token = self.token();
        let owner = &mut self.owners[id].as_mut().expect("an owner").1;
        if !owner.read(&key(k), token) {
            return Ok(());
        }
        owner.flush(self.now);
        self.pending.insert(token, Pending::Read { owner: id, key: k, floor: self.keys[k].acked });
        self.drain(id)
    }

    fn crash(&mut self, l: usize, down: u64) {
        let log = &mut self.logs[l];
        if !log.up {
            return;
        }
        // What a crash leaves: a seeded part of what was written but not synced, perhaps with its last record torn.
        let land = self.rng.below(log.unsynced.len() as u64 + 1) as usize;
        let lost = log.unsynced.len() - land;
        log.disk.extend(log.unsynced.drain(..land).map(Slot::Rec));
        if lost > 0 && !self.rng.one_in(4) {
            log.disk.push(Slot::Torn);
            self.stats.torn += 1;
        }
        log.unsynced.clear();
        log.held.clear();
        log.up = false;
        log.down_until = self.now + down;
        self.stats.crashes += 1;
        self.note("crash", l as u64, lost as u64);
        for id in 0..self.owners.len() {
            if self.owners[id].as_ref().is_some_and(|o| o.0 == l) {
                self.kill(id);
            }
        }
    }

    fn restart(&mut self, l: usize) {
        let keep_torn = self.lines.keep_torn_tail;
        let log = &mut self.logs[l];
        if let Some(t) = log.disk.iter().position(|s| matches!(s, Slot::Torn))
            && !keep_torn
        {
            log.disk.truncate(t);
        }
        let recs = log.disk.iter().map_while(|s| match s {
            Slot::Rec(r) => Some(r),
            Slot::Torn => None,
        });
        log.replica = Replica::replay(recs, self.lines.core);
        log.up = true;
        log.boot = self.rng.next();
        log.starts = 0;
        log.checked = log.replica.base().index;
        let last = log.replica.last().index;
        self.note("restart", l as u64, last);
    }

    fn sync(&mut self, l: usize) {
        let log = &mut self.logs[l];
        for rec in log.unsynced.drain(..) {
            // Once a checkpoint is on disk the records before it go, as old segments do.
            if matches!(rec, Rec::Checkpoint(_)) {
                log.disk.clear();
            }
            log.disk.push(Slot::Rec(rec));
        }
        for (owner, answer) in std::mem::take(&mut log.held) {
            self.answer(l, owner, answer);
        }
    }

    fn answer(&mut self, l: usize, owner: usize, answer: FromLog) {
        let log = &mut self.logs[l];
        log.shown = log.shown.max(log.replica.promised());
        if matches!(answer, FromLog::Fetched { snap: Some(_), .. }) {
            self.stats.fetched_maps += 1;
        }
        let at = self.now + 1 + self.rng.below(4);
        self.packets.push(Packet { at, owner, log: l, payload: Payload::From(answer) });
    }

    fn cut_off(&self, owner: usize, log: usize) -> bool {
        let Some((node, _)) = self.cut else { return false };
        let Some(Some((at, ..))) = self.owners.get(owner) else { return false };
        *at != log && (*at == node || log == node)
    }

    fn deliver(&mut self, p: Packet) -> Result<(), String> {
        if self.cut_off(p.owner, p.log) {
            return Ok(());
        }
        match p.payload {
            Payload::To(msg) => {
                let log = &mut self.logs[p.log];
                if !log.up {
                    return Ok(());
                }
                let last = log.replica.last().index;
                let (answer, recs) = log.replica.handle(msg);
                if let FromLog::Appended { ballot, .. } = answer
                    && ballot < log.shown
                {
                    return Err(format!("I3: log {} took ballot {ballot:?} after it showed {:?}", p.log, log.shown));
                }
                for rec in &recs {
                    if let Rec::Entries { after, .. } = rec
                        && *after < last
                    {
                        self.stats.cut_entries += 1;
                    }
                }
                let log = &mut self.logs[p.log];
                log.unsynced.extend(recs);
                if self.lines.ack_before_sync {
                    self.answer(p.log, p.owner, answer);
                } else {
                    log.held.push((p.owner, answer));
                }
                Ok(())
            }
            Payload::From(answer) => {
                let Some(Some((_, owner, _))) = self.owners.get_mut(p.owner) else { return Ok(()) };
                owner.on_reply(p.log, answer, self.now);
                self.drain(p.owner)
            }
        }
    }

    /// Every log's newly committed entries against the record.
    fn check_logs(&mut self) -> Result<(), String> {
        for l in 0..LOGS {
            if !self.logs[l].up {
                continue;
            }
            let (from, to) = (self.logs[l].checked + 1, self.logs[l].replica.committed());
            for index in from..=to {
                if let Some(e) = self.logs[l].replica.entry(index).cloned() {
                    self.commit(index, &e, format!("log {l}"))?;
                }
            }
            self.logs[l].checked = to;
        }
        Ok(())
    }

    fn step(&mut self) -> Result<(), String> {
        self.now += 1;
        if !self.calm {
            let alive: Vec<usize> = (0..self.owners.len()).filter(|&i| self.owners[i].is_some()).collect();
            if self.rng.one_in(if alive.is_empty() { 15 } else { 250 }) {
                self.start_owner();
            }
            if !alive.is_empty() && self.rng.one_in(300) {
                let id = alive[self.rng.below(alive.len() as u64) as usize];
                self.kill(id);
            }
            if self.cut.is_none() && self.rng.one_in(400) {
                self.cut = Some((self.rng.below(LOGS as u64) as usize, self.now + 20 + self.rng.below(200)));
                self.stats.partitions += 1;
            }
            if self.rng.one_in(60) {
                let l = self.rng.below(LOGS as u64) as usize;
                if self.logs[l].up {
                    let keep = self.rng.below(4);
                    self.logs[l].replica.compact(keep, u64::MAX);
                    let rec = self.logs[l].replica.checkpoint();
                    self.logs[l].unsynced.push(rec);
                    self.stats.checkpoints += 1;
                }
            }
            if self.rng.one_in(2) {
                self.write()?;
            }
            if self.rng.one_in(3)
                && let Some(id) = self.serving()
            {
                let k = self.rng.below(KEYS as u64) as usize;
                self.read(k, id)?;
            }
            if !self.packets.is_empty() && self.rng.one_in(15) {
                let i = self.rng.below(self.packets.len() as u64) as usize;
                self.packets.swap_remove(i);
            }
            if !self.packets.is_empty() && self.rng.one_in(15) {
                let i = self.rng.below(self.packets.len() as u64) as usize;
                let p = &self.packets[i];
                let payload = match &p.payload {
                    Payload::To(m) => Payload::To(m.clone()),
                    Payload::From(m) => Payload::From(m.clone()),
                };
                let copy = Packet { at: self.now + self.rng.below(20), owner: p.owner, log: p.log, payload };
                self.packets.push(copy);
            }
        }
        if self.cut.is_some_and(|(_, until)| self.now >= until) {
            self.cut = None;
        }
        for l in 0..LOGS {
            if !self.logs[l].up && self.now >= self.logs[l].down_until {
                self.restart(l);
            }
        }
        let now = self.now;
        for (_, id, log, seq) in self.timeouts.extract_if(.., |t| t.0 <= now) {
            if let Some(Some((_, owner, _))) = self.owners.get_mut(id) {
                owner.failed(log, seq);
            }
        }
        for id in 0..self.owners.len() {
            if let Some((_, owner, _)) = self.owners[id].as_mut() {
                owner.tick(self.now);
                self.drain(id)?;
            }
        }
        let mut delivered = 0;
        while delivered < 4 {
            let due: Vec<usize> = (0..self.packets.len()).filter(|&i| self.packets[i].at <= self.now).collect();
            if due.is_empty() {
                break;
            }
            let p = self.packets.swap_remove(due[self.rng.below(due.len() as u64) as usize]);
            self.deliver(p)?;
            delivered += 1;
        }
        // A crash comes between the writes and their sync, where it can lose or tear what was written.
        if !self.calm && self.rng.one_in(250) {
            let l = self.rng.below(LOGS as u64) as usize;
            let down = 10 + self.rng.below(150);
            self.crash(l, down);
        }
        for l in 0..LOGS {
            if self.logs[l].up && (self.calm || self.rng.one_in(3)) {
                self.sync(l);
            }
        }
        self.stats.steps += 1;
        self.check_logs()
    }

    /// The faults stop, one log misses the next owner's promise and comes back; every log must then catch up.
    fn settle(&mut self) -> Result<(), String> {
        self.calm = true;
        self.cut = None;
        for id in 0..self.owners.len() {
            self.kill(id);
        }
        for l in 0..LOGS {
            if !self.logs[l].up {
                self.restart(l);
            }
        }
        let missed = self.rng.below(LOGS as u64) as usize;
        self.crash(missed, 60);
        let mut owner = None;
        for _ in 0..6000 {
            if owner.is_none_or(|id: usize| self.owners[id].is_none()) {
                let node = (missed + 1 + self.rng.below(2) as usize) % LOGS;
                self.epoch += 1;
                self.start_at(node, self.epoch);
                owner = Some(self.owners.len() - 1);
            }
            self.step()?;
            let id = owner.expect("an owner");
            let Some((_, o, _)) = self.owners[id].as_ref() else { continue };
            // A log may keep an old owner's entries past the owner's last: harmless, as nothing commits them.
            let caught_up = |log: &Log| log.up && log.replica.committed() == o.committed();
            if o.status() == Status::Serving && self.pending.is_empty() && self.logs.iter().all(caught_up) {
                return self.read_back(id);
            }
        }
        let logs: Vec<(u64, Pos)> = self.logs.iter().map(|l| (l.replica.committed(), l.replica.last())).collect();
        let o = owner.and_then(|id| self.owners[id].as_ref()).map(|o| (o.1.status(), o.1.committed(), o.1.last()));
        Err(format!("I12: the logs did not catch up after the faults stopped: logs {logs:?}, owner {o:?}"))
    }

    fn read_back(&mut self, id: usize) -> Result<(), String> {
        for k in 0..KEYS {
            self.read(k, id)?;
        }
        for _ in 0..200 {
            if self.pending.is_empty() {
                break;
            }
            self.step()?;
        }
        if !self.pending.is_empty() {
            return Err("I12: the last reads were not answered".into());
        }
        let map = self.owners[id].as_ref().expect("the last owner").1.map().clone();
        for (l, log) in self.logs.iter().enumerate() {
            if log.replica.applied() != &map {
                return Err(format!("I2: log {l} holds {:?}, the owner {map:?}", log.replica.applied()));
            }
        }
        Ok(())
    }
}

fn run(seed: u64, lines: Lines) -> Result<Stats, String> {
    let mut sim = Sim::new(seed, lines);
    for _ in 0..STEPS {
        sim.step()?;
    }
    sim.settle()?;
    Ok(sim.stats)
}

#[test]
fn every_acknowledged_write_survives_and_every_read_is_current() {
    let total = Mutex::new(Stats::default());
    check_seeds(seeds(0..8), |seed| {
        let stats = run(seed, Lines::default()).unwrap_or_else(|e| panic!("SIM_SEED={seed}: {e}"));
        total.lock().expect("stats").add(&stats);
    })
    .unwrap_or_else(|f| panic!("SIM_SEED={} failed: {}", f.seed, f.message));
    let t = total.into_inner().expect("stats");
    println!("{t:?}");
    assert!(t.acked > 0 && t.reads > 0 && t.served > 0, "the simulation did nothing: {t:?}");
}

#[test]
fn a_seed_runs_the_same_twice() {
    for seed in [1, 2] {
        let a = run(seed, Lines::default()).expect("passes");
        let b = run(seed, Lines::default()).expect("passes");
        assert_eq!(a.digest, b.digest, "seed {seed}");
    }
}

/// Each line weakened alone must fail within 64 seeds.
#[test]
fn every_weakened_line_fails_within_64_seeds() {
    let core = |f: fn(&mut Weak)| {
        let mut weak = Weak::default();
        f(&mut weak);
        Lines { core: weak, ..Lines::default() }
    };
    // Each line, and the check its lowest failing seed must fail. A panic elsewhere, or another check, is not the
    // failure this table claims.
    let lines: [(&str, &str, Lines); 12] = [
        ("ack before the sync", "I1", Lines { ack_before_sync: true, ..Lines::default() }),
        ("promise not synced", "I3", core(|w| w.promise_unsynced = true)),
        ("no ballot check on Append", "I3", core(|w| w.no_ballot_check = true)),
        ("no prev check", "I2", core(|w| w.no_prev_check = true)),
        ("commit not capped at the matched index", "I2", core(|w| w.commit_uncapped = true)),
        ("choose the longest log", "I2", core(|w| w.choose_longest = true)),
        ("no Begin", "I5", core(|w| w.no_begin = true)),
        ("ack at 1 of 3", "I2", core(|w| w.quorum_one = true)),
        ("ballot without node, boot and start", "I2", Lines { round_only: true, ..Lines::default() }),
        ("no truncate after replay", "I1", Lines { keep_torn_tail: true, ..Lines::default() }),
        ("promise dropped with a removed segment", "I3", core(|w| w.checkpoint_drops_promise = true)),
        ("an Append with a higher ballot refused as Fenced", "I12", core(|w| w.higher_fenced = true)),
    ];
    let mut wrong = Vec::new();
    for (name, check, lines) in lines {
        let failed = check_seeds(0..64, |seed| {
            if let Err(e) = run(seed, lines) {
                panic!("{e}");
            }
        });
        match failed {
            Err(f) if f.message.starts_with(&format!("{check}:")) => println!("{name}: seed {}: {}", f.seed, f.message),
            Err(f) => wrong.push(format!("{name}: seed {} failed another way than {check}: {}", f.seed, f.message)),
            Ok(()) => wrong.push(format!("{name}: passed 64 seeds")),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The schedule of the design's I1 row: a log that holds an old owner's entry hears a commit index past it on an
/// Append that matched only what came before. Committing its own entry there would commit another entry than the
/// owner did.
#[test]
fn a_stale_log_does_not_commit_its_own_entry_on_a_commit_only_append() {
    for weak in [Weak::default(), Weak { commit_uncapped: true, ..Weak::default() }] {
        let mut logs: Vec<Replica> = (0..LOGS).map(|_| Replica::new(weak)).collect();
        let b1 = Ballot { round: 1, node: 0, boot: 1, start: 1 };
        let b2 = Ballot { round: 2, node: 1, boot: 2, start: 1 };
        let begin = |b| Entry { ballot: b, op: Op::Begin };
        let x = Entry { ballot: b1, op: Op::Set(vec![(key(0), value(1))]) };
        // Owner b1 committed its Begin at 1 everywhere and wrote x at 2 to log 0 only.
        for log in logs.iter_mut() {
            log.handle(ToLog::Append {
                ballot: b1,
                seq: 1,
                prev: Pos::default(),
                entries: vec![begin(b1)],
                committed: 1,
            });
        }
        logs[0].handle(ToLog::Append {
            ballot: b1,
            seq: 2,
            prev: Pos { ballot: b1, index: 1 },
            entries: vec![x],
            committed: 1,
        });
        // Owner b2, from logs 1 and 2, commits its Begin at 2.
        for log in &mut logs[1..] {
            let prev = Pos { ballot: b1, index: 1 };
            log.handle(ToLog::Append { ballot: b2, seq: 1, prev, entries: vec![begin(b2)], committed: 2 });
        }
        // Its first Append to log 0 has not reached it; an empty one that follows on from 1 does.
        let prev = Pos { ballot: b1, index: 1 };
        logs[0].handle(ToLog::Append { ballot: b2, seq: 2, prev, entries: vec![], committed: 2 });
        let stale = logs[0].committed() == 2;
        assert_eq!(stale, weak.commit_uncapped, "log 0 committed {:?} at 2", logs[0].entry(2));
    }
}
