//! A [`Replica`] is one shard's part of one node's log; an [`Owner`] is the shard's running owner. The Log actor and
//! the shard carry their messages, write a replica's [`Rec`]s and hold its answers until the records are on disk.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;

/// One start of a shard's owner. Compared field by field: the lease epoch, the node's place in the storage set, a
/// number drawn at process start, and `SpawnContext::start`, the count of starts in the process. So each start has a
/// ballot of its own, though axon may give two starts one epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ballot {
    pub round: u64,
    pub node: u8,
    pub boot: u64,
    pub start: u64,
}

/// An entry's place: the ballot that wrote it and its index. Two logs that hold the same `Pos` hold the same
/// entries up to it. Ordered by ballot first, which is how a starting owner picks the log to go on from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos {
    pub ballot: Ballot,
    pub index: u64,
}

pub type Map = BTreeMap<Bytes, Bytes>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// The first entry of each owner: once a majority holds it, everything before it is committed.
    Begin,
    Set(Vec<(Bytes, Bytes)>),
    Del(Vec<Bytes>),
    Clear,
}

impl Op {
    /// Bytes of keys and values, which bound one Append.
    pub fn size(&self) -> usize {
        match self {
            Op::Begin | Op::Clear => 0,
            Op::Set(pairs) => pairs.iter().map(|(k, v)| k.len() + v.len()).sum(),
            Op::Del(keys) => keys.iter().map(Bytes::len).sum(),
        }
    }

    /// Applies it to `map`; the number of keys `Del` removed.
    pub fn apply(&self, map: &mut Map) -> u64 {
        match self {
            Op::Begin => 0,
            Op::Set(pairs) => {
                for (k, v) in pairs {
                    map.insert(k.clone(), v.clone());
                }
                0
            }
            Op::Del(keys) => keys.iter().filter(|k| map.remove(*k).is_some()).count() as u64,
            Op::Clear => {
                map.clear();
                0
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub ballot: Ballot,
    pub op: Op,
}

/// A committed map and the place it stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub pos: Pos,
    pub map: Map,
}

/// Owner to log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToLog {
    /// Promise `ballot`. `state` asks for the applied map and the entries after it: the owner's own node's log only.
    Open { ballot: Ballot, state: bool },
    /// The entries after `prev`, and how far the owner has committed. `seq` comes back in the answer.
    Append { ballot: Ballot, seq: u64, prev: Pos, entries: Vec<Entry>, committed: u64 },
    /// The owner's committed map, for a log behind the entries the owner keeps.
    Install { ballot: Ballot, seq: u64, snap: Snapshot },
    /// The entries from `from` on, or the applied map and the entries after it.
    Fetch { ballot: Ballot, from: u64 },
}

impl ToLog {
    /// The `seq` an Append or Install carries, for `Owner::failed`.
    pub fn seq(&self) -> Option<u64> {
        match self {
            ToLog::Append { seq, .. } | ToLog::Install { seq, .. } => Some(*seq),
            ToLog::Open { .. } | ToLog::Fetch { .. } => None,
        }
    }
}

/// A log's answer to `Open`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Opened {
    pub last: Pos,
    pub committed: Pos,
    /// Each ballot among the entries, with the last index it wrote, in order.
    pub runs: Vec<(Ballot, u64)>,
    pub state: Option<(Map, Vec<Entry>)>,
}

/// Log to owner. Each carries the ballot it answers, so an answer to an earlier ballot of the owner is left alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromLog {
    Opened {
        ballot: Ballot,
        opened: Opened,
    },
    /// The log has promised a higher ballot: to `Open` and `Fetch`.
    Refused {
        ballot: Ballot,
        promised: Ballot,
    },
    /// The log holds the owner's entries through `matched`.
    Appended {
        ballot: Ballot,
        seq: u64,
        matched: u64,
    },
    /// The log has promised a higher ballot: to `Append` and `Install`.
    Fenced {
        ballot: Ballot,
        seq: u64,
        promised: Ballot,
    },
    /// `prev` is not in the log: what it has, to find where the two agree.
    Behind {
        ballot: Ballot,
        seq: u64,
        committed: Pos,
        runs: Vec<(Ballot, u64)>,
    },
    Fetched {
        ballot: Ballot,
        snap: Option<Snapshot>,
        from: u64,
        entries: Vec<Entry>,
    },
}

/// What a log writes. Replaying a replica's records in order gives the replica back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rec {
    Promise(Ballot),
    /// Cuts the entries after `after`, then adds these.
    Entries {
        after: u64,
        entries: Vec<Entry>,
    },
    Commit(u64),
    Install(Snapshot),
    /// The whole replica, so the records before it can go.
    Checkpoint(Box<State>),
}

/// Lines `tests/repl_sim.rs` weakens one at a time, to show that its checks catch each.
/// Every field is false in kv itself.
#[derive(Clone, Copy, Debug, Default)]
pub struct Weak {
    /// A log promises without writing the promise.
    pub promise_unsynced: bool,
    /// A log takes an Append below its promise.
    pub no_ballot_check: bool,
    /// A log refuses an Append above its promise as fenced, instead of promising it.
    pub higher_fenced: bool,
    /// A log adds entries without checking the one before them.
    pub no_prev_check: bool,
    /// A log commits up to its last entry, not the last one the Append matched.
    pub commit_uncapped: bool,
    /// A checkpoint does not hold the promise.
    pub checkpoint_drops_promise: bool,
    /// A starting owner goes on from the longest log, not the one with the highest last ballot.
    pub choose_longest: bool,
    /// A starting owner serves at once, and commits any index a majority holds.
    pub no_begin: bool,
    /// One log is a majority.
    pub quorum_one: bool,
}

/// A log's entries as a replica or an owner holds them: those up to `base` are in a map only.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Chain {
    pub(super) base: Pos,
    /// `base.index + 1 ..= last`.
    pub(super) entries: VecDeque<Entry>,
}

impl Chain {
    pub(super) fn last(&self) -> Pos {
        match self.entries.back() {
            Some(e) => Pos { ballot: e.ballot, index: self.base.index + self.entries.len() as u64 },
            None => self.base,
        }
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        let i = index.checked_sub(self.base.index + 1)?;
        self.entries.get(usize::try_from(i).ok()?)
    }

    fn ballot_at(&self, index: u64) -> Option<Ballot> {
        if index == self.base.index { Some(self.base.ballot) } else { self.entry(index).map(|e| e.ballot) }
    }

    fn pos(&self, index: u64) -> Pos {
        Pos { ballot: self.ballot_at(index).expect("an index at or above base and at or below last"), index }
    }

    /// Each ballot among the entries, with the last index it wrote, in order.
    fn runs(&self) -> Vec<(Ballot, u64)> {
        let mut runs = vec![(self.base.ballot, self.base.index)];
        for (index, e) in (self.base.index + 1..).zip(&self.entries) {
            match runs.last_mut() {
                Some(run) if run.0 == e.ballot => run.1 = index,
                _ => runs.push((e.ballot, index)),
            }
        }
        runs
    }

    fn after(&self, index: u64) -> Vec<Entry> {
        let skip = (index.saturating_sub(self.base.index)) as usize;
        self.entries.iter().skip(skip).cloned().collect()
    }

    /// Drops the first entry, which becomes the base.
    fn pop_front(&mut self) -> Entry {
        let e = self.entries.pop_front().expect("an entry past the base");
        self.base = Pos { ballot: e.ballot, index: self.base.index + 1 };
        e
    }

    /// Drops the entries through `index`, which becomes the base.
    fn compact(&mut self, index: u64) {
        let pos = self.pos(index);
        self.entries.drain(..(index - self.base.index) as usize);
        self.base = pos;
    }

    /// Applies the entries `from..=to` to `map`, with what each gave.
    fn apply(&self, from: u64, to: u64, map: &mut Map) -> impl Iterator<Item = (u64, u64)> {
        (from..=to).map(|i| (i, self.entry(i).expect("an index at or below last").op.apply(map)))
    }
}

/// One shard's replica, as a log holds it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub(super) promised: Ballot,
    /// `base.index <= committed`.
    pub(super) chain: Chain,
    pub(super) committed: u64,
    /// The map at `committed`.
    pub(super) applied: Map,
}

impl State {
    fn apply(&mut self, rec: &Rec) {
        match rec {
            Rec::Promise(b) => self.promised = self.promised.max(*b),
            Rec::Entries { after, entries } => {
                debug_assert!(*after >= self.committed, "entries cut below the committed index");
                let chain = &mut self.chain;
                chain.entries.truncate((after.saturating_sub(chain.base.index)) as usize);
                chain.entries.extend(entries.iter().cloned());
            }
            Rec::Commit(c) => {
                let c = (*c).min(self.chain.last().index);
                self.chain.apply(self.committed + 1, c, &mut self.applied).for_each(drop);
                self.committed = self.committed.max(c);
            }
            Rec::Install(snap) => {
                if snap.pos.index <= self.committed {
                    return;
                }
                if self.chain.ballot_at(snap.pos.index) == Some(snap.pos.ballot) {
                    self.chain.compact(snap.pos.index);
                } else {
                    self.chain = Chain { base: snap.pos, entries: VecDeque::new() };
                }
                self.committed = snap.pos.index;
                self.applied = snap.map.clone();
            }
            Rec::Checkpoint(state) => *self = (**state).clone(),
        }
    }
}

/// One shard's replica in a log: the promise, the entries, and the map of the committed ones.
#[derive(Clone, Debug, Default)]
pub struct Replica {
    s: State,
    weak: Weak,
}

impl Replica {
    pub fn new(weak: Weak) -> Self {
        Self { s: State::default(), weak }
    }

    /// The replica its records give, in the order they were written.
    pub fn replay<'a>(recs: impl IntoIterator<Item = &'a Rec>, weak: Weak) -> Self {
        let mut r = Self::new(weak);
        for rec in recs {
            r.s.apply(rec);
        }
        r
    }

    /// The replica a checkpoint's state gives, before the records written after it.
    pub fn restore(state: State, weak: Weak) -> Self {
        Self { s: state, weak }
    }

    /// One record read back, in the order written.
    pub fn apply(&mut self, rec: &Rec) {
        self.s.apply(rec);
    }

    /// Drops the committed entries from memory but the last `keep`, and of those only the newest that fit in
    /// `max_bytes`: they are in the applied map.
    pub fn compact(&mut self, keep: u64, max_bytes: u64) {
        let s = &mut self.s;
        let floor = s.committed.saturating_sub(keep).max(s.chain.base.index);
        let mut bytes = 0;
        let to = (floor + 1..=s.committed).rev().find(|&i| {
            bytes += s.chain.entry(i).expect("committed and past the base: held").op.size() as u64;
            bytes > max_bytes
        });
        s.chain.compact(to.unwrap_or(floor));
    }

    /// The entries held past the applied map, and their bytes of keys and values.
    pub fn held(&self) -> (u64, u64) {
        let entries = &self.s.chain.entries;
        (entries.len() as u64, entries.iter().map(|e| e.op.size() as u64).sum())
    }

    pub fn promised(&self) -> Ballot {
        self.s.promised
    }

    pub fn last(&self) -> Pos {
        self.s.chain.last()
    }

    pub fn committed(&self) -> u64 {
        self.s.committed
    }

    pub fn base(&self) -> Pos {
        self.s.chain.base
    }

    pub fn entry(&self, index: u64) -> Option<&Entry> {
        self.s.chain.entry(index)
    }

    pub fn applied(&self) -> &Map {
        &self.s.applied
    }

    pub fn state(&self) -> &State {
        &self.s
    }

    fn emit(&mut self, recs: &mut Vec<Rec>, rec: Rec) {
        self.s.apply(&rec);
        recs.push(rec);
    }

    fn promise(&mut self, ballot: Ballot, recs: &mut Vec<Rec>) {
        if self.weak.promise_unsynced {
            self.s.promised = ballot;
        } else {
            self.emit(recs, Rec::Promise(ballot));
        }
    }

    /// Fenced when `ballot` is below the promise; a higher one is promised.
    fn take_ballot(&mut self, ballot: Ballot, seq: u64, recs: &mut Vec<Rec>) -> Option<FromLog> {
        let fenced = Some(FromLog::Fenced { ballot, seq, promised: self.s.promised });
        match ballot.cmp(&self.s.promised) {
            std::cmp::Ordering::Less if !self.weak.no_ballot_check => fenced,
            std::cmp::Ordering::Greater if self.weak.higher_fenced => fenced,
            std::cmp::Ordering::Greater => {
                self.promise(ballot, recs);
                None
            }
            _ => None,
        }
    }

    /// The answer, and the records to write before it goes: none of it may leave before they are on disk, nor
    /// before every record written earlier is.
    pub fn handle(&mut self, msg: ToLog) -> (FromLog, Vec<Rec>) {
        let mut recs = Vec::new();
        let answer = match msg {
            ToLog::Open { ballot, state } => {
                if ballot < self.s.promised {
                    FromLog::Refused { ballot, promised: self.s.promised }
                } else {
                    if ballot > self.s.promised {
                        self.promise(ballot, &mut recs);
                    }
                    let s = &self.s;
                    let opened = Opened {
                        last: s.chain.last(),
                        committed: s.chain.pos(s.committed),
                        runs: s.chain.runs(),
                        state: state.then(|| (s.applied.clone(), s.chain.after(s.committed))),
                    };
                    FromLog::Opened { ballot, opened }
                }
            }
            ToLog::Append { ballot, seq, prev, entries, committed } => match self.take_ballot(ballot, seq, &mut recs) {
                Some(fenced) => fenced,
                None => self.append(ballot, seq, prev, entries, committed, &mut recs),
            },
            ToLog::Install { ballot, seq, snap } => match self.take_ballot(ballot, seq, &mut recs) {
                Some(fenced) => fenced,
                None => {
                    let matched = snap.pos.index;
                    if matched > self.s.committed {
                        self.emit(&mut recs, Rec::Install(snap));
                    }
                    FromLog::Appended { ballot, seq, matched }
                }
            },
            ToLog::Fetch { ballot, from } => {
                let s = &self.s;
                if ballot != s.promised {
                    FromLog::Refused { ballot, promised: s.promised }
                } else if from > s.chain.base.index && from <= s.chain.last().index + 1 {
                    FromLog::Fetched { ballot, snap: None, from, entries: s.chain.after(from - 1) }
                } else {
                    let snap = Snapshot { pos: s.chain.pos(s.committed), map: s.applied.clone() };
                    let (from, entries) = (s.committed + 1, s.chain.after(s.committed));
                    FromLog::Fetched { ballot, snap: Some(snap), from, entries }
                }
            }
        };
        (answer, recs)
    }

    fn append(
        &mut self,
        ballot: Ballot,
        seq: u64,
        prev: Pos,
        entries: Vec<Entry>,
        committed: u64,
        recs: &mut Vec<Rec>,
    ) -> FromLog {
        let last = self.s.chain.last().index;
        // The committed prefix is every log's, so an Append that follows on from it needs no ballot to match.
        let follows = prev.index <= self.s.committed || self.s.chain.ballot_at(prev.index) == Some(prev.ballot);
        if !follows && !self.weak.no_prev_check {
            return FromLog::Behind {
                ballot,
                seq,
                committed: self.s.chain.pos(self.s.committed),
                runs: self.s.chain.runs(),
            };
        }
        let start = if self.weak.no_prev_check { prev.index.min(last) } else { prev.index };
        let matched = start + entries.len() as u64;
        // The first entry the log does not hold already: from there, what the log has is cut and replaced.
        let new = entries.iter().enumerate().position(|(k, e)| {
            let index = start + 1 + k as u64;
            index > self.s.committed && self.s.chain.ballot_at(index) != Some(e.ballot)
        });
        if let Some(k) = new {
            let mut entries = entries;
            let tail = entries.split_off(k);
            self.emit(recs, Rec::Entries { after: start + k as u64, entries: tail });
        }
        // Raft's rule: commit only what this Append matched, never an entry the log holds from an older owner.
        let reach = if self.weak.commit_uncapped { self.s.chain.last().index } else { matched };
        let commit = committed.min(reach);
        if commit > self.s.committed {
            self.emit(recs, Rec::Commit(commit));
        }
        FromLog::Appended { ballot, seq, matched }
    }

    /// The record that stands for the whole replica, so the records before it can go once it is on disk. It holds the
    /// promise, and no committed entry: the map stands for them.
    pub fn checkpoint(&self) -> Rec {
        let s = &self.s;
        let chain = Chain { base: s.chain.pos(s.committed), entries: s.chain.after(s.committed).into() };
        let promised = if self.weak.checkpoint_drops_promise { Ballot::default() } else { s.promised };
        Rec::Checkpoint(Box::new(State { promised, chain, committed: s.committed, applied: s.applied.clone() }))
    }
}

/// How an owner runs. Times are in the caller's unit (ms in kv).
#[derive(Clone, Debug)]
pub struct Config {
    /// Logs in the storage set: 1 or 3.
    pub logs: usize,
    /// This node's log, whose applied map the owner starts from.
    pub local: Option<usize>,
    /// At most this many entries, and this many bytes past the first entry, in one Append.
    pub max_entries: usize,
    pub max_bytes: usize,
    /// Committed entries kept to send a log that is behind, and of those only the newest within `keep_bytes` of keys
    /// and values; a log behind them gets the map.
    pub keep: u64,
    pub keep_bytes: u64,
    /// An unanswered message goes again after this; an Append or Install only once its call ended without an answer
    /// (`Owner::failed`), as one that still runs may hold a 32 MiB entry.
    pub resend: u64,
    /// An Append goes to each log at least this often, which carries the committed index.
    pub heartbeat: u64,
    /// A start that does not serve within this stops.
    pub give_up: u64,
    /// Times a start opens again above a promise it was refused.
    pub reopens: u32,
    /// Hold each read until a majority has answered an Append sent after it arrived. Otherwise answer at once.
    pub quorum_reads: bool,
    pub weak: Weak,
}

impl Config {
    pub fn new(logs: usize, local: Option<usize>) -> Self {
        Self {
            logs,
            local,
            max_entries: 1024,
            max_bytes: 512 << 10,
            keep: 4096,
            keep_bytes: 64 << 20,
            resend: 1000,
            heartbeat: 10,
            give_up: 5000,
            reopens: 3,
            quorum_reads: true,
            weak: Weak::default(),
        }
    }

    pub fn quorum(&self) -> usize {
        if self.weak.quorum_one { 1 } else { self.logs / 2 + 1 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Waiting for a majority of promises.
    Opening,
    /// Waiting for the chosen log's entries this node's log lacks.
    Fetching,
    /// Waiting for a majority to hold `Begin`.
    Beginning,
    Serving,
    Stopped(Stop),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// No majority answered within `give_up`.
    NoQuorum,
    /// A log promised a higher ballot.
    Deposed(Ballot),
    /// The chosen log did not give what it said it held.
    Lost,
}

/// What an owner asks of its caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    Send(usize, ToLog),
    /// A write is committed: its index and what it gave (`Del`'s count).
    Acked {
        token: u64,
        index: u64,
        result: u64,
    },
    Read {
        token: u64,
        value: Option<Bytes>,
    },
    /// The owner stopped before it could answer.
    Dropped {
        token: u64,
    },
}

/// A shard's running owner: it opens a ballot on a majority of logs, goes on from the log with the highest last
/// ballot, commits `Begin`, then serves.
pub struct Owner {
    cfg: Config,
    ballot: Ballot,
    status: Status,
    started: u64,
    reopens: u32,
    opened: Vec<Option<Opened>>,
    asked: Vec<u64>,
    chosen: Option<(usize, Pos)>,
    /// Entries up to its base are in `map` only. `base.index <= committed`.
    chain: Chain,
    committed: u64,
    /// Bytes of the committed entries in `chain`, kept for a log that is behind.
    kept: u64,
    map: Map,
    begin: u64,
    next: Vec<u64>,
    matched: Vec<u64>,
    /// The highest `seq` each log has answered without refusing.
    answered: Vec<u64>,
    /// The `seq` of the Append in flight to each log, sent at `sent_at`, and whether its call ended unanswered.
    flight: Vec<Option<u64>>,
    failed: Vec<bool>,
    sent_commit: Vec<u64>,
    sent_at: Vec<u64>,
    seq: u64,
    writes: VecDeque<(u64, u64)>,
    /// Reads held for a round: the `seq` they need, the token and the value.
    reads: VecDeque<(u64, u64, Option<Bytes>)>,
    out: Vec<Out>,
}

impl Owner {
    pub fn start(ballot: Ballot, cfg: Config, now: u64) -> Self {
        let n = cfg.logs;
        let mut owner = Self {
            cfg,
            ballot,
            status: Status::Opening,
            started: now,
            reopens: 0,
            opened: vec![None; n],
            asked: vec![0; n],
            chosen: None,
            chain: Chain::default(),
            committed: 0,
            kept: 0,
            map: Map::new(),
            begin: 0,
            next: vec![1; n],
            matched: vec![0; n],
            answered: vec![0; n],
            flight: vec![None; n],
            failed: vec![false; n],
            sent_commit: vec![0; n],
            sent_at: vec![0; n],
            seq: 0,
            writes: VecDeque::new(),
            reads: VecDeque::new(),
            out: Vec::new(),
        };
        for log in 0..n {
            owner.open(log, now);
        }
        owner
    }

    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn committed(&self) -> u64 {
        self.committed
    }

    pub fn last(&self) -> Pos {
        self.chain.last()
    }

    pub fn map(&self) -> &Map {
        &self.map
    }

    /// Bytes of the committed entries kept for a log that is behind.
    pub fn kept(&self) -> u64 {
        self.kept
    }

    /// Writes not yet committed.
    pub fn pending(&self) -> usize {
        self.writes.len()
    }

    /// What the caller must do: send, answer.
    pub fn take_out(&mut self) -> Vec<Out> {
        std::mem::take(&mut self.out)
    }

    fn open(&mut self, log: usize, now: u64) {
        self.asked[log] = now;
        let state = self.cfg.local == Some(log);
        self.out.push(Out::Send(log, ToLog::Open { ballot: self.ballot, state }));
    }

    fn fetch(&mut self, now: u64) {
        let (log, _) = self.chosen.expect("fetching from the chosen log");
        self.asked[log] = now;
        self.out.push(Out::Send(log, ToLog::Fetch { ballot: self.ballot, from: self.chain.base.index + 1 }));
    }

    fn stop(&mut self, why: Stop) {
        self.status = Status::Stopped(why);
        for (_, token) in self.writes.drain(..) {
            self.out.push(Out::Dropped { token });
        }
        for (_, token, _) in self.reads.drain(..) {
            self.out.push(Out::Dropped { token });
        }
    }

    /// Queues a write; `Err` gives it back when the owner does not serve. Sent at the next `flush`.
    pub fn write(&mut self, op: Op, token: u64) -> Result<u64, Op> {
        if self.status != Status::Serving {
            return Err(op);
        }
        self.chain.entries.push_back(Entry { ballot: self.ballot, op });
        let index = self.last().index;
        self.writes.push_back((index, token));
        Ok(index)
    }

    /// Reads `key` from the committed map; with quorum reads the answer waits for the next round. False when the
    /// owner does not serve.
    #[must_use]
    pub fn read(&mut self, key: &[u8], token: u64) -> bool {
        if self.status != Status::Serving {
            return false;
        }
        let value = self.map.get(key).cloned();
        if self.cfg.quorum_reads {
            self.reads.push_back((self.seq + 1, token, value));
        } else {
            self.out.push(Out::Read { token, value });
        }
        true
    }

    pub fn on_reply(&mut self, log: usize, reply: FromLog, now: u64) {
        match (self.status, reply) {
            (Status::Opening, FromLog::Opened { ballot, opened }) if ballot == self.ballot => {
                self.opened[log] = Some(opened);
                if self.opened.iter().flatten().count() >= self.cfg.quorum() {
                    self.choose(now);
                }
            }
            (Status::Opening, FromLog::Refused { ballot, promised }) if ballot == self.ballot => {
                // A lease epoch can come out below a promise (a clock stepped back, an acceptor that forgot): open
                // again above it. Safety rests on the promises; the lease only keeps owners from fighting.
                if self.reopens >= self.cfg.reopens {
                    return self.stop(Stop::Deposed(promised));
                }
                self.reopens += 1;
                self.ballot.round = promised.round + 1;
                self.opened.iter_mut().for_each(|o| *o = None);
                for log in 0..self.cfg.logs {
                    self.open(log, now);
                }
            }
            (Status::Fetching, FromLog::Fetched { ballot, snap, from, entries }) if ballot == self.ballot => {
                let Some((chosen, pos)) = self.chosen else { return };
                if log != chosen {
                    return;
                }
                match snap {
                    Some(snap) => {
                        self.chain.base = snap.pos;
                        self.committed = snap.pos.index;
                        self.map = snap.map;
                    }
                    None if from != self.chain.base.index + 1 => return,
                    None => {}
                }
                self.chain.entries = entries.into();
                if self.last() != pos {
                    return self.stop(Stop::Lost);
                }
                self.begin(now);
            }
            (Status::Fetching, FromLog::Refused { ballot, promised }) if ballot == self.ballot => {
                self.stop(if promised > ballot { Stop::Deposed(promised) } else { Stop::Lost })
            }
            (Status::Beginning | Status::Serving, reply) => self.on_append_reply(log, reply, now),
            _ => {}
        }
    }

    fn on_append_reply(&mut self, log: usize, reply: FromLog, now: u64) {
        let seq = match reply {
            FromLog::Appended { ballot, seq, matched } if ballot == self.ballot => {
                self.matched[log] = self.matched[log].max(matched);
                self.next[log] = self.next[log].max(matched + 1);
                seq
            }
            FromLog::Behind { ballot, seq, committed, ref runs } if ballot == self.ballot => {
                let common = self.common(runs, committed.index);
                self.next[log] = common + 1;
                self.matched[log] = self.matched[log].max(common);
                seq
            }
            FromLog::Fenced { ballot, seq, promised } if ballot == self.ballot => {
                if promised > self.ballot {
                    return self.stop(Stop::Deposed(promised));
                }
                if self.flight[log] == Some(seq) {
                    self.flight[log] = None;
                }
                return;
            }
            _ => return,
        };
        if self.flight[log] == Some(seq) {
            self.flight[log] = None;
        }
        self.answered[log] = self.answered[log].max(seq);
        self.advance();
        self.release_reads();
        self.flush(now);
    }

    /// Goes on from the answer with the highest last `Pos`: the log of the highest ballot to write, then the longest.
    fn choose(&mut self, now: u64) {
        let longest = self.cfg.weak.choose_longest;
        let local = self.cfg.local;
        let (chosen, pos) = self
            .opened
            .iter()
            .enumerate()
            .filter_map(|(log, o)| o.as_ref().map(|o| (log, o.last)))
            .max_by_key(|&(log, last)| {
                let first = if longest { Pos { ballot: Ballot::default(), index: last.index } } else { last };
                (first, last, Some(log) == local)
            })
            .expect("a majority answered");
        self.chosen = Some((chosen, pos));
        let local_state =
            local.and_then(|l| self.opened[l].as_mut()).and_then(|o| Some((o.committed, o.state.take()?)));
        if let Some((committed, (map, tail))) = local_state {
            self.chain = Chain { base: committed, entries: tail.into() };
            self.committed = committed.index;
            self.map = map;
            if Some(chosen) == local {
                return self.begin(now);
            }
        }
        self.status = Status::Fetching;
        self.fetch(now);
    }

    /// The highest index where `runs` agrees with this owner's log. Both committed prefixes are every log's; past
    /// them, entries of one ballot agree wherever both logs hold them.
    fn common(&self, theirs: &[(Ballot, u64)], committed: u64) -> u64 {
        let mine = self.chain.runs();
        let mut common = committed.min(self.committed);
        for &(ballot, last) in theirs {
            if let Some(&(_, mine)) = mine.iter().find(|r| r.0 == ballot) {
                common = common.max(last.min(mine));
            }
        }
        common
    }

    fn begin(&mut self, now: u64) {
        let chosen = self.last().index;
        if self.cfg.weak.no_begin {
            self.begin = chosen;
            self.status = Status::Serving;
        } else {
            self.chain.entries.push_back(Entry { ballot: self.ballot, op: Op::Begin });
            self.begin = chosen + 1;
            self.status = Status::Beginning;
        }
        for log in 0..self.cfg.logs {
            // A log that did not answer is sent the chosen log's last as `prev`; `Behind` corrects it.
            if let Some(o) = self.opened[log].take() {
                let common = self.common(&o.runs, o.committed.index);
                self.next[log] = common + 1;
                self.matched[log] = common;
            } else {
                self.next[log] = chosen + 1;
            }
        }
        self.flush(now);
    }

    fn advance(&mut self) {
        let mut matched = self.matched.clone();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let c = matched[self.cfg.quorum() - 1].min(self.last().index);
        if c <= self.committed || (c < self.begin && !self.cfg.weak.no_begin) {
            return;
        }
        for (index, result) in self.chain.apply(self.committed + 1, c, &mut self.map) {
            while self.writes.front().is_some_and(|w| w.0 == index) {
                let (_, token) = self.writes.pop_front().expect("a front");
                self.out.push(Out::Acked { token, index, result });
            }
        }
        let chain = &self.chain;
        self.kept +=
            (self.committed + 1..=c).map(|i| chain.entry(i).expect("committed: held").op.size() as u64).sum::<u64>();
        self.committed = c;
        if self.status == Status::Beginning && c >= self.begin {
            self.status = Status::Serving;
        }
        // The oldest go first, so what is kept is what a log a few entries behind needs.
        while self.chain.base.index < c
            && (c - self.chain.base.index > self.cfg.keep || self.kept > self.cfg.keep_bytes)
        {
            self.kept -= self.chain.pop_front().op.size() as u64;
        }
    }

    fn release_reads(&mut self) {
        while let Some(&(need, token, _)) = self.reads.front() {
            if self.answered.iter().filter(|&&a| a >= need).count() < self.cfg.quorum() {
                break;
            }
            let (_, _, value) = self.reads.pop_front().expect("a front");
            self.out.push(Out::Read { token, value });
        }
    }

    /// Sends each log that has nothing in flight what it lacks, the committed index, or a round for held reads.
    /// Call it at the end of each turn.
    pub fn flush(&mut self, now: u64) {
        if !matches!(self.status, Status::Beginning | Status::Serving) {
            return;
        }
        let last = self.last().index;
        let need = self.reads.back().map(|r| r.0);
        for log in 0..self.cfg.logs {
            let due = self.next[log] <= last
                || self.sent_commit[log] < self.committed
                || need.is_some_and(|n| self.answered[log] < n)
                || now >= self.sent_at[log] + self.cfg.heartbeat;
            if self.flight[log].is_none() && due {
                self.send(log, now);
            }
        }
    }

    fn send(&mut self, log: usize, now: u64) {
        self.seq += 1;
        let (ballot, seq) = (self.ballot, self.seq);
        let msg = if self.next[log] <= self.chain.base.index {
            let snap = Snapshot { pos: self.chain.pos(self.committed), map: self.map.clone() };
            self.next[log] = self.committed + 1;
            ToLog::Install { ballot, seq, snap }
        } else {
            let from = self.next[log];
            let prev = self.chain.pos(from - 1);
            let mut entries = Vec::new();
            let mut bytes = 0;
            while let Some(e) = self.chain.entry(from + entries.len() as u64) {
                if !entries.is_empty()
                    && (entries.len() >= self.cfg.max_entries || bytes + e.op.size() > self.cfg.max_bytes)
                {
                    break;
                }
                bytes += e.op.size();
                entries.push(e.clone());
            }
            self.next[log] = from + entries.len() as u64;
            ToLog::Append { ballot, seq, prev, entries, committed: self.committed }
        };
        self.flight[log] = Some(seq);
        self.failed[log] = false;
        self.sent_at[log] = now;
        self.sent_commit[log] = self.committed;
        self.out.push(Out::Send(log, msg));
    }

    /// The call that carried the Append or Install `seq` to `log` ended without an answer: it goes again at the next
    /// `tick` once `resend` has passed since it was sent. An answer that comes later still counts.
    pub fn failed(&mut self, log: usize, seq: u64) {
        if self.flight[log] == Some(seq) {
            self.failed[log] = true;
        }
    }

    /// Sends again what went unanswered, and stops a start that has not served within `give_up`.
    pub fn tick(&mut self, now: u64) {
        let resend = self.cfg.resend;
        let late = |at: u64| now >= at + resend;
        match self.status {
            Status::Opening => {
                for log in 0..self.cfg.logs {
                    if self.opened[log].is_none() && late(self.asked[log]) {
                        self.open(log, now);
                    }
                }
            }
            Status::Fetching => {
                if let Some((log, _)) = self.chosen
                    && late(self.asked[log])
                {
                    self.fetch(now);
                }
            }
            Status::Beginning | Status::Serving => {
                for log in 0..self.cfg.logs {
                    if self.flight[log].is_some() && self.failed[log] && late(self.sent_at[log]) {
                        self.flight[log] = None;
                        self.next[log] = self.matched[log] + 1;
                    }
                }
            }
            Status::Stopped(_) => return,
        }
        if self.status != Status::Serving && now >= self.started + self.cfg.give_up {
            return self.stop(Stop::NoQuorum);
        }
        self.flush(now);
    }
}
