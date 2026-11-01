//! The journal's pure core.
//!
//! **Replica.** One [`axon::files::Log`] per history node holds every run's records: the key is the run id, and an
//! owner's segment `(run, e)` is the log's epoch `e` of that key. A replica takes `Append { run, epoch, seq }` only
//! as the next record of a segment whose epoch is at or above the run's fence, so it holds a prefix of each segment.
//! `Seal` raises the fence, and the answer waits until the fence is durable. One write of the log per turn is the
//! group commit; answers wait for it.
//!
//! **Writer.** A run's owner seals all replicas at its epoch and waits for a majority. From the newest segment it
//! follows the `Own.prev` chain back, counting the newest segment up to the longest answer, and reads what it lacks.
//! Then it appends `Own { prev, carry, summary, .. }`, where `carry` holds the newest segment's records that fewer than a
//! majority of the answers hold, so that every record it counts is on a majority once `Own` is. A record is acked once
//! a majority has synced it. A carry past `max_carry_bytes` is not sent: the writer waits for every replica's seal
//! instead, and counts the newest segment up to the majority's shortest, where no record past it was ever acked.
//!
//! **Index.** A replica indexes each run from its newest segment ([`super::index`]), and repairs from a peer the
//! segments at or above its fence that the peer holds more of.
//!
//! Neither reads a clock or sends: the caller passes `now` in ms and carries the messages, so the actors and
//! `tests/journal_sim.rs` drive the same code.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::Arc;

use axon::files::{Address, Entry as LogEntry, FileFuture, Files, Log, LogConfig, LogError, Lsn, crc32};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::entry::{Entry, Own, encode};
use super::index::{Index, Indexed, Retention, Summary};

/// Records of a run per replica read in one answer, at most: sent in parts.
pub const READ_BYTES: usize = 4 << 20;

/// Seals a replica remembers, so that a seal sent again is answered again: past it, it forgets them all, and a seal
/// sent again then takes the next epoch.
const SEALS: usize = 65_536;

/// Runs whose newest segment a replica reads at once when it rebuilds their summaries.
const REBUILDS: usize = 64;

/// The replica's log: 64 MiB segments, zero-filled ahead of their frames, so most writes' syncs need not commit a new
/// file length (3.3 times faster on build-1, axon's spec/durable-log.md).
pub fn replica_config() -> LogConfig {
    // A run is a key, and a removed one counts until its tomb's segment goes, which can wait until the log copies
    // forward at half of `max_bytes`: 500 runs a second need over the default 2^20 (the soak stalled).
    LogConfig { name: "journal".into(), max_record: 1 << 20, max_keys: 1 << 22, ..LogConfig::default() }
}

/// The journal epoch of a run's owner under lease epoch `lease`. A lease epoch fences two owners on two nodes, but
/// two starts of a run on one node may share it, so the low 8 bits count the starts under one lease: a seal refused
/// at an epoch of its own lease moves to the next ([`Writer`]).
pub fn journal_epoch(lease: u64) -> Option<u64> {
    lease.checked_mul(1 << 8)
}

/// A weakened rule, for the mutation runs that must fail (`scripts/mutations.sh`). `None` everywhere else.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mutation {
    #[default]
    None,
    /// The replica answers a seal without raising the fence.
    SkipFence,
    /// The replica answers a seal before its fence is durable.
    NoFenceSync,
    /// The writer counts a record acked once one replica has it.
    AckOnOne,
    /// The writer recovers from the first seal answer.
    ReadOneReplica,
    /// The writer's `Own` carries nothing.
    NoCarry,
}

impl Mutation {
    pub fn parse(name: &str) -> Option<Mutation> {
        Some(match name {
            "" | "none" => Mutation::None,
            "skip_fence" => Mutation::SkipFence,
            "no_fence_sync" => Mutation::NoFenceSync,
            "ack_on_one" => Mutation::AckOnOne,
            "read_one_replica" => Mutation::ReadOneReplica,
            "no_carry" => Mutation::NoCarry,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Raise the run's fence to `epoch`: answered once durable, with each segment's length. `token` is the writer's:
    /// the same seal again is answered again.
    Seal { run: Bytes, epoch: u64, token: u64 },
    /// Records from `seq` on, of the segment `epoch`.
    Append { run: Bytes, epoch: u64, seq: u64, records: Vec<Bytes> },
    /// Records `from..=to` of the segment `epoch`, or fewer.
    Read { run: Bytes, epoch: u64, from: u64, to: u64 },
    /// The run's segments, raising nothing: is there a run of this id at all.
    Probe { run: Bytes },
    /// Runs after `after` in id order, each with its segments and whether it is gone or soon to go: what repair
    /// compares.
    Pages { after: Option<Bytes>, limit: u32 },
}

/// A replica's answer. Each names the epoch of the request it answers, so a writer can drop an answer to an
/// earlier try.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    Sealed {
        epoch: u64,
        segments: Vec<(u64, u64)>,
    },
    /// The segment holds `have` records, all durable.
    Acked {
        epoch: u64,
        have: u64,
    },
    /// The segment holds `have` records: send from `have + 1`.
    Gap {
        epoch: u64,
        have: u64,
    },
    /// The run is fenced at `fence`, above the request's epoch or equal to a seal's.
    Fenced {
        epoch: u64,
        fence: u64,
    },
    /// The log refused it, changing nothing (`Behind`, `Full`, `Keys`, `TooLarge`): try again later.
    Refused {
        epoch: u64,
        why: String,
    },
    Records {
        epoch: u64,
        from: u64,
        records: Vec<Bytes>,
    },
    /// The run's segments, and its summary as this replica indexes it, with the segment it is of.
    Probed {
        segments: Vec<(u64, u64)>,
        summary: Option<(u64, Summary)>,
    },
    Pages {
        runs: Vec<Listed>,
        more: bool,
    },
}

/// A run as a page lists it: its id, its segments, and whether it is gone or soon to go.
pub type Listed = (Bytes, Vec<(u64, u64)>, bool);

/// What [`Replica::handle`] did with a request.
pub enum Handled<R> {
    Now(R, Answer),
    /// Answered by a later [`Replica::written`].
    Queued,
    /// Run the read, and send what it gives.
    Read(R, FileFuture<Answer>),
}

/// A history replica's state: its log, its index, and the answers waiting for the log to reach disk.
pub struct Replica<R> {
    log: Log,
    index: Index,
    /// In the order taken: each waits until the log is durable through its LSN.
    waiting: VecDeque<(Lsn, R, Answer)>,
    /// The newest LSN this replica's changes took.
    last: Lsn,
    /// Each run's last seal taken, and its token.
    seals: BTreeMap<Bytes, (u64, u64)>,
    /// The wall clock in µs, as the caller last set it: the index's update times.
    pub now_us: u64,
    pub mutation: Mutation,
}

impl<R> Replica<R> {
    /// Opens the log and indexes each run, from `snapshot` where it still matches the run's newest segment, and by
    /// reading that segment where it does not.
    /// `now_us` is the update time of each run read again.
    pub fn open(files: Arc<dyn Files>, cfg: LogConfig, snapshot: Option<Bytes>, now_us: u64) -> FileFuture<Self>
    where
        R: Send + 'static,
    {
        let open = Log::open(files, cfg);
        Box::pin(async move {
            let log = open.await?;
            let mut index = snapshot.and_then(|s| load(&s)).unwrap_or_default();
            let mut stale = Vec::new();
            let mut from: Option<Bytes> = None;
            loop {
                let keys = log.keys(from.as_deref(), 4096);
                let Some((last, _)) = keys.last() else { break };
                from = Some(Bytes::from([last.as_ref(), &[0]].concat()));
                for (run, info) in keys {
                    let newest = newest(&info.epochs);
                    if index.get(&run).is_none_or(|ix| Some((ix.segment, ix.len)) != newest) {
                        index.remove(&run);
                        stale.extend(newest.map(|(epoch, _)| (run, epoch)));
                    }
                }
            }
            let gone: Vec<Bytes> = index.iter().map(|(run, _)| run.clone()).filter(|r| log.key(r).is_none()).collect();
            for run in gone {
                index.remove(&run);
            }
            for chunk in stale.chunks(REBUILDS) {
                let reads =
                    chunk.iter().map(|(run, epoch)| log.read(run, Address { epoch: *epoch, seq: 1 }, usize::MAX));
                for ((run, epoch), read) in chunk.iter().zip(futures_util::future::join_all(reads).await) {
                    for entry in read? {
                        match entry {
                            LogEntry::Record { at, body } if at.epoch == *epoch => index.record(run, at, &body, now_us),
                            _ => {}
                        }
                    }
                }
            }
            let last = log.durable();
            Ok(Replica {
                log,
                index,
                waiting: VecDeque::new(),
                last,
                seals: BTreeMap::new(),
                now_us,
                mutation: Mutation::None,
            })
        })
    }

    pub fn log(&self) -> &Log {
        &self.log
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn set_retention(&mut self, retention: Retention) {
        self.index.set_retention(retention);
    }

    /// Answers waiting for a write.
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    pub fn handle(&mut self, request: Request, reply: R) -> Handled<R> {
        match request {
            Request::Seal { run, epoch, token } => self.seal(run, epoch, token, reply),
            Request::Append { run, epoch, seq, records } => self.append(run, epoch, seq, records, reply),
            Request::Read { run, epoch, from, to } => {
                let read = self.log.read(&run, Address { epoch, seq: from }, READ_BYTES);
                Handled::Read(
                    reply,
                    Box::pin(async move {
                        let records = read
                            .await?
                            .into_iter()
                            .filter_map(|e| match e {
                                LogEntry::Record { at, body } if at.epoch == epoch && at.seq <= to => Some((at, body)),
                                _ => None,
                            })
                            .enumerate()
                            .take_while(|(i, (at, _))| at.seq == from + *i as u64)
                            .map(|(_, (_, body))| body)
                            .collect();
                        Ok(Answer::Records { epoch, from, records })
                    }),
                )
            }
            Request::Probe { run } => {
                let segments = self.segments(&run);
                let summary = self.index.get(&run).map(|ix| (ix.segment, ix.summary.clone()));
                self.later(reply, Answer::Probed { segments, summary })
            }
            Request::Pages { after, limit } => {
                let limit = limit as usize;
                let mut keys = self.log.keys(after.as_deref(), limit + 1);
                if after.is_some_and(|after| keys.first().is_some_and(|(k, _)| *k == after)) {
                    keys.remove(0);
                }
                let more = keys.len() > limit;
                // Past half its retention a finished run is as good as gone: a peer that holds none of it may have
                // removed it already, and must not take it back.
                let going = |run: &[u8]| {
                    self.index.get(run).is_none_or(|ix| {
                        let (status, keep) =
                            (ix.summary.status, self.index.retention().of(&ix.summary.workflow, ix.summary.status));
                        status.is_finished() && self.now_us >= ix.updated_us.saturating_add(keep / 2)
                    })
                };
                let runs = keys.into_iter().take(limit).map(|(run, info)| (going(&run), run, info.epochs));
                let runs = runs.map(|(g, run, epochs)| (run, epochs, g)).collect();
                Handled::Now(reply, Answer::Pages { runs, more })
            }
        }
    }

    fn segments(&self, run: &[u8]) -> Vec<(u64, u64)> {
        self.log.key(run).map(|k| k.epochs).unwrap_or_default()
    }

    fn length(&self, run: &[u8], epoch: u64) -> u64 {
        self.log.key(run).and_then(|k| k.epochs.iter().find(|(e, _)| *e == epoch).map(|(_, n)| *n)).unwrap_or(0)
    }

    fn fence(&self, run: &[u8]) -> u64 {
        self.log.key(run).map_or(0, |k| k.fence)
    }

    /// Answered once every change taken so far is durable.
    fn later(&mut self, reply: R, answer: Answer) -> Handled<R> {
        if self.log.durable() >= self.last {
            return Handled::Now(reply, answer);
        }
        self.waiting.push_back((self.last, reply, answer));
        Handled::Queued
    }

    fn seal(&mut self, run: Bytes, epoch: u64, token: u64, reply: R) -> Handled<R> {
        if self.mutation == Mutation::SkipFence || self.seals.get(&run) == Some(&(epoch, token)) {
            let segments = self.segments(&run);
            return self.later(reply, Answer::Sealed { epoch, segments });
        }
        match self.log.fence(&run, epoch) {
            Ok((lsn, segments)) => {
                self.last = self.last.max(lsn);
                if self.seals.len() >= SEALS {
                    self.seals.clear();
                }
                self.seals.insert(run, (epoch, token));
                let answer = Answer::Sealed { epoch, segments };
                if self.mutation == Mutation::NoFenceSync {
                    return Handled::Now(reply, answer);
                }
                self.later(reply, answer)
            }
            Err(LogError::Fenced { fence }) => Handled::Now(reply, Answer::Fenced { epoch, fence }),
            Err(e) => Handled::Now(reply, Answer::Refused { epoch, why: e.to_string() }),
        }
    }

    fn append(&mut self, run: Bytes, epoch: u64, seq: u64, records: Vec<Bytes>, reply: R) -> Handled<R> {
        let fence = self.fence(&run);
        if epoch < fence {
            return Handled::Now(reply, Answer::Fenced { epoch, fence });
        }
        let mut have = self.length(&run, epoch);
        if seq == 0 || seq > have + 1 {
            return Handled::Now(reply, Answer::Gap { epoch, have });
        }
        // Records it holds already, from a try sent again: a replica holds a prefix of the segment.
        let held = (have + 1 - seq) as usize;
        for body in records.into_iter().skip(held) {
            let at = Address { epoch, seq: have + 1 };
            match self.log.append_at(&run, at, body.clone()) {
                Ok(lsn) => {
                    self.last = self.last.max(lsn);
                    self.index.record(&run, at, &body, self.now_us);
                    have += 1;
                }
                // What went in is answered once durable; the writer sends the rest again.
                Err(_) if have >= seq => break,
                Err(LogError::Fenced { fence }) => return Handled::Now(reply, Answer::Fenced { epoch, fence }),
                Err(LogError::Gap { have }) => return Handled::Now(reply, Answer::Gap { epoch, have }),
                Err(e) => return Handled::Now(reply, Answer::Refused { epoch, why: e.to_string() }),
            }
        }
        self.later(reply, Answer::Acked { epoch, have })
    }

    /// What this replica lacks of a page a peer sent: each segment at or above the run's fence here that the peer
    /// holds more of, oldest first, as `(run, epoch, from, to)`. One below the fence is sealed here, and stays. A
    /// run the peer has as good as gone, and this replica holds no record of, is left: it may be one this replica
    /// removed.
    pub fn lacks(&self, runs: &[Listed]) -> Vec<(Bytes, u64, u64, u64)> {
        let mut lacks = Vec::new();
        for (run, segments, going) in runs {
            if *going && self.index.get(run).is_none() {
                continue;
            }
            let fence = self.fence(run);
            for &(epoch, n) in segments.iter().filter(|(e, _)| *e >= fence) {
                let have = self.length(run, epoch);
                if n > have {
                    lacks.push((run.clone(), epoch, have + 1, n));
                }
            }
        }
        lacks
    }

    /// Removes up to `limit` finished runs whose last record here is older than their retention. How many went.
    pub fn expire(&mut self, limit: usize) -> usize {
        let runs: Vec<Bytes> = self.index.expired(self.now_us).take(limit).cloned().collect();
        let mut gone = 0;
        for run in runs {
            let Ok(lsn) = self.log.remove(&run) else { break };
            self.last = self.last.max(lsn);
            self.index.remove(&run);
            self.seals.remove(&run);
            gone += 1;
        }
        gone
    }

    /// Up to `limit` runs of the index from `from` on, encoded for [`open`](Self::open); the run to go on from.
    /// Each names the segment and length it is of, so a snapshot taken over many turns, or of records that never
    /// reached disk, is read back right: a run that does not match its log is read again.
    pub fn snapshot(&self, from: Option<&Bytes>, limit: usize, out: &mut Vec<u8>) -> Option<Bytes> {
        let mut runs = self.index.from(from);
        for run in runs.by_ref().take(limit) {
            out.extend_from_slice(&encode(&run));
        }
        runs.next().map(|(run, _)| run.clone())
    }

    /// The group commit: every change since the last write, in one write. Run it, then hand its result to
    /// [`written`](Self::written).
    pub fn take_write(&mut self, now_ms: u64) -> Option<FileFuture<()>> {
        self.log.take_write(now_ms)
    }

    /// The answers the write made durable.
    pub fn written(&mut self, result: io::Result<()>) -> Vec<(R, Answer)> {
        let durable = self.log.written(result);
        let mut due = Vec::new();
        while self.waiting.front().is_some_and(|(lsn, ..)| *lsn <= durable) {
            if let Some((_, reply, answer)) = self.waiting.pop_front() {
                due.push((reply, answer));
            }
        }
        due
    }
}

/// A run's newest segment that holds records, and its length.
fn newest(epochs: &[(u64, u64)]) -> Option<(u64, u64)> {
    epochs.iter().filter(|(_, n)| *n > 0).max().copied()
}

/// Seals a snapshot that [`Replica::snapshot`] filled: its CRC-32 after it.
pub fn seal_snapshot(mut out: Vec<u8>) -> Bytes {
    out.extend_from_slice(&crc32(&out).to_le_bytes());
    out.into()
}

/// A snapshot's index, `None` for one that does not read back whole.
fn load(snapshot: &Bytes) -> Option<Index> {
    let body = snapshot.len().checked_sub(4).map(|n| snapshot.slice(..n))?;
    if crc32(&body).to_le_bytes() != snapshot[body.len()..] {
        return None;
    }
    let (mut index, mut rest) = (Index::default(), &body[..]);
    while !rest.is_empty() {
        let ((run, ix), more) = postcard::take_from_bytes::<(Bytes, Indexed)>(rest).ok()?;
        index.put(run, ix);
        rest = more;
    }
    Some(index)
}

#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    pub replicas: usize,
    /// Answers a seal waits for, and replicas a record must be synced on to count: a majority.
    pub quorum: usize,
    /// The records of one `Append`, at most: it fits a frame of 1 MiB with its header.
    pub max_batch_bytes: usize,
    /// Records not yet acked, at most: past it, `append` refuses and the run waits.
    pub max_unacked_bytes: usize,
    /// Records kept for a replica that lags, at most: past it, the replica is left to repair.
    pub max_kept_bytes: usize,
    /// A carry past this is not sent: the writer waits for every replica's seal instead. `Own` must fit a record.
    pub max_carry_bytes: usize,
    /// A request with no answer this long is sent again.
    pub resend_ms: u64,
    pub mutation: Mutation,
}

impl WriterConfig {
    pub fn new(replicas: usize) -> Self {
        Self {
            replicas,
            quorum: replicas / 2 + 1,
            max_batch_bytes: 960 << 10,
            max_unacked_bytes: 1 << 20,
            max_kept_bytes: 4 << 20,
            max_carry_bytes: 512 << 10,
            resend_ms: 1_000,
            mutation: Mutation::None,
        }
    }
}

/// What the writer asks of its caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    Send(usize, Request),
    /// The run's journal before this owner's `Own`, which a majority now holds: replay it.
    Opened(Vec<Bytes>),
    /// The journal's first `n` entries are on a majority, this owner's `Own` included.
    Acked(u64),
    /// A newer owner sealed the run, or a replica holds a fence this writer cannot pass: stop.
    Fenced(u64),
    /// The journal does not read back: a record it counts is on no replica of the majority, or does not decode.
    Failed(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum AppendError {
    /// The writer is not open yet, or has ended.
    NotOpen,
    /// `max_unacked_bytes` are not acked yet: wait for `Out::Acked`.
    Behind,
    /// Past `max_batch_bytes`: an `Append` of it alone would not fit a frame, and would be sent again for ever.
    TooLarge,
}

enum Phase {
    Sealing(Sealing),
    Reading(Reading),
    Writing(Writing),
    Ended,
}

struct Sealing {
    answers: Vec<Option<Vec<(u64, u64)>>>,
    sent: Vec<u64>,
    /// Waits for every replica, and carries nothing.
    all: bool,
}

struct Reading {
    /// The replicas that answered the seal, and their segments.
    sealed: Vec<(usize, BTreeMap<u64, u64>)>,
    /// Segments resolved so far, newest first, each with the records it counts.
    chain: Vec<(u64, u64)>,
    /// The segment being resolved.
    at: (u64, u64),
    pool: BTreeMap<Address, Bytes>,
    /// The read out: the replica, its first record, and when it was sent.
    read: Option<(usize, u64, u64)>,
    carry: Vec<Address>,
    prev: Option<Address>,
}

struct Writing {
    /// The journal before `Own`, until `Own` is acked.
    opened: Option<Vec<Bytes>>,
    /// Entries before this owner's `Own`.
    base: u64,
    /// Records from `first` on, kept until every replica that keeps up has them.
    records: VecDeque<Bytes>,
    first: u64,
    kept_bytes: usize,
    next: u64,
    acked: Vec<u64>,
    sent: Vec<u64>,
    /// When a replica last answered or was last sent to, in ms.
    heard: Vec<u64>,
    /// Left to repair: it lags past what is kept.
    behind: Vec<bool>,
    committed: u64,
    /// Bytes of the records past `committed`.
    unacked_bytes: usize,
}

/// A run's owner's side of the journal: seal, read, `Own`, then appends.
pub struct Writer {
    run: Bytes,
    lease: u64,
    token: u64,
    epoch: u64,
    cfg: WriterConfig,
    phase: Phase,
    out: Vec<Out>,
    /// Written into `Own`: the node and its wall clock, in µs, shown on the console.
    by: (String, u64),
}

impl Writer {
    /// Starts the open: seals every replica at the journal epoch of `lease`. `token` tells this writer's seals from
    /// another's: a random number.
    pub fn open(run: Bytes, lease: u64, token: u64, cfg: WriterConfig, now: u64) -> Writer {
        let mut w =
            Writer { run, lease, token, epoch: 0, cfg, phase: Phase::Ended, out: Vec::new(), by: Default::default() };
        match journal_epoch(lease) {
            Some(epoch) => w.seal(epoch, false, now),
            None => w.fail(format!("lease epoch {lease} is past the journal's epochs")),
        }
        w
    }

    /// Names this owner in its `Own`: `node` and its wall clock at the open, in µs.
    pub fn opened_by(mut self, node: &str, at_us: u64) -> Writer {
        self.by = (node.to_string(), at_us);
        self
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn is_open(&self) -> bool {
        matches!(&self.phase, Phase::Writing(w) if w.opened.is_none())
    }

    /// What to do now, in order.
    pub fn take_out(&mut self) -> Vec<Out> {
        std::mem::take(&mut self.out)
    }

    fn quorum(&self, acks: bool) -> usize {
        match self.cfg.mutation {
            Mutation::AckOnOne if acks => 1,
            Mutation::ReadOneReplica if !acks => 1,
            _ => self.cfg.quorum,
        }
    }

    fn fail(&mut self, why: String) {
        self.phase = Phase::Ended;
        self.out.push(Out::Failed(why));
    }

    fn seal(&mut self, epoch: u64, all: bool, now: u64) {
        self.epoch = epoch;
        let n = self.cfg.replicas;
        self.phase = Phase::Sealing(Sealing { answers: vec![None; n], sent: vec![now; n], all });
        let seal = Request::Seal { run: self.run.clone(), epoch, token: self.token };
        self.out.extend((0..n).map(|r| Out::Send(r, seal.clone())));
    }

    /// Queues an entry and sends it. Its index in the journal once `Out::Acked` passes it.
    pub fn append(&mut self, body: Bytes, now: u64) -> Result<u64, AppendError> {
        let index = self.queue(body)?;
        self.flush(now);
        Ok(index)
    }

    /// Queues an entry to send at the next `flush`: entries made together go in one `Append`, not one each.
    pub fn queue(&mut self, body: Bytes) -> Result<u64, AppendError> {
        let max = self.cfg.max_unacked_bytes;
        if body.len() > self.cfg.max_batch_bytes {
            return Err(AppendError::TooLarge);
        }
        let Phase::Writing(w) = &mut self.phase else { return Err(AppendError::NotOpen) };
        if w.opened.is_some() {
            return Err(AppendError::NotOpen);
        }
        if w.unacked_bytes > 0 && w.unacked_bytes + body.len() > max {
            return Err(AppendError::Behind);
        }
        let index = w.base + w.next - 1;
        w.unacked_bytes += body.len();
        w.kept_bytes += body.len();
        w.records.push_back(body);
        w.next += 1;
        Ok(index)
    }

    /// Sends each replica the records it lacks, as far as the batch bound goes.
    pub fn flush(&mut self, now: u64) {
        let Phase::Writing(w) = &mut self.phase else { return };
        for r in 0..self.cfg.replicas {
            while !w.behind[r] && w.sent[r] + 1 < w.next {
                let seq = w.sent[r] + 1;
                let mut records = Vec::new();
                let mut bytes = 0;
                for body in w.records.range((seq - w.first) as usize..) {
                    if !records.is_empty() && bytes + body.len() > self.cfg.max_batch_bytes {
                        break;
                    }
                    bytes += body.len();
                    records.push(body.clone());
                }
                w.sent[r] += records.len() as u64;
                w.heard[r] = now;
                self.out.push(Out::Send(r, Request::Append { run: self.run.clone(), epoch: self.epoch, seq, records }));
            }
        }
    }

    /// Sends again what got no answer within `resend_ms`.
    pub fn tick(&mut self, now: u64) {
        let resend = self.cfg.resend_ms;
        match &mut self.phase {
            Phase::Sealing(s) => {
                for r in 0..self.cfg.replicas {
                    if s.answers[r].is_none() && now >= s.sent[r] + resend {
                        s.sent[r] = now;
                        let seal = Request::Seal { run: self.run.clone(), epoch: self.epoch, token: self.token };
                        self.out.push(Out::Send(r, seal));
                    }
                }
            }
            Phase::Reading(rd) => {
                if let Some((r, _, sent)) = rd.read
                    && now >= sent + resend
                {
                    // Another replica, if one has it.
                    rd.read = None;
                    self.next_read(now, Some(r));
                }
            }
            Phase::Writing(w) => {
                for r in 0..self.cfg.replicas {
                    if now < w.heard[r] + resend {
                        continue;
                    }
                    if !w.behind[r] && w.acked[r] + 1 < w.next {
                        w.sent[r] = w.acked[r];
                    } else if w.behind[r] {
                        // Has repair brought it up to what is kept? Its answer says.
                        w.heard[r] = now;
                        let probe =
                            Request::Append { run: self.run.clone(), epoch: self.epoch, seq: w.first, records: vec![] };
                        self.out.push(Out::Send(r, probe));
                    }
                }
                self.flush(now);
            }
            Phase::Ended => {}
        }
    }

    pub fn answer(&mut self, replica: usize, answer: Answer, now: u64) {
        if replica >= self.cfg.replicas {
            return;
        }
        match answer {
            Answer::Sealed { epoch, segments } if epoch == self.epoch => self.sealed(replica, segments, now),
            Answer::Fenced { epoch, fence } if epoch == self.epoch => self.fenced(replica, fence, now),
            Answer::Records { epoch, from, records } => self.records(replica, epoch, from, records, now),
            Answer::Acked { epoch, have } if epoch == self.epoch => self.acked(replica, have, now),
            Answer::Gap { epoch, have } if epoch == self.epoch => self.gap(replica, have, now),
            // Refused or late: the resend tries again.
            _ => {}
        }
    }

    fn fenced(&mut self, replica: usize, fence: u64, now: u64) {
        match &self.phase {
            // A seal of ours that came twice: its first answer counts.
            Phase::Sealing(s) if fence == self.epoch && s.answers[replica].is_some() => {}
            // A start before this one on this node, under the same lease, sealed it: take the next epoch.
            Phase::Sealing(_) if fence >> 8 == self.lease && fence & 0xff < 0xff => self.seal(fence + 1, false, now),
            // An answer to a seal, which a seal of our own sent twice can give, after the open.
            Phase::Reading(_) | Phase::Writing(_) if fence == self.epoch => {}
            Phase::Ended => {}
            _ => {
                self.phase = Phase::Ended;
                self.out.push(Out::Fenced(fence));
            }
        }
    }

    fn sealed(&mut self, replica: usize, segments: Vec<(u64, u64)>, now: u64) {
        let (quorum, q) = (self.quorum(false), self.cfg.quorum);
        let Phase::Sealing(s) = &mut self.phase else { return };
        s.answers[replica].get_or_insert(segments);
        let all = s.all;
        if s.answers.iter().flatten().count() < if all { s.answers.len() } else { quorum } {
            return;
        }
        let epoch = self.epoch;
        let sealed: Vec<(usize, BTreeMap<u64, u64>)> = s
            .answers
            .iter()
            .enumerate()
            .filter_map(|(r, a)| a.as_ref().map(|a| (r, a.iter().copied().filter(|(e, _)| *e < epoch).collect())))
            .collect();
        // Each segment's lengths, longest first.
        let mut lens: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for (e, n) in sealed.iter().flat_map(|(_, s)| s.iter()) {
            lens.entry(*e).or_default().push(*n);
        }
        for l in lens.values_mut() {
            l.resize(sealed.len(), 0);
            l.sort_unstable_by(|a, b| b.cmp(a));
        }
        // From every replica, a segment whose `Own` is on fewer than a majority acked nothing, and records past the
        // quorum-th longest were never on a majority: none was acked.
        let held = |l: &Vec<u64>, i: usize| l.get(i).copied().unwrap_or(0);
        let newest = lens.iter().rev().find(|(_, l)| held(l, if all { q - 1 } else { 0 }) > 0);
        let Some((&newest, l)) = newest else {
            // A run with no record on the majority: this owner's is its first segment.
            self.start_writing(Vec::new(), None, Vec::new(), now);
            return;
        };
        // Records up to the quorum-th longest answer are on a majority, which every later seal meets.
        let on_majority = held(l, q - 1);
        let longest = if all { on_majority } else { l[0] };
        self.phase = Phase::Reading(Reading {
            sealed,
            chain: Vec::new(),
            at: (newest, longest),
            pool: BTreeMap::new(),
            read: None,
            // The rest of the newest segment goes in the new Own, once read.
            carry: (on_majority + 1..=longest).map(|seq| Address { epoch: newest, seq }).collect(),
            prev: Some(Address { epoch: newest, seq: longest }),
        });
        self.next_read(now, None);
    }

    /// Resolves segments from what is read, and asks for the next record missing, until the chain ends.
    fn next_read(&mut self, now: u64, avoid: Option<usize>) {
        loop {
            let Phase::Reading(rd) = &mut self.phase else { return };
            if rd.read.is_some() {
                return;
            }
            let (epoch, len) = rd.at;
            if let Some(seq) = (1..=len).find(|&seq| !rd.pool.contains_key(&Address { epoch, seq })) {
                // A replica of the majority that answered it holds the record.
                let mut holders: Vec<(usize, u64)> = rd
                    .sealed
                    .iter()
                    .map(|(r, s)| (*r, s.get(&epoch).copied().unwrap_or(0)))
                    .filter(|(_, n)| *n >= seq)
                    .collect();
                holders.sort_by_key(|(r, n)| (std::cmp::Reverse(*n), *r));
                let Some(&(r, n)) = holders.iter().find(|(r, _)| Some(*r) != avoid).or(holders.first()) else {
                    let why = format!("record {seq} of segment {epoch} is on no replica of the majority");
                    self.fail(why);
                    return;
                };
                rd.read = Some((r, seq, now));
                let to = len.min(n);
                self.out.push(Out::Send(r, Request::Read { run: self.run.clone(), epoch, from: seq, to }));
                return;
            }
            let own = rd.pool.get(&Address { epoch, seq: 1 }).map(|b| Entry::decode(b));
            let Some(Ok(Entry::Own(own))) = own else {
                let why = format!("segment {epoch} does not start with Own");
                self.fail(why);
                return;
            };
            for (at, body) in own.carry {
                rd.pool.entry(at).or_insert(body);
            }
            rd.chain.push((epoch, len));
            match own.prev {
                Some(prev) if prev.epoch < epoch => rd.at = (prev.epoch, prev.seq),
                Some(prev) => {
                    let why = format!("segment {epoch} names segment {} before it", prev.epoch);
                    self.fail(why);
                    return;
                }
                None => break,
            }
        }
        let Phase::Reading(rd) = std::mem::replace(&mut self.phase, Phase::Ended) else { return };
        let mut journal = Vec::new();
        for (epoch, len) in rd.chain.iter().rev() {
            for seq in 1..=*len {
                journal.extend(rd.pool.get(&Address { epoch: *epoch, seq }).cloned());
            }
        }
        let carry: Vec<(Address, Bytes)> = if self.cfg.mutation == Mutation::NoCarry {
            Vec::new()
        } else {
            rd.carry.into_iter().filter_map(|at| Some((at, rd.pool.get(&at)?.clone()))).collect()
        };
        if carry.iter().map(|(_, b)| b.len()).sum::<usize>() > self.cfg.max_carry_bytes {
            // The replicas that answered have this seal, and answer it again.
            self.seal(self.epoch, true, now);
            return;
        }
        self.start_writing(journal, rd.prev, carry, now);
    }

    fn records(&mut self, replica: usize, epoch: u64, from: u64, records: Vec<Bytes>, now: u64) {
        let Phase::Reading(rd) = &mut self.phase else { return };
        if epoch != rd.at.0 || rd.read.is_none_or(|(r, seq, _)| r != replica || seq != from) {
            return;
        }
        rd.read = None;
        if records.is_empty() {
            // It holds less than it answered: a fault, or a mutation. Ask another.
            for (r, s) in &mut rd.sealed {
                if *r == replica {
                    s.insert(epoch, from - 1);
                }
            }
        }
        for (i, body) in records.into_iter().enumerate() {
            rd.pool.insert(Address { epoch, seq: from + i as u64 }, body);
        }
        self.next_read(now, None);
    }

    fn start_writing(&mut self, journal: Vec<Bytes>, prev: Option<Address>, carry: Vec<(Address, Bytes)>, now: u64) {
        let (node, at_us) = self.by.clone();
        let own = Entry::Own(Own { prev, carry, summary: Summary::of(&journal), node, at_us }).encode();
        let n = self.cfg.replicas;
        let base = journal.len() as u64;
        self.phase = Phase::Writing(Writing {
            opened: Some(journal),
            base,
            unacked_bytes: own.len(),
            kept_bytes: own.len(),
            records: VecDeque::from([own]),
            first: 1,
            next: 2,
            acked: vec![0; n],
            sent: vec![0; n],
            heard: vec![now; n],
            behind: vec![false; n],
            committed: 0,
        });
        self.flush(now);
    }

    fn acked(&mut self, replica: usize, have: u64, now: u64) {
        let quorum = self.quorum(true);
        let max_kept = self.cfg.max_kept_bytes;
        let Phase::Writing(w) = &mut self.phase else { return };
        if have >= w.next {
            let why = format!("a replica holds {have} records of segment {}, written {}", self.epoch, w.next - 1);
            self.fail(why);
            return;
        }
        w.heard[replica] = now;
        if w.behind[replica] && have + 1 >= w.first {
            // Repair brought it up to what is kept: a probe's answer.
            w.behind[replica] = false;
            w.sent[replica] = have;
        }
        w.acked[replica] = w.acked[replica].max(have);
        w.sent[replica] = w.sent[replica].max(have);
        let mut acked = w.acked.clone();
        acked.sort_unstable_by(|a, b| b.cmp(a));
        let committed = acked[quorum - 1];
        if committed > w.committed {
            for seq in w.committed + 1..=committed {
                w.unacked_bytes -= w.records.get((seq - w.first) as usize).map_or(0, Bytes::len);
            }
            w.committed = committed;
            if let Some(journal) = w.opened.take() {
                self.out.push(Out::Opened(journal));
            }
            self.out.push(Out::Acked(w.base + committed));
        }
        // Keep what a replica that keeps up still lacks, up to max_kept_bytes; past it the slowest is left to repair.
        loop {
            let low = (0..w.acked.len()).filter(|r| !w.behind[*r]).map(|r| w.acked[r]).min().unwrap_or(committed);
            while w.first <= low.min(w.committed) {
                if let Some(body) = w.records.pop_front() {
                    w.kept_bytes -= body.len();
                }
                w.first += 1;
            }
            if w.kept_bytes <= max_kept {
                break;
            }
            let slowest =
                (0..w.acked.len()).filter(|r| !w.behind[*r] && w.acked[*r] < w.committed).min_by_key(|r| w.acked[*r]);
            let Some(slowest) = slowest else { break };
            w.behind[slowest] = true;
        }
        self.flush(now);
    }

    fn gap(&mut self, replica: usize, have: u64, now: u64) {
        let Phase::Writing(w) = &mut self.phase else { return };
        w.heard[replica] = now;
        if have + 1 < w.first {
            w.behind[replica] = true;
        } else {
            w.sent[replica] = have.min(w.next - 1);
        }
        self.flush(now);
    }
}

#[cfg(test)]
mod tests;
