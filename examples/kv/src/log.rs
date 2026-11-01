//! The node's Log: this node's replica of every shard, kept in one `axon::files::Log`, the write-ahead log, and in
//! a snapshot file per shard. A shard's owner asks it with `repl::ToLog`; it answers once the records the replica
//! wrote for that shard are durable. Every change of a turn goes in one write and one sync (group commit).
//!
//! The WAL holds one key. Each record is one answer's records for one shard. A shard's snapshot holds its whole
//! replica, the promise with it, through one WAL record; the records every snapshot covers are trimmed, and the Log
//! removes a segment once nothing in it is live. An `Install` of a map is written as the shard's snapshot, not in
//! the WAL, as a map may be larger than a WAL record.
//!
//! Owners on other nodes reach it as `kv.log` ([`crate::wire::LogApi`]): a message larger than a frame comes as an
//! upload, at most `UPLOADS_AT_ONCE` read at a time, and a `Fetch` is answered in parts, at most `FETCHES_AT_ONCE` at a
//! time.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::files::{Address, Entry, FileOp, Files, Log, LogConfig, LogError, LogStats};
use axon::{Actor, Context, ReplyTo, TaskAdmissionError};
use axon_cluster::{CallError, Reply};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Stats;
use crate::repl::codec::{self, entries_len};
use crate::repl::{FromLog, Rec, Replica, State, ToLog, Weak};
use crate::shard::{PART, read_upload, split};
use crate::slot::SHARDS;
use crate::wire::{from_log_bytes, read_log_body};

/// The WAL's one key, written at one epoch.
const KEY: &[u8] = b"wal";
const EPOCH: u64 = 1;
/// How long the Log waits after a failed write before it writes the same bytes again.
const REST: Duration = Duration::from_millis(100);
/// Shards in one snapshot write.
const SNAPS_AT_ONCE: usize = 8;
/// Bytes of WAL read at once on start: `Files::read_range` syncs the file on every call.
const REPLAY_READ: usize = 8 << 20;
/// What a WAL record adds to the records it holds: the frame, the shard, a promise, a commit and an Install's
/// position, 117 bytes, with room. An estimate below the record makes `append` refuse it, and the Log end.
const OVERHEAD: usize = 256;
/// `Fetch` answers sent in parts at once; the rest wait. Each holds a shard's map encoded, and a node whose shards
/// all moved away and back asks for about 85 at once.
const FETCHES_AT_ONCE: usize = 8;
/// Upload bodies read at once; past this one is not answered, and its owner sends it again. A restarted node's Log is
/// sent an Install for each of its shards, from every owner at once.
const UPLOADS_AT_ONCE: usize = 32;
/// The Log's jobs: the uploads, the answers in parts, a write and a snapshot round. A refused write or snapshot ends
/// the Log, so the other jobs never take their places.
pub const JOBS: usize = UPLOADS_AT_ONCE + FETCHES_AT_ONCE + 2;
/// Calls held for one shard while its Install is written; past this a call is not answered, and its owner sends it
/// again.
const HELD_PER_SHARD: usize = 16;
/// The largest upload taken: an Install of a shard's whole map.
const MAX_UPLOAD: usize = 2 << 30;
const SNAP_MAGIC: &[u8; 4] = b"KVSN";
const SNAP_VERSION: u16 = 1;

/// The WAL's files: segments of 64 MiB, and a record as large as the largest command, 33 MiB, with room.
pub fn wal_config() -> LogConfig {
    LogConfig {
        name: "wal".into(),
        max_record: 40 << 20,
        max_write_bytes: 64 << 20,
        max_unwritten_bytes: 256 << 20,
        max_bytes: 64 << 30,
        ..LogConfig::default()
    }
}

#[derive(Clone, Debug)]
pub struct LogSettings {
    pub wal: LogConfig,
    /// The WAL on disk past which the shards holding its oldest records are snapshotted: this, or the bytes of the
    /// last snapshots if more, so a large data set is not written out whole for every few MiB of log.
    pub wal_floor: u64,
    /// Committed entries kept in memory past the applied map, not in snapshots: a new owner whose own Log is this
    /// many commits behind fetches entries, not the whole map, while they fit in `keep_bytes`. A node of one Log
    /// keeps none.
    pub keep: u64,
    /// Of those, at most this many bytes of keys and values per shard: an entry larger is not kept. 1 MiB: the 256
    /// shards' together are at most the 256 MiB of WAL that bounded them before, between snapshot rounds.
    pub keep_bytes: u64,
    pub weak: Weak,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self { wal: wal_config(), wal_floor: 256 << 20, keep: 64, keep_bytes: 1 << 20, weak: Weak::default() }
    }
}

/// Where a call's answer goes: an owner on this node, or one on another node.
pub enum To {
    Local(ReplyTo<FromLog>),
    Remote(Reply<Bytes>),
    /// In parts: a `Fetch` from another node.
    Parts(Reply<Bytes>),
}

pub enum LogMsg {
    /// A shard's owner asks its replica here; answered once what it wrote is durable.
    Call(u16, ToLog, To),
    /// A call from another node larger than a frame: its body comes as an upload.
    Upload(Reply<Bytes>),
    /// An upload's body, or `None` where it was cut or past `MAX_UPLOAD`.
    Uploaded(u64, Option<Bytes>),
    /// An answer in parts ended.
    Sent,
    Status(ReplyTo<LogStatus>),
    /// Each shard's committed index and a hash of its applied map: equal on every Log once writes stop.
    Digest(ReplyTo<Vec<(u64, u64)>>),
    Opened(io::Result<Box<Loaded>>),
    Written(io::Result<()>),
    /// The rest after a failed write is over.
    Rested,
    /// The rest after a failed snapshot write is over.
    SnapRested,
    /// Shards, each with the WAL record its snapshot holds and the snapshot's length.
    Snapped(Vec<(u16, u64, u64)>, io::Result<()>),
}

#[derive(Clone, Debug, Default)]
pub struct LogStatus {
    pub wal: LogStats,
    /// Entries of the shards' writes in records made durable: `wal.writes` made them so.
    pub entries: u64,
    pub snaps: u64,
    /// The bytes of the snapshot files there are now.
    pub snap_bytes: u64,
    /// Answers held for a write to land, and calls held for room in the WAL.
    pub waiting: usize,
    pub backlog: usize,
    /// Installs written as snapshots and answered; those being written, and the calls held behind them.
    pub installs: u64,
    pub installing: usize,
    pub held: usize,
    pub failing: bool,
    /// Entries the replicas hold in memory past their applied maps, and their bytes of keys and values: those not
    /// committed, and at most `keep` committed ones each, in `keep_bytes`.
    pub in_memory: u64,
    pub in_memory_bytes: u64,
}

/// What a start read: the WAL and each shard's replica.
pub struct Loaded {
    log: Log,
    shards: Vec<ShardLog>,
}

#[derive(Default)]
struct ShardLog {
    replica: Replica,
    /// The WAL position (LSN) of its last record that is not a commit alone: its answers wait for it.
    need: u64,
    /// The LSN and WAL number of its last record.
    last: u64,
    last_seq: u64,
    /// The WAL number of its first record no snapshot covers.
    first: Option<u64>,
    /// While a snapshot is written: the first record after it.
    after: Option<u64>,
    snapping: bool,
    /// The WAL record its snapshot holds, and the snapshot's length.
    through: u64,
    snap_bytes: u64,
    /// An Install taken but not yet on disk, with its answer, and the shard's calls that came after it.
    install: Option<(To, FromLog)>,
    /// The snapshot being written holds the Install: one taken before it came does not.
    install_snapping: bool,
    held: VecDeque<(ToLog, To)>,
}

pub struct LogActor {
    wal: Arc<dyn Files>,
    snaps: Arc<dyn Files>,
    settings: LogSettings,
    stats: Arc<Stats>,
    log: Option<Log>,
    shards: Vec<ShardLog>,
    /// Answers by the LSN each waits for, then arrival.
    waiting: BTreeMap<(u64, u64), (To, FromLog)>,
    arrivals: u64,
    /// Calls that wait for room in the WAL, in arrival order.
    backlog: VecDeque<(u16, ToLog, To)>,
    /// Uploads being read, by job.
    uploads: HashMap<u64, Reply<Bytes>>,
    next_upload: u64,
    /// Answers in parts being sent, and those waiting their turn.
    sending: usize,
    to_send: VecDeque<(Reply<Bytes>, FromLog)>,
    /// Entries in each record not yet durable, by LSN.
    unwritten: VecDeque<(u64, u64)>,
    resting: bool,
    snapping: bool,
    status: LogStatus,
}

impl LogActor {
    pub fn new(wal: Arc<dyn Files>, snaps: Arc<dyn Files>, settings: LogSettings, stats: Arc<Stats>) -> Self {
        Self {
            wal,
            snaps,
            settings,
            stats,
            log: None,
            shards: Vec::new(),
            waiting: BTreeMap::new(),
            arrivals: 0,
            backlog: VecDeque::new(),
            uploads: HashMap::new(),
            next_upload: 0,
            sending: 0,
            to_send: VecDeque::new(),
            unwritten: VecDeque::new(),
            resting: false,
            snapping: false,
            status: LogStatus::default(),
        }
    }

    fn log(&mut self) -> &mut Log {
        self.log.as_mut().expect("calls wait for the open")
    }

    /// Room in the WAL for what `msg` may write: refused, the write is held until a write or a trim makes room.
    fn room(&self, msg: &ToLog) -> bool {
        let log = self.log.as_ref().expect("calls wait for the open");
        let st = log.stats();
        let cfg = &self.settings.wal;
        let est = estimate(msg);
        st.unwritten_bytes + est <= cfg.max_unwritten_bytes && st.live_bytes + est as u64 <= cfg.max_bytes
    }

    /// A call now, or once the WAL has room.
    fn admit(&mut self, shard: u16, msg: ToLog, to: To, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if usize::from(shard) >= self.shards.len() {
            return Ok(());
        }
        if self.backlog.is_empty() && self.room(&msg) {
            self.call(shard, msg, to, cx)
        } else {
            self.backlog.push_back((shard, msg, to));
            Ok(())
        }
    }

    fn call(&mut self, shard: u16, msg: ToLog, to: To, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let s = usize::from(shard);
        let sh = &mut self.shards[s];
        if sh.install.is_some() {
            // Its records go after the Install's snapshot. Dropped past the bound: the owner sends it again.
            if sh.held.len() < HELD_PER_SHARD {
                sh.held.push_back((msg, to));
            }
            return Ok(());
        }
        if estimate(&msg) > self.settings.wal.max_record {
            // Larger than a WAL record: only from a node with larger limits than this one. Not answered.
            return Ok(());
        }
        let (answer, recs) = sh.replica.handle(msg);
        // Committed entries go as they commit. Left to the next snapshot, every shard's went in one burst of about a
        // second on this thread, on every node at once.
        sh.replica.compact(self.settings.keep, self.settings.keep_bytes);
        if recs.iter().any(|r| matches!(r, Rec::Install(_))) {
            sh.install = Some((to, answer));
            return Ok(());
        }
        if !recs.is_empty() {
            let entries: u64 =
                recs.iter().map(|r| if let Rec::Entries { entries, .. } = r { entries.len() as u64 } else { 0 }).sum();
            let lazy = recs.iter().all(|r| matches!(r, Rec::Commit(_)));
            let appended = self
                .log()
                .append(KEY, EPOCH, wal_record(shard, &recs))
                .map_err(|e| format!("the WAL refused a record of shard {shard}: {e}"))?;
            let sh = &mut self.shards[s];
            (sh.last, sh.last_seq) = (appended.lsn.0, appended.at.seq);
            if !lazy {
                sh.need = appended.lsn.0;
            }
            sh.first.get_or_insert(appended.at.seq);
            if sh.snapping {
                sh.after.get_or_insert(appended.at.seq);
            }
            self.unwritten.push_back((appended.lsn.0, entries));
        }
        let need = self.shards[s].need;
        if need <= self.log().durable().0 {
            self.answer(to, answer, cx);
        } else {
            self.arrivals += 1;
            self.waiting.insert((need, self.arrivals), (to, answer));
        }
        Ok(())
    }

    fn answer(&mut self, to: To, answer: FromLog, cx: &mut Context<'_, Self>) {
        match to {
            To::Local(reply) => drop(reply.reply(answer)),
            To::Remote(reply) => drop(reply.send(from_log_bytes(&answer))),
            To::Parts(reply) => {
                self.to_send.push_back((reply, answer));
                self.send_parts(cx);
            }
        }
    }

    /// Answers in parts while fewer than `FETCHES_AT_ONCE` are out. Pulled as the caller reads; a refused job drops
    /// the answer, and the caller asks again.
    fn send_parts(&mut self, cx: &mut Context<'_, Self>) {
        while self.sending < FETCHES_AT_ONCE
            && let Some((reply, answer)) = self.to_send.pop_front()
        {
            // The owner gave up on it and asked again: the newer one is behind it in the queue.
            if reply.is_abandoned() {
                continue;
            }
            let body = from_log_bytes(&answer);
            let size = reply.max_payload().saturating_sub(16).clamp(1, PART);
            let sending = reply.send_parts(futures_util::stream::iter(split(body, size).map(Ok::<_, CallError>)));
            let job = cx.submit_future(async move {
                let _ = sending.await;
                LogMsg::Sent
            });
            self.sending += usize::from(job.is_ok());
        }
    }

    /// The body of an upload, read whole in a job: each part read gives the caller more credit.
    fn upload(&mut self, reply: Reply<Bytes>, cx: &mut Context<'_, Self>) {
        if self.uploads.len() >= UPLOADS_AT_ONCE {
            return;
        }
        let Some(upload) = reply.upload() else { return };
        let id = self.next_upload;
        self.next_upload += 1;
        let read = async move { LogMsg::Uploaded(id, read_upload(upload, MAX_UPLOAD).await.ok()) };
        // Refused (the Log is ending): the reply is dropped.
        if cx.submit_future(read).is_ok() {
            self.uploads.insert(id, reply);
        }
    }

    /// FNV-1a of each shard's applied map, with its committed index: the same on every Log that holds the same.
    fn digest(&self) -> Vec<(u64, u64)> {
        self.shards
            .iter()
            .map(|sh| {
                let mut h: u64 = 0xcbf2_9ce4_8422_2325;
                for (k, v) in sh.replica.applied() {
                    for part in
                        [&(k.len() as u32).to_be_bytes()[..], &k[..], &(v.len() as u32).to_be_bytes()[..], &v[..]]
                    {
                        for b in part {
                            h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
                        }
                    }
                }
                (sh.replica.committed(), h)
            })
            .collect()
    }

    fn written(&mut self, result: io::Result<()>, cx: &mut Context<'_, Self>) -> Result<(), String> {
        // A disk that fails at once would otherwise be tried again at once, and again.
        self.resting = result.is_err() && cx.schedule_after(REST, LogMsg::Rested).is_ok();
        let durable = self.log().written(result).0;
        while let Some(entry) = self.waiting.first_entry()
            && entry.key().0 <= durable
        {
            let (to, answer) = entry.remove();
            self.answer(to, answer, cx);
        }
        let mut landed = 0;
        while let Some(&(lsn, entries)) = self.unwritten.front()
            && lsn <= durable
        {
            landed += entries;
            self.unwritten.pop_front();
        }
        self.status.entries += landed;
        self.drain(cx)
    }

    /// Takes the held calls a write or a trim made room for.
    fn drain(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        while let Some((_, msg, _)) = self.backlog.front()
            && self.room(msg)
        {
            let (shard, msg, to) = self.backlog.pop_front().expect("a front");
            self.call(shard, msg, to, cx)?;
        }
        Ok(())
    }

    /// Snapshots the shards with an Install to write, then, once the WAL is past its target, those holding the
    /// oldest records: up to `SNAPS_AT_ONCE` shards. A shard is snapshotted only once its last record is durable, so
    /// a snapshot never holds what the WAL may yet lose.
    fn snapshot(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_ref() else { return Ok(()) };
        if self.snapping {
            return Ok(());
        }
        let durable = log.durable().0;
        let ready = |sh: &ShardLog| sh.last <= durable;
        let mut picks: Vec<usize> =
            (0..self.shards.len()).filter(|&s| self.shards[s].install.is_some() && ready(&self.shards[s])).collect();
        picks.truncate(SNAPS_AT_ONCE);
        // Only a segment before the one being written can go, so with one segment a snapshot frees nothing.
        let st = log.stats();
        if st.segments >= 2 && st.bytes > self.settings.wal_floor.max(self.status.snap_bytes) {
            let mut old: Vec<usize> = (0..self.shards.len())
                .filter(|&s| {
                    let sh = &self.shards[s];
                    sh.first.is_some() && sh.install.is_none() && ready(sh)
                })
                .collect();
            old.sort_by_key(|&s| self.shards[s].first);
            old.truncate(SNAPS_AT_ONCE - picks.len());
            picks.extend(old);
        }
        if picks.is_empty() {
            return Ok(());
        }
        let mut states = Vec::with_capacity(picks.len());
        for s in picks {
            let sh = &mut self.shards[s];
            let Rec::Checkpoint(state) = sh.replica.checkpoint() else { unreachable!() };
            (sh.snapping, sh.after, sh.install_snapping) = (true, None, sh.install.is_some());
            states.push((s as u16, sh.last_seq, state));
        }
        let snaps = self.snaps.clone();
        let job = cx.submit_future(async move {
            let mut ops = Vec::with_capacity(states.len());
            let mut done = Vec::with_capacity(states.len());
            for (s, through, state) in states {
                let bytes = snap_file(s, through, &state);
                done.push((s, through, bytes.len() as u64));
                ops.push(FileOp::Replace(snap_name(s), bytes));
            }
            let result = axon::catch_panic(snaps.apply(ops)).await;
            LogMsg::Snapped(done, result.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
        });
        self.snapping = true;
        job.map(drop).or_else(|e| ending(e.reason))
    }

    fn snapped(
        &mut self,
        done: Vec<(u16, u64, u64)>,
        result: io::Result<()>,
        cx: &mut Context<'_, Self>,
    ) -> Result<(), String> {
        let ok = result.is_ok();
        // A disk that refuses at once would otherwise be asked again at the end of every turn.
        self.snapping = !ok && cx.schedule_after(REST, LogMsg::SnapRested).is_ok();
        let mut installed = Vec::new();
        for (s, through, bytes) in done {
            let sh = &mut self.shards[usize::from(s)];
            sh.snapping = false;
            let after = sh.after.take();
            let holds_install = std::mem::take(&mut sh.install_snapping);
            if ok {
                self.status.snap_bytes = self.status.snap_bytes - sh.snap_bytes + bytes;
                (sh.first, sh.through, sh.snap_bytes) = (after, through, bytes);
                self.status.snaps += 1;
                if holds_install && let Some(install) = sh.install.take() {
                    installed.push((s, install, std::mem::take(&mut sh.held)));
                }
            }
        }
        // An Install is answered once its snapshot is on disk, then the calls held behind it run in order.
        for (s, (to, answer), held) in installed {
            self.status.installs += 1;
            self.answer(to, answer, cx);
            for (msg, to) in held {
                self.admit(s, msg, to, cx)?;
            }
        }
        if !ok {
            return Ok(());
        }
        // Every record before the first that no snapshot covers goes.
        let through = match self.shards.iter().filter_map(|s| s.first).min() {
            Some(first) => first - 1,
            None => self.shards.iter().map(|s| s.last_seq).max().unwrap_or(0),
        };
        if through > 0 {
            let at = Address { epoch: EPOCH, seq: through };
            match self.log().trim(KEY, at) {
                // No room for the trim's frame while writes are held: the next snapshot round trims as far.
                Ok(_) | Err(LogError::Behind) => {}
                Err(e) => return Err(format!("the WAL refused a trim: {e}")),
            }
        }
        self.drain(cx)
    }

    fn publish(&mut self) {
        let Some(log) = self.log.as_ref() else { return };
        let st = log.stats();
        self.status.wal = st;
        self.status.waiting = self.waiting.len();
        self.status.backlog = self.backlog.len();
        self.status.installing = self.shards.iter().filter(|sh| sh.install.is_some()).count();
        self.status.held = self.shards.iter().map(|sh| sh.held.len()).sum();
        self.status.failing = log.failing().is_some();
        let w = &self.stats.wal;
        w.writes.store(st.writes, Relaxed);
        w.entries.store(self.status.entries, Relaxed);
        w.bytes.store(st.bytes, Relaxed);
        w.segments.store(st.segments as u64, Relaxed);
        w.snaps.store(self.status.snaps, Relaxed);
        w.snap_bytes.store(self.status.snap_bytes, Relaxed);
        w.failed_writes.store(st.failed_writes, Relaxed);
    }
}

/// A job refused because the Log is ending goes with it: what waits on it is dropped, and its callers hear so.
fn ending(reason: TaskAdmissionError) -> Result<(), String> {
    match reason {
        TaskAdmissionError::Draining | TaskAdmissionError::Closed => Ok(()),
        other => Err(format!("a job was refused: {other:?}")),
    }
}

fn estimate(msg: &ToLog) -> usize {
    OVERHEAD
        + match msg {
            ToLog::Append { entries, .. } => entries_len(entries),
            // An Install's map goes in a snapshot; only a promise with it goes in the WAL.
            ToLog::Open { .. } | ToLog::Install { .. } | ToLog::Fetch { .. } => 0,
        }
}

impl Actor for LogActor {
    type Message = LogMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let load = load(self.wal.clone(), self.snaps.clone(), self.settings.clone());
        let job = cx.submit_future(async move {
            let loaded = axon::catch_panic(load).await;
            LogMsg::Opened(loaded.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
        });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: LogMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.log.is_none() {
            // The pause in on_start holds every other message until this one.
            let LogMsg::Opened(loaded) = msg else { return Err("a call before the WAL was read".into()) };
            let loaded = loaded.map_err(|e| format!("the WAL or a snapshot does not read back: {e}"))?;
            (self.log, self.shards) = (Some(loaded.log), loaded.shards);
            self.status.snap_bytes = self.shards.iter().map(|s| s.snap_bytes).sum();
            self.publish();
            return Ok(());
        }
        match msg {
            LogMsg::Call(shard, msg, to) => self.admit(shard, msg, to, cx)?,
            LogMsg::Upload(reply) => self.upload(reply, cx),
            LogMsg::Uploaded(id, body) => {
                let Some(reply) = self.uploads.remove(&id) else { return Ok(()) };
                // One that is cut or does not decode is not answered: the owner sends it again.
                if let Some((shard, msg)) = body.and_then(read_log_body) {
                    self.admit(shard, msg, To::Remote(reply), cx)?;
                }
            }
            LogMsg::Sent => {
                self.sending -= 1;
                self.send_parts(cx);
            }
            LogMsg::Digest(reply) => drop(reply.reply(self.digest())),
            LogMsg::Written(result) => self.written(result, cx)?,
            LogMsg::Rested => self.resting = false,
            LogMsg::SnapRested => self.snapping = false,
            LogMsg::Snapped(done, result) => self.snapped(done, result, cx)?,
            LogMsg::Status(reply) => {
                self.publish();
                let held = self.shards.iter().map(|sh| sh.replica.held());
                (self.status.in_memory, self.status.in_memory_bytes) =
                    held.fold((0, 0), |(n, b), (sn, sb)| (n + sn, b + sb));
                drop(reply.reply(self.status.clone()));
            }
            LogMsg::Opened(_) => return Err("opened twice".into()),
        }
        Ok(())
    }

    /// Every record of the turn goes in one write; those of the turns while it is out go in the next.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else { return Ok(()) };
        if !self.resting
            && let Some(write) = log.take_write(axon::trace::unix_micros() / 1000)
        {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                LogMsg::Written(written.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
            });
            job.map(drop).or_else(|e| ending(e.reason))?;
        }
        self.snapshot(cx)?;
        self.publish();
        Ok(())
    }
}

/// One WAL record: the shard, then its records.
fn wal_record(shard: u16, recs: &[Rec]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u16(shard);
    out.put_u32(recs.len() as u32);
    for rec in recs {
        codec::put_rec(&mut out, rec);
    }
    out.freeze()
}

fn read_wal_record(mut body: Bytes) -> Option<(u16, Vec<Rec>)> {
    if body.remaining() < 6 {
        return None;
    }
    let (shard, n) = (body.get_u16(), body.get_u32());
    let mut recs = Vec::new();
    for _ in 0..n {
        recs.push(codec::get_rec(&mut body)?);
    }
    (!body.has_remaining() && shard < SHARDS).then_some((shard, recs))
}

fn snap_name(shard: u16) -> String {
    format!("snap-{shard:03}")
}

/// A shard's snapshot file: magic, version, the CRC of the rest, then the shard, the WAL record it holds through and
/// the state.
fn snap_file(shard: u16, through: u64, state: &State) -> Bytes {
    let mut body = BytesMut::new();
    body.put_u16(shard);
    body.put_u64(through);
    codec::put_state(&mut body, state);
    let mut out = BytesMut::with_capacity(10 + body.len());
    out.put_slice(SNAP_MAGIC);
    out.put_u16(SNAP_VERSION);
    out.put_u32(axon::files::crc32(&body));
    out.put_slice(&body);
    out.freeze()
}

fn read_snap_file(mut data: Bytes) -> Option<(u16, u64, State)> {
    if data.remaining() < 20 || &data[..4] != SNAP_MAGIC {
        return None;
    }
    data.advance(4);
    let (version, crc) = (data.get_u16(), data.get_u32());
    if version != SNAP_VERSION || axon::files::crc32(&data) != crc {
        return None;
    }
    let (shard, through) = (data.get_u16(), data.get_u64());
    let state = codec::get_state(&mut data)?;
    (!data.has_remaining()).then_some((shard, through, state))
}

fn invalid(what: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// Reads the snapshots, then the WAL records after each, in 8 MiB reads. `Log::open` has cut a torn tail; anything
/// else that does not read back stops the start.
async fn load(wal: Arc<dyn Files>, snaps: Arc<dyn Files>, settings: LogSettings) -> io::Result<Box<Loaded>> {
    let log = Log::open(wal, settings.wal.clone()).await?;
    let mut shards: Vec<ShardLog> = (0..SHARDS).map(|_| ShardLog::default()).collect();
    for sh in &mut shards {
        sh.replica = Replica::new(settings.weak);
    }
    for (name, _) in snaps.list().await? {
        let Some(shard) = name.strip_prefix("snap-").and_then(|s| s.parse::<u16>().ok()).filter(|s| *s < SHARDS) else {
            continue;
        };
        let data = Bytes::from(snaps.read(&name).await?);
        let len = data.len() as u64;
        let (s, through, state) = read_snap_file(data)
            .filter(|(s, ..)| *s == shard)
            .ok_or_else(|| invalid(format!("{name} does not read back")))?;
        let sh = &mut shards[usize::from(s)];
        (sh.replica, sh.through, sh.snap_bytes, sh.last_seq) =
            (Replica::restore(state, settings.weak), through, len, through);
    }
    let last = log.key(KEY).and_then(|k| k.epochs.last().map(|e| e.1)).unwrap_or(0);
    if let Some((s, sh)) = shards.iter().enumerate().find(|(_, sh)| sh.through > last) {
        return Err(invalid(format!(
            "{} holds WAL record {}, past the WAL's last, {last}",
            snap_name(s as u16),
            sh.through
        )));
    }
    let mut from = Address { epoch: EPOCH, seq: 0 };
    loop {
        let entries = log.read(KEY, from, REPLAY_READ).await?;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            let Entry::Record { at, body } = entry else { continue };
            let (s, recs) =
                read_wal_record(body).ok_or_else(|| invalid(format!("WAL record {} does not decode", at.seq)))?;
            let sh = &mut shards[usize::from(s)];
            if at.seq > sh.through {
                for rec in &recs {
                    sh.replica.apply(rec);
                }
                sh.replica.compact(settings.keep, settings.keep_bytes);
                sh.first.get_or_insert(at.seq);
                sh.last_seq = at.seq;
            }
            from = Address { epoch: EPOCH, seq: at.seq + 1 };
        }
    }
    Ok(Box::new(Loaded { log, shards }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::{Ballot, Entry as ReplEntry, Op, Pos};

    #[test]
    fn a_wal_record_and_a_snapshot_read_back_and_a_flipped_byte_does_not() {
        let ballot = Ballot { round: 3, node: 0, boot: 9, start: 1 };
        let entry = ReplEntry { ballot, op: Op::Set(vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v"))]) };
        let recs = vec![Rec::Promise(ballot), Rec::Entries { after: 0, entries: vec![entry] }, Rec::Commit(1)];
        let body = wal_record(7, &recs);
        assert_eq!(read_wal_record(body.clone()), Some((7, recs.clone())));
        assert_eq!(read_wal_record(body.slice(..body.len() - 1)), None);

        let Rec::Checkpoint(state) = Replica::replay(&recs, Weak::default()).checkpoint() else { unreachable!() };
        let file = snap_file(7, 42, &state);
        let (s, through, back) = read_snap_file(file.clone()).expect("reads back");
        assert_eq!((s, through, back.clone()), (7, 42, *state));
        assert_eq!(Replica::restore(back, Weak::default()).promised(), ballot, "the snapshot holds the promise");
        for at in 0..file.len() {
            let mut bad = file.to_vec();
            bad[at] ^= 0x40;
            assert!(read_snap_file(Bytes::from(bad)).is_none(), "a flip at {at} read back");
        }
        assert_eq!(Replica::restore(State::default(), Weak::default()).last(), Pos::default());
    }
}
