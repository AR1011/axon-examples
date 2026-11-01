//! A history node's actors. `Journal` keeps this node's replica of every run's journal: it answers the writers,
//! commits each turn's changes in one write, sends reads in parts, repairs from its peers, removes finished runs past
//! their retention and snapshots its index. `Waker` fires the due timers that the index holds.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use axon::files::{FileOp, Files, LogConfig, LogStats};
use axon::trace::unix_micros;
use axon::{
    Actor, ActorHandle, ActorSystem, Context, MailboxConfig, MailboxLimits, ReplyTo, RequestError, StartError,
    SystemError,
};
use axon_cluster::{CallError, Cluster, Nodes, Placement, Reply, WeakCluster};
use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::core::{Answer, Handled, Mutation, Replica, Request, replica_config, seal_snapshot};
use super::index::{Indexed, Retention};
use super::wire::{INDEX, IndexAnswer, IndexApi, IndexCall, JOURNAL, JournalApi, MAX_LIST};

/// The snapshot's file, in its own directory.
pub const SNAPSHOT: &str = "index.snap";
/// How long the log waits after a failed write before it writes the same bytes again.
const REST: Duration = Duration::from_millis(100);
/// Reads out at once: with the write, the repair and the snapshot, fewer than the mailbox's 256 jobs.
const READS: usize = 64;
/// Reads waiting for one of those, at most: past it a read is refused, and the writer asks another replica.
const READS_WAITING: usize = 1_024;
/// Runs a repair page holds.
const PAGE: u32 = 1_000;
/// Runs a snapshot encodes per turn, so that other calls go between.
const SNAPSHOT_TURN: usize = 20_000;
/// Runs a retention tick removes, at most.
const EXPIRE_TURN: usize = 1_000;
/// Parts of a read, at most: other calls on the session go between them.
const PART: usize = 256 << 10;

type Waiter = Option<Reply<Answer>>;

#[derive(Clone, Debug)]
pub struct JournalConfig {
    pub log: LogConfig,
    /// The history nodes, this one too: repair asks each of the others in turn.
    pub replicas: Vec<Arc<str>>,
    pub repair_every: Duration,
    pub snapshot_every: Duration,
    /// How long a finished run stays after its last record here; a failed one longer, so that it can be redriven.
    pub keep: Duration,
    pub keep_failed: Duration,
    /// A workflow's own keep, failed or not: `App::retention`.
    pub keep_workflows: BTreeMap<String, Duration>,
    #[doc(hidden)]
    pub mutation: Mutation,
}

impl JournalConfig {
    pub fn new(replicas: Vec<Arc<str>>) -> Self {
        Self {
            log: replica_config(),
            replicas,
            repair_every: Duration::from_secs(10),
            snapshot_every: Duration::from_secs(300),
            keep: Duration::from_secs(7 * 86_400),
            keep_failed: Duration::from_secs(30 * 86_400),
            keep_workflows: BTreeMap::new(),
            mutation: Mutation::None,
        }
    }
}

/// A run as one replica holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunState {
    pub run: Bytes,
    pub fence: u64,
    pub segments: Vec<(u64, u64)>,
    pub indexed: Option<Indexed>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    pub runs: usize,
    /// Records copied from peers.
    pub repaired: u64,
    pub expired: u64,
    pub snapshots: u64,
    pub failed_writes: u64,
    pub log: LogStats,
}

pub enum ToJournal {
    Opened(io::Result<Box<Replica<Waiter>>>),
    Call(Request, Waiter),
    Written(io::Result<()>),
    Rested,
    /// A read ended; with the call, when a segment it planned to read was gone, to read again.
    ReadDone(Option<(Request, Waiter)>),
    Tick,
    Snap,
    Snapshotted(io::Result<()>),
    Page,
    Paged(Result<Answer, String>),
    Fetched(Bytes, u64, u64, Vec<Bytes>),
    /// Timers due at a wall clock in µs, after a timer and run, the earliest first.
    Due(Option<(u64, Bytes)>, u64, usize, ReplyTo<Vec<(u64, Bytes)>>),
    /// Runs that may need a wake and took no record since a wall clock in µs, after an update and run, the least
    /// recent first: what the reconciler wakes.
    Stale(Option<(u64, Bytes)>, u64, usize, ReplyTo<Vec<(u64, Bytes)>>),
    Runs(ReplyTo<Vec<RunState>>),
    Stats(ReplyTo<JournalStats>),
    Index(IndexCall, Option<Reply<IndexAnswer>>),
}

#[derive(Default)]
struct Repair {
    /// The peer asked, as a count of the peers asked so far.
    turn: usize,
    peer: Option<Arc<str>>,
    after: Option<Bytes>,
    busy: bool,
    more: bool,
    todo: VecDeque<(Bytes, u64, u64, u64)>,
    last_us: u64,
}

pub struct Journal {
    cfg: JournalConfig,
    files: Arc<dyn Files>,
    snaps: Arc<dyn Files>,
    cluster: WeakCluster,
    me: Arc<str>,
    replica: Option<Box<Replica<Waiter>>>,
    reads: usize,
    to_read: VecDeque<(Request, Waiter)>,
    resting: bool,
    repair: Repair,
    /// The snapshot being encoded: the run it goes on from, and what it holds so far.
    snapshot: Option<(Option<Bytes>, Vec<u8>)>,
    snapshotting: bool,
    last_snapshot_us: u64,
    stats: JournalStats,
}

fn job_error(e: impl std::fmt::Debug) -> String {
    format!("{e:?}")
}

impl Journal {
    fn handle(&mut self, request: Request, reply: Waiter, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let replica = self.replica.as_mut().ok_or("not open")?;
        let again = matches!(request, Request::Read { .. }).then(|| request.clone());
        match replica.handle(request, reply) {
            Handled::Now(reply, answer) => drop(reply.map(|r| r.send(answer))),
            Handled::Queued => {}
            Handled::Read(reply, read) => {
                self.reads += 1;
                let job = cx.submit_future(async move {
                    match read.await {
                        Ok(answer) => {
                            if let Some(reply) = reply {
                                send_parts(reply, answer).await;
                            }
                            ToJournal::ReadDone(None)
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => ToJournal::ReadDone(again.map(|r| (r, reply))),
                        // The caller gets NoReply, and asks again.
                        Err(_) => ToJournal::ReadDone(None),
                    }
                });
                job.map_err(|e| job_error(e.reason))?;
            }
        }
        Ok(())
    }

    fn tick(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let replica = self.replica.as_mut().ok_or("not open")?;
        self.stats.expired += replica.expire(EXPIRE_TURN) as u64;
        let now = replica.now_us;
        if !self.repair.busy && now >= self.repair.last_us + self.cfg.repair_every.as_micros() as u64 {
            self.page(cx)?;
        }
        if self.snapshot.is_none()
            && !self.snapshotting
            && now >= self.last_snapshot_us + self.cfg.snapshot_every.as_micros() as u64
        {
            self.last_snapshot_us = now;
            self.snapshot = Some((None, Vec::new()));
            self.snap(cx)?;
        }
        Ok(())
    }

    fn snap(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (Some(replica), Some((from, out))) = (self.replica.as_ref(), self.snapshot.as_mut()) else { return Ok(()) };
        let next = replica.snapshot(from.as_ref(), SNAPSHOT_TURN, out);
        if next.is_some() {
            *from = next;
            if cx.schedule_after(Duration::ZERO, ToJournal::Snap).is_err() {
                self.snapshot = None;
            }
            return Ok(());
        }
        let Some((_, out)) = self.snapshot.take() else { return Ok(()) };
        let (snaps, data) = (self.snaps.clone(), seal_snapshot(out));
        // Replace: after any crash the file holds the old snapshot or the new.
        let job = cx.submit_future(async move {
            ToJournal::Snapshotted(snaps.apply(vec![FileOp::Replace(SNAPSHOT.into(), data)]).await)
        });
        self.snapshotting = job.map_err(|e| job_error(e.reason)).is_ok();
        Ok(())
    }

    /// Asks the next peer for a page of its runs.
    fn page(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let peers: Vec<&Arc<str>> = self.cfg.replicas.iter().filter(|r| ***r != *self.me).collect();
        let (Some(cluster), false) = (self.cluster.upgrade(), peers.is_empty()) else { return Ok(()) };
        let peer = self.repair.peer.get_or_insert_with(|| peers[self.repair.turn % peers.len()].clone()).clone();
        let request = Request::Pages { after: self.repair.after.clone(), limit: PAGE };
        let job = cx.submit_future(async move {
            let remote = cluster.resolve_on::<JournalApi>(&peer, JOURNAL).await.map_err(|e| e.to_string());
            match remote {
                Ok(remote) => ToJournal::Paged(remote.request(request).await.map_err(|e| e.to_string())),
                Err(e) => ToJournal::Paged(Err(e)),
            }
        });
        job.map_err(|e| job_error(e.reason))?;
        self.repair.busy = true;
        Ok(())
    }

    /// Fetches the next segment the page showed this replica lacks, or ends the page.
    fn fetch(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (Some((run, epoch, from, to)), Some(peer), Some(cluster)) =
            (self.repair.todo.pop_front(), self.repair.peer.clone(), self.cluster.upgrade())
        else {
            if self.repair.more && cx.schedule_after(REST, ToJournal::Page).is_ok() {
                return Ok(());
            }
            let r = &mut self.repair;
            (r.busy, r.peer, r.after, r.turn) = (false, None, None, r.turn + 1);
            r.last_us = unix_micros();
            return Ok(());
        };
        let job = cx.submit_future(async move {
            let request = Request::Read { run: run.clone(), epoch, from, to };
            let records = match cluster.resolve_on::<JournalApi>(&peer, JOURNAL).await {
                Ok(remote) => read_parts(&remote, request).await.ok(),
                Err(_) => None,
            };
            let records = match records {
                Some(Answer::Records { epoch: e, from: f, records }) if (e, f) == (epoch, from) => records,
                _ => Vec::new(),
            };
            ToJournal::Fetched(run, epoch, from, records)
        });
        job.map(drop).map_err(|e| job_error(e.reason))
    }
}

/// A read's records in parts of at most `PART`, or the replica's answer.
async fn send_parts(reply: Reply<Answer>, answer: Answer) {
    let Answer::Records { epoch, from, records } = answer else {
        drop(reply.send(answer));
        return;
    };
    // Room for the part's own fields.
    let max = reply.max_payload().min(PART).saturating_sub(64);
    let mut parts: Vec<Result<Answer, CallError>> = Vec::new();
    let (mut at, mut part, mut bytes) = (from, Vec::new(), 0);
    for body in records {
        if !part.is_empty() && bytes + body.len() + 4 > max {
            let n = part.len() as u64;
            parts.push(Ok(Answer::Records { epoch, from: at, records: std::mem::take(&mut part) }));
            (at, bytes) = (at + n, 0);
        }
        bytes += body.len() + 4;
        part.push(body);
    }
    parts.push(Ok(Answer::Records { epoch, from: at, records: part }));
    // What it returns is about the reply: the caller has been told by then.
    drop(reply.send_parts(futures_util::stream::iter(parts)).await);
}

/// A read from a replica, its parts in one answer. A read cut part way gives what came.
pub async fn read_parts(remote: &axon_cluster::RemoteRef<JournalApi>, request: Request) -> Result<Answer, CallError> {
    let mut parts = remote.request_parts(request).await?;
    let mut whole: Option<Answer> = None;
    while let Some(part) = parts.next().await {
        match (part, &mut whole) {
            (
                Ok(Answer::Records { epoch, from, records }),
                Some(Answer::Records { epoch: e, from: f, records: all }),
            ) if (epoch, from) == (*e, *f + all.len() as u64) => all.extend(records),
            (Ok(answer), None) => whole = Some(answer),
            (Err(e), None) => return Err(e),
            _ => break,
        }
    }
    whole.ok_or(CallError::NoReply)
}

impl Actor for Journal {
    type Message = ToJournal;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (files, snaps, log, now) = (self.files.clone(), self.snaps.clone(), self.cfg.log.clone(), unix_micros());
        self.last_snapshot_us = now;
        let job = cx.submit_future(async move {
            // A snapshot that does not read back whole is not used: each run is read again from the log.
            let snapshot = snaps.read(SNAPSHOT).await.ok().map(Bytes::from);
            ToJournal::Opened(Replica::open(files, log, snapshot, now).await.map(Box::new))
        });
        cx.pause_until(&job.map_err(|e| job_error(e.reason))?).map_err(job_error)
    }

    fn on_message(&mut self, msg: ToJournal, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(replica) = self.replica.as_mut() else {
            // The pause in on_start holds every other message until this one.
            let ToJournal::Opened(opened) = msg else { return Err("not open".into()) };
            let mut replica = opened.map_err(|e| e.to_string())?;
            let micros = |d: Duration| d.as_micros() as u64;
            replica.mutation = self.cfg.mutation;
            replica.set_retention(Retention {
                keep_us: micros(self.cfg.keep),
                failed_us: micros(self.cfg.keep_failed),
                workflows: self.cfg.keep_workflows.iter().map(|(w, d)| (w.clone(), micros(*d))).collect(),
            });
            self.replica = Some(replica);
            // Timers pass a pause: one started before the log is read would tick into a replica not open yet.
            return cx.schedule_every(Duration::from_secs(1), || ToJournal::Tick).map(drop).map_err(job_error);
        };
        replica.now_us = unix_micros();
        match msg {
            ToJournal::Opened(_) => return Err("opened twice".into()),
            ToJournal::Call(Request::Read { epoch, .. }, reply) if self.to_read.len() >= READS_WAITING => {
                drop(reply.map(|r| r.send(Answer::Refused { epoch, why: "busy".into() })));
            }
            ToJournal::Call(request @ Request::Read { .. }, reply) if self.reads >= READS => {
                self.to_read.push_back((request, reply));
            }
            ToJournal::Call(request, reply) => self.handle(request, reply, cx)?,
            ToJournal::ReadDone(again) => {
                self.reads -= 1;
                self.to_read.extend(again);
            }
            ToJournal::Written(result) => {
                // A disk that fails at once would otherwise be written again at once, and again.
                self.stats.failed_writes += u64::from(result.is_err());
                self.resting = result.is_err() && cx.schedule_after(REST, ToJournal::Rested).is_ok();
                for (reply, answer) in replica.written(result) {
                    drop(reply.map(|r| r.send(answer)));
                }
            }
            ToJournal::Rested => self.resting = false,
            ToJournal::Tick => self.tick(cx)?,
            ToJournal::Snap => self.snap(cx)?,
            ToJournal::Snapshotted(done) => {
                self.snapshotting = false;
                self.stats.snapshots += u64::from(done.is_ok());
            }
            ToJournal::Page => self.page(cx)?,
            ToJournal::Paged(Ok(Answer::Pages { runs, more })) => {
                self.repair.todo = replica.lacks(&runs).into();
                self.repair.more = more;
                self.repair.after = runs.last().map(|(run, ..)| run.clone()).filter(|_| more);
                self.fetch(cx)?;
            }
            ToJournal::Paged(_) => {
                self.repair.more = false;
                self.fetch(cx)?;
            }
            ToJournal::Fetched(run, epoch, from, records) => {
                self.stats.repaired += records.len() as u64;
                if !records.is_empty() {
                    self.handle(Request::Append { run, epoch, seq: from, records }, None, cx)?;
                }
                self.fetch(cx)?;
            }
            ToJournal::Due(after, now_us, limit, reply) => {
                drop(reply.reply(replica.index().due(after.as_ref(), now_us).take(limit).cloned().collect()));
            }
            ToJournal::Stale(after, before_us, limit, reply) => {
                drop(reply.reply(replica.index().stale(after.as_ref(), before_us).take(limit).cloned().collect()));
            }
            ToJournal::Runs(reply) => {
                let (log, index) = (replica.log(), replica.index());
                let runs = log.keys(None, usize::MAX).into_iter().map(|(run, info)| RunState {
                    indexed: index.get(&run).cloned(),
                    run,
                    fence: info.fence,
                    segments: info.epochs,
                });
                drop(reply.reply(runs.collect()));
            }
            ToJournal::Index(call, reply) => {
                let answer = match call {
                    IndexCall::List(mut query) => {
                        query.limit = query.limit.clamp(1, MAX_LIST);
                        IndexAnswer::Page(replica.index().query(&query))
                    }
                    IndexCall::Counts => IndexAnswer::Counts(replica.index().counts(replica.now_us)),
                };
                drop(reply.map(|r| r.send(answer)));
            }
            ToJournal::Stats(reply) => {
                let (runs, log) = (replica.index().len(), replica.log().stats());
                drop(reply.reply(JournalStats { runs, log, ..self.stats.clone() }));
            }
        }
        Ok(())
    }

    /// Every change of the turn goes in one write; those of the turns while it is out go in the next.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        while self.reads < READS
            && let Some((request, reply)) = self.to_read.pop_front()
        {
            self.handle(request, reply, cx)?;
        }
        let Some(replica) = self.replica.as_mut() else { return Ok(()) };
        if !self.resting
            && let Some(write) = replica.take_write(unix_micros() / 1000)
        {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                ToJournal::Written(written.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
            });
            job.map_err(|e| job_error(e.reason))?;
        }
        Ok(())
    }
}

/// Starts this history node's `Journal` on `files`, with its snapshot in `snaps`, and publishes it once open.
pub async fn start(
    system: &ActorSystem,
    cluster: &Cluster,
    files: Arc<dyn Files>,
    snaps: Arc<dyn Files>,
    cfg: JournalConfig,
) -> Result<ActorHandle<ToJournal>, String> {
    let journal = Journal {
        cfg,
        files,
        snaps,
        cluster: cluster.downgrade(),
        me: cluster.record().id.into(),
        replica: None,
        reads: 0,
        to_read: VecDeque::new(),
        resting: false,
        repair: Repair::default(),
        snapshot: None,
        snapshotting: false,
        last_snapshot_us: 0,
        stats: JournalStats::default(),
    };
    let limits = MailboxLimits { messages: 16_384, bytes: 128 << 20, jobs: 256, ..MailboxLimits::default() };
    let mailbox = MailboxConfig::from(limits).with_weight(|msg: &ToJournal| {
        size_of::<ToJournal>()
            + match msg {
                ToJournal::Call(Request::Append { records, .. }, _) | ToJournal::Fetched(_, _, _, records) => {
                    records.iter().map(Bytes::len).sum()
                }
                _ => 0,
            }
    });
    let handle = system.spawn(journal).name(JOURNAL).mailbox(mailbox).start().map_err(job_error)?;
    // The open reads the whole log, longer than the system's start timeout for a big one, and a wait that times out
    // leaves it running: wait again until it opens or fails.
    loop {
        match handle.ready().await {
            Err(SystemError::Start(StartError::TimedOut)) => continue,
            done => break done.map_err(job_error)?,
        }
    }
    cluster.publish::<JournalApi>(&handle, JOURNAL).map_err(job_error)?;
    cluster.publish::<IndexApi>(&handle, INDEX).map_err(job_error)?;
    Ok(handle)
}

/// Wakes a run: a `Wake` to its keyed actor, which journals `TimerFired` once for each timer due, and otherwise opens
/// the run and lets it go on.
pub type Wake = Arc<dyn Fn(Bytes) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

#[derive(Clone)]
pub struct WakerConfig {
    /// The history nodes, whose order for a run picks its waker.
    pub history: Placement,
    pub wake: Wake,
    pub every: Duration,
    /// A waker not first in a run's order fires it this late, if its index still shows it due.
    pub late: Duration,
    pub in_flight: usize,
    /// How often the reconciler looks for runs no timer wakes that took no record for `stale`: an owner that is gone
    /// and that eager did not replace (a lapse listed too briefly, a failed start, an outage), or a run parked on a
    /// version a build may now have.
    pub reconcile_every: Duration,
    pub stale: Duration,
    /// A run that failed to open this many times in a row is tried once an hour, and counted stuck.
    pub stuck_after: u32,
}

impl WakerConfig {
    pub fn new(history: Placement, wake: Wake) -> Self {
        Self {
            history,
            wake,
            every: Duration::from_secs(1),
            late: Duration::from_secs(5),
            in_flight: 128,
            reconcile_every: Duration::from_secs(30),
            stale: Duration::from_secs(60),
            stuck_after: 3,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WakerStats {
    pub woken: u64,
    pub failed: u64,
    /// Runs the reconciler woke.
    pub reconciled: u64,
    /// Runs that failed to open `stuck_after` times in a row: `wf_stuck_runs`.
    pub stuck: u64,
}

pub enum ToWaker {
    Tick,
    Due(Result<Vec<(u64, Bytes)>, RequestError>),
    Woke(Bytes, Result<(), String>),
    Reconcile,
    Stale(Result<Vec<(u64, Bytes)>, RequestError>),
    Reconciled(Bytes, Result<(), String>),
    Stats(ReplyTo<WakerStats>),
}

/// How long a stuck run waits for the reconciler's next try, in µs.
const HOUR_US: u64 = 3_600_000_000;

pub struct Waker {
    cfg: WakerConfig,
    journal: ActorHandle<ToJournal>,
    cluster: WeakCluster,
    me: Arc<str>,
    asking: bool,
    /// The last due timer looked at: the next ask goes on after it, so that the timers at the front, other nodes'
    /// or already woken, do not hide the later ones. `None` once the ask reached the last due.
    after: Option<(u64, Bytes)>,
    in_flight: BTreeSet<Bytes>,
    /// Runs woken lately, and when: not again until `late` has passed, while the run journals that it fired.
    recent: BTreeMap<Bytes, u64>,
    reconciling: bool,
    stale_after: Option<(u64, Bytes)>,
    /// Runs the reconciler failed to wake: failures in a row, and when to try again, in µs.
    failing: BTreeMap<Bytes, (u32, u64)>,
    stats: WakerStats,
}

impl Waker {
    /// Due timers one ask reads.
    fn limit(&self) -> usize {
        2 * self.cfg.in_flight
    }

    fn first(&self, nodes: &Nodes, run: &[u8]) -> bool {
        nodes.order(&String::from_utf8_lossy(run)).first().is_some_and(|n| *n == self.me)
    }

    /// Wakes the stale runs this node is first for, and the others' once they are `late` past it.
    fn reconcile(&mut self, stale: Vec<(u64, Bytes)>, now: u64, cx: &mut Context<'_, Self>) {
        let Some(cluster) = self.cluster.upgrade() else { return };
        let nodes = cluster.nodes(&self.cfg.history);
        let (late, limit) = (self.cfg.late.as_micros() as u64, self.limit());
        let before = now.saturating_sub(self.cfg.stale.as_micros() as u64);
        self.failing.retain(|_, (_, next)| *next + HOUR_US > now);
        let (full, mut stopped) = (stale.len() == limit, false);
        for (updated, run) in stale {
            // Half the wakes at most: the timers' wakes and the next ask keep the rest of the jobs.
            if self.in_flight.len() >= (self.cfg.in_flight / 2).max(1) {
                stopped = true;
                break;
            }
            let waiting = self.failing.get(&run).is_some_and(|(_, next)| *next > now);
            let skip = waiting || self.in_flight.contains(&run) || self.recent.contains_key(&run);
            if !skip && (self.first(&nodes, &run) || updated + late <= before) {
                let (wake, woke) = ((self.cfg.wake)(run.clone()), run.clone());
                // The jobs are full: the next round goes on from here.
                if cx.submit_future(async move { ToWaker::Reconciled(woke, wake.await) }).is_err() {
                    stopped = true;
                    break;
                }
                self.in_flight.insert(run.clone());
            }
            self.stale_after = Some((updated, run));
        }
        if full && !stopped {
            drop(cx.schedule_after(Duration::ZERO, ToWaker::Reconcile));
        } else if !stopped {
            self.stale_after = None;
        }
    }
}

impl Actor for Waker {
    type Message = ToWaker;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        cx.schedule_every(self.cfg.reconcile_every, || ToWaker::Reconcile).map_err(job_error)?;
        cx.schedule_every(self.cfg.every, || ToWaker::Tick).map(drop).map_err(job_error)
    }

    fn on_message(&mut self, msg: ToWaker, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (now, late) = (unix_micros(), self.cfg.late.as_micros() as u64);
        match msg {
            ToWaker::Tick if !self.asking => {
                let (journal, after, limit) = (self.journal.clone(), self.after.clone(), self.limit());
                let job = cx.submit_future(async move {
                    ToWaker::Due(journal.ask(|r| ToJournal::Due(after, now, limit, r)).await)
                });
                self.asking = job.map_err(|e| job_error(e.reason)).is_ok();
            }
            ToWaker::Tick => {}
            ToWaker::Due(due) => {
                self.asking = false;
                self.recent.retain(|_, at| *at + late > now);
                let Some(cluster) = self.cluster.upgrade() else { return Ok(()) };
                let nodes = cluster.nodes(&self.cfg.history);
                let due = due.unwrap_or_default();
                let (full, mut stopped) = (!due.is_empty() && due.len() == self.limit(), false);
                for (at, run) in due {
                    if self.in_flight.len() >= self.cfg.in_flight || self.recent.len() >= 65_536 {
                        stopped = true;
                        break;
                    }
                    self.after = Some((at, run.clone()));
                    if self.in_flight.contains(&run) || self.recent.contains_key(&run) {
                        continue;
                    }
                    if !self.first(&nodes, &run) && now < at + late {
                        continue;
                    }
                    let wake = (self.cfg.wake)(run.clone());
                    let woke = run.clone();
                    if cx.submit_future(async move { ToWaker::Woke(woke, wake.await) }).is_ok() {
                        self.in_flight.insert(run);
                    }
                }
                if full && !stopped {
                    // More may be due past this page: ask for them now, not a tick later.
                    drop(cx.schedule_after(Duration::ZERO, ToWaker::Tick));
                } else if !stopped {
                    self.after = None;
                }
            }
            ToWaker::Woke(run, woke) => {
                self.in_flight.remove(&run);
                self.recent.insert(run, now);
                match woke {
                    Ok(()) => self.stats.woken += 1,
                    Err(_) => self.stats.failed += 1,
                }
            }
            ToWaker::Reconcile if !self.reconciling => {
                let (journal, after, limit) = (self.journal.clone(), self.stale_after.clone(), self.limit());
                let before = now.saturating_sub(self.cfg.stale.as_micros() as u64);
                let job = cx.submit_future(async move {
                    ToWaker::Stale(journal.ask(|r| ToJournal::Stale(after, before, limit, r)).await)
                });
                self.reconciling = job.is_ok();
            }
            ToWaker::Reconcile => {}
            ToWaker::Stale(stale) => {
                self.reconciling = false;
                self.reconcile(stale.unwrap_or_default(), now, cx);
            }
            ToWaker::Reconciled(run, woke) => {
                self.in_flight.remove(&run);
                self.recent.insert(run.clone(), now);
                match woke {
                    Ok(()) => {
                        self.stats.reconciled += 1;
                        self.failing.remove(&run);
                    }
                    Err(_) => {
                        let fails = self.failing.get(&run).map_or(0, |(n, _)| *n) + 1;
                        let wait = if fails >= self.cfg.stuck_after { HOUR_US } else { 0 };
                        self.failing.insert(run, (fails, now + wait));
                    }
                }
                let stuck = self.failing.values().filter(|(n, _)| *n >= self.cfg.stuck_after).count();
                self.stats.stuck = stuck as u64;
            }
            ToWaker::Stats(reply) => drop(reply.reply(self.stats.clone())),
        }
        Ok(())
    }
}

pub fn start_waker(
    system: &ActorSystem,
    cluster: &Cluster,
    journal: ActorHandle<ToJournal>,
    cfg: WakerConfig,
) -> Result<ActorHandle<ToWaker>, String> {
    let limits = MailboxLimits { jobs: cfg.in_flight + 8, ..MailboxLimits::default() };
    let waker = Waker {
        cfg,
        journal,
        cluster: cluster.downgrade(),
        me: cluster.record().id.into(),
        asking: false,
        after: None,
        in_flight: BTreeSet::new(),
        recent: BTreeMap::new(),
        reconciling: false,
        stale_after: None,
        failing: BTreeMap::new(),
        stats: WakerStats::default(),
    };
    system.spawn(waker).name("waker").mailbox(MailboxConfig::from(limits)).start().map_err(job_error)
}
