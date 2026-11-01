//! The `Run` actor: one per run id. It opens the run's journal with a [`Writer`] (seal, recover, `Own`), replays it
//! through a [`Machine`], and then journals each input before it acts on it: an answer to a caller, an attempt of an
//! activity, waits until the entries before it are on a majority. The writer's messages go out as jobs through a
//! [`Transport`]; nothing in a handler waits.
//!
//! A run starts by probing the replicas, with or without a caller: eager starts it on a new node when its owner is
//! gone, and the reconciler with a `Wake`. A `Start` seals at once. Otherwise the probe decides: when no majority
//! holds the run, what waits is answered `not_found`, with no fence raised and nothing written; a run that a majority
//! indexes ended answers from that summary, but for a waker's `Wake`, sent where a replica behind shows it going
//! on; and only a run that goes on, or is woken, is sealed and opened. A probe that opens
//! nothing answers the calls of the next `LINGER` too, then the actor stops: a caller's request comes after the
//! spawn it asked for, and may land after the probe.
//!
//! A run with nothing to do stops itself (passivation): once its journal is on a majority, no attempt is offered or
//! runs, no call to another run waits, nothing is due within `KEEP_WARM` and no one read it within `WATCHED`, it
//! journals `Idle` and stops, and its next call or due timer opens it again.
//!
//! An activity on the `local` queue runs as a job of the run. One on any other queue is offered to that queue, again
//! every `OFFER_AGAIN` until a worker says it started it, and the run keeps the attempt's start-to-close and heartbeat
//! times itself: it does not stop while an attempt is out, so those need no durable timer. A worker that leaves the
//! cluster fails the attempts it started, through the engine's worker watch and [`OnWorkers`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axon::{Actor, Context, JobHandle, StopReason, TimerId};
use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::machine::{Effect, MAX_PAYLOAD, Machine, Refusal, Start, input_hash};
use super::wire::{Caller, Code, RunAnswer, RunCall, refused};
use crate::journal::{
    Answer, AppendError, Entry, Out, Policy, Probe, Status, Summary, Transport, Writer, WriterConfig,
};
use crate::metrics::EngineMetrics;
use crate::queue::Task;
use crate::workflow::{ActivityCx, App, Failure, LOCAL, Numbering, not_in_build};

/// The wall clock in µs since the Unix epoch: `axon::trace::unix_micros` in a node, a manual one in tests.
pub type Wall = Arc<dyn Fn() -> u64 + Send + Sync>;

/// What a run reaches outside itself: other runs (its children, its parent) and the task queues.
pub trait Peers: Send + Sync {
    /// Sends `call` to run `id` until it answers other than `busy`, or a deadline passes.
    fn run(&self, id: &str, call: RunCall) -> BoxFuture<'static, Result<RunAnswer, String>>;
    /// Hands `task` to its queue's partition. A lost offer is made again.
    fn offer(&self, task: Task) -> BoxFuture<'static, Result<(), String>>;
}

/// Outside a cluster: a run reaches nothing.
pub struct NoPeers;

impl Peers for NoPeers {
    fn run(&self, id: &str, _: RunCall) -> BoxFuture<'static, Result<RunAnswer, String>> {
        let why = format!("no cluster to reach run {id}");
        Box::pin(async move { Err(why) })
    }

    fn offer(&self, task: Task) -> BoxFuture<'static, Result<(), String>> {
        let why = format!("no cluster to reach queue {}", task.queue);
        Box::pin(async move { Err(why) })
    }
}

/// The runs of a node with an attempt started on each worker node: what its worker watch tells when one leaves.
#[derive(Default)]
pub struct OnWorkers(Mutex<BTreeMap<String, BTreeSet<String>>>);

impl OnWorkers {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, BTreeSet<String>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set(&self, run: &str, was: &BTreeSet<String>, now: &BTreeSet<String>) {
        let mut map = self.lock();
        for node in was.difference(now) {
            if let Some(runs) = map.get_mut(node) {
                runs.remove(run);
                if runs.is_empty() {
                    map.remove(node);
                }
            }
        }
        for node in now.difference(was) {
            map.entry(node.clone()).or_default().insert(run.to_owned());
        }
    }

    /// The runs with an attempt on `node`.
    pub fn runs(&self, node: &str) -> Vec<String> {
        self.lock().get(node).map(|runs| runs.iter().cloned().collect()).unwrap_or_default()
    }

    /// The worker nodes with an attempt of some run.
    pub fn nodes(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }
}

/// What every run of a node shares.
pub struct Env {
    pub app: Arc<App>,
    pub wall: Wall,
    pub writer: WriterConfig,
    /// The node's id, as each attempt's `ActivityCx::node` names it.
    pub node: Arc<str>,
    pub peers: Arc<dyn Peers>,
    pub on_workers: Arc<OnWorkers>,
    pub metrics: Arc<EngineMetrics>,
    #[doc(hidden)]
    pub numbering: Numbering,
}

impl Env {
    /// Runs of `app` whose journals are on `replicas` history nodes.
    pub fn new(app: impl Into<Arc<App>>, wall: Wall, replicas: usize) -> Env {
        let numbering = if crate::mutation() == "number_at_poll" { Numbering::AtPoll } else { Numbering::AtCreation };
        Env {
            app: app.into(),
            wall,
            writer: WriterConfig::new(replicas),
            node: Arc::from(""),
            peers: Arc::new(NoPeers),
            on_workers: Arc::default(),
            metrics: Arc::default(),
            numbering,
        }
    }
}

pub enum RunMsg {
    Call(RunCall, Caller),
    Probed(Result<Probe, String>),
    Journal(usize, Result<Answer, String>),
    Attempt(u32, u32, Result<Bytes, Failure>),
    Sent(Outgoing, Result<RunAnswer, String>),
    Tick,
    Due,
    OpenTimeout,
    /// The run may stop: see whether it stayed quiet.
    Rest,
    /// Offers to make again, attempts past their time, calls to send again.
    Watch,
}

/// A call the run makes to a queue or another run.
#[derive(Clone)]
pub enum Outgoing {
    Offer(Task),
    /// Starts the child at `step`.
    Child(u32, String, RunCall),
    /// Tells the parent how this run ended.
    Notify(String, RunCall),
    Cancel(String),
}

/// An attempt the run gave out and has no result of yet.
struct Attempt {
    attempt: u32,
    generation: u32,
    name: String,
    input: Bytes,
    queue: String,
    policy: Policy,
    /// Each offer given back unstarted is made again as the next round, so a queue takes it again.
    round: u32,
    worker: Option<String>,
    offered_at: Duration,
    started_at: Option<Duration>,
    beat_at: Duration,
    /// A local attempt's job.
    job: Option<JobHandle>,
}

impl Attempt {
    fn cancel(self) {
        if let Some(job) = self.job {
            job.cancel();
        }
    }
}

/// How often the writer looks for requests to send again.
const TICK: Duration = Duration::from_millis(100);
/// How long an open may take before the run gives up and its callers try again.
const OPEN_WITHIN: Duration = Duration::from_secs(4);
/// Calls held while the run opens, at most.
const WAITING: usize = 256;
/// Tags on a run, at most: they are list filters, not data.
const MAX_TAGS: usize = 8;
/// A run with something due within this stays: a short sleep does not cost a lease and an open.
const KEEP_WARM: Duration = Duration::from_secs(30);
/// A run with nothing to do stops once it had no message for this long.
const QUIET: Duration = Duration::from_secs(1);
/// A run read in this long stays, ended or not: the console reads it every 2 s, and each open journals an owner.
const WATCHED: Duration = Duration::from_secs(10);
/// How long a probe that found nothing to open is kept for the calls behind it: a caller's request reaches a run
/// after the spawn it asked for, and may come after the probe.
const LINGER: Duration = Duration::from_secs(2);
/// An offer no worker started is made again after this: queues keep offers in memory only.
const OFFER_AGAIN: Duration = Duration::from_secs(10);
/// How often the run looks at its attempts and its calls to send again.
const WATCH: Duration = Duration::from_secs(1);
/// Journal entries a history page holds, at most, and their bytes: well inside a frame.
const HISTORY_PAGE: usize = 1_000;
const HISTORY_BYTES: usize = 512 << 10;
/// Batches of entries timed until their ack, at most: past it a batch is not timed.
const TIMED: usize = 1_024;
/// A run's journal, at most: past it the run fails. Checked after each poll, so it may pass by one poll's entries.
pub const MAX_JOURNAL: usize = 32 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Probing,
    Opening,
    Open,
    Stopped,
}

enum Held {
    Answer(Caller, RunAnswer),
    Effect(Effect),
}

pub struct Run {
    env: Arc<Env>,
    journal: Arc<dyn Transport>,
    id: String,
    lease: u64,
    phase: Phase,
    writer: Option<Writer>,
    machine: Option<Machine>,
    waiting: VecDeque<(RunCall, Caller)>,
    /// The journal index the next entry gets.
    next_index: u64,
    /// The journal's first `acked` entries are on a majority.
    acked: u64,
    /// Entries the writer did not take yet: it is `max_unacked_bytes` ahead.
    unsent: VecDeque<Bytes>,
    /// What waits for the journal: each with the index of the last entry made before it.
    held: VecDeque<(u64, Held)>,
    /// By step.
    attempts: BTreeMap<u32, Attempt>,
    /// The worker nodes `attempts` started on, as `OnWorkers` holds them.
    on_workers: BTreeSet<String>,
    /// Calls to make, as job slots allow; those that failed, sent again at the next watch; jobs out.
    outbox: VecDeque<Outgoing>,
    later: Vec<Outgoing>,
    sending: usize,
    /// The bytes of the run's journal.
    journal_bytes: usize,
    /// The actor's clock at its last message.
    now: Duration,
    tick: Option<TimerId>,
    watch: Option<TimerId>,
    due: Option<(u64, TimerId)>,
    /// While opening, `OPEN_WITHIN`; while a probe found nothing to open, `LINGER`.
    open_timer: Option<TimerId>,
    rest: Option<TimerId>,
    /// A probe that found no run, or one that ended, with no call waiting: what answers the calls in `LINGER`.
    found: Option<Probe>,
    /// When the run last took a message, on the actor's clock.
    busy_at: Duration,
    /// When the run was last read (`Get`, `History`), on the actor's clock.
    read_at: Option<Duration>,
    /// The journal's last entry is `Idle`.
    idle: bool,
    /// `Idle` is journaled: the run stops once it is on a majority.
    resting: bool,
    /// When this message's handling began, and the open began.
    turn_at: Instant,
    opening_at: Instant,
    /// The wall clock at the open, in µs: the timeline shows it.
    opened_us: u64,
    /// Batches of entries not yet acked: the index of the last, when its message came, when it went to the writer.
    timed: VecDeque<(u64, Instant, Option<Instant>)>,
}

impl Run {
    /// The run `id`, owned under lease epoch `lease`, its journal reached through `journal`.
    pub fn new(env: Arc<Env>, journal: Arc<dyn Transport>, id: String, lease: u64) -> Run {
        Run {
            env,
            journal,
            id,
            lease,
            phase: Phase::Probing,
            writer: None,
            machine: None,
            waiting: VecDeque::new(),
            next_index: 0,
            acked: 0,
            unsent: VecDeque::new(),
            held: VecDeque::new(),
            attempts: BTreeMap::new(),
            on_workers: BTreeSet::new(),
            outbox: VecDeque::new(),
            later: Vec::new(),
            sending: 0,
            journal_bytes: 0,
            now: Duration::ZERO,
            tick: None,
            watch: None,
            due: None,
            open_timer: None,
            rest: None,
            found: None,
            busy_at: Duration::ZERO,
            read_at: None,
            idle: false,
            resting: false,
            turn_at: Instant::now(),
            opening_at: Instant::now(),
            opened_us: 0,
            timed: VecDeque::new(),
        }
    }

    fn wall(&self) -> u64 {
        (self.env.wall)()
    }

    fn call(&mut self, call: RunCall, caller: Caller, cx: &mut Context<'_, Self>) {
        self.resting = false;
        match self.phase {
            Phase::Open => return self.handle(call, caller),
            Phase::Stopped => return caller.answer(refused(Code::Busy, "the run is stopping; try again")),
            Phase::Probing | Phase::Opening => {}
        }
        // Nothing is sealed or written for a start that cannot run, nor does it open the run.
        if let RunCall::Start { workflow, version, input, tags, .. } = &call
            && let Err(refusal) = check_start(&self.env.app, workflow, *version, input, tags)
        {
            return caller.answer(refusal);
        }
        if self.waiting.len() >= WAITING {
            return caller.answer(refused(Code::Busy, "the run is opening; try again"));
        }
        // A start opens the run, unless the probe found it ended: its summary answers.
        let start = matches!(call, RunCall::Start { .. }) && !matches!(self.found, Some(Probe::Finished(_)));
        self.waiting.push_back((call, caller));
        if start && self.phase == Phase::Probing {
            self.open(cx);
        } else if let Some(found) = self.found.clone() {
            self.probed(Ok(found), cx);
        }
    }

    /// What the probe found decides: no run, a run that ended, or one to open.
    fn probed(&mut self, probe: Result<Probe, String>, cx: &mut Context<'_, Self>) {
        let summary = match probe {
            Err(why) => {
                self.refuse_waiting(Code::Unavailable, &why);
                return self.stop(cx);
            }
            Ok(Probe::Unknown) => {
                self.refuse_waiting(Code::NotFound, "no run with this id");
                return self.linger(Probe::Unknown, cx);
            }
            // A child that has not told its parent opens, and tells it.
            Ok(Probe::Finished(summary)) if summary.owes_parent => Some(summary),
            Ok(Probe::Finished(summary)) => {
                let waiting = std::mem::take(&mut self.waiting);
                for (call, caller) in waiting {
                    match answer_ended(&call, &summary) {
                        Some(answer) => caller.answer(answer),
                        None => self.waiting.push_back((call, caller)),
                    }
                }
                if self.waiting.is_empty() {
                    return self.linger(Probe::Finished(summary), cx);
                }
                None
            }
            Ok(Probe::Exists(summary)) => summary,
        };
        // Parked on a version this build lacks: only a call it must keep (a signal, an action) opens it.
        let lacks = summary.is_some_and(|s| s.parked_version && !self.env.app.has(&s.workflow, s.version));
        if lacks && self.waiting.iter().all(|(call, _)| matches!(call, RunCall::Wake)) {
            for (_, caller) in self.waiting.drain(..) {
                caller.answer(RunAnswer::Accepted);
            }
            return self.stop(cx);
        }
        self.open(cx);
    }

    /// Nothing to open: the run stops after `LINGER`, and what `found` says answers the calls before then.
    fn linger(&mut self, found: Probe, cx: &mut Context<'_, Self>) {
        if self.found.is_none() {
            self.open_timer = cx.schedule_after(LINGER, RunMsg::OpenTimeout).ok();
        }
        self.found = Some(found);
    }

    fn open(&mut self, cx: &mut Context<'_, Self>) {
        self.phase = Phase::Opening;
        self.found = None;
        if let Some(timer) = self.open_timer.take() {
            cx.cancel_timer(timer);
        }
        let (run, now) = (Bytes::from(self.id.clone()), now_ms(cx));
        let writer = Writer::open(run, self.lease, axon::trace::random_u64(), self.env.writer, now);
        self.opened_us = self.wall();
        self.writer = Some(writer.opened_by(&self.env.node, self.opened_us));
        self.opening_at = Instant::now();
        self.open_timer = cx.schedule_after(OPEN_WITHIN, RunMsg::OpenTimeout).ok();
    }

    fn opened(&mut self, journal: Vec<Bytes>, cx: &mut Context<'_, Self>) {
        let mut entries = Vec::with_capacity(journal.len());
        for (i, body) in journal.iter().enumerate() {
            match Entry::decode(body) {
                Ok(entry) => entries.push(entry),
                Err(e) => return self.fail(format!("journal entry {i} does not decode: {e}"), cx),
            }
        }
        self.next_index = journal.len() as u64 + 1;
        self.journal_bytes = journal.iter().map(Bytes::len).sum();
        // An owner that opened the run and left again changed nothing: its `Own` is not activity.
        self.idle = matches!(entries.iter().rev().find(|e| !matches!(e, Entry::Own(_))), Some(Entry::Idle));
        let wall = self.wall();
        let mut machine = Machine::open(self.env.app.clone(), &self.id, entries, self.env.numbering, wall);
        machine.opened_by(&self.env.node, self.opened_us);
        self.machine = Some(machine);
        self.env.metrics.open.record(self.opening_at.elapsed());
        self.phase = Phase::Open;
        if let Some(timer) = self.open_timer.take() {
            cx.cancel_timer(timer);
        }
        self.drain();
        while let Some((call, caller)) = self.waiting.pop_front() {
            self.handle(call, caller);
        }
    }

    /// A call to the open run: what it changes is journaled, and the answer waits for that.
    fn handle(&mut self, call: RunCall, caller: Caller) {
        let wall = self.wall();
        let id = self.id.clone();
        let Some(m) = &mut self.machine else { return caller.answer(refused(Code::Busy, "the run is not open")) };
        let started = m.started().map(|(w, v, hash)| (w.to_owned(), v, hash));
        if started.is_some() && m.status() != Status::Running {
            // Nothing a worker runs is wanted once the run ended or parked.
            if let RunCall::Heartbeat { .. } = call {
                return caller.answer(RunAnswer::Beat { cancel: true });
            }
        }
        if m.status() == Status::Deleted {
            return caller.answer(answer_deleted(&call));
        }
        let answer = match call {
            RunCall::Start { workflow, version, input, tags, parent, run_timeout_ms } => match started {
                Some((w, v, hash)) => start_again(&workflow, &input, (&w, v, hash)),
                None => match check_start(&self.env.app, &workflow, version, &input, &tags) {
                    Ok(version) => {
                        m.start(Start { workflow, version, input, tags, parent, run_timeout_ms }, wall);
                        RunAnswer::Started { version, created: true }
                    }
                    Err(refusal) => refusal,
                },
            },
            _ if started.is_none() => refused(Code::NotFound, "no run with this id"),
            RunCall::Signal { signal_id, name, payload } => match m.signal(signal_id, name, payload, wall) {
                Ok(()) => RunAnswer::Signalled { duplicate: false },
                Err(Refusal::Duplicate) => RunAnswer::Signalled { duplicate: true },
                Err(refusal) => refuse(refusal),
            },
            RunCall::Cancel { reason } => m.cancel(reason, wall).map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::Terminate { reason } => m.terminate(reason).map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::Get => {
                let mut view = m.view(&id);
                (view.owner, view.epoch) = (self.env.node.to_string(), self.lease);
                RunAnswer::Run(Box::new(view))
            }
            RunCall::Pause => m.pause().map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::Resume => m.resume(wall).map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::Retry => m.redrive(wall).map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::Delete => m.delete().map_or_else(refuse, |()| RunAnswer::Accepted),
            RunCall::History { after, limit } => {
                let (entries, next) = m.history(after, (limit as usize).clamp(1, HISTORY_PAGE), HISTORY_BYTES);
                RunAnswer::History { entries, next }
            }
            RunCall::Complete { step, attempt, result } => match m.attempt(step, attempt, result, wall) {
                Ok(()) | Err(Refusal::Duplicate) => RunAnswer::Accepted,
                Err(refusal) => refuse(refusal),
            },
            RunCall::Wake => {
                m.fire(wall);
                RunAnswer::Accepted
            }
            RunCall::ChildDone { step, result } => match m.child_done(step, result, wall) {
                Ok(()) | Err(Refusal::Duplicate) => RunAnswer::Accepted,
                Err(refusal) => refuse(refusal),
            },
            RunCall::Started { step, attempt, worker } => {
                let current = m.current_attempt(step);
                match self.attempts.get_mut(&step) {
                    Some(a) if current == Some(attempt) && a.attempt == attempt && a.queue != LOCAL => {
                        match &a.worker {
                            None => {
                                (a.worker, a.started_at, a.beat_at) = (Some(worker), Some(self.now), self.now);
                                m.picked(step);
                                RunAnswer::Accepted
                            }
                            Some(w) if *w == worker => RunAnswer::Accepted,
                            Some(_) => refused(Code::Exists, "another worker runs this attempt"),
                        }
                    }
                    _ => refused(Code::Finished, "the run does not want this attempt"),
                }
            }
            RunCall::Heartbeat { step, attempt } => {
                let wanted = m.current_attempt(step) == Some(attempt);
                match self.attempts.get_mut(&step).filter(|a| a.attempt == attempt && wanted) {
                    Some(a) => {
                        a.beat_at = self.now;
                        RunAnswer::Beat { cancel: false }
                    }
                    None => RunAnswer::Beat { cancel: true },
                }
            }
            RunCall::Released { step, attempt, round, worker } => {
                let held = |a: &&mut Attempt| {
                    a.attempt == attempt && a.round == round && a.worker.as_ref().is_none_or(|w| *w == worker)
                };
                if let Some(a) = self.attempts.get_mut(&step).filter(held) {
                    (a.worker, a.started_at, a.round, a.offered_at) = (None, None, a.round + 1, self.now);
                    let task = task(&self.env, &id, self.lease, step, a);
                    self.outbox.push_back(Outgoing::Offer(task));
                }
                RunAnswer::Accepted
            }
            RunCall::WorkerLeft { node } => {
                let gone: Vec<(u32, u32)> = self
                    .attempts
                    .iter()
                    .filter(|(_, a)| a.worker.as_deref() == Some(&node))
                    .map(|(step, a)| (*step, a.attempt))
                    .collect();
                for (step, attempt) in gone {
                    self.attempts.remove(&step);
                    let _ = m.attempt(step, attempt, Err(Failure::retry(format!("worker {node} left"))), wall);
                }
                RunAnswer::Accepted
            }
        };
        self.drain();
        self.held.push_back((self.next_index - 1, Held::Answer(caller, answer)));
    }

    /// Takes what the machine made: its entries to journal, its effects to hold until they are. A journal past
    /// `MAX_JOURNAL` fails the run.
    fn drain(&mut self) {
        let Some(m) = &mut self.machine else { return };
        let before = self.next_index;
        loop {
            for entry in m.take_out() {
                let body = entry.encode();
                self.journal_bytes += body.len();
                self.unsent.push_back(body);
                self.next_index += 1;
                // Something happened after `Idle`: the run journals it again before it stops.
                (self.idle, self.resting) = (false, false);
            }
            if self.journal_bytes <= MAX_JOURNAL || m.status().is_finished() {
                break;
            }
            m.give_up(format!("the journal has {} bytes, past {MAX_JOURNAL}", self.journal_bytes));
        }
        for late in m.take_late() {
            self.env.metrics.lateness.record(Duration::from_micros(late));
        }
        if self.next_index > before && self.timed.len() < TIMED {
            self.timed.push_back((self.next_index - 1, self.turn_at, None));
        }
        let gate = self.next_index - 1;
        self.held.extend(m.take_effects().into_iter().map(|e| (gate, Held::Effect(e))));
    }

    /// Hands the writer what it can take, carries its messages, and lets go of what the journal now holds.
    fn pump(&mut self, cx: &mut Context<'_, Self>) {
        let now = now_ms(cx);
        loop {
            self.launch(cx);
            self.drain();
            let Some(writer) = &mut self.writer else { return };
            if writer.is_open() {
                while let Some(body) = self.unsent.front() {
                    match writer.queue(body.clone()) {
                        Ok(_) => {
                            self.unsent.pop_front();
                            let queued = self.next_index - 1 - self.unsent.len() as u64;
                            let at = Instant::now();
                            for t in self.timed.iter_mut().filter(|t| t.0 <= queued && t.2.is_none()) {
                                t.2 = Some(at);
                            }
                        }
                        Err(AppendError::Behind) => break,
                        Err(e) => return self.fail(format!("the journal refused an entry: {e:?}"), cx),
                    }
                }
                writer.flush(now);
            }
            let outs = writer.take_out();
            if outs.is_empty() {
                break;
            }
            for out in outs {
                match out {
                    Out::Send(r, request) => {
                        // A refused job is sent again by the writer's tick.
                        let call = self.journal.call(r, request);
                        drop(cx.submit_future(async move { RunMsg::Journal(r, call.await) }));
                    }
                    Out::Opened(journal) => self.opened(journal, cx),
                    Out::Acked(n) => {
                        self.acked = self.acked.max(n);
                        let metrics = &self.env.metrics;
                        while let Some((_, at, queued)) = self.timed.front().filter(|t| t.0 < self.acked).copied() {
                            self.timed.pop_front();
                            metrics.step.record(at.elapsed());
                            metrics.append.record(queued.unwrap_or(at).elapsed());
                        }
                    }
                    Out::Fenced(fence) => {
                        self.env.metrics.fenced.fetch_add(1, Ordering::Relaxed);
                        return self.fail(format!("a newer owner sealed the run at epoch {fence}"), cx);
                    }
                    Out::Failed(why) => return self.fail(why, cx),
                }
                if self.phase == Phase::Stopped {
                    return;
                }
            }
            self.release();
        }
        self.release();
        self.launch(cx);
        self.drain();
        self.prune();
        self.arm(cx);
    }

    fn release(&mut self) {
        while let Some((gate, _)) = self.held.front()
            && *gate < self.acked
        {
            let Some((_, held)) = self.held.pop_front() else { break };
            match held {
                Held::Answer(caller, answer) => caller.answer(answer),
                Held::Effect(Effect::Attempt { step, attempt, generation, name, input, queue, policy }) => {
                    let a = Attempt {
                        attempt,
                        generation,
                        name,
                        input,
                        queue,
                        policy,
                        round: 1,
                        worker: None,
                        offered_at: self.now,
                        started_at: None,
                        beat_at: self.now,
                        job: None,
                    };
                    if a.queue != LOCAL {
                        self.outbox.push_back(Outgoing::Offer(task(&self.env, &self.id, self.lease, step, &a)));
                    }
                    if let Some(old) = self.attempts.insert(step, a) {
                        old.cancel();
                    }
                }
                Held::Effect(Effect::StopAttempts) => self.let_go(),
                Held::Effect(Effect::StartChild { step, id, workflow, input }) => {
                    let start = RunCall::Start {
                        workflow,
                        version: None,
                        input,
                        tags: Vec::new(),
                        parent: Some(self.id.clone()),
                        run_timeout_ms: None,
                    };
                    self.outbox.push_back(Outgoing::Child(step, id, start));
                }
                Held::Effect(Effect::CancelChild { id }) => self.outbox.push_back(Outgoing::Cancel(id)),
                Held::Effect(Effect::NotifyParent { parent, step, result }) => {
                    self.outbox.push_back(Outgoing::Notify(parent, RunCall::ChildDone { step, result }));
                }
            }
        }
    }

    /// Lets go of the attempts the run no longer wants: their step has its outcome, or waits for its next attempt.
    fn prune(&mut self) {
        let Some(m) = &self.machine else { return };
        let stale: Vec<u32> = (self.attempts.iter())
            .filter(|(step, a)| m.current_attempt(**step) != Some(a.attempt))
            .map(|(step, _)| *step)
            .collect();
        for step in stale {
            if let Some(a) = self.attempts.remove(&step) {
                a.cancel();
            }
        }
        let now: BTreeSet<String> = self.attempts.values().filter_map(|a| a.worker.clone()).collect();
        if now != self.on_workers {
            self.env.on_workers.set(&self.id, &self.on_workers, &now);
            self.on_workers = now;
        }
    }

    /// Starts the local attempts the journal now allows and makes the calls waiting, as far as the mailbox's jobs go.
    fn launch(&mut self, cx: &mut Context<'_, Self>) {
        let waiting: Vec<u32> = (self.attempts.iter())
            .filter(|(_, a)| a.queue == LOCAL && a.job.is_none())
            .map(|(step, _)| *step)
            .collect();
        for step in waiting {
            let Some(a) = self.attempts.get(&step) else { continue };
            let (attempt, name) = (a.attempt, a.name.clone());
            let cx_of =
                ActivityCx::new(&self.id, step, attempt).redone(a.generation).by(self.env.node.clone(), self.lease);
            let activity = self.env.app.activity_future(&name, &a.input, cx_of);
            let work = async move {
                let result = match activity {
                    None => Err(Failure::fatal(format!("activity {name} is not in this build"))),
                    Some(activity) => axon::catch_panic(activity).await.unwrap_or_else(|panic| {
                        Err(Failure::retry(format!("panic: {}", panic.message().unwrap_or("no message"))))
                    }),
                };
                RunMsg::Attempt(step, attempt, result)
            };
            // A refused job is started again by the tick.
            let Ok(job) = cx.submit_future(work) else { break };
            if let Some(a) = self.attempts.get_mut(&step) {
                (a.job, a.started_at) = (Some(job), Some(self.now));
            }
            if let Some(m) = &mut self.machine {
                m.picked(step);
            }
        }
        while let Some(send) = self.outbox.pop_front() {
            let peers = self.env.peers.clone();
            let out = send.clone();
            let work = async move {
                let answer = match &out {
                    Outgoing::Offer(task) => peers.offer(task.clone()).await.map(|()| RunAnswer::Accepted),
                    Outgoing::Child(_, id, call) | Outgoing::Notify(id, call) => peers.run(id, call.clone()).await,
                    Outgoing::Cancel(id) => {
                        peers.run(id, RunCall::Cancel { reason: "its parent was cancelled".into() }).await
                    }
                };
                RunMsg::Sent(out, answer)
            };
            if cx.submit_future(work).is_ok() {
                self.sending += 1;
            } else {
                // The tick tries again.
                self.outbox.push_front(send);
                break;
            }
        }
    }

    /// What a call to a queue or another run came to.
    fn sent(&mut self, send: Outgoing, answer: Result<RunAnswer, String>) {
        self.sending -= 1;
        let wall = self.wall();
        let Some(m) = &mut self.machine else { return };
        match (send, answer) {
            // An offer that did not arrive is made again at `OFFER_AGAIN`.
            (Outgoing::Offer(_), _) => {}
            (Outgoing::Child(..), Ok(RunAnswer::Started { .. })) => {}
            // `unavailable` says nothing of whether the child started: its journal may hold the start.
            (Outgoing::Child(step, _, _), Ok(RunAnswer::Refused { code, message }))
                if !matches!(code, Code::Busy | Code::Unavailable) =>
            {
                let _ = m.child_done(step, Err(format!("the child did not start: {message}")), wall);
            }
            (
                Outgoing::Notify(..),
                Ok(RunAnswer::Accepted | RunAnswer::Refused { code: Code::Finished | Code::NotFound, .. }),
            ) => {
                m.notified();
            }
            (
                Outgoing::Cancel(_),
                Ok(RunAnswer::Accepted | RunAnswer::Refused { code: Code::Finished | Code::NotFound, .. }),
            ) => {}
            (send, _) => self.later.push(send),
        }
    }

    /// Offers again what no worker started, fails attempts past their time, and sends again what did not arrive.
    fn watch(&mut self) {
        let (now, wall) = (self.now, self.wall());
        let Some(m) = &mut self.machine else { return };
        let mut late = Vec::new();
        for (step, a) in &mut self.attempts {
            match a.started_at {
                None if a.queue != LOCAL && now.saturating_sub(a.offered_at) >= OFFER_AGAIN => {
                    a.offered_at = now;
                    self.outbox.push_back(Outgoing::Offer(task(&self.env, &self.id, self.lease, *step, a)));
                }
                None => {}
                Some(at) => {
                    let past = |ms: Option<u64>, since: Duration| {
                        ms.is_some_and(|ms| now.saturating_sub(since) >= Duration::from_millis(ms))
                    };
                    if past(a.policy.start_to_close_ms, at) {
                        late.push((*step, a.attempt, "start_to_close"));
                    } else if a.worker.is_some() && past(a.policy.heartbeat_ms, a.beat_at) {
                        late.push((*step, a.attempt, "heartbeat"));
                    }
                }
            }
        }
        for (step, attempt, what) in late {
            if let Some(a) = self.attempts.remove(&step) {
                a.cancel();
            }
            let _ = m.attempt(step, attempt, Err(Failure::retry(format!("timeout: {what}"))), wall);
        }
        self.outbox.extend(self.later.drain(..));
    }

    /// The writer's tick while anything waits on it, and one timer at the run's next due time.
    fn arm(&mut self, cx: &mut Context<'_, Self>) {
        let local = self.attempts.values().any(|a| a.queue == LOCAL && a.job.is_none());
        let waiting = self.phase == Phase::Opening
            || !self.unsent.is_empty()
            || self.next_index > self.acked
            || local
            || !self.outbox.is_empty();
        if waiting && self.tick.is_none() {
            self.tick = cx.schedule_after(TICK, RunMsg::Tick).ok();
        }
        if (!self.attempts.is_empty() || !self.later.is_empty()) && self.watch.is_none() {
            self.watch = cx.schedule_after(WATCH, RunMsg::Watch).ok();
        }
        let next = self.machine.as_ref().and_then(Machine::next_due);
        if self.due.as_ref().map(|(at, _)| *at) == next {
            return;
        }
        if let Some((_, timer)) = self.due.take() {
            cx.cancel_timer(timer);
        }
        if let Some(at) = next {
            let after = Duration::from_micros(at.saturating_sub(self.wall()));
            if let Ok(timer) = cx.schedule_after(after, RunMsg::Due) {
                self.due = Some((at, timer));
            }
        }
    }

    /// Nothing to do: the journal is on a majority, nothing waits on it, no attempt is out, no call to another run
    /// or a queue waits, nothing is due soon.
    fn can_rest(&self) -> bool {
        // `RUN_MUTATION=passivate_busy`: a run stops with attempts in flight.
        let attempts = self.attempts.is_empty() || crate::mutation() == "passivate_busy";
        let calls = self.outbox.is_empty() && self.later.is_empty() && self.sending == 0;
        let soon = self
            .machine
            .as_ref()
            .and_then(Machine::next_due)
            .is_some_and(|at| at <= self.wall().saturating_add(KEEP_WARM.as_micros() as u64));
        self.phase == Phase::Open
            && self.unsent.is_empty()
            && self.acked >= self.next_index
            && self.held.is_empty()
            && self.waiting.is_empty()
            && attempts
            && calls
            && !soon
    }

    /// Stops the run once it rested `QUIET`: a run that goes on journals `Idle` first, so the reconciler leaves it
    /// alone, and stops once that is on a majority.
    fn rest(&mut self, cx: &mut Context<'_, Self>) {
        if !self.can_rest() {
            return;
        }
        let watched = self.read_at.map_or(Duration::ZERO, |at| (at + WATCHED).saturating_sub(cx.now()));
        let quiet = QUIET.saturating_sub(cx.now().saturating_sub(self.busy_at));
        let left = if self.resting { watched } else { quiet.max(watched) };
        if !left.is_zero() {
            if self.rest.is_none() {
                self.rest = cx.schedule_after(left, RunMsg::Rest).ok();
            }
            return;
        }
        let running = self.machine.as_ref().is_some_and(|m| m.status() == Status::Running);
        if self.resting || !running || self.idle {
            return self.stop(cx);
        }
        self.unsent.push_back(Entry::Idle.encode());
        self.next_index += 1;
        (self.idle, self.resting) = (true, true);
    }

    fn refuse_waiting(&mut self, code: Code, why: &str) {
        for (_, caller) in self.waiting.drain(..) {
            caller.answer(refused(code, why));
        }
    }

    /// Stops the run: what it holds is answered `unavailable`, and its next call opens it again from the journal.
    fn fail(&mut self, why: String, cx: &mut Context<'_, Self>) {
        self.refuse_waiting(Code::Unavailable, &why);
        for (_, held) in self.held.drain(..) {
            if let Held::Answer(caller, _) = held {
                caller.answer(refused(Code::Unavailable, why.clone()));
            }
        }
        self.stop(cx);
    }

    fn stop(&mut self, cx: &mut Context<'_, Self>) {
        self.phase = Phase::Stopped;
        self.let_go();
        cx.stop();
    }

    /// The attempts end with the actor: their jobs stop, and the worker watch no longer counts them here.
    fn let_go(&mut self) {
        std::mem::take(&mut self.attempts).into_values().for_each(Attempt::cancel);
        self.env.on_workers.set(&self.id, &self.on_workers, &BTreeSet::new());
        self.on_workers.clear();
    }
}

/// The task a queue hands a worker for the attempt `a` at `step`.
fn task(env: &Env, run: &str, epoch: u64, step: u32, a: &Attempt) -> Task {
    Task {
        run: run.to_owned(),
        step,
        attempt: a.attempt,
        generation: a.generation,
        round: a.round,
        name: a.name.clone(),
        queue: a.queue.clone(),
        input: a.input.clone(),
        heartbeat_ms: a.policy.heartbeat_ms,
        owner: env.node.to_string(),
        epoch,
    }
}

/// The version a start runs, or why it cannot.
fn check_start(
    app: &App,
    workflow: &str,
    version: Option<u32>,
    input: &[u8],
    tags: &[(String, String)],
) -> Result<u32, RunAnswer> {
    let Some(latest) = app.latest(workflow) else {
        return Err(refused(Code::Invalid, format!("no workflow named {workflow:?} in this build")));
    };
    let version = version.unwrap_or(latest);
    if !app.has(workflow, version) {
        return Err(refused(Code::VersionMissing, not_in_build(workflow, version)));
    }
    if input.len() > MAX_PAYLOAD {
        return Err(refused(Code::TooLarge, format!("an input of {} bytes, past {MAX_PAYLOAD}", input.len())));
    }
    if tags.len() > MAX_TAGS || tags.iter().any(|(k, v)| k.is_empty() || k.len() > 128 || v.len() > 128) {
        return Err(refused(
            Code::Invalid,
            format!("at most {MAX_TAGS} tags; a key is 1 to 128 bytes, a value up to 128"),
        ));
    }
    app.check_input(workflow, version, input).map_err(|why| refused(Code::Invalid, why))?;
    Ok(version)
}

fn refuse(refusal: Refusal) -> RunAnswer {
    match refusal {
        Refusal::Finished => refused(Code::Finished, "the run has ended"),
        Refusal::Duplicate => refused(Code::Exists, "already done"),
        Refusal::Full => refused(Code::Limited, "the run holds too many signals no wait took"),
        Refusal::TooLarge => refused(Code::TooLarge, format!("past {MAX_PAYLOAD} bytes")),
        Refusal::UnknownStep => refused(Code::NotFound, "no activity at that step"),
        Refusal::Paused => refused(Code::Paused, "the run is paused or parked"),
        Refusal::NotResumable => refused(Code::NotResumable, "the run is not paused or parked"),
        Refusal::NotFailed => refused(Code::NotFailed, "the run did not fail on an activity, or is a child run"),
        Refusal::NotFinished => refused(Code::NotFinished, "the run has not ended"),
    }
}

/// The answer to a start of a run already started with `workflow`, `version` and an input of `hash`: the same start
/// again, or another one.
fn start_again(workflow: &str, input: &[u8], (w, version, hash): (&str, u32, u64)) -> RunAnswer {
    // `RUN_MUTATION=no_input_check`: a start of another input is taken as the same start.
    if workflow == w && (input_hash(workflow, input) == hash || crate::mutation() == "no_input_check") {
        RunAnswer::Started { version, created: false }
    } else {
        refused(Code::Exists, "a run with this id exists, with another workflow or input")
    }
}

/// A deleted run is gone for every call but its own start, until retention removes its journal.
fn answer_deleted(call: &RunCall) -> RunAnswer {
    match call {
        RunCall::Start { .. } => refused(Code::Exists, "a deleted run had this id"),
        RunCall::Wake | RunCall::WorkerLeft { .. } => RunAnswer::Accepted,
        RunCall::Heartbeat { .. } => RunAnswer::Beat { cancel: true },
        _ => refused(Code::NotFound, "the run was deleted"),
    }
}

/// The answer to `call` of a run that a majority indexes ended, or `None` where it needs the run's journal.
pub fn answer_ended(call: &RunCall, summary: &Summary) -> Option<RunAnswer> {
    if summary.status == Status::Deleted {
        return Some(answer_deleted(call));
    }
    Some(match call {
        RunCall::Start { workflow, input, .. } => {
            start_again(workflow, input, (&summary.workflow, summary.version, summary.input_hash))
        }
        // A waker sends one where its replica's index shows the run going on: that replica is behind, and may stay so
        // where an open cut short sealed it above the records it lacks. An owner's `Own` takes it the summary.
        RunCall::Wake => return None,
        // A signal may be one the run took before it ended, sent again: only its journal says.
        RunCall::Get | RunCall::Signal { .. } | RunCall::History { .. } | RunCall::Delete => return None,
        RunCall::Retry if summary.status == Status::Failed => return None,
        RunCall::Retry => refused(Code::NotFailed, "the run did not fail"),
        RunCall::WorkerLeft { .. } => RunAnswer::Accepted,
        RunCall::Heartbeat { .. } => RunAnswer::Beat { cancel: true },
        RunCall::Cancel { .. }
        | RunCall::Terminate { .. }
        | RunCall::Complete { .. }
        | RunCall::Started { .. }
        | RunCall::Released { .. }
        | RunCall::ChildDone { .. }
        | RunCall::Pause
        | RunCall::Resume => refused(Code::Finished, "the run has ended"),
    })
}

fn now_ms(cx: &Context<'_, Run>) -> u64 {
    cx.now().as_millis() as u64
}

impl Actor for Run {
    type Message = RunMsg;
    type Error = String;

    /// Probes the run, whether a call started it or eager did: a run that goes on opens without waiting for a call.
    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let probe = self.journal.probe(Bytes::from(self.id.clone()));
        cx.submit_future(async move { RunMsg::Probed(probe.await) }).map(drop).map_err(|e| format!("{:?}", e.reason))
    }

    fn on_message(&mut self, msg: RunMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let now = now_ms(cx);
        self.now = cx.now();
        self.turn_at = Instant::now();
        if !matches!(msg, RunMsg::Rest | RunMsg::Tick | RunMsg::Watch) {
            self.busy_at = cx.now();
        }
        if let RunMsg::Call(RunCall::Get | RunCall::History { .. }, _) = msg {
            self.read_at = Some(cx.now());
        }
        match msg {
            RunMsg::Call(call, caller) => self.call(call, caller, cx),
            RunMsg::Probed(probe) if self.phase == Phase::Probing => self.probed(probe, cx),
            RunMsg::Probed(_) => {}
            RunMsg::Journal(r, Ok(answer)) => {
                if let Some(writer) = &mut self.writer {
                    writer.answer(r, answer, now);
                }
            }
            // The writer's tick sends it again.
            RunMsg::Journal(_, Err(_)) => {}
            RunMsg::Attempt(step, attempt, result) => {
                if self.attempts.get(&step).is_some_and(|a| a.attempt == attempt) {
                    self.attempts.remove(&step);
                }
                let wall = self.wall();
                if let Some(m) = &mut self.machine {
                    // A refusal is a late or doubled result: the step's first outcome stands.
                    let _ = m.attempt(step, attempt, result, wall);
                }
            }
            RunMsg::Tick => {
                self.tick = None;
                if let Some(writer) = &mut self.writer {
                    writer.tick(now);
                }
            }
            RunMsg::Due => {
                self.due = None;
                let wall = self.wall();
                if let Some(m) = &mut self.machine {
                    m.fire(wall);
                }
            }
            RunMsg::OpenTimeout => {
                self.open_timer = None;
                match self.phase {
                    Phase::Probing => self.stop(cx),
                    Phase::Opening => self.fail("the journal did not open in time".into(), cx),
                    Phase::Open | Phase::Stopped => {}
                }
            }
            RunMsg::Rest => self.rest = None,
            RunMsg::Sent(send, answer) => self.sent(send, answer),
            RunMsg::Watch => {
                self.watch = None;
                self.watch();
            }
        }
        self.env.metrics.poll.record(self.turn_at.elapsed());
        Ok(())
    }

    fn on_stop(&mut self, _: &StopReason) {
        self.let_go();
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.phase != Phase::Stopped {
            self.pump(cx);
        }
        if self.phase != Phase::Stopped {
            self.rest(cx);
            if self.resting {
                self.pump(cx);
            }
        }
        Ok(())
    }
}
