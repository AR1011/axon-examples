//! A run's pure core: the workflow's future, polled with a no-op waker, and the journal it is replayed from.
//!
//! Every entry goes through [`Machine::apply`], whether it comes from the opened journal or was just made: an
//! outcome, a timer that fired, a signal, a cancel each change the run's state and then poll the code once. So a
//! replay polls the code at the same points, with the same outcomes, as the run that wrote the journal, and the code
//! asks for the same steps. A step the journal holds is not journaled again; one it lacks while entries are left to
//! apply means the code changed, and the run parks.
//!
//! What the machine decides itself (a wait takes a signal, the code ends, a run timeout fires, the run parks) is
//! journaled only once it is live: while it replays, the journal already holds those decisions, and after the
//! replay [`Machine::open`] makes any that the last owner did not get to write.
//!
//! It acts through [`Effect`]s, which its caller runs only once the entries before them are on a majority.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;

use crate::journal::entry::{redrivable, voided};
use crate::journal::{Entry, Own, Policy, Status};
use crate::workflow::{
    App, Command, Ctx, Failure, Numbering, Outcome, Recorded, Shared, Wake, WorkflowFuture, child_id, clip,
    next_attempt, not_in_build,
};

use super::view::{RunView, signal_names, timeline};

/// An activity's output, a signal's payload, a run's input or output, at most: one entry stays well inside an
/// append (960 KiB), and a run's journal grows by known steps.
pub const MAX_PAYLOAD: usize = 256 << 10;
/// Signals a run holds that no wait took yet, at most.
pub const MAX_SIGNALS: usize = 1_000;

/// What the machine asks its caller to do, once the journal holds what came before.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Run attempt `attempt` (from 1) of the activity at `step`, on its queue's workers or, for `local`, here.
    /// `generation`: how many activities a redrive undid at this step before this one (`ActivityCx::redone`).
    Attempt { step: u32, attempt: u32, generation: u32, name: String, input: Bytes, queue: String, policy: Policy },
    /// Stop the attempts in flight: the run ended.
    StopAttempts,
    /// Start the child run `id`; a start already made is answered as one.
    StartChild { step: u32, id: String, workflow: String, input: Bytes },
    /// Cancel the child run `id`: this run was cancelled.
    CancelChild { id: String },
    /// Tell the parent how this child ended, at the parent's step `step`.
    NotifyParent { parent: String, step: u32, result: Result<Bytes, String> },
}

/// How a start or a call ended, when the machine refuses it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Finished,
    Duplicate,
    Full,
    TooLarge,
    UnknownStep,
    /// A pause of a run paused or parked.
    Paused,
    /// A resume of a run neither paused nor parked.
    NotResumable,
    /// A redrive of a run that did not fail on an activity.
    NotFailed,
    /// A delete of a run that goes on.
    NotFinished,
}

/// A start as the run journals it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Start {
    pub workflow: String,
    pub version: u32,
    pub input: Bytes,
    pub tags: Vec<(String, String)>,
    pub parent: Option<String>,
    pub run_timeout_ms: Option<u64>,
}

impl Start {
    pub fn input_hash(&self) -> u64 {
        input_hash(&self.workflow, &self.input)
    }
}

/// What a second start of an id must match: the workflow and the input. FNV-1a: stable across builds and machines,
/// unlike std's hasher.
pub fn input_hash(workflow: &str, input: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in workflow.bytes().chain([0]).chain(input.iter().copied()) {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[derive(Default)]
struct Signals {
    /// In the order received: id, name, payload, taken.
    list: Vec<(String, String, Bytes, bool)>,
    ids: BTreeMap<String, usize>,
    untaken: usize,
}

pub struct Machine {
    app: Arc<App>,
    shared: Arc<Mutex<Shared>>,
    future: Option<WorkflowFuture>,
    /// The run's entries, `Own` left out: the opened journal, then what this owner made.
    journal: Vec<Entry>,
    /// Each owner's `Own`, with only its node and time, and how many entries of `journal` came before it.
    owners: Vec<(usize, Own)>,
    started: Option<(String, u32, u64)>,
    /// The ending the code or a run timeout came to, before it is journaled.
    finished: Option<Entry>,
    end: Option<Entry>,
    parked_in_journal: bool,
    signals: Signals,
    /// Each activity step's failed attempts, and when its next one is due.
    failures: BTreeMap<u32, u32>,
    retry_at: BTreeMap<u32, u64>,
    /// Each step's attempts that a redrive undid: its policy counts the attempts after them, and a late result of
    /// one is dropped.
    base: BTreeMap<u32, u32>,
    /// Each step's activities that a redrive undid, where the code made another activity at the step again.
    generation: BTreeMap<u32, u32>,
    /// The last `ctx.set_status`, in JSON.
    custom_status: Option<Bytes>,
    deleted: bool,
    /// How late each timer fired, in µs, until the actor takes them.
    late_us: Vec<u64>,
    /// The parent run, and its step this run is, for a child.
    parent: Option<(String, u32)>,
    notified: bool,
    live: bool,
    out: Vec<Entry>,
    effects: Vec<Effect>,
}

/// The step that times the whole run out: the code's steps count from 1.
const RUN_TIMEOUT: u32 = 0;

fn polls(entry: &Entry) -> bool {
    matches!(
        entry,
        Entry::Started { .. }
            | Entry::ActivityDone { .. }
            | Entry::ActivityFailed { .. }
            | Entry::TimerFired { .. }
            | Entry::SignalReceived { .. }
            | Entry::SignalConsumed { .. }
            | Entry::ChildDone { .. }
            | Entry::CancelRequested { .. }
    )
}

impl Machine {
    /// Replays `journal` (the run's entries before this owner) through the code, then makes what the last owner
    /// decided but did not journal. `wall_us` is the wall clock now.
    pub fn open(app: Arc<App>, run: &str, journal: Vec<Entry>, numbering: Numbering, wall_us: u64) -> Machine {
        let shared = Shared::new(run.to_owned(), numbering, app.log());
        let mut m = Machine {
            app,
            shared: Arc::new(Mutex::new(shared)),
            future: None,
            journal: Vec::with_capacity(journal.len()),
            owners: Vec::new(),
            started: None,
            finished: None,
            end: None,
            parked_in_journal: false,
            signals: Signals::default(),
            failures: BTreeMap::new(),
            retry_at: BTreeMap::new(),
            base: BTreeMap::new(),
            generation: BTreeMap::new(),
            custom_status: None,
            deleted: false,
            late_us: Vec::new(),
            parent: None,
            notified: false,
            live: false,
            out: Vec::new(),
            effects: Vec::new(),
        };
        let mut before = 0;
        for entry in &journal {
            match entry {
                Entry::Own(own) => {
                    m.owners.push((before, Own { node: own.node.clone(), at_us: own.at_us, ..Own::default() }))
                }
                Entry::Idle => {}
                _ => before += 1,
            }
        }
        let journal: Vec<Entry> = journal.into_iter().filter(|e| !matches!(e, Entry::Own(_) | Entry::Idle)).collect();
        // What a redrive undid is kept, and not replayed.
        let void = voided(&journal);
        // A park or a pause that a resume follows did not happen, for the code: it replays past it.
        let last_resume = journal.iter().rposition(|e| matches!(e, Entry::Resumed));
        // A run parked on a version this build has resumes: what came after the park are calls, not the code's
        // steps, so from the park on the code runs live.
        let resume = m.resumable(&journal);
        let cut = resume.unwrap_or(journal.len());
        let kept: Vec<Entry> = journal.iter().zip(&void).filter(|(_, v)| !**v).map(|(e, _)| e.clone()).collect();
        m.record(&kept);
        {
            let mut s = m.lock();
            s.wall_us = wall_us;
            s.replay_events = journal[..cut].iter().zip(&void).filter(|(e, v)| polls(e) && !**v).count();
            s.statuses_recorded = kept.iter().filter(|e| matches!(e, Entry::StatusSet { .. })).count();
        }
        if resume.is_some() {
            m.out.push(Entry::Resumed);
        }
        for (i, entry) in journal.into_iter().enumerate() {
            if void[i] {
                // The attempts of a step a redrive undid go on from the last number, and its activity made again
                // gets a key of its own.
                match entry {
                    Entry::ActivityFailed { step, attempt, .. } | Entry::ActivityDone { step, attempt, .. } => {
                        m.failures.insert(step, attempt);
                        m.base.insert(step, attempt);
                    }
                    Entry::ActivityScheduled { step, .. } => *m.generation.entry(step).or_default() += 1,
                    _ => {}
                }
                m.journal.push(entry);
                continue;
            }
            if polls(&entry) && i < cut {
                m.lock().replay_events -= 1;
            }
            let passed = matches!(entry, Entry::Parked { .. } | Entry::Paused) && last_resume > Some(i);
            if passed || Some(i) == resume {
                m.journal.push(entry);
                continue;
            }
            m.apply(entry);
        }
        m.owners.push((m.journal.len(), Own::default()));
        if resume.is_some() {
            m.journal.push(Entry::Resumed);
        }
        m.live = true;
        m.lock().live = true;
        m.settle();
        // A child that ended and was gone before it told its parent tells it now; a cancel that was gone before its
        // children heard it goes to them again.
        m.notify_parent();
        if m.end.is_none() && m.lock().cancelled {
            m.cancel_children();
        }
        m
    }

    fn cancel_children(&mut self) {
        let s = self.lock();
        let children = s.open.iter().filter(|step| matches!(s.issued.get(step), Some(Command::Child { .. })));
        let ids: Vec<String> = children.map(|step| child_id(&s.run, *step)).collect();
        drop(s);
        self.effects.extend(ids.into_iter().map(|id| Effect::CancelChild { id }));
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        lock(&self.shared)
    }

    /// The index of the park this build resumes: the last, with no resume after it, for a version this build has.
    fn resumable(&self, journal: &[Entry]) -> Option<usize> {
        let (workflow, version) = journal.iter().find_map(|e| match e {
            Entry::Started { workflow, version, .. } => Some((workflow, *version)),
            _ => None,
        })?;
        let last = journal.iter().rposition(|e| matches!(e, Entry::Parked { .. }))?;
        let Entry::Parked { reason } = &journal[last] else { return None };
        let resumed = journal[last..].iter().any(|e| matches!(e, Entry::Resumed));
        let fits = !resumed && *reason == not_in_build(workflow, version) && self.app.has(workflow, version);
        fits.then_some(last)
    }

    /// What the journal holds of each step's command.
    fn record(&mut self, journal: &[Entry]) {
        let names = signal_names(journal);
        let mut recorded: BTreeMap<u32, Recorded> = BTreeMap::new();
        for entry in journal {
            match entry {
                Entry::ActivityScheduled { step, name, .. } => {
                    recorded.entry(*step).or_default().activity = Some(name.clone());
                }
                Entry::TimerStarted { step, wake_at_us, .. } => {
                    recorded.entry(*step).or_default().timer = Some(*wake_at_us)
                }
                Entry::SignalConsumed { step, signal_id, .. } => {
                    let name = names.get(signal_id.as_str()).map_or(String::new(), |n| n.to_string());
                    recorded.entry(*step).or_default().signal = Some(name);
                }
                Entry::Value { step, kind, value } => {
                    recorded.entry(*step).or_default().value = Some((*kind, value.clone()));
                }
                Entry::ChildStarted { step, id, .. } => recorded.entry(*step).or_default().child = Some(id.clone()),
                _ => {}
            }
        }
        self.lock().recorded = recorded;
    }

    /// Whether the run has a start, and its workflow, version and input hash.
    pub fn started(&self) -> Option<(&str, u32, u64)> {
        self.started.as_ref().map(|(w, v, h)| (w.as_str(), *v, *h))
    }

    pub fn status(&self) -> Status {
        if self.deleted {
            return Status::Deleted;
        }
        match &self.end {
            Some(Entry::Completed { .. }) => Status::Completed,
            Some(Entry::Failed { .. }) => Status::Failed,
            Some(Entry::Cancelled) => Status::Cancelled,
            Some(Entry::Terminated { .. }) => Status::Terminated,
            Some(_) => Status::Deleted,
            None if self.lock().park.is_some() => Status::Parked,
            None if self.lock().paused => Status::Paused,
            None => Status::Running,
        }
    }

    /// The entries made since the last call, to journal in this order.
    pub fn take_out(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.out)
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    /// How late each timer fired since the last call, in µs.
    pub fn take_late(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.late_us)
    }

    /// Starts a new run: journals its start and polls the code for the first time.
    pub fn start(&mut self, start: Start, wall_us: u64) {
        let input_hash = start.input_hash();
        let Start { workflow, version, input, tags, parent, run_timeout_ms } = start;
        self.lock().wall_us = wall_us;
        let at_us = wall_us;
        self.emit(Entry::Started { workflow, version, input, input_hash, tags, parent, run_timeout_ms, at_us });
        self.settle();
    }

    /// A signal: journaled once per id. An id already received is a duplicate, also once the run ended: the call
    /// that sent it was tried again, and the first may have ended the run.
    pub fn signal(&mut self, signal_id: String, name: String, payload: Bytes, wall_us: u64) -> Result<(), Refusal> {
        // `RUN_MUTATION=no_signal_dedup` takes a signal sent again as a new one: `scripts/mutations.sh`.
        if self.signals.ids.contains_key(&signal_id) && crate::mutation() != "no_signal_dedup" {
            return Err(Refusal::Duplicate);
        }
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        if payload.len() > MAX_PAYLOAD {
            return Err(Refusal::TooLarge);
        }
        if self.signals.untaken >= MAX_SIGNALS {
            return Err(Refusal::Full);
        }
        self.lock().wall_us = wall_us;
        self.emit(Entry::SignalReceived { signal_id, name, payload, at_us: wall_us });
        self.settle();
        Ok(())
    }

    /// From now on every step outside `ctx.cleanup` fails with `Cancelled`.
    pub fn cancel(&mut self, reason: String, wall_us: u64) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        if !self.lock().cancelled {
            self.lock().wall_us = wall_us;
            self.emit(Entry::CancelRequested { reason: clip(reason) });
            self.settle();
        }
        Ok(())
    }

    /// Ends the run at once: no cleanup runs.
    pub fn terminate(&mut self, reason: String) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        self.emit(Entry::Terminated { reason: clip(reason) });
        self.settle();
        Ok(())
    }

    /// An attempt's result. The first outcome of a step wins, whatever its attempt; a failure of an attempt that is
    /// not the step's latest is dropped, and so is any result of an attempt a redrive undid.
    pub fn attempt(
        &mut self,
        step: u32,
        attempt: u32,
        result: Result<Bytes, Failure>,
        wall_us: u64,
    ) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        let policy = match self.lock().issued.get(&step) {
            Some(Command::Activity { policy, .. }) => policy.clone(),
            _ => return Err(Refusal::UnknownStep),
        };
        if !self.lock().open.contains(&step) || attempt <= self.base.get(&step).copied().unwrap_or(0) {
            return Err(Refusal::Duplicate);
        }
        self.lock().wall_us = wall_us;
        let result = result.and_then(|out| match out.len() {
            n if n > MAX_PAYLOAD => Err(Failure::fatal(format!("an output of {n} bytes, past {MAX_PAYLOAD}"))),
            _ => Ok(out),
        });
        match result {
            Ok(output) => self.emit(Entry::ActivityDone { step, attempt, output, at_us: wall_us }),
            Err(failure) => {
                let latest = self.failures.get(&step).copied().unwrap_or(0) + 1;
                if attempt != latest || self.retry_at.contains_key(&step) {
                    return Err(Refusal::Duplicate);
                }
                let counted = attempt - self.base.get(&step).copied().unwrap_or(0);
                let retry_at_us = next_attempt(&policy, counted, &failure, wall_us);
                let error = clip(failure.to_string());
                self.emit(Entry::ActivityFailed { step, attempt, error, retry_at_us, at_us: wall_us });
            }
        }
        self.settle();
        Ok(())
    }

    /// The attempt of the activity at `step` that may run now: `None` once the step has its outcome, or while its
    /// next attempt waits for its backoff.
    pub fn current_attempt(&self, step: u32) -> Option<u32> {
        let s = self.lock();
        let open = matches!(s.issued.get(&step), Some(Command::Activity { .. })) && s.open.contains(&step);
        (open && self.end.is_none() && !s.halted() && !self.retry_at.contains_key(&step))
            .then(|| self.failures.get(&step).copied().unwrap_or(0) + 1)
    }

    /// An attempt of the activity at `step` started: its schedule-to-start timer, if it has one, stops for good.
    pub fn picked(&mut self, step: u32) {
        let timer = matches!(self.lock().issued.get(&step), Some(Command::Activity { timer: Some(_), .. }));
        if timer && self.current_attempt(step).is_some() {
            self.emit(Entry::Picked { step });
        }
    }

    /// The result of the child at `step`: the first for a step wins.
    pub fn child_done(&mut self, step: u32, result: Result<Bytes, String>, wall_us: u64) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        if !matches!(self.lock().issued.get(&step), Some(Command::Child { .. })) {
            return Err(Refusal::UnknownStep);
        }
        if !self.lock().open.contains(&step) {
            return Err(Refusal::Duplicate);
        }
        let result = result.map_err(clip).and_then(|out| match out.len() {
            n if n > MAX_PAYLOAD => Err(format!("an output of {n} bytes, past {MAX_PAYLOAD}")),
            _ => Ok(out),
        });
        self.lock().wall_us = wall_us;
        self.emit(Entry::ChildDone { step, result, at_us: wall_us });
        self.settle();
        Ok(())
    }

    /// The parent took this child's result.
    pub fn notified(&mut self) {
        if !self.notified {
            self.emit(Entry::ParentNotified);
        }
    }

    /// Fails the run at once: it went past a limit the code does not see, as the size of its journal.
    pub fn give_up(&mut self, error: String) {
        if self.end.is_none() {
            self.future = None;
            self.emit(Entry::Failed { error: clip(error), timeout: false });
            self.settle();
        }
    }

    /// Holds the run where it is: no step starts, no timer fires and no signal is taken until a resume. Attempts out
    /// are let go; one that ends meanwhile is journaled, and the code sees it after the resume.
    pub fn pause(&mut self) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        if self.lock().halted() {
            return Err(Refusal::Paused);
        }
        self.emit(Entry::Paused);
        Ok(())
    }

    /// Goes on with a paused or parked run: the code replays its journal again, past the pause or the park. A run
    /// parked on a mismatch the code still has parks again.
    pub fn resume(&mut self, wall_us: u64) -> Result<(), Refusal> {
        if self.end.is_some() {
            return Err(Refusal::Finished);
        }
        let halted = {
            let s = self.lock();
            s.paused || s.park.is_some()
        };
        if !halted {
            return Err(Refusal::NotResumable);
        }
        self.reopen(Entry::Resumed, wall_us);
        Ok(())
    }

    /// Runs a run that failed on an activity again from that step: its later steps, and the failure, are undone,
    /// and the step gets the attempts of its policy again. A child is not redriven: its parent took its failure.
    pub fn redrive(&mut self, wall_us: u64) -> Result<(), Refusal> {
        let step = redrivable(&self.journal).filter(|_| self.parent.is_none() && !self.deleted);
        let Some(step) = step else { return Err(Refusal::NotFailed) };
        self.reopen(Entry::Redriven { step }, wall_us);
        Ok(())
    }

    /// Journals `entry` and builds the run again from its whole journal, as an open does.
    fn reopen(&mut self, entry: Entry, wall_us: u64) {
        self.out.push(entry.clone());
        let mut journal = std::mem::take(&mut self.journal);
        journal.push(entry);
        let (run, numbering) = {
            let s = self.lock();
            (s.run.clone(), s.numbering)
        };
        let mut again = Machine::open(self.app.clone(), &run, journal, numbering, wall_us);
        // `journal` holds no `Own`: the owners, this one too, are this machine's, at the same indexes.
        again.owners = std::mem::take(&mut self.owners);
        again.out.splice(0..0, self.out.drain(..));
        again.effects.splice(0..0, self.effects.drain(..));
        again.late_us.splice(0..0, self.late_us.drain(..));
        *self = again;
    }

    /// Marks an ended run deleted: from now it is not found, and retention removes its journal.
    pub fn delete(&mut self) -> Result<(), Refusal> {
        if self.end.is_none() {
            return Err(Refusal::NotFinished);
        }
        if !self.deleted {
            self.emit(Entry::Deleted);
        }
        Ok(())
    }

    /// The journal from entry `after` (from 0), up to `limit` entries and `max_bytes`, each in postcard; and where
    /// the next page starts, if there is one.
    pub fn history(&self, after: u64, limit: usize, max_bytes: usize) -> (Vec<Bytes>, Option<u64>) {
        let (mut page, mut bytes) = (Vec::new(), 0);
        for (i, entry) in self.journal.iter().enumerate().skip(after as usize) {
            let body = entry.encode();
            if page.len() == limit || (!page.is_empty() && bytes + body.len() > max_bytes) {
                return (page, Some(i as u64));
            }
            bytes += body.len();
            page.push(body);
        }
        (page, None)
    }

    /// The earliest time, in µs of the wall clock, something is due: a sleep, a signal's timeout, the run's
    /// timeout, an activity's next attempt.
    pub fn next_due(&self) -> Option<u64> {
        let s = self.lock();
        if s.halted() {
            return None;
        }
        let timers = s.open.iter().filter_map(|step| s.issued.get(step)?.wake_at());
        timers.chain(self.retry_at.values().copied()).min()
    }

    /// Fires what is due at `wall_us`: never before its time, and nothing once the run ended or parked.
    pub fn fire(&mut self, wall_us: u64) {
        self.lock().wall_us = wall_us;
        loop {
            if self.lock().halted() {
                return;
            }
            let due = {
                let s = self.lock();
                let due = s.open.iter().filter_map(|step| Some((s.issued.get(step)?.wake_at()?, *step)));
                due.filter(|(at, _)| *at <= wall_us).min()
            };
            let Some((at, step)) = due else { break };
            self.late_us.push(wall_us - at);
            self.emit(Entry::TimerFired { step, at_us: wall_us });
            self.settle();
        }
        let retries: Vec<u32> = self.retry_at.iter().filter(|(_, at)| **at <= wall_us).map(|(s, _)| *s).collect();
        for step in retries {
            self.retry_at.remove(&step);
            self.start_attempt(step);
        }
    }

    /// Names this owner, `node`, and its wall clock at the open, in µs, in the run's timeline.
    pub fn opened_by(&mut self, node: &str, at_us: u64) {
        if let Some((_, own)) = self.owners.last_mut() {
            (own.node, own.at_us) = (node.to_owned(), at_us);
        }
    }

    pub fn view(&self, run: &str) -> RunView {
        let (output, error) = match &self.end {
            Some(Entry::Completed { output }) => (Some(String::from_utf8_lossy(output).into_owned()), None),
            Some(Entry::Failed { error, .. }) => (None, Some(error.clone())),
            Some(Entry::Terminated { reason }) => (None, Some(reason.clone())),
            _ => (None, None),
        };
        let (workflow, version) = self.started.as_ref().map_or((String::new(), 0), |(w, v, _)| (w.clone(), *v));
        let (status, due_us) = (self.status(), self.next_due());
        let s = self.lock();
        let parked = s.park.clone().filter(|_| self.end.is_none());
        let (tags, parent, started_us) = match self.journal.first() {
            Some(Entry::Started { tags, parent, at_us, .. }) => (tags.clone(), parent.clone(), Some(*at_us)),
            _ => (Vec::new(), None, None),
        };
        let mut journal = Vec::with_capacity(self.journal.len() + self.owners.len());
        let mut owners = self.owners.iter().peekable();
        for (i, entry) in self.journal.iter().enumerate() {
            while let Some((_, own)) = owners.next_if(|(at, _)| *at <= i) {
                journal.push(Entry::Own(own.clone()));
            }
            journal.push(entry.clone());
        }
        journal.extend(owners.map(|(_, own)| Entry::Own(own.clone())));
        let (steps, events) = timeline(&journal, &s.issued);
        RunView {
            id: run.to_owned(),
            workflow,
            version,
            status,
            output,
            error,
            parked,
            steps,
            events,
            started_us,
            due_us,
            custom_status: self.custom_status.as_deref().map(|b| String::from_utf8_lossy(b).into_owned()),
            tags,
            parent,
            ..RunView::default()
        }
    }

    /// Journals `entry` and applies it.
    fn emit(&mut self, entry: Entry) {
        self.out.push(entry.clone());
        self.apply(entry);
    }

    fn apply(&mut self, entry: Entry) {
        self.journal.push(entry.clone());
        let poll = polls(&entry);
        match entry {
            Entry::Started { workflow, version, input, input_hash, run_timeout_ms, parent, .. } => {
                let run = self.lock().run.clone();
                // A child's id is its parent's, then its step.
                let step = parent.as_deref().and_then(|p| run.strip_prefix(p)?.strip_prefix('/')?.parse().ok());
                self.parent = parent.zip(step);
                self.begin(workflow, version, &input, input_hash, run_timeout_ms);
            }
            Entry::ActivityDone { step, output, .. } => self.outcome(step, Outcome::Output(output)),
            Entry::ActivityFailed { step, attempt, error, retry_at_us, .. } => {
                self.failures.insert(step, attempt);
                match retry_at_us {
                    Some(at) => _ = self.retry_at.insert(step, at),
                    None => self.outcome(step, Outcome::Failed(error)),
                }
            }
            Entry::TimerFired { step: RUN_TIMEOUT, .. } => {
                self.outcome(RUN_TIMEOUT, Outcome::Fired);
                self.future = None;
                self.finished.get_or_insert(Entry::Failed { error: "the run timed out".into(), timeout: true });
            }
            Entry::TimerFired { step, .. } => {
                let activity = matches!(self.lock().issued.get(&step), Some(Command::Activity { .. }));
                self.outcome(step, if activity { Outcome::NotStarted } else { Outcome::Fired });
            }
            Entry::Picked { step } => {
                if let Some(Command::Activity { timer, .. }) = self.lock().issued.get_mut(&step) {
                    *timer = None;
                }
            }
            Entry::ChildDone { step, result, .. } => {
                self.outcome(step, result.map_or_else(Outcome::Failed, Outcome::Output));
            }
            Entry::ParentNotified => self.notified = true,
            Entry::Paused => self.lock().paused = true,
            Entry::Resumed => self.lock().paused = false,
            Entry::Redriven { .. } => {}
            Entry::StatusSet { status } => self.custom_status = Some(status),
            Entry::Deleted => self.deleted = true,
            Entry::SignalReceived { signal_id, name, payload, .. } => {
                let signals = &mut self.signals;
                signals.ids.insert(signal_id.clone(), signals.list.len());
                signals.list.push((signal_id, name, payload, false));
                signals.untaken += 1;
            }
            Entry::SignalConsumed { step, signal_id, .. } => {
                let taken = self.signals.ids.get(&signal_id).and_then(|i| self.signals.list.get_mut(*i));
                let payload = match taken {
                    Some((_, _, payload, taken)) if !*taken => {
                        *taken = true;
                        self.signals.untaken -= 1;
                        payload.clone()
                    }
                    _ => Bytes::from_static(b"null"),
                };
                self.outcome(step, Outcome::Output(payload));
            }
            Entry::CancelRequested { .. } => {
                self.lock().cancelled = true;
                if self.live {
                    self.cancel_children();
                }
            }
            Entry::Parked { reason } => {
                self.parked_in_journal = true;
                self.lock().park(reason);
                self.future = None;
            }
            ref
            end @ (Entry::Completed { .. } | Entry::Failed { .. } | Entry::Cancelled | Entry::Terminated { .. }) => {
                self.end = Some(end.clone());
                self.future = None;
                self.lock().ended = true;
                if self.live {
                    self.effects.push(Effect::StopAttempts);
                    self.notify_parent();
                }
            }
            _ => {}
        }
        if poll {
            self.poll();
        }
    }

    fn outcome(&mut self, step: u32, outcome: Outcome) {
        self.retry_at.remove(&step);
        let mut s = self.lock();
        s.open.remove(&step);
        s.outcomes.entry(step).or_insert(outcome);
    }

    /// Asks to tell the parent how this run ended, if it has one that was not told.
    fn notify_parent(&mut self) {
        let (Some((parent, step)), false) = (&self.parent, self.notified) else { return };
        let result = match &self.end {
            Some(Entry::Completed { output }) => Ok(output.clone()),
            Some(Entry::Failed { error, .. }) => Err(error.clone()),
            Some(Entry::Cancelled) => Err("cancelled".into()),
            Some(Entry::Terminated { reason }) => Err(format!("terminated: {reason}")),
            _ => return,
        };
        self.effects.push(Effect::NotifyParent { parent: parent.clone(), step: *step, result });
    }

    fn begin(&mut self, workflow: String, version: u32, input: &[u8], input_hash: u64, run_timeout_ms: Option<u64>) {
        self.started = Some((workflow.clone(), version, input_hash));
        if let Some(ms) = run_timeout_ms {
            let shared = self.shared.clone();
            let mut s = lock(&shared);
            let wake_at = match s.recorded.get(&RUN_TIMEOUT).and_then(|r| r.timer) {
                Some(at) => at,
                None => {
                    let at = s.wall_us.saturating_add(ms.saturating_mul(1_000));
                    let timer = Entry::TimerStarted { step: RUN_TIMEOUT, wake_at_us: at, at_us: s.wall_us };
                    self.journal.push(timer.clone());
                    self.out.push(timer);
                    at
                }
            };
            s.issued.insert(RUN_TIMEOUT, Command::Sleep(Wake::At(wake_at)));
            s.open.insert(RUN_TIMEOUT);
        }
        match self.app.instance(&workflow, version, Ctx::new(self.shared.clone()), input) {
            Ok(future) => self.future = Some(future),
            Err(why) => self.lock().park(why),
        }
    }

    /// Polls the code once, unless it ended or parked.
    fn poll(&mut self) {
        if self.lock().halted() {
            self.future = None;
        }
        let Some(future) = &mut self.future else { return };
        let polled = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut Context::from_waker(Waker::noop()))));
        let shared = self.shared.clone();
        let mut s = lock(&shared);
        for entry in s.out.drain(..) {
            if let Entry::StatusSet { status } = &entry {
                self.custom_status = Some(status.clone());
            }
            self.journal.push(entry.clone());
            self.out.push(entry);
        }
        if let Some(error) = s.limit.clone() {
            self.finished.get_or_insert(Entry::Failed { error, timeout: false });
            drop(s);
            self.future = None;
            return;
        }
        match polled {
            Ok(Poll::Pending) => {}
            Ok(Poll::Ready(result)) => {
                let unissued = s.recorded.keys().find(|step| **step != RUN_TIMEOUT && !s.issued.contains_key(step));
                if let Some(step) = unissued.copied() {
                    let have = s.recorded.get(&step).map(Recorded::describe).unwrap_or_default();
                    s.park(format!("step {step}: journal has `{have}`, code ends before it"));
                } else if s.park.is_none() {
                    self.finished.get_or_insert(match result {
                        Ok(output) if output.len() > MAX_PAYLOAD => Entry::Failed {
                            error: format!("an output of {} bytes, past {MAX_PAYLOAD}", output.len()),
                            timeout: false,
                        },
                        Ok(output) => Entry::Completed { output },
                        Err(Failure::Cancelled) => Entry::Cancelled,
                        Err(failure) => Entry::Failed { error: clip(failure.to_string()), timeout: false },
                    });
                }
                drop(s);
                self.future = None;
            }
            Err(panic) => {
                let why = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "a panic with no message".into());
                let at = s.next_step;
                s.park(format!("panic at step {at}: {why}"));
                drop(s);
                self.future = None;
            }
        }
    }

    /// Once live: journals what the run decided, gives waiting steps their signals, and starts the attempts of the
    /// activities the code asked for.
    fn settle(&mut self) {
        if !self.live {
            return;
        }
        loop {
            if self.end.is_none()
                && let Some(end) = self.finished.take()
            {
                self.emit(end);
                continue;
            }
            let park = self.lock().park.clone();
            if let Some(reason) = park
                && self.end.is_none()
                && !self.parked_in_journal
            {
                self.emit(Entry::Parked { reason });
                continue;
            }
            if let Some((step, signal_id)) = self.waiting_signal() {
                let at_us = self.lock().wall_us;
                self.emit(Entry::SignalConsumed { step, signal_id, at_us });
                continue;
            }
            break;
        }
        let fresh = std::mem::take(&mut self.lock().fresh);
        for step in fresh {
            self.start_attempt(step);
        }
    }

    /// The lowest step waiting for a signal that one not taken yet answers, and the oldest such signal.
    fn waiting_signal(&self) -> Option<(u32, String)> {
        if self.end.is_some() || self.signals.untaken == 0 {
            return None;
        }
        let s = self.lock();
        if s.halted() {
            return None;
        }
        s.open.iter().find_map(|step| match s.issued.get(step)? {
            Command::Signal { name, .. } => self
                .signals
                .list
                .iter()
                .find(|(_, n, _, taken)| n == name && !taken)
                .map(|(id, ..)| (*step, id.clone())),
            _ => None,
        })
    }

    fn start_attempt(&mut self, step: u32) {
        if self.retry_at.contains_key(&step) {
            return;
        }
        let s = self.lock();
        if s.halted() {
            return;
        }
        if !s.open.contains(&step) {
            return;
        }
        let effect = match s.issued.get(&step) {
            Some(Command::Activity { name, input, queue, policy, .. }) => Effect::Attempt {
                step,
                attempt: self.failures.get(&step).copied().unwrap_or(0) + 1,
                generation: self.generation.get(&step).copied().unwrap_or(0),
                name: name.clone(),
                input: input.clone(),
                queue: queue.clone(),
                policy: policy.clone(),
            },
            Some(Command::Child { workflow, input }) => Effect::StartChild {
                step,
                id: child_id(&s.run, step),
                workflow: workflow.clone(),
                input: input.clone(),
            },
            _ => return,
        };
        drop(s);
        self.effects.push(effect);
    }
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}
