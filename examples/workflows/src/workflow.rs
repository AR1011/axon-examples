//! What a workflow's author writes against: [`Ctx`] and its steps, [`Activity`], [`Failure`] and [`Retry`], and the
//! [`App`] that names each workflow and activity.
//!
//! A workflow is an `async fn(Ctx, In) -> Result<Out, Failure>`. Each `ctx` call that does something durable is a
//! step, numbered when it is made, not when it is first awaited: the run's journal holds each step's command and
//! outcome by that number, and a run that opens again replays the journal through the same code. The code between
//! steps must do the same thing each time it runs; what changes from run to run (the time, a random number) comes
//! from `ctx.now()`, `ctx.random()` and `ctx.uuid()`, which the journal keeps.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::journal::{Entry, Policy, ValueKind};
use crate::run::machine::MAX_PAYLOAD;

/// Why a step or a workflow did not give a value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Failure {
    /// An activity failed, and may work if tried again.
    Retry(String),
    /// Trying again will not help: an activity's last failure, or the workflow's own.
    Fatal(String),
    /// The run was cancelled: every step outside `ctx.cleanup` gives this from then on.
    Cancelled,
    /// A time limit passed; the text says which.
    Timeout(String),
}

impl Failure {
    pub fn retry(why: impl fmt::Display) -> Self {
        Failure::Retry(why.to_string())
    }

    pub fn fatal(why: impl fmt::Display) -> Self {
        Failure::Fatal(why.to_string())
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Retry(why) | Failure::Fatal(why) => f.write_str(why),
            Failure::Cancelled => f.write_str("cancelled"),
            Failure::Timeout(what) => write!(f, "timeout: {what}"),
        }
    }
}

impl std::error::Error for Failure {}

/// Work with effects outside the run: a call to another system, a write, an email. It may run more than once (a
/// retry, or again after its run moves to another node), so it should make the same effect once per
/// [`ActivityCx::idempotency_key`].
pub trait Activity: Serialize + DeserializeOwned + Send + 'static {
    const NAME: &'static str;
    /// The task queue whose workers run it: `local` runs it on the run's own node, any other name on a worker that
    /// polls that queue.
    const QUEUE: &'static str = "local";
    type Output: Serialize + DeserializeOwned + Send;
    fn run(self, cx: ActivityCx) -> impl Future<Output = Result<Self::Output, Failure>> + Send;
}

/// An attempt of an activity: which run and step it serves, and which attempt it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityCx {
    run: String,
    step: u32,
    attempt: u32,
    generation: u32,
    node: Arc<str>,
    epoch: u64,
}

impl ActivityCx {
    pub fn new(run: impl Into<String>, step: u32, attempt: u32) -> Self {
        Self { run: run.into(), step, attempt, generation: 0, node: Arc::from(""), epoch: 0 }
    }

    /// A redrive undid `generation` activities at this step before this one: the code made the step again.
    pub fn redone(mut self, generation: u32) -> Self {
        self.generation = generation;
        self
    }

    /// Run by the owner on `node` under lease `epoch`: what a witness of each execution records.
    pub fn by(mut self, node: Arc<str>, epoch: u64) -> Self {
        (self.node, self.epoch) = (node, epoch);
        self
    }

    /// The node whose owner of the run started this attempt; empty outside a cluster.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The lease epoch of that owner.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn run_id(&self) -> &str {
        &self.run
    }

    pub fn step(&self) -> u32 {
        self.step
    }

    /// From 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The same for every attempt of the step: `<run>/<step>`. A step a redrive undid and the code made again is
    /// another activity: `<run>/<step>.<n>` for the n-th made again.
    pub fn idempotency_key(&self) -> String {
        match self.generation {
            0 => format!("{}/{}", self.run, self.step),
            n => format!("{}/{}.{n}", self.run, self.step),
        }
    }
}

/// How an activity is tried again: `attempts` in all, waiting `initial`, then twice as long each time up to `max`.
/// A failure whose text is one of `non_retryable`, or starts with one and a `:`, is not tried again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retry(Policy);

impl Retry {
    pub fn exponential(attempts: u32) -> Self {
        Retry(Policy { attempts: attempts.max(1), ..Policy::default() })
    }

    /// One attempt only.
    pub fn none() -> Self {
        Self::exponential(1)
    }

    pub fn initial(mut self, wait: Duration) -> Self {
        self.0.initial_ms = wait.as_millis() as u64;
        self
    }

    pub fn max(mut self, wait: Duration) -> Self {
        self.0.max_ms = wait.as_millis() as u64;
        self
    }

    pub fn non_retryable(mut self, codes: &[&str]) -> Self {
        self.0.non_retryable = codes.iter().map(|c| c.to_string()).collect();
        self
    }
}

/// When attempt `attempt` (from 1) failed at `now_us`, when the next one is due; `None` when the policy says no more.
pub fn next_attempt(policy: &Policy, attempt: u32, error: &Failure, now_us: u64) -> Option<u64> {
    let Failure::Retry(why) = error else { return None };
    let code = why.split(':').next().unwrap_or_default();
    if attempt >= policy.attempts || policy.non_retryable.iter().any(|c| c == why || c == code) {
        return None;
    }
    let wait_ms = policy.initial_ms.saturating_mul(1 << (attempt - 1).min(32)).min(policy.max_ms);
    Some(now_us.saturating_add(wait_ms.saturating_mul(1_000)))
}

pub fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

pub fn mins(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}

pub fn days(n: u64) -> Duration {
    Duration::from_secs(n * 86_400)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        })
    }
}

/// Where `ctx.log` lines go: the run, the level, the line.
pub type LogSink = Arc<dyn Fn(&str, Level, &str) + Send + Sync>;

pub(crate) type WorkflowFuture = Pin<Box<dyn Future<Output = Result<Bytes, Failure>> + Send>>;
type MakeWorkflow = Arc<dyn Fn(Ctx, &[u8]) -> Result<WorkflowFuture, String> + Send + Sync>;
pub(crate) type ActivityFuture = Pin<Box<dyn Future<Output = Result<Bytes, Failure>> + Send>>;
type MakeActivity = Arc<dyn Fn(&[u8], ActivityCx) -> ActivityFuture + Send + Sync>;
type Witness = Arc<dyn Fn(&ActivityCx) + Send + Sync>;

/// The workflows and activities a build runs, by name and version.
#[derive(Clone, Default)]
pub struct App {
    workflows: BTreeMap<String, BTreeMap<u32, MakeWorkflow>>,
    activities: BTreeMap<String, (MakeActivity, &'static str)>,
    log: Option<LogSink>,
    retention: BTreeMap<String, Duration>,
    witness: Option<Witness>,
}

impl App {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump `version` when a step is added, removed, moved or renamed, or changes its kind: runs in flight stay on
    /// the version they started on, so keep the old one registered until they end.
    pub fn workflow<I, O, F, Fut>(mut self, name: &str, version: u32, f: F) -> Self
    where
        I: DeserializeOwned + 'static,
        O: Serialize + 'static,
        F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Failure>> + Send + 'static,
    {
        let f = Arc::new(f);
        let make = move |ctx: Ctx, input: &[u8]| -> Result<WorkflowFuture, String> {
            let input: I = serde_json::from_slice(input).map_err(|e| format!("the input does not fit: {e}"))?;
            let run = f(ctx, input);
            Ok(Box::pin(async move {
                let out = run.await?;
                serde_json::to_vec(&out).map(Bytes::from).map_err(|e| Failure::fatal(format!("the output: {e}")))
            }))
        };
        self.workflows.entry(name.to_owned()).or_default().insert(version, Arc::new(make));
        self
    }

    pub fn activity<A: Activity>(mut self) -> Self {
        let make = |input: &[u8], cx: ActivityCx| -> ActivityFuture {
            let activity = serde_json::from_slice::<A>(input);
            Box::pin(async move {
                let activity = activity.map_err(|e| Failure::fatal(format!("the input of {}: {e}", A::NAME)))?;
                let out = activity.run(cx).await?;
                serde_json::to_vec(&out).map(Bytes::from).map_err(|e| Failure::fatal(format!("the output: {e}")))
            })
        };
        self.activities.insert(A::NAME.to_owned(), (Arc::new(make), A::QUEUE));
        self
    }

    /// The task queues the activities name by default, `local` left out: those a worker of this build can serve.
    pub fn queues(&self) -> BTreeSet<&'static str> {
        self.activities.values().map(|(_, queue)| *queue).filter(|q| *q != LOCAL).collect()
    }

    /// How long a finished run of `workflow` is kept, failed or not, in place of the node's `retention=` and
    /// `failed_retention=`.
    pub fn retention(mut self, workflow: &str, keep: Duration) -> Self {
        self.retention.insert(workflow.to_owned(), keep);
        self
    }

    pub fn retentions(&self) -> &BTreeMap<String, Duration> {
        &self.retention
    }

    /// Where `ctx.log` writes, in place of standard error.
    pub fn log_to(mut self, sink: impl Fn(&str, Level, &str) + Send + Sync + 'static) -> Self {
        self.log = Some(Arc::new(sink));
        self
    }

    /// Called as each activity attempt starts, before its code: the checker's witness of what ran.
    pub fn witness(mut self, sink: impl Fn(&ActivityCx) + Send + Sync + 'static) -> Self {
        self.witness = Some(Arc::new(sink));
        self
    }

    /// Each workflow with its versions, lowest first.
    pub fn workflows(&self) -> impl Iterator<Item = (&str, Vec<u32>)> {
        self.workflows.iter().map(|(name, versions)| (name.as_str(), versions.keys().copied().collect()))
    }

    pub fn latest(&self, workflow: &str) -> Option<u32> {
        self.workflows.get(workflow)?.keys().next_back().copied()
    }

    pub fn has(&self, workflow: &str, version: u32) -> bool {
        self.workflows.get(workflow).is_some_and(|v| v.contains_key(&version))
    }

    pub(crate) fn instance(
        &self,
        workflow: &str,
        version: u32,
        ctx: Ctx,
        input: &[u8],
    ) -> Result<WorkflowFuture, String> {
        let make = self.workflows.get(workflow).and_then(|v| v.get(&version));
        let make = make.ok_or_else(|| not_in_build(workflow, version))?;
        make(ctx, input)
    }

    /// Whether `input` fits the workflow's input type, without running it.
    pub(crate) fn check_input(&self, workflow: &str, version: u32, input: &[u8]) -> Result<(), String> {
        let log: LogSink = Arc::new(|_: &str, _: Level, _: &str| {});
        let shared = Shared::new(String::new(), Numbering::AtCreation, log);
        self.instance(workflow, version, Ctx::new(Arc::new(Mutex::new(shared))), input).map(drop)
    }

    pub(crate) fn activity_future(&self, name: &str, input: &[u8], cx: ActivityCx) -> Option<ActivityFuture> {
        let (make, _) = self.activities.get(name)?;
        if let Some(witness) = &self.witness {
            witness(&cx);
        }
        Some(make(input, cx))
    }

    pub(crate) fn log(&self) -> LogSink {
        self.log
            .clone()
            .unwrap_or_else(|| Arc::new(|run: &str, level: Level, line: &str| eprintln!("{run} {level}: {line}")))
    }
}

/// The queue whose activities run on the run's own node.
pub const LOCAL: &str = "local";
/// Steps a run takes, at most: a step past it fails the run. A workflow that loops goes past it.
pub const MAX_STEPS: u32 = 10_000;
/// A `ctx.set_status` value, at most.
pub const MAX_STATUS: usize = 4 << 10;
/// Steps one `ctx.all` waits for, at most.
pub const MAX_FANOUT: usize = 1_000;

/// Why a run parks when its node lacks its version: a node that has it resumes the run.
pub fn not_in_build(workflow: &str, version: u32) -> String {
    format!("version {version} of {workflow} is not in this build")
}

/// How a step's number is taken. Only the mutation run uses the other.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Numbering {
    #[default]
    AtCreation,
    /// Weakened: at the step's first poll.
    AtPoll,
}

/// When a sleep ends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Wake {
    After(Duration),
    /// µs of the wall clock.
    At(u64),
}

impl Wake {
    fn at(&self, now_us: u64) -> u64 {
        match self {
            Wake::After(d) => now_us.saturating_add(d.as_micros() as u64),
            Wake::At(t) => *t,
        }
    }
}

/// What the code asks a step to do.
#[derive(Clone, Debug)]
pub(crate) enum Command {
    Activity {
        name: String,
        queue: String,
        input: Bytes,
        policy: Policy,
        /// The schedule-to-start timer, until a worker starts an attempt.
        timer: Option<Wake>,
    },
    Sleep(Wake),
    Child {
        workflow: String,
        input: Bytes,
    },
    Signal {
        name: String,
        timeout: Option<Wake>,
    },
    Value(ValueKind),
    /// A step the code could not even ask for: its input does not encode.
    Invalid(String),
}

impl Command {
    fn describe(&self) -> String {
        match self {
            Command::Activity { name, .. } => name.clone(),
            Command::Sleep(_) => "sleep".into(),
            Command::Child { workflow, .. } => format!("child {workflow}"),
            Command::Signal { name, .. } => format!("signal {name}"),
            Command::Value(kind) => value_name(*kind).into(),
            Command::Invalid(_) => "nothing".into(),
        }
    }

    /// The µs of the wall clock it fires at, for a sleep or a signal's timeout.
    pub(crate) fn wake_at(&self) -> Option<u64> {
        match self {
            Command::Sleep(Wake::At(t))
            | Command::Signal { timeout: Some(Wake::At(t)), .. }
            | Command::Activity { timer: Some(Wake::At(t)), .. } => Some(*t),
            _ => None,
        }
    }
}

/// A reason or an error as the journal keeps it, at most: a panic's message or an activity's error may be any size.
const MAX_TEXT: usize = 4 << 10;

pub(crate) fn clip(mut text: String) -> String {
    if text.len() > MAX_TEXT {
        let mut end = MAX_TEXT;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

pub(crate) fn value_name(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Now => "now",
        ValueKind::Random => "random",
        ValueKind::Uuid => "uuid",
    }
}

/// What the journal holds of a step's command, gathered from the whole journal when the run opens.
#[derive(Clone, Debug, Default)]
pub(crate) struct Recorded {
    pub activity: Option<String>,
    pub timer: Option<u64>,
    pub signal: Option<String>,
    pub value: Option<(ValueKind, Bytes)>,
    pub child: Option<String>,
}

impl Recorded {
    pub(crate) fn describe(&self) -> String {
        match (&self.activity, &self.signal, &self.value, &self.child) {
            (Some(name), ..) => name.clone(),
            (_, Some(name), ..) => format!("signal {name}"),
            (_, _, Some((kind, _)), _) => value_name(*kind).into(),
            (.., Some(id)) => format!("child {id}"),
            _ => "sleep".into(),
        }
    }

    /// Takes the recorded wake time into `cmd`, or says what the journal has where the code asks for another kind.
    fn adopt(&self, cmd: &mut Command) -> Result<(), String> {
        let fits = match cmd {
            Command::Activity { name, timer, .. } => {
                let fits = self.activity.as_ref() == Some(name)
                    && self.timer.is_some() == timer.is_some()
                    && self.signal.is_none()
                    && self.child.is_none();
                if let (true, Some(t)) = (fits, self.timer) {
                    *timer = Some(Wake::At(t));
                }
                fits
            }
            Command::Child { .. } => {
                self.child.is_some() && self.activity.is_none() && self.timer.is_none() && self.signal.is_none()
            }
            Command::Sleep(wake) => match self.timer {
                Some(t)
                    if self.activity.is_none()
                        && self.signal.is_none()
                        && self.value.is_none()
                        && self.child.is_none() =>
                {
                    *wake = Wake::At(t);
                    true
                }
                _ => false,
            },
            Command::Signal { name, timeout } => {
                let fits = self.activity.is_none()
                    && self.value.is_none()
                    && self.child.is_none()
                    && self.signal.as_ref().is_none_or(|n| n == name)
                    && self.timer.is_some() == timeout.is_some();
                if let (true, Some(t)) = (fits, self.timer) {
                    *timeout = Some(Wake::At(t));
                }
                fits
            }
            Command::Value(_) | Command::Invalid(_) => false,
        };
        if fits { Ok(()) } else { Err(self.describe()) }
    }
}

/// A step's outcome, as the journal records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// An activity's output or a signal's payload, in JSON.
    Output(Bytes),
    /// An activity's last failure.
    Failed(String),
    /// A sleep ended, or a signal's wait timed out.
    Fired,
    /// No worker started the activity in its schedule-to-start time.
    NotStarted,
}

/// What a run's code and its machine share: the steps asked for, their outcomes, and what the code's last poll
/// asked to journal.
pub(crate) struct Shared {
    pub run: String,
    pub numbering: Numbering,
    pub next_step: u32,
    pub recorded: BTreeMap<u32, Recorded>,
    /// The machine has applied the opened journal: from here the code runs past what the journal holds.
    pub live: bool,
    /// Entries of the opened journal still to apply that poll the code: while any is left, a step the journal does
    /// not hold means the code changed.
    pub replay_events: usize,
    pub issued: BTreeMap<u32, Command>,
    /// Issued steps with no outcome yet.
    pub open: BTreeSet<u32>,
    pub outcomes: BTreeMap<u32, Outcome>,
    /// Steps issued since the machine last looked.
    pub fresh: Vec<u32>,
    /// What the polls asked to journal, in order.
    pub out: Vec<Entry>,
    pub park: Option<String>,
    /// A limit the code went past: the run fails with it.
    pub limit: Option<String>,
    pub ended: bool,
    /// Paused by an operator: the code waits for a resume.
    pub paused: bool,
    pub cancelled: bool,
    /// `ctx.set_status` calls so far, and the `StatusSet` entries the journal holds: a replay journals none again.
    pub statuses: usize,
    pub statuses_recorded: usize,
    pub wall_us: u64,
    pub log: LogSink,
}

impl Shared {
    pub fn new(run: String, numbering: Numbering, log: LogSink) -> Self {
        Self {
            run,
            numbering,
            next_step: 1,
            recorded: BTreeMap::new(),
            live: false,
            replay_events: 0,
            issued: BTreeMap::new(),
            open: BTreeSet::new(),
            outcomes: BTreeMap::new(),
            fresh: Vec::new(),
            out: Vec::new(),
            park: None,
            limit: None,
            ended: false,
            paused: false,
            cancelled: false,
            statuses: 0,
            statuses_recorded: 0,
            wall_us: 0,
            log,
        }
    }

    pub fn halted(&self) -> bool {
        self.park.is_some() || self.limit.is_some() || self.ended || self.paused
    }

    pub fn replaying(&self) -> bool {
        !self.live
    }

    fn take_step(&mut self) -> u32 {
        let step = self.next_step;
        self.next_step += 1;
        step
    }

    /// A step past [`MAX_STEPS`] fails the run, unless the journal holds it: a run that took it before the limit
    /// replays it.
    fn past_limit(&mut self, step: u32) -> bool {
        if step <= MAX_STEPS || self.recorded.contains_key(&step) {
            return false;
        }
        self.limit.get_or_insert_with(|| format!("step {step}: a run takes at most {MAX_STEPS} steps"));
        true
    }

    pub fn park(&mut self, reason: String) {
        self.park.get_or_insert_with(|| clip(reason));
    }

    /// Takes the step's command: as the journal holds it, or new, and then journaled.
    fn issue(&mut self, step: u32, mut cmd: Command, cleanup: bool) -> Result<(), Failure> {
        if self.halted() || self.issued.contains_key(&step) {
            return Ok(());
        }
        if let Command::Invalid(why) = cmd {
            return Err(Failure::Fatal(why));
        }
        if self.past_limit(step) {
            return Ok(());
        }
        match self.recorded.get(&step) {
            Some(rec) => {
                if let Err(have) = rec.adopt(&mut cmd) {
                    self.park(format!("step {step}: journal has `{have}`, code asks for `{}`", cmd.describe()));
                    return Ok(());
                }
            }
            // Never journaled, so never in the journal: not a sign the code changed.
            None if self.cancelled && !cleanup => return Err(Failure::Cancelled),
            // A wait for a signal with no timeout journals nothing until it takes one.
            None if self.replay_events > 0 && !matches!(cmd, Command::Signal { timeout: None, .. }) => {
                self.park(format!("step {step}: journal has nothing, code asks for `{}`", cmd.describe()));
                return Ok(());
            }
            None => match &mut cmd {
                Command::Activity { name, queue, input, policy, timer } => {
                    self.out.push(Entry::ActivityScheduled {
                        step,
                        name: name.clone(),
                        queue: queue.clone(),
                        input: input.clone(),
                        policy: policy.clone(),
                        at_us: self.wall_us,
                    });
                    if let Some(wake) = timer {
                        let wake_at_us = wake.at(self.wall_us);
                        *wake = Wake::At(wake_at_us);
                        self.out.push(Entry::TimerStarted { step, wake_at_us, at_us: self.wall_us });
                    }
                }
                Command::Child { .. } => {
                    let id = child_id(&self.run, step);
                    self.out.push(Entry::ChildStarted { step, id, at_us: self.wall_us });
                }
                Command::Sleep(wake) | Command::Signal { timeout: Some(wake), .. } => {
                    let wake_at_us = wake.at(self.wall_us);
                    *wake = Wake::At(wake_at_us);
                    self.out.push(Entry::TimerStarted { step, wake_at_us, at_us: self.wall_us });
                }
                Command::Signal { timeout: None, .. } | Command::Value(_) | Command::Invalid(_) => {}
            },
        }
        self.issued.insert(step, cmd);
        if !self.outcomes.contains_key(&step) {
            self.open.insert(step);
        }
        self.fresh.push(step);
        Ok(())
    }

    /// A journaled value: the journal's on replay, `make`'s otherwise.
    fn value(&mut self, kind: ValueKind, make: impl FnOnce(u64) -> Bytes) -> Option<Bytes> {
        let step = self.take_step();
        if self.halted() || self.past_limit(step) {
            return None;
        }
        let value = match self.recorded.get(&step) {
            Some(Recorded { value: Some((k, v)), activity: None, timer: None, signal: None, child: None })
                if *k == kind =>
            {
                v.clone()
            }
            Some(rec) => {
                let have = rec.describe();
                self.park(format!("step {step}: journal has `{have}`, code asks for `{}`", value_name(kind)));
                return None;
            }
            None if self.replay_events > 0 => {
                self.park(format!("step {step}: journal has nothing, code asks for `{}`", value_name(kind)));
                return None;
            }
            None => {
                let value = make(self.wall_us);
                self.out.push(Entry::Value { step, kind, value: value.clone() });
                value
            }
        };
        self.issued.insert(step, Command::Value(kind));
        self.outcomes.insert(step, Outcome::Output(value.clone()));
        Some(value)
    }
}

/// A workflow's handle on its run. Cheap to clone; every clone is the same run.
#[derive(Clone)]
pub struct Ctx {
    shared: Arc<Mutex<Shared>>,
    cleanup: bool,
}

impl Ctx {
    pub(crate) fn new(shared: Arc<Mutex<Shared>>) -> Self {
        Self { shared, cleanup: false }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn step<T>(&self, cmd: Command, decode: fn(&Outcome) -> Result<T, Failure>) -> Step<T> {
        let mut s = self.lock();
        let step = (s.numbering == Numbering::AtCreation).then(|| s.take_step());
        Step { ctx: self.clone(), step, cmd: Some(cmd), decode }
    }

    pub fn run_id(&self) -> String {
        self.lock().run.clone()
    }

    /// True while the code runs again over what the journal already holds.
    pub fn is_replaying(&self) -> bool {
        self.lock().replaying()
    }

    /// A line to the log, written once: not again on replay.
    pub fn log(&self, level: Level, line: impl AsRef<str>) {
        let (sink, run) = {
            let s = self.lock();
            if s.replaying() || s.halted() {
                return;
            }
            (s.log.clone(), s.run.clone())
        };
        sink(&run, level, line.as_ref());
    }

    /// What the run page shows as the run's own status, in JSON: journaled, but not a step, so code may add or
    /// drop a call without a new version. One past [`MAX_STATUS`] bytes is kept as a note that it was.
    pub fn set_status(&self, status: impl Serialize) {
        let mut body = serde_json::to_vec(&status).unwrap_or_default();
        if body.len() > MAX_STATUS {
            body = format!("{{\"error\":\"a status of {} bytes, past {MAX_STATUS}\"}}", body.len()).into_bytes();
        }
        let mut s = self.lock();
        if s.halted() {
            return;
        }
        s.statuses += 1;
        if s.statuses > s.statuses_recorded {
            s.statuses_recorded = s.statuses;
            s.out.push(Entry::StatusSet { status: body.into() });
        }
    }

    pub fn activity<A: Activity>(&self, activity: A) -> Step<A::Output> {
        let cmd = match serde_json::to_vec(&activity) {
            // Past it the entry would not fit an append, and the run would fail at each open.
            Ok(input) if input.len() > MAX_PAYLOAD => {
                Command::Invalid(format!("the input of {}: {} bytes, past {MAX_PAYLOAD}", A::NAME, input.len()))
            }
            Ok(input) => Command::Activity {
                name: A::NAME.into(),
                queue: A::QUEUE.into(),
                input: input.into(),
                policy: Policy::default(),
                timer: None,
            },
            Err(e) => Command::Invalid(format!("the input of {}: {e}", A::NAME)),
        };
        self.step(cmd, decode_output::<A::Output>)
    }

    pub fn sleep(&self, wait: Duration) -> Step<()> {
        self.step(Command::Sleep(Wake::After(wait)), fired)
    }

    /// Until `unix_us`, µs of the wall clock.
    pub fn sleep_until(&self, unix_us: u64) -> Step<()> {
        self.step(Command::Sleep(Wake::At(unix_us)), fired)
    }

    /// A child run of `workflow`, at its highest version, with id `<this run>/<step>`: its output, or its failure.
    /// Cancelling this run cancels the child.
    pub fn child<O: DeserializeOwned>(&self, workflow: &str, input: impl Serialize) -> Step<O> {
        let cmd = match serde_json::to_vec(&input) {
            Ok(input) if input.len() > MAX_PAYLOAD => {
                Command::Invalid(format!("the input of child {workflow}: {} bytes, past {MAX_PAYLOAD}", input.len()))
            }
            Ok(input) => Command::Child { workflow: workflow.into(), input: input.into() },
            Err(e) => Command::Invalid(format!("the input of child {workflow}: {e}")),
        };
        self.step(cmd, decode_output::<O>)
    }

    /// The oldest signal named `name` that no wait took yet; one sent before the wait began counts.
    pub fn signal<T: DeserializeOwned>(&self, name: &str) -> Step<T> {
        self.step(Command::Signal { name: name.into(), timeout: None }, decode_output::<T>)
    }

    /// Every step's value in the order given, or the first failure among them. The steps keep the numbers they
    /// were made with, whatever the order here. More than [`MAX_FANOUT`] steps fail at once, and none of them runs.
    pub fn all<T, F>(&self, steps: impl IntoIterator<Item = F>) -> All<T, F>
    where
        F: Future<Output = Result<T, Failure>>,
    {
        let steps: Vec<_> = steps.into_iter().map(|f| Some(Box::pin(f))).collect();
        let refused = (steps.len() > MAX_FANOUT)
            .then(|| Failure::fatal(format!("ctx.all of {} steps, past {MAX_FANOUT}", steps.len())));
        let done = steps.iter().map(|_| None).collect();
        All { steps, done, refused }
    }

    /// Steps made inside `f` run though the run is cancelled: what undoes the work before the cancel.
    pub fn cleanup<R>(&self, f: impl FnOnce(&Ctx) -> R) -> R {
        f(&Ctx { shared: self.shared.clone(), cleanup: true })
    }

    /// The wall clock in µs since the Unix epoch, the same on replay.
    pub fn now(&self) -> u64 {
        self.value(ValueKind::Now, |wall_us| json(&wall_us))
    }

    pub fn random(&self) -> u64 {
        self.value(ValueKind::Random, |_| json(&axon::trace::random_u64()))
    }

    /// A random UUID, version 4.
    pub fn uuid(&self) -> String {
        let make = |_: u64| {
            let (a, b) = (axon::trace::random_u64(), axon::trace::random_u64());
            let a = (a & !0xf000) | 0x4000;
            let b = (b & !(0xc << 60)) | (0x8 << 60);
            let id = format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                a >> 32,
                (a >> 16) & 0xffff,
                a & 0xffff,
                b >> 48,
                b & 0xffff_ffff_ffff
            );
            json(&id)
        };
        self.value(ValueKind::Uuid, make)
    }

    /// A journaled value, read back; the default once the run halts, as the code goes no further then.
    fn value<T: DeserializeOwned + Default>(&self, kind: ValueKind, make: impl FnOnce(u64) -> Bytes) -> T {
        let value = self.lock().value(kind, make);
        value.and_then(|v| serde_json::from_slice(&v).ok()).unwrap_or_default()
    }
}

/// A child's run id: its parent's, then its step.
pub fn child_id(parent: &str, step: u32) -> String {
    format!("{parent}/{step}")
}

fn json<T: Serialize>(value: &T) -> Bytes {
    serde_json::to_vec(value).map(Bytes::from).unwrap_or_default()
}

fn decode_output<T: DeserializeOwned>(outcome: &Outcome) -> Result<T, Failure> {
    match outcome {
        Outcome::Output(bytes) => {
            serde_json::from_slice(bytes).map_err(|e| Failure::fatal(format!("the step's value does not fit: {e}")))
        }
        Outcome::Failed(why) => Err(Failure::Fatal(why.clone())),
        Outcome::Fired => Err(Failure::Timeout("signal".into())),
        Outcome::NotStarted => Err(Failure::Timeout("schedule_to_start".into())),
    }
}

fn decode_or_none<T: DeserializeOwned>(outcome: &Outcome) -> Result<Option<T>, Failure> {
    match outcome {
        Outcome::Fired => Ok(None),
        other => decode_output(other).map(Some),
    }
}

fn fired(outcome: &Outcome) -> Result<(), Failure> {
    match outcome {
        Outcome::Fired => Ok(()),
        Outcome::Failed(why) => Err(Failure::Fatal(why.clone())),
        Outcome::Output(_) | Outcome::NotStarted => Err(Failure::fatal("a sleep ended with a value")),
    }
}

/// One durable step: await it for its value.
#[must_use = "a step does nothing until it is awaited"]
pub struct Step<T> {
    ctx: Ctx,
    step: Option<u32>,
    cmd: Option<Command>,
    decode: fn(&Outcome) -> Result<T, Failure>,
}

impl<T> Step<T> {
    /// An activity's retry policy.
    pub fn retry(self, retry: Retry) -> Self {
        let Retry(r) = retry;
        self.policy(|p| {
            (p.attempts, p.initial_ms, p.max_ms, p.non_retryable) =
                (r.attempts, r.initial_ms, r.max_ms, r.non_retryable)
        })
    }

    /// An activity's task queue, in place of its `QUEUE`.
    pub fn queue(mut self, queue: &str) -> Self {
        if let Some(Command::Activity { queue: q, .. }) = &mut self.cmd {
            *q = queue.into();
        }
        self
    }

    /// An attempt that runs longer fails, and the step is tried again by its retry policy.
    pub fn start_to_close(self, d: Duration) -> Self {
        self.policy(|p| p.start_to_close_ms = Some(millis(d)))
    }

    /// An attempt on a worker whose heartbeats stop for this long fails, and the step is tried again. A worker
    /// beats at half of it.
    pub fn heartbeat(self, d: Duration) -> Self {
        self.policy(|p| p.heartbeat_ms = Some(millis(d)))
    }

    /// When no worker starts the activity this long after it is scheduled, the step fails with
    /// `Timeout("schedule_to_start")`, not tried again.
    pub fn schedule_to_start(mut self, d: Duration) -> Self {
        if let Some(Command::Activity { timer, .. }) = &mut self.cmd {
            *timer = Some(Wake::After(d));
        }
        self.policy(|p| p.schedule_to_start_ms = Some(millis(d)))
    }

    fn policy(mut self, f: impl FnOnce(&mut Policy)) -> Self {
        if let Some(Command::Activity { policy, .. }) = &mut self.cmd {
            f(policy);
        }
        self
    }
}

fn millis(d: Duration) -> u64 {
    d.as_millis().try_into().unwrap_or(u64::MAX)
}

impl<T: DeserializeOwned> Step<T> {
    /// A signal's wait that ends with `None` after `wait`.
    pub fn timeout(mut self, wait: Duration) -> Step<Option<T>> {
        if let Some(Command::Signal { timeout, .. }) = &mut self.cmd {
            *timeout = Some(Wake::After(wait));
        }
        Step { ctx: self.ctx, step: self.step, cmd: self.cmd, decode: decode_or_none::<T> }
    }
}

impl<T> Future for Step<T> {
    type Output = Result<T, Failure>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut s = this.ctx.lock();
        let step = *this.step.get_or_insert_with(|| s.take_step());
        if let Some(cmd) = this.cmd.take()
            && let Err(failure) = s.issue(step, cmd, this.ctx.cleanup)
        {
            return Poll::Ready(Err(failure));
        }
        if s.halted() {
            return Poll::Pending;
        }
        if let Some(outcome) = s.outcomes.get(&step) {
            return Poll::Ready((this.decode)(outcome));
        }
        if s.cancelled && !this.ctx.cleanup {
            return Poll::Ready(Err(Failure::Cancelled));
        }
        Poll::Pending
    }
}

/// [`Ctx::all`]'s future.
pub struct All<T, F> {
    steps: Vec<Option<Pin<Box<F>>>>,
    done: Vec<Option<T>>,
    refused: Option<Failure>,
}

impl<T, F> Unpin for All<T, F> {}

impl<T, F: Future<Output = Result<T, Failure>>> Future for All<T, F> {
    type Output = Result<Vec<T>, Failure>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(failure) = this.refused.clone() {
            return Poll::Ready(Err(failure));
        }
        for (slot, done) in this.steps.iter_mut().zip(this.done.iter_mut()) {
            let Some(step) = slot else { continue };
            match step.as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => {
                    *done = Some(value);
                    *slot = None;
                }
                Poll::Ready(Err(failure)) => return Poll::Ready(Err(failure)),
                Poll::Pending => {}
            }
        }
        if this.steps.iter().any(Option::is_some) {
            return Poll::Pending;
        }
        Poll::Ready(Ok(this.done.iter_mut().filter_map(Option::take).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_wait_longer_each_time_up_to_the_max() {
        let policy = Retry::exponential(4).initial(secs(1)).max(secs(3)).non_retryable(&["card_declined"]).0;
        let retry = Failure::retry("down");
        assert_eq!(next_attempt(&policy, 1, &retry, 0), Some(1_000_000));
        assert_eq!(next_attempt(&policy, 2, &retry, 0), Some(2_000_000));
        assert_eq!(next_attempt(&policy, 3, &retry, 10), Some(3_000_010));
        assert_eq!(next_attempt(&policy, 4, &retry, 0), None, "4 attempts in all");
        assert_eq!(next_attempt(&policy, 1, &Failure::fatal("no"), 0), None);
        assert_eq!(next_attempt(&policy, 1, &Failure::retry("card_declined"), 0), None);
        assert_eq!(next_attempt(&policy, 1, &Failure::retry("card_declined: expired"), 0), None);
        assert_eq!(next_attempt(&policy, 1, &Failure::retry("card_declined_soon"), 0), Some(1_000_000));
        let wide = Retry::exponential(100).initial(secs(1)).max(days(1)).0;
        assert_eq!(next_attempt(&wide, 99, &retry, 0), Some(86_400_000_000), "no overflow");
    }
}
