//! The run engine on one node: a `Run` actor on an `EmbeddedDriver`, its jobs run by hand, a manual clock and a
//! manual wall clock that move together, and its journal on one replica in memory (R=1). A new owner of a run is a
//! new actor at the next lease epoch over the same replica; a run that stopped itself gets one at its next call, as
//! a cluster starts the key again. No waker runs here: a stopped run's timer fires at its next call. What a run sends
//! to queues and other runs goes to a `Fake` that keeps it and answers as told. Every test ends with the checker over
//! that replica.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use axon::files::{FileFuture, LogConfig, MemoryFiles};
use axon::{
    ActorRef, Clock, EmbeddedDriver, JobExecutor, LifecycleHandle, MailboxConfig, MailboxLimits, ManualClock,
    ManualJobs,
};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use workflows::check;
use workflows::journal::{Entry, LocalJournal, Status, replica_config};
use workflows::queue::Task;
use workflows::run::actor::{MAX_JOURNAL, Peers};
use workflows::run::{Caller, Code, Env, Run, RunAnswer, RunCall, RunMsg, RunView};
use workflows::workflow::{Activity, ActivityCx, App, Ctx, Failure, Level, Retry, secs};

/// The wall clock when each test begins, in µs.
const T0: u64 = 1_760_000_000_000_000;

fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}

/// A `MemoryFiles` call's result: ready at once.
fn ready<T>(mut f: FileFuture<T>) -> T {
    match poll(f.as_mut()) {
        Poll::Ready(out) => out.expect("files in memory"),
        Poll::Pending => panic!("files in memory waited"),
    }
}

fn log_config() -> LogConfig {
    // Small segments, not preallocated: a test's journal is a few KiB.
    LogConfig { segment_bytes: 2 << 20, preallocate: false, max_write_bytes: 2 << 20, ..replica_config() }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An activity's execution: run, step, attempt, activity.
type Ran = (String, u32, u32, &'static str);

static RAN: LazyLock<Mutex<Vec<Ran>>> = LazyLock::new(Mutex::default);
/// What `ctx.log` wrote: run, line.
static LOGS: LazyLock<Mutex<Vec<(String, String)>>> = LazyLock::new(Mutex::default);
/// A gate per run that `Gate` waits at.
static GATES: LazyLock<Mutex<BTreeMap<String, Arc<Semaphore>>>> = LazyLock::new(Mutex::default);

fn gate(run: &str) -> Arc<Semaphore> {
    lock(&GATES).entry(run.to_owned()).or_insert_with(|| Arc::new(Semaphore::new(0))).clone()
}

fn ran(cx: &ActivityCx, name: &'static str) {
    lock(&RAN).push((cx.run_id().to_owned(), cx.step(), cx.attempt(), name));
}

/// The executions of a run: step, attempt, activity.
fn executions(run: &str) -> Vec<(u32, u32, &'static str)> {
    lock(&RAN).iter().filter(|r| r.0 == run).map(|r| (r.1, r.2, r.3)).collect()
}

fn logs(run: &str) -> Vec<String> {
    lock(&LOGS).iter().filter(|l| l.0 == run).map(|l| l.1.clone()).collect()
}

#[derive(Serialize, Deserialize)]
struct Echo {
    v: Value,
}

impl Activity for Echo {
    const NAME: &'static str = "echo";
    type Output = Value;
    async fn run(self, cx: ActivityCx) -> Result<Value, Failure> {
        ran(&cx, Self::NAME);
        Ok(self.v)
    }
}

/// Echo under another name: a renamed step.
#[derive(Serialize, Deserialize)]
struct Other {
    v: Value,
}

impl Activity for Other {
    const NAME: &'static str = "other";
    type Output = Value;
    async fn run(self, cx: ActivityCx) -> Result<Value, Failure> {
        ran(&cx, Self::NAME);
        Ok(self.v)
    }
}

/// Fails its first `fails` attempts; then gives the attempt.
#[derive(Serialize, Deserialize)]
struct Flaky {
    fails: u32,
    fatal: bool,
}

impl Activity for Flaky {
    const NAME: &'static str = "flaky";
    type Output = u32;
    async fn run(self, cx: ActivityCx) -> Result<u32, Failure> {
        ran(&cx, Self::NAME);
        match (cx.attempt() <= self.fails, self.fatal) {
            (false, _) => Ok(cx.attempt()),
            (true, false) => Err(Failure::retry("down")),
            (true, true) => Err(Failure::fatal("no")),
        }
    }
}

/// Panics in its first `times` attempts.
#[derive(Serialize, Deserialize)]
struct Boom {
    times: u32,
}

impl Activity for Boom {
    const NAME: &'static str = "boom";
    type Output = String;
    async fn run(self, cx: ActivityCx) -> Result<String, Failure> {
        ran(&cx, Self::NAME);
        assert!(cx.attempt() > self.times, "boom");
        Ok("ok".into())
    }
}

/// Waits at its run's gate.
#[derive(Serialize, Deserialize)]
struct Gate {}

impl Activity for Gate {
    const NAME: &'static str = "gate";
    type Output = String;
    async fn run(self, cx: ActivityCx) -> Result<String, Failure> {
        ran(&cx, Self::NAME);
        let gate = gate(cx.run_id());
        gate.acquire().await.map_err(|e| Failure::retry(e.to_string()))?.forget();
        Ok("inside".into())
    }
}

/// Waits at its run's gate, then answers its idempotency key.
#[derive(Serialize, Deserialize)]
struct Key {}

impl Activity for Key {
    const NAME: &'static str = "key";
    type Output = String;
    async fn run(self, cx: ActivityCx) -> Result<String, Failure> {
        ran(&cx, Self::NAME);
        gate(cx.run_id()).acquire().await.map_err(|e| Failure::retry(e.to_string()))?.forget();
        Ok(cx.idempotency_key())
    }
}

#[derive(Serialize, Deserialize)]
struct Undo {}

impl Activity for Undo {
    const NAME: &'static str = "undo";
    type Output = ();
    async fn run(self, cx: ActivityCx) -> Result<(), Failure> {
        ran(&cx, Self::NAME);
        Ok(())
    }
}

async fn steps(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.log(Level::Info, "began");
    let a = ctx.activity(Echo { v: json!(1) }).await?;
    let (now, random) = (ctx.now(), ctx.random());
    ctx.sleep(secs(10)).await?;
    let go: Value = ctx.signal("go").await?;
    let b = ctx.activity(Echo { v: json!(2) }).await?;
    ctx.log(Level::Info, format!("ended, now {now}"));
    Ok(json!({ "a": a, "now": now, "random": random, "go": go, "b": b }))
}

async fn retries(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    let flaky = ctx.activity(Flaky { fails: 2, fatal: false }).retry(Retry::exponential(5).initial(secs(1))).await?;
    let boom = ctx.activity(Boom { times: 1 }).retry(Retry::exponential(3).initial(secs(1))).await?;
    let fatal = ctx.activity(Flaky { fails: 9, fatal: true }).retry(Retry::exponential(5)).await;
    let spent = ctx.activity(Flaky { fails: 9, fatal: false }).retry(Retry::exponential(2).initial(secs(1))).await;
    Ok(json!([flaky, boom, fatal.err().map(|e| e.to_string()), spent.err().map(|e| e.to_string())]))
}

async fn waits(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    let first: Option<Value> = ctx.signal("go").timeout(secs(5)).await?;
    let second: Option<Value> = ctx.signal("go").timeout(secs(5)).await?;
    Ok(json!([first, second]))
}

async fn held(ctx: Ctx, _: Value) -> Result<String, Failure> {
    ctx.activity(Gate {}).retry(Retry::exponential(3).initial(secs(1))).await
}

async fn cancels(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!("hold") }).await?;
    match ctx.signal::<Value>("go").await {
        Ok(v) => Ok(v),
        Err(e) => {
            ctx.cleanup(|c| c.activity(Gate {})).await?;
            // Not a cleanup step: it fails at once and is never scheduled.
            let after = ctx.activity(Echo { v: json!("after") }).await;
            ctx.cleanup(|c| c.activity(Undo {})).await?;
            Err(after.err().unwrap_or(e))
        }
    }
}

async fn slow(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!(1) }).await?;
    if let Err(e) = ctx.sleep(secs(3_600)).await {
        ctx.cleanup(|c| c.activity(Undo {})).await?;
        return Err(e);
    }
    Ok(json!("woke"))
}

async fn panics(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!(1) }).await?;
    panic!("the code is wrong");
}

/// `edit` as first written: two steps, a wait, a third step.
async fn edit(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!("a") }).await?;
    ctx.activity(Echo { v: json!("b") }).await?;
    let go: Value = ctx.signal("go").await?;
    ctx.activity(Echo { v: json!("c") }).await?;
    Ok(go)
}

/// On queue `q`, with a heartbeat every 5 s.
async fn remote(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!("far") })
        .queue("q")
        .heartbeat(secs(5))
        .retry(Retry::exponential(5).initial(secs(1)))
        .await
}

/// On queue `q`, started within 30 s or not at all.
async fn unstarted(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.activity(Echo { v: json!(1) }).queue("q").schedule_to_start(secs(30)).retry(Retry::exponential(5)).await
}

async fn parent(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    ctx.child("kid", json!(5)).await
}

async fn kid(_: Ctx, input: Value) -> Result<Value, Failure> {
    Ok(json!({ "kid": input }))
}

/// `n` steps.
async fn many(ctx: Ctx, n: u32) -> Result<u32, Failure> {
    for _ in 0..n {
        ctx.now();
    }
    Ok(n)
}

/// `n` sleeps at once.
async fn wide(ctx: Ctx, n: u32) -> Result<usize, Failure> {
    Ok(ctx.all((0..n).map(|_| ctx.sleep(secs(1)))).await?.len())
}

/// `n` steps of about 200 KiB of journal each.
async fn big(ctx: Ctx, n: u32) -> Result<u32, Failure> {
    for _ in 0..n {
        ctx.activity(Echo { v: json!("x".repeat(100 << 10)) }).await?;
    }
    Ok(n)
}

/// Fails its step for good on its first two attempts, then undoes the first step; a redrive gets it through, and
/// another activity takes the undo's step.
async fn redrives(ctx: Ctx, _: Value) -> Result<Value, Failure> {
    let a = ctx.activity(Echo { v: json!("a") }).await?;
    ctx.set_status(json!({ "at": "flaky" }));
    let retry = Retry::exponential(2).initial(secs(1));
    let f = match ctx.activity(Flaky { fails: 2, fatal: false }).retry(retry).await {
        Ok(f) => f,
        Err(e) => {
            ctx.cleanup(|c| c.activity(Undo {})).await?;
            return Err(e);
        }
    };
    ctx.set_status(json!({ "at": "end" }));
    let k = ctx.activity(Key {}).await?;
    Ok(json!([a, f, k]))
}

fn activities(app: App) -> App {
    app.activity::<Echo>()
        .activity::<Other>()
        .activity::<Flaky>()
        .activity::<Boom>()
        .activity::<Gate>()
        .activity::<Undo>()
        .activity::<Key>()
        .log_to(|run, _, line| lock(&LOGS).push((run.to_owned(), line.to_owned())))
}

fn app() -> App {
    activities(App::new())
        .workflow("steps", 1, steps)
        .workflow("retries", 1, retries)
        .workflow("waits", 1, waits)
        .workflow("held", 1, held)
        .workflow("cancels", 1, cancels)
        .workflow("slow", 1, slow)
        .workflow("panics", 1, panics)
        .workflow("edit", 1, edit)
        .workflow("remote", 1, remote)
        .workflow("unstarted", 1, unstarted)
        .workflow("parent", 1, parent)
        .workflow("kid", 1, kid)
        .workflow("many", 1, many)
        .workflow("wide", 1, wide)
        .workflow("big", 1, big)
        .workflow("redrives", 1, redrives)
}

/// What a run sent to queues and other runs; `down` makes every call to a run fail, `unavailable` every start
/// answer `unavailable`.
#[derive(Default)]
struct Fake {
    offers: Mutex<Vec<Task>>,
    calls: Mutex<Vec<(String, RunCall)>>,
    down: AtomicBool,
    unavailable: AtomicBool,
}

impl Peers for Fake {
    fn run(&self, id: &str, call: RunCall) -> BoxFuture<'static, Result<RunAnswer, String>> {
        lock(&self.calls).push((id.to_owned(), call.clone()));
        let answer = match call {
            _ if self.down.load(Ordering::SeqCst) => Err("down".to_owned()),
            RunCall::Start { .. } if self.unavailable.load(Ordering::SeqCst) => {
                Ok(RunAnswer::Refused { code: Code::Unavailable, message: "no majority".into() })
            }
            RunCall::Start { .. } => Ok(RunAnswer::Started { version: 1, created: true }),
            _ => Ok(RunAnswer::Accepted),
        };
        Box::pin(async move { answer })
    }

    fn offer(&self, task: Task) -> BoxFuture<'static, Result<(), String>> {
        lock(&self.offers).push(task);
        Box::pin(async { Ok(()) })
    }
}

impl Fake {
    /// Each offer: step, attempt, round.
    fn offers(&self) -> Vec<(u32, u32, u32)> {
        lock(&self.offers).iter().map(|t| (t.step, t.attempt, t.round)).collect()
    }

    fn calls(&self) -> Vec<(String, RunCall)> {
        lock(&self.calls).clone()
    }
}

struct Owner {
    driver: EmbeddedDriver<Run>,
    actor: ActorRef<RunMsg>,
    lifecycle: LifecycleHandle,
    _jobs: ManualJobs,
}

struct Rig {
    id: String,
    files: MemoryFiles,
    journal: LocalJournal,
    clock: ManualClock,
    wall: Arc<AtomicU64>,
    lease: u64,
    app: App,
    fake: Arc<Fake>,
    owner: Option<Owner>,
}

impl Rig {
    fn new(id: &str) -> Rig {
        let files = MemoryFiles::new();
        let journal = ready(LocalJournal::open(Arc::new(files.clone()), log_config()));
        let mut rig = Rig {
            id: id.to_owned(),
            files,
            journal,
            clock: Clock::manual(),
            wall: Arc::new(AtomicU64::new(T0)),
            lease: 0,
            app: app(),
            fake: Arc::default(),
            owner: None,
        };
        rig.own(app());
        rig
    }

    /// A new owner of the run, at the next lease epoch, running `app`; the last one is gone without a word.
    fn own(&mut self, app: App) {
        self.owner = None;
        self.lease += 1;
        self.app = app.clone();
        let wall = self.wall.clone();
        let mut env = Env::new(app, Arc::new(move || wall.load(Ordering::SeqCst)), 1);
        env.peers = self.fake.clone();
        env.node = Arc::from("e1");
        let env = Arc::new(env);
        let run = Run::new(env, Arc::new(self.journal.clone()), self.id.clone(), self.lease);
        let (executor, jobs) = JobExecutor::manual(64);
        let limits = MailboxLimits { timers: 5, jobs: 64, ..MailboxLimits::default() };
        let (driver, spawn) =
            EmbeddedDriver::with_jobs(run, MailboxConfig::from(limits), self.clock.clock(), executor).expect("spawns");
        self.owner = Some(Owner { driver, actor: spawn.address, lifecycle: spawn.lifecycle, _jobs: jobs });
    }

    /// The run stopped itself: it has nothing to do, it found no run, or the run ended.
    fn stopped(&self) -> bool {
        self.owner.as_ref().is_none_or(|owner| owner.lifecycle.exit().is_some())
    }

    fn idle(&mut self) {
        let owner = self.owner.as_mut().expect("an owner");
        owner.driver.run_until_idle(10_000).expect("the run settles");
    }

    /// Sends `call` until the run takes it, as `call_idempotent` does: a stopped run is started again.
    fn call(&mut self, call: RunCall) -> RunAnswer {
        for _ in 0..4 {
            if self.stopped() {
                self.own(self.app.clone());
            }
            let owner = self.owner.as_mut().expect("an owner");
            let mut answer = pin!(owner.actor.request(None, |reply| RunMsg::Call(call.clone(), Caller::Local(reply))));
            for _ in 0..100 {
                match poll(answer.as_mut()) {
                    Poll::Ready(Ok(RunAnswer::Refused { code: Code::Busy, .. }) | Err(_)) => break,
                    Poll::Ready(Ok(answer)) => return answer,
                    Poll::Pending => drop(owner.driver.run_until_idle(10_000).expect("the run settles")),
                }
            }
        }
        panic!("no answer");
    }

    /// Both clocks on by `by`, and the run handles what came due.
    fn advance(&mut self, by: Duration) {
        self.clock.advance(by);
        self.wall.fetch_add(by.as_micros() as u64, Ordering::SeqCst);
        self.idle();
    }

    fn start(&mut self, workflow: &str, run_timeout_ms: Option<u64>) -> RunAnswer {
        self.start_with(workflow, Bytes::from_static(b"{}"), None, run_timeout_ms)
    }

    fn start_with(
        &mut self,
        workflow: &str,
        input: Bytes,
        parent: Option<&str>,
        run_timeout_ms: Option<u64>,
    ) -> RunAnswer {
        let call = RunCall::Start {
            workflow: workflow.into(),
            version: None,
            input,
            tags: Vec::new(),
            parent: parent.map(str::to_owned),
            run_timeout_ms,
        };
        let answer = self.call(call);
        self.idle();
        answer
    }

    fn signal(&mut self, signal_id: &str, payload: Value) -> RunAnswer {
        let payload = Bytes::from(payload.to_string());
        let answer = self.call(RunCall::Signal { signal_id: signal_id.into(), name: "go".into(), payload });
        self.idle();
        answer
    }

    fn get(&mut self) -> RunView {
        match self.call(RunCall::Get) {
            RunAnswer::Run(view) => *view,
            other => panic!("a get answered {other:?}"),
        }
    }

    fn output(&mut self) -> Value {
        let view = self.get();
        assert_eq!(view.status, Status::Completed, "{view:#?}");
        serde_json::from_str(view.output.as_deref().unwrap_or("null")).expect("JSON")
    }

    /// The run's journal as the checker reads it from the replica, once it holds no broken invariant.
    fn checked(&self) -> Vec<Entry> {
        let dump = ready(check::dump(Arc::new(self.files.clone()), log_config()));
        let report = check::check(&[dump], &[], None);
        report.assert_ok();
        let journal = report.journals.get(self.id.as_bytes()).cloned().unwrap_or_default();
        journal.iter().map(|body| Entry::decode(body).expect("decodes")).collect()
    }
}

/// The journal, one short line an entry, `Own` left out.
fn lines(journal: &[Entry]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| {
            Some(match e {
                Entry::Own(_) => return None,
                Entry::Started { workflow, .. } => format!("started {workflow}"),
                Entry::Value { step, .. } => format!("{step} value"),
                Entry::ActivityScheduled { step, name, .. } => format!("{step} scheduled {name}"),
                Entry::ActivityFailed { step, attempt, retry_at_us, .. } => {
                    format!("{step} failed {attempt}{}", if retry_at_us.is_some() { " retry" } else { "" })
                }
                Entry::ActivityDone { step, attempt, .. } => format!("{step} done {attempt}"),
                Entry::TimerStarted { step, .. } => format!("{step} timer"),
                Entry::TimerFired { step, .. } => format!("{step} fired"),
                Entry::SignalReceived { signal_id, .. } => format!("signal {signal_id}"),
                Entry::SignalConsumed { step, signal_id, .. } => format!("{step} took {signal_id}"),
                Entry::CancelRequested { .. } => "cancel".into(),
                Entry::Parked { .. } => "parked".into(),
                Entry::Completed { .. } => "completed".into(),
                Entry::Failed { timeout, .. } => format!("failed timeout={timeout}"),
                Entry::Cancelled => "cancelled".into(),
                Entry::Terminated { .. } => "terminated".into(),
                other => format!("{other:?}"),
            })
        })
        .collect()
}

fn refused_with(answer: RunAnswer) -> Code {
    match answer {
        RunAnswer::Refused { code, .. } => code,
        other => panic!("not refused: {other:?}"),
    }
}

#[test]
fn steps_run_in_order_and_a_signal_sent_early_waits_for_its_step() {
    let mut r = Rig::new("steps-1");
    assert_eq!(r.start("steps", None), RunAnswer::Started { version: 1, created: true });
    let view = r.get();
    assert_eq!(view.status, Status::Running);
    assert_eq!(view.due_us, Some(T0 + 10_000_000), "the sleep's end");
    // Before the wait begins: the run keeps it, once.
    assert_eq!(r.signal("g1", json!({ "ok": true })), RunAnswer::Signalled { duplicate: false });
    assert_eq!(r.signal("g1", json!("again")), RunAnswer::Signalled { duplicate: true });
    r.advance(Duration::from_millis(9_999));
    assert_eq!(r.get().status, Status::Running, "no timer fires before its time");
    r.advance(Duration::from_millis(1));
    let out = r.output();
    assert_eq!(out, json!({ "a": 1, "now": T0, "random": out["random"], "go": { "ok": true }, "b": 2 }));
    assert_eq!(executions("steps-1"), [(1, 1, "echo"), (6, 1, "echo")]);
    assert_eq!(
        lines(&r.checked()),
        [
            "started steps",
            "1 scheduled echo",
            "1 done 1",
            "2 value",
            "3 value",
            "4 timer",
            "signal g1",
            "4 fired",
            "5 took g1",
            "6 scheduled echo",
            "6 done 1",
            "completed"
        ]
    );
    // The same start again is the same run; another input under the id is refused.
    assert_eq!(r.start("steps", None), RunAnswer::Started { version: 1, created: false });
    assert_eq!(refused_with(r.start("retries", None)), Code::Exists);
}

#[test]
fn a_new_owner_replays_with_the_same_values_and_logs_nothing_again() {
    let mut r = Rig::new("move-1");
    r.start("steps", None);
    let before = r.get().steps;
    assert_eq!(logs("move-1"), ["began"]);
    // The owner is gone; the next one opens the run an hour later.
    r.own(app());
    r.wall.fetch_add(3_600_000_000, Ordering::SeqCst);
    assert_eq!(r.get().steps[..3], before[..3], "the replay keeps each value");
    assert_eq!(logs("move-1"), ["began"], "ctx.log is silent on replay");
    r.signal("g1", json!(true));
    let out = r.output();
    assert_eq!((out["now"].as_u64(), &out["go"]), (Some(T0), &json!(true)));
    assert_eq!(
        serde_json::from_str::<Value>(before[2].output.as_deref().unwrap_or("")).ok(),
        Some(out["random"].clone())
    );
    assert_eq!(logs("move-1"), ["began".to_string(), format!("ended, now {T0}")]);
    // A third owner opens the ended run: nothing runs or logs again.
    let journal = lines(&r.checked());
    r.own(app());
    assert_eq!(r.output(), out);
    assert_eq!(logs("move-1").len(), 2);
    assert_eq!(lines(&r.checked()), journal);
    assert_eq!(executions("move-1"), [(1, 1, "echo"), (6, 1, "echo")]);
}

#[test]
fn retries_wait_out_the_policy_and_fatal_failures_end_the_step() {
    let mut r = Rig::new("retries-1");
    r.start("retries", None);
    assert_eq!(executions("retries-1"), [(1, 1, "flaky")]);
    r.advance(Duration::from_millis(999));
    assert_eq!(executions("retries-1").len(), 1, "no attempt before its time");
    r.advance(Duration::from_millis(1));
    assert_eq!(executions("retries-1"), [(1, 1, "flaky"), (1, 2, "flaky")]);
    r.advance(secs(2));
    for _ in 0..10 {
        if r.get().status != Status::Running {
            break;
        }
        r.advance(secs(1));
    }
    assert_eq!(r.output(), json!([3, "ok", "no", "down"]));
    assert_eq!(
        executions("retries-1"),
        [
            (1, 1, "flaky"),
            (1, 2, "flaky"),
            (1, 3, "flaky"),
            (2, 1, "boom"),
            (2, 2, "boom"),
            (3, 1, "flaky"),
            (4, 1, "flaky"),
            (4, 2, "flaky")
        ]
    );
    let journal = lines(&r.checked());
    assert!(journal.contains(&"2 failed 1 retry".to_string()), "a panic is a failure to try again: {journal:?}");
    assert!(journal.contains(&"3 failed 1".to_string()), "a fatal one is not: {journal:?}");
    assert!(journal.contains(&"4 failed 2".to_string()), "nor one past the attempts: {journal:?}");
}

#[test]
fn a_signal_and_its_timeout_at_once_give_the_step_one_outcome() {
    let mut r = Rig::new("race-1");
    r.start("waits", None);
    // The timeout comes due and a signal arrives before the run looks: whichever it takes first is the outcome.
    r.clock.advance(secs(5));
    r.wall.fetch_add(5_000_000, Ordering::SeqCst);
    assert_eq!(r.signal("s1", json!("hi")), RunAnswer::Signalled { duplicate: false });
    r.advance(secs(5));
    let out = r.output();
    assert!(out == json!(["hi", null]) || out == json!([null, "hi"]), "{out}");
    let journal = lines(&r.checked());
    assert_eq!(journal.iter().filter(|l| l.contains("took s1")).count(), 1, "{journal:?}");
}

#[test]
fn the_first_completion_of_a_step_wins_whatever_its_attempt() {
    let mut r = Rig::new("first-1");
    r.start("held", None);
    assert_eq!(executions("first-1"), [(1, 1, "gate")]);
    let complete = |attempt, result: Result<&str, &str>| RunCall::Complete {
        step: 1,
        attempt,
        result: result.map(|v| Bytes::from(v.to_owned())).map_err(Failure::retry),
    };
    assert_eq!(r.call(complete(1, Err("down"))), RunAnswer::Accepted);
    // A failure of an attempt that is not the latest is dropped; a late success of an old one wins.
    assert_eq!(r.call(complete(1, Err("again"))), RunAnswer::Accepted);
    assert_eq!(r.call(complete(1, Ok("\"late\""))), RunAnswer::Accepted);
    assert_eq!(refused_with(r.call(complete(2, Ok("\"second\"")))), Code::Finished);
    gate("first-1").add_permits(4);
    r.advance(secs(1));
    assert_eq!(r.output(), json!("late"));
    assert_eq!(lines(&r.checked()), ["started held", "1 scheduled gate", "1 failed 1 retry", "1 done 1", "completed"]);
}

#[test]
fn nothing_follows_the_end() {
    let mut r = Rig::new("end-1");
    r.start("waits", None);
    r.signal("s1", json!(1));
    r.signal("s2", json!(2));
    assert_eq!(r.output(), json!([1, 2]));
    let journal = r.checked();
    assert_eq!(refused_with(r.signal("s3", json!(3))), Code::Finished);
    assert_eq!(refused_with(r.call(RunCall::Cancel { reason: "late".into() })), Code::Finished);
    assert_eq!(refused_with(r.call(RunCall::Terminate { reason: "late".into() })), Code::Finished);
    let complete = RunCall::Complete { step: 1, attempt: 1, result: Ok(Bytes::from_static(b"1")) };
    assert_eq!(refused_with(r.call(complete)), Code::Finished);
    assert_eq!(r.call(RunCall::Wake), RunAnswer::Accepted);
    r.advance(secs(3_600));
    assert_eq!(r.checked(), journal);
}

#[test]
fn a_cancel_runs_only_cleanup_steps_also_after_a_move() {
    let mut r = Rig::new("cancel-1");
    r.start("cancels", None);
    assert_eq!(r.call(RunCall::Cancel { reason: "user".into() }), RunAnswer::Accepted);
    r.idle();
    assert_eq!(executions("cancel-1"), [(1, 1, "echo"), (3, 1, "gate")]);
    // The owner goes while the cleanup step runs; the next one replays the cancel and runs that step again.
    r.own(app());
    assert_eq!(r.get().status, Status::Running);
    gate("cancel-1").add_permits(4);
    r.advance(secs(1));
    assert_eq!(r.get().status, Status::Cancelled);
    assert_eq!(executions("cancel-1"), [(1, 1, "echo"), (3, 1, "gate"), (3, 1, "gate"), (5, 1, "undo")]);
    let journal = lines(&r.checked());
    let cancel = journal.iter().position(|l| l == "cancel").expect("a cancel");
    let after: Vec<&String> = journal[cancel..].iter().filter(|l| l.contains("scheduled")).collect();
    assert_eq!(after, ["3 scheduled gate", "5 scheduled undo"], "{journal:?}");
    r.own(app());
    assert_eq!(r.get().status, Status::Cancelled);
    assert_eq!(lines(&r.checked()), journal);
}

#[test]
fn a_terminate_ends_the_run_at_once_with_no_cleanup() {
    let mut r = Rig::new("term-1");
    r.start("cancels", None);
    assert_eq!(r.call(RunCall::Terminate { reason: "stop".into() }), RunAnswer::Accepted);
    r.advance(secs(1));
    let view = r.get();
    assert_eq!((view.status, view.error.as_deref()), (Status::Terminated, Some("stop")));
    assert_eq!(lines(&r.checked()).last().map(String::as_str), Some("terminated"));
    assert_eq!(executions("term-1"), [(1, 1, "echo")]);
}

#[test]
fn a_run_timeout_fails_the_run_with_no_cleanup() {
    let mut r = Rig::new("timeout-1");
    r.start("slow", Some(60_000));
    r.advance(Duration::from_millis(59_999));
    assert_eq!(r.get().status, Status::Running);
    r.advance(Duration::from_millis(1));
    let view = r.get();
    assert_eq!((view.status, view.error.as_deref()), (Status::Failed, Some("the run timed out")));
    let journal = lines(&r.checked());
    assert_eq!(
        journal,
        ["started slow", "0 timer", "1 scheduled echo", "1 done 1", "2 timer", "0 fired", "failed timeout=true"]
    );
    r.own(app());
    assert_eq!(r.get().status, Status::Failed);
    r.advance(secs(3_600));
    assert_eq!(lines(&r.checked()), journal);
    assert_eq!(executions("timeout-1"), [(1, 1, "echo")]);
}

/// Runs `edit` to its wait under the first code, then opens it under `changed` and returns the view.
fn edited(id: &str, changed: App) -> (Rig, RunView) {
    let mut r = Rig::new(id);
    r.start("edit", None);
    r.own(changed);
    let view = r.get();
    (r, view)
}

#[test]
fn a_changed_step_a_missing_version_or_a_panic_parks_the_run() {
    let moved = activities(App::new()).workflow("edit", 1, |ctx: Ctx, _: Value| async move {
        let go: Value = ctx.signal("go").await?;
        ctx.activity(Echo { v: json!("a") }).await?;
        ctx.activity(Echo { v: json!("b") }).await?;
        Ok(go)
    });
    let renamed = activities(App::new()).workflow("edit", 1, |ctx: Ctx, _: Value| async move {
        ctx.activity(Other { v: json!("a") }).await?;
        ctx.activity(Echo { v: json!("b") }).await?;
        ctx.signal::<Value>("go").await
    });
    let removed = activities(App::new()).workflow("edit", 1, |ctx: Ctx, _: Value| async move {
        ctx.activity(Echo { v: json!("a") }).await?;
        ctx.signal::<Value>("go").await
    });
    let cut = activities(App::new())
        .workflow("edit", 1, |ctx: Ctx, _: Value| async move { ctx.activity(Echo { v: json!("a") }).await });
    let missing = activities(App::new()).workflow("edit", 2, edit);
    let panicked = activities(App::new()).workflow("edit", 1, |ctx: Ctx, _: Value| async move {
        ctx.activity(Echo { v: json!("a") }).await?;
        if !ctx.is_replaying() {
            return Ok(Value::Null);
        }
        panic!("the code is wrong");
    });
    let cases = [
        ("moved", moved, "step 1: journal has `echo`, code asks for `signal go`"),
        ("renamed", renamed, "step 1: journal has `echo`, code asks for `other`"),
        ("removed", removed, "step 2: journal has `echo`, code asks for `signal go`"),
        ("cut", cut, "step 2: journal has `echo`, code ends before it"),
        ("missing", missing, "version 1 of edit is not in this build"),
        ("panicked", panicked, "panic at step 2: the code is wrong"),
    ];
    for (name, changed, why) in cases {
        let id = format!("park-{name}");
        let (mut r, view) = edited(&id, changed);
        assert_eq!((view.status, view.parked.as_deref()), (Status::Parked, Some(why)), "{name}");
        // A signal is kept, and the run goes no further.
        assert_eq!(r.signal("g1", json!(1)), RunAnswer::Signalled { duplicate: false }, "{name}");
        r.advance(secs(60));
        assert_eq!(r.get().status, Status::Parked, "{name}");
        let journal = lines(&r.checked());
        let parked = journal.iter().position(|l| l == "parked").expect("parked in the journal");
        assert_eq!(journal[parked..], ["parked", "signal g1"], "{name}");
        assert_eq!(executions(&id), [(1, 1, "echo"), (2, 1, "echo")], "{name}");
    }
    // A panic in a new run's code parks it too.
    let mut r = Rig::new("park-fresh");
    r.start("panics", None);
    let view = r.get();
    assert_eq!((view.status, view.parked.as_deref()), (Status::Parked, Some("panic at step 2: the code is wrong")));
    r.checked();
}

#[test]
fn a_parked_run_fires_no_timer_and_starts_no_attempt() {
    let code = |changed: bool| {
        activities(App::new()).workflow("waiting", 1, move |ctx: Ctx, _: Value| async move {
            if changed {
                return ctx.activity(Echo { v: json!(1) }).await;
            }
            ctx.sleep(secs(10)).await?;
            ctx.activity(Flaky { fails: 9, fatal: false }).retry(Retry::exponential(5).initial(secs(1))).await?;
            Ok(Value::Null)
        })
    };
    // Parked once on its sleep, once on its activity's next attempt.
    for (id, by) in [("parked-sleep", secs(1)), ("parked-retry", secs(11))] {
        let mut r = Rig::new(id);
        r.own(code(false));
        r.start("waiting", None);
        r.advance(by);
        r.own(code(true));
        let view = r.get();
        assert_eq!(view.status, Status::Parked, "{id}");
        assert_eq!(view.due_us, None, "{id}");
        r.advance(secs(60));
        let journal = lines(&r.checked());
        assert_eq!(journal.last().map(String::as_str), Some("parked"), "{id}: {journal:?}");
        let attempts = executions(id).len();
        assert_eq!(attempts, usize::from(id == "parked-retry"), "{id}");
    }
}

/// Code B makes the same steps as code A in the same order, and hands them to `ctx.all` the other way round. Steps
/// are numbered when they are made, so a run started on A goes on under B; numbered at their first poll
/// (`RUN_MUTATION=number_at_poll`), B asks for `other` where the journal has `echo`, and the run parks.
#[test]
fn all_order() {
    fn code(swap: bool) -> App {
        activities(App::new()).workflow("fan", 1, move |ctx: Ctx, _: Value| async move {
            let a = ctx.activity(Echo { v: json!("a") });
            let b = ctx.activity(Other { v: json!("b") });
            let both = if swap { ctx.all([b, a]).await? } else { ctx.all([a, b]).await? };
            let go: Value = ctx.signal("go").await?;
            Ok(json!([both, go]))
        })
    }
    let mut r = Rig::new("all-order");
    r.own(code(false));
    r.start("fan", None);
    r.own(code(true));
    r.signal("g1", json!(1));
    let view = r.get();
    assert_eq!(view.parked, None, "{view:#?}");
    assert_eq!(r.output(), json!([["b", "a"], 1]));
    r.checked();
}

#[test]
fn an_unknown_run_or_a_start_that_cannot_run_writes_nothing() {
    let mut r = Rig::new("unknown-1");
    assert_eq!(refused_with(r.call(RunCall::Get)), Code::NotFound);
    r.own(app());
    assert_eq!(refused_with(r.signal("s1", json!(1))), Code::NotFound);
    r.own(app());
    assert_eq!(refused_with(r.start("nope", None)), Code::Invalid);
    r.own(app());
    let call = RunCall::Start {
        workflow: "steps".into(),
        version: Some(9),
        input: Bytes::from_static(b"{}"),
        tags: Vec::new(),
        parent: None,
        run_timeout_ms: None,
    };
    assert_eq!(refused_with(r.call(call)), Code::VersionMissing);
    let dump = ready(check::dump(Arc::new(r.files.clone()), log_config()));
    assert!(dump.runs.is_empty(), "{dump:?}");
}

/// The `Own` entries of the journal: one an open.
fn opens(journal: &[Entry]) -> usize {
    journal.iter().filter(|e| matches!(e, Entry::Own(_))).count()
}

#[test]
fn a_run_with_nothing_to_do_stops_and_its_next_call_opens_it() {
    let mut r = Rig::new("rest-1");
    r.start("steps", None);
    r.advance(secs(2));
    assert!(!r.stopped(), "a sleep due within KEEP_WARM keeps the run");
    r.advance(secs(8));
    assert!(!r.stopped(), "a wait for a signal with no timer keeps it the quiet second");
    r.advance(secs(1));
    assert!(r.stopped(), "then it stops");
    let journal = r.checked();
    assert_eq!(lines(&journal).last().map(String::as_str), Some("Idle"), "{:?}", lines(&journal));
    // The signal opens the run again, at a new epoch, and it goes on.
    assert_eq!(r.signal("g1", json!(1)), RunAnswer::Signalled { duplicate: false });
    assert_eq!(r.output()["go"], json!(1));
    let journal = r.checked();
    assert_eq!(opens(&journal), 2);
    r.advance(secs(11));
    assert!(r.stopped(), "an ended run stops once the reads end (WATCHED)");
    assert_eq!(r.checked(), journal, "and writes nothing to stop");

    // A long sleep: the run journals `Idle`, stops, and its timer fires at the next call (a waker's `Wake`).
    let mut r = Rig::new("rest-2");
    r.start("slow", None);
    r.advance(secs(2));
    assert!(r.stopped());
    assert_eq!(lines(&r.checked()), ["started slow", "1 scheduled echo", "1 done 1", "2 timer", "Idle"]);
    r.advance(secs(3_600));
    assert_eq!(r.call(RunCall::Wake), RunAnswer::Accepted);
    assert_eq!(r.output(), json!("woke"));
    assert_eq!(executions("rest-2"), [(1, 1, "echo")], "the replay runs nothing again");
}

/// A run that a console reads every 2 s stays open, running or ended: one that stopped between reads would open again
/// at each, and journal an owner each time.
#[test]
fn a_run_read_often_stays_open_and_stops_once_the_reads_end() {
    let mut r = Rig::new("watched-1");
    r.start("steps", None);
    r.advance(secs(10));
    let reads = |r: &mut Rig, status: Status| {
        for _ in 0..10 {
            assert_eq!(r.get().status, status);
            r.advance(secs(2));
            assert!(!r.stopped(), "a read in the last 10 s keeps the run");
        }
        r.advance(secs(11));
        assert!(r.stopped(), "then it stops");
    };
    reads(&mut r, Status::Running);
    let journal = r.checked();
    assert_eq!(opens(&journal), 1, "{:?}", lines(&journal));
    assert_eq!(lines(&journal).last().map(String::as_str), Some("Idle"));
    // An ended run opens for a read too: the journal holds what the view shows.
    r.signal("g1", json!(1));
    assert_eq!(r.output()["go"], json!(1));
    reads(&mut r, Status::Completed);
    assert_eq!(opens(&r.checked()), 2);
}

/// An attempt in flight keeps the run open however long it takes: a run that stopped would leave it unjournaled
/// (`RUN_MUTATION=passivate_busy`).
#[test]
fn an_attempt_in_flight_keeps_the_run() {
    let mut r = Rig::new("busy-1");
    r.start("held", None);
    r.advance(secs(60));
    assert!(!r.stopped(), "the gate holds the attempt");
    gate("busy-1").add_permits(1);
    r.idle();
    assert_eq!(r.output(), json!("inside"));
    assert_eq!(executions("busy-1"), [(1, 1, "gate")]);
    assert!(!lines(&r.checked()).contains(&"Idle".to_string()));
}

#[test]
fn a_run_parked_on_a_missing_version_resumes_on_a_build_that_has_it() {
    let missing = activities(App::new()).workflow("edit", 2, edit);
    let (mut r, view) = edited("resume-1", missing.clone());
    assert_eq!(view.parked.as_deref(), Some("version 1 of edit is not in this build"));
    r.advance(secs(11));
    assert!(r.stopped(), "a parked run stops once the reads end");
    let journal = r.checked();
    // A waker's or reconciler's wake on a build still without the version: answered from the probe, nothing opened.
    r.own(missing);
    assert_eq!(r.call(RunCall::Wake), RunAnswer::Accepted);
    r.idle();
    assert!(r.stopped());
    assert_eq!(r.checked(), journal);
    // A build with it opens the run where it parked.
    r.own(app());
    assert_eq!(r.get().status, Status::Running);
    r.signal("g1", json!("go"));
    assert_eq!(r.output(), json!("go"));
    let journal = lines(&r.checked());
    assert!(journal.contains(&"Resumed".to_string()), "{journal:?}");
    assert_eq!(executions("resume-1"), [(1, 1, "echo"), (2, 1, "echo"), (4, 1, "echo")]);
}

/// Eager starts a run on a new node with no caller: one that does not exist or has ended writes nothing, not even an
/// `Own`, and stops; one that goes on opens and runs. A waker's wake opens an ended run.
#[test]
fn a_run_started_with_no_caller_opens_only_if_it_goes_on() {
    let mut r = Rig::new("eager-1");
    r.idle();
    // A caller's request comes after the spawn it asked for: the probe answers it for a while.
    assert!(!r.stopped(), "the probe is kept");
    assert_eq!(refused_with(r.call(RunCall::Get)), Code::NotFound);
    r.advance(secs(2));
    assert!(r.stopped(), "no run");
    let dump = ready(check::dump(Arc::new(r.files.clone()), log_config()));
    assert!(dump.runs.is_empty(), "{dump:?}");

    r.start("waits", None);
    r.signal("s1", json!(1));
    r.signal("s2", json!(2));
    assert_eq!(r.output(), json!([1, 2]));
    let journal = r.checked();
    r.own(app());
    r.idle();
    r.advance(secs(2));
    assert!(r.stopped(), "an ended run");
    assert_eq!(r.checked(), journal);
    // An action is answered from the index's summary; a start seals at once and opens the run, and so does a
    // signal, which may be one the run took, sent again.
    assert_eq!(refused_with(r.call(RunCall::Cancel { reason: "late".into() })), Code::Finished);
    assert_eq!(r.checked(), journal);
    // A wake comes from a node whose index shows the run going on, a replica behind the others: it opens the ended
    // run, and its owner's `Own` takes the summary to each replica's index.
    let segments =
        |r: &Rig| ready(check::dump(Arc::new(r.files.clone()), log_config())).runs[&b"eager-1"[..]].segments.len();
    let before = segments(&r);
    assert_eq!(r.call(RunCall::Wake), RunAnswer::Accepted);
    r.idle();
    assert_eq!(segments(&r), before + 1, "an owner wrote its Own");
    assert_eq!(lines(&r.checked()), lines(&journal));
    assert_eq!(r.signal("s2", json!(2)), RunAnswer::Signalled { duplicate: true });
    assert_eq!(refused_with(r.signal("s3", json!(3))), Code::Finished);
    assert_eq!(r.start("waits", None), RunAnswer::Started { version: 1, created: false });

    let mut r = Rig::new("eager-2");
    r.start("held", None);
    r.own(app());
    r.idle();
    assert!(!r.stopped(), "a run that goes on opens");
    assert_eq!(executions("eager-2"), [(1, 1, "gate"), (1, 1, "gate")], "and starts its attempt again");
    gate("eager-2").add_permits(2);
    r.idle();
    assert_eq!(r.output(), json!("inside"));
    r.checked();
}

fn json_bytes(v: Value) -> Bytes {
    Bytes::from(v.to_string())
}

#[test]
fn an_attempt_on_a_queue_is_offered_until_a_worker_starts_it_and_fails_when_its_worker_goes() {
    let mut r = Rig::new("remote-1");
    r.start("remote", None);
    assert_eq!(r.fake.offers(), [(1, 1, 1)]);
    // No worker said it started: the offer is made again after 10 s.
    r.advance(secs(9));
    assert_eq!(r.fake.offers().len(), 1);
    r.advance(secs(1));
    assert_eq!(r.fake.offers(), [(1, 1, 1), (1, 1, 1)]);
    let started = |attempt, worker: &str| RunCall::Started { step: 1, attempt, worker: worker.into() };
    assert_eq!(r.call(started(1, "w1")), RunAnswer::Accepted);
    assert_eq!(r.call(started(1, "w1")), RunAnswer::Accepted, "the same worker again");
    assert_eq!(refused_with(r.call(started(1, "w2"))), Code::Exists);
    assert_eq!(refused_with(r.call(started(2, "w2"))), Code::Finished, "not the attempt the run gave out");
    let beat = |attempt| RunCall::Heartbeat { step: 1, attempt };
    r.advance(secs(4));
    assert_eq!(r.call(beat(1)), RunAnswer::Beat { cancel: false });
    // No heartbeat for 5 s: the attempt fails, and the next is offered after its backoff.
    r.advance(secs(5));
    r.advance(secs(1));
    assert_eq!(r.fake.offers().last(), Some(&(1, 2, 1)));
    assert_eq!(r.call(beat(1)), RunAnswer::Beat { cancel: true }, "the old attempt is not wanted");
    // Given back unstarted: offered again as the next round, and not an attempt.
    let released = RunCall::Released { step: 1, attempt: 2, round: 1, worker: "w9".into() };
    assert_eq!(r.call(released.clone()), RunAnswer::Accepted);
    assert_eq!(r.fake.offers().last(), Some(&(1, 2, 2)));
    assert_eq!(r.call(released), RunAnswer::Accepted, "a round given back twice is offered once");
    assert_eq!(r.fake.offers().iter().filter(|o| **o == (1, 2, 2)).count(), 1);
    assert_eq!(r.call(started(2, "w2")), RunAnswer::Accepted);
    // Its worker left the cluster: the attempt fails, and the next is offered.
    assert_eq!(r.call(RunCall::WorkerLeft { node: "w1".into() }), RunAnswer::Accepted);
    assert_eq!(r.fake.offers().last(), Some(&(1, 2, 2)), "another worker's leave changes nothing");
    assert_eq!(r.call(RunCall::WorkerLeft { node: "w2".into() }), RunAnswer::Accepted);
    // The second backoff is twice the first.
    r.advance(secs(2));
    assert_eq!(r.fake.offers().last(), Some(&(1, 3, 1)));
    let complete = RunCall::Complete { step: 1, attempt: 3, result: Ok(json_bytes(json!("far"))) };
    assert_eq!(r.call(complete), RunAnswer::Accepted);
    r.idle();
    assert_eq!(r.output(), json!("far"));
    let journal = lines(&r.checked());
    let outcomes: Vec<&String> = journal.iter().filter(|l| l.contains("failed") || l.contains("done")).collect();
    assert_eq!(outcomes, ["1 failed 1 retry", "1 failed 2 retry", "1 done 3"]);
    let errors: Vec<String> = (r.checked().into_iter())
        .filter_map(|e| match e {
            Entry::ActivityFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect();
    assert_eq!(errors, ["timeout: heartbeat", "worker w2 left"]);
}

#[test]
fn schedule_to_start_fails_the_step_once_and_a_start_in_time_stops_it() {
    let mut r = Rig::new("late-1");
    r.start("unstarted", None);
    r.advance(secs(29));
    assert_eq!(r.get().status, Status::Running);
    r.advance(secs(1));
    let view = r.get();
    assert_eq!(view.status, Status::Failed, "{view:#?}");
    assert!(view.error.as_deref().is_some_and(|e| e.contains("schedule_to_start")), "{view:#?}");
    assert_eq!(view.steps[0].status, "timed_out");
    assert!(r.fake.offers().iter().all(|o| o.1 == 1), "not tried again: {:?}", r.fake.offers());
    // A worker that says it started takes the timer away.
    let mut r = Rig::new("late-2");
    r.start("unstarted", None);
    assert_eq!(r.call(RunCall::Started { step: 1, attempt: 1, worker: "w1".into() }), RunAnswer::Accepted);
    r.advance(secs(31));
    assert_eq!(r.get().status, Status::Running);
    let complete = RunCall::Complete { step: 1, attempt: 1, result: Ok(json_bytes(json!(1))) };
    assert_eq!(r.call(complete), RunAnswer::Accepted);
    r.idle();
    assert_eq!(r.output(), json!(1));
    let journal = lines(&r.checked());
    assert!(journal.contains(&"Picked { step: 1 }".to_owned()), "{journal:?}");
    assert!(!journal.contains(&"1 fired".to_owned()), "{journal:?}");
}

#[test]
fn a_parent_starts_its_child_and_takes_its_result_once() {
    let mut r = Rig::new("parent-1");
    r.start("parent", None);
    let calls = r.fake.calls();
    assert!(
        matches!(&calls[..], [(id, RunCall::Start { workflow, parent: Some(p), input, .. })]
            if id == "parent-1/1" && workflow == "kid" && p == "parent-1" && input.as_ref() == b"5"),
        "{calls:?}"
    );
    let done = RunCall::ChildDone { step: 1, result: Ok(json_bytes(json!({ "kid": 5 }))) };
    assert_eq!(r.call(done.clone()), RunAnswer::Accepted);
    r.idle();
    // Again, as a child that restarted sends it: the parent ended, and the child takes `finished` as told.
    assert_eq!(refused_with(r.call(done)), Code::Finished);
    assert_eq!(r.output(), json!({ "kid": 5 }));
    let journal = lines(&r.checked());
    assert_eq!(journal.iter().filter(|l| l.starts_with("ChildDone")).count(), 1, "{journal:?}");
    assert_eq!(refused_with(r.call(RunCall::ChildDone { step: 7, result: Err("x".into()) })), Code::Finished);
}

#[test]
fn a_child_start_answered_unavailable_is_sent_again_not_taken_as_failed() {
    let mut r = Rig::new("parent-4");
    r.fake.unavailable.store(true, Ordering::SeqCst);
    r.start("parent", None);
    r.advance(secs(2));
    // The child's journal may hold its start: the step waits, and the start goes again.
    assert_eq!(r.get().status, Status::Running);
    let starts = |r: &Rig| r.fake.calls().iter().filter(|(_, c)| matches!(c, RunCall::Start { .. })).count();
    assert!(starts(&r) >= 2, "{:?}", r.fake.calls());
    r.fake.unavailable.store(false, Ordering::SeqCst);
    r.advance(secs(1));
    let sent = starts(&r);
    r.advance(secs(2));
    assert_eq!(starts(&r), sent, "a started child is not started again");
    let done = RunCall::ChildDone { step: 1, result: Ok(json_bytes(json!({ "kid": 5 }))) };
    assert_eq!(r.call(done), RunAnswer::Accepted);
    assert_eq!(r.output(), json!({ "kid": 5 }));
    r.checked();
}

#[test]
fn a_child_tells_its_parent_until_it_arrives_then_journals_that() {
    let mut r = Rig::new("parent-2/1");
    r.fake.down.store(true, Ordering::SeqCst);
    r.start_with("kid", json_bytes(json!(5)), Some("parent-2"), None);
    assert_eq!(r.get().status, Status::Completed);
    r.advance(secs(2));
    assert!(r.fake.calls().len() >= 2, "sent again: {:?}", r.fake.calls());
    assert!(!r.stopped(), "the run stays while its parent is not told");
    r.fake.down.store(false, Ordering::SeqCst);
    r.advance(secs(1));
    let calls = r.fake.calls();
    let (id, call) = calls.last().expect("a call");
    assert_eq!(id, "parent-2");
    assert_eq!(call, &RunCall::ChildDone { step: 1, result: Ok(json_bytes(json!({ "kid": 5 }))) });
    let journal = lines(&r.checked());
    assert_eq!(journal.last().map(String::as_str), Some("ParentNotified"), "{journal:?}");
}

#[test]
fn cancelling_a_parent_cancels_its_child() {
    let mut r = Rig::new("parent-3");
    r.start("parent", None);
    assert_eq!(r.call(RunCall::Cancel { reason: "stop".into() }), RunAnswer::Accepted);
    r.idle();
    assert_eq!(r.get().status, Status::Cancelled);
    let calls = r.fake.calls();
    assert!(calls.iter().any(|(id, c)| id == "parent-3/1" && matches!(c, RunCall::Cancel { .. })), "{calls:?}");
    r.checked();
}

#[test]
fn a_run_takes_ten_thousand_steps_and_fails_at_the_next() {
    let mut r = Rig::new("cap-steps-1");
    r.start_with("many", json_bytes(json!(10_000)), None, None);
    assert_eq!(r.output(), json!(10_000));
    let mut r = Rig::new("cap-steps-2");
    r.start_with("many", json_bytes(json!(10_001)), None, None);
    let view = r.get();
    assert_eq!(view.status, Status::Failed);
    assert_eq!(view.error.as_deref(), Some("step 10001: a run takes at most 10000 steps"));
    assert_eq!(r.checked().iter().filter(|e| matches!(e, Entry::Value { .. })).count(), 10_000);
}

#[test]
fn ctx_all_takes_a_thousand_steps_and_refuses_more_before_any_runs() {
    let mut r = Rig::new("cap-all-1");
    r.start_with("wide", json_bytes(json!(1_000)), None, None);
    r.advance(secs(1));
    assert_eq!(r.output(), json!(1_000));
    let mut r = Rig::new("cap-all-2");
    r.start_with("wide", json_bytes(json!(1_001)), None, None);
    let view = r.get();
    assert_eq!(view.status, Status::Failed);
    assert_eq!(view.error.as_deref(), Some("ctx.all of 1001 steps, past 1000"));
    assert!(!r.checked().iter().any(|e| matches!(e, Entry::TimerStarted { .. })));
}

#[test]
fn a_journal_past_32_mib_fails_the_run() {
    let mut r = Rig::new("cap-journal-1");
    r.start_with("big", json_bytes(json!(150)), None, None);
    assert_eq!(r.output(), json!(150));
    let bytes: usize = r.checked().iter().map(|e| e.encode().len()).sum();
    assert!(bytes > 29 << 20 && bytes <= MAX_JOURNAL, "{bytes}");
    let mut r = Rig::new("cap-journal-2");
    r.start_with("big", json_bytes(json!(170)), None, None);
    let view = r.get();
    assert_eq!(view.status, Status::Failed);
    assert!(view.error.as_deref().is_some_and(|e| e.contains("past 33554432")), "{:?}", view.error);
    let journal = r.checked();
    let before_end: usize = journal[..journal.len() - 1].iter().map(|e| e.encode().len()).sum();
    // The step that crossed the limit is the last: one step's entries past it at most.
    assert!(before_end <= MAX_JOURNAL + (450 << 10), "{before_end}");
}

#[test]
fn a_run_holds_a_thousand_untaken_signals_and_refuses_the_next() {
    let mut r = Rig::new("cap-signals-1");
    r.start("slow", None);
    for i in 0..1_000 {
        assert_eq!(r.signal(&format!("s{i}"), json!(i)), RunAnswer::Signalled { duplicate: false });
    }
    assert_eq!(refused_with(r.signal("s1000", json!(0))), Code::Limited);
    assert_eq!(r.signal("s0", json!(0)), RunAnswer::Signalled { duplicate: true }, "a duplicate is still known");
    r.checked();
}

#[test]
fn payloads_take_256_kib_and_refuse_one_byte_more() {
    let text = |n: usize| json_bytes(json!("x".repeat(n - 2)));
    let mut r = Rig::new("cap-payload-1");
    assert_eq!(r.start_with("slow", text(256 << 10), None, None), RunAnswer::Started { version: 1, created: true });
    let signal = |payload| RunCall::Signal { signal_id: "s".into(), name: "go".into(), payload };
    assert_eq!(refused_with(r.call(signal(text((256 << 10) + 1)))), Code::TooLarge);
    assert_eq!(r.call(signal(text(256 << 10))), RunAnswer::Signalled { duplicate: false });
    let mut r = Rig::new("cap-payload-2");
    assert_eq!(refused_with(r.start_with("slow", text((256 << 10) + 1), None, None)), Code::TooLarge);
    r.checked();
}

fn accepted(answer: RunAnswer) {
    assert_eq!(answer, RunAnswer::Accepted);
}

#[test]
fn a_paused_run_takes_calls_and_goes_on_only_after_a_resume() {
    let mut r = Rig::new("pause-1");
    r.start("steps", None);
    assert_eq!(refused_with(r.call(RunCall::Resume)), Code::NotResumable);
    accepted(r.call(RunCall::Pause));
    assert_eq!(refused_with(r.call(RunCall::Pause)), Code::Paused);
    let view = r.get();
    assert_eq!((view.status, view.due_us), (Status::Paused, None));
    // A signal is kept; the sleep's end passes, and nothing fires or runs.
    assert_eq!(r.signal("g1", json!(1)), RunAnswer::Signalled { duplicate: false });
    r.advance(secs(60));
    // A new owner opens it paused too.
    r.own(app());
    assert_eq!(r.get().status, Status::Paused);
    r.advance(secs(60));
    assert_eq!(executions("pause-1"), [(1, 1, "echo")]);
    accepted(r.call(RunCall::Resume));
    r.advance(secs(1));
    assert_eq!(r.output()["go"], json!(1));
    assert_eq!(
        lines(&r.checked()),
        [
            "started steps",
            "1 scheduled echo",
            "1 done 1",
            "2 value",
            "3 value",
            "4 timer",
            "Paused",
            "signal g1",
            "Resumed",
            "4 fired",
            "5 took g1",
            "6 scheduled echo",
            "6 done 1",
            "completed"
        ]
    );
    for call in [RunCall::Pause, RunCall::Resume] {
        assert_eq!(refused_with(r.call(call)), Code::Finished);
    }
    // The journal in pages, from a majority.
    let RunAnswer::History { entries, next } = r.call(RunCall::History { after: 0, limit: 3 }) else { panic!() };
    assert_eq!((entries.len(), next), (3, Some(3)));
    assert!(matches!(Entry::decode(&entries[0]), Ok(Entry::Started { .. })));
    let n = lines(&r.checked()).len() as u64;
    let RunAnswer::History { entries, next } = r.call(RunCall::History { after: n - 2, limit: 3 }) else { panic!() };
    assert_eq!((entries.len(), next), (2, None));
}

#[test]
fn a_pause_lets_go_of_the_attempt_out_and_a_resume_runs_it_again() {
    let mut r = Rig::new("pause-2");
    r.start("held", None);
    assert_eq!(executions("pause-2"), [(1, 1, "gate")]);
    accepted(r.call(RunCall::Pause));
    gate("pause-2").add_permits(1);
    r.advance(secs(5));
    assert_eq!(r.get().status, Status::Paused, "the attempt was let go");
    accepted(r.call(RunCall::Resume));
    gate("pause-2").add_permits(1);
    r.advance(secs(1));
    assert_eq!(r.output(), json!("inside"));
    assert_eq!(executions("pause-2"), [(1, 1, "gate"), (1, 1, "gate")]);
    r.checked();
}

#[test]
fn a_redrive_runs_the_failed_step_again_and_keeps_what_came_before() {
    let mut r = Rig::new("redrive-1");
    assert_eq!(refused_with(r.call(RunCall::Retry)), Code::NotFound);
    r.start("redrives", None);
    assert_eq!(refused_with(r.call(RunCall::Retry)), Code::NotFailed, "it runs");
    r.advance(secs(1));
    let view = r.get();
    assert_eq!(view.status, Status::Failed, "{view:#?}");
    assert_eq!(view.custom_status.as_deref(), Some(r#"{"at":"flaky"}"#));
    assert_eq!(executions("redrive-1"), [(1, 1, "echo"), (2, 1, "flaky"), (2, 2, "flaky"), (3, 1, "undo")]);
    accepted(r.call(RunCall::Retry));
    r.idle();
    // The undo's result, late: taken as a duplicate, it does not end the activity step 3 is now.
    accepted(r.call(RunCall::Complete { step: 3, attempt: 1, result: Ok(Bytes::from_static(b"null")) }));
    assert_eq!(r.get().status, Status::Running);
    gate("redrive-1").add_permits(1);
    r.idle();
    let output = json!(["a", 3, "redrive-1/3.1"]);
    assert_eq!(r.output(), output, "attempt 3 is the policy's first again; the key step 3 took is another");
    let view = r.get();
    assert_eq!(view.custom_status.as_deref(), Some(r#"{"at":"end"}"#));
    let rows: Vec<(u32, &str, u32)> = view.steps.iter().map(|s| (s.step, s.status.as_str(), s.attempts)).collect();
    assert_eq!(rows, [(1, "done", 1), (2, "done", 3), (3, "done", 2)], "the undone cleanup is not shown");
    assert_eq!(executions("redrive-1")[4..], [(2, 3, "flaky"), (3, 2, "key")]);
    assert_eq!(refused_with(r.call(RunCall::Retry)), Code::NotFailed, "completed");
    // A new owner replays past the redrive, and journals no status again.
    let journal = r.checked();
    r.own(app());
    assert_eq!(r.output(), output);
    assert_eq!(lines(&r.checked()), lines(&journal));
    assert_eq!(journal.iter().filter(|e| matches!(e, Entry::StatusSet { .. })).count(), 2);
    assert_eq!(executions("redrive-1").len(), 6);
}

#[test]
fn a_retry_keeps_the_owners_in_the_timeline() {
    let owners = |view: &RunView| -> Vec<(String, String)> {
        let opens = view.events.iter().filter(|e| matches!(e.kind.as_str(), "opened" | "reopened" | "moved"));
        opens.map(|e| (e.kind.clone(), e.text.clone())).collect()
    };
    let mut r = Rig::new("owners-1");
    r.start("redrives", None);
    r.advance(secs(1));
    let failed = r.get();
    assert_eq!(failed.status, Status::Failed, "{failed:#?}");
    let before = owners(&failed);
    assert_eq!(before.first(), Some(&("opened".to_owned(), "e1".to_owned())), "{:#?}", failed.events);
    // A retry builds the run again from its journal, which holds no owner's `Own`.
    accepted(r.call(RunCall::Retry));
    r.idle();
    let after = r.get();
    assert!(owners(&after).starts_with(&before), "{before:?} then {:#?}", after.events);
    assert!(after.events.iter().any(|e| e.kind == "retried"), "{:#?}", after.events);
    gate("owners-1").add_permits(1);
    r.idle();
    assert_eq!(r.get().status, Status::Completed);
}

#[test]
fn a_deleted_run_is_not_found_and_its_id_stays_taken() {
    let mut r = Rig::new("delete-1");
    r.start("slow", None);
    assert_eq!(refused_with(r.call(RunCall::Delete)), Code::NotFinished);
    accepted(r.call(RunCall::Terminate { reason: "test".into() }));
    accepted(r.call(RunCall::Delete));
    for call in [RunCall::Get, RunCall::Signal { signal_id: "s".into(), name: "go".into(), payload: Bytes::new() }] {
        assert_eq!(refused_with(r.call(call)), Code::NotFound);
    }
    assert_eq!(refused_with(r.start("slow", None)), Code::Exists);
    // A new owner finds it deleted in the index, and opens nothing.
    r.own(app());
    assert_eq!(refused_with(r.call(RunCall::Get)), Code::NotFound);
    assert_eq!(refused_with(r.call(RunCall::History { after: 0, limit: 10 })), Code::NotFound);
    assert_eq!(lines(&r.checked()).last().map(String::as_str), Some("Deleted"));
}
