//! The real `Scheduler` actor on `EmbeddedDriver`, a manual clock and manual jobs: every turn, job and tick is
//! stepped by hand, and the wall clock the schedule follows is stepped back and forward under it.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{
    Clock, EmbeddedDriver, JobExecutor, LifecycleHandle, MailboxLimits, ManualClock, ManualJobs, StopReason, TurnBudget,
};
use cron_singleton::{Fire, Job, Log, LogAnswer, LogCall, Outcome, Pending, RunLogActor, Scheduler, Wall, audit};

const BASE: i64 = 1_800_000_000_000; // a wall time on a 5 s boundary

#[derive(Default)]
struct Mem {
    fired: Vec<(Fire, u64)>, // with the wall time it fired at
    refused: Vec<(Fire, Outcome)>,
}

/// The run log in memory. The answer is worked out when the job runs, not when the call is made, so a call can be
/// held in flight while another owner claims, as a pause mid-request would.
#[derive(Clone)]
struct MemLog {
    log: Arc<Mutex<RunLogActor>>,
    mem: Arc<Mutex<Mem>>,
    wall: Wall,
}

impl Log for MemLog {
    fn call(&self, call: LogCall) -> Pending {
        let this = self.clone();
        Box::pin(async move {
            let fires = match &call {
                LogCall::Fire { fires, .. } => fires.clone(),
                LogCall::Claim { .. } => Vec::new(),
            };
            let answer = this.log.lock().unwrap().answer(call);
            if let LogAnswer::Outcomes(outcomes) = &answer {
                let mut mem = this.mem.lock().unwrap();
                for (fire, outcome) in fires.into_iter().zip(outcomes) {
                    match outcome {
                        Outcome::Fired { from } => mem.fired.push((Fire { from: *from, ..fire }, (this.wall)())),
                        refused => mem.refused.push((fire, *refused)),
                    }
                }
            }
            Ok(answer)
        })
    }
}

struct Rig {
    driver: EmbeddedDriver<Scheduler<MemLog>>,
    life: LifecycleHandle,
    jobs: ManualJobs,
}

impl Rig {
    fn start(jobs: Vec<Job>, epoch: u64, clock: &ManualClock, log: &MemLog) -> Rig {
        let scheduler = Scheduler::new(jobs, epoch, format!("node-{epoch}"), log.wall.clone(), log.clone());
        let (executor, manual) = JobExecutor::manual(8);
        let (driver, spawn) =
            EmbeddedDriver::with_jobs(scheduler, MailboxLimits::default(), clock.clock(), executor).unwrap();
        let mut rig = Rig { driver, life: spawn.lifecycle, jobs: manual };
        rig.settle();
        rig
    }

    fn settle(&mut self) {
        self.driver.run_until_idle(10_000).expect("the scheduler never goes quiet: it fires in a loop");
    }

    fn turn_only(&mut self) {
        self.driver.run_turn(TurnBudget::default());
    }
}

fn jobs() -> Vec<Job> {
    vec![
        Job { name: "beat".into(), every_ms: 1000, offset_ms: 0, max_runs: 1024 },
        Job { name: "digest".into(), every_ms: 5000, offset_ms: 2500, max_runs: 1 },
    ]
}

fn world() -> (ManualClock, Arc<AtomicI64>, MemLog) {
    let clock = Clock::manual();
    let step = Arc::new(AtomicI64::new(0));
    let (c, s) = (clock.clock(), step.clone());
    let wall: Wall = Arc::new(move || (BASE + c.now().as_millis() as i64 + s.load(Ordering::SeqCst)) as u64);
    let log = MemLog { log: Arc::new(Mutex::new(RunLogActor::new(|_| {}))), mem: Arc::default(), wall };
    (clock, step, log)
}

fn fired(log: &MemLog) -> Vec<Fire> {
    log.mem.lock().unwrap().fired.iter().map(|(f, _)| f.clone()).collect()
}

#[test]
fn clock_steps_neither_double_fire_nor_skip_nor_fire_early() {
    let (clock, step, log) = world();
    let jobs = jobs();
    let mut rig = Rig::start(jobs.clone(), 10, &clock, &log);
    for ms in (100..=60_000).step_by(100) {
        clock.advance(Duration::from_millis(100));
        match ms {
            20_000 => drop(step.fetch_sub(7_500, Ordering::SeqCst)), // NTP steps the clock back
            35_000 => drop(step.fetch_add(42_000, Ordering::SeqCst)), // and then far forward
            _ => {}
        }
        rig.settle();
    }
    let covered = audit(&fired(&log)).expect("every tick exactly once");
    let now = (log.wall)();
    for job in &jobs {
        let first = job.tick_at(BASE as u64).unwrap();
        assert_eq!(covered[&job.name], (first, job.tick_at(now).unwrap()), "{} covers its ticks to now", job.name);
    }
    let mem = log.mem.lock().unwrap();
    for (fire, at) in &mem.fired {
        let job = jobs.iter().find(|j| j.name == fire.job).unwrap();
        assert!(*at >= job.starts_at(fire.tick), "{fire:?} fired at {at}, before its tick");
    }
    // The forward step: `beat` fired each of the 42 ticks it jumped, `digest` folded its missed ones into one run.
    assert!(mem.fired.iter().any(|(f, _)| f.job == "digest" && f.from + 8 == f.tick), "{:?}", mem.fired);
    assert!(mem.fired.iter().filter(|(f, _)| f.job == "beat").all(|(f, _)| f.from == f.tick));
    assert!(mem.refused.is_empty(), "{:?}", mem.refused);
    assert_eq!(rig.life.exit(), None);
}

#[test]
fn an_owner_paused_mid_request_is_fenced_by_the_next_claim_and_nothing_is_lost() {
    let (clock, _step, log) = world();
    let mut old = Rig::start(jobs(), 10, &clock, &log);
    for _ in 0..50 {
        clock.advance(Duration::from_millis(100));
        old.settle();
    }
    // The old owner's next fire leaves, and it freezes before the answer comes back.
    clock.advance(Duration::from_millis(1000));
    old.turn_only();
    assert_eq!(old.jobs.pending(), 1, "a fire in flight");

    // Its lease runs out; the next owner claims under a larger epoch and runs for a while.
    let mut new = Rig::start(jobs(), 11, &clock, &log);
    for _ in 0..30 {
        clock.advance(Duration::from_millis(100));
        new.settle();
    }
    // The old owner thaws: its request lands now, is refused, and the old owner stops.
    old.settle();
    let exit = old.life.exit().expect("the old owner stopped");
    assert_eq!(exit.reason, StopReason::Stopped);
    let mem = log.mem.lock().unwrap();
    assert!(mem.refused.iter().all(|(_, o)| *o == Outcome::Fenced) && !mem.refused.is_empty(), "{:?}", mem.refused);
    drop(mem);
    for _ in 0..30 {
        clock.advance(Duration::from_millis(100));
        new.settle();
    }
    let covered = audit(&fired(&log)).expect("every tick exactly once across the handover");
    assert_eq!(covered["beat"].1, jobs()[0].tick_at((log.wall)()).unwrap());
}
