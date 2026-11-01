//! The whole story in one process, checked as it goes: a queue node, three workers, a producer that is held back by
//! a full queue, a worker killed and one added mid-run, the dead set retried, and last the queue node killed.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon_cluster::{MemoryDiscovery, NamedRef};
use serde::{Deserialize, Serialize};

use crate::broker::{Broker, BrokerConfig};
use crate::worker::{WorkerConfig, start_worker};
use crate::{
    Answer, BROKER, BrokerApi, Call, Delivery, JobView, NewJob, Node, Result, State, Status, Ticket, unexpected,
};

pub const QUEUES: [&str; 3] = ["email", "reports", "webhooks"];

/// What a demo job does. A real one names its work instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Work {
    pub sleep_ms: u64,
    pub spin_us: u64,
    /// Fails while the job has failed fewer times than this: 0 never fails, `u32::MAX` always does.
    pub fail_first: u32,
    /// Fails by panicking.
    #[serde(default)]
    pub panic: bool,
}

impl Work {
    /// Sleeps, burns a little CPU, and fails as planned.
    pub async fn run(self, failures: u32) -> std::result::Result<(), String> {
        // A zero sleep would still wait for the timer's next millisecond.
        if self.sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        }
        let until = Instant::now() + Duration::from_micros(self.spin_us);
        let mut x = 0u64;
        while Instant::now() < until {
            x = std::hint::black_box(x.wrapping_mul(31).wrapping_add(7));
        }
        match failures < self.fail_first {
            true if self.panic => panic!("planned panic {}", failures + 1),
            true => Err(format!("planned failure {}", failures + 1)),
            false => Ok(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Params {
    pub jobs: u64,
    pub max_attempts: u32,
    pub capacity: usize,
    pub concurrency: usize,
    pub visibility: Duration,
}

impl Default for Params {
    fn default() -> Self {
        Self { jobs: 10_000, max_attempts: 3, capacity: 1000, concurrency: 8, visibility: Duration::from_secs(1) }
    }
}

/// The `i`th job of the demo: 5 % fail once or twice and then succeed, one in a thousand never succeeds (job 500 by
/// panicking), one in ten waits up to a second before it may run.
pub fn job(i: u64, max_attempts: u32) -> (NewJob, Work) {
    let fail_first = match i {
        i if i % 1000 == 500 => u32::MAX,
        i if i % 20 == 7 => 1 + (i / 20 % 2) as u32,
        _ => 0,
    };
    let work = Work { sleep_ms: 2 + i * 13 % 19, spin_us: 50, fail_first, panic: i == 500 };
    let spec = NewJob {
        id: i,
        queue: QUEUES[(i % 3) as usize].into(),
        priority: (i * 7 % 3) as u8,
        delay_ms: if i % 10 == 3 { 100 + i % 20 * 50 } else { 0 },
        max_attempts,
        payload: serde_json::to_string(&work).unwrap_or_default(),
    };
    (spec, work)
}

/// One run of a job, as its side effect would show it.
#[derive(Clone, Debug)]
pub struct Run {
    pub ticket: Ticket,
    pub worker: String,
    pub ok: bool,
    pub start: Instant,
    pub end: Instant,
}

#[derive(Default)]
pub struct Ledger {
    pub runs: Mutex<Vec<Run>>,
    running: Mutex<HashMap<String, Arc<AtomicUsize>>>,
    pub most_at_once: Mutex<BTreeMap<String, usize>>,
}

impl Ledger {
    fn gauge(&self, worker: &str) -> Arc<AtomicUsize> {
        let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
        running.entry(worker.into()).or_default().clone()
    }

    /// A worker handler that runs the demo's `Work` and records each run, the way a real job writes to a store.
    pub fn handler(
        self: &Arc<Self>,
        worker: &str,
    ) -> impl Fn(Delivery) -> std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>> + Send + 'static
    {
        let (ledger, worker) = (self.clone(), worker.to_owned());
        move |d: Delivery| {
            let (ledger, worker) = (ledger.clone(), worker.clone());
            Box::pin(async move {
                let work: Work = serde_json::from_str(&d.payload).map_err(|e| e.to_string())?;
                let gauge = ledger.gauge(&worker);
                let now = gauge.fetch_add(1, Ordering::SeqCst) + 1;
                {
                    let mut most = ledger.most_at_once.lock().unwrap_or_else(|e| e.into_inner());
                    let most = most.entry(worker.clone()).or_default();
                    *most = (*most).max(now);
                }
                let start = Instant::now();
                let run = Run { ticket: d.ticket, worker, ok: false, start, end: start };
                let mut record = Record { ledger, gauge, run, ended: false };
                let result = work.run(d.failures).await;
                (record.run.ok, record.ended) = (result.is_ok(), true);
                drop(record);
                result
            })
        }
    }
}

/// Records a run that ended, by returning or by panicking, but not one that was dropped: a crashed worker's.
struct Record {
    ledger: Arc<Ledger>,
    gauge: Arc<AtomicUsize>,
    run: Run,
    ended: bool,
}

impl Drop for Record {
    fn drop(&mut self) {
        self.gauge.fetch_sub(1, Ordering::SeqCst);
        if self.ended || std::thread::panicking() {
            let run = Run { end: Instant::now(), ..self.run.clone() };
            self.ledger.runs.lock().unwrap_or_else(|e| e.into_inner()).push(run);
        }
    }
}

async fn ask(broker: &NamedRef<BrokerApi>, call: Call) -> Result<Answer> {
    Ok(broker.request(call).await?)
}

pub async fn status(broker: &NamedRef<BrokerApi>) -> Result<Status> {
    match ask(broker, Call::Status).await? {
        Answer::Status(status) => Ok(*status),
        other => Err(unexpected(other).into()),
    }
}

pub async fn inspect(broker: &NamedRef<BrokerApi>, ids: Vec<u64>) -> Result<Vec<JobView>> {
    match ask(broker, Call::Inspect(ids)).await? {
        Answer::Views(views) => Ok(views),
        other => Err(unexpected(other).into()),
    }
}

/// Enqueues `jobs` in batches, waiting while the queue is full. Returns when each was taken, and how many times the
/// producer was held back.
pub async fn produce(broker: NamedRef<BrokerApi>, jobs: Vec<NewJob>) -> Result<(HashMap<u64, Instant>, u64)> {
    let (mut sent, mut held) = (HashMap::new(), 0);
    for batch in jobs.chunks(100) {
        let mut rest = batch;
        while !rest.is_empty() {
            let at = Instant::now();
            let accepted = match ask(&broker, Call::Enqueue(rest.to_vec())).await? {
                Answer::Enqueued { accepted } => accepted,
                other => return Err(unexpected(other).into()),
            };
            sent.extend(rest[..accepted].iter().map(|j| (j.id, at)));
            rest = &rest[accepted..];
            if !rest.is_empty() {
                held += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    Ok((sent, held))
}

struct Cluster {
    discovery: MemoryDiscovery,
    secret: &'static str,
}

impl Cluster {
    async fn node(&self, id: &str, role: &str) -> Result<Node> {
        let bind = ([127, 0, 0, 1], 0).into();
        crate::start_node(crate::transport(id, self.secret)?, bind, None, role, self.discovery.clone()).await
    }

    async fn queue(&self, cfg: &BrokerConfig) -> Result<Node> {
        let node = self.node("queue", "queue").await?;
        let broker = node.system.spawn(Broker::new(cfg.clone())).name("broker").start()?;
        node.cluster.publish::<BrokerApi>(&broker, BROKER)?;
        Ok(node)
    }

    async fn worker(&self, id: &str, cfg: &WorkerConfig, ledger: &Arc<Ledger>) -> Result<Node> {
        let node = self.node(id, "worker").await?;
        start_worker(&node, cfg.clone(), ledger.handler(id))?;
        Ok(node)
    }
}

fn check(ok: bool, what: impl FnOnce() -> String) -> Result<()> {
    if ok { Ok(()) } else { Err(what().into()) }
}

/// Everything the demo measured, for the test and the README.
#[derive(Debug)]
pub struct Report {
    pub jobs_per_sec: f64,
    pub redelivered: usize,
    pub redelivery: Duration,
    pub duplicates: usize,
    pub done_by: BTreeMap<String, u64>,
}

pub async fn demo(p: Params) -> Result<Report> {
    let started = Instant::now();
    let say = |what: String| println!("{:>6.2}s  {what}", started.elapsed().as_secs_f64());
    let cluster = Cluster { discovery: MemoryDiscovery::new(), secret: "task-queue-example-secret" };
    let ledger = Arc::new(Ledger::default());
    let wcfg =
        WorkerConfig { concurrency: p.concurrency, poll: p.visibility / 10, job_timeout: Duration::from_secs(10) };

    let cfg = BrokerConfig {
        capacity: p.capacity,
        visibility: p.visibility,
        // Retries within the demo's run.
        backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
        ..BrokerConfig::default()
    };
    let queue = cluster.queue(&cfg).await?;
    let mut workers = BTreeMap::new();
    for id in ["worker-1", "worker-2", "worker-3"] {
        workers.insert(id.to_owned(), cluster.worker(id, &wcfg, &ledger).await?);
    }
    let producer = cluster.node("producer", "producer").await?;
    let broker = crate::find_broker(&producer.cluster, Duration::from_secs(10)).await?;
    say(format!(
        "queue, producer and 3 workers up: {} jobs at most waiting, {} at once per worker, visibility {:?}",
        p.capacity, p.concurrency, p.visibility
    ));

    let (specs, works): (Vec<NewJob>, Vec<Work>) = (0..p.jobs).map(|i| job(i, p.max_attempts)).unzip();
    let plans: HashMap<u64, (NewJob, Work)> = specs.iter().cloned().zip(works).map(|(s, w)| (s.id, (s, w))).collect();
    let producing = tokio::spawn(produce(broker.clone(), specs));
    let enqueued_at = Instant::now();

    let (mut crashed, mut taken, mut added) = (None, None, None);
    let mut deadline = Instant::now() + Duration::from_secs(45);
    let finished = loop {
        let s = status(&broker).await?;
        let t = s.total();
        let (ended, all) = (t.done + t.dead, p.jobs);
        if crashed.is_none() && ended >= all * 3 / 10 {
            let w2 = s.workers.get("worker-2").cloned().unwrap_or_default();
            drop(workers.remove("worker-2")); // dropped without a shutdown: a crash
            crashed = Some((Instant::now(), w2.lost));
            let running = w2.running;
            say(format!("worker-2 crashed with {running} jobs running; {ended} of {all} ended"));
        }
        let w2 = s.workers.get("worker-2").cloned().unwrap_or_default();
        if let (Some((at, before)), None, true) = (crashed, taken, crashed.is_some_and(|(_, b)| w2.lost > b)) {
            taken = Some(at.elapsed());
            let n = w2.lost - before;
            say(format!(
                "worker-2's {n} jobs taken back {:.2?} after the crash, back in their old place in line",
                at.elapsed()
            ));
        }
        if added.is_none() && ended >= all * 6 / 10 {
            workers.insert("worker-4".into(), cluster.worker("worker-4", &wcfg, &ledger).await?);
            added = Some(Instant::now());
            say(format!("worker-4 added; {ended} of {all} ended"));
        }
        if ended == all && producing.is_finished() {
            break Instant::now();
        }
        check(Instant::now() < deadline, || format!("not finished in time: {s:?}"))?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let (sent, held) = producing.await??;
    let crashed = crashed.ok_or("worker-2 never crashed")?.0;
    let added = added.ok_or("worker-4 never added")?;
    let s = status(&broker).await?;
    let t = s.total();
    let rate = p.jobs as f64 / (finished - enqueued_at).as_secs_f64();
    say(format!(
        "all {} ended: {} done, {} dead, {rate:.0} jobs/s; the producer was held back {held} times",
        p.jobs, t.done, t.dead
    ));
    check(t.pending + t.delayed + t.running == 0, || format!("jobs left over: {t:?}"))?;
    check(held > 0 && s.refused > 0, || "the producer never felt the queue fill".into())?;
    let slack = p.concurrency * (workers.len() + 1);
    check(s.max_queued <= p.capacity + slack, || format!("{} waited at once, capacity {}", s.max_queued, p.capacity))?;

    // Every job ended as its plan says, and each run the broker did not count is one of a delivery it took back.
    let mut views = Vec::new();
    let ids: Vec<u64> = (0..p.jobs).collect();
    for page in ids.chunks(1000) {
        views.extend(inspect(&broker, page.to_vec()).await?);
    }
    check(views.len() as u64 == p.jobs, || format!("{} of {} jobs known to the broker", views.len(), p.jobs))?;
    let runs = ledger.runs.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let mut by_job: HashMap<u64, Vec<&Run>> = HashMap::new();
    for run in &runs {
        by_job.entry(run.ticket.id).or_default().push(run);
    }
    let (mut duplicates, mut taken_back, mut after_crash) = (0, 0, Duration::ZERO);
    for v in &views {
        let (spec, work) = &plans[&v.id];
        let (state, failures) = match work.fail_first {
            0 => (State::Done, 0),
            u32::MAX => (State::Dead, p.max_attempts),
            k => (State::Done, k),
        };
        check(v.state == state && v.failures == failures, || format!("job {} ended {v:?}, planned {work:?}", v.id))?;
        let mine = by_job.get(&v.id).map_or(&[][..], Vec::as_slice);
        let counted = failures + u32::from(state == State::Done);
        let extra = mine.len() as i64 - i64::from(counted);
        let back = v.deliveries - counted;
        check(extra >= 0 && extra <= i64::from(back), || format!("job {} ran {} times: {v:?}", v.id, mine.len()))?;
        let succeeded = mine.iter().any(|r| r.ok);
        check(succeeded == (state == State::Done), || format!("job {} ended {state:?}, its runs: {mine:?}", v.id))?;
        duplicates += extra as usize;
        taken_back += back as usize;
        if spec.delay_ms > 0 {
            let first = mine.iter().map(|r| r.start).min().ok_or("a delayed job never ran")?;
            check(first >= sent[&v.id] + Duration::from_millis(spec.delay_ms), || format!("job {} ran early", v.id))?;
        }
        for r in mine.iter().filter(|r| r.worker == "worker-2") {
            after_crash = after_crash.max(r.end.saturating_duration_since(crashed));
        }
    }
    let lost: u64 = s.workers.values().map(|w| w.lost).sum();
    check(lost as usize == taken_back, || format!("{lost} deliveries lost, {taken_back} not counted"))?;
    // A dropped system cancels each job at its next poll; one in the middle of its last poll still finishes.
    check(after_crash < Duration::from_millis(5), || format!("worker-2 ran on for {after_crash:?} after its crash"))?;
    let most = ledger.most_at_once.lock().unwrap_or_else(|e| e.into_inner()).clone();
    check(most.values().all(|&n| n <= p.concurrency), || format!("more than {} at once: {most:?}", p.concurrency))?;

    let w2_lost = s.workers.get("worker-2").map_or(0, |w| w.lost);
    let redelivery = taken.ok_or("worker-2's jobs were never taken back")?;
    let tail = match after_crash {
        Duration::ZERO => "none of its runs ended after the crash".to_owned(),
        late => format!("one run, mid-poll when it crashed, ended {late:.1?} after"),
    };
    say(format!(
        "{duplicates} runs were duplicates, each of a delivery the broker took back ({taken_back} taken back in all, \
         {w2_lost} of them from worker-2; {tail})"
    ));

    // Higher priority waits less, among jobs that could run at once.
    let mut wait: BTreeMap<u8, (f64, u32)> = BTreeMap::new();
    for v in &views {
        let spec = &plans[&v.id].0;
        if let (0, Some(first)) = (spec.delay_ms, by_job.get(&v.id).and_then(|r| r.iter().map(|r| r.start).min())) {
            let w = wait.entry(spec.priority).or_default();
            *w = (w.0 + (first - sent[&v.id]).as_secs_f64(), w.1 + 1);
        }
    }
    let mean: BTreeMap<u8, f64> = wait.iter().map(|(p, (sum, n))| (*p, sum / f64::from(*n) * 1000.0)).collect();
    say(format!(
        "mean wait by priority, ms: {}",
        mean.iter().map(|(p, w)| format!("{p}: {w:.0}")).collect::<Vec<_>>().join(", ")
    ));
    check(mean.get(&2) < mean.get(&0), || format!("priority 2 waited longer than 0: {mean:?}"))?;

    let done_by: BTreeMap<String, u64> = s.workers.iter().map(|(n, w)| (n.clone(), w.done)).collect();
    let alive = |w: &str| match w {
        "worker-2" => crashed - enqueued_at,
        "worker-4" => finished - added,
        _ => finished - enqueued_at,
    };
    say(format!(
        "done per worker: {}",
        done_by
            .iter()
            .map(|(w, n)| format!("{w} {n} ({:.0}/s)", *n as f64 / alive(w).as_secs_f64()))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    check(done_by.get("worker-4").is_some_and(|&n| n > 0), || "worker-4 did nothing".into())?;

    // The dead set holds exactly the jobs planned to die; retried, they run their attempts again and die again.
    let dead = match ask(&broker, Call::Dead { limit: 100 }).await? {
        Answer::Views(v) => v,
        other => return Err(unexpected(other).into()),
    };
    let planned: Vec<u64> = (0..p.jobs).filter(|i| plans[i].1.fail_first == u32::MAX).collect();
    check(dead.iter().map(|v| v.id).collect::<Vec<_>>() == planned, || format!("dead set {dead:?}"))?;
    let panicked = dead.iter().filter(|v| v.last_error.as_deref().is_some_and(|e| e.starts_with("panicked"))).count();
    check(panicked == 1, || format!("{panicked} dead by a panic, not 1: {dead:?}"))?;
    let ids: Vec<u64> = dead.iter().map(|v| v.id).collect();
    let retried = match ask(&broker, Call::Retry(ids.clone())).await? {
        Answer::Retried(n) => n,
        other => return Err(unexpected(other).into()),
    };
    say(format!(
        "dead set is the {} jobs planned to die: {}; retried {retried}",
        planned.len(),
        dead[0].last_error.as_deref().unwrap_or("")
    ));
    deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let views = inspect(&broker, ids.clone()).await?;
        if views.iter().all(|v| v.state == State::Dead && v.failures == 2 * p.max_attempts) {
            break;
        }
        check(Instant::now() < deadline, || format!("retried jobs did not die again: {views:?}"))?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    say(format!("the retried jobs failed {} more times each and are dead again", p.max_attempts));

    // The queue node crashes: every job it held is gone. The producer enqueues again what it has not seen end, the
    // workers find the new broker by themselves, and what had run under the old one runs again.
    let payload = serde_json::to_string(&Work { sleep_ms: 20, spin_us: 50, fail_first: 0, panic: false })?;
    let late: Vec<NewJob> = (p.jobs..p.jobs + 300)
        .map(|id| NewJob {
            id,
            queue: "late".into(),
            priority: 1,
            delay_ms: if id % 2 == 0 { 0 } else { 2000 },
            max_attempts: p.max_attempts,
            payload: payload.clone(),
        })
        .collect();
    produce(broker.clone(), late.clone()).await?;
    let before = loop {
        let s = status(&broker).await?;
        let counts = s.queues.get("late").cloned().unwrap_or_default();
        if counts.done >= 50 {
            break counts;
        }
        check(Instant::now() < deadline, || format!("late jobs did not start: {counts:?}"))?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    drop(queue); // a crash
    let crashed = Instant::now();
    say(format!(
        "queue node crashed holding the 300 late jobs: {} pending, {} delayed, {} running, {} done",
        before.pending, before.delayed, before.running, before.done
    ));
    let queue = cluster.queue(&cfg).await?;
    // The same reference finds the new broker.
    while !status(&broker).await.is_ok_and(|s| s.queues.is_empty()) {
        check(crashed.elapsed() < Duration::from_secs(15), || "the new broker never answered".into())?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    say("a new queue node is up with an empty broker; the producer enqueues the 300 again, same ids".into());
    produce(broker.clone(), late.clone()).await?;
    deadline = Instant::now() + Duration::from_secs(20);
    let s = loop {
        let s = status(&broker).await?;
        if s.queues.get("late").is_some_and(|c| c.done == 300) {
            break s;
        }
        check(Instant::now() < deadline, || format!("late jobs did not finish on the new broker: {s:?}"))?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let runs = ledger.runs.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let late_runs: Vec<&Run> = runs.iter().filter(|r| r.ticket.id >= p.jobs).collect();
    let old = late_runs.iter().filter(|r| r.end < crashed).map(|r| r.ticket.boot).next().ok_or("no late run")?;
    let mut again = 0;
    for job in &late {
        let mine: Vec<&&Run> = late_runs.iter().filter(|r| r.ticket.id == job.id).collect();
        check(mine.iter().any(|r| r.ok && r.ticket.boot != old), || format!("late job {} never ran anew", job.id))?;
        again += usize::from(mine.iter().any(|r| r.ok && r.ticket.boot == old));
    }
    check(again as u64 >= before.done, || format!("{again} ran twice, {} were done before", before.done))?;
    let first = late_runs.iter().filter(|r| r.ticket.boot != old).map(|r| r.start).min().ok_or("no new run")?;
    say(format!(
        "all 300 done on the new broker, the first {:.2?} after the crash; {again} had run under the old one and ran \
         again, and {} outcomes for the old broker's deliveries were ignored",
        first - crashed,
        s.stale
    ));

    for node in workers.into_values().chain([producer, queue]) {
        node.system.shutdown().await?;
    }
    Ok(Report { jobs_per_sec: rate, redelivered: w2_lost as usize, redelivery, duplicates, done_by })
}
