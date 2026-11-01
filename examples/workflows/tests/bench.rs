//! The journal's numbers on real disks, for the gate after phase 2. Run on the bench box, in a release build:
//!
//! ```text
//! cargo test --release -p workflows --test bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `BENCH_DIR` names the directory the replicas write in (default: the target dir's tmp). Each test prints one line.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon::files::{FileOp, Files, OsFiles};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use bytes::Bytes;
use workflows::journal::client::{JournalClient, Owner};
use workflows::journal::core::seal_snapshot;
use workflows::journal::service::{self, JournalConfig, SNAPSHOT};
use workflows::journal::{Entry, Own, Replica, Request, ValueKind, WriterConfig, journal_epoch, replica_config};

const REPLICAS: [&str; 3] = ["h1", "h2", "h3"];

fn dir() -> tempfile::TempDir {
    let base = std::env::var("BENCH_DIR").map_or_else(|_| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

async fn member(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    // Loopback only.
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// 100 runs each append a 200-byte entry every 10 ms, 10,000 a second, to three replicas on their own systems over
/// loopback TCP; each append's time is from the call to its ack by a majority, synced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run it on the bench box"]
async fn append_p99_at_ten_thousand_records_a_second() {
    const RUNS: usize = 100;
    const EVERY: Duration = Duration::from_millis(10);
    const WARM: Duration = Duration::from_secs(3);
    const MEASURE: Duration = Duration::from_secs(20);
    let (dir, discovery) = (dir(), MemoryDiscovery::new());
    let replicas: Vec<Arc<str>> = REPLICAS.map(Arc::from).to_vec();
    let mut held = Vec::new();
    for id in REPLICAS {
        let (system, cluster) = member(id, &discovery).await;
        let files = Arc::new(OsFiles::new(dir.path().join(id).join("journal")));
        let snaps = Arc::new(OsFiles::new(dir.path().join(id).join("journal-snap")));
        service::start(&system, &cluster, files, snaps, JournalConfig::new(replicas.clone())).await.unwrap();
        held.push((system, cluster));
    }
    let (_engine, cluster) = member("e1", &discovery).await;
    while cluster.view().members.len() < 4 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let client = JournalClient::new(cluster.clone(), replicas);
    let samples = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..RUNS {
        let (client, samples) = (client.clone(), samples.clone());
        tasks.spawn(async move {
            let run = Bytes::from(format!("bench-{i:03}"));
            let (mut owner, _) =
                Owner::open(client, run, 1, WriterConfig::new(3), Duration::from_secs(10)).await.unwrap();
            // Spread over the period, so that the runs do not append in step.
            tokio::time::sleep(EVERY * i as u32 / RUNS as u32).await;
            let mut tick = tokio::time::interval(EVERY);
            let mut mine = Vec::new();
            for step in 1u32.. {
                tick.tick().await;
                if start.elapsed() > WARM + MEASURE {
                    break;
                }
                let value = Bytes::from(vec![step as u8; 200]);
                let body = Entry::Value { step, kind: ValueKind::Random, value }.encode();
                let at = Instant::now();
                owner.append(vec![body], Duration::from_secs(5)).await.unwrap();
                if start.elapsed() > WARM {
                    mine.push(at.elapsed());
                }
            }
            samples.lock().unwrap().extend(mine);
        });
    }
    while let Some(done) = tasks.join_next().await {
        done.unwrap();
    }
    let mut all = std::mem::take(&mut *samples.lock().unwrap());
    all.sort_unstable();
    let rate = all.len() as f64 / MEASURE.as_secs_f64();
    println!(
        "append: {rate:.0} records/s acked over {MEASURE:?}, p50 {:?} p99 {:?} p99.9 {:?} max {:?}",
        percentile(&all, 0.5),
        percentile(&all, 0.99),
        percentile(&all, 0.999),
        all[all.len() - 1]
    );
    for (system, _cluster) in held {
        system.shutdown().await.unwrap();
    }
}

/// Changes taken so far, written and synced.
async fn flush(replica: &mut Replica<()>) {
    while let Some(write) = replica.take_write(0) {
        let written = write.await;
        assert!(written.is_ok(), "{written:?}");
        replica.written(written);
    }
}

/// A replica of 1,000,000 sleeping runs, each its seal, `Own`, `Started` and `TimerStarted`, opened as its `Journal`
/// starts: once with its snapshot, once without.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run it on the bench box"]
async fn replica_restart_with_a_million_runs() {
    const RUNS: usize = 1_000_000;
    let dir = dir();
    let (journal, snap) = (dir.path().join("journal"), dir.path().join("journal-snap"));
    let built = Instant::now();
    {
        let files = Arc::new(OsFiles::new(&journal));
        let mut replica: Replica<()> = Replica::open(files, replica_config(), None, 0).await.unwrap();
        let epoch = journal_epoch(1).unwrap();
        let own = Entry::Own(Own::default()).encode();
        for i in 0..RUNS {
            let run = Bytes::from(format!("run-{i:07}"));
            let input = Bytes::from(vec![7; 64]);
            let tags = vec![("tenant".to_owned(), format!("t{}", i % 100))];
            let started = Entry::Started {
                workflow: "sleeper".into(),
                version: 1,
                input,
                input_hash: i as u64,
                tags,
                parent: None,
                run_timeout_ms: None,
                at_us: 0,
            };
            let timer = Entry::TimerStarted { step: 1, wake_at_us: 2_000_000_000_000_000 + i as u64, at_us: 0 };
            replica.handle(Request::Seal { run: run.clone(), epoch, token: 0 }, ());
            let records = vec![own.clone(), started.encode(), timer.encode()];
            replica.handle(Request::Append { run, epoch, seq: 1, records }, ());
            if i % 10_000 == 0 {
                flush(&mut replica).await;
            }
        }
        flush(&mut replica).await;
        let mut out = Vec::new();
        let mut from = None;
        while let Some(next) = replica.snapshot(from.as_ref(), 20_000, &mut out) {
            from = Some(next);
        }
        let snaps = OsFiles::new(&snap);
        snaps.apply(vec![FileOp::Replace(SNAPSHOT.into(), seal_snapshot(out))]).await.unwrap();
    }
    let built = built.elapsed();
    let bytes: u64 = std::fs::read_dir(&journal).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum();
    let open = |snapshot: bool| {
        let (journal, snap) = (journal.clone(), snap.clone());
        async move {
            let at = Instant::now();
            let snaps = OsFiles::new(&snap);
            let read = if snapshot { snaps.read(SNAPSHOT).await.ok().map(Bytes::from) } else { None };
            let replica: Replica<()> =
                Replica::open(Arc::new(OsFiles::new(&journal)), replica_config(), read, 1).await.unwrap();
            assert_eq!(replica.index().len(), RUNS);
            assert_eq!(replica.index().due(None, u64::MAX).count(), RUNS);
            at.elapsed()
        }
    };
    let with = open(true).await;
    let without = open(false).await;
    println!(
        "restart: {RUNS} runs, {} MiB of log built in {built:?}; open with the snapshot {with:?}, without {without:?}",
        bytes >> 20
    );
}
