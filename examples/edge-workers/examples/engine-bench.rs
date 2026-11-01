//! Measures the real engine for the README: `cargo run --release -p edge-workers --example engine-bench`.
//! Start, deploy and load times, the artifact's size, a hello-world request's latency, requests per second on one
//! thread and on all, the memory a request in flight holds, and what a request to a durable object costs when
//! every one is a new instance (`OBJECTS_ONLY=1` for that part alone).

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use edge_workers::engine::starling::{Starling, StarlingConfig};
use edge_workers::engine::{Engine, Env, Host, Limits, Objects, Request, Response, Scope, StorageAnswer, StorageOp};

const HELLO: &str = r#"export default { async fetch(request, env, ctx) { return new Response("hello world") } }"#;
const JSON: &str = r#"export default { async fetch(request, env) {
    const url = new URL(request.url);
    const body = { path: url.pathname, greeting: env.GREETING, items: Array.from({ length: 20 }, (_, i) => ({ id: i, name: `item-${i}` })) };
    return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } }) } }"#;
const WAIT: &str = r#"export default { async fetch() { await new Promise((done) => setTimeout(done, 4000)); return new Response("ok") } }"#;

const COUNTER: &str = r#"
export class Counter {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    return new Response(String(n));
  }
}
export default { fetch(request, env) { return env.COUNTER.get(env.COUNTER.idFromName("a")).fetch(request); } };"#;

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The host an object has on a runner, without the cluster: its storage is a map in memory, and a call of an
/// object runs it in this engine.
#[derive(Clone)]
struct InProcess {
    engine: Starling,
    version: <Starling as Engine>::Version,
    stored: Arc<Mutex<BTreeMap<String, String>>>,
}

impl Host for InProcess {
    fn fetch(&self, class: String, id: String, request: Request) -> Boxed<Result<Response, String>> {
        let host = self.clone();
        Box::pin(async move {
            let scope = Scope { object: Some((class, id)), host: Some(Arc::new(host.clone())) };
            let outcome = host.engine.invoke_in(&host.version, request, Limits::default(), scope).await;
            let outcome = outcome.map_err(|failed| failed.failure.to_string())?;
            Ok(Response { status: outcome.status, headers: outcome.headers, body: outcome.body })
        })
    }

    fn storage(&self, op: StorageOp) -> Boxed<Result<StorageAnswer, String>> {
        let mut stored = self.stored.lock().expect("no panic under the lock");
        let answer = match op {
            StorageOp::Get { key } => StorageAnswer::Value(stored.get(&key).cloned()),
            StorageOp::Put { key, value } => {
                stored.insert(key, value);
                StorageAnswer::Done
            }
            StorageOp::Delete { key } => StorageAnswer::Deleted(stored.remove(&key).is_some()),
            StorageOp::List { .. } => StorageAnswer::Entries(Vec::new()),
        };
        Box::pin(std::future::ready(Ok(answer)))
    }
}

/// A counter object's request, each in a new instance: alone, and behind the handler that calls it.
fn objects(engine: &Starling, all: &tokio::runtime::Runtime, one: &tokio::runtime::Runtime) {
    let bound = Objects::from([("COUNTER".to_owned(), "Counter".to_owned())]);
    let version = all.block_on(engine.deploy_bound(COUNTER, &Env::new(), &bound)).unwrap_or_else(|e| panic!("{e}"));
    let host = InProcess { engine: engine.clone(), version: version.clone(), stored: Arc::default() };
    for (name, object) in
        [("the object alone", Some(("Counter".to_owned(), "a".to_owned()))), ("handler and object", None)]
    {
        let (mut walls, mut cpus) = (Vec::new(), Vec::new());
        one.block_on(async {
            for i in 0..5200 {
                let scope = Scope { object: object.clone(), host: Some(Arc::new(host.clone())) };
                let began = Instant::now();
                let outcome = engine.invoke_in(&version, Request::get("/bench"), Limits::default(), scope).await;
                let outcome = outcome.expect("a response");
                assert_eq!(outcome.status, 200, "{:?}", outcome.body);
                if i >= 200 {
                    walls.push(began.elapsed());
                    cpus.push(outcome.usage.cpu);
                }
            }
        });
        walls.sort();
        cpus.sort();
        println!(
            "counter, {name}: new instance, a get and a put, 5000 in a row on one thread: p50 {:?} p99 {:?} max {:?}; cpu as metered p50 {:?}",
            percentile(&walls, 0.5),
            percentile(&walls, 0.99),
            walls[walls.len() - 1],
            percentile(&cpus, 0.5),
        );
    }
}

fn rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

fn runtime(threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().expect("a runtime")
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// `tasks` loops of requests for `secs`: requests per second.
async fn throughput(engine: &Starling, version: &<Starling as Engine>::Version, tasks: usize, secs: u64) -> f64 {
    let done = Arc::new(AtomicU64::new(0));
    let began = Instant::now();
    let until = began + Duration::from_secs(secs);
    let mut running = Vec::new();
    for _ in 0..tasks {
        let (engine, version, done) = (engine.clone(), version.clone(), done.clone());
        running.push(tokio::spawn(async move {
            while Instant::now() < until {
                engine.invoke(&version, Request::get("/bench"), Limits::default()).await.expect("a response");
                done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for task in running {
        task.await.expect("the loop does not panic");
    }
    done.load(Ordering::Relaxed) as f64 / began.elapsed().as_secs_f64()
}

fn main() {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("engine");
    let cache = tempfile::tempdir().expect("a directory for the compile cache");
    let mut cfg = StarlingConfig::new(dir);
    cfg.cache_dir = Some(cache.path().to_owned());
    // `KEEP_RESIDENT=0` measures what the default of 4 MiB buys.
    if let Ok(bytes) = std::env::var("KEEP_RESIDENT") {
        cfg.keep_resident = bytes.parse().expect("KEEP_RESIDENT is a number of bytes");
    }
    println!("keep_resident: {} bytes", cfg.keep_resident);
    let began = Instant::now();
    let engine = Starling::new(cfg.clone()).unwrap_or_else(|e| panic!("{e}"));
    println!("engine start, cold cache: {:?}", began.elapsed());
    let began = Instant::now();
    drop(Starling::new(cfg.clone()).unwrap_or_else(|e| panic!("{e}")));
    println!("engine start, warm cache: {:?}; RSS {:?}", began.elapsed(), rss());

    let all = runtime(cores);
    if std::env::var_os("OBJECTS_ONLY").is_some() {
        return objects(&engine, &all, &runtime(1));
    }
    let env = Env::from([("GREETING".to_owned(), "hi".to_owned())]);
    let (mut deploys, mut versions) = (Vec::new(), Vec::new());
    let before = rss();
    for _ in 0..5 {
        let began = Instant::now();
        versions.push(all.block_on(engine.deploy(HELLO, &env)).unwrap_or_else(|e| panic!("{e}")));
        deploys.push(began.elapsed());
    }
    deploys.sort();
    println!(
        "deploy (snapshot and compile, {cores} threads): min {:?} median {:?} max {:?}",
        deploys[0], deploys[2], deploys[4]
    );
    if let (Some(before), Some(after)) = (before, rss()) {
        println!("5 versions held: RSS {before} -> {after} bytes, {} per version", after.saturating_sub(before) / 5);
    }
    let hello = versions.pop().expect("five deploys");
    drop(versions);
    let artifact = engine.artifact(&hello);
    println!("artifact: {} bytes", artifact.len());
    if std::env::var_os("DEPLOY_ONLY").is_some() {
        return;
    }
    let began = Instant::now();
    all.block_on(engine.load(artifact.clone())).unwrap_or_else(|e| panic!("{e}"));
    println!("load of an artifact this cache holds: {:?}", began.elapsed());
    let one = runtime(1);
    let began = Instant::now();
    let fresh = tempfile::tempdir().expect("a directory");
    let mut cold = cfg.clone();
    cold.cache_dir = Some(fresh.path().to_owned());
    let other = Starling::new(cold).unwrap_or_else(|e| panic!("{e}"));
    let started = began.elapsed();
    let began = Instant::now();
    all.block_on(other.load(artifact.clone())).unwrap_or_else(|e| panic!("{e}"));
    println!(
        "load of an artifact on a node new to it ({cores} threads): {:?} (after an engine start of {started:?})",
        began.elapsed()
    );
    drop(other);

    let json = all.block_on(engine.deploy(JSON, &env)).unwrap_or_else(|e| panic!("{e}"));
    for (name, version) in [("hello", &hello), ("json", &json)] {
        let (mut walls, mut cpus) = (Vec::new(), Vec::new());
        one.block_on(async {
            for i in 0..5200 {
                let began = Instant::now();
                let outcome =
                    engine.invoke(version, Request::get("/bench"), Limits::default()).await.expect("a response");
                if i >= 200 {
                    walls.push(began.elapsed());
                    cpus.push(outcome.usage.cpu);
                }
            }
        });
        walls.sort();
        cpus.sort();
        println!(
            "{name}: instantiate and request, 5000 in a row on one thread: p50 {:?} p99 {:?} max {:?}; cpu as metered p50 {:?} p99 {:?}",
            percentile(&walls, 0.5),
            percentile(&walls, 0.99),
            walls[walls.len() - 1],
            percentile(&cpus, 0.5),
            percentile(&cpus, 0.99)
        );
        println!(
            "{name}: one runtime thread, 1 loop: {:.0} requests/s",
            one.block_on(throughput(&engine, version, 1, 5))
        );
        println!(
            "{name}: {cores} runtime threads, {} loops: {:.0} requests/s",
            cores * 2,
            all.block_on(throughput(&engine, version, cores * 2, 5))
        );
    }

    objects(&engine, &all, &one);

    let wait = all.block_on(engine.deploy(WAIT, &env)).unwrap_or_else(|e| panic!("{e}"));
    let in_flight = 500;
    all.block_on(async {
        // One first, so what every request shares is in the baseline.
        engine.invoke(&hello, Request::get("/"), Limits::default()).await.expect("a response");
        let before = rss();
        let mut running = Vec::new();
        for _ in 0..in_flight {
            let (engine, wait) = (engine.clone(), wait.clone());
            running.push(tokio::spawn(async move { engine.invoke(&wait, Request::get("/"), Limits::default()).await }));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let during = rss();
        for task in running {
            task.await.expect("no panic").expect("a response");
        }
        match (before, during, rss()) {
            (Some(before), Some(during), Some(after)) => println!(
                "{in_flight} requests in flight, each waiting on a timer: RSS {before} -> {during} bytes, {} per request; {after} after they end",
                (during.saturating_sub(before)) / in_flight
            ),
            _ => println!("no /proc/self/status here: memory not measured"),
        }
    });
}
