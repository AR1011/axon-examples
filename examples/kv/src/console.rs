//! `kv console`: a web page of a running cluster and of the bench runs. Every second it asks each node `INFO` over
//! RESP, as any client can, and keeps an hour of each node's rates; it reads the runs from the results directory each
//! time a page asks. It changes nothing and has no sign-in, so `kv console` binds to loopback unless told otherwise.
//! The page is `ui/dist`, built by `npm run build` in `ui/` and held in the binary.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::{Path as UrlPath, Query, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use include_dir::{Dir, include_dir};
use serde_json::{Value as Json, json};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::COMMANDS;
use crate::client::Client;
use crate::resp::Value;
use crate::slot::{SHARDS, SLOTS_PER_SHARD};

static UI: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");
/// The page loads scripts, styles and fonts from this origin only, and no other site may frame it.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
                   font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; \
                   object-src 'none'";
/// The rates each node keeps: an hour at one sample a second.
const KEEP: Duration = Duration::from_secs(3600);
/// How many points a series of `/api/ops` has at most: longer windows add samples up into buckets.
const POINTS: u64 = 360;

pub struct ConsoleConfig {
    /// The nodes' RESP addresses.
    pub addrs: Vec<SocketAddr>,
    pub bind: SocketAddr,
    /// Where `bench.sh` writes its runs, one directory each.
    pub results: PathBuf,
    /// For `AUTH`, when the nodes have `--password`.
    pub password: Option<String>,
    /// How often each node is asked.
    pub every: Duration,
}

pub struct Console {
    pub addr: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl Console {
    pub fn stop(self) {
        for task in self.tasks {
            task.abort();
        }
    }
}

/// Reads per second, writes per second and failed calls per second, between two samples.
type Rate = [f64; 3];

/// A node's counters at one sample.
#[derive(Clone, Copy)]
struct Totals {
    pid: u64,
    counts: [u64; 3],
    at: Instant,
}

/// What the console knows of one node.
struct NodeView {
    addr: SocketAddr,
    /// It answered the last time it was asked.
    up: bool,
    error: Option<String>,
    /// When it last answered, and when it first did not after that.
    seen_ms: Option<u64>,
    down_ms: Option<u64>,
    /// Its last `INFO`: the server, clients and kv sections.
    info: BTreeMap<String, String>,
    /// `(shard, node, lease epoch)` of each shard running there.
    hosts: Vec<(u16, String, u64)>,
    last: Option<Totals>,
    /// A sample each time it was asked, `None` where it did not answer or had just started.
    points: VecDeque<(u64, Option<Rate>)>,
}

struct Shared {
    nodes: Vec<Mutex<NodeView>>,
    results: PathBuf,
    every: Duration,
}

pub async fn start(cfg: ConsoleConfig) -> std::io::Result<Console> {
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    let addr = listener.local_addr()?;
    let views = cfg.addrs.iter().map(|&addr| {
        Mutex::new(NodeView {
            addr,
            up: false,
            error: None,
            seen_ms: None,
            down_ms: None,
            info: BTreeMap::new(),
            hosts: Vec::new(),
            last: None,
            points: VecDeque::new(),
        })
    });
    let shared = Arc::new(Shared { nodes: views.collect(), results: cfg.results, every: cfg.every });
    let mut tasks: Vec<JoinHandle<()>> =
        (0..cfg.addrs.len()).map(|i| tokio::spawn(watch(shared.clone(), i, cfg.password.clone()))).collect();
    let app = router(shared);
    tasks.push(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("kv console: the HTTP server ended: {e}");
        }
    }));
    Ok(Console { addr, tasks })
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Asks node `i` every `every`, on a new connection each time: one that a node ended, or a node held by SIGSTOP,
/// shows at the next sample.
async fn watch(shared: Arc<Shared>, i: usize, password: Option<String>) {
    let addr = shared.nodes[i].lock().expect("a node view").addr;
    let (origin, origin_ms) = (Instant::now(), now_ms());
    let mut tick = tokio::time::interval(shared.every);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let patience = shared.every.max(Duration::from_secs(2));
    let keep = (KEEP.as_millis() / shared.every.as_millis().max(1)) as usize;
    loop {
        // A sample is filed under the time it was due, not the time its answer came: a slow answer near a bucket's
        // edge would put two samples in one bucket of `/api/ops` and none in the next.
        let due = tick.tick().await;
        let due_ms = origin_ms + due.duration_since(origin).as_millis() as u64;
        let answer = match tokio::time::timeout(patience, ask(addr, password.as_deref())).await {
            Ok(answer) => answer,
            Err(_) => Err(format!("no answer within {} ms", patience.as_millis())),
        };
        let (at, at_ms) = (Instant::now(), now_ms());
        let mut view = shared.nodes[i].lock().expect("a node view");
        let rate = match answer {
            Ok((info, counts, hosts)) => {
                let pid = info.get("process_id").and_then(|p| p.parse().ok()).unwrap_or(0);
                let now = Totals { pid, counts, at };
                // A node that started again counts from zero: no rate across it.
                let rate = view.last.filter(|l| l.pid == pid && (0..3).all(|k| counts[k] >= l.counts[k])).map(|l| {
                    let secs = at.duration_since(l.at).as_secs_f64().max(1e-3);
                    [0, 1, 2].map(|k| (counts[k] - l.counts[k]) as f64 / secs)
                });
                view.last = Some(now);
                view.up = true;
                view.error = None;
                view.seen_ms = Some(at_ms);
                view.down_ms = None;
                view.info = info;
                view.hosts = hosts;
                rate
            }
            Err(e) => {
                view.up = false;
                view.error = Some(e);
                view.down_ms.get_or_insert(at_ms);
                view.last = None;
                view.hosts.clear();
                None
            }
        };
        view.points.push_back((due_ms, rate));
        while view.points.len() > keep {
            view.points.pop_front();
        }
    }
}

/// `INFO all` and `INFO kvhosted` of one node: its fields, its reads, writes and failed calls so far, and where each
/// of its shards runs.
async fn ask(
    addr: SocketAddr,
    password: Option<&str>,
) -> Result<(BTreeMap<String, String>, [u64; 3], Vec<(u16, String, u64)>), String> {
    let mut client = Client::connect(addr).await.map_err(|e| e.to_string())?;
    let mut call = async |args: &[&[u8]]| match client.call(args).await {
        Ok(Value::Bulk(Some(text))) => Ok(String::from_utf8_lossy(&text).into_owned()),
        Ok(Value::Simple(text)) => Ok(text),
        Ok(Value::Error(e)) => Err(e),
        Ok(other) => Err(format!("an answer out of place: {other:?}")),
        Err(e) => Err(e.to_string()),
    };
    if let Some(password) = password {
        call(&[b"AUTH", password.as_bytes()]).await?;
    }
    let all = call(&[b"INFO", b"all"]).await?;
    let hosted = call(&[b"INFO", b"kvhosted"]).await?;
    let mut info = BTreeMap::new();
    let mut counts = [0; 3];
    for (key, value) in all.lines().filter_map(|l| l.split_once(':')) {
        let Some(name) = key.strip_prefix("cmdstat_") else {
            info.insert(key.to_owned(), value.to_owned());
            continue;
        };
        let Some((_, _, flags, ..)) = COMMANDS.iter().find(|c| c.0 == name) else { continue };
        let field = |f: &str| value.split(',').find_map(|kv| kv.strip_prefix(f)?.strip_prefix('=')?.parse().ok());
        let (calls, failed): (u64, u64) = (field("calls").unwrap_or(0), field("failed_calls").unwrap_or(0));
        // The data commands only: the console's own INFO, and a client's PING, are not load.
        if flags.contains(&"readonly") {
            counts[0] += calls;
        } else if flags.contains(&"write") {
            counts[1] += calls;
        } else {
            continue;
        }
        counts[2] += failed;
    }
    let hosts = hosted
        .lines()
        .filter_map(|line| {
            let (shard, host) = line.strip_prefix('s')?.split_once(':')?;
            let (node, epoch) = host.rsplit_once('@')?;
            Some((shard.parse().ok()?, node.to_owned(), epoch.parse().ok()?))
        })
        .collect();
    Ok((info, counts, hosts))
}

fn router(shared: Arc<Shared>) -> Router {
    let api = Router::new()
        .route("/cluster", get(cluster))
        .route("/ops", get(ops))
        .route("/runs", get(runs))
        .route("/runs/{name}", get(run))
        .fallback(no_route)
        .with_state(shared);
    Router::new().nest("/api", api).fallback(page)
}

async fn no_route() -> Response {
    missing("no such route")
}

/// A refusal as the page reads it: `{error, message}`.
fn missing(message: &str) -> Response {
    let body = json!({ "error": "not_found", "message": message });
    (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
}

fn rate_json(rate: Option<Rate>) -> Json {
    rate.map_or(Json::Null, |[reads, writes, failed]| json!({ "reads": reads, "writes": writes, "failed": failed }))
}

async fn cluster(State(shared): State<Arc<Shared>>) -> Response {
    let mut shards = vec![Vec::new(); usize::from(SHARDS)];
    let mut nodes = Vec::new();
    for view in &shared.nodes {
        let view = view.lock().expect("a node view");
        for (shard, node, epoch) in &view.hosts {
            if let Some(hosts) = shards.get_mut(usize::from(*shard)) {
                hosts.push(json!({ "node": node, "epoch": epoch }));
            }
        }
        let field = |k: &str| view.info.get(k);
        let number = |k: &str| field(k).and_then(|v| v.parse::<u64>().ok());
        // `INFO kv` is the node's own: the server and clients fields are shown apart.
        let kv: serde_json::Map<String, Json> = view
            .info
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "redis_version" | "redis_mode" | "kv_version" | "tcp_port"))
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        nodes.push(json!({
            "addr": view.addr.to_string(),
            "id": field("node"),
            "up": view.up,
            "error": view.error,
            "seen_ms": view.seen_ms,
            "down_ms": view.down_ms,
            "mode": field("mode"),
            "reads": field("reads"),
            "pid": number("process_id"),
            "uptime_s": number("uptime_in_seconds"),
            "clients": number("connected_clients"),
            "shards_here": if view.up { Some(view.hosts.len()) } else { None },
            "rate": rate_json(view.points.back().and_then(|p| p.1)),
            "info": kv,
        }));
    }
    let shards: Vec<Json> = shards.into_iter().map(|hosts| json!({ "hosts": hosts })).collect();
    axum::Json(json!({
        "at_ms": now_ms(),
        "every_ms": shared.every.as_millis() as u64,
        "slots_per_shard": SLOTS_PER_SHARD,
        "nodes": nodes,
        "shards": shards,
    }))
    .into_response()
}

/// Each node's rates over the last `minutes` (5 by default, at most 60), as `[at_ms, reads, writes, failed]`, with
/// nulls where it did not answer. At most [`POINTS`] a node: a longer window adds samples up into buckets.
async fn ops(State(shared): State<Arc<Shared>>, Query(query): Query<BTreeMap<String, String>>) -> Response {
    let minutes = query.get("minutes").and_then(|m| m.parse::<u64>().ok()).unwrap_or(5).clamp(1, 60);
    let every = shared.every.as_millis().max(1) as u64;
    let window = minutes * 60_000;
    let bucket = every.max(window / POINTS).div_ceil(every) * every;
    // From the first bucket that starts inside the window, so it holds at most POINTS buckets.
    let from = (now_ms().saturating_sub(window) / bucket + 1) * bucket;
    let mut nodes = Vec::new();
    for view in &shared.nodes {
        let view = view.lock().expect("a node view");
        let mut points: Vec<Json> = Vec::new();
        let mut current: Option<(u64, [f64; 3], usize)> = None;
        let flush = |points: &mut Vec<Json>, (at, sum, n): (u64, [f64; 3], usize)| {
            points.push(if n == 0 {
                json!([at, null, null, null])
            } else {
                json!([at, sum[0] / n as f64, sum[1] / n as f64, sum[2] / n as f64])
            });
        };
        for &(at, rate) in view.points.iter().filter(|(at, _)| *at >= from) {
            let start = at / bucket * bucket;
            if current.is_some_and(|c| c.0 != start) {
                flush(&mut points, current.take().expect("a bucket"));
            }
            let (_, sum, n) = current.get_or_insert((start, [0.0; 3], 0));
            if let Some(rate) = rate {
                (0..3).for_each(|k| sum[k] += rate[k]);
                *n += 1;
            }
        }
        if let Some(last) = current {
            flush(&mut points, last);
        }
        nodes.push(json!({ "addr": view.addr.to_string(), "id": view.info.get("node"), "points": points }));
    }
    axum::Json(json!({ "every_ms": bucket, "nodes": nodes })).into_response()
}

/// A run's directory, if `name` is one: a plain name, no path.
fn run_dir(results: &Path, name: &str) -> Option<PathBuf> {
    let plain = !name.is_empty() && !name.starts_with('.') && !name.contains(['/', '\\']);
    Some(results.join(name)).filter(|dir| plain && dir.is_dir())
}

/// `results.csv` as its header and rows. A field in double quotes may hold a comma or a doubled quote.
pub fn parse_csv(text: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let mut lines = text.lines().filter(|l| !l.is_empty()).map(|line| {
        let mut fields = vec![String::new()];
        let mut quoted = false;
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            let field = fields.last_mut().expect("a field");
            match c {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    chars.next();
                    field.push('"');
                }
                '"' => quoted = !quoted,
                ',' if !quoted => fields.push(String::new()),
                c => field.push(c),
            }
        }
        fields
    });
    let header = lines.next().unwrap_or_default();
    (header, lines.collect())
}

/// What a run holds: its environment, its summary, its rows and its fault phases, each `None` where it has none.
struct RunFiles {
    env: Option<String>,
    summary: Option<String>,
    csv: Option<(Vec<String>, Vec<Vec<String>>)>,
    faults: Vec<Json>,
}

fn read_run(dir: &Path) -> RunFiles {
    let text = |name: &str| std::fs::read_to_string(dir.join(name)).ok();
    RunFiles {
        env: text("env.txt"),
        summary: text("summary.md"),
        csv: text("results.csv").map(|t| parse_csv(&t)),
        faults: text("fault/faults.jsonl")
            .map(|t| t.lines().filter_map(|l| serde_json::from_str(l).ok()).collect())
            .unwrap_or_default(),
    }
}

/// A run in the list: where and how it ran, and what it holds.
fn run_entry(name: &str, files: &RunFiles) -> Json {
    let env = |key: &str| {
        let text = files.env.as_deref()?;
        text.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix(": ")).map(str::to_owned)
    };
    let (columns, rows) = files.csv.as_ref().map_or((&[][..], &[][..]), |(c, r)| (&c[..], &r[..]));
    let column = |name: &str| columns.iter().position(|c| c == name);
    let values = |name: &str| -> Vec<&str> {
        column(name).map_or(Vec::new(), |i| rows.iter().filter_map(|r| r.get(i).map(String::as_str)).collect())
    };
    let mut targets: Vec<&str> = Vec::new();
    for target in values("target") {
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    let first = |name: &str| values(name).first().map(|v| (*v).to_owned());
    // The rows summary.md leaves out of its tables and lists apart: a count of them says more than a sum of very
    // unequal error counts.
    let (errors, phase) = (column("errors"), column("phase"));
    let errored = |row: &Vec<String>| {
        let cell = |i: Option<usize>| i.and_then(|i| row.get(i)).map(String::as_str);
        cell(phase) != Some("warmup") && cell(errors).and_then(|e| e.parse::<u64>().ok()).is_some_and(|e| e > 0)
    };
    let soak = |f: &&Json| f["phase"] == "soak";
    let phases = files.faults.iter().filter(|f| !soak(f)).count();
    let passed = files.faults.iter().filter(|f| !soak(f) && f["pass"] == true).count();
    let soaked = files.faults.iter().find(soak).map(|f| f["pass"].clone());
    json!({
        "name": name,
        "date": name.get(..10),
        "host": env("host").or_else(|| first("host")),
        // "full (indicative: ..)": the word before the reason.
        "kind": env("kind").map(|k| k.split(' ').next().unwrap_or_default().to_owned()).or_else(|| first("run_kind")),
        "rounds": values("round").iter().filter_map(|r| r.parse::<u64>().ok()).max(),
        "rows": rows.len(),
        "error_rows": rows.iter().filter(|row| errored(row)).count(),
        "targets": targets,
        "faults": if phases + usize::from(soaked.is_some()) == 0 { Json::Null } else { json!({ "phases": phases, "passed": passed, "soak": soaked }) },
        "summary": files.summary.is_some(),
    })
}

async fn runs(State(shared): State<Arc<Shared>>) -> Response {
    let results = shared.results.clone();
    let listed = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<Json>> {
        let mut names: Vec<String> = std::fs::read_dir(&results)?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| run_dir(&results, name).is_some())
            .collect();
        names.sort_by(|a, b| b.cmp(a));
        Ok(names.iter().map(|name| run_entry(name, &read_run(&results.join(name)))).collect())
    })
    .await;
    match listed {
        Ok(Ok(runs)) => {
            axum::Json(json!({ "dir": shared.results.display().to_string(), "runs": runs })).into_response()
        }
        // No results directory yet: no runs, and the page says where it looked.
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            axum::Json(json!({ "dir": shared.results.display().to_string(), "runs": [] })).into_response()
        }
        Ok(Err(e)) => {
            let body = json!({ "error": "unreadable", "message": format!("{}: {e}", shared.results.display()) });
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
        }
        Err(e) => {
            let body = json!({ "error": "failed", "message": e.to_string() });
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
        }
    }
}

async fn run(State(shared): State<Arc<Shared>>, UrlPath(name): UrlPath<String>) -> Response {
    let Some(dir) = run_dir(&shared.results, &name) else {
        return missing(&format!("no run {name} in {}", shared.results.display()));
    };
    let Ok(files) = tokio::task::spawn_blocking(move || read_run(&dir)).await else {
        return missing(&format!("the run {name} could not be read"));
    };
    let mut body = run_entry(&name, &files);
    let (columns, rows) = files.csv.unwrap_or_default();
    body["env"] = json!(files.env);
    body["summary"] = json!(files.summary);
    body["columns"] = json!(columns);
    body["rows"] = json!(rows);
    body["faults"] = json!(files.faults);
    axum::Json(body).into_response()
}

/// A file of the page, or the page itself for any other path: the browser's router reads the path, and a run's name
/// may hold a dot. A missing file under `assets/` is a 404, so a mistyped asset is not answered with the page.
async fn page(request: Request) -> Response {
    let path = request.uri().path().trim_start_matches('/');
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return missing("the console takes GET only");
    }
    let secured = |mut response: Response, cache: &'static str| {
        let headers = response.headers_mut();
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
        headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
        response
    };
    if let Some(file) = UI.get_file(path).filter(|_| path != "index.html") {
        let mime = match path.rsplit('.').next() {
            Some("js") => "text/javascript",
            Some("css") => "text/css",
            Some("svg") => "image/svg+xml",
            Some("woff2") => "font/woff2",
            Some("png") => "image/png",
            _ => "application/octet-stream",
        };
        // A built file's name holds the hash of what is in it.
        let cache = if path.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "no-store" };
        return secured(([(header::CONTENT_TYPE, mime)], file.contents()).into_response(), cache);
    }
    match UI.get_file("index.html").filter(|_| !path.starts_with("assets/")) {
        Some(index) => secured(
            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], index.contents()).into_response(),
            "no-store",
        ),
        None => missing(&format!("nothing at /{path}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quoted_field_keeps_its_comma_and_its_quotes() {
        let (header, rows) = parse_csv("a,b,c\n1,\"x, y\",3\n\"say \"\"hi\"\"\",,\n");
        assert_eq!(header, ["a", "b", "c"]);
        assert_eq!(rows, [vec!["1", "x, y", "3"], vec!["say \"hi\"", "", ""]]);
    }

    #[test]
    fn a_run_is_a_plain_name_of_a_directory() {
        let results = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/runs");
        assert!(run_dir(&results, "2026-10-11-epyc-4484px-full").is_some());
        for name in ["", ".", "..", "../src", "a/b", "nothing-here", ".hidden"] {
            assert!(run_dir(&results, name).is_none(), "{name:?}");
        }
    }
}
