//! `wf`: the command line over the HTTP API. `wf help` lists the commands.

use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use workflows::auth::hash_password;
use workflows::check::{Ack, Replicas, Witness, audit_checkout, check, read_lines};
use workflows::client::{Client, StartRun, segment};
use workflows::journal::{Entry, Summary};
use workflows::run::machine::input_hash;

const HELP: &str = "\
wf: start and watch workflow runs

  wf login --url http://host:port [--password P]   sign in; the token is kept in
                                                    ~/.config/workflows/credentials.json (WF_CONFIG)
  wf start <workflow> [--input JSON|@file] [--id ID] [--version N] [--tag k=v].. [--run-timeout 30s] [--wait]
                                                    start a run and print its id
  wf get <id> [--wait] [--timeout SECONDS]          a run's state; --wait prints each step as it ends, then
                                                    the output, and fails when the run does not complete
  wf signal <id> <name> [--signal-id ID] [JSON]     send a signal; the same id again is a duplicate
  wf cancel <id> [--reason R]                       cancel: the run's cleanup steps run
  wf terminate <id> [--reason R]                    stop at once: no cleanup runs
  wf retry|resume|pause|delete <id>                 redrive a failed run from its failed step, resume a parked or
                                                    paused run, pause one, remove one that ended
  wf retry|resume|pause|delete --filter k=v,..      the same for each run the filter lists, 50 a second; prints
                                                    how many were done, skipped (not in that state) and refused
  wf list [--filter k=v,..] [--limit N]             runs, newest first; filters: workflow, status, tag (k:v),
                                                    prefix, parent, since (1h), from (2026-10-09T10:00, UTC)
  wf history <id>                                   the run's journal, one entry a line
  wf workflows|queues|workers|metrics|cluster       the console's pages
  wf load <workflow> rate=N secs=S [input=JSON] [prefix=P] [acks=FILE] [wait=S]
                                                    starts at N a second (checkout runs get their approval), then
                                                    the start ack's p50 and p99, the answers, and with wait= each
                                                    run's start to end once all ended
  wf check <dir>.. [--witness FILE].. [--acks FILE].. the offline checker over stopped history nodes' directories
  wf check <dir>.. --run ID                         each replica's segments of one run, entry by entry
  wf logout                                         forget the kept token
  wf hash-password                                  an argon2id hash of WF_PASSWORD or of a line of stdin

Every command takes --url and --json. WF_URL and WF_TOKEN override the kept ones.";

type Failure = String;

/// The words of a command line, and each `--flag value` (or `--flag=value`) as often as given.
struct Args {
    words: Vec<String>,
    flags: BTreeMap<String, Vec<String>>,
}

const SWITCHES: [&str; 3] = ["json", "wait", "help"];

impl Args {
    fn parse(argv: impl Iterator<Item = String>) -> Result<Self, Failure> {
        let (mut words, mut flags) = (Vec::new(), BTreeMap::<String, Vec<String>>::new());
        let mut argv = argv.peekable();
        while let Some(arg) = argv.next() {
            let Some(name) = arg.strip_prefix("--").filter(|name| !name.is_empty()) else {
                words.push(arg);
                continue;
            };
            let (name, value) = match name.split_once('=') {
                Some((name, value)) => (name.to_owned(), value.to_owned()),
                None if SWITCHES.contains(&name) => (name.to_owned(), String::new()),
                None => (name.to_owned(), argv.next().ok_or(format!("--{name} needs a value"))?),
            };
            flags.entry(name).or_default().push(value);
        }
        Ok(Self { words, flags })
    }

    fn flag(&self, name: &str) -> Option<&str> {
        self.flags.get(name).and_then(|values| values.last()).map(String::as_str)
    }

    fn all(&self, name: &str) -> &[String] {
        self.flags.get(name).map_or(&[], Vec::as_slice)
    }

    fn has(&self, name: &str) -> bool {
        self.flags.contains_key(name)
    }

    fn word(&self, at: usize, what: &str) -> Result<&str, Failure> {
        self.words.get(at).map(String::as_str).ok_or(format!("missing {what}: see `wf help`"))
    }
}

fn config_file() -> Result<PathBuf, Failure> {
    if let Ok(file) = std::env::var("WF_CONFIG") {
        return Ok(file.into());
    }
    let home = std::env::var("HOME").map_err(|_| "no HOME to keep the credentials in: set WF_CONFIG")?;
    Ok(PathBuf::from(home).join(".config/workflows/credentials.json"))
}

fn kept() -> Value {
    let read = config_file().ok().and_then(|file| std::fs::read(file).ok());
    read.and_then(|text| serde_json::from_slice(&text).ok()).unwrap_or(Value::Null)
}

fn client(args: &Args) -> Result<Client, Failure> {
    let kept = kept();
    let env = |name| std::env::var(name).ok();
    let url = args.flag("url").map(String::from).or_else(|| env("WF_URL"));
    let url = url.or_else(|| kept["url"].as_str().map(String::from));
    let url =
        url.ok_or("no api node: `wf login --url http://host:port`, or set WF_URL")?.trim_end_matches('/').to_owned();
    // A token belongs to the node that signed it.
    let same = kept["url"].as_str() == Some(url.as_str());
    let token = env("WF_TOKEN").or_else(|| kept["token"].as_str().filter(|_| same).map(String::from));
    Ok(Client::new(&url, token))
}

/// One line of stdin, without its end.
fn line() -> Result<String, Failure> {
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| format!("reading stdin: {e}"))?;
    Ok(line.trim_end_matches(['\n', '\r']).to_owned())
}

fn password(args: &Args) -> Result<String, Failure> {
    if let Some(password) = args.flag("password").map(String::from).or_else(|| std::env::var("WF_PASSWORD").ok()) {
        return Ok(password);
    }
    if std::io::stdin().is_terminal() {
        eprint!("password: ");
    }
    line()
}

async fn login(args: &Args) -> Result<(), Failure> {
    let url = args.flag("url").map(String::from).or_else(|| std::env::var("WF_URL").ok());
    let url = url.ok_or("wf login --url http://host:port")?.trim_end_matches('/').to_owned();
    let (token, expires_ms) = Client::new(&url, None).login(&password(args)?).await?;
    let file = config_file()?;
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let kept = json!({ "url": url, "token": token, "expires_ms": expires_ms });
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        let opened = options.write(true).create(true).truncate(true).mode(0o600).open(&file);
        let written = opened.and_then(|mut file| std::io::Write::write_all(&mut file, kept.to_string().as_bytes()));
        written.map_err(|e| format!("{}: {e}", file.display()))?;
    }
    println!("signed in to {url}");
    Ok(())
}

async fn logout(args: &Args) -> Result<(), Failure> {
    // The kept value is a session: the node forgets nothing, so it is the file that goes.
    if let Ok(api) = client(args) {
        drop(api.logout().await);
    }
    let file = config_file()?;
    match std::fs::remove_file(&file) {
        Ok(()) => println!("signed out"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => println!("not signed in"),
        Err(e) => return Err(format!("{}: {e}", file.display())),
    }
    Ok(())
}

/// JSON from the text, or from the file after `@`.
fn json_arg(text: &str) -> Result<Value, Failure> {
    let text = match text.strip_prefix('@') {
        Some(file) => std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?,
        None => text.to_owned(),
    };
    serde_json::from_str(&text).map_err(|e| format!("not JSON: {e}"))
}

async fn start(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let mut tags = Vec::new();
    for tag in args.all("tag") {
        let (k, v) = tag.split_once('=').ok_or(format!("--tag {tag}: not k=v"))?;
        tags.push((k.to_owned(), v.to_owned()));
    }
    let version = args.flag("version").map(|v| v.parse().map_err(|_| format!("--version {v}: not a number")));
    let start = StartRun {
        workflow: args.word(1, "a workflow")?.to_owned(),
        version: version.transpose()?,
        id: args.flag("id").map(String::from),
        input: args.flag("input").map_or(Ok(Value::Null), json_arg)?,
        tags,
        run_timeout: args.flag("run-timeout").map(String::from),
    };
    let said = api.start(&start).await?;
    let id = said["id"].as_str().unwrap_or_default().to_owned();
    match args.has("json") {
        true => println!("{said}"),
        false if said["created"] == json!(false) => println!("{id} (already started with this input)"),
        false => println!("{id}"),
    }
    if args.has("wait") {
        return wait(args, &api, &id).await;
    }
    Ok(())
}

fn step_line(step: &Value) -> String {
    let name = step["name"].as_str().filter(|n| !n.is_empty()).map_or(String::new(), |n| format!(" {n}"));
    let mut line =
        format!("step {} {}{}: {}", step["step"], step["kind"].as_str().unwrap_or("?"), name, step["status"]);
    if let Some(error) = step["error"].as_str() {
        line.push_str(&format!(" ({error})"));
    }
    line.replace('"', "")
}

/// Polls the run every 250 ms: each step once it ends, then the output. Fails when the run ends other than
/// `completed`, or parks.
async fn wait(args: &Args, api: &Client, id: &str) -> Result<(), Failure> {
    let timeout = args.flag("timeout").map_or(Ok(300), |t| t.parse().map_err(|_| format!("--timeout {t}")))?;
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let mut shown = BTreeSet::new();
    loop {
        let run = api.get(id).await?;
        for step in run["steps"].as_array().into_iter().flatten() {
            let ended = matches!(step["status"].as_str(), Some("done" | "failed" | "timed_out"));
            if ended && shown.insert(step["step"].as_u64()) && !args.has("json") {
                println!("{}", step_line(step));
            }
        }
        let status = run["status"].as_str().unwrap_or_default();
        match status {
            "running" | "paused" if Instant::now() < deadline => {}
            "running" | "paused" => return Err(format!("run {id} still {status} after {timeout} s")),
            "completed" => {
                match args.has("json") {
                    true => println!("{run}"),
                    false => println!("completed: {}", run["output"]),
                }
                return Ok(());
            }
            "parked" => return Err(format!("run {id} parked: {}", run["parked"].as_str().unwrap_or_default())),
            other => return Err(format!("run {id} {other}: {}", run["error"].as_str().unwrap_or_default())),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn get(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let id = args.word(1, "a run id")?;
    if args.has("wait") {
        return wait(args, &api, id).await;
    }
    let run = api.get(id).await?;
    if args.has("json") {
        println!("{run}");
        return Ok(());
    }
    println!("{} {}@{}: {}", run["id"], run["workflow"], run["version"], run["status"]);
    for step in run["steps"].as_array().into_iter().flatten() {
        println!("  {}", step_line(step));
    }
    for (key, label) in [("output", "output"), ("error", "error"), ("parked", "parked")] {
        if !run[key].is_null() {
            println!("{label}: {}", run[key]);
        }
    }
    Ok(())
}

async fn signal(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let (id, name) = (args.word(1, "a run id")?, args.word(2, "a signal name")?);
    let payload = args.words.get(3).map_or(Ok(Value::Null), |text| json_arg(text))?;
    let said = api.signal(id, name, args.flag("signal-id"), payload).await?;
    match (args.has("json"), said["duplicate"].as_bool()) {
        (true, _) => println!("{said}"),
        (false, Some(true)) => println!("duplicate: the run has signal {}", said["signal_id"]),
        (false, _) => println!("sent {}", said["signal_id"]),
    }
    Ok(())
}

async fn end(args: &Args, terminate: bool) -> Result<(), Failure> {
    let api = client(args)?;
    let id = args.word(1, "a run id")?;
    let reason = args.flag("reason").unwrap_or_default();
    match terminate {
        true => api.terminate(id, reason).await?,
        false => api.cancel(id, reason).await?,
    };
    println!("{} {id}", if terminate { "terminated" } else { "cancel sent to" });
    Ok(())
}

/// `k=v,..` as the list API's query string; `from` (a UTC time) becomes `since_ms`.
fn filter_query(filter: &str) -> Result<String, Failure> {
    let mut query = Vec::new();
    for pair in filter.split(',').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').ok_or(format!("--filter {pair}: not k=v"))?;
        let (k, v) = match k {
            "from" => ("since_ms", unix_ms(v).ok_or(format!("from={v}: not YYYY-MM-DDTHH:MM[:SS], UTC"))?.to_string()),
            _ => (k, v.to_owned()),
        };
        query.push(format!("{}={}", segment(k), segment(&v)));
    }
    Ok(query.join("&"))
}

/// `2026-10-09T10:00` or `2026-10-09T10:00:30`, UTC, in Unix ms.
fn unix_ms(text: &str) -> Option<u64> {
    let (date, time) = text.trim_end_matches('Z').split_once('T')?;
    let mut d = date.split('-').map(|n| n.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let mut t = time.split(':').map(|n| n.parse::<i64>().ok());
    let (h, min, sec) = (t.next()??, t.next()??, t.next().unwrap_or(Some(0))?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) || h > 23 || min > 59 || sec > 60 {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    u64::try_from(((days * 24 + h) * 60 + min) * 60 + sec).ok().map(|s| s * 1_000)
}

/// Every row the list gives under the query, page by page.
async fn listed(api: &Client, query: &str) -> Result<Vec<Value>, Failure> {
    let (mut rows, mut cursor) = (Vec::new(), None::<String>);
    loop {
        let page = match &cursor {
            Some(c) => format!("{query}&limit=500&cursor={}", segment(c)),
            None => format!("{query}&limit=500"),
        };
        let mut said = api.list(page.trim_start_matches('&')).await?;
        if let Value::Array(runs) = said["runs"].take() {
            rows.extend(runs);
        }
        match said["next"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return Ok(rows),
        }
    }
}

/// `retry`, `resume`, `pause` or `delete`, on one run or on each the filter lists.
async fn act(args: &Args, action: &str) -> Result<(), Failure> {
    let api = client(args)?;
    let send = |id: String| {
        let api = api.clone();
        async move {
            let path = format!("/runs/{}", segment(&id));
            let said = match action {
                "delete" => api.send(Method::DELETE, &path, None).await,
                _ => api.send(Method::POST, &format!("{path}/{action}"), Some(json!({}))).await,
            };
            (id, said)
        }
    };
    let Some(filter) = args.flag("filter") else {
        let id = args.word(1, "a run id, or --filter")?;
        match action {
            "delete" => api.delete(id).await?,
            _ => api.act(id, action).await?,
        };
        println!("{action}: {id}");
        return Ok(());
    };
    let rows = listed(&api, &filter_query(filter)?).await?;
    let ids = rows.iter().filter_map(|r| r["id"].as_str().map(String::from));
    let (mut done, mut skipped, mut refused) = (0, 0, Vec::new());
    // 50 a second, so a bulk action does not crowd out the runs' own calls.
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    for id in ids {
        tick.tick().await;
        match send(id).await {
            (_, Ok((status, _))) if status.is_success() => done += 1,
            (_, Ok((StatusCode::CONFLICT | StatusCode::NOT_FOUND, _))) => skipped += 1,
            (id, Ok((status, said))) => refused.push(format!("{id}: {} ({status})", said["message"])),
            (id, Err(e)) => refused.push(format!("{id}: {e}")),
        }
    }
    match args.has("json") {
        true => println!("{}", json!({ "done": done, "skipped": skipped, "refused": refused.len() })),
        false => println!("{action}: {done} done, {skipped} skipped, {} refused", refused.len()),
    }
    for line in refused.iter().take(10) {
        eprintln!("  {line}");
    }
    Ok(())
}

fn ms_ago(ms: &Value) -> String {
    let now = unix_us() / 1_000;
    match ms.as_u64() {
        Some(ms) => format!("{} s ago", now.saturating_sub(ms) / 1_000),
        None => "-".into(),
    }
}

async fn list(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let mut query = filter_query(args.flag("filter").unwrap_or_default())?;
    if let Some(limit) = args.flag("limit") {
        query = format!("{query}&limit={}", segment(limit));
    }
    let said = api.list(query.trim_start_matches('&')).await?;
    if args.has("json") {
        println!("{said}");
        return Ok(());
    }
    for run in said["runs"].as_array().into_iter().flatten() {
        let workflow = format!("{}@{}", run["workflow"].as_str().unwrap_or("?"), run["version"]);
        let status = run["status"].as_str().unwrap_or("?");
        println!(
            "{:<40} {workflow:<16} {status:<10} {}",
            run["id"].as_str().unwrap_or("?"),
            ms_ago(&run["updated_ms"])
        );
    }
    if let Some(next) = said["next"].as_str() {
        println!("more: --filter ..,cursor={next}");
    }
    Ok(())
}

async fn history(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let id = args.word(1, "a run id")?;
    let mut after = 0;
    loop {
        let said = api.history(id, after, 500).await?;
        for (i, entry) in said["entries"].as_array().into_iter().flatten().enumerate() {
            println!("{} {entry}", after + i as u64);
        }
        match said["next"].as_u64() {
            Some(next) => after = next,
            None => return Ok(()),
        }
    }
}

async fn page(args: &Args, name: &str) -> Result<(), Failure> {
    let said = client(args)?.page(name).await?;
    if args.has("json") {
        println!("{said}");
        return Ok(());
    }
    let rows = |key: &str| said[key].as_array().cloned().unwrap_or_default();
    let text = |v: &Value| v.as_str().map_or_else(|| v.to_string(), String::from);
    match name {
        "workflows" => {
            for w in rows("workflows") {
                let runs: Vec<String> =
                    w["runs"].as_object().into_iter().flatten().map(|(s, n)| format!("{s} {n}")).collect();
                println!("{:<16} versions {} runs: {}", text(&w["name"]), w["versions"], runs.join(", "));
            }
        }
        "queues" => {
            for q in rows("queues") {
                println!(
                    "{:<16} backlog {} oldest {} ms pollers {} workers {}",
                    text(&q["name"]),
                    q["backlog"],
                    q["oldest_ms"],
                    q["pollers"],
                    q["workers"]
                );
            }
        }
        "workers" => {
            for w in rows("workers") {
                let state = if w["draining"] == true { "draining" } else { "serving" };
                println!("{:<8} {state:<9} queues {} slots {}", text(&w["node"]), w["queues"], w["slots"]);
            }
        }
        "cluster" => {
            let up: Vec<String> = rows("replicas")
                .iter()
                .map(|r| format!("{} {}", text(&r["node"]), if r["up"] == true { "up" } else { "down" }))
                .collect();
            println!("history: {}; quorum {}", up.join(", "), said["quorum"]);
            for m in rows("members") {
                println!(
                    "{:<8} {} {}",
                    text(&m["node"]),
                    m["roles"],
                    if m["available"] == true { "up" } else { "leaving" }
                );
            }
        }
        _ => println!("{said}"),
    }
    Ok(())
}

/// The input `wf load` gives each run of a sample, unless `input=` is given.
fn load_input(workflow: &str, n: u64) -> Value {
    match workflow {
        // One order in ten is declined: its stock is released.
        "checkout" => json!({
            "sku": "tea", "qty": 1, "account": if n % 10 == 9 { "declined".into() } else { format!("a{n}") },
            "parcels": [{ "to": "Oslo" }], "ship_after_s": 1, "charge_ms": 100,
        }),
        "onboarding" => json!({ "user": format!("u{n}"), "follow_up_s": 5 }),
        "batch" => json!({ "count": 10 }),
        "sleeper" => json!({ "secs": 5 }),
        _ => Value::Null,
    }
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    let at = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len().max(1)) - 1;
    sorted.get(at).map_or(0.0, |us| *us as f64 / 1_000.0)
}

#[derive(Default)]
struct Load {
    /// Start acks, µs.
    acks: Vec<u64>,
    statuses: BTreeMap<u16, u64>,
    failed: Vec<String>,
    ids: Vec<String>,
    log: Vec<Ack>,
}

fn unix_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64)
}

/// `wf load`: an open loop of starts at `rate` a second for `secs`, each run's id `<prefix>-<n>`.
async fn load(args: &Args) -> Result<(), Failure> {
    let api = client(args)?;
    let workflow = args.word(1, "a workflow")?.to_owned();
    let kv: BTreeMap<&str, &str> = args.words.iter().skip(2).filter_map(|w| w.split_once('=')).collect();
    let number = |key: &str, default: f64| -> Result<f64, Failure> {
        kv.get(key).map_or(Ok(default), |v| v.parse().map_err(|_| format!("{key}={v}: not a number")))
    };
    let (rate, secs, wait) = (number("rate", 100.0)?, number("secs", 10.0)?, number("wait", 0.0)?);
    let input = kv.get("input").map(|text| json_arg(text)).transpose()?;
    let prefix = kv.get("prefix").map_or_else(|| format!("load-{:x}", unix_us()), |p| (*p).to_owned());
    let total = (rate * secs).round() as u64;
    let state = Arc::new(Mutex::new(Load::default()));
    let mut tasks = tokio::task::JoinSet::new();
    let begun = tokio::time::Instant::now();
    let mut skipped = 0;
    for n in 0..total {
        tokio::time::sleep_until(begun + Duration::from_secs_f64(n as f64 / rate)).await;
        // Bounded: past this many starts in flight the next is not sent, and counted. The ended ones go first, or
        // a full set would skip every start after it.
        while tasks.try_join_next().is_some() {}
        if tasks.len() >= 8_192 {
            skipped += 1;
            continue;
        }
        let (api, state, workflow) = (api.clone(), state.clone(), workflow.clone());
        let input = input.clone().unwrap_or_else(|| load_input(&workflow, n));
        let id = format!("{prefix}-{n}");
        tasks.spawn(async move {
            let hash = input_hash(&workflow, &serde_json::to_vec(&input).unwrap_or_default());
            let body = json!({ "workflow": workflow, "id": id, "input": input });
            let sent = std::time::Instant::now();
            // A start that got no answer, or `unavailable` or `busy`, may have started the run: the same id and
            // input again, for 30 s, answer 200 if it did. A `limited` start is not sent again.
            let mut said = api.send(Method::POST, "/runs", Some(body.clone())).await;
            while sent.elapsed() < Duration::from_secs(30)
                && matches!(&said, Err(_) | Ok((StatusCode::SERVICE_UNAVAILABLE, _)))
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
                said = api.send(Method::POST, "/runs", Some(body.clone())).await;
            }
            let took = sent.elapsed().as_micros() as u64;
            let ok = matches!(&said, Ok((status, _)) if status.is_success());
            {
                let mut s = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                match &said {
                    Ok((status, said)) => {
                        *s.statuses.entry(status.as_u16()).or_default() += 1;
                        if !status.is_success() && s.failed.len() < 10 {
                            s.failed.push(format!("{id}: {status} {said}"));
                        }
                    }
                    Err(e) => {
                        *s.statuses.entry(0).or_default() += 1;
                        if s.failed.len() < 10 {
                            s.failed.push(format!("{id}: {e}"));
                        }
                    }
                }
                if ok {
                    s.acks.push(took);
                    s.ids.push(id.clone());
                    s.log.push(Ack::Started { run: id.clone(), input_hash: hash, t: unix_us() });
                }
            }
            if ok && workflow == "checkout" {
                let signal_id = format!("approve-{id}");
                let path = format!("/runs/{}/signals/approve", segment(&id));
                // Through a node's crash or restart: a minute.
                for _ in 0..120 {
                    let said = api.send(Method::POST, &path, Some(json!({ "signal_id": signal_id }))).await;
                    match said {
                        Ok((status, _)) if status.is_success() => {
                            let mut s = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            s.log.push(Ack::Signal { run: id.clone(), signal_id, t: unix_us() });
                            break;
                        }
                        // Ended before the approval came: a declined order.
                        Ok((StatusCode::CONFLICT, _)) => break,
                        _ => tokio::time::sleep(Duration::from_millis(500)).await,
                    }
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    let elapsed = begun.elapsed().as_secs_f64();
    let mut s = std::mem::take(&mut *state.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
    s.acks.sort_unstable();
    if let Some(file) = kv.get("acks") {
        let lines: String = s.log.iter().filter_map(|a| serde_json::to_string(a).ok()).map(|l| l + "\n").collect();
        let mut options = std::fs::OpenOptions::new();
        let opened = options.create(true).append(true).open(file);
        opened
            .and_then(|mut f| std::io::Write::write_all(&mut f, lines.as_bytes()))
            .map_err(|e| format!("{file}: {e}"))?;
    }
    let mut said = json!({
        "workflow": workflow, "prefix": prefix, "sent": total - skipped, "skipped": skipped, "secs": elapsed,
        "statuses": s.statuses.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
        "ack_p50_ms": percentile(&s.acks, 0.5), "ack_p99_ms": percentile(&s.acks, 0.99),
        "ack_max_ms": percentile(&s.acks, 1.0), "started_per_s": s.acks.len() as f64 / elapsed,
    });
    if wait > 0.0 {
        said["ended"] = ended(&api, &prefix, s.ids.len(), Duration::from_secs_f64(wait)).await?;
    }
    for line in &s.failed {
        eprintln!("  {line}");
    }
    println!("{said}");
    Ok(())
}

/// Waits until every run under `prefix` ended, up to `within`: how many ended each way, and the p50 and p99 from
/// start to end of the completed ones, as their rows say.
async fn ended(api: &Client, prefix: &str, started: usize, within: Duration) -> Result<Value, Failure> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        // Through a restart of the cluster: asked again until the deadline.
        let rows = match listed(api, &format!("prefix={}", segment(prefix))).await {
            Ok(rows) => rows,
            Err(e) if tokio::time::Instant::now() >= deadline => return Err(e),
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let (mut by, mut took, mut open) = (BTreeMap::<String, u64>::new(), Vec::new(), Vec::new());
        for run in &rows {
            let status = run["status"].as_str().unwrap_or("?").to_owned();
            if status == "completed" {
                let (a, b) = (run["started_ms"].as_u64().unwrap_or(0), run["updated_ms"].as_u64().unwrap_or(0));
                took.push(b.saturating_sub(a) * 1_000);
            }
            if ["running", "paused", "parked"].contains(&status.as_str()) {
                open.extend(run["id"].as_str().map(String::from));
            }
            *by.entry(status).or_default() += 1;
        }
        let counted = by.values().sum::<u64>() as usize;
        if (open.is_empty() && counted >= started) || tokio::time::Instant::now() >= deadline {
            took.sort_unstable();
            // What the runs the index shows open say themselves: one that ended is an index behind.
            let mut asked = serde_json::Map::new();
            for id in open.iter().take(20) {
                let said = api.get(id).await.map_or_else(Value::String, |run| run["status"].clone());
                asked.insert(id.clone(), said);
            }
            return Ok(json!({
                "by_status": by, "complete": open.is_empty() && counted >= started, "open": asked,
                "end_p50_ms": percentile(&took, 0.5), "end_p99_ms": percentile(&took, 0.99),
            }));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The runs by the id before their last `-` (a `wf load`'s prefix): how many ended each way, the first few that did not
/// complete, and the attempts that failed other than for a declined card.
#[derive(Default)]
struct Groups(BTreeMap<String, Group>);

#[derive(Default)]
struct Group {
    status: BTreeMap<String, u64>,
    not_completed: Vec<String>,
    failed_attempts: u64,
}

impl Groups {
    fn add(&mut self, report: &workflows::check::Report) {
        for (run, journal) in &report.journals {
            let id = String::from_utf8_lossy(run);
            let prefix = id.rsplit_once('-').map_or(id.as_ref(), |(prefix, _)| prefix).to_owned();
            let group = self.0.entry(prefix).or_default();
            let status = serde_json::to_value(Summary::of(journal).status).unwrap_or_default();
            let status = status.as_str().unwrap_or("?");
            *group.status.entry(status.to_owned()).or_default() += 1;
            if status != "completed" && group.not_completed.len() < 5 {
                group.not_completed.push(id.into_owned());
            }
            group.failed_attempts += journal
                .iter()
                .filter_map(|body| Entry::decode(body).ok())
                .filter(|e| matches!(e, Entry::ActivityFailed { error, .. } if !error.starts_with("card_declined")))
                .count() as u64;
        }
    }

    fn json(self) -> BTreeMap<String, Value> {
        let group = |g: Group| json!({ "status": g.status, "not_completed": g.not_completed, "failed_attempts": g.failed_attempts });
        self.0.into_iter().map(|(prefix, g)| (prefix, group(g))).collect()
    }
}

/// Each JSON line of each file, one at a time.
fn each_line<T: serde::de::DeserializeOwned>(files: &[String], mut f: impl FnMut(T)) -> Result<(), Failure> {
    for file in files {
        let open = std::fs::File::open(file).map_err(|e| format!("{file}: {e}"))?;
        for line in read_lines(std::io::BufReader::new(open)) {
            f(line.map_err(|e| format!("{file}: {e}"))?);
        }
    }
    Ok(())
}

/// `wf check`: the history nodes' directories (a node's, or its `journal`), stopped. It checks a batch of runs at a
/// time, so it holds one batch's journals and not the replicas' whole.
async fn check_dirs(args: &Args) -> Result<(), Failure> {
    let dirs: Vec<PathBuf> = args
        .words
        .iter()
        .skip(1)
        .map(PathBuf::from)
        .map(|dir| if dir.join("journal").is_dir() { dir.join("journal") } else { dir })
        .collect();
    if dirs.is_empty() {
        return Err("wf check <dir>..: the history nodes' directories".into());
    }
    let replicas = Replicas::open(&dirs).await.map_err(|e| format!("reading the replicas: {e}"))?;
    if let Some(run) = args.flag("run") {
        let dumps = replicas
            .dump(&[Bytes::copy_from_slice(run.as_bytes())])
            .await
            .map_err(|e| format!("reading the replicas: {e}"))?;
        for (dir, dump) in dirs.iter().zip(&dumps) {
            let Some(found) = dump.runs.get(run.as_bytes()) else {
                println!("{}: none", dir.display());
                continue;
            };
            println!("{}: fence {}", dir.display(), found.fence);
            for (epoch, records) in &found.segments {
                for (i, body) in records.iter().enumerate() {
                    println!("  {epoch} {} {:?}", i + 1, Entry::decode(body));
                }
            }
        }
        return Ok(());
    }
    let (mut acks, mut acked) = (BTreeMap::<String, Vec<Ack>>::new(), 0);
    each_line(args.all("acks"), |ack: Ack| {
        acked += 1;
        acks.entry(ack.run().to_owned()).or_default().push(ack);
    })?;
    let (mut witness, mut executions) = (BTreeMap::<String, Vec<Witness>>::new(), 0);
    each_line(args.all("witness"), |w: Witness| {
        executions += 1;
        witness.entry(w.run.clone()).or_default().push(w);
    })?;
    let witnessed = !args.all("witness").is_empty();
    let runs: Vec<Bytes> = replicas.runs().into_iter().collect();
    let (mut entries, mut violations, mut groups) = (0, Vec::new(), Groups::default());
    // Each replica's records: one behind the others lacks what repair has not copied yet.
    let mut records = vec![0; dirs.len()];
    for batch in runs.chunks(4096) {
        let dumps = replicas.dump(batch).await.map_err(|e| format!("reading the replicas: {e}"))?;
        let ids = || batch.iter().filter_map(|run| std::str::from_utf8(run).ok());
        let batch_acks: Vec<Ack> = ids().filter_map(|run| acks.remove(run)).flatten().collect();
        let batch_witness: Vec<Witness> = ids().filter_map(|run| witness.remove(run)).flatten().collect();
        let mut report = check(&dumps, &batch_acks, witnessed.then_some(batch_witness.as_slice()));
        report.violations.extend(audit_checkout(&report));
        groups.add(&report);
        for (n, dump) in records.iter_mut().zip(&dumps) {
            *n += dump.runs.values().flat_map(|r| r.segments.values()).map(Vec::len).sum::<usize>();
        }
        entries += report.entries;
        violations.extend(report.violations);
    }
    // Acks of runs no replica holds.
    let rest: Vec<Ack> = acks.into_values().flatten().collect();
    violations.extend(check(&[], &rest, None).violations);
    let groups = groups.json();
    match args.has("json") {
        true => println!(
            "{}",
            json!({ "runs": runs.len(), "entries": entries, "records": records, "acks": acked,
                "witnessed": executions, "groups": groups, "violations": violations.iter().map(|v| json!({
                    "invariant": v.invariant, "run": v.run, "what": v.what })).collect::<Vec<_>>() })
        ),
        false => {
            println!("{} runs, {entries} entries, {} violations", runs.len(), violations.len());
            for v in violations.iter().take(20) {
                println!("  {} run {}: {}", v.invariant, v.run, v.what);
            }
            println!("{acked} acks, {executions} executions; records per replica {records:?}");
            for (prefix, group) in &groups {
                println!("  {prefix}: {group}");
            }
        }
    }
    if violations.is_empty() { Ok(()) } else { Err(format!("{} violations", violations.len())) }
}

async fn run(args: &Args) -> Result<(), Failure> {
    let command = args.words.first().map(String::as_str).unwrap_or("help");
    match command {
        "login" => login(args).await,
        "logout" => logout(args).await,
        "list" => list(args).await,
        "history" => history(args).await,
        "retry" | "resume" | "pause" | "delete" => act(args, command).await,
        "workflows" | "queues" | "workers" | "metrics" | "cluster" => page(args, command).await,
        "load" => load(args).await,
        "check" => check_dirs(args).await,
        "start" => start(args).await,
        "get" => get(args).await,
        "signal" => signal(args).await,
        "cancel" => end(args, false).await,
        "terminate" => end(args, true).await,
        "hash-password" => {
            let password = std::env::var("WF_PASSWORD").or_else(|_| line())?;
            println!("{}", hash_password(&password)?);
            Ok(())
        }
        "help" => {
            println!("{HELP}");
            Ok(())
        }
        other => Err(format!("no command {other:?}: see `wf help`")),
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let done = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => run(&args).await,
        Err(e) => Err(e),
    };
    match done {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wf: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
