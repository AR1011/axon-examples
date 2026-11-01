//! `edge`: the operator's command line, over the control node's HTTP API. `edge help` lists the commands.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use edge_workers::auth::hash_password;
use edge_workers::engine::starling::percent_encode;
use edge_workers::templates::{TEMPLATES, template};
use serde_json::{Value, json};

const HELP: &str = "\
edge: deploy and run workers on an edge-workers cluster

  edge login --url http://host:port [--password P]   sign in to the control node; the token is kept in
                                                      ~/.config/edge-workers/credentials.json (EDGE_CONFIG)
  edge templates                                      the templates a worker can start from
  edge init <template> [file]                         write a template's source to a file
  edge deploy <file> [--name N] [--message M] [--var K=V].. [--object NAME=Class].. [--no-deploy] [--canary PERCENT]
                                                      a new version, deployed at once unless told otherwise;
                                                      --var replaces the variables whole, --object the object
                                                      bindings: env.NAME is the objects of the exported Class
  edge list                                           every worker with what takes its traffic and its URL
  edge versions <name>                                a worker's URLs and its versions, newest first
  edge rollback <name> [--to VERSION]                 back to the version before the deployed one
  edge canary <name> <percent> [--version V]          a share of the requests to the latest version (or V);
                                                      0 ends the canary, 100 makes it the deployed version
  edge delete <name>
  edge var list|set|delete <name> [KEY] [value]       plain variables; a change is a new deployed version
  edge secret put|delete <name> <KEY> [value]         a secret is never shown again; the value is read from
                                                      stdin when not given
  edge invoke <name> [path] [-X METHOD] [-H 'k: v'].. [-d body]    one request for the deployed worker
  edge preview <file> [path] [--name N] [-X ..] [-H ..] [-d ..]    run a file once without deploying it
  edge tail [name] [--status S] [--q TEXT] [--node N] [--count N]  invocations as they end, with their logs
  edge objects <name>                                 a worker's objects: where each runs, under which lease
  edge object get|delete <name> <Class> <id>          what an object stores, and under which epoch; delete
                                                      removes it all: if the object runs, its next request
                                                      fails, and the one after starts from nothing
  edge metrics <name>                                 requests, errors, CPU and wall time, by runner
  edge cluster                                        the nodes, their roles, each gateway's URL, what each
                                                      runner serves
  edge load --gateway http://host:port --mix a=3,b=1 [--rps 100] [--duration 10] [--path /] [--account A] [--method GET]
            [--accounts N] [--paths N]                requests at a steady rate, then a latency summary; over N
                                                      accounts (A-0..), and for N paths (the path and 0..)
  edge hash-password                                  an argon2id hash of EDGE_PASSWORD or of a line of stdin,
                                                      for the control node's EDGE_ADMIN_HASH

Every command takes --url and --json (the API's answer as it is). EDGE_URL and EDGE_TOKEN override the kept ones.";

type Failure = String;

/// The words of a command line: what is not a flag, and each `--flag value` (or `--flag=value`) as often as given.
struct Args {
    words: Vec<String>,
    flags: BTreeMap<String, Vec<String>>,
}

const SWITCHES: [&str; 3] = ["json", "no-deploy", "help"];

impl Args {
    fn parse(argv: impl Iterator<Item = String>) -> Result<Self, Failure> {
        let (mut words, mut flags) = (Vec::new(), BTreeMap::<String, Vec<String>>::new());
        let mut argv = argv.peekable();
        while let Some(arg) = argv.next() {
            let name = match arg.as_str() {
                "-X" => "method",
                "-H" => "header",
                "-d" => "data",
                other => match other.strip_prefix("--") {
                    Some(name) if !name.is_empty() => name,
                    _ => {
                        words.push(arg);
                        continue;
                    }
                },
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

    fn number<T: std::str::FromStr>(&self, name: &str) -> Result<Option<T>, Failure> {
        self.flag(name).map(|value| value.parse().map_err(|_| format!("--{name} {value}: not a number"))).transpose()
    }

    /// The word at `at` after the command, or what to say when it is missing.
    fn word(&self, at: usize, what: &str) -> Result<&str, Failure> {
        self.words.get(at).map(String::as_str).ok_or(format!("missing {what}: see `edge help`"))
    }
}

fn config_file() -> Result<PathBuf, Failure> {
    if let Ok(file) = std::env::var("EDGE_CONFIG") {
        return Ok(file.into());
    }
    let home = std::env::var("HOME").map_err(|_| "no HOME to keep the credentials in: set EDGE_CONFIG")?;
    Ok(PathBuf::from(home).join(".config/edge-workers/credentials.json"))
}

fn kept() -> Value {
    let read = config_file().ok().and_then(|file| std::fs::read(file).ok());
    read.and_then(|text| serde_json::from_slice(&text).ok()).unwrap_or(Value::Null)
}

struct Api {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Api {
    fn new(args: &Args) -> Result<Self, Failure> {
        let kept = kept();
        let env = |name| std::env::var(name).ok();
        let url = args.flag("url").map(String::from).or_else(|| env("EDGE_URL"));
        let url = url.or_else(|| kept["url"].as_str().map(String::from));
        let url = url.ok_or("no control node: `edge login --url http://host:port`, or set EDGE_URL")?;
        // A token belongs to the node that signed it.
        let same = kept["url"].as_str() == Some(url.as_str());
        let token = env("EDGE_TOKEN").or_else(|| kept["token"].as_str().filter(|_| same).map(String::from));
        Ok(Self { http: reqwest::Client::new(), url: url.trim_end_matches('/').to_owned(), token })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.http.request(method, format!("{}/api{path}", self.url));
        match &self.token {
            Some(token) => request.header("authorization", format!("Bearer {token}")),
            None => request,
        }
    }

    /// The API's answer, or its refusal as one line.
    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value, Failure> {
        let request = self.request(method, path);
        let request = match body {
            Some(body) => request.header("content-type", "application/json").body(body.to_string()),
            None => request,
        };
        let response = request.send().await.map_err(|e| format!("cannot reach {}: {e}", self.url))?;
        let status = response.status();
        let text = response.bytes().await.map_err(|e| format!("{}: {e}", self.url))?;
        let said: Value =
            serde_json::from_slice(&text).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&text).into()));
        if status.is_success() {
            return Ok(said);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED && path != "/login" {
            return Err(signed_out(&self.url));
        }
        let message = said["message"].as_str().map(String::from).unwrap_or_else(|| said.to_string());
        Err(format!(
            "{message} ({})",
            said["error"].as_str().map_or(status.to_string(), |code| format!("{status}, {code}"))
        ))
    }

    async fn get(&self, path: &str) -> Result<Value, Failure> {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, Failure> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }
}

fn password(args: &Args) -> Result<String, Failure> {
    if let Some(password) = args.flag("password").map(String::from).or_else(|| std::env::var("EDGE_PASSWORD").ok()) {
        return Ok(password);
    }
    if std::io::stdin().is_terminal() {
        eprint!("password: ");
    }
    line()
}

/// One line of stdin, without its end.
fn line() -> Result<String, Failure> {
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| format!("reading stdin: {e}"))?;
    Ok(line.trim_end_matches(['\n', '\r']).to_owned())
}

/// `2026-10-04 09:30:00` in UTC, from Unix milliseconds.
fn utc(ms: u64) -> String {
    let (days, of_day) = ((ms / 86_400_000) as i64, ms / 1000 % 86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let (day, month) = (doy - (153 * mp + 2) / 5 + 1, if mp < 10 { mp + 3 } else { mp - 9 });
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}", of_day / 3600, of_day / 60 % 60, of_day % 60)
}

fn ms(us: &Value) -> String {
    us.as_u64().map_or("-".into(), |us| format!("{:.1} ms", us as f64 / 1000.0))
}

fn show(args: &Args, said: &Value, human: impl FnOnce(&Value)) {
    match args.has("json") {
        true => println!("{said}"),
        false => human(said),
    }
}

async fn login(args: &Args) -> Result<(), Failure> {
    let url = args.flag("url").map(String::from).or_else(|| std::env::var("EDGE_URL").ok());
    let url = url.ok_or("edge login --url http://host:port")?.trim_end_matches('/').to_owned();
    let api = Api { http: reqwest::Client::new(), url: url.clone(), token: None };
    let said = api.post("/login", json!({ "password": password(args)?, "token": true })).await?;
    let file = config_file()?;
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let kept = json!({ "url": url, "token": said["token"], "expires_ms": said["expires_ms"] });
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        let opened = options.write(true).create(true).truncate(true).mode(0o600).open(&file);
        let written = opened.and_then(|mut file| std::io::Write::write_all(&mut file, kept.to_string().as_bytes()));
        written.map_err(|e| format!("{}: {e}", file.display()))?;
    }
    println!("signed in to {url} until {} UTC", utc(said["expires_ms"].as_u64().unwrap_or(0)));
    Ok(())
}

fn init(args: &Args) -> Result<(), Failure> {
    let name = args.word(1, "a template")?;
    let names = || TEMPLATES.iter().map(|t| t.name).collect::<Vec<_>>().join(", ");
    let template = template(name).ok_or_else(|| format!("no template {name:?}: {}", names()))?;
    let file = args.words.get(2).cloned().unwrap_or_else(|| format!("{name}.js"));
    let mut options = std::fs::OpenOptions::new();
    let opened = options.write(true).create_new(true).open(&file);
    let written = opened.and_then(|mut file| std::io::Write::write_all(&mut file, template.source.as_bytes()));
    written.map_err(|e| format!("{file}: {e}"))?;
    let objects: String = template.objects.iter().map(|(name, class)| format!(" --object {name}={class}")).collect();
    println!("wrote {file}: deploy it with `edge deploy {file}{objects}`");
    Ok(())
}

/// `NAME=value` flags as a map; `None` if one has no `=`.
fn pairs(given: &[String]) -> Option<BTreeMap<String, String>> {
    given.iter().map(|pair| pair.split_once('=').map(|(k, v)| (k.to_owned(), v.to_owned()))).collect()
}

fn signed_out(url: &str) -> Failure {
    format!("not signed in to {url}: run `edge login --url {url}`")
}

fn source(file: &str) -> Result<String, Failure> {
    std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))
}

/// A worker's name from `--name`, or from the file's name without its extension.
fn named(args: &Args, file: &str) -> Result<String, Failure> {
    let stem = || std::path::Path::new(file).file_stem().and_then(|stem| stem.to_str()).map(String::from);
    args.flag("name").map(String::from).or_else(stem).ok_or(format!("no name in {file:?}: give --name"))
}

async fn deploy(args: &Args, api: &Api) -> Result<(), Failure> {
    let file = args.word(1, "a file")?;
    let name = named(args, file)?;
    let canary: Option<u8> = args.number("canary")?;
    let mut body = json!({
        "source": source(file)?,
        "message": args.flag("message").unwrap_or(""),
        "deploy": !(args.has("no-deploy") || canary.is_some()),
    });
    if args.has("var") {
        body["vars"] = json!(pairs(args.all("var")).ok_or("--var takes KEY=value")?);
    }
    if args.has("object") {
        body["objects"] = json!(pairs(args.all("object")).ok_or("--object takes NAME=Class")?);
    }
    let path = format!("/workers/{}", percent_encode(&name));
    let mut said = api.post(&format!("{path}/versions"), body).await?;
    let version = said["version"].clone();
    if let Some(percent) = canary {
        let stable = api.get(&path).await?["deployment"]["stable"].clone();
        if stable.is_null() {
            return Err(format!(
                "version {version} of {name} is uploaded, but nothing is deployed for it to be a canary of"
            ));
        }
        let split = json!({ "version": stable, "canary": { "version": version, "percent": percent } });
        said["seq"] = api.post(&format!("{path}/deploy"), split).await?["seq"].clone();
    }
    if !said["seq"].is_null() {
        // Where the gateways serve it.
        said["urls"] = api.get(&path).await?["urls"].clone();
    }
    show(args, &said, |said| {
        match (&said["seq"], canary) {
            (Value::Null, _) => println!("uploaded {name} version {version}, not deployed"),
            (seq, Some(percent)) => println!("deployed {name} version {version} to {percent} % (seq {seq})"),
            (seq, None) => println!("deployed {name} version {version} (seq {seq})"),
        }
        urls(said).for_each(|url| println!("  {url}"));
    });
    Ok(())
}

/// The URLs the gateways serve a worker at.
fn urls(worker: &Value) -> impl Iterator<Item = &str> {
    worker["urls"].as_array().into_iter().flatten().filter_map(|at| at["url"].as_str())
}

fn deployment(worker: &Value) -> String {
    let deployment = &worker["deployment"];
    match (&deployment["stable"], &deployment["canary"]) {
        (Value::Null, _) => "not deployed".into(),
        (stable, Value::Null) => format!("v{stable}"),
        (stable, canary) => format!("v{stable}, v{} at {} %", canary["version"], canary["percent"]),
    }
}

async fn list(args: &Args, api: &Api) -> Result<(), Failure> {
    let said = api.get("/workers").await?;
    show(args, &said, |said| {
        println!("{:<24} {:<22} {:>10} {:>8} {:>10}  URL", "NAME", "DEPLOYED", "REQUESTS", "ERRORS", "CPU P50");
        for worker in said["workers"].as_array().into_iter().flatten() {
            let metrics = &worker["metrics"];
            // The first gateway's; `edge versions <name>` and --json have them all.
            println!(
                "{:<24} {:<22} {:>10} {:>8} {:>10}  {}",
                worker["name"].as_str().unwrap_or(""),
                deployment(worker),
                metrics["requests"].to_string(),
                metrics["errors"].to_string(),
                ms(&metrics["cpu_p50_us"]),
                urls(worker).next().unwrap_or("-")
            );
        }
    });
    Ok(())
}

async fn versions(args: &Args, api: &Api) -> Result<(), Failure> {
    let said = api.get(&format!("/workers/{}", percent_encode(args.word(1, "a worker")?))).await?;
    show(args, &said, |said| {
        println!("{} is {}", said["name"].as_str().unwrap_or(""), deployment(said));
        urls(said).for_each(|url| println!("  {url}"));
        for version in said["versions"].as_array().into_iter().flatten() {
            let created = utc(version["created_ms"].as_u64().unwrap_or(0));
            println!(
                "  v{:<5} {created}  {:>7} bytes  build {:.1} MiB  {}",
                version["id"].to_string(),
                version["source_bytes"].to_string(),
                version["build_bytes"].as_u64().unwrap_or(0) as f64 / f64::from(1 << 20),
                version["message"].as_str().unwrap_or("")
            );
        }
    });
    Ok(())
}

async fn rollback(args: &Args, api: &Api) -> Result<(), Failure> {
    let name = args.word(1, "a worker")?;
    let to: Option<u32> = args.number("to")?;
    let said = api.post(&format!("/workers/{}/rollback", percent_encode(name)), json!({ "version": to })).await?;
    show(args, &said, |said| {
        println!(
            "rolled {name} back from version {} to version {} (seq {})",
            said["from"], said["version"], said["seq"]
        );
    });
    Ok(())
}

async fn canary(args: &Args, api: &Api) -> Result<(), Failure> {
    let name = args.word(1, "a worker")?;
    let percent: u8 = args.word(2, "a percent")?.parse().map_err(|_| "a percent is 0 to 100")?;
    let path = format!("/workers/{}", percent_encode(name));
    let worker = api.get(&path).await?;
    let stable = worker["deployment"]["stable"].clone();
    let split = &worker["deployment"]["canary"]["version"];
    let version = match args.number::<u32>("version")? {
        Some(version) => json!(version),
        None if !split.is_null() => split.clone(),
        None => worker["latest"].clone(),
    };
    let body = match percent {
        0 => json!({ "version": stable }),
        100 => json!({ "version": version }),
        percent => json!({ "version": stable, "canary": { "version": version, "percent": percent } }),
    };
    let said = api.post(&format!("{path}/deploy"), body).await?;
    show(args, &said, |said| match percent {
        0 => println!("{name}: every request to version {stable} (seq {})", said["seq"]),
        100 => println!("{name}: every request to version {version} (seq {})", said["seq"]),
        percent => {
            println!("{name}: {percent} % to version {version}, the rest to version {stable} (seq {})", said["seq"])
        }
    });
    Ok(())
}

async fn vars(args: &Args, api: &Api, secret: bool) -> Result<(), Failure> {
    let (action, name) = (args.word(1, "list, set, put or delete")?, args.word(2, "a worker")?);
    let path = format!("/workers/{}/vars", percent_encode(name));
    let said = match action {
        "list" => api.get(&path).await?,
        "set" | "put" => {
            let key = args.word(3, "a variable's name")?;
            let value = match args.words.get(4) {
                Some(value) => value.clone(),
                None if secret => {
                    let mut all = String::new();
                    std::io::stdin().read_to_string(&mut all).map_err(|e| format!("reading stdin: {e}"))?;
                    all.trim_end_matches(['\n', '\r']).to_owned()
                }
                None => return Err("missing a value".into()),
            };
            let body = json!({ "type": if secret { "secret" } else { "plain" }, "value": value });
            api.call(reqwest::Method::PUT, &format!("{path}/{}", percent_encode(key)), Some(body)).await?
        }
        "delete" => {
            let key = args.word(3, "a variable's name")?;
            api.call(reqwest::Method::DELETE, &format!("{path}/{}", percent_encode(key)), None).await?
        }
        other => return Err(format!("no `{other}`: list, set, put or delete")),
    };
    show(args, &said, |said| match said["vars"].as_array() {
        Some(vars) => {
            for var in vars {
                let value = var["value"].as_str().map_or("(secret)".to_owned(), |value| format!("= {value}"));
                println!("{} {value}", var["name"].as_str().unwrap_or(""));
            }
        }
        None => println!("{name}: version {} deployed (seq {})", said["version"], said["seq"]),
    });
    Ok(())
}

async fn objects(args: &Args, api: &Api) -> Result<(), Failure> {
    let name = args.word(1, "a worker")?;
    let said = api.get(&format!("/workers/{}/objects", percent_encode(name))).await?;
    show(args, &said, |said| {
        for object in said["objects"].as_array().into_iter().flatten() {
            let runs = match object["node"].as_str() {
                Some(node) => format!("on {node}, epoch {}", object["epoch"]),
                None => "idle".to_owned(),
            };
            let stored = if object["stored"] == true { "" } else { "  (stores nothing)" };
            println!(
                "{} {}  {runs}{stored}",
                object["class"].as_str().unwrap_or(""),
                object["id"].as_str().unwrap_or("")
            );
        }
    });
    Ok(())
}

async fn object(args: &Args, api: &Api) -> Result<(), Failure> {
    let action = args.word(1, "get or delete")?;
    let (name, class, id) = (args.word(2, "a worker")?, args.word(3, "a class")?, args.word(4, "an id")?);
    let path = format!("/workers/{}/objects/{}/{}", percent_encode(name), percent_encode(class), percent_encode(id));
    match action {
        "get" => {
            let said = api.get(&path).await?;
            show(args, &said, |said| {
                println!("{class} {id} of {name}: {} bytes, last written under epoch {}", said["bytes"], said["epoch"]);
                for entry in said["entries"].as_array().into_iter().flatten() {
                    println!("{} = {}", entry["key"].as_str().unwrap_or(""), entry["value"]);
                }
            });
        }
        "delete" => {
            let said = api.call(reqwest::Method::DELETE, &path, None).await?;
            show(args, &said, |_| println!("deleted what {class} {id} of {name} stored"));
        }
        other => return Err(format!("no `{other}`: get or delete")),
    }
    Ok(())
}

/// The request `-X`, `-H`, `-d` and the path describe.
fn ask(args: &Args, path: Option<&String>) -> Result<Value, Failure> {
    let header = |header: &String| header.split_once(':').map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()));
    let headers: Option<BTreeMap<String, String>> = args.all("header").iter().map(header).collect();
    Ok(json!({
        "method": args.flag("method").unwrap_or(if args.has("data") { "POST" } else { "GET" }),
        "path": path.map_or("/", String::as_str),
        "headers": headers.ok_or("-H takes 'name: value'")?,
        "body": args.flag("data").unwrap_or(""),
    }))
}

/// The response of a run: what it took on stderr, its body on stdout. A run with no response of the worker's
/// own is this command's failure.
fn ran(args: &Args, said: &Value) -> Result<(), Failure> {
    show(args, said, |said| {
        let on = |key: &str, label: &str| match &said[key] {
            Value::Null => String::new(),
            value => format!(", {label} {}", value.as_str().map_or(value.to_string(), String::from)),
        };
        eprintln!(
            "HTTP {} (cpu {}, wall {}{}{}{})",
            said["status"],
            ms(&said["cpu_us"]),
            ms(&said["wall_us"]),
            on("node", "node"),
            on("version", "version"),
            on("trace", "trace")
        );
        for line in said["logs"].as_array().into_iter().flatten() {
            eprintln!("  {}", line.as_str().unwrap_or(""));
        }
        print!("{}", said["body"].as_str().unwrap_or(""));
    });
    match said["error"].as_str() {
        Some(error) => Err(format!("the worker gave no response of its own: {error}")),
        None => Ok(()),
    }
}

async fn invoke(args: &Args, api: &Api) -> Result<(), Failure> {
    let name = args.word(1, "a worker")?;
    let said = api.post(&format!("/workers/{}/invoke", percent_encode(name)), ask(args, args.words.get(2))?).await?;
    ran(args, &said)
}

async fn preview(args: &Args, api: &Api) -> Result<(), Failure> {
    let file = args.word(1, "a file")?;
    let body = json!({ "source": source(file)?, "request": ask(args, args.words.get(2))? });
    let said = api.post(&format!("/workers/{}/preview", percent_encode(&named(args, file)?)), body).await?;
    ran(args, &said)
}

fn invocation(record: &Value) {
    let text = |key: &str| record[key].as_str().unwrap_or("");
    let at = utc(record["at_ms"].as_u64().unwrap_or(0));
    println!(
        "{} {} {:<12} {} {}  {} v{} on {}  cpu {} wall {}  {}",
        &at[11..],
        record["status"],
        text("outcome"),
        text("method"),
        text("url"),
        text("worker"),
        record["version"],
        text("node"),
        ms(&record["cpu_us"]),
        ms(&record["wall_us"]),
        text("trace")
    );
    for line in record["logs"].as_array().into_iter().flatten() {
        println!("    {}", line.as_str().unwrap_or(""));
    }
}

/// Reads the API's server-sent events until the connection ends or `--count` invocations were shown.
async fn tail(args: &Args, api: &Api) -> Result<(), Failure> {
    let path = match args.words.get(1) {
        Some(name) => format!("/workers/{}/tail", percent_encode(name)),
        None => "/tail".into(),
    };
    let filters = ["status", "q", "node"].into_iter().filter_map(|key| Some((key, args.flag(key)?)));
    let query: Vec<String> = filters.map(|(key, value)| format!("{key}={}", percent_encode(value))).collect();
    let count: Option<usize> = args.number("count")?;
    let request = api.request(reqwest::Method::GET, &format!("{path}?{}", query.join("&")));
    let mut response = request.send().await.map_err(|e| format!("cannot reach {}: {e}", api.url))?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(signed_out(&api.url));
    }
    if !response.status().is_success() {
        return Err(format!("{}: {}", api.url, response.status()));
    }
    eprintln!("tailing {}: ctrl-c to stop", args.words.get(1).map_or("every worker", String::as_str));
    let (mut buffer, mut shown) = (Vec::new(), 0);
    while let Some(chunk) = response.chunk().await.map_err(|e| format!("the tail ended: {e}"))? {
        buffer.extend_from_slice(&chunk);
        while let Some(end) = buffer.windows(2).position(|pair| pair == b"\n\n") {
            let event: Vec<u8> = buffer.drain(..end + 2).collect();
            let event = String::from_utf8_lossy(&event);
            let field = |name: &str| event.lines().find_map(|line| line.strip_prefix(name)).map(str::trim);
            match (field("event:"), field("data:")) {
                (Some("invocation"), Some(data)) if args.has("json") => println!("{data}"),
                (Some("invocation"), Some(data)) => invocation(&serde_json::from_str(data).unwrap_or(Value::Null)),
                (Some("missed"), Some(missed)) => eprintln!("({missed} invocations missed: the tail fell behind)"),
                // A keep-alive comment.
                _ => continue,
            }
            shown += 1;
            if count.is_some_and(|count| shown >= count) {
                return Ok(());
            }
        }
    }
    Err("the control node closed the tail".into())
}

async fn metrics(args: &Args, api: &Api) -> Result<(), Failure> {
    let said = api.get(&format!("/workers/{}/metrics", percent_encode(args.word(1, "a worker")?))).await?;
    show(args, &said, |said| {
        println!(
            "{:<12} {:>9} {:>7} {:>6} {:>10} {:>10} {:>10} {:>10}",
            "NODE", "REQUESTS", "ERRORS", "BUSY", "CPU P50", "CPU P99", "WALL P50", "WALL P99"
        );
        let total = std::iter::once(("total".to_owned(), &said["total"]));
        let nodes = said["by_node"].as_array().into_iter().flatten();
        for (node, row) in nodes.map(|row| (row["node"].as_str().unwrap_or("").to_owned(), row)).chain(total) {
            println!(
                "{node:<12} {:>9} {:>7} {:>6} {:>10} {:>10} {:>10} {:>10}",
                row["requests"].to_string(),
                row["errors"].to_string(),
                row["busy"].to_string(),
                ms(&row["cpu_p50_us"]),
                ms(&row["cpu_p99_us"]),
                ms(&row["wall_p50_us"]),
                ms(&row["wall_p99_us"])
            );
        }
        for silent in said["silent"].as_array().into_iter().flatten() {
            println!("{} did not answer", silent.as_str().unwrap_or(""));
        }
    });
    Ok(())
}

async fn cluster(args: &Args, api: &Api) -> Result<(), Failure> {
    let said = api.get("/cluster").await?;
    show(args, &said, |said| {
        for node in said["nodes"].as_array().into_iter().flatten() {
            let roles: Vec<&str> = node["roles"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            let state = if node["available"] == true { "up" } else { "unavailable" };
            println!(
                "{:<10} {:<22} {:<16} {state}{}",
                node["id"].as_str().unwrap_or(""),
                node["address"].as_str().unwrap_or(""),
                roles.join("+"),
                node["url"].as_str().map(|url| format!("  {url}")).unwrap_or_default()
            );
            for (name, served) in node["serving"].as_object().into_iter().flatten() {
                let canary = match &served["canary"] {
                    Value::Null => String::new(),
                    canary => format!(", v{} at {} %", canary["version"], canary["percent"]),
                };
                let warm = if served["warm"] == true { "" } else { " (idle)" };
                println!("    {name}: v{}{canary}, seq {}{warm}", served["stable"], served["seq"]);
            }
        }
    });
    Ok(())
}

#[derive(Default)]
struct Tally {
    /// Each answer's worker, status and time; status 0 for a request that got none.
    answers: Vec<(usize, u16, Duration)>,
}

/// Requests at a steady rate whatever the answers take, so a slow cluster shows as latency and not as fewer
/// requests. At most `IN_FLIGHT` wait at once: one past that is counted and not sent.
async fn load(args: &Args) -> Result<(), Failure> {
    const IN_FLIGHT: usize = 4096;
    let gateway = args.flag("gateway").ok_or("edge load --gateway http://host:port --mix name=weight,..")?;
    let gateway = gateway.trim_end_matches('/').to_owned();
    let rps: f64 = args.number("rps")?.unwrap_or(100.0);
    let seconds: f64 = args.number("duration")?.unwrap_or(10.0);
    if !(rps > 0.0 && seconds > 0.0) {
        return Err("--rps and --duration are more than 0".into());
    }
    let weighed = |part: &str| {
        let (name, weight) = part.split_once('=').unwrap_or((part, "1"));
        Some((name.to_owned(), weight.parse::<usize>().ok().filter(|weight| *weight > 0)?))
    };
    let mix: Option<Vec<(String, usize)>> =
        args.flag("mix").unwrap_or("").split(',').filter(|p| !p.is_empty()).map(weighed).collect();
    let mix = mix.filter(|mix| !mix.is_empty()).ok_or("--mix takes name=weight,name=weight: at least one worker")?;
    // One turn of the mix, each worker as often as its weight, spread out.
    let turn: Vec<usize> = {
        let most = mix.iter().map(|(_, weight)| *weight).max().unwrap_or(1);
        (0..most)
            .flat_map(|round| mix.iter().enumerate().filter(move |(_, (_, w))| round < *w).map(|(i, _)| i))
            .collect()
    };
    let path = args.flag("path").unwrap_or("/");
    let urls: Vec<String> = mix.iter().map(|(name, _)| format!("{gateway}/w/{}{path}", percent_encode(name))).collect();
    let account = args.flag("account").unwrap_or("loadgen").to_owned();
    // Past one account's rate, and to many objects: request n is of account n % accounts, for path n % paths.
    let accounts = args.number::<usize>("accounts")?.unwrap_or(1).max(1);
    let paths = args.number::<usize>("paths")?.map(|n| n.max(1));
    let method = args.flag("method").unwrap_or("GET").to_uppercase();
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| "--method takes an HTTP method")?;
    let http = reqwest::Client::new();
    let tally = Arc::new(Mutex::new(Tally::default()));
    let slots = Arc::new(tokio::sync::Semaphore::new(IN_FLIGHT));

    let total = (rps * seconds).round().max(1.0) as usize;
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rps));
    let started = Instant::now();
    let (mut requests, mut skipped) = (tokio::task::JoinSet::new(), 0usize);
    for sent in 0..total {
        tick.tick().await;
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            skipped += 1;
            continue;
        };
        let worker = turn[sent % turn.len()];
        let url = paths.map_or_else(|| urls[worker].clone(), |paths| format!("{}{}", urls[worker], sent % paths));
        let account = if accounts > 1 { format!("{account}-{}", sent % accounts) } else { account.clone() };
        let request = http.request(method.clone(), url).header("x-account", account);
        let tally = tally.clone();
        requests.spawn(async move {
            let asked = Instant::now();
            let status = match request.send().await {
                // The time to the whole answer.
                Ok(response) => {
                    let status = response.status().as_u16();
                    response.bytes().await.map_or(0, |_| status)
                }
                Err(_) => 0,
            };
            tally.lock().unwrap_or_else(|e| e.into_inner()).answers.push((worker, status, asked.elapsed()));
            drop(slot);
        });
        // Ended requests are let go of as the run goes, so the set holds only those in flight.
        while requests.try_join_next().is_some() {}
    }
    let sending = started.elapsed();
    while requests.join_next().await.is_some() {}

    let mut answers = std::mem::take(&mut tally.lock().unwrap_or_else(|e| e.into_inner()).answers);
    answers.sort_by_key(|(_, _, took)| *took);
    let quantile = |of: &[&Duration], q: f64| {
        of.get(((of.len() as f64 * q) as usize).min(of.len().saturating_sub(1))).map(|d| d.as_micros() as u64)
    };
    let summary = |worker: Option<usize>| {
        let mine = answers.iter().filter(|(f, _, _)| worker.is_none_or(|worker| worker == *f));
        let mut statuses = BTreeMap::<String, usize>::new();
        let mut times = Vec::new();
        for (_, status, took) in mine {
            let key = if *status == 0 { "no answer".to_owned() } else { status.to_string() };
            *statuses.entry(key).or_default() += 1;
            times.push(took);
        }
        json!({
            "answered": times.len() - statuses.get("no answer").copied().unwrap_or(0),
            "statuses": statuses,
            "p50_us": quantile(&times, 0.5),
            "p90_us": quantile(&times, 0.9),
            "p99_us": quantile(&times, 0.99),
            "max_us": times.last().map(|d| d.as_micros() as u64),
        })
    };
    let per_worker: BTreeMap<&str, Value> =
        mix.iter().enumerate().map(|(i, (name, _))| (name.as_str(), summary(Some(i)))).collect();
    let mut said = summary(None);
    said["asked"] = json!(total);
    said["sent"] = json!(total - skipped);
    said["skipped"] = json!(skipped);
    said["seconds"] = json!(sending.as_secs_f64());
    said["rps"] = json!((total - skipped) as f64 / sending.as_secs_f64().max(f64::EPSILON));
    said["workers"] = json!(per_worker);
    show(args, &said, |said| {
        let row = |name: &str, row: &Value| {
            let statuses: Vec<String> =
                row["statuses"].as_object().into_iter().flatten().map(|(status, n)| format!("{status}: {n}")).collect();
            println!(
                "{name:<16} {:>8}  p50 {:>10}  p90 {:>10}  p99 {:>10}  max {:>10}  {}",
                row["answered"],
                ms(&row["p50_us"]),
                ms(&row["p90_us"]),
                ms(&row["p99_us"]),
                ms(&row["max_us"]),
                statuses.join(", ")
            );
        };
        println!(
            "sent {} of {total} in {:.1} s ({:.1} a second), {skipped} not sent with {IN_FLIGHT} in flight",
            total - skipped,
            sending.as_secs_f64(),
            said["rps"].as_f64().unwrap_or(0.0)
        );
        for (name, _) in &mix {
            row(name, &said["workers"][name.as_str()]);
        }
        row("all", said);
    });
    match said["statuses"]["no answer"].as_u64() == Some((total - skipped) as u64) {
        true => Err(format!("no request was answered: is a gateway at {gateway}?")),
        false => Ok(()),
    }
}

async fn run(args: &Args) -> Result<(), Failure> {
    let command = args.words.first().map_or("help", String::as_str);
    match command {
        _ if args.has("help") => println!("{HELP}"),
        "help" => println!("{HELP}"),
        "login" => login(args).await?,
        "templates" => TEMPLATES.iter().for_each(|t| println!("{:<14} {}", t.name, t.description)),
        "init" => init(args)?,
        "hash-password" => println!("{}", hash_password(&password(args)?)?),
        "load" => load(args).await?,
        _ => {
            let api = Api::new(args)?;
            match command {
                "deploy" => deploy(args, &api).await?,
                "list" => list(args, &api).await?,
                "versions" => versions(args, &api).await?,
                "rollback" => rollback(args, &api).await?,
                "canary" => canary(args, &api).await?,
                "delete" => {
                    let name = args.word(1, "a worker")?;
                    let said =
                        api.call(reqwest::Method::DELETE, &format!("/workers/{}", percent_encode(name)), None).await?;
                    show(args, &said, |_| println!("deleted {name}"));
                }
                "var" => vars(args, &api, false).await?,
                "secret" => vars(args, &api, true).await?,
                "invoke" => invoke(args, &api).await?,
                "preview" => preview(args, &api).await?,
                "tail" => tail(args, &api).await?,
                "objects" => objects(args, &api).await?,
                "object" => object(args, &api).await?,
                "metrics" => metrics(args, &api).await?,
                "cluster" => cluster(args, &api).await?,
                other => return Err(format!("no command `{other}`: see `edge help`")),
            }
        }
    }
    Ok(())
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
            eprintln!("edge: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
