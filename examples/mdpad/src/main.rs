//! `mdpad help` lists the commands: one node with every role, or a seed server and the nodes of a cluster, the
//! accounts, a user's documents over the HTTP API, bots that type, and the check of their logs. See the README.

mod cli;

use std::collections::BTreeMap;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axon::RuntimeConfig;
use axon::files::{Files, OsFiles};
use axon_cluster::{SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use mdpad::accounts;
use mdpad::auth::{Auth, Invite, hash_password, hex, random_key, unhex};
use mdpad::core::accounts::Event;
use mdpad::load::{self, LoadConfig};
use mdpad::node::{self, Node, NodeConfig, Roles};
use serde_json::json;

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Args(BTreeMap<String, String>);

impl Args {
    fn get(&self, key: &str) -> Result<&str, Error> {
        self.0.get(key).map(String::as_str).ok_or_else(|| format!("missing {key}=").into())
    }
    fn or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.0.get(key).map_or(default, String::as_str)
    }
    fn dir(&self) -> PathBuf {
        PathBuf::from(self.or("dir", "mdpad-data"))
    }
    fn list(&self, key: &str, default: &str) -> Vec<String> {
        self.or(key, default).split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()
    }
    fn addrs(&self, key: &str, default: &str) -> Result<Vec<SocketAddr>, Error> {
        Ok(self.list(key, default).iter().map(|a| a.parse()).collect::<Result<_, _>>()?)
    }
    fn addr(&self, key: &str) -> Result<Option<SocketAddr>, Error> {
        Ok(self.0.get(key).map(|a| a.parse()).transpose()?)
    }

    /// What every node takes: `health=`, `console=`, `idle=` (seconds a document nobody calls stays) and `workers=`.
    fn node(&self, cfg: &mut NodeConfig) -> Result<(), Error> {
        (cfg.health, cfg.console) = (self.addr("health")?, self.addr("console")?);
        // Every process is sampled, so a console on any node shows them all.
        cfg.telemetry = true;
        cfg.workers = self.or("workers", "2").parse()?;
        if let Some(idle) = self.0.get("idle") {
            cfg.doc_idle = Duration::from_secs_f64(idle.parse()?);
        }
        Ok(())
    }
}

/// The JSON line a node prints once it serves.
fn started(id: &str, node: &Node) -> serde_json::Value {
    let http = node.web.as_ref().map(|_| node.http().to_string());
    let (health, console) = (node.health.map(|a| a.to_string()), node.console.map(|a| a.to_string()));
    let cluster = node.cluster.local_addr().to_string();
    json!({ "node": id, "pid": std::process::id(), "http": http, "cluster": cluster, "health": health, "console": console })
}

async fn terminated() {
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// The signing key at `path`, made on the first start: sign-ins and links stay valid across restarts.
fn key(path: &Path) -> Result<Vec<u8>, Error> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(unhex(text.trim()).filter(|k| k.len() >= 32).ok_or("the key file is not 64 hex digits")?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = random_key()?;
            // Who reads it can sign in as anyone: the owner only.
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
            file.write_all((hex(&key) + "\n").as_bytes())?;
            Ok(key)
        }
        Err(e) => Err(e.into()),
    }
}

fn files(dir: &Path, name: &str) -> Result<Arc<dyn Files>, Error> {
    std::fs::create_dir_all(dir.join(name))?;
    Ok(Arc::new(OsFiles::new(dir.join(name))))
}

async fn serve(args: Args) -> Result<(), Error> {
    let dir = args.dir();
    std::fs::create_dir_all(&dir)?;
    let mut cfg = NodeConfig::new(files(&dir, "docs")?, files(&dir, "accounts")?, key(&dir.join("key"))?);
    cfg.http = Some(args.or("http", "127.0.0.1:8080").parse()?);
    args.node(&mut cfg)?;
    let node = Node::start(cfg).await?;
    println!("{}", started("h1", &node));
    terminated().await;
    node.shutdown().await;
    Ok(())
}

/// A seed server: where the nodes of a cluster find each other.
async fn seed(args: Args) -> Result<(), Error> {
    let addr: SocketAddr = args.or("addr", "127.0.0.1:7100").parse()?;
    let server = SeedServer::bind(SeedServerConfig::new(addr, &node::transport("s0")?)).await?;
    println!("{}", json!({ "node": "seed", "pid": std::process::id(), "addr": addr.to_string() }));
    terminated().await;
    drop(server);
    Ok(())
}

/// One node of a cluster: `id= seeds= roles=web,home,accounts homes=h1,h2,h3 dir= http= cluster= key= preallocate=`.
async fn cluster_node(args: Args) -> Result<(), Error> {
    let id = args.get("id")?.to_string();
    let dir = args.dir();
    std::fs::create_dir_all(&dir)?;
    let roles = Roles::parse(&id, args.or("roles", "web"))?;
    let key_path = args.0.get("key").map_or_else(|| dir.join("key"), PathBuf::from);
    let mut cfg = NodeConfig {
        id: id.clone(),
        homes: args.list("homes", "h1"),
        docs: roles.home.as_ref().map(|_| files(&dir, "docs")).transpose()?,
        accounts: roles.accounts.then(|| files(&dir, "accounts")).transpose()?,
        http: roles.web.then(|| args.or("http", "127.0.0.1:8080").parse()).transpose()?,
        // Two heartbeats after the seeds answer: a record of a node still running is in the view by then.
        settle: Duration::from_secs(1),
        roles,
        ..NodeConfig::with_key(key(&key_path)?)
    };
    cfg.docs_log.preallocate = args.or("preallocate", "true").parse()?;
    args.node(&mut cfg)?;
    if cfg.roles.home.is_some() && !cfg.homes.contains(&id) {
        return Err(format!("home {id} is not in homes=").into());
    }
    let transport = node::transport(&id)?;
    let seeds = args.list("seeds", "127.0.0.1:7100");
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    let bind: SocketAddr = args.or("cluster", "127.0.0.1:0").parse()?;
    let ccfg = cfg.cluster(bind, transport, discovery);
    let runtime = cfg.runtime(RuntimeConfig::default());
    let node = Node::boot(cfg, ccfg, runtime).await?;
    println!("{}", started(&id, &node));
    terminated().await;
    node.shutdown().await;
    Ok(())
}

const HELP: &str = "\
mdpad: a collaborative Markdown editor

  mdpad serve dir= http= [console=addr]         every role in one process
  mdpad seed addr=; mdpad node id= seeds= roles=web,home,accounts homes= dir= http=   a cluster
      no TLS between nodes: a seed and each node take --plaintext, for loopback or a private network only
      each node also takes health=addr (/healthz, /readyz, /metrics), console=addr (axon's console),
      idle=secs (a document nobody calls stops, 900) and workers=
  mdpad user invite name= [url=] [days=7] [dir=|key=]   a link that makes the account: works while the node runs
  mdpad user reset name= [url=] [dir=]          a link that sets a new password (node stopped)
  mdpad user add|password|disable|enable|list name= password= dir=   the accounts, while the node is stopped
  mdpad user operator name= [on=false] dir=     make a user an operator, or not (node stopped)
  mdpad load http=a,b user= password= [docs=1 editors=4 viewers=0 hot=0 rate=5 secs=10 size=0 keep= run=dir]
      bots on the web nodes; a JSON report; exits 1 when an acked or seen token is lost, a copy differs or a cursor
      stays. offline=N: one editor types N updates offline, then merges. create=N lists=M: N documents, M lists
  mdpad check <run>                             a load run's logs read back
  mdpad hash-password password=
";

/// A link that sets a password: signed with the node's key, so it needs no running node. `version` 0 makes the
/// account.
fn invite(args: &Args, name: &str, version: u64) -> Result<(), Error> {
    if !mdpad::core::accounts::valid_name(name) {
        return Err(format!("{name:?} is not a user name: 1 to 32 of a-z, 0-9, - and _").into());
    }
    let key_path = args.0.get("key").map_or_else(|| args.dir().join("key"), PathBuf::from);
    if !key_path.exists() {
        return Err(format!("no key at {}: give the node's dir= or key=", key_path.display()).into());
    }
    let auth = Auth::new(&key(&key_path)?)?;
    let days: u64 = args.or("days", "7").parse()?;
    let valid = days.checked_mul(86_400_000).filter(|_| days > 0).ok_or("days= is a number of days from 1")?;
    let expires_ms = (axon::trace::unix_micros() / 1000).saturating_add(valid);
    let token = auth.invite(&Invite { name: name.into(), version, expires_ms });
    let link = format!("{}/i/{token}", args.or("url", "http://127.0.0.1:8080").trim_end_matches('/'));
    println!("{}", json!({ "name": name, "link": link, "expires_ms": expires_ms }));
    Ok(())
}

/// Changes the accounts while the node is stopped: a running node keeps their directory locked.
async fn user(action: &str, args: Args) -> Result<(), Error> {
    let name = || args.get("name").map(str::to_string);
    let event = match action {
        "invite" => return invite(&args, &name()?, 0),
        "reset" => {
            let accounts = accounts::offline(files(&args.dir(), "accounts")?, None).await?;
            let name = name()?;
            let user = accounts.users.get(&name).ok_or(format!("no user {name}"))?;
            return invite(&args, &name, user.version);
        }
        "list" => None,
        "add" => Some(Event::Add { name: name()?, hash: hash_password(args.get("password")?)? }),
        "password" => Some(Event::Password { name: name()?, hash: hash_password(args.get("password")?)? }),
        "disable" => Some(Event::Disable { name: name()? }),
        "enable" => Some(Event::Enable { name: name()? }),
        "operator" => Some(Event::Operator { name: name()?, operator: args.or("on", "true").parse()? }),
        _ => return Err(HELP.into()),
    };
    let accounts = accounts::offline(files(&args.dir(), "accounts")?, event).await?;
    for (name, user) in &accounts.users {
        println!("{}", json!({ "name": name, "disabled": user.disabled, "operator": user.operator }));
    }
    Ok(())
}

async fn load(args: Args) -> Result<(), Error> {
    let cfg = LoadConfig {
        http: args.addrs("http", "127.0.0.1:8080")?,
        user: args.get("user")?.into(),
        password: args.get("password")?.into(),
        docs: args.or("docs", "1").parse()?,
        editors: args.or("editors", "4").parse()?,
        viewers: args.or("viewers", "0").parse()?,
        duration: Duration::from_secs_f64(args.or("secs", "10").parse()?),
        rate: args.or("rate", "5").parse()?,
        hot_viewers: args.or("hot", "0").parse()?,
        size: args.or("size", "0").parse()?,
        keep: args.0.get("keep").map(|k| k.parse()).transpose()?,
        run: args.0.get("run").map(PathBuf::from),
    };
    let (offline, created, lists): (usize, usize, usize) =
        (args.or("offline", "0").parse()?, args.or("create", "0").parse()?, args.or("lists", "0").parse()?);
    if offline > 0 {
        let report = load::offline(cfg, offline).await?;
        println!("{}", serde_json::to_string(&report)?);
        return if report.equal { Ok(()) } else { Err("the texts differ after the merge".into()) };
    }
    if created + lists > 0 {
        println!("{}", serde_json::to_string(&load::stored(cfg, created, lists).await?)?);
        return Ok(());
    }
    let report = load::run(cfg).await?;
    println!("{}", serde_json::to_string(&report)?);
    gate(&report.check)
}

fn gate(check: &load::Check) -> Result<(), Error> {
    match check {
        load::Check { lost_acked: 0, seen_lost: 0, diverged: 0, presence_left: 0 } => Ok(()),
        _ => Err(format!("{check:?}").into()),
    }
}

/// `mdpad check <run>`: what a `load run=<run>` logged, read back.
fn check(run: &str) -> Result<(), Error> {
    let check = load::check(Path::new(run))?;
    println!("{}", serde_json::to_string(&check)?);
    gate(&check)
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("mdpad: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Error> {
    let mut argv = std::env::args().skip(1);
    let name = argv.next().unwrap_or_default();
    let action = if name == "user" || name == "check" { argv.next().unwrap_or_default() } else { String::new() };
    let (mut words, mut pairs, mut switches) = (Vec::new(), BTreeMap::new(), Vec::new());
    for arg in argv {
        match (arg.strip_prefix("--"), arg.split_once('=')) {
            (Some(switch), _) => switches.push(switch.to_string()),
            (None, Some((k, v))) if !k.contains('/') => drop(pairs.insert(k.to_string(), v.to_string())),
            _ => words.push(arg),
        }
    }
    if ["login", "docs", "doc", "export", "import", "share", "versions", "restore", "cluster"].contains(&name.as_str())
    {
        let json = switches.iter().any(|s| s == "json");
        let all = switches.iter().any(|s| s == "all");
        return Ok(cli::run(&name, cli::Cli { words, args: pairs, json, all }).await?);
    }
    let args = Args(pairs);
    match name.as_str() {
        "serve" => serve(args).await,
        "seed" | "node" if !switches.iter().any(|s| s == "plaintext") => {
            Err("mdpad has no TLS between nodes: add --plaintext to run on loopback or a private network only".into())
        }
        "seed" => seed(args).await,
        "node" => cluster_node(args).await,
        "check" => check(&action),
        "user" => user(&action, args).await,
        "hash-password" => {
            println!("{}", hash_password(args.get("password")?)?);
            Ok(())
        }
        "load" => load(args).await,
        "help" => {
            println!("{HELP}{}", cli::HELP);
            Ok(())
        }
        _ => Err(format!("usage:\n{HELP}{}", cli::HELP).into()),
    }
}
