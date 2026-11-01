//! `kv check`: a history client and its verdict. Writers each own their keys and write increasing numbers to them;
//! readers read every writer's keys and check each value against what was acked before the read went out and what
//! was sent before its answer came; a probe per shard measures how long each shard could not take a write. At the
//! end every key is read once more: one below its last acked number lost an acknowledged write. `--save` keeps each
//! key's numbers, and `--verify` reads them back later, after a fault that leaves nothing to write through.

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use serde_json::json;
use tokio::time::{Instant, timeout};

use crate::client::Client;
use crate::resp::Value;
use crate::slot::{SHARDS, tag_for};

#[derive(Clone, Debug)]
pub struct CheckConfig {
    pub addrs: Vec<SocketAddr>,
    pub writers: usize,
    pub readers: usize,
    pub keys_per_writer: usize,
    /// How often each shard's probe writes.
    pub probe: Duration,
    pub run: Duration,
    /// How long one operation may take before it counts as failed and its connection is dropped.
    pub op_timeout: Duration,
    pub out: Option<PathBuf>,
    /// Memory mode's control: the run passes only if it saw a lost write.
    pub expect_loss: bool,
    pub password: Option<String>,
    /// The writers' keys and the probes only in these shards; every shard when empty.
    pub shards: Vec<u16>,
    /// The start of every writer's key, so two checks against one cluster write keys apart.
    pub prefix: String,
    /// Where to keep each key's last acknowledged, last sent and failed numbers, for a later `verify`.
    pub save: Option<PathBuf>,
}

impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            addrs: vec![([127, 0, 0, 1], 6379).into()],
            writers: 64,
            readers: 16,
            keys_per_writer: 16,
            probe: Duration::from_millis(10),
            run: Duration::from_secs(40),
            op_timeout: Duration::from_secs(3),
            out: None,
            expect_loss: false,
            password: None,
            shards: Vec::new(),
            prefix: "kvc".into(),
            save: None,
        }
    }
}

/// The last number sent to a key, the last one acked, and those whose write failed: such a write may still land
/// later, after one acked, and a read of it is then no anomaly.
#[derive(Default)]
struct Key {
    name: String,
    sent: AtomicU64,
    acked: AtomicU64,
    failed: Mutex<Vec<u64>>,
}

impl Key {
    fn new(name: String) -> Self {
        Self { name, ..Self::default() }
    }

    /// A read of `v` that misses an acked write: not a write that failed, which may have landed late.
    fn misses(&self, v: u64, acked: u64) -> bool {
        v < acked && !self.failed.lock().expect("not poisoned").contains(&v)
    }
}

#[derive(Default)]
struct Counts {
    writes_ok: AtomicU64,
    writes_failed: AtomicU64,
    reads_ok: AtomicU64,
    reads_failed: AtomicU64,
    stale: AtomicU64,
    bad: AtomicU64,
    probes_ok: AtomicU64,
    probes_failed: AtomicU64,
}

struct Shared {
    cfg: CheckConfig,
    keys: Vec<Key>,
    counts: Counts,
    stop: AtomicBool,
    started: Instant,
    /// Anomalies and unavailability windows, one JSON line each.
    lines: Mutex<Vec<serde_json::Value>>,
    errors: Mutex<std::collections::BTreeMap<String, u64>>,
}

/// The writers' keys in order, `keys_per_writer` each: `kvc:w0:0` and on, or with `shards`, writer w's keys in
/// shard `shards[w % len]`, through its hash tag.
fn key_names(cfg: &CheckConfig) -> Vec<String> {
    let tags: Vec<String> = cfg.shards.iter().map(|&s| tag_for(s)).collect();
    (0..cfg.writers)
        .flat_map(|w| {
            let tag = (!tags.is_empty()).then(|| format!("{{{}}}", tags[w % tags.len()]));
            let prefix = &cfg.prefix;
            (0..cfg.keys_per_writer).map(move |i| match &tag {
                None => format!("{prefix}:w{w}:{i}"),
                Some(tag) => format!("{prefix}:{tag}w{w}:{i}"),
            })
        })
        .collect()
}

/// An error's kind: its words without the numbers in it, at most six, so `CLUSTERDOWN shard 7 no majority of logs`
/// and that of shard 9 count as one.
fn error_kind(what: &str) -> String {
    what.split_whitespace().filter(|w| w.parse::<u64>().is_err()).take(6).collect::<Vec<_>>().join(" ")
}

impl Shared {
    fn ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn error(&self, what: &str) {
        *self.errors.lock().expect("not poisoned").entry(error_kind(what)).or_default() += 1;
    }

    fn line(&self, line: serde_json::Value) {
        self.lines.lock().expect("not poisoned").push(line);
    }
}

/// The verdict and what it rests on.
#[derive(Debug)]
pub struct Verdict {
    pub pass: bool,
    pub lost: u64,
    pub stale: u64,
    pub bad: u64,
    pub summary: serde_json::Value,
}

/// A connection to the task's own node, or to the next one that takes it: as a cluster client, the check goes on
/// through the nodes that are up.
async fn connect(shared: &Shared, i: usize) -> Option<Client> {
    let addrs = &shared.cfg.addrs;
    let mut client = None;
    for addr in (0..addrs.len()).map(|n| addrs[(i + n) % addrs.len()]) {
        if let Ok(Ok(c)) = timeout(shared.cfg.op_timeout, Client::connect(addr)).await {
            client = Some(c);
            break;
        }
    }
    let mut client = client?;
    if let Some(password) = &shared.cfg.password {
        match timeout(shared.cfg.op_timeout, client.call(&[b"AUTH", password.as_bytes()])).await {
            Ok(Ok(Value::Simple(_))) => {}
            _ => return None,
        }
    }
    Some(client)
}

/// One command with the operation timeout: the reply, or why there is none. A connection that failed or timed out is
/// dropped and the next one goes to the next node: a stopped node still takes connections, and a task that went back
/// to it would see a shard that moved only at its next timeout.
async fn op(shared: &Shared, client: &mut Option<Client>, node: &mut usize, args: &[&[u8]]) -> Result<Value, String> {
    if client.is_none() {
        *client = connect(shared, *node).await;
    }
    let Some(c) = client.as_mut() else { return Err("connect failed".into()) };
    match timeout(shared.cfg.op_timeout, c.call(args)).await {
        Ok(Ok(Value::Error(e))) => Err(e),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
            *client = None;
            *node += 1;
            Err(format!("connection {e}"))
        }
        Err(_) => {
            *client = None;
            *node += 1;
            Err("client timeout".into())
        }
    }
}

fn number(value: &Value) -> Option<u64> {
    match value {
        Value::Bulk(None) => Some(0),
        Value::Bulk(Some(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

async fn writer(shared: Arc<Shared>, w: usize) {
    let (mut client, mut node) = (None, w);
    let first = w * shared.cfg.keys_per_writer;
    let mut j = 0;
    while !shared.stop.load(Relaxed) {
        let k = first + j % shared.cfg.keys_per_writer;
        j += 1;
        let seq = shared.keys[k].sent.load(Relaxed) + 1;
        shared.keys[k].sent.store(seq, Relaxed);
        let key = &shared.keys[k].name;
        match op(&shared, &mut client, &mut node, &[b"SET", key.as_bytes(), seq.to_string().as_bytes()]).await {
            Ok(_) => {
                shared.keys[k].acked.fetch_max(seq, Relaxed);
                shared.counts.writes_ok.fetch_add(1, Relaxed);
            }
            Err(e) => {
                shared.keys[k].failed.lock().expect("not poisoned").push(seq);
                shared.counts.writes_failed.fetch_add(1, Relaxed);
                shared.error(&e);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
}

async fn reader(shared: Arc<Shared>, r: usize) {
    let (mut client, mut node) = (None, r + 1);
    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (r as u64 + 1);
    while !shared.stop.load(Relaxed) {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let k = (rng % shared.keys.len() as u64) as usize;
        let lower = shared.keys[k].acked.load(Relaxed);
        let key = &shared.keys[k].name;
        let sent_at = shared.ms();
        match op(&shared, &mut client, &mut node, &[b"GET", key.as_bytes()]).await {
            Ok(value) => {
                let upper = shared.keys[k].sent.load(Relaxed);
                shared.counts.reads_ok.fetch_add(1, Relaxed);
                match number(&value) {
                    Some(v) if shared.keys[k].misses(v, lower) => {
                        shared.counts.stale.fetch_add(1, Relaxed);
                        shared.line(
                            json!({"anomaly": "stale", "key": key, "read": v, "acked_before": lower, "at_ms": sent_at}),
                        );
                    }
                    Some(v) if v > upper => {
                        shared.counts.bad.fetch_add(1, Relaxed);
                        shared.line(
                            json!({"anomaly": "never_sent", "key": key, "read": v, "sent": upper, "at_ms": sent_at}),
                        );
                    }
                    Some(_) => {}
                    None => {
                        shared.counts.bad.fetch_add(1, Relaxed);
                        shared.line(json!({"anomaly": "not_a_number", "key": key, "reply": format!("{value:?}"), "at_ms": sent_at}));
                    }
                }
            }
            Err(e) => {
                shared.counts.reads_failed.fetch_add(1, Relaxed);
                shared.error(&e);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
}

/// Writes to one shard every `probe`; a run of failures is a window the shard could not take a write.
async fn probe(shared: Arc<Shared>, shard: u16) {
    let (mut client, mut node) = (None, usize::from(shard));
    let key = format!("kvp:{{{}}}", tag_for(shard));
    let mut tick = tokio::time::interval(shared.cfg.probe);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (mut n, mut down_since) = (0u64, None);
    while !shared.stop.load(Relaxed) {
        tick.tick().await;
        n += 1;
        let at = shared.ms();
        match op(&shared, &mut client, &mut node, &[b"SET", key.as_bytes(), n.to_string().as_bytes()]).await {
            Ok(_) => {
                shared.counts.probes_ok.fetch_add(1, Relaxed);
                if let Some((from, error)) = down_since.take() {
                    shared
                        .line(json!({"window": shard, "from_ms": from, "to_ms": at, "ms": at - from, "error": error}));
                }
            }
            Err(e) => {
                shared.counts.probes_failed.fetch_add(1, Relaxed);
                down_since.get_or_insert((at, e));
            }
        }
    }
    if let Some((from, error)) = down_since {
        let at = shared.ms();
        shared.line(
            json!({"window": shard, "from_ms": from, "to_ms": at, "ms": at - from, "error": error, "open": true}),
        );
    }
}

fn shared(cfg: &CheckConfig, keys: Vec<Key>) -> Arc<Shared> {
    Arc::new(Shared {
        cfg: cfg.clone(),
        keys,
        counts: Counts::default(),
        stop: AtomicBool::new(false),
        started: Instant::now(),
        lines: Mutex::default(),
        errors: Mutex::default(),
    })
}

/// Every key once more, after every write has had its answer: how many lost an acknowledged write, and how many
/// could not be read.
async fn read_back(shared: &Shared) -> (u64, u64) {
    let (mut lost, mut unread) = (0u64, 0u64);
    let (mut client, mut node) = (None, 0);
    for key in &shared.keys {
        let (acked, sent) = (key.acked.load(Relaxed), key.sent.load(Relaxed));
        // Each key is asked again for up to 5 s: a node that answers only after its deadline is tried 2 or 3 times.
        let until = Instant::now() + Duration::from_secs(5);
        let value = loop {
            match op(shared, &mut client, &mut node, &[b"GET", key.name.as_bytes()]).await {
                Ok(value) => break Some(value),
                Err(_) if Instant::now() < until => tokio::time::sleep(Duration::from_millis(250)).await,
                Err(_) => break None,
            }
        };
        match value.as_ref().and_then(number) {
            Some(v) if key.misses(v, acked) => {
                lost += 1;
                shared.line(json!({"anomaly": "lost", "key": key.name, "read": v, "acked": acked}));
            }
            Some(v) if v > sent => {
                shared.counts.bad.fetch_add(1, Relaxed);
                shared.line(json!({"anomaly": "never_sent", "key": key.name, "read": v, "sent": sent}));
            }
            Some(_) => {}
            None => unread += 1,
        }
    }
    (lost, unread)
}

/// One JSON line each: the verdict then every anomaly and window for `--out`, each key's numbers for `--save`.
fn write_lines<'a>(path: &PathBuf, lines: impl IntoIterator<Item = &'a serde_json::Value>) -> Result<(), String> {
    let mut file = std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
    for line in lines {
        writeln!(file, "{line}").map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Runs the history and gives the verdict; writes the lines to `cfg.out` and the keys to `cfg.save` when set.
pub async fn run(cfg: CheckConfig) -> Result<Verdict, String> {
    if cfg.addrs.is_empty() || cfg.writers == 0 || cfg.keys_per_writer == 0 {
        return Err("check needs an address, a writer and a key".into());
    }
    if let Some(&s) = cfg.shards.iter().find(|&&s| s >= SHARDS) {
        return Err(format!("no shard {s}: there are {SHARDS}"));
    }
    let shared = shared(&cfg, key_names(&cfg).into_iter().map(Key::new).collect());
    let mut tasks = Vec::new();
    for w in 0..cfg.writers {
        tasks.push(tokio::spawn(writer(shared.clone(), w)));
    }
    for r in 0..cfg.readers {
        tasks.push(tokio::spawn(reader(shared.clone(), r)));
    }
    if !cfg.probe.is_zero() {
        let probed: Vec<u16> = if cfg.shards.is_empty() { (0..SHARDS).collect() } else { cfg.shards.clone() };
        for s in probed {
            tasks.push(tokio::spawn(probe(shared.clone(), s)));
        }
    }
    tokio::time::sleep(cfg.run).await;
    shared.stop.store(true, Relaxed);
    for task in tasks {
        let _ = task.await;
    }

    let (lost, unread) = read_back(&shared).await;
    let c = &shared.counts;
    let (stale, bad) = (c.stale.load(Relaxed), c.bad.load(Relaxed));
    let lines = std::mem::take(&mut *shared.lines.lock().expect("not poisoned"));
    let windows: Vec<u64> = lines.iter().filter_map(|l| l.get("ms").and_then(|m| m.as_u64())).collect();
    // Memory mode's loss shows in reads that miss an acknowledged write (stale), and in a key's last value only
    // where no write came after the move (lost): the writers go on, and their next write hides it.
    let pass =
        if cfg.expect_loss { lost + stale > 0 && bad == 0 } else { lost == 0 && stale == 0 && bad == 0 && unread == 0 };
    let summary = json!({
        "summary": if pass { "PASS" } else { "FAIL" },
        "expect_loss": cfg.expect_loss,
        "secs": cfg.run.as_secs_f64(),
        "keys": shared.keys.len(),
        "writes_ok": c.writes_ok.load(Relaxed),
        "writes_failed": c.writes_failed.load(Relaxed),
        "reads_ok": c.reads_ok.load(Relaxed),
        "reads_failed": c.reads_failed.load(Relaxed),
        "probes_ok": c.probes_ok.load(Relaxed),
        "probes_failed": c.probes_failed.load(Relaxed),
        "lost": lost,
        "stale": stale,
        "bad": bad,
        "unread": unread,
        "windows": windows.len(),
        "longest_window_ms": windows.iter().max().copied().unwrap_or(0),
        "errors": *shared.errors.lock().expect("not poisoned"),
    });
    if let Some(path) = &cfg.out {
        write_lines(path, std::iter::once(&summary).chain(&lines))?;
    }
    if let Some(path) = &cfg.save {
        let keys: Vec<_> = shared
            .keys
            .iter()
            .map(|key| {
                let failed = key.failed.lock().expect("not poisoned").clone();
                json!({"key": key.name, "acked": key.acked.load(Relaxed), "sent": key.sent.load(Relaxed), "failed": failed})
            })
            .collect();
        write_lines(path, &keys)?;
    }
    Ok(Verdict { pass, lost, stale, bad, summary })
}

/// Reads back the keys a check saved: none may be below its last acknowledged number or above its last sent one,
/// and each must answer. It writes nothing.
pub async fn verify(cfg: CheckConfig, saved: &std::path::Path) -> Result<Verdict, String> {
    let text = std::fs::read_to_string(saved).map_err(|e| format!("{}: {e}", saved.display()))?;
    let mut keys = Vec::new();
    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).map_err(|e| format!("{}: {e}", saved.display()))?;
        let field = |name: &str| v.get(name).and_then(|n| n.as_u64()).ok_or(format!("a saved key without {name}"));
        let key = Key::new(v.get("key").and_then(|k| k.as_str()).ok_or("a saved key without its name")?.to_owned());
        key.acked.store(field("acked")?, Relaxed);
        key.sent.store(field("sent")?, Relaxed);
        *key.failed.lock().expect("not poisoned") =
            v.get("failed").and_then(|f| f.as_array()).into_iter().flatten().filter_map(|n| n.as_u64()).collect();
        keys.push(key);
    }
    let shared = shared(&cfg, keys);
    let (lost, unread) = read_back(&shared).await;
    let bad = shared.counts.bad.load(Relaxed);
    let pass = lost == 0 && bad == 0 && unread == 0 && !shared.keys.is_empty();
    let lines = std::mem::take(&mut *shared.lines.lock().expect("not poisoned"));
    let summary = json!({
        "summary": if pass { "PASS" } else { "FAIL" },
        "verify": saved.display().to_string(),
        "keys": shared.keys.len(),
        "acked_keys": shared.keys.iter().filter(|k| k.acked.load(Relaxed) > 0).count(),
        "lost": lost,
        "bad": bad,
        "unread": unread,
    });
    if let Some(path) = &cfg.out {
        write_lines(path, std::iter::once(&summary).chain(&lines))?;
    }
    Ok(Verdict { pass, lost, stale: 0, bad, summary })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_write_that_lands_after_an_acked_one_is_no_anomaly() {
        let key = Key::default();
        key.failed.lock().expect("not poisoned").push(5);
        assert!(!key.misses(5, 6), "write 5 failed, so it may have landed after 6");
        assert!(key.misses(4, 6), "write 4 was acked before 6 and is gone");
        assert!(!key.misses(6, 6));
    }

    #[test]
    fn keys_stay_in_the_shards_named() {
        let cfg = CheckConfig { writers: 5, keys_per_writer: 3, ..CheckConfig::default() };
        assert_eq!(key_names(&cfg)[..4], ["kvc:w0:0", "kvc:w0:1", "kvc:w0:2", "kvc:w1:0"]);
        let cfg = CheckConfig { shards: vec![3, 200], prefix: "q".into(), ..cfg };
        let names = key_names(&cfg);
        assert_eq!(names.len(), 15);
        for (k, name) in names.iter().enumerate() {
            let want = cfg.shards[k / cfg.keys_per_writer % cfg.shards.len()];
            assert_eq!(crate::slot::shard_of(name.as_bytes()), want, "{name}");
            assert!(name.starts_with("q:{"), "{name}");
        }
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), names.len());
    }

    #[test]
    fn errors_of_one_kind_count_together() {
        assert_eq!(error_kind("CLUSTERDOWN shard 7 no majority of logs"), "CLUSTERDOWN shard no majority of logs");
        assert_eq!(error_kind("TRYAGAIN shard 12 moving"), error_kind("TRYAGAIN shard 3 moving"));
        assert_eq!(error_kind("ERR outcome unknown"), "ERR outcome unknown");
    }
}
