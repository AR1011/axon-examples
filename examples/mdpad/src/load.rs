//! `mdpad load`: bots that speak the protocol. Editors type a unique token at a steady rate, show a cursor, and time
//! each token from the keystroke to its `saved`; viewers only follow, and note every token they see. A bot whose web
//! node does not answer joins through the next one. At the end every client settles and leaves, a fresh viewer of
//! each document counts the cursors still shown, and the report says how many acked tokens the document lacks
//! (`lost_acked`), how many seen ones (`seen_lost`), how many clients hold a text other than the server's
//! (`diverged`), and how many cursors were left (`presence_left`).
//!
//! With `run=<dir>` each bot's log, each document's final text and the cursors left go to files, and `mdpad check
//! <dir>` reads them back: what `fault.sh` gates on.
//!
//! The numbers of design §7 come from the same bots: each editor notes when another editor's token, typed through
//! another web node, reaches it (`remote_*`, one process and one clock); each bot how long it took to be synced again
//! after it lost its socket or had to join again (`resync_*`); each editor when it saved again after a stall
//! (`resumed_unix_ms`). `offline=N` and `create=N`/`lists=N` are runs of their own: [`offline`] and [`stored`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::client::{Api, Client, Credential, Stop};

pub struct LoadConfig {
    /// The web nodes: the bots spread over them.
    pub http: Vec<SocketAddr>,
    pub user: String,
    pub password: String,
    pub docs: usize,
    pub editors: usize,
    pub viewers: usize,
    /// More viewers on the first document.
    pub hot_viewers: usize,
    /// The most of its own saved tokens an editor keeps in the text: each token past it removes the oldest, so a long
    /// run stays under the state's cap.
    pub keep: Option<usize>,
    pub duration: Duration,
    /// Keystrokes a second per editor.
    pub rate: f64,
    /// Bytes of text each document starts with.
    pub size: usize,
    pub run: Option<PathBuf>,
}

#[derive(Serialize, Default)]
pub struct Report {
    pub docs: usize,
    pub editors: usize,
    pub viewers: usize,
    pub secs: f64,
    pub typed: usize,
    pub acked: usize,
    pub ack_p50_ms: f64,
    pub ack_p99_ms: f64,
    pub ack_max_ms: f64,
    pub reconnects: usize,
    /// Why the clients joined again: `rejoin` for a gap or a new generation, else the close code.
    pub reasons: BTreeMap<String, usize>,
    /// Notices the clients were sent, by kind.
    pub notices: BTreeMap<String, u64>,
    pub received_bytes: u64,
    pub sent_bytes: u64,
    /// Received and sent, per client and second.
    pub bytes_per_client_s: f64,
    /// Another editor's token, typed through another web node: from the keystroke to here.
    pub remote_n: usize,
    pub remote_p50_ms: f64,
    pub remote_p99_ms: f64,
    pub remote_max_ms: f64,
    /// From a lost socket or a join again to synced: joined, the state applied, nothing unsaved.
    pub resyncs: usize,
    pub resync_p50_ms: f64,
    pub resync_p99_ms: f64,
    pub resync_max_ms: f64,
    /// When an editor saved again after a stall of over 1 s, in ms since the epoch: a fault run compares it with
    /// when the node was back.
    pub resumed_unix_ms: Vec<u64>,
    #[serde(flatten)]
    pub check: Check,
}

/// What `mdpad check` prints: each must be 0.
#[derive(Serialize, Default, Debug, PartialEq, Eq)]
pub struct Check {
    pub lost_acked: usize,
    pub seen_lost: usize,
    pub diverged: usize,
    pub presence_left: usize,
}

/// One bot's log.
#[derive(Serialize, Deserialize, Default)]
pub struct Log {
    pub doc: String,
    pub name: String,
    pub acked: Vec<String>,
    /// Its own saved tokens it removed again (`keep=`).
    #[serde(default)]
    pub removed: Vec<String>,
    /// Tokens seen in the text, read every half second.
    pub seen: BTreeSet<String>,
    /// The text once it settled.
    pub text: String,
}

#[derive(Default)]
struct Bot {
    log: Log,
    tokens: Vec<String>,
    latencies: Vec<Duration>,
    remote: Vec<Duration>,
    resyncs: Vec<Duration>,
    resumed: Vec<u64>,
    last_ack: Option<Instant>,
    /// The acked tokens it has removed, or found gone: the next to remove is `log.acked[cut]` (`keep=`).
    cut: usize,
    reconnects: usize,
    reasons: BTreeMap<String, usize>,
    notices: BTreeMap<String, u64>,
    received: u64,
    sent: u64,
}

/// Each token typed, with when and through which web node: shared by the bots of a run.
type Stamps = Arc<Mutex<BTreeMap<String, (Instant, SocketAddr)>>>;

const SETTLE: Duration = Duration::from_secs(30);
const SCAN_EVERY: Duration = Duration::from_millis(500);
const AWARE_EVERY: Duration = Duration::from_secs(1);
/// A clean leave clears in under 1 s, a killed web node's in about 2 s: the probe waits past both.
const PROBE_AFTER: Duration = Duration::from_secs(5);

/// Typed tokens not yet saved: the mark, when it was typed and its index in `Bot::tokens`.
type Pending = BTreeMap<u64, (Instant, usize)>;

/// A bot's token: unique in the run.
fn is_token(word: &str) -> bool {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let Some((d, rest)) = word.strip_prefix('d').and_then(|w| w.split_once('b')) else { return false };
    let Some((b, k)) = rest.split_once('k') else { return false };
    digits(d) && digits(b) && digits(k)
}

fn tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split_whitespace().filter(|w| is_token(w))
}

/// The tokens in an update's bytes: a bot types each with a space on both sides, as one insert.
fn tokens_in(update: &[u8]) -> Vec<String> {
    update
        .split(|b| *b == b' ')
        .filter_map(|w| std::str::from_utf8(w).ok())
        .filter(|w| is_token(w))
        .map(str::to_string)
        .collect()
}

/// The text a document starts with: its heading, then paragraphs up to `size` bytes.
fn starting_text(d: usize, size: usize) -> String {
    let mut text = format!("# Load {d}\n");
    while text.len() + 48 <= size {
        text.push_str("\nA paragraph of the text that the editors start with.\n");
    }
    text
}

impl Bot {
    /// Moves the pending tokens the client has heard saved into the log, with their time to the ack. An ack after a
    /// stall of over `stall` is when the editor saved again.
    fn take_saved(&mut self, pending: &mut Pending, client: &Client, stall: Duration) {
        let before = self.log.acked.len();
        pending.retain(|mark, (at, i)| {
            let saved = client.saved.contains(mark);
            if saved {
                self.latencies.push(at.elapsed());
                self.log.acked.push(self.tokens[*i].clone());
            }
            !saved
        });
        if self.log.acked.len() > before {
            let now = Instant::now();
            if self.last_ack.is_some_and(|last| now - last > stall) {
                self.resumed.push(axon::trace::unix_micros() / 1000);
            }
            self.last_ack = Some(now);
        }
    }
}

/// Joins through `http[first]`, or the next while it does not answer.
async fn join(client: &mut Client, http: &[SocketAddr], at: &mut usize) -> bool {
    client.http = http[*at % http.len()];
    if client.connect().await.is_ok() {
        return true;
    }
    *at += 1;
    false
}

/// Joins and runs until `until`, typing every `period` when it has one, then settles and leaves.
#[expect(clippy::too_many_arguments, reason = "one call site, in `run`")]
async fn bot(
    http: Vec<SocketAddr>,
    mut at: usize,
    id: String,
    credential: Credential,
    name: String,
    period: Option<Duration>,
    keep: Option<usize>,
    until: Instant,
    stamps: Stamps,
) -> Bot {
    let home = at;
    let mut client = Client::new(http[at % http.len()], &id, credential);
    let mut bot = Bot { log: Log { doc: id, name: name.clone(), ..Default::default() }, ..Default::default() };
    // An editor hears the others' tokens as their updates arrive.
    let heard: Arc<Mutex<Vec<String>>> = Arc::default();
    if period.is_some() {
        let heard = heard.clone();
        let observed = client.core.doc.observe_update_v1("load", move |_, event| {
            let found = tokens_in(&event.update);
            if !found.is_empty() {
                heard.lock().expect("no bot panics holding it").extend(found);
            }
        });
        observed.expect("no transaction is open yet");
    }
    let own = format!("{name}k");
    let stall = period.map_or(Duration::MAX, |p| (3 * p).max(Duration::from_secs(1)));
    let mut pending = Pending::new();
    let (mut next, mut scan, mut aware) = (Instant::now(), Instant::now(), Instant::now());
    let mut failures = 0u32;
    let mut connected = false;
    // Since when it lost its socket or had to join again, until it is synced.
    let mut lost: Option<Instant> = None;
    loop {
        let now = Instant::now();
        if now >= until {
            break;
        }
        if !connected {
            // Its own web node first, as a balancer would send it: else every bot ends on the one node never killed.
            if failures == 0 {
                at = home;
            }
            connected = join(&mut client, &http, &mut at).await;
            if !connected {
                lost.get_or_insert(now);
                failures += 1;
                tokio::time::sleep(Duration::from_millis(50 << failures.min(6))).await;
                continue;
            }
        }
        if let Some(period) = period
            && now >= next
        {
            let mark = bot.tokens.len() as u64;
            let token = format!(" {name}k{mark} ");
            stamps.lock().expect("no bot panics holding it").insert(token.trim().to_string(), (now, client.http));
            let cut = keep.filter(|&keep| bot.log.acked.len() - bot.cut > keep).map(|_| {
                bot.cut += 1;
                bot.log.acked[bot.cut - 1].clone()
            });
            let (update, removed) = keystroke(&client.core.doc, &name, &token, cut.as_deref());
            // Only a token still in the text is removed: one an acked edit lost stays lost for the check.
            bot.log.removed.extend(cut.filter(|_| removed));
            if client.edit(update, mark).await.is_err() {
                connected = false;
                lost.get_or_insert(now);
            }
            pending.insert(mark, (now, bot.tokens.len()));
            bot.tokens.push(token.trim().to_string());
            next += period;
        }
        if period.is_some() && now >= aware && client.core.welcome.is_some() {
            let cursor = serde_json::json!({ "cursor": { "anchor": crate::crdt::len(&client.core.doc) } });
            let frame = client.core.aware(Some(cursor));
            if client.send(frame).await.is_err() {
                connected = false;
            }
            aware = now + AWARE_EVERY;
        }
        if period.is_none() && now >= scan {
            bot.log.seen.extend(tokens(&client.text()).map(str::to_string));
            scan = now + SCAN_EVERY;
        }
        // An editor wakes to type and, once joined, to send its cursor; a viewer to read the text.
        let due = match period {
            Some(_) if client.core.welcome.is_some() => next.min(aware),
            Some(_) => next,
            None => scan,
        };
        let wake = due.min(until).max(now + Duration::from_millis(1));
        match client.step(wake).await {
            Ok(()) | Err(Stop::Timeout) => failures = 0,
            Err(stop) => {
                (connected, bot.reconnects) = (false, bot.reconnects + 1);
                lost.get_or_insert(Instant::now());
                let reason = match stop {
                    Stop::Closed(code) => code.to_string(),
                    _ => "rejoin".into(),
                };
                *bot.reasons.entry(reason).or_default() += 1;
            }
        }
        bot.take_saved(&mut pending, &client, stall);
        if let Some(since) = lost
            && client.synced()
        {
            bot.resyncs.push(since.elapsed());
            lost = None;
        }
        let heard: Vec<String> = std::mem::take(&mut *heard.lock().expect("no bot panics holding it"));
        if !heard.is_empty() {
            let (now, stamps) = (Instant::now(), stamps.lock().expect("no bot panics holding it"));
            for token in heard.iter().filter(|t| !t.starts_with(&own)) {
                if let Some((typed, web)) = stamps.get(token)
                    && *web != client.http
                {
                    bot.remote.push(now - *typed);
                }
            }
        }
    }
    // Settle: everything typed is saved and the state is the server's.
    let settle = Instant::now() + SETTLE;
    while Instant::now() < settle {
        if !connected && !join(&mut client, &http, &mut at).await {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        connected = true;
        match client.run_until(settle.saturating_duration_since(Instant::now()), Client::synced).await {
            Ok(()) => break,
            Err(stop) => connected = stop == Stop::Timeout,
        }
    }
    // A last read of what is live, then the text, and a clean leave.
    let _ = client.run_until(Duration::from_millis(500), |_| false).await;
    bot.take_saved(&mut pending, &client, stall);
    bot.log.text = client.text();
    bot.log.seen.extend(tokens(&bot.log.text).map(str::to_string));
    if period.is_some() && connected {
        let leave = client.core.aware(None);
        let _ = client.send(leave).await;
    }
    (bot.received, bot.sent) = (client.received, client.sent);
    bot.notices = client.core.notices.clone();
    client.disconnect().await;
    bot
}

/// Cursors a fresh viewer of `id` is shown: those of the bots that left.
async fn cursors_left(http: SocketAddr, id: &str, credential: Credential) -> Result<usize, String> {
    let mut probe = Client::new(http, id, credential);
    probe.connect().await?;
    let _ = probe.run_until(Duration::from_secs(2), |_| false).await;
    let left = probe.core.presence.len();
    probe.disconnect().await;
    Ok(left)
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    match sorted.len() {
        0 => 0.0,
        n => sorted[((n as f64 * p).ceil() as usize).clamp(1, n) - 1].as_secs_f64() * 1000.0,
    }
}

/// Signs in through the first web node that answers.
async fn login(cfg: &LoadConfig) -> Result<(Api, SocketAddr), String> {
    let mut last = String::from("no web node");
    for http in &cfg.http {
        match Api::login(*http, &cfg.user, &cfg.password).await {
            Ok(api) => return Ok((api, *http)),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// Makes `n` documents, 32 at a time: their ids in order, the time it took, and the 503s answered "Try again" on the
/// way. Each is tried again after 100 ms, doubling to 2 s, 10 times at most.
async fn create(api: &Api, n: usize, text: impl Fn(usize) -> String) -> Result<Created, String> {
    let start = Instant::now();
    let made: Vec<Result<(String, Vec<String>), String>> = futures_util::stream::iter(0..n)
        .map(|d| {
            let text = text(d);
            async move {
                let mut refused = Vec::new();
                loop {
                    match api.create(&text).await {
                        Ok(id) => return Ok((id, refused)),
                        Err(e) if e.starts_with("503") && refused.len() < 10 => {
                            let wait = Duration::from_millis(100 << refused.len()).min(Duration::from_secs(2));
                            refused.push(e);
                            tokio::time::sleep(wait).await;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        })
        .buffered(32)
        .collect()
        .await;
    let mut created = Created { ids: Vec::with_capacity(n), took: start.elapsed(), refused: Vec::new() };
    for one in made {
        let (id, refused) = one?;
        created.ids.push(id);
        created.refused.extend(refused);
    }
    Ok(created)
}

struct Created {
    ids: Vec<String>,
    took: Duration,
    refused: Vec<String>,
}

pub async fn run(cfg: LoadConfig) -> Result<Report, String> {
    let (api, _) = login(&cfg).await?;
    let ids = create(&api, cfg.docs, |d| starting_text(d, cfg.size)).await?.ids;
    let stamps = Stamps::default();
    let until = Instant::now() + cfg.duration;
    let period = Duration::from_secs_f64(1.0 / cfg.rate.max(0.01));
    let mut tasks = Vec::new();
    for (d, id) in ids.iter().enumerate() {
        let viewers = cfg.viewers + if d == 0 { cfg.hot_viewers } else { 0 };
        for b in 0..cfg.editors + viewers {
            let (id, credential, name) = (id.clone(), api.credential(), format!("d{d}b{b}"));
            let period = (b < cfg.editors).then_some(period);
            let http = cfg.http.clone();
            tasks.push(tokio::spawn(bot(http, d + b, id, credential, name, period, cfg.keep, until, stamps.clone())));
        }
    }
    let mut report = Report {
        docs: cfg.docs,
        editors: cfg.editors,
        viewers: cfg.viewers,
        secs: cfg.duration.as_secs_f64(),
        ..Default::default()
    };
    let (mut latencies, mut remote, mut resyncs) = (Vec::new(), Vec::new(), Vec::new());
    let mut logs = Vec::new();
    let clients = tasks.len();
    for task in tasks {
        let bot = task.await.map_err(|e| e.to_string())?;
        report.typed += bot.tokens.len();
        report.acked += bot.log.acked.len();
        report.reconnects += bot.reconnects;
        for (reason, n) in bot.reasons {
            *report.reasons.entry(reason).or_default() += n;
        }
        for (kind, n) in bot.notices {
            *report.notices.entry(kind).or_default() += n;
        }
        report.received_bytes += bot.received;
        report.sent_bytes += bot.sent;
        report.resumed_unix_ms.extend(bot.resumed);
        latencies.extend(bot.latencies);
        remote.extend(bot.remote);
        resyncs.extend(bot.resyncs);
        logs.push(bot.log);
    }
    tokio::time::sleep(PROBE_AFTER).await;
    let (api, http) = login(&cfg).await?;
    // Each probe listens 2 s: 64 at once, so a run of thousands of documents ends in minutes.
    let (api, mut texts, mut left) = (&api, BTreeMap::new(), BTreeMap::new());
    let mut probes = futures_util::stream::iter(&ids)
        .map(|id| async move {
            Ok::<_, String>((id, api.markdown(id).await?, cursors_left(http, id, api.credential()).await?))
        })
        .buffered(64);
    while let Some(probe) = probes.next().await {
        let (id, text, cursors) = probe?;
        texts.insert(id.clone(), text);
        left.insert(id.clone(), cursors);
    }
    drop(probes);
    if let Some(run) = &cfg.run {
        save(run, &logs, &texts, &left).map_err(|e| format!("{}: {e}", run.display()))?;
    }
    report.check = check_logs(&logs, &texts, &left);
    [
        (report.ack_p50_ms, report.ack_p99_ms, report.ack_max_ms),
        (report.remote_p50_ms, report.remote_p99_ms, report.remote_max_ms),
        (report.resync_p50_ms, report.resync_p99_ms, report.resync_max_ms),
    ] = [&mut latencies, &mut remote, &mut resyncs].map(|all| {
        all.sort();
        (percentile(all, 0.50), percentile(all, 0.99), percentile(all, 1.0))
    });
    (report.remote_n, report.resyncs) = (remote.len(), resyncs.len());
    report.resumed_unix_ms.sort();
    report.bytes_per_client_s =
        (report.received_bytes + report.sent_bytes) as f64 / clients.max(1) as f64 / cfg.duration.as_secs_f64();
    Ok(report)
}

#[derive(Serialize)]
pub struct OfflineReport {
    pub offline_updates: usize,
    /// Tokens another editor typed meanwhile.
    pub online_updates: usize,
    /// From the join after the offline typing to synced: everything merged and saved.
    pub merge_ms: f64,
    pub equal: bool,
}

/// `offline=N`: an editor leaves, types `N` tokens as `N` updates while another types online, then joins again.
pub async fn offline(cfg: LoadConfig, n: usize) -> Result<OfflineReport, String> {
    let (api, http) = login(&cfg).await?;
    let id = api.create(&starting_text(0, cfg.size)).await?;
    let synced = Duration::from_secs(30);
    let (mut away, mut online) = (
        Client::new(http, &id, api.credential()),
        Client::new(*cfg.http.last().unwrap_or(&http), &id, api.credential()),
    );
    for client in [&mut away, &mut online] {
        client.connect().await?;
        client.run_until(synced, Client::synced).await.map_err(|e| format!("join: {e:?}"))?;
    }
    away.disconnect().await;
    for k in 0..n {
        let end = crate::crdt::len(&away.core.doc);
        away.insert(end, &format!(" off{k} "), k as u64).await?;
    }
    let online_updates = 100;
    for k in 0..online_updates {
        online.insert(0, &format!(" on{k} "), k as u64).await?;
    }
    online.run_until(synced, Client::synced).await.map_err(|e| format!("online: {e:?}"))?;
    let start = Instant::now();
    away.connect().await?;
    away.run_until(synced, Client::synced).await.map_err(|e| format!("merge: {e:?}"))?;
    let merge_ms = start.elapsed().as_secs_f64() * 1000.0;
    // The other editor hears the merge; equal or not, the check below says.
    let _ = online.run_until(Duration::from_secs(2), |c| c.text() == away.text()).await;
    let server = api.markdown(&id).await?;
    let equal = server == away.text() && server == online.text();
    away.disconnect().await;
    online.disconnect().await;
    Ok(OfflineReport { offline_updates: n, online_updates, merge_ms, equal })
}

#[derive(Serialize)]
pub struct StoredReport {
    pub created: usize,
    pub create_secs: f64,
    pub created_per_sec: f64,
    /// Creates answered 503 and tried again, and each answer's count.
    pub refused: usize,
    pub reasons: BTreeMap<String, usize>,
    pub lists: usize,
    /// `GET /api/docs`, its first page of 50.
    pub list_p50_ms: f64,
    pub list_p99_ms: f64,
    pub list_max_ms: f64,
}

/// `create=N lists=M`: makes `N` small documents, then lists the user's documents `M` times, one after another.
pub async fn stored(cfg: LoadConfig, n: usize, lists: usize) -> Result<StoredReport, String> {
    let (api, _) = login(&cfg).await?;
    let Created { took, refused, .. } = create(&api, n, |d| format!("# Stored {d}\n")).await?;
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for e in &refused {
        *reasons.entry(e.clone()).or_default() += 1;
    }
    let mut times = Vec::new();
    for _ in 0..lists {
        let start = Instant::now();
        let page = api.call(reqwest::Method::GET, "/api/docs", None).await?;
        if page["partial"].as_bool() == Some(true) {
            return Err("a home did not answer the list".into());
        }
        times.push(start.elapsed());
    }
    times.sort();
    Ok(StoredReport {
        created: n,
        create_secs: took.as_secs_f64(),
        created_per_sec: n as f64 / took.as_secs_f64().max(1e-9),
        refused: refused.len(),
        reasons,
        lists,
        list_p50_ms: percentile(&times, 0.50),
        list_p99_ms: percentile(&times, 0.99),
        list_max_ms: percentile(&times, 1.0),
    })
}

fn save(
    run: &Path,
    logs: &[Log],
    texts: &BTreeMap<String, String>,
    left: &BTreeMap<String, usize>,
) -> std::io::Result<()> {
    std::fs::create_dir_all(run.join("bots"))?;
    std::fs::create_dir_all(run.join("texts"))?;
    for log in logs {
        std::fs::write(run.join("bots").join(format!("{}.json", log.name)), serde_json::to_vec(log)?)?;
    }
    for (id, text) in texts {
        std::fs::write(run.join("texts").join(format!("{id}.md")), text)?;
    }
    std::fs::write(run.join("presence.json"), serde_json::to_vec(left)?)
}

fn check_logs(logs: &[Log], texts: &BTreeMap<String, String>, left: &BTreeMap<String, usize>) -> Check {
    let mut check = Check { presence_left: left.values().sum(), ..Check::default() };
    let removed: BTreeSet<&str> = logs.iter().flat_map(|log| &log.removed).map(String::as_str).collect();
    for log in logs {
        let Some(text) = texts.get(&log.doc) else {
            check.diverged += 1;
            continue;
        };
        let held: BTreeSet<&str> = tokens(text).chain(removed.iter().copied()).collect();
        check.lost_acked += log.acked.iter().filter(|t| !held.contains(t.as_str())).count();
        check.seen_lost += log.seen.iter().filter(|t| !held.contains(t.as_str())).count();
        check.diverged += usize::from(log.text != *text);
    }
    check
}

/// One editor's keystroke: `token` at the end of the editor's own line, which the first one starts at the end of the
/// text, and with `cut` that older token of its own removed, as one update. People type in a place of their own, and
/// so the CRDT keeps each one's run as one item, and a removed run as one tombstone: tokens all typed at the end of
/// the text interleave, cost about 13 bytes each in the state, removed or not, and fill its cap in some 20,000. With
/// it, whether `cut` was in the text.
fn keystroke(doc: &yrs::Doc, name: &str, token: &str, cut: Option<&str>) -> (Vec<u8>, bool) {
    let utf16 = |s: &str| s.encode_utf16().count() as u32;
    let text = crate::crdt::text(doc);
    let marker = format!("\n{name}:");
    let update = match text.find(&marker) {
        Some(line) => {
            let end = text[line + 1..].find('\n').map_or(text.len(), |n| line + 1 + n);
            crate::crdt::insert(doc, utf16(&text[..end]), token)
        }
        None => crate::crdt::insert(doc, utf16(&text), &format!("{marker}{token}")),
    };
    let text = crate::crdt::text(doc);
    match cut.and_then(|oldest| Some((text.find(&format!(" {oldest} "))?, oldest))) {
        Some((byte, oldest)) => {
            let cut = crate::crdt::remove(doc, utf16(&text[..byte]), utf16(oldest) + 2);
            (crate::crdt::merge(&[&update, &cut]).expect("two updates of one document merge").0, true)
        }
        None => (update, false),
    }
}

/// `mdpad check <run>`: the run's logs read back.
pub fn check(run: &Path) -> Result<Check, String> {
    let read = |path: PathBuf| std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()));
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(run.join("bots")).map_err(|e| format!("{}/bots: {e}", run.display()))? {
        let path = entry.map_err(|e| e.to_string())?.path();
        logs.push(serde_json::from_slice::<Log>(&read(path.clone())?).map_err(|e| format!("{}: {e}", path.display()))?);
    }
    let mut texts = BTreeMap::new();
    for entry in std::fs::read_dir(run.join("texts")).map_err(|e| format!("{}/texts: {e}", run.display()))? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let id = path.file_stem().and_then(|s| s.to_str()).unwrap_or_default().to_string();
        texts.insert(id, String::from_utf8_lossy(&read(path)?).into_owned());
    }
    let left: BTreeMap<String, usize> =
        serde_json::from_slice(&read(run.join("presence.json"))?).map_err(|e| format!("presence.json: {e}"))?;
    Ok(check_logs(&logs, &texts, &left))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editors_that_keep_a_hundred_tokens_each_stay_far_under_the_cap() {
        // 10 editors by turns, each removing its oldest token past 100: 30,000 keystrokes. Typed at the end of the
        // text, as the bots did before, the state passed 256 KiB at about 20,000.
        let server = crate::crdt::doc(1);
        let editors: Vec<_> = (0..10u64).map(|i| crate::crdt::doc(100 + i)).collect();
        let mut kept: Vec<Vec<String>> = vec![Vec::new(); editors.len()];
        for k in 0..3000 {
            for (i, doc) in editors.iter().enumerate() {
                let token = format!("d0b{i}k{k}");
                kept[i].push(token.clone());
                let cut = (kept[i].len() > 100).then(|| kept[i].remove(0));
                let (update, removed) = keystroke(doc, &format!("d0b{i}"), &format!(" {token} "), cut.as_deref());
                assert_eq!(removed, cut.is_some());
                crate::crdt::apply_any(&server, &update);
                for (j, other) in editors.iter().enumerate() {
                    if j != i {
                        crate::crdt::apply_any(other, &update);
                    }
                }
            }
        }
        let text = crate::crdt::text(&server);
        assert_eq!(tokens(&text).count(), 10 * 100, "{text}");
        assert!(editors.iter().all(|doc| crate::crdt::text(doc) == text));
        let state = crate::crdt::state(&server).len();
        assert!(state < 64 << 10, "{state} bytes");
    }

    #[test]
    fn tokens_are_heard_in_an_update_and_the_text_starts_at_its_size() {
        let doc = crate::crdt::doc(1);
        let update = crate::crdt::insert(&doc, 0, " d0b1k5  d2b0k10 ");
        assert_eq!(tokens_in(&update), ["d0b1k5", "d2b0k10"]);
        let text = starting_text(3, 20 * 1024);
        assert!(text.starts_with("# Load 3\n") && (20 * 1024 - 48..=20 * 1024).contains(&text.len()), "{}", text.len());
        assert_eq!(tokens(&text).count(), 0);
    }

    /// A token an acked edit lost is not in the text to cut: it must not count as removed, or `check` misses the loss.
    #[test]
    fn a_cut_of_a_token_not_in_the_text_says_so() {
        let doc = crate::crdt::doc(1);
        let (_, removed) = keystroke(&doc, "d0b1", " d0b1k0 ", None);
        assert!(!removed);
        let (_, removed) = keystroke(&doc, "d0b1", " d0b1k1 ", Some("d0b1k7"));
        assert!(!removed);
        assert_eq!(tokens(&crate::crdt::text(&doc)).collect::<Vec<_>>(), ["d0b1k0", "d0b1k1"]);
        let (_, removed) = keystroke(&doc, "d0b1", " d0b1k2 ", Some("d0b1k0"));
        assert!(removed);
        assert_eq!(tokens(&crate::crdt::text(&doc)).collect::<Vec<_>>(), ["d0b1k1", "d0b1k2"]);
    }

    #[test]
    fn tokens_are_told_from_text() {
        let found: Vec<&str> = tokens("# Load 0\n d0b1k5  d12b0k0 dbk1 d1b2 d1b2k3x x d1b2k3").collect();
        assert_eq!(found, ["d0b1k5", "d12b0k0", "d1b2k3"]);
    }

    #[test]
    fn check_counts_what_the_final_text_lacks_and_not_what_an_editor_removed() {
        let texts = BTreeMap::from([("h1-a".to_string(), " d0b0k0  d0b0k1 ".to_string())]);
        let strings = |all: &[&str]| all.iter().map(|t| t.to_string()).collect();
        let log = |name: &str, acked: &[&str], removed: &[&str], seen: &[&str], text: &str| Log {
            doc: "h1-a".into(),
            name: name.into(),
            acked: strings(acked),
            removed: strings(removed),
            seen: strings(seen).into_iter().collect(),
            text: text.into(),
        };
        let logs = [
            log("d0b0", &["d0b0k0", "d0b0k2", "d0b0k4"], &["d0b0k4"], &[], " d0b0k0  d0b0k1 "),
            log("d0b1", &[], &[], &["d0b0k1", "d0b0k3", "d0b0k4"], "other"),
        ];
        let left = BTreeMap::from([("h1-a".to_string(), 1)]);
        assert_eq!(
            check_logs(&logs, &texts, &left),
            Check { lost_acked: 1, seen_lost: 1, diverged: 1, presence_left: 1 }
        );
    }
}
