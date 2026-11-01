//! The scale run: an alerts process and three twin processes on this machine, this process as the fleet, and a
//! fourth twin process started a third of the way in. Prints one line a second and a summary, then checks that
//! every report was applied once and every alert arrived.
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use axon::Histogram;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::demo::expected_alerts;
use crate::load::{Fleet, Run};
use crate::{NodeStats, Result, http_get};

/// What a node process prints once it serves.
#[derive(Debug, Serialize, Deserialize)]
pub struct Ready {
    pub node: String,
    pub seed: Option<SocketAddr>,
    pub udp: Option<SocketAddr>,
    pub http: SocketAddr,
}

pub struct Scale {
    pub devices: u64,
    /// Readings each device sends, one every `period`.
    pub reports: u32,
    pub period: Duration,
}

struct Node {
    process: tokio::process::Child,
    ready: Ready,
    /// Resident memory once it served, before any twin.
    rss_kb: u64,
}

async fn launch(role: &'static str, id: String, seeds: String) -> Result<Node> {
    let mut process = Command::new(std::env::current_exe()?)
        .arg(role)
        .env("NODE_ID", &id)
        .env("SEEDS", seeds)
        .env("IDLE_SECS", "600")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(process.stdout.take().ok_or("no stdout")?).lines();
    let ready = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(line) = lines.next_line().await? {
            match line.strip_prefix("READY ") {
                Some(json) => return Ok::<Ready, crate::Error>(serde_json::from_str(json)?),
                None => println!("[{id}] {line}"),
            }
        }
        Err(format!("{id} exited before it served").into())
    })
    .await
    .map_err(|_| "a node took over 90 s to start")??;
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            println!("[{id}] {line}");
        }
    });
    let rss_kb = stats(ready.http).await?.rss_kb;
    Ok(Node { process, ready, rss_kb })
}

async fn stats(http: SocketAddr) -> Result<NodeStats> {
    let (status, body) = http_get(http, "/stats").await?;
    if status != 200 {
        return Err(format!("/stats on {http}: {status}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

async fn all_stats(nodes: &[Node]) -> Result<Vec<NodeStats>> {
    let mut all = Vec::new();
    for node in nodes {
        all.push(stats(node.ready.http).await?);
    }
    Ok(all)
}

fn merged(stats: &[NodeStats]) -> Histogram {
    let mut all = Histogram::default();
    stats.iter().for_each(|s| all.merge(&s.latency));
    all
}

fn ms(h: &Histogram, p: f64) -> String {
    h.percentile(p).map_or("-".into(), |d| format!("{:.1}", d.as_secs_f64() * 1e3))
}

pub async fn scale(cfg: Scale) -> Result<()> {
    let started = Instant::now();
    let alerts = launch("alerts", "alerts".into(), String::new()).await?;
    let seeds = alerts.ready.seed.ok_or("the alerts node runs no seed server")?.to_string();
    let mut twins = Vec::new();
    for i in 1..=3 {
        twins.push(launch("twin", format!("twin-{i}"), seeds.clone()).await?);
    }
    println!("{:>6.1}s  alerts and 3 twin nodes serve, the acceptor is warm", started.elapsed().as_secs_f64());

    let targets: Vec<_> = twins.iter().filter_map(|t| t.ready.udp).collect();
    let mut run = Run::new(targets, cfg.period, Some(cfg.reports));
    run.deadline = cfg.period * cfg.reports * 3 + Duration::from_secs(120);
    let mut fleet = Fleet::new(cfg.devices, 16);
    let load = fleet.stats.clone();
    let at = Instant::now();
    let mut reporting = tokio::spawn(async move {
        let result = fleet.run(&run).await;
        (fleet, result)
    });

    let join_at = cfg.period * cfg.reports / 3;
    let (mut joining, mut joined) = (None, false);
    let (mut last, mut last_at) = (all_stats(&twins).await?, Instant::now());
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    println!(
        "     t  twins per node                      reports/s   p50 ms  p99 ms  handoffs  isolated  leases lost  late  resent  rss MB per node"
    );
    let mut rows = Vec::new();
    let (fleet, result) = loop {
        tokio::select! {
            done = &mut reporting => break done?,
            _ = tick.tick() => {}
        }
        if !joined && at.elapsed() >= join_at {
            joined = true;
            println!("{:>6.1}s  starting twin-4", at.elapsed().as_secs_f64());
            joining = Some(tokio::spawn(launch("twin", "twin-4".into(), seeds.clone())));
        }
        if let Some(join) = joining.take_if(|j| j.is_finished()) {
            twins.push(join.await??);
            println!("{:>6.1}s  twin-4 serves", at.elapsed().as_secs_f64());
            last.push(NodeStats::default());
        }
        let now = all_stats(&twins).await?;
        let dt = last_at.elapsed().as_secs_f64();
        let reports: u64 = now.iter().zip(&last).map(|(n, l)| n.twin.reports - l.twin.reports).sum();
        // None only if a node's histogram went back, which no restart here makes: then no latency for the row.
        let latency = merged(&now).since(&merged(&last)).unwrap_or_default();
        let per_node: Vec<String> = now.iter().map(|s| s.twins.to_string()).collect();
        let rss: Vec<String> = now.iter().map(|s| (s.rss_kb / 1024).to_string()).collect();
        let handoffs: u64 = now.iter().map(|s| s.handoffs).sum();
        let isolated: u64 = now.iter().map(|s| s.isolations).sum();
        let leases_lost: u64 = now.iter().map(|s| s.leases_lost).sum();
        let rate = reports as f64 / dt;
        println!(
            "{:>6.1}  {:<34}  {:>9.0}  {:>7}  {:>6}  {:>8}  {:>8}  {:>11}  {:>4}  {:>6}  {}",
            at.elapsed().as_secs_f64(),
            per_node.join(" "),
            rate,
            ms(&latency, 0.5),
            ms(&latency, 0.99),
            handoffs,
            isolated,
            leases_lost,
            load.late.load(Relaxed),
            load.resent.load(Relaxed),
            rss.join(" ")
        );
        rows.push((at.elapsed(), rate, latency));
        (last, last_at) = (now, Instant::now());
    };
    result?;
    let took = at.elapsed();

    let want: u64 = fleet.acked().map(|(d, acked)| expected_alerts(d, acked).len() as u64).sum();
    let deadline = Instant::now() + Duration::from_secs(60);
    let seen = loop {
        let counts = stats(alerts.ready.http).await?.alerts.unwrap_or_default();
        let seen = counts.raised + counts.cleared;
        if seen >= want || Instant::now() > deadline {
            break seen;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let end = all_stats(&twins).await?;
    let applied: u64 = end.iter().map(|s| s.twin.reports).sum();
    let acked = load.acked.load(Relaxed);
    let live: u64 = end.iter().map(|s| s.twins as u64).sum();
    let mut all = Histogram::default();
    rows.iter().for_each(|(.., h)| all.merge(h));
    let steady: Vec<f64> = rows.iter().filter(|(t, ..)| *t > Duration::from_secs(5)).map(|(_, r, _)| *r).collect();
    println!("--");
    println!(
        "{} devices x {} readings in {:.1?}: {acked} acked, {applied} applied, {live} twins live",
        cfg.devices, cfg.reports, took
    );
    println!(
        "reports/s: mean {:.0} over the run, median second {:.0}; latency p50 {} ms p99 {} ms (bucket bounds, at most 12.5 % high)",
        acked as f64 / took.as_secs_f64(),
        median(steady),
        ms(&all, 0.5),
        ms(&all, 0.99)
    );
    for (node, s) in twins.iter().zip(&end) {
        let per_twin = (s.rss_kb.saturating_sub(node.rss_kb) * 1024).checked_div(s.twins as u64).unwrap_or(0);
        println!(
            "{}: {} twins, rss {} MB (+{} MB, {per_twin} B per twin), handoffs {} (failed {}), refused {}, keyed_failed {}, unacked {}",
            s.node,
            s.twins,
            s.rss_kb / 1024,
            s.rss_kb.saturating_sub(node.rss_kb) / 1024,
            s.handoffs,
            s.handoffs_failed,
            s.spawn_refused,
            s.keyed_failed,
            s.unacked
        );
        println!("  isolations {}, leases lost {}, {:?}", s.isolations, s.leases_lost, s.twin);
    }
    let a = stats(alerts.ready.http).await?;
    println!("alerts: {seen} of {want} expected; alerts node rss {} MB; leases {:?}", a.rss_kb / 1024, a.leases);

    let lost: u64 = end.iter().map(|s| s.twin.gaps + s.parked_dropped + s.twin.publish_errors).sum();
    let restored: u64 = end.iter().map(|s| s.twin.restored_local + s.twin.restored_remote).sum();
    let fresh: u64 = end.iter().map(|s| s.twin.fresh).sum();
    println!("twin starts: {fresh} fresh, {restored} restored from a parked state; gaps and losses {lost}");
    for mut node in twins.into_iter().chain([alerts]) {
        if let Some(pid) = node.process.id() {
            Command::new("kill").arg("-INT").arg(pid.to_string()).status().await?;
        }
        tokio::time::timeout(Duration::from_secs(30), node.process.wait()).await??;
    }
    let expected = cfg.devices * u64::from(cfg.reports);
    if acked != expected || applied != expected || seen != want || lost > 0 || fresh != cfg.devices {
        return Err(format!(
            "acked {acked}, applied {applied}, expected {expected}; alerts {seen} of {want}; lost {lost}; fresh {fresh}"
        )
        .into());
    }
    Ok(())
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v.get(v.len() / 2).copied().unwrap_or(0.0)
}
