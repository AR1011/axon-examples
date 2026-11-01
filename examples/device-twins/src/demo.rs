//! The whole story in one process, on loopback, checked as it goes: an alerts node and three twin nodes, a fleet
//! of devices reporting over UDP to every node in turn, twins stopping when the fleet goes quiet, and coming back
//! with their state while a fourth twin node joins and takes a quarter of them.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_cluster::{Cluster, MemoryDiscovery, MemoryLeases, Placement};
use bytes::Bytes;

use crate::load::{Fleet, Run};
use crate::twin::{Event, KIND, Op, State, TwinApi, View};
use crate::wire::temperature;
use crate::{AlertsNode, NodeStats, Result, Settings, TwinNode, http_get};

pub struct DemoConfig {
    pub devices: u64,
    /// New readings per device in each of the two rounds.
    pub reports: u32,
    pub period: Duration,
    pub idle: Duration,
}

impl Default for DemoConfig {
    fn default() -> Self {
        // A 2 s idle time, so the demo's twins stop and park within its run.
        Self { devices: 20_000, reports: 8, period: Duration::from_millis(250), idle: Duration::from_secs(2) }
    }
}

/// The alerts `device` raises and clears over its readings `1..=last`, by the same rule the twins run.
pub fn expected_alerts(device: u64, last: u32) -> Vec<(u32, Event)> {
    let mut state = State::default();
    (1..=last).filter_map(|seq| state.apply(seq, temperature(device, seq)).flatten().map(|e| (seq, e))).collect()
}

fn total(nodes: &[TwinNode], of: impl Fn(&NodeStats) -> u64) -> u64 {
    nodes.iter().map(|n| of(&n.stats())).sum()
}

fn seen_by_all(nodes: &[TwinNode], n: usize) -> bool {
    nodes.iter().all(|t| Placement::default().candidates(&t.cluster.view(), KIND).len() == n)
}

async fn until(what: &str, within: Duration, mut done: impl FnMut() -> bool) -> Result<Duration> {
    let started = Instant::now();
    while !done() {
        if started.elapsed() > within {
            return Err(format!("{what}: not within {within:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(started.elapsed())
}

/// Asks every device's twin for its view and checks it against what the fleet had acked: every acked report
/// applied once, none skipped, on the node rendezvous hashing names for the device. Returns twins per node.
async fn check_twins(cluster: &Cluster, fleet: &Fleet) -> Result<BTreeMap<String, u64>> {
    let owner = |device: u64| cluster.owner(KIND, &device.to_string(), &Placement::default());
    let mut asks = tokio::task::JoinSet::new();
    let (mut per_node, mut wrong) = (BTreeMap::new(), Vec::new());
    for (device, acked) in fleet.acked() {
        let (cluster, owner) = (cluster.clone(), owner(device));
        asks.spawn(async move {
            let view = ask(&cluster, device).await?;
            let right = (view.last_seq, view.reports, view.gaps) == (acked, u64::from(acked), 0);
            let placed = owner.as_deref() == Some(view.node.as_str());
            Ok::<_, crate::Error>((view, acked, right && placed))
        });
        while asks.len() >= 256 {
            tally(asks.join_next().await, &mut per_node, &mut wrong)?;
        }
    }
    while let Some(done) = asks.join_next().await {
        tally(Some(done), &mut per_node, &mut wrong)?;
    }
    if !wrong.is_empty() {
        return Err(
            format!("{} twins disagree with the fleet, e.g. {:?}", wrong.len(), &wrong[..wrong.len().min(3)]).into()
        );
    }
    Ok(per_node)
}

/// A twin can be moving, or stop for idleness between the lookup and the call; `request_until` asks again, which
/// starts it again.
async fn ask(cluster: &Cluster, device: u64) -> Result<View> {
    let twin = cluster.keyed::<TwinApi>(KIND, &device.to_string(), Bytes::new(), &Placement::default());
    let asked = twin.request_until(Op::Get, tokio::time::Instant::now() + Duration::from_secs(10)).await;
    asked.map_err(|e| format!("device {device}: {e:?}"))?.ok_or_else(|| "a report's answer to a get".into())
}

type Asked = std::result::Result<Result<(View, u32, bool)>, tokio::task::JoinError>;

fn tally(done: Option<Asked>, per_node: &mut BTreeMap<String, u64>, wrong: &mut Vec<String>) -> Result<()> {
    let Some(done) = done else { return Ok(()) };
    let (view, acked, right) = done??;
    *per_node.entry(view.node.clone()).or_default() += 1;
    if !right {
        wrong.push(format!("device {} acked {acked}: {view:?}", view.device));
    }
    Ok(())
}

/// The alerts node saw exactly the events the rule gives for what every device reported, once each.
async fn check_alerts(alerts: &AlertsNode, fleet: &Fleet) -> Result<u64> {
    let expected: BTreeMap<u64, Vec<(u32, Event)>> = fleet
        .acked()
        .map(|(device, acked)| (device, expected_alerts(device, acked)))
        .filter(|(_, e)| !e.is_empty())
        .collect();
    let want: u64 = expected.values().map(|e| e.len() as u64).sum();
    let seen = || {
        let c = alerts.log.counts();
        c.raised + c.cleared
    };
    until("alerts to arrive", Duration::from_secs(10), || seen() >= want).await?;
    tokio::time::sleep(Duration::from_millis(200)).await; // and no more than that
    let mut history = alerts.log.history();
    history.values_mut().for_each(|events| events.sort());
    let got: BTreeMap<u64, Vec<(u32, Event)>> = history.into_iter().collect();
    if got != expected {
        let differ = expected.iter().find(|(d, e)| got.get(d) != Some(e));
        return Err(format!(
            "alerts differ from the rule: {} seen, {want} expected; first: {differ:?} vs {:?}",
            seen(),
            differ.and_then(|(d, _)| got.get(d))
        )
        .into());
    }
    Ok(want)
}

pub async fn demo(cfg: DemoConfig) -> Result<()> {
    let started = Instant::now();
    let say = |what: String| println!("{:>6.2}s  {what}", started.elapsed().as_secs_f64());
    let discovery = MemoryDiscovery::new();
    let leases = MemoryLeases::new();
    // The demo's idle time, above.
    let settings = |id: &str| Settings { idle: cfg.idle, ..Settings::local(id) };

    let s = settings("alerts");
    let alerts =
        AlertsNode::start(&s, s.transport()?, discovery.clone(), Arc::new(leases.authority()), true, false).await?;
    let mut nodes = Vec::new();
    for id in ["twin-1", "twin-2", "twin-3"] {
        let s = settings(id);
        nodes.push(TwinNode::start(&s, s.transport()?, discovery.clone(), Arc::new(leases.authority())).await?);
    }
    until("three twin nodes", Duration::from_secs(10), || seen_by_all(&nodes, 3)).await?;
    until("the alerts node to follow all three", Duration::from_secs(10), || alerts.log.counts().following.len() == 3)
        .await?;
    let following = alerts.log.counts().following;
    say(format!("alerts node and 3 twin nodes up; alerts follows {}", following.join(" ")));

    // Round one: every device's first readings, each to the next node in turn.
    let targets: Vec<_> = nodes.iter().map(|n| n.udp).collect();
    let mut fleet = Fleet::new(cfg.devices, 8);
    let run = Run::new(targets.clone(), cfg.period, Some(cfg.reports));
    let at = Instant::now();
    fleet.run(&run).await?;
    let acked = fleet.stats.acked.load(std::sync::atomic::Ordering::Relaxed);
    say(format!(
        "{} devices sent {} readings each over UDP in {:.1?}: {acked} acked, {} resent",
        cfg.devices,
        cfg.reports,
        at.elapsed(),
        fleet.stats.resent.load(std::sync::atomic::Ordering::Relaxed)
    ));
    let placed = check_twins(&nodes[0].cluster, &fleet).await?;
    say(format!("every twin holds exactly what its device had acked, on its rendezvous owner: {placed:?}"));
    let alerted = check_alerts(&alerts, &fleet).await?;
    say(format!("the alerts node got exactly the {alerted} raises and clears the rule gives"));

    // Quiet: every twin stops after the idle time and parks its state.
    let quiet =
        until("every twin to stop", cfg.idle * 3 + Duration::from_secs(10), || total(&nodes, |s| s.twins as u64) == 0)
            .await?;
    let parked = total(&nodes, |s| s.parked as u64);
    let stopped = total(&nodes, |s| s.twin.stopped);
    // A twin whose device finished early can stop before the checks above, which start it again.
    if parked != cfg.devices || stopped < cfg.devices {
        return Err(format!("{stopped} twins stopped idle and {parked} parked, of {}", cfg.devices).into());
    }
    say(format!("fleet quiet: {stopped} idle stops within {quiet:.1?}, and all {} states parked", cfg.devices));

    // Any node answers for any device; asking brings the twin back.
    for (node, device) in nodes.iter().zip([8u64, 9, 16]) {
        let (status, body) = http_get(node.http, &format!("/devices/{device}")).await?;
        let view: View = serde_json::from_str(&body).map_err(|e| format!("{status} {body}: {e}"))?;
        if status != 200 || view.last_seq != cfg.reports || view.reports != u64::from(cfg.reports) {
            return Err(format!("GET /devices/{device} on {}: {status} {body}", node.shared.node).into());
        }
    }
    say(format!(
        "GET /devices/<id> on each node answers from the parked state, e.g. {}",
        http_get(nodes[1].http, "/devices/8").await?.1.trim()
    ));

    // Round two: the fleet comes back, and a fourth node joins while it reports.
    let mut run = run;
    run.deadline = Duration::from_secs(90);
    let at = Instant::now();
    let reporting = tokio::spawn(async move {
        let result = fleet.run(&run).await;
        (fleet, result)
    });
    tokio::time::sleep(cfg.period * 2).await;
    let s = settings("twin-4");
    nodes.push(TwinNode::start(&s, s.transport()?, discovery.clone(), Arc::new(leases.authority())).await?);
    say("twin-4 joined while the fleet reports".into());
    let (fleet, result) = reporting.await?;
    result?;
    until("four twin nodes", Duration::from_secs(10), || seen_by_all(&nodes, 4)).await?;
    say(format!("round two done in {:.1?}", at.elapsed()));
    let placed = check_twins(&nodes[0].cluster, &fleet).await?;
    say(format!("every twin again exact and on its owner among four: {placed:?}"));
    let restored = total(&nodes, |s| s.twin.restored_local + s.twin.restored_remote);
    let (remote, fresh) = (total(&nodes, |s| s.twin.restored_remote), total(&nodes, |s| s.twin.fresh));
    if fresh != cfg.devices {
        return Err(format!("{fresh} twins started with no state, not just the {} first starts", cfg.devices).into());
    }
    say(format!("{restored} twin starts took parked state, {remote} of them from another node; none started empty"));
    let alerted = check_alerts(&alerts, &fleet).await?;
    say(format!("alerts still exact across the stop, the restart and the move: {alerted}"));

    let lost: Vec<String> = nodes
        .iter()
        .map(|n| n.stats())
        .filter(|s| s.twin.gaps + s.parked_dropped + s.twin.publish_errors + s.malformed > 0)
        .map(|s| format!("{s:?}"))
        .collect();
    if !lost.is_empty() || alerts.log.counts().resets > 0 {
        return Err(format!("something was dropped: {lost:?}, alert resets {}", alerts.log.counts().resets).into());
    }
    let dups = total(&nodes, |s| s.twin.duplicates);
    say(format!("nothing lost: no gaps, nothing dropped from parking; {dups} resent reports dropped as duplicates"));

    // A device that never reported: asking starts its twin, which has nothing to show.
    let (status, body) = http_get(nodes[0].http, &format!("/devices/{}", cfg.devices)).await?;
    if status != 404 {
        return Err(format!("GET /devices/<id> of a device that never reported: {status} {body}").into());
    }

    for node in nodes {
        node.shutdown().await?;
    }
    alerts.shutdown().await?;
    Ok(())
}
