//! The fault scenario: a seed server and three nodes as processes of their own, a stream of orders from a
//! client node, one node killed with SIGKILL while it runs sagas, and a shipping failure injected into a tenth
//! of the orders. It then waits for every saga to end and checks the databases.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, Placement, Retry, SeedConfig, SeedDiscovery};
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinSet;

use crate::app::{SAGA, SagaApi, SagaCall, retry};
use crate::core::{Fault, Order, Phase};
use crate::store::{SagaRow, Store};
use crate::{Report, catalog, check, orders, transport};

pub struct Plan {
    /// The `saga` binary.
    pub exe: PathBuf,
    pub data: PathBuf,
    pub sagas: usize,
    /// The node is killed once this many sagas were submitted, at the first moment after it when one of its sagas
    /// is past its first step.
    pub kill_after: usize,
    pub fault: Fault,
    pub seed: u64,
}

#[derive(Debug)]
pub struct CrashReport {
    pub report: Report,
    /// Unfinished sagas on the killed node when it died: those its keys placed there.
    pub orphaned: usize,
    /// Of those, the ones that were past their first step.
    pub mid_saga: usize,
    /// Orphaned sagas that ended: nothing calls a saga after its submit, so the cluster started them again.
    /// With `.eager` removed they stay unfinished (README).
    pub resumed: usize,
    pub elapsed: Duration,
    /// From the kill to the end of every saga.
    pub recovery: Duration,
}

const NODES: [&str; 3] = ["n1", "n2", "n3"];
const VICTIM: &str = "n2";

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, String> {
    tokio::task::spawn_blocking(f).await.map_err(|e| e.to_string())
}

/// Starts `exe args` and waits for its first line on stdout.
async fn launch(exe: &PathBuf, args: &[String]) -> Result<(Child, String), String> {
    let mut child = Command::new(exe)
        .args(args)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("{args:?}: {e}"))?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let line = tokio::time::timeout(Duration::from_secs(30), BufReader::new(stdout).lines().next_line())
        .await
        .map_err(|_| format!("{args:?} said nothing for 30 s"))?
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("{args:?} exited"))?;
    Ok((child, line))
}

async fn signal(child: &Child, name: &str) -> Result<(), String> {
    let pid = child.id().ok_or("the process has ended")?;
    let status = Command::new("kill").arg(format!("-{name}")).arg(pid.to_string()).status().await;
    match status.map_err(|e| e.to_string())? {
        s if s.success() => Ok(()),
        s => Err(format!("kill -{name} {pid}: {s}")),
    }
}

/// Submits an order and returns once its saga is stored. Submitting again is harmless, so any error that may
/// clear is retried, for up to a minute.
pub async fn submit(cluster: &Cluster, order: &Order) -> Result<Phase, String> {
    let saga = cluster.keyed::<SagaApi>(SAGA, &order.id, Bytes::new(), &Placement::default());
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut pause = Duration::from_millis(20);
    loop {
        match saga.request(SagaCall::Submit(order.clone())).await {
            Ok(answer) => return answer,
            Err(e) if retry(&e) != Retry::Never && Instant::now() < deadline => {
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_millis(500));
            }
            Err(e) => return Err(format!("{}: {e:?}", order.id)),
        }
    }
}

/// Waits for the saga's end: its actor answers `Wait` when it ends. A wait cut off by the request timeout, a
/// move or a full waiting list is asked again, for up to two minutes.
pub async fn ended(cluster: &Cluster, order: &Order) -> Result<Phase, String> {
    let saga = cluster.keyed::<SagaApi>(SAGA, &order.id, Bytes::new(), &Placement::default());
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match saga.request(SagaCall::Wait).await {
            Ok(Ok(phase)) if phase.terminal() => return Ok(phase),
            Ok(Err(why)) => return Err(format!("{}: {why}", order.id)),
            Err(e) if retry(&e) == Retry::Never || Instant::now() > deadline => {
                return Err(format!("{}: {e:?}", order.id));
            }
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Joins the cluster found through the seed at `seed` as a member that runs nothing, and waits until `nodes`
/// nodes run sagas.
pub async fn client(id: &str, seed: &str, nodes: usize) -> Result<(ActorSystem, Cluster), String> {
    let system = ActorSystem::builder().build().map_err(|e| e.to_string())?;
    let transport = transport(id);
    let discovery = SeedDiscovery::start(SeedConfig::new([seed], &transport)).await.map_err(|e| e.to_string())?;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().expect("an address"), transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    let cluster = Cluster::start(&system, cfg).await.map_err(|e| e.to_string())?;
    let running = move |v: &axon_cluster::View| v.members.iter().filter(|m| m.record.has_kind(SAGA)).count() >= nodes;
    tokio::time::timeout(Duration::from_secs(30), cluster.wait_view(running))
        .await
        .map_err(|_| format!("{nodes} nodes did not join within 30 s"))?
        .map_err(|e| e.to_string())?;
    Ok((system, cluster))
}

pub async fn run(plan: Plan) -> Result<CrashReport, String> {
    let store = Store::open(&plan.data);
    let s = store.clone();
    blocking(move || s.create(&catalog())).await?.map_err(|e| e.to_string())?;
    let data = plan.data.display().to_string();

    let (seed, line) = launch(&plan.exe, &["seed".into()]).await?;
    let seed_addr = line.strip_prefix("seed ").ok_or_else(|| format!("seed said {line}"))?.to_owned();
    let mut nodes = Vec::new();
    for id in NODES {
        let args = [
            "node",
            "--id",
            id,
            "--seeds",
            &seed_addr,
            "--acceptors",
            &NODES.join(","),
            "--data",
            &data,
            "--fail-shipping-every",
            &plan.fault.shipping_every.to_string(),
        ];
        let (child, line) = launch(&plan.exe, &args.map(String::from)).await?;
        if !line.starts_with("ready") {
            return Err(format!("{id} said {line}"));
        }
        nodes.push(child);
    }

    let (system, cluster) = client("client", &seed_addr, NODES.len()).await?;

    let started = Instant::now();
    let mut flight = JoinSet::new();
    let mut killed = None;
    let mut orphans: Vec<SagaRow> = Vec::new();
    for (i, order) in orders(plan.seed, plan.sagas).into_iter().enumerate() {
        if flight.len() == 32 {
            flight.join_next().await.expect("a submit in flight").map_err(|e| e.to_string())??;
        }
        if i >= plan.kill_after && killed.is_none() {
            // Frozen, the victim's sagas stay where the database says they are: it is killed only when one of
            // them is past its first step, else it goes on and is looked at again after the next submit. Its
            // sagas are the keys the view places on it, where a submit started them.
            let victim = &mut nodes[NODES.iter().position(|n| *n == VICTIM).expect("the victim is a node")];
            signal(victim, "STOP").await?;
            let s = store.clone();
            let rows = blocking(move || s.sagas()).await?.map_err(|e| e.to_string())?;
            let on_victim = |id: &str| cluster.owner(SAGA, id, &Placement::default()).as_deref() == Some(VICTIM);
            let held: Vec<SagaRow> =
                rows.into_values().filter(|r| !r.phase.terminal() && on_victim(&r.order.id)).collect();
            if held.iter().any(|r| r.phase != Phase::Forward(0)) {
                victim.start_kill().map_err(|e| e.to_string())?;
                victim.wait().await.map_err(|e| e.to_string())?;
                killed = Some(Instant::now());
                orphans = held;
            } else {
                signal(victim, "CONT").await?;
            }
        }
        let cluster = cluster.clone();
        flight.spawn(async move { submit(&cluster, &order).await });
    }
    while let Some(done) = flight.join_next().await {
        done.map_err(|e| e.to_string())??;
    }

    // Nothing reports the end of every saga to one place, so the check watches the database.
    let deadline = Instant::now() + Duration::from_secs(180);
    let rows = loop {
        let s = store.clone();
        let rows = blocking(move || s.sagas()).await?.map_err(|e| e.to_string())?;
        // Past the deadline the check reports what is still open.
        if rows.values().all(|r| r.phase.terminal()) || Instant::now() > deadline {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let elapsed = started.elapsed();
    let recovery = killed.map_or(Duration::ZERO, |k| k.elapsed());
    let (s, fault) = (store.clone(), plan.fault);
    let report = blocking(move || check(&s, fault)).await?.map_err(|e| e.to_string())?;
    // Not `moves > o.moves`: the orphans are where the view places them, and one the view misplaced ends where it
    // ran without being loaded again.
    let resumed = orphans.iter().filter(|o| rows.get(&o.order.id).is_some_and(|r| r.phase.terminal())).count();
    let mid_saga = orphans.iter().filter(|o| o.phase != Phase::Forward(0)).count();

    drop(cluster);
    system.shutdown().await.map_err(|e| e.to_string())?;
    for mut child in nodes.into_iter().chain([seed]) {
        drop(child.start_kill());
        drop(child.wait().await);
    }
    Ok(CrashReport { report, orphaned: orphans.len(), mid_saga, resumed, elapsed, recovery })
}
