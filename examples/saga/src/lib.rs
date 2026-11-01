//! Order fulfilment as sagas on axon. See README.md.

pub mod app;
pub mod core;
pub mod crash;
pub mod store;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, ClusterError, PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery};
use axon_net::SharedSecret;

use crate::core::{Fault, Hold, INVENTORY, Line, Order, PAYMENT, Phase, Pool, SERVICES, SHIPPING};
use crate::store::{Store, StoreError};

pub const SKUS: usize = 20;
pub const ACCOUNTS: usize = 50;
pub const REGIONS: [&str; 3] = ["eu", "us", "apac"];

/// The pools the databases start with: stock per sku, balance per account, shipping slots per region.
pub fn catalog() -> Vec<(&'static str, String, u64)> {
    let skus = (0..SKUS).map(|i| (INVENTORY, format!("sku-{i:02}"), 250));
    let accounts = (0..ACCOUNTS).map(|i| (PAYMENT, format!("acct-{i:02}"), 1500));
    let regions = REGIONS.iter().map(|r| (SHIPPING, (*r).to_owned(), 800));
    skus.chain(accounts).chain(regions).collect()
}

fn price(sku: usize) -> u64 {
    5 + sku as u64
}

/// `n` orders from `seed`: one to three distinct skus of one to three units, an account and a region.
pub fn orders(seed: u64, n: usize) -> Vec<Order> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move |below: usize| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % below as u64) as usize
    };
    (0..n)
        .map(|i| {
            let first = next(SKUS);
            let lines: Vec<Line> = (0..1 + next(3))
                .map(|j| Line { sku: format!("sku-{:02}", (first + j * 7) % SKUS), qty: 1 + next(3) as u64 })
                .collect();
            let amount = lines.iter().map(|l| l.qty * price(l.sku[4..].parse::<usize>().unwrap_or(0))).sum();
            Order {
                id: format!("order-{seed}-{i:05}"),
                lines,
                account: format!("acct-{:02}", next(ACCOUNTS)),
                amount,
                region: REGIONS[next(REGIONS.len())].to_owned(),
            }
        })
        .collect()
}

pub fn transport(id: &str) -> axon_net::Config {
    let mut transport =
        axon_net::Config::new(id, SharedSecret::new(*b"saga example secret, loopback only").expect("16 bytes or more"));
    transport.allow_plaintext = true; // loopback only
    transport
}

/// A node of a cluster found through seed servers, whose keys are owned under leases from `acceptors`.
pub async fn start_node(
    system: &ActorSystem,
    id: &str,
    seeds: &[String],
    acceptors: &[String],
    data: &Path,
) -> Result<Cluster, ClusterError> {
    let transport = transport(id);
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().expect("an address"), transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    let mut paxos = PaxosConfig::new(acceptors);
    // An acceptor logs who holds each key here; a node that is not one never reads it.
    paxos.store = Some(Arc::new(axon::files::OsFiles::new(data.join("leases").join(id))));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
    Cluster::start(system, cfg).await
}

/// What the databases hold once every saga has ended.
#[derive(Debug, Default)]
pub struct Report {
    pub sagas: usize,
    pub done: usize,
    pub compensated: usize,
    pub unfinished: usize,
    /// Why sagas compensated, and how many.
    pub reasons: BTreeMap<String, usize>,
    /// Sagas taken up again at least once before they ended: by another node, or after a restart.
    pub moved: usize,
    pub violations: Vec<String>,
}

/// Checks the databases: [`verify`] on what they hold.
pub fn check(store: &Store, fault: Fault) -> Result<Report, StoreError> {
    let sagas = store.sagas()?.into_iter().map(|(id, row)| (id, (row.order, row.phase, row.moves))).collect();
    let mut pools = BTreeMap::new();
    for service in SERVICES {
        for (key, pool) in store.pools(service)? {
            pools.insert((service, key), pool);
        }
    }
    Ok(verify(&sagas, &pools, fault))
}

/// Checks every saga, given as `(order, phase, moves)`, and every pool, given as `(starting amount, pool)`,
/// against each other:
/// - a done saga holds exactly its amount in each of its pools, a compensated one holds nothing anywhere;
/// - every pool's available amount plus what sagas hold equals what it started with, so nothing was applied twice
///   or released twice;
/// - every hold belongs to a saga, and every saga the fault refuses has compensated.
pub fn verify(
    sagas: &BTreeMap<String, (Order, Phase, u64)>,
    pools: &BTreeMap<(&str, String), (u64, Pool)>,
    fault: Fault,
) -> Report {
    let mut report = Report { sagas: sagas.len(), ..Default::default() };
    for (id, (order, phase, moves)) in sagas {
        report.moved += usize::from(*moves > 0);
        let done = match phase {
            Phase::Done => {
                report.done += 1;
                true
            }
            Phase::Compensated { reason } => {
                report.compensated += 1;
                *report.reasons.entry(reason_class(reason)).or_default() += 1;
                false
            }
            other => {
                report.unfinished += 1;
                report.violations.push(format!("{id} unfinished in {other:?}"));
                continue;
            }
        };
        if done && fault.refuses(SHIPPING, id) {
            report.violations.push(format!("{id} done though shipping refuses it"));
        }
        for step in order.steps() {
            let hold = pools.get(&(step.service, step.key.clone())).and_then(|p| p.1.holds.get(id).copied());
            let held = hold.filter(|h| matches!(h, Hold::Applied(_)));
            if held != done.then_some(Hold::Applied(step.amount)) {
                report.violations.push(format!("{id} {phase:?}: {} {} holds {hold:?}", step.service, step.key));
            }
        }
    }
    for ((service, key), (initial, pool)) in pools {
        if pool.available + pool.held() != *initial {
            let (available, held) = (pool.available, pool.held());
            report.violations.push(format!("{service} {key}: {available} available + {held} held != {initial}"));
        }
        for saga in pool.holds.keys().filter(|s| !sagas.contains_key(*s)) {
            report.violations.push(format!("{service} {key} holds for unknown saga {saga}"));
        }
    }
    report
}

fn reason_class(reason: &str) -> String {
    match reason {
        r if r.contains("available") => "insufficient".into(),
        r => r.split(':').next().unwrap_or(r).into(),
    }
}
