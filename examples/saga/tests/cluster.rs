//! Three nodes in one process on `PaxosLeases`, one of them leaving in the middle: every saga ends, done or
//! compensated, and the databases agree with each other.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::MemoryFiles;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, PaxosConfig, PaxosLeases};
use axon_testkit::eventually_within;
use saga::app::{SAGA, serve};
use saga::core::{Fault, Phase, SHIPPING};
use saga::crash::{ended, submit};
use saga::store::Store;
use saga::{catalog, check, orders, transport};
use tokio::task::JoinSet;

fn scratch(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::UNIX_EPOCH.elapsed().expect("after 1970").subsec_nanos();
    std::env::temp_dir().join(format!("saga-{name}-{}-{nanos}", std::process::id()))
}

/// Each node is an acceptor too: the saga kind is eager, which needs an authority that lists a node's keys.
async fn node(id: &str, discovery: &MemoryDiscovery, store: &Store, fault: Fault) -> (ActorSystem, Cluster) {
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport(id), discovery.clone());
    let mut paxos = PaxosConfig::new(["a", "b", "c"]);
    paxos.store = Some(Arc::new(MemoryFiles::new()));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    serve(&cluster, store.clone(), fault).await.unwrap();
    (system, cluster)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_saga_ends_consistent_while_a_node_leaves() {
    let dir = scratch("leave");
    let store = Store::open(&dir);
    store.create(&catalog()).unwrap();
    let fault = Fault { shipping_every: 10 };
    let discovery = MemoryDiscovery::new();
    let (system_a, a) = node("a", &discovery, &store, fault).await;
    let (system_b, b) = node("b", &discovery, &store, fault).await;
    let (system_c, c) = node("c", &discovery, &store, fault).await;
    for cluster in [&a, &b, &c] {
        cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind(SAGA)).count() == 3).await.unwrap();
    }
    // The acceptors grant nothing for a little over a lease after they start.
    eventually_within("the acceptors grant leases", Duration::from_secs(20), async || {
        [&a, &b, &c].iter().all(|c| !c.warming())
    })
    .await;

    let all = orders(5, 600);
    let mut flight: JoinSet<Result<Phase, String>> = JoinSet::new();
    let mut b = Some((system_b, b));
    for (i, order) in all.iter().cloned().enumerate() {
        if flight.len() == 32 {
            flight.join_next().await.unwrap().unwrap().unwrap();
        }
        if i == 200 {
            // A clean leave: b's sagas and pools stop, give up their leases, and start again on a or c.
            let (system, cluster) = b.take().unwrap();
            drop(cluster);
            system.shutdown().await.unwrap();
        }
        let cluster = [&a, &c][i % 2].clone();
        flight.spawn(async move { submit(&cluster, &order).await });
    }
    while let Some(done) = flight.join_next().await {
        done.unwrap().unwrap();
    }
    for order in &all {
        ended(&a, order).await.unwrap();
    }

    let report = check(&store, fault).unwrap();
    assert!(report.violations.is_empty(), "{:#?}", report.violations);
    assert_eq!((report.sagas, report.unfinished), (all.len(), 0));
    let refused = all.iter().filter(|o| fault.refuses(SHIPPING, &o.id)).count();
    assert!(report.reasons["injected failure"] > 0 && report.reasons["injected failure"] <= refused, "{report:?}");
    assert!(report.done > all.len() / 2, "{report:?}");
    assert!(report.moved > 0, "no saga moved off the leaving node: {report:?}");

    drop((a, c));
    system_a.shutdown().await.unwrap();
    system_c.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
