//! What the tests on the World share: a node of the edge on a host, the World of them booted, and the check that
//! what a node serves never goes backwards.
#![allow(dead_code, reason = "each test file that includes this uses part of it")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon::files::Files;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_testkit::world::{Disk, Host, World, WorldConfig};
use edge_workers::engine::fake::{FakeEngine, FakeVersion};
use edge_workers::follower::FollowerConfig;
use edge_workers::node::{self, Roles, Settings, Stores};

#[derive(Clone)]
pub struct Node {
    pub cluster: Cluster,
    pub node: node::Node<FakeVersion>,
    _system: ActorSystem,
}

/// A node with `roles` on `host`. `tune` changes its cluster's settings and its stores, given the machine's disk.
pub async fn boot(
    host: Host,
    discovery: MemoryDiscovery,
    roles: Roles,
    engine: FakeEngine,
    tune: impl FnOnce(&mut ClusterConfig, &mut Stores, &Disk),
) -> Node {
    let secret = SharedSecret::new("at-least-16-bytes-long").unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(secret), discovery);
    cfg.labels = roles.labels();
    // One disk a machine, a directory a store, as `main.rs` gives each its own.
    let disk = host.disk();
    let dir = |name: &str| Arc::new(disk.dir(name)) as Arc<dyn Files>;
    let mut stores = Stores {
        streams: dir("streams"),
        registry: dir("registry"),
        objects: dir("objects"),
        cache: FollowerConfig::new(dir("cache")),
    };
    tune(&mut cfg, &mut stores, &disk);
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let node = node::start(&system, &cluster, roles, stores, engine, Settings::default()).await.unwrap();
    Node { cluster, node, _system: system }
}

/// The World of `names`, each booted by `boot` with the discovery they share, once every one sees the others.
pub fn booted<Fut: Future<Output = Node> + 'static>(
    seed: u64,
    names: &[&str],
    boot: impl Fn(Host, MemoryDiscovery) -> Fut + Copy + 'static,
) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in names {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    let n = names.len();
    w.run_until("every node booted and sees the others", Duration::from_secs(60), |w| {
        w.nodes().count() == n && w.nodes().all(|(_, node)| node.cluster.view().members.len() == n)
    });
    w
}

pub fn serves(w: &World<Node>, node: &str, worker: &str) -> Option<u64> {
    w.nodes().find(|(name, _)| *name == node)?.1.node.serving.as_ref()?.seq(worker)
}

/// What each node was last seen serving of each worker: a lower seq later, on the same boot or the next, fails.
#[derive(Default)]
pub struct Seen(BTreeMap<(String, String), u64>);

impl Seen {
    pub fn look(&mut self, w: &World<Node>) {
        for (node, n) in w.nodes() {
            let Some(serving) = &n.node.serving else { continue };
            for (worker, current) in serving.all().iter() {
                let last = self.0.entry((node.to_owned(), worker.clone())).or_default();
                let now = current.manifest.seq;
                assert!(now >= *last, "{node} went back from {last} to {now} of {worker}");
                *last = now;
            }
        }
    }
}
