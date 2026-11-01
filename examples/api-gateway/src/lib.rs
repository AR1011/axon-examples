//! An HTTP API gateway in front of backend actors reached by remote calls.
//!
//! [`service`] is the backend: a `Quote` actor published on backend nodes. [`balancer`] is the
//! gateway's I/O-free routing core (least-loaded choice, outlier ejection, retry budget), driven by
//! the real gateway and by the seeded simulator test alike. [`gateway`] is the HTTP front and the
//! membership-driven backend list.

pub mod balancer;
pub mod gateway;
pub mod service;

use std::time::Duration;

/// Metadata key a backend sets once its service is published: the gateway's cue to resolve it.
pub const SERVES: &str = "serves";
pub const SERVICE: &str = "quote";

pub fn transport(id: &str) -> axon_net::Config {
    let secret = axon_net::SharedSecret::new(*b"api-gateway example secret").expect("a 26-byte secret");
    let mut transport = axon_net::Config::new(id, secret);
    transport.allow_plaintext = true; // tests on loopback, the binary only with --plaintext
    transport
}

/// Cluster settings for [`axon_cluster::SeedDiscovery`], as the cluster guide asks.
pub fn seed_timing(cfg: &mut axon_cluster::ClusterConfig) {
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
}
