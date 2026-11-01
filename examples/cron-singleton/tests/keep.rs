//! The keeper over a real node: a singleton that ends before the watch ever shows it running is started again.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, MemoryLeases, SpawnContext};
use axon_net::SharedSecret;
use bytes::Bytes;
use cron_singleton::{CronApi, KIND, Msg, keep};

/// Ends as it starts, as a scheduler fenced at its claim does.
struct Quits(Arc<AtomicUsize>);

impl Actor for Quits {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.0.fetch_add(1, Ordering::Relaxed);
        cx.stop();
        Ok(())
    }

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_singleton_that_ends_before_the_watch_sees_it_is_started_again() {
    let mut transport = axon_net::Config::new("s1", SharedSecret::new(b"keep-test-secret").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    cfg.ownership = Some(Arc::new(MemoryLeases::new().authority()));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    let counted = starts.clone();
    let build = move |_: &SpawnContext, _: Bytes| Ok(Quits(counted.clone()));
    cluster.kind::<CronApi>(KIND).register(build).await.unwrap();

    let keeper = tokio::spawn(keep(cluster.clone(), Bytes::new()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while starts.load(Ordering::Relaxed) < 3 {
        assert!(tokio::time::Instant::now() < deadline, "started {} times in 10 s", starts.load(Ordering::Relaxed));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!keeper.is_finished(), "the keeper stopped");
    keeper.abort();
    system.shutdown().await.unwrap();
}
