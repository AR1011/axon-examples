//! What isolation does to a home's keyed actors with no lease authority (README § Isolation): a seed, a home `h1`
//! hosting one document actor and a web node `w1` that calls it. The home is cut from both for 2, 5, 7 or 10 s, at
//! `heartbeat_grace` 1 s, the node's own: with no lease authority axon refuses a grace that outlasts the seeds' drop,
//! so 3 s no longer starts. Each run prints when the home called itself isolated, when the actor stopped, and
//! how long after the heal the web node's call answered again. The asserts hold what the design and the README take
//! from it: the document never runs anywhere but its home, a cut of 5 s stops nothing and one of 10 s stops it,
//! and it answers again soon after the heal.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, KEYED_ENDS, Placement, Rejection, Reply, SeedConfig,
    SeedDiscovery, SeedServer, SeedServerConfig, SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

const SEED_PORT: u16 = 7000;

/// Answers each call with its node's id.
struct Doc(String);

impl Actor for Doc {
    type Message = Reply<String>;
    type Error = ();
    fn on_message(&mut self, reply: Reply<String>, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.send(self.0.clone()));
        Ok(())
    }
}

struct DocApi;

impl WireProtocol for DocApi {
    type Message = ();
    type Reply = String;
    const ID: u64 = 0x4D44_00FF;
    const SCHEMA: u16 = 1;
    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }
    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for DocApi {
    type Mailbox = Reply<String>;
    fn encode_reply(node: String) -> Result<Bytes, CallError> {
        Ok(Bytes::from(node))
    }
    fn decode(_body: Bytes, reply: Option<Reply<String>>) -> Result<Reply<String>, Rejection> {
        reply.ok_or(Rejection::InvalidMessage)
    }
}

#[derive(Clone)]
struct Node {
    _system: Option<ActorSystem>,
    cluster: Option<Cluster>,
    _seed: Option<Arc<SeedServer>>,
}

fn home() -> Placement {
    Placement::default().require("home", "h1")
}

async fn boot(host: Host, grace: Duration) -> Node {
    let transport = host.transport(SharedSecret::new("mdpad-spike-secret-16").unwrap());
    let seed = SocketAddr::new(host.ip_of("s"), SEED_PORT);
    if host.name() == "s" {
        let server = SeedServer::bind(SeedServerConfig::new(seed, &transport)).await.unwrap();
        return Node { _system: None, cluster: None, _seed: Some(Arc::new(server)) };
    }
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let discovery = SeedDiscovery::start(SeedConfig::new([seed.to_string()], &transport)).await.unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    cfg.labels = match host.name() {
        "h1" => BTreeMap::from([("homes".into(), "1".into()), ("home".into(), "h1".into())]),
        _ => BTreeMap::from([("web".into(), "1".into())]),
    };
    // As guide/cluster.md § Seed servers asks; the grace is what this spike varies.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = grace;
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    if host.name() == "h1" {
        let name = host.name().to_string();
        cluster.kind::<DocApi>("doc").register(move |_: &SpawnContext, _| Ok(Doc(name.clone()))).await.unwrap();
    }
    Node { _system: Some(system), cluster: Some(cluster), _seed: None }
}

/// Calls the document from `n` until it answers, and returns the node that answered.
async fn call(n: Node) -> String {
    let cluster = n.cluster.expect("a member");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "the document does not answer");
        if let Ok(found) = cluster.get_or_spawn::<DocApi>("doc", "d1", Bytes::new(), &home()).await
            && let Ok(node) = found.request(()).await
        {
            return node;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Run {
    isolated_at: Option<Duration>,
    stopped_at: Option<Duration>,
    answered_after_heal: Duration,
}

fn hosted(w: &World<Node>) -> Option<(bool, usize, u64)> {
    w.read("h1", |n| {
        let cluster = n.cluster.as_ref().expect("a member");
        let hosted = cluster.hosted(10);
        let kind = hosted.kinds.iter().find(|k| k.kind == "doc");
        let isolated = KEYED_ENDS.iter().position(|e| *e == "isolated").unwrap();
        (cluster.isolated(), kind.map_or(0, |k| k.count), kind.map_or(0, |k| k.ended[isolated]))
    })
}

fn isolate(seed: u64, grace: Duration, cut: Duration) -> Run {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in ["s", "h1", "w1"] {
        w.add(name, move |host| boot(host, grace));
    }
    w.run_until("two members, the home with its kind", Duration::from_secs(30), |w| {
        let members =
            |n: &Node| n.cluster.as_ref().map(|c| c.view().members.iter().filter(|m| m.record.has_kind("doc")).count());
        w.nodes().count() == 3 && w.nodes().all(|(_, n)| members(n).is_none_or(|count| count == 1))
    });
    assert_eq!(w.block_on("w1", call).unwrap(), "h1");
    assert_eq!(hosted(&w), Some((false, 1, 0)));

    w.partition(&["h1"], &["s", "w1"]);
    let (start, mut run) = (w.now(), Run { isolated_at: None, stopped_at: None, answered_after_heal: Duration::ZERO });
    while w.now() - start < cut {
        w.run_for(Duration::from_millis(50));
        let (isolated, count, _) = hosted(&w).expect("h1 is up");
        if isolated && run.isolated_at.is_none() {
            run.isolated_at = Some(w.now() - start);
        }
        if count == 0 && run.stopped_at.is_none() {
            run.stopped_at = Some(w.now() - start);
        }
    }
    w.heal();
    let healed = w.now();
    let answered = w.block_on("w1", call).unwrap();
    run.answered_after_heal = w.now() - healed;
    assert_eq!(answered, "h1", "only the home runs its documents");
    let (_, count, ended) = hosted(&w).unwrap();
    assert_eq!((count, ended > 0), (1, run.stopped_at.is_some()), "a stop while cut off is an isolation stop");
    run
}

#[test]
fn a_home_cut_off_from_the_seed_and_the_web_nodes() {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let grace = Duration::from_secs(1);
    for cut in [2, 5, 7, 10].map(Duration::from_secs) {
        let rows = rows.clone();
        check_seeds(seeds(0..4), move |seed| {
            let run = isolate(seed, grace, cut);
            // Within the documented bounds: the seeds drop a silent node after 8 to 10 s, and a call finds the
            // home again within a few heartbeats of the heal.
            assert!(run.answered_after_heal < Duration::from_secs(15), "seed {seed}: {:?}", run.answered_after_heal);
            if cut <= Duration::from_secs(5) {
                assert_eq!(run.stopped_at, None, "seed {seed}: a {cut:?} cut stopped the document");
            }
            if cut >= Duration::from_secs(10) {
                assert!(run.stopped_at.is_some(), "seed {seed}: a {cut:?} cut at grace {grace:?} stopped nothing");
            }
            rows.lock().unwrap().push((grace, cut, seed, run.isolated_at, run.stopped_at, run.answered_after_heal));
        })
        .unwrap();
    }
    let mut rows = rows.lock().unwrap().clone();
    rows.sort_by_key(|r| (r.0, r.1, r.2));
    println!("| grace | cut | seed | isolated after | actor stopped after | answers again after the heal |");
    println!("|---|---|---|---|---|---|");
    for (grace, cut, seed, isolated, stopped, answered) in rows {
        let at = |d: Option<Duration>| d.map_or("no".to_string(), |d| format!("{:.1} s", d.as_secs_f64()));
        println!(
            "| {} s | {} s | {seed} | {} | {} | {:.1} s |",
            grace.as_secs(),
            cut.as_secs(),
            at(isolated),
            at(stopped),
            answered.as_secs_f64()
        );
    }
}
