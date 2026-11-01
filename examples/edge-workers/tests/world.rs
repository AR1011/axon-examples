//! The control plane on the World: a control node, two gateways and three runners as real nodes on virtual time,
//! with the fake engine. A deploy and a rollback reach every node, the control node restarts with everything, a
//! runner cut off during a deploy catches up, and one that starts while the control node is down serves from its
//! disk. Through all of it, what a node serves never goes backwards.

#[path = "support/world.rs"]
mod on_world;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon_cluster::MemoryDiscovery;
use axon_testkit::world::{Crash, Host, World};
use axon_testkit::{check_seeds, seeds};
use edge_workers::engine::fake::{FakeEngine, FakeRun};
use edge_workers::engine::{Engine, Env, Limits, Request};
use edge_workers::node::{Client, Error, Roles};
use edge_workers::wire::{Refused, Upload};
use on_world::{Node, Seen, serves};

const CONTROL: &str = "control";
const GATEWAYS: [&str; 2] = ["gw-1", "gw-2"];
const RUNNERS: [&str; 3] = ["r-1", "r-2", "r-3"];
const WITHIN: Duration = Duration::from_secs(60);

/// Each handler answers its own name, and the `GREETING` it was deployed with.
fn engine() -> FakeEngine {
    let answer = |name: &'static str| {
        move |_: &Request, env: &Env| FakeRun::ok(format!("{name}{}", env.get("GREETING").map_or("", String::as_str)))
    };
    FakeEngine::new().handler("one", answer("one")).handler("two", answer("two")).handler("three", answer("three"))
}

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let name = host.name();
    let roles = Roles { control: name == CONTROL, gateway: name.starts_with("gw"), runner: name.starts_with("r-") };
    // The fake engine's bundles are a few bytes: this makes each come in several pieces.
    on_world::boot(host, discovery, roles, engine(), |_, stores, _| stores.cache.piece = 3).await
}

fn world(seed: u64) -> World<Node> {
    let names: Vec<_> = [CONTROL].into_iter().chain(GATEWAYS).chain(RUNNERS).collect();
    on_world::booted(seed, &names, boot)
}

/// Steps until each of `nodes` serves `worker` at `seq`, watching that no node goes backwards on the way.
fn reach(w: &mut World<Node>, seen: &mut Seen, nodes: &[&str], worker: &str, seq: u64) {
    let what = format!("{nodes:?} serving {worker} at {seq}");
    w.run_until(&what, WITHIN, |w| {
        seen.look(w);
        nodes.iter().all(|node| serves(w, node, worker) == Some(seq))
    });
}

/// Times the node's table of what it serves reached its disk, since it started.
fn saved(n: &Node) -> u64 {
    n.node.serving.as_ref().map_or(0, |serving| serving.stats.saved.load(Relaxed))
}

fn followers() -> Vec<&'static str> {
    GATEWAYS.into_iter().chain(RUNNERS).collect()
}

fn registry<T: 'static, Fut: Future<Output = Result<T, Error>> + 'static>(
    w: &mut World<Node>,
    from: &str,
    call: impl FnOnce(Client) -> Fut + 'static,
) -> Result<T, Error> {
    w.block_on(from, move |n| call(Client::new(&n.cluster))).expect("the calling node is up")
}

fn upload(name: &str, source: &str, deploy: bool) -> Upload {
    Upload { name: name.into(), source: Some(source.into()), deploy, ..Upload::default() }
}

/// What a request to `worker` on `runner` answers, run on the version the runner would pick for `sample`.
fn answer(w: &mut World<Node>, runner: &str, worker: &'static str, sample: u64) -> String {
    w.block_on(runner, move |n| async move {
        let current = n.node.serving.as_ref().unwrap().get(worker).expect("the worker is served");
        let (_, version) = current.pick(sample).expect("a runner holds the version prepared");
        let outcome = engine().invoke(version, Request::get("/"), Limits::default()).await.unwrap();
        String::from_utf8(outcome.body.to_vec()).unwrap()
    })
    .unwrap()
}

fn a_deploy_a_rollback_a_canary_and_a_removal_reach_every_node(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    let from = GATEWAYS[w.pick(0..2) as usize];

    // A source the engine does not take is refused with the engine's reason, and makes nothing.
    let refused = registry(&mut w, from, |r| async move { r.upload(upload("hello", "nope", true)).await });
    assert!(matches!(&refused, Err(Error::Refused(Refused::Load(why))) if why.contains("nope")), "{refused:?}");
    assert_eq!(registry(&mut w, from, |r| async move { r.list().await }).unwrap(), vec![]);

    let first = registry(&mut w, from, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    assert_eq!(first, (1, Some(1)));
    reach(&mut w, &mut seen, &followers(), "hello", 1);
    let second = registry(&mut w, from, |r| async move { r.upload(upload("hello", "two", true)).await }).unwrap();
    assert_eq!(second, (2, Some(2)));
    reach(&mut w, &mut seen, &followers(), "hello", 2);
    for runner in RUNNERS {
        assert_eq!(answer(&mut w, runner, "hello", 0), "two", "seed {seed}: {runner}");
    }

    // A rollback is a deployment like any other: a higher seq that names the older version.
    assert_eq!(registry(&mut w, from, |r| async move { r.deploy("hello", 1, None).await }).unwrap(), 3);
    reach(&mut w, &mut seen, &followers(), "hello", 3);
    for runner in RUNNERS {
        assert_eq!(answer(&mut w, runner, "hello", 0), "one", "seed {seed}: {runner}");
    }

    // A canary: both versions prepared before the switch, and the canary takes its percent.
    assert_eq!(registry(&mut w, from, |r| async move { r.deploy("hello", 1, Some((2, 10))).await }).unwrap(), 4);
    reach(&mut w, &mut seen, &followers(), "hello", 4);
    for runner in RUNNERS {
        assert_eq!(answer(&mut w, runner, "hello", 9), "two", "seed {seed}: {runner}");
        assert_eq!(answer(&mut w, runner, "hello", 10), "one", "seed {seed}: {runner}");
    }
    // A gateway routes by the manifest and prepares nothing.
    let gateway = &w.nodes().find(|(name, _)| *name == GATEWAYS[0]).unwrap().1.node;
    let routed = gateway.serving.as_ref().unwrap().get("hello").unwrap();
    assert_eq!(routed.manifest.pick(9).id, 2);
    assert!(routed.pick(9).is_none());

    registry(&mut w, from, |r| async move { r.delete("hello").await }).unwrap();
    w.run_until("no node serving the removed worker", WITHIN, |w| {
        followers().iter().all(|node| serves(w, node, "hello").is_none())
    });
    println!("world digest deploy seed {seed}: {}", w.digest());
}

fn the_control_node_restarts_with_everything(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    let with_env = Upload {
        vars: Some(Env::from([("GREETING".into(), " hi".into())])),
        secrets: BTreeMap::from([("TOKEN".into(), Some("s3cret".into()))]),
        message: "with a greeting".into(),
        ..upload("hello", "two", false)
    };
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    registry(&mut w, CONTROL, |r| async move { r.upload(with_env).await }).unwrap();
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("other", "three", true)).await }).unwrap();
    let read = |r: Client| async move { Ok((r.list().await?, r.get("hello").await?, r.version("hello", 2).await?)) };
    let before = registry(&mut w, CONTROL, read).unwrap();
    assert_eq!(before.2.source, "two");
    assert_eq!(before.2.secrets, vec!["TOKEN".to_owned()], "a secret's name, never its value");
    assert!(!format!("{before:?}").contains("s3cret"));

    // The crash comes right after the last answer or a little later: an answered change is on the disk either way,
    // though its manifest may not have reached the stream's files yet.
    let pause = Duration::from_millis(w.pick(0..20));
    w.run_for(pause);
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash(CONTROL, how);
    let down = Duration::from_millis(w.pick(0..15_000));
    w.run_for(down);
    seen.look(&w);
    w.restart(CONTROL);

    assert_eq!(registry(&mut w, CONTROL, read).unwrap(), before, "seed {seed}, {how:?}");
    reach(&mut w, &mut seen, &followers(), "hello", 1);
    reach(&mut w, &mut seen, &followers(), "other", 2);
    // The next deployment goes on from the last seq, and the version uploaded before the crash is there to deploy.
    assert_eq!(registry(&mut w, CONTROL, |r| async move { r.deploy("hello", 2, None).await }).unwrap(), 3);
    reach(&mut w, &mut seen, &followers(), "hello", 3);
    for runner in RUNNERS {
        assert_eq!(answer(&mut w, runner, "hello", 0), "two hi", "seed {seed}, {how:?}: {runner}");
    }
    println!("world digest restart seed {seed}: {}", w.digest());
}

fn a_runner_cut_off_during_a_deploy_catches_up(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    reach(&mut w, &mut seen, &followers(), "hello", 1);

    let cut = RUNNERS[w.pick(0..3) as usize];
    let others: Vec<&str> = [CONTROL].into_iter().chain(followers()).filter(|n| *n != cut).collect();
    let reached: Vec<&str> = followers().into_iter().filter(|n| *n != cut).collect();
    w.partition(&[cut], &others);
    let apart = Duration::from_millis(w.pick(0..3000));
    w.run_for(apart);
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "two", true)).await }).unwrap();
    reach(&mut w, &mut seen, &reached, "hello", 2);
    // In half the seeds a second deploy lands while it is away: it goes to the last, never through a lower one.
    let last = if w.pick(0..2) == 0 {
        registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "three", true)).await }).unwrap();
        reach(&mut w, &mut seen, &reached, "hello", 3);
        (3, "three")
    } else {
        (2, "two")
    };
    let apart = Duration::from_millis(w.pick(0..20_000));
    w.run_for(apart);
    assert_eq!(serves(&w, cut, "hello"), Some(1), "seed {seed}: {cut} is cut off");
    assert_eq!(answer(&mut w, cut, "hello", 0), "one", "seed {seed}: it serves what it had");

    w.heal();
    reach(&mut w, &mut seen, &followers(), "hello", last.0);
    assert_eq!(answer(&mut w, cut, "hello", 0), last.1, "seed {seed}");
    println!("world digest cut seed {seed}: {}", w.digest());
}

fn a_runner_that_starts_while_the_control_node_is_down_serves_from_its_disk(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    reach(&mut w, &mut seen, &followers(), "hello", 1);
    // The table is saved after the switch: a machine crash before the write has landed would lose it.
    w.run_until("each runner's table on its disk", WITHIN, |w| {
        w.nodes().filter(|(name, _)| RUNNERS.contains(name)).all(|(_, n)| saved(n) >= 1)
    });

    let runner = RUNNERS[w.pick(0..3) as usize];
    w.crash(CONTROL, Crash::Machine);
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash(runner, how);
    let down = Duration::from_millis(w.pick(0..10_000));
    w.run_for(down);
    w.restart(runner);
    reach(&mut w, &mut seen, &[runner], "hello", 1);
    assert_eq!(answer(&mut w, runner, "hello", 0), "one", "seed {seed}");
    let stats = w.nodes().find(|(name, _)| *name == runner).unwrap().1.node.serving.as_ref().unwrap().stats.clone();
    assert_eq!(
        (stats.from_cache.load(Relaxed), stats.fetched.load(Relaxed)),
        (1, 0),
        "seed {seed}: nothing to fetch from"
    );

    // The control node comes back, and the runner follows it again.
    w.restart(CONTROL);
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "two", true)).await }).unwrap();
    reach(&mut w, &mut seen, &followers(), "hello", 2);
    assert_eq!(answer(&mut w, runner, "hello", 0), "two", "seed {seed}");
    println!("world digest cache seed {seed}: {}", w.digest());
}

/// An upload that keeps the source builds on the version it found. A removal that lands while it builds takes that
/// version away: the upload is refused, and does not bring the worker back with what the removal took.
fn an_upload_overtaken_by_a_removal_is_refused(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    reach(&mut w, &mut seen, &followers(), "hello", 1);

    let from = [CONTROL, GATEWAYS[0]][w.pick(0..2) as usize];
    let (changed, removed, listed) = w
        .block_on(from, |n| async move {
            let (first, second) = (Client::new(&n.cluster), Client::new(&n.cluster));
            let secret = BTreeMap::from([("TOKEN".to_owned(), Some("s3cret".to_owned()))]);
            let change = Upload { name: "hello".into(), secrets: secret, deploy: true, ..Upload::default() };
            let (changed, removed) = tokio::join!(first.upload(change), second.delete("hello"));
            (changed, removed, first.list().await)
        })
        .expect("the calling node is up");
    removed.unwrap_or_else(|e| panic!("seed {seed}: the removal was refused: {e}"));
    // Refused while it built, or not found once the removal had landed, or taken whole before the removal.
    let fine = matches!(&changed, Ok(_) | Err(Error::Refused(Refused::Busy | Refused::NotFound)));
    assert!(fine, "seed {seed}: {changed:?}");
    assert_eq!(listed.unwrap(), vec![], "seed {seed}: the worker is back after its removal ({changed:?})");
    w.run_until("no node serving the removed worker", WITHIN, |w| {
        followers().iter().all(|node| serves(w, node, "hello").is_none())
    });
}

/// A control node that kept its registry and lost the stream's files starts a stream its followers are ahead of.
/// They take it from its first record all the same, so the next deployment reaches them.
fn a_control_node_that_lost_its_stream_is_followed_again(seed: u64) {
    let mut w = world(seed);
    let mut seen = Seen::default();
    for (source, seq) in [("one", 1), ("two", 2), ("three", 3)] {
        registry(&mut w, CONTROL, move |r| async move { r.upload(upload("hello", source, true)).await }).unwrap();
        reach(&mut w, &mut seen, &followers(), "hello", seq);
    }
    w.crash(CONTROL, Crash::Process);
    for dir in ["streams", "cache", "objects"].map(|name| w.disk(CONTROL).dir(name)) {
        dir.list().into_iter().for_each(|(file, _)| dir.remove(&file));
    }
    let down = Duration::from_millis(w.pick(0..5000));
    w.run_for(down);
    w.restart(CONTROL);
    let (version, deployed) =
        registry(&mut w, CONTROL, |r| async move { r.upload(upload("hello", "one", true)).await }).unwrap();
    assert_eq!((version, deployed), (4, Some(4)), "seed {seed}: the registry kept its versions");
    reach(&mut w, &mut seen, &followers(), "hello", 4);
    assert_eq!(answer(&mut w, RUNNERS[0], "hello", 0), "one", "seed {seed}");
}

#[test]
fn an_upload_overtaken_by_a_removal_is_refused_on_every_seed() {
    // In most seeds the two land one after the other; a few have the removal land while the upload builds.
    check_seeds(seeds(0..64), an_upload_overtaken_by_a_removal_is_refused).unwrap();
}

#[test]
fn a_control_node_that_lost_its_stream_is_followed_again_on_every_seed() {
    check_seeds(seeds(0..16), a_control_node_that_lost_its_stream_is_followed_again).unwrap();
}

#[test]
fn a_deploy_a_rollback_a_canary_and_a_removal_reach_every_node_on_every_seed() {
    check_seeds(seeds(0..8), a_deploy_a_rollback_a_canary_and_a_removal_reach_every_node).unwrap();
}

#[test]
fn the_control_node_restarts_with_everything_on_every_seed() {
    check_seeds(seeds(0..16), the_control_node_restarts_with_everything).unwrap();
}

#[test]
fn a_runner_cut_off_during_a_deploy_catches_up_on_every_seed() {
    check_seeds(seeds(0..16), a_runner_cut_off_during_a_deploy_catches_up).unwrap();
}

#[test]
fn a_runner_that_starts_while_the_control_node_is_down_serves_from_its_disk_on_every_seed() {
    check_seeds(seeds(0..16), a_runner_that_starts_while_the_control_node_is_down_serves_from_its_disk).unwrap();
}
