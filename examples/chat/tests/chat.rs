//! Several nodes in one process, real websockets.

use std::time::Duration;

use axon_cluster::MemoryDiscovery;
use chat::client::{Client, Tracker};
use chat::{ClientFrame, Node, NodeConfig, ServerFrame};
use tokio::time::timeout;

async fn cluster(n: usize) -> Vec<Node> {
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for i in 0..n {
        nodes.push(Node::start(NodeConfig::local(&format!("n{i}")), discovery.clone()).await.expect("node starts"));
    }
    for node in &nodes {
        let full = node.cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind("room")).count() == n);
        timeout(Duration::from_secs(10), full).await.expect("view in time").expect("cluster open");
    }
    nodes
}

async fn shutdown(nodes: Vec<Node>) {
    for node in nodes {
        node.shutdown().await;
    }
}

/// Reads frames into the tracker until `done` holds.
async fn until(
    client: &mut Client,
    tracker: &mut Tracker,
    done: impl Fn(&Tracker, &ServerFrame) -> bool,
) -> ServerFrame {
    // A stall fails the test, not a slow machine: the deadline is per frame, not for the whole read.
    loop {
        let frame =
            timeout(Duration::from_secs(20), client.next()).await.expect("frame in time").expect("connection open");
        if std::env::var_os("CHAT_DEBUG").is_some() {
            eprintln!("{frame:?}");
        }
        tracker.apply(&frame);
        if done(tracker, &frame) {
            return frame;
        }
    }
}

async fn joined(node: &Node, user: &str, room: &str, after: Option<(u64, u64)>) -> (Client, Tracker) {
    let mut client = Client::connect(node.http, user).await.expect("connects");
    client.send(&ClientFrame::Join { room: room.into(), after }).await.expect("join sent");
    let mut tracker = Tracker::default();
    until(&mut client, &mut tracker, |_, f| matches!(f, ServerFrame::Ready { .. })).await;
    (client, tracker)
}

fn received(tracker: &Tracker, room: &str) -> u64 {
    tracker.rooms.get(room).map_or(0, |l| l.received)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_post_on_one_node_reaches_sessions_on_every_node() {
    let nodes = cluster(3).await;
    let mut sessions = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        sessions.push(joined(node, &format!("u{i}"), "lobby", None).await);
    }
    let (poster, _) = &mut sessions[0];
    for id in 0..20 {
        poster.send(&ClientFrame::Post { room: "lobby".into(), id, text: format!("m{id}") }).await.expect("post sent");
    }
    for (client, tracker) in &mut sessions {
        until(client, tracker, |t, _| received(t, "lobby") == 20).await;
        let log = &tracker.rooms["lobby"];
        assert_eq!((log.duplicates, log.gaps, log.breaks), (0, 0, 0), "{log:?}");
    }
    shutdown(nodes).await;
}

/// Posts `n` messages keeping at most 16 unanswered, and returns once every one is acknowledged.
async fn post_all(client: &mut Client, tracker: &mut Tracker, room: &str, ids: std::ops::Range<u64>, size: usize) {
    let (mut next, end, mut acked) = (ids.start, ids.end, 0);
    let text = "x".repeat(size);
    // Per step, as in `until`: a stall fails, a slow machine does not.
    let step = Duration::from_secs(20);
    while acked < end - ids.start {
        while next < end && next - ids.start - acked < 16 {
            let post = ClientFrame::Post { room: room.into(), id: next, text: text.clone() };
            timeout(step, client.send(&post)).await.expect("post sent in time").expect("post sent");
            next += 1;
        }
        let frame = timeout(step, client.next()).await.expect("ack in time").expect("connection open");
        tracker.apply(&frame);
        match frame {
            ServerFrame::Ack { .. } => acked += 1,
            ServerFrame::Nack { why, .. } => panic!("post refused: {why}"),
            _ => {}
        }
    }
}

async fn owner(node: &Node, room: &str) -> String {
    let placement = axon_cluster::Placement::default();
    let found = node.cluster.get_or_spawn::<axon_cluster::Unaddressed>("room", room, bytes::Bytes::new(), &placement);
    found.await.expect("room placed").node().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_that_reconnects_to_another_node_resumes_after_its_cursor() {
    let nodes = cluster(3).await;
    let (mut poster, mut poster_log) = joined(&nodes[0], "poster", "news", None).await;
    let (mut reader, mut log) = joined(&nodes[1], "reader", "news", None).await;
    post_all(&mut poster, &mut poster_log, "news", 0..10, 16).await;
    until(&mut reader, &mut log, |t, _| received(t, "news") == 10).await;
    let cursor = log.rooms["news"].cursor;
    drop(reader);

    post_all(&mut poster, &mut poster_log, "news", 10..25, 16).await;
    // A node with no session in the room yet: it asks its parents for what follows the cursor.
    let mut again = Client::connect(nodes[2].http, "reader").await.expect("connects");
    again.send(&ClientFrame::Join { room: "news".into(), after: cursor }).await.expect("join sent");
    until(&mut again, &mut log, |t, _| received(t, "news") == 25).await;
    let news = &log.rooms["news"];
    assert_eq!((news.duplicates, news.gaps, news.breaks), (0, 0, 0), "{news:?}");
    assert_eq!(news.runs.len(), 1, "one contiguous run from 1: {news:?}");
    shutdown(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_client_is_paused_and_catches_up_from_the_ring_without_slowing_the_room() {
    let nodes = cluster(2).await;
    let (mut poster, mut poster_log) = joined(&nodes[0], "poster", "busy", None).await;
    let (mut fast, mut fast_log) = joined(&nodes[1], "fast", "busy", None).await;
    let (mut slow, mut slow_log) = joined(&nodes[1], "slow", "busy", None).await;
    let paused = chat::session::PAUSES.load(std::sync::atomic::Ordering::Relaxed);

    // 12 MB: more than the sockets buffer, less than the 16 MiB history ring. The slow client reads nothing.
    let (n, started) = (8000, std::time::Instant::now());
    let fast_reads = async {
        until(&mut fast, &mut fast_log, |t, _| received(t, "busy") == n).await;
        started.elapsed()
    };
    let ((), fast_done) = tokio::join!(post_all(&mut poster, &mut poster_log, "busy", 0..n, 1500), fast_reads);
    assert!(chat::session::PAUSES.load(std::sync::atomic::Ordering::Relaxed) > paused, "the slow session paused");

    until(&mut slow, &mut slow_log, |t, _| received(t, "busy") == n).await;
    for log in [&fast_log.rooms["busy"], &slow_log.rooms["busy"]] {
        assert_eq!((log.duplicates, log.gaps, log.breaks, log.runs.len()), (0, 0, 0, 1), "{log:?}");
    }
    eprintln!("fast client had all {n} after {fast_done:?}");
    shutdown(nodes).await;
}

/// Three nodes, `room` placed on one of them, a poster and a reader joined on the other two, and 5 posts read: the
/// nodes, the owner's index among them, and the two sessions.
async fn three_with_room(room: &str) -> (Vec<Node>, usize, (Client, Tracker), (Client, Tracker)) {
    let nodes = cluster(3).await;
    let host = owner(&nodes[0], room).await;
    let at = nodes.iter().position(|n| *n.cluster.identity().node_id == *host).expect("owner is a node");
    let others: Vec<usize> = (0..3).filter(|&i| i != at).collect();
    let (mut poster, mut poster_log) = joined(&nodes[others[0]], "poster", room, None).await;
    let (mut reader, mut log) = joined(&nodes[others[1]], "reader", room, None).await;
    post_all(&mut poster, &mut poster_log, room, 0..5, 16).await;
    until(&mut reader, &mut log, |t, _| received(t, room) == 5).await;
    (nodes, at, (poster, poster_log), (reader, log))
}

/// Posts once to `room` until it is acknowledged, and returns the `Ack`.
async fn post_until_acked(client: &mut Client, tracker: &mut Tracker, room: &str) -> ServerFrame {
    let posted = async {
        loop {
            client.send(&ClientFrame::Post { room: room.into(), id: 100, text: "after".into() }).await.expect("sent");
            let answer = |_: &Tracker, f: &ServerFrame| matches!(f, ServerFrame::Ack { .. } | ServerFrame::Nack { .. });
            match until(client, tracker, answer).await {
                ack @ ServerFrame::Ack { .. } => return ack,
                // ponytail: the view may still name the old owner for a moment; a person would press send again.
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    };
    timeout(Duration::from_secs(20), posted).await.expect("a post lands after the move")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_room_whose_node_leaves_restarts_elsewhere_and_its_sessions_are_told() {
    let (mut nodes, at, (mut poster, mut poster_log), (mut reader, mut log)) = three_with_room("moving").await;

    nodes.remove(at).shutdown().await;
    // The next post finds the room's new owner and starts it there, in a new generation.
    post_until_acked(&mut poster, &mut poster_log, "moving").await;
    until(&mut reader, &mut log, |t, _| received(t, "moving") == 6).await;
    let moving = &log.rooms["moving"];
    assert_eq!((moving.duplicates, moving.gaps), (0, 0), "{moving:?}");
    assert!(moving.breaks >= 1, "the restart is explicit: {moving:?}");
    assert_eq!(moving.runs.len(), 2, "two generations: {moving:?}");
    shutdown(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_room_whose_node_crashes_restarts_elsewhere_for_a_session_that_only_reads() {
    let (mut nodes, at, (mut poster, mut poster_log), (mut reader, mut log)) = three_with_room("crashing").await;
    let before = log.rooms["crashing"].cursor.expect("a cursor").0;

    nodes.remove(at).crash();
    // Nobody posts: the reader's session is told the origin is lost, asks for the room, and the room starts on
    // its next node in a new generation, which the same subscription shows as a new `ready`.
    until(&mut reader, &mut log, |_, f| matches!(f, ServerFrame::Ready { at: Some((g, _)), .. } if *g != before)).await;
    match post_until_acked(&mut poster, &mut poster_log, "crashing").await {
        ServerFrame::Ack { generation, .. } => assert_ne!(generation, before, "acknowledged in the dead generation"),
        other => unreachable!("{other:?}"),
    }
    until(&mut reader, &mut log, |t, _| received(t, "crashing") == 6).await;
    let crashing = &log.rooms["crashing"];
    assert_eq!((crashing.duplicates, crashing.gaps), (0, 0), "{crashing:?}");
    assert_eq!(crashing.breaks, 1, "the restart is told once: {crashing:?}");
    assert_eq!(crashing.runs.len(), 2, "two generations: {crashing:?}");
    shutdown(nodes).await;
}
