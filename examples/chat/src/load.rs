//! Many simulated websocket sessions against a running cluster, and the bookkeeping that says what was lost.
//!
//! Every session joins its rooms before anyone posts, so every acknowledged post should reach every session in
//! its room. A session whose node goes away reconnects to the next node and resumes after its cursor.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{Barrier, watch};
use tokio::task::JoinSet;

use crate::client::{Client, Tracker};
use crate::proto::{ClientFrame, ServerFrame};
use crate::room::now_ms;

#[derive(Clone, Debug)]
pub struct LoadConfig {
    pub nodes: Vec<SocketAddr>,
    pub sessions: usize,
    pub rooms: usize,
    /// Rooms each session joins.
    pub per_session: usize,
    /// The first `posters` sessions post, round robin over their rooms.
    pub posters: usize,
    /// Posts per second, over all posters.
    pub rate: f64,
    pub duration: Duration,
    /// How long sessions keep reading after the last post.
    pub settle: Duration,
    /// The last `slow` sessions stop reading while posts flow, then read everything during the settle.
    pub slow: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub sessions: usize,
    pub slow_sessions: usize,
    pub rooms: usize,
    pub posts: u64,
    pub acked: u64,
    pub nacked: BTreeMap<String, u64>,
    /// Posts with no answer: the connection or the room's node went away with them.
    pub unanswered: u64,
    /// Of `unknown` nacks and unanswered posts, how many the poster later saw in its room anyway.
    pub unconfirmed_delivered: u64,
    pub received: u64,
    pub received_per_sec: f64,
    /// Room timestamp to receipt, sampled, over sessions that read; a fault's catch-up counts.
    pub latency_ms_p50: u64,
    pub latency_ms_p99: u64,
    pub latency_ms_max: u64,
    /// Both must stay 0.
    pub duplicates: u64,
    pub gaps: u64,
    /// Explicit breaks sessions were told about: resets and room restarts.
    pub breaks: u64,
    pub reconnects: u64,
    /// Acknowledged posts, by how many of the sessions in their room hold them.
    pub acked_held_by_all: u64,
    pub acked_held_by_some: u64,
    pub acked_held_by_none: u64,
    /// Rooms whose acknowledgements show more than one generation: their node went away.
    pub restarted_rooms: Vec<String>,
    /// Acknowledged posts not held by every session, in rooms that never restarted. Must stay 0.
    pub lost_in_steady_rooms: u64,
    /// The same in a restarted room's newest generation: every session follows the new host. Must stay 0.
    pub lost_in_last_generation: u64,
    /// The same in the generations of hosts that died: what the fault cost.
    pub lost_in_dead_generations: u64,
    /// `error` frames sessions got, by reason.
    pub errors: BTreeMap<String, u64>,
    /// `reset` frames sessions got, by reason.
    pub resets: BTreeMap<String, u64>,
    pub slow_received: u64,
    /// Acknowledged posts a slow session lacks at the end, outside dead generations. Must stay 0: paused, not dropped.
    pub slow_missing: u64,
}

struct Outcome {
    index: usize,
    tracker: Tracker,
    slow: bool,
    posts: u64,
    acks: Vec<(String, u64, u64)>,
    nacks: BTreeMap<String, u64>,
    errors: BTreeMap<String, u64>,
    resets: BTreeMap<String, u64>,
    unanswered: u64,
    unconfirmed_delivered: u64,
    latencies: Vec<u32>,
    reconnects: u64,
}

fn rooms_of(i: usize, cfg: &LoadConfig) -> Vec<String> {
    (0..cfg.per_session.min(cfg.rooms)).map(|k| format!("r{}", (i * cfg.per_session + k) % cfg.rooms)).collect()
}

/// Connects to the node for session `i`, or the next one that answers, and joins its rooms after their cursors.
async fn connect(
    cfg: &LoadConfig,
    i: usize,
    attempt: &mut usize,
    user: &str,
    tracker: &Tracker,
    rooms: &[String],
) -> Result<Client, String> {
    let mut last = String::new();
    // ponytail: one pass over the nodes with a short pause each; a load tool, not a client library.
    for _ in 0..cfg.nodes.len() * 3 {
        let addr = cfg.nodes[(i + *attempt) % cfg.nodes.len()];
        match Client::connect(addr, user).await {
            Ok(mut client) => {
                for room in rooms {
                    let after = tracker.rooms.get(room).and_then(|log| log.cursor);
                    client.send(&ClientFrame::Join { room: room.clone(), after }).await?;
                }
                return Ok(client);
            }
            Err(e) => last = e,
        }
        *attempt += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("session {i}: no node answers: {last}"))
}

async fn session(
    cfg: Arc<LoadConfig>,
    i: usize,
    joined: Arc<Barrier>,
    mut stop: watch::Receiver<bool>,
) -> Result<Outcome, String> {
    let user = format!("u{i}");
    let rooms = rooms_of(i, &cfg);
    let slow = i >= cfg.sessions - cfg.slow;
    let poster = i < cfg.posters;
    let mut out = Outcome {
        index: i,
        tracker: Tracker::default(),
        slow,
        posts: 0,
        acks: Vec::new(),
        nacks: BTreeMap::new(),
        errors: BTreeMap::new(),
        resets: BTreeMap::new(),
        unanswered: 0,
        unconfirmed_delivered: 0,
        latencies: Vec::new(),
        reconnects: 0,
    };
    let mut attempt = 0;
    let mut client = connect(&cfg, i, &mut attempt, &user, &out.tracker, &rooms).await?;
    // Wait for every room's Ready, so every session is in before the first post.
    while out.tracker.rooms.len() < rooms.len() {
        let frame = client.next().await.ok_or_else(|| format!("session {i} closed while joining"))?;
        if let ServerFrame::Error { why } = &frame {
            return Err(format!("session {i}: {why}"));
        }
        out.tracker.apply(&frame);
    }
    joined.wait().await;

    let period = Duration::from_secs_f64(cfg.posters.max(1) as f64 / cfg.rate.max(0.001));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let started = Instant::now();
    let (mut next_id, mut unconfirmed) = (0u64, BTreeSet::<u64>::new());
    let mut pending = BTreeSet::new();
    let mut posting = true;
    let settle_until = |started: Instant| started + cfg.duration + cfg.settle;
    loop {
        if posting && *stop.borrow() {
            posting = false;
        }
        // A slow session does not read while posts flow.
        if slow && posting {
            if stop.changed().await.is_err() {
                posting = false;
            }
            continue;
        }
        let deadline = tokio::time::sleep_until(settle_until(started).into());
        tokio::select! {
            _ = deadline => break,
            _ = tick.tick(), if poster && posting => {
                let room = rooms[next_id as usize % rooms.len()].clone();
                let text = format!("hello {next_id} from {user}");
                if client.send(&ClientFrame::Post { room, id: next_id, text }).await.is_ok() {
                    pending.insert(next_id);
                    out.posts += 1;
                }
                next_id += 1;
            }
            _ = stop.changed(), if posting => {}
            frame = client.next() => {
                let Some(frame) = frame else {
                    // The node went away: what was in flight has no answer.
                    out.unanswered += pending.len() as u64;
                    unconfirmed.extend(std::mem::take(&mut pending));
                    out.reconnects += 1;
                    attempt += 1;
                    client = connect(&cfg, i, &mut attempt, &user, &out.tracker, &rooms).await?;
                    continue;
                };
                match &frame {
                    ServerFrame::Msg { m, .. } => {
                        let now = now_ms();
                        for record in m {
                            // Posted to held, while posts flow; a slow session's backlog is its own choice, not latency.
                            if !slow && posting && out.latencies.len() < 4096 && (record.id + i as u64).is_multiple_of(8) {
                                out.latencies.push(now.saturating_sub(record.ts) as u32);
                            }
                            if record.from == user && unconfirmed.remove(&record.id) {
                                out.unconfirmed_delivered += 1;
                            }
                        }
                    }
                    ServerFrame::Ack { id, room, generation, seq } => {
                        pending.remove(id);
                        out.acks.push((room.clone(), *generation, *seq));
                    }
                    ServerFrame::Nack { id, why, .. } => {
                        pending.remove(id);
                        if why == "unknown" {
                            unconfirmed.insert(*id);
                        }
                        *out.nacks.entry(why.clone()).or_default() += 1;
                    }
                    ServerFrame::Reset { why, .. } => *out.resets.entry(why.clone()).or_default() += 1,
                    ServerFrame::Error { why } => {
                        *out.errors.entry(why.chars().take(96).collect()).or_default() += 1;
                    }
                    _ => {}
                }
                out.tracker.apply(&frame);
            }
        }
    }
    out.unanswered += pending.len() as u64;
    Ok(out)
}

pub async fn run(cfg: LoadConfig) -> Result<Report, String> {
    let cfg = Arc::new(cfg);
    let joined = Arc::new(Barrier::new(cfg.sessions + 1));
    let (stop_tx, stop) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for i in 0..cfg.sessions {
        tasks.spawn(session(cfg.clone(), i, joined.clone(), stop.clone()));
        if i % 64 == 63 {
            tokio::task::yield_now().await; // spreads the connects a little
        }
    }
    // ponytail: a session that fails to join leaves the barrier short; the caller's timeout reports it.
    joined.wait().await;
    eprintln!("{}", serde_json::json!({ "joined": cfg.sessions }));
    let started = Instant::now();
    tokio::time::sleep(cfg.duration).await;
    let _ = stop_tx.send(true);
    let mut outcomes = Vec::with_capacity(cfg.sessions);
    while let Some(done) = tasks.join_next().await {
        outcomes.push(done.map_err(|e| e.to_string())??);
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(report(&cfg, outcomes, elapsed))
}

fn report(cfg: &LoadConfig, outcomes: Vec<Outcome>, elapsed: f64) -> Report {
    let mut r = Report { sessions: cfg.sessions, slow_sessions: cfg.slow, rooms: cfg.rooms, ..Default::default() };
    let mut latencies = Vec::new();
    let mut generations: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for o in &outcomes {
        r.posts += o.posts;
        r.acked += o.acks.len() as u64;
        r.unanswered += o.unanswered;
        r.unconfirmed_delivered += o.unconfirmed_delivered;
        r.reconnects += o.reconnects;
        for (why, n) in &o.nacks {
            *r.nacked.entry(why.clone()).or_default() += n;
        }
        for (why, n) in &o.errors {
            *r.errors.entry(why.clone()).or_default() += n;
        }
        for (why, n) in &o.resets {
            *r.resets.entry(why.clone()).or_default() += n;
        }
        latencies.extend(&o.latencies);
        for log in o.tracker.rooms.values() {
            r.duplicates += log.duplicates;
            r.gaps += log.gaps;
            r.breaks += log.breaks;
            if o.slow {
                r.slow_received += log.received;
            } else {
                r.received += log.received;
            }
        }
        for (room, generation, _) in &o.acks {
            let seen = generations.entry(room).or_default();
            if !seen.contains(generation) {
                seen.push(*generation);
            }
        }
    }
    r.restarted_rooms = generations.iter().filter(|(_, g)| g.len() > 1).map(|(room, _)| room.to_string()).collect();
    r.received_per_sec = r.received as f64 / elapsed.max(0.001);
    latencies.sort_unstable();
    let at = |q: f64| latencies.get(((latencies.len() as f64 * q) as usize).min(latencies.len().saturating_sub(1)));
    (r.latency_ms_p50, r.latency_ms_p99) =
        (at(0.5).copied().unwrap_or(0) as u64, at(0.99).copied().unwrap_or(0) as u64);
    r.latency_ms_max = latencies.last().copied().unwrap_or(0) as u64;

    let mut members: BTreeMap<String, Vec<&Outcome>> = BTreeMap::new();
    let mut slow = members.clone();
    for o in &outcomes {
        for room in rooms_of(o.index, cfg) {
            if o.slow { &mut slow } else { &mut members }.entry(room).or_default().push(o);
        }
    }
    for o in &outcomes {
        for (room, generation, seq) in &o.acks {
            let inside = members.get(room).map_or(&[][..], Vec::as_slice);
            let held =
                inside.iter().filter(|m| m.tracker.rooms.get(room).is_some_and(|l| l.holds(*generation, *seq))).count();
            match held {
                n if n == inside.len() => r.acked_held_by_all += 1,
                0 => r.acked_held_by_none += 1,
                _ => r.acked_held_by_some += 1,
            }
            let (steady, last) =
                (!r.restarted_rooms.contains(room), generations[room.as_str()].iter().max() == Some(generation));
            if steady || last {
                let behind = slow.get(room).map_or(&[][..], Vec::as_slice);
                r.slow_missing += behind
                    .iter()
                    .filter(|m| !m.tracker.rooms.get(room).is_some_and(|l| l.holds(*generation, *seq)))
                    .count() as u64;
            }
            if held < inside.len() {
                *match (steady, last) {
                    (true, _) => &mut r.lost_in_steady_rooms,
                    (false, true) => &mut r.lost_in_last_generation,
                    (false, false) => &mut r.lost_in_dead_generations,
                } += 1;
            }
        }
    }
    r
}
