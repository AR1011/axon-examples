//! A seeded model of one session following one room, run under axon-testkit. The room's node is modelled from the
//! streams guide's contract (Ready, replay after a cursor, live, a ring of `ring` records, `HistoryUnavailable` past
//! it, a live follow told `OriginLost` and then seeing the restart as a new `Ready`, a cursor follow ending with
//! `GenerationChanged`); the session side is the app's own [`Follow`], `msg_frame` and client [`Tracker`]. The
//! simulator carries each subscription's events on its own link, so a replaced or cancelled subscription's events
//! arrive late and interleaved with the next one's. Faults: room restarts, slow-consumer resets, upstream loss,
//! client pauses, reconnects to another node.

use axon_stream::{Cursor, EndReason, ResetReason, SequencedBatch, StartPosition, StreamEvent};
use axon_testkit::{NodeId, SimConfig, Simulator, check_seeds};
use bytes::Bytes;
use chat::client::Tracker;
use chat::proto::{Record, ServerFrame, json, msg_frame, pair};
use chat::{Act, Follow};
use std::time::Duration;

/// (session incarnation, subscription epoch, event); `None` is the job's end.
type Ev = (u32, u32, Option<StreamEvent>);

struct Sub {
    src: NodeId,
    epoch: u32,
    next: u64,
    live: bool,
}

struct World {
    ring: u64,
    generation: u64,
    head: u64,
    restarts: u32,
    refusals: u32,
    blind_rejoins: u32,
    net: Simulator<Ev>,
    session: NodeId,
    incarnation: u32,
    follow: Follow,
    active: Option<Sub>,
    tracker: Tracker,
    faults: bool,
}

impl World {
    fn client(&mut self, frame: ServerFrame) {
        // Through JSON, as the websocket carries it.
        let frame: ServerFrame = serde_json::from_str(&json(&frame)).expect("frame parses");
        self.tracker.apply(&frame);
    }

    fn emit(&mut self, src: NodeId, epoch: u32, event: Option<StreamEvent>) {
        let incarnation = self.incarnation;
        self.net.send(src, self.session, (incarnation, epoch, event)).expect("in flight");
    }

    /// The session cancels a job: its queued events still arrive, and so, sometimes, does its end.
    fn cancel(&mut self) {
        if let Some(sub) = self.active.take()
            && self.net.pick(0..2) == 0
        {
            self.emit(sub.src, sub.epoch, None);
        }
    }

    /// Ends the active subscription from the node's side: a terminal event, then the job's end.
    fn end(&mut self, terminal: StreamEvent) {
        if let Some(sub) = self.active.take() {
            self.emit(sub.src, sub.epoch, Some(terminal));
            self.emit(sub.src, sub.epoch, None);
        }
    }

    /// What `Session::subscribe` does, against the modelled node.
    fn subscribe(&mut self, start: StartPosition) {
        let from = match start {
            StartPosition::After(cursor) => Some(cursor),
            StartPosition::First => Some(Cursor { generation: self.generation, sequence: 0 }),
            _ => None,
        };
        // The node carries the stream, so these are refused at subscribe, as `Session::subscribe` expects.
        if let Some(c) = from
            && c.generation == self.generation
            && c.sequence < self.head.saturating_sub(self.ring)
        {
            self.refusals += 1;
            self.follow.cursor = None;
            self.client(ServerFrame::Reset { room: "r".into(), why: "history_unavailable".into() });
            return self.subscribe(StartPosition::Live);
        }
        if let Some(c) = from
            && c.generation != self.generation
            && self.net.pick(0..2) == 0
        {
            self.follow.cursor = None;
            self.client(ServerFrame::Reset { room: "r".into(), why: "generation_changed".into() });
            return self.subscribe(StartPosition::First);
        }
        self.cancel();
        let epoch = self.follow.subscribed();
        let src = self.net.add_node("sub").expect("node");
        match from {
            // Or, on a node that learns the position later, ended once it does.
            Some(c) if c.generation != self.generation => {
                self.emit(src, epoch, Some(StreamEvent::ResetRequired(ResetReason::GenerationChanged)));
                self.emit(src, epoch, None);
            }
            Some(c) => {
                self.emit(src, epoch, Some(StreamEvent::Ready { start: Some(c) }));
                self.active = Some(Sub { src, epoch, next: c.sequence + 1, live: false });
            }
            None => {
                let at = Cursor { generation: self.generation, sequence: self.head };
                self.emit(src, epoch, Some(StreamEvent::Ready { start: Some(at) }));
                self.active = Some(Sub { src, epoch, next: self.head + 1, live: true });
            }
        }
    }

    /// The node hands the active subscription what the room has published since.
    fn feed(&mut self) {
        let Some(sub) = &mut self.active else { return };
        let (src, epoch) = (sub.src, sub.epoch);
        let mut batches = Vec::new();
        while sub.next <= self.head {
            let last = (sub.next + 3).min(self.head);
            let records: Vec<Bytes> = (sub.next..=last)
                .map(|s| {
                    let record = Record { from: "u".into(), id: s, text: format!("m{s}"), ts: 0 };
                    Bytes::from(serde_json::to_vec(&record).expect("record"))
                })
                .collect();
            batches.push(SequencedBatch::new(self.generation, sub.next, records).expect("batch"));
            sub.next = last + 1;
        }
        for batch in batches {
            self.emit(src, epoch, Some(StreamEvent::Batch(batch)));
        }
    }

    fn deliver(&mut self, n: usize) {
        for _ in 0..n {
            let Some(delivery) = self.net.step() else { return };
            let (incarnation, epoch, event) = delivery.payload;
            if incarnation != self.incarnation {
                continue; // the old session's actor is gone
            }
            let fits = !self.faults || self.net.pick(0..12) != 0;
            match self.follow.on_event(epoch, event, fits) {
                // The model's room restarts by itself; the app asks for it here.
                Act::Nothing | Act::Lost => {}
                Act::Ready(at) => self.client(ServerFrame::Ready { room: "r".into(), at: at.map(pair) }),
                Act::Deliver(batch) => {
                    let frame = msg_frame("r", &batch).expect("utf-8");
                    let frame: ServerFrame = serde_json::from_str(&frame).expect("msg frame parses");
                    self.tracker.apply(&frame);
                }
                Act::Reset(why) => self.client(ServerFrame::Reset { room: "r".into(), why: why.into() }),
                Act::Pause => self.cancel(),
                Act::Subscribe(start) => self.subscribe(start),
            }
        }
    }

    fn tick(&mut self, step: u64) {
        match self.net.pick(0..100) {
            0..=1 => {
                self.restarts += 1;
                (self.generation, self.head) = (1_000 + step, 0);
                if let Some(sub) = &mut self.active {
                    if sub.live {
                        let (src, epoch) = (sub.src, sub.epoch);
                        sub.next = 1;
                        let start = Some(Cursor { generation: self.generation, sequence: 0 });
                        self.emit(src, epoch, Some(StreamEvent::OriginLost));
                        self.emit(src, epoch, Some(StreamEvent::Ready { start }));
                    } else {
                        self.end(StreamEvent::ResetRequired(ResetReason::GenerationChanged));
                    }
                }
            }
            2..=4 => self.end(StreamEvent::ResetRequired(ResetReason::SlowConsumer)),
            5 => self.end(StreamEvent::End(EndReason::UpstreamLost)),
            6..=7 => {
                // The client reconnects to another node and joins after the cursor it holds.
                self.cancel();
                self.incarnation += 1;
                let cursor = self.tracker.rooms.get("r").and_then(|l| l.cursor);
                self.blind_rejoins += u32::from(cursor.is_none());
                self.follow = Follow::new(cursor.map(|(generation, sequence)| Cursor { generation, sequence }));
                let start = self.follow.start();
                self.subscribe(start);
            }
            _ => self.head += 1 + self.net.pick(0..3),
        }
    }
}

fn run(seed: u64) -> String {
    let net =
        Simulator::new(SimConfig { seed, jitter: Duration::from_millis(3), max_nodes: 100_000, ..Default::default() });
    let mut w = World {
        ring: 0,
        generation: 1_000,
        head: 0,
        restarts: 0,
        refusals: 0,
        blind_rejoins: 0,
        net,
        session: 0,
        incarnation: 0,
        follow: Follow::new(None),
        active: None,
        tracker: Tracker::default(),
        faults: true,
    };
    w.ring = [8, 64, 100_000][w.net.pick(0..3) as usize];
    w.session = w.net.add_node("session").expect("node");
    w.subscribe(StartPosition::Live);
    for step in 0..400 {
        w.tick(step);
        w.feed();
        let n = w.net.pick(0..4) as usize;
        w.deliver(n);
        if let Some(start) = w.follow.resume()
            && w.net.pick(0..3) == 0
        {
            w.subscribe(start);
        }
    }
    // No more faults: the session must end holding the room's head.
    w.faults = false;
    w.head += 5;
    for _ in 0..1_000 {
        if let Some(start) = w.follow.resume() {
            w.subscribe(start);
        }
        w.feed();
        w.deliver(usize::MAX);
        if w.net.pending() == 0 && w.active.as_ref().is_some_and(|s| s.next > w.head) {
            break;
        }
    }
    let log = &w.tracker.rooms["r"];
    let why = format!("seed {seed}, ring {}, {} restarts, {} refusals: {log:?}", w.ring, w.restarts, w.refusals);
    assert_eq!((log.duplicates, log.gaps), (0, 0), "{why}");
    assert_eq!(log.cursor, Some((w.generation, w.head)), "ends at the head: {why}");
    assert!(log.breaks <= u64::from(w.restarts + w.refusals), "every break has a cause: {why}");
    if w.refusals == 0 && w.blind_rejoins == 0 {
        assert!((1..=w.head).all(|s| log.holds(w.generation, s)), "the whole last generation: {why}");
    }
    why
}

#[test]
fn a_session_resumes_without_gaps_or_duplicates_through_restarts_pauses_and_reconnects() {
    check_seeds(0..500, |seed| drop(run(seed))).expect("every seed holds");
}

#[test]
fn a_seed_replays_the_same() {
    assert_eq!(run(7), run(7));
}
