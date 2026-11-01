//! The protocol on the testkit's `Simulator` (design §8): yrs clients, their sessions and one document with its log,
//! running the same cores as the server (`SessionCore`, `ClientCore`, `DocCore`, `pipeline::append`) over a network
//! that drops, duplicates and reorders. A websocket is ordered and whole, so a frame lost or out of order on one ends
//! its connection, and the client joins again; calls between a session and the document are lost, doubled and
//! reordered as they are, and asked again after a timeout. Writes land late or fail, the home crashes and loses what
//! was not written (`CutFiles`), and a session's subscription is reset or paused at random, or snapshots a relay
//! that took the generation before its first records (an empty state, then those records live).
//!
//! Each seed ends with every client and the server holding the same text and position (I3), every token a client was
//! told was saved present after each reload (I1), and each start of the document at a higher generation (I11).

mod support;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use axon::files::{Address, FileFuture, Log, LogConfig, Lsn};
use axon_stream::{
    Cursor, EndReason, ResetReason, SequencedBatch, Snapshot, StartPosition, StreamEvent, keyed, split_keyed,
};
use axon_testkit::{NodeId, SimConfig, Simulator, check_seeds, seeds};
use bytes::Bytes;
use mdpad::core::acl::Principal;
use mdpad::core::doc::{DocCore, Edit};
use mdpad::core::pipeline;
use mdpad::crdt;
use mdpad::follow::{ClientAct, ClientCore, Effect, SessionCore};
use mdpad::wire::{ClientFrame, DocAnswer, DocReq, ServerFrame, close};
use support::CutFiles;

const KEY: &[u8] = b"doc-1";
const CLIENTS: usize = 4;
/// Clients type, and the home crashes, until then; then the network heals into agreement.
const TYPING: Duration = Duration::from_secs(3);
const GIVE_UP: Duration = Duration::from_secs(300);
const CALL_TIMEOUT: Duration = Duration::from_millis(300);
/// A client with unsaved edits that hears nothing for this long joins again.
const SILENT: Duration = Duration::from_secs(1);
const RING: usize = 64;
/// The reply id of the document's create, which no session waits for.
const NOBODY: u64 = u64::MAX;

/// `CutFiles` and the log answer at once: a future is polled once, to the end.
fn now<T>(fut: impl Future<Output = T>) -> T {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("CutFiles answers at once without a delay"),
    }
}

fn log_config() -> LogConfig {
    LogConfig {
        name: "docs".into(),
        segment_bytes: 1 << 20,
        preallocate: false,
        max_record: 384 << 10,
        max_write_bytes: 1 << 20,
        max_unwritten_bytes: 4 << 20,
        ..LogConfig::default()
    }
}

#[derive(Clone, Debug)]
enum Wire {
    Up { conn: u64, n: u64, frame: ClientFrame },
    Down { conn: u64, n: u64, frame: ServerFrame },
    Closed { conn: u64, code: u16 },
    Call { id: u64, req: DocReq },
    Answer { id: u64, answer: DocAnswer },
}

#[derive(Debug)]
enum Timer {
    Type(usize),
    Tick(usize),
    Reconnect(usize),
    Flush(usize, u64),
    CallTimeout(u64, u64),
    Resume(u64),
    WriteDone(u64),
    WriteAgain(u64),
    Crash,
    Check,
}

struct Client {
    node: NodeId,
    core: ClientCore,
    conn: Option<u64>,
    sent: u64,
    received: u64,
    held: BTreeMap<u64, ServerFrame>,
    heard: Duration,
    typed: u64,
}

struct Session {
    core: SessionCore,
    client: usize,
    sent: u64,
    received: u64,
    held: BTreeMap<u64, ClientFrame>,
    call: Option<u64>,
    /// The epoch of the subscription the stream feeds, `None` while paused or between subscriptions.
    sub: Option<u32>,
}

struct Stream {
    generation: u64,
    seq: u64,
    table: BTreeMap<Bytes, Bytes>,
    ring: VecDeque<(u64, Bytes)>,
}

struct Home {
    files: CutFiles,
    log: Option<Log>,
    doc: Option<DocCore<u64>>,
    writing: Option<FileFuture<()>>,
    waiting: Option<Lsn>,
    stream: Option<Stream>,
    incarnation: u64,
}

struct Model {
    sim: Simulator<Wire>,
    timers: BTreeMap<(Duration, u64), Timer>,
    next_timer: u64,
    web: NodeId,
    home_node: NodeId,
    clients: Vec<Client>,
    sessions: BTreeMap<u64, Session>,
    home: Home,
    next_conn: u64,
    next_call: u64,
    /// Tokens by their mark, and those a client was told are saved.
    tokens: BTreeMap<u64, String>,
    acked: BTreeSet<String>,
    generations: Vec<u64>,
    log: Vec<String>,
}

fn who() -> Principal {
    Principal::User("ana".into())
}

impl Model {
    fn new(seed: u64) -> Self {
        let mut sim = Simulator::new(SimConfig {
            seed,
            jitter: Duration::from_millis(3),
            drop_every: 150,
            duplicate_every: 100,
            ordered: false,
            ..SimConfig::default()
        });
        let web = sim.add_node("web").unwrap();
        let home_node = sim.add_node("home").unwrap();
        let clients = (0..CLIENTS)
            .map(|i| Client {
                node: sim.add_node(&format!("c{i}")).unwrap(),
                core: ClientCore::new(i as u64 + 1),
                conn: None,
                sent: 0,
                received: 0,
                held: BTreeMap::new(),
                heard: Duration::ZERO,
                typed: 0,
            })
            .collect();
        let home = Home {
            files: CutFiles::new(None),
            log: None,
            doc: None,
            writing: None,
            waiting: None,
            stream: None,
            incarnation: 0,
        };
        let mut model = Self {
            sim,
            timers: BTreeMap::new(),
            next_timer: 0,
            web,
            home_node,
            clients,
            sessions: BTreeMap::new(),
            home,
            next_conn: 1,
            next_call: 1,
            tokens: BTreeMap::new(),
            acked: BTreeSet::new(),
            generations: Vec::new(),
            log: Vec::new(),
        };
        model.create();
        for i in 0..CLIENTS {
            model.connect(i);
            let typing = Duration::from_millis(model.sim.pick(1..50));
            model.timer(typing, Timer::Type(i));
            model.timer(Duration::from_millis(100), Timer::Tick(i));
        }
        for _ in 0..model.sim.pick(0..4) {
            let at = Duration::from_millis(model.sim.pick(100..TYPING.as_millis() as u64));
            model.timer(at, Timer::Crash);
        }
        model.timer(TYPING, Timer::Check);
        model
    }

    fn timer(&mut self, after: Duration, timer: Timer) {
        self.next_timer += 1;
        self.timers.insert((self.sim.now() + after, self.next_timer), timer);
    }

    fn note(&mut self, line: String) {
        self.log.push(format!("{:?} {line}", self.sim.now()));
        if self.log.len() > 200 {
            self.log.remove(0);
        }
    }

    /// Sends, and reports whether the network lost it.
    fn send(&mut self, from: NodeId, to: NodeId, wire: Wire) -> bool {
        let dropped = self.sim.stats().dropped;
        self.sim.send(from, to, wire).unwrap();
        self.sim.stats().dropped > dropped
    }

    fn create(&mut self) {
        let log = Log::open(Arc::new(self.home.files.clone()), log_config());
        let mut log = now(log).unwrap();
        log.fence(KEY, 1).unwrap();
        let mut doc = DocCore::load(1, Vec::new(), false).unwrap();
        assert!(doc.create("ana", "# Notes\n", 0, NOBODY).is_none());
        let lsn = pipeline::append(&mut log, KEY, 1, doc.take().unwrap()).unwrap();
        let write = log.take_write(0).unwrap();
        assert!(log.written(now(write)) >= lsn);
        let done = doc.written();
        self.generations.push(1);
        let mut stream = Stream { generation: 1, seq: 0, table: BTreeMap::new(), ring: VecDeque::new() };
        for batch in done.publish {
            stream.publish(&batch);
        }
        (self.home.log, self.home.doc, self.home.stream) = (Some(log), Some(doc), Some(stream));
    }

    /// The document's first call after a crash starts it: a new epoch, and everything the log holds.
    fn load(&mut self) {
        if self.home.doc.is_some() {
            return;
        }
        let files = Arc::new(self.home.files.clone());
        let log = self.home.log.get_or_insert_with(|| now(Log::open(files, log_config())).unwrap());
        let epoch = log.key(KEY).map_or(1, |k| k.fence + 1);
        let (lsn, _) = log.fence(KEY, epoch).unwrap();
        let write = log.take_write(0).expect("the fence");
        assert!(log.written(now(write)) >= lsn);
        let entries = now(log.read(KEY, Address::default(), usize::MAX)).unwrap();
        let mut doc = DocCore::load(epoch, entries, true).unwrap();
        let text = doc.text();
        for token in &self.acked {
            assert!(text.contains(token), "I1: {token} was saved and is gone after a reload");
        }
        assert!(self.generations.last().is_none_or(|g| *g < epoch), "I11: generation {epoch} again");
        self.generations.push(epoch);
        self.note(format!("load at generation {epoch}"));
        let mut stream = Stream { generation: epoch, seq: 0, table: BTreeMap::new(), ring: VecDeque::new() };
        stream.publish(&doc.born());
        (self.home.doc, self.home.stream) = (Some(doc), Some(stream));
    }

    fn crash(&mut self) {
        self.note("crash".into());
        self.home.files.crash(false);
        self.home.files = self.home.files.reopen();
        (self.home.log, self.home.doc, self.home.writing, self.home.waiting, self.home.stream) =
            (None, None, None, None, None);
        self.home.incarnation += 1;
        self.sim.crash_node(self.web).unwrap();
        self.sim.crash_node(self.home_node).unwrap();
        self.sessions.clear();
        for i in 0..CLIENTS {
            if self.clients[i].conn.is_some() {
                self.disconnect(i);
            }
        }
    }

    fn connect(&mut self, i: usize) {
        let conn = self.next_conn;
        self.next_conn += 1;
        let session = self.sim.pick(1..u64::MAX);
        let core = SessionCore::new(session, who(), "#e0b252".into());
        self.sessions.insert(
            conn,
            Session { core, client: i, sent: 0, received: 0, held: BTreeMap::new(), call: None, sub: None },
        );
        let client = &mut self.clients[i];
        (client.conn, client.sent, client.received, client.heard) = (Some(conn), 0, 0, self.sim.now());
        client.held.clear();
        let hello = client.core.connect();
        self.up(i, hello);
    }

    /// The client's end of its connection closes; the server's end learns of it at once, as from TCP.
    fn disconnect(&mut self, i: usize) {
        if let Some(conn) = self.clients[i].conn.take() {
            self.sessions.remove(&conn);
        }
        self.clients[i].core.disconnected();
        let after = Duration::from_millis(self.sim.pick(1..40));
        self.timer(after, Timer::Reconnect(i));
    }

    fn up(&mut self, i: usize, frame: ClientFrame) {
        let client = &mut self.clients[i];
        let Some(conn) = client.conn else { return };
        client.sent += 1;
        let (from, n) = (client.node, client.sent);
        if self.send(from, self.web, Wire::Up { conn, n, frame }) {
            self.disconnect(i);
        }
    }

    fn close(&mut self, conn: u64, code: u16) {
        if let Some(session) = self.sessions.remove(&conn) {
            self.note(format!("close {conn} {code}"));
            let to = self.clients[session.client].node;
            self.send(self.web, to, Wire::Closed { conn, code });
        }
    }

    fn effects(&mut self, conn: u64, effects: Vec<Effect>) {
        let mut queue: VecDeque<Effect> = effects.into();
        while let Some(effect) = queue.pop_front() {
            let Some(session) = self.sessions.get_mut(&conn) else { return };
            match effect {
                Effect::Send(frame) => {
                    session.sent += 1;
                    let (i, n) = (session.client, session.sent);
                    if self.send(self.web, self.clients[i].node, Wire::Down { conn, n, frame }) {
                        return self.disconnect(i);
                    }
                }
                Effect::Call(req) => {
                    let id = self.next_call;
                    self.next_call += 1;
                    session.call = Some(id);
                    self.send(self.web, self.home_node, Wire::Call { id, req });
                    self.timer(CALL_TIMEOUT, Timer::CallTimeout(conn, id));
                }
                Effect::Subscribe(start) => {
                    let epoch = session.core.subscribed();
                    session.sub = Some(epoch);
                    queue.extend(self.subscribe(conn, epoch, start));
                }
                Effect::Pause => {
                    session.sub = None;
                    let after = Duration::from_millis(self.sim.pick(1..30));
                    self.timer(after, Timer::Resume(conn));
                }
                Effect::Close(code) => {
                    assert!(code != close::FORBIDDEN && code != close::NOT_FOUND, "closed {code}");
                    return self.close(conn, code);
                }
                // Presence is not part of this model: the World stories carry it.
                Effect::Aware(_) | Effect::SubscribePresence | Effect::PausePresence => {}
            }
        }
    }

    fn subscribe(&mut self, conn: u64, epoch: u32, start: StartPosition) -> Vec<Effect> {
        let behind = self.sim.pick(0..4) == 0;
        let Some(stream) = &self.home.stream else { return vec![Effect::Close(close::UNAVAILABLE)] };
        let session = self.sessions.get_mut(&conn).expect("subscribing");
        let generation = stream.generation;
        match start {
            // A relay that took this generation before its first records: an empty state, then the records live.
            StartPosition::SnapshotThenLive if behind && stream.ring.front().is_some_and(|(seq, _)| *seq == 1) => {
                let snapshot = Snapshot::new(Cursor { generation, sequence: 0 }, vec![]);
                let mut effects = session.core.event(epoch, Some(StreamEvent::Snapshot(snapshot)), true);
                let records: Vec<Bytes> = stream.ring.iter().map(|(_, r)| r.clone()).collect();
                let batch = SequencedBatch::new(generation, 1, records).expect("the ring holds records");
                effects.extend(session.core.event(epoch, Some(StreamEvent::Batch(batch)), true));
                effects
            }
            StartPosition::SnapshotThenLive => {
                let records: Vec<Bytes> = stream.table.iter().map(|(k, v)| keyed(k, v)).collect();
                let snapshot = Snapshot::new(Cursor { generation, sequence: stream.seq }, records);
                session.core.event(epoch, Some(StreamEvent::Snapshot(snapshot)), true)
            }
            StartPosition::After(cursor) => {
                let first = stream.ring.front().map_or(stream.seq + 1, |(seq, _)| *seq);
                if cursor.generation != generation || cursor.sequence > stream.seq || cursor.sequence + 1 < first {
                    return session.core.refused();
                }
                let records: Vec<Bytes> =
                    stream.ring.iter().filter(|(seq, _)| *seq > cursor.sequence).map(|(_, r)| r.clone()).collect();
                match SequencedBatch::new(generation, cursor.sequence + 1, records) {
                    Ok(batch) => session.core.event(epoch, Some(StreamEvent::Batch(batch)), true),
                    Err(_) => vec![], // nothing after it yet
                }
            }
            other => panic!("a session subscribes {other:?}"),
        }
    }

    fn publish(&mut self, records: Vec<Bytes>) {
        let stream = self.home.stream.as_mut().expect("published while running");
        let first = stream.seq + 1;
        stream.publish(&records);
        let batch = SequencedBatch::new(stream.generation, first, records).unwrap();
        let conns: Vec<(u64, u32)> = self.sessions.iter().filter_map(|(c, s)| Some((*c, s.sub?))).collect();
        for (conn, epoch) in conns {
            let effects = match self.sim.pick(0..60) {
                // The relay resets a slow branch, or the stream ends under it.
                0 | 1 => {
                    let event = if self.sim.pick(0..2) == 0 {
                        StreamEvent::ResetRequired(ResetReason::SlowConsumer)
                    } else {
                        StreamEvent::End(EndReason::StreamClosed)
                    };
                    let session = self.sessions.get_mut(&conn).unwrap();
                    session.sub = None;
                    let mut effects = session.core.event(epoch, Some(event), true);
                    effects.extend(session.core.event(epoch, None, true));
                    effects
                }
                n => {
                    let room = n > 3;
                    self.sessions.get_mut(&conn).unwrap().core.event(
                        epoch,
                        Some(StreamEvent::Batch(batch.clone())),
                        room,
                    )
                }
            };
            self.effects(conn, effects);
        }
    }

    fn pump(&mut self) {
        let home = &mut self.home;
        let (Some(doc), Some(log)) = (home.doc.as_mut(), home.log.as_mut()) else { return };
        if home.waiting.is_some() {
            return;
        }
        let Some(ops) = doc.take() else { return };
        home.waiting = Some(pipeline::append(log, KEY, doc.generation(), ops).unwrap());
        self.start_write();
    }

    fn start_write(&mut self) {
        let Some(log) = self.home.log.as_mut() else { return };
        if self.home.writing.is_some() {
            return;
        }
        if let Some(write) = log.take_write(self.sim.now().as_millis() as u64) {
            self.home.writing = Some(write);
            let after = Duration::from_millis(self.sim.pick(1..8));
            self.timer(after, Timer::WriteDone(self.home.incarnation));
        }
    }

    fn write_done(&mut self) {
        let write = self.home.writing.take().expect("a write out");
        let failed = self.sim.pick(0..25) == 0;
        let result = if failed { Err(std::io::Error::other("the disk fails once")) } else { now(write) };
        let durable = self.home.log.as_mut().expect("writing").written(result);
        if failed {
            let incarnation = self.home.incarnation;
            return self.timer(Duration::from_millis(5), Timer::WriteAgain(incarnation));
        }
        if self.home.waiting.is_some_and(|lsn| lsn <= durable) {
            self.home.waiting = None;
            let done = self.home.doc.as_mut().expect("writing").written();
            for batch in done.publish {
                self.publish(batch);
            }
            for waiter in done.waiters.into_iter().filter(|w| w.reply != NOBODY) {
                self.send(self.home_node, self.web, Wire::Answer { id: waiter.reply, answer: waiter.answer });
            }
        }
        self.pump();
        self.start_write();
    }

    fn call(&mut self, id: u64, req: DocReq) {
        self.load();
        let doc = self.home.doc.as_mut().expect("loaded");
        let answer = match req {
            DocReq::Join { who } => doc.join(&who, id),
            DocReq::Edit { session, who, update, offline } => {
                doc.edit(&who, Edit { session, update: &update, offline, now_ms: 0 }, id)
            }
            other => panic!("the sim does not call {other:?}"),
        };
        if let Some((id, answer)) = answer {
            self.send(self.home_node, self.web, Wire::Answer { id, answer });
        }
        self.pump();
    }

    fn deliver(&mut self, wire: Wire) {
        match wire {
            // Frames of a connection are put back in order, as TCP does; a lost one has ended the connection.
            Wire::Up { conn, n, frame } => {
                let Some(session) = self.sessions.get_mut(&conn) else { return };
                if n > session.received {
                    session.held.insert(n, frame);
                }
                loop {
                    let Some(session) = self.sessions.get_mut(&conn) else { return };
                    let Some(frame) = session.held.remove(&(session.received + 1)) else { return };
                    session.received += 1;
                    let effects = session.core.client(frame);
                    self.effects(conn, effects);
                }
            }
            Wire::Down { conn, n, frame } => {
                let Some(i) = self.clients.iter().position(|c| c.conn == Some(conn)) else { return };
                if n > self.clients[i].received {
                    self.clients[i].held.insert(n, frame);
                }
                while self.clients[i].conn == Some(conn) {
                    let client = &mut self.clients[i];
                    let Some(frame) = client.held.remove(&(client.received + 1)) else { break };
                    (client.received, client.heard) = (client.received + 1, self.sim.now());
                    self.client_frame(i, conn, frame);
                }
            }
            Wire::Closed { conn, code } => {
                if let Some(i) = self.clients.iter().position(|c| c.conn == Some(conn)) {
                    assert_eq!(code, close::UNAVAILABLE);
                    self.disconnect(i);
                }
            }
            Wire::Call { id, req } => self.call(id, req),
            Wire::Answer { id, answer } => {
                let Some((&conn, session)) = self.sessions.iter_mut().find(|(_, s)| s.call == Some(id)) else { return };
                session.call = None;
                let effects = session.core.answered(answer);
                self.effects(conn, effects);
            }
        }
    }

    fn client_frame(&mut self, i: usize, conn: u64, frame: ServerFrame) {
        for act in self.clients[i].core.frame(frame) {
            match act {
                ClientAct::Send(frame) => self.up(i, frame),
                ClientAct::Rejoin => {
                    self.note(format!("client {i} rejoins on a gap"));
                    return self.disconnect(i);
                }
                ClientAct::RetryAfter(ms) => self.timer(Duration::from_millis(ms), Timer::Flush(i, conn)),
                ClientAct::Saved(marks) => {
                    let saved = marks.iter().filter_map(|m| self.tokens.get(m).cloned());
                    self.acked.extend(saved);
                }
            }
        }
    }

    fn type_once(&mut self, i: usize) {
        let client = &mut self.clients[i];
        let text = crdt::text(&client.core.doc);
        let tildes: Vec<usize> = text.char_indices().filter(|(_, c)| *c == '~').map(|(at, _)| at).collect();
        let delete = !tildes.is_empty() && self.sim.pick(0..4) == 0;
        let (update, mark) = if delete {
            let at = tildes[self.sim.pick(0..tildes.len() as u64) as usize];
            (crdt::remove(&client.core.doc, at as u32, 1), u64::MAX)
        } else {
            // Only between tokens, so none is ever split.
            let mut spots: Vec<usize> =
                text.char_indices().filter(|(_, c)| *c == '>' || *c == '~').map(|(at, _)| at + 1).collect();
            spots.push(0);
            let at = spots[self.sim.pick(0..spots.len() as u64) as usize];
            let mark = (i as u64) << 32 | client.typed;
            client.typed += 1;
            let token = format!("<{i}.{}>", mark & 0xFFFF_FFFF);
            self.tokens.insert(mark, token.clone());
            (crdt::insert(&client.core.doc, at as u32, &format!("{token}~~")), mark)
        };
        if let Some(frame) = client.core.local(update, mark) {
            self.up(i, frame);
        }
    }

    fn fire(&mut self, timer: Timer) {
        match timer {
            Timer::Type(i) => {
                if self.sim.now() < TYPING {
                    self.type_once(i);
                    let after = Duration::from_millis(self.sim.pick(5..60));
                    self.timer(after, Timer::Type(i));
                }
            }
            Timer::Tick(i) => {
                let client = &self.clients[i];
                let waiting = client.core.unsaved() > 0 || client.core.welcome.is_none();
                if client.conn.is_some() && waiting && self.sim.now() - client.heard > SILENT {
                    self.note(format!("client {i} hears nothing"));
                    self.disconnect(i);
                }
                self.timer(Duration::from_millis(100), Timer::Tick(i));
            }
            Timer::Reconnect(i) => {
                if self.clients[i].conn.is_none() {
                    self.connect(i);
                }
            }
            Timer::Flush(i, conn) => {
                if self.clients[i].conn == Some(conn)
                    && let Some(frame) = self.clients[i].core.retry()
                {
                    self.up(i, frame);
                }
            }
            Timer::CallTimeout(conn, id) => {
                let Some(session) = self.sessions.get_mut(&conn) else { return };
                if session.call == Some(id) {
                    session.call = None;
                    let effects = session.core.failed();
                    self.effects(conn, effects);
                }
            }
            Timer::Resume(conn) => {
                let Some(session) = self.sessions.get_mut(&conn) else { return };
                if session.sub.is_none()
                    && let Some(start) = session.core.resume()
                {
                    self.effects(conn, vec![Effect::Subscribe(start)]);
                }
            }
            Timer::WriteDone(incarnation) if incarnation == self.home.incarnation => self.write_done(),
            Timer::WriteAgain(incarnation) if incarnation == self.home.incarnation => self.start_write(),
            Timer::WriteDone(_) | Timer::WriteAgain(_) => {}
            Timer::Crash => self.crash(),
            Timer::Check => {}
        }
    }

    /// Everyone holds the server's text at its last position, and nothing waits.
    fn settled(&self) -> bool {
        let (Some(doc), Some(stream)) = (&self.home.doc, &self.home.stream) else { return false };
        let text = doc.text();
        doc.is_idle()
            && self.home.writing.is_none()
            && self.clients.iter().all(|c| {
                c.conn.is_some()
                    && c.core.welcome.is_some()
                    && c.core.unsaved() == 0
                    && c.core.position == Some((stream.generation, stream.seq))
                    && crdt::text(&c.core.doc) == text
            })
    }

    fn status(&self) -> String {
        let stream = self.home.stream.as_ref().map(|s| (s.generation, s.seq));
        let text = self.home.doc.as_ref().map(|d| d.text()).unwrap_or_default();
        let idle = self.home.doc.as_ref().map(|d| d.is_idle());
        let mut lines = vec![format!("stream {stream:?} idle {idle:?} writing {}", self.home.writing.is_some())];
        for (i, c) in self.clients.iter().enumerate() {
            lines.push(format!(
                "client {i}: conn {:?} welcome {} unsaved {} position {:?} same text {}",
                c.conn,
                c.core.welcome.is_some(),
                c.core.unsaved(),
                c.core.position,
                crdt::text(&c.core.doc) == text
            ));
        }
        for (conn, s) in &self.sessions {
            lines.push(format!("session {conn}: client {} call {:?} sub {:?}", s.client, s.call, s.sub));
        }
        lines.join("\n")
    }

    fn run(mut self) -> String {
        loop {
            let timer_at = self.timers.first_key_value().map(|((at, _), _)| *at);
            match (self.sim.next_delivery_at(), timer_at) {
                (next, Some(at)) if next.is_none_or(|next| at < next) => {
                    self.sim.advance_idle_to(at).unwrap();
                    let (_, timer) = self.timers.pop_first().expect("peeked");
                    self.fire(timer);
                }
                (Some(_), _) => {
                    if let Some(delivery) = self.sim.step() {
                        self.deliver(delivery.payload);
                    }
                }
                (None, _) => panic!("nothing left to run"),
            }
            let now = self.sim.now();
            if now >= TYPING && self.settled() {
                break;
            }
            assert!(now < GIVE_UP, "not settled after {GIVE_UP:?}:\n{}\n{}", self.log.join("\n"), self.status());
        }
        let doc = self.home.doc.as_ref().expect("settled");
        let text = doc.text();
        let server = crdt::state_vector(doc.ydoc());
        for client in &self.clients {
            let theirs = crdt::state_vector(&client.core.doc);
            assert!(!crdt::ahead(&client.core.doc, &server) && !crdt::ahead(doc.ydoc(), &theirs), "I3: state vectors");
            assert_eq!(crdt::text(&client.core.doc), text, "I3: a client's text");
        }
        assert!(self.acked.len() > 10, "too little was saved to show anything: {}", self.acked.len());
        // A last crash: everything saved is on disk.
        self.crash();
        self.load();
        let stats = self.sim.stats();
        format!(
            "{text}|{:?}|{}|{:?}",
            self.generations,
            self.acked.len(),
            (stats.sent, stats.dropped, stats.duplicated)
        )
    }
}

impl Stream {
    fn publish(&mut self, records: &[Bytes]) {
        for record in records {
            self.seq += 1;
            let (key, value) = split_keyed(record).expect("keyed");
            if value.is_empty() {
                self.table.remove(&key);
            } else {
                self.table.insert(key, value);
            }
            self.ring.push_back((self.seq, record.clone()));
            if self.ring.len() > RING {
                self.ring.pop_front();
            }
        }
    }
}

#[test]
fn clients_and_the_document_agree_and_keep_what_was_saved() {
    check_seeds(seeds(0..500), |seed| {
        Model::new(seed).run();
    })
    .unwrap();
}

#[test]
fn a_seed_runs_the_same_twice() {
    for seed in [3, 11] {
        let run = Model::new(seed).run();
        let (text, rest) = run.split_once('|').expect("a summary");
        eprintln!("seed {seed}: {} characters, {} lines|{rest}", text.len(), text.lines().count());
        assert_eq!(run, Model::new(seed).run());
    }
}
