//! One actor per websocket, running [`SessionCore`]. The socket's halves are attached to it, so its I/O never runs on
//! the actor's thread; the document's stream and its presence stream are subscriptions attached as sources. What the
//! socket cannot take yet waits in a bounded outbox; past it the subscriptions pause, the document's keeping its
//! position, until the client has read half. A batch already on its way still lands, so the socket closes only past
//! room for one more.
//!
//! A subscriber of a document's state is never told that the document's home went (`axon-stream/src/hub.rs:970`),
//! so the session watches where the document runs (`watch_key`). When it stops, the client is told the home is out of
//! reach, and the session asks the document to join again, with backoff, until it answers: the document starts only
//! when it is called. The new generation then ends the subscription, and the client gets the state again.
//!
//! The socket is a stream of [`In`] and a sink of [`Out`]: a websocket's on a web node, a pair of channels in the
//! World tests, whose network carries no websockets.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axon::{Actor, Backoff, Context, JobHandle, RejectReason, SinkEvent, SinkHandle};
use axon_cluster::{Cluster, ClusterError, KeyedRef, Owner, Retry};
use axon_stream::{
    BranchLimits, RecoveryError, StartPosition, StreamError, StreamEvent, StreamKey, StreamNode, SubscribeOptions,
};
use bytes::Bytes;
use futures_util::{Sink, Stream};

use crate::doc;
use crate::follow::{Effect, SessionCore, close_code};
use crate::wire::{Aware, ClientFrame, DocAnswer, DocP, DocReq, Notice, ServerFrame, close};

/// Frames the socket's writer queues before `try_send` says `Full`.
pub const SINK_CAPACITY: usize = 64;
/// Frames, or bytes, waiting behind a full socket: past either the subscriptions pause (design §5).
pub const OUTBOX: usize = 256;
pub const OUTBOX_BYTES: usize = 4 << 20;
/// A call to the document that is not answered in this long is asked again.
const CALL_WITHIN: Duration = Duration::from_secs(5);
/// A session that could not reach its document for this long closes, and the client joins again.
const GIVE_UP: Duration = Duration::from_secs(30);
/// A cursor goes to the document at most this often; the last one wins.
const AWARE_EVERY: Duration = Duration::from_millis(100);
/// The presence is told again this often, so the document keeps it and stays open.
const RENEW_EVERY: Duration = Duration::from_secs(15);
/// The asks to join again while the home is out of reach. At most a second apart, so a home started again saves within
/// 2 s (design §7): with 8 s, 100 SIGKILLs of a home saved again 2.6 s after its start at p50 and 6.7 s at p99. A call
/// to a node that is down fails at once, so a crowd of sessions asking costs little.
const PROBE: Backoff = Backoff { initial: Duration::from_millis(250), max: Duration::from_secs(1), jitter: 0.5 };

/// Sessions open now, and times a session paused for a slow client: process-wide, for tests and the load report.
pub static OPEN: AtomicU64 = AtomicU64::new(0);
pub static PAUSES: AtomicU64 = AtomicU64::new(0);

/// What the client sent.
pub enum In {
    Frame(Bytes),
    /// Text, a ping: nothing to the protocol.
    Other,
    Closed,
}

/// What goes to the client.
pub enum Out {
    Frame(Bytes),
    Close(u16),
}

pub type Reader = Pin<Box<dyn Stream<Item = In> + Send>>;
pub type Writer = Pin<Box<dyn Sink<Out, Error = String> + Send>>;

pub enum Msg {
    Client(Option<In>),
    Socket(SinkEvent<String>),
    Answer(Result<DocAnswer, ClusterError>),
    /// The call out failed earlier: ask again.
    Again,
    Event(u32, Option<StreamEvent>),
    Presence(u32, Option<StreamEvent>),
    /// The stream was not there: start the document, then subscribe.
    Ensure,
    Ensured(Result<DocAnswer, ClusterError>),
    /// Where the document runs changed.
    Owner(Option<Owner>),
    /// Ask the document to join while its home is out of reach.
    Probe,
    Probed(Result<DocAnswer, ClusterError>),
    /// The cursor waiting to go to the document goes now.
    AwareDue,
    Renew,
    Told,
    /// The user's sign-ins ended: a disable or a new password.
    Revoked,
    /// A close frame went out and the socket has not closed.
    Hangup,
}

pub struct Session {
    core: SessionCore,
    io: Option<(Reader, Writer)>,
    cluster: Cluster,
    doc: KeyedRef<DocP>,
    /// This web node, as presence names it.
    node: String,
    streams: StreamNode,
    key: StreamKey,
    presence_key: StreamKey,
    tx: Option<SinkHandle<Out>>,
    outbox: Outbox,
    subscription: Option<JobHandle>,
    presence: Option<JobHandle>,
    /// Failures in a row of the calls out and the subscribes, and since when.
    failures: u32,
    failing_since: Option<Duration>,
    /// The cursor waiting for `AwareDue`, and the last one told, which renewals tell again.
    aware: Option<Option<Aware>>,
    aware_due: bool,
    told: Option<Option<Aware>>,
    /// The home is out of reach: the asks to join, and whether one is out.
    unreachable: bool,
    probes: u32,
    probing: bool,
    /// Where the leave is told from once the actor has stopped.
    runtime: tokio::runtime::Handle,
    closing: bool,
}

/// The frames for the socket, and their bytes.
#[derive(Default)]
struct Outbox {
    frames: VecDeque<Out>,
    bytes: usize,
}

fn size(frame: &Out) -> usize {
    match frame {
        Out::Frame(b) => b.len(),
        Out::Close(_) => 0,
    }
}

impl Outbox {
    fn push_back(&mut self, frame: Out) {
        self.bytes += size(&frame);
        self.frames.push_back(frame);
    }

    fn push_front(&mut self, frame: Out) {
        self.bytes += size(&frame);
        self.frames.push_front(frame);
    }

    fn pop_front(&mut self) -> Option<Out> {
        let frame = self.frames.pop_front()?;
        self.bytes -= size(&frame);
        Some(frame)
    }

    fn has_room(&self) -> bool {
        self.frames.len() < OUTBOX && self.bytes < OUTBOX_BYTES
    }

    fn read_half(&self) -> bool {
        self.frames.len() <= OUTBOX / 2 && self.bytes <= OUTBOX_BYTES / 2
    }

    /// Past twice a full outbox and one batch of each subscription's (a compaction is one, with a frame for each key
    /// it removes): a client that does not read.
    fn overflows(&self) -> bool {
        let batch = BranchLimits::default();
        self.frames.len() > 2 * OUTBOX + 2 * batch.max_records || self.bytes > 2 * OUTBOX_BYTES + 2 * batch.max_bytes
    }
}

fn cancel(job: &mut Option<JobHandle>) {
    if let Some(job) = job.take() {
        job.cancel();
    }
}

/// A failed call again: at most a second apart, as `PROBE`, since an edit that waits starts the document too.
const BACKOFF: Backoff = Backoff { initial: Duration::from_millis(100), max: Duration::from_secs(1), jitter: 0.5 };

impl Session {
    /// A session of document `id` on this web node, over `io`. Made inside a tokio runtime.
    pub fn new(core: SessionCore, io: (Reader, Writer), cluster: &Cluster, streams: StreamNode, id: &str) -> Self {
        Self {
            core,
            io: Some(io),
            cluster: cluster.clone(),
            doc: doc::keyed(cluster, id),
            node: cluster.record().id.clone(),
            streams,
            key: doc::stream_key(id),
            presence_key: doc::presence_key(id),
            tx: None,
            outbox: Outbox::default(),
            subscription: None,
            presence: None,
            failures: 0,
            failing_since: None,
            aware: None,
            aware_due: false,
            told: None,
            unreachable: false,
            probes: 0,
            probing: false,
            runtime: tokio::runtime::Handle::current(),
            closing: false,
        }
    }

    fn effects(&mut self, cx: &mut Context<'_, Self>, effects: Vec<Effect>) {
        for effect in effects {
            if self.closing {
                return;
            }
            match effect {
                Effect::Send(frame) => self.outbox.push_back(Out::Frame(frame.encode().into())),
                Effect::Call(req) => self.call(cx, req, Msg::Answer),
                Effect::Subscribe(start) => self.subscribe(cx, start),
                Effect::SubscribePresence => self.subscribe_presence(cx),
                Effect::Pause => {
                    PAUSES.fetch_add(1, Ordering::Relaxed);
                    cancel(&mut self.subscription);
                }
                Effect::PausePresence => cancel(&mut self.presence),
                Effect::Aware(entry) => {
                    self.aware = Some(entry);
                    if !self.aware_due {
                        self.aware_due = cx.schedule_after(AWARE_EVERY, Msg::AwareDue).is_ok();
                    }
                }
                Effect::Close(code) => self.close(cx, code),
            }
        }
    }

    fn call(&mut self, cx: &mut Context<'_, Self>, req: DocReq, done: fn(Result<DocAnswer, ClusterError>) -> Msg) {
        let doc = self.doc.clone();
        if cx.submit_future(async move { done(doc.request_within(req, CALL_WITHIN).await) }).is_err() {
            self.close(cx, close::UNAVAILABLE);
        }
    }

    /// Tells the document this session's presence. A lost one is told again at the next renewal.
    fn tell_aware(&mut self, cx: &mut Context<'_, Self>, entry: Option<Aware>) {
        let req = DocReq::Aware { session: self.core.session, node: self.node.clone(), entry: entry.clone() };
        let doc = self.doc.clone();
        let tell = async move {
            drop(doc.tell(req).await);
            Msg::Told
        };
        if cx.submit_future(tell).is_ok() {
            // An editor whose client left has nothing to renew.
            self.told = Some(entry).filter(|e| e.as_ref().is_none_or(|e| e.state.is_some()));
        }
    }

    fn subscribe(&mut self, cx: &mut Context<'_, Self>, start: StartPosition) {
        cancel(&mut self.subscription);
        let options = SubscribeOptions { start, ..Default::default() };
        let subscription = match self.streams.subscribe(&self.key, options) {
            Ok(subscription) => subscription,
            Err(StreamError::Recovery(RecoveryError::HistoryUnavailable | RecoveryError::GenerationMismatch)) => {
                let effects = self.core.refused();
                return self.effects(cx, effects);
            }
            // No stream here: the document stopped. Start it, then subscribe.
            Err(_) => return self.retry(cx, Msg::Ensure),
        };
        let epoch = self.core.subscribed();
        match cx.attach(subscription, move |event| Msg::Event(epoch, event)) {
            Ok(job) => self.subscription = Some(job),
            Err(_) => self.close(cx, close::UNAVAILABLE),
        }
    }

    /// Presence is shown on a best effort: a subscription refused comes again with the next new generation.
    fn subscribe_presence(&mut self, cx: &mut Context<'_, Self>) {
        cancel(&mut self.presence);
        let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
        let Ok(subscription) = self.streams.subscribe(&self.presence_key, options) else { return };
        let epoch = self.core.presence_subscribed();
        self.presence = cx.attach(subscription, move |event| Msg::Presence(epoch, event)).ok();
    }

    /// After a failure: `msg` once the backoff has passed, or a close once the document has been out of reach for long.
    fn retry(&mut self, cx: &mut Context<'_, Self>, msg: Msg) {
        let since = *self.failing_since.get_or_insert(cx.now());
        if cx.now().saturating_sub(since) > GIVE_UP {
            return self.close(cx, close::UNAVAILABLE);
        }
        self.failures += 1;
        if cx.schedule_after(BACKOFF.delay(self.failures), msg).is_err() {
            self.close(cx, close::UNAVAILABLE);
        }
    }

    fn reached(&mut self) {
        (self.failures, self.failing_since) = (0, None);
    }

    /// The home answers again: the client is told, and a new generation means the state again.
    fn reachable(&mut self, cx: &mut Context<'_, Self>, generation: u64) {
        if std::mem::take(&mut self.unreachable) {
            self.probes = 0;
            self.effects(cx, vec![Effect::Send(ServerFrame::Notice(Notice::Reachable))]);
        }
        if self.core.cursor.is_some_and(|c| c.generation != generation) {
            let effects = self.core.refused();
            self.effects(cx, effects);
            self.subscribe_presence(cx);
        }
    }

    /// A refusal that asking again cannot change: the document is gone, or the call was not one.
    fn refused(e: &ClusterError) -> Option<u16> {
        match e {
            _ if doc::unknown(e) => Some(close::NOT_FOUND),
            _ if e.retry() == Retry::Never => Some(close::UNAVAILABLE),
            _ => None,
        }
    }

    fn close(&mut self, cx: &mut Context<'_, Self>, code: u16) {
        if self.closing {
            return;
        }
        self.closing = true;
        cancel(&mut self.subscription);
        cancel(&mut self.presence);
        // What was queued for the client still goes before the close; the timer ends a socket that does not read.
        self.outbox.push_back(Out::Close(code));
        if cx.schedule_after(Duration::from_secs(2), Msg::Hangup).is_err() {
            cx.stop();
        }
    }
}

impl Actor for Session {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (read, write) = self.io.take().ok_or("started twice")?;
        cx.attach(read, Msg::Client).map_err(|e| format!("{e:?}"))?;
        self.tx = Some(cx.attach_sink(write, SINK_CAPACITY, Msg::Socket).map_err(|e| format!("{e:?}"))?);
        let id = self.key.name();
        let watch = self.cluster.watch_key(doc::KIND, id, &doc::home(doc::home_of(id)));
        cx.attach(watch, Msg::Owner).map_err(|e| format!("{e:?}"))?;
        cx.schedule_every(RENEW_EVERY, || Msg::Renew).map_err(|e| format!("{e:?}"))?;
        // Last: on_stop, which takes it back, runs only after a start that succeeded.
        OPEN.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Client(Some(In::Frame(bytes))) if !self.closing => match ClientFrame::decode(&bytes) {
                Ok(frame) => {
                    let effects = self.core.client(frame);
                    self.effects(cx, effects);
                }
                Err(_) => self.close(cx, close::TOO_LARGE),
            },
            Msg::Client(Some(In::Closed) | None) | Msg::Socket(SinkEvent::Closed(_)) | Msg::Hangup => cx.stop(),
            Msg::Client(Some(_)) => {} // what comes after a close went out, and pings
            Msg::Socket(SinkEvent::Ready) => {}
            Msg::Answer(Ok(answer)) => {
                self.reached();
                let effects = self.core.answered(answer);
                self.effects(cx, effects);
            }
            Msg::Answer(Err(e)) => match Self::refused(&e) {
                Some(code) => self.close(cx, code),
                None => self.retry(cx, Msg::Again),
            },
            Msg::Again => {
                let effects = self.core.failed();
                self.effects(cx, effects);
            }
            Msg::Event(epoch, event) => {
                let room = self.outbox.has_room();
                let effects = self.core.event(epoch, event, room);
                self.effects(cx, effects);
            }
            Msg::Presence(epoch, event) => {
                let room = self.outbox.has_room();
                let effects = self.core.presence_event(epoch, event, room);
                self.effects(cx, effects);
            }
            Msg::Ensure if !self.closing => {
                let req = DocReq::Join { who: self.core.who().clone() };
                self.call(cx, req, Msg::Ensured);
            }
            Msg::Ensure => {}
            Msg::Ensured(Ok(DocAnswer::Joined { .. })) => {
                self.reached();
                let effects = self.core.refused();
                self.effects(cx, effects);
                self.subscribe_presence(cx);
            }
            Msg::Ensured(Ok(DocAnswer::Refused(why))) => self.close(cx, close_code(why)),
            Msg::Ensured(Ok(_)) => self.retry(cx, Msg::Ensure),
            Msg::Ensured(Err(e)) => match Self::refused(&e) {
                Some(code) => self.close(cx, code),
                None => self.retry(cx, Msg::Ensure),
            },
            Msg::Owner(Some(Owner::Stopped)) if !self.unreachable && !self.closing => {
                self.unreachable = true;
                self.effects(cx, vec![Effect::Send(ServerFrame::Notice(Notice::Unreachable))]);
                if !self.probing {
                    self.probing = cx.schedule_after(PROBE.delay(0), Msg::Probe).is_ok();
                }
            }
            // A probe, or another session's call started the document: the join's answer names its generation, and a
            // new one means the state again. Waiting for the stream to end the old one left 5 of 600 sessions behind
            // for minutes in 100 SIGKILLs of a home (`scripts/measure.sh kills`).
            Msg::Owner(Some(Owner::Running { .. })) | Msg::Probe if self.unreachable && !self.closing => {
                let req = DocReq::Join { who: self.core.who().clone() };
                self.call(cx, req, Msg::Probed);
            }
            Msg::Owner(_) => {}
            Msg::Probe => self.probing = false,
            Msg::Probed(Ok(DocAnswer::Joined { generation, .. })) => {
                self.probing = false;
                self.reachable(cx, generation);
            }
            Msg::Probed(Ok(DocAnswer::Refused(why))) => self.close(cx, close_code(why)),
            Msg::Probed(_) => {
                self.probes += 1;
                self.probing = cx.schedule_after(PROBE.delay(self.probes), Msg::Probe).is_ok();
            }
            Msg::AwareDue => {
                self.aware_due = false;
                if let Some(entry) = self.aware.take() {
                    self.tell_aware(cx, entry);
                }
            }
            Msg::Renew => {
                if let Some(entry) = self.told.clone() {
                    self.tell_aware(cx, entry);
                }
            }
            Msg::Told => {}
            Msg::Revoked => self.close(cx, close::UNAUTHORIZED),
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(tx) = &self.tx else { return Ok(()) };
        while let Some(frame) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(frame) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message); // until SinkEvent::Ready
                }
                break;
            }
        }
        if self.outbox.overflows() {
            cx.stop(); // a client that does not read
            return Ok(());
        }
        if self.outbox.read_half() {
            if let Some(start) = self.core.resume() {
                self.subscribe(cx, start);
            }
            if self.core.resume_presence() {
                self.subscribe_presence(cx);
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &axon::StopReason) {
        OPEN.fetch_sub(1, Ordering::Relaxed);
        // The others stop seeing this session at once; a leave that is lost expires in 30 s.
        if self.told.is_some() {
            let (doc, session) = (self.doc.clone(), self.core.session);
            drop(self.runtime.spawn(async move { drop(doc.tell(DocReq::Leave { session }).await) }));
        }
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Client(_) => "session.client",
            Msg::Event(..) => "session.event",
            Msg::Presence(..) | Msg::AwareDue | Msg::Renew => "session.aware",
            _ => "session.other",
        }
    }
}
