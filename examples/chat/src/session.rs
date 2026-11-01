//! One actor per websocket. The socket's halves are attached to it, so its I/O never runs on the actor's thread;
//! each joined room is a stream subscription attached as a source. What the socket cannot take yet waits in a
//! bounded outbox; past that a room is paused (see [`Follow`]) instead of buffering.
//!
//! A joined room is asked for once, which starts it, and then followed by its name wherever it runs: a room that
//! moves or restarts shows as a new generation on the same subscription. When its node dies the subscription says
//! `OriginLost`, and the session asks for the room again, which starts it on its next node.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axon::{Actor, Context, JobHandle, RejectReason, SinkEvent, SinkHandle};
use axon_cluster::{Cluster, ClusterError, KeyedRef, Placement, Retry, Unaddressed};
use axon_stream::{Cursor, RecoveryError, StartPosition, StreamError, StreamEvent, StreamNode, SubscribeOptions};
use axum::extract::ws::{Message, WebSocket};
use bytes::Bytes;
use futures_util::StreamExt;

use crate::follow::{Act, Follow};
use crate::proto::{ClientFrame, Post, Posted, RoomApi, ServerFrame, json, msg_frame, pair, valid_room};
use crate::room;

/// Frames the socket's writer queues before `try_send` says `Full`.
pub const SINK_CAPACITY: usize = 64;
/// Frames waiting behind a full socket; past this the session's rooms pause.
pub const OUTBOX: usize = 256;
pub const MAX_ROOMS: usize = 64;
/// Posts waiting behind the one in flight; past this a post is refused `busy`.
pub const MAX_POSTS: usize = 32;
/// A room whose owner, by this node's view, is not answering is asked for again after this.
pub const ENSURE_RETRY: Duration = Duration::from_millis(500);

/// Times a session paused its rooms for a slow client, process-wide.
pub static PAUSES: AtomicU64 = AtomicU64::new(0);

pub enum Msg {
    Client(Option<Result<Message, axum::Error>>),
    Socket(SinkEvent<axum::Error>),
    Ensure(Arc<str>),
    Ensured(Arc<str>, Result<(), ClusterError>),
    Event(Arc<str>, u32, Option<StreamEvent>),
    Posted(Arc<str>, u64, Result<Posted, ClusterError>),
}

struct Joined {
    follow: Follow,
    job: Option<JobHandle>,
    poster: KeyedRef<RoomApi>,
    /// The room is being asked for, or will be again on a timer: an `OriginLost` meanwhile asks nothing more.
    asking: bool,
}

pub struct Session {
    user: String,
    socket: Option<WebSocket>,
    cluster: Cluster,
    streams: StreamNode,
    tx: Option<SinkHandle<Message>>,
    outbox: VecDeque<Message>,
    rooms: HashMap<Arc<str>, Joined>,
    /// One post in flight at a time keeps a client's posts in the order it sent them.
    posts: VecDeque<(Arc<str>, u64, String)>,
    posting: bool,
}

impl Session {
    pub fn new(user: String, socket: WebSocket, cluster: Cluster, streams: StreamNode) -> Self {
        let (tx, outbox, rooms, posts) = (None, VecDeque::new(), HashMap::new(), VecDeque::new());
        Self { user, socket: Some(socket), cluster, streams, tx, outbox, rooms, posts, posting: false }
    }

    fn say(&mut self, frame: String) {
        self.outbox.push_back(Message::Text(frame.into()));
    }

    fn join(&mut self, cx: &mut Context<'_, Self>, room: String, after: Option<(u64, u64)>) {
        if !valid_room(&room) || self.rooms.len() >= MAX_ROOMS || self.rooms.contains_key(room.as_str()) {
            return self.say(json(&ServerFrame::Error { why: format!("cannot join {room:?}") }));
        }
        let room: Arc<str> = room.into();
        let cursor = after.map(|(generation, sequence)| Cursor { generation, sequence });
        let poster = self.cluster.keyed(room::KIND, &room, Bytes::new(), &Placement::default());
        let joined = Joined { follow: Follow::new(cursor), job: None, poster, asking: true };
        self.rooms.insert(room.clone(), joined);
        self.ensure(cx, room);
    }

    /// Starts the room before the first subscribe, so the subscription finds its stream open; and again when its
    /// origin is lost, which starts it on its next node.
    fn ensure(&mut self, cx: &mut Context<'_, Self>, room: Arc<str>) {
        let (cluster, name) = (self.cluster.clone(), room.clone());
        let ensure = async move {
            let placement = Placement::default();
            let ensured = cluster.get_or_spawn::<Unaddressed>(room::KIND, &name, Bytes::new(), &placement).await;
            Msg::Ensured(name, ensured.map(drop))
        };
        if cx.submit_future(ensure).is_err() {
            self.drop_room(&room, "busy".into());
        }
    }

    fn subscribe(&mut self, cx: &mut Context<'_, Self>, room: &Arc<str>, start: StartPosition) {
        let Some(joined) = self.rooms.get_mut(room) else { return };
        let options = SubscribeOptions { start, ..Default::default() };
        let subscription = match self.streams.subscribe(&room::key(room), options) {
            // This node's ring no longer holds what follows the client's cursor: carry on live.
            Err(StreamError::Recovery(RecoveryError::HistoryUnavailable)) => {
                joined.follow.cursor = None;
                let name = room.clone();
                self.say(json(&ServerFrame::Reset { room: room.to_string(), why: "history_unavailable".into() }));
                return self.subscribe(cx, &name, StartPosition::Live);
            }
            // The cursor is from another generation (a client back after its room restarted): all of this one is new.
            Err(StreamError::Recovery(RecoveryError::GenerationMismatch)) if start != StartPosition::First => {
                joined.follow.cursor = None;
                let name = room.clone();
                self.say(json(&ServerFrame::Reset { room: room.to_string(), why: "generation_changed".into() }));
                return self.subscribe(cx, &name, StartPosition::First);
            }
            Err(e) => return self.drop_room(room, format!("subscribe {e:?}")),
            Ok(subscription) => subscription,
        };
        let epoch = joined.follow.subscribed();
        let name = room.clone();
        match cx.attach(subscription, move |event| Msg::Event(name.clone(), epoch, event)) {
            Ok(job) => {
                if let Some(old) = joined.job.replace(job) {
                    old.cancel();
                }
            }
            Err(e) => self.drop_room(room, format!("attach {e:?}")),
        }
    }

    fn drop_room(&mut self, room: &Arc<str>, why: String) {
        if let Some(job) = self.rooms.remove(room).and_then(|j| j.job) {
            job.cancel();
        }
        self.say(json(&ServerFrame::Error { why: format!("{room}: {why}") }));
    }

    fn client(&mut self, cx: &mut Context<'_, Self>, frame: ClientFrame) {
        match frame {
            ClientFrame::Join { room, after } => self.join(cx, room, after),
            ClientFrame::Leave { room } => {
                if let Some(job) = self.rooms.remove(room.as_str()).and_then(|j| j.job) {
                    job.cancel();
                }
            }
            ClientFrame::Post { room, id, text } => match self.rooms.get(room.as_str()) {
                None => self.say(json(&ServerFrame::Nack { id, room, why: "not_joined".into() })),
                Some(_) if self.posts.len() >= MAX_POSTS => {
                    self.say(json(&ServerFrame::Nack { id, room, why: "busy".into() }))
                }
                Some(_) => {
                    self.posts.push_back((room.into(), id, text));
                    self.post_next(cx);
                }
            },
        }
    }

    fn post_next(&mut self, cx: &mut Context<'_, Self>) {
        while !self.posting {
            let Some((room, id, text)) = self.posts.pop_front() else { return };
            let Some(joined) = self.rooms.get(&room) else {
                self.say(json(&ServerFrame::Nack { id, room: room.to_string(), why: "not_joined".into() }));
                continue;
            };
            let (poster, from) = (joined.poster.clone(), self.user.clone());
            let name = room.clone();
            let post = async move { Msg::Posted(name, id, poster.request(Post { from, id, text }).await) };
            match cx.submit_future(post) {
                Ok(_) => self.posting = true,
                Err(_) => self.say(json(&ServerFrame::Nack { id, room: room.to_string(), why: "busy".into() })),
            }
        }
    }

    fn event(&mut self, cx: &mut Context<'_, Self>, room: Arc<str>, epoch: u32, event: Option<StreamEvent>) {
        let fits = self.outbox.len() < OUTBOX;
        let Some(joined) = self.rooms.get_mut(&room) else { return };
        match joined.follow.on_event(epoch, event, fits) {
            Act::Nothing => {}
            Act::Ready(at) => self.say(json(&ServerFrame::Ready { room: room.to_string(), at: at.map(pair) })),
            Act::Deliver(batch) => {
                if let Some(frame) = msg_frame(&room, &batch) {
                    self.say(frame);
                }
            }
            Act::Reset(why) => self.say(json(&ServerFrame::Reset { room: room.to_string(), why: why.into() })),
            Act::Pause => {
                PAUSES.fetch_add(1, Ordering::Relaxed);
                if let Some(job) = joined.job.take() {
                    job.cancel();
                }
            }
            Act::Subscribe(start) => self.subscribe(cx, &room, start),
            Act::Lost if !joined.asking => {
                joined.asking = true;
                self.ensure(cx, room);
            }
            Act::Lost => {} // already being asked for
        }
    }
}

impl Actor for Session {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (write, read) = self.socket.take().ok_or("started twice")?.split();
        cx.attach(read, Msg::Client).map_err(|e| format!("{e:?}"))?;
        self.tx = Some(cx.attach_sink(write, SINK_CAPACITY, Msg::Socket).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Client(Some(Ok(Message::Text(text)))) => match serde_json::from_str(&text) {
                Ok(frame) => self.client(cx, frame),
                Err(e) => self.say(json(&ServerFrame::Error { why: e.to_string() })),
            },
            Msg::Client(Some(Ok(Message::Close(_))) | Some(Err(_)) | None) | Msg::Socket(SinkEvent::Closed(_)) => {
                cx.stop()
            }
            Msg::Client(Some(Ok(_))) => {} // pings are answered by the socket; binary frames mean nothing here
            Msg::Socket(SinkEvent::Ready) => {} // on_turn_end writes the outbox
            Msg::Ensure(room) if self.rooms.contains_key(&room) => self.ensure(cx, room),
            Msg::Ensure(_) => {} // left meanwhile
            Msg::Ensured(room, Ok(())) => {
                let Some(joined) = self.rooms.get_mut(&room) else { return Ok(()) };
                joined.asking = false;
                // The first ask is the join's: subscribe now. Later ones were for a lost origin, and the
                // subscription is still there.
                if joined.follow.epoch == 0 {
                    let start = joined.follow.start();
                    self.subscribe(cx, &room, start);
                }
            }
            // ponytail: this node's view still names a gone owner; ask again until membership catches up. Asking
            // is idempotent, so an unknown outcome is asked again too.
            Msg::Ensured(room, Err(e)) if self.rooms.contains_key(&room) && e.retry() != Retry::Never => {
                if let Err(timer) = cx.schedule_after(ENSURE_RETRY, Msg::Ensure(room.clone())) {
                    self.drop_room(&room, format!("room unavailable: {e}, and no timer: {timer:?}"));
                }
            }
            Msg::Ensured(room, Err(e)) => {
                if self.rooms.contains_key(&room) {
                    self.drop_room(&room, format!("room unavailable: {e}"));
                }
            }
            Msg::Event(room, epoch, event) => self.event(cx, room, epoch, event),
            Msg::Posted(room, id, posted) => {
                self.posting = false;
                self.post_next(cx);
                let room = room.to_string();
                let frame = match posted {
                    Ok(Ok((generation, seq))) => ServerFrame::Ack { id, room, generation, seq },
                    Ok(Err(why)) => ServerFrame::Nack { id, room, why },
                    Err(e) => {
                        let why = match e.retry() {
                            axon_cluster::Retry::IfIdempotent => "unknown".to_string(),
                            axon_cluster::Retry::Later => "busy".to_string(),
                            _ => format!("{e}"),
                        };
                        ServerFrame::Nack { id, room, why }
                    }
                };
                self.say(json(&frame));
            }
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
        // Room frames stop at OUTBOX (the rooms pause); what else piles up past twice that is a client not reading.
        if self.outbox.len() > 2 * OUTBOX {
            cx.stop();
            return Ok(());
        }
        if self.outbox.len() <= OUTBOX / 2 {
            let paused: Vec<(Arc<str>, StartPosition)> =
                self.rooms.iter().filter_map(|(room, j)| Some((room.clone(), j.follow.resume()?))).collect();
            for (room, start) in paused {
                self.subscribe(cx, &room, start);
            }
        }
        Ok(())
    }
}
