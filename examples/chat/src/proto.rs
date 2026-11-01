//! What crosses the websocket and the room's cluster endpoint.

use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use axon_stream::{Cursor, SequencedBatch, StreamError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Room names go into JSON unescaped and into stream keys, so they are kept plain.
pub fn valid_room(name: &str) -> bool {
    (1..=64).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ClientFrame {
    /// `after` is the `[generation, sequence]` of the last message the client holds; absent joins live.
    Join {
        room: String,
        after: Option<(u64, u64)>,
    },
    Leave {
        room: String,
    },
    Post {
        room: String,
        id: u64,
        text: String,
    },
}

/// One stored message: the payload of one stream record.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Record {
    pub from: String,
    /// The poster's own id for the post, so a client can recognise its own messages.
    pub id: u64,
    pub text: String,
    /// Wall clock at the room, in milliseconds.
    pub ts: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerFrame {
    /// The subscription starts after `at`: the join point. A new generation means the room restarted: continuity
    /// broke. Posts made before a join's `ready` may or may not be delivered to it.
    Ready {
        room: String,
        at: Option<(u64, u64)>,
    },
    /// Records `first..` of generation `gen`, in order.
    Msg {
        room: String,
        #[serde(rename = "gen")]
        generation: u64,
        first: u64,
        m: Vec<Record>,
    },
    /// Continuity broke for a reason other than a restart; the session carries on live.
    Reset {
        room: String,
        why: String,
    },
    Ack {
        id: u64,
        room: String,
        #[serde(rename = "gen")]
        generation: u64,
        seq: u64,
    },
    /// `why` is `busy` (refused, try later), `unknown` (it may have been posted) or another refusal.
    Nack {
        id: u64,
        room: String,
        why: String,
    },
    Error {
        why: String,
    },
}

/// Builds the `msg` frame from the stored payloads as they are, with no parse and re-serialize per session.
pub fn msg_frame(room: &str, batch: &SequencedBatch) -> Option<String> {
    let mut out =
        format!(r#"{{"t":"msg","room":"{room}","gen":{},"first":{},"m":["#, batch.generation(), batch.first())
            .into_bytes();
    for (i, record) in batch.records().iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(record);
    }
    out.extend_from_slice(b"]}");
    String::from_utf8(out).ok()
}

pub fn json(frame: &ServerFrame) -> String {
    serde_json::to_string(frame).expect("server frames serialize")
}

pub fn pair(cursor: Cursor) -> (u64, u64) {
    (cursor.generation, cursor.sequence)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Post {
    pub from: String,
    pub id: u64,
    pub text: String,
}

/// The room's answer: where the post landed, or why it did not.
pub type Posted = Result<(u64, u64), String>;

pub struct RoomApi;

fn codec<T: Serialize>(value: &T) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

impl WireProtocol for RoomApi {
    type Message = Post;
    type Reply = Posted;
    const ID: u64 = 0xC4A7_0001;
    const SCHEMA: u16 = 1;
    fn encode(post: Post) -> Result<Bytes, CallError> {
        codec(&post)
    }
    fn decode_reply(body: Bytes) -> Result<Posted, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

/// What a room handles: a post, and the end of its stream's open.
pub enum RoomMsg {
    Post(Post, Option<Reply<Posted>>),
    Opened(Result<(), StreamError>),
}

impl ActorProtocol for RoomApi {
    type Mailbox = RoomMsg;
    fn encode_reply(posted: Posted) -> Result<Bytes, CallError> {
        codec(&posted)
    }
    fn decode(body: Bytes, reply: Option<Reply<Posted>>) -> Result<Self::Mailbox, Rejection> {
        serde_json::from_slice(&body).map(|post| RoomMsg::Post(post, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}
