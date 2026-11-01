//! The order-entry contract, and the market-data records on each symbol's stream.

use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use axon_stream::StreamError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::book::{Command, Fill, Level, Outcome, Price, Qty};

pub const KIND: &str = "book";
pub const MARKET_DATA: &str = "md";

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Call {
    Submit(Command),
    /// The top `depth` levels, once everything accepted before it is journaled.
    Snapshot {
        depth: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// `seq` is the command's place in the symbol's journal.
    Done {
        seq: u64,
        outcome: Outcome,
    },
    Snapshot(Snapshot),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The node the book runs on.
    pub node: String,
    pub seq: u64,
    pub epoch: u64,
    pub digest: u64,
    pub resting: usize,
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
}

/// One record on `md/<symbol>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketData {
    Trade { seq: u64, fill: Fill },
    Level { seq: u64, level: Level },
}

pub struct BookApi;

pub fn json<T: Serialize>(value: &T) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

impl WireProtocol for BookApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0x0B00_C0DE;
    const SCHEMA: u16 = 1;

    fn encode(call: Call) -> Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

/// What the book actor's mailbox holds.
pub enum Msg {
    Call(Call, Option<Reply<Answer>>),
    Loaded(Result<crate::journal::Loaded, String>),
    Opened(Result<(), StreamError>),
    Written(Result<(), String>),
}

impl ActorProtocol for BookApi {
    type Mailbox = Msg;
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        serde_json::from_slice(&body).map(|call| Msg::Call(call, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}
