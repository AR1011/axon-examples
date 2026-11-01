//! The calls a run takes, from the api and the waker, and their bytes in postcard.

use axon::ReplyTo;
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::actor::RunMsg;
use super::view::RunView;
use crate::journal::entry::{decode, encode};
use crate::workflow::Failure;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunCall {
    /// Starts the run, or answers `created: false` for a start of the same workflow and input.
    Start {
        workflow: String,
        /// `None`: the highest this build has.
        version: Option<u32>,
        /// In JSON.
        input: Bytes,
        tags: Vec<(String, String)>,
        parent: Option<String>,
        run_timeout_ms: Option<u64>,
    },
    Signal {
        signal_id: String,
        name: String,
        /// In JSON.
        payload: Bytes,
    },
    Cancel {
        reason: String,
    },
    Terminate {
        reason: String,
    },
    Get,
    /// An attempt's result from a worker: the first for a step wins.
    Complete {
        step: u32,
        attempt: u32,
        result: Result<Bytes, Failure>,
    },
    /// A timer may be due.
    Wake,
    /// Worker `worker` starts the attempt: `Accepted`, or a refusal, and then it does not run it.
    Started {
        step: u32,
        attempt: u32,
        worker: String,
    },
    /// The attempt still runs; the answer is `Beat`.
    Heartbeat {
        step: u32,
        attempt: u32,
    },
    /// Worker `worker` gives back offer `round` of the attempt unstarted, as it drains: the run offers it again, and
    /// it does not count as an attempt.
    Released {
        step: u32,
        attempt: u32,
        round: u32,
        worker: String,
    },
    /// Worker node `node` left the cluster: its attempts fail and are tried again. From the engine's own watch.
    WorkerLeft {
        node: String,
    },
    /// How the child run at `step` ended.
    ChildDone {
        step: u32,
        result: Result<Bytes, String>,
    },
    /// An operator's: hold the run until `Resume`.
    Pause,
    /// Go on with a paused or parked run.
    Resume,
    /// Run a run that failed on an activity again from that step (a redrive).
    Retry,
    /// Remove an ended run.
    Delete,
    /// The journal from entry `after` (from 0), at most `limit` entries.
    History {
        after: u64,
        limit: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunAnswer {
    Started {
        version: u32,
        created: bool,
    },
    Signalled {
        duplicate: bool,
    },
    Accepted,
    Run(Box<RunView>),
    Refused {
        code: Code,
        message: String,
    },
    /// A heartbeat's answer: `cancel` when the attempt is no longer wanted.
    Beat {
        cancel: bool,
    },
    /// A page of the journal, each entry in postcard, and where the next page starts.
    History {
        entries: Vec<Bytes>,
        next: Option<u64>,
    },
}

/// Why a run refused a call; the api's error codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Code {
    Invalid,
    VersionMissing,
    NotFound,
    Exists,
    Finished,
    TooLarge,
    Limited,
    Busy,
    Unavailable,
    NotFinished,
    NotFailed,
    NotResumable,
    Paused,
}

impl Code {
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Invalid => "invalid",
            Code::VersionMissing => "version_missing",
            Code::NotFound => "not_found",
            Code::Exists => "exists",
            Code::Finished => "finished",
            Code::TooLarge => "too_large",
            Code::Limited => "limited",
            Code::Busy => "busy",
            Code::Unavailable => "unavailable",
            Code::NotFinished => "not_finished",
            Code::NotFailed => "not_failed",
            Code::NotResumable => "not_resumable",
            Code::Paused => "paused",
        }
    }
}

pub fn refused(code: Code, message: impl Into<String>) -> RunAnswer {
    RunAnswer::Refused { code, message: message.into() }
}

/// Who waits for a call's answer.
pub enum Caller {
    Remote(Reply<RunAnswer>),
    Local(ReplyTo<RunAnswer>),
    None,
}

impl Caller {
    pub fn answer(self, answer: RunAnswer) {
        match self {
            Caller::Remote(reply) => drop(reply.send(answer)),
            Caller::Local(reply) => drop(reply.reply(answer)),
            Caller::None => {}
        }
    }
}

/// The run's kind: one keyed actor per run id.
pub const RUN: &str = "run";

/// A run id: 1 to 128 bytes of `[A-Za-z0-9._:/-]`.
pub fn valid_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'/' | b'-'))
}

pub struct RunApi;

impl WireProtocol for RunApi {
    type Message = RunCall;
    type Reply = RunAnswer;
    const ID: u64 = 0x5746_524e; // "WFRN"
    const SCHEMA: u16 = 3;

    fn encode(call: RunCall) -> Result<Bytes, CallError> {
        Ok(encode(&call))
    }

    fn decode_reply(body: Bytes) -> Result<RunAnswer, CallError> {
        decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for RunApi {
    type Mailbox = RunMsg;

    fn encode_reply(answer: RunAnswer) -> Result<Bytes, CallError> {
        Ok(encode(&answer))
    }

    fn decode(body: Bytes, reply: Option<Reply<RunAnswer>>) -> Result<RunMsg, Rejection> {
        let call = decode(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(RunMsg::Call(call, reply.map_or(Caller::None, Caller::Remote)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_call_reads_back_and_the_bytes_are_pinned() {
        let b = |s: &'static str| Bytes::from_static(s.as_bytes());
        let calls = [
            RunCall::Start {
                workflow: "w".into(),
                version: Some(2),
                input: b("{}"),
                tags: vec![("k".into(), "v".into())],
                parent: None,
                run_timeout_ms: Some(1_000),
            },
            RunCall::Signal { signal_id: "s".into(), name: "n".into(), payload: b("1") },
            RunCall::Cancel { reason: "r".into() },
            RunCall::Terminate { reason: "r".into() },
            RunCall::Get,
            RunCall::Complete { step: 3, attempt: 1, result: Err(Failure::fatal("no")) },
            RunCall::Wake,
            RunCall::Started { step: 3, attempt: 1, worker: "w1".into() },
            RunCall::Heartbeat { step: 3, attempt: 1 },
            RunCall::Released { step: 3, attempt: 1, round: 2, worker: "w1".into() },
            RunCall::WorkerLeft { node: "w1".into() },
            RunCall::ChildDone { step: 4, result: Ok(b("1")) },
            RunCall::Pause,
            RunCall::Resume,
            RunCall::Retry,
            RunCall::Delete,
            RunCall::History { after: 300, limit: 50 },
        ];
        for call in calls {
            let bytes = encode(&call);
            assert_eq!(decode(&bytes), Ok(call));
            assert!(decode::<RunCall>(&bytes[..bytes.len() - 1]).is_err());
        }
        let answers = [
            RunAnswer::Started { version: 1, created: true },
            RunAnswer::Signalled { duplicate: true },
            RunAnswer::Accepted,
            RunAnswer::Run(Box::default()),
            refused(Code::Finished, "done"),
            RunAnswer::Beat { cancel: true },
            RunAnswer::History { entries: vec![b("e")], next: Some(2) },
            refused(Code::Paused, "paused"),
        ];
        for answer in answers {
            let bytes = encode(&answer);
            assert_eq!(decode(&bytes), Ok(answer));
        }
        // Get is variant 4; Started is variant 0, then the version and `created`.
        assert_eq!(encode(&RunCall::Get).as_ref(), [4]);
        assert_eq!(encode(&RunAnswer::Started { version: 2, created: true }).as_ref(), [0, 2, 1]);
        // A worker's calls come after the first seven; a fatal failure is variant 1 of `Failure`.
        assert_eq!(encode(&RunCall::Heartbeat { step: 3, attempt: 1 }).as_ref(), [8, 3, 1]);
        let fatal = RunCall::Complete { step: 3, attempt: 1, result: Err(Failure::fatal("no")) };
        assert_eq!(encode(&fatal).as_ref(), [5, 3, 1, 1, 1, 2, b'n', b'o']);
        // An operator's calls come after the worker's and the child's: History is variant 16, 300 as a varint.
        assert_eq!(encode(&RunCall::History { after: 300, limit: 50 }).as_ref(), [16, 0xac, 2, 50]);
        assert_eq!(encode(&refused(Code::NotResumable, "")).as_ref(), [4, 11, 0]);
    }
}
