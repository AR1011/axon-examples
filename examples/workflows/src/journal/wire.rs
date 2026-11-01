//! The journal's calls between nodes, and their bytes: [`Request`] and [`Answer`] in postcard, as in [`super::entry`].
//! A history node's index answers the api's lists and counts as [`IndexApi`], from the same actor.

use std::sync::Arc;
use std::time::Duration;

use axon_cluster::{ActorProtocol, CallError, Cluster, ClusterError, Rejection, Reply, WireProtocol};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::core::{Answer, Request};
use super::entry::{decode, encode};
use super::index::{Counts, Page, Query};
use super::service::ToJournal;

/// What each history node publishes its `Journal` as.
pub const JOURNAL: &str = "journal";
/// And its index.
pub const INDEX: &str = "index";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexCall {
    /// At most [`MAX_LIST`] runs.
    List(Query),
    Counts,
}

/// The most runs one list answers.
pub const MAX_LIST: usize = 500;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexAnswer {
    Page(Page),
    Counts(Counts),
}

/// The first replica, in the order every node lists them, that answers `call` within `within`: its name and answer;
/// or the last refusal.
pub async fn ask_index(
    cluster: &Cluster,
    replicas: &[Arc<str>],
    call: IndexCall,
    within: Duration,
) -> Result<(Arc<str>, IndexAnswer), String> {
    let mut last = String::from("no history replica is listed");
    for replica in replicas {
        let answer = match cluster.resolve_on::<IndexApi>(replica, INDEX).await {
            Ok(remote) => remote.request_within(call.clone(), within).await.map_err(ClusterError::Call),
            Err(e) => Err(e),
        };
        match answer {
            Ok(answer) => return Ok((replica.clone(), answer)),
            Err(e) => last = format!("{replica}: {e}"),
        }
    }
    Err(last)
}

pub struct IndexApi;

impl WireProtocol for IndexApi {
    type Message = IndexCall;
    type Reply = IndexAnswer;
    const ID: u64 = 0x5746_4958; // "WFIX"
    const SCHEMA: u16 = 1;

    fn encode(call: IndexCall) -> Result<Bytes, CallError> {
        Ok(encode(&call))
    }

    fn decode_reply(body: Bytes) -> Result<IndexAnswer, CallError> {
        decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for IndexApi {
    type Mailbox = ToJournal;

    fn encode_reply(answer: IndexAnswer) -> Result<Bytes, CallError> {
        Ok(encode(&answer))
    }

    fn decode(body: Bytes, reply: Option<Reply<IndexAnswer>>) -> Result<ToJournal, Rejection> {
        decode(&body).map(|c| ToJournal::Index(c, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

pub struct JournalApi;

impl WireProtocol for JournalApi {
    type Message = Request;
    type Reply = Answer;
    const ID: u64 = 0x5746_4a4e; // "WFJN"
    const SCHEMA: u16 = 3;

    fn encode(request: Request) -> Result<Bytes, CallError> {
        Ok(encode(&request))
    }

    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for JournalApi {
    type Mailbox = ToJournal;

    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        Ok(encode(&answer))
    }

    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<ToJournal, Rejection> {
        decode(&body).map(|r| ToJournal::Call(r, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::Summary;

    #[test]
    fn every_call_reads_back_and_the_bytes_are_pinned() {
        let b = |s: &'static str| Bytes::from_static(s.as_bytes());
        let requests = [
            Request::Seal { run: b("r"), epoch: 256, token: 7 },
            Request::Append { run: b("r"), epoch: 256, seq: 2, records: vec![b("a"), b("bc")] },
            Request::Read { run: b("r"), epoch: 256, from: 1, to: 9 },
            Request::Probe { run: b("r") },
            Request::Pages { after: Some(b("q")), limit: 1000 },
            Request::Pages { after: None, limit: 1 },
        ];
        for request in requests {
            let bytes = encode(&request);
            assert_eq!(decode(&bytes), Ok(request));
            assert!(decode::<Request>(&bytes[..bytes.len() - 1]).is_err());
        }
        let answers = [
            Answer::Sealed { epoch: 256, segments: vec![(1, 2), (3, 4)] },
            Answer::Acked { epoch: 1, have: 2 },
            Answer::Gap { epoch: 1, have: 2 },
            Answer::Fenced { epoch: 1, fence: 2 },
            Answer::Refused { epoch: 1, why: "full".into() },
            Answer::Records { epoch: 1, from: 2, records: vec![b("x")] },
            Answer::Probed { segments: vec![], summary: None },
            Answer::Probed { segments: vec![(256, 3)], summary: Some((256, Summary::default())) },
            Answer::Pages { runs: vec![(b("r"), vec![(256, 3)], true)], more: true },
        ];
        for answer in answers {
            let bytes = encode(&answer);
            assert_eq!(decode(&bytes), Ok(answer));
            assert!(decode::<Answer>(&bytes[..bytes.len() - 1]).is_err());
        }
        // Seal is variant 0, then the run's length and bytes, the epoch 0x0102 as a varint, the token.
        assert_eq!(encode(&Request::Seal { run: b("r"), epoch: 0x0102, token: 3 }).as_ref(), [0, 1, b'r', 0x82, 2, 3]);
        assert_eq!(encode(&Answer::Gap { epoch: 1, have: 2 }).as_ref(), [2, 1, 2]);
        // Probed is variant 6, then one segment (2, 1), then no summary.
        assert_eq!(encode(&Answer::Probed { segments: vec![(2, 1)], summary: None }).as_ref(), [6, 1, 2, 1, 0]);
        let list =
            IndexCall::List(Query { status: Some(crate::journal::Status::Failed), limit: 50, ..Query::default() });
        assert_eq!(decode::<IndexCall>(&encode(&list)), Ok(list.clone()));
        // List is variant 0, then the query: no workflow, the status Failed (4), no prefix, tag, since or cursor, 50.
        assert_eq!(encode(&list).as_ref(), [0, 0, 1, 4, 0, 0, 0, 0, 50]);
        let counts = IndexAnswer::Counts(Counts { idle: 3, ..Counts::default() });
        assert_eq!(decode::<IndexAnswer>(&encode(&counts)), Ok(counts));
    }
}
