//! The calls between roles, but a document's: a home's catalog, the accounts and each web node's gate. They are rare
//! next to edits, so their bodies are JSON, pinned in the tests below; `DocP`, on every keystroke, is lib0.
//!
//! The catalog and the accounts are supervised, and a name is published for a plain actor only, so each is served by
//! a [`Front`]: a plain actor that asks whichever generation runs, in a job, and answers the caller.

use std::sync::Arc;

use axon::{Actor, Context, MailboxLimits};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::core::accounts::{Event as AccountsEvent, Listed, User};
use crate::core::catalog::{Row, Tab};

/// The name each role publishes its actor under.
pub const CATALOG: &str = "catalog";
pub const ACCOUNTS: &str = "accounts";
pub const GATE: &str = "gate";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "q", rename_all = "snake_case")]
pub enum CatalogReq {
    /// A user's rows of one tab, newest edit first, after `after` (the last row's edit time and id).
    List {
        user: String,
        tab: Tab,
        after: Option<(u64, String)>,
        limit: usize,
    },
    Create {
        owner: String,
    },
    Hide {
        id: String,
        user: String,
    },
    /// What the home runs: its open documents from `limit` on in id order, its disk and its streams.
    Hosted {
        from: Option<String>,
        limit: usize,
    },
}

/// An open document on a home.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenDoc {
    pub id: String,
    pub title: String,
    pub owner: String,
}

/// A home's answer to `Hosted`, for the admin page and `mdpad cluster`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStats {
    /// Documents running here, listed or not.
    pub open: usize,
    pub docs: Vec<OpenDoc>,
    pub more: bool,
    /// Ids in the catalog.
    pub stored: usize,
    /// Edits answered saved since the process started.
    pub saved: u64,
    pub log_bytes: u64,
    pub log_live_bytes: u64,
    pub log_segments: usize,
    pub failed_writes: u64,
    /// Streams the node carries, the documents' and their presence.
    pub streams: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "a", rename_all = "snake_case")]
pub enum CatalogAnswer {
    Rows {
        rows: Vec<(String, Row)>,
    },
    Created {
        id: String,
    },
    Hidden {
        hidden: bool,
    },
    Hosted(HomeStats),
    /// The catalog's actor did not answer: between two generations, or stopped.
    Unavailable {
        why: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "q", rename_all = "snake_case")]
pub enum AccountsReq {
    Get { name: String },
    List,
    Record { event: AccountsEvent },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "a", rename_all = "snake_case")]
pub enum AccountsAnswer {
    User { user: Option<User> },
    Users { users: Vec<Listed> },
    Recorded { result: Result<(), String> },
    Unavailable { why: String },
}

/// Told to every web node's gate when a user's sign-ins end: a disable, or a new password. `Stats` asks for the
/// node's sessions, for the admin page and `mdpad cluster`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "q", rename_all = "snake_case")]
pub enum GateReq {
    Revoke { user: String, version: u64 },
    Stats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebStats {
    /// Sockets open, guests' too.
    pub sessions: u64,
    /// Signed-in users with a socket here.
    pub users: usize,
    /// Streams the node relays.
    pub streams: usize,
}

fn encode(value: &impl Serialize) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CallError> {
    serde_json::from_slice(body).map_err(|e| CallError::Codec(e.to_string()))
}

macro_rules! json_protocol {
    ($name:ident, $message:ty, $reply:ty, $id:expr, $schema:expr) => {
        pub struct $name;

        impl WireProtocol for $name {
            type Message = $message;
            type Reply = $reply;
            const ID: u64 = $id;
            const SCHEMA: u16 = $schema;

            fn encode(message: $message) -> Result<Bytes, CallError> {
                encode(&message)
            }

            fn decode_reply(body: Bytes) -> Result<$reply, CallError> {
                decode(&body)
            }
        }
    };
}

// 2: `Hosted`. Accounts 2: operators, and the list's rows. Gate 2: `Stats`.
json_protocol!(CatalogP, CatalogReq, CatalogAnswer, 0x4D44_0002, 2);
json_protocol!(AccountsP, AccountsReq, AccountsAnswer, 0x4D44_0003, 2);
json_protocol!(GateP, GateReq, WebStats, 0x4D44_0004, 2);

/// A call's answer, made off the actor's thread.
pub type Serve<Q, A> = Arc<dyn Fn(Q) -> BoxFuture<'static, A> + Send + Sync>;

/// Serves a published name with `serve`, one job a call.
pub struct Front<P: WireProtocol> {
    serve: Serve<P::Message, P::Reply>,
}

pub enum Call<P: WireProtocol> {
    In(P::Message, Option<Reply<P::Reply>>),
    Done,
}

impl<P: WireProtocol> Front<P> {
    pub fn new(serve: Serve<P::Message, P::Reply>) -> Self {
        Self { serve }
    }

    /// Calls at once: past it a call is dropped and its caller gets `NoReply`, which a web node's sign-in check takes
    /// as the accounts not answering (`web::FAIL_OPEN`).
    pub fn limits() -> MailboxLimits {
        MailboxLimits { jobs: 64, ..MailboxLimits::default() }
    }
}

impl<P: WireProtocol> Actor for Front<P>
where
    P::Message: Send,
    P::Reply: Send,
{
    type Message = Call<P>;
    type Error = String;

    fn on_message(&mut self, msg: Call<P>, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if let Call::In(q, reply) = msg {
            let answer = (self.serve)(q);
            // A refused job drops the reply, which answers the caller `NoReply`.
            drop(cx.submit_future(async move {
                let answer = answer.await;
                if let Some(reply) = reply {
                    drop(reply.send(answer));
                }
                Call::Done
            }));
        }
        Ok(())
    }
}

macro_rules! front_protocol {
    ($name:ident) => {
        impl ActorProtocol for $name {
            type Mailbox = Call<$name>;

            fn encode_reply(answer: <$name as WireProtocol>::Reply) -> Result<Bytes, CallError> {
                encode(&answer)
            }

            fn decode(
                body: Bytes,
                reply: Option<Reply<<$name as WireProtocol>::Reply>>,
            ) -> Result<Call<$name>, Rejection> {
                serde_json::from_slice(&body).map(|q| Call::In(q, reply)).map_err(|_| Rejection::InvalidMessage)
            }
        }
    };
}

front_protocol!(CatalogP);
front_protocol!(AccountsP);

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes another node of this version reads: change them only with `SCHEMA`.
    #[test]
    fn calls_are_pinned() {
        let list = CatalogReq::List { user: "ana".into(), tab: Tab::Shared, after: Some((5, "h1-x".into())), limit: 2 };
        assert_eq!(
            CatalogP::encode(list.clone()).unwrap().as_ref(),
            br#"{"q":"list","user":"ana","tab":"shared","after":[5,"h1-x"],"limit":2}"#
        );
        assert_eq!(serde_json::from_slice::<CatalogReq>(&encode(&list).unwrap()).unwrap(), list);
        assert_eq!(
            encode(&CatalogAnswer::Created { id: "h2-a".into() }).unwrap().as_ref(),
            br#"{"a":"created","id":"h2-a"}"#
        );
        let get = AccountsReq::Get { name: "ana".into() };
        assert_eq!(AccountsP::encode(get).unwrap().as_ref(), br#"{"q":"get","name":"ana"}"#);
        let disable = AccountsReq::Record { event: AccountsEvent::Disable { name: "ana".into() } };
        assert_eq!(
            AccountsP::encode(disable).unwrap().as_ref(),
            br#"{"q":"record","event":{"e":"disable","name":"ana"}}"#
        );
        let user =
            AccountsAnswer::User { user: Some(User { hash: "h".into(), disabled: false, version: 3, operator: true }) };
        assert_eq!(AccountsP::decode_reply(encode(&user).unwrap()).unwrap(), user);
        let revoke = GateReq::Revoke { user: "ana".into(), version: 4 };
        assert_eq!(GateP::encode(revoke).unwrap().as_ref(), br#"{"q":"revoke","user":"ana","version":4}"#);
        assert_eq!(GateP::encode(GateReq::Stats).unwrap().as_ref(), br#"{"q":"stats"}"#);
        let hosted = CatalogReq::Hosted { from: None, limit: 200 };
        assert_eq!(CatalogP::encode(hosted).unwrap().as_ref(), br#"{"q":"hosted","from":null,"limit":200}"#);
        let stats = CatalogAnswer::Hosted(HomeStats { open: 1, ..HomeStats::default() });
        assert_eq!(CatalogP::decode_reply(encode(&stats).unwrap()).unwrap(), stats);
        let listed =
            AccountsAnswer::Users { users: vec![Listed { name: "ana".into(), disabled: false, operator: true }] };
        assert_eq!(
            encode(&listed).unwrap().as_ref(),
            br#"{"a":"users","users":[{"name":"ana","disabled":false,"operator":true}]}"#
        );
        assert!(CatalogP::decode_reply(Bytes::from_static(b"{}")).is_err());
    }
}
