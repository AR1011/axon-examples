//! One gate per web node, published as `gate`: it knows each signed-in user's sessions on this node, and closes them
//! when the accounts tell it the user's sign-ins ended (a disable, a new password). As a backstop it asks the
//! accounts for each user's version every minute, once per user. While the accounts cannot be reached that check
//! lets the sessions go on, and counts it: cookies are checked by their signature alone meanwhile.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axon::{Actor, ActorHandle, Context};
use axon_cluster::{ActorProtocol, CallError, NamedRef, Rejection, Reply};
use axon_stream::StreamNode;
use bytes::Bytes;

use crate::core::accounts::User;
use crate::remote::{AccountsAnswer, AccountsP, AccountsReq, GateP, GateReq, WebStats};
use crate::session;

/// How often each user's version is asked again.
pub const CHECK_EVERY: Duration = Duration::from_secs(60);

/// Sessions closed because their user's sign-ins ended; checks that could not reach the accounts.
pub static REVOKED: AtomicU64 = AtomicU64::new(0);
pub static UNCHECKED: AtomicU64 = AtomicU64::new(0);

pub enum Msg {
    Revoke {
        user: String,
        version: u64,
    },
    /// A user's session on this node opened, signed at `version`.
    Open {
        user: String,
        version: u64,
        session: u64,
        handle: ActorHandle<session::Msg>,
    },
    Closed(u64),
    Check,
    /// Each user's account, or `None` where the accounts did not answer.
    Checked(Vec<(String, Option<Option<User>>)>),
    Stats(Option<Reply<WebStats>>),
}

pub struct Gate {
    accounts: NamedRef<AccountsP>,
    streams: StreamNode,
    sessions: BTreeMap<u64, (String, u64, ActorHandle<session::Msg>)>,
    users: BTreeMap<String, BTreeSet<u64>>,
    checking: bool,
}

impl Gate {
    pub fn new(accounts: NamedRef<AccountsP>, streams: StreamNode) -> Self {
        Self { accounts, streams, sessions: BTreeMap::new(), users: BTreeMap::new(), checking: false }
    }

    /// Closes `user`'s sessions signed before `version`, or all of them.
    fn revoke(&mut self, user: &str, version: Option<u64>) {
        for session in self.users.get(user).into_iter().flatten() {
            let (_, signed, handle) = &self.sessions[session];
            if version.is_none_or(|v| *signed < v) {
                REVOKED.fetch_add(1, Ordering::Relaxed);
                drop(handle.tell(session::Msg::Revoked));
            }
        }
    }
}

impl Actor for Gate {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        cx.schedule_every(CHECK_EVERY, || Msg::Check).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Revoke { user, version } => self.revoke(&user, Some(version)),
            Msg::Stats(reply) => {
                let stats = WebStats {
                    sessions: session::OPEN.load(Ordering::Relaxed),
                    users: self.users.len(),
                    streams: self.streams.stats().hub.streams,
                };
                drop(reply.map(|reply| reply.send(stats)));
            }
            Msg::Open { user, version, session, handle } => {
                // Watched until it ends; a session that already ended is told at once.
                // One the gate cannot watch it could not revoke: it ends, and its client joins again.
                match cx.watch(&handle.lifecycle, move |_| Msg::Closed(session)) {
                    Ok(_) => {
                        self.users.entry(user.clone()).or_default().insert(session);
                        self.sessions.insert(session, (user, version, handle));
                    }
                    Err(_) => handle.lifecycle.stop(),
                }
            }
            Msg::Closed(session) => {
                if let Some((user, _, _)) = self.sessions.remove(&session)
                    && let Some(sessions) = self.users.get_mut(&user)
                {
                    sessions.remove(&session);
                    if sessions.is_empty() {
                        self.users.remove(&user);
                    }
                }
            }
            Msg::Check if self.checking || self.users.is_empty() => {}
            Msg::Check => {
                let (accounts, users) = (self.accounts.clone(), self.users.keys().cloned().collect::<Vec<_>>());
                let check = async move {
                    let mut checked = Vec::new();
                    // A few at a time: the accounts serve 64 calls at once for every web node.
                    for chunk in users.chunks(8) {
                        let asks = chunk.iter().map(|name| {
                            let get = AccountsReq::Get { name: name.clone() };
                            let accounts = accounts.clone();
                            async move {
                                match crate::web::ask(&accounts, get).await {
                                    Ok(AccountsAnswer::User { user }) => (name.clone(), Some(user)),
                                    _ => (name.clone(), None),
                                }
                            }
                        });
                        checked.extend(futures_util::future::join_all(asks).await);
                    }
                    Msg::Checked(checked)
                };
                self.checking = cx.submit_future(check).is_ok();
            }
            Msg::Checked(checked) => {
                self.checking = false;
                for (user, account) in checked {
                    match account {
                        None => drop(UNCHECKED.fetch_add(1, Ordering::Relaxed)),
                        Some(Some(account)) if !account.disabled => self.revoke(&user, Some(account.version)),
                        Some(_) => self.revoke(&user, None),
                    }
                }
            }
        }
        Ok(())
    }
}

impl ActorProtocol for GateP {
    type Mailbox = Msg;

    fn encode_reply(stats: WebStats) -> Result<Bytes, CallError> {
        serde_json::to_vec(&stats).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
    }

    fn decode(body: Bytes, reply: Option<Reply<WebStats>>) -> Result<Msg, Rejection> {
        match serde_json::from_slice(&body) {
            Ok(GateReq::Revoke { user, version }) => Ok(Msg::Revoke { user, version }),
            Ok(GateReq::Stats) => Ok(Msg::Stats(reply)),
            Err(_) => Err(Rejection::InvalidMessage),
        }
    }
}
