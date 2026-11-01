//! One `Account` actor per account id, started by the cluster on the key's owner. Its disk work runs as jobs, one
//! write at a time: the commands of a turn are decided against the state, written as one group, and answered once
//! the write is durable. A transfer debits here, then credits the other account from a job, and answers once the
//! credit is settled or refunded; one left unsettled by a crash is sent again by whoever recovers the account.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::{Actor, Context, StopReason, SubmitError, TaskAdmissionError};
use axon_cluster::{ActorProtocol, CallError, KeyedRefs, Rejection, Reply, WireProtocol};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::store::{self, Event, Log, Outcome, State, StoreError};

pub const KIND: &str = "account";
const MAX_QUEUED: usize = 1024;
const MAX_CREDITS: usize = 8;
const MAX_AMOUNT: u64 = 1 << 40;

/// Writes and recoveries in this process refused because another owner had taken the account over. Counted in the
/// job: an owner woken past its lease is stopped by its fence before it sees the refusal.
pub static FENCED: AtomicU64 = AtomicU64::new(0);

/// Accounts this process runs: recovered, and not stopped since.
pub static LIVE: AtomicU64 = AtomicU64::new(0);

fn counted<T>(done: Result<T, StoreError>) -> Result<T, StoreError> {
    if matches!(done, Err(StoreError::Fenced)) {
        FENCED.fetch_add(1, Relaxed);
    }
    done
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Command {
    Open {
        key: String,
    },
    Deposit {
        key: String,
        amount: u64,
    },
    Withdraw {
        key: String,
        amount: u64,
    },
    /// `tid` is the idempotency key here and, with this account's id, at `to`.
    Transfer {
        tid: String,
        to: String,
        amount: u64,
    },
    /// From the source account of a transfer to its destination.
    Credit {
        from: String,
        tid: String,
        amount: u64,
    },
    Balance,
}

/// `balance` and `pending` (transfers debited and not yet settled) are as durable as the outcome.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answer {
    pub outcome: Outcome,
    pub balance: i64,
    pub pending: usize,
}

pub fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl Command {
    fn valid(&self) -> bool {
        let key = |k: &String| (1..=128).contains(&k.len());
        match self {
            Command::Open { key: k } => key(k),
            Command::Deposit { key: k, amount } | Command::Withdraw { key: k, amount } => {
                key(k) && *amount <= MAX_AMOUNT
            }
            Command::Transfer { tid, to, amount } => key(tid) && valid_id(to) && *amount <= MAX_AMOUNT,
            Command::Credit { from, tid, amount } => key(tid) && valid_id(from) && *amount <= MAX_AMOUNT,
            Command::Balance => true,
        }
    }
}

pub struct AccountApi;

impl WireProtocol for AccountApi {
    type Message = Command;
    type Reply = Answer;
    const ID: u64 = 0x1ED6;
    const SCHEMA: u16 = 1;
    fn encode(command: Command) -> Result<Bytes, CallError> {
        serde_json::to_vec(&command).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = Msg;
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        serde_json::to_vec(&answer).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        match serde_json::from_slice::<Command>(&body) {
            Ok(command) if command.valid() => Ok(Msg::Call(command, reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

pub enum Msg {
    Call(Command, Option<Reply<Answer>>),
    Recovered(Result<(Log, State), StoreError>),
    Written(Log, Result<(), StoreError>),
    Credited(String, Result<Answer, String>),
    Pump,
}

enum Work {
    Call(Command, Option<Reply<Answer>>),
    /// The destination's answer to a credit: settle or refund.
    Credited(String, Outcome),
}

/// What the write in flight releases once it is durable.
enum After {
    Reply(Reply<Answer>, Outcome),
    Settled(String, Outcome),
}

pub struct Account {
    id: String,
    epoch: u64,
    dir: PathBuf,
    every: usize,
    /// The accounts this one credits, shared by every account on the node.
    accounts: KeyedRefs<AccountApi>,
    state: State,
    /// `None` while a job holds it: recovering, or a write in flight.
    log: Option<Log>,
    queue: VecDeque<Work>,
    after: Vec<After>,
    waiting: HashMap<String, Vec<Reply<Answer>>>,
    crediting: HashSet<String>,
    pump_due: bool,
    live: bool,
}

impl Account {
    pub fn new(id: String, epoch: u64, dir: PathBuf, every: usize, accounts: KeyedRefs<AccountApi>) -> Self {
        Account {
            id,
            epoch,
            dir,
            every,
            accounts,
            state: State::default(),
            log: None,
            queue: VecDeque::new(),
            after: Vec::new(),
            waiting: HashMap::new(),
            crediting: HashSet::new(),
            pump_due: false,
            live: false,
        }
    }

    fn decide(&self, command: &Command) -> (Option<Event>, Outcome) {
        let s = &self.state;
        let refuse = |key: &str, why| (Some(Event::Refused(key.to_owned(), why)), why);
        let seen = |key: &str| s.done.get(key).map(|o| (None, *o));
        match command {
            Command::Balance => (None, Outcome::Ok),
            Command::Open { key } => seen(key).unwrap_or_else(|| match s.open {
                true => refuse(key, Outcome::Exists),
                false => (Some(Event::Open(key.clone())), Outcome::Ok),
            }),
            Command::Deposit { key, amount } => seen(key).unwrap_or_else(|| match s.open {
                false => refuse(key, Outcome::Closed),
                true => (Some(Event::Deposit(key.clone(), *amount)), Outcome::Ok),
            }),
            Command::Withdraw { key, amount } => seen(key).unwrap_or_else(|| {
                if !s.open {
                    refuse(key, Outcome::Closed)
                } else if *amount as i64 > s.balance {
                    refuse(key, Outcome::Overdraft)
                } else {
                    (Some(Event::Withdraw(key.clone(), *amount)), Outcome::Ok)
                }
            }),
            Command::Transfer { tid, to, amount } => seen(tid).unwrap_or_else(|| {
                if !s.open {
                    refuse(tid, Outcome::Closed)
                } else if *to == self.id {
                    refuse(tid, Outcome::Invalid)
                } else if *amount as i64 > s.balance {
                    refuse(tid, Outcome::Overdraft)
                } else {
                    (Some(Event::Out { tid: tid.clone(), to: to.clone(), amount: *amount }), Outcome::Pending)
                }
            }),
            Command::Credit { from, tid, amount } => match s.ins.get(&format!("{from}/{tid}")) {
                Some(true) => (None, Outcome::Ok),
                Some(false) => (None, Outcome::NoAccount),
                None if !s.open => (Some(Event::Reject { from: from.clone(), tid: tid.clone() }), Outcome::NoAccount),
                None => (Some(Event::In { from: from.clone(), tid: tid.clone(), amount: *amount }), Outcome::Ok),
            },
        }
    }

    fn answer(&self, outcome: Outcome) -> Answer {
        Answer { outcome, balance: self.state.balance, pending: self.state.outs.len() }
    }

    fn release(&mut self) {
        for after in std::mem::take(&mut self.after) {
            match after {
                After::Reply(reply, outcome) => drop(reply.send(self.answer(outcome))),
                After::Settled(tid, outcome) => {
                    for reply in self.waiting.remove(&tid).unwrap_or_default() {
                        let _ = reply.send(self.answer(outcome));
                    }
                }
            }
        }
    }

    /// Decides everything queued and writes it as one group. Only called with no write in flight.
    fn commit(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let mut events = Vec::new();
        while let Some(work) = self.queue.pop_front() {
            let (event, after) = match work {
                Work::Call(command, reply) => {
                    let (event, outcome) = self.decide(&command);
                    let after = match (reply, &command) {
                        (Some(reply), Command::Transfer { tid, .. }) if outcome == Outcome::Pending => {
                            self.waiting.entry(tid.clone()).or_default().push(reply);
                            None
                        }
                        (reply, _) => reply.map(|reply| After::Reply(reply, outcome)),
                    };
                    (event, after)
                }
                Work::Credited(tid, outcome) if self.state.outs.contains_key(&tid) => {
                    let (event, outcome) = match outcome {
                        Outcome::Ok => (Event::Settled(tid.clone()), Outcome::Ok),
                        _ => (Event::Refund(tid.clone()), Outcome::NoAccount),
                    };
                    (Some(event), Some(After::Settled(tid, outcome)))
                }
                Work::Credited(..) => (None, None),
            };
            if let Some(event) = event {
                self.state.apply(&event);
                events.push(event);
            }
            self.after.extend(after);
        }
        if events.is_empty() {
            self.release();
            return Ok(());
        }
        let Some(mut log) = self.log.take() else { return Err("commit with a write in flight".into()) };
        let snapshot = log.due(events.len()).then(|| self.state.clone());
        let job = cx.submit_io(move || {
            let written = counted(log.append(&events, snapshot.as_ref()));
            Msg::Written(log, written)
        });
        match job {
            Ok(_) => Ok(()),
            // Handing off to another node: whatever it did not write, its new owner is asked again.
            Err(SubmitError { reason: TaskAdmissionError::Draining, .. }) => {
                cx.stop();
                Ok(())
            }
            Err(e) => Err(format!("write refused: {e:?}")),
        }
    }

    /// Sends a credit for every durable, unsettled transfer that has none in flight.
    fn pump(&mut self, cx: &mut Context<'_, Self>) {
        let due: Vec<_> = self
            .state
            .outs
            .iter()
            .filter(|(tid, _)| !self.crediting.contains(*tid))
            .take(MAX_CREDITS.saturating_sub(self.crediting.len()))
            .map(|(tid, (to, amount))| (tid.clone(), to.clone(), *amount))
            .collect();
        for (tid, to, amount) in due {
            let credit = Command::Credit { from: self.id.clone(), tid: tid.clone(), amount };
            let id = tid.clone();
            let dest = self.accounts.get(&to, Bytes::new());
            let job = cx
                .submit_future(async move { Msg::Credited(id, dest.request(credit).await.map_err(|e| e.to_string())) });
            if job.is_err() {
                self.retry_later(cx);
                return;
            }
            self.crediting.insert(tid);
        }
    }

    /// Fails the actor; its replies are dropped, so their callers retry wherever the account runs next.
    fn fail(&self, e: StoreError) -> String {
        format!("{} at epoch {}: {e}", self.id, self.epoch)
    }

    fn retry_later(&mut self, cx: &mut Context<'_, Self>) {
        if !self.pump_due && cx.schedule_after(Duration::from_millis(100), Msg::Pump).is_ok() {
            self.pump_due = true;
        }
    }
}

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (dir, id, epoch, every) = (self.dir.clone(), self.id.clone(), self.epoch, self.every);
        cx.submit_io(move || Msg::Recovered(counted(store::recover(&dir, &id, epoch, every))))
            .map_err(|e| format!("recovery refused: {e:?}"))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            // Over the limit the reply is dropped: the caller sees `NoReply` and retries.
            Msg::Call(command, reply) if self.queue.len() < MAX_QUEUED => {
                self.queue.push_back(Work::Call(command, reply))
            }
            Msg::Call(..) => {}
            Msg::Recovered(recovered) => {
                let (log, state) = recovered.map_err(|e| self.fail(e))?;
                (self.log, self.state, self.live) = (Some(log), state, true);
                LIVE.fetch_add(1, Relaxed);
            }
            Msg::Written(log, written) => {
                written.map_err(|e| self.fail(e))?;
                self.log = Some(log);
                self.release();
            }
            Msg::Credited(tid, answer) => {
                self.crediting.remove(&tid);
                match answer {
                    Ok(answer) if answer.outcome != Outcome::Pending => {
                        self.queue.push_back(Work::Credited(tid, answer.outcome))
                    }
                    _ => self.retry_later(cx),
                }
            }
            Msg::Pump => self.pump_due = false,
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.log.is_none() {
            return Ok(()); // the job that holds it ends the wait with a message, and so another turn
        }
        self.pump(cx); // before commit: only what is already durable is credited
        self.commit(cx)
    }

    fn on_stop(&mut self, _: &StopReason) {
        if self.live {
            LIVE.fetch_sub(1, Relaxed);
        }
    }
}
