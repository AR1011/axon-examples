//! The actors and their wire contracts: a keyed actor per pool of each service, and an eager keyed actor per saga.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use axon::{Actor, Context, RestartPolicy};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterError, KeyedRef, Placement, Rejection, Reply, Retry, SpawnContext,
    WeakCluster, WireProtocol,
};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::core::{Action, Answer, Fault, Order, Outcome, Phase, Pool, SERVICES, Step};
use crate::store::{SagaRow, Store, StoreError};

pub const SAGA: &str = "saga";

/// How long a saga keeps repeating a step whose outcome it does not know before it compensates. Longer than a
/// node's keys take to move after it dies (the 6 s lease and the retries around it).
pub const STEP_DEADLINE: Duration = Duration::from_secs(30);
/// At most this many callers wait on one saga's end; later ones get its phase at once.
const MAX_WAITERS: usize = 16;

/// `err.retry()`, except that `Closed`, this node shutting down, is not a reason to compensate: the saga's next
/// owner does the step.
pub fn retry(err: &ClusterError) -> Retry {
    match err {
        ClusterError::Call(CallError::Closed) => Retry::Later,
        e => e.retry(),
    }
}

fn json<T: Serialize>(value: &T) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

fn unjson<T: DeserializeOwned>(body: &[u8]) -> Result<T, CallError> {
    serde_json::from_slice(body).map_err(|e| CallError::Codec(e.to_string()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PoolCall {
    Hold { saga: String, amount: u64 },
    Release { saga: String },
}

/// Inventory, payment and shipping share one contract: each holds parts of its pools for sagas.
pub struct PoolApi;

impl WireProtocol for PoolApi {
    type Message = PoolCall;
    type Reply = Answer;
    const ID: u64 = 0x5A6A_0001;
    const SCHEMA: u16 = 1;
    fn encode(call: PoolCall) -> Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        unjson(&body)
    }
}

impl ActorProtocol for PoolApi {
    type Mailbox = PoolMsg;
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<PoolMsg, Rejection> {
        unjson(&body).map(|call| PoolMsg::Call(call, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SagaCall {
    /// Stores the saga, the first time, and answers with its phase. The key is the order's id.
    Submit(Order),
    /// Answers once the saga has ended, or with its phase when too many already wait.
    Wait,
}

/// The saga's stored phase, or why there is none: an order sent to another order's key, or a wait on a saga
/// never submitted.
pub type SagaAnswer = Result<Phase, String>;

pub struct SagaApi;

impl WireProtocol for SagaApi {
    type Message = SagaCall;
    type Reply = SagaAnswer;
    const ID: u64 = 0x5A6A_0002;
    const SCHEMA: u16 = 2;
    fn encode(call: SagaCall) -> Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> Result<SagaAnswer, CallError> {
        unjson(&body)
    }
}

impl ActorProtocol for SagaApi {
    type Mailbox = SagaMsg;
    fn encode_reply(answer: SagaAnswer) -> Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<SagaAnswer>>) -> Result<SagaMsg, Rejection> {
        match unjson(&body) {
            Ok(SagaCall::Submit(order)) if order.validate().is_err() => Err(Rejection::InvalidMessage),
            Ok(call) => Ok(SagaMsg::Call(call, reply)),
            Err(_) => Err(Rejection::InvalidMessage),
        }
    }
}

pub enum PoolMsg {
    Call(PoolCall, Option<Reply<Answer>>),
    Loaded(Result<Option<Pool>, StoreError>),
    Committed(Result<(), StoreError>),
}

/// One pool of one service. Answers go out only once what they report is in the database; calls that arrive
/// while a commit runs are committed together after it.
struct PoolActor {
    service: &'static str,
    key: String,
    epoch: u64,
    store: Store,
    fault: Fault,
    /// `None`: the service has no such pool, and refuses every hold on it.
    pool: Option<Pool>,
    dirty: BTreeSet<String>,
    queued: Vec<(Reply<Answer>, Answer)>,
    committing: Option<Vec<(Reply<Answer>, Answer)>>,
}

impl PoolActor {
    fn flush(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.committing.is_some() {
            return Ok(());
        }
        let Some(pool) = self.pool.as_ref().filter(|_| !self.dirty.is_empty()) else {
            // Nothing changed since the last commit: what these answers report is stored already.
            for (reply, answer) in self.queued.drain(..) {
                drop(reply.send(answer));
            }
            return Ok(());
        };
        let holds: Vec<_> = std::mem::take(&mut self.dirty).into_iter().map(|s| (s.clone(), pool.holds[&s])).collect();
        let (store, service, key, epoch, available) =
            (self.store.clone(), self.service, self.key.clone(), self.epoch, pool.available);
        cx.submit_io(move || PoolMsg::Committed(store.commit_pool(service, &key, epoch, available, &holds)))
            .map_err(|e| format!("{e:?}"))?;
        self.committing = Some(std::mem::take(&mut self.queued));
        Ok(())
    }
}

impl Actor for PoolActor {
    type Message = PoolMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (store, service, key, epoch) = (self.store.clone(), self.service, self.key.clone(), self.epoch);
        let load = cx
            .submit_io(move || PoolMsg::Loaded(store.load_pool(service, &key, epoch)))
            .map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&load).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: PoolMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            PoolMsg::Loaded(loaded) => self.pool = loaded.map_err(|e| e.to_string())?,
            PoolMsg::Call(call, reply) => {
                let (answer, changed, saga) = match (&mut self.pool, call) {
                    (None, PoolCall::Hold { saga, .. }) => {
                        (Answer::Refused(format!("no {} {}", self.service, self.key)), false, saga)
                    }
                    (None, PoolCall::Release { saga }) => (Answer::Applied, false, saga),
                    (Some(pool), PoolCall::Hold { saga, amount }) => {
                        let (answer, changed) = pool.hold(&saga, amount, self.fault.refuses(self.service, &saga));
                        (answer, changed, saga)
                    }
                    (Some(pool), PoolCall::Release { saga }) => {
                        let (answer, changed) = pool.release(&saga);
                        (answer, changed, saga)
                    }
                };
                if changed {
                    self.dirty.insert(saga);
                }
                if let Some(reply) = reply {
                    self.queued.push((reply, answer));
                }
                self.flush(cx)?;
            }
            PoolMsg::Committed(done) => {
                // A failed commit drops its answers: their callers see `NoReply` and repeat, and the restarted
                // actor answers them from what the database holds.
                done.map_err(|e| e.to_string())?;
                for (reply, answer) in self.committing.take().into_iter().flatten() {
                    drop(reply.send(answer));
                }
                self.flush(cx)?;
            }
        }
        Ok(())
    }

    fn label(msg: &PoolMsg) -> &'static str {
        match msg {
            PoolMsg::Call(PoolCall::Hold { .. }, _) => "hold",
            PoolMsg::Call(PoolCall::Release { .. }, _) => "release",
            PoolMsg::Loaded(_) => "loaded",
            PoolMsg::Committed(_) => "committed",
        }
    }
}

pub enum SagaMsg {
    Call(SagaCall, Option<Reply<SagaAnswer>>),
    Loaded(Result<Option<SagaRow>, StoreError>),
    Created(Result<(), StoreError>, Option<Reply<SagaAnswer>>),
    Answered(Result<Answer, (Retry, String)>),
    Saved(Result<(), StoreError>),
    Retry,
}

/// One order's saga. It does one action at a time and stores each new phase before the next action, so a saga
/// taken over by another node repeats at most the action it was in, which every service answers as before. It
/// loads its order by its key, so it starts the same whether a submit or the cluster's eager restart started it.
struct SagaActor {
    id: String,
    /// Empty until the saga is loaded or submitted.
    steps: Vec<Step>,
    epoch: u64,
    store: Store,
    cluster: WeakCluster,
    phase: Option<Phase>,
    /// The phase the database holds. Callers are answered with this one: a phase not yet saved may be lost.
    saved: Option<Phase>,
    refs: BTreeMap<usize, KeyedRef<PoolApi>>,
    waiters: Vec<Reply<SagaAnswer>>,
    busy: bool,
    tries: u32,
    since: Option<Instant>,
}

impl SagaActor {
    fn drive(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(phase) = self.phase.as_ref().filter(|_| !self.busy) else { return Ok(()) };
        let (index, call) = match phase.action() {
            None => {
                for waiter in self.waiters.drain(..) {
                    drop(waiter.send(Ok(phase.clone())));
                }
                return Ok(());
            }
            Some(Action::Do(i)) => (i, PoolCall::Hold { saga: self.id.clone(), amount: self.steps[i].amount }),
            Some(Action::Undo(i)) => (i, PoolCall::Release { saga: self.id.clone() }),
        };
        let target = match self.refs.get(&index) {
            Some(target) => target.clone(),
            None => {
                let Some(cluster) = self.cluster.upgrade() else { return Ok(()) }; // shutting down
                let step = &self.steps[index];
                let target = cluster.keyed::<PoolApi>(step.service, &step.key, Bytes::new(), &Placement::default());
                self.refs.insert(index, target.clone());
                target
            }
        };
        self.since.get_or_insert_with(Instant::now);
        self.busy = true;
        cx.submit_future(async move {
            let answer = target.request(call).await.map_err(|e| match e {
                // This node is shutting down: the saga's next owner does the step again.
                ClusterError::Closed => (Retry::Later, e.to_string()),
                e => (retry(&e), e.to_string()),
            });
            SagaMsg::Answered(answer)
        })
        .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    /// The saga as stored, its steps already known.
    fn known(&mut self, phase: Phase, cx: &mut Context<'_, Self>) -> Result<(), String> {
        (self.phase, self.saved) = (Some(phase.clone()), Some(phase));
        self.drive(cx)
    }

    /// Stores a submitted saga; calls behind it wait until it is.
    fn create(
        &mut self,
        order: Order,
        reply: Option<Reply<SagaAnswer>>,
        cx: &mut Context<'_, Self>,
    ) -> Result<(), String> {
        self.steps = order.steps();
        let (store, epoch) = (self.store.clone(), self.epoch);
        let create = cx
            .submit_io(move || SagaMsg::Created(store.create_saga(&order, epoch), reply))
            .map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&create).map_err(|e| format!("{e:?}"))
    }

    fn enter(&mut self, next: Phase, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (store, id, epoch) = (self.store.clone(), self.id.clone(), self.epoch);
        let json = next.clone();
        cx.submit_io(move || SagaMsg::Saved(store.save_saga(&id, epoch, &json))).map_err(|e| format!("{e:?}"))?;
        self.phase = Some(next);
        self.busy = true;
        (self.tries, self.since) = (0, None);
        Ok(())
    }
}

impl Actor for SagaActor {
    type Message = SagaMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (store, id, epoch) = (self.store.clone(), self.id.clone(), self.epoch);
        let load = cx.submit_io(move || SagaMsg::Loaded(store.load_saga(&id, epoch))).map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&load).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: SagaMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            SagaMsg::Loaded(row) => {
                if let Some(row) = row.map_err(|e| e.to_string())? {
                    self.steps = row.order.steps();
                    self.known(row.phase, cx)?;
                }
            }
            SagaMsg::Created(created, reply) => {
                // A failed insert drops the reply: the caller sees `NoReply` and submits again.
                created.map_err(|e| e.to_string())?;
                drop(reply.map(|reply| reply.send(Ok(Phase::Forward(0)))));
                self.known(Phase::Forward(0), cx)?;
            }
            SagaMsg::Call(SagaCall::Submit(order), reply) if order.id != self.id => {
                drop(reply.map(|reply| reply.send(Err(format!("order {} sent to saga {}", order.id, self.id)))));
            }
            SagaMsg::Call(SagaCall::Submit(order), reply) if self.saved.is_none() => self.create(order, reply, cx)?,
            SagaMsg::Call(call, reply) => {
                if let Some(reply) = reply {
                    match (&self.saved, call) {
                        (None, _) => drop(reply.send(Err(format!("no saga {}", self.id)))),
                        (Some(phase), SagaCall::Wait) if !phase.terminal() && self.waiters.len() < MAX_WAITERS => {
                            self.waiters.push(reply);
                        }
                        (Some(phase), _) => drop(reply.send(Ok(phase.clone()))),
                    }
                }
                if self.saved.as_ref().is_none_or(Phase::terminal) {
                    // A call started an ended saga, or one never submitted: nothing to run, so the key is given
                    // up again, as an ended saga's is.
                    cx.stop();
                }
            }
            SagaMsg::Answered(answer) => {
                self.busy = false;
                let phase = self.phase.clone().expect("loaded before any action");
                let forward = matches!(phase, Phase::Forward(_));
                let outcome = match answer {
                    Ok(Answer::Applied) => Outcome::Applied,
                    Ok(Answer::Refused(reason)) => Outcome::Refused(reason),
                    Err((Retry::Never, e)) if forward => Outcome::Abandoned(e),
                    Err(_) if forward && self.since.is_some_and(|t| t.elapsed() > STEP_DEADLINE) => {
                        Outcome::Abandoned(format!("no answer for {STEP_DEADLINE:?}"))
                    }
                    // Unknown: do it again. Every service answers a repeat as it answered the first.
                    Err(_) => {
                        let pause = Duration::from_millis(20 << self.tries.min(6));
                        self.tries += 1;
                        cx.schedule_after(pause, SagaMsg::Retry).map_err(|_| "no timer")?;
                        self.busy = true;
                        return Ok(());
                    }
                };
                self.enter(phase.advance(self.steps.len(), outcome), cx)?;
            }
            SagaMsg::Saved(saved) => {
                saved.map_err(|e| e.to_string())?;
                self.saved.clone_from(&self.phase);
                self.busy = false;
                self.drive(cx)?;
                if self.saved.as_ref().is_some_and(Phase::terminal) {
                    // Ended: give the key up, so a dead node's ended sagas are not started again. A later call
                    // starts it from the database and is answered from there.
                    cx.stop();
                }
            }
            SagaMsg::Retry => {
                self.busy = false;
                self.drive(cx)?;
            }
        }
        Ok(())
    }

    fn label(msg: &SagaMsg) -> &'static str {
        match msg {
            SagaMsg::Call(SagaCall::Submit(_), _) => "submit",
            SagaMsg::Call(SagaCall::Wait, _) => "wait",
            SagaMsg::Loaded(_) => "loaded",
            SagaMsg::Created(..) => "created",
            SagaMsg::Answered(_) => "answered",
            SagaMsg::Saved(_) => "saved",
            SagaMsg::Retry => "retry",
        }
    }
}

fn policy() -> RestartPolicy {
    RestartPolicy { max_restarts: 10, ..RestartPolicy::default() }
}

/// Registers the saga kind and the three services on this node. Every node runs all of it: there is no
/// coordinator. The saga kind is eager: when a node dies, its sagas start again on the others without a caller, so it
/// needs an ownership authority that lists a node's keys (`PaxosLeases`).
pub async fn serve(cluster: &Cluster, store: Store, fault: Fault) -> Result<(), ClusterError> {
    for service in SERVICES {
        let store = store.clone();
        cluster
            .kind::<PoolApi>(service)
            .policy(policy())
            .register(move |cx: &SpawnContext, _args: Bytes| {
                Ok(PoolActor {
                    service,
                    key: cx.key.clone().ok_or("a pool has a key")?,
                    epoch: cx.epoch.unwrap_or(0),
                    store: store.clone(),
                    fault,
                    pool: None,
                    dirty: BTreeSet::new(),
                    queued: Vec::new(),
                    committing: None,
                })
            })
            .await?;
    }
    cluster
        .kind::<SagaApi>(SAGA)
        .policy(policy())
        // Every saga starts from no arguments: it loads its order by its key.
        .eager(Bytes::new(), Placement::default())
        .register(move |cx: &SpawnContext, _args: Bytes| {
            Ok(SagaActor {
                id: cx.key.clone().ok_or("a saga has a key")?,
                steps: Vec::new(),
                epoch: cx.epoch.unwrap_or(0),
                store: store.clone(),
                cluster: cx.cluster.clone(),
                phase: None,
                saved: None,
                refs: BTreeMap::new(),
                waiters: Vec::new(),
                busy: false,
                tries: 0,
                since: None,
            })
        })
        .await
}
