//! Durable objects: a class of a worker with one instance per id in the whole cluster, and storage of its own.
//!
//! An object is a keyed actor of the kind `objects` on the runners, key `worker/class/id`: axon places it, a
//! lease makes it one, and the lease's epoch fences what it writes (see [`crate::durable`]). The [`Object`] actor
//! takes one request at a time. Each is one job, with the mailbox paused until it ends: open the storage if this
//! actor has not yet, run the class's `fetch` in the engine off the actor's thread, then send the request's writes
//! to the store, and answer only once the store has them. A request that wrote nothing asks the store too, with no
//! writes: an answer leaves only from the actor that is the object still, on a copy that is what is stored. A
//! request that fails, or whose writes are refused, has written nothing. The version is the worker's stable one
//! as the runner serves it when the request starts.
//!
//! Between requests the actor keeps what the object stores, so a `get` inside a request asks nobody. It does not
//! keep the JavaScript instance: the engine gives every request a new one (see the README for what that costs).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorHandle, Context, JobExecutor, MailboxConfig, MailboxLimits};
use axon_cluster::{Cluster, ClusterError, KeyedRefs, NamedRef, Reply, WeakCluster};
use bytes::Bytes;
use tokio::time::Instant;

use crate::durable::Data;
use crate::engine::{
    Engine, Failed, Failure, Host, Request, Response, Scope, StorageAnswer, StorageOp, Usage, guarded, submit,
};
use crate::follower::{self, Serving};
use crate::invoker::{Ending, Run};
use crate::node::runners;
use crate::wire::{
    HEAD_ROOM, Hosted, Invocation, MAX_OBJECT_ID, OBJECTS, ObjectApi, Ran, StoreApi, StoreAsk, StoreOp, StoreTold,
    Told, answer, object_key, split_object_key,
};

#[derive(Clone, Debug)]
pub struct ObjectConfig {
    /// An object nobody called for this long ends, and gives up its lease. Its next request starts it again,
    /// wherever axon places it then.
    pub idle: Duration,
    /// References to objects a runner keeps, each with where its object runs.
    pub references: usize,
}

impl Default for ObjectConfig {
    fn default() -> Self {
        Self { idle: Duration::from_secs(60), references: 4096 }
    }
}

/// A node's way to the objects. A clone shares the references it keeps.
#[derive(Clone)]
pub struct Caller {
    /// Weak: an object's factory holds one, and the cluster holds the factory.
    cluster: WeakCluster,
    /// Each with where its object runs. Where objects run is the runners, the same on every node that asks for
    /// one, as a keyed actor's placement must be.
    references: KeyedRefs<ObjectApi>,
}

impl Caller {
    pub fn new(cluster: &Cluster, references: usize) -> Self {
        Self { cluster: cluster.downgrade(), references: cluster.keyed_refs(OBJECTS, &runners(), references) }
    }

    /// One request to the object, answered by `deadline`. Asked again while the errors say it was not delivered
    /// (`Retry::Later`, `Retry::Resolve`: the object is starting, moving, or its mailbox is full); a request that
    /// may have run is never sent twice, and its error is the caller's.
    ///
    /// Each try waits for the cluster's `request_timeout` at most, and one that timed out is not sent again. The
    /// object reads what is left of the try from its `Reply`: it gives up when its caller does, and stores nothing
    /// after.
    pub async fn call(&self, key: &str, invocation: Invocation, deadline: Instant) -> Result<Ran, String> {
        match self.references.get(key, Bytes::new()).request_until(invocation, deadline).await {
            Ok(Told::Ran(ran)) => Ok(ran),
            Ok(Told::Busy) => Err("the object's runner is busy".into()),
            Ok(Told::NotServed) => Err("the object's worker is not deployed".into()),
            Ok(other) => Err(format!("an answer to another call: {other:?}")),
            Err(e) => Err(format!("the object did not answer: {e}")),
        }
    }

    /// The objects this node hosts.
    pub fn hosted(&self, prefix: &str, from: Option<&str>, limit: usize) -> (Vec<Hosted>, bool) {
        let Some(cluster) = self.cluster.upgrade() else { return (Vec::new(), false) };
        let (keys, more) = cluster.hosted_keys(OBJECTS, prefix, from, limit);
        (keys.into_iter().map(|hosted| Hosted { key: hosted.key, epoch: hosted.epoch }).collect(), more)
    }
}

/// One request's view of its object's storage: reads see its own writes, and the writes leave only with
/// [`Txn::finish`].
pub struct Txn(Mutex<Work>);

struct Work {
    data: Arc<Data>,
    ops: BTreeMap<String, Option<String>>,
}

impl Txn {
    pub fn new(data: Arc<Data>) -> Self {
        Self(Mutex::new(Work { data, ops: BTreeMap::new() }))
    }

    fn run(&self, op: StorageOp) -> Result<StorageAnswer, String> {
        let mut work = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Work { data, ops } = &mut *work;
        Ok(match op {
            StorageOp::Get { key } => StorageAnswer::Value(data.get(&key).cloned()),
            StorageOp::Put { key, value } => {
                Arc::make_mut(data).put(&key, value.clone())?;
                ops.insert(key, Some(value));
                StorageAnswer::Done
            }
            StorageOp::Delete { key } => {
                let deleted = Arc::make_mut(data).delete(&key);
                if deleted {
                    ops.insert(key, None);
                }
                StorageAnswer::Deleted(deleted)
            }
            StorageOp::List { prefix, limit } => StorageAnswer::Entries(data.list(&prefix, limit)),
        })
    }

    /// The storage as the request left it, and the writes that make the store's the same: the last per key.
    pub fn finish(&self) -> (Arc<Data>, Vec<StoreOp>) {
        let mut work = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (work.data.clone(), std::mem::take(&mut work.ops).into_iter().collect())
    }
}

/// The time a request sent with no deadline is given: the cluster's default `request_timeout`, as a caller's `request` waits.
pub const UNTIMED: Duration = Duration::from_secs(5);

/// What a run of `worker` reaches: the worker's objects, and its own storage when it is an object's run.
pub struct Reach {
    pub caller: Caller,
    pub worker: String,
    /// The run's own: a call it makes is given what is left of it.
    pub deadline: Instant,
    /// The key of the object that runs, and its storage.
    pub own: Option<(String, Arc<Txn>)>,
}

impl Host for Reach {
    fn fetch(
        &self,
        class: String,
        id: String,
        request: Request,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Response, String>> + Send>> {
        let key = object_key(&self.worker, &class, &id);
        let own = self.own.as_ref().is_some_and(|(own, _)| *own == key);
        let (caller, worker, deadline) = (self.caller.clone(), self.worker.clone(), self.deadline);
        Box::pin(async move {
            if id.is_empty() || id.len() > MAX_OBJECT_ID || id.chars().any(char::is_control) {
                return Err(format!("an object's id is 1 to {MAX_OBJECT_ID} bytes with no control character"));
            }
            if own {
                return Err("an object cannot call itself: it takes one request at a time".into());
            }
            let Request { method, url, headers, body } = request;
            let invocation = Invocation { worker, method, url, headers, sample: 0, body };
            let ran = caller.call(&key, invocation, deadline).await?;
            match ran.failure {
                Some(_) => Err(format!("the object failed: {}", String::from_utf8_lossy(&ran.body).trim_end())),
                None => Ok(Response { status: ran.status, headers: ran.headers, body: ran.body }),
            }
        })
    }

    fn storage(&self, op: StorageOp) -> std::pin::Pin<Box<dyn Future<Output = Result<StorageAnswer, String>> + Send>> {
        let answer = match &self.own {
            Some((_, txn)) => txn.run(op),
            None => Err("storage belongs to an object: this is a worker's own request".into()),
        };
        Box::pin(std::future::ready(answer))
    }
}

/// What every object on a runner shares.
pub struct Shared<E: Engine> {
    pub engine: E,
    pub serving: Serving<E::Version>,
    pub follower: ActorHandle<follower::Msg<E::Version>>,
    /// Where an object's runs go: `InvokerConfig::runtime`'s.
    pub jobs: Option<JobExecutor>,
    pub caller: Caller,
    pub store: NamedRef<StoreApi>,
    /// The runner's: an object's run is counted and logged beside its workers'.
    pub ending: Ending,
}

/// Lets this runner host objects.
pub async fn register<E: Engine + Clone>(
    cluster: &Cluster,
    shared: Arc<Shared<E>>,
    cfg: &ObjectConfig,
) -> Result<(), ClusterError> {
    let limits = MailboxLimits::default();
    let mailbox = MailboxConfig::from(limits).with_weight(|msg: &Msg| {
        size_of::<Msg>()
            + match msg {
                Msg::Ask(invocation, _) => invocation.body.len() + invocation.url.len(),
                Msg::Done(done) => match &done.told {
                    Told::Ran(ran) => ran.body.len(),
                    _ => 0,
                },
            }
    });
    let build = move |cx: &axon_cluster::SpawnContext, _args: Bytes| {
        let key = cx.key.clone().filter(|key| split_object_key(key).is_some());
        let key = key.ok_or("an object's key is worker/class/id")?;
        Ok(Object { shared: shared.clone(), key, epoch: cx.epoch, held: None, reply: None, running: false })
    };
    cluster.kind::<ObjectApi>(OBJECTS).mailbox(mailbox).idle(cfg.idle).register(build).await
}

pub enum Msg {
    Ask(Invocation, Option<Reply<Told>>),
    Done(Box<Done>),
}

/// What the object stores as this actor read it, and the version of that.
type Held = (Arc<Data>, u64);

pub struct Done {
    told: Told,
    /// `None` when the actor's copy is not the store's, or may not be: the next request reads it from the store.
    held: Option<Held>,
    /// The actor ends: another node owns the object, or its worker is gone.
    stop: bool,
}

pub struct Object<E: Engine> {
    shared: Arc<Shared<E>>,
    key: String,
    /// The lease this actor runs under. `None` where the cluster has no lease authority: nothing then makes the
    /// object one, and it refuses every request.
    epoch: Option<u64>,
    held: Option<Held>,
    reply: Option<Reply<Told>>,
    running: bool,
}

/// One request, start to answer by `deadline`. `room` is the body the reply carries.
async fn serve<E: Engine + Clone>(
    shared: Arc<Shared<E>>,
    key: String,
    epoch: u64,
    held: Option<Held>,
    invocation: Invocation,
    (deadline, room): (Instant, usize),
) -> Done {
    let left = || deadline.saturating_duration_since(Instant::now());
    let Some((worker, class, id)) = split_object_key(&key) else {
        return Done { told: Told::NotServed, held, stop: true };
    };
    // Before the store is asked anything: a key of no worker here is nobody's object.
    let Some(mut current) = shared.serving.get(worker) else {
        return Done { told: Told::NotServed, held, stop: true };
    };
    let Invocation { method, url, headers, body, .. } = invocation;
    let mut run = Run { worker: worker.to_owned(), version: 0, method: method.clone(), url: url.clone() };
    let unstored = |why: String| Err(Failed { failure: Failure::Storage(why), usage: Usage::default() });
    let fenced = |seen| format!("the object runs elsewhere now: epoch {seen} is past this actor's {epoch}");
    let (data, base) = match held {
        Some(held) => held,
        // Opening twice under one epoch changes nothing, so it is asked again while the store cannot be reached.
        None => match shared.store.request_until(StoreAsk::Open { object: key.clone(), epoch }, deadline).await {
            Ok(StoreTold::Snapshot { version, entries, .. }) => (Arc::new(Data::new(entries)), version),
            opened => {
                let (why, stop) = match opened {
                    Ok(StoreTold::Fenced { seen }) => (fenced(seen), true),
                    Ok(other) => (format!("the store answered {other:?}"), false),
                    Err(e) => (format!("the object's storage did not answer: {e}"), false),
                };
                return Done { told: Told::Ran(shared.ending.of("object", run, unstored(why))), held: None, stop };
            }
        },
    };

    if !current.is_warm() {
        // The worker as the answer has it: a sweep may let go of it again before the table could be read. Still cold
        // at the caller's deadline, or the follower gone: as the table has it, and busy below while cold.
        let warmed = follower::warm(&shared.follower, worker, left()).await;
        match warmed.unwrap_or_else(|_| shared.serving.get(worker)) {
            Some(warmed) => current = warmed,
            None => return Done { told: Told::NotServed, held: Some((data, base)), stop: true },
        }
    }
    // The stable version: one instance cannot take a canary's percent of its own requests.
    let Some(prepared) = current.stable().cloned() else {
        return Done { told: Told::Busy, held: Some((data, base)), stop: false };
    };
    let version = current.manifest.stable.clone();
    run.version = version.id;
    let mut limits = version.limits;
    limits.wall = limits.wall.min(left());
    limits.response_bytes = limits.response_bytes.min(room);

    let txn = Arc::new(Txn::new(data.clone()));
    let reach = Reach {
        caller: shared.caller.clone(),
        worker: worker.to_owned(),
        deadline,
        own: Some((key.clone(), txn.clone())),
    };
    let scope = Scope { object: Some((class.to_owned(), id.to_owned())), host: Some(Arc::new(reach)) };
    let request = Request { method, url, headers, body };
    let engine = shared.engine.clone();
    let ran = guarded(engine.invoke_in(&prepared, request, limits, scope)).await;
    let result = ran.unwrap_or_else(|failure| Err(Failed { failure, usage: Usage::default() }));

    // The output gate: the answer leaves only after the store has the request's writes, made onto the version this
    // actor read. A request that wrote nothing asks as well, with no writes, so a copy of what was cleared, or of
    // what another actor wrote past, answers nobody. The same write twice leaves what once does, so it is asked
    // again while the store cannot be reached. A request that failed wrote nothing, and its answer is its failure.
    let (after, ops) = txn.finish();
    let outcome = match result {
        Ok(outcome) => outcome,
        failed => {
            return Done {
                told: Told::Ran(shared.ending.of("object", run, failed)),
                held: Some((data, base)),
                stop: false,
            };
        }
    };
    let write = StoreAsk::Write { object: key.clone(), epoch, base, ops };
    let (held, gate, stop) = match shared.store.request_until(write, deadline).await {
        Ok(StoreTold::Written { version }) => (Some((after, version)), Ok(()), false),
        Ok(StoreTold::Fenced { seen }) => (None, Err(fenced(seen)), true),
        Ok(StoreTold::Stale { version }) => {
            let why = format!("the object's storage is at version {version}, and this actor read version {base}");
            (None, Err(why), false)
        }
        Ok(StoreTold::Refused(why)) => (Some((data, base)), Err(format!("the store refused the writes: {why}")), false),
        Ok(other) => (None, Err(format!("the store answered {other:?}")), false),
        // They may be written: the next request reads what the store has.
        Err(e) => (None, Err(format!("the store did not acknowledge the request: {e}")), false),
    };
    let result = match gate {
        Ok(()) => Ok(outcome),
        Err(why) => Err(Failed { failure: Failure::Storage(why), usage: outcome.usage }),
    };
    Done { told: Told::Ran(shared.ending.of("object", run, result)), held, stop }
}

impl<E: Engine + Clone> Actor for Object<E> {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Ask(invocation, reply) => {
                if reply.as_ref().is_some_and(Reply::is_abandoned) {
                    return Ok(());
                }
                let Some(epoch) = self.epoch else {
                    cx.stop();
                    let why = "objects need a lease authority: start every node with acceptors=";
                    let (failure, body) = (Some("storage".to_owned()), format!("{why}\n").into());
                    let ran =
                        Ran { status: 503, headers: Vec::new(), version: 0, cpu_us: 0, wall_us: 0, failure, body };
                    answer(reply, Told::Ran(ran));
                    return Ok(());
                };
                // Only if the pause below was refused: the request in hand keeps the actor's one reply.
                if self.running {
                    answer(reply, Told::Busy);
                    return Ok(());
                }
                let room = reply.as_ref().map_or(usize::MAX, |reply| reply.max_payload().saturating_sub(HEAD_ROOM));
                let left = reply.as_ref().and_then(Reply::time_left).unwrap_or(UNTIMED);
                let to = (Instant::now() + left, room);
                let (shared, key, held) = (self.shared.clone(), self.key.clone(), self.held.clone());
                let job = async move { Msg::Done(Box::new(serve(shared, key, epoch, held, invocation, to).await)) };
                match submit(cx, self.shared.jobs.as_ref(), job) {
                    Ok(job) => {
                        // One request at a time: the next waits in the mailbox, whose bound holds its sender back.
                        // A pause refused leaves `running` to turn the next away.
                        let _ = cx.pause_until(&job);
                        (self.running, self.reply) = (true, reply);
                    }
                    Err(_) => answer(reply, Told::Busy),
                }
            }
            Msg::Done(done) => {
                let Done { told, held, stop } = *done;
                (self.running, self.held) = (false, held);
                answer(self.reply.take(), told);
                if stop {
                    cx.stop();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_reads_its_own_writes_and_hands_over_the_last_per_key() {
        let data = Arc::new(Data::new(vec![("a".into(), "1".into()), ("b".into(), "2".into())]));
        let txn = Txn::new(data.clone());
        let put = |key: &str, value: &str| StorageOp::Put { key: key.into(), value: value.into() };
        let get = |key: &str| txn.run(StorageOp::Get { key: key.into() }).unwrap();
        assert_eq!(get("a"), StorageAnswer::Value(Some("1".into())));
        txn.run(put("a", "10")).unwrap();
        txn.run(put("a", "11")).unwrap();
        txn.run(put("c", "3")).unwrap();
        assert_eq!(txn.run(StorageOp::Delete { key: "b".into() }).unwrap(), StorageAnswer::Deleted(true));
        assert_eq!(txn.run(StorageOp::Delete { key: "zz".into() }).unwrap(), StorageAnswer::Deleted(false));
        assert_eq!(get("a"), StorageAnswer::Value(Some("11".into())));
        assert_eq!(get("b"), StorageAnswer::Value(None));
        let listed = txn.run(StorageOp::List { prefix: String::new(), limit: Some(1) }).unwrap();
        assert_eq!(listed, StorageAnswer::Entries(vec![("a".into(), "11".into())]));
        assert!(txn.run(put("", "1")).is_err(), "a key with no name");

        let (after, ops) = txn.finish();
        let expected: Vec<StoreOp> =
            vec![("a".into(), Some("11".into())), ("b".into(), None), ("c".into(), Some("3".into()))];
        assert_eq!(ops, expected);
        // The copy the actor kept is as it was until the store has the writes; applied there, the two agree.
        assert_eq!(data.get("a").map(String::as_str), Some("1"));
        let mut stored = (*data).clone();
        stored.apply(&ops).unwrap();
        assert_eq!(stored, *after);
    }
}
