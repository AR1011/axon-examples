//! The `shard` keyed actor. In memory mode, the shard's map and nothing on disk: a shard that moves starts empty. In
//! durable mode, a `repl::Owner` that writes each change to the storage set's Logs and answers once it is committed.

use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorHandle, Context};
use axon_cluster::{CallError, RemoteRef, Reply, Retry, Upload, WeakCluster};
use bytes::{Bytes, BytesMut};

use crate::log::{LogMsg, To};
use crate::repl::{Ballot, Config, FromLog, Map, Op, Out, Owner, Status, Stop, ToLog};
use crate::wire::{Answer, Call, LogApi, LogCall, ShardMsg, log_body, read_from_log};
use crate::{Limits, Stats};

/// Parts of an answer or an upload body: in the 64 to 256 KiB the cluster guide asks for.
pub const PART: usize = 256 << 10;

/// `body` in parts of at most `size` bytes, each a slice of it.
pub fn split(body: Bytes, size: usize) -> impl Iterator<Item = Bytes> {
    (0..body.len()).step_by(size).map(move |at| body.slice(at..(at + size).min(body.len())))
}

/// An upload's body whole, read in a job: each part read gives the caller more credit. `Err(None)` past `max`.
pub async fn read_upload(mut upload: Upload, max: usize) -> Result<Bytes, Option<CallError>> {
    let (mut parts, mut len) = (Vec::new(), 0);
    while let Some(part) = upload.next().await {
        let part = part.map_err(Some)?;
        len += part.len();
        if len > max {
            return Err(None);
        }
        parts.push(part);
    }
    Ok(concat(&parts))
}

/// The parts in one buffer of their size. Grown part by part it would have up to twice that, and a value kept from
/// it, which is not copied (`COPY_BELOW`), holds the whole buffer: a 10 MB value 16 MiB.
pub fn concat(parts: &[Bytes]) -> Bytes {
    let mut body = BytesMut::with_capacity(parts.iter().map(Bytes::len).sum());
    for part in parts {
        body.extend_from_slice(part);
    }
    body.freeze()
}

/// Values shorter than this are copied when stored, so a stored value does not keep alive the whole buffer it
/// arrived in.
const COPY_BELOW: usize = 1 << 20;

/// How often a durable shard's owner sends again what went unanswered.
const TICK: Duration = Duration::from_millis(100);
/// How long an owner waits for a Log on another node: shorter than `request_timeout`, so a hung Log is passed over
/// sooner. The owner sends again after `Config::resend`, the same second.
const LOG_WITHIN: Duration = Duration::from_secs(1);

/// What every durable shard on a node shares: the storage set's Logs and how its owners run.
pub struct Storage {
    /// This node's place in the storage set, and its Log.
    pub local: usize,
    pub log: ActorHandle<LogMsg>,
    /// The storage set, in `--nodes` order: a Log's place is its node's.
    pub nodes: Vec<String>,
    pub cluster: WeakCluster,
    pub limits: Limits,
    pub quorum_reads: bool,
    /// A start that does not serve within this stops: the system's `start_timeout`.
    pub give_up: Duration,
    pub keep: u64,
    pub keep_bytes: u64,
    pub stats: Arc<Stats>,
    /// The other nodes' Logs by place, resolved once and again after `Retry::Resolve`.
    pub remotes: Mutex<HashMap<usize, RemoteRef<LogApi>>>,
}

impl Storage {
    /// One message to the Log at `log` and its answer. This node's Log gets it as is; another node's Log as bytes,
    /// in an upload when larger than a frame, and a `Fetch`'s answer in parts.
    async fn call(&self, log: usize, shard: u16, msg: ToLog) -> Result<FromLog, String> {
        if log == self.local {
            return self.log.ask(|r| LogMsg::Call(shard, msg, To::Local(r))).await.map_err(|e| e.to_string());
        }
        let known = self.remotes.lock().expect("not poisoned").get(&log).cloned();
        let remote = match known {
            Some(remote) => remote,
            None => {
                let cluster = self.cluster.upgrade().ok_or("the cluster is gone")?;
                let remote =
                    cluster.resolve_on::<LogApi>(&self.nodes[log], "kv.log").await.map_err(|e| e.to_string())?;
                self.remotes.lock().expect("not poisoned").insert(log, remote.clone());
                remote
            }
        };
        let fetch = matches!(msg, ToLog::Fetch { .. });
        let append = matches!(msg, ToLog::Append { .. });
        let sent = tokio::time::Instant::now();
        let body = log_body(shard, &msg);
        let answer = if fetch {
            self.parts(&remote, body).await
        } else if body.len() > self.limits.frame_body {
            self.stats.log_uploads.fetch_add(1, Relaxed);
            remote.upload(LogCall::Upload, futures_util::stream::iter(split(body, PART).map(Ok))).await
        } else {
            remote.request_within(LogCall::Plain(body), LOG_WITHIN).await
        };
        if append && answer.is_ok() {
            self.stats.append_rtt.record(sent.elapsed());
        }
        match answer {
            Ok(body) => read_from_log(body).ok_or_else(|| format!("a Log's answer to shard {shard} does not decode")),
            Err(e) => {
                if e.retry() == Retry::Resolve {
                    self.remotes.lock().expect("not poisoned").remove(&log);
                }
                Err(e.to_string())
            }
        }
    }

    /// A `Fetch`, its answer in parts: the shard's whole map, when the owner is far behind.
    async fn parts(&self, remote: &RemoteRef<LogApi>, body: Bytes) -> Result<Bytes, CallError> {
        let mut parts = remote.request_parts_within(LogCall::Parts(body), self.give_up).await?;
        let mut answer = Vec::new();
        while let Some(part) = parts.next().await {
            answer.push(part?);
        }
        Ok(concat(&answer))
    }
}

pub struct Shard {
    id: u16,
    limits: Limits,
    store: Store,
    /// Uploads being read, by job, with the reply each waits for.
    uploads: HashMap<u64, Reply<Answer>>,
    next_upload: u64,
}

enum Store {
    Memory(HashMap<Bytes, Bytes>),
    Durable(Box<Durable>),
}

/// A durable shard: its owner, the Logs it reaches, and the callers each owner token answers.
struct Durable {
    ballot: Ballot,
    owner: Option<Owner>,
    storage: Arc<Storage>,
    held: HashMap<u64, Held>,
    next_token: u64,
    serving: bool,
    /// When `on_start` ran, in ms: the recovery's time runs from here until the owner serves.
    started: u64,
    /// The owner's kept bytes as last added to `Stats::owner_kept_bytes`.
    kept: u64,
}

impl Drop for Durable {
    fn drop(&mut self) {
        self.storage.stats.owner_kept_bytes.fetch_sub(self.kept, Relaxed);
    }
}

enum Held {
    /// A write, answered with `Del`'s count or OK once committed.
    Write(Option<Reply<Answer>>, bool),
    /// A read's answer, taken when it arrived, given once a round to the Logs has come back.
    Read(Option<Reply<Answer>>, Answer),
    Parts(Reply<Answer>, Answer),
}

/// A read from a map.
fn read(call: &Call, get: impl Fn(&[u8]) -> Option<Bytes>, len: usize) -> Answer {
    match call {
        Call::Get(k) => Answer::Value(get(k)),
        Call::Exists(keys) => Answer::Count(keys.iter().filter(|k| get(k).is_some()).count() as u64),
        Call::Len => Answer::Count(len as u64),
        Call::MGet(keys) => Answer::Values(keys.iter().map(|k| get(k)).collect()),
        _ => Answer::Refused("ERR not a read".into()),
    }
}

impl Shard {
    pub fn new(id: u16, limits: Limits) -> Self {
        Self { id, limits, store: Store::Memory(HashMap::new()), uploads: HashMap::new(), next_upload: 0 }
    }

    /// A durable shard that starts as `ballot`.
    pub fn durable(id: u16, limits: Limits, ballot: Ballot, storage: Arc<Storage>) -> Self {
        let durable = Durable {
            ballot,
            owner: None,
            storage,
            held: HashMap::new(),
            next_token: 0,
            serving: false,
            started: 0,
            kept: 0,
        };
        Self { id, limits, store: Store::Durable(Box::new(durable)), uploads: HashMap::new(), next_upload: 0 }
    }

    fn own(b: Bytes) -> Bytes {
        if b.len() < COPY_BELOW { Bytes::copy_from_slice(&b) } else { b }
    }

    fn too_large(&self, call: &Call) -> bool {
        match call {
            Call::Set(_, v) => v.len() > self.limits.value,
            Call::MSet(pairs) => pairs.iter().any(|(_, v)| v.len() > self.limits.value),
            _ => false,
        }
    }

    /// Runs a call that is not `Upload` or `Parts` on a memory shard's map.
    pub fn apply(&mut self, call: Call) -> Answer {
        if self.too_large(&call) {
            return Answer::Refused("ERR value too large".into());
        }
        let Store::Memory(map) = &mut self.store else { return Answer::Refused("ERR a durable shard".into()) };
        match call {
            Call::Set(k, v) => {
                map.insert(Self::own(k), Self::own(v));
                Answer::Ok
            }
            Call::Del(keys) => Answer::Count(keys.iter().filter(|k| map.remove(*k).is_some()).count() as u64),
            Call::Clear => {
                *map = HashMap::new();
                Answer::Ok
            }
            Call::MSet(pairs) => {
                for (k, v) in pairs {
                    map.insert(Self::own(k), Self::own(v));
                }
                Answer::Ok
            }
            Call::Upload | Call::Parts(_) => Answer::Refused(format!("ERR shard {}: a call inside a call", self.id)),
            read_call => read(&read_call, |k| map.get(k).cloned(), map.len()),
        }
    }

    /// The answer as it may leave: `TooLarge` when it would not fit a frame, so the caller asks again in parts.
    fn fit(&self, answer: Answer) -> Answer {
        let len = answer_len(&answer);
        if len > self.limits.reply {
            Answer::Refused("ERR reply too large".into())
        } else if len > self.limits.frame_body {
            Answer::TooLarge(len as u64)
        } else {
            answer
        }
    }

    fn answer(&mut self, call: Call) -> Answer {
        let answer = self.apply(call);
        self.fit(answer)
    }

    /// A read's answer in parts: pulled as the caller reads. A refused job drops it, and the caller hears NoReply and
    /// asks again.
    fn send_parts(reply: Reply<Answer>, answer: Answer, limits: &Limits, cx: &mut Context<'_, Self>) {
        let answer =
            if answer_len(&answer) > limits.reply { Answer::Refused("ERR reply too large".into()) } else { answer };
        let body = answer.encode();
        let size = reply.max_payload().saturating_sub(16).clamp(1, PART);
        let parts = split(body, size).map(|part| Ok::<_, CallError>(Answer::Chunk(part)));
        let sending = reply.send_parts(futures_util::stream::iter(parts));
        let _ = cx.submit_future(async move {
            let _ = sending.await;
            ShardMsg::Sent
        });
    }

    /// A call to a durable shard: a write goes to its owner, a read takes its answer now and waits for a round.
    /// Before the owner serves, the call waits in the stash.
    fn durable_call(&mut self, call: Call, reply: Option<Reply<Answer>>, parts: bool, cx: &mut Context<'_, Self>) {
        let id = self.id;
        if self.too_large(&call) {
            drop(reply.map(|r| r.send(Answer::Refused("ERR value too large".into()))));
            return;
        }
        let Store::Durable(d) = &mut self.store else { return };
        let Some(owner) = d.owner.as_mut().filter(|o| o.status() == Status::Serving) else {
            let msg =
                if parts { ShardMsg::Call(Call::Parts(Box::new(call)), reply) } else { ShardMsg::Call(call, reply) };
            if let Err(ShardMsg::Call(_, Some(reply))) = cx.stash(msg) {
                drop(reply.send(Answer::Refused(format!("BUSY shard {id} is starting"))));
            }
            return;
        };
        d.next_token += 1;
        let token = d.next_token;
        let held = match call {
            Call::Set(k, v) => Op::Set(vec![(Self::own(k), Self::own(v))]),
            Call::MSet(pairs) => Op::Set(pairs.into_iter().map(|(k, v)| (Self::own(k), Self::own(v))).collect()),
            Call::Del(keys) => Op::Del(keys),
            Call::Clear => Op::Clear,
            Call::Upload | Call::Parts(_) => {
                drop(reply.map(|r| r.send(Answer::Refused(format!("ERR shard {id}: a call inside a call")))));
                return;
            }
            read_call => {
                let map: &Map = owner.map();
                let answer = read(&read_call, |k| map.get(k).cloned(), map.len());
                // A key no read asks for: what releases the answer is the round, not the value.
                let barrier = owner.read(&[], token);
                debug_assert!(barrier, "the owner serves");
                let held = match (parts, reply) {
                    (true, Some(reply)) => Held::Parts(reply, answer),
                    (_, reply) => Held::Read(reply, answer),
                };
                d.held.insert(token, held);
                return;
            }
        };
        let del = matches!(held, Op::Del(_));
        match owner.write(held, token) {
            Ok(_) => drop(d.held.insert(token, Held::Write(reply, del))),
            Err(_) => drop(reply.map(|r| r.send(Answer::Refused(format!("TRYAGAIN shard {id} stopping"))))),
        }
    }

    /// Carries out what the owner asks: messages to its Logs as jobs, and answers. During the start each Log job
    /// holds the actor (`pause_until`), so the start ends only once the owner serves.
    fn pump(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let id = self.id;
        let Store::Durable(d) = &mut self.store else { return Ok(()) };
        let Some(owner) = d.owner.as_mut() else { return Ok(()) };
        let starting = owner.status() != Status::Serving;
        let mut answers = Vec::new();
        for out in owner.take_out() {
            match out {
                // One job each, so a Log that is slow or gone holds back none of the others.
                Out::Send(log, msg) => {
                    if matches!(msg, ToLog::Install { .. }) {
                        d.storage.stats.owner_installs.fetch_add(1, Relaxed);
                    }
                    let storage = d.storage.clone();
                    let seq = msg.seq();
                    let job = cx.submit_future(async move {
                        let answer = storage.call(log, id, msg).await;
                        ShardMsg::Logged(log, seq, answer)
                    });
                    match job {
                        Ok(job) if starting => cx.pause_until(&job).map_err(|e| format!("{e:?}"))?,
                        Ok(_) => {}
                        // Refused (too many jobs): the owner sends it again after `resend`.
                        Err(_) => {
                            if let Some(seq) = seq {
                                owner.failed(log, seq);
                            }
                        }
                    }
                }
                Out::Acked { token, result, .. } => {
                    if let Some(Held::Write(reply, del)) = d.held.remove(&token) {
                        answers.push((reply, if del { Answer::Count(result) } else { Answer::Ok }));
                    }
                }
                Out::Read { token, .. } => match d.held.remove(&token) {
                    Some(Held::Read(reply, answer)) => answers.push((reply, answer)),
                    Some(Held::Parts(reply, answer)) => Self::send_parts(reply, answer, &self.limits, cx),
                    _ => {}
                },
                // The owner stopped: the caller hears NoReply, and a write's outcome is unknown.
                Out::Dropped { token } => drop(d.held.remove(&token)),
            }
        }
        let was = std::mem::replace(&mut d.kept, owner.kept());
        if d.kept != was {
            // Wrapping: a fall adds its two's complement.
            d.storage.stats.owner_kept_bytes.fetch_add(d.kept.wrapping_sub(was), Relaxed);
        }
        let status = owner.status();
        if status == Status::Serving && !d.serving {
            d.serving = true;
            let took = Duration::from_millis(Self::now(cx).saturating_sub(d.started));
            d.storage.stats.recovery.record(took);
            d.storage.stats.recovery_max_ms.fetch_max(took.as_millis() as u64, Relaxed);
            cx.unstash_all();
        }
        for (reply, answer) in answers {
            let answer = self.fit(answer);
            drop(reply.map(|r| r.send(answer)));
        }
        match status {
            Status::Stopped(Stop::NoQuorum) => Err(format!("KV-NOQUORUM shard {id}: no majority of logs answered")),
            Status::Stopped(Stop::Deposed(by)) => Err(format!("shard {id}: a newer owner, {by:?}")),
            Status::Stopped(Stop::Lost) => Err(format!("shard {id}: the chosen log did not give its entries")),
            _ => Ok(()),
        }
    }

    fn now(cx: &Context<'_, Self>) -> u64 {
        cx.now().as_millis() as u64
    }
}

/// The bytes of an answer once encoded, without encoding it.
pub fn answer_len(answer: &Answer) -> usize {
    let maybe = |v: &Option<Bytes>| 1 + v.as_ref().map_or(0, |v| 4 + v.len());
    match answer {
        Answer::Value(v) => 1 + maybe(v),
        Answer::Values(vs) => 5 + vs.iter().map(maybe).sum::<usize>(),
        other => other.encode().len(),
    }
}

impl Actor for Shard {
    type Message = ShardMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let now = Self::now(cx);
        let Store::Durable(d) = &mut self.store else { return Ok(()) };
        let st = &d.storage;
        let mut cfg = Config::new(st.nodes.len(), Some(st.local));
        // A commit goes out at once to every Log with nothing in flight, and a held read sends a round of its own,
        // so an idle shard needs no heartbeat of 10 ms: one a second keeps a Log that missed one up to date.
        (cfg.heartbeat, cfg.quorum_reads, cfg.give_up) = (1000, st.quorum_reads, st.give_up.as_millis() as u64);
        (cfg.keep, cfg.keep_bytes) = (st.keep, st.keep_bytes);
        d.started = now;
        d.owner = Some(Owner::start(d.ballot, cfg, now));
        cx.schedule_every(TICK, || ShardMsg::Tick).map_err(|e| format!("{:?}", e.reason))?;
        self.pump(cx)
    }

    fn on_message(&mut self, msg: ShardMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let durable = matches!(self.store, Store::Durable(_));
        match msg {
            ShardMsg::Call(Call::Upload, Some(reply)) => {
                let Some(upload) = reply.upload() else {
                    let _ = reply.send(Answer::Refused("ERR an upload without a body".into()));
                    return Ok(());
                };
                let (id, max) = (self.next_upload, self.limits.command);
                self.next_upload += 1;
                self.uploads.insert(id, reply);
                let read = async move {
                    let body = read_upload(upload, max).await.map_err(|e| match e {
                        None => "ERR value too large".to_owned(),
                        Some(e) => format!("ERR upload cut: {e}"),
                    });
                    ShardMsg::Uploaded(id, body)
                };
                if cx.submit_future(read).is_err() {
                    let reply = self.uploads.remove(&id).expect("the reply was just kept");
                    let _ = reply.send(Answer::Refused(format!("BUSY shard {} has too many uploads", self.id)));
                }
            }
            ShardMsg::Uploaded(id, body) => {
                let Some(reply) = self.uploads.remove(&id) else { return Ok(()) };
                match body.map(|mut body| Call::decode(&mut body)) {
                    Ok(Some(call @ (Call::Set(..) | Call::MSet(_)))) if durable => {
                        self.durable_call(call, Some(reply), false, cx)
                    }
                    Ok(Some(call @ (Call::Set(..) | Call::MSet(_)))) => drop(reply.send(self.answer(call))),
                    Ok(_) => drop(
                        reply.send(Answer::Refused(format!("ERR shard {}: an upload that is not a write", self.id))),
                    ),
                    Err(text) => drop(reply.send(Answer::Refused(text))),
                }
            }
            ShardMsg::Call(Call::Parts(call), Some(reply)) => {
                // Only a read is asked again in parts: a write here would run twice.
                if call.is_write() {
                    drop(reply.send(Answer::Refused(format!("ERR shard {}: a write in parts", self.id))));
                } else if durable {
                    self.durable_call(*call, Some(reply), true, cx);
                } else {
                    let answer = self.apply(*call);
                    Self::send_parts(reply, answer, &self.limits, cx);
                }
            }
            ShardMsg::Call(call, reply) if durable => self.durable_call(call, reply, false, cx),
            ShardMsg::Call(call, reply) => {
                let answer = self.answer(call);
                if let Some(reply) = reply {
                    let _ = reply.send(answer);
                }
            }
            ShardMsg::Logged(log, seq, answer) => {
                let now = Self::now(cx);
                if let Store::Durable(d) = &mut self.store
                    && let Some(owner) = d.owner.as_mut()
                {
                    match (answer, seq) {
                        (Ok(answer), _) => owner.on_reply(log, answer, now),
                        (Err(_), Some(seq)) => owner.failed(log, seq),
                        (Err(_), None) => {}
                    }
                }
            }
            ShardMsg::Tick => {
                let now = Self::now(cx);
                if let Store::Durable(d) = &mut self.store
                    && let Some(owner) = d.owner.as_mut()
                {
                    owner.tick(now);
                }
            }
            ShardMsg::Sent => {}
        }
        self.pump(cx)
    }

    /// The writes and reads of the turn go to the Log in one Append.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let now = Self::now(cx);
        if let Store::Durable(d) = &mut self.store
            && let Some(owner) = d.owner.as_mut()
        {
            owner.flush(now);
        }
        self.pump(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn calls_change_and_read_the_map() {
        let mut shard = Shard::new(0, Limits::default());
        assert_eq!(shard.apply(Call::Set(b("a"), b("1"))), Answer::Ok);
        assert_eq!(shard.apply(Call::MSet(vec![(b("b"), b("2")), (b("c"), b("3"))])), Answer::Ok);
        assert_eq!(shard.apply(Call::Get(b("a"))), Answer::Value(Some(b("1"))));
        assert_eq!(shard.apply(Call::Exists(vec![b("a"), b("a"), b("x")])), Answer::Count(2));
        assert_eq!(shard.apply(Call::MGet(vec![b("c"), b("x")])), Answer::Values(vec![Some(b("3")), None]));
        assert_eq!(shard.apply(Call::Del(vec![b("a"), b("x")])), Answer::Count(1));
        assert_eq!(shard.apply(Call::Len), Answer::Count(2));
        assert_eq!(shard.apply(Call::Clear), Answer::Ok);
        assert_eq!(shard.apply(Call::Len), Answer::Count(0));
    }

    #[test]
    fn answers_past_a_frame_say_so_and_past_the_reply_limit_are_refused() {
        let limits = Limits { frame_body: 1000, reply: 5000, value: 4000, ..Limits::default() };
        let mut shard = Shard::new(0, limits);
        shard.apply(Call::Set(b("small"), Bytes::from(vec![1; 900])));
        shard.apply(Call::Set(b("big"), Bytes::from(vec![1; 3000])));
        assert!(matches!(shard.answer(Call::Get(b("small"))), Answer::Value(Some(_))));
        // The length it names is the encoded answer's, which the caller reserves before it asks in parts.
        let big = Answer::Value(Some(Bytes::from(vec![1; 3000])));
        assert_eq!(shard.answer(Call::Get(b("big"))), Answer::TooLarge(big.encode().len() as u64));
        let two = shard.answer(Call::MGet(vec![b("big"), b("big")]));
        assert_eq!(two, Answer::Refused("ERR reply too large".into()));
        assert_eq!(
            shard.apply(Call::Set(b("x"), Bytes::from(vec![1; 4001]))),
            Answer::Refused("ERR value too large".into())
        );
        for answer in [big, Answer::Values(vec![None, Some(b("v"))]), Answer::Ok, Answer::Count(3)] {
            assert_eq!(answer_len(&answer), answer.encode().len(), "{answer:?}");
        }
    }

    #[test]
    fn small_values_do_not_keep_their_buffer_alive() {
        let mut shard = Shard::new(0, Limits::default());
        let buffer = Bytes::from(vec![7u8; 64 << 10]);
        shard.apply(Call::Set(buffer.slice(0..3), buffer.slice(3..10)));
        assert!(buffer.is_unique(), "the stored key and value are copies");
    }

    #[test]
    fn a_body_from_parts_has_no_spare_room() {
        let parts: Vec<Bytes> = split(Bytes::from(vec![7u8; 10_000_000]), PART).collect();
        let body = concat(&parts).try_into_mut().expect("unique");
        // Grown part by part it would have 16 MiB, all kept alive by a value stored from it.
        assert_eq!((body.len(), body.capacity()), (10_000_000, 10_000_000));
    }
}
