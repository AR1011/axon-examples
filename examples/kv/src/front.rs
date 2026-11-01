//! The RESP front end on every node: one task per connection, one command in flight on each, each command sent to
//! the shard that owns its keys, wherever that runs.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::{ActorHandle, Histogram};
use axon_cluster::{
    CallError, Cluster, ClusterError, KeyedRef, OwnershipAuthority, Placement, Rejection, Retry, SpawnRefusal,
};
use bytes::{Bytes, BytesMut};
use futures_util::future::join_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout, timeout_at};

use crate::log::LogMsg;
use crate::resp::{Out, Parsed, Value, parse};
use crate::shard::{PART, split};
use crate::slot::{SHARDS, SLOTS_PER_SHARD, key_slot, shard_key, shard_of_slot};
use crate::wire::{Answer, Call, ShardApi};
use crate::{BASE_INPUT, COMMANDS, Command, Limits, Stats, command_index};

/// A connection with output it cannot write for this long is dropped.
const WRITE_STALL: Duration = Duration::from_secs(30);

/// Bytes of the node's budget a connection holds; given back when dropped.
pub struct Held {
    budget: Arc<Semaphore>,
    n: usize,
}

impl Held {
    pub fn new(budget: &Arc<Semaphore>) -> Self {
        Self { budget: budget.clone(), n: 0 }
    }

    /// Holds `to` bytes, waiting until `until` for more. False if they did not come.
    pub async fn resize(&mut self, to: usize, until: Instant) -> bool {
        if to <= self.n {
            self.budget.add_permits(self.n - to);
            self.n = to;
            return true;
        }
        let Ok(more) = u32::try_from(to - self.n) else { return false };
        match timeout_at(until, self.budget.acquire_many(more)).await {
            Ok(Ok(permit)) => {
                permit.forget();
                self.n = to;
                true
            }
            _ => false,
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.budget.add_permits(self.n);
    }
}

/// Where calls go: the shards, or, for `kv stub`, nowhere.
pub enum Dispatch {
    Shards(Shards),
    /// Answers every GET with this value and every write with OK: the front end alone.
    Stub(Bytes),
}

pub struct Shards {
    pub cluster: Cluster,
    pub refs: Vec<KeyedRef<ShardApi>>,
    pub deadline: Duration,
    pub limits: Limits,
    pub stats: Arc<Stats>,
    /// The leases, asked whether a grant could reach this node.
    pub ownership: Arc<dyn OwnershipAuthority>,
}

impl Shards {
    pub fn new(
        cluster: Cluster,
        deadline: Duration,
        limits: Limits,
        stats: Arc<Stats>,
        ownership: Arc<dyn OwnershipAuthority>,
    ) -> Self {
        let refs = (0..SHARDS)
            .map(|s| cluster.keyed::<ShardApi>("shard", &shard_key(s), Bytes::new(), &Placement::default()))
            .collect();
        Self { cluster, refs, deadline, limits, stats, ownership }
    }
}

/// What the front end answers for a call that failed. `write` says the call changes data,
/// so it is never sent again once it may have run. `can_grant` is the leases' own view: without a majority of
/// acceptors, a call that found its shard moving at the deadline is the cluster down, not a move.
pub fn map_error(e: &ClusterError, write: bool, shard: u16, can_grant: bool) -> Value {
    use axon_net::SubmitReason::Oversized;
    match e {
        ClusterError::Call(CallError::NotSent(Oversized)) => Value::err("ERR value too large"),
        ClusterError::Call(CallError::Rejected(Rejection::Full) | CallError::Busy) => {
            Value::err(format!("BUSY shard {shard} is full"))
        }
        ClusterError::Spawn(SpawnRefusal::Failed, text) if text.contains("KV-NOQUORUM") => {
            Value::err(format!("CLUSTERDOWN shard {shard} no majority of logs"))
        }
        e if matches!(e, ClusterError::Spawn(SpawnRefusal::Isolated, _))
            || (!can_grant && matches!(e.retry(), Retry::Later | Retry::Resolve)) =>
        {
            Value::err(format!("CLUSTERDOWN shard {shard} no majority of acceptors"))
        }
        e => match e.retry() {
            Retry::IfIdempotent if write => Value::err("ERR outcome unknown"),
            Retry::IfIdempotent => Value::err(format!("TRYAGAIN shard {shard}")),
            Retry::Later | Retry::Resolve => Value::err(format!("TRYAGAIN shard {shard} moving")),
            Retry::Never => Value::err(format!("ERR shard {shard}: {e}")),
        },
    }
}

/// Whether a failed call is tried again before its deadline: one that did not run, or a read.
pub fn again(e: &ClusterError, write: bool) -> bool {
    match e.retry() {
        Retry::Later | Retry::Resolve => true,
        Retry::IfIdempotent => !write,
        Retry::Never => false,
    }
}

async fn pause(step: &mut Duration, deadline: Instant) -> bool {
    let wake = Instant::now() + *step;
    if wake >= deadline {
        return false;
    }
    tokio::time::sleep_until(wake).await;
    *step = (*step * 2).min(Duration::from_millis(100));
    true
}

impl Shards {
    /// One call to one shard, until the deadline: an answer, or the RESP error to give. A large answer comes with
    /// the budget it holds until it is written.
    async fn call(&self, shard: u16, call: Call, budget: &Arc<Semaphore>) -> Result<(Answer, Option<Held>), Value> {
        let deadline = Instant::now() + self.deadline;
        let write = call.is_write();
        let target = &self.refs[usize::from(shard)];
        let mut step = Duration::from_millis(10);
        let answer = loop {
            let result = if call.encoded_len() > self.limits.frame_body {
                self.upload(target, &call).await
            } else {
                target.request_until(call.clone(), deadline).await
            };
            match result {
                Ok(answer) => break answer,
                Err(e) if again(&e, write) && pause(&mut step, deadline).await => {}
                Err(e) => return Err(map_error(&e, write, shard, self.ownership.can_grant())),
            }
        };
        match answer {
            Answer::TooLarge(n) if !write => {
                let mut held = Held::new(budget);
                if !held.resize(n as usize, deadline).await {
                    return Err(Value::err("BUSY the node's budget for large replies is used up"));
                }
                Ok((self.parts(target, shard, call, deadline).await?, Some(held)))
            }
            Answer::Refused(text) => Err(Value::Error(text)),
            answer => Ok((answer, None)),
        }
    }

    /// A call too large for a frame: its encoding as the body of an upload. Nothing is sent again once it went.
    async fn upload(&self, target: &KeyedRef<ShardApi>, call: &Call) -> Result<Answer, ClusterError> {
        let parts = split(call.encode(), PART).map(Ok::<_, CallError>);
        self.stats.uploads.fetch_add(1, Relaxed);
        target.upload(Call::Upload, futures_util::stream::iter(parts)).await
    }

    /// A read whose answer is larger than a frame: asked again, the answer in parts.
    async fn parts(
        &self,
        target: &KeyedRef<ShardApi>,
        shard: u16,
        call: Call,
        deadline: Instant,
    ) -> Result<Answer, Value> {
        let mut step = Duration::from_millis(10);
        self.stats.parts_replies.fetch_add(1, Relaxed);
        loop {
            let ask = Call::Parts(Box::new(call.clone()));
            let within = deadline.saturating_duration_since(Instant::now());
            let result: Result<BytesMut, ClusterError> = async {
                let mut parts = target.request_parts_within(ask, within).await?;
                let mut body = BytesMut::new();
                while let Some(part) = parts.next().await {
                    match part.map_err(ClusterError::Call)? {
                        Answer::Chunk(chunk) if body.len() + chunk.len() <= self.limits.reply + 64 => {
                            body.extend_from_slice(&chunk)
                        }
                        _ => return Err(ClusterError::Call(CallError::Codec("a part of an answer".into()))),
                    }
                }
                Ok(body)
            }
            .await;
            match result {
                Ok(body) => {
                    return match Answer::decode(body.freeze()) {
                        Some(Answer::Refused(text)) => Err(Value::Error(text)),
                        Some(answer) => Ok(answer),
                        None => Err(Value::err(format!("ERR shard {shard}: an answer that does not decode"))),
                    };
                }
                Err(e) if again(&e, false) && pause(&mut step, deadline).await => {}
                Err(e) => return Err(map_error(&e, false, shard, self.ownership.can_grant())),
            }
        }
    }
}

/// This node, for `INFO` and `CLUSTER`.
pub struct NodeInfo {
    pub id: String,
    pub mode: String,
    /// The storage set, also the acceptors.
    pub nodes: Vec<String>,
    pub resp: SocketAddr,
    /// Durable mode: the node's Log, for `INFO kvdigest`.
    pub log: Option<ActorHandle<LogMsg>>,
    /// `INFO kv`'s `reads` and lease timings: "" for the stub.
    pub reads: &'static str,
    pub lease: LeaseTimes,
}

/// The lease's times as the cluster takes them from `PaxosConfig`, for the fault run's bounds.
#[derive(Clone, Copy, Debug, Default)]
pub struct LeaseTimes {
    /// The acceptors' lease, T.
    pub lease: Duration,
    /// How long a holder may act after a renew it sent: `OwnershipAuthority::hold`.
    pub hold: Duration,
    /// When an owner that could not renew stops: 7/8 of `hold`, the cluster's default.
    pub grace: Duration,
    /// How often leases are renewed: 2/5 of `grace`, the cluster's default.
    pub renew: Duration,
}

pub struct Front {
    pub dispatch: Dispatch,
    pub limits: Limits,
    pub password: Option<String>,
    pub stats: Arc<Stats>,
    pub info: NodeInfo,
    budget: Arc<Semaphore>,
    connections: Arc<Semaphore>,
    started: Instant,
}

/// One connection's state.
struct Conn {
    authed: bool,
}

/// What a command gives: its reply, whether to close after it, and budget held until the reply is written.
struct Outcome {
    value: Value,
    close: bool,
    held: Option<Held>,
}

impl From<Value> for Outcome {
    fn from(value: Value) -> Self {
        Outcome { value, close: false, held: None }
    }
}

fn arity(name: &str) -> Value {
    Value::err(format!("ERR wrong number of arguments for '{name}' command"))
}

fn crossslot() -> Value {
    Value::err("CROSSSLOT Keys in request don't hash to the same slot")
}

fn int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

fn lower(arg: &[u8]) -> String {
    String::from_utf8_lossy(arg).to_ascii_lowercase()
}

/// `COMMAND`: [`COMMANDS`] in Redis 5's six fields. A cluster client reads the keys' positions here to send each
/// command to its shard's node; without them go-redis sends each to a node at random and logs a line for it.
fn command_table() -> Value {
    let entry = |&(name, arity, flags, first, last, step): &Command| {
        let flags = flags.iter().map(|f| Value::Simple((*f).into())).collect();
        let int = Value::Int;
        Value::Array(Some(vec![
            Value::bulk(name),
            int(arity),
            Value::Array(Some(flags)),
            int(first),
            int(last),
            int(step),
        ]))
    };
    Value::Array(Some(COMMANDS.iter().map(entry).collect()))
}

/// Redis 7's words for a command it does not know.
pub fn unknown(args: &[Bytes]) -> Value {
    let name: String = String::from_utf8_lossy(&args[0]).chars().take(128).collect();
    let mut shown = String::new();
    for arg in &args[1..] {
        if shown.len() >= 128 {
            break;
        }
        let room = 128 - shown.len();
        let arg: String = String::from_utf8_lossy(arg).chars().take(room).collect();
        shown.push_str(&format!("'{arg}' "));
    }
    Value::err(format!("ERR unknown command '{name}', with args beginning with: {shown}"))
}

/// A node's id, its RESP address and the slot ranges it owns.
type SlotOwner = (String, SocketAddr, Vec<(u16, u16)>);

impl Front {
    pub fn new(
        dispatch: Dispatch,
        limits: Limits,
        password: Option<String>,
        stats: Arc<Stats>,
        info: NodeInfo,
    ) -> Self {
        let budget = Arc::new(Semaphore::new(limits.input_budget));
        let connections = Arc::new(Semaphore::new(limits.connections));
        Self { dispatch, limits, password, stats, info, budget, connections, started: Instant::now() }
    }

    /// Accepts connections until the listener fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                // Out of descriptors, most often: a pause, so the loop does not spin while it lasts.
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            let front = self.clone();
            tokio::spawn(async move { front.connection(stream).await });
        }
    }

    async fn connection(self: Arc<Self>, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let (mut rd, mut wr) = stream.into_split();
        let Ok(_slot) = self.connections.clone().try_acquire_owned() else {
            self.stats.connections_refused.fetch_add(1, Relaxed);
            let _ = timeout(WRITE_STALL, wr.write_all(b"-ERR max number of clients reached\r\n")).await;
            return;
        };
        self.stats.connections.fetch_add(1, Relaxed);
        self.stats.connections_total.fetch_add(1, Relaxed);
        let _ = self.run(&mut rd, &mut wr).await;
        self.stats.connections.fetch_sub(1, Relaxed);
    }

    async fn run(&self, rd: &mut tokio::net::tcp::OwnedReadHalf, wr: &mut OwnedWriteHalf) -> std::io::Result<()> {
        let mut conn = Conn { authed: self.password.is_none() };
        let mut buf = BytesMut::with_capacity(16 << 10);
        let mut out = Out::default();
        let mut input = Held::new(&self.budget);
        let mut replies = Vec::new();
        loop {
            let need = loop {
                match parse(&mut buf, self.limits.command, self.limits.command) {
                    Ok(Parsed::Command(args)) if args.is_empty() => {}
                    Ok(Parsed::Command(args)) => {
                        let outcome = self.execute(&mut conn, args).await;
                        out.push(&outcome.value);
                        replies.extend(outcome.held);
                        if outcome.close {
                            flush(wr, &mut out).await?;
                            return Ok(());
                        }
                        if out.len > BASE_INPUT {
                            flush(wr, &mut out).await?;
                            replies.clear();
                        }
                    }
                    Ok(Parsed::Need(n)) => break n,
                    Err(e) => {
                        out.push(&Value::err(format!("ERR Protocol error: {}", e.0)));
                        flush(wr, &mut out).await?;
                        return Ok(());
                    }
                }
            };
            flush(wr, &mut out).await?;
            replies.clear();
            // A buffer grown for a large command is not kept for the small ones after it.
            if buf.capacity() > 4 * BASE_INPUT && buf.len() < BASE_INPUT {
                buf = BytesMut::from(&buf[..]);
            }
            let want = need.max(buf.len() + (16 << 10)).min(self.limits.command + 64);
            let until = Instant::now() + self.limits.input_wait;
            if !input.resize(want.saturating_sub(BASE_INPUT), until).await {
                out.push(&Value::err("ERR the node's input budget is used up"));
                flush(wr, &mut out).await?;
                return Ok(());
            }
            // A header line of garbage can hold more than `want`: the parser refuses it once it passes MAX_INLINE.
            buf.reserve(want.saturating_sub(buf.len()));
            if rd.read_buf(&mut buf).await? == 0 {
                return Ok(());
            }
        }
    }

    async fn execute(&self, conn: &mut Conn, args: Vec<Bytes>) -> Outcome {
        let index = command_index(&args[0]);
        let name = index.map_or("", |i| COMMANDS[i].0);
        if !conn.authed && !matches!(name, "auth" | "hello" | "quit") {
            return Value::err("NOAUTH Authentication required.").into();
        }
        let Some(index) = index else { return unknown(&args).into() };
        let started = std::time::Instant::now();
        let outcome = self.command(conn, name, args).await;
        let usec = started.elapsed().as_micros() as u64;
        self.stats.count(index, usec, outcome.value.is_error());
        outcome
    }

    async fn command(&self, conn: &mut Conn, name: &str, args: Vec<Bytes>) -> Outcome {
        let n = args.len();
        match name {
            "get" if n == 2 => self.keyed(&args[1..], |k| Call::Get(k[0].clone())).await,
            "set" if n == 3 => {
                if args[2].len() > self.limits.value {
                    return Value::err("ERR value too large").into();
                }
                self.keyed(&args[1..2], |k| Call::Set(k[0].clone(), args[2].clone())).await
            }
            "set" if n > 3 => Value::err("ERR syntax error").into(),
            "del" | "exists" | "mget" if n >= 2 => {
                let call = |keys: &[Bytes]| match name {
                    "del" => Call::Del(keys.to_vec()),
                    "exists" => Call::Exists(keys.to_vec()),
                    _ => Call::MGet(keys.to_vec()),
                };
                self.keyed(&args[1..], call).await
            }
            "mset" if n >= 3 && n % 2 == 1 => {
                if args[2..].iter().step_by(2).any(|v| v.len() > self.limits.value) {
                    return Value::err("ERR value too large").into();
                }
                let keys: Vec<Bytes> = args[1..].iter().step_by(2).cloned().collect();
                let pairs: Vec<(Bytes, Bytes)> = args[1..].chunks(2).map(|p| (p[0].clone(), p[1].clone())).collect();
                self.keyed(&keys, move |_| Call::MSet(pairs)).await
            }
            // As Redis in cluster mode: the keys' slots are checked before the pairs.
            "mset" if n >= 3 && args[1..].iter().step_by(2).any(|k| key_slot(k) != key_slot(&args[1])) => {
                crossslot().into()
            }
            "dbsize" if n == 1 => self.every_shard(Call::Len).await,
            "flushdb" | "flushall" if n == 1 || (n == 2 && matches!(lower(&args[1]).as_str(), "async" | "sync")) => {
                self.every_shard(Call::Clear).await
            }
            "flushdb" | "flushall" if n == 2 => Value::err("ERR syntax error").into(),
            "ping" if n == 1 => Value::Simple("PONG".into()).into(),
            "ping" if n == 2 => Value::Bulk(Some(args[1].clone())).into(),
            "echo" if n == 2 => Value::Bulk(Some(args[1].clone())).into(),
            "quit" => Outcome { value: Value::ok(), close: true, held: None },
            "select" if n == 2 => match int(&args[1]) {
                Some(0) => Value::ok().into(),
                Some(_) => Value::err("ERR DB index is out of range").into(),
                None => Value::err("ERR value is not an integer or out of range").into(),
            },
            "auth" if n == 2 || n == 3 => self.auth(conn, &args).into(),
            "hello" => unknown(&args).into(),
            "readonly" if n == 1 => Value::ok().into(),
            "client" if n >= 2 => match lower(&args[1]).as_str() {
                "setname" if n == 3 => Value::ok().into(),
                "setinfo" if n == 4 => Value::ok().into(),
                sub @ ("setname" | "setinfo") => arity(&format!("client|{sub}")).into(),
                _ => subcommand(&args[1], "CLIENT").into(),
            },
            "command" if n == 1 => command_table().into(),
            "command" => Value::Array(Some(Vec::new())).into(),
            "config" if n >= 2 => match lower(&args[1]).as_str() {
                // What redis-benchmark asks: kv keeps nothing on disk in memory mode. Other names match nothing.
                "get" if n >= 3 => {
                    let found = |name: &'static str, value: &'static str| {
                        Value::Array(Some(vec![Value::bulk(name), Value::bulk(value)]))
                    };
                    match lower(&args[2]).as_str() {
                        "save" => found("save", ""),
                        "appendonly" => found("appendonly", "no"),
                        _ => Value::Array(Some(Vec::new())),
                    }
                    .into()
                }
                "get" => arity("config|get").into(),
                _ => subcommand(&args[1], "CONFIG").into(),
            },
            "info" if n == 2 && lower(&args[1]) == "kvdigest" => self.digest().await.into(),
            "info" if n <= 2 => {
                let section = args.get(1).map(|s| lower(s)).unwrap_or_default();
                Value::bulk(self.info(&section)).into()
            }
            "cluster" if n >= 2 => self.cluster(&args).into(),
            _ => arity(name).into(),
        }
    }

    fn auth(&self, conn: &mut Conn, args: &[Bytes]) -> Value {
        let Some(password) = &self.password else {
            if args.len() == 2 {
                return Value::err(
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure \
                     your configuration is correct?",
                );
            }
            return Value::err("WRONGPASS invalid username-password pair or user is disabled.");
        };
        let (user, given) = if args.len() == 3 { (&args[1][..], &args[2]) } else { (&b"default"[..], &args[1]) };
        if user == b"default" && given == password.as_bytes() {
            conn.authed = true;
            Value::ok()
        } else {
            Value::err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }

    /// A call on keys that must share a slot.
    async fn keyed(&self, keys: &[Bytes], call: impl FnOnce(&[Bytes]) -> Call) -> Outcome {
        if keys.iter().any(|k| k.len() > self.limits.key) {
            return Value::err("ERR key too large").into();
        }
        let slot = key_slot(&keys[0]);
        if keys[1..].iter().any(|k| key_slot(k) != slot) {
            return crossslot().into();
        }
        let call = call(keys);
        if call.encoded_len() > self.limits.command + 64 {
            return Value::err("ERR value too large").into();
        }
        match self.dispatch(shard_of_slot(slot), call).await {
            Ok((answer, held)) => Outcome { value: to_value(answer), close: false, held },
            Err(value) => value.into(),
        }
    }

    async fn dispatch(&self, shard: u16, call: Call) -> Result<(Answer, Option<Held>), Value> {
        match &self.dispatch {
            Dispatch::Shards(shards) => shards.call(shard, call, &self.budget).await,
            Dispatch::Stub(value) => Ok((stub(call, value), None)),
        }
    }

    /// `DBSIZE` and `FLUSHDB`: every shard at once. Not a snapshot, and not atomic across shards.
    async fn every_shard(&self, call: Call) -> Outcome {
        let answers = join_all((0..SHARDS).map(|s| self.dispatch(s, call.clone()))).await;
        let mut total = 0;
        for answer in answers {
            match answer {
                Ok((Answer::Count(n), _)) => total += n,
                Ok(_) => {}
                Err(e) => return e.into(),
            }
        }
        match call {
            Call::Len => Value::Int(total as i64).into(),
            _ => Value::ok().into(),
        }
    }

    fn cluster(&self, args: &[Bytes]) -> Value {
        let n = args.len();
        match lower(&args[1]).as_str() {
            "keyslot" if n == 3 => Value::Int(i64::from(key_slot(&args[2]))),
            "myid" if n == 2 => Value::bulk(self.info.id.clone()),
            "slots" if n == 2 => Value::Array(Some(
                self.slots()
                    .into_iter()
                    .map(|(first, last, id, addr)| {
                        Value::Array(Some(vec![
                            Value::Int(i64::from(first)),
                            Value::Int(i64::from(last)),
                            Value::Array(Some(vec![
                                Value::bulk(addr.ip().to_string()),
                                Value::Int(i64::from(addr.port())),
                                Value::bulk(id),
                            ])),
                        ]))
                    })
                    .collect(),
            )),
            "info" if n == 2 => {
                let nodes = self.nodes();
                let assigned: u32 = nodes.iter().flat_map(|n| &n.2).map(|(a, b)| u32::from(b - a) + 1).sum();
                let state = if assigned == 16384 { "ok" } else { "fail" };
                Value::bulk(format!(
                    "cluster_enabled:1\r\ncluster_state:{state}\r\ncluster_slots_assigned:{assigned}\r\n\
                     cluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
                     cluster_known_nodes:{}\r\ncluster_size:{}\r\n",
                    self.members(),
                    nodes.len()
                ))
            }
            // What redis-benchmark --cluster reads: one line per node that owns slots, every node a master.
            "nodes" if n == 2 => {
                let mut out = String::new();
                for (id, addr, ranges) in self.nodes() {
                    let flags = if id == self.info.id { "myself,master" } else { "master" };
                    let ranges: Vec<String> = ranges
                        .iter()
                        .map(|&(first, last)| if first == last { first.to_string() } else { format!("{first}-{last}") })
                        .collect();
                    out.push_str(&format!("{id} {addr}@0 {flags} - 0 0 0 connected {}\n", ranges.join(" ")));
                }
                Value::bulk(out)
            }
            // As Redis 7.4 (tests/conformance.rs): a shard per node that owns slots, each a master with no replicas,
            // its fields in Redis's order. kv keeps no replication offset; a node's Log is not a replica's stream.
            "shards" if n == 2 => Value::Array(Some(
                self.nodes()
                    .into_iter()
                    .map(|(id, addr, ranges)| {
                        let slots = ranges.iter().flat_map(|&(a, b)| [Value::Int(a.into()), Value::Int(b.into())]);
                        let ip = addr.ip().to_string();
                        let node = vec![
                            Value::bulk("id"),
                            Value::bulk(id),
                            Value::bulk("port"),
                            Value::Int(i64::from(addr.port())),
                            Value::bulk("ip"),
                            Value::bulk(ip.clone()),
                            Value::bulk("endpoint"),
                            Value::bulk(ip),
                            Value::bulk("role"),
                            Value::bulk("master"),
                            Value::bulk("replication-offset"),
                            Value::Int(0),
                            Value::bulk("health"),
                            Value::bulk("online"),
                        ];
                        Value::Array(Some(vec![
                            Value::bulk("slots"),
                            Value::Array(Some(slots.collect())),
                            Value::bulk("nodes"),
                            Value::Array(Some(vec![Value::Array(Some(node))])),
                        ]))
                    })
                    .collect(),
            )),
            sub @ ("keyslot" | "myid" | "slots" | "info" | "nodes" | "shards") => arity(&format!("cluster|{sub}")),
            _ => subcommand(&args[1], "CLUSTER"),
        }
    }

    /// Slot ranges and the node each shard's owner is, by this node's view, with its RESP address.
    fn slots(&self) -> Vec<(u16, u16, String, SocketAddr)> {
        let owners: Vec<Option<(String, SocketAddr)>> = match &self.dispatch {
            Dispatch::Stub(_) => vec![Some((self.info.id.clone(), self.info.resp)); usize::from(SHARDS)],
            Dispatch::Shards(shards) => {
                let view = shards.cluster.view();
                let resp_of = |id: &str| {
                    view.members
                        .iter()
                        .find(|m| m.record.id == id)
                        .and_then(|m| m.record.metadata.get("kv.resp"))
                        .and_then(|a| a.parse().ok())
                };
                (0..SHARDS)
                    .map(|s| {
                        let owner = shards.cluster.owner("shard", &shard_key(s), &Placement::default())?;
                        let addr = if *owner == *self.info.id { Some(self.info.resp) } else { resp_of(&owner) }?;
                        Some((owner.to_string(), addr))
                    })
                    .collect()
            }
        };
        let mut ranges: Vec<(u16, u16, String, SocketAddr)> = Vec::new();
        for (s, owner) in owners.into_iter().enumerate() {
            let Some((id, addr)) = owner else { continue };
            let (first, last) = (s as u16 * SLOTS_PER_SHARD, (s as u16 + 1) * SLOTS_PER_SHARD - 1);
            match ranges.last_mut() {
                Some(range) if range.2 == id && range.1 + 1 == first => range.1 = last,
                _ => ranges.push((first, last, id, addr)),
            }
        }
        ranges
    }

    /// The nodes that own slots, in the order `slots` first names them, each with its slot ranges.
    fn nodes(&self) -> Vec<SlotOwner> {
        let mut nodes: Vec<SlotOwner> = Vec::new();
        for (first, last, id, addr) in self.slots() {
            match nodes.iter_mut().find(|(i, ..)| *i == id) {
                Some(node) => node.2.push((first, last)),
                None => nodes.push((id, addr, vec![(first, last)])),
            }
        }
        nodes
    }

    fn members(&self) -> usize {
        match &self.dispatch {
            Dispatch::Stub(_) => 1,
            Dispatch::Shards(shards) => shards.cluster.view().members.len(),
        }
    }

    fn info(&self, section: &str) -> String {
        let all = matches!(section, "all" | "everything");
        let default = section.is_empty() || section == "default";
        let mut out = String::new();
        if all || default || section == "server" {
            out.push_str(&format!(
                "# Server\r\nredis_version:7.0.0-kv\r\nredis_mode:cluster\r\nkv_version:{}\r\nprocess_id:{}\r\n\
                 tcp_port:{}\r\nuptime_in_seconds:{}\r\n\r\n",
                env!("CARGO_PKG_VERSION"),
                std::process::id(),
                self.info.resp.port(),
                self.started.elapsed().as_secs()
            ));
        }
        if all || default || section == "clients" {
            out.push_str(&format!(
                "# Clients\r\nconnected_clients:{}\r\ntotal_connections_received:{}\r\nrejected_connections:{}\r\n\r\n",
                self.stats.connections.load(Relaxed),
                self.stats.connections_total.load(Relaxed),
                self.stats.connections_refused.load(Relaxed)
            ));
        }
        if all || default || section == "kv" {
            out.push_str(&self.kv_section());
        }
        if all || section == "commandstats" {
            out.push_str(&self.stats.commandstats());
            out.push_str("\r\n");
        }
        if section == "kvhosted" {
            out.push_str(&self.hosted());
        }
        out
    }

    /// `INFO kvhosted`: a line per shard running here, `s<shard>:<node>@<lease epoch>`. A shard that moves, or starts
    /// again here under a new lease, shows a new line: `CLUSTER SLOTS` gives only the node each shard is asked on.
    fn hosted(&self) -> String {
        let Dispatch::Shards(shards) = &self.dispatch else { return String::new() };
        let (keys, _) = shards.cluster.hosted_keys("shard", "", None, usize::from(SHARDS));
        keys.iter().map(|k| format!("s{}:{}@{}\r\n", k.key, self.info.id, k.epoch.unwrap_or(0))).collect()
    }

    /// `INFO kvdigest`: a line per shard of this node's Log, `s<shard>:<committed index>:<hash of the map>`.
    async fn digest(&self) -> Value {
        let Some(log) = &self.info.log else { return Value::err("ERR no Log here: the node is not durable") };
        match log.ask(LogMsg::Digest).await {
            Ok(shards) => Value::bulk(
                shards.iter().enumerate().map(|(s, (c, h))| format!("s{s}:{c}:{h:016x}\r\n")).collect::<String>(),
            ),
            Err(e) => Value::err(format!("ERR the Log did not answer: {e}")),
        }
    }

    fn kv_section(&self) -> String {
        let mut out = format!(
            "# Kv\r\nnode:{}\r\nmode:{}\r\nstorage_set:{}\r\nshards:{SHARDS}\r\nshard_starts:{}\r\n",
            self.info.id,
            self.info.mode,
            self.info.nodes.join(","),
            self.stats.shard_starts.load(Relaxed)
        );
        let lease = &self.info.lease;
        if !lease.lease.is_zero() {
            out.push_str(&format!(
                "lease_ms:{}\r\nlease_hold_ms:{}\r\nlease_grace_ms:{}\r\nlease_renew_ms:{}\r\n",
                lease.lease.as_millis(),
                lease.hold.as_millis(),
                lease.grace.as_millis(),
                lease.renew.as_millis()
            ));
        }
        if self.info.mode == "memory" {
            out.push_str(&format!("empty_starts:{}\r\n", self.stats.shard_starts.load(Relaxed)));
        } else if self.info.mode == "durable" {
            // What bench.sh divides by the fsyncs strace counts: entries per write is entries per sync.
            let wal = &self.stats.wal;
            let (writes, entries) = (wal.writes.load(Relaxed), wal.entries.load(Relaxed));
            out.push_str(&format!(
                "wal_writes:{writes}\r\nwal_failed_writes:{}\r\nwal_entries:{entries}\r\nentries_per_write:{:.2}\r\n\
                 wal_bytes:{}\r\nwal_segments:{}\r\nsnapshots:{}\r\nsnapshot_bytes:{}\r\n",
                wal.failed_writes.load(Relaxed),
                entries as f64 / writes.max(1) as f64,
                wal.bytes.load(Relaxed),
                wal.segments.load(Relaxed),
                wal.snaps.load(Relaxed),
                wal.snap_bytes.load(Relaxed),
            ));
            let snapshot = self.stats.recovery.snapshot();
            let recovery = Histogram::from(&snapshot);
            let ms = |p| recovery.percentile(p).map_or(0, |d| d.as_millis());
            out.push_str(&format!(
                "recoveries:{}\r\nrecovery_p50_ms:{}\r\nrecovery_p99_ms:{}\r\nrecovery_max_ms:{}\r\nlog_uploads:{}\r\n",
                snapshot.count(),
                ms(0.5),
                ms(0.99),
                self.stats.recovery_max_ms.load(Relaxed),
                self.stats.log_uploads.load(Relaxed),
            ));
            let rtt = Histogram::from(&self.stats.append_rtt.snapshot());
            let us = |p| rtt.percentile(p).map_or(0, |d| d.as_micros());
            out.push_str(&format!(
                "reads:{}\r\nappend_rtt_p50_us:{}\r\nappend_rtt_p99_us:{}\r\nowner_kept_bytes:{}\r\nowner_installs:{}\r\n",
                self.info.reads,
                us(0.5),
                us(0.99),
                self.stats.owner_kept_bytes.load(Relaxed),
                self.stats.owner_installs.load(Relaxed),
            ));
        }
        if let Dispatch::Shards(shards) = &self.dispatch {
            let hosted = shards.cluster.hosted(0);
            if let Some(kind) = hosted.kinds.iter().find(|k| k.kind == "shard") {
                out.push_str(&format!("shards_here:{}\r\n", kind.count));
                for (how, n) in axon_cluster::KEYED_STARTS.iter().zip(kind.started) {
                    out.push_str(&format!("started_by_{how}:{n}\r\n"));
                }
                for (why, n) in axon_cluster::KEYED_ENDS.iter().zip(kind.ended) {
                    out.push_str(&format!("ended_{why}:{n}\r\n"));
                }
            }
        }
        out.push_str(&format!(
            "uploads:{}\r\nparts_replies:{}\r\nops:{}\r\n\r\n",
            self.stats.uploads.load(Relaxed),
            self.stats.parts_replies.load(Relaxed),
            self.stats.calls.iter().map(|c| c.load(Relaxed)).sum::<u64>()
        ));
        out
    }
}

fn subcommand(sub: &[u8], command: &str) -> Value {
    Value::err(format!("ERR unknown subcommand '{}'. Try {command} HELP.", String::from_utf8_lossy(sub)))
}

fn to_value(answer: Answer) -> Value {
    match answer {
        Answer::Ok => Value::ok(),
        Answer::Value(v) => Value::Bulk(v),
        Answer::Count(n) => Value::Int(n as i64),
        Answer::Values(vs) => Value::Array(Some(vs.into_iter().map(Value::Bulk).collect())),
        Answer::Refused(text) => Value::Error(text),
        Answer::TooLarge(_) | Answer::Chunk(_) => Value::err("ERR an answer out of place"),
    }
}

fn stub(call: Call, value: &Bytes) -> Answer {
    match call {
        Call::Get(_) => Answer::Value(Some(value.clone())),
        Call::MGet(keys) => Answer::Values(keys.iter().map(|_| Some(value.clone())).collect()),
        Call::Del(_) | Call::Exists(_) | Call::Len => Answer::Count(0),
        _ => Answer::Ok,
    }
}

async fn flush(wr: &mut OwnedWriteHalf, out: &mut Out) -> std::io::Result<()> {
    for chunk in out.take() {
        timeout(WRITE_STALL, wr.write_all(&chunk))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "a client that does not read"))??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axon_net::SubmitReason;

    fn text(v: Value) -> String {
        match v {
            Value::Error(s) => s,
            other => panic!("{other:?}"),
        }
    }

    fn call(e: CallError) -> ClusterError {
        ClusterError::Call(e)
    }

    // One test per error.
    #[test]
    fn a_write_that_may_have_run_is_outcome_unknown_and_never_sent_again() {
        for e in [CallError::Timeout, CallError::OutcomeUnknown, CallError::NoReply] {
            assert_eq!(text(map_error(&call(e.clone()), true, 7, true)), "ERR outcome unknown");
            assert!(!again(&call(e), true), "a write is never resent after it may have run");
        }
    }

    #[test]
    fn a_read_that_may_have_run_is_tried_again_then_tryagain() {
        for e in [CallError::Timeout, CallError::OutcomeUnknown, CallError::NoReply] {
            assert!(again(&call(e.clone()), false));
            assert_eq!(text(map_error(&call(e), false, 7, true)), "TRYAGAIN shard 7");
        }
    }

    #[test]
    fn what_did_not_run_is_tried_again_then_moving() {
        let errors = [
            call(CallError::NotSent(SubmitReason::Full)),
            call(CallError::NotSent(SubmitReason::Deadline)),
            call(CallError::Rejected(Rejection::Stopped)),
            call(CallError::Rejected(Rejection::UnknownActor)),
            call(CallError::StaleNode),
            ClusterError::Spawn(SpawnRefusal::Held, String::new()),
            ClusterError::Spawn(SpawnRefusal::NotOwner, String::new()),
        ];
        for e in errors {
            assert!(again(&e, true) && again(&e, false), "{e:?}");
            assert_eq!(text(map_error(&e, true, 3, true)), "TRYAGAIN shard 3 moving", "{e:?}");
        }
    }

    #[test]
    fn no_majority_is_clusterdown() {
        let logs = ClusterError::Spawn(SpawnRefusal::Failed, "KV-NOQUORUM 1 of 3 logs answered".into());
        assert_eq!(text(map_error(&logs, true, 1, true)), "CLUSTERDOWN shard 1 no majority of logs");
        assert!(!again(&logs, false));
        let acceptors = ClusterError::Spawn(SpawnRefusal::Isolated, String::new());
        assert_eq!(text(map_error(&acceptors, false, 1, true)), "CLUSTERDOWN shard 1 no majority of acceptors");
        assert!(again(&acceptors, false), "until the deadline");
    }

    #[test]
    fn moving_without_a_majority_of_acceptors_is_clusterdown() {
        let held = ClusterError::Spawn(SpawnRefusal::Held, String::new());
        for e in [call(CallError::Rejected(Rejection::Stopped)), held] {
            assert_eq!(text(map_error(&e, true, 2, false)), "CLUSTERDOWN shard 2 no majority of acceptors", "{e:?}");
        }
        let unknown = call(CallError::Timeout);
        assert_eq!(text(map_error(&unknown, true, 2, false)), "ERR outcome unknown", "a write that may have run");
    }

    #[test]
    fn full_is_busy() {
        for e in [call(CallError::Rejected(Rejection::Full)), call(CallError::Busy)] {
            assert!(again(&e, true));
            assert_eq!(text(map_error(&e, true, 2, true)), "BUSY shard 2 is full");
        }
    }

    #[test]
    fn oversized_is_value_too_large() {
        let e = call(CallError::NotSent(SubmitReason::Oversized));
        assert!(!again(&e, true));
        assert_eq!(text(map_error(&e, true, 2, true)), "ERR value too large");
    }

    #[test]
    fn unknown_commands_in_redis_words() {
        let args: Vec<Bytes> = ["HSET", "a", "f", "v"].iter().map(|s| Bytes::from(*s)).collect();
        assert_eq!(text(unknown(&args)), "ERR unknown command 'HSET', with args beginning with: 'a' 'f' 'v' ");
        assert!(text(unknown(&[Bytes::from("HELLO"), Bytes::from("3")])).starts_with("ERR unknown command 'HELLO'"));
    }

    /// The largest call that goes in one frame, and the smallest that goes as an upload, at the frame's edge.
    #[test]
    fn a_call_at_the_frame_goes_whole_and_one_byte_over_goes_in_parts() {
        let limits = Limits::default();
        let at = |n: usize| Call::Set(Bytes::from("k"), Bytes::from(vec![0; n]));
        let fits = limits.frame_body - at(0).encoded_len();
        assert_eq!(at(fits).encoded_len(), limits.frame_body);
        assert_eq!(at(fits).encode().len(), limits.frame_body);
        assert!(at(fits + 1).encoded_len() > limits.frame_body);
        // A frame body the transport accepts, with room for the deadline and trace context axon adds.
        assert!(
            limits.frame_body + 64
                <= axon_net::Config::new("n", axon_net::SharedSecret::new("0123456789abcdef").unwrap())
                    .lanes
                    .live
                    .limits
                    .max_frame
        );
    }
}
