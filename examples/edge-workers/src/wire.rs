//! What crosses nodes: the manifest records on the `deploy/manifests` stream, the registry's contract and the
//! bundle fetch.

use std::collections::BTreeMap;
use std::fmt;

use axon::{HistogramSnapshot, TraceContext};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use axon_stream::{StreamKey, keyed, split_keyed};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::{Env, Limits, Objects};

/// The name the control node publishes its registry under.
pub const REGISTRY: &str = "registry";
/// And its bundle store.
pub const BUNDLES: &str = "bundles";

/// Every manifest, each under 1000 bytes as a record, fits the one snapshot a follower starts from.
pub const MAX_WORKERS: usize = 512;
/// A name is a host label: 1 to 63 of `a-z`, `0-9` and `-`, no `-` at either end.
pub const MAX_NAME: usize = 63;
/// A source travels in one request, under the transport's 1 MiB frame.
pub const MAX_SOURCE: usize = 512 << 10;
/// Variables and secrets of one version, keys and values together.
pub const MAX_ENV: usize = 64 << 10;
/// A prepared bundle. StarlingMonkey's are 12 MB.
pub const MAX_BUNDLE: u64 = 64 << 20;
/// Versions of a worker the registry keeps, the newest and the deployed ones.
pub const MAX_VERSIONS: usize = 100;

/// The one compacted stream every gateway and runner follows: a record per worker, keyed by its name.
pub fn manifests() -> StreamKey {
    StreamKey::new("deploy", "manifests")
}

/// SHA-256, in hex: what a bundle or a source is stored and asked for by.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hash(String);

impl Hash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect())
    }

    /// A hash read back from a file's name.
    pub fn of_hex(hex: &str) -> Self {
        Self(hex.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether it can be a file name: a hash that came over the wire is checked before it names one.
    pub fn is_valid(&self) -> bool {
        self.0.len() == 64 && self.0.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0[..self.0.len().min(12)])
    }
}

/// A version as a node needs it to serve: which bundle, and under what limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRef {
    pub id: u32,
    pub bundle: Hash,
    pub size: u64,
    pub limits: Limits,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Canary {
    pub version: VersionRef,
    /// Of every 100 requests, how many it takes: 1 to 99.
    pub percent: u8,
}

/// A worker's deployment. `seq` grows with every deployment the registry makes, of any worker, a rollback
/// included: a node applies a manifest only over a lower one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub seq: u64,
    pub stable: VersionRef,
    pub canary: Option<Canary>,
}

impl Manifest {
    pub fn record(&self) -> Bytes {
        keyed(self.name.as_bytes(), &serde_json::to_vec(self).expect("a manifest is plain data"))
    }

    /// The record that removes a worker.
    pub fn removal(name: &str) -> Bytes {
        keyed(name.as_bytes(), b"")
    }

    /// The worker a record is about, and its manifest unless the record removes it. `None` for a record that is
    /// not one of ours.
    pub fn parse(record: &Bytes) -> Option<(String, Option<Manifest>)> {
        let (key, value) = split_keyed(record)?;
        let name = String::from_utf8(key.to_vec()).ok()?;
        if value.is_empty() {
            return Some((name, None));
        }
        let manifest: Manifest = serde_json::from_slice(&value).ok()?;
        (manifest.name == name).then_some((name, Some(manifest)))
    }

    /// The version a request runs on: the canary for `percent` of every 100 values of `sample`.
    pub fn pick(&self, sample: u64) -> &VersionRef {
        match &self.canary {
            Some(canary) if sample % 100 < u64::from(canary.percent) => &canary.version,
            _ => &self.stable,
        }
    }

    pub fn versions(&self) -> impl Iterator<Item = &VersionRef> {
        std::iter::once(&self.stable).chain(self.canary.as_ref().map(|c| &c.version))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Upload {
    pub name: String,
    /// `None` keeps the source of the version it builds on: a change of variables or secrets alone.
    pub source: Option<String>,
    /// The variables whole. `None` keeps those of the version it builds on.
    pub vars: Option<Env>,
    /// On top of those variables: `Some` sets one, `None` removes it. The registry applies it to the version it
    /// builds on when the upload arrives, so two changes of two variables both hold.
    pub set: BTreeMap<String, Option<String>>,
    /// On top of that version's secrets, likewise. A name set here or in `set` is that from then on: a secret set
    /// takes the name from the variables, a variable set takes it from the secrets.
    pub secrets: BTreeMap<String, Option<String>>,
    /// The object bindings whole: the name `env` carries, and the exported class behind it. `None` keeps those of
    /// the version it builds on.
    pub objects: Option<Objects>,
    pub limits: Option<Limits>,
    pub message: String,
    /// Make it the stable version at once, at 100 %.
    pub deploy: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Call {
    /// A new immutable version, of a new worker if the name is.
    Upload(Upload),
    /// Which versions take the traffic. A rollback is this with an older `stable`.
    Deploy {
        name: String,
        stable: u32,
        canary: Option<(u32, u8)>,
    },
    Delete {
        name: String,
    },
    List,
    Get {
        name: String,
    },
    Version {
        name: String,
        id: u32,
    },
    /// Runs a source once, where the registry is, and keeps nothing of it.
    Preview(PreviewAsk),
}

/// Unsaved code and one request for it. It runs with the variables of the version the worker's next upload would
/// build on, if the worker exists, under `env`; with none of its secrets, which unsaved code could hand back.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviewAsk {
    pub name: String,
    pub source: String,
    pub env: Env,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// What a preview's one request did. `error` says why there is no response of the worker's own.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Previewed {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// As text, and no more than `MAX_PREVIEW_BODY` of it.
    pub body: String,
    pub body_truncated: bool,
    pub logs: Vec<String>,
    pub cpu_us: u64,
    pub wall_us: u64,
    pub error: Option<String>,
}

/// A preview's request body and what is shown of its response: both travel in one frame with the source or the logs.
pub const MAX_PREVIEW_BODY: usize = 128 << 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// `seq` when the upload also deployed it.
    Uploaded {
        version: u32,
        seq: Option<u64>,
    },
    Deployed {
        seq: u64,
    },
    Deleted,
    Workers(Vec<WorkerInfo>),
    Worker(WorkerInfo, Vec<VersionInfo>),
    Version(VersionDetail),
    Previewed(Previewed),
    Refused(Refused),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refused {
    NotFound,
    NoSuchVersion(u32),
    /// A bad name, an oversized source or environment, a percent outside 1 to 99.
    Invalid(String),
    /// The engine did not take the source: a syntax error, no `fetch`, a top level that threw.
    Load(String),
    /// Another upload of this worker is being prepared, the worker changed while this one was, or the registry
    /// has as much in hand as it takes.
    Busy,
    Storage(String),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::NotFound => f.write_str("no such worker"),
            Refused::NoSuchVersion(id) => write!(f, "no version {id}"),
            Refused::Invalid(why) => write!(f, "invalid: {why}"),
            Refused::Load(why) => write!(f, "the source did not load: {why}"),
            Refused::Busy => f.write_str("busy, try again"),
            Refused::Storage(why) => write!(f, "storage: {why}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    pub seq: u64,
    pub stable: u32,
    pub canary: Option<(u32, u8)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub name: String,
    /// `None` until a version is deployed.
    pub deployment: Option<Deployment>,
    pub latest: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub id: u32,
    pub created_ms: u64,
    pub message: String,
    pub bundle: Hash,
    /// The bundle's length: what the engine built from the source.
    pub size: u64,
    /// The source's length.
    pub source_bytes: u64,
    pub limits: Limits,
}

/// A secret's value never leaves the registry this way: only its name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionDetail {
    pub info: VersionInfo,
    pub source: String,
    pub vars: Env,
    pub secrets: Vec<String>,
    pub objects: Objects,
}

/// Answers a call, when it has a caller.
pub(crate) fn answer<R>(reply: Option<Reply<R>>, told: R) {
    // A caller that has gone, or an answer over a frame, leaves nothing to do here.
    drop(reply.map(|reply| reply.send(told)));
}

fn json<T: Serialize>(value: &T) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

fn unjson<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, CallError> {
    serde_json::from_slice(body).map_err(|e| CallError::Codec(e.to_string()))
}

pub struct RegistryApi;

impl WireProtocol for RegistryApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0xED6E_0001;
    const SCHEMA: u16 = 6;

    fn encode(call: Call) -> Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        unjson(&body)
    }
}

impl ActorProtocol for RegistryApi {
    type Mailbox = crate::registry::Msg;
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Self::Mailbox, Rejection> {
        let call = serde_json::from_slice(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(crate::registry::Msg::Call(call, reply))
    }
}

/// A bundle from `offset` on, answered in parts of at most `max` bytes: a cut fetch asks again from what it has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    pub hash: Hash,
    pub offset: u64,
    pub max: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Piece {
    Missing,
    /// `total` is the whole bundle's length; `bytes` are never none.
    Chunk {
        total: u64,
        bytes: Bytes,
    },
}

pub struct BundleApi;

impl WireProtocol for BundleApi {
    type Message = Fetch;
    type Reply = Piece;
    const ID: u64 = 0xED6E_0002;
    const SCHEMA: u16 = 2;

    fn encode(fetch: Fetch) -> Result<Bytes, CallError> {
        let mut out = BytesMut::with_capacity(12 + fetch.hash.0.len());
        out.put_u64(fetch.offset);
        out.put_u32(fetch.max);
        out.put_slice(fetch.hash.0.as_bytes());
        Ok(out.freeze())
    }
    fn decode_reply(mut body: Bytes) -> Result<Piece, CallError> {
        let short = |_| CallError::Codec("short piece".into());
        match body.try_get_u8().map_err(short)? {
            0 => Ok(Piece::Missing),
            _ => Ok(Piece::Chunk { total: body.try_get_u64().map_err(short)?, bytes: body }),
        }
    }
}

/// The header of a `Piece::Chunk` on the wire.
pub const PIECE_HEADER: usize = 9;

impl ActorProtocol for BundleApi {
    type Mailbox = crate::registry::BundleMsg;
    fn encode_reply(piece: Piece) -> Result<Bytes, CallError> {
        Ok(match piece {
            Piece::Missing => Bytes::from_static(&[0]),
            Piece::Chunk { total, bytes } => {
                let mut out = BytesMut::with_capacity(PIECE_HEADER + bytes.len());
                out.put_u8(1);
                out.put_u64(total);
                out.put_slice(&bytes);
                out.freeze()
            }
        })
    }
    fn decode(mut body: Bytes, reply: Option<Reply<Piece>>) -> Result<Self::Mailbox, Rejection> {
        let offset = body.try_get_u64().map_err(|_| Rejection::InvalidMessage)?;
        let max = body.try_get_u32().map_err(|_| Rejection::InvalidMessage)?;
        let hash = Hash(String::from_utf8(body.to_vec()).map_err(|_| Rejection::InvalidMessage)?);
        if !hash.is_valid() {
            return Err(Rejection::InvalidMessage);
        }
        Ok(crate::registry::BundleMsg::Fetch(Fetch { hash, offset, max }, reply))
    }
}

/// The name each runner publishes its invoker under.
pub const INVOKE: &str = "invoke";

/// Each runner's stream of invocation records, `logs/<node id>`: one origin a stream, so a runner's record costs
/// it no hop.
pub fn logs(node: &str) -> StreamKey {
    StreamKey::new(LOGS, node)
}

pub const LOGS: &str = "logs";

/// A request for a worker, as a gateway sends it to a runner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invocation {
    pub worker: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// Picks the canary for its percent of the values. A retry carries the same one.
    pub sample: u64,
    #[serde(skip)]
    pub body: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    Run(Invocation),
    Stats,
    /// The objects this runner hosts whose key starts with `prefix`, in key order from `from` on, `limit` at most.
    Objects {
        prefix: String,
        from: Option<String>,
        limit: u32,
    },
}

/// An object as its runner hosts it now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hosted {
    pub key: String,
    /// The lease it runs under, which its writes carry.
    pub epoch: Option<u64>,
}

/// A run that ended, with the worker's own response or the engine's failure as one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ran {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub version: u32,
    pub cpu_us: u64,
    pub wall_us: u64,
    /// Why there is no response of the worker's own: one of `OUTCOMES` but the first.
    pub failure: Option<String>,
    #[serde(skip)]
    pub body: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Told {
    Ran(Ran),
    /// Not run: the worker, or the node, has as many runs in hand as it takes. Another runner may take it.
    Busy,
    /// Not run: this runner does not serve the worker, or not yet.
    NotServed,
    Stats(NodeStats),
    /// And whether the runner hosts more than it listed.
    Objects(Vec<Hosted>, bool),
}

/// How a run ended, for the counters and the log record.
pub const OUTCOMES: [&str; 8] =
    ["ok", "exception", "cpu_limit", "memory_limit", "wall_timeout", "engine", "response_too_large", "storage"];

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStats {
    /// Runs that ended, by `OUTCOMES`.
    pub outcomes: [u64; OUTCOMES.len()],
    /// Requests refused at the worker's cap on this node.
    pub busy: u64,
    /// Times it was loaded again after it had gone idle.
    pub cold_starts: u64,
    pub in_flight: u64,
    pub cpu: HistogramSnapshot,
    pub wall: HistogramSnapshot,
}

/// A worker as a runner serves it now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Served {
    pub seq: u64,
    pub stable: u32,
    pub canary: Option<(u32, u8)>,
    /// Loaded in the engine; not after it went idle, until its next request.
    pub warm: bool,
}

/// One runner's counters since it started, and what it serves.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStats {
    pub node: String,
    pub workers: BTreeMap<String, WorkerStats>,
    pub serving: BTreeMap<String, Served>,
    /// Requests whose caller had given up before they were looked at.
    pub abandoned: u64,
    pub logs_published: u64,
    /// Records the stream refused.
    pub logs_dropped: u64,
}

/// One invocation, as its runner publishes it on `logs/<node>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub at_ms: u64,
    pub worker: String,
    pub version: u32,
    pub node: String,
    /// The trace the gateway started for the request, as in its `x-trace-id` header.
    pub trace: Option<String>,
    pub method: String,
    pub url: String,
    pub status: u16,
    /// One of `OUTCOMES`.
    pub outcome: String,
    pub cpu_us: u64,
    pub wall_us: u64,
    /// Console lines in order, cut at the runner's `log_bytes`.
    pub logs: Vec<String>,
    pub truncated: bool,
}

impl LogRecord {
    pub fn record(&self) -> Bytes {
        serde_json::to_vec(self).expect("a log record is plain data").into()
    }

    pub fn parse(record: &[u8]) -> Option<Self> {
        serde_json::from_slice(record).ok()
    }
}

pub fn trace_id(trace: TraceContext) -> String {
    format!("{:016x}", trace.trace)
}

/// A JSON head, then the body as it is: `[u32 head length][head][body]`.
fn with_body<T: Serialize>(tag: u8, head: &T, body: &Bytes) -> Result<Bytes, CallError> {
    let head = serde_json::to_vec(head).map_err(|e| CallError::Codec(e.to_string()))?;
    let mut out = BytesMut::with_capacity(5 + head.len() + body.len());
    out.put_u8(tag);
    out.put_u32(head.len() as u32);
    out.put_slice(&head);
    out.put_slice(body);
    Ok(out.freeze())
}

fn split_body<T: for<'de> Deserialize<'de>>(mut frame: Bytes) -> Option<(T, Bytes)> {
    let len = frame.try_get_u32().ok()? as usize;
    if frame.len() < len {
        return None;
    }
    let head = serde_json::from_slice(&frame.split_to(len)).ok()?;
    Some((head, frame))
}

/// What a `Ran` or an `Invocation` adds to its body on the wire, at most: the gateway and the runner keep a body
/// this much under the frame, and the headers within it.
pub const HEAD_ROOM: usize = 64 << 10;

/// Whether headers, with the rest of a head, fit `HEAD_ROOM` as the JSON they travel as.
pub fn headers_fit(headers: &[(String, String)]) -> bool {
    // The rest of a `Ran`'s head is a few numbers and a name.
    serde_json::to_vec(headers).is_ok_and(|json| json.len() + (1 << 10) <= HEAD_ROOM)
}

fn tagged<T: Serialize>(tag: u8, value: &T) -> Result<Bytes, CallError> {
    let mut out = vec![tag];
    serde_json::to_writer(&mut out, value).map_err(|e| CallError::Codec(e.to_string()))?;
    Ok(out.into())
}

fn encode_told(told: Told) -> Result<Bytes, CallError> {
    match told {
        Told::Ran(ran) => with_body(0, &ran, &ran.body),
        Told::Busy => Ok(Bytes::from_static(&[1])),
        Told::NotServed => Ok(Bytes::from_static(&[2])),
        Told::Stats(stats) => tagged(3, &stats),
        Told::Objects(hosted, more) => tagged(4, &(hosted, more)),
    }
}

fn decode_told(mut body: Bytes) -> Result<Told, CallError> {
    let bad = || CallError::Codec("not a runner's answer".into());
    match body.try_get_u8().map_err(|_| bad())? {
        0 => {
            let (ran, body): (Ran, Bytes) = split_body(body).ok_or_else(bad)?;
            Ok(Told::Ran(Ran { body, ..ran }))
        }
        1 => Ok(Told::Busy),
        2 => Ok(Told::NotServed),
        3 => serde_json::from_slice(&body).map(Told::Stats).map_err(|_| bad()),
        4 => serde_json::from_slice(&body).map(|(hosted, more)| Told::Objects(hosted, more)).map_err(|_| bad()),
        _ => Err(bad()),
    }
}

fn decode_run(body: Bytes) -> Result<Invocation, Rejection> {
    let (invocation, body): (Invocation, Bytes) = split_body(body).ok_or(Rejection::InvalidMessage)?;
    // The frame's buffer is the session's: a body kept across a run is copied out of it.
    Ok(Invocation { body: Bytes::copy_from_slice(&body), ..invocation })
}

pub struct InvokeApi;

impl WireProtocol for InvokeApi {
    type Message = Ask;
    type Reply = Told;
    const ID: u64 = 0xED6E_0003;
    const SCHEMA: u16 = 7;

    fn encode(ask: Ask) -> Result<Bytes, CallError> {
        match ask {
            Ask::Run(invocation) => with_body(0, &invocation, &invocation.body),
            Ask::Stats => Ok(Bytes::from_static(&[1])),
            Ask::Objects { prefix, from, limit } => tagged(2, &(prefix, from, limit)),
        }
    }
    fn decode_reply(body: Bytes) -> Result<Told, CallError> {
        decode_told(body)
    }
}

impl ActorProtocol for InvokeApi {
    type Mailbox = crate::invoker::Msg;
    fn encode_reply(told: Told) -> Result<Bytes, CallError> {
        encode_told(told)
    }
    fn decode(mut body: Bytes, reply: Option<Reply<Told>>) -> Result<Self::Mailbox, Rejection> {
        let ask = match body.try_get_u8() {
            Ok(0) => Ask::Run(decode_run(body)?),
            Ok(1) => Ask::Stats,
            Ok(2) => {
                let (prefix, from, limit) = serde_json::from_slice(&body).map_err(|_| Rejection::InvalidMessage)?;
                Ask::Objects { prefix, from, limit }
            }
            _ => return Err(Rejection::InvalidMessage),
        };
        Ok(crate::invoker::Msg::Ask(ask, reply))
    }
}

/// The kind an object is a keyed actor of, on the runners. Its key is `worker/class/id`.
pub const OBJECTS: &str = "objects";
/// The name the control node publishes the objects' storage under.
pub const STORE: &str = "object-store";
/// Bindings a version declares.
pub const MAX_BINDINGS: usize = 16;
/// With a worker's name and a class it makes a key under 256 bytes, which axon names the actor by.
pub const MAX_OBJECT_ID: usize = 128;
/// A storage key, and a value as the JSON text it is kept as. Neither holds a control character, so as a JSON string
/// on the wire each is at most twice as long: every byte a quote or a backslash.
pub const MAX_KEY: usize = 512;
pub const MAX_VALUE: usize = 64 << 10;
/// Everything one object stores: its keys, its values and `ENTRY_COST` an entry. An object's storage travels whole
/// in one frame when its actor starts, and a request's writes in another: a frame is 1 MiB, four times this.
pub const MAX_OBJECT_BYTES: usize = 256 << 10;
pub const ENTRY_COST: usize = 8;

pub fn object_key(worker: &str, class: &str, id: &str) -> String {
    format!("{worker}/{class}/{id}")
}

/// The worker, the class and the id. Neither a worker's name nor a class has a `/`; an id may.
pub fn split_object_key(key: &str) -> Option<(&str, &str, &str)> {
    let mut parts = key.splitn(3, '/');
    Some((parts.next()?, parts.next()?, parts.next()?))
}

/// One request to an object, from the runner whose worker called it. `worker` is the caller's own, which the
/// key repeats; `sample` is unused: an object runs the stable version.
pub struct ObjectApi;

impl WireProtocol for ObjectApi {
    type Message = Invocation;
    type Reply = Told;
    const ID: u64 = 0xED6E_0005;
    const SCHEMA: u16 = 3;

    fn encode(invocation: Invocation) -> Result<Bytes, CallError> {
        with_body(0, &invocation, &invocation.body)
    }
    fn decode_reply(body: Bytes) -> Result<Told, CallError> {
        decode_told(body)
    }
}

impl ActorProtocol for ObjectApi {
    type Mailbox = crate::object::Msg;
    fn encode_reply(told: Told) -> Result<Bytes, CallError> {
        encode_told(told)
    }
    fn decode(mut body: Bytes, reply: Option<Reply<Told>>) -> Result<Self::Mailbox, Rejection> {
        body.try_get_u8().map_err(|_| Rejection::InvalidMessage)?;
        Ok(crate::object::Msg::Ask(decode_run(body)?, reply))
    }
}

/// A put, or with `None` a delete.
pub type StoreOp = (String, Option<String>);

/// What the store is asked. An object's actor names the lease `epoch` it runs under in all it sends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreAsk {
    /// An object's actor started under `epoch`: from now on a write of an older one is refused. Answered with all
    /// the object stores, and the version that is.
    Open { object: String, epoch: u64 },
    /// One request's writes, all or none, onto the object as its actor read it: `base` is the version of its copy.
    /// With no `ops` nothing is written: the store only says whether the actor is the object's still, and its copy
    /// what is stored.
    Write { object: String, epoch: u64, base: u64, ops: Vec<StoreOp> },
    /// What the object stores, for an operator: no lease, nothing changed.
    Read { object: String },
    /// Removes what the object stores. The actor that runs it holds a copy of a version that is no more: the store
    /// answers its next request `Stale`, and it reads again.
    Clear { object: String },
    /// The objects that store something, by the prefix of their key, in key order after `after`.
    List { prefix: String, after: Option<String>, limit: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreTold {
    /// `version` counts the object's writes; 0 is an object that stores nothing.
    Snapshot {
        epoch: u64,
        version: u64,
        entries: Vec<(String, String)>,
    },
    /// On disk, or with no `ops` nothing to write: the object is at `version`.
    Written {
        version: u64,
    },
    /// The store has seen a newer epoch of this object: another node owns it now.
    Fenced {
        seen: u64,
    },
    /// The object is at `version`, not at the write's `base`: it was written, cleared or restored since the actor
    /// read it. Nothing was written.
    Stale {
        version: u64,
    },
    Objects(Vec<String>),
    /// Past a limit, or the disk failed. Nothing was written.
    Refused(String),
}

pub struct StoreApi;

impl WireProtocol for StoreApi {
    type Message = StoreAsk;
    type Reply = StoreTold;
    const ID: u64 = 0xED6E_0004;
    const SCHEMA: u16 = 2;

    fn encode(ask: StoreAsk) -> Result<Bytes, CallError> {
        json(&ask)
    }
    fn decode_reply(body: Bytes) -> Result<StoreTold, CallError> {
        unjson(&body)
    }
}

impl ActorProtocol for StoreApi {
    type Mailbox = crate::durable::Msg;
    fn encode_reply(told: StoreTold) -> Result<Bytes, CallError> {
        json(&told)
    }
    fn decode(body: Bytes, reply: Option<Reply<StoreTold>>) -> Result<Self::Mailbox, Rejection> {
        let ask = serde_json::from_slice(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(crate::durable::Msg::Ask(ask, reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(id: u32) -> VersionRef {
        VersionRef { id, bundle: Hash::of(&id.to_be_bytes()), size: 1, limits: Limits::default() }
    }

    #[test]
    fn a_manifest_survives_its_record_and_a_removal_is_told_apart() {
        let manifest = Manifest {
            name: "hello".into(),
            seq: 7,
            stable: version(1),
            canary: Some(Canary { version: version(2), percent: 10 }),
        };
        assert_eq!(Manifest::parse(&manifest.record()), Some(("hello".into(), Some(manifest.clone()))));
        assert_eq!(Manifest::parse(&Manifest::removal("hello")), Some(("hello".into(), None)));
        // A record whose key is not its manifest's name is not ours.
        let forged = keyed(b"other", &serde_json::to_vec(&manifest).unwrap());
        assert_eq!(Manifest::parse(&forged), None);
        assert_eq!(Manifest::parse(&Bytes::from_static(b"\x00")), None);
    }

    #[test]
    fn the_canary_takes_its_percent_of_the_samples() {
        let mut manifest = Manifest { name: "f".into(), seq: 1, stable: version(1), canary: None };
        assert!((0..1000).all(|s| manifest.pick(s).id == 1));
        manifest.canary = Some(Canary { version: version(2), percent: 10 });
        assert_eq!((0..1000).filter(|s| manifest.pick(*s).id == 2).count(), 100);
    }

    #[test]
    fn a_piece_and_a_fetch_survive_the_wire() {
        let fetch = Fetch { hash: Hash::of(b"x"), offset: 1 << 33, max: 4096 };
        let encoded = BundleApi::encode(fetch.clone()).unwrap();
        let crate::registry::BundleMsg::Fetch(decoded, None) = BundleApi::decode(encoded, None).unwrap() else {
            panic!("a fetch")
        };
        assert_eq!(decoded, fetch);
        for piece in [Piece::Missing, Piece::Chunk { total: 9, bytes: Bytes::from_static(b"abc") }] {
            assert_eq!(BundleApi::decode_reply(BundleApi::encode_reply(piece.clone()).unwrap()).unwrap(), piece);
        }
        // A hash that could name a file outside the store is refused before it reaches the actor.
        let mut bad = BytesMut::new();
        bad.put_u64(0);
        bad.put_u32(1);
        bad.put_slice(b"../../etc/passwd");
        assert!(matches!(BundleApi::decode(bad.freeze(), None), Err(Rejection::InvalidMessage)));
    }

    #[test]
    fn an_invocation_and_its_answers_survive_the_wire_with_their_bodies() {
        let invocation = Invocation {
            worker: "hello".into(),
            method: "POST".into(),
            url: "http://hello.example/x?y=1".into(),
            headers: vec![("a".into(), "b".into())],
            sample: 42,
            body: Bytes::from_static(b"\x00\xffbody"),
        };
        let encoded = InvokeApi::encode(Ask::Run(invocation.clone())).unwrap();
        let crate::invoker::Msg::Ask(decoded, None) = InvokeApi::decode(encoded, None).unwrap() else {
            panic!("an ask")
        };
        assert_eq!(decoded, Ask::Run(invocation));
        let ran = Ran {
            status: 201,
            headers: vec![("content-type".into(), "text/plain".into())],
            version: 3,
            cpu_us: 150,
            wall_us: 900,
            failure: None,
            body: Bytes::from_static(b"{not json"),
        };
        let mut stats = NodeStats { node: "r1".into(), ..NodeStats::default() };
        let mut cpu = axon::Histogram::default();
        cpu.record(std::time::Duration::from_micros(150));
        stats.workers.insert("hello".into(), WorkerStats { cpu: (&cpu).into(), ..WorkerStats::default() });
        stats.serving.insert("hello".into(), Served { seq: 4, stable: 2, canary: Some((3, 10)), warm: true });
        let hosted = vec![Hosted { key: "hello/Counter/a".into(), epoch: Some(7) }];
        for told in [Told::Ran(ran), Told::Busy, Told::NotServed, Told::Stats(stats), Told::Objects(hosted, true)] {
            assert_eq!(InvokeApi::decode_reply(InvokeApi::encode_reply(told.clone()).unwrap()).unwrap(), told);
        }
        let listing = Ask::Objects { prefix: "hello/".into(), from: Some("hello/Counter/a".into()), limit: 10 };
        let crate::invoker::Msg::Ask(decoded, None) =
            InvokeApi::decode(InvokeApi::encode(listing.clone()).unwrap(), None).unwrap()
        else {
            panic!("an ask")
        };
        assert_eq!(decoded, listing);
        // A head longer than the frame, and no head at all.
        assert!(matches!(
            InvokeApi::decode(Bytes::from_static(&[0, 0, 0, 0, 9, b'{']), None),
            Err(Rejection::InvalidMessage)
        ));
        assert!(InvokeApi::decode_reply(Bytes::from_static(&[0])).is_err());
    }

    #[test]
    fn an_object_call_survives_the_wire_and_a_key_splits_at_its_first_two_slashes() {
        let invocation = Invocation {
            worker: "rooms".into(),
            method: "POST".into(),
            url: "http://rooms.example/incr".into(),
            headers: vec![],
            sample: 0,
            body: Bytes::from_static(b"\x00body"),
        };
        let encoded = ObjectApi::encode(invocation.clone()).unwrap();
        let crate::object::Msg::Ask(decoded, None) = ObjectApi::decode(encoded, None).unwrap() else {
            panic!("an ask")
        };
        assert_eq!(decoded, invocation);
        let key = object_key("rooms", "Counter", "a/b");
        assert_eq!(split_object_key(&key), Some(("rooms", "Counter", "a/b")));
        assert_eq!(split_object_key("rooms/Counter"), None);
    }
}
