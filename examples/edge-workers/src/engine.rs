//! What runs a worker: an [`Engine`] turns source into a prepared version once, at deploy, and then runs each
//! request against it under [`Limits`]. [`starling::Starling`] is the real one (JavaScript on StarlingMonkey under
//! wasmtime); [`fake::FakeEngine`] is a deterministic stand-in for World tests.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, Context, JobExecutor, JobHandle, SubmitError};
use bytes::Bytes;

pub mod fake;
pub mod starling;

/// A worker's string bindings, the `env` its `fetch` receives. Changing one is a new version.
pub type Env = BTreeMap<String, String>;

/// A version's object bindings: the name `env` carries a namespace under, and the exported class behind it.
pub type Objects = BTreeMap<String, String>;

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// What a run reaches outside its own state, on the node it runs on.
pub trait Host: Send + Sync + 'static {
    /// One request to the object `id` of `class`, a class of the worker that runs. `Err` is thrown in the script.
    fn fetch(&self, class: String, id: String, request: Request) -> Boxed<Result<Response, String>>;
    /// The storage of the object that runs. `Err` is thrown in the script.
    fn storage(&self, op: StorageOp) -> Boxed<Result<StorageAnswer, String>>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum StorageOp {
    Get {
        key: String,
    },
    /// `value` is JSON text.
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    List {
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        limit: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageAnswer {
    Value(Option<String>),
    Done,
    Deleted(bool),
    Entries(Vec<(String, String)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// Where a run stands: as an object's request when `object` names its class and id, and with the `host` it
/// reaches objects and its storage through. Without a host both are errors in the script.
#[derive(Clone, Default)]
pub struct Scope {
    pub object: Option<(String, String)>,
    pub host: Option<Arc<dyn Host>>,
}

pub trait Engine: Send + Sync + 'static {
    /// A deployed version, ready to run. Cheap to clone; requests share it and nothing else.
    type Version: Clone + Send + Sync + 'static;

    /// Prepares `source` with `env`, running the module's top level once. Seconds of CPU on the real engine, which
    /// does it off the caller's thread: an actor calls it through `cx.submit_future`.
    fn deploy(&self, source: &str, env: &Env) -> impl Future<Output = Result<Self::Version, Failure>> + Send {
        async move { self.deploy_bound(source, env, &Objects::new()).await }
    }

    /// [`Engine::deploy`] with object bindings: each name in `env` is a namespace of the class it binds, and a
    /// class the module does not export fails the deploy.
    fn deploy_bound(
        &self,
        source: &str,
        env: &Env,
        objects: &Objects,
    ) -> impl Future<Output = Result<Self::Version, Failure>> + Send;

    /// What [`Engine::load`] on another node turns back into the same version without the source or the env.
    fn artifact(&self, version: &Self::Version) -> Bytes;

    /// Like [`Engine::deploy`], for less: the top level does not run again.
    fn load(&self, artifact: Bytes) -> impl Future<Output = Result<Self::Version, Failure>> + Send;

    /// Runs one request in a state of its own: nothing a request writes outlives it.
    fn invoke(
        &self,
        version: &Self::Version,
        request: Request,
        limits: Limits,
    ) -> impl Future<Output = Result<Outcome, Failed>> + Send {
        self.invoke_in(version, request, limits, Scope::default())
    }

    /// [`Engine::invoke`] in a `scope`: the default export's `fetch`, or the `fetch` of a new instance of the
    /// object's class. A request to `scope.host` does not count as CPU time while it waits.
    fn invoke_in(
        &self,
        version: &Self::Version,
        request: Request,
        limits: Limits,
        scope: Scope,
    ) -> impl Future<Output = Result<Outcome, Failed>> + Send;
}

/// Runs the engine's `work` in an actor's job, so a panic in it is that work's failure. Left to unwind, it would end
/// the actor whose job it is.
pub async fn guarded<T>(work: impl Future<Output = T>) -> Result<T, Failure> {
    axon::catch_panic(work).await.map_err(|panic| Failure::Runtime(format!("it panicked: {panic}")))
}

/// Submits an actor's job that runs the engine: on `jobs` when there is one, so a worker's CPU time is not taken from
/// the threads that serve the cluster's sessions, and with the actor's other jobs otherwise, as a World needs.
pub fn submit<A: Actor, W>(
    cx: &mut Context<'_, A>,
    jobs: Option<&JobExecutor>,
    work: W,
) -> Result<JobHandle, SubmitError<W>>
where
    W: Future<Output = A::Message> + Send + 'static,
{
    match jobs {
        Some(jobs) => cx.submit_future_on(jobs, work),
        None => cx.submit_future(work),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    /// Absolute (`http://host/path?query`), or a path, which is served as of `http://localhost`.
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl Request {
    pub fn get(url: &str) -> Self {
        Self { method: "GET".into(), url: url.into(), headers: Vec::new(), body: Bytes::new() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Limits {
    /// Time the worker may compute for. Time it waits (on a subrequest, a timer) does not count.
    pub cpu: Duration,
    /// Its whole linear memory, the engine's own included.
    pub memory: usize,
    pub wall: Duration,
    /// Outbound `fetch` calls per request.
    pub subrequests: u32,
    /// The response body. A runner holds it to what one reply carries, whatever a version asks for.
    pub response_bytes: usize,
    /// Console output kept per request; the rest is dropped.
    pub log_bytes: usize,
}

impl Default for Limits {
    /// Workers' own: 50 ms of CPU, 128 MB, 50 subrequests.
    fn default() -> Self {
        Self {
            cpu: Duration::from_millis(50),
            memory: 128 << 20,
            wall: Duration::from_secs(30),
            subrequests: 50,
            response_bytes: 16 << 20,
            log_bytes: 256 << 10,
        }
    }
}

/// What a run used, whether it answered or failed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage {
    /// Console lines in order, as the engine printed them (`Log: ..`, `Error: ..`), then its own report of a failure.
    pub logs: Vec<String>,
    pub cpu: Duration,
    pub wall: Duration,
    pub subrequests: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub usage: Usage,
}

/// A run that gave no response of the worker's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failed {
    pub failure: Failure,
    pub usage: Usage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    CpuLimit,
    MemoryLimit,
    WallTimeout,
    /// The handler threw, or what it returned was not a `Response`. The message; the stack is in the logs.
    Exception(String),
    /// The source did not load: a syntax error, a top level that threw or ran too long, no `fetch` to call.
    Load(String),
    /// The engine itself failed, not the worker.
    Runtime(String),
    /// The response body is over `Limits::response_bytes`, or its headers are more than a runner passes on.
    ResponseTooLarge,
    /// An object's request the store did not take, or did not answer: nothing a caller is told was left unstored.
    /// Not the engine's: the object's actor says it.
    Storage(String),
}

impl Failure {
    /// The status a gateway answers with: 500 for the worker's own fault, 503 where it ran out of a limit (as
    /// Workers' 1102 does), 504 past the wall time.
    pub fn status(&self) -> u16 {
        match self {
            Failure::Exception(_) | Failure::Load(_) | Failure::Runtime(_) | Failure::ResponseTooLarge => 500,
            Failure::CpuLimit | Failure::MemoryLimit | Failure::Storage(_) => 503,
            Failure::WallTimeout => 504,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::CpuLimit => f.write_str("exceeded its CPU time limit"),
            Failure::MemoryLimit => f.write_str("exceeded its memory limit"),
            Failure::WallTimeout => f.write_str("exceeded its wall time limit"),
            Failure::Exception(m) => write!(f, "threw: {m}"),
            Failure::Load(m) => write!(f, "did not load: {m}"),
            Failure::Runtime(m) => write!(f, "engine failure: {m}"),
            Failure::ResponseTooLarge => f.write_str("returned a response larger than a runner passes on"),
            Failure::Storage(m) => write!(f, "failed at its storage: {m}"),
        }
    }
}

impl std::error::Error for Failure {}
