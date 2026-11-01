//! An engine with no JavaScript in it, for World tests: the "source" is the name of a Rust closure, and a run
//! takes the simulated CPU the closure says, on tokio's clock, which a World drives.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::{Engine, Env, Failed, Failure, Limits, Objects, Outcome, Request, Scope, Usage};

/// What a handler does with one request.
#[derive(Debug, Clone)]
pub struct FakeRun {
    /// Simulated compute: the run takes this long, and fails with `CpuLimit` at the limit if it is more.
    pub cpu: Duration,
    pub logs: Vec<String>,
    pub result: Result<(u16, Bytes), Failure>,
}

impl FakeRun {
    pub fn ok(body: impl Into<Bytes>) -> Self {
        Self { cpu: Duration::ZERO, logs: Vec::new(), result: Ok((200, body.into())) }
    }

    pub fn fail(failure: Failure) -> Self {
        Self { cpu: Duration::ZERO, logs: Vec::new(), result: Err(failure) }
    }

    pub fn cpu(mut self, cpu: Duration) -> Self {
        self.cpu = cpu;
        self
    }
}

type Handler = Arc<dyn Fn(Request, Arc<Env>, Scope) -> Pin<Box<dyn Future<Output = FakeRun> + Send>> + Send + Sync>;

#[derive(Clone, Default)]
pub struct FakeEngine {
    handlers: BTreeMap<String, Handler>,
}

#[derive(Clone)]
pub struct FakeVersion {
    name: String,
    env: Arc<Env>,
    handler: Handler,
}

impl FakeEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Source that is exactly `name` deploys to `handler`.
    pub fn handler(mut self, name: &str, handler: impl Fn(&Request, &Env) -> FakeRun + Send + Sync + 'static) -> Self {
        let handler = move |request: Request, env: Arc<Env>, _: Scope| {
            let run = handler(&request, &env);
            Box::pin(async move { run }) as Pin<Box<dyn Future<Output = FakeRun> + Send>>
        };
        self.handlers.insert(name.to_owned(), Arc::new(handler));
        self
    }

    /// A handler that sees the run's scope: whether it runs as an object's request, and the host it calls objects
    /// and the object's storage through, as a script does.
    pub fn scoped<F>(mut self, name: &str, handler: impl Fn(Request, Scope) -> F + Send + Sync + 'static) -> Self
    where
        F: Future<Output = FakeRun> + Send + 'static,
    {
        let handler = move |request: Request, _: Arc<Env>, scope: Scope| {
            Box::pin(handler(request, scope)) as Pin<Box<dyn Future<Output = FakeRun> + Send>>
        };
        self.handlers.insert(name.to_owned(), Arc::new(handler));
        self
    }

    fn version(&self, name: &str, env: Env) -> Result<FakeVersion, Failure> {
        let handler = self.handlers.get(name).ok_or_else(|| Failure::Load(format!("no handler named {name:?}")))?;
        Ok(FakeVersion { name: name.to_owned(), env: Arc::new(env), handler: handler.clone() })
    }
}

impl Engine for FakeEngine {
    type Version = FakeVersion;

    async fn deploy_bound(&self, source: &str, env: &Env, _objects: &Objects) -> Result<FakeVersion, Failure> {
        self.version(source.trim(), env.clone())
    }

    /// The name, then each binding's key and value, all separated by NUL.
    fn artifact(&self, version: &FakeVersion) -> Bytes {
        let mut out = version.name.clone();
        for (key, value) in version.env.iter() {
            out.push_str(&format!("\0{key}\0{value}"));
        }
        out.into()
    }

    async fn load(&self, artifact: Bytes) -> Result<FakeVersion, Failure> {
        let text = std::str::from_utf8(&artifact).map_err(|e| Failure::Load(e.to_string()))?;
        let mut parts = text.split('\0');
        let name = parts.next().unwrap_or_default();
        let mut env = Env::new();
        while let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            env.insert(key.to_owned(), value.to_owned());
        }
        self.version(name, env)
    }

    async fn invoke_in(
        &self,
        version: &FakeVersion,
        request: Request,
        limits: Limits,
        scope: Scope,
    ) -> Result<Outcome, Failed> {
        let run = (version.handler)(request, version.env.clone(), scope).await;
        let cpu = run.cpu.min(limits.cpu);
        let wall = cpu.min(limits.wall);
        tokio::time::sleep(wall).await;
        let usage = Usage { logs: run.logs, cpu, wall, subrequests: 0 };
        let failure = if cpu > limits.wall {
            Failure::WallTimeout
        } else if run.cpu > limits.cpu {
            Failure::CpuLimit
        } else {
            match run.result {
                Ok((_, body)) if body.len() > limits.response_bytes => Failure::ResponseTooLarge,
                Ok((status, body)) => return Ok(Outcome { status, headers: Vec::new(), body, usage }),
                Err(failure) => failure,
            }
        };
        Err(Failed { failure, usage })
    }
}
