//! JavaScript on StarlingMonkey (SpiderMonkey compiled to wasm) under wasmtime.
//!
//! Deploy runs the module's top level once inside the engine and snapshots its memory (Wizer), so a request starts
//! from an initialised heap: a new instance per request, mapped copy-on-write from the snapshot, dropped after.
//! CPU time is what the request's future spends being polled, checked at every 1 ms epoch tick; memory is the
//! instance's linear memory, capped by the store's limiter.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use wasmtime::component::{Component, ResourceTable};
use wasmtime::{
    Cache, CacheConfig, Config, InstanceAllocationStrategy, Module, PoolingAllocationConfig, ResourceLimiter, Store,
    StoreLimits, StoreLimitsBuilder, UpdateDeadline,
};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::p2::pipe::MemoryInputPipe;
use wasmtime_wasi::p2::{OutputStream, Pollable, StreamError};
use wasmtime_wasi::{FsPerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::{RequestOptions, WasiBody, WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks, WasiHttpView};
use wasmtime_wizer::{WasmtimeWizer, Wizer};

use super::{Engine, Env, Failed, Failure, Host, Limits, Objects, Outcome, Request, Response, Scope, StorageOp, Usage};

const RAW: &str = "starling-raw.wasm";
const ADAPTER: &str = "preview1-adapter.wasm";
const INIT: &str = "wizer-initialize";
/// What StarlingMonkey prints on stderr, which a script cannot write to, before it answers 500 for a handler that
/// threw.
const THREW: &str = "Error while running request handler: ";
/// The host a script's `fetch` never leaves the engine for: a call of an object (`/object`) or of the running
/// object's storage (`/storage`), answered by the run's [`Host`]. An outbound `fetch` is the one way out of the
/// guest that waits without being charged as CPU time, and it needs no import the engine does not already have.
const INTERNAL: &str = "edge.internal";
/// On a request the engine runs as an object's: `class/id`, the id percent-encoded. On a call of an object, the
/// same, with the URL the object sees in `URL`. Taken off every request that comes from outside.
const OBJECT: &str = "x-edge-object";
const URL: &str = "x-edge-url";
/// Marks an answer of the internal host that the shim throws as an error.
const FAULT: &str = "x-edge-fault";

#[derive(Debug, Clone)]
pub struct StarlingConfig {
    /// Holds `starling-raw.wasm` and `preview1-adapter.wasm` (`scripts/fetch-engine.sh`).
    pub engine_dir: PathBuf,
    /// Compiled code, kept by content: a version compiles once per directory. None compiles at every start.
    pub cache_dir: Option<PathBuf>,
    /// Requests in flight at once, over all versions. One past it fails with `Failure::Runtime`.
    pub max_in_flight: u32,
    /// The most a request's `Limits::memory` can grant; address space reserved per request in flight.
    pub max_memory: usize,
    /// What a finished request's memory keeps mapped for the next one, which then starts without page faults:
    /// up to `max_in_flight` times this stays resident after a burst. 0 gives every page back.
    pub keep_resident: usize,
    /// Time a module's top level may take at deploy. Workers allows 1 s.
    pub startup: Duration,
    /// Lets `fetch` reach loopback and private addresses: for a development machine and for tests.
    pub allow_private_fetch: bool,
    /// Compiles a version with Winch, not Cranelift: a deploy takes a fraction of the time and the code runs
    /// slower, and `cache_dir` keeps only the engine. For tests, which deploy more than they run.
    pub quick_compile: bool,
}

impl StarlingConfig {
    pub fn new(engine_dir: impl Into<PathBuf>) -> Self {
        Self {
            engine_dir: engine_dir.into(),
            cache_dir: None,
            max_in_flight: 1000,
            max_memory: 512 << 20,
            keep_resident: 4 << 20,
            startup: Duration::from_secs(1),
            allow_private_fetch: false,
            quick_compile: false,
        }
    }
}

#[derive(Clone)]
pub struct Starling {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: StarlingConfig,
    serve: wasmtime::Engine,
    linker: wasmtime::component::Linker<Run>,
    init: wasmtime::Engine,
    /// The raw engine instrumented for Wizer, compiled and linked once; every deploy instantiates it.
    init_pre: wasmtime::InstancePre<Init>,
    raw: Vec<u8>,
    adapter: Vec<u8>,
}

#[derive(Clone)]
pub struct Version {
    pre: ProxyPre<Run>,
    artifact: Bytes,
}

impl Starling {
    /// Reads and compiles the engine: seconds without a warm `cache_dir`, and it blocks.
    pub fn new(cfg: StarlingConfig) -> Result<Self, Failure> {
        let raw = read_engine(&cfg.engine_dir, RAW)?;
        let adapter = read_engine(&cfg.engine_dir, ADAPTER)?;
        Self::build(cfg, raw, adapter).map_err(|e| Failure::Runtime(format!("{e:#}")))
    }

    fn build(cfg: StarlingConfig, raw: Vec<u8>, adapter: Vec<u8>) -> wasmtime::Result<Self> {
        let cache = match &cfg.cache_dir {
            Some(dir) => Some(Cache::new(CacheConfig::new().with_directory(dir).clone())?),
            None => None,
        };
        let mut config = Config::new();
        config.epoch_interruption(true).cache(cache);
        let init = wasmtime::Engine::new(&config)?;

        let n = cfg.max_in_flight;
        let mut pool = PoolingAllocationConfig::new();
        pool.total_component_instances(n)
            .total_core_instances(n.saturating_mul(8))
            .total_memories(n.saturating_mul(2))
            .total_tables(n.saturating_mul(4))
            .total_stacks(n)
            .max_memory_size(cfg.max_memory)
            .max_component_instance_size(4 << 20)
            .max_core_instance_size(4 << 20)
            .table_elements(100_000);
        // Measured on hello world: 390 µs a request without these, 156 µs with 4 MiB kept (README).
        pool.linear_memory_keep_resident(cfg.keep_resident)
            .table_keep_resident(cfg.keep_resident)
            .pagemap_scan(wasmtime::Enabled::Auto);
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        if cfg.quick_compile {
            config.strategy(wasmtime::Strategy::Winch).cache(None);
        }
        let serve = wasmtime::Engine::new(&config)?;

        let mut linker = wasmtime::component::Linker::new(&serve);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;

        let (_, instrumented) = wizer().instrument(&raw)?;
        let module = Module::new(&init, &instrumented)?;
        let mut init_linker = wasmtime::Linker::new(&init);
        wasmtime_wasi::p1::add_to_linker_async(&mut init_linker, |s: &mut Init| &mut s.wasi)?;
        // The engine imports wasi:http and the rest of WASI 0.2 too; the top level of a module calls none of it.
        init_linker.define_unknown_imports_as_traps(&module)?;
        let init_pre = init_linker.instantiate_pre(&module)?;

        // Epochs are the only clock wasm code is stopped by. The thread ends with the engines.
        let (weak_serve, weak_init) = (serve.weak(), init.weak());
        std::thread::Builder::new().name("starling-epoch".into()).spawn(move || {
            while let (Some(serve), Some(init)) = (weak_serve.upgrade(), weak_init.upgrade()) {
                serve.increment_epoch();
                init.increment_epoch();
                drop((serve, init));
                std::thread::sleep(Duration::from_millis(1));
            }
        })?;
        Ok(Self { inner: Arc::new(Inner { cfg, serve, linker, init, init_pre, raw, adapter }) })
    }

    /// Runs the shim and the module's top level in the raw engine and returns its memory as a component.
    async fn snapshot(&self, source: &str, env: &Env, objects: &Objects) -> Result<Vec<u8>, Failure> {
        let inner = &self.inner;
        let runtime = |e: &dyn std::fmt::Display| Failure::Runtime(format!("{e:#}"));
        let dir = tempfile::tempdir().map_err(|e| runtime(&e))?;
        let env = serde_json::to_string(env).map_err(|e| runtime(&e))?;
        let objects = serde_json::to_string(objects).map_err(|e| runtime(&e))?;
        let shim = SHIM.replace("__ENV__", &env).replace("__OBJECTS__", &objects);
        std::fs::write(dir.path().join("worker.js"), source).map_err(|e| runtime(&e))?;
        std::fs::write(dir.path().join("index.js"), shim).map_err(|e| runtime(&e))?;

        let output = Sink::new(64 << 10);
        let mut wasi = WasiCtxBuilder::new();
        wasi.stdin(MemoryInputPipe::new("index.js\n")).stdout(output.stream(false)).stderr(output.stream(true));
        wasi.preopened_dir(dir.path(), ".", FsPerms::ReadOnly).map_err(|e| runtime(&e))?;
        let limits = StoreLimitsBuilder::new().memory_size(inner.cfg.max_memory).build();
        let mut store = Store::new(&inner.init, Init { wasi: wasi.build_p1(), limits });
        store.limiter(|s| &mut s.limits);
        store.set_epoch_deadline(1);
        let meter = Arc::new(Meter::new());
        let (startup, spent) = (inner.cfg.startup, meter.clone());
        store.epoch_deadline_callback(move |_| {
            wasmtime::ensure!(spent.cpu() < startup, "the module's top level ran past the startup limit");
            Ok(UpdateDeadline::Yield(1))
        });

        let wizer = wizer();
        let (cx, _) = wizer.instrument(&inner.raw).map_err(|e| runtime(&e))?;
        let run = async {
            let instance = inner.init_pre.instantiate_async(&mut store).await?;
            if let Some(start) = instance.get_func(&mut store, "_initialize") {
                start.typed::<(), ()>(&store)?.call_async(&mut store, ()).await?;
            }
            instance.get_typed_func::<(), ()>(&mut store, INIT)?.call_async(&mut store, ()).await?;
            wasmtime::Result::<wasmtime::Instance>::Ok(instance)
        };
        let run = Metered { meter, inner: Box::pin(run) };
        let instance = match run.await {
            Ok(instance) => instance,
            Err(e) => {
                let (lines, _) = output.take();
                let said = if lines.is_empty() { format!("{e:#}") } else { lines.join("\n") };
                return Err(Failure::Load(said));
            }
        };
        let module =
            wizer.snapshot(&cx, &mut WasmtimeWizer { store: &mut store, instance }).await.map_err(|e| runtime(&e))?;
        let mut encoder = wit_component::ComponentEncoder::default();
        let encoder = encoder.validate(true).module(&module).map_err(|e| runtime(&e))?;
        let encoder = encoder.adapter("wasi_snapshot_preview1", &inner.adapter).map_err(|e| runtime(&e))?;
        encoder.encode().map_err(|e| runtime(&e))
    }

    async fn compile(&self, artifact: Bytes) -> Result<Version, Failure> {
        let inner = self.inner.clone();
        // Cranelift on 10 MB of wasm: seconds, unless the cache holds it.
        let compiled = tokio::task::spawn_blocking(move || {
            let component = Component::new(&inner.serve, &artifact)?;
            let pre = ProxyPre::new(inner.linker.instantiate_pre(&component)?)?;
            wasmtime::Result::<Version>::Ok(Version { pre, artifact })
        });
        match compiled.await {
            Ok(Ok(version)) => Ok(version),
            Ok(Err(e)) => Err(Failure::Load(format!("{e:#}"))),
            Err(e) => Err(Failure::Runtime(e.to_string())),
        }
    }

    async fn run(
        &self,
        version: &Version,
        request: Request,
        limits: Limits,
        scope: Scope,
        meter: Arc<Meter>,
        sink: &Sink,
    ) -> Ran {
        let inner = &self.inner;
        let url = if request.url.starts_with('/') { format!("http://localhost{}", request.url) } else { request.url };
        let mut builder = hyper::Request::builder().method(request.method.as_str()).uri(url);
        // Only the host says a request is an object's.
        for (name, value) in request.headers.iter().filter(|(name, _)| !name.eq_ignore_ascii_case(OBJECT)) {
            builder = builder.header(name, value);
        }
        if let Some((class, id)) = &scope.object {
            builder = builder.header(OBJECT, format!("{class}/{}", percent_encode(id)));
        }
        let body = Full::new(request.body).map_err(|never| -> wasmtime_wasi_http::Error { match never {} });
        let request = builder.body(body).map_err(|e| format!("not a request: {e}"))?;
        let scheme = if request.uri().scheme_str() == Some("https") { Scheme::Https } else { Scheme::Http };

        let mut wasi = WasiCtxBuilder::new();
        wasi.stdout(sink.stream(false)).stderr(sink.stream(true));
        let memory = limits.memory.min(inner.cfg.max_memory);
        let state = Run {
            wasi: wasi.build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            limiter: Limiter { limits: StoreLimitsBuilder::new().memory_size(memory).build(), meter: meter.clone() },
            outbound: Outbound {
                meter: meter.clone(),
                max: limits.subrequests,
                allow_private: inner.cfg.allow_private_fetch,
                scope,
            },
        };
        let mut store = Store::new(&inner.serve, state);
        store.limiter(|s| &mut s.limiter);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_| {
            if meter.cpu() >= limits.cpu {
                meter.cpu_exceeded.store(true, Ordering::Relaxed);
                wasmtime::bail!("CPU time limit");
            }
            // Yields, so a worker that computes shares its thread with the others every tick.
            Ok(UpdateDeadline::Yield(1))
        });

        let (tx, rx) = tokio::sync::oneshot::channel();
        let text = |e: wasmtime::Error| format!("{e:#}");
        let incoming = store.data_mut().http().new_incoming_request(scheme, request).map_err(text)?;
        let out = store.data_mut().http().new_response_outparam(tx).map_err(text)?;
        let pre = version.pre.clone();
        // The store goes with the call: the response's sender lives in it, and `rx` ends only when it is dropped.
        let call = async move {
            let proxy = pre.instantiate_async(&mut store).await?;
            proxy.wasi_http_incoming_handler().call_handle(&mut store, incoming, out).await
        };
        let response = async {
            let (parts, body) = match rx.await {
                Ok(Ok(response)) => response.into_parts(),
                Ok(Err(code)) => return Err(format!("the response was refused: {code:?}")),
                Err(_) => return Ok(None),
            };
            // Read no further than the limit: `None` for a body past it.
            let body = match Limited::new(body, limits.response_bytes).collect().await {
                Ok(body) => Some(body.to_bytes()),
                Err(e) if e.is::<LengthLimitError>() => None,
                Err(e) => return Err(format!("the response body: {e}")),
            };
            Ok(Some((parts, body)))
        };
        let (called, response) = tokio::join!(call, response);
        called.map_err(text)?;
        response?.ok_or_else(|| "the engine ended without a response".to_owned())
    }
}

type Ran = Result<(http::response::Parts, Option<Bytes>), String>;

impl Engine for Starling {
    type Version = Version;

    async fn deploy_bound(&self, source: &str, env: &Env, objects: &Objects) -> Result<Version, Failure> {
        let component = self.snapshot(source, env, objects).await?;
        self.compile(component.into()).await
    }

    fn artifact(&self, version: &Version) -> Bytes {
        version.artifact.clone()
    }

    async fn load(&self, artifact: Bytes) -> Result<Version, Failure> {
        self.compile(artifact).await
    }

    async fn invoke_in(
        &self,
        version: &Version,
        request: Request,
        limits: Limits,
        scope: Scope,
    ) -> Result<Outcome, Failed> {
        let started = Instant::now();
        let meter = Arc::new(Meter::new());
        let sink = Sink::new(limits.log_bytes);
        let run = self.run(version, request, limits, scope, meter.clone(), &sink);
        let run = Metered { meter: meter.clone(), inner: Box::pin(run) };
        let ran = tokio::time::timeout(limits.wall, run).await;

        let (logs, stderr) = sink.take();
        let usage = Usage {
            logs,
            cpu: meter.cpu(),
            wall: started.elapsed(),
            subrequests: meter.subrequests.load(Ordering::Relaxed),
        };
        let threw = || stderr.split_once(THREW).map(|(_, rest)| rest.lines().next().unwrap_or_default().to_owned());
        let failure = match ran {
            Err(_) => Failure::WallTimeout,
            _ if meter.cpu_exceeded.load(Ordering::Relaxed) => Failure::CpuLimit,
            Ok(Ok((_, None))) => Failure::ResponseTooLarge,
            Ok(Ok((parts, Some(body)))) => match threw().filter(|_| parts.status.as_u16() == 500) {
                // A refused grow the engine could not collect its way out of surfaces as a JS out-of-memory error.
                Some(_) if meter.memory_denied.load(Ordering::Relaxed) => Failure::MemoryLimit,
                Some(message) => Failure::Exception(message),
                None => {
                    let headers = parts.headers.iter();
                    let headers = headers.map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into()));
                    return Ok(Outcome { status: parts.status.as_u16(), headers: headers.collect(), body, usage });
                }
            },
            Ok(Err(_)) if meter.memory_denied.load(Ordering::Relaxed) => Failure::MemoryLimit,
            Ok(Err(e)) => Failure::Runtime(e),
        };
        Err(Failed { failure, usage })
    }
}

fn wizer() -> Wizer {
    let mut wizer = Wizer::new();
    wizer.init_func(INIT);
    wizer
}

fn read_engine(dir: &Path, name: &str) -> Result<Vec<u8>, Failure> {
    let path = dir.join(name);
    std::fs::read(&path).map_err(|e| {
        Failure::Runtime(format!(
            "{}: {e}. The StarlingMonkey engine is not in the repository: run examples/edge-workers/scripts/fetch-engine.sh",
            path.display()
        ))
    })
}

/// Makes a Workers module the handler of StarlingMonkey's `fetch` event: its default export's `fetch`, or, for a
/// request the host marks as an object's, the `fetch` of a new instance of that class. A new instance per request:
/// what an object keeps between requests is in `state.storage`.
const SHIM: &str = r#"import * as module from "./worker.js";
const worker = module.default;
if (typeof worker?.fetch !== "function") {
  throw new TypeError("the module's default export has no fetch(request, env, ctx)");
}
const classes = __OBJECTS__;
for (const [name, cls] of Object.entries(classes)) {
  if (typeof module[cls] !== "function") throw new TypeError(`binding ${name}: the module exports no class ${cls}`);
}
const host = async (path, init) => {
  const response = await fetch("http://edge.internal" + path, init);
  if (response.headers.has("x-edge-fault")) throw new Error(await response.text());
  return response;
};
const objectId = (name) => Object.freeze({ name, toString: () => name });
const namespace = (cls) => Object.freeze({
  idFromName: (name) => objectId(String(name)),
  get: (id) => Object.freeze({
    id,
    async fetch(input, init) {
      // The engine traps ("uninitialized element") on a copy of the incoming request, the one most often passed on.
      const request = input instanceof Request && init === undefined ? input : new Request(input, init);
      const headers = new Headers(request.headers);
      headers.set("x-edge-object", cls + "/" + encodeURIComponent(id.name));
      headers.set("x-edge-url", request.url);
      const body = request.body === null ? undefined : await request.arrayBuffer();
      return host("/object", { method: request.method, headers, body });
    },
  }),
});
const env = Object.freeze({
  ...__ENV__,
  ...Object.fromEntries(Object.entries(classes).map(([name, cls]) => [name, namespace(cls)])),
});
const op = async (body) => (await host("/storage", { method: "POST", body: JSON.stringify(body) })).json();
const storage = Object.freeze({
  async get(key) {
    const { value } = await op({ op: "get", key: String(key) });
    return value === null ? undefined : JSON.parse(value);
  },
  async put(key, value) {
    const text = JSON.stringify(value);
    if (text === undefined) throw new TypeError("storage.put: the value is not JSON-serialisable");
    await op({ op: "put", key: String(key), value: text });
  },
  async delete(key) {
    return (await op({ op: "delete", key: String(key) })).deleted;
  },
  async list(options = {}) {
    const { entries } = await op({ op: "list", prefix: options.prefix ?? "", limit: options.limit ?? null });
    return new Map(entries.map(([key, value]) => [key, JSON.parse(value)]));
  },
});
addEventListener("fetch", (event) => {
  const ctx = { waitUntil: (promise) => event.waitUntil(promise), passThroughOnException() {} };
  event.respondWith((async () => {
    const target = event.request.headers.get("x-edge-object");
    let response;
    if (target === null) {
      response = await worker.fetch(event.request, env, ctx);
    } else {
      const cls = target.slice(0, target.indexOf("/"));
      if (!Object.values(classes).includes(cls)) throw new TypeError(`no binding names the class ${cls}`);
      const id = objectId(decodeURIComponent(target.slice(cls.length + 1)));
      response = await new module[cls](Object.freeze({ id, storage }), env).fetch(event.request);
    }
    if (!(response instanceof Response)) throw new TypeError("fetch() did not return a Response");
    return response;
  })());
});
"#;

struct Init {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

struct Run {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    limiter: Limiter,
    outbound: Outbound,
}

impl WasiView for Run {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

impl WasiHttpView for Run {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView { ctx: &mut self.http, table: &mut self.table, hooks: &mut self.outbound }
    }
}

/// One request's counters, shared by what outlives its store when the wall timeout drops it.
struct Meter {
    /// CPU time of the finished polls in ns, and the polling thread's CPU time when the poll under way began.
    spent: AtomicU64,
    polling_since: AtomicU64,
    cpu_exceeded: AtomicBool,
    memory_denied: AtomicBool,
    subrequests: AtomicU32,
}

impl Meter {
    fn new() -> Self {
        Self {
            spent: AtomicU64::new(0),
            polling_since: AtomicU64::new(u64::MAX),
            cpu_exceeded: AtomicBool::new(false),
            memory_denied: AtomicBool::new(false),
            subrequests: AtomicU32::new(0),
        }
    }

    /// Read between polls, or within one on its thread, as the epoch callback is.
    fn cpu(&self) -> Duration {
        let under_way = thread_cpu().saturating_sub(self.polling_since.load(Ordering::Relaxed));
        Duration::from_nanos(self.spent.load(Ordering::Relaxed) + under_way)
    }
}

/// The CPU time of this thread in ns. Not the clock: on a machine with more to run than cores, the time a thread
/// waits for a core is not the request's.
fn thread_cpu() -> u64 {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

/// Counts the CPU time its future is polled for as the request's: a poll runs wasm until it waits or yields.
struct Metered<F> {
    meter: Arc<Meter>,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Metered<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let began = thread_cpu();
        self.meter.polling_since.store(began, Ordering::Relaxed);
        let polled = self.inner.as_mut().poll(cx);
        self.meter.polling_since.store(u64::MAX, Ordering::Relaxed);
        self.meter.spent.fetch_add(thread_cpu() - began, Ordering::Relaxed);
        polled
    }
}

struct Limiter {
    limits: StoreLimits,
    meter: Arc<Meter>,
}

impl ResourceLimiter for Limiter {
    fn memory_growing(&mut self, current: usize, desired: usize, maximum: Option<usize>) -> wasmtime::Result<bool> {
        let allowed = self.limits.memory_growing(current, desired, maximum)?;
        if !allowed {
            // Refused, not trapped: SpiderMonkey collects and tries again before it gives up.
            self.meter.memory_denied.store(true, Ordering::Relaxed);
        }
        Ok(allowed)
    }

    fn table_growing(&mut self, current: usize, desired: usize, maximum: Option<usize>) -> wasmtime::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }
}

struct Outbound {
    meter: Arc<Meter>,
    max: u32,
    allow_private: bool,
    scope: Scope,
}

type Io = Box<dyn Future<Output = Result<(), wasmtime_wasi_http::Error>> + Send>;

impl WasiHttpHooks for Outbound {
    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        options: Option<RequestOptions>,
        _body_errors: Io,
    ) -> Box<dyn Future<Output = wasmtime_wasi_http::Result<(http::Response<WasiBody>, Io)>> + Send> {
        let internal = request.uri().host() == Some(INTERNAL);
        // An object's own storage is not a subrequest; a call of an object is.
        let counted = !(internal && request.uri().path() == "/storage");
        let over = counted && self.meter.subrequests.fetch_add(1, Ordering::Relaxed) >= self.max;
        let (allow_private, host) = (self.allow_private, self.scope.host.clone());
        Box::new(async move {
            if over {
                return Err(wasmtime_wasi_http::Error::HttpRequestDenied);
            }
            if internal {
                let answer = hosted(host, request).await.unwrap_or_else(|message| Response {
                    status: 500,
                    headers: vec![(FAULT.to_owned(), "1".to_owned())],
                    body: message.into(),
                });
                let mut builder = http::Response::builder().status(answer.status);
                for (name, value) in &answer.headers {
                    builder = builder.header(name, value);
                }
                let body = Full::new(answer.body).map_err(|never| -> wasmtime_wasi_http::Error { match never {} });
                let response = builder
                    .body(body.boxed_unsync())
                    .map_err(|e| wasmtime_wasi_http::Error::InternalError(Some(e.to_string())))?;
                return Ok((response, Box::new(async { Ok(()) }) as Io));
            }
            if !allow_private {
                refuse_private(request.uri()).await?;
            }
            let (response, io) = wasmtime_wasi_http::default_send_request(request, options).await?;
            Ok((response.map(BodyExt::boxed_unsync), Box::new(io) as Io))
        })
    }
}

/// A request to the internal host: `Err` is what the script is thrown.
async fn hosted(host: Option<Arc<dyn Host>>, request: http::Request<WasiBody>) -> Result<Response, String> {
    let (parts, body) = request.into_parts();
    // What one frame between nodes carries, and more than a storage operation is.
    let body = Limited::new(body, 1 << 20).collect().await.map_err(|e| format!("the request's body: {e}"))?;
    let body = body.to_bytes();
    // After the body: an answer while the script still writes it ends the script's event loop, not its call.
    let host = host.ok_or("objects cannot be reached from this run")?;
    match parts.uri.path() {
        "/storage" => {
            let op: StorageOp = serde_json::from_slice(&body).map_err(|e| format!("not a storage operation: {e}"))?;
            let answer = serde_json::to_vec(&host.storage(op).await?).map_err(|e| e.to_string())?;
            Ok(Response { status: 200, headers: Vec::new(), body: answer.into() })
        }
        "/object" => {
            let header = |name: &str| parts.headers.get(name).and_then(|value| value.to_str().ok());
            let target = header(OBJECT).and_then(|target| target.split_once('/'));
            let (class, id) = target.ok_or("a call of an object names no object")?;
            let id = percent_decode(id).ok_or("an object's id is not text")?;
            let url = header(URL).ok_or("a call of an object has no URL")?.to_owned();
            let ours = |name: &http::HeaderName| name == OBJECT || name == URL || name == http::header::HOST;
            let headers = parts.headers.iter().filter(|(name, _)| !ours(name));
            let headers = headers.map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into()));
            let request = Request { method: parts.method.to_string(), url, headers: headers.collect(), body };
            host.fetch(class.to_owned(), id, request).await
        }
        other => Err(format!("the engine has no {other}")),
    }
}

/// As JavaScript's `encodeURIComponent` does: what `decodeURIComponent` reads back.
pub fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn percent_decode(text: &str) -> Option<String> {
    let (bytes, mut out, mut at) = (text.as_bytes(), Vec::with_capacity(text.len()), 0);
    while let Some(&byte) = bytes.get(at) {
        if byte == b'%' {
            let hex = std::str::from_utf8(bytes.get(at + 1..at + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            at += 3;
        } else {
            out.push(byte);
            at += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Refuses a URL when any address its host resolves to is not a public one.
async fn refuse_private(uri: &http::Uri) -> wasmtime_wasi_http::Result<()> {
    use wasmtime_wasi_http::Error;
    let host = uri.host().ok_or(Error::HttpRequestUriInvalid)?;
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("https") { 443 } else { 80 });
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let mut resolved = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| Error::DnsError { rcode: Some(e.to_string()), info_code: None })?;
    if resolved.any(|addr| !is_public(addr.ip())) {
        return Err(Error::DestinationIpProhibited);
    }
    Ok(())
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            let shared = a == 100 && (64..128).contains(&b);
            // 192.0.0.0/24, the protocol assignments, and 198.18.0.0/15, the benchmarking range.
            let reserved = (a == 192 && b == 0 && c == 0) || (a == 198 && (18..20).contains(&b));
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_documentation()
                || shared
                || reserved
                || a == 0
                || a >= 240)
        }
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_public(IpAddr::V4(v4)),
            None => {
                let [first, second, third, ..] = v6.segments();
                let unique_local = first & 0xfe00 == 0xfc00;
                // fe80::/10, and the site-local fec0::/10 that came before unique local.
                let link_or_site_local = first & 0xff80 == 0xfe80;
                // An IPv4 address inside an IPv6 one, which a gateway or the host's own stack turns back into it:
                // ::/96, NAT64 (64:ff9b::/96 and 64:ff9b:1::/48), 6to4 (2002::/16) and Teredo (2001::/32). Refused
                // whole: what they wrap is reached by its own address.
                let wraps_v4 = v6.segments()[..6] == [0; 6]
                    || (first == 0x64 && second == 0xff9b && third <= 1)
                    || first == 0x2002
                    || (first == 0x2001 && second == 0);
                let documentation = first == 0x2001 && second == 0xdb8;
                !(v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || unique_local
                    || link_or_site_local
                    || wraps_v4
                    || documentation)
            }
        },
    }
}

/// A request's console: stdout and stderr in the order written, up to a size, the rest dropped.
#[derive(Clone)]
struct Sink {
    written: Arc<Mutex<Written>>,
    stderr: bool,
}

struct Written {
    all: Vec<u8>,
    stderr: Vec<u8>,
    room: usize,
}

impl Sink {
    fn new(bytes: usize) -> Self {
        Self {
            written: Arc::new(Mutex::new(Written { all: Vec::new(), stderr: Vec::new(), room: bytes })),
            stderr: false,
        }
    }

    fn stream(&self, stderr: bool) -> Self {
        Self { written: self.written.clone(), stderr }
    }

    fn push(&self, bytes: &[u8]) {
        let mut written = self.written.lock().expect("no panic while the lock is held");
        let kept = &bytes[..bytes.len().min(written.room)];
        written.room -= kept.len();
        written.all.extend_from_slice(kept);
        if self.stderr {
            written.stderr.extend_from_slice(kept);
        }
    }

    /// Every line, and stderr's text alone.
    fn take(&self) -> (Vec<String>, String) {
        let written = self.written.lock().expect("no panic while the lock is held");
        let lines = String::from_utf8_lossy(&written.all).lines().map(str::to_owned).collect();
        (lines, String::from_utf8_lossy(&written.stderr).into_owned())
    }
}

impl IsTerminal for Sink {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for Sink {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }

    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for Sink {
    async fn ready(&mut self) {}
}

impl OutputStream for Sink {
    fn write(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        self.push(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StreamError> {
        Ok(())
    }

    fn check_write(&mut self) -> Result<usize, StreamError> {
        Ok(64 << 10)
    }
}

impl tokio::io::AsyncWrite for Sink {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, bytes: &[u8]) -> Poll<std::io::Result<usize>> {
        self.push(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::{is_public, percent_decode, percent_encode};

    #[test]
    fn an_object_id_survives_its_header() {
        for id in ["room-1", "a/b c", "é%\n", ""] {
            let encoded = percent_encode(id);
            assert!(encoded.bytes().all(|b| b.is_ascii_graphic()), "{encoded}");
            assert_eq!(percent_decode(&encoded).as_deref(), Some(id));
        }
        assert_eq!(percent_decode("%4"), None);
        assert_eq!(percent_decode("%FF"), None);
    }

    #[test]
    fn only_public_addresses_pass() {
        for refused in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "192.0.0.8",
            "198.18.0.1",
            "198.19.255.255",
            "::127.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b:1::a00:1",
            "2002:7f00:1::1",
            "2001:0:4136:e378:8000:63bf:3fff:fdd2",
            "2001:db8::1",
            "fec0::1",
        ] {
            assert!(!is_public(refused.parse().unwrap()), "{refused} must be refused");
        }
        for allowed in
            ["1.1.1.1", "93.184.216.34", "198.20.0.1", "2606:4700:4700::1111", "2001:4860:4860::8888", "::ffff:1.1.1.1"]
        {
            assert!(is_public(allowed.parse().unwrap()), "{allowed} must pass");
        }
    }
}
