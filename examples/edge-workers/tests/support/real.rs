//! What the tests on real nodes over loopback TCP share: the engine and its threads, a member's cluster and its
//! stores, and a directory whose writes can be made to fail and whose reads slow.
#![allow(dead_code, reason = "each test file that includes this uses part of it")]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axon::ActorSystem;
use axon::files::{FileFuture, FileOp, Files, MemoryFiles, OsFiles};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use edge_workers::engine::Engine;
use edge_workers::engine::starling::{Starling, StarlingConfig};
use edge_workers::follower::FollowerConfig;
use edge_workers::node::{self, Node, Roles, Settings, Stores};

/// The engine built for the tests: its compiled code cached across runs, and compiled quickly.
pub fn starling() -> StarlingConfig {
    let mut cfg = StarlingConfig::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("engine"));
    cfg.cache_dir = Some(PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("edge-workers-cache"));
    cfg.quick_compile = true;
    cfg
}

/// One engine for every test of a file: starting it compiles its pooling setup, and nothing in it is a test's own.
/// `allow_private_fetch` lets a worker fetch from loopback; the first call's holds for the file.
pub fn engine(allow_private_fetch: bool) -> Starling {
    static ENGINE: OnceLock<Starling> = OnceLock::new();
    let start = || {
        let mut cfg = starling();
        cfg.max_in_flight = 256;
        cfg.allow_private_fetch = allow_private_fetch;
        Starling::new(cfg).unwrap_or_else(|e| panic!("{e}"))
    };
    ENGINE.get_or_init(start).clone()
}

/// The workers run on threads of their own, as in the binary. Leaked: a runtime cannot be dropped inside a test's.
pub fn js() -> tokio::runtime::Handle {
    static JS: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let start = || {
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_name("js").enable_all().build().unwrap()
    };
    JS.get_or_init(start).handle().clone()
}

/// A member's cluster settings on loopback, with `roles` as its labels.
pub fn config(id: &str, roles: Roles, discovery: &MemoryDiscovery) -> ClusterConfig {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels = roles.labels();
    cfg
}

pub async fn cluster(cfg: ClusterConfig) -> (ActorSystem, Cluster) {
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

pub struct Member<V> {
    pub id: String,
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub node: Node<V>,
}

/// A member started from `cfg`, its node running `roles`.
pub async fn member<E: Engine + Clone>(
    cfg: ClusterConfig,
    roles: Roles,
    stores: Stores,
    engine: E,
    settings: Settings,
) -> Member<E::Version> {
    let (system, cluster) = cluster(cfg).await;
    let node = node::start(&system, &cluster, roles, stores, engine, settings).await.unwrap();
    Member { id: cluster.record().id, system, cluster, node }
}

/// A member's stores, each a directory under `dir`.
pub fn os_stores(dir: &Path) -> Stores {
    let files = |name: &str| -> Arc<dyn Files> { Arc::new(OsFiles::new(dir.join(name))) };
    Stores {
        streams: files("streams"),
        registry: files("registry"),
        objects: files("objects"),
        cache: FollowerConfig::new(files("cache")),
    }
}

/// A member's stores, each in memory.
pub fn memory_stores() -> Stores {
    let memory = || -> Arc<dyn Files> { Arc::new(MemoryFiles::new()) };
    Stores { streams: memory(), registry: memory(), objects: memory(), cache: FollowerConfig::new(memory()) }
}

/// How a [`Faulty`] directory takes a write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Takes,
    /// As a full disk fails it: what the write removes is gone, what it appends is not there.
    Full,
    /// Nothing of it lands.
    Refuses,
}

/// A directory in memory whose writes can be made to fail, and its reads slow.
#[derive(Default)]
pub struct Faulty {
    inner: MemoryFiles,
    mode: Mutex<Mode>,
    unlisted: bool,
    read_delay: Mutex<Duration>,
}

impl Faulty {
    /// A directory that cannot be read at all.
    pub fn unlisted() -> Self {
        Self { unlisted: true, ..Self::default() }
    }

    pub fn set(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// Each read from now on takes this long.
    pub fn delay_reads(&self, delay: Duration) {
        *self.read_delay.lock().unwrap() = delay;
    }
}

impl Files for Faulty {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        match self.unlisted {
            true => Box::pin(std::future::ready(Err(io::Error::other("no such directory")))),
            false => self.inner.list(),
        }
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        let (read, delay) = (self.inner.read(name), *self.read_delay.lock().unwrap());
        if delay.is_zero() {
            return read;
        }
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            read.await
        })
    }

    fn apply(&self, mut ops: Vec<FileOp>) -> FileFuture<()> {
        let mode = *self.mode.lock().unwrap();
        match mode {
            Mode::Takes => return self.inner.apply(ops),
            Mode::Full => ops.retain(|op| matches!(op, FileOp::Remove(_))),
            Mode::Refuses => return Box::pin(std::future::ready(Err(io::ErrorKind::StorageFull.into()))),
        }
        let applied = self.inner.apply(ops);
        Box::pin(async move {
            applied.await?;
            Err(io::ErrorKind::StorageFull.into())
        })
    }
}
