//! What a gateway or a runner serves: the [`Follower`] actor follows the manifest stream, and [`Serving`] is the
//! table the request path reads.
//!
//! A runner switches a worker to a manifest only once every bundle it names is prepared in the engine: fetched
//! from the control node by hash, in pieces, checked against the hash, kept on the node's disk, and loaded off the
//! actor's thread. A manifest replaces only one with a lower `seq`, so what a node serves never goes backwards, and
//! a request that picked a version keeps it until it ends. The table and the bundles are on disk, so a node that
//! starts while the control node is away serves what it served before.
//!
//! A runner also lets go of a worker nobody calls: `Msg::Evict` drops its prepared versions and keeps the bundles
//! on disk, and `Msg::Warm` loads them again for the next request.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axon::files::{FileOp, Files};
use axon::{Actor, ActorHandle, Context, ReplyTo, RequestError};
use axon_cluster::{Cluster, NamedRef, Retry};
use axon_stream::{StartPosition, StreamEvent, StreamNode, SubscribeOptions};
use bytes::{Bytes, BytesMut};
use tokio::time::Instant;

use crate::engine::Engine;
use crate::node::on_control;
use crate::store;
use crate::wire::{BUNDLES, BundleApi, Fetch, Hash, MAX_BUNDLE, Manifest, Piece, Served, VersionRef, manifests};

/// A worker as this node serves it now.
pub struct Current<V> {
    pub manifest: Manifest,
    /// Prepared on a runner, until the worker goes idle; a node that only routes holds none.
    stable: Option<V>,
    canary: Option<V>,
}

impl<V> Current<V> {
    /// Whether a request can run now. Not on a node that only routes, nor for a worker that went idle.
    pub fn is_warm(&self) -> bool {
        self.stable.is_some()
    }

    /// The stable version, prepared. `None` as for [`Current::pick`].
    pub fn stable(&self) -> Option<&V> {
        self.stable.as_ref()
    }

    /// The version a request runs on, the canary for its percent of `sample`'s values. `None` on a node that
    /// prepares nothing, and for a worker that went idle here.
    pub fn pick(&self, sample: u64) -> Option<(&VersionRef, &V)> {
        let version = self.manifest.pick(sample);
        let loaded = if version.id == self.manifest.stable.id { &self.stable } else { &self.canary };
        Some((version, loaded.as_ref()?))
    }

    /// What this node serves of the worker, as its stats say.
    pub fn served(&self) -> Served {
        let canary = self.manifest.canary.as_ref().map(|canary| (canary.version.id, canary.percent));
        Served { seq: self.manifest.seq, stable: self.manifest.stable.id, canary, warm: self.is_warm() }
    }
}

type Table<V> = BTreeMap<String, Arc<Current<V>>>;

/// Who waits for an idle worker to be loaded again.
type Warming<V> = ReplyTo<Option<Arc<Current<V>>>>;

/// Asks the follower for an idle worker back, waiting up to `left` rather than the system's request timeout: a load
/// from disk under a busy CPU can outlast that timeout and still be in its caller's time.
pub async fn warm<V: Send + Sync + 'static>(
    follower: &ActorHandle<Msg<V>>,
    name: &str,
    left: Duration,
) -> Result<Option<Arc<Current<V>>>, RequestError> {
    let until = follower.address.clock().now().saturating_add(left);
    follower.address.request(Some(until), |reply| Msg::Warm(name.to_owned(), reply)).await
}

#[derive(Debug, Default)]
pub struct Stats {
    /// Times a worker moved to a new manifest here.
    pub switches: AtomicU64,
    /// Bundles prepared from this node's disk, and from the control node.
    pub from_cache: AtomicU64,
    pub fetched: AtomicU64,
    /// Fetches, loads and cache writes that failed. Each is tried again.
    pub failures: AtomicU64,
    /// Times the table reached this node's disk.
    pub saved: AtomicU64,
    /// Idle workers let go of, and loaded again for a request.
    pub evicted: AtomicU64,
    pub warmed: AtomicU64,
}

/// The table a node's request path reads. A clone shares it.
#[derive(Clone)]
pub struct Serving<V> {
    table: Arc<RwLock<Arc<Table<V>>>>,
    pub stats: Arc<Stats>,
}

impl<V> Default for Serving<V> {
    fn default() -> Self {
        Self { table: Arc::default(), stats: Arc::default() }
    }
}

impl<V> Serving<V> {
    /// The worker as served now. A request keeps the `Arc` for its whole run, so a switch never moves it.
    pub fn get(&self, name: &str) -> Option<Arc<Current<V>>> {
        self.all().get(name).cloned()
    }

    pub fn all(&self) -> Arc<Table<V>> {
        self.table.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn seq(&self, name: &str) -> Option<u64> {
        self.all().get(name).map(|current| current.manifest.seq)
    }

    fn change(&self, change: impl FnOnce(&mut Table<V>)) {
        let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
        let mut next = (**table).clone();
        change(&mut next);
        *table = Arc::new(next);
    }
}

#[derive(Clone)]
pub struct FollowerConfig {
    /// Where the node keeps its bundles and its table.
    pub files: Arc<dyn Files>,
    /// Bytes asked for per part of a bundle. The control node gives less where its frame holds less.
    pub piece: u32,
    /// Bundles being prepared at once.
    pub prepares: usize,
    /// How long a bundle's fetch may go without a part, the lookup of the control node included.
    pub patience: Duration,
    /// The pause after a failure, doubling up to 32 times this.
    pub retry: Duration,
}

impl FollowerConfig {
    pub fn new(files: Arc<dyn Files>) -> Self {
        Self { files, piece: 512 << 10, prepares: 4, patience: Duration::from_secs(10), retry: Duration::from_secs(1) }
    }
}

pub enum Msg<V> {
    Restored(Result<Restored, String>),
    Event(Option<StreamEvent>),
    Subscribe,
    /// A bundle read from this node's disk, `None` when it was not there whole.
    Cached(VersionRef, Option<Bytes>),
    /// A bundle loaded in the engine; `true` when it came from the control node.
    Prepared(Hash, Result<(V, bool), String>),
    /// The table is on disk, and so many of the bundle files nothing refers to are gone.
    Saved(Result<usize, String>),
    Retry,
    /// Nobody calls the worker: drop its prepared versions, keep its bundles on disk.
    Evict(String),
    /// A request came for a worker that went idle: answered with the worker once it can run again, already in the
    /// table, and `None` for one no longer served.
    Warm(String, Warming<V>),
}

pub struct Restored {
    manifests: Vec<Manifest>,
    bundles: BTreeSet<Hash>,
}

enum Bundle<V> {
    Preparing,
    Ready(V),
    /// Until the next retry.
    Failed,
    /// Of an idle worker: on disk, not in the engine.
    Cold,
}

/// The pause before a bundle is asked for again after a refusal or a cut.
const AGAIN: Duration = Duration::from_millis(50);

/// The table as it is kept on disk, replaced whole at each save.
const TABLE: &str = "manifests";

fn bundle_file(hash: &Hash) -> String {
    format!("bundle.{}", hash.as_str())
}

pub struct Follower<E: Engine> {
    control: NamedRef<BundleApi>,
    streams: StreamNode,
    /// `None` on a node that only routes: it takes each manifest as it comes.
    engine: Option<E>,
    cfg: FollowerConfig,
    serving: Serving<E::Version>,
    /// The newest manifest seen of each worker that is not served yet.
    wanted: BTreeMap<String, Manifest>,
    bundles: BTreeMap<Hash, Bundle<E::Version>>,
    on_disk: BTreeSet<Hash>,
    /// Bundle files nothing refers to any more, removed with the next save that lands.
    garbage: Vec<Hash>,
    /// Who waits for an idle worker to run again.
    warming: BTreeMap<String, Vec<Warming<E::Version>>>,
    dirty: bool,
    saving: bool,
    failures: u32,
    retrying: bool,
}

impl<E: Engine + Clone> Follower<E> {
    pub fn new(cluster: &Cluster, streams: StreamNode, engine: Option<E>, cfg: FollowerConfig) -> Self {
        Self {
            control: on_control(cluster, BUNDLES),
            streams,
            engine,
            cfg,
            serving: Serving::default(),
            wanted: BTreeMap::new(),
            bundles: BTreeMap::new(),
            on_disk: BTreeSet::new(),
            garbage: Vec::new(),
            warming: BTreeMap::new(),
            dirty: false,
            saving: false,
            failures: 0,
            retrying: false,
        }
    }

    pub fn serving(&self) -> Serving<E::Version> {
        self.serving.clone()
    }

    fn subscribe(&mut self, cx: &mut Context<'_, Self>) {
        let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
        let attached = match self.streams.subscribe(&manifests(), options) {
            Ok(subscription) => cx.attach(subscription, Msg::Event).is_ok(),
            Err(_) => false,
        };
        if !attached {
            drop(cx.schedule_after(self.cfg.retry, Msg::Subscribe));
        }
    }

    /// Takes a manifest only over a lower one: the stream says everything again after the control node restarts.
    fn offer(&mut self, manifest: Manifest) {
        let behind = |seq: Option<u64>| seq.is_none_or(|seq| seq < manifest.seq);
        let fits = manifest.versions().all(|v| v.size <= MAX_BUNDLE && v.bundle.is_valid());
        if fits && behind(self.serving.seq(&manifest.name)) && behind(self.wanted.get(&manifest.name).map(|m| m.seq)) {
            self.wanted.insert(manifest.name.clone(), manifest);
        }
    }

    fn remove(&mut self, name: &str) {
        self.wanted.remove(name);
        if self.serving.get(name).is_some() {
            self.serving.change(|table| drop(table.remove(name)));
            self.dirty = true;
        }
        for waiting in self.warming.remove(name).into_iter().flatten() {
            let _ = waiting.reply(None); // an asker that gave up
        }
    }

    fn failed(&mut self, hash: Option<&Hash>, cx: &mut Context<'_, Self>) {
        self.serving.stats.failures.fetch_add(1, Relaxed);
        if let Some(bundle) = hash.and_then(|hash| self.bundles.get_mut(hash)) {
            *bundle = Bundle::Failed;
        }
        if !self.retrying {
            let pause = self.cfg.retry * (1 << self.failures.min(5));
            self.failures += 1;
            self.retrying = cx.schedule_after(pause, Msg::Retry).is_ok();
        }
    }

    /// Fetches the bundle if this node's disk did not have it, keeps it there, and loads it in the engine.
    fn prepare(&mut self, version: VersionRef, cached: Option<Bytes>, cx: &mut Context<'_, Self>) {
        let Some(engine) = self.engine.clone() else { return };
        let (files, cfg) = (self.cfg.files.clone(), self.cfg.clone());
        let control = self.control.clone();
        let hash = version.bundle.clone();
        let job = async move {
            let fetched = cached.is_none();
            let prepared = async {
                let bundle = match cached {
                    Some(bundle) => bundle,
                    None => {
                        let bundle = fetch(&control, &version, &cfg).await?;
                        // A bundle that cannot be kept is served all the same.
                        let _ = files.apply(vec![FileOp::Replace(bundle_file(&version.bundle), bundle.clone())]).await;
                        bundle
                    }
                };
                engine.load(bundle).await.map_err(|e| e.to_string())
            };
            Msg::Prepared(version.bundle.clone(), prepared.await.map(|loaded| (loaded, fetched)))
        };
        if cx.submit_future(job).is_err() {
            self.failed(Some(&hash), cx);
        }
    }

    /// Switches every worker whose bundles are ready, lets go of what nothing refers to, and starts on what is
    /// still missing.
    fn advance(&mut self, cx: &mut Context<'_, Self>) {
        let ready =
            |bundles: &BTreeMap<Hash, Bundle<E::Version>>, version: &VersionRef| match bundles.get(&version.bundle) {
                Some(Bundle::Ready(loaded)) => Some(loaded.clone()),
                _ => None,
            };
        let routes = self.engine.is_none();
        // A deployment, or a request, that names an idle worker's bundle has it loaded again.
        for version in self.wanted.values().flat_map(Manifest::versions) {
            if matches!(self.bundles.get(&version.bundle), Some(Bundle::Cold)) {
                self.bundles.remove(&version.bundle);
            }
        }
        let mut switched = Vec::new();
        for manifest in self.wanted.values() {
            let stable = ready(&self.bundles, &manifest.stable);
            let canary = manifest.canary.as_ref().map(|c| ready(&self.bundles, &c.version));
            if routes || (stable.is_some() && canary.as_ref().is_none_or(Option::is_some)) {
                switched.push(Arc::new(Current { manifest: manifest.clone(), stable, canary: canary.flatten() }));
            }
        }
        if !switched.is_empty() {
            let mut warmed = Vec::new();
            for current in &switched {
                let name = &current.manifest.name;
                self.wanted.remove(name);
                // The same deployment again is an idle worker back, and changes nothing on disk.
                let back = self.serving.seq(name) == Some(current.manifest.seq);
                let stats = &self.serving.stats;
                if back { &stats.warmed } else { &stats.switches }.fetch_add(1, Relaxed);
                self.dirty |= !back;
                let waiting = self.warming.remove(name).into_iter().flatten();
                warmed.extend(waiting.map(|waiting| (waiting, current.clone())));
            }
            self.serving.change(|table| table.extend(switched.into_iter().map(|c| (c.manifest.name.clone(), c))));
            // Only now: an asker reads the table as soon as it has the answer, maybe before this handler ends.
            for (waiting, current) in warmed {
                let _ = waiting.reply(Some(current));
            }
        }
        if routes {
            return self.save(cx);
        }

        let served = self.serving.all();
        let needed: BTreeMap<Hash, VersionRef> = served
            .values()
            .map(|current| &current.manifest)
            .chain(self.wanted.values())
            .flat_map(Manifest::versions)
            .map(|version| (version.bundle.clone(), version.clone()))
            .collect();
        self.bundles.retain(|hash, _| needed.contains_key(hash));
        let unused: Vec<Hash> = self.on_disk.iter().filter(|hash| !needed.contains_key(*hash)).cloned().collect();
        for hash in unused {
            self.on_disk.remove(&hash);
            self.garbage.push(hash);
            self.dirty = true;
        }
        let mut preparing = self.bundles.values().filter(|b| matches!(b, Bundle::Preparing)).count();
        for (hash, version) in needed {
            if preparing >= self.cfg.prepares {
                break;
            }
            if self.bundles.contains_key(&hash) {
                continue;
            }
            self.bundles.insert(hash.clone(), Bundle::Preparing);
            preparing += 1;
            if !self.on_disk.contains(&hash) {
                self.prepare(version, None, cx);
                continue;
            }
            let files = self.cfg.files.clone();
            let read = async move {
                let bundle = store::read(&*files, &bundle_file(&version.bundle)).await.ok().flatten();
                let whole = bundle.filter(|b| Hash::of(b) == version.bundle).map(Bytes::from);
                Msg::Cached(version, whole)
            };
            if cx.submit_future(read).is_err() {
                self.failed(Some(&hash), cx);
            }
        }
        self.save(cx);
    }

    /// Replaces the table on disk: a write that fails leaves the last one whole, and is made again.
    fn save(&mut self, cx: &mut Context<'_, Self>) {
        if !self.dirty || self.saving {
            return;
        }
        let manifests: Vec<Manifest> = self.serving.all().values().map(|c| c.manifest.clone()).collect();
        let body = serde_json::to_vec(&manifests).expect("a table is plain data");
        let mut ops = vec![FileOp::Replace(TABLE.to_owned(), body.into())];
        ops.extend(self.garbage.iter().map(|hash| FileOp::Remove(bundle_file(hash))));
        let removed = self.garbage.len();
        // Called in the job: a `Files` may write when `apply` is called, and a save whose job cannot start must not land.
        let files = self.cfg.files.clone();
        let job = async move { Msg::Saved(files.apply(ops).await.map(|()| removed).map_err(|e| e.to_string())) };
        match cx.submit_future(job) {
            Ok(_) => (self.dirty, self.saving) = (false, true),
            Err(_) => self.failed(None, cx),
        }
    }
}

/// The bundle in parts, refused unless it is the size and has the hash its manifest says. A reply cut part way is
/// asked for again from what came, until nothing has come for `patience`.
async fn fetch(control: &NamedRef<BundleApi>, version: &VersionRef, cfg: &FollowerConfig) -> Result<Bytes, String> {
    let mut bundle = BytesMut::with_capacity(version.size as usize);
    let (mut deadline, mut last) = (Instant::now() + cfg.patience, String::from("nothing came"));
    while (bundle.len() as u64) < version.size {
        if Instant::now() >= deadline {
            return Err(format!("fetching {}: {last}", version.bundle));
        }
        let ask = Fetch { hash: version.bundle.clone(), offset: bundle.len() as u64, max: cfg.piece };
        let mut parts = match control.request_parts(ask).await {
            Ok(parts) => parts,
            Err(e) if matches!(e.retry(), Retry::Later | Retry::Resolve) => {
                last = e.to_string();
                tokio::time::sleep(AGAIN).await;
                continue;
            }
            Err(e) => return Err(format!("fetching {}: {e}", version.bundle)),
        };
        loop {
            match parts.next().await {
                Some(Ok(Piece::Chunk { total, bytes })) if total == version.size => {
                    bundle.extend_from_slice(&bytes);
                    deadline = Instant::now() + cfg.patience;
                }
                Some(Ok(Piece::Chunk { total, .. })) => {
                    return Err(format!("{} is {total} bytes, not {}", version.bundle, version.size));
                }
                Some(Ok(Piece::Missing)) => {
                    return Err(format!("the control node has no bundle {}", version.bundle));
                }
                Some(Err(e)) => {
                    last = e.to_string();
                    tokio::time::sleep(AGAIN).await;
                    break;
                }
                None if (bundle.len() as u64) < version.size => {
                    return Err(format!("{} ended at {} bytes, not {}", version.bundle, bundle.len(), version.size));
                }
                None => break,
            }
        }
    }
    let bundle = bundle.freeze();
    if bundle.len() as u64 != version.size || Hash::of(&bundle) != version.bundle {
        return Err(format!("what came as {} is something else", version.bundle));
    }
    Ok(bundle)
}

/// The table, and the bundles on the disk.
async fn restore(files: Arc<dyn Files>) -> Result<Restored, String> {
    let listed = files.list().await.map_err(|e| format!("listing the cache: {e}"))?;
    let bundles = listed.iter().filter_map(|(name, _)| name.strip_prefix("bundle.")).map(Hash::of_hex).collect();
    let table = store::read(&*files, TABLE).await.map_err(|e| format!("reading {TABLE}: {e}"))?;
    // One that does not parse is none: the stream says the whole table again.
    let manifests = table.and_then(|table| serde_json::from_slice(&table).ok()).unwrap_or_default();
    Ok(Restored { manifests, bundles })
}

impl<E: Engine + Clone> Actor for Follower<E> {
    type Message = Msg<E::Version>;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let restore = restore(self.cfg.files.clone());
        let job =
            cx.submit_future(async move { Msg::Restored(restore.await) }).map_err(|e| format!("{:?}", e.reason))?;
        // What the stream says is weighed against what the disk held, so the disk comes first.
        cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
        self.subscribe(cx);
        Ok(())
    }

    fn on_message(&mut self, msg: Self::Message, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Restored(restored) => {
                let restored = restored?;
                self.on_disk = restored.bundles;
                restored.manifests.into_iter().for_each(|manifest| self.offer(manifest));
            }
            Msg::Event(Some(StreamEvent::Snapshot(state))) => {
                // The whole table: a worker it does not name was removed while this node was away.
                let mut named = BTreeSet::new();
                for (name, manifest) in state.records().iter().filter_map(Manifest::parse) {
                    named.insert(name);
                    manifest.into_iter().for_each(|manifest| self.offer(manifest));
                }
                let served = self.serving.all();
                let gone: Vec<String> =
                    served.keys().chain(self.wanted.keys()).filter(|name| !named.contains(*name)).cloned().collect();
                gone.iter().for_each(|name| self.remove(name));
            }
            Msg::Event(Some(StreamEvent::Batch(batch) | StreamEvent::Replacement { batch })) => {
                for (name, manifest) in batch.records().iter().filter_map(Manifest::parse) {
                    match manifest {
                        Some(manifest) => self.offer(manifest),
                        None => self.remove(&name),
                    }
                }
            }
            // Ready, CaughtUp, and an end or a reset, after which the subscription closes.
            Msg::Event(Some(_)) => return Ok(()),
            Msg::Event(None) | Msg::Subscribe => {
                self.subscribe(cx);
                return Ok(());
            }
            Msg::Cached(version, Some(bundle)) => self.prepare(version, Some(bundle), cx),
            Msg::Cached(version, None) => {
                self.on_disk.remove(&version.bundle);
                self.prepare(version, None, cx);
            }
            Msg::Prepared(hash, Ok((loaded, fetched))) => {
                let stats = &self.serving.stats;
                if fetched { &stats.fetched } else { &stats.from_cache }.fetch_add(1, Relaxed);
                self.failures = 0;
                self.on_disk.insert(hash.clone());
                // One nothing waits for any more is dropped here, and its file goes with the next save.
                if let Some(bundle) = self.bundles.get_mut(&hash) {
                    *bundle = Bundle::Ready(loaded);
                }
            }
            Msg::Prepared(hash, Err(_)) => self.failed(Some(&hash), cx),
            Msg::Saved(result) => {
                self.saving = false;
                let Ok(removed) = result else {
                    self.dirty = true;
                    self.failed(None, cx);
                    return Ok(());
                };
                self.garbage.drain(..removed);
                self.serving.stats.saved.fetch_add(1, Relaxed);
            }
            Msg::Retry => {
                self.retrying = false;
                self.bundles.retain(|_, bundle| !matches!(bundle, Bundle::Failed));
            }
            Msg::Evict(name) => {
                let idle = self.serving.get(&name).filter(|current| current.is_warm());
                let Some(current) = idle.filter(|_| !self.wanted.contains_key(&name)) else { return Ok(()) };
                for version in current.manifest.versions() {
                    self.bundles.insert(version.bundle.clone(), Bundle::Cold);
                }
                let cold = Arc::new(Current { manifest: current.manifest.clone(), stable: None, canary: None });
                self.serving.change(|table| drop(table.insert(name, cold)));
                self.serving.stats.evicted.fetch_add(1, Relaxed);
                return Ok(());
            }
            Msg::Warm(name, reply) => match self.serving.get(&name) {
                Some(current) if !current.is_warm() && self.engine.is_some() => {
                    let waiting = self.warming.entry(name.clone()).or_default();
                    // Those that stopped waiting go: a load that keeps failing would otherwise gather one per ask.
                    waiting.retain(|waiting| !waiting.is_abandoned());
                    waiting.push(reply);
                    self.wanted.entry(name).or_insert_with(|| current.manifest.clone());
                }
                current => {
                    let _ = reply.reply(current);
                    return Ok(());
                }
            },
        }
        self.advance(cx);
        Ok(())
    }
}
