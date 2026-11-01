//! A node's roles and what each starts: `control` keeps the registry and is the origin of the manifest stream,
//! `gateway` follows the manifests to route by, `runner` follows them and prepares each version in the engine. One
//! node may carry several.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use axon::files::Files;
use axon::{ActorSystem, Exit, JobExecutor, LifecycleHandle};
use axon_cluster::{Cluster, ClusterError, Member, NamedRef, Placement, QuotaConfig, Quotas, View, WireProtocol};
use axon_stream::{DiskConfig, Retention, RingLimits, StreamNode, StreamNodeConfig};
use futures_util::future::join_all;

use crate::admin::{Admin, AdminConfig};
use crate::durable::{Counters, Store};
use crate::engine::Engine;
use crate::follower::{Follower, FollowerConfig, Serving};
use crate::gateway::{Gateway, GatewayConfig};
use crate::invoker::{self, Invoker, InvokerConfig};
use crate::object::{self, Caller, ObjectConfig};
use crate::registry::{Bundles, Registry};
use crate::wire::{
    Answer, Ask, BUNDLES, BundleApi, Call, Hosted, INVOKE, InvokeApi, LOGS, NodeStats, REGISTRY, Refused, RegistryApi,
    STORE, StoreApi, Told, Upload, VersionDetail, VersionInfo, WorkerInfo, manifests,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Roles {
    pub control: bool,
    pub gateway: bool,
    pub runner: bool,
}

impl Roles {
    /// From `control,gateway,runner` or any of them.
    pub fn parse(list: &str) -> Result<Self, String> {
        let mut roles = Self::default();
        for role in list.split(',').filter(|r| !r.is_empty()) {
            match role {
                "control" => roles.control = true,
                "gateway" => roles.gateway = true,
                "runner" => roles.runner = true,
                other => return Err(format!("no role {other:?}: control, gateway or runner")),
            }
        }
        Ok(roles)
    }

    /// A label per role, for `ClusterConfig::labels`: a label has one value, and a node may have several roles.
    /// `role` names them all for the console.
    pub fn labels(&self) -> BTreeMap<String, String> {
        let roles = [("control", self.control), ("gateway", self.gateway), ("runner", self.runner)];
        let has: Vec<&str> = roles.into_iter().filter(|(_, has)| *has).map(|(role, _)| role).collect();
        let mut labels: BTreeMap<String, String> = has.iter().map(|role| (role.to_string(), "1".to_owned())).collect();
        if !has.is_empty() {
            labels.insert("role".into(), has.join("+"));
        }
        labels
    }
}

/// The nodes that run workers, for `cluster.nodes` and `resolve_all_among`.
pub fn runners() -> Placement {
    Placement::default().require("runner", "1")
}

/// The view's members that carry `role`'s label and can be sent to.
pub fn having<'a>(view: &'a View, role: &'a str) -> impl Iterator<Item = &'a Member> {
    view.members.iter().filter(move |member| member.available() && member.record.label(role).is_some())
}

/// A node's directories. One it has no role for is never touched.
#[derive(Clone)]
pub struct Stores {
    /// Control: the manifest stream's history.
    pub streams: Arc<dyn Files>,
    /// Control: the registry's log, the bundles and the sources.
    pub registry: Arc<dyn Files>,
    /// Control: what the objects store.
    pub objects: Arc<dyn Files>,
    /// Gateway and runner: the table served and, on a runner, its bundles.
    pub cache: FollowerConfig,
}

/// How a node's request path is set. The quota limits in `gateway` are the same on every node.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub invoker: InvokerConfig,
    pub gateway: GatewayConfig,
    pub objects: ObjectConfig,
    pub quotas: QuotaConfig,
    /// The most each runner's log stream keeps for a subscriber that comes late or falls behind: the oldest
    /// records go first.
    pub log_ring: LogRing,
    /// On the control node: the operator's API, started when this is set.
    pub admin: Option<AdminConfig>,
}

#[derive(Clone, Copy, Debug)]
pub struct LogRing {
    pub records: usize,
    pub bytes: usize,
}

impl Default for LogRing {
    fn default() -> Self {
        Self { records: 4096, bytes: 8 << 20 }
    }
}

#[derive(Clone)]
pub struct Node<V> {
    pub streams: StreamNode,
    /// What a gateway routes by and a runner runs; `None` on a node that is only control.
    pub serving: Option<Serving<V>>,
    pub gateway: Option<Gateway>,
    /// A runner's counters.
    pub invoked: Option<invoker::Stats>,
    /// On a gateway and on the control node.
    pub quotas: Option<Quotas>,
    /// On the control node, when `Settings::admin` is set.
    pub admin: Option<Admin>,
    /// On the control node: what the objects' store wrote and fenced.
    pub stored: Option<Arc<Counters>>,
    /// The actors the roles started, by name.
    actors: Vec<(&'static str, LifecycleHandle)>,
}

impl<V> Node<V> {
    /// Waits until one of the node's actors has ended, and says which and how. None of them is supervised: each
    /// holds what a restart would lose under its peers (the invoker its callers, the registry a write under way),
    /// so a node whose actor ended stops as a whole and starts again from its disk.
    pub async fn ended(&self) -> (&'static str, Exit) {
        let joins = self.actors.iter().map(|(name, actor)| Box::pin(async move { (*name, actor.join().await) }));
        futures_util::future::select_all(joins).await.0
    }
}

#[derive(Debug)]
pub enum Error {
    Refused(Refused),
    Cluster(ClusterError),
    /// An answer of another call's kind, or an actor that would not start.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Refused(refused) => refused.fmt(f),
            Error::Cluster(e) => e.fmt(f),
            Error::Other(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for Error {}

/// Starts what the roles ask for on a cluster node that carries `roles.labels()`.
pub async fn start<E: Engine + Clone>(
    system: &ActorSystem,
    cluster: &Cluster,
    roles: Roles,
    stores: Stores,
    engine: E,
    settings: Settings,
) -> Result<Node<E::Version>, Error> {
    let other = |e: &dyn fmt::Debug| Error::Other(format!("{e:?}"));
    let role = |role: &str| vec![BTreeMap::from([(role.to_owned(), "1".to_owned())])];
    let upstream = if roles.control { vec![] } else { role("control") };
    // Each runner is the origin of its own log stream, so whoever reads the logs asks the runners.
    let mut cfg = StreamNodeConfig::new(upstream).upstream_for(LOGS, role("runner"));
    let LogRing { records, bytes } = settings.log_ring;
    cfg.core.rings.push(RingLimits { namespace: LOGS.to_owned(), max_records: records, max_bytes: bytes });
    if roles.control {
        // The stream is compacted, so its files hold the table whatever goes; this bounds the changes kept beside it.
        let keep = Retention {
            namespace: manifests().namespace().to_owned(),
            max_bytes: 64 << 20,
            max_age: Duration::from_secs(30 * 86_400),
        };
        cfg.disk = Some(DiskConfig::new(stores.streams, vec![keep]));
    }
    let streams = StreamNode::start(cluster, cfg).await.map_err(Error::Cluster)?;
    let (mut actors, mut stored) = (Vec::new(), None);
    if roles.control {
        let runtime = settings.invoker.runtime.clone();
        let registry = Registry::new(engine.clone(), runtime, stores.registry.clone(), streams.clone());
        let mailbox = Registry::<E>::mailbox();
        let registry = system.spawn(registry).name(REGISTRY).mailbox(mailbox).start().map_err(|e| other(&e))?;
        cluster.publish::<RegistryApi>(&registry, REGISTRY).map_err(Error::Cluster)?;
        let bundles = system.spawn(Bundles::new(stores.registry)).name(BUNDLES).start().map_err(|e| other(&e))?;
        cluster.publish::<BundleApi>(&bundles, BUNDLES).map_err(Error::Cluster)?;
        let store = Store::new(stores.objects);
        stored = Some(store.counters());
        let store = system.spawn(store).name(STORE).mailbox(Store::mailbox()).start();
        let store = store.map_err(|e| other(&e))?;
        cluster.publish::<StoreApi>(&store, STORE).map_err(Error::Cluster)?;
        actors.extend([
            (REGISTRY, registry.lifecycle.clone()),
            (BUNDLES, bundles.lifecycle.clone()),
            (STORE, store.lifecycle.clone()),
        ]);
        // Its start reads the log and opens the stream.
        registry.ready().await.map_err(|e| Error::Other(format!("the registry did not start: {e}")))?;
    }
    let mut node = Node {
        streams: streams.clone(),
        serving: None,
        gateway: None,
        invoked: None,
        quotas: None,
        admin: None,
        stored,
        actors: Vec::new(),
    };
    if roles.gateway || roles.control {
        node.quotas = Some(Quotas::start(cluster, settings.quotas).await.map_err(Error::Cluster)?);
    }
    if roles.gateway || roles.runner {
        let follower = Follower::new(cluster, streams.clone(), roles.runner.then(|| engine.clone()), stores.cache);
        let serving = follower.serving();
        let follower = system.spawn(follower).name(FOLLOWER).start().map_err(|e| other(&e))?;
        actors.push((FOLLOWER, follower.lifecycle.clone()));
        if roles.runner {
            let id = &cluster.identity().node_id;
            let mailbox = Invoker::<E>::mailbox(&settings.invoker);
            let caller = Caller::new(cluster, settings.objects.references);
            let (jobs, objects) = (settings.invoker.runtime.clone().map(JobExecutor::tokio), engine.clone());
            let invoker = Invoker::new(
                id,
                engine,
                serving.clone(),
                follower.clone(),
                streams.clone(),
                caller.clone(),
                settings.invoker,
            );
            let shared = object::Shared {
                engine: objects,
                serving: serving.clone(),
                follower,
                jobs,
                caller,
                store: on_control(cluster, STORE),
                ending: invoker.ending(),
            };
            node.invoked = Some(invoker.stats());
            let invoker = system.spawn(invoker).name(INVOKE).mailbox(mailbox).start().map_err(|e| other(&e))?;
            // Its log stream is open before a gateway can find it.
            invoker.ready().await.map_err(|e| Error::Other(format!("the invoker did not start: {e}")))?;
            cluster.publish::<InvokeApi>(&invoker, INVOKE).map_err(Error::Cluster)?;
            actors.push((INVOKE, invoker.lifecycle.clone()));
            object::register(cluster, Arc::new(shared), &settings.objects).await.map_err(Error::Cluster)?;
        }
        if let (true, Some(quotas)) = (roles.gateway, node.quotas.clone()) {
            let routes = serving.clone();
            let deployed = move |name: &str| routes.get(name).is_some();
            let gateway = Gateway::start(system, cluster, quotas, deployed, settings.gateway.clone());
            let gateway = gateway.map_err(Error::Other)?;
            actors.push(("runners", gateway.lifecycle()));
            node.gateway = Some(gateway);
        }
        node.serving = Some(serving);
    }
    if let (true, Some(admin), Some(quotas)) = (roles.control, settings.admin, node.quotas.clone()) {
        // The admin's own way to the runners. A node that is only control follows no manifests: the admin asks the
        // registry whether a worker exists before it sends a request for it.
        let gateway = match node.gateway.clone() {
            Some(gateway) => gateway,
            None => Gateway::start(system, cluster, quotas, |_| true, settings.gateway).map_err(Error::Other)?,
        };
        actors.push(("runners", gateway.lifecycle()));
        let admin = Admin::start(system, cluster, streams, gateway, admin).map_err(Error::Other)?;
        actors.push(("observer", admin.lifecycle()));
        node.admin = Some(admin);
    }
    node.actors = actors;
    Ok(node)
}

/// The name a gateway's or a runner's follower of the manifests has on its node.
pub const FOLLOWER: &str = "follower";

/// Every available runner's counters, and the runners that did not answer, asked side by side.
pub async fn stats(cluster: &Cluster) -> (Vec<NodeStats>, Vec<String>) {
    let (invokers, _) = cluster.resolve_all_among::<InvokeApi>(INVOKE, &runners()).await;
    let told = join_all(invokers.iter().map(|invoker| invoker.request(Ask::Stats))).await;
    let answered: Vec<NodeStats> = told
        .into_iter()
        .filter_map(|told| match told {
            Ok(Told::Stats(stats)) => Some(stats),
            _ => None,
        })
        .collect();
    let silent = silent(cluster, answered.iter().map(|stats| stats.node.as_str()));
    (answered, silent)
}

/// The runners not among `answered`.
fn silent<'a>(cluster: &Cluster, answered: impl Iterator<Item = &'a str> + Clone) -> Vec<String> {
    let runners = cluster.nodes(&runners()).ids().into_iter();
    runners.filter(|node| answered.clone().all(|a| a != &**node)).map(|node| node.to_string()).collect()
}

/// The objects of `worker` (all of them for `None`) that run now, each with its runner, and the runners that did
/// not answer, or not to the end. In key order, asked of each runner `page` at a time, the runners side by side.
pub async fn objects(cluster: &Cluster, worker: Option<&str>, page: u32) -> (Vec<(String, Hosted)>, Vec<String>) {
    let prefix = worker.map(|worker| format!("{worker}/")).unwrap_or_default();
    let (invokers, _) = cluster.resolve_all_among::<InvokeApi>(INVOKE, &runners()).await;
    let pages = invokers.iter().map(|invoker| async {
        let (node, mut hosted, mut from) = (invoker.node().to_owned(), Vec::new(), None);
        loop {
            let ask = Ask::Objects { prefix: prefix.clone(), from: from.take(), limit: page.max(1) };
            let Ok(Told::Objects(objects, more)) = invoker.request(ask).await else { return None };
            // The first key past the page's last: no key holds a NUL.
            from = objects.last().filter(|_| more).map(|last| format!("{}\0", last.key));
            hosted.extend(objects.into_iter().map(|object| (node.clone(), object)));
            if from.is_none() {
                return Some((node, hosted));
            }
        }
    });
    let answered: Vec<(String, Vec<(String, Hosted)>)> = join_all(pages).await.into_iter().flatten().collect();
    let silent = silent(cluster, answered.iter().map(|(node, _)| node.as_str()));
    let mut hosted: Vec<(String, Hosted)> = answered.into_iter().flat_map(|(_, hosted)| hosted).collect();
    hosted.sort_by(|a, b| a.1.key.cmp(&b.1.key));
    (hosted, silent)
}

/// A name the control node publishes, looked up on the nodes labelled `control` alone: one silent node of the
/// others, a runner just cut off, holds no call up.
pub fn on_control<P: WireProtocol>(cluster: &Cluster, name: &str) -> NamedRef<P> {
    cluster.named::<P>(name).among(&Placement::default().require("control", "1"))
}

/// The registry, from any node.
pub struct Client(NamedRef<RegistryApi>);

/// An upload compiles: seconds on the real engine, more than a request's default.
const PATIENCE: Duration = Duration::from_secs(60);

impl Client {
    pub fn new(cluster: &Cluster) -> Self {
        Self(on_control(cluster, REGISTRY))
    }

    /// Sent once: a change that timed out may have happened.
    pub async fn call(&self, call: Call) -> Result<Answer, Error> {
        match self.0.request_within(call, PATIENCE).await.map_err(Error::Cluster)? {
            Answer::Refused(refused) => Err(Error::Refused(refused)),
            answer => Ok(answer),
        }
    }

    /// The new version's id, and the deployment's seq if it was deployed.
    pub async fn upload(&self, upload: Upload) -> Result<(u32, Option<u64>), Error> {
        match self.call(Call::Upload(upload)).await? {
            Answer::Uploaded { version, seq } => Ok((version, seq)),
            other => Err(unexpected(other)),
        }
    }

    pub async fn deploy(&self, name: &str, stable: u32, canary: Option<(u32, u8)>) -> Result<u64, Error> {
        match self.call(Call::Deploy { name: name.into(), stable, canary }).await? {
            Answer::Deployed { seq } => Ok(seq),
            other => Err(unexpected(other)),
        }
    }

    pub async fn delete(&self, name: &str) -> Result<(), Error> {
        match self.call(Call::Delete { name: name.into() }).await? {
            Answer::Deleted => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn list(&self) -> Result<Vec<WorkerInfo>, Error> {
        match self.call(Call::List).await? {
            Answer::Workers(workers) => Ok(workers),
            other => Err(unexpected(other)),
        }
    }

    pub async fn get(&self, name: &str) -> Result<(WorkerInfo, Vec<VersionInfo>), Error> {
        match self.call(Call::Get { name: name.into() }).await? {
            Answer::Worker(info, versions) => Ok((info, versions)),
            other => Err(unexpected(other)),
        }
    }

    pub async fn version(&self, name: &str, id: u32) -> Result<VersionDetail, Error> {
        match self.call(Call::Version { name: name.into(), id }).await? {
            Answer::Version(detail) => Ok(detail),
            other => Err(unexpected(other)),
        }
    }
}

pub(crate) fn unexpected(answer: Answer) -> Error {
    Error::Other(format!("an answer to another call: {answer:?}"))
}
