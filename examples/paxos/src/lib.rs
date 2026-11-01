//! Paxos leases with the acceptors in processes of their own and one `Owner` actor per key on the apps.
//! `demo` runs the whole story in one process; `main.rs` runs each role as its own process.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, Discovery, MemoryDiscovery, PaxosConfig,
    PaxosLeases, Placement, Rejection, Reply, SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub const KIND: &str = "owner";

/// One per key. It answers with where it runs and its epoch; a real one passes the epoch to every store it writes
/// to, so a store can refuse a holder that lost its lease without knowing yet.
struct Owner(String);

impl Actor for Owner {
    type Message = Option<Reply<String>>;
    type Error = ();
    fn on_message(&mut self, reply: Option<Reply<String>>, _: &mut Context<'_, Self>) -> std::result::Result<(), ()> {
        if let Some(reply) = reply {
            let _ = reply.send(self.0.clone());
        }
        Ok(())
    }
}

pub struct OwnerApi;

impl WireProtocol for OwnerApi {
    type Message = ();
    type Reply = String;
    const ID: u64 = 0x9A;
    const SCHEMA: u16 = 1;
    fn encode(_: ()) -> std::result::Result<Bytes, CallError> {
        Ok(Bytes::new())
    }
    fn decode_reply(body: Bytes) -> std::result::Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for OwnerApi {
    type Mailbox = Option<Reply<String>>;
    fn encode_reply(owner: String) -> std::result::Result<Bytes, CallError> {
        Ok(Bytes::from(owner))
    }
    fn decode(_: Bytes, reply: Option<Reply<String>>) -> std::result::Result<Self::Mailbox, Rejection> {
        Ok(reply)
    }
}

/// What one node needs. `acceptors`, `lease` and `secret` must be the same on every node.
pub struct Settings {
    pub id: String,
    pub bind: SocketAddr,
    /// Where the others reach this node; needed when `bind` is a wildcard.
    pub advertise: Option<SocketAddr>,
    pub acceptors: Vec<String>,
    pub lease: Duration,
    pub secret: String,
    /// Where an acceptor logs who holds each key, in a directory named after the node.
    pub dir: PathBuf,
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
}

impl Settings {
    pub fn transport(&self, id: &str) -> Result<axon_net::Config> {
        let mut transport = axon_net::Config::new(id, SharedSecret::new(self.secret.as_bytes())?);
        // `demo` on loopback, an acceptor or app only with --plaintext: across the internet, set `ClusterConfig::tls`.
        transport.allow_plaintext = true;
        Ok(transport)
    }

    /// A node on the acceptor list serves as one; any node can hold leases. `transport` is the one `discovery` was
    /// built with: each `transport()` is a new incarnation of the node.
    pub async fn start(&self, transport: axon_net::Config, discovery: impl Discovery) -> Result<Node> {
        let mut paxos = PaxosConfig::new(self.acceptors.iter().cloned());
        // The operator's lease, from LEASE_SECS.
        paxos.lease = self.lease;
        // An acceptor logs who holds each key here; a node that is not one never reads it.
        let id = transport.identity.node_id.to_string();
        paxos.store = Some(Arc::new(axon::files::OsFiles::new(self.dir.join(id))));
        let mut cfg = ClusterConfig::new(self.bind, transport, discovery);
        cfg.advertise = self.advertise;
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
        // What seed discovery wants; harmless with the others.
        (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
        let system = ActorSystem::builder().build()?;
        let cluster = Cluster::start(&system, cfg).await?;
        Ok(Node { system, cluster })
    }

    /// An app node: it can host an `Owner` for any key.
    pub async fn start_app(&self, transport: axon_net::Config, discovery: impl Discovery) -> Result<Node> {
        let node = self.start(transport, discovery).await?;
        let id = self.id.clone();
        let build = move |cx: &SpawnContext, _: Bytes| {
            let key = cx.key.clone().unwrap_or_default();
            Ok(Owner(format!("{key} on {id} at epoch {}", cx.epoch.unwrap_or_default())))
        };
        node.cluster.kind::<OwnerApi>(KIND).register(build).await?;
        Ok(node)
    }
}

/// Where each key runs, as `(node, epoch)`, once none runs on `avoid`, asking every key's `Owner` to be sure.
async fn place(cluster: &Cluster, keys: &[String], avoid: &str, within: Duration) -> Result<Vec<(String, u64)>> {
    let deadline = Instant::now() + within;
    let mut placed = Vec::new();
    for key in keys {
        loop {
            let error = match cluster.get_or_spawn::<OwnerApi>(KIND, key, Bytes::new(), &Placement::default()).await {
                Ok(owner) if owner.node() != avoid => {
                    let (node, epoch) = (owner.node().to_owned(), owner.epoch().unwrap_or_default());
                    match owner.request(()).await {
                        Ok(said) if said == format!("{key} on {node} at epoch {epoch}") => {
                            placed.push((node, epoch));
                            break;
                        }
                        Ok(said) => {
                            return Err(format!("{key} is on {node} at epoch {epoch}, its actor says {said}").into());
                        }
                        Err(error) => error.to_string(),
                    }
                }
                Ok(owner) => format!("still on {}", owner.node()),
                Err(error) => error.to_string(),
            };
            if Instant::now() > deadline {
                return Err(format!("{key} not placed in {within:?}: {error}").into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    Ok(placed)
}

fn show(keys: &[String], placed: &[(String, u64)]) -> String {
    let mut by_node = BTreeMap::<&str, Vec<&str>>::new();
    for (key, (node, _)) in keys.iter().zip(placed) {
        by_node.entry(node).or_default().push(key);
    }
    by_node.iter().map(|(node, keys)| format!("{node}: {}", keys.join(" "))).collect::<Vec<_>>().join(", ")
}

/// Three acceptors and two apps in one process, on loopback. An app crashes and its keys run on the other; two
/// acceptors stop and the actors stop with their leases; one comes back and they run again.
pub async fn demo(lease: Duration) -> Result<()> {
    let started = Instant::now();
    let say = |what: String| println!("{:>6.2}s  {what}", started.elapsed().as_secs_f64());
    let settings = |id: &str| Settings {
        id: id.into(),
        bind: ([127, 0, 0, 1], 0).into(),
        advertise: None,
        acceptors: vec!["lease-0".into(), "lease-1".into(), "lease-2".into()],
        lease,
        secret: "paxos-example-secret".into(),
        dir: std::env::temp_dir().join(format!("paxos-demo-{}", std::process::id())),
    };
    let discovery = MemoryDiscovery::new();
    let mut acceptors = Vec::new();
    for id in ["lease-0", "lease-1", "lease-2"] {
        let s = settings(id);
        acceptors.push(s.start(s.transport(id)?, discovery.clone()).await?);
    }
    let mut apps = Vec::new();
    for id in ["app-a", "app-b"] {
        let s = settings(id);
        apps.push(s.start_app(s.transport(id)?, discovery.clone()).await?);
    }
    say(format!("3 acceptors and 2 apps up, lease {lease:?}; the acceptors warm up for a little over one lease"));

    let keys: Vec<String> = (0..8).map(|k| format!("k{k}")).collect();
    let patience = lease * 5 + Duration::from_secs(5);
    let first = place(&apps[0].cluster, &keys, "", patience).await?;
    say(format!("placed  {}", show(&keys, &first)));

    let crashed = first[0].0.clone();
    apps.retain(|app| app.cluster.identity().node_id != crashed); // dropped without a shutdown: a crash
    let at = Instant::now();
    say(format!("{crashed} crashed"));
    let moved = place(&apps[0].cluster, &keys, &crashed, patience).await?;
    say(format!("moved   {} in {:.1?}, once {crashed}'s leases ran out", show(&keys, &moved), at.elapsed()));
    for ((before, old), (_, new)) in first.iter().zip(&moved) {
        if before == &crashed && new <= old {
            return Err(format!("a moved key kept epoch {old}: {new}").into());
        }
    }

    let host = &apps[0].cluster;
    for acceptor in acceptors.drain(1..) {
        acceptor.system.shutdown().await?;
    }
    let at = Instant::now();
    say("lease-1 and lease-2 stopped: no majority".into());
    while host.stats().spawned > 0 {
        if at.elapsed() > lease * 2 {
            return Err(format!("{} actors ran {:?} without a majority", host.stats().spawned, at.elapsed()).into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    say(format!("every actor stopped in {:.1?}, before its lease could be granted again", at.elapsed()));
    match host.get_or_spawn::<OwnerApi>(KIND, &keys[0], Bytes::new(), &Placement::default()).await {
        Ok(owner) => return Err(format!("{} started without a majority", owner.node()).into()),
        Err(error) => say(format!("k0 refused: {error}")),
    }

    let s = settings("lease-1");
    acceptors.push(s.start(s.transport("lease-1")?, discovery.clone()).await?);
    say("lease-1 back, as a new process with the same id; it warms up first".into());
    let again = place(host, &keys, "", patience).await?;
    say(format!("placed  {}", show(&keys, &again)));

    for node in apps.into_iter().chain(acceptors) {
        node.system.shutdown().await?;
    }
    Ok(())
}
