//! An event-sourced ledger: one `Account` actor per account, owned under Paxos leases by one of three nodes, its
//! events in a log on a directory every node shares. `demo` runs the whole story in one process and `audit` checks
//! the logs on disk.

pub mod account;
pub mod audit;
pub mod demo;
pub mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, Discovery, KeyedRefs, PaxosConfig, PaxosLeases, Placement, Retry,
    SpawnContext,
};
use axon_net::SharedSecret;
use bytes::Bytes;

pub use account::{Account, AccountApi, Answer, Command, KIND};
pub use store::Outcome;

pub use demo::{Report, demo};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What every node needs. All but `bind` must be the same on every node.
#[derive(Clone)]
pub struct Settings {
    pub bind: SocketAddr,
    pub advertise: Option<SocketAddr>,
    /// The acceptors: the ids of the nodes that host accounts.
    pub hosts: Vec<String>,
    pub lease: Duration,
    pub secret: String,
    /// The directory every node shares; it stands in for shared storage.
    pub dir: PathBuf,
    pub snapshot_every: usize,
    pub request_timeout: Duration,
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
}

impl Settings {
    /// On loopback, with a short `request_timeout` so that a slow call is retried rather than waited for.
    pub fn new(hosts: Vec<String>, lease: Duration) -> Self {
        Settings {
            bind: ([127, 0, 0, 1], 0).into(),
            advertise: None,
            hosts,
            lease,
            secret: "ledger-example-secret".into(),
            dir: "ledger-data".into(),
            snapshot_every: 32,
            // Short on loopback, so a slow call is retried rather than waited for.
            request_timeout: Duration::from_secs(1),
        }
    }

    pub fn transport(&self, id: &str) -> Result<axon_net::Config> {
        let mut transport = axon_net::Config::new(id, SharedSecret::new(self.secret.as_bytes())?);
        transport.allow_plaintext = true; // `demo` on loopback, `host` and `load` with --plaintext
        Ok(transport)
    }

    /// A node that hosts accounts when its id is one of `hosts`, and is only a client otherwise.
    pub async fn start(&self, transport: axon_net::Config, discovery: impl Discovery) -> Result<Node> {
        let id = transport.identity.node_id.to_string();
        let mut paxos = PaxosConfig::new(self.hosts.iter().cloned());
        // The operator's lease, from Settings.
        paxos.lease = self.lease;
        // An acceptor logs who holds each key here; a node that is not one never reads it.
        paxos.store = Some(Arc::new(axon::files::OsFiles::new(self.dir.join(store::LEASES).join(&id))));
        let mut cfg = ClusterConfig::new(self.bind, transport, discovery);
        // The operator's request timeout, from Settings.
        (cfg.advertise, cfg.request_timeout) = (self.advertise, self.request_timeout);
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
        // With seeds, as guide/cluster.md § Seed servers asks.
        (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
        let system = ActorSystem::builder().build()?;
        let cluster = Cluster::start(&system, cfg).await?;
        if self.hosts.contains(&id) {
            std::fs::create_dir_all(&self.dir)?;
            let (dir, every) = (self.dir.clone(), self.snapshot_every);
            let accounts = cluster.keyed_refs(KIND, &Placement::default(), REFS);
            let build = move |cx: &SpawnContext, _: Bytes| {
                let id = cx
                    .key
                    .clone()
                    .filter(|id| account::valid_id(id))
                    .ok_or("an account id is 1..=64 of [A-Za-z0-9_-]")?;
                let epoch = cx.epoch.ok_or("accounts need an ownership authority")?;
                Ok(Account::new(id, epoch, dir.clone(), every, accounts.clone()))
            };
            cluster.kind::<AccountApi>(KIND).register(build).await?;
        }
        Ok(Node { system, cluster })
    }
}

/// The account references a node or a client keeps: more than the demo's accounts.
const REFS: usize = 1 << 16;

/// Calls accounts through one reference per account, which finds the account again after it moves.
pub struct Client {
    accounts: KeyedRefs<AccountApi>,
}

impl Client {
    pub fn new(cluster: &Cluster) -> Self {
        Client { accounts: cluster.keyed_refs(KIND, &Placement::default(), REFS) }
    }

    /// One attempt, and the node that answered.
    pub async fn once(&mut self, account: &str, command: Command) -> Result<(Answer, String), ClusterError> {
        let remote = self.accounts.get(account, Bytes::new());
        let answer = remote.request(command).await?;
        Ok((answer, remote.node().expect("an answered request keeps its reference").to_string()))
    }

    /// The same command until it is answered or refused for good: its idempotency key makes every retry safe.
    /// Returns the answer, the node and the number of attempts.
    pub async fn call(&mut self, account: &str, command: Command, patience: Duration) -> Result<(Answer, String, u32)> {
        let deadline = Instant::now() + patience;
        for attempt in 1.. {
            match self.once(account, command.clone()).await {
                Ok((answer, node)) => return Ok((answer, node, attempt)),
                Err(e) if e.retry() == Retry::Never || Instant::now() > deadline => {
                    return Err(format!("{account}: {command:?} unanswered after {attempt} attempts: {e}").into());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        unreachable!("the loop only ends by returning")
    }
}
