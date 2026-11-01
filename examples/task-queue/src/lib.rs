//! A background-job queue. One `Broker` actor on the queue node holds every queue's jobs; each worker node runs one
//! `Worker` actor that pulls jobs, runs them with a concurrency limit and reports the outcomes on its next pull.
//! `demo` runs the whole story in one process; `main.rs` runs each role as its own process.

pub mod broker;
pub mod demo;
pub mod worker;

use std::collections::BTreeMap;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, Discovery, NamedRef, Rejection, Reply, Retry, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// The name the broker is published under.
pub const BROKER: &str = "task-queue.broker";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewJob {
    /// Chosen by the producer, so that enqueueing again after a lost answer is not a second job.
    pub id: u64,
    pub queue: String,
    /// Higher runs first; equal priorities run in the order they were enqueued.
    pub priority: u8,
    /// Relative to when the broker takes the job, so that no two clocks have to agree.
    pub delay_ms: u64,
    /// Failed runs before the job is dead.
    pub max_attempts: u32,
    pub payload: String,
}

/// Names one hand-out of a job: `boot` is the broker's, since a restarted broker starts counting again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Ticket {
    pub boot: u64,
    pub id: u64,
    pub delivery: u32,
}

/// A job handed to a worker. `failures` counts reported failures over the job's life.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delivery {
    pub ticket: Ticket,
    pub queue: String,
    pub payload: String,
    pub failures: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub ticket: Ticket,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    Pending,
    Delayed,
    Running,
    Done,
    Dead,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub pending: u64,
    pub delayed: u64,
    pub running: u64,
    pub done: u64,
    pub dead: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Load {
    pub running: u64,
    pub done: u64,
    pub failed: u64,
    /// Deliveries taken back from this worker: it stopped polling, or no longer listed them.
    pub lost: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub queues: BTreeMap<String, Counts>,
    pub workers: BTreeMap<String, Load>,
    pub capacity: usize,
    /// The most jobs ever waiting (pending and delayed) at once.
    pub max_queued: usize,
    /// New jobs refused because the queue was full.
    pub refused: u64,
    /// Outcomes for a delivery that had been taken back already, or that an earlier boot of the broker handed out.
    pub stale: u64,
}

impl Status {
    pub fn total(&self) -> Counts {
        let mut t = Counts::default();
        for c in self.queues.values() {
            t.pending += c.pending;
            t.delayed += c.delayed;
            t.running += c.running;
            t.done += c.done;
            t.dead += c.dead;
        }
        t
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobView {
    pub id: u64,
    pub queue: String,
    pub state: State,
    pub deliveries: u32,
    pub failures: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Call {
    /// Answered with `Enqueued`: the first `accepted` jobs are in, the rest were refused because the queue is full.
    Enqueue(Vec<NewJob>),
    /// Reports outcomes and asks for up to `want` jobs. `running` lists every delivery the worker still runs: the
    /// broker takes back any other it handed this worker.
    Fetch {
        worker: String,
        want: u32,
        running: Vec<Ticket>,
        outcomes: Vec<Outcome>,
    },
    Status,
    Inspect(Vec<u64>),
    Dead {
        limit: usize,
    },
    /// Moves dead jobs back to pending with a fresh set of attempts. Answered with `Retried`.
    Retry(Vec<u64>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Answer {
    Enqueued { accepted: usize },
    Jobs(Vec<Delivery>),
    Status(Box<Status>),
    Views(Vec<JobView>),
    Retried(usize),
}

pub struct BrokerApi;

impl WireProtocol for BrokerApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0x7A5C;
    const SCHEMA: u16 = 1;
    fn encode(call: Call) -> std::result::Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> std::result::Result<Answer, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for BrokerApi {
    type Mailbox = broker::Msg;
    fn encode_reply(answer: Answer) -> std::result::Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> std::result::Result<broker::Msg, Rejection> {
        let call = serde_json::from_slice(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(broker::Msg::Call(call, reply))
    }
}

pub fn json<T: Serialize>(value: &T) -> std::result::Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

/// A call whose answer has the wrong variant: the two sides disagree on the contract.
pub fn unexpected(answer: Answer) -> CallError {
    CallError::Codec(format!("unexpected answer {answer:?}"))
}

pub struct Node {
    pub system: ActorSystem,
    pub cluster: Cluster,
}

/// One node of the cluster, labelled with its role. `secret` must be the same on every node.
pub async fn start_node(
    transport: axon_net::Config,
    bind: SocketAddr,
    advertise: Option<SocketAddr>,
    role: &str,
    discovery: impl Discovery,
) -> Result<Node> {
    let mut cfg = ClusterConfig::new(bind, transport, discovery);
    cfg.advertise = advertise;
    cfg.labels.insert("role".into(), role.into());
    // What seed discovery wants; harmless with the others.
    (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
    let system = ActorSystem::builder().build()?;
    let cluster = Cluster::start(&system, cfg).await?;
    Ok(Node { system, cluster })
}

pub fn transport(id: &str, secret: &str) -> Result<axon_net::Config> {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(secret.as_bytes())?);
    // `demo` on loopback, the binary's nodes only with --plaintext: across the internet, set `ClusterConfig::tls`.
    transport.allow_plaintext = true;
    Ok(transport)
}

/// Waits until the broker answers. The reference finds it again after it moves.
pub async fn find_broker(cluster: &Cluster, within: Duration) -> Result<NamedRef<BrokerApi>> {
    let broker = cluster.named(BROKER);
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match broker.request_until(Call::Status, deadline).await {
            Ok(_) => return Ok(broker),
            // A status is a read: one whose answer may not have arrived is asked again.
            Err(e) if e.retry() == Retry::IfIdempotent && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => return Err(format!("no broker in {within:?}: {e:?}").into()),
        }
    }
}
