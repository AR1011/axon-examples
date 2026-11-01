//! One per worker node. It keeps at most `concurrency` jobs running, each as a job of its own, and polls the broker
//! whenever one ends and every `poll` otherwise. The poll carries the outcomes and the list of what still runs, which
//! is what keeps this worker's jobs its own.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use axon::{Actor, ActorHandle, Context, MailboxLimits};
use axon_cluster::{ClusterError, NamedRef, Retry};

use crate::{Answer, BROKER, BrokerApi, Call, Delivery, Node, Outcome, Result, Ticket};

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub concurrency: usize,
    /// Must be well under the broker's `visibility`: a poll is also this worker's heartbeat.
    pub poll: Duration,
    /// A run that takes longer counts as a failure.
    pub job_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self { concurrency: 8, poll: Duration::from_millis(100), job_timeout: Duration::from_secs(60) }
    }
}

pub enum Msg {
    Poll,
    Fetched(std::result::Result<Answer, ClusterError>, Vec<Outcome>),
    Ran(Outcome),
}

pub struct Worker<H> {
    id: String,
    cfg: WorkerConfig,
    broker: NamedRef<BrokerApi>,
    handler: H,
    running: BTreeSet<Ticket>,
    outcomes: Vec<Outcome>,
    busy: bool,
    again: bool,
}

/// Starts this node's worker. `handler` runs one job; an `Err` is a failure the broker retries.
pub fn start_worker<H, F>(node: &Node, cfg: WorkerConfig, handler: H) -> Result<ActorHandle<Msg>>
where
    H: Fn(Delivery) -> F + Send + 'static,
    F: Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    // Each run is a job, and so is the poll, which finds the broker when it has to.
    let limits = MailboxLimits { jobs: cfg.concurrency + 1, ..MailboxLimits::default() };
    let worker = Worker {
        id: node.cluster.identity().node_id.clone(),
        cfg,
        broker: node.cluster.named(BROKER),
        handler,
        running: BTreeSet::new(),
        outcomes: Vec::new(),
        busy: false,
        again: true,
    };
    Ok(node.system.spawn(worker).name("worker").mailbox(limits).start()?)
}

impl<H, F> Worker<H>
where
    H: Fn(Delivery) -> F + Send + 'static,
    F: Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    fn poll(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        self.busy = true;
        self.again = false;
        let broker = self.broker.clone();
        let outcomes = std::mem::take(&mut self.outcomes);
        let call = Call::Fetch {
            worker: self.id.clone(),
            want: self.cfg.concurrency.saturating_sub(self.running.len()) as u32,
            running: self.running.iter().copied().collect(),
            outcomes: outcomes.clone(),
        };
        cx.submit_future(async move { Msg::Fetched(broker.request(call).await, outcomes) })
            .map(drop)
            .map_err(|e| format!("{e:?}"))
    }

    fn run(&mut self, d: Delivery, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        let (ticket, timeout) = (d.ticket, self.cfg.job_timeout);
        let job = (self.handler)(d);
        self.running.insert(ticket);
        let run = async move {
            // A panic would fail the worker for one bad job: it is the run's failure instead.
            let error = match tokio::time::timeout(timeout, axon::catch_panic(job)).await {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(e))) => Some(e),
                Ok(Err(panic)) => Some(format!("panicked: {panic}")),
                Err(_) => Some(format!("timed out after {timeout:?}")),
            };
            Msg::Ran(Outcome { ticket, error })
        };
        cx.submit_future(run).map(drop).map_err(|e| format!("{e:?}"))
    }
}

impl<H, F> Actor for Worker<H>
where
    H: Fn(Delivery) -> F + Send + 'static,
    F: Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        cx.schedule_every(self.cfg.poll, || Msg::Poll).map_err(|e| format!("{:?}", e.reason))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        match msg {
            Msg::Poll => self.again = true,
            Msg::Fetched(Ok(Answer::Jobs(jobs)), _) => {
                self.busy = false;
                // A full batch says there may be more; otherwise wait for a run to end or the next poll.
                self.again = !jobs.is_empty() && self.running.len() + jobs.len() < self.cfg.concurrency;
                for job in jobs {
                    self.run(job, cx)?;
                }
            }
            Msg::Fetched(result, outcomes) => {
                self.busy = false;
                // Sent again with the next poll; the broker ignores an outcome it has already counted.
                self.outcomes.extend(outcomes);
                match result {
                    Err(ClusterError::Call(e)) if e.retry() == Retry::Never => {
                        return Err(format!("a fetch failed: {e:?}"));
                    }
                    // A fetch that did not arrive, or whose answer did not, hands out nothing that stays lost. A
                    // failed lookup of the broker (none yet, or two while one takes over) is tried with the next poll.
                    Err(_) => {}
                    Ok(answer) => return Err(format!("unexpected answer to a fetch: {answer:?}")),
                }
            }
            Msg::Ran(outcome) => {
                self.running.remove(&outcome.ticket);
                self.outcomes.push(outcome);
                self.again = true;
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> std::result::Result<(), String> {
        if self.again && !self.busy { self.poll(cx) } else { Ok(()) }
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Poll => "poll",
            Msg::Fetched(..) => "fetched",
            Msg::Ran(_) => "ran",
        }
    }
}
