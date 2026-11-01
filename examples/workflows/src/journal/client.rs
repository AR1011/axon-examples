//! An owner's side of the journal over the network. [`JournalClient`] carries a request to one replica, a read in
//! parts, and asks a majority whether a run exists. [`Owner`] drives one [`Writer`] through its open and its
//! appends: the writer decides the quorum, the resends after a `Gap` or silence, and the recovery reads; the owner
//! only carries its messages and its clock. The World stories and the bench write through it; the run actor of a
//! later phase drives the same writer from its own turns.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon_cluster::{Cluster, RemoteRef, Retry};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::core::{Answer, AppendError, Out, Request, Writer, WriterConfig};
use super::index::{Status, Summary};
use super::service::read_parts;
use super::wire::{JOURNAL, JournalApi};

#[derive(Clone)]
pub struct JournalClient {
    cluster: Cluster,
    replicas: Arc<[Arc<str>]>,
    refs: Arc<Mutex<Vec<Option<RemoteRef<JournalApi>>>>>,
}

impl JournalClient {
    pub fn new(cluster: Cluster, replicas: Vec<Arc<str>>) -> Self {
        let refs = Arc::new(Mutex::new(vec![None; replicas.len()]));
        Self { cluster, replicas: replicas.into(), refs }
    }

    /// One request to replica `r`; a read comes in parts, and its records in one answer.
    pub async fn call(&self, r: usize, request: Request) -> Result<Answer, String> {
        let cached = self.refs.lock().map_err(|e| e.to_string())?[r].clone();
        let remote = match cached {
            Some(remote) => remote,
            None => {
                let remote = self.cluster.resolve_on::<JournalApi>(&self.replicas[r], JOURNAL).await;
                let remote = remote.map_err(|e| e.to_string())?;
                self.refs.lock().map_err(|e| e.to_string())?[r] = Some(remote.clone());
                remote
            }
        };
        let answer = match request {
            Request::Read { .. } => read_parts(&remote, request).await,
            request => remote.request(request).await,
        };
        answer.map_err(|e| {
            if e.retry() == Retry::Resolve
                && let Ok(mut refs) = self.refs.lock()
            {
                refs[r] = None;
            }
            e.to_string()
        })
    }

    /// What a majority says of a run, raising no fence.
    pub async fn probe(&self, run: Bytes) -> Result<Probe, String> {
        let mut asked = tokio::task::JoinSet::new();
        for r in 0..self.replicas.len() {
            let (client, run) = (self.clone(), run.clone());
            asked.spawn(async move { client.call(r, Request::Probe { run }).await });
        }
        let mut tally = Tally::new(self.replicas.len());
        while let Some(answer) = asked.join_next().await {
            if let Some(probe) = tally.add(answer.ok().and_then(Result::ok)) {
                return probe;
            }
        }
        Err("no majority of the replicas answered".into())
    }
}

/// What a probe found of a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Probe {
    /// A majority holds no record of it: there is no such run.
    Unknown,
    /// A replica holds records of it; the summary of the newest segment any replica indexes.
    Exists(Option<Summary>),
    /// A majority indexes it ended, from the same segment with the same status: a start, a signal or an action is
    /// answered from this, and no owner need open the run.
    Finished(Summary),
}

/// The answers to a probe, until they decide.
pub struct Tally {
    replicas: usize,
    answered: usize,
    none: usize,
    newest: Option<(u64, Option<Summary>)>,
    /// Answers that index the run ended, by segment and status.
    ended: BTreeMap<(u64, Status), (Summary, usize)>,
}

impl Tally {
    pub fn new(replicas: usize) -> Self {
        Self { replicas, answered: 0, none: 0, newest: None, ended: BTreeMap::new() }
    }

    /// One replica's answer, or `None` where it gave none: what the answers decide so far.
    pub fn add(&mut self, answer: Option<Answer>) -> Option<Result<Probe, String>> {
        self.answered += 1;
        if let Some(Answer::Probed { segments, summary }) = answer {
            // A seal with no record after it, as an open cut short leaves, is not a run.
            if segments.iter().all(|(_, n)| *n == 0) {
                self.none += 1;
            } else {
                let segment = summary.as_ref().map_or(0, |(segment, _)| *segment);
                if let Some((segment, s)) = &summary
                    && s.status.is_finished()
                {
                    self.ended.entry((*segment, s.status)).or_insert_with(|| (s.clone(), 0)).1 += 1;
                }
                if self.newest.as_ref().is_none_or(|(newest, _)| segment >= *newest) {
                    self.newest = Some((segment, summary.map(|(_, s)| s)));
                }
            }
        }
        let quorum = self.replicas / 2 + 1;
        if self.none >= quorum {
            return Some(Ok(Probe::Unknown));
        }
        if let Some((s, _)) = self.ended.values().find(|(_, n)| *n >= quorum) {
            return Some(Ok(Probe::Finished(s.clone())));
        }
        let ended = self.ended.values().map(|(_, n)| *n).max().unwrap_or(0);
        let left = self.replicas - self.answered;
        match &self.newest {
            Some((_, summary)) if ended + left < quorum => Some(Ok(Probe::Exists(summary.clone()))),
            _ if left == 0 => Some(Err("no majority of the replicas answered".into())),
            _ => None,
        }
    }
}

/// What a run's writer sends its requests through: the replicas over the network, or one in this process.
pub trait Transport: Send + Sync + 'static {
    fn replicas(&self) -> usize;
    fn call(&self, r: usize, request: Request) -> BoxFuture<'static, Result<Answer, String>>;
    /// What a majority says of the run, raising no fence.
    fn probe(&self, run: Bytes) -> BoxFuture<'static, Result<Probe, String>>;
}

impl Transport for JournalClient {
    fn replicas(&self) -> usize {
        self.replicas.len()
    }

    fn call(&self, r: usize, request: Request) -> BoxFuture<'static, Result<Answer, String>> {
        let client = self.clone();
        Box::pin(async move { client.call(r, request).await })
    }

    fn probe(&self, run: Bytes) -> BoxFuture<'static, Result<Probe, String>> {
        let client = self.clone();
        Box::pin(async move { client.probe(run).await })
    }
}

/// Why an owner stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ended {
    /// A newer owner sealed the run.
    Fenced(u64),
    Failed(String),
    /// No majority answered in time.
    Timeout,
}

/// One owner of one run, driving its writer from the task that calls it.
pub struct Owner {
    client: JournalClient,
    writer: Writer,
    answers: mpsc::Receiver<(usize, Answer)>,
    to_self: mpsc::Sender<(usize, Answer)>,
    start: Instant,
    opened: Option<Vec<Bytes>>,
    /// Journal entries on a majority.
    acked: u64,
}

/// How often the writer looks for requests to send again.
const TICK: Duration = Duration::from_millis(100);

impl Owner {
    /// Seals the run at `lease`'s epoch and recovers its journal: the journal before this owner, once its `Own` is on
    /// a majority.
    pub async fn open(
        client: JournalClient,
        run: Bytes,
        lease: u64,
        cfg: WriterConfig,
        within: Duration,
    ) -> Result<(Owner, Vec<Bytes>), Ended> {
        let (to_self, answers) = mpsc::channel(4096);
        let writer = Writer::open(run, lease, axon::trace::random_u64(), cfg, 0);
        let mut owner = Owner { client, writer, answers, to_self, start: Instant::now(), opened: None, acked: 0 };
        owner.drive(within, |o| o.opened.is_some()).await?;
        let journal = owner.opened.take().unwrap_or_default();
        Ok((owner, journal))
    }

    pub fn epoch(&self) -> u64 {
        self.writer.epoch()
    }

    /// Appends the entries, and returns once all are on a majority: the journal index of each.
    pub async fn append(&mut self, entries: Vec<Bytes>, within: Duration) -> Result<Vec<u64>, Ended> {
        let deadline = Instant::now() + within;
        let mut indexes = Vec::with_capacity(entries.len());
        for body in entries {
            loop {
                match self.writer.append(body.clone(), self.now()) {
                    Ok(i) => {
                        indexes.push(i);
                        break;
                    }
                    Err(AppendError::Behind) => {
                        let acked = self.acked;
                        self.drive(deadline.saturating_duration_since(Instant::now()), |o| o.acked > acked).await?;
                    }
                    Err(AppendError::NotOpen) => return Err(Ended::Failed("the writer is not open".into())),
                    Err(AppendError::TooLarge) => return Err(Ended::Failed("an entry past the batch bound".into())),
                }
            }
        }
        let last = indexes.last().copied();
        self.drive(deadline.saturating_duration_since(Instant::now()), |o| last.is_none_or(|i| o.acked > i)).await?;
        Ok(indexes)
    }

    fn now(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Carries the writer's messages until `done`, or it ends.
    async fn drive(&mut self, within: Duration, done: impl Fn(&Owner) -> bool) -> Result<(), Ended> {
        let deadline = Instant::now() + within;
        let mut tick = tokio::time::interval(TICK);
        loop {
            for out in self.writer.take_out() {
                match out {
                    Out::Send(r, request) => {
                        let (client, to_self) = (self.client.clone(), self.to_self.clone());
                        tokio::spawn(async move {
                            if let Ok(answer) = client.call(r, request).await {
                                drop(to_self.send((r, answer)).await);
                            }
                        });
                    }
                    Out::Opened(journal) => self.opened = Some(journal),
                    Out::Acked(n) => self.acked = n,
                    Out::Fenced(fence) => return Err(Ended::Fenced(fence)),
                    Out::Failed(why) => return Err(Ended::Failed(why)),
                }
            }
            if done(self) {
                return Ok(());
            }
            tokio::select! {
                Some((r, answer)) = self.answers.recv() => self.writer.answer(r, answer, self.now()),
                _ = tick.tick() => self.writer.tick(self.now()),
                () = tokio::time::sleep_until(deadline) => return Err(Ended::Timeout),
            }
        }
    }
}
