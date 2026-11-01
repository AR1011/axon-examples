//! What the control node knows of the request path: the [`Observer`] actor follows every runner's log stream and
//! asks every runner for its counters.
//!
//! Each runner is the origin of `logs/<node>`, so the observer follows the view: it subscribes to a runner that
//! joins, from the first record its ring still holds, and lets go of one that leaves. It keeps the newest records
//! for the invocation list, hands each one to whoever tails, and turns the counters into one point per worker
//! every `sample`. All of it is in memory and bounded: a restart of the control node starts again from what the
//! runners' rings hold.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, Context, Histogram, JobHandle, ReplyTo};
use axon_cluster::{Cluster, NodeChange, Nodes};
use axon_stream::{
    Cursor, EndReason, ResetReason, StartPosition, StreamError, StreamEvent, StreamNode, SubscribeOptions,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::node;
use crate::wire::{LogRecord, NodeStats, logs};

#[derive(Clone, Debug)]
pub struct ObserverConfig {
    /// The invocations kept for the list, the oldest going first.
    pub recent: usize,
    pub recent_bytes: usize,
    /// How often the runners are asked for their counters, and how many points a worker's series keeps.
    pub sample: Duration,
    pub points: usize,
    /// Records a tail may fall behind by before it misses some.
    pub tail: usize,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        Self { recent: 2048, recent_bytes: 16 << 20, sample: Duration::from_secs(10), points: 360, tail: 1024 }
    }
}

/// The median and the 99th percentile of `h`, in microseconds.
pub(crate) fn p50_p99_us(h: &Histogram) -> (Option<u64>, Option<u64>) {
    let us = |p| h.percentile(p).map(|d| d.as_micros() as u64);
    (us(0.5), us(0.99))
}

/// What a worker did between two samples, over every runner.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Point {
    pub at_ms: u64,
    pub requests: u64,
    /// Runs with no response of the worker's own: an exception, a limit, the engine.
    pub errors: u64,
    pub cpu_p50_us: Option<u64>,
    pub cpu_p99_us: Option<u64>,
    pub wall_p50_us: Option<u64>,
    pub wall_p99_us: Option<u64>,
}

/// Which invocations a list or a tail shows.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Filter {
    #[serde(skip)]
    pub worker: Option<String>,
    /// A status (`404`), a class (`5xx`) or an outcome (`exception`).
    pub status: Option<String>,
    /// Text in the URL or in a console line.
    pub q: Option<String>,
    pub node: Option<String>,
    pub trace: Option<String>,
    pub limit: Option<usize>,
}

impl Filter {
    pub fn admits(&self, record: &LogRecord) -> bool {
        let is = |wanted: &Option<String>, has: &str| wanted.as_deref().is_none_or(|wanted| wanted == has);
        let status = |wanted: &str| {
            let status = record.status.to_string();
            wanted == status || wanted == record.outcome || wanted.strip_suffix("xx") == Some(&status[..1])
        };
        is(&self.worker, &record.worker)
            && is(&self.node, &record.node)
            && self.trace.as_deref().is_none_or(|trace| record.trace.as_deref() == Some(trace))
            && self.status.as_deref().is_none_or(status)
            && self.q.as_deref().is_none_or(|q| record.url.contains(q) || record.logs.iter().any(|l| l.contains(q)))
    }
}

pub enum Msg {
    Runners(Option<NodeChange>),
    /// An event of the runner's subscription the number names: one replaced still delivers what it had sent.
    Event(String, u64, Option<StreamEvent>),
    /// A subscription that was refused is asked for again.
    Follow,
    Sample,
    Sampled(Vec<NodeStats>),
    /// Newest first, at most `Filter::limit`.
    Recent(Filter, ReplyTo<Vec<Arc<LogRecord>>>),
    Series(String, ReplyTo<Vec<Point>>),
}

/// A runner's counters of one worker at the last sample.
struct Seen {
    runs: u64,
    errors: u64,
    cpu: Histogram,
    wall: Histogram,
}

pub struct Observer {
    cluster: Cluster,
    runners: Nodes,
    streams: StreamNode,
    cfg: ObserverConfig,
    /// Each runner's subscription, and its number.
    following: BTreeMap<String, (u64, JobHandle)>,
    followed: u64,
    /// The last record handled of each runner's stream: where a subscription that ended goes on from.
    cursors: BTreeMap<String, Cursor>,
    recent: VecDeque<(Arc<LogRecord>, usize)>,
    recent_bytes: usize,
    tail: broadcast::Sender<Arc<LogRecord>>,
    seen: BTreeMap<(String, String), Seen>,
    sampled: bool,
    sampling: bool,
    retrying: bool,
    series: BTreeMap<String, VecDeque<Point>>,
}

const RETRY: Duration = Duration::from_secs(1);

impl Observer {
    /// The actor, and where a tail subscribes.
    pub fn new(
        cluster: &Cluster,
        streams: StreamNode,
        cfg: ObserverConfig,
    ) -> (Self, broadcast::Sender<Arc<LogRecord>>) {
        let (tail, _) = broadcast::channel(cfg.tail.max(1));
        let observer = Self {
            cluster: cluster.clone(),
            runners: cluster.nodes(&node::runners()),
            streams,
            cfg,
            following: BTreeMap::new(),
            followed: 0,
            cursors: BTreeMap::new(),
            recent: VecDeque::new(),
            recent_bytes: 0,
            tail: tail.clone(),
            seen: BTreeMap::new(),
            sampled: false,
            sampling: false,
            retrying: false,
            series: BTreeMap::new(),
        };
        (observer, tail)
    }

    /// Follows every runner in the view, and no other: level-triggered, like the gateway's table.
    fn refresh(&mut self, cx: &mut Context<'_, Self>) {
        let runners = self.runners.ids();
        let gone: Vec<String> =
            self.following.keys().filter(|node| runners.iter().all(|r| **r != ***node)).cloned().collect();
        for node in gone {
            if let Some((_, job)) = self.following.remove(&node) {
                job.cancel();
            }
            // It may come back with a stream of another generation, or with none of the old records.
            self.cursors.remove(&node);
        }
        for node in runners {
            if !self.following.contains_key(&*node) {
                self.follow(node.to_string(), cx);
            }
        }
    }

    fn follow(&mut self, node: String, cx: &mut Context<'_, Self>) {
        let start = self.cursors.get(&node).map_or(StartPosition::First, |cursor| StartPosition::After(*cursor));
        let (name, number) = (node.clone(), self.followed);
        self.followed += 1;
        let attached = match self.streams.subscribe(&logs(&node), SubscribeOptions { start, ..Default::default() }) {
            Ok(subscription) => cx.attach(subscription, move |event| Msg::Event(name.clone(), number, event)).ok(),
            // The runner's ring has passed the position, or it is of a stream the runner had before it restarted.
            Err(StreamError::Recovery(_)) => {
                self.cursors.remove(&node);
                None
            }
            Err(_) => None,
        };
        match attached {
            Some(job) => drop(self.following.insert(node, (number, job)).map(|(_, old)| old.cancel())),
            None if !self.retrying => self.retrying = cx.schedule_after(RETRY, Msg::Follow).is_ok(),
            None => {}
        }
    }

    fn keep(&mut self, record: LogRecord) {
        let size = size_of::<LogRecord>()
            + record.url.len()
            + record.worker.len()
            + record.logs.iter().map(|line| line.len() + size_of::<String>()).sum::<usize>();
        let record = Arc::new(record);
        // Nobody tailing is no error.
        drop(self.tail.send(record.clone()));
        self.recent.push_back((record, size));
        self.recent_bytes += size;
        while self.recent.len() > self.cfg.recent || self.recent_bytes > self.cfg.recent_bytes {
            match self.recent.pop_front() {
                Some((_, size)) => self.recent_bytes -= size,
                None => break,
            }
        }
    }

    /// One point per worker: what every runner gained since the sample before.
    fn sampled(&mut self, stats: Vec<NodeStats>) {
        let at_ms = axon::trace::unix_micros() / 1000;
        let mut seen = BTreeMap::new();
        let mut gained: BTreeMap<&str, (u64, u64, Histogram, Histogram)> = BTreeMap::new();
        for runner in &stats {
            for (name, worker) in &runner.workers {
                let runs: u64 = worker.outcomes.iter().sum();
                let now = Seen {
                    runs,
                    errors: runs - worker.outcomes[0],
                    cpu: Histogram::from(&worker.cpu),
                    wall: Histogram::from(&worker.wall),
                };
                let key = (runner.node.clone(), name.clone());
                let since = self.seen.get(&key).and_then(|before| {
                    let (runs, errors) = (now.runs.checked_sub(before.runs)?, now.errors.checked_sub(before.errors)?);
                    Some((runs, errors, now.cpu.since(&before.cpu)?, now.wall.since(&before.wall)?))
                });
                // A runner that started again counts from zero: what it has is what it gained.
                let (runs, errors, cpu, wall) =
                    since.unwrap_or_else(|| (now.runs, now.errors, now.cpu.clone(), now.wall.clone()));
                let total = gained.entry(name).or_default();
                (total.0, total.1) = (total.0 + runs, total.1 + errors);
                total.2.merge(&cpu);
                total.3.merge(&wall);
                seen.insert(key, now);
            }
        }
        // The first sample only says where the counters stand: what a runner did before is not this moment's.
        if std::mem::replace(&mut self.sampled, true) {
            self.series.retain(|name, _| gained.contains_key(name.as_str()));
            for (name, (requests, errors, cpu, wall)) in gained {
                let ((cpu_p50_us, cpu_p99_us), (wall_p50_us, wall_p99_us)) = (p50_p99_us(&cpu), p50_p99_us(&wall));
                let point = Point { at_ms, requests, errors, cpu_p50_us, cpu_p99_us, wall_p50_us, wall_p99_us };
                let series = self.series.entry(name.to_owned()).or_default();
                series.push_back(point);
                while series.len() > self.cfg.points {
                    series.pop_front();
                }
            }
        }
        self.seen = seen;
    }
}

impl Actor for Observer {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        cx.attach(self.runners.changes(), Msg::Runners).map_err(|e| format!("{e:?}"))?;
        cx.schedule_every(self.cfg.sample, || Msg::Sample).map_err(|e| format!("{:?}", e.reason))?;
        self.refresh(cx);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            // The cluster shut down.
            Msg::Runners(None) => {}
            Msg::Runners(Some(_)) => self.refresh(cx),
            Msg::Follow => {
                self.retrying = false;
                self.refresh(cx);
            }
            Msg::Event(node, number, _) if self.following.get(&node).is_none_or(|(n, _)| *n != number) => {}
            Msg::Event(node, _, Some(StreamEvent::Batch(batch) | StreamEvent::Replacement { batch })) => {
                self.cursors.insert(node, batch.cursor());
                batch
                    .records()
                    .iter()
                    .filter_map(|record| LogRecord::parse(record))
                    .for_each(|record| self.keep(record));
            }
            Msg::Event(node, _, Some(event)) if names_nothing(&event) => drop(self.cursors.remove(&node)),
            // Ready, CaughtUp, OriginLost: the subscription goes on. Any other end or reset: followed again from
            // the cursor, so nothing kept is read twice.
            Msg::Event(_, _, Some(_)) => {}
            // The subscription ended. Followed again here and not by a message, which a full mailbox could refuse.
            Msg::Event(node, _, None) => {
                self.following.remove(&node);
                self.refresh(cx);
            }
            Msg::Sample if self.sampling => {}
            Msg::Sample => {
                let cluster = self.cluster.clone();
                self.sampling = cx.submit_future(async move { Msg::Sampled(node::stats(&cluster).await.0) }).is_ok();
            }
            Msg::Sampled(stats) => {
                self.sampling = false;
                self.sampled(stats);
            }
            Msg::Recent(filter, reply) => {
                let mut found: Vec<Arc<LogRecord>> =
                    self.recent.iter().filter(|(record, _)| filter.admits(record)).map(|(r, _)| r.clone()).collect();
                // Each runner's records arrive in its order; over the runners the time orders them.
                found.sort_by_key(|record| std::cmp::Reverse(record.at_ms));
                found.truncate(filter.limit.unwrap_or(100).min(1000));
                drop(reply.reply(found));
            }
            Msg::Series(name, reply) => {
                drop(
                    reply.reply(
                        self.series.get(&name).map(|series| series.iter().cloned().collect()).unwrap_or_default(),
                    ),
                );
            }
        }
        Ok(())
    }
}

/// Whether the event says the position reached is of no use: the runner started again, or its ring has passed it.
/// After any other end (a slow reader, a parent lost) the records after the cursor are still there to read.
fn names_nothing(event: &StreamEvent) -> bool {
    use ResetReason::{GenerationChanged, HistoryUnavailable, InvalidRecovery};
    matches!(
        event,
        StreamEvent::ResetRequired(GenerationChanged | HistoryUnavailable | InvalidRecovery)
            | StreamEvent::End(EndReason::StreamClosed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_is_kept_unless_the_stream_it_names_is_gone() {
        for kept in [
            StreamEvent::End(EndReason::SlowConsumer),
            StreamEvent::End(EndReason::UpstreamLost),
            StreamEvent::ResetRequired(ResetReason::SlowConsumer),
            StreamEvent::ResetRequired(ResetReason::Gap),
        ] {
            assert!(!names_nothing(&kept), "{kept:?}");
        }
        for gone in [
            StreamEvent::End(EndReason::StreamClosed),
            StreamEvent::ResetRequired(ResetReason::GenerationChanged),
            StreamEvent::ResetRequired(ResetReason::HistoryUnavailable),
        ] {
            assert!(names_nothing(&gone), "{gone:?}");
        }
    }

    #[test]
    fn a_filter_takes_a_status_a_class_an_outcome_a_node_and_text() {
        let record = LogRecord {
            at_ms: 1,
            worker: "hello".into(),
            version: 1,
            node: "r1".into(),
            trace: Some("00ab".into()),
            method: "GET".into(),
            url: "http://gw/some/path".into(),
            status: 503,
            outcome: "cpu_limit".into(),
            cpu_us: 1,
            wall_us: 2,
            logs: vec!["Log: looked up 42".into()],
            truncated: false,
        };
        let with = |set: fn(&mut Filter)| {
            let mut filter = Filter::default();
            set(&mut filter);
            filter.admits(&record)
        };
        assert!(Filter::default().admits(&record));
        assert!(with(|f| f.status = Some("503".into())) && with(|f| f.status = Some("5xx".into())));
        assert!(with(|f| f.status = Some("cpu_limit".into())) && !with(|f| f.status = Some("4xx".into())));
        assert!(!with(|f| f.status = Some("50".into())) && !with(|f| f.status = Some("ok".into())));
        assert!(with(|f| f.q = Some("some/path".into())) && with(|f| f.q = Some("up 42".into())));
        assert!(!with(|f| f.q = Some("absent".into())));
        assert!(with(|f| f.node = Some("r1".into())) && !with(|f| f.node = Some("r2".into())));
        assert!(with(|f| f.worker = Some("hello".into())) && !with(|f| f.worker = Some("hell".into())));
        assert!(with(|f| f.trace = Some("00ab".into())) && !with(|f| f.trace = Some("00ac".into())));
    }
}
