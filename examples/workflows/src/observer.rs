//! The overview chart's numbers on an api node: the [`Observer`] asks a history replica for its index's counts every
//! `sample`, and turns the counts of started and ended runs into one point a sample, for an hour.
//! It asks the replicas in the order every node lists them, so that its counters come from one replica while it
//! answers. All of it is in memory: a restart starts the chart again ("collecting since").

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, Context, ReplyTo};
use axon_cluster::WeakCluster;
use serde::Serialize;

use crate::journal::Status;
use crate::journal::index::Counts;
use crate::journal::wire::{IndexAnswer, IndexCall, ask_index};

/// How long a replica has to answer a sample.
const ASK_WITHIN: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct ObserverConfig {
    pub sample: Duration,
    /// Points kept: an hour of 10 s samples.
    pub points: usize,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        Self { sample: Duration::from_secs(10), points: 360 }
    }
}

/// Runs started and ended between two samples, per minute.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Point {
    pub at_ms: u64,
    pub started: f64,
    pub completed: f64,
    pub failed: f64,
}

/// The newest counts, the replica that gave them and when.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Latest {
    at_ms: u64,
    replica: String,
    counts: Counts,
}

pub enum Msg {
    Sample,
    Sampled(u64, Option<(String, Counts)>),
    /// Points from a time on, in ms, the oldest first; and since when the observer collects.
    Points(u64, ReplyTo<(Vec<Point>, u64)>),
}

pub struct Observer {
    cluster: WeakCluster,
    replicas: Vec<Arc<str>>,
    sample: Duration,
    sampling: bool,
    chart: Chart,
}

/// The newest counts and the points made from them.
#[derive(Default)]
struct Chart {
    keep: usize,
    since_ms: u64,
    latest: Option<Latest>,
    points: VecDeque<Point>,
}

/// Started, completed and failed runs, as one replica counted them since it started.
fn totals(counts: &Counts) -> (u64, u64, u64) {
    let ended = |status| counts.finished.iter().filter(|f| f.1 == status).map(|f| f.2).sum::<u64>();
    (counts.started.iter().map(|s| s.1).sum(), ended(Status::Completed), ended(Status::Failed))
}

fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1_000
}

impl Observer {
    pub fn new(cluster: WeakCluster, replicas: Vec<Arc<str>>, cfg: ObserverConfig) -> Self {
        let chart = Chart { keep: cfg.points, since_ms: now_ms(), ..Chart::default() };
        Self { cluster, replicas, sample: cfg.sample, sampling: false, chart }
    }

    fn sample(&mut self, cx: &mut Context<'_, Self>) {
        let Some(cluster) = self.cluster.upgrade().filter(|_| !self.sampling) else { return };
        let replicas = self.replicas.clone();
        let job = cx.submit_future(async move {
            let counts = match ask_index(&cluster, &replicas, IndexCall::Counts, ASK_WITHIN).await {
                Ok((replica, IndexAnswer::Counts(counts))) => Some((replica.to_string(), counts)),
                _ => None,
            };
            Msg::Sampled(now_ms(), counts)
        });
        self.sampling = job.is_ok();
    }
}

impl Chart {
    fn sampled(&mut self, at_ms: u64, counts: Option<(String, Counts)>) {
        let Some((replica, counts)) = counts else { return };
        // A point needs two samples of one replica; one that restarted counts from zero again.
        if let Some(last) = self.latest.as_ref().filter(|l| l.replica == replica && at_ms > l.at_ms) {
            let (was, now) = (totals(&last.counts), totals(&counts));
            if now.0 >= was.0 && now.1 >= was.1 && now.2 >= was.2 {
                let per_minute = |a: u64, b: u64| (b - a) as f64 * 60_000.0 / (at_ms - last.at_ms) as f64;
                self.points.push_back(Point {
                    at_ms,
                    started: per_minute(was.0, now.0),
                    completed: per_minute(was.1, now.1),
                    failed: per_minute(was.2, now.2),
                });
                while self.points.len() > self.keep {
                    self.points.pop_front();
                }
            }
        }
        self.latest = Some(Latest { at_ms, replica, counts });
    }
}

impl Actor for Observer {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.sample(cx);
        cx.schedule_every(self.sample, || Msg::Sample).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Sample => self.sample(cx),
            Msg::Sampled(at_ms, counts) => {
                self.sampling = false;
                self.chart.sampled(at_ms, counts);
            }
            Msg::Points(from_ms, reply) => {
                let points = self.chart.points.iter().filter(|p| p.at_ms >= from_ms).cloned().collect();
                drop(reply.reply((points, self.chart.since_ms)));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(started: u64, completed: u64, failed: u64) -> Counts {
        Counts {
            started: vec![("a".into(), started)],
            finished: vec![("a".into(), Status::Completed, completed), ("a".into(), Status::Failed, failed)],
            ..Counts::default()
        }
    }

    #[test]
    fn points_are_per_minute_between_samples_of_one_replica() {
        let mut o = Chart { keep: 2, ..Chart::default() };
        o.sampled(10_000, Some(("h1".into(), counts(10, 5, 1))));
        assert!(o.points.is_empty(), "one sample is no point");
        o.sampled(20_000, Some(("h1".into(), counts(30, 10, 1))));
        assert_eq!(o.points[0], Point { at_ms: 20_000, started: 120.0, completed: 30.0, failed: 0.0 });
        // Another replica's counters are not this one's: the next point waits for a second sample of it.
        o.sampled(30_000, Some(("h2".into(), counts(1, 0, 0))));
        o.sampled(40_000, None);
        assert_eq!(o.latest.as_ref().map(|l| l.replica.as_str()), Some("h2"));
        o.sampled(50_000, Some(("h2".into(), counts(2, 1, 1))));
        o.sampled(60_000, Some(("h2".into(), counts(4, 1, 1))));
        assert_eq!(o.points.iter().map(|p| p.at_ms).collect::<Vec<_>>(), [50_000, 60_000], "two kept");
        assert_eq!(o.points[0].started, 3.0);
    }
}
