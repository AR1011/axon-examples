//! What a history replica derives from the journal it holds: each run's summary, the indexes runs are listed by,
//! and the timers its waker fires. A run's summary is the one its newest segment's `Own` carries, with that
//! segment's later records applied in order, so a replica needs no other segment of the run, and once it holds the
//! newest segment whole its summary is the journal's (I14).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included, Unbounded};

use axon::files::Address;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::entry::Entry;
use crate::workflow::not_in_build;

/// Named in lower case in JSON; on disk, by its index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    #[default]
    Running,
    Paused,
    Parked,
    Completed,
    Failed,
    Cancelled,
    Terminated,
    Deleted,
}

impl Status {
    pub fn is_finished(self) -> bool {
        self >= Status::Completed
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub workflow: String,
    pub version: u32,
    pub status: Status,
    pub input_hash: u64,
    pub parent: Option<String>,
    pub tags: Vec<(String, String)>,
    /// Durable timers not yet fired, by step: a sleep's end, an activity's next attempt; µs of the wall clock.
    pub timers: BTreeMap<u32, u64>,
    /// The run stopped itself, waiting only for a call or a timer: its last entry is `Idle`.
    pub idle: bool,
    /// Parked on a version this build did not have: a build that has it resumes the run.
    pub parked_version: bool,
    /// A child run that has not told its parent how it ended: it is opened again until it has.
    pub owes_parent: bool,
    /// When it started, by the starting node's wall clock in µs.
    pub started_us: u64,
}

impl Summary {
    pub fn of<'a>(journal: impl IntoIterator<Item = &'a Bytes>) -> Summary {
        let mut summary = Summary::default();
        for entry in journal.into_iter().filter_map(|body| Entry::decode(body).ok()) {
            summary.apply(&entry);
        }
        summary
    }

    pub fn apply(&mut self, entry: &Entry) {
        // An owner's `Own` changes nothing: a run opened and left again is as idle as before.
        if !matches!(entry, Entry::Own(_)) {
            self.idle = matches!(entry, Entry::Idle);
        }
        match entry {
            Entry::Started { workflow, version, input_hash, tags, parent, at_us, .. } => {
                (self.workflow, self.version, self.input_hash) = (workflow.clone(), *version, *input_hash);
                self.started_us = *at_us;
                (self.tags, self.parent) = (tags.clone(), parent.clone());
                self.owes_parent = parent.is_some();
            }
            Entry::ParentNotified => self.owes_parent = false,
            Entry::TimerStarted { step, wake_at_us, .. }
            | Entry::ActivityFailed { step, retry_at_us: Some(wake_at_us), .. } => {
                self.timers.insert(*step, *wake_at_us);
            }
            Entry::TimerFired { step, .. }
            | Entry::ActivityFailed { step, .. }
            | Entry::ActivityDone { step, .. }
            | Entry::SignalConsumed { step, .. }
            | Entry::ChildDone { step, .. }
            | Entry::Picked { step } => {
                self.timers.remove(step);
            }
            Entry::Paused => self.status = Status::Paused,
            Entry::Parked { reason } => {
                self.status = Status::Parked;
                self.parked_version = *reason == not_in_build(&self.workflow, self.version);
            }
            Entry::Resumed | Entry::Redriven { .. } => {
                self.status = Status::Running;
                self.parked_version = false;
            }
            Entry::Completed { .. } => self.end(Status::Completed),
            Entry::Failed { .. } => self.end(Status::Failed),
            Entry::Cancelled => self.end(Status::Cancelled),
            Entry::Terminated { .. } => self.end(Status::Terminated),
            Entry::Deleted => self.end(Status::Deleted),
            _ => {}
        }
    }

    fn end(&mut self, status: Status) {
        self.status = status;
        self.timers.clear();
    }
}

/// How long a finished run stays after its last record here, in µs: a failed one longer, so that it can be
/// redriven, a deleted one not at all, and a workflow's own keep, failed or not, over both. The default keeps every
/// run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retention {
    pub keep_us: u64,
    pub failed_us: u64,
    pub workflows: BTreeMap<String, u64>,
}

impl Default for Retention {
    fn default() -> Self {
        Self { keep_us: u64::MAX, failed_us: u64::MAX, workflows: BTreeMap::new() }
    }
}

impl Retention {
    pub fn of(&self, workflow: &str, status: Status) -> u64 {
        if status == Status::Deleted {
            return 0;
        }
        match self.workflows.get(workflow) {
            Some(keep) => *keep,
            None if status == Status::Failed => self.failed_us,
            None => self.keep_us,
        }
    }
}

/// A run as one replica indexes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Indexed {
    pub summary: Summary,
    /// The segment the summary is of, and its records applied.
    pub segment: u64,
    pub len: u64,
    /// When this replica last took a record of the run, by its wall clock in µs.
    pub updated_us: u64,
}

/// A list stops after scanning this many runs that the filter left out: narrow the filter.
pub const SCAN: usize = 10_000;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub workflow: Option<String>,
    pub status: Option<Status>,
    /// Runs whose id starts with it, in id order: a parent's children are `<parent>/`.
    pub prefix: Option<String>,
    /// Scanned for, inside the index the other filters pick.
    pub tag: Option<(String, String)>,
    pub since_us: Option<u64>,
    /// Where the last page ended: its last run's update and id.
    pub after: Option<(u64, Bytes)>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    pub runs: Vec<(Bytes, Indexed)>,
    pub next: Option<(u64, Bytes)>,
    /// The scan stopped at [`SCAN`] rows.
    pub cut: bool,
}

/// What a replica counts of the runs it holds, and of what it saw since it started.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Runs held, by workflow, version and status.
    pub runs: Vec<(String, u32, Status, u64)>,
    /// Running runs that stopped themselves: they sleep, or wait for a signal or a child.
    pub idle: u64,
    /// Running runs with no record for [`STUCK`] that did not stop themselves, up to [`SCAN`].
    pub stuck: u64,
    /// Running runs with no record for [`OLD`], up to [`SCAN`].
    pub old: u64,
    /// Since this replica started: runs it saw start, by workflow, and end, by workflow and status.
    pub started: Vec<(String, u64)>,
    pub finished: Vec<(String, Status, u64)>,
}

/// A running run that did not stop itself and took no record for this long is stuck (I12 counts lease, reconciler and
/// 10 s): `wf_stuck_runs`.
pub const STUCK: u64 = 60_000_000;
/// The console's "waiting over 7 days".
pub const OLD: u64 = 7 * 86_400_000_000;

/// Every run a replica holds, by id, by status, by workflow, by both and by update, newest first, and its timers.
#[derive(Default)]
pub struct Index {
    runs: BTreeMap<Bytes, Indexed>,
    by_updated: BTreeSet<(u64, Bytes)>,
    by_status: BTreeSet<(Status, u64, Bytes)>,
    by_workflow: BTreeSet<(String, u64, Bytes)>,
    by_both: BTreeSet<(String, Status, u64, Bytes)>,
    timers: BTreeSet<(u64, Bytes)>,
    /// By update: the runs the reconciler looks at, those that may need a wake no timer gives.
    loose: BTreeSet<(u64, Bytes)>,
    retention: Retention,
    /// Finished runs, by when their retention ends.
    by_expiry: BTreeSet<(u64, Bytes)>,
    by_kind: BTreeMap<(String, u32, Status), u64>,
    idle: u64,
    started: BTreeMap<String, u64>,
    finished: BTreeMap<(String, Status), u64>,
}

/// A run no timer or call may wake: one that runs and did not stop itself (its owner may be gone), one parked on a
/// version that a build may now have, or a child that ended and has not told its parent.
fn is_loose(s: &Summary) -> bool {
    (s.status == Status::Running && !s.idle)
        || (s.status == Status::Parked && s.parked_version)
        || (s.status.is_finished() && s.owes_parent)
}

impl Index {
    pub fn get(&self, run: &[u8]) -> Option<&Indexed> {
        self.runs.get(run)
    }

    pub fn retention(&self) -> &Retention {
        &self.retention
    }

    pub fn set_retention(&mut self, retention: Retention) {
        self.retention = retention;
        let expiries = self.runs.iter().filter_map(|(run, ix)| Some((self.expiry(ix)?, run.clone())));
        self.by_expiry = expiries.collect();
    }

    /// When a finished run's retention ends, in µs of the wall clock.
    fn expiry(&self, ix: &Indexed) -> Option<u64> {
        let s = &ix.summary;
        s.status.is_finished().then(|| ix.updated_us.saturating_add(self.retention.of(&s.workflow, s.status)))
    }

    pub fn len(&self) -> usize {
        self.runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Indexed)> {
        self.runs.iter()
    }

    /// The runs from `from` on, in id order.
    pub fn from(&self, from: Option<&Bytes>) -> impl Iterator<Item = (&Bytes, &Indexed)> {
        self.runs.range::<[u8], _>((from.map_or(Unbounded, |f| Included(&f[..])), Unbounded))
    }

    /// A record the replica took, at `at` of the run: the next of the run's newest segment, or the first of a newer.
    pub fn record(&mut self, run: &Bytes, at: Address, body: &Bytes, now_us: u64) {
        let (was, was_us) = self.runs.get(run).map(|ix| (ix.summary.status, ix.updated_us)).unzip();
        let next = match self.runs.get(run) {
            Some(ix) if ix.segment == at.epoch && ix.len + 1 == at.seq => true,
            ix if at.seq == 1 && ix.is_none_or(|ix| at.epoch > ix.segment) => false,
            _ => return,
        };
        // Taken out rather than cloned: this runs for each record a replica takes.
        let mut summary = self.remove(run).filter(|_| next).map(|ix| ix.summary).unwrap_or_default();
        let mut updated_us = now_us;
        match Entry::decode(body) {
            Ok(Entry::Own(own)) => {
                // A call that opens an ended run makes it no newer: its end stays its time, for a list and retention.
                if own.summary.status.is_finished() && was.is_some_and(Status::is_finished) {
                    updated_us = was_us.unwrap_or(now_us);
                }
                summary = own.summary;
            }
            Ok(entry) => {
                summary.apply(&entry);
                if let Entry::Started { workflow, .. } = entry {
                    *self.started.entry(workflow).or_default() += 1;
                }
            }
            Err(_) => {}
        }
        let ended = summary.status.is_finished() && summary.status != Status::Deleted;
        if ended && !was.is_some_and(Status::is_finished) {
            *self.finished.entry((summary.workflow.clone(), summary.status)).or_default() += 1;
        }
        self.put(run.clone(), Indexed { summary, segment: at.epoch, len: at.seq, updated_us });
    }

    pub fn put(&mut self, run: Bytes, ix: Indexed) {
        self.remove(&run);
        let (s, u) = (&ix.summary, ix.updated_us);
        self.by_updated.insert((u, run.clone()));
        self.by_status.insert((s.status, u, run.clone()));
        self.by_workflow.insert((s.workflow.clone(), u, run.clone()));
        self.by_both.insert((s.workflow.clone(), s.status, u, run.clone()));
        // A parked or paused run waits for an operator, not its timers: the waker leaves it alone.
        if s.status == Status::Running {
            self.timers.extend(s.timers.values().map(|at| (*at, run.clone())));
        }
        if is_loose(s) {
            self.loose.insert((u, run.clone()));
        }
        if let Some(at) = self.expiry(&ix) {
            self.by_expiry.insert((at, run.clone()));
        }
        *self.by_kind.entry((s.workflow.clone(), s.version, s.status)).or_default() += 1;
        self.idle += u64::from(s.status == Status::Running && s.idle);
        self.runs.insert(run, ix);
    }

    pub fn remove(&mut self, run: &Bytes) -> Option<Indexed> {
        let ix = self.runs.remove(run)?;
        let (s, u) = (&ix.summary, ix.updated_us);
        self.by_updated.remove(&(u, run.clone()));
        self.by_status.remove(&(s.status, u, run.clone()));
        self.by_workflow.remove(&(s.workflow.clone(), u, run.clone()));
        self.by_both.remove(&(s.workflow.clone(), s.status, u, run.clone()));
        for at in s.timers.values() {
            self.timers.remove(&(*at, run.clone()));
        }
        self.loose.remove(&(u, run.clone()));
        if let Some(at) = self.expiry(&ix) {
            self.by_expiry.remove(&(at, run.clone()));
        }
        let kind = (s.workflow.clone(), s.version, s.status);
        if let Some(n) = self.by_kind.get_mut(&kind) {
            *n -= 1;
            if *n == 0 {
                self.by_kind.remove(&kind);
            }
        }
        self.idle -= u64::from(s.status == Status::Running && s.idle);
        Some(ix)
    }

    /// Timers due at `now_us` after `after`, the earliest first: the time and the run.
    pub fn due(&self, after: Option<&(u64, Bytes)>, now_us: u64) -> impl Iterator<Item = &(u64, Bytes)> {
        self.timers.range((after.map_or(Unbounded, Excluded), Unbounded)).take_while(move |(at, _)| *at <= now_us)
    }

    /// Runs that may need a wake and took no record here since `before_us`, after `after`, the least recent first.
    pub fn stale(&self, after: Option<&(u64, Bytes)>, before_us: u64) -> impl Iterator<Item = &(u64, Bytes)> {
        self.loose.range((after.map_or(Unbounded, Excluded), Unbounded)).take_while(move |(at, _)| *at < before_us)
    }

    /// Finished runs whose retention ended before `now_us`, the first to end first.
    pub fn expired(&self, now_us: u64) -> impl Iterator<Item = &Bytes> {
        self.by_expiry.range(..(now_us, Bytes::new())).map(|(_, run)| run)
    }

    pub fn counts(&self, now_us: u64) -> Counts {
        let before = |age: u64| (now_us.saturating_sub(age), Bytes::new());
        let stuck =
            self.loose.range(..before(STUCK)).filter(|(_, run)| self.runs[run].summary.status == Status::Running);
        let running = (Status::Running, 0, Bytes::new());
        let (to, r) = before(OLD);
        Counts {
            // A run whose `Started` has not come here yet has no workflow: not one to count.
            runs: self
                .by_kind
                .iter()
                .filter(|((w, ..), _)| !w.is_empty())
                .map(|((w, v, s), n)| (w.clone(), *v, *s, *n))
                .collect(),
            idle: self.idle,
            stuck: stuck.take(SCAN).count() as u64,
            old: self.by_status.range(running..(Status::Running, to, r)).take(SCAN).count() as u64,
            started: self.started.iter().map(|(w, n)| (w.clone(), *n)).collect(),
            finished: self.finished.iter().map(|((w, s), n)| (w.clone(), *s, *n)).collect(),
        }
    }

    /// One page of the runs `q` asks for, newest first, or in id order for a prefix.
    pub fn query(&self, q: &Query) -> Page {
        let (u, r) = q.after.clone().unwrap_or((u64::MAX, Bytes::new()));
        let wf = q.workflow.clone().unwrap_or_default();
        let rows: Box<dyn Iterator<Item = (u64, &Bytes)>> = match (&q.prefix, &q.workflow, q.status) {
            (Some(prefix), ..) => {
                let from = if q.after.is_some() { Excluded(&r[..]) } else { Included(prefix.as_bytes()) };
                let runs = self.runs.range::<[u8], _>((from, Unbounded));
                Box::new(
                    runs.take_while(|(run, _)| run.starts_with(prefix.as_bytes())).map(|(r, ix)| (ix.updated_us, r)),
                )
            }
            (None, Some(_), Some(st)) => Box::new(
                self.by_both.range((wf.clone(), st, 0, Bytes::new())..(wf, st, u, r)).rev().map(|(_, _, u, r)| (*u, r)),
            ),
            (None, Some(_), None) => Box::new(
                self.by_workflow.range((wf.clone(), 0, Bytes::new())..(wf, u, r)).rev().map(|(_, u, r)| (*u, r)),
            ),
            (None, None, Some(st)) => {
                Box::new(self.by_status.range((st, 0, Bytes::new())..(st, u, r)).rev().map(|(_, u, r)| (*u, r)))
            }
            (None, None, None) => Box::new(self.by_updated.range(..(u, r)).rev().map(|(u, r)| (*u, r))),
        };
        let mut page = Page::default();
        for (scanned, (updated, run)) in rows.enumerate() {
            let ix = &self.runs[run];
            let s = &ix.summary;
            if q.since_us.is_some_and(|since| updated < since) {
                if q.prefix.is_some() {
                    continue;
                }
                break;
            }
            let fits = q.workflow.as_ref().is_none_or(|w| *w == s.workflow)
                && q.status.is_none_or(|st| st == s.status)
                && q.tag.as_ref().is_none_or(|t| s.tags.contains(t));
            if fits && page.runs.len() == q.limit {
                page.next = page.runs.last().map(|(run, ix)| (ix.updated_us, run.clone()));
                break;
            }
            if fits {
                page.runs.push((run.clone(), ix.clone()));
            } else if scanned + 1 - page.runs.len() >= SCAN {
                page.cut = true;
                break;
            }
        }
        page
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(workflow: &str, tags: &[(&str, &str)]) -> Entry {
        let tags = tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Entry::Started {
            workflow: workflow.into(),
            version: 1,
            input: Bytes::new(),
            input_hash: 9,
            tags,
            parent: None,
            run_timeout_ms: None,
            at_us: 0,
        }
    }

    fn own(summary: Summary) -> Bytes {
        Entry::Own(super::super::Own { summary, ..Default::default() }).encode()
    }

    fn at(epoch: u64, seq: u64) -> Address {
        Address { epoch, seq }
    }

    #[test]
    fn a_summary_follows_its_newest_segment() {
        let mut ix = Index::default();
        let run = Bytes::from_static(b"r1");
        ix.record(&run, at(256, 1), &own(Summary::default()), 1);
        ix.record(&run, at(256, 2), &started("checkout", &[]).encode(), 2);
        ix.record(&run, at(256, 3), &Entry::TimerStarted { step: 1, wake_at_us: 50, at_us: 0 }.encode(), 3);
        assert_eq!(ix.get(b"r1").unwrap().summary.timers.values().min(), Some(&50));
        assert_eq!(ix.due(None, 49).count(), 0);
        assert_eq!(ix.due(None, 50).map(|(_, r)| r.clone()).collect::<Vec<_>>(), vec![run.clone()]);
        assert_eq!(ix.due(None, u64::MAX).count(), 1);
        assert_eq!(ix.due(Some(&(50, run.clone())), u64::MAX).count(), 0, "after the last");
        assert_eq!(ix.stale(None, 4).count(), 1, "a run that is not idle may need a wake");
        ix.record(&run, at(256, 4), &Entry::Idle.encode(), 3);
        assert!(ix.get(b"r1").unwrap().summary.idle);
        assert_eq!(ix.stale(None, u64::MAX).count(), 0, "an idle run waits for a call or its timer");
        ix.record(&run, at(256, 5), &Entry::Parked { reason: "r".into() }.encode(), 3);
        assert_eq!(ix.due(None, u64::MAX).count(), 0, "a parked run's timers wait for its resume");
        assert_eq!(ix.stale(None, u64::MAX).count(), 0, "parked on a mismatch: an operator's to resume");
        ix.record(&run, at(256, 6), &Entry::Resumed.encode(), 3);
        assert_eq!(ix.due(None, u64::MAX).count(), 1);
        ix.record(&run, at(256, 7), &Entry::Parked { reason: not_in_build("checkout", 1) }.encode(), 3);
        assert!(ix.get(b"r1").unwrap().summary.parked_version);
        assert_eq!(ix.stale(None, u64::MAX).count(), 1, "parked on a version: a build may have it");
        ix.record(&run, at(256, 8), &Entry::Resumed.encode(), 3);
        // A newer owner's Own carries the summary of what it counted: the timer was not.
        let counted = Summary { workflow: "checkout".into(), version: 1, input_hash: 9, ..Summary::default() };
        ix.record(&run, at(512, 1), &own(counted.clone()), 4);
        // A record of the older segment, as repair or a late append gives it, changes nothing.
        ix.record(&run, at(256, 9), &Entry::Completed { output: Bytes::new() }.encode(), 5);
        assert_eq!(ix.get(b"r1").unwrap(), &Indexed { summary: counted, segment: 512, len: 1, updated_us: 4 });
        assert_eq!(ix.due(None, u64::MAX - 1).count(), 0);
        ix.record(&run, at(512, 2), &Entry::Completed { output: Bytes::new() }.encode(), 6);
        assert_eq!(ix.stale(None, u64::MAX).count(), 0, "a finished run needs no wake");
        // A get that opens the ended run: its owner's Own leaves the run as old as its end.
        let ended = ix.get(b"r1").unwrap().summary.clone();
        ix.record(&run, at(768, 1), &own(ended), 9);
        assert_eq!(ix.get(b"r1").unwrap().updated_us, 6);
        // Counted once each, though a newer segment came between.
        let counts = ix.counts(10);
        assert_eq!(counts.started, [("checkout".to_owned(), 1)]);
        assert_eq!(counts.finished, [("checkout".to_owned(), Status::Completed, 1)]);
        assert_eq!(counts.runs, [("checkout".to_owned(), 1, Status::Completed, 1)]);
        // Updated at 6: at 10, older than 3 and not than 4, and a workflow's own keep over the node's.
        let keep = |keep_us, workflows: &[(&str, u64)]| Retention {
            keep_us,
            failed_us: 0,
            workflows: workflows.iter().map(|(w, k)| (w.to_string(), *k)).collect(),
        };
        ix.set_retention(keep(3, &[]));
        assert_eq!(ix.expired(10).collect::<Vec<_>>(), vec![&run]);
        ix.set_retention(keep(4, &[]));
        assert_eq!(ix.expired(10).count(), 0);
        ix.set_retention(keep(3, &[("checkout", 4)]));
        assert_eq!(ix.expired(10).count(), 0);
        ix.set_retention(keep(4, &[("checkout", 3)]));
        assert_eq!(ix.expired(10).count(), 1);
        ix.record(&run, at(768, 2), &Entry::Deleted.encode(), 7);
        assert_eq!(ix.expired(8).count(), 1, "deleted: kept no longer");
        let b = super::super::entry::encode(ix.get(b"r1").unwrap());
        assert_eq!(super::super::entry::decode::<Indexed>(&b).as_ref(), Ok(ix.get(b"r1").unwrap()));
    }

    #[test]
    fn a_run_whose_start_is_not_here_yet_is_not_counted() {
        let mut ix = Index::default();
        let run = Bytes::from_static(b"r1");
        // The owner's Own comes first, without a workflow: the console showed a workflow without a name.
        ix.record(&run, at(256, 1), &own(Summary::default()), 1);
        assert_eq!(ix.counts(2).runs, []);
        ix.record(&run, at(256, 2), &started("checkout", &[]).encode(), 2);
        assert_eq!(ix.counts(3).runs, [("checkout".to_owned(), 1, Status::Running, 1)]);
    }

    #[test]
    fn each_filter_has_its_index_and_a_scan_stops() {
        let mut ix = Index::default();
        for i in 0..30_000u64 {
            let mut summary = Summary::of(&[started(["a", "b"][i as usize % 2], &[("t", &i.to_string())]).encode()]);
            if i % 3 == 0 {
                summary.apply(&Entry::Failed { error: String::new(), timeout: false });
            }
            ix.put(Bytes::from(format!("run-{i:05}")), Indexed { summary, segment: 256, len: 2, updated_us: i });
        }
        let page = |q: Query| ix.query(&Query { limit: 3, ..q });
        let ids = |p: &Page| p.runs.iter().map(|(r, _)| String::from_utf8_lossy(r).into_owned()).collect::<Vec<_>>();
        let all = page(Query::default());
        assert_eq!(ids(&all), ["run-29999", "run-29998", "run-29997"]);
        assert_eq!(ids(&page(Query { after: all.next, ..Query::default() })), ["run-29996", "run-29995", "run-29994"]);
        let failed_a = page(Query { workflow: Some("a".into()), status: Some(Status::Failed), ..Query::default() });
        assert_eq!(ids(&failed_a), ["run-29994", "run-29988", "run-29982"]);
        assert_eq!(ids(&page(Query { workflow: Some("b".into()), ..Query::default() }))[0], "run-29999");
        assert_eq!(ids(&page(Query { status: Some(Status::Running), ..Query::default() }))[0], "run-29999");
        let prefix = page(Query { prefix: Some("run-1".into()), ..Query::default() });
        assert_eq!(ids(&prefix), ["run-10000", "run-10001", "run-10002"]);
        assert_eq!(
            ids(&page(Query { prefix: Some("run-1".into()), after: prefix.next, ..Query::default() }))[0],
            "run-10003"
        );
        let tag = |v: &str| Query { tag: Some(("t".into(), v.into())), ..Query::default() };
        assert_eq!(ids(&page(tag("25000"))), ["run-25000"]);
        let far = page(tag("5"));
        assert!(far.cut && far.runs.is_empty(), "{far:?}");
        assert_eq!(page(Query { since_us: Some(29_999), ..Query::default() }).runs.len(), 1);
        // Two in three run; the counts of old and stuck runs stop at the scan's bound.
        let counts = ix.counts(OLD + 30_000);
        assert_eq!((counts.old, counts.stuck, counts.idle), (SCAN as u64, SCAN as u64, 0));
        assert_eq!((ix.counts(OLD + 3_000).old, ix.counts(STUCK + 3_000).stuck), (2_000, 2_000));
        let running: u64 = counts.runs.iter().filter(|r| r.2 == Status::Running).map(|r| r.3).sum();
        assert_eq!(running, 20_000);
    }
}
