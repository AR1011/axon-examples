//! The checker that `wf check`, the journal simulation, the World stories and `fault.sh` end with. It reads every
//! history replica's directory, the acks clients got and the witness log the test activity keeps, rebuilds each
//! run's journal from the replicas as a new owner would, and lists every invariant it finds broken:
//!
//! - I2: an acked entry is missing from the journal, or in another place;
//! - I3: the journal forks: replicas disagree on a record, a chain link counts a record nothing holds, or a segment
//!   does not start with `Own`;
//! - I4: a done activity has no execution, or an attempt started after its `ActivityDone` was acked;
//! - I5: one step has two outcomes;
//! - I6: a timer fired before its `wake_at`, on the clock of the node that fired it;
//! - I7: an acked signal is not received once, a signal is taken twice, or signals of one name are taken out of the
//!   order they came in;
//! - I8: a run's journal starts twice, or a start was acked with input other than the journal's;
//! - I10: an entry follows a terminal entry, other than `Own`, `ParentNotified`, `Deleted`, and `Redriven` after
//!   `Failed`;
//! - I14: a replica's summary of a run is not its journal's, once the replicas are quiet and repaired;
//! - I16: a run goes on past its `run_timeout` firing, or ends then other than `Failed { timeout }`;
//! - I15, for the `checkout` sample only ([`audit_checkout`]): an order that ended left stock held or a charge
//!   neither shipped nor refunded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

use axon::files::{Address, Entry as LogEntry, FileFuture, Files, Log, LogConfig, OsFiles, crc32};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::journal::{Entry, Summary, replica_config};

/// One replica's journal: each run's fence and segments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Dump {
    pub runs: BTreeMap<Bytes, RunDump>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunDump {
    pub fence: u64,
    /// Each segment's records from its first, by epoch.
    pub segments: BTreeMap<u64, Vec<Bytes>>,
}

/// What a client was told is on a majority, one JSON object a line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ack {
    /// The journal's entry `index` (from 0) has this CRC-32; `t` in µs.
    Entry { run: String, index: u64, crc: u32, t: u64 },
    /// The run was started with an input of this hash (`run::machine::input_hash`).
    Started { run: String, input_hash: u64, t: u64 },
    /// The run took the signal.
    Signal { run: String, signal_id: String, t: u64 },
}

impl Ack {
    pub fn run(&self) -> &str {
        match self {
            Ack::Entry { run, .. } | Ack::Started { run, .. } | Ack::Signal { run, .. } => run,
        }
    }
}

/// An execution of an activity attempt, as the test activity appends it when it starts; `t` in µs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Witness {
    pub run: String,
    pub step: u32,
    pub attempt: u32,
    pub node: String,
    pub epoch: u64,
    pub t: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub invariant: &'static str,
    pub run: String,
    pub what: String,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub entries: usize,
    /// Each run's journal as its next owner would read it.
    pub journals: BTreeMap<Bytes, Vec<Bytes>>,
    pub violations: Vec<Violation>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }

    #[track_caller]
    pub fn assert_ok(&self) {
        assert!(self.is_ok(), "{self}");
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} runs, {} entries, {} violations", self.journals.len(), self.entries, self.violations.len())?;
        for v in self.violations.iter().take(20) {
            write!(f, "\n  {} run {}: {}", v.invariant, v.run, v.what)?;
        }
        Ok(())
    }
}

/// Reads a replica's log whole. It writes nothing; on disk, the replica must be stopped (its directory is locked).
pub fn dump(files: Arc<dyn Files>, cfg: LogConfig) -> FileFuture<Dump> {
    let open = Log::open(files, cfg);
    Box::pin(async move {
        let replicas = Replicas(vec![open.await?]);
        let runs: Vec<Bytes> = replicas.runs().into_iter().collect();
        Ok(replicas.dump(&runs).await?.remove(0))
    })
}

/// The replicas' logs, open together and read a few runs at a time: the soak's 1.4 million runs, read whole on three
/// replicas, took 60 GB.
pub struct Replicas(Vec<Log>);

impl Replicas {
    /// Each replica directory on disk, in the replicas' own layout, stopped.
    pub async fn open(dirs: &[PathBuf]) -> std::io::Result<Self> {
        let mut logs = Vec::new();
        for dir in dirs {
            logs.push(Log::open(Arc::new(OsFiles::new(dir)), replica_config()).await?);
        }
        Ok(Self(logs))
    }

    /// Every run a replica holds, in order.
    pub fn runs(&self) -> BTreeSet<Bytes> {
        let mut runs = BTreeSet::new();
        for log in &self.0 {
            let mut from: Option<Bytes> = None;
            loop {
                let keys = log.keys(from.as_deref(), 4096);
                let Some((last, _)) = keys.last() else { break };
                from = Some(Bytes::from([last.as_ref(), &[0]].concat()));
                runs.extend(keys.into_iter().map(|(run, _)| run));
            }
        }
        runs
    }

    /// Each replica's dump of these runs, of those it holds.
    pub async fn dump(&self, runs: &[Bytes]) -> std::io::Result<Vec<Dump>> {
        let mut dumps = Vec::new();
        for log in &self.0 {
            let mut dump = Dump::default();
            // Each read waits on the blocking pool: 64 at once, not each in turn.
            let reads: Vec<_> = runs
                .iter()
                .filter_map(|run| {
                    let fence = log.key(run)?.fence;
                    let (read, run) = (log.read(run, Address::default(), usize::MAX), run.clone());
                    Some(async move { (run, fence, read.await) })
                })
                .collect();
            let mut reads = futures_util::stream::iter(reads).buffered(64);
            while let Some((run, fence, entries)) = reads.next().await {
                let mut found = RunDump { fence, segments: BTreeMap::new() };
                for entry in entries? {
                    if let LogEntry::Record { at, body } = entry {
                        let segment = found.segments.entry(at.epoch).or_default();
                        if at.seq != segment.len() as u64 + 1 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("run {}: segment {} has a hole before {}", show(&run), at.epoch, at.seq),
                            ));
                        }
                        segment.push(body);
                    }
                }
                dump.runs.insert(run, found);
            }
            dumps.push(dump);
        }
        Ok(dumps)
    }
}

/// Reads JSON lines one at a time, skipping blank ones; an error names its line.
pub fn read_lines<T: DeserializeOwned>(text: impl BufRead) -> impl Iterator<Item = Result<T, String>> {
    text.lines().enumerate().filter_map(|(i, line)| match line {
        Ok(line) if line.trim().is_empty() => None,
        Ok(line) => Some(serde_json::from_str(&line).map_err(|e| format!("line {}: {e}", i + 1))),
        Err(e) => Some(Err(format!("line {}: {e}", i + 1))),
    })
}

fn show(run: &[u8]) -> String {
    String::from_utf8_lossy(run).into_owned()
}

/// Checks every run the replicas hold against the acks and, when given, the witness log.
pub fn check(dumps: &[Dump], acks: &[Ack], witness: Option<&[Witness]>) -> Report {
    let mut report = Report::default();
    // Each run's acks, entry index to when, and its executions.
    let mut acked_at: BTreeMap<&[u8], BTreeMap<u64, u64>> = BTreeMap::new();
    for ack in acks {
        if let Ack::Entry { run, index, t, .. } = ack {
            acked_at.entry(run.as_bytes()).or_default().insert(*index, *t);
        }
    }
    let mut executed: BTreeMap<&[u8], Vec<&Witness>> = BTreeMap::new();
    for w in witness.into_iter().flatten() {
        executed.entry(w.run.as_bytes()).or_default().push(w);
    }
    let runs: BTreeSet<&Bytes> = dumps.iter().flat_map(|d| d.runs.keys()).collect();
    let mut decoded: BTreeMap<Bytes, Vec<Option<Entry>>> = BTreeMap::new();
    for run in runs {
        let mut v = Vec::new();
        let journal = rebuild(dumps, run, &mut v);
        let entries = scan(&journal, &mut v);
        if witness.is_some() {
            let mine = executed.get(&run[..]).map_or(&[][..], Vec::as_slice);
            executions(&entries, acked_at.get(&run[..]), mine, &mut v);
        }
        report.entries += journal.len();
        report.violations.extend(v.into_iter().map(|(invariant, what)| Violation { invariant, run: show(run), what }));
        report.journals.insert(run.clone(), journal);
        decoded.insert(run.clone(), entries);
    }
    let entries = |run: &str| decoded.get(run.as_bytes()).into_iter().flatten().flatten();
    for ack in acks {
        let (invariant, run, what) = match ack {
            Ack::Entry { run, index, crc, .. } => {
                let what = match report.journals.get(run.as_bytes()).and_then(|j| j.get(*index as usize)) {
                    Some(body) if crc32(body) == *crc => continue,
                    Some(_) => format!("entry {index} was acked with another body"),
                    None => format!("entry {index} was acked and is not in the journal"),
                };
                ("I2", run, what)
            }
            Ack::Started { run, input_hash, .. } => {
                let started = entries(run).find_map(|e| match e {
                    Entry::Started { input_hash, .. } => Some(*input_hash),
                    _ => None,
                });
                match started {
                    Some(hash) if hash == *input_hash => continue,
                    Some(hash) => ("I8", run, format!("started with input hash {hash:x}, acked {input_hash:x}")),
                    None => ("I2", run, "a start was acked and the journal has none".into()),
                }
            }
            Ack::Signal { run, signal_id, .. } => {
                let received = entries(run)
                    .filter(|e| matches!(e, Entry::SignalReceived { signal_id: id, .. } if id == signal_id))
                    .count();
                if received == 1 {
                    continue;
                }
                ("I7", run, format!("signal {signal_id} was acked and is received {received} times"))
            }
        };
        report.violations.push(Violation { invariant, run: run.clone(), what });
    }
    report
}

/// I15 over the `checkout` sample's runs that ended: a completed one reserved, charged and shipped; a failed or
/// cancelled one released each hold it took and refunded each charge it made. A terminated run is left out: it
/// stops without its cleanup by design. What a redrive undid does not count.
pub fn audit_checkout(report: &Report) -> Vec<Violation> {
    let mut v = Vec::new();
    for (run, journal) in &report.journals {
        let whole: Vec<Entry> = journal.iter().map(|b| Entry::decode(b).unwrap_or(Entry::Idle)).collect();
        let live = crate::journal::entry::kept(&whole);
        let (mut checkout, mut names, mut done, mut end) = (false, BTreeMap::new(), BTreeMap::new(), None);
        for entry in &live {
            match entry {
                Entry::Started { workflow, .. } => checkout = workflow == "checkout",
                Entry::ActivityScheduled { step, name, .. } => drop(names.insert(*step, name.as_str())),
                Entry::ActivityDone { step, .. } => {
                    *done.entry(names.get(step).copied().unwrap_or("?")).or_insert(0u32) += 1;
                }
                Entry::Completed { .. } => end = Some("completed"),
                Entry::Failed { .. } => end = Some("failed"),
                Entry::Cancelled => end = Some("cancelled"),
                Entry::Terminated { .. } => end = Some("terminated"),
                _ => {}
            }
        }
        let n = |name: &str| done.get(name).copied().unwrap_or(0);
        let what = match end {
            _ if !checkout => continue,
            Some("completed") if n("reserve") == 1 && n("charge") == 1 && n("ship") >= 1 => continue,
            Some("completed") => format!("completed with {done:?}"),
            Some("failed" | "cancelled") if n("release") >= n("reserve") && n("refund") >= n("charge") => continue,
            Some(how @ ("failed" | "cancelled")) => format!("{how} with {done:?}: stock held or a charge kept"),
            _ => continue,
        };
        v.push(Violation { invariant: "I15", run: show(run), what });
    }
    v
}

/// I14 for one replica, quiet and repaired: each run it indexes has the summary of its journal, and each run with a
/// journal is indexed.
pub fn check_summaries(report: &Report, replica: &str, summaries: &[(Bytes, Summary)]) -> Vec<Violation> {
    let mut v = Vec::new();
    let indexed: BTreeMap<&Bytes, &Summary> = summaries.iter().map(|(run, s)| (run, s)).collect();
    for (run, journal) in report.journals.iter().filter(|(_, j)| !j.is_empty()) {
        let what = match indexed.get(run) {
            Some(s) if **s == Summary::of(journal) => continue,
            Some(s) => format!("{replica} indexes {s:?}, the journal is {:?}", Summary::of(journal)),
            None => format!("{replica} does not index it"),
        };
        v.push(Violation { invariant: "I14", run: show(run), what });
    }
    for run in indexed.keys().filter(|run| report.journals.get(**run).is_none_or(Vec::is_empty)) {
        v.push(Violation {
            invariant: "I14",
            run: show(run),
            what: format!("{replica} indexes a run with no journal"),
        });
    }
    v
}

/// The run's journal as a new owner reads it, from every replica at once: the newest segment, then each `Own.prev`
/// back to the first, each record from a replica or a carry.
fn rebuild(dumps: &[Dump], run: &Bytes, v: &mut Vec<(&'static str, String)>) -> Vec<Bytes> {
    // Replicas hold prefixes of one segment: the longest holds them all.
    let mut segments: BTreeMap<u64, Vec<Bytes>> = BTreeMap::new();
    for (r, dump) in dumps.iter().enumerate() {
        for (epoch, records) in dump.runs.get(run).map(|d| &d.segments).into_iter().flatten() {
            let merged = segments.entry(*epoch).or_default();
            if let Some(i) = merged.iter().zip(records).position(|(a, b)| a != b) {
                v.push(("I3", format!("replica {r} holds another record {} of segment {epoch}", i + 1)));
            }
            if records.len() > merged.len() {
                merged.extend_from_slice(&records[merged.len()..]);
            }
        }
    }
    let Some((&newest, records)) = segments.iter().rev().find(|(_, records)| !records.is_empty()) else {
        return Vec::new();
    };
    let mut at = (newest, records.len() as u64);
    let mut pool: BTreeMap<Address, Bytes> = BTreeMap::new();
    let mut chain = Vec::new();
    loop {
        let (epoch, len) = at;
        let mut records = Vec::new();
        for seq in 1..=len {
            let held = segments.get(&epoch).and_then(|s| s.get(seq as usize - 1));
            let carried = pool.get(&Address { epoch, seq });
            match (held, carried) {
                (Some(a), Some(b)) if a != b => {
                    v.push(("I3", format!("record {seq} of segment {epoch} differs from its carry")));
                }
                (None, None) => {
                    v.push(("I3", format!("record {seq} of segment {epoch} is counted and nowhere")));
                    return Vec::new();
                }
                _ => {}
            }
            records.extend(held.or(carried).cloned());
        }
        let Ok(Entry::Own(own)) = Entry::decode(&records[0]) else {
            v.push(("I3", format!("segment {epoch} does not start with Own")));
            return Vec::new();
        };
        for (at, body) in own.carry {
            let held = (at.seq as usize).checked_sub(1).and_then(|i| segments.get(&at.epoch)?.get(i));
            if held.is_some_and(|held| *held != body) {
                v.push(("I3", format!("a carry of segment {epoch} differs from record {} of {}", at.seq, at.epoch)));
            }
            pool.entry(at).or_insert(body);
        }
        chain.push(records);
        match own.prev {
            None => break,
            Some(prev) if prev.epoch < epoch && prev.seq > 0 => at = (prev.epoch, prev.seq),
            Some(prev) => {
                v.push(("I3", format!("segment {epoch} names ({}, {}) before it", prev.epoch, prev.seq)));
                return Vec::new();
            }
        }
    }
    chain.into_iter().rev().flatten().collect()
}

/// I5, I6, I7, I8, I10 and I16, over the decoded entries; what a redrive undid is left out, here and in what it
/// returns.
fn scan(journal: &[Bytes], v: &mut Vec<(&'static str, String)>) -> Vec<Option<Entry>> {
    let entries: Vec<Option<Entry>> = journal.iter().map(|b| Entry::decode(b).ok()).collect();
    let whole: Vec<Entry> = entries.iter().map(|e| e.clone().unwrap_or(Entry::Idle)).collect();
    // An entry that does not decode reads as `Idle`, which no redrive voids.
    let void = crate::journal::entry::voided(&whole);
    let mut outcomes: BTreeMap<u32, usize> = BTreeMap::new();
    let mut ended: Option<&Entry> = None;
    let mut wake_at: BTreeMap<u32, u64> = BTreeMap::new();
    // Each signal's name and place among those received, and the place of the last one taken of each name.
    let mut received: BTreeMap<&str, (&str, usize)> = BTreeMap::new();
    let mut taken: BTreeMap<&str, usize> = BTreeMap::new();
    let mut timed_out = false;
    let mut started = None;
    for (i, entry) in entries.iter().enumerate().filter(|(i, _)| !void[*i]) {
        let Some(entry) = entry else {
            v.push(("I3", format!("entry {i} does not decode")));
            continue;
        };
        // After the run's timeout fired, the next entry that is not an owner's ends it so.
        match entry {
            _ if !timed_out => {}
            Entry::Own(_) | Entry::Idle => {}
            Entry::Failed { timeout: true, .. } => timed_out = false,
            e => {
                v.push(("I16", format!("entry {i} {e:?} after the run timed out")));
                timed_out = false;
            }
        }
        match entry {
            Entry::Started { .. } => {
                if let Some(first) = started.replace(i) {
                    v.push(("I8", format!("entry {i} starts the run again, after entry {first}")));
                }
            }
            Entry::TimerStarted { step, wake_at_us, .. } => drop(wake_at.insert(*step, *wake_at_us)),
            Entry::TimerFired { step, at_us } => {
                if let Some(at) = wake_at.get(step).filter(|at| at_us < *at) {
                    v.push(("I6", format!("the timer of step {step} fired at {at_us}, before {at}")));
                }
                timed_out |= *step == 0;
            }
            Entry::SignalReceived { signal_id, name, .. } => {
                let place = received.len();
                if received.insert(signal_id, (name, place)).is_some() {
                    v.push(("I7", format!("signal {signal_id} is received twice")));
                }
            }
            Entry::SignalConsumed { signal_id, .. } => match received.get(signal_id.as_str()) {
                None => v.push(("I7", format!("signal {signal_id} is taken and was not received"))),
                Some((name, place)) => {
                    if let Some(last) = taken.insert(name, *place)
                        && last >= *place
                    {
                        v.push(("I7", format!("signal {signal_id} is taken out of order or twice")));
                    }
                }
            },
            _ => {}
        }
        if let Some(step) = entry.outcome_of()
            && let Some(first) = outcomes.insert(step, i)
        {
            v.push(("I5", format!("step {step} has a second outcome at entry {i}, after entry {first}")));
        }
        match (ended, entry) {
            (None, e) if e.is_terminal() => ended = Some(e),
            (None, _) | (Some(_), Entry::Own(_) | Entry::ParentNotified | Entry::Deleted) => {}
            (Some(Entry::Failed { .. }), Entry::Redriven { .. }) => ended = None,
            (Some(end), e) => v.push(("I10", format!("entry {i} {e:?} after {end:?}"))),
        }
    }
    entries.into_iter().zip(void).map(|(entry, void)| entry.filter(|_| !void)).collect()
}

/// I4: each `ActivityDone` has its execution, and no attempt of its step starts after it was acked.
fn executions(
    entries: &[Option<Entry>],
    acked_at: Option<&BTreeMap<u64, u64>>,
    mine: &[&Witness],
    v: &mut Vec<(&'static str, String)>,
) {
    for (i, entry) in entries.iter().enumerate() {
        let Some(Entry::ActivityDone { step, attempt, .. }) = entry else { continue };
        if !mine.iter().any(|w| w.step == *step && w.attempt == *attempt) {
            v.push(("I4", format!("step {step} attempt {attempt} is done with no execution")));
        }
        if let Some(t) = acked_at.and_then(|a| a.get(&(i as u64)))
            && let Some(late) = mine.iter().find(|w| w.step == *step && w.t > *t)
        {
            v.push(("I4", format!("step {step} attempt {} started after its result was acked", late.attempt)));
        }
    }
}

#[cfg(test)]
mod tests {
    use axon::files::MemoryFiles;

    use super::*;
    use crate::journal::{Answer, Handled, Own, Replica, Request};
    use crate::ready;

    fn own(prev: Option<(u64, u64)>, carry: &[((u64, u64), &Bytes)]) -> Bytes {
        let at = |(epoch, seq)| Address { epoch, seq };
        let carry = carry.iter().map(|(a, b)| (at(*a), (*b).clone())).collect();
        Entry::Own(Own { prev: prev.map(at), carry, ..Own::default() }).encode()
    }

    fn done(step: u32, attempt: u32) -> Bytes {
        Entry::ActivityDone { step, attempt, output: Bytes::new(), at_us: 0 }.encode()
    }

    fn held(run: &str, segments: Vec<(u64, Vec<Bytes>)>) -> Dump {
        let segments = segments.into_iter().collect();
        Dump { runs: BTreeMap::from([(Bytes::from(run.to_owned()), RunDump { fence: 0, segments })]) }
    }

    fn ack(index: u64, body: &Bytes, t: u64) -> Ack {
        Ack::Entry { run: "r".into(), index, crc: crc32(body), t }
    }

    fn invariants(report: &Report) -> Vec<&'static str> {
        report.violations.iter().map(|v| v.invariant).collect()
    }

    #[test]
    fn a_checkout_that_ended_holds_no_stock_and_keeps_no_charge() {
        let started = |workflow: &str| Entry::Started {
            workflow: workflow.into(),
            version: 1,
            input: Bytes::new(),
            input_hash: 0,
            tags: Vec::new(),
            parent: None,
            run_timeout_ms: None,
            at_us: 0,
        };
        let did = |step: u32, name: &str| {
            let scheduled = Entry::ActivityScheduled {
                step,
                name: name.into(),
                queue: String::new(),
                input: Bytes::new(),
                policy: Default::default(),
                at_us: 0,
            };
            [scheduled, Entry::ActivityDone { step, attempt: 1, output: Bytes::new(), at_us: 0 }]
        };
        let journal = |workflow: &str, names: &[&str], end: Entry| {
            let mut entries = vec![started(workflow)];
            for (i, name) in names.iter().enumerate() {
                entries.extend(did(i as u32 + 1, name));
            }
            entries.push(end);
            entries.iter().map(Entry::encode).collect::<Vec<_>>()
        };
        let failed = || Entry::Failed { error: "x".into(), timeout: false };
        let mut report = Report::default();
        let runs = [
            ("ok", journal("checkout", &["reserve", "charge", "ship"], Entry::Completed { output: Bytes::new() })),
            ("refunded", journal("checkout", &["reserve", "charge", "refund", "release"], failed())),
            ("released", journal("checkout", &["reserve", "release"], Entry::Cancelled)),
            ("kept", journal("checkout", &["reserve", "charge", "release"], failed())),
            ("held", journal("checkout", &["reserve"], Entry::Cancelled)),
            ("unshipped", journal("checkout", &["reserve", "charge"], Entry::Completed { output: Bytes::new() })),
            ("stopped", journal("checkout", &["reserve"], Entry::Terminated { reason: String::new() })),
            ("other", journal("batch", &["reserve"], failed())),
        ];
        for (run, entries) in runs {
            report.journals.insert(Bytes::from(run), entries);
        }
        let bad: Vec<String> = audit_checkout(&report).into_iter().map(|v| v.run).collect();
        assert_eq!(bad, ["held", "kept", "unshipped"]);
    }

    #[test]
    fn a_chain_reads_through_carries_and_ghost_segments() {
        let (o1, a, x) = (own(None, &[]), done(1, 1), done(2, 1));
        let o2 = own(Some((1, 3)), &[((1, 3), &x)]);
        let ghost = own(Some((1, 2)), &[]);
        let o3 = own(Some((3, 1)), &[((3, 1), &o2)]);
        // r0 holds x; r1 and r2 do not; the ghost (epoch 2) is on r2 alone and on no chain.
        let r0 = held("r", vec![(1, vec![o1.clone(), a.clone(), x.clone()]), (3, vec![o2.clone()])]);
        let r1 = held("r", vec![(1, vec![o1.clone(), a.clone()]), (4, vec![o3.clone()])]);
        let r2 = held("r", vec![(1, vec![o1.clone()]), (2, vec![ghost]), (4, vec![o3.clone()])]);
        let acks = [ack(1, &a, 0), ack(2, &x, 0), ack(4, &o3, 0)];
        let report = check(&[r0, r1.clone(), r2.clone()], &acks, None);
        report.assert_ok();
        assert_eq!(report.journals[b"r".as_slice()], vec![o1.clone(), a.clone(), x.clone(), o2.clone(), o3.clone()]);
        // Without r0, x comes from the carry inside o3's carry.
        assert_eq!(check(&[r1.clone(), r2.clone()], &acks, None).journals[b"r".as_slice()].len(), 5);
        // Without the carry, x is counted and nowhere.
        let bare = own(Some((3, 1)), &[]);
        let r1 = held("r", vec![(1, vec![o1.clone(), a.clone()]), (4, vec![bare.clone()])]);
        let r2 = held("r", vec![(1, vec![o1.clone()]), (4, vec![bare])]);
        assert_eq!(invariants(&check(&[r1, r2], &acks, None)), vec!["I3", "I2", "I2", "I2"]);
    }

    #[test]
    fn forks_lost_acks_second_outcomes_and_late_entries_are_found() {
        let o1 = own(None, &[]);
        let (a, b) = (done(1, 1), done(1, 2));
        let fork = check(
            &[held("r", vec![(1, vec![o1.clone(), a.clone()])]), held("r", vec![(1, vec![o1.clone(), b.clone()])])],
            &[],
            None,
        );
        assert_eq!(invariants(&fork), vec!["I3"]);
        let twice = check(&[held("r", vec![(1, vec![o1.clone(), a.clone(), b.clone()])])], &[], None);
        assert_eq!(invariants(&twice), vec!["I5"]);
        let lost = check(&[held("r", vec![(1, vec![o1.clone()])])], &[ack(1, &a, 0)], None);
        assert_eq!(invariants(&lost), vec!["I2"]);
        let moved = check(&[held("r", vec![(1, vec![o1.clone(), a.clone(), b.clone()])])], &[ack(1, &b, 0)], None);
        assert_eq!(invariants(&moved), vec!["I5", "I2"]);
        let failed = Entry::Failed { error: "e".into(), timeout: false }.encode();
        let redriven = Entry::Redriven { step: 1 }.encode();
        let ok =
            [o1.clone(), failed.clone(), o1.clone(), redriven, done(2, 1), failed.clone(), Entry::Deleted.encode()];
        assert!(check(&[held("r", vec![(1, ok.to_vec())])], &[], None).is_ok());
        let late = check(&[held("r", vec![(1, vec![o1.clone(), failed, done(3, 1)])])], &[], None);
        assert_eq!(invariants(&late), vec!["I10"]);
        // A carry at record 0 names nothing, and the checker reads on.
        let zero = own(Some((1, 1)), &[((1, 0), &a)]);
        assert!(check(&[held("r", vec![(1, vec![o1.clone()]), (2, vec![zero])])], &[], None).is_ok());
    }

    #[test]
    fn the_witness_holds_each_done_attempt_and_none_after_its_ack() {
        let o1 = own(None, &[]);
        let d = done(1, 2);
        let replicas = [held("r", vec![(1, vec![o1, d.clone()])])];
        let w = |attempt, t| Witness { run: "r".into(), step: 1, attempt, node: "w1".into(), epoch: 1, t };
        let acks = [ack(1, &d, 100)];
        assert!(check(&replicas, &acks, Some(&[w(1, 10), w(2, 50)])).is_ok());
        assert_eq!(invariants(&check(&replicas, &acks, Some(&[w(1, 10)]))), vec!["I4"]);
        assert_eq!(invariants(&check(&replicas, &acks, Some(&[w(2, 50), w(3, 150)]))), vec!["I4"]);
        let lines = "{\"run\":\"r\",\"step\":1,\"attempt\":2,\"node\":\"w1\",\"epoch\":1,\"t\":50}\n\n";
        assert_eq!(read_lines::<Witness>(lines.as_bytes()).collect::<Result<Vec<_>, _>>(), Ok(vec![w(2, 50)]));
        let acks_text = serde_json::to_string(&acks[0]).unwrap();
        assert_eq!(
            acks_text,
            format!("{{\"kind\":\"entry\",\"run\":\"r\",\"index\":1,\"crc\":{},\"t\":100}}", crc32(&d))
        );
        let bad: Result<Vec<Ack>, _> = read_lines(&b"{}"[..]).collect();
        assert!(bad.unwrap_err().starts_with("line 1:"));
    }

    #[test]
    fn early_timers_lost_or_doubled_signals_and_runs_past_their_timeout_are_found() {
        let o1 = own(None, &[]);
        let one = |entries: Vec<Entry>| {
            let records = std::iter::once(o1.clone()).chain(entries.iter().map(Entry::encode)).collect();
            [held("r", vec![(1, records)])]
        };
        let timer = |step, wake_at_us| Entry::TimerStarted { step, wake_at_us, at_us: 0 };
        let fired = |step, at_us| Entry::TimerFired { step, at_us };
        let got = |id: &str, name: &str| Entry::SignalReceived {
            signal_id: id.into(),
            name: name.into(),
            payload: Bytes::new(),
            at_us: 0,
        };
        let took = |step, id: &str| Entry::SignalConsumed { step, signal_id: id.into(), at_us: 0 };
        assert!(check(&one(vec![timer(1, 10), fired(1, 10)]), &[], None).is_ok());
        assert_eq!(invariants(&check(&one(vec![timer(1, 10), fired(1, 9)]), &[], None)), vec!["I6"]);
        let signal = |id: &str| Ack::Signal { run: "r".into(), signal_id: id.into(), t: 0 };
        let in_order =
            one(vec![got("a", "go"), got("b", "go"), got("c", "no"), took(1, "a"), took(2, "c"), took(3, "b")]);
        assert!(check(&in_order, &[signal("a"), signal("b")], None).is_ok());
        assert_eq!(invariants(&check(&in_order, &[signal("x")], None)), vec!["I7"], "lost");
        assert_eq!(invariants(&check(&one(vec![got("a", "go"), got("a", "go")]), &[], None)), vec!["I7"]);
        let swapped = one(vec![got("a", "go"), got("b", "go"), took(1, "b"), took(2, "a")]);
        assert_eq!(invariants(&check(&swapped, &[], None)), vec!["I7"]);
        let twice = one(vec![got("a", "go"), took(1, "a"), took(2, "a")]);
        assert_eq!(invariants(&check(&twice, &[], None)), vec!["I7"]);
        let failed = |timeout| Entry::Failed { error: "e".into(), timeout };
        assert!(check(&one(vec![timer(0, 5), fired(0, 5), Entry::Idle, failed(true)]), &[], None).is_ok());
        let past = one(vec![timer(0, 5), fired(0, 5), got("a", "go"), failed(true)]);
        assert_eq!(invariants(&check(&past, &[], None)), vec!["I16"]);
        let started = |hash| Ack::Started { run: "r".into(), input_hash: hash, t: 0 };
        let start = Entry::Started {
            workflow: "w".into(),
            version: 1,
            input_hash: 7,
            input: Bytes::new(),
            tags: Vec::new(),
            parent: None,
            run_timeout_ms: None,
            at_us: 0,
        };
        assert!(check(&one(vec![start.clone()]), &[started(7)], None).is_ok());
        assert_eq!(invariants(&check(&one(vec![start.clone(), start.clone()]), &[], None)), vec!["I8"]);
        assert_eq!(invariants(&check(&one(vec![start]), &[started(8)], None)), vec!["I8"]);
        assert_eq!(invariants(&check(&one(vec![]), &[started(7)], None)), vec!["I2"]);
    }

    /// Writes a run on a replica's log through the replica core, as the journal actor will.
    fn write(replica: &mut Replica<()>, run: &str, epoch: u64, records: Vec<Bytes>) -> std::io::Result<()> {
        let request = Request::Append { run: Bytes::from(run.to_owned()), epoch, seq: 1, records };
        assert!(matches!(replica.handle(request, ()), Handled::Queued));
        let write = replica.take_write(0).expect("a write");
        let result = ready(write);
        assert!(matches!(&replica.written(result)[..], [((), Answer::Acked { .. })]));
        Ok(())
    }

    #[test]
    fn a_replica_in_memory_reads_back_through_its_log() {
        let files = MemoryFiles::new();
        let mut replica: Replica<()> =
            ready(Replica::open(Arc::new(files.clone()), replica_config(), None, 0)).unwrap();
        let o1 = own(None, &[]);
        write(&mut replica, "r", 1, vec![o1.clone(), done(1, 1)]).unwrap();
        let dumped = ready(dump(Arc::new(files), replica_config())).unwrap();
        assert_eq!(dumped.runs[b"r".as_slice()].segments[&1], vec![o1, done(1, 1)]);
        check(&[dumped], &[], None).assert_ok();
    }

    #[tokio::test]
    async fn replicas_on_disk_read_back_a_batch_of_runs_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let o1 = own(None, &[]);
        {
            let open = Replica::open(Arc::new(OsFiles::new(dir.path())), replica_config(), None, 0);
            let mut replica: Replica<()> = open.await.unwrap();
            for run in [&b"r"[..], b"s"] {
                let request =
                    Request::Append { run: Bytes::copy_from_slice(run), epoch: 1, seq: 1, records: vec![o1.clone()] };
                assert!(matches!(replica.handle(request, ()), Handled::Queued));
            }
            let write = replica.take_write(0).expect("a write");
            let result = write.await;
            assert_eq!(replica.written(result).len(), 2);
        }
        let replicas = Replicas::open(&[dir.path().to_owned()]).await.unwrap();
        let (r, s) = (Bytes::from_static(b"r"), Bytes::from_static(b"s"));
        assert_eq!(replicas.runs(), BTreeSet::from([r.clone(), s]));
        let dumps = replicas.dump(&[r]).await.unwrap();
        assert_eq!(dumps[0].runs.keys().collect::<Vec<_>>(), [b"r".as_slice()]);
        assert_eq!(dumps[0].runs[b"r".as_slice()].segments[&1], vec![o1]);
        check(&dumps, &[], None).assert_ok();
    }
}
