//! A run as the API shows it: its state and one row per step, built from its journal.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::journal::entry::voided;
use crate::journal::{Entry, Own, Status};
use crate::workflow::{Command, value_name};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunView {
    pub id: String,
    pub workflow: String,
    pub version: u32,
    pub status: Status,
    /// The output in JSON, once completed.
    pub output: Option<String>,
    pub error: Option<String>,
    pub parked: Option<String>,
    pub steps: Vec<StepView>,
    /// Owners' opens, signals, calls and the end, in journal order.
    pub events: Vec<EventView>,
    /// When the run started, in µs of the starting node's wall clock.
    pub started_us: Option<u64>,
    /// When something is next due, in µs of the wall clock.
    pub due_us: Option<u64>,
    /// The last `ctx.set_status`, in JSON.
    pub custom_status: Option<String>,
    pub tags: Vec<(String, String)>,
    pub parent: Option<String>,
    /// The node that runs it, and its lease epoch there.
    pub owner: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    pub step: u32,
    /// `activity`, `sleep`, `signal`, `child`, `now`, `random` or `uuid`.
    pub kind: String,
    /// The activity's or the signal's name, or the child's run id.
    pub name: String,
    /// `running`, `retrying`, `waiting`, `done`, `failed` or `timed_out`.
    pub status: String,
    pub attempts: u32,
    pub wake_at_us: Option<u64>,
    pub error: Option<String>,
    /// In JSON.
    pub output: Option<String>,
    pub input: Option<String>,
    /// The journal index of the step's first entry: steps and events show in this order.
    pub seq: u64,
    /// The wall clock of the step's first and last entries, in µs; `ended_us` once it ended. An entry with no time
    /// of its own has the time of the one before it.
    pub started_us: Option<u64>,
    pub ended_us: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventView {
    pub seq: u64,
    pub at_us: Option<u64>,
    /// `opened`, `reopened` (on the same node), `moved`, `signal`, `cancel`, `paused`, `resumed`, `retried`, `parked`, `completed`, `failed`,
    /// `cancelled` or `terminated`.
    pub kind: String,
    /// The node, the signal's name, the reason or the error.
    pub text: String,
}

/// Each signal's name by its id.
pub(crate) fn signal_names(journal: &[Entry]) -> BTreeMap<&str, &str> {
    journal
        .iter()
        .filter_map(|e| match e {
            Entry::SignalReceived { signal_id, name, .. } => Some((signal_id.as_str(), name.as_str())),
            _ => None,
        })
        .collect()
}

fn text(bytes: &[u8]) -> Option<String> {
    Some(String::from_utf8_lossy(bytes).into_owned())
}

/// The time of an entry that carries one, in µs of the journaling node's wall clock.
fn at_of(entry: &Entry) -> Option<u64> {
    match entry {
        // An owner that did not name itself (a writer of the tests) has no time either.
        Entry::Own(Own { node, .. }) if node.is_empty() => None,
        Entry::Own(Own { at_us, .. })
        | Entry::Started { at_us, .. }
        | Entry::ActivityScheduled { at_us, .. }
        | Entry::ActivityFailed { at_us, .. }
        | Entry::ActivityDone { at_us, .. }
        | Entry::TimerStarted { at_us, .. }
        | Entry::TimerFired { at_us, .. }
        | Entry::SignalReceived { at_us, .. }
        | Entry::SignalConsumed { at_us, .. }
        | Entry::ChildStarted { at_us, .. }
        | Entry::ChildDone { at_us, .. } => Some(*at_us),
        _ => None,
    }
}

/// The `seq` of a row the journal has not named yet.
const UNSEEN: u64 = u64::MAX;

/// The row of a step of the code's, made when the journal names it first: not the run's timeout, step 0. Its end is
/// set again by the entry that ends it.
fn row<'a>(
    steps: &'a mut BTreeMap<u32, StepView>,
    step: u32,
    kind: &str,
    seq: u64,
    at: Option<u64>,
) -> Option<&'a mut StepView> {
    (step > 0).then(|| {
        let v = steps.entry(step).or_insert_with(|| StepView {
            step,
            kind: kind.into(),
            seq: UNSEEN,
            ..StepView::default()
        });
        if v.seq == UNSEEN {
            (v.seq, v.started_us) = (seq, at);
        }
        v.ended_us = None;
        v
    })
}

/// What the run went through besides its steps: an owner's open, a signal, a call, the end.
fn event(entry: &Entry, owner: &mut Option<String>) -> Option<(&'static str, String)> {
    Some(match entry {
        Entry::Own(own) if own.node.is_empty() => return None,
        Entry::Own(own) => {
            let kind = match owner.replace(own.node.clone()) {
                None => "opened",
                Some(last) if last == own.node => "reopened",
                Some(_) => "moved",
            };
            (kind, own.node.clone())
        }
        Entry::SignalReceived { name, .. } => ("signal", name.clone()),
        Entry::CancelRequested { reason } => ("cancel", reason.clone()),
        Entry::Paused => ("paused", String::new()),
        Entry::Resumed => ("resumed", String::new()),
        Entry::Redriven { step } => ("retried", step.to_string()),
        Entry::Parked { reason } => ("parked", reason.clone()),
        Entry::Completed { .. } => ("completed", String::new()),
        Entry::Failed { error, .. } => ("failed", error.clone()),
        Entry::Cancelled => ("cancelled", String::new()),
        Entry::Terminated { reason } => ("terminated", reason.clone()),
        _ => return None,
    })
}

/// One row per step, in step order: from the journal, and from the code's steps the journal does not hold yet (a
/// wait for a signal). Then the run's events in journal order. `journal` holds each owner's `Own`; what a redrive
/// undid makes no step, but stays an event.
pub(crate) fn timeline(journal: &[Entry], issued: &BTreeMap<u32, Command>) -> (Vec<StepView>, Vec<EventView>) {
    let names = signal_names(journal);
    let void = voided(journal);
    let mut steps: BTreeMap<u32, StepView> = BTreeMap::new();
    let mut events = Vec::new();
    for (step, cmd) in issued.iter().filter(|(step, _)| **step > 0) {
        let (kind, name, input) = match cmd {
            Command::Activity { name, input, .. } => ("activity", name.clone(), text(input)),
            Command::Sleep(_) => ("sleep", String::new(), None),
            Command::Signal { name, .. } => ("signal", name.clone(), None),
            Command::Child { workflow, input } => ("child", workflow.clone(), text(input)),
            Command::Value(kind) => (value_name(*kind), String::new(), None),
            Command::Invalid(_) => continue,
        };
        let status = if matches!(kind, "activity" | "child") { "running" } else { "waiting" };
        steps.insert(
            *step,
            StepView {
                step: *step,
                kind: kind.into(),
                name,
                status: status.into(),
                input,
                seq: UNSEEN,
                ..StepView::default()
            },
        );
    }
    let (mut last_at, mut owner) = (None, None);
    for (seq, (entry, void)) in journal.iter().zip(void).enumerate() {
        let seq = seq as u64;
        let at = at_of(entry).or(last_at);
        last_at = at;
        if let Some((kind, detail)) = event(entry, &mut owner) {
            events.push(EventView { seq, at_us: at, kind: kind.into(), text: detail });
        }
        if void {
            continue;
        }
        let ended = |v: &mut StepView| v.ended_us = at;
        match entry {
            Entry::ActivityScheduled { step, name, input, .. } => {
                if let Some(v) = row(&mut steps, *step, "activity", seq, at) {
                    (v.name, v.status, v.attempts, v.input) = (name.clone(), "running".into(), 1, text(input));
                }
            }
            Entry::ActivityFailed { step, attempt, error, retry_at_us, .. } => {
                if let Some(v) = row(&mut steps, *step, "activity", seq, at) {
                    v.error = Some(error.clone());
                    v.wake_at_us = *retry_at_us;
                    v.attempts = *attempt + u32::from(retry_at_us.is_some());
                    v.status = if retry_at_us.is_some() { "retrying" } else { "failed" }.into();
                    if retry_at_us.is_none() {
                        ended(v);
                    }
                }
            }
            Entry::ActivityDone { step, attempt, output, .. } => {
                if let Some(v) = row(&mut steps, *step, "activity", seq, at) {
                    (v.status, v.attempts, v.output, v.wake_at_us) = ("done".into(), *attempt, text(output), None);
                    ended(v);
                }
            }
            // An activity's timer is its `schedule_to_start`: the row stays `running`.
            Entry::TimerStarted { step, wake_at_us, .. } => {
                if let Some(v) = row(&mut steps, *step, "sleep", seq, at) {
                    if v.kind != "activity" {
                        v.status = "waiting".into();
                    }
                    v.wake_at_us = Some(*wake_at_us);
                }
            }
            Entry::Picked { step } => {
                if let Some(v) = row(&mut steps, *step, "activity", seq, at) {
                    v.wake_at_us = None;
                }
            }
            Entry::TimerFired { step, .. } => {
                if let Some(v) = row(&mut steps, *step, "sleep", seq, at) {
                    v.status =
                        if matches!(v.kind.as_str(), "signal" | "activity") { "timed_out" } else { "done" }.into();
                    v.wake_at_us = None;
                    ended(v);
                }
            }
            Entry::ChildStarted { step, id, .. } => {
                if let Some(v) = row(&mut steps, *step, "child", seq, at) {
                    (v.name, v.status) = (id.clone(), "running".into());
                }
            }
            Entry::ChildDone { step, result, .. } => {
                if let Some(v) = row(&mut steps, *step, "child", seq, at) {
                    match result {
                        Ok(output) => (v.status, v.output) = ("done".into(), text(output)),
                        Err(error) => (v.status, v.error) = ("failed".into(), Some(error.clone())),
                    }
                    ended(v);
                }
            }
            Entry::SignalConsumed { step, signal_id, .. } => {
                if let Some(v) = row(&mut steps, *step, "signal", seq, at) {
                    v.name = names.get(signal_id.as_str()).map_or(String::new(), |n| n.to_string());
                    (v.kind, v.status, v.wake_at_us) = ("signal".into(), "done".into(), None);
                    ended(v);
                }
            }
            Entry::Value { step, kind, value } => {
                if let Some(v) = row(&mut steps, *step, value_name(*kind), seq, at) {
                    (v.status, v.output) = ("done".into(), text(value));
                    ended(v);
                }
            }
            _ => {}
        }
    }
    let end = journal.len() as u64;
    for v in steps.values_mut().filter(|v| v.seq == UNSEEN) {
        (v.seq, v.started_us) = (end, last_at);
    }
    (steps.into_values().collect(), events)
}
