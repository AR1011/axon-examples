//! A run's journal entries, and their bytes in postcard. A record in the replicas' logs is one entry.

use axon::files::Address;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use super::index::Status;
use super::index::Summary;

/// The first record of each owner's segment: where the segment before it ends, and that segment's records past what
/// every replica of the majority it read held, so each record a run counts is on a majority once this one is.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "OwnBytes", into = "OwnBytes")]
pub struct Own {
    /// The segment before: its epoch, and how many of its records count (`seq`). `None` for a run's first segment.
    pub prev: Option<Address>,
    /// Records of other segments by their address, oldest first. One may be an `Own` with a carry of its own.
    pub carry: Vec<(Address, Bytes)>,
    /// The run as the journal before it leaves it, so a replica indexes the run from its newest segment alone.
    pub summary: Summary,
    /// The node that opened this segment, and its wall clock then, in µs: shown, never compared.
    pub node: String,
    pub at_us: u64,
}

/// `Own` with its addresses as pairs: `Address` has no serde.
#[derive(Serialize, Deserialize)]
struct OwnBytes(Option<(u64, u64)>, Vec<((u64, u64), Bytes)>, Summary, String, u64);

impl From<Own> for OwnBytes {
    fn from(own: Own) -> Self {
        let pair = |a: Address| (a.epoch, a.seq);
        let carry = own.carry.into_iter().map(|(a, b)| (pair(a), b)).collect();
        OwnBytes(own.prev.map(pair), carry, own.summary, own.node, own.at_us)
    }
}

impl From<OwnBytes> for Own {
    fn from(OwnBytes(prev, carry, summary, node, at_us): OwnBytes) -> Self {
        let at = |(epoch, seq): (u64, u64)| Address { epoch, seq };
        let carry = carry.into_iter().map(|(a, b)| (at(a), b)).collect();
        Own { prev: prev.map(at), carry, summary, node, at_us }
    }
}

/// An activity's retry policy, as the run journals it when it schedules the step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub attempts: u32,
    pub initial_ms: u64,
    pub max_ms: u64,
    pub start_to_close_ms: Option<u64>,
    pub heartbeat_ms: Option<u64>,
    pub schedule_to_start_ms: Option<u64>,
    pub non_retryable: Vec<String>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            attempts: 10,
            initial_ms: 1_000,
            max_ms: 60_000,
            start_to_close_ms: None,
            heartbeat_ms: None,
            schedule_to_start_ms: None,
            non_retryable: Vec::new(),
        }
    }
}

/// What `ctx.now()`, `ctx.random()` and `ctx.uuid()` journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueKind {
    Now,
    Random,
    Uuid,
}

/// A variant's index is its tag on disk: a new one goes last. An `at_us` is the wall clock of the node that journaled
/// the entry, in µs: the console's timeline shows it, and nothing compares it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Entry {
    Own(Own),
    Started {
        workflow: String,
        version: u32,
        input: Bytes,
        input_hash: u64,
        tags: Vec<(String, String)>,
        parent: Option<String>,
        run_timeout_ms: Option<u64>,
        /// The starting node's wall clock, in µs.
        at_us: u64,
    },
    Value {
        step: u32,
        kind: ValueKind,
        value: Bytes,
    },
    ActivityScheduled {
        step: u32,
        name: String,
        queue: String,
        input: Bytes,
        policy: Policy,
        at_us: u64,
    },
    ActivityFailed {
        step: u32,
        attempt: u32,
        error: String,
        /// When the next attempt is due, in µs of the wall clock; `None` when the step failed for good.
        retry_at_us: Option<u64>,
        at_us: u64,
    },
    ActivityDone {
        step: u32,
        attempt: u32,
        output: Bytes,
        at_us: u64,
    },
    TimerStarted {
        step: u32,
        wake_at_us: u64,
        at_us: u64,
    },
    TimerFired {
        step: u32,
        /// The firing node's wall clock when it fired: never before `wake_at_us` (I6).
        at_us: u64,
    },
    SignalReceived {
        signal_id: String,
        name: String,
        payload: Bytes,
        at_us: u64,
    },
    SignalConsumed {
        step: u32,
        signal_id: String,
        at_us: u64,
    },
    ChildStarted {
        step: u32,
        id: String,
        at_us: u64,
    },
    ChildDone {
        step: u32,
        result: Result<Bytes, String>,
        at_us: u64,
    },
    ParentNotified,
    CancelRequested {
        reason: String,
    },
    Paused,
    Resumed,
    Redriven {
        step: u32,
    },
    StatusSet {
        status: Bytes,
    },
    Parked {
        reason: String,
    },
    Completed {
        output: Bytes,
    },
    Failed {
        error: String,
        timeout: bool,
    },
    Cancelled,
    Terminated {
        reason: String,
    },
    Deleted,
    /// The run stopped itself with nothing to do until a call or a durable timer: the reconciler leaves it alone.
    Idle,
    /// A worker started an attempt of the activity at `step`: its schedule-to-start timer no longer fires.
    Picked {
        step: u32,
    },
}

/// A value's bytes in postcard: a tag or length as a varint, integers above a byte as varints.
pub fn encode<T: Serialize>(value: &T) -> Bytes {
    postcard::to_allocvec(value).expect("the journal's types have no unsized sequences").into()
}

/// A value from all of `bytes`: an error for bytes that are cut short, or that go on after it.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> postcard::Result<T> {
    match postcard::take_from_bytes(bytes)? {
        (value, []) => Ok(value),
        _ => Err(postcard::Error::DeserializeBadEncoding),
    }
}

impl Entry {
    pub fn encode(&self) -> Bytes {
        encode(self)
    }

    pub fn decode(bytes: &[u8]) -> postcard::Result<Entry> {
        decode(bytes)
    }

    /// The step whose outcome this is: at most one per step (I5).
    pub fn outcome_of(&self) -> Option<u32> {
        match self {
            Entry::ActivityDone { step, .. }
            | Entry::TimerFired { step, .. }
            | Entry::SignalConsumed { step, .. }
            | Entry::ChildDone { step, .. }
            | Entry::Value { step, .. }
            | Entry::ActivityFailed { step, retry_at_us: None, .. } => Some(*step),
            _ => None,
        }
    }

    /// The run ended with it.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Entry::Completed { .. } | Entry::Failed { .. } | Entry::Cancelled | Entry::Terminated { .. })
    }
}

/// Where a redrive of a run ending with `journal` would go back to: the index of the step's last failure and the
/// step. Only a run that failed for good on an activity (not a run timeout) after its last redrive.
fn redrive_point(journal: &[Entry]) -> Option<(usize, u32)> {
    let from = journal.iter().rposition(|e| matches!(e, Entry::Redriven { .. })).map_or(0, |r| r + 1);
    let end = from + journal[from..].iter().rposition(Entry::is_terminal)?;
    let Entry::Failed { timeout: false, .. } = journal[end] else { return None };
    let at = journal[from..end].iter().rposition(|e| matches!(e, Entry::ActivityFailed { retry_at_us: None, .. }))?;
    match journal[from + at] {
        Entry::ActivityFailed { step, .. } => Some((from + at, step)),
        _ => None,
    }
}

/// The step a redrive of this journal tries again, if it may be redriven.
pub fn redrivable(journal: &[Entry]) -> Option<u32> {
    redrive_point(journal).map(|(_, step)| step)
}

/// Which entries a redrive undid: for each `Redriven`, from the step's last failure through the run's `Failed`,
/// except the calls the run took (signals, a cancel). The code runs past them as if the step had not failed for
/// good, so what it did after the failure is made again; the attempts undone still count for the attempts' numbers.
pub fn voided(journal: &[Entry]) -> Vec<bool> {
    let mut voided = vec![false; journal.len()];
    for r in (0..journal.len()).filter(|r| matches!(journal[*r], Entry::Redriven { .. })) {
        let Some((at, _)) = redrive_point(&journal[..r]) else { continue };
        for (i, entry) in journal.iter().enumerate().take(r).skip(at) {
            let kept = matches!(
                entry,
                Entry::SignalReceived { .. } | Entry::CancelRequested { .. } | Entry::Own(_) | Entry::Idle
            );
            voided[i] = !kept;
        }
    }
    voided
}

/// The entries no redrive undid.
pub fn kept(journal: &[Entry]) -> Vec<Entry> {
    journal.iter().zip(voided(journal)).filter(|(_, void)| !void).map(|(e, _)| e.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_kind() -> Vec<Entry> {
        let b = |s: &'static str| Bytes::from_static(s.as_bytes());
        vec![
            Entry::Own(Own {
                prev: Some(Address { epoch: 0x100, seq: 2 }),
                carry: vec![(Address { epoch: 0x100, seq: 2 }, b("x"))],
                summary: Summary {
                    workflow: "w".into(),
                    version: 2,
                    status: Status::Parked,
                    input_hash: 3,
                    parent: Some("p".into()),
                    tags: vec![("k".into(), "v".into())],
                    timers: [(4, 5)].into(),
                    idle: true,
                    parked_version: true,
                    owes_parent: true,
                    started_us: 12,
                },
                node: "e1".into(),
                at_us: 13,
            }),
            Entry::Own(Own::default()),
            Entry::Started {
                workflow: "checkout".into(),
                version: 1,
                input: b("{}"),
                input_hash: 7,
                tags: vec![("k".into(), "v".into())],
                parent: Some("p".into()),
                run_timeout_ms: None,
                at_us: 12,
            },
            Entry::Value { step: 1, kind: ValueKind::Uuid, value: b("u") },
            Entry::ActivityScheduled {
                step: 2,
                name: "charge".into(),
                queue: "payments".into(),
                input: b("i"),
                policy: Policy { non_retryable: vec!["card_declined".into()], ..Policy::default() },
                at_us: 8,
            },
            Entry::ActivityFailed { step: 2, attempt: 1, error: "e".into(), retry_at_us: Some(9), at_us: 8 },
            Entry::ActivityDone { step: 2, attempt: 2, output: b("o"), at_us: 9 },
            Entry::TimerStarted { step: 3, wake_at_us: 10, at_us: 9 },
            Entry::TimerFired { step: 3, at_us: 11 },
            Entry::SignalReceived { signal_id: "s1".into(), name: "approve".into(), payload: b("p"), at_us: 11 },
            Entry::SignalConsumed { step: 4, signal_id: "s1".into(), at_us: 11 },
            Entry::ChildStarted { step: 5, id: "r/5".into(), at_us: 12 },
            Entry::ChildDone { step: 5, result: Ok(b("c")), at_us: 13 },
            Entry::ChildDone { step: 6, result: Err("no".into()), at_us: 14 },
            Entry::ParentNotified,
            Entry::CancelRequested { reason: "r".into() },
            Entry::Paused,
            Entry::Resumed,
            Entry::Redriven { step: 2 },
            Entry::StatusSet { status: b("{}") },
            Entry::Parked { reason: "r".into() },
            Entry::Completed { output: b("o") },
            Entry::Failed { error: "e".into(), timeout: true },
            Entry::Cancelled,
            Entry::Terminated { reason: "r".into() },
            Entry::Deleted,
            Entry::Idle,
            Entry::Picked { step: 2 },
        ]
    }

    #[test]
    fn every_entry_reads_back() {
        for entry in every_kind() {
            assert_eq!(Entry::decode(&entry.encode()).as_ref(), Ok(&entry));
        }
    }

    #[test]
    fn the_bytes_are_pinned() {
        let own = Entry::Own(Own {
            prev: Some(Address { epoch: 0x0102, seq: 3 }),
            carry: vec![(Address { epoch: 0x0102, seq: 3 }, Bytes::from_static(b"ab"))],
            summary: Summary { version: 2, status: Status::Failed, ..Summary::default() },
            node: "e2".into(),
            at_us: 300,
        });
        // Own is variant 0, then prev (Some, epoch 0x0102 as a varint, seq), the carry, then the summary: workflow,
        // version, status, input hash, parent, tags, timers, idle, parked for a version, owes its parent, started;
        // then the node and its clock.
        let own_bytes =
            [0, 1, 0x82, 2, 3, 1, 0x82, 2, 3, 2, b'a', b'b', 0, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 2, b'e', b'2', 0xac, 2];
        assert_eq!(own.encode().as_ref(), own_bytes);
        let done = Entry::ActivityDone { step: 2, attempt: 1, output: Bytes::from_static(b"o"), at_us: 5 };
        assert_eq!(done.encode().as_ref(), [5, 2, 1, 1, b'o', 5]);
        let failed = Entry::Failed { error: "e".into(), timeout: true };
        assert_eq!(failed.encode().as_ref(), [20, 1, b'e', 1]);
        assert_eq!(Entry::Idle.encode().as_ref(), [24]);
        assert_eq!(Entry::Picked { step: 3 }.encode().as_ref(), [25, 3]);
    }

    #[test]
    fn a_redrive_voids_from_the_failure_to_the_end_and_keeps_the_calls() {
        let failed = |step, retry: Option<u64>| Entry::ActivityFailed {
            step,
            attempt: 1,
            error: "e".into(),
            retry_at_us: retry,
            at_us: 0,
        };
        let end = Entry::Failed { error: "e".into(), timeout: false };
        let signal = Entry::SignalReceived { signal_id: "s".into(), name: "n".into(), payload: Bytes::new(), at_us: 0 };
        let mut journal = vec![
            failed(1, Some(5)),
            failed(1, None),
            Entry::ActivityScheduled {
                step: 2,
                name: "undo".into(),
                queue: "local".into(),
                input: Bytes::new(),
                policy: Policy::default(),
                at_us: 0,
            },
            signal,
            end.clone(),
        ];
        assert_eq!(redrivable(&journal), Some(1));
        assert_eq!(redrivable(&journal[..4]), None, "not ended");
        journal.push(Entry::Redriven { step: 1 });
        assert_eq!(voided(&journal), [false, true, true, false, true, false]);
        assert_eq!(redrivable(&journal), None, "running again");
        // It fails again: a second redrive voids only what came after the first.
        journal.extend([failed(1, None), end.clone()]);
        assert_eq!(redrivable(&journal), Some(1));
        journal.push(Entry::Redriven { step: 1 });
        assert_eq!(voided(&journal), [false, true, true, false, true, false, true, true, false]);
        // A run timeout, or a run that failed with no activity failing for good, is not redriven.
        assert_eq!(redrivable(&[failed(1, None), Entry::Failed { error: "t".into(), timeout: true }]), None);
        assert_eq!(redrivable(&[failed(1, Some(5)), end]), None);
    }

    #[test]
    fn a_cut_or_padded_entry_is_refused() {
        for entry in every_kind() {
            let bytes = entry.encode();
            for cut in 0..bytes.len() {
                assert!(Entry::decode(&bytes.slice(..cut)).is_err(), "{entry:?} cut at {cut}");
            }
            assert!(Entry::decode(&[&bytes[..], &[0]].concat()).is_err(), "{entry:?} padded");
        }
        assert!(Entry::decode(&[99]).is_err());
    }
}
