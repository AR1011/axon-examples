//! A state kept as events in a key of the node's log: each change is applied, journaled as one JSON record, and
//! answered once it is durable. Once there are `CHECKPOINT_EVERY` records since the last checkpoint and they are as
//! large as it, the whole state goes in as a checkpoint, which stands for the records before it: a large state is
//! written again about as often as what changed adds up to it. The catalog and the accounts are kept this way.

use axon::files::Entry;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::core::pipeline::{Done, Op, Pipeline};

pub const CHECKPOINT_EVERY: usize = 1000;

pub trait Book: Default + Serialize + DeserializeOwned + Send + 'static {
    type Event: Serialize + DeserializeOwned + Send + 'static;
    fn apply(&mut self, event: &Self::Event);
}

pub struct Kept<B: Book, W> {
    pub state: B,
    pub epoch: u64,
    pipeline: Pipeline<W>,
    /// Records since the last checkpoint, those read at the start too, and their bytes.
    since: usize,
    since_bytes: usize,
    /// The last checkpoint's bytes.
    checkpoint_bytes: usize,
}

impl<B: Book, W> Kept<B, W> {
    pub fn load(epoch: u64, entries: Vec<Entry>) -> Result<Self, String> {
        let mut kept = Self {
            state: B::default(),
            epoch,
            pipeline: Pipeline::default(),
            since: 0,
            since_bytes: 0,
            checkpoint_bytes: 0,
        };
        for entry in entries {
            match entry {
                Entry::Checkpoint { state, .. } => {
                    kept.state = serde_json::from_slice(&state).map_err(|e| format!("checkpoint: {e}"))?;
                    (kept.since, kept.since_bytes, kept.checkpoint_bytes) = (0, 0, state.len());
                }
                Entry::Record { body, .. } => {
                    let event = serde_json::from_slice(&body).map_err(|e| format!("record: {e}"))?;
                    kept.state.apply(&event);
                    (kept.since, kept.since_bytes) = (kept.since + 1, kept.since_bytes + body.len());
                }
            }
        }
        Ok(kept)
    }

    /// Applies `event` now; `waiter` is answered once it is durable.
    pub fn record(&mut self, event: B::Event, waiter: Option<W>) {
        self.state.apply(&event);
        let body = serde_json::to_vec(&event).expect("an event serialises");
        (self.since, self.since_bytes) = (self.since + 1, self.since_bytes + body.len());
        self.pipeline.push(Op::Record(Bytes::from(body)), Vec::new(), waiter);
        if self.since >= CHECKPOINT_EVERY && self.since_bytes >= self.checkpoint_bytes {
            let state = serde_json::to_vec(&self.state).expect("a state serialises");
            (self.since, self.since_bytes, self.checkpoint_bytes) = (0, 0, state.len());
            self.pipeline.push(Op::Checkpoint(Bytes::from(state)), Vec::new(), None);
        }
    }

    /// Answers `waiter` once what was applied before it is durable; `Err` hands it back to answer at once.
    pub fn after(&mut self, waiter: W) -> Result<(), W> {
        self.pipeline.wait(waiter)
    }

    pub fn take(&mut self) -> Option<Vec<Op>> {
        self.pipeline.take()
    }

    pub fn written(&mut self) -> Vec<W> {
        let Done { waiters, .. } = self.pipeline.written();
        waiters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Default, Serialize, Deserialize)]
    struct Sum(u64);

    impl Book for Sum {
        type Event = u64;
        fn apply(&mut self, n: &u64) {
            self.0 += n;
        }
    }

    fn entries(ops: Vec<Op>) -> Vec<Entry> {
        let address = axon::files::Address::default();
        let entry = |op| match op {
            Op::Record(body) => Entry::Record { at: address, body },
            Op::Checkpoint(state) => Entry::Checkpoint { through: address, state },
            Op::Put(..) | Op::Remove(_) => unreachable!("a book writes its own key only"),
        };
        ops.into_iter().map(entry).collect()
    }

    #[test]
    fn a_checkpoint_every_thousand_records_stands_for_those_before_it() {
        let mut kept = Kept::<Sum, u64>::load(1, Vec::new()).unwrap();
        for n in 0..CHECKPOINT_EVERY as u64 + 1 {
            kept.record(n, Some(n));
        }
        assert_eq!(kept.after(99), Ok(()));
        let ops = kept.take().unwrap();
        assert_eq!(ops.len(), CHECKPOINT_EVERY + 2, "every record and one checkpoint");
        assert!(matches!(ops[CHECKPOINT_EVERY], Op::Checkpoint(_)));
        assert_eq!(kept.written().len(), CHECKPOINT_EVERY + 2);
        // What the log keeps after the checkpoint trims what it stands for.
        let reloaded = Kept::<Sum, u64>::load(2, entries(ops[CHECKPOINT_EVERY..].to_vec())).unwrap();
        assert_eq!(reloaded.state.0, kept.state.0);
        assert_eq!(kept.state.0, (0..=CHECKPOINT_EVERY as u64).sum::<u64>());
    }

    #[derive(Default, Serialize, Deserialize)]
    struct Bag(Vec<String>);

    impl Book for Bag {
        type Event = String;
        fn apply(&mut self, s: &String) {
            self.0.push(s.clone());
        }
    }

    /// A catalog of 100,000 documents is about 20 MB: a checkpoint every 1,000 records would write it again for each
    /// 100 KB that changed.
    #[test]
    fn a_large_state_checkpoints_once_the_records_since_add_up_to_it() {
        let mut kept = Kept::<Bag, u64>::load(1, Vec::new()).unwrap();
        for n in 0..20_000u64 {
            kept.record(format!("{n:0>64}"), None);
        }
        let ops = kept.take().unwrap();
        let at: Vec<usize> = (0..ops.len()).filter(|i| matches!(ops[*i], Op::Checkpoint(_))).collect();
        // After 1,000 records, then each time the records since are as large as the state: 2,016, 4,062, 8,185 and
        // 16,494 records in.
        assert_eq!(at.len(), 5, "{at:?}");
        let reloaded = Kept::<Bag, u64>::load(2, entries(ops[at[4]..].to_vec())).unwrap();
        assert_eq!(reloaded.state.0, kept.state.0);
        // A start reads the records since too, and goes on counting them.
        assert_eq!((reloaded.since, reloaded.since_bytes), (kept.since, kept.since_bytes));
    }
}
