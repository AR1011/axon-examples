//! Apply, write, publish, answer: the order every store here keeps. A change is applied in memory, queued as an op,
//! written with the ops queued beside it in one write, and only once that write is durable are its records published
//! and its callers answered. One write is out at a time; what comes meanwhile goes in the next.

use std::collections::VecDeque;

use axon::files::{Address, Log, LogError, Lsn};
use bytes::Bytes;

/// What one write holds for a key, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Record(Bytes),
    /// Stands for every record before it.
    Checkpoint(Bytes),
    /// Another key's one value: appended, and what the key held before trimmed away.
    Put(Bytes, Bytes),
    /// Another key, and all it holds, go.
    Remove(Bytes),
}

impl Op {
    pub fn len(&self) -> usize {
        match self {
            Self::Record(b) | Self::Checkpoint(b) | Self::Put(_, b) => b.len(),
            Self::Remove(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct Pending<W> {
    op: Op,
    /// Stream records to publish once the op is durable.
    publish: Vec<Bytes>,
    waiters: Vec<W>,
}

pub struct Pipeline<W> {
    queued: VecDeque<Pending<W>>,
    out: Vec<Pending<W>>,
    unwritten: usize,
}

impl<W> Default for Pipeline<W> {
    fn default() -> Self {
        Self { queued: VecDeque::new(), out: Vec::new(), unwritten: 0 }
    }
}

/// What a durable write lets go: the records to publish, in order, then the callers to answer.
pub struct Done<W> {
    pub publish: Vec<Vec<Bytes>>,
    pub waiters: Vec<W>,
}

impl<W> Pipeline<W> {
    pub fn push(&mut self, op: Op, publish: Vec<Bytes>, waiter: Option<W>) {
        self.unwritten += op.len();
        self.queued.push_back(Pending { op, publish, waiters: waiter.into_iter().collect() });
    }

    /// Answers `waiter` with the last op: a caller whose change was already applied waits until what applied it is
    /// durable. `Err` gives it back when nothing is unwritten, to answer at once.
    pub fn wait(&mut self, waiter: W) -> Result<(), W> {
        match self.queued.back_mut().or(self.out.last_mut()) {
            Some(last) => {
                last.waiters.push(waiter);
                Ok(())
            }
            None => Err(waiter),
        }
    }

    /// The next write, unless one is out: `None` from a write's start until `written`.
    pub fn take(&mut self) -> Option<Vec<Op>> {
        if !self.out.is_empty() || self.queued.is_empty() {
            return None;
        }
        self.out = self.queued.drain(..).collect();
        Some(self.out.iter().map(|p| p.op.clone()).collect())
    }

    /// The write out is durable.
    pub fn written(&mut self) -> Done<W> {
        let mut done = Done { publish: Vec::new(), waiters: Vec::new() };
        for pending in self.out.drain(..) {
            self.unwritten -= pending.op.len();
            if !pending.publish.is_empty() {
                done.publish.push(pending.publish);
            }
            done.waiters.extend(pending.waiters);
        }
        done
    }

    /// Takes back the callers `pick` chooses, to answer now: their ops stay and are written as before.
    pub fn release(&mut self, mut pick: impl FnMut(&W) -> bool) -> Vec<W> {
        let mut released = Vec::new();
        for pending in self.out.iter_mut().chain(self.queued.iter_mut()) {
            let (go, stay) = std::mem::take(&mut pending.waiters).into_iter().partition(&mut pick);
            pending.waiters = stay;
            released.extend::<Vec<W>>(go);
        }
        released
    }

    /// Bytes applied and not yet durable.
    pub fn unwritten(&self) -> usize {
        self.unwritten
    }

    pub fn is_idle(&self) -> bool {
        self.out.is_empty() && self.queued.is_empty()
    }
}

/// Puts a write's ops in the node's log under `key`: records at `epoch`, and a checkpoint through the key's last
/// record; the other keys' values at `epoch` too. Returns the LSN that makes them all durable. A write of an older
/// start of `key` is refused whole; any other refusal leaves the ops before it in the log.
pub fn append(log: &mut Log, key: &[u8], epoch: u64, ops: Vec<Op>) -> Result<Lsn, LogError> {
    if let Some(fence) = log.key(key).map(|k| k.fence).filter(|&fence| fence > epoch) {
        return Err(LogError::Fenced { fence });
    }
    let mut lsn = log.durable();
    for op in ops {
        lsn = match op {
            Op::Record(body) => log.append(key, epoch, body)?.lsn,
            Op::Checkpoint(state) => {
                let info = log.key(key).unwrap_or_default();
                let last = info.epochs.last().map(|&(epoch, seq)| Address { epoch, seq });
                log.checkpoint(key, last.or(info.trimmed).unwrap_or_default(), state)?
            }
            Op::Put(other, value) => {
                let at = log.append(&other, epoch, value)?.at;
                // Through the record before it: an address below the key's first trims nothing.
                log.trim(&other, Address { seq: at.seq - 1, ..at })?
            }
            Op::Remove(other) => log.remove(&other)?,
        };
    }
    Ok(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(b: &'static [u8]) -> Op {
        Op::Record(Bytes::from_static(b))
    }

    #[test]
    fn one_write_out_and_answers_after_it() {
        let mut p = Pipeline::default();
        assert_eq!(p.wait(0), Err(0), "nothing unwritten: answer at once");
        p.push(record(b"a"), vec![Bytes::from_static(b"A")], Some(1));
        p.push(record(b"bb"), vec![], Some(2));
        assert_eq!(p.unwritten(), 3);
        assert_eq!(p.take(), Some(vec![record(b"a"), record(b"bb")]));
        assert_eq!(p.take(), None, "one out at a time");
        p.push(Op::Checkpoint(Bytes::from_static(b"s")), vec![Bytes::from_static(b"S")], None);
        assert_eq!(p.wait(3), Ok(()), "waits for the checkpoint queued behind the write out");
        let done = p.written();
        assert_eq!(done.publish, vec![vec![Bytes::from_static(b"A")]]);
        assert_eq!(done.waiters, vec![1, 2]);
        assert_eq!(p.take(), Some(vec![Op::Checkpoint(Bytes::from_static(b"s"))]));
        assert_eq!(p.wait(4), Ok(()), "waits for the write out");
        let done = p.written();
        assert_eq!(done.waiters, vec![3, 4]);
        assert!(p.is_idle());
        assert_eq!(p.unwritten(), 0);
    }

    #[test]
    fn released_callers_leave_their_ops_to_be_written() {
        let mut p = Pipeline::default();
        p.push(record(b"a"), vec![Bytes::from_static(b"A")], Some(1));
        p.take();
        p.push(record(b"b"), vec![Bytes::from_static(b"B")], Some(2));
        p.push(record(b"c"), vec![], Some(3));
        assert_eq!(p.release(|w| *w != 2), vec![1, 3]);
        let done = p.written();
        assert_eq!((done.publish, done.waiters), (vec![vec![Bytes::from_static(b"A")]], vec![]));
        assert_eq!(p.take(), Some(vec![record(b"b"), record(b"c")]));
        assert_eq!(p.written().waiters, vec![2]);
    }
}
