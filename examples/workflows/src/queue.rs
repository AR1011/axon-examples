//! A task queue: the offers of activity attempts that runs make, held until a worker polls for them. One keyed
//! actor per partition, `<queue>/<p>` with p in 0..`PARTITIONS` by the run's id, on the engines and not eager. The
//! queue keeps nothing on disk: a run offers again every 10 s until a worker says it started, so a lost queue loses
//! only time. A worker's poll waits in the queue as a held `Reply` until a task comes or its deadline is near.

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use axon::{Actor, Context, TimerId};
use axon_cluster::{ActorProtocol, CallError, Placement, Rejection, Reply, WireProtocol};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::journal::entry::{decode, encode};

/// The queue's kind.
pub const QUEUE: &str = "queue";
/// Partitions per queue: a worker polls each.
pub const PARTITIONS: u32 = 4;
/// Every caller of the `queue` kind passes these args.
pub const QUEUE_ARGS: Bytes = Bytes::new();

pub fn queue_placement() -> Placement {
    crate::node::run_placement()
}

/// The partition key of `run`'s tasks on `queue`.
pub fn partition(queue: &str, run: &str) -> String {
    format!("{queue}/{}", axon::files::crc32(run.as_bytes()) % PARTITIONS)
}

/// The partition keys of `queue`.
pub fn partitions(queue: &str) -> Vec<String> {
    (0..PARTITIONS).map(|p| format!("{queue}/{p}")).collect()
}

/// An activity attempt to run, as its run offers it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub run: String,
    pub step: u32,
    pub attempt: u32,
    /// As `ActivityCx::redone` names it.
    pub generation: u32,
    /// Each offer the worker gave back unstarted is offered again as the next round.
    pub round: u32,
    pub name: String,
    pub queue: String,
    /// In JSON.
    pub input: Bytes,
    pub heartbeat_ms: Option<u64>,
    /// The engine node and lease epoch of the run that offered it, as `ActivityCx` names them.
    pub owner: String,
    pub epoch: u64,
}

impl Task {
    fn key(&self) -> (String, u32, u32, u32) {
        (self.run.clone(), self.step, self.attempt, self.round)
    }

    fn bytes(&self) -> usize {
        self.input.len() + self.run.len() + self.name.len() + self.queue.len() + self.owner.len() + 32
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueCall {
    Offer(Task),
    /// Worker `worker` takes up to `free` tasks; the answer waits until there is one or the poll's deadline is near.
    Poll {
        worker: String,
        free: u32,
    },
    /// Worker `worker` stops polling: its held poll is answered with nothing.
    Unpoll {
        worker: String,
    },
    Stats,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueAnswer {
    Accepted,
    /// The backlog is at its cap: the run offers again later.
    Full,
    Tasks(Vec<Task>),
    Stats(QueueStats),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub backlog: u64,
    pub backlog_bytes: u64,
    /// How long the oldest task in the backlog has waited.
    pub oldest_ms: u64,
    pub pollers: u64,
    /// Offers refused at the cap, and polls refused at theirs, since the partition started.
    pub refused: u64,
    pub handed: u64,
}

impl QueueStats {
    pub fn add(&mut self, other: &QueueStats) {
        self.backlog += other.backlog;
        self.backlog_bytes += other.backlog_bytes;
        self.oldest_ms = self.oldest_ms.max(other.oldest_ms);
        self.pollers += other.pollers;
        self.refused += other.refused;
        self.handed += other.handed;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueueLimits {
    pub tasks: usize,
    pub bytes: usize,
    /// Held polls; one more is answered with nothing at once.
    pub polls: usize,
}

impl Default for QueueLimits {
    fn default() -> Self {
        QueueLimits { tasks: 10_000, bytes: 64 << 20, polls: 256 }
    }
}

/// The backlog is at its cap.
#[derive(Debug, PartialEq, Eq)]
pub struct Full;

/// A held poll is answered this long before its caller's deadline, so the answer arrives in time.
pub const ANSWER_BEFORE: Duration = Duration::from_millis(500);
/// A poll with no deadline is held this long.
const HOLD: Duration = Duration::from_secs(30);

struct Held<P> {
    worker: String,
    free: u32,
    reply: P,
    until: Duration,
    max_bytes: usize,
}

/// The queue without I/O: `P` is what answers a poll.
pub struct QueueCore<P> {
    limits: QueueLimits,
    backlog: VecDeque<(Duration, Task)>,
    keys: BTreeSet<(String, u32, u32, u32)>,
    bytes: usize,
    polls: VecDeque<Held<P>>,
    refused: u64,
    handed: u64,
}

impl<P> QueueCore<P> {
    pub fn new(limits: QueueLimits) -> Self {
        QueueCore {
            limits,
            backlog: VecDeque::new(),
            keys: BTreeSet::new(),
            bytes: 0,
            polls: VecDeque::new(),
            refused: 0,
            handed: 0,
        }
    }

    /// Queues `task`, or refuses it past the cap; a task already queued is taken once. Then the polls it answers.
    pub fn offer(&mut self, task: Task, now: Duration) -> Result<Vec<(P, Vec<Task>)>, Full> {
        if self.keys.contains(&task.key()) {
            return Ok(Vec::new());
        }
        if self.backlog.len() >= self.limits.tasks || self.bytes + task.bytes() > self.limits.bytes {
            self.refused += 1;
            return Err(Full);
        }
        self.keys.insert(task.key());
        self.bytes += task.bytes();
        self.backlog.push_back((now, task));
        Ok(self.hand())
    }

    /// Answers the poll at once with tasks, or holds it until `until`; one past the cap of held polls, or a second
    /// from the same worker, takes the place of nothing and is answered empty.
    pub fn poll(
        &mut self,
        worker: String,
        free: u32,
        reply: P,
        until: Duration,
        max_bytes: usize,
    ) -> Vec<(P, Vec<Task>)> {
        let mut out = Vec::new();
        if let Some(i) = self.polls.iter().position(|h| h.worker == worker)
            && let Some(old) = self.polls.remove(i)
        {
            out.push((old.reply, Vec::new()));
        }
        if self.polls.len() >= self.limits.polls {
            self.refused += 1;
            out.push((reply, Vec::new()));
            return out;
        }
        self.polls.push_back(Held { worker, free: free.max(1), reply, until, max_bytes });
        out.extend(self.hand());
        out
    }

    /// The held poll of `worker`, answered empty.
    pub fn unpoll(&mut self, worker: &str) -> Option<P> {
        let i = self.polls.iter().position(|h| h.worker == worker)?;
        self.polls.remove(i).map(|h| h.reply)
    }

    /// The held polls whose time is up or whose caller has gone, answered empty.
    pub fn expire(&mut self, now: Duration, gone: impl Fn(&P) -> bool) -> Vec<P> {
        let (out, kept): (Vec<_>, Vec<_>) = self.polls.drain(..).partition(|h| h.until <= now || gone(&h.reply));
        self.polls = kept.into();
        out.into_iter().map(|h| h.reply).collect()
    }

    /// The earliest deadline of a held poll.
    pub fn next_due(&self) -> Option<Duration> {
        self.polls.iter().map(|h| h.until).min()
    }

    pub fn stats(&self, now: Duration) -> QueueStats {
        QueueStats {
            backlog: self.backlog.len() as u64,
            backlog_bytes: self.bytes as u64,
            oldest_ms: self.backlog.front().map_or(0, |(at, _)| now.saturating_sub(*at).as_millis() as u64),
            pollers: self.polls.len() as u64,
            refused: self.refused,
            handed: self.handed,
        }
    }

    /// Gives the backlog's first tasks to the held polls, oldest poll first, each up to its free slots and bytes.
    fn hand(&mut self) -> Vec<(P, Vec<Task>)> {
        let mut out = Vec::new();
        while !self.backlog.is_empty()
            && let Some(held) = self.polls.pop_front()
        {
            let (mut tasks, mut bytes) = (Vec::new(), 0);
            while let Some((_, task)) = self.backlog.front()
                && tasks.len() < held.free as usize
                && (tasks.is_empty() || bytes + task.bytes() <= held.max_bytes)
            {
                bytes += task.bytes();
                let Some((_, task)) = self.backlog.pop_front() else { break };
                self.keys.remove(&task.key());
                self.bytes -= task.bytes();
                tasks.push(task);
            }
            self.handed += tasks.len() as u64;
            out.push((held.reply, tasks));
        }
        out
    }
}

pub enum QueueMsg {
    Call(QueueCall, Option<Reply<QueueAnswer>>),
    Expire,
}

/// A partition's actor.
pub struct Queue {
    core: QueueCore<Reply<QueueAnswer>>,
    timer: Option<(TimerId, Duration)>,
}

impl Queue {
    pub fn new(limits: QueueLimits) -> Queue {
        Queue { core: QueueCore::new(limits), timer: None }
    }

    fn arm(&mut self, cx: &mut Context<'_, Self>) {
        let Some(due) = self.core.next_due() else { return };
        // Each second at most, so a caller that went away is let go of soon.
        let due = due.min(cx.now() + Duration::from_secs(1));
        if self.timer.is_some_and(|(_, at)| at <= due) {
            return;
        }
        if let Some((id, _)) = self.timer.take() {
            cx.cancel_timer(id);
        }
        let after = due.saturating_sub(cx.now());
        self.timer = cx.schedule_after(after, QueueMsg::Expire).ok().map(|id| (id, due));
    }
}

fn answer(reply: Option<Reply<QueueAnswer>>, answer: QueueAnswer) {
    if let Some(reply) = reply {
        // A caller that has gone gets nothing.
        drop(reply.send(answer));
    }
}

impl Actor for Queue {
    type Message = QueueMsg;
    type Error = std::convert::Infallible;

    fn on_message(&mut self, msg: QueueMsg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let now = cx.now();
        match msg {
            QueueMsg::Call(QueueCall::Offer(task), reply) => match self.core.offer(task, now) {
                Ok(handed) => {
                    answer(reply, QueueAnswer::Accepted);
                    for (poll, tasks) in handed {
                        drop(poll.send(QueueAnswer::Tasks(tasks)));
                    }
                }
                Err(Full) => answer(reply, QueueAnswer::Full),
            },
            QueueMsg::Call(QueueCall::Poll { worker, free }, Some(reply)) => {
                let left = reply.time_left().unwrap_or(HOLD + ANSWER_BEFORE);
                let until = now + left.saturating_sub(ANSWER_BEFORE);
                // A task's postcard bytes are a little more than `Task::bytes` counts for its strings.
                let max_bytes = reply.max_payload() / 2;
                for (poll, tasks) in self.core.poll(worker, free, reply, until, max_bytes) {
                    drop(poll.send(QueueAnswer::Tasks(tasks)));
                }
            }
            QueueMsg::Call(QueueCall::Poll { .. }, None) => {}
            QueueMsg::Call(QueueCall::Unpoll { worker }, reply) => {
                if let Some(poll) = self.core.unpoll(&worker) {
                    drop(poll.send(QueueAnswer::Tasks(Vec::new())));
                }
                answer(reply, QueueAnswer::Accepted);
            }
            QueueMsg::Call(QueueCall::Stats, reply) => answer(reply, QueueAnswer::Stats(self.core.stats(now))),
            QueueMsg::Expire => self.timer = None,
        }
        for poll in self.core.expire(now, Reply::is_abandoned) {
            drop(poll.send(QueueAnswer::Tasks(Vec::new())));
        }
        self.arm(cx);
        Ok(())
    }
}

pub struct QueueApi;

impl WireProtocol for QueueApi {
    type Message = QueueCall;
    type Reply = QueueAnswer;
    const ID: u64 = 0x5746_5155; // "WFQU"
    const SCHEMA: u16 = 2;

    fn encode(call: QueueCall) -> Result<Bytes, CallError> {
        Ok(encode(&call))
    }

    fn decode_reply(body: Bytes) -> Result<QueueAnswer, CallError> {
        decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for QueueApi {
    type Mailbox = QueueMsg;

    fn encode_reply(answer: QueueAnswer) -> Result<Bytes, CallError> {
        Ok(encode(&answer))
    }

    fn decode(body: Bytes, reply: Option<Reply<QueueAnswer>>) -> Result<QueueMsg, Rejection> {
        let call = decode(&body).map_err(|_| Rejection::InvalidMessage)?;
        Ok(QueueMsg::Call(call, reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(run: &str, step: u32, input: usize) -> Task {
        Task {
            run: run.into(),
            step,
            attempt: 1,
            generation: 0,
            round: 1,
            name: "a".into(),
            queue: "q".into(),
            input: Bytes::from(vec![b'1'; input]),
            heartbeat_ms: None,
            owner: "e1".into(),
            epoch: 1,
        }
    }

    const S: Duration = Duration::from_secs(1);

    #[test]
    fn the_backlog_takes_its_cap_and_refuses_one_more() {
        let mut q = QueueCore::<u32>::new(QueueLimits { tasks: 3, bytes: 1 << 20, polls: 2 });
        for step in 0..3 {
            assert_eq!(q.offer(task("r", step, 1), S), Ok(Vec::new()));
        }
        // The same offer again is taken once; a fourth task is refused.
        assert_eq!(q.offer(task("r", 0, 1), S), Ok(Vec::new()));
        assert_eq!(q.offer(task("r", 3, 1), S), Err(Full));
        let stats = q.stats(3 * S);
        assert_eq!((stats.backlog, stats.oldest_ms, stats.refused), (3, 2_000, 1));
        // A poll empties it, and the counters return to zero.
        let out = q.poll("w".into(), 10, 7, 30 * S, 1 << 20);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.iter().map(|t| t.step).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!((q.stats(3 * S).backlog, q.stats(3 * S).backlog_bytes, q.stats(3 * S).pollers), (0, 0, 0));
        assert_eq!(q.offer(task("r", 3, 1), S).map(|v| v.len()), Ok(0));
    }

    #[test]
    fn bytes_cap_the_backlog_and_an_answer() {
        let one = task("r", 0, 100).bytes();
        let mut q = QueueCore::<u32>::new(QueueLimits { tasks: 100, bytes: 2 * one, polls: 2 });
        assert!(q.offer(task("r", 0, 100), S).is_ok());
        assert!(q.offer(task("r", 1, 100), S).is_ok());
        assert_eq!(q.offer(task("r", 2, 100), S), Err(Full));
        // An answer holds what fits its bytes, and at least one task.
        let out = q.poll("w".into(), 10, 1, 30 * S, one);
        assert_eq!(out[0].1.len(), 1);
        let out = q.poll("w".into(), 10, 2, 30 * S, 1);
        assert_eq!(out[0].1.len(), 1);
    }

    #[test]
    fn polls_wait_for_offers_and_expire() {
        let mut q = QueueCore::<u32>::new(QueueLimits { tasks: 10, bytes: 1 << 20, polls: 2 });
        assert!(q.poll("w1".into(), 1, 1, 30 * S, 1 << 20).is_empty());
        assert!(q.poll("w2".into(), 1, 2, 10 * S, 1 << 20).is_empty());
        // A third poll is past the cap: answered empty at once.
        assert_eq!(q.poll("w3".into(), 1, 3, 30 * S, 1 << 20), [(3, Vec::new())]);
        assert_eq!(q.next_due(), Some(10 * S));
        // An offer goes to the oldest poll.
        let out = q.offer(task("r", 0, 1), S).expect("room");
        assert_eq!(out.iter().map(|(p, t)| (*p, t.len())).collect::<Vec<_>>(), [(1, 1)]);
        assert_eq!(q.expire(10 * S, |_| false), [2]);
        assert!(q.poll("w1".into(), 1, 4, 30 * S, 1 << 20).is_empty());
        assert_eq!(q.unpoll("w1"), Some(4));
        assert!(q.poll("w1".into(), 1, 5, 30 * S, 1 << 20).is_empty());
        // A gone caller is let go of; a worker's second poll takes the place of its first.
        assert_eq!(q.poll("w1".into(), 1, 6, 30 * S, 1 << 20), [(5, Vec::new())]);
        assert_eq!(q.expire(S, |p| *p == 6), [6]);
        assert_eq!(q.stats(S).pollers, 0);
    }

    #[test]
    fn calls_read_back() {
        let calls = [
            QueueCall::Offer(task("r", 1, 2)),
            QueueCall::Poll { worker: "w".into(), free: 4 },
            QueueCall::Unpoll { worker: "w".into() },
            QueueCall::Stats,
        ];
        for call in calls {
            assert_eq!(decode(&encode(&call)), Ok(call));
        }
        let answers = [
            QueueAnswer::Accepted,
            QueueAnswer::Full,
            QueueAnswer::Tasks(vec![task("r", 1, 2)]),
            QueueAnswer::Stats(QueueStats::default()),
        ];
        for answer in answers {
            assert_eq!(decode(&encode(&answer)), Ok(answer));
        }
    }
}
