//! A node's log: one `axon::files::Log` in one directory, kept by one actor, so a write and its sync cover every key
//! that changed meanwhile (the cluster guide's "A key's state on disk"). The documents, the catalog and the accounts
//! each hold a key here; each answers its callers only once the journal says their write is durable.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axon::files::{Address, Entry, Files, Log, LogConfig, LogError, LogStats, Lsn};
use axon::{Actor, Context, ReplyTo};
use bytes::Bytes;

use crate::core::pipeline::{self, Op};

/// How long the journal waits after a failed write before it writes the same bytes again.
pub const REST: Duration = Duration::from_millis(500);
/// Reads out at once. With the write, fewer jobs than the actor's `MailboxLimits::jobs` (16).
const READS: usize = 8;

/// A key's epoch for this start, and everything the log holds of it.
pub type Loaded = io::Result<(u64, Vec<Entry>)>;

pub enum ToJournal {
    Opened(io::Result<Box<Log>>),
    /// A key's owner starts: the key is fenced at a new epoch, and read once the fence is durable.
    Load(Bytes, ReplyTo<Loaded>),
    Read(Bytes, u64, io::Result<Vec<Entry>>, ReplyTo<Loaded>),
    /// What the log holds of a key, unfenced: a document's versions. The epoch answered is 0.
    Fetch(Bytes, ReplyTo<Loaded>),
    /// Answered once every op is durable, however long the disk fails: ask it with no deadline. A refusal may leave
    /// the ops before it in the log: the owner starts again.
    Write(Bytes, u64, Vec<Op>, ReplyTo<Result<(), LogError>>),
    /// The keys and all they hold go. Answered once that is durable.
    Remove(Vec<Bytes>, ReplyTo<Result<(), LogError>>),
    Written(io::Result<()>),
    Rested,
    Stats(ReplyTo<LogStats>),
}

enum Waiter {
    Load(Bytes, u64, ReplyTo<Loaded>),
    Write(ReplyTo<Result<(), LogError>>),
}

pub struct Journal {
    files: Arc<dyn Files>,
    cfg: Option<LogConfig>,
    log: Option<Log>,
    /// In LSN order.
    waiting: VecDeque<(Lsn, Waiter)>,
    to_read: VecDeque<(Bytes, u64, ReplyTo<Loaded>)>,
    reads: usize,
    resting: bool,
    failing: Arc<AtomicBool>,
}

impl Journal {
    pub fn new(files: Arc<dyn Files>, cfg: LogConfig) -> Self {
        let (waiting, to_read) = (VecDeque::new(), VecDeque::new());
        let failing = Arc::default();
        Self { files, cfg: Some(cfg), log: None, waiting, to_read, reads: 0, resting: false, failing }
    }

    /// Set while the last write failed: the owners of its keys refuse new changes meanwhile (`cannot_save`).
    pub fn failing(&self) -> Arc<AtomicBool> {
        self.failing.clone()
    }
}

/// The log of a home's `docs/`. A record holds the catalog's checkpoint, about 28 MB for `catalog::MAX_ROWS`
/// documents, and one write takes it whole. The bytes not yet durable may be the default 64 MiB besides it: else a
/// large checkpoint is refused `Behind` while the documents' records wait, and the catalog starts again.
pub fn docs_config() -> LogConfig {
    let max_record = 48 << 20;
    LogConfig {
        name: "docs".into(),
        max_record,
        max_write_bytes: max_record + (1 << 17),
        max_unwritten_bytes: max_record + (64 << 20),
        ..LogConfig::default()
    }
}

/// A small log: the accounts.
pub fn small_config(name: &str) -> LogConfig {
    LogConfig { name: name.into(), segment_bytes: 4 << 20, max_record: 1 << 20, ..LogConfig::default() }
}

/// A refused job, as an actor's error.
pub fn job_error(e: axon::SubmitError<impl Sized>) -> String {
    format!("{:?}", e.reason)
}

impl Actor for Journal {
    type Message = ToJournal;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let open = Log::open(self.files.clone(), self.cfg.take().ok_or("started twice")?);
        let job = cx.submit_future(async move { ToJournal::Opened(open.await.map(Box::new)) });
        cx.pause_until(&job.map_err(job_error)?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: ToJournal, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else {
            let ToJournal::Opened(opened) = msg else { return Err("not open".into()) };
            self.log = Some(*opened.map_err(|e| format!("the log does not open: {e}"))?);
            return Ok(());
        };
        match msg {
            ToJournal::Opened(_) => return Err("opened twice".into()),
            ToJournal::Load(key, reply) => {
                // A new epoch at each start: what an older start of the key still sends is refused `Fenced`.
                let epoch = log.key(&key).map_or(1, |k| k.fence + 1);
                match log.fence(&key, epoch) {
                    Ok((lsn, _)) => self.waiting.push_back((lsn, Waiter::Load(key, epoch, reply))),
                    Err(refused) => drop(reply.reply(Err(io::Error::other(refused)))),
                }
            }
            // A write removed a segment before the read got to it.
            ToJournal::Read(key, epoch, Err(e), reply) if e.kind() == io::ErrorKind::NotFound => {
                self.reads -= 1;
                self.to_read.push_front((key, epoch, reply));
            }
            ToJournal::Read(_, epoch, entries, reply) => {
                self.reads -= 1;
                drop(reply.reply(entries.map(|e| (epoch, e))));
            }
            ToJournal::Write(key, epoch, ops, reply) => match pipeline::append(log, &key, epoch, ops) {
                Ok(lsn) => self.waiting.push_back((lsn, Waiter::Write(reply))),
                Err(refused) => drop(reply.reply(Err(refused))),
            },
            ToJournal::Fetch(key, reply) => self.to_read.push_back((key, 0, reply)),
            ToJournal::Remove(keys, reply) => {
                match keys.iter().try_fold(log.durable(), |lsn, key| Ok::<_, LogError>(lsn.max(log.remove(key)?))) {
                    Ok(lsn) => self.waiting.push_back((lsn, Waiter::Write(reply))),
                    Err(refused) => drop(reply.reply(Err(refused))),
                }
            }
            ToJournal::Rested => self.resting = false,
            ToJournal::Stats(reply) => drop(reply.reply(log.stats())),
            ToJournal::Written(result) => {
                // A disk that fails at once would otherwise be tried again at once, and again.
                self.failing.store(result.is_err(), Ordering::Relaxed);
                self.resting = result.is_err() && cx.schedule_after(REST, ToJournal::Rested).is_ok();
                let durable = log.written(result);
                let done = self.waiting.partition_point(|(lsn, _)| *lsn <= durable);
                for (_, waiter) in self.waiting.drain(..done) {
                    match waiter {
                        Waiter::Load(key, epoch, reply) => self.to_read.push_back((key, epoch, reply)),
                        Waiter::Write(reply) => drop(reply.reply(Ok(()))),
                    }
                }
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else { return Ok(()) };
        while self.reads < READS
            && let Some((key, epoch, reply)) = self.to_read.pop_front()
        {
            let read = log.read(&key, Address::default(), usize::MAX);
            cx.submit_future(async move { ToJournal::Read(key, epoch, read.await.map(owned), reply) })
                .map_err(job_error)?;
            self.reads += 1;
        }
        if !self.resting
            && let Some(write) = log.take_write(axon::trace::unix_micros() / 1000)
        {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                ToJournal::Written(written.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
            });
            job.map_err(job_error)?;
        }
        Ok(())
    }

    fn label(msg: &ToJournal) -> &'static str {
        match msg {
            ToJournal::Load(..) | ToJournal::Read(..) | ToJournal::Fetch(..) => "journal.load",
            ToJournal::Written(_) => "journal.written",
            ToJournal::Stats(_) => "journal.stats",
            _ => "journal.write",
        }
    }
}

/// Each entry's bytes in a buffer of their own. `Log::read` slices them from one read of the segment that also holds
/// the other keys' frames between them, about 11 MB for a document's 1,000 records among 70 others typed at once, and
/// a document waiting in line to start would keep all of it.
fn owned(entries: Vec<Entry>) -> Vec<Entry> {
    let own = |b: Bytes| Bytes::copy_from_slice(&b);
    let entry = |entry| match entry {
        Entry::Checkpoint { through, state } => Entry::Checkpoint { through, state: own(state) },
        Entry::Record { at, body } => Entry::Record { at, body: own(body) },
    };
    entries.into_iter().map(entry).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axon::files::MemoryFiles;

    #[tokio::test]
    async fn a_read_holds_only_its_own_bytes() {
        let mut log = Log::open(Arc::new(MemoryFiles::new()), small_config("t")).await.expect("opens");
        for i in 0..4u8 {
            for key in [&b"a"[..], b"b"] {
                log.append(key, 1, Bytes::from(vec![i; 100])).expect("room");
            }
        }
        let written = log.take_write(0).expect("a write").await;
        log.written(written);
        let read = log.read(b"a", Address::default(), usize::MAX).await.expect("reads");
        assert_eq!(read.len(), 4);
        assert!(!read.iter().all(|e| matches!(e, Entry::Record { body, .. } if body.is_unique())), "one buffer");
        for entry in owned(read) {
            let Entry::Record { body, .. } = entry else { panic!("a record") };
            assert!(body.is_unique() && body.len() == 100);
        }
    }
}
