//! Each account is a directory, `<dir>/<id>/`, with one log segment per owner, `<epoch>.log`, one JSON event per
//! line, and a snapshot, `snap`, every `every` events. An owner appends only to its own segment, so two owners never
//! interleave, and no owner ever waits for another: a paused or slow one cannot hold anything the next one needs.
//!
//! A new owner creates its segment first, then reads the older ones and seals them where it read them, in the `Own`
//! record that starts its segment. An owner acknowledges a write only once it is durable and no newer segment exists.
//! So a write that was acknowledged was on disk before the new owner looked, and a write that finds a newer segment
//! is refused and lies past the seal, where nobody reads it.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// A transfer debited here and not yet credited or refunded.
    Pending,
    Overdraft,
    /// The account is not open.
    Closed,
    /// Open on an open account.
    Exists,
    /// A transfer to an account that is not open; the debit was refunded.
    NoAccount,
    /// A transfer to itself.
    Invalid,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Event {
    /// The first record of each owner's segment: how long each older segment was when this owner read it.
    Own {
        sealed: BTreeMap<u64, u64>,
    },
    Open(String),
    Deposit(String, u64),
    Withdraw(String, u64),
    Refused(String, Outcome),
    Out {
        tid: String,
        to: String,
        amount: u64,
    },
    Settled(String),
    Refund(String),
    In {
        from: String,
        tid: String,
        amount: u64,
    },
    Reject {
        from: String,
        tid: String,
    },
}

/// An account as its log says. `done` maps every idempotency key it has seen to its outcome.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct State {
    pub open: bool,
    pub balance: i64,
    pub done: BTreeMap<String, Outcome>,
    /// Transfers debited and not yet settled: tid -> (to, amount).
    pub outs: BTreeMap<String, (String, u64)>,
    /// Credits by `from/tid`: true credited, false rejected.
    pub ins: BTreeMap<String, bool>,
}

impl State {
    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Own { .. } => {}
            Event::Open(key) => {
                self.open = true;
                self.done.insert(key.clone(), Outcome::Ok);
            }
            Event::Deposit(key, amount) => {
                self.balance += *amount as i64;
                self.done.insert(key.clone(), Outcome::Ok);
            }
            Event::Withdraw(key, amount) => {
                self.balance -= *amount as i64;
                self.done.insert(key.clone(), Outcome::Ok);
            }
            Event::Refused(key, why) => drop(self.done.insert(key.clone(), *why)),
            Event::Out { tid, to, amount } => {
                self.balance -= *amount as i64;
                self.outs.insert(tid.clone(), (to.clone(), *amount));
                self.done.insert(tid.clone(), Outcome::Pending);
            }
            Event::Settled(tid) => {
                self.outs.remove(tid);
                self.done.insert(tid.clone(), Outcome::Ok);
            }
            Event::Refund(tid) => {
                if let Some((_, amount)) = self.outs.remove(tid) {
                    self.balance += amount as i64;
                }
                self.done.insert(tid.clone(), Outcome::NoAccount);
            }
            Event::In { from, tid, amount } => {
                self.balance += *amount as i64;
                self.ins.insert(format!("{from}/{tid}"), true);
            }
            Event::Reject { from, tid } => drop(self.ins.insert(format!("{from}/{tid}"), false)),
        }
    }
}

#[derive(Debug)]
pub enum StoreError {
    /// A newer owner has a segment, or this epoch already has one.
    Fenced,
    Io(io::Error),
    Corrupt(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    epoch: u64,
    offset: u64,
    state: State,
}

/// One account's log as its newest owner sees it: each segment up to its seal, oldest first.
#[derive(Debug, Default)]
pub struct History {
    pub segments: Vec<Segment>,
    /// Bytes past a seal: writes refused because a newer owner had started, or cut short by a crash.
    pub fenced_bytes: u64,
}

#[derive(Debug)]
pub struct Segment {
    pub epoch: u64,
    bytes: Vec<u8>,
}

impl Segment {
    pub fn events_from(&self, offset: usize) -> Result<Vec<Event>, StoreError> {
        self.bytes[offset..]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).map_err(|e| StoreError::Corrupt(e.to_string())))
            .collect()
    }

    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

fn segment_name(epoch: u64) -> String {
    format!("{epoch:020}.log")
}

fn epochs(account: &Path) -> io::Result<Vec<u64>> {
    let mut epochs = Vec::new();
    for entry in fs::read_dir(account)? {
        let name = entry?.file_name();
        if let Some(epoch) = name.to_str().and_then(|n| n.strip_suffix(".log")).and_then(|n| n.parse().ok()) {
            epochs.push(epoch);
        }
    }
    epochs.sort_unstable();
    Ok(epochs)
}

/// The segments of `account` other than `mine`, up to their seals. `Fenced` if one is newer than `mine`.
fn history(account: &Path, mine: Option<u64>) -> Result<History, StoreError> {
    let mut raw = Vec::new();
    for epoch in epochs(account)? {
        match mine {
            Some(mine) if epoch > mine => return Err(StoreError::Fenced),
            Some(mine) if epoch == mine => continue,
            _ => {}
        }
        let bytes = fs::read(account.join(segment_name(epoch)))?;
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        raw.push((epoch, bytes, complete));
    }
    // The newest segment whose owner got as far as its `Own` record holds the seals of all the older ones. Newer ones
    // are owners that stopped before writing anything, and are empty.
    let own = |bytes: &[u8]| match bytes.split(|b| *b == b'\n').next().map(serde_json::from_slice) {
        Some(Ok(Event::Own { sealed })) => Some(sealed),
        _ => None,
    };
    let latest = raw.iter().rposition(|(_, bytes, complete)| own(&bytes[..*complete]).is_some());
    let sealed = latest.and_then(|i| own(&raw[i].1[..raw[i].2])).unwrap_or_default();
    let mut history = History::default();
    for (i, (epoch, mut bytes, complete)) in raw.into_iter().enumerate() {
        let end = match latest {
            Some(latest) if i == latest => complete, // past it: a write cut short, not refused
            _ => sealed.get(&epoch).map_or(0, |end| *end as usize),
        };
        if end > complete {
            return Err(StoreError::Corrupt(format!("{} sealed at {end}, is {complete} long", account.display())));
        }
        if Some(i) != latest {
            history.fenced_bytes += (bytes.len() - end) as u64;
        }
        bytes.truncate(end);
        history.segments.push(Segment { epoch, bytes });
    }
    Ok(history)
}

/// Appends in this process waiting for a `cut` to finish: none has written yet, or looked for a newer owner.
pub static WAITING: AtomicU64 = AtomicU64::new(0);

/// The writing end of one account's log, held by its owner.
pub struct Log {
    file: File,
    account: PathBuf,
    epoch: u64,
    len: u64,
    since_snapshot: usize,
    every: usize,
}

/// Takes the account over at `epoch`: its state from the snapshot and the segments after it, each up to its seal.
/// `Fenced` when a newer owner has started, or this epoch has a segment already.
pub fn recover(dir: &Path, id: &str, epoch: u64, every: usize) -> Result<(Log, State), StoreError> {
    let account = dir.join(id);
    fs::create_dir_all(&account)?;
    let file = OpenOptions::new().append(true).create_new(true).open(account.join(segment_name(epoch)));
    let file = file.map_err(|e| match e.kind() {
        io::ErrorKind::AlreadyExists => StoreError::Fenced,
        _ => StoreError::Io(e),
    })?;
    let history = history(&account, Some(epoch))?;
    let snapshot = fs::read(account.join("snap")).ok().and_then(|b| serde_json::from_slice::<Snapshot>(&b).ok());
    // A snapshot is used only where it lies inside the sealed history: one written by a fenced owner is older, not
    // wrong, and one past a seal is not used.
    let start = snapshot.and_then(|s| {
        let i = history.segments.iter().position(|seg| seg.epoch == s.epoch)?;
        let offset = s.offset as usize;
        let at_line = offset == 0 || history.segments[i].bytes.get(offset - 1) == Some(&b'\n');
        (offset <= history.segments[i].bytes.len() && at_line).then_some((i, offset, s.state))
    });
    let (first, offset, mut state) = start.unwrap_or((0, 0, State::default()));
    let mut replayed = 0;
    for (i, segment) in history.segments.iter().enumerate().skip(first) {
        for event in segment.events_from(if i == first { offset } else { 0 })? {
            state.apply(&event);
            replayed += 1;
        }
    }
    let sealed = history.segments.iter().map(|s| (s.epoch, s.len())).collect();
    let mut log = Log { file, account, epoch, len: 0, since_snapshot: replayed, every };
    log.append(&[Event::Own { sealed }], None)?;
    Ok((log, state))
}

impl Log {
    /// Whether appending `n` more events is due a snapshot; pass the state after them to `append` if so.
    pub fn due(&self, n: usize) -> bool {
        self.since_snapshot + n >= self.every
    }

    /// Durable and still the newest owner when it returns `Ok`.
    pub fn append(&mut self, events: &[Event], snapshot: Option<&State>) -> Result<(), StoreError> {
        let mut buf = Vec::new();
        for event in events {
            serde_json::to_writer(&mut buf, event).map_err(|e| StoreError::Corrupt(e.to_string()))?;
            buf.push(b'\n');
        }
        // Shared, so writers never wait for each other; `cut` takes it alone to read every log at one instant.
        let cut = cut_lock(self.account.parent().unwrap_or(&self.account))?;
        match cut.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                WAITING.fetch_add(1, Relaxed);
                let locked = cut.lock_shared();
                WAITING.fetch_sub(1, Relaxed);
                locked?;
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        self.file.write_all(&buf)?;
        self.file.sync_data()?;
        self.len += buf.len() as u64;
        self.since_snapshot += events.len();
        if epochs(&self.account)?.last().is_some_and(|newest| *newest > self.epoch) {
            return Err(StoreError::Fenced);
        }
        drop(cut);
        if let Some(state) = snapshot {
            let snapshot = Snapshot { epoch: self.epoch, offset: self.len, state: state.clone() };
            let bytes = serde_json::to_vec(&snapshot).map_err(|e| StoreError::Corrupt(e.to_string()))?;
            let tmp = self.account.join(format!("snap.{}.tmp", self.epoch));
            let mut out = File::create(&tmp)?;
            out.write_all(&bytes)?;
            out.sync_data()?;
            fs::rename(&tmp, self.account.join("snap"))?; // ponytail: no directory fsync; a power cut can lose the rename, not the log
            self.since_snapshot = 0;
        }
        Ok(())
    }
}

/// The directory beside the accounts where the hosts, as lease acceptors, log who holds each account.
pub const LEASES: &str = "leases";

// ponytail: opened per append rather than kept, so an account costs one descriptor, not two.
fn cut_lock(dir: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("cut.lock"))
}

/// Every account's history at one instant: no write is between its append and its check while this reads. A paused
/// writer makes it wait; owners never do.
pub fn cut(dir: &Path) -> Result<BTreeMap<String, History>, StoreError> {
    let lock = cut_lock(dir)?;
    lock.lock()?;
    let mut accounts = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && let Some(id) = entry.file_name().to_str()
            && id != LEASES
        {
            accounts.insert(id.to_owned(), history(&entry.path(), None)?);
        }
    }
    Ok(accounts)
}
