//! The Log actor on files that hold each write back until the test lets it land: no answer leaves before its sync
//! (I4), one write takes what many shards sent meanwhile, a failed or panicking write is written again, and a
//! restart reads back the promises and the maps, from the WAL and from snapshots once the WAL is trimmed.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use axon::files::{FileFuture, FileOp, Files, LogConfig, MemoryFiles};
use axon::{ActorHandle, ActorSystem};
use bytes::Bytes;
use kv::Stats;
use kv::log::{LogActor, LogMsg, LogSettings, LogStatus, To};
use kv::node::log_mailbox;
use kv::repl::{Ballot, Entry, FromLog, Map, Op, Pos, Snapshot, ToLog};
use tokio::sync::Semaphore;

/// Files whose writes wait for a permit, and may fail or panic.
#[derive(Clone)]
struct Held {
    inner: MemoryFiles,
    gate: Arc<Semaphore>,
    applies: Arc<AtomicUsize>,
    fail: Arc<AtomicUsize>,
    panic: Arc<AtomicUsize>,
}

impl Held {
    fn new(permits: usize) -> Self {
        Self {
            inner: MemoryFiles::default(),
            gate: Arc::new(Semaphore::new(permits)),
            applies: Arc::default(),
            fail: Arc::default(),
            panic: Arc::default(),
        }
    }

    fn open() -> Self {
        Self::new(Semaphore::MAX_PERMITS / 2)
    }

    fn shut() -> Self {
        Self::new(0)
    }

    fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }
}

fn take(n: &AtomicUsize) -> bool {
    let mut left = n.load(SeqCst);
    while left > 0 {
        match n.compare_exchange(left, left - 1, SeqCst, SeqCst) {
            Ok(_) => return true,
            Err(now) => left = now,
        }
    }
    false
}

impl Files for Held {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.inner.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.inner.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let held = self.clone();
        Box::pin(async move {
            held.gate.acquire().await.expect("never closed").forget();
            held.applies.fetch_add(1, SeqCst);
            if take(&held.fail) {
                return Err(io::Error::other("a failed write"));
            }
            assert!(!take(&held.panic), "a write that panics");
            held.inner.apply(ops).await
        })
    }
}

fn ballot(round: u64) -> Ballot {
    Ballot { round, node: 0, boot: 1, start: round }
}

fn set(ballot: Ballot, k: &str, v: &str) -> Entry {
    Entry { ballot, op: Op::Set(vec![(Bytes::copy_from_slice(k.as_bytes()), Bytes::copy_from_slice(v.as_bytes()))]) }
}

struct Node {
    system: ActorSystem,
    log: ActorHandle<LogMsg>,
}

async fn start<S: Files + Clone + 'static>(wal: &Held, snaps: &S, settings: LogSettings) -> Result<Node, String> {
    let system = ActorSystem::builder().build().expect("a system");
    let actor = LogActor::new(Arc::new(wal.clone()), Arc::new(snaps.clone()), settings, Arc::new(Stats::default()));
    let log = system.spawn(actor).mailbox(log_mailbox()).start().map_err(|e| format!("{e:?}"))?;
    log.ready().await.map_err(|e| format!("{e:?}"))?;
    Ok(Node { system, log })
}

async fn call(log: &ActorHandle<LogMsg>, shard: u16, msg: ToLog) -> FromLog {
    log.ask(|r| LogMsg::Call(shard, msg, To::Local(r))).await.expect("the Log answers")
}

async fn status(log: &ActorHandle<LogMsg>) -> LogStatus {
    log.ask(LogMsg::Status).await.expect("the Log answers")
}

fn open(ballot: Ballot) -> ToLog {
    ToLog::Open { ballot, state: true }
}

fn append(ballot: Ballot, prev: Pos, entries: Vec<Entry>, committed: u64) -> ToLog {
    ToLog::Append { ballot, seq: 1, prev, entries, committed }
}

/// I4: the promise is answered only once the write holding it has landed, and so is an Append.
#[tokio::test]
async fn nothing_is_answered_before_its_write_lands() {
    let (wal, snaps) = (Held::shut(), MemoryFiles::new());
    let node = start(&wal, &snaps, LogSettings::default()).await.unwrap();
    let log = node.log.clone();
    let opening = tokio::spawn(async move { call(&log, 3, open(ballot(1))).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!opening.is_finished(), "the promise was answered before its write landed");
    assert_eq!(wal.applies.load(SeqCst), 0);
    wal.release(1);
    let FromLog::Opened { opened, .. } = opening.await.unwrap() else { panic!("opened") };
    assert_eq!(opened.last, Pos::default());

    let log = node.log.clone();
    let b = ballot(1);
    let appending =
        tokio::spawn(async move { call(&log, 3, append(b, Pos::default(), vec![set(b, "a", "1")], 0)).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!appending.is_finished(), "an entry was answered before its write landed");
    wal.release(1);
    assert_eq!(appending.await.unwrap(), FromLog::Appended { ballot: b, seq: 1, matched: 1 });
    // A shard with nothing unwritten is answered at once, though another's write is held.
    let log = node.log.clone();
    let other = tokio::spawn(async move { call(&log, 4, open(ballot(1))).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!other.is_finished());
    let empty = call(&node.log, 3, append(b, Pos { ballot: b, index: 1 }, vec![], 0)).await;
    assert_eq!(empty, FromLog::Appended { ballot: b, seq: 1, matched: 1 });
    wal.release(10);
    other.await.unwrap();
    node.system.shutdown().await.unwrap();
}

/// While one write is held, 64 shards open and append: the next write takes all of them.
#[tokio::test]
async fn one_write_takes_what_every_shard_sent_while_the_last_was_out() {
    let (wal, snaps) = (Held::shut(), MemoryFiles::new());
    let node = start(&wal, &snaps, LogSettings::default()).await.unwrap();
    let log = node.log.clone();
    let first = tokio::spawn(async move { call(&log, 0, open(ballot(1))).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut shards = Vec::new();
    for s in 1..=64u16 {
        let log = node.log.clone();
        shards.push(tokio::spawn(async move {
            let b = ballot(1);
            call(&log, s, open(b)).await;
            let entries = (0..4).map(|i| set(b, &format!("k{i}"), "v")).collect();
            call(&log, s, append(b, Pos::default(), entries, 0)).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    wal.release(1000);
    first.await.unwrap();
    for shard in shards {
        assert!(matches!(shard.await.unwrap(), FromLog::Appended { matched: 4, .. }));
    }
    let st = status(&node.log).await;
    // The first promise, then the 64 promises, then the 64 Appends: three writes, maybe four.
    assert!(wal.applies.load(SeqCst) <= 4, "{} writes for 129 records", wal.applies.load(SeqCst));
    assert_eq!(st.entries, 256);
    assert!(st.entries > st.wal.writes, "entries per write above 1: {st:?}");
    node.system.shutdown().await.unwrap();
}

/// A write that fails, or panics, is written again after a rest, and its answer waits for it.
#[tokio::test]
async fn a_failed_or_panicking_write_is_written_again() {
    let (wal, snaps) = (Held::open(), MemoryFiles::new());
    let node = start(&wal, &snaps, LogSettings::default()).await.unwrap();
    wal.fail.store(3, SeqCst);
    wal.panic.store(2, SeqCst);
    let answer = call(&node.log, 9, open(ballot(5))).await;
    assert!(matches!(answer, FromLog::Opened { .. }));
    let st = status(&node.log).await;
    assert_eq!(st.wal.failed_writes, 5, "{st:?}");
    assert!(!st.failing);
    node.system.shutdown().await.unwrap();

    let node = start(&wal, &snaps, LogSettings::default()).await.unwrap();
    let refused = call(&node.log, 9, open(ballot(4))).await;
    assert_eq!(refused, FromLog::Refused { ballot: ballot(4), promised: ballot(5) });
    node.system.shutdown().await.unwrap();
}

/// Small segments and a small target: shards are snapshotted, the WAL trimmed and its old segments removed, and a
/// restart reads back each shard's promise and map from its snapshot and the records after it.
#[tokio::test]
async fn snapshots_hold_the_promises_and_maps_once_the_wal_is_trimmed() {
    let (wal, snaps) = (Held::open(), MemoryFiles::new());
    let settings = small();
    let node = start(&wal, &snaps, settings.clone()).await.unwrap();
    let shards = 16u16;
    let mut last = vec![Pos::default(); usize::from(shards)];
    for s in 0..shards {
        call(&node.log, s, open(ballot(2))).await;
    }
    let value = "x".repeat(900);
    for round in 0..200u64 {
        for s in 0..shards {
            let b = ballot(2);
            let prev = last[usize::from(s)];
            let entries = vec![set(b, &format!("k{}", round % 50), &format!("{round}-{value}"))];
            let FromLog::Appended { matched, .. } = call(&node.log, s, append(b, prev, entries, prev.index)).await
            else {
                panic!("appended")
            };
            last[usize::from(s)] = Pos { ballot: b, index: matched };
        }
    }
    // Shard 0 opens higher once more, and writes nothing after: its promise must outlive the records.
    call(&node.log, 0, open(ballot(7))).await;
    for s in 1..shards {
        let prev = last[usize::from(s)];
        call(&node.log, s, append(ballot(2), prev, vec![], prev.index)).await;
    }
    // A commit alone is answered at once, before its record is written: wait for it, or a shutdown may drop it.
    let mut st = status(&node.log).await;
    for _ in 0..500 {
        if st.wal.unwritten_bytes == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        st = status(&node.log).await;
    }
    assert!(st.snaps > 0, "{st:?}");
    // Three or four segments of 128 KiB and the snapshots' share: 3.2 MiB of records were written.
    assert!(st.wal.bytes <= 1 << 20, "the WAL stays bounded: {st:?}");
    node.system.shutdown().await.unwrap();

    let node = start(&wal, &snaps, settings).await.unwrap();
    let refused = call(&node.log, 0, open(ballot(6))).await;
    assert_eq!(refused, FromLog::Refused { ballot: ballot(6), promised: ballot(7) });
    for s in 1..shards {
        let FromLog::Opened { opened, .. } = call(&node.log, s, open(ballot(8))).await else { panic!("opened") };
        let (map, tail) = opened.state.expect("the state");
        assert!(tail.is_empty());
        assert_eq!(map.len(), 50, "shard {s}");
        assert_eq!(&map[&Bytes::from_static(b"k49")][..4], b"199-", "shard {s}");
        assert_eq!(opened.last, last[usize::from(s)]);
    }
    node.system.shutdown().await.unwrap();
}

/// The replicas drop each committed entry past `keep` as the commit comes, not at the next snapshot: there a node's
/// Log dropped every entry since the last one, all its shards' within a second, and held its writes behind that. The
/// `keep` they hold is what a new owner whose own Log is a few commits behind fetches, not the shard's whole map.
#[tokio::test]
async fn committed_entries_leave_memory_as_they_commit() {
    let (wal, snaps) = (Held::open(), MemoryFiles::new());
    let settings = LogSettings { wal_floor: u64::MAX, ..small() };
    let keep = settings.keep;
    let node = start(&wal, &snaps, settings).await.unwrap();
    assert!(keep > 0 && keep < 100, "{keep}");
    let b = ballot(1);
    let mut last = [Pos::default(); 2];
    for s in 0..2u16 {
        call(&node.log, s, open(b)).await;
    }
    for i in 0..100 {
        for (s, prev) in (0..).zip(&mut last) {
            let entries = vec![set(b, &format!("k{}", i % 10), "v")];
            let answer = call(&node.log, s, append(b, *prev, entries, prev.index)).await;
            let FromLog::Appended { matched, .. } = answer else { panic!("appended") };
            *prev = Pos { ballot: b, index: matched };
        }
    }
    let st = status(&node.log).await;
    assert_eq!(st.snaps, 0, "{st:?}");
    // Each shard's last entry, not yet committed, and the committed ones it keeps.
    assert_eq!(st.in_memory, 2 * (1 + keep), "{st:?}");

    // An owner fetches from its own Log's committed index on: up to `keep` behind gets entries, one more the map.
    let committed = last[0].index - 1;
    for (behind, map) in [(keep, false), (keep + 1, true)] {
        let from = committed - behind + 1;
        let FromLog::Fetched { snap, entries, .. } = call(&node.log, 0, ToLog::Fetch { ballot: b, from }).await else {
            panic!("fetched")
        };
        assert_eq!(snap.is_some(), map, "{behind} behind");
        assert_eq!(entries.len() as u64, if map { 1 } else { behind + 1 }, "{behind} behind");
    }
    node.system.shutdown().await.unwrap();
}

/// The entries kept are also bounded in bytes: one larger than `keep_bytes` leaves memory as it commits, with those
/// before it, and an owner behind it gets the map. With large values, 64 entries per shard were GiBs.
#[tokio::test]
async fn committed_entries_past_the_byte_budget_leave_memory() {
    let (wal, snaps) = (Held::open(), MemoryFiles::new());
    let settings = LogSettings { wal_floor: u64::MAX, keep_bytes: 4 << 10, ..small() };
    let node = start(&wal, &snaps, settings).await.unwrap();
    let (b, small_value, large_value) = (ballot(1), "v".repeat(1000), "v".repeat(5 << 10));
    call(&node.log, 0, open(b)).await;
    let mut prev = Pos::default();
    let mut write = async |value: &str| {
        let answer = call(&node.log, 0, append(b, prev, vec![set(b, "k", value)], prev.index)).await;
        let FromLog::Appended { matched, .. } = answer else { panic!("appended") };
        prev = Pos { ballot: b, index: matched };
    };
    for _ in 0..10 {
        write(&small_value).await;
    }
    // Four of 1,001 bytes fit in 4 KiB, and the last is not committed.
    let st = status(&node.log).await;
    assert_eq!((st.in_memory, st.in_memory_bytes), (5, 5 * 1001), "{st:?}");
    write(&large_value).await;
    write(&large_value).await;
    // The first large one is committed: it does not fit, so it goes with those before it.
    let st = status(&node.log).await;
    assert_eq!((st.in_memory, st.in_memory_bytes), (1, 1 + (5 << 10)), "{st:?}");
    let committed = prev.index - 1;
    let FromLog::Fetched { snap, .. } = call(&node.log, 0, ToLog::Fetch { ballot: b, from: committed }).await else {
        panic!("fetched")
    };
    assert!(snap.is_some(), "one commit behind the large entry gets the map");
    node.system.shutdown().await.unwrap();
}

/// An Install that comes while a snapshot of its shard taken before it is being written is answered only once a
/// snapshot that holds it lands: the one before does not, and a restart then would not find the installed map.
#[tokio::test]
async fn an_install_waits_for_a_snapshot_that_holds_it() {
    let (wal, snaps) = (Held::open(), Held::shut());
    let node = start(&wal, &snaps, small()).await.unwrap();
    let b = ballot(2);
    call(&node.log, 0, open(b)).await;
    let value = "x".repeat(8 << 10);
    let mut prev = Pos::default();
    // Past the WAL floor of 512 KiB in two segments or more: shard 0's snapshot goes out, and is held.
    for i in 0..100 {
        let entries = vec![set(b, &format!("k{}", i % 10), &value)];
        let FromLog::Appended { matched, .. } = call(&node.log, 0, append(b, prev, entries, prev.index)).await else {
            panic!("appended")
        };
        prev = Pos { ballot: b, index: matched };
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(status(&node.log).await.snaps, 0, "the snapshot is held");
    let map: Map = [(Bytes::from_static(b"installed"), Bytes::from_static(b"yes"))].into_iter().collect();
    let pos = Pos { ballot: b, index: prev.index + 100 };
    let install = ToLog::Install { ballot: b, seq: 9, snap: Snapshot { pos, map: map.clone() } };
    let log = node.log.clone();
    let installing = tokio::spawn(async move { call(&log, 0, install).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    snaps.release(1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(status(&node.log).await.snaps, 1, "the snapshot taken before the Install landed");
    assert!(!installing.is_finished(), "the Install was answered from a snapshot that does not hold it");
    snaps.release(1);
    assert_eq!(installing.await.unwrap(), FromLog::Appended { ballot: b, seq: 9, matched: pos.index });
    node.system.shutdown().await.unwrap();

    let node = start(&wal, &snaps, small()).await.unwrap();
    let FromLog::Opened { opened, .. } = call(&node.log, 0, open(ballot(3))).await else { panic!("opened") };
    assert_eq!(opened.state.expect("the state").0, map);
    assert_eq!(opened.last, pos);
    node.system.shutdown().await.unwrap();
}

fn small() -> LogSettings {
    LogSettings {
        wal: LogConfig {
            segment_bytes: 128 << 10,
            max_record: 32 << 10,
            max_write_bytes: 128 << 10,
            max_unwritten_bytes: 1 << 20,
            ..kv::log::wal_config()
        },
        wal_floor: 512 << 10,
        ..LogSettings::default()
    }
}

/// A record that does not read back in a segment with one after it is not a torn tail: the Log does not start, and a
/// node does not register its shards. (In the last segment, axon cuts it and what follows as a torn write.)
#[tokio::test]
async fn a_corrupt_wal_stops_the_start() {
    let (wal, snaps) = (Held::open(), MemoryFiles::new());
    let settings = LogSettings { wal_floor: u64::MAX, ..small() };
    let node = start(&wal, &snaps, settings.clone()).await.unwrap();
    let b = ballot(1);
    call(&node.log, 0, open(b)).await;
    let value = "x".repeat(8 << 10);
    let mut prev = Pos::default();
    for i in 0..40 {
        let entries = vec![set(b, &format!("k{i}"), &value)];
        let FromLog::Appended { matched, .. } = call(&node.log, 0, append(b, prev, entries, prev.index)).await else {
            panic!("appended")
        };
        prev = Pos { ballot: b, index: matched };
    }
    node.system.shutdown().await.unwrap();
    let mut files = wal.inner.list().await.unwrap();
    files.sort();
    assert!(files.len() >= 2, "{files:?}");
    let flip = FileOp::Write(files[0].0.clone(), 64 + 40, Bytes::from_static(&[0xff]));
    wal.inner.apply(vec![flip]).await.unwrap();
    let err = start(&wal, &snaps, settings).await.err().expect("the start fails");
    assert!(err.contains("does not read back"), "{err}");
}
