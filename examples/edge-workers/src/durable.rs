//! What objects store, on the control node: the [`Store`] actor keeps each object's keys and values in a file of
//! its own, so they outlive the runner that ran the object.
//!
//! An object runs under a lease, and the lease's epoch is the fence: its actor opens its storage with the epoch it
//! started under, all it sends carries it, and the store refuses an epoch older than the newest it has seen for the
//! object. An actor that lost its lease without knowing (a pause, a partition) can therefore not write over the
//! one that took the object after it. An object also has a version, the count of its writes, and a write names the
//! version its actor read: the store refuses one made on another, so a copy of what was cleared, or of what a lost
//! or restored directory no longer holds, is never written onto. A write is answered once it is on disk and synced.
//!
//! An object that stores nothing has no file, no fence and no place among a worker's `MAX_OBJECTS`: opening one
//! writes nothing, and its first write, from version 0, makes it.
//!
//! ponytail: every object's data is held in memory (`MAX_OBJECTS` a worker, `MAX_OBJECT_BYTES` each), and one
//! write is on disk before the next starts. Read from the files on demand, and group the syncs, when either is a
//! limit.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axon::files::{FileOp, Files};
use axon::{Actor, Context, MailboxConfig, MailboxLimits};
use axon_cluster::Reply;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::store;
use crate::wire::{ENTRY_COST, Hash, MAX_KEY, MAX_OBJECT_BYTES, MAX_VALUE, StoreAsk, StoreOp, StoreTold, answer};

/// Objects of one worker that store something, or did and were not cleared.
pub const MAX_OBJECTS: usize = 10_000;
/// A file is written anew, as what the object stores now, once it is this much and four times that.
const SLACK: usize = 64 << 10;

/// One object's keys and values, the values as JSON text. An object's actor works on a copy between writes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Data {
    entries: BTreeMap<String, String>,
    bytes: usize,
}

impl Data {
    pub fn new(entries: Vec<(String, String)>) -> Self {
        let bytes = entries.iter().map(|(key, value)| key.len() + value.len() + ENTRY_COST).sum();
        Self { entries: entries.into_iter().collect(), bytes }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.entries.get(key)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Refused past a limit, and the data is as it was.
    pub fn put(&mut self, key: &str, value: String) -> Result<(), String> {
        if key.is_empty() || key.len() > MAX_KEY || key.chars().any(char::is_control) {
            return Err(format!("a key is 1 to {MAX_KEY} bytes with no control character"));
        }
        if value.len() > MAX_VALUE {
            return Err(format!("a value over {MAX_VALUE} bytes as JSON"));
        }
        // JSON text has none. One would be six bytes in a frame, and an object within its limits past a frame's.
        if value.bytes().any(|byte| byte < 0x20) {
            return Err("a value is JSON text, with no control character".into());
        }
        let old = self.entries.get(key).map_or(0, |old| key.len() + old.len() + ENTRY_COST);
        let bytes = self.bytes - old + key.len() + value.len() + ENTRY_COST;
        if bytes > MAX_OBJECT_BYTES {
            return Err(format!("an object stores at most {MAX_OBJECT_BYTES} bytes"));
        }
        self.bytes = bytes;
        self.entries.insert(key.to_owned(), value);
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        let Some(old) = self.entries.remove(key) else { return false };
        self.bytes -= key.len() + old.len() + ENTRY_COST;
        true
    }

    /// In key order.
    pub fn list(&self, prefix: &str, limit: Option<usize>) -> Vec<(String, String)> {
        let from = self.entries.range(prefix.to_owned()..).take_while(|(key, _)| key.starts_with(prefix));
        from.take(limit.unwrap_or(usize::MAX)).map(|(key, value)| (key.clone(), value.clone())).collect()
    }

    /// All of `ops` in order, or none of them.
    pub fn apply(&mut self, ops: &[StoreOp]) -> Result<(), String> {
        let mut next = self.clone();
        for (key, value) in ops {
            match value {
                Some(value) => next.put(key, value.clone())?,
                None => drop(next.delete(key)),
            }
        }
        *self = next;
        Ok(())
    }
}

/// A file's records. The first is always a `Snapshot`, whole in one frame, so a file a crash tore at its start is
/// told from an object's.
#[derive(Debug, Serialize, Deserialize)]
enum Record {
    Snapshot { object: String, epoch: u64, version: u64, entries: Vec<(String, String)> },
    Epoch(u64),
    Write { epoch: u64, ops: Vec<StoreOp> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Stored {
    /// The newest lease epoch seen: the fence.
    epoch: u64,
    /// The writes it has had.
    version: u64,
    data: Data,
    /// Which of the object's files holds it, and that file's length.
    generation: u64,
    logged: usize,
}

/// A change checked against the state: what it leaves of the object, and the writes that make it durable.
#[derive(Debug)]
pub struct Step {
    object: String,
    /// `None`: the object is gone.
    next: Option<Stored>,
    ops: Vec<FileOp>,
}

#[derive(Debug, Default)]
pub struct State {
    objects: BTreeMap<String, Stored>,
}

fn file(object: &str, generation: u64) -> String {
    format!("obj.{}.{generation}", Hash::of(object.as_bytes()).as_str())
}

fn framed(record: &Record) -> Bytes {
    store::frame(&serde_json::to_vec(record).expect("a record is plain data"))
}

fn whole(object: &str, stored: &Stored) -> Bytes {
    let (object, entries) = (object.to_owned(), stored.data.list("", None));
    framed(&Record::Snapshot { object, epoch: stored.epoch, version: stored.version, entries })
}

impl State {
    /// What `ask` changes, `Ok(None)` when it changes nothing, or the refusal. Nothing is changed until
    /// [`State::commit`], after the step's writes are on disk.
    pub fn plan(&self, ask: &StoreAsk) -> Result<Option<Step>, StoreTold> {
        let (object, epoch, write) = match ask {
            StoreAsk::Open { object, epoch } => (object, *epoch, None),
            StoreAsk::Write { object, epoch, base, ops } => (object, *epoch, Some((*base, ops))),
            StoreAsk::Clear { object } => {
                // The file goes, and with it the object's place among its worker's. The actor that runs it holds
                // a version the store no longer has.
                let Some(stored) = self.objects.get(object) else { return Ok(None) };
                let ops = vec![FileOp::Remove(file(object, stored.generation))];
                return Ok(Some(Step { object: object.clone(), next: None, ops }));
            }
            StoreAsk::Read { .. } | StoreAsk::List { .. } => return Ok(None),
        };
        let Some(current) = self.objects.get(object) else {
            let ops = match write {
                Some((0, ops)) if !ops.is_empty() => ops,
                Some((base, _)) if base != 0 => return Err(StoreTold::Stale { version: 0 }),
                _ => return Ok(None),
            };
            let worker = format!("{}/", object.split('/').next().unwrap_or_default());
            let kept = self.objects.range(worker.clone()..).take_while(|(key, _)| key.starts_with(&worker));
            if kept.count() >= MAX_OBJECTS {
                let why = format!("the store keeps at most {MAX_OBJECTS} objects of a worker");
                return Err(StoreTold::Refused(why));
            }
            let mut next = Stored { epoch, version: 1, ..Stored::default() };
            next.data.apply(ops).map_err(StoreTold::Refused)?;
            let whole = whole(object, &next);
            next.logged = whole.len();
            let ops = vec![FileOp::Append(file(object, 0), whole)];
            return Ok(Some(Step { object: object.clone(), next: Some(next), ops }));
        };
        if epoch < current.epoch {
            return Err(StoreTold::Fenced { seen: current.epoch });
        }
        let mut next = current.clone();
        next.epoch = epoch;
        let record = match write {
            None if epoch == current.epoch => return Ok(None),
            None => Record::Epoch(epoch),
            Some((base, ops)) if base != current.version => {
                // The write before this one, from the same actor, already in what is stored: it is asked again
                // because its answer was lost.
                let last = !ops.is_empty() && base + 1 == current.version && epoch == current.epoch;
                let again = last && ops.iter().all(|(key, value)| current.data.get(key) == value.as_ref());
                return if again { Ok(None) } else { Err(StoreTold::Stale { version: current.version }) };
            }
            Some((_, ops)) if ops.is_empty() => return Ok(None),
            Some((_, ops)) => {
                next.data.apply(ops).map_err(StoreTold::Refused)?;
                next.version += 1;
                Record::Write { epoch, ops: ops.clone() }
            }
        };
        let appended = framed(&record);
        if next.logged + appended.len() > SLACK + 4 * next.data.bytes() {
            // The file anew, as what the object stores now. `Files::apply` syncs it before the old one is removed;
            // a crash in between leaves both, and the newer generation wins.
            let old = FileOp::Remove(file(object, next.generation));
            let whole = whole(object, &next);
            next.generation += 1;
            next.logged = whole.len();
            let ops = vec![FileOp::Append(file(object, next.generation), whole), old];
            return Ok(Some(Step { object: object.clone(), next: Some(next), ops }));
        }
        next.logged += appended.len();
        let ops = vec![FileOp::Append(file(object, next.generation), appended)];
        Ok(Some(Step { object: object.clone(), next: Some(next), ops }))
    }

    pub fn commit(&mut self, step: Step) {
        match step.next {
            Some(next) => drop(self.objects.insert(step.object, next)),
            None => drop(self.objects.remove(&step.object)),
        }
    }

    /// The answer to `ask` from the state as it is: after its step, if it had one.
    pub fn answer(&self, ask: &StoreAsk) -> StoreTold {
        let of = |object: &String| self.objects.get(object);
        match ask {
            StoreAsk::Open { object, .. } | StoreAsk::Read { object } => {
                let entries = of(object).map(|stored| stored.data.list("", None)).unwrap_or_default();
                let (epoch, version) = of(object).map_or((0, 0), |stored| (stored.epoch, stored.version));
                StoreTold::Snapshot { epoch, version, entries }
            }
            StoreAsk::Write { object, .. } | StoreAsk::Clear { object } => {
                StoreTold::Written { version: of(object).map_or(0, |stored| stored.version) }
            }
            StoreAsk::List { prefix, after, limit } => {
                let after = after.as_ref().filter(|after| *after >= prefix);
                let start = after.map_or(Bound::Included(prefix), Bound::Excluded);
                let from = self.objects.range::<String, _>((start, Bound::Unbounded));
                let stores = from.take_while(|(key, _)| key.starts_with(prefix)).filter(|(_, s)| s.data.bytes() > 0);
                StoreTold::Objects(stores.take(*limit as usize).map(|(key, _)| key.clone()).collect())
            }
        }
    }

    /// From the files: each object from the newest of its files that starts with a whole snapshot. The files to
    /// remove are the others.
    async fn load(files: &dyn Files) -> Result<(Self, Vec<FileOp>), String> {
        let mut state = Self::default();
        let mut tidy = Vec::new();
        let listed = files.list().await.map_err(|e| format!("listing the objects: {e}"))?;
        for (name, _) in listed.into_iter().filter(|(name, _)| name.starts_with("obj.")) {
            let data = files.read(&name).await.map_err(|e| format!("reading {name}: {e}"))?;
            let (records, good) = store::frames(&data);
            let mut records = records.into_iter().map(serde_json::from_slice::<Record>);
            let generation = name.rsplit('.').next().and_then(|generation| generation.parse().ok());
            let (Some(Ok(Record::Snapshot { object, epoch, version, entries })), Some(generation)) =
                (records.next(), generation)
            else {
                tidy.push(FileOp::Remove(name));
                continue;
            };
            let mut stored = Stored { epoch, version, data: Data::new(entries), generation, logged: good };
            for record in records {
                match record.map_err(|e| format!("{name} holds a record this build cannot read: {e}"))? {
                    Record::Epoch(epoch) => stored.epoch = epoch,
                    Record::Write { epoch, ops } => {
                        (stored.epoch, stored.version) = (epoch, stored.version + 1);
                        stored.data.apply(&ops).map_err(|e| format!("{name}: {e}"))?;
                    }
                    Record::Snapshot { .. } => return Err(format!("{name} holds a second snapshot")),
                }
            }
            match state.objects.get(&object) {
                Some(other) if other.generation > generation => tidy.push(FileOp::Remove(name)),
                other => {
                    tidy.extend(other.map(|other| FileOp::Remove(file(&object, other.generation))));
                    if good < data.len() {
                        // A torn tail: cut, so the next append follows a whole record.
                        tidy.push(FileOp::Truncate(name, good as u64));
                    }
                    state.objects.insert(object, stored);
                }
            }
        }
        Ok((state, tidy))
    }
}

pub enum Msg {
    Ask(StoreAsk, Option<Reply<StoreTold>>),
    /// The objects, the files no object is read from removed.
    Loaded(Result<Box<State>, String>),
    Written(Result<(), String>),
}

/// What the store did since it started.
#[derive(Debug, Default)]
pub struct Counters {
    /// Writes on disk and answered.
    pub written: AtomicU64,
    /// Opens and writes refused for an epoch older than the object's newest: an actor that had lost its lease.
    pub fenced: AtomicU64,
}

pub struct Store {
    files: Arc<dyn Files>,
    state: State,
    counters: Arc<Counters>,
    /// The change being written, and who asked for it. The mailbox is paused until it is on disk.
    writing: Option<(Step, StoreAsk, Option<Reply<StoreTold>>)>,
}

impl Store {
    pub fn new(files: Arc<dyn Files>) -> Self {
        Self { files, state: State::default(), counters: Arc::default(), writing: None }
    }

    pub fn counters(&self) -> Arc<Counters> {
        self.counters.clone()
    }

    /// A write is weighed with what it carries, so the writes that wait hold megabytes, not an object each.
    pub fn mailbox() -> MailboxConfig<Msg> {
        MailboxConfig::from(MailboxLimits::default()).with_weight(|msg: &Msg| {
            size_of::<Msg>()
                + match msg {
                    Msg::Ask(StoreAsk::Write { ops, .. }, _) => {
                        ops.iter().map(|(key, value)| key.len() + value.as_ref().map_or(0, String::len)).sum()
                    }
                    _ => 0,
                }
        })
    }
}

impl Actor for Store {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let files = self.files.clone();
        let load = async move {
            let (state, stale) = State::load(&*files).await?;
            // One that stayed would be taken for the object's after a clear.
            files.apply(stale).await.map_err(|e| format!("removing the objects' old files: {e}"))?;
            Ok(Box::new(state))
        };
        let job = cx.submit_future(async move { Msg::Loaded(load.await) }).map_err(|e| format!("{:?}", e.reason))?;
        cx.pause_until(&job).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => self.state = *loaded?,
            Msg::Ask(ask, reply) => {
                if reply.as_ref().is_some_and(Reply::is_abandoned) {
                    return Ok(());
                }
                let mut step = match self.state.plan(&ask) {
                    Ok(Some(step)) => step,
                    Ok(None) => {
                        answer(reply, self.state.answer(&ask));
                        return Ok(());
                    }
                    Err(refused) => {
                        if matches!(refused, StoreTold::Fenced { .. }) {
                            self.counters.fenced.fetch_add(1, Relaxed);
                        }
                        answer(reply, refused);
                        return Ok(());
                    }
                };
                // The mailbox is paused until the write is done.
                let (files, ops) = (self.files.clone(), std::mem::take(&mut step.ops));
                let write = async move { Msg::Written(files.apply(ops).await.map_err(|e| e.to_string())) };
                match cx.submit_future(write) {
                    Ok(job) => {
                        cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
                        self.writing = Some((step, ask, reply));
                    }
                    Err(_) => answer(reply, StoreTold::Refused("the store is busy".into())),
                }
            }
            Msg::Written(written) => {
                let Some((step, ask, reply)) = self.writing.take() else { return Ok(()) };
                match written {
                    Ok(()) => {
                        if matches!(&ask, StoreAsk::Write { .. }) {
                            self.counters.written.fetch_add(1, Relaxed);
                        }
                        self.state.commit(step);
                        answer(reply, self.state.answer(&ask));
                    }
                    Err(e) => answer(reply, StoreTold::Refused(format!("the write failed: {e}"))),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use axon::files::MemoryFiles;

    use super::*;
    use crate::wire::StoreApi;
    use axon_cluster::ActorProtocol;

    const KEY: &str = "rooms/Counter/a";

    /// Plans, writes and commits, as the actor does.
    async fn ask(state: &mut State, files: &MemoryFiles, ask: StoreAsk) -> StoreTold {
        match state.plan(&ask) {
            Ok(Some(mut step)) => {
                files.apply(std::mem::take(&mut step.ops)).await.unwrap();
                state.commit(step);
                state.answer(&ask)
            }
            Ok(None) => state.answer(&ask),
            Err(refused) => refused,
        }
    }

    fn put_in(object: &str, epoch: u64, base: u64, key: &str, value: &str) -> StoreAsk {
        StoreAsk::Write { object: object.into(), epoch, base, ops: vec![(key.into(), Some(value.into()))] }
    }

    fn put(epoch: u64, base: u64, key: &str, value: &str) -> StoreAsk {
        put_in(KEY, epoch, base, key, value)
    }

    fn open(epoch: u64) -> StoreAsk {
        StoreAsk::Open { object: KEY.into(), epoch }
    }

    /// A request that wrote nothing: is the actor the object's still, and its copy what is stored?
    fn check(epoch: u64, base: u64) -> StoreAsk {
        StoreAsk::Write { object: KEY.into(), epoch, base, ops: Vec::new() }
    }

    fn entries(told: StoreTold) -> Vec<(String, String)> {
        match told {
            StoreTold::Snapshot { entries, .. } => entries,
            other => panic!("a snapshot, not {other:?}"),
        }
    }

    fn snapshot(epoch: u64, version: u64, entries: &[(&str, &str)]) -> StoreTold {
        let entries = entries.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect();
        StoreTold::Snapshot { epoch, version, entries }
    }

    const WRITTEN: fn(u64) -> StoreTold = |version| StoreTold::Written { version };

    #[tokio::test]
    async fn a_write_of_an_older_epoch_is_refused_once_a_newer_one_opened() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        assert_eq!(ask(&mut state, &files, put(5, 0, "n", "1")).await, WRITTEN(1));
        // The object moved: its new actor opens under a later lease and sees the write.
        assert_eq!(ask(&mut state, &files, open(9)).await, snapshot(9, 1, &[("n", "1")]));
        // The actor it moved from did not notice. It writes, and it answers a request that only read: refused,
        // and nothing changes.
        assert_eq!(ask(&mut state, &files, put(5, 1, "n", "100")).await, StoreTold::Fenced { seen: 9 });
        assert_eq!(ask(&mut state, &files, check(5, 1)).await, StoreTold::Fenced { seen: 9 });
        assert_eq!(ask(&mut state, &files, open(5)).await, StoreTold::Fenced { seen: 9 });
        assert_eq!(ask(&mut state, &files, put(9, 1, "n", "2")).await, WRITTEN(2));
        let read = ask(&mut state, &files, StoreAsk::Read { object: KEY.into() }).await;
        assert_eq!(read, snapshot(9, 2, &[("n", "2")]));
        // The same epoch again, as after a restart under the same lease, is the owner still.
        assert_eq!(entries(ask(&mut state, &files, open(9)).await).len(), 1);
    }

    #[tokio::test]
    async fn a_write_onto_another_version_than_its_actor_read_is_refused_and_the_same_write_again_is_not() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        assert_eq!(ask(&mut state, &files, put(5, 0, "n", "1")).await, WRITTEN(1));
        assert_eq!(ask(&mut state, &files, put(5, 1, "n", "2")).await, WRITTEN(2));
        let records = files.read(&file(KEY, 0)).await.unwrap().len();
        // Its answer was lost and it is asked again: stored already, and not written twice.
        assert_eq!(ask(&mut state, &files, put(5, 1, "n", "2")).await, WRITTEN(2));
        assert_eq!(files.read(&file(KEY, 0)).await.unwrap().len(), records);
        // A copy that is not what is stored, whichever way: nothing is written onto it, and no read answered.
        for base in [0, 1, 3] {
            let stale = StoreTold::Stale { version: 2 };
            assert_eq!(ask(&mut state, &files, put(5, base, "n", "9")).await, stale, "a write on {base}");
            assert_eq!(ask(&mut state, &files, check(5, base)).await, stale, "a read on {base}");
        }
        assert_eq!(ask(&mut state, &files, check(5, 2)).await, WRITTEN(2));
        assert_eq!(files.read(&file(KEY, 0)).await.unwrap().len(), records, "a request that only read writes nothing");

        // The directory is lost under a running actor: its copy is of a version the store has not.
        let (mut lost, files) = (State::default(), MemoryFiles::default());
        assert_eq!(ask(&mut lost, &files, put(5, 2, "m", "1")).await, StoreTold::Stale { version: 0 });
        assert_eq!(ask(&mut lost, &files, check(5, 2)).await, StoreTold::Stale { version: 0 });
        assert_eq!(files.list().await.unwrap(), []);
    }

    #[tokio::test]
    async fn the_fence_and_the_data_survive_a_restart() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        ask(&mut state, &files, put(5, 0, "a", "1")).await;
        ask(&mut state, &files, put(5, 1, "b", "[2]")).await;
        let delete = StoreAsk::Write { object: KEY.into(), epoch: 5, base: 2, ops: vec![("a".into(), None)] };
        assert_eq!(ask(&mut state, &files, delete).await, WRITTEN(3));
        ask(&mut state, &files, open(7)).await;
        let (mut loaded, stale) = State::load(&files).await.unwrap();
        assert_eq!((&loaded.objects, stale.len()), (&state.objects, 0));
        assert_eq!(ask(&mut loaded, &files, put(5, 3, "b", "0")).await, StoreTold::Fenced { seen: 7 });
        assert_eq!(ask(&mut loaded, &files, open(7)).await, snapshot(7, 3, &[("b", "[2]")]));
    }

    #[tokio::test]
    async fn a_write_past_a_limit_changes_nothing() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        let refused = |told| matches!(told, StoreTold::Refused(_));
        assert!(refused(ask(&mut state, &files, put(1, 0, &"k".repeat(MAX_KEY + 1), "1")).await));
        assert!(refused(ask(&mut state, &files, put(1, 0, "k", &"1".repeat(MAX_VALUE + 1))).await));
        assert!(refused(ask(&mut state, &files, put(1, 0, "k", "\"\u{1}\"")).await), "not JSON text");
        // All of a request's writes or none: the second put is over the object's total.
        let value = "1".repeat(MAX_VALUE);
        let fill: Vec<StoreOp> = (0..4).map(|i| (format!("k{i}"), Some(value.clone()))).collect();
        let write = |ops| StoreAsk::Write { object: KEY.into(), epoch: 1, base: 0, ops };
        assert!(refused(ask(&mut state, &files, write(fill)).await));
        assert_eq!(ask(&mut state, &files, open(1)).await, snapshot(0, 0, &[]));
        let fits: Vec<StoreOp> = (0..3).map(|i| (format!("k{i}"), Some(value.clone()))).collect();
        assert_eq!(ask(&mut state, &files, write(fits)).await, WRITTEN(1));
    }

    #[tokio::test]
    async fn an_object_at_its_limits_fits_a_frame_however_its_text_is_escaped() {
        // The worst text there is without a control character: every byte a quote, two bytes each in a frame.
        let (mut state, files) = (State::default(), MemoryFiles::default());
        let value = "\"".repeat(MAX_VALUE);
        for (base, key) in ["a", "b", "c", "d"].into_iter().enumerate() {
            let told = ask(&mut state, &files, put(1, base as u64, &key.repeat(MAX_KEY), &value)).await;
            assert_eq!(told == WRITTEN(base as u64 + 1), base < 3, "{key}: 256 KiB an object");
        }
        let whole = StoreApi::encode_reply(ask(&mut state, &files, open(1)).await).unwrap();
        let frame = axon_net::Limits::default().max_frame;
        assert!(whole.len() > 2 * 3 * MAX_VALUE && whole.len() < frame / 2, "{} of {frame}", whole.len());
    }

    #[tokio::test]
    async fn an_object_that_stores_nothing_takes_no_place_and_a_worker_s_objects_are_its_own_limit() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        // Requests that only read, to ids nobody wrote to: opened, checked, and nothing is kept of them.
        for id in 0..2 * MAX_OBJECTS {
            let object = format!("rooms/Counter/{id}");
            let opened = ask(&mut state, &files, StoreAsk::Open { object: object.clone(), epoch: 5 }).await;
            assert_eq!(opened, snapshot(0, 0, &[]));
            let read = StoreAsk::Write { object, epoch: 5, base: 0, ops: Vec::new() };
            assert_eq!(ask(&mut state, &files, read).await, WRITTEN(0));
        }
        assert_eq!((state.objects.len(), files.list().await.unwrap().len()), (0, 0));

        for id in 0..MAX_OBJECTS {
            let told = ask(&mut state, &files, put_in(&format!("rooms/Counter/{id}"), 5, 0, "n", "1")).await;
            assert_eq!(told, WRITTEN(1), "object {id}");
        }
        let full = ask(&mut state, &files, put_in("rooms/Counter/one-more", 5, 0, "n", "1")).await;
        assert!(matches!(&full, StoreTold::Refused(why) if why.contains("objects of a worker")), "{full:?}");
        // Another worker's objects are not held up by it, and one cleared makes room for one more.
        assert_eq!(ask(&mut state, &files, put_in("room/Counter/a", 5, 0, "n", "1")).await, WRITTEN(1));
        assert_eq!(ask(&mut state, &files, put_in("rooms2/Counter/a", 5, 0, "n", "1")).await, WRITTEN(1));
        let clear = StoreAsk::Clear { object: "rooms/Counter/7".into() };
        assert_eq!(ask(&mut state, &files, clear).await, WRITTEN(0));
        assert_eq!(ask(&mut state, &files, put_in("rooms/Counter/one-more", 5, 0, "n", "1")).await, WRITTEN(1));
        assert_eq!(files.list().await.unwrap().len(), MAX_OBJECTS + 2);
    }

    #[tokio::test]
    async fn a_clear_leaves_nothing_and_the_actor_that_holds_a_copy_is_told_so() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        for n in 0..2000 {
            assert_eq!(ask(&mut state, &files, put(5, n, "n", &n.to_string())).await, WRITTEN(n + 1));
        }
        let listed = files.list().await.unwrap();
        assert_eq!(listed.len(), 1, "one file an object: {listed:?}");
        assert!(listed[0].1 < 2 * SLACK as u64 && !listed[0].0.ends_with(".0"), "rewritten on the way: {listed:?}");
        let (loaded, _) = State::load(&files).await.unwrap();
        assert_eq!(loaded.objects, state.objects);
        assert_eq!((loaded.objects[KEY].version, loaded.objects[KEY].data.get("n").unwrap().as_str()), (2000, "1999"));

        let listing =
            |after: Option<&str>| StoreAsk::List { prefix: "rooms/".into(), after: after.map(Into::into), limit: 1 };
        ask(&mut state, &files, put_in("rooms/Counter/b", 5, 0, "n", "1")).await;
        ask(&mut state, &files, put_in("rooms2/Counter/a", 5, 0, "n", "1")).await;
        assert_eq!(ask(&mut state, &files, listing(None)).await, StoreTold::Objects(vec![KEY.into()]));
        let next = ask(&mut state, &files, listing(Some(KEY))).await;
        assert_eq!(next, StoreTold::Objects(vec!["rooms/Counter/b".into()]));
        assert_eq!(ask(&mut state, &files, listing(Some("rooms/Counter/b"))).await, StoreTold::Objects(vec![]));

        assert_eq!(ask(&mut state, &files, StoreAsk::Clear { object: KEY.into() }).await, WRITTEN(0));
        assert_eq!(ask(&mut state, &files, listing(None)).await, StoreTold::Objects(vec!["rooms/Counter/b".into()]));
        assert_eq!(files.list().await.unwrap().len(), 2, "its file is gone");
        // The actor that ran it holds what was cleared: neither its write nor its read is answered from that.
        assert_eq!(ask(&mut state, &files, put(5, 2000, "n", "2000")).await, StoreTold::Stale { version: 0 });
        assert_eq!(ask(&mut state, &files, check(5, 2000)).await, StoreTold::Stale { version: 0 });
        assert_eq!(ask(&mut state, &files, open(5)).await, snapshot(0, 0, &[]));
        let (loaded, stale) = State::load(&files).await.unwrap();
        assert!(!loaded.objects.contains_key(KEY) && stale.is_empty(), "{stale:?}");
    }

    #[tokio::test]
    async fn a_file_is_written_anew_before_the_old_one_is_removed() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        let mut n = 0;
        let step = loop {
            let step = state.plan(&put(5, n, "n", &n.to_string())).unwrap().expect("a write");
            if step.ops.len() > 1 {
                break step;
            }
            assert!(matches!(&step.ops[..], [FileOp::Append(..)]), "{:?}", step.ops);
            files.apply(step.ops.clone()).await.unwrap();
            state.commit(step);
            n += 1;
        };
        // One apply, which syncs the new file before the removal takes effect.
        let (new, old) = (file(KEY, 1), file(KEY, 0));
        assert!(matches!(&step.ops[..], [FileOp::Append(a, _), FileOp::Remove(r)] if *a == new && *r == old));
        // A crash after the new file and before the removal: both are there, and the newer one is the object.
        files.apply(step.ops[..1].to_vec()).await.unwrap();
        let (loaded, stale) = State::load(&files).await.unwrap();
        assert_eq!(loaded.objects[KEY].data.get("n"), Some(&n.to_string()));
        assert_eq!((loaded.objects[KEY].version, loaded.objects[KEY].generation), (n + 1, 1));
        assert_eq!(stale, [FileOp::Remove(file(KEY, 0))]);
    }

    #[tokio::test]
    async fn a_file_torn_at_its_start_is_not_an_object_and_the_older_generation_stands() {
        let (mut state, files) = (State::default(), MemoryFiles::default());
        ask(&mut state, &files, put(5, 0, "n", "1")).await;
        // A crash while the next generation was written: half a snapshot beside the whole file.
        let whole = framed(&Record::Snapshot { object: KEY.into(), epoch: 5, version: 1, entries: vec![] });
        files.apply(vec![FileOp::Append(file(KEY, 1), whole.slice(..whole.len() / 2))]).await.unwrap();
        // And a crash mid-append: a torn record at the end of the whole one.
        files.apply(vec![FileOp::Append(file(KEY, 0), Bytes::from_static(b"\0\0\0\x09torn"))]).await.unwrap();
        let (loaded, stale) = State::load(&files).await.unwrap();
        assert_eq!(loaded.objects, state.objects);
        assert!(stale.contains(&FileOp::Remove(file(KEY, 1))), "{stale:?}");
        assert!(stale.contains(&FileOp::Truncate(file(KEY, 0), state.objects[KEY].logged as u64)), "{stale:?}");
    }
}
