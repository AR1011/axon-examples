//! The `doc` keyed actor: one per open document, on its home. It loads the document from the home's journal before
//! any call, applies each call through [`DocCore`], writes what the turn changed in one write, and only once that is
//! durable publishes it on the document's stream and answers. Apply, write, publish, answer.
//!
//! A publish refused after the write fails the actor (I14): its restart publishes the state, which holds the edit,
//! as the new generation's `base`, and the caller's retry is answered `saved` once its empty diff is durable.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axon::files::{Entry, LogError};
use axon::{Actor, ActorHandle, Backoff, Context, RequestError, RestartPolicy, StopReason};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterError, KeyedRef, NodeChange, Placement, Rejection, Reply, SpawnRefusal,
    WeakCluster,
};
use axon_stream::{CompactLimits, DeliveryPolicy, StreamError, StreamKey, StreamNode};
use bytes::Bytes;

use crate::catalog;
use crate::core::acl::{Acl, Action, Principal};
use crate::core::catalog::Event;
use crate::core::doc::{DocCore, Edit, STREAM_BYTES, STREAM_KEYS};
use crate::core::presence::Presence;
use crate::core::versions::{self, Version};
use crate::journal::{self, ToJournal, job_error};
use crate::keeper;
use crate::wire::{DocAnswer, DocP, DocReq, Refusal};

pub const KIND: &str = "doc";
/// The catalog hears a document's title and size at most this often while it is edited: the last edits of a burst
/// when the time is up, and at the stop.
const TOUCH_EVERY: Duration = Duration::from_secs(10);

/// What the factory answers for an id its home never made, or purged.
pub const UNKNOWN: &str = "unknown document";

/// A call refused because the id is not a document of its home: asking again cannot change it.
pub fn unknown(e: &ClusterError) -> bool {
    matches!(e, ClusterError::Spawn(SpawnRefusal::Failed, why) if why.contains(UNKNOWN))
}

/// The home a document id names.
pub fn home_of(id: &str) -> &str {
    id.split_once('-').map_or("", |(home, _)| home)
}

pub fn namespace(home: &str) -> String {
    format!("doc.{home}")
}

pub fn stream_key(id: &str) -> StreamKey {
    StreamKey::new(namespace(home_of(id)), id)
}

/// The namespace of the presence of a home's documents: apart from the edits, so cursors cannot push edits out of
/// the ring.
pub fn presence_namespace(home: &str) -> String {
    format!("presence.{home}")
}

pub fn presence_key(id: &str) -> StreamKey {
    StreamKey::new(presence_namespace(home_of(id)), id)
}

/// Where a home's documents run: the one node with its label.
pub fn home(home: &str) -> Placement {
    Placement::default().require("home", home)
}

/// The web nodes, whose sessions hold presence.
pub fn web() -> Placement {
    Placement::default().require("web", "1")
}

/// The document's actor, on its home.
pub fn keyed(cluster: &Cluster, id: &str) -> KeyedRef<DocP> {
    cluster.keyed::<DocP>(KIND, id, Bytes::new(), &home(home_of(id)))
}

pub fn limits() -> CompactLimits {
    CompactLimits { max_keys: STREAM_KEYS, max_bytes: STREAM_BYTES }
}

/// Presence: at most 400 entries of 256 B, and a viewer count per web node.
pub fn presence_limits() -> CompactLimits {
    CompactLimits { max_keys: 1024, max_bytes: 256 << 10 }
}

/// Presence changes go out at most this often.
const PRESENCE_EVERY: Duration = Duration::from_millis(100);
const EXPIRE_EVERY: Duration = Duration::from_secs(5);

/// Presence publishes refused, process-wide: presence is shown on a best effort.
pub static PRESENCE_REFUSED: AtomicU64 = AtomicU64::new(0);
/// Edits answered saved, process-wide: `mdpad cluster` shows saves per second from it.
pub static SAVED: AtomicU64 = AtomicU64::new(0);

fn log_key(id: &str) -> Bytes {
    Bytes::from(format!("d/{id}"))
}

pub fn policy() -> RestartPolicy {
    RestartPolicy {
        max_restarts: 3,
        within: Duration::from_secs(60),
        backoff: Backoff { initial: Duration::from_millis(20), max: Duration::from_secs(1), jitter: 0.2 },
        ..RestartPolicy::default()
    }
}

type Answer = Option<Reply<DocAnswer>>;
type Written = Result<Result<(), LogError>, RequestError>;

/// What a version's Markdown, read from the log, is for.
pub enum Then {
    Show(Version, Answer),
    Restore(Principal, Version, Answer),
}

pub enum Msg {
    Call(DocReq, Answer),
    /// The document's entries in the log, and its list of versions'.
    Loaded(Result<(u64, Vec<Entry>, Vec<Entry>), String>),
    /// The version's Markdown, or what to answer: the version is gone, or the read failed.
    Fetched(Then, Result<String, DocAnswer>),
    /// An automatic version may be due.
    Version,
    Written(Written),
    Removed(Written, Answer),
    /// Publish the presence that changed.
    Flush,
    Expire,
    /// The catalog hears the title and size of the edits made since the last tell.
    Touch,
    /// A web node joined or left the cluster.
    Node(Option<NodeChange>),
    /// While a write is out: tell the editors waiting on it when the disk refuses it.
    Check,
}

impl ActorProtocol for DocP {
    type Mailbox = Msg;

    fn encode_reply(answer: DocAnswer) -> Result<Bytes, CallError> {
        Ok(answer.encode())
    }

    fn decode(body: Bytes, reply: Option<Reply<DocAnswer>>) -> Result<Msg, Rejection> {
        DocReq::decode(&body).map(|req| Msg::Call(req, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// What a document's actor is given by the home.
#[derive(Clone)]
pub struct Home {
    pub journal: ActorHandle<ToJournal>,
    pub catalog: catalog::Handle,
    pub streams: StreamNode,
    /// The stream's limits: `limits()`, but for a test that makes a publish fail.
    pub limits: CompactLimits,
    /// The journal's last write failed: edits are refused `cannot_save` until one lands.
    pub failing: Arc<AtomicBool>,
}

pub struct Doc {
    id: String,
    key: StreamKey,
    presence_key: StreamKey,
    home: Home,
    cluster: WeakCluster,
    presence: Presence,
    flushing: bool,
    /// The web nodes' comings and goings are followed, and stale entries expired, once a session is present.
    following: bool,
    /// The catalog lists it as created: a log that holds nothing of it is then an error (I4).
    ready: bool,
    core: Option<DocCore<Answer>>,
    removing: bool,
    /// What the catalog was last told.
    told: Option<Acl>,
    touched: Option<Duration>,
    edited: bool,
    /// A `Touch` is scheduled for the edits made since the last tell.
    touching: bool,
    checking: bool,
    /// An automatic version's check is scheduled.
    versioning: bool,
}

/// The disk refuses writes: the journal tries again after its rest.
fn cannot_save() -> DocAnswer {
    DocAnswer::CannotSave { retry_after_ms: journal::REST.as_millis() as u64 }
}

fn answer(reply: Answer, answer: DocAnswer) {
    if let Some(reply) = reply {
        drop(reply.send(answer));
    }
}

fn gone() -> DocAnswer {
    DocAnswer::Refused(Refusal::NotFound)
}

fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

impl Doc {
    pub fn new(id: String, ready: bool, home: Home, cluster: WeakCluster) -> Self {
        let (key, presence_key) = (stream_key(&id), presence_key(&id));
        Self {
            id,
            key,
            presence_key,
            home,
            cluster,
            presence: Presence::default(),
            flushing: false,
            following: false,
            ready,
            core: None,
            removing: false,
            told: None,
            touched: None,
            edited: false,
            touching: false,
            checking: false,
            versioning: false,
        }
    }

    /// A presence change: published within `PRESENCE_EVERY`, and its sessions' nodes followed from now on.
    fn present(&mut self, cx: &mut Context<'_, Self>) {
        if !self.following {
            let Some(cluster) = self.cluster.upgrade() else { return };
            // Once: an attach refused leaves the entries of a node that left to expire.
            self.following = true;
            drop(cx.attach(cluster.nodes(&web()).changes(), Msg::Node));
            drop(cx.schedule_every(EXPIRE_EVERY, || Msg::Expire));
        }
        if !self.flushing {
            self.flushing = cx.schedule_after(PRESENCE_EVERY, Msg::Flush).is_ok();
        }
    }

    fn tell_catalog(&self, event: Event) {
        // An index: a lost tell heals at the next load.
        drop(self.home.catalog.tell(keeper::Msg::Request(catalog::Request::Doc(event))));
    }

    fn touch(&mut self, now: Duration) {
        let Some(core) = &self.core else { return };
        let by = core.last_author().unwrap_or_default().to_string();
        let (title, size, ms) = (core.title(), core.size(), now_ms());
        self.tell_catalog(Event::Touched { id: self.id.clone(), title, size, ms, by });
        (self.touched, self.edited) = (Some(now), false);
    }

    fn call(&mut self, req: DocReq, reply: Answer, cx: &mut Context<'_, Self>) {
        let core = self.core.as_mut().expect("calls wait for the load");
        if self.removing {
            return answer(reply, DocAnswer::Refused(Refusal::NotFound));
        }
        let at_once = match req {
            DocReq::Join { who } => core.join(&who, reply),
            // Not applied: what is applied must be written before it is answered, and the disk refuses writes.
            DocReq::Edit { .. } if self.home.failing.load(Ordering::Relaxed) => Some((reply, cannot_save())),
            DocReq::Edit { session, who, update, offline } => {
                self.edited = true;
                if !self.versioning {
                    self.versioning = cx.schedule_after(versions::IDLE, Msg::Version).is_ok();
                }
                core.edit(&who, Edit { session, update: &update, offline, now_ms: now_ms() }, reply)
            }
            DocReq::Versions { who } => Some((reply, core.list_versions(&who))),
            DocReq::Version { who, n } => match core.version(&who, n, Action::History) {
                Ok(version) => self.fetch(Then::Show(version, reply), cx),
                Err(refused) => Some((reply, refused)),
            },
            DocReq::NameVersion { who, name } => core.name_version(&who, &name, now_ms(), reply),
            DocReq::Restore { .. } | DocReq::Replace { .. } if self.home.failing.load(Ordering::Relaxed) => {
                Some((reply, cannot_save()))
            }
            DocReq::Restore { who, n } => match core.version(&who, n, Action::Restore) {
                Ok(version) => self.fetch(Then::Restore(who, version, reply), cx),
                Err(refused) => Some((reply, refused)),
            },
            DocReq::Replace { who, markdown } => {
                self.edited = true;
                core.replace(&who, &markdown, now_ms(), reply)
            }
            DocReq::Aware { session, node, entry } => {
                self.presence.aware(session, &node, entry, cx.now());
                self.present(cx);
                None
            }
            DocReq::Leave { session } => {
                self.presence.leave(session);
                self.present(cx);
                // An editor that leaves is often on its way to the list: it shows what was typed.
                if self.edited {
                    self.touch(cx.now());
                }
                None
            }
            DocReq::Create { owner, markdown } => core.create(&owner, &markdown, now_ms(), reply),
            DocReq::Text { who } => Some((reply, core.read_text(&who))),
            DocReq::Info { who } => Some((reply, core.info(&who))),
            DocReq::Share { who, change } => core.share(&who, &change, reply),
            DocReq::Purge { who } => match core.may_purge(&who) {
                Err(refused) => Some((reply, refused)),
                Ok(()) => {
                    let journal = self.home.journal.clone();
                    let mut keys = core.version_keys();
                    keys.push(log_key(&self.id));
                    let remove = async move { journal.ask(|r| ToJournal::Remove(keys, r)).await };
                    // A refused job drops the reply in it, which answers its caller `NoReply`.
                    self.removing = cx.submit_future(async move { Msg::Removed(remove.await, reply) }).is_ok();
                    None
                }
            },
        };
        if let Some((reply, a)) = at_once {
            answer(reply, a);
        }
    }

    /// Reads a version's Markdown from the log, then goes on with `then`. Answered `busy` when the read cannot start.
    fn fetch(&self, then: Then, cx: &mut Context<'_, Self>) -> Option<(Answer, DocAnswer)> {
        let n = match &then {
            Then::Show(v, _) | Then::Restore(_, v, _) => v.n,
        };
        let (journal, key) = (self.home.journal.clone(), versions::body_key(&self.id, n));
        let read = async move {
            let Ok(Ok((_, entries))) = journal.ask(|r| ToJournal::Fetch(key, r)).await else {
                return Err(DocAnswer::Busy { retry_after_ms: 1000 });
            };
            // Thinned away since the caller saw the list.
            match entries.into_iter().last() {
                Some(Entry::Record { body, .. }) => String::from_utf8(body.to_vec()).map_err(|_| gone()),
                _ => Err(gone()),
            }
        };
        // A refused job drops `then` and the reply in it: its caller is answered `NoReply`, and asks again.
        drop(cx.submit_future(async move { Msg::Fetched(then, read.await) }));
        None
    }

    /// What the write let go: its records on the stream, then its callers.
    fn written(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let core = self.core.as_mut().ok_or("a write before the load")?;
        let done = core.written();
        for records in done.publish {
            if let Err(refused) = self.home.streams.publish(&self.key, records) {
                return Err(format!("publish after the write: {:?}", refused.error));
            }
        }
        let saved = done.waiters.iter().filter(|w| matches!(w.answer, DocAnswer::Saved { .. })).count();
        SAVED.fetch_add(saved as u64, Ordering::Relaxed);
        for waiter in done.waiters {
            answer(waiter.reply, waiter.answer);
        }
        if core.acl() != self.told.as_ref() {
            let acl = core.acl().cloned();
            if let Some(acl) = &acl {
                self.tell_catalog(Event::sharing(&self.id, acl));
            }
            if self.told.is_none() {
                self.touch(cx.now());
            }
            self.told = acl;
        }
        if self.edited {
            let since = self.touched.map(|at| cx.now().saturating_sub(at));
            match since {
                Some(since) if since < TOUCH_EVERY => {
                    // The last edits of a burst reach the list too, not only at the next edit or the stop.
                    if !self.touching {
                        self.touching = cx.schedule_after(TOUCH_EVERY - since, Msg::Touch).is_ok();
                    }
                }
                _ => self.touch(cx.now()),
            }
        }
        Ok(())
    }
}

impl Actor for Doc {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (journal, streams) = (self.home.journal.clone(), self.home.streams.clone());
        let opens = [(self.key.clone(), self.home.limits), (self.presence_key.clone(), presence_limits())];
        let (log_key, index_key) = (log_key(&self.id), versions::index_key(&self.id));
        let load = async move {
            let loaded = journal.ask(|r| ToJournal::Load(log_key, r)).await;
            let (epoch, entries) = loaded.map_err(|e| format!("{e:?}"))?.map_err(|e| e.to_string())?;
            let index = journal.ask(|r| ToJournal::Fetch(index_key, r)).await;
            let (_, index) = index.map_err(|e| format!("{e:?}"))?.map_err(|e| e.to_string())?;
            // The journal's epoch for this start is the streams' generation: each start is a new one.
            for (key, limits) in opens {
                match streams.open_stream(&key, epoch, DeliveryPolicy::Compacted(limits)).await {
                    // A restart in this process: the stream is still ours.
                    Err(StreamError::AlreadyExists) => streams.restart_stream(&key, epoch),
                    opened => opened,
                }
                .map_err(|e| format!("open the stream {}: {e:?}", key.namespace()))?;
            }
            Ok((epoch, entries, index))
        };
        let job = cx.submit_future(async move { Msg::Loaded(load.await) }).map_err(job_error)?;
        cx.pause_until(&job).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let (epoch, entries, index) = loaded?;
                let mut core = DocCore::load(epoch, entries, self.ready)?;
                core.set_versions(&self.id, index)?;
                let born = core.born();
                if !born.is_empty() {
                    self.home
                        .streams
                        .publish(&self.key, born)
                        .map_err(|e| format!("publish the state: {:?}", e.error))?;
                }
                if let Some(acl) = core.acl() {
                    self.tell_catalog(Event::sharing(&self.id, acl));
                    self.told = Some(acl.clone());
                }
                self.core = Some(core);
            }
            Msg::Call(req, reply) => self.call(req, reply, cx),
            Msg::Fetched(then, read) => {
                let core = self.core.as_mut().ok_or("a read before the load")?;
                let at_once = match (then, read) {
                    // A version the actor cannot read is no reason to stop the document's editors.
                    (Then::Show(_, reply) | Then::Restore(_, _, reply), Err(refused)) => Some((reply, refused)),
                    (Then::Show(version, reply), Ok(markdown)) => {
                        Some((reply, DocAnswer::Version(Box::new(version), markdown)))
                    }
                    (Then::Restore(who, version, reply), Ok(markdown)) => {
                        self.edited = true;
                        core.restore(&who, version.n, &markdown, now_ms(), reply)
                    }
                };
                if let Some((reply, a)) = at_once {
                    answer(reply, a);
                }
            }
            Msg::Version => {
                self.versioning = false;
                let now = now_ms();
                if let Some(at) = self.core.as_mut().and_then(|core| core.auto_version(now)) {
                    let wait = Duration::from_millis(at.saturating_sub(now));
                    self.versioning = cx.schedule_after(wait, Msg::Version).is_ok();
                }
            }
            Msg::Written(Ok(Ok(()))) => self.written(cx)?,
            // Fenced by a newer start, refused, or no answer: what was applied may not be on disk, so start again
            // from what is.
            Msg::Written(failed) => return Err(format!("write: {failed:?}")),
            Msg::Removed(Ok(Ok(())), reply) => {
                // A touch at the stop would make the row again.
                self.edited = false;
                self.tell_catalog(Event::Purged { id: self.id.clone() });
                drop(self.home.streams.close_stream(&self.key));
                answer(reply, DocAnswer::Done);
                cx.stop();
            }
            Msg::Removed(failed, reply) => {
                self.removing = false;
                answer(reply, DocAnswer::Busy { retry_after_ms: 1000 });
                return Err(format!("remove: {failed:?}"));
            }
            Msg::Flush => {
                self.flushing = false;
                let records = self.presence.take();
                if !records.is_empty() && self.home.streams.publish(&self.presence_key, records).is_err() {
                    PRESENCE_REFUSED.fetch_add(1, Ordering::Relaxed);
                }
            }
            Msg::Touch => {
                self.touching = false;
                if self.edited {
                    self.touch(cx.now());
                }
            }
            Msg::Expire => {
                self.presence.expire(cx.now());
                self.present(cx);
            }
            Msg::Node(Some(NodeChange::Left(node))) => {
                self.presence.left(&node);
                self.present(cx);
            }
            Msg::Node(_) => {}
            Msg::Check => {
                self.checking = false;
                if self.home.failing.load(Ordering::Relaxed)
                    && let Some(core) = self.core.as_mut()
                {
                    for reply in core.hold() {
                        answer(reply, cannot_save());
                    }
                }
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(core) = self.core.as_mut() else { return Ok(()) };
        if !self.checking && !core.is_idle() {
            self.checking = cx.schedule_after(journal::REST, Msg::Check).is_ok();
        }
        // Nothing while a write is out.
        let Some(ops) = core.take() else { return Ok(()) };
        let (journal, key, epoch) = (self.home.journal.clone(), log_key(&self.id), core.generation());
        // No deadline: a disk that fails holds the write, and the journal tries it again until it lands.
        let write = journal.address.request(None, |r| ToJournal::Write(key, epoch, ops, r));
        let write = async move { Msg::Written(write.await) };
        // Draining: what was applied cannot be written, so it is never answered; what comes now is answered busy.
        core.draining = cx.submit_future(write).is_err();
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        if self.edited {
            let now = self.touched.unwrap_or_default();
            self.touch(now);
        }
        // Again after a purge, a refused close: also when the actor fails while the removal is out.
        drop(self.home.streams.close_stream(&self.key));
        drop(self.home.streams.close_stream(&self.presence_key));
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Call(DocReq::Edit { .. }, _) => "doc.edit",
            Msg::Call(DocReq::Join { .. }, _) => "doc.join",
            Msg::Call(DocReq::Aware { .. } | DocReq::Leave { .. }, _) | Msg::Flush | Msg::Expire | Msg::Node(_) => {
                "doc.aware"
            }
            Msg::Call(
                DocReq::Versions { .. }
                | DocReq::Version { .. }
                | DocReq::NameVersion { .. }
                | DocReq::Restore { .. }
                | DocReq::Replace { .. },
                _,
            )
            | Msg::Fetched(..)
            | Msg::Version => "doc.version",
            Msg::Call(..) => "doc.call",
            Msg::Loaded(_) => "doc.load",
            Msg::Touch => "doc.touch",
            Msg::Written(_) | Msg::Removed(..) | Msg::Check => "doc.write",
        }
    }
}
