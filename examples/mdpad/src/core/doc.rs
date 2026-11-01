//! One document, with no I/O: its Yjs state, its sharing, the checks an edit passes, the records it journals and
//! publishes, and when it compacts. The actor in `doc.rs` carries the ops to the node's log and the records to the
//! stream, in the order [`Pipeline`] keeps.
//!
//! The stream holds `acl`, `base` (the state at the last compaction) and one `u/<n>` per update since, each
//! `session (8 bytes) ‖ diff`. A compaction puts the state as it stands in the queue as a checkpoint, before any later
//! update, so a `base` never holds what its `u/*` records did not carry first: a client that follows the stream live
//! needs only its position.

use axon_stream::keyed;
use bytes::Bytes;

use crate::core::acl::{Acl, Action, Change, Level, Principal, Refused, Role};
use crate::core::pipeline::{Done, Op, Pipeline};
use crate::core::versions::{self, Kind, Version, Versions};
use crate::crdt;
use crate::wire::{DocAnswer, DocInfo, Malformed, Reader, Refusal, Writer};

/// The largest update a client may send.
pub const MAX_UPDATE: usize = 256 << 10;
/// The encoded state an insert may not take past (Q4). Deletes are still taken.
pub const MAX_STATE: usize = 256 << 10;
/// The stream's `CompactLimits`.
pub const STREAM_BYTES: usize = 448 << 10;
pub const STREAM_KEYS: usize = 1024;
/// Past this many bytes applied and not yet durable, an edit is answered busy.
pub const MAX_UNWRITTEN: usize = 1 << 20;
pub const BUSY_MS: u64 = 200;
/// The `u/*` bytes that make a compaction: the size of `base`, and at least this.
const COMPACT_MIN: usize = 32 << 10;
/// Kept free under the stream's limits for the `acl` record and the framing.
const STREAM_ROOM: usize = 16 << 10;
const KEYS_ROOM: usize = 64;

/// The Yjs client id of what the server writes itself: a new document's first text. Clients draw theirs at random.
pub const SERVER: u64 = 0xFFFF_FFFF;

const UPDATE: u8 = 0;
const ACL: u8 = 1;
const FORMAT: u8 = 1;

/// An edit's bytes and where it came from.
#[derive(Clone, Copy)]
pub struct Edit<'a> {
    pub session: u64,
    pub update: &'a [u8],
    pub offline: bool,
    pub now_ms: u64,
}

/// A caller to answer once the change it waits for is durable.
pub struct Waiter<R> {
    pub reply: R,
    pub answer: DocAnswer,
}

pub struct DocCore<R> {
    doc: yrs::Doc,
    /// `None` until the document has a first state.
    acl: Option<Acl>,
    created_ms: u64,
    /// The log's epoch for this start, which is the stream's generation.
    generation: u64,
    /// The encoded state as of the last compaction, plus every diff since.
    size: usize,
    base: usize,
    /// The `u/*` keys published, or queued to be, since `base`.
    updates: Vec<Bytes>,
    update_bytes: usize,
    next: u64,
    pipeline: Pipeline<Waiter<R>>,
    /// Set once a write could not start: the actor is stopping, and an edit applied now could not be saved.
    pub draining: bool,
    /// The document id, which names its versions' keys.
    id: String,
    versions: Versions,
    /// Who edited since the newest version, in the order they first did, and when the last edit came; `None`
    /// when nothing changed since.
    authors: Vec<String>,
    last_edit_ms: Option<u64>,
}

fn refusal(refused: Refused) -> DocAnswer {
    DocAnswer::Refused(match refused {
        Refused::Forbidden => Refusal::Forbidden,
        Refused::Trashed => Refusal::Trashed,
    })
}

fn record_bytes(key: &[u8], value: &[u8]) -> usize {
    record_len(key.len(), value.len())
}

fn record_len(key: usize, value: usize) -> usize {
    2 + key + value + 4
}

impl<R> DocCore<R> {
    /// The document as its log holds it. `ready` says the catalog lists it as created: then a log that holds nothing
    /// of it is an error, not a new document (I4).
    pub fn load(generation: u64, entries: Vec<axon::files::Entry>, ready: bool) -> Result<Self, String> {
        let mut core = Self {
            doc: crdt::doc(SERVER),
            acl: None,
            created_ms: 0,
            generation,
            size: 0,
            base: 0,
            updates: Vec::new(),
            update_bytes: 0,
            next: 1,
            pipeline: Pipeline::default(),
            draining: false,
            id: String::new(),
            versions: Versions::default(),
            authors: Vec::new(),
            last_edit_ms: None,
        };
        for entry in entries {
            match entry {
                axon::files::Entry::Checkpoint { state, .. } => {
                    let (created_ms, acl, state) = decode_checkpoint(&state).map_err(|e| format!("checkpoint: {e}"))?;
                    core.doc = crdt::doc(SERVER);
                    crdt::apply(&core.doc, &state).map_err(|e| format!("checkpoint state: {e:?}"))?;
                    (core.created_ms, core.acl) = (created_ms, Some(acl));
                }
                axon::files::Entry::Record { body, .. } => match Record::decode(&body).map_err(|e| e.to_string())? {
                    Record::Update { diff, .. } => {
                        crdt::apply(&core.doc, &diff).map_err(|e| format!("update: {e:?}"))?;
                    }
                    Record::Acl(acl) => core.acl = Some(acl),
                },
            }
        }
        if core.acl.is_none() && ready {
            return Err("the log holds nothing of a document the catalog lists".into());
        }
        core.size = crdt::state(&core.doc).len();
        Ok(core)
    }

    /// The document's id and its list of versions, as its log holds them.
    pub fn set_versions(&mut self, id: &str, entries: Vec<axon::files::Entry>) -> Result<(), String> {
        self.id = id.into();
        if let Some(axon::files::Entry::Record { body, .. }) = entries.into_iter().last() {
            self.versions = Versions::decode(&body).map_err(|e| format!("versions: {e}"))?;
        }
        Ok(())
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn versions(&self) -> &Versions {
        &self.versions
    }

    /// Who wrote the text last, for the list.
    pub fn last_author(&self) -> Option<&str> {
        self.authors.last().map(String::as_str)
    }

    pub fn acl(&self) -> Option<&Acl> {
        self.acl.as_ref()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn ydoc(&self) -> &yrs::Doc {
        &self.doc
    }

    pub fn text(&self) -> String {
        crdt::text(&self.doc)
    }

    /// The first line, without its heading marks.
    pub fn title(&self) -> String {
        let text = self.text();
        let line = text.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("");
        line.trim_start_matches('#').trim().chars().take(120).collect()
    }

    /// What a new stream generation starts with: the sharing and the whole state as `base`.
    pub fn born(&mut self) -> Vec<Bytes> {
        let Some(acl) = &self.acl else { return Vec::new() };
        let state = crdt::state(&self.doc);
        (self.size, self.base, self.update_bytes) = (state.len(), state.len(), 0);
        self.updates.clear();
        vec![keyed(b"acl", &acl.encode()), keyed(b"base", &state)]
    }

    /// Answered once everything applied so far is durable and published, so a session never subscribes to a stream
    /// that lacks what the answer counts, nor gets a `base` live that holds what no record carried.
    pub fn join(&mut self, who: &Principal, reply: R) -> Option<(R, DocAnswer)> {
        let Some(acl) = &self.acl else { return Some((reply, DocAnswer::Refused(Refusal::NotFound))) };
        let answer = match acl.check(who, Action::Open) {
            Ok(_) => DocAnswer::Joined {
                generation: self.generation,
                level: acl.level(who),
                size: self.size,
                max_size: MAX_STATE,
            },
            Err(refused) => return Some((reply, refusal(refused))),
        };
        self.after_last(reply, answer)
    }

    pub fn read_text(&self, who: &Principal) -> DocAnswer {
        let Some(acl) = &self.acl else { return DocAnswer::Refused(Refusal::NotFound) };
        // At `/p/` anyone reads a published text; everywhere else reading is exporting.
        let action = if *who == Principal::Reader { Action::ReadPublished } else { Action::Export };
        match acl.check(who, action) {
            Ok(_) => DocAnswer::Text(self.text()),
            Err(refused) => refusal(refused),
        }
    }

    /// Anyone with a role sees the metadata, the owner of a trashed document too: what they may do is in it.
    pub fn info(&self, who: &Principal) -> DocAnswer {
        let Some(acl) = &self.acl else { return DocAnswer::Refused(Refusal::NotFound) };
        match acl.role(who) {
            // The owner still sees a trashed document's info: what the list's Restore needs.
            Some(role) if role != Role::Owner && acl.trashed_ms.is_some() => DocAnswer::Refused(Refusal::Trashed),
            Some(_) => DocAnswer::Info(Box::new(DocInfo {
                acl: acl.clone(),
                level: acl.level(who),
                title: self.title(),
                size: self.size,
                max_size: MAX_STATE,
            })),
            None => DocAnswer::Refused(Refusal::Forbidden),
        }
    }

    /// The first state, journaled as a checkpoint. A document that has one answers `Done` and changes nothing.
    pub fn create(&mut self, owner: &str, markdown: &str, now_ms: u64, reply: R) -> Option<(R, DocAnswer)> {
        if self.acl.is_some() {
            return self.after_last(reply, DocAnswer::Done);
        }
        if markdown.len() > MAX_STATE / 2 {
            return Some((reply, DocAnswer::Refused(Refusal::Full)));
        }
        if !markdown.is_empty() {
            crdt::insert(&self.doc, 0, markdown);
        }
        (self.acl, self.created_ms) = (Some(Acl::new(owner)), now_ms);
        let publish = self.born();
        let state = crdt::state(&self.doc);
        let body = encode_checkpoint(self.created_ms, self.acl.as_ref().expect("set above"), &state);
        self.pipeline.push(Op::Checkpoint(body), publish, Some(Waiter { reply, answer: DocAnswer::Done }));
        None
    }

    /// An edit from session `session`: refused, answered at once, or queued to be answered once it is durable.
    /// `offline` says it carries work from while the client was away: a large one saves a version first.
    pub fn edit(&mut self, who: &Principal, edit: Edit<'_>, reply: R) -> Option<(R, DocAnswer)> {
        let Edit { session, update, offline, now_ms } = edit;
        let Some(acl) = &self.acl else { return Some((reply, DocAnswer::Refused(Refusal::NotFound))) };
        if let Err(busy) = self.ready() {
            return Some((reply, busy));
        }
        if let Err(refused) = acl.check(who, Action::Edit) {
            return Some((reply, refusal(refused)));
        }
        if update.len() > MAX_UPDATE {
            return Some((reply, DocAnswer::Refused(Refusal::TooLarge)));
        }
        let Some(decoded) = crdt::decode(update) else {
            return Some((reply, DocAnswer::Refused(Refusal::Resync)));
        };
        if crdt::inserts(&decoded) && self.size + update.len() > MAX_STATE {
            return Some((reply, DocAnswer::Refused(Refusal::Full)));
        }
        // The update's record could take the stream past its limits: compact first, with the state before it.
        let room = record_bytes(b"u/0000000000", update) + 8;
        if self.stream_bytes() + room > STREAM_BYTES - STREAM_ROOM || self.updates.len() + KEYS_ROOM > STREAM_KEYS {
            self.compact();
        }
        if offline && update.len() > versions::OFFLINE_BYTES && crdt::inserts(&decoded) {
            let author = vec![who.name().to_string()];
            self.save_version(Kind::BeforeOffline, String::new(), Some(author), now_ms);
        }
        let applied = match crdt::apply_update(&self.doc, decoded) {
            Ok(applied) => applied,
            Err(_) => return Some((reply, DocAnswer::Refused(Refusal::Resync))),
        };
        if !applied.is_empty() {
            self.edited(who.name(), now_ms);
            self.size += applied.diff.len();
        }
        let answer = match applied.missing {
            true => DocAnswer::Refused(Refusal::Resync),
            false => DocAnswer::Saved { size: self.size },
        };
        if applied.is_empty() {
            // A retry, or offline work the document already holds: answered once what applied it first is durable.
            return self.after_last(reply, answer);
        }
        let key = Bytes::from(format!("u/{}", self.next));
        self.next += 1;
        let mut value = session.to_be_bytes().to_vec();
        value.extend_from_slice(&applied.diff);
        self.update_bytes += record_bytes(&key, &value);
        let publish = vec![keyed(&key, &value)];
        self.updates.push(key);
        let body = Record::Update { session, who: who.clone(), diff: applied.diff }.encode();
        self.pipeline.push(Op::Record(body), publish, Some(Waiter { reply, answer }));
        if self.update_bytes >= self.base.max(COMPACT_MIN) {
            self.compact();
        }
        None
    }

    /// A change of sharing, journaled and published as the new `acl`.
    pub fn share(&mut self, who: &Principal, change: &Change, reply: R) -> Option<(R, DocAnswer)> {
        let Some(acl) = &self.acl else { return Some((reply, DocAnswer::Refused(Refusal::NotFound))) };
        let next = match change.apply(acl, who) {
            Ok(next) => next,
            Err(refused) => return Some((reply, refusal(refused))),
        };
        if next == *acl {
            return self.after_last(reply, DocAnswer::Done);
        }
        let publish = vec![keyed(b"acl", &next.encode())];
        self.pipeline.push(
            Op::Record(Record::Acl(next.clone()).encode()),
            publish,
            Some(Waiter { reply, answer: DocAnswer::Done }),
        );
        self.acl = Some(next);
        None
    }

    fn edited(&mut self, who: &str, now_ms: u64) {
        if let Some(i) = self.authors.iter().position(|a| a == who) {
            // The last author last: the list shows who edited last.
            let who = self.authors.remove(i);
            self.authors.push(who);
        } else {
            self.authors.push(who.into());
        }
        self.last_edit_ms = Some(now_ms);
    }

    /// Saves the text as it stands as a version: its Markdown, then the list, then what thinning let go, in the
    /// next write. `authors` stands for those who edited since the version before.
    fn save_version(&mut self, kind: Kind, name: String, authors: Option<Vec<String>>, now_ms: u64) -> Version {
        let text = self.text();
        let authors = authors.unwrap_or_else(|| std::mem::take(&mut self.authors));
        let version = Version {
            n: 0,
            at_ms: now_ms,
            kind,
            name,
            authors,
            bytes: text.len() as u64,
            crc: axon::files::crc32(text.as_bytes()),
        };
        let (version, gone) = self.versions.add(version);
        self.pipeline.push(Op::Put(versions::body_key(&self.id, version.n), text.into()), Vec::new(), None);
        self.pipeline.push(Op::Put(versions::index_key(&self.id), self.versions.encode().into()), Vec::new(), None);
        for n in gone {
            self.pipeline.push(Op::Remove(versions::body_key(&self.id, n)), Vec::new(), None);
        }
        version
    }

    /// An automatic version, when one is due: `Some(ms)` says when to ask again.
    pub fn auto_version(&mut self, now_ms: u64) -> Option<u64> {
        let last_edit = self.last_edit_ms?;
        // A new document's first version waits for the pause too: its 30 minutes count from its creation.
        let newest = self.versions.newest().map_or(self.created_ms, |v| v.at_ms);
        if !versions::due(now_ms, last_edit, newest) || self.draining {
            let idle = last_edit + versions::IDLE.as_millis() as u64;
            return Some(idle.min(newest + versions::EVERY.as_millis() as u64).max(now_ms + 1000));
        }
        self.last_edit_ms = None;
        let text = self.text();
        if self.versions.newest().is_some_and(|v| v.crc == axon::files::crc32(text.as_bytes())) {
            self.authors.clear();
            return None;
        }
        self.save_version(Kind::Auto, String::new(), None, now_ms);
        None
    }

    pub fn list_versions(&self, who: &Principal) -> DocAnswer {
        match self.allowed(who, Action::History) {
            Ok(()) => DocAnswer::Versions(self.versions.list().to_vec()),
            Err(refused) => refused,
        }
    }

    /// A version `who` may read, whose Markdown the actor reads from the log.
    pub fn version(&self, who: &Principal, n: u64, action: Action) -> Result<Version, DocAnswer> {
        self.allowed(who, action)?;
        self.versions.get(n).cloned().ok_or(DocAnswer::Refused(Refusal::NotFound))
    }

    pub fn name_version(&mut self, who: &Principal, name: &str, now_ms: u64, reply: R) -> Option<(R, DocAnswer)> {
        let name: String = name.trim().chars().take(versions::MAX_NAME).collect();
        if let Err(refused) = self.allowed(who, Action::NameVersion).and(self.ready()) {
            return Some((reply, refused));
        }
        let version = self.save_version(Kind::Named, name, None, now_ms);
        self.last_edit_ms = None;
        self.after_last(reply, DocAnswer::Version(Box::new(version), String::new()))
    }

    /// Makes the text version `version`'s, `markdown`, as one forward edit by "<who> (restore)", after a "before
    /// restore" version. The log is never rewound: an offline peer's text typed meanwhile stays (journey 7).
    pub fn restore(
        &mut self,
        who: &Principal,
        n: u64,
        markdown: &str,
        now_ms: u64,
        reply: R,
    ) -> Option<(R, DocAnswer)> {
        if let Err(refused) = self.allowed(who, Action::Restore).and(self.ready()) {
            return Some((reply, refused));
        }
        let label = format!("{} (restore)", who.name());
        self.rewrite(Kind::BeforeRestore, &label, markdown, Some(n), now_ms, reply)
    }

    /// An import into the document: the text becomes `markdown`, after a "before import" version.
    pub fn replace(&mut self, who: &Principal, markdown: &str, now_ms: u64, reply: R) -> Option<(R, DocAnswer)> {
        if let Err(refused) = self.allowed(who, Action::Edit).and(self.ready()) {
            return Some((reply, refused));
        }
        let label = format!("{} (import)", who.name());
        self.rewrite(Kind::BeforeImport, &label, markdown, None, now_ms, reply)
    }

    fn rewrite(
        &mut self,
        kind: Kind,
        label: &str,
        markdown: &str,
        restored: Option<u64>,
        now_ms: u64,
        reply: R,
    ) -> Option<(R, DocAnswer)> {
        // The new text's characters are inserts: past the cap the document is full, as for an edit.
        if markdown.len() > MAX_STATE / 2 || self.size + markdown.len() > MAX_STATE {
            return Some((reply, DocAnswer::Refused(Refusal::Full)));
        }
        let before = self.save_version(kind, String::new(), None, now_ms);
        let diff = crdt::replace(&self.doc, markdown);
        if diff.is_empty() {
            return self.after_last(reply, DocAnswer::Version(Box::new(before), String::new()));
        }
        self.edited(label, now_ms);
        self.size += diff.len();
        let key = Bytes::from(format!("u/{}", self.next));
        self.next += 1;
        let mut value = 0u64.to_be_bytes().to_vec();
        value.extend_from_slice(&diff);
        self.update_bytes += record_bytes(&key, &value);
        let mut publish = vec![keyed(&key, &value)];
        if let Some(n) = restored {
            publish.push(keyed(b"r", &n.to_be_bytes()));
        }
        self.updates.push(key);
        let body = Record::Update { session: 0, who: Principal::User(label.into()), diff }.encode();
        let answer = DocAnswer::Version(Box::new(before), String::new());
        self.pipeline.push(Op::Record(body), publish, Some(Waiter { reply, answer }));
        // Folded: the whole change goes out as a new base in the same write, so the stream stays in its limits.
        self.compact();
        None
    }

    /// Busy while the disk is behind or the actor drains, as for an edit.
    fn ready(&self) -> Result<(), DocAnswer> {
        match self.draining || self.pipeline.unwritten() > MAX_UNWRITTEN {
            true => Err(DocAnswer::Busy { retry_after_ms: BUSY_MS }),
            false => Ok(()),
        }
    }

    fn allowed(&self, who: &Principal, action: Action) -> Result<(), DocAnswer> {
        let acl = self.acl.as_ref().ok_or(DocAnswer::Refused(Refusal::NotFound))?;
        acl.check(who, action).map(drop).map_err(refusal)
    }

    /// Every key the document holds in its node's log besides its own: its versions.
    pub fn version_keys(&self) -> Vec<Bytes> {
        let bodies = self.versions.list().iter().map(|v| versions::body_key(&self.id, v.n));
        bodies.chain([versions::index_key(&self.id)]).collect()
    }

    /// Only the owner deletes for good, and only from the trash.
    pub fn may_purge(&self, who: &Principal) -> Result<(), DocAnswer> {
        let acl = self.acl.as_ref().ok_or(DocAnswer::Refused(Refusal::NotFound))?;
        match acl.role(who) == Some(Role::Owner) && acl.trashed_ms.is_some() {
            true => Ok(()),
            false => Err(DocAnswer::Refused(Refusal::Forbidden)),
        }
    }

    /// The state as it stands goes in the queue as a checkpoint, and the stream's `u/*` records give way to it.
    fn compact(&mut self) {
        let Some(acl) = &self.acl else { return };
        let state = crdt::state(&self.doc);
        let mut publish = vec![keyed(b"base", &state)];
        publish.extend(self.updates.drain(..).map(|key| keyed(&key, b"")));
        let body = encode_checkpoint(self.created_ms, acl, &state);
        (self.size, self.base, self.update_bytes) = (state.len(), state.len(), 0);
        self.pipeline.push(Op::Checkpoint(body), publish, None);
    }

    fn stream_bytes(&self) -> usize {
        let acl = self.acl.as_ref().map_or(0, |acl| acl.encode().len());
        record_len(3, acl) + record_len(4, self.base) + self.update_bytes
    }

    fn after_last(&mut self, reply: R, answer: DocAnswer) -> Option<(R, DocAnswer)> {
        self.pipeline.wait(Waiter { reply, answer }).err().map(|w| (w.reply, w.answer))
    }

    pub fn take(&mut self) -> Option<Vec<Op>> {
        self.pipeline.take()
    }

    /// The editors waiting on a write the disk refuses, to tell them now. Their edits stay applied and are written
    /// when the disk takes them; an edit sent again is applied as nothing and answered once that write lands.
    pub fn hold(&mut self) -> Vec<R> {
        let edits = self.pipeline.release(|w| matches!(w.answer, DocAnswer::Saved { .. }));
        edits.into_iter().map(|w| w.reply).collect()
    }

    pub fn written(&mut self) -> Done<Waiter<R>> {
        self.pipeline.written()
    }

    pub fn is_idle(&self) -> bool {
        self.pipeline.is_idle()
    }

    pub fn level(&self, who: &Principal) -> Level {
        self.acl.as_ref().map_or(Level::None, |acl| acl.level(who))
    }
}

/// A record in the node's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Update { session: u64, who: Principal, diff: Vec<u8> },
    Acl(Acl),
}

impl Record {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            Self::Update { session, who, diff } => {
                w.u8(UPDATE).uint(*session);
                who.write(&mut w);
                w.bytes(diff);
            }
            Self::Acl(acl) => {
                w.u8(ACL);
                acl.write(&mut w);
            }
        }
        w.into_vec().into()
    }

    pub fn decode(body: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(body);
        let record = match r.u8()? {
            UPDATE => Self::Update { session: r.uint()?, who: Principal::read(&mut r)?, diff: r.bytes()?.to_vec() },
            ACL => Self::Acl(Acl::read(&mut r)?),
            _ => return Err(Malformed),
        };
        r.end()?;
        Ok(record)
    }
}

pub fn encode_checkpoint(created_ms: u64, acl: &Acl, state: &[u8]) -> Bytes {
    let mut w = Writer::new();
    w.u8(FORMAT).uint(created_ms);
    acl.write(&mut w);
    w.bytes(state);
    w.into_vec().into()
}

pub fn decode_checkpoint(body: &[u8]) -> Result<(u64, Acl, Vec<u8>), Malformed> {
    let mut r = Reader::new(body);
    if r.u8()? != FORMAT {
        return Err(Malformed);
    }
    let (created_ms, acl, state) = (r.uint()?, Acl::read(&mut r)?, r.bytes()?.to_vec());
    r.end()?;
    Ok((created_ms, acl, state))
}

#[cfg(test)]
mod tests {
    use axon::files::{Address, Entry};
    use axon_stream::split_keyed;

    use super::*;

    fn ana() -> Principal {
        Principal::User("ana".into())
    }

    fn ed(session: u64, update: &[u8]) -> Edit<'_> {
        Edit { session, update, offline: false, now_ms: 0 }
    }

    type Others = std::collections::BTreeMap<Bytes, Bytes>;

    /// Writes what the core queued, as a log would, and returns the published records and the answers.
    fn write(core: &mut DocCore<u32>, log: &mut Vec<Entry>) -> (Vec<Bytes>, Vec<(u32, DocAnswer)>) {
        write_all(core, log, &mut Others::new())
    }

    /// [`write`], with the other keys' values in `others`.
    fn write_all(
        core: &mut DocCore<u32>,
        log: &mut Vec<Entry>,
        others: &mut Others,
    ) -> (Vec<Bytes>, Vec<(u32, DocAnswer)>) {
        let Some(ops) = core.take() else { return (Vec::new(), Vec::new()) };
        for op in ops {
            let at = Address { epoch: core.generation, seq: log.len() as u64 + 1 };
            log.push(match op {
                Op::Record(body) => Entry::Record { at, body },
                Op::Checkpoint(state) => Entry::Checkpoint { through: at, state },
                Op::Put(key, value) => {
                    others.insert(key, value);
                    continue;
                }
                Op::Remove(key) => {
                    others.remove(&key);
                    continue;
                }
            });
        }
        let done = core.written();
        (done.publish.concat(), done.waiters.into_iter().map(|w| (w.reply, w.answer)).collect())
    }

    fn created() -> (DocCore<u32>, Vec<Entry>) {
        let mut core = DocCore::load(1, Vec::new(), false).unwrap();
        assert_eq!(core.join(&ana(), 9), Some((9, DocAnswer::Refused(Refusal::NotFound))), "unborn");
        assert!(core.create("ana", "# Plan\n", 5, 0).is_none());
        assert!(core.join(&ana(), 8).is_none(), "joins once the first state is durable");
        let mut log = Vec::new();
        let (published, answers) = write(&mut core, &mut log);
        assert_eq!(answers[0], (0, DocAnswer::Done));
        assert!(matches!(answers[1], (8, DocAnswer::Joined { generation: 1, .. })));
        assert_eq!(published.len(), 2, "acl and base");
        (core, log)
    }

    #[test]
    fn a_second_create_changes_nothing_and_a_listed_document_never_loads_empty() {
        let (mut core, mut log) = created();
        assert_eq!(core.create("ben", "other", 6, 1), Some((1, DocAnswer::Done)));
        assert_eq!(core.text(), "# Plan\n");
        assert!(write(&mut core, &mut log).1.is_empty());
        assert!(DocCore::<u32>::load(2, Vec::new(), true).is_err(), "I4: the catalog says it was created");
        let again = DocCore::<u32>::load(2, log, true).unwrap();
        assert_eq!((again.text(), again.acl().unwrap().owner.as_str()), ("# Plan\n".to_string(), "ana"));
        assert_eq!(again.title(), "Plan");
    }

    #[test]
    fn an_edit_is_answered_after_its_write_and_a_retry_after_the_first_copy() {
        let (mut core, mut log) = created();
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let update = crdt::insert(&client, 7, "go");
        assert!(core.edit(&ana(), ed(42, &update), 1).is_none());
        assert!(core.edit(&ana(), ed(42, &update), 2).is_none(), "a retry waits for the first copy's write");
        let (published, answers) = write(&mut core, &mut log);
        assert_eq!(answers.iter().map(|a| a.0).collect::<Vec<_>>(), [1, 2]);
        assert!(matches!(answers[0].1, DocAnswer::Saved { .. }));
        let (key, value) = split_keyed(&published[0]).unwrap();
        assert_eq!((&key[..], &value[..8]), (&b"u/1"[..], &42u64.to_be_bytes()[..]));
        assert!(matches!(core.edit(&ana(), ed(42, &update), 3), Some((3, DocAnswer::Saved { .. }))), "durable already");
        let reloaded = DocCore::<u32>::load(2, log, true).unwrap();
        assert_eq!(reloaded.text(), "# Plan\ngo");
    }

    #[test]
    fn refusals_change_nothing() {
        let (mut core, _) = created();
        let viewer = Principal::User("ben".into());
        let client = crdt::doc(9);
        let update = crdt::insert(&client, 0, "x");
        assert_eq!(core.edit(&viewer, ed(1, &update), 1), Some((1, DocAnswer::Refused(Refusal::Forbidden))));
        assert_eq!(core.edit(&ana(), ed(1, b"\xFF\xFF"), 2), Some((2, DocAnswer::Refused(Refusal::Resync))));
        let big = crdt::insert(&client, 1, &"y".repeat(MAX_UPDATE));
        assert_eq!(core.edit(&ana(), ed(1, &big), 3), Some((3, DocAnswer::Refused(Refusal::TooLarge))));
        // An update that skips a client's clock: refused before it applies.
        let later = crdt::insert(&client, 1, "z");
        assert_eq!(core.edit(&ana(), ed(1, &later), 4), Some((4, DocAnswer::Refused(Refusal::Resync))));
        assert_eq!(core.text(), "# Plan\n");
        assert!(core.is_idle());
    }

    #[test]
    fn inserts_stop_at_the_cap_and_deletes_go_on() {
        let (mut core, mut log) = created();
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let chunk = "z".repeat(60 << 10);
        let mut n = 0;
        loop {
            let update = crdt::insert(&client, 0, &chunk);
            match core.edit(&ana(), ed(1, &update), n) {
                None => {}
                Some((_, DocAnswer::Refused(Refusal::Full))) => break,
                other => panic!("{:?}", other.map(|o| o.1)),
            }
            write(&mut core, &mut log);
            n += 1;
            assert!(n < 10);
        }
        assert!(core.size() <= MAX_STATE);
        let client = crdt::doc(10);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let delete = crdt::remove(&client, 0, 1000);
        assert!(core.edit(&ana(), ed(2, &delete), 99).is_none(), "a delete past the cap is taken");
    }

    /// I10: while the disk is slow, what waits to be written stays under `MAX_UNWRITTEN`, and the session is told busy.
    #[test]
    fn past_the_unwritten_limit_an_edit_is_told_busy() {
        let (mut core, _) = created();
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let chunk = "w".repeat(100 << 10);
        for n in 0.. {
            assert!(n < 20, "never busy");
            let update = crdt::insert(&client, 0, &chunk);
            if let Some((_, answer)) = core.edit(&ana(), ed(1, &update), n) {
                assert_eq!(answer, DocAnswer::Busy { retry_after_ms: BUSY_MS });
                break;
            }
            let delete = crdt::remove(&client, 0, crdt::len(&client));
            if let Some((_, answer)) = core.edit(&ana(), ed(1, &delete), n) {
                assert_eq!(answer, DocAnswer::Busy { retry_after_ms: BUSY_MS });
                break;
            }
        }
        assert!(core.pipeline.unwritten() <= MAX_UNWRITTEN + MAX_UPDATE + MAX_STATE);
    }

    /// Every publish stays within the stream's limits, and the stream's records always rebuild the document (I14).
    #[test]
    fn compaction_keeps_the_stream_within_its_limits() {
        let (mut core, mut log) = created();
        let mut table: std::collections::BTreeMap<Bytes, Bytes> = Default::default();
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        for i in 0..3000u32 {
            let text = if i % 50 == 0 { "w".repeat(9000) } else { format!("{i} ") };
            let at = crdt::len(&client);
            let update =
                if i % 7 == 6 { crdt::remove(&client, 0, 3.min(at)) } else { crdt::insert(&client, at, &text) };
            match core.edit(&ana(), ed(1, &update), i) {
                None => {}
                Some((_, DocAnswer::Refused(Refusal::Full))) => break,
                Some(other) => panic!("{:?}", other.1),
            }
            if i % 5 == 0 {
                let (published, _) = write(&mut core, &mut log);
                for record in published {
                    let (key, value) = split_keyed(&record).unwrap();
                    if value.is_empty() {
                        table.remove(&key);
                    } else {
                        table.insert(key, value);
                    }
                }
                let bytes: usize = table.iter().map(|(k, v)| record_bytes(k, v)).sum();
                assert!(bytes <= STREAM_BYTES && table.len() <= STREAM_KEYS, "{bytes} B in {} keys", table.len());
            }
        }
        let (published, _) = write(&mut core, &mut log);
        for record in published {
            let (key, value) = split_keyed(&record).unwrap();
            table.insert(key, value);
        }
        table.retain(|_, v| !v.is_empty());
        let updates: Vec<&[u8]> = table
            .iter()
            .filter(|(k, _)| &k[..] != b"acl")
            .map(|(k, v)| if &k[..] == b"base" { &v[..] } else { &v[8..] })
            .collect();
        let rebuilt = crdt::doc(1);
        crdt::apply(&rebuilt, &crdt::merge(&updates).unwrap().0).unwrap();
        assert_eq!(crdt::text(&rebuilt), core.text());
        let reloaded = DocCore::<u32>::load(2, log, true).unwrap();
        assert_eq!(reloaded.text(), core.text());
    }

    #[test]
    fn sharing_is_journaled_and_published() {
        let (mut core, mut log) = created();
        let change = Change::Member("ben".into(), Level::View);
        assert!(core.share(&ana(), &change, 1).is_none());
        let (published, answers) = write(&mut core, &mut log);
        assert_eq!(answers, [(1, DocAnswer::Done)]);
        let (key, value) = split_keyed(&published[0]).unwrap();
        assert_eq!(&key[..], b"acl");
        assert_eq!(Acl::decode(&value).unwrap().members["ben"].level, Level::View);
        assert_eq!(core.share(&ana(), &change, 2), Some((2, DocAnswer::Done)), "no change");
        let ben = Principal::User("ben".into());
        assert_eq!(core.share(&ben, &Change::Link(Level::Edit), 3), Some((3, DocAnswer::Refused(Refusal::Forbidden))));
        assert!(core.may_purge(&ana()).is_err(), "not in the trash");
        assert!(core.share(&ana(), &Change::Trash(9), 4).is_none());
        write(&mut core, &mut log);
        assert_eq!(core.join(&ben, 5), Some((5, DocAnswer::Refused(Refusal::Trashed))));
        assert!(core.may_purge(&ana()).is_ok());
        assert!(core.may_purge(&ben).is_err());
        let reloaded = DocCore::<u32>::load(2, log, true).unwrap();
        assert_eq!(reloaded.acl().unwrap().trashed_ms, Some(9));
    }

    /// Journey 7 at the core: a restore is a forward edit, so the offline peer's sentence stays, the restored-away text
    /// never comes back, and the list of versions only grows (I7).
    #[test]
    fn a_restore_keeps_what_an_offline_peer_typed_and_the_versions_only_grow() {
        let mut core = DocCore::load(1, Vec::new(), false).unwrap();
        core.set_versions("d1", Vec::new()).unwrap();
        let (mut log, mut others) = (Vec::new(), Others::new());
        assert!(core.create("ana", "# Plan\n\nIntro.\n", 5, 0).is_none());
        write_all(&mut core, &mut log, &mut others);
        assert!(core.name_version(&ana(), "10:40", 10, 1).is_none());
        let (_, answers) = write_all(&mut core, &mut log, &mut others);
        let DocAnswer::Version(first, _) = &answers[0].1 else { panic!("{answers:?}") };
        assert_eq!((first.n, first.kind, first.name.as_str()), (1, Kind::Named, "10:40"));
        let (ana_client, ben_client) = (crdt::doc(7), crdt::doc(8));
        crdt::apply(&ana_client, &crdt::state(&core.doc)).unwrap();
        let draft = crdt::insert(&ana_client, crdt::len(&ana_client), "## Draft\nOld idea.\n");
        assert!(core.edit(&ana(), Edit { now_ms: 20, ..ed(1, &draft) }, 2).is_none());
        write_all(&mut core, &mut log, &mut others);
        // Ben goes offline with the draft, and types after "Old idea.".
        crdt::apply(&ben_client, &crdt::state(&core.doc)).unwrap();
        let ben_update = crdt::insert(&ben_client, crdt::len(&ben_client) - 1, " Better idea.");
        let old = &others[&versions::body_key("d1", 1)];
        let old = std::str::from_utf8(old).unwrap().to_string();
        assert_eq!(
            core.version(&Principal::User("cid".into()), 1, Action::History),
            Err(DocAnswer::Refused(Refusal::Forbidden))
        );
        let viewer = Principal::Guest { name: "cleo".into(), link_version: 0 };
        assert!(core.restore(&viewer, 1, &old, 30, 3).is_some(), "a guest cannot restore");
        assert!(core.restore(&ana(), 1, &old, 30, 4).is_none());
        let (published, answers) = write_all(&mut core, &mut log, &mut others);
        assert!(matches!(&answers[0], (4, DocAnswer::Version(v, _)) if v.kind == Kind::BeforeRestore && v.n == 2));
        assert_eq!(core.text(), "# Plan\n\nIntro.\n");
        assert!(
            published
                .iter()
                .any(|r| split_keyed(r).is_some_and(|(k, v)| &k[..] == b"r" && v[..] == 1u64.to_be_bytes()))
        );
        let ben = Principal::User("ben".into());
        assert!(core.share(&ana(), &Change::Member("ben".into(), Level::Edit), 5).is_none());
        write_all(&mut core, &mut log, &mut others);
        let back = Edit { offline: true, now_ms: 40, ..ed(2, &ben_update) };
        assert!(core.edit(&ben, back, 6).is_none());
        write_all(&mut core, &mut log, &mut others);
        assert_eq!(core.text(), "# Plan\n\nIntro.\n Better idea.");
        let kinds: Vec<Kind> = core.versions().list().iter().map(|v| v.kind).collect();
        assert_eq!(kinds, [Kind::Named, Kind::BeforeRestore]);
        // The log rebuilds the text and the list.
        let mut again = DocCore::<u32>::load(2, log, true).unwrap();
        let index = others[&versions::index_key("d1")].clone();
        let at = Address { epoch: 1, seq: 1 };
        again.set_versions("d1", vec![Entry::Record { at, body: index }]).unwrap();
        assert_eq!((again.text(), again.versions()), (core.text(), core.versions()));
        assert_eq!(others.len(), 3, "the list and two versions' Markdown");
    }

    /// On the wall clock too: a new document's first version waits for the edits to pause.
    #[test]
    fn the_first_version_waits_for_a_pause_on_the_wall_clock() {
        let (mut core, mut log, t, min) =
            (DocCore::load(1, Vec::new(), false).unwrap(), Vec::new(), 1_760_000_000_000, 60_000);
        assert!(core.create("ana", "# Plan\n", t, 0).is_none());
        write(&mut core, &mut log);
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let update = crdt::insert(&client, 0, "x");
        assert!(core.edit(&ana(), Edit { now_ms: t + min, ..ed(1, &update) }, 1).is_none());
        assert_eq!(core.auto_version(t + 2 * min), Some(t + 11 * min), "10 minutes after the edit, not at once");
    }

    #[test]
    fn an_automatic_version_waits_for_a_pause_and_a_change() {
        let (mut core, mut log) = created();
        assert_eq!(core.auto_version(0), None, "nothing edited");
        let client = crdt::doc(9);
        crdt::apply(&client, &crdt::state(&core.doc)).unwrap();
        let min = 60_000;
        let update = crdt::insert(&client, 0, "x");
        assert!(core.edit(&ana(), Edit { now_ms: min, ..ed(1, &update) }, 1).is_none());
        let next = core.auto_version(2 * min).expect("not yet");
        assert_eq!(next, 11 * min, "10 minutes after the edit");
        assert_eq!(core.auto_version(next), None);
        write(&mut core, &mut log);
        assert_eq!(core.versions().list().len(), 1);
        assert_eq!(core.versions().list()[0].authors, ["ana"]);
        // Edits that come back to the same text save nothing.
        for update in [crdt::insert(&client, 0, "y"), crdt::remove(&client, 0, 1)] {
            assert!(core.edit(&ana(), Edit { now_ms: 20 * min, ..ed(1, &update) }, 2).is_none());
        }
        write(&mut core, &mut log);
        assert_eq!(core.auto_version(40 * min), None);
        assert_eq!(core.versions().list().len(), 1);
    }

    #[test]
    fn an_import_replaces_the_text_after_a_version_and_respects_the_cap() {
        let (mut core, mut log) = created();
        let viewer = Principal::User("ben".into());
        assert_eq!(core.replace(&viewer, "x", 1, 1), Some((1, DocAnswer::Refused(Refusal::Forbidden))));
        let big = "y".repeat(MAX_STATE / 2 + 1);
        assert_eq!(core.replace(&ana(), &big, 1, 2), Some((2, DocAnswer::Refused(Refusal::Full))));
        assert!(core.replace(&ana(), "# New\n\nText 😀\n", 1, 3).is_none());
        let (_, answers) = write(&mut core, &mut log);
        assert!(matches!(&answers[0], (3, DocAnswer::Version(v, _)) if v.kind == Kind::BeforeImport));
        assert_eq!(core.text(), "# New\n\nText 😀\n");
        assert_eq!(DocCore::<u32>::load(2, log, true).unwrap().text(), core.text());
    }
}
