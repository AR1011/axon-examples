//! One websocket's session, with no I/O: what a client frame asks of the document, what each stream event sends the
//! client, and the edit queue with one call out. The actor in `session.rs` and the model in `tests/sim.rs` both run
//! this, and carry out the [`Effect`]s it returns.

use std::collections::{BTreeMap, VecDeque};

use axon_stream::{Cursor, ResetReason, StartPosition, StreamEvent, split_keyed};
use bytes::Bytes;

use crate::core::acl::{Acl, Level, Principal};
use crate::core::doc::{MAX_STATE, MAX_UPDATE};
use crate::core::presence::{self, Record};
use crate::crdt;
use crate::wire::{Aware, ClientFrame, DocAnswer, DocReq, Notice, Refusal, ServerFrame, Welcome, close};

/// Edits a client may have waiting behind the one out. A client sends one at a time, so more is a broken client.
pub const MAX_EDITS: usize = 16;

/// The close code of a refusal that ends a socket.
pub fn close_code(why: Refusal) -> u16 {
    match why {
        Refusal::Forbidden | Refusal::Trashed => close::FORBIDDEN,
        Refusal::NotFound => close::NOT_FOUND,
        Refusal::TooLarge | Refusal::Full | Refusal::Resync => close::TOO_LARGE,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    Send(ServerFrame),
    /// Call the document; the answer goes to [`SessionCore::answered`], a failed call to [`SessionCore::failed`].
    Call(DocReq),
    /// Replace the subscription with one from here, then call [`SessionCore::subscribed`].
    Subscribe(StartPosition),
    /// Drop the subscription and keep the position: the client's outbox is full.
    Pause,
    Close(u16),
    /// Tell the document this session's presence: an editor's entry, or `None` for a viewer. Then again every 15 s.
    Aware(Option<Aware>),
    /// Replace the presence subscription with a new one from its state, then call [`SessionCore::presence_subscribed`].
    SubscribePresence,
    /// Drop the presence subscription: the client's outbox is full. Its state comes again once it has read half.
    PausePresence,
}

/// What the call out is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Out {
    Join,
    Edit,
}

pub struct SessionCore {
    pub session: u64,
    who: Principal,
    colour: String,
    level: Level,
    /// The client's position, from its hello, until the join answers.
    resume: Option<(u64, u64)>,
    joined: bool,
    out: Option<Out>,
    edits: VecDeque<(u64, u64, Vec<u8>)>,
    /// Tags the current subscription's events; a replaced one's are ignored.
    epoch: u32,
    /// The last position the client was told.
    pub cursor: Option<Cursor>,
    next: Option<StartPosition>,
    paused: bool,
    size_noticed: usize,
    /// The client id this socket speaks for: the first its awareness named.
    client_id: Option<u64>,
    presence_epoch: u32,
    /// The others' entries this client was told, by client id, with their clocks.
    shown: BTreeMap<u64, u64>,
    /// The presence subscription was dropped for a full outbox: it comes again once the client has read half.
    presence_lost: bool,
    /// Each web node's count of viewers and guests, and the sum the client was last told: `None` after a presence
    /// snapshot, so each new state tells the sum, 0 too, and a client that rejoined drops what it was told before.
    viewers: BTreeMap<String, u64>,
    viewers_told: Option<u64>,
    /// `Some` from the join until its subscription has caught up, with the last sharing it replayed: the join checked
    /// access, and only the sharing the replay ends with counts, so a trash and its restore replayed do not close the
    /// socket. A later subscription (after a pause or a reset) checks each sharing at once.
    replayed_acl: Option<Option<Bytes>>,
    /// The client holds a `base` of this generation: its snapshot had one, or one came live and went on. A relay
    /// that took a new generation before the home published its first records has an empty snapshot, and the `base`
    /// then comes live, with the edits the home wrote and never published before it stopped. Dropped as a
    /// compaction's, it left the other clients without such an edit, and so without all their writer typed after:
    /// 3 clients of 600 in 100 SIGKILLs of a home (`scripts/measure.sh kills`).
    based: bool,
}

impl SessionCore {
    pub fn new(session: u64, who: Principal, colour: String) -> Self {
        Self {
            session,
            who,
            colour,
            level: Level::None,
            resume: None,
            joined: false,
            out: None,
            edits: VecDeque::new(),
            epoch: 0,
            cursor: None,
            next: None,
            paused: false,
            size_noticed: 0,
            client_id: None,
            presence_epoch: 0,
            shown: BTreeMap::new(),
            presence_lost: false,
            viewers: BTreeMap::new(),
            viewers_told: None,
            replayed_acl: None,
            based: false,
        }
    }

    pub fn who(&self) -> &Principal {
        &self.who
    }

    pub fn client(&mut self, frame: ClientFrame) -> Vec<Effect> {
        match frame {
            ClientFrame::Hello { .. } if self.joined || self.out == Some(Out::Join) => {
                vec![Effect::Close(close::FORBIDDEN)]
            }
            ClientFrame::Hello { resume } => {
                self.resume = resume;
                self.call(Out::Join)
            }
            ClientFrame::Edit { .. } if !self.joined => vec![],
            ClientFrame::Edit { .. } if self.level < Level::Edit => vec![Effect::Close(close::FORBIDDEN)],
            ClientFrame::Edit { update, .. } if update.len() > MAX_UPDATE => vec![Effect::Close(close::TOO_LARGE)],
            ClientFrame::Edit { .. } if self.edits.len() >= MAX_EDITS => vec![Effect::Close(close::TOO_LARGE)],
            ClientFrame::Edit { seq, offline, update } => {
                self.edits.push_back((seq, offline, update));
                if self.out.is_none() { self.call(Out::Edit) } else { vec![] }
            }
            ClientFrame::Awareness(_) if !self.joined || self.level < Level::Edit => vec![],
            ClientFrame::Awareness(update) => {
                let Ok(entries) = Aware::decode_update(&update) else { return vec![Effect::Close(close::TOO_LARGE)] };
                // A socket speaks for one client id, the first it names: entries of others are dropped.
                let Some(first) = entries.first() else { return vec![] };
                let own = *self.client_id.get_or_insert(first.client);
                let entry = entries.into_iter().find(|e| e.client == own).and_then(|e| self.entry(e));
                entry.map(|e| vec![Effect::Aware(Some(e))]).unwrap_or_default()
            }
        }
    }

    /// The client's entry with the name and colour the server gives it: what it says of itself is not shown.
    fn entry(&self, mut entry: Aware) -> Option<Aware> {
        if let Some(state) = &entry.state {
            let serde_json::Value::Object(mut state) = serde_json::from_str(state).ok()? else { return None };
            let user = serde_json::json!({ "name": self.who.name(), "color": self.colour });
            state.insert("user".into(), user);
            entry.state = Some(serde_json::Value::Object(state).to_string());
        }
        (entry.state.as_ref().map_or(0, String::len) <= presence::MAX_ENTRY).then_some(entry)
    }

    fn call(&mut self, out: Out) -> Vec<Effect> {
        let req = match out {
            Out::Join => DocReq::Join { who: self.who.clone() },
            Out::Edit => {
                let Some((_, offline, update)) = self.edits.front() else { return vec![] };
                DocReq::Edit {
                    session: self.session,
                    who: self.who.clone(),
                    update: update.clone(),
                    offline: *offline > 0,
                }
            }
        };
        self.out = Some(out);
        vec![Effect::Call(req)]
    }

    /// The call out got no answer: ask again. A retried edit that already applied is answered once it is durable.
    pub fn failed(&mut self) -> Vec<Effect> {
        match self.out.take() {
            Some(out) => self.call(out),
            None => vec![],
        }
    }

    pub fn answered(&mut self, answer: DocAnswer) -> Vec<Effect> {
        let Some(out) = self.out.take() else { return vec![] };
        match (out, answer) {
            (Out::Join, DocAnswer::Joined { generation, level, size, max_size }) => {
                let first = !self.joined;
                (self.joined, self.level, self.replayed_acl) = (true, level, Some(None));
                let resume = self.resume.take().filter(|(g, _)| *g == generation);
                let welcome = Welcome {
                    level,
                    user: self.who.name().to_string(),
                    colour: self.colour.clone(),
                    max_update: MAX_UPDATE,
                    size,
                    max_size,
                    resumed: resume.is_some(),
                    generation,
                };
                self.cursor = resume.map(|(generation, sequence)| Cursor { generation, sequence });
                let mut effects =
                    vec![Effect::Send(ServerFrame::Welcome(welcome)), Effect::Subscribe(self.after_cursor())];
                if first {
                    effects.push(Effect::SubscribePresence);
                    // A viewer is counted; an editor is shown once its client says where its cursor is.
                    if level < Level::Edit {
                        effects.push(Effect::Aware(None));
                    }
                }
                effects
            }
            (Out::Join, DocAnswer::Refused(why)) => vec![Effect::Close(close_code(why))],
            (Out::Join, _) => vec![Effect::Close(close::UNAVAILABLE)],
            (Out::Edit, DocAnswer::Saved { size }) => {
                let Some((seq, _, _)) = self.edits.pop_front() else { return vec![] };
                let mut effects = vec![Effect::Send(ServerFrame::Saved { seq })];
                // A notice at 80 % of the cap, and at each 5 % after.
                let step = size * 20 / MAX_STATE;
                if step >= 16 && step != self.size_noticed {
                    effects.push(Effect::Send(ServerFrame::Notice(Notice::Size { bytes: size, max: MAX_STATE })));
                }
                self.size_noticed = step;
                effects.extend(self.call(Out::Edit));
                effects
            }
            (Out::Edit, DocAnswer::Busy { retry_after_ms }) => {
                // Not taken: the client sends it again after the wait.
                self.edits.clear();
                vec![Effect::Send(ServerFrame::Notice(Notice::Busy { retry_after_ms }))]
            }
            (Out::Edit, DocAnswer::CannotSave { retry_after_ms }) => {
                self.edits.clear();
                vec![
                    Effect::Send(ServerFrame::Notice(Notice::CannotSave)),
                    Effect::Send(ServerFrame::Notice(Notice::Busy { retry_after_ms })),
                ]
            }
            (Out::Edit, DocAnswer::Refused(Refusal::Full)) => {
                self.edits.pop_front();
                let mut effects = vec![Effect::Send(ServerFrame::Notice(Notice::Full))];
                effects.extend(self.call(Out::Edit));
                effects
            }
            (Out::Edit, DocAnswer::Refused(Refusal::Resync)) => {
                // The state follows; the client then sends what the server lacks.
                self.edits.clear();
                vec![
                    Effect::Send(ServerFrame::Notice(Notice::Resync)),
                    Effect::Subscribe(StartPosition::SnapshotThenLive),
                ]
            }
            (Out::Edit, DocAnswer::Refused(why)) => vec![Effect::Close(close_code(why))],
            (Out::Edit, _) => vec![Effect::Close(close::UNAVAILABLE)],
        }
    }

    /// A new subscription replaced the old one; returns the epoch to tag its events with.
    pub fn subscribed(&mut self) -> u32 {
        self.epoch = self.epoch.wrapping_add(1);
        (self.next, self.paused) = (None, false);
        self.epoch
    }

    /// `subscribe` refused `After` (the ring moved on, or another generation): the snapshot path.
    pub fn refused(&mut self) -> Vec<Effect> {
        self.cursor = None;
        vec![Effect::Subscribe(StartPosition::SnapshotThenLive)]
    }

    pub fn presence_subscribed(&mut self) -> u32 {
        self.presence_epoch = self.presence_epoch.wrapping_add(1);
        self.presence_lost = false;
        self.presence_epoch
    }

    /// Once the client has read half its outbox: the presence subscription a full one dropped.
    pub fn resume_presence(&self) -> bool {
        self.presence_lost
    }

    /// An event of the presence stream: the others' entries as one awareness update, with a `null` for each that
    /// went. A snapshot replaces what the client was shown. A client that cannot take more loses the subscription
    /// until it has read half, then gets the state again.
    pub fn presence_event(&mut self, epoch: u32, event: Option<StreamEvent>, room: bool) -> Vec<Effect> {
        if epoch != self.presence_epoch || self.presence_lost {
            return vec![];
        }
        let records: Vec<Bytes> = match event {
            None => return vec![Effect::SubscribePresence],
            Some(StreamEvent::Snapshot(_) | StreamEvent::Batch(_)) if !room => {
                self.presence_lost = true;
                return vec![Effect::PausePresence];
            }
            Some(StreamEvent::Snapshot(snapshot)) => {
                // Every entry and count the snapshot lacks went meanwhile.
                let mut gone = std::mem::take(&mut self.shown);
                (self.viewers, self.viewers_told) = (BTreeMap::new(), None);
                let mut entries = self.presence_entries(snapshot.records());
                entries.retain(|e| e.state.is_some());
                for e in &entries {
                    gone.remove(&e.client);
                }
                entries.extend(gone.into_iter().map(|(client, clock)| Aware { client, clock: clock + 1, state: None }));
                return self.show(entries);
            }
            Some(StreamEvent::Batch(batch)) => batch.records().to_vec(),
            Some(_) => return vec![],
        };
        let entries = self.presence_entries(&records);
        self.show(entries)
    }

    fn presence_entries(&mut self, records: &[Bytes]) -> Vec<Aware> {
        let mut entries = Vec::new();
        for record in records {
            let Some((key, value)) = split_keyed(record) else { continue };
            let (session, client, entry) = match presence::read(&key, &value) {
                Some(Record::Entry { session, client, entry }) => (session, client, entry),
                Some(Record::Viewers { node, count: 0 }) => {
                    self.viewers.remove(&node);
                    continue;
                }
                Some(Record::Viewers { node, count }) => {
                    self.viewers.insert(node, count);
                    continue;
                }
                None => continue,
            };
            if session == self.session {
                continue;
            }
            match entry {
                Some(entry) => {
                    self.shown.insert(client, entry.clock);
                    entries.push(entry);
                }
                None => {
                    if let Some(clock) = self.shown.remove(&client) {
                        entries.push(Aware { client, clock: clock + 1, state: None });
                    }
                }
            }
        }
        entries
    }

    /// The others' entries that changed, and the count of viewers when it changed.
    fn show(&mut self, entries: Vec<Aware>) -> Vec<Effect> {
        let mut effects = Vec::new();
        if !entries.is_empty() {
            effects.push(Effect::Send(ServerFrame::Awareness(Aware::encode_update(&entries))));
        }
        let count: u64 = self.viewers.values().sum();
        if self.viewers_told != Some(count) {
            self.viewers_told = Some(count);
            effects.push(Effect::Send(ServerFrame::Notice(Notice::Viewers { count })));
        }
        effects
    }

    /// Once the client has read half its outbox: where a paused subscription goes on.
    pub fn resume(&self) -> Option<StartPosition> {
        self.paused.then(|| self.after_cursor())
    }

    /// After the last position the client was told, or the state when there is none.
    fn after_cursor(&self) -> StartPosition {
        self.cursor.map_or(StartPosition::SnapshotThenLive, StartPosition::After)
    }

    /// `room` says whether the client's outbox takes another batch.
    pub fn event(&mut self, epoch: u32, event: Option<StreamEvent>, room: bool) -> Vec<Effect> {
        if epoch != self.epoch || self.paused {
            return vec![];
        }
        let Some(event) = event else {
            return vec![Effect::Subscribe(self.next.take().unwrap_or(StartPosition::SnapshotThenLive))];
        };
        match event {
            StreamEvent::Snapshot(snapshot) => {
                let through = snapshot.through();
                let mut updates = Vec::new();
                let mut effects = Vec::new();
                self.based = false;
                for record in snapshot.records() {
                    let Some((key, value)) = split_keyed(record) else { continue };
                    match &key[..] {
                        b"acl" => effects.extend(self.check(&value)),
                        b"base" => {
                            self.based = true;
                            updates.push(value);
                        }
                        b"r" => {}
                        _ if value.len() > 8 => updates.push(value.slice(8..)),
                        _ => {}
                    }
                }
                let refs: Vec<&[u8]> = updates.iter().map(|u| &u[..]).collect();
                let Some((state, sv)) = crdt::merge(&refs) else { return vec![Effect::Close(close::UNAVAILABLE)] };
                effects.push(Effect::Send(ServerFrame::Step2(state)));
                effects.push(Effect::Send(ServerFrame::Step1(sv)));
                let (generation, seq) = (through.generation, through.sequence);
                effects.push(Effect::Send(ServerFrame::Update { generation, seq, update: Vec::new() }));
                self.cursor = Some(through);
                effects
            }
            StreamEvent::Batch(_) if !room => {
                self.paused = true;
                vec![Effect::Pause]
            }
            StreamEvent::Batch(batch) => {
                let mut effects = Vec::new();
                for (i, record) in batch.records().iter().enumerate() {
                    let seq = batch.first() + i as u64;
                    let update = match split_keyed(record) {
                        Some((key, value)) if &key[..] == b"acl" => {
                            effects.extend(self.check(&value));
                            Vec::new()
                        }
                        Some((key, value)) if &key[..] == b"r" => {
                            if let Ok(n) = <[u8; 8]>::try_from(&value[..]) {
                                let version = u64::from_be_bytes(n);
                                effects.push(Effect::Send(ServerFrame::Notice(Notice::Restored { version })));
                            }
                            Vec::new()
                        }
                        Some((key, value)) if &key[..] == b"base" && !self.based => {
                            self.based = true;
                            value.to_vec()
                        }
                        Some((key, value))
                            if key.starts_with(b"u/")
                                && value.len() > 8
                                && value[..8] != self.session.to_be_bytes() =>
                        {
                            value[8..].to_vec()
                        }
                        // The session's own update, a compaction's base or a removal: the client needs only the
                        // position.
                        _ => Vec::new(),
                    };
                    effects.push(Effect::Send(ServerFrame::Update { generation: batch.generation(), seq, update }));
                }
                self.cursor = Some(batch.cursor());
                effects
            }
            StreamEvent::ResetRequired(ResetReason::SlowConsumer) => {
                // The branch overflowed while the actor was busy: the ring may still hold what follows.
                self.next = Some(self.after_cursor());
                vec![]
            }
            // Every other end: the state again, which the client merges with no visible change.
            StreamEvent::ResetRequired(_) | StreamEvent::End(_) => {
                self.next = Some(StartPosition::SnapshotThenLive);
                vec![]
            }
            StreamEvent::CaughtUp { .. } => match self.replayed_acl.take().flatten() {
                Some(acl) => self.check(&acl),
                None => vec![],
            },
            StreamEvent::Ready { .. } | StreamEvent::Replacement { .. } | StreamEvent::OriginLost => vec![],
        }
    }

    /// The document's sharing changed: a principal it no longer lets in is closed out.
    fn check(&mut self, acl: &Bytes) -> Vec<Effect> {
        if let Some(replayed) = &mut self.replayed_acl {
            *replayed = Some(acl.clone());
            return vec![];
        }
        let Ok(acl) = Acl::decode(acl) else { return vec![] };
        self.level = if acl.trashed_ms.is_some() { Level::None } else { acl.level(&self.who) };
        if self.level == Level::None { vec![Effect::Close(close::FORBIDDEN)] } else { vec![] }
    }
}

/// One client's side of the protocol, with no I/O: the Rust twin of `ui/src/editor/provider.ts`, for the simulation
/// and `mdpad load`.
pub struct ClientCore {
    pub doc: yrs::Doc,
    /// The last record of the document's stream this copy holds.
    pub position: Option<(u64, u64)>,
    /// After a state: the next update gives the position, with no gap check.
    expect_position: bool,
    /// Edits wait here until a state comes and says what the server lacks.
    awaiting_state: bool,
    connected: bool,
    next_seq: u64,
    /// Unsaved edits, the first out when `sent`.
    outbox: VecDeque<Unsaved>,
    sent: bool,
    /// The server said busy: nothing goes until [`ClientCore::retry`], whatever comes meanwhile.
    busy: bool,
    /// Local changes made while not connected.
    offline: u64,
    pub welcome: Option<Welcome>,
    pub full: bool,
    pub client_id: u64,
    /// The others present, by client id: their clock and state.
    pub presence: BTreeMap<u64, (u64, String)>,
    aware_clock: u64,
    /// Notices the server sent, by kind.
    pub notices: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct Unsaved {
    pub seq: u64,
    pub offline: u64,
    pub update: Vec<u8>,
    /// Local changes merged into this edit: a token's id for a bot.
    pub marks: Vec<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientAct {
    Send(ClientFrame),
    /// A gap or a generation the client did not follow: close and join again.
    Rejoin,
    /// Send the edit out again after this many ms.
    RetryAfter(u64),
    /// These marks are saved.
    Saved(Vec<u64>),
}

impl ClientCore {
    pub fn new(client_id: u64) -> Self {
        Self {
            doc: crdt::doc(client_id),
            position: None,
            expect_position: false,
            awaiting_state: true,
            connected: false,
            next_seq: 1,
            outbox: VecDeque::new(),
            sent: false,
            busy: false,
            offline: 0,
            welcome: None,
            full: false,
            client_id,
            presence: BTreeMap::new(),
            aware_clock: 0,
            notices: BTreeMap::new(),
        }
    }

    /// This client's cursor, as y-protocols awareness says it: `None` when it leaves.
    pub fn aware(&mut self, state: Option<serde_json::Value>) -> ClientFrame {
        self.aware_clock += 1;
        let entry = Aware { client: self.client_id, clock: self.aware_clock, state: state.map(|s| s.to_string()) };
        ClientFrame::Awareness(Aware::encode_update(&[entry]))
    }

    pub fn unsaved(&self) -> usize {
        self.outbox.len()
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// The hello of a new connection.
    pub fn connect(&mut self) -> ClientFrame {
        (self.connected, self.sent, self.busy, self.awaiting_state, self.welcome) = (true, false, false, true, None);
        ClientFrame::Hello { resume: self.position }
    }

    pub fn disconnected(&mut self) {
        (self.connected, self.sent, self.welcome) = (false, false, None);
    }

    /// A change made here, with a mark to know it by once it is saved.
    pub fn local(&mut self, update: Vec<u8>, mark: u64) -> Option<ClientFrame> {
        if !self.connected || self.welcome.is_none() {
            self.offline += 1;
        }
        let offline = u64::from(!self.connected);
        let mergeable = !(self.sent && self.outbox.len() == 1);
        match self.outbox.back_mut() {
            // Merged into the edit waiting behind the one out, up to the largest edit.
            Some(last) if mergeable && last.update.len() + update.len() < MAX_UPDATE / 2 => {
                last.update = yrs::merge_updates_v1([&last.update[..], &update[..]]).expect("updates this doc made");
                last.marks.push(mark);
                last.offline += offline;
            }
            _ => {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.outbox.push_back(Unsaved { seq, offline, update, marks: vec![mark] });
            }
        }
        self.flush()
    }

    /// The wait a busy answer asked for has passed: the first unsaved edit goes again.
    pub fn retry(&mut self) -> Option<ClientFrame> {
        self.busy = false;
        self.flush()
    }

    /// The first unsaved edit, unless one is out, the server's state is still to come, or the server is busy.
    pub fn flush(&mut self) -> Option<ClientFrame> {
        if !self.connected || self.welcome.is_none() || self.awaiting_state || self.sent || self.busy {
            return None;
        }
        let first = self.outbox.front()?;
        self.sent = true;
        Some(ClientFrame::Edit { seq: first.seq, offline: first.offline, update: first.update.clone() })
    }

    pub fn frame(&mut self, frame: ServerFrame) -> Vec<ClientAct> {
        let mut acts = Vec::new();
        if let ServerFrame::Notice(notice) = &frame {
            let kind = serde_json::to_value(notice).ok().and_then(|v| v["kind"].as_str().map(str::to_string));
            *self.notices.entry(kind.unwrap_or_default()).or_default() += 1;
        }
        match frame {
            ServerFrame::Welcome(welcome) => {
                // Resumed: the server goes on from this copy's position, and the unsaved edits go again.
                self.awaiting_state = !welcome.resumed;
                if !welcome.resumed {
                    self.position = None;
                }
                self.welcome = Some(welcome);
            }
            ServerFrame::Step2(update) => crdt::apply_any(&self.doc, &update),
            ServerFrame::Step1(sv) => {
                // Everything the server lacks, this copy's unsaved edits among it, goes as one edit.
                let marks: Vec<u64> = self.outbox.drain(..).flat_map(|u| u.marks).collect();
                if let Some(update) =
                    crdt::diff(&self.doc, &sv).filter(|_| !marks.is_empty() || crdt::ahead(&self.doc, &sv))
                {
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    self.outbox.push_back(Unsaved { seq, offline: self.offline.max(1), update, marks });
                }
                (self.offline, self.sent, self.awaiting_state, self.expect_position) = (0, false, false, true);
            }
            ServerFrame::Update { generation, seq, update } => {
                match self.position {
                    _ if self.expect_position => self.expect_position = false,
                    Some((g, s)) if g == generation && seq == s + 1 => {}
                    _ => return vec![ClientAct::Rejoin],
                }
                self.position = Some((generation, seq));
                if !update.is_empty() {
                    crdt::apply_any(&self.doc, &update);
                }
            }
            ServerFrame::Saved { seq } => {
                let mut marks = Vec::new();
                while self.outbox.front().is_some_and(|u| u.seq <= seq) {
                    marks.extend(self.outbox.pop_front().expect("checked").marks);
                    self.sent = false;
                }
                if !marks.is_empty() {
                    acts.push(ClientAct::Saved(marks));
                }
            }
            ServerFrame::Notice(Notice::Busy { retry_after_ms }) => {
                (self.sent, self.busy) = (false, true);
                return vec![ClientAct::RetryAfter(retry_after_ms)];
            }
            ServerFrame::Notice(Notice::Resync) => (self.sent, self.awaiting_state) = (false, true),
            ServerFrame::Notice(Notice::Full) => {
                self.outbox.pop_front();
                (self.sent, self.full) = (false, true);
            }
            ServerFrame::Notice(_) => {}
            ServerFrame::Awareness(update) => {
                for entry in Aware::decode_update(&update).unwrap_or_default() {
                    match entry.state {
                        Some(state) => drop(self.presence.insert(entry.client, (entry.clock, state))),
                        None => drop(self.presence.remove(&entry.client)),
                    }
                }
            }
        }
        acts.extend(self.flush().map(ClientAct::Send));
        acts
    }
}

#[cfg(test)]
mod tests {
    use axon_stream::{EndReason, SequencedBatch, Snapshot, keyed};

    use super::*;
    use crate::core::doc::SERVER;

    fn ana() -> Principal {
        Principal::User("ana".into())
    }

    fn joined(core: &mut SessionCore, resume: Option<(u64, u64)>, generation: u64) -> Vec<Effect> {
        assert_eq!(core.client(ClientFrame::Hello { resume }), [Effect::Call(DocReq::Join { who: ana() })]);
        core.answered(DocAnswer::Joined { generation, level: Level::Edit, size: 1, max_size: MAX_STATE })
    }

    fn sends(effects: &[Effect]) -> Vec<&ServerFrame> {
        effects.iter().filter_map(|e| if let Effect::Send(f) = e { Some(f) } else { None }).collect()
    }

    #[test]
    fn a_resume_of_the_same_generation_goes_on_and_another_takes_the_state() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        let effects = joined(&mut core, Some((3, 10)), 3);
        assert_eq!(effects[1], Effect::Subscribe(StartPosition::After(Cursor { generation: 3, sequence: 10 })));
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        let effects = joined(&mut core, Some((2, 10)), 3);
        assert!(matches!(sends(&effects)[0], ServerFrame::Welcome(Welcome { resumed: false, generation: 3, .. })));
        assert_eq!(effects[1], Effect::Subscribe(StartPosition::SnapshotThenLive));
        assert_eq!(core.refused(), [Effect::Subscribe(StartPosition::SnapshotThenLive)]);
    }

    #[test]
    fn a_base_after_an_empty_snapshot_goes_to_the_client_and_a_compaction_s_does_not() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        joined(&mut core, None, 2);
        let epoch = core.subscribed();
        let snapshot = Snapshot::new(Cursor { generation: 2, sequence: 0 }, vec![]);
        core.event(epoch, Some(StreamEvent::Snapshot(snapshot)), true);
        let server = crdt::doc(SERVER);
        crdt::insert(&server, 0, "written, never published");
        let born = vec![keyed(b"acl", &Acl::new("ana").encode()), keyed(b"base", &crdt::state(&server))];
        let effects = core.event(epoch, Some(StreamEvent::Batch(SequencedBatch::new(2, 1, born).unwrap())), true);
        let Some(ServerFrame::Update { seq: 2, update, .. }) = sends(&effects).last().copied() else {
            panic!("{effects:?}")
        };
        let client = crdt::doc(5);
        crdt::apply(&client, update).unwrap();
        assert_eq!(crdt::text(&client), "written, never published");
        let compacted = vec![keyed(b"base", &crdt::state(&server))];
        let effects = core.event(epoch, Some(StreamEvent::Batch(SequencedBatch::new(2, 3, compacted).unwrap())), true);
        assert_eq!(sends(&effects), [&ServerFrame::Update { generation: 2, seq: 3, update: vec![] }]);
    }

    #[test]
    fn records_become_frames_with_no_gaps_and_the_own_ones_empty() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        joined(&mut core, None, 1);
        let epoch = core.subscribed();
        let server = crdt::doc(SERVER);
        let base = crdt::state(&server);
        let a = crdt::insert(&server, 0, "a");
        let mut acl = Acl::new("ana");
        let records =
            vec![keyed(b"acl", &acl.encode()), keyed(b"base", &base), keyed(b"u/1", &[&[0; 8][..], &a].concat())];
        let snapshot = Snapshot::new(Cursor { generation: 1, sequence: 3 }, records);
        let effects = core.event(epoch, Some(StreamEvent::Snapshot(snapshot)), true);
        let frames = sends(&effects);
        let ServerFrame::Step2(state) = frames[0] else { panic!("{frames:?}") };
        let client = crdt::doc(5);
        crdt::apply(&client, state).unwrap();
        assert_eq!(crdt::text(&client), "a");
        assert_eq!(frames[2], &ServerFrame::Update { generation: 1, seq: 3, update: vec![] });
        let caught_up = StreamEvent::CaughtUp { through: Cursor { generation: 1, sequence: 3 } };
        assert!(sends(&core.event(epoch, Some(caught_up), true)).is_empty());
        let b = crdt::insert(&server, 1, "b");
        let own = keyed(b"u/2", &[&7u64.to_be_bytes()[..], &b].concat());
        let other = keyed(b"u/3", &[&[1; 8][..], &b].concat());
        let batch = SequencedBatch::new(1, 4, vec![own, other, keyed(b"u/1", b"")]).unwrap();
        let effects = core.event(epoch, Some(StreamEvent::Batch(batch.clone())), true);
        let updates: Vec<(u64, usize)> = sends(&effects)
            .iter()
            .map(|f| match f {
                ServerFrame::Update { seq, update, .. } => (*seq, update.len()),
                f => panic!("{f:?}"),
            })
            .collect();
        assert_eq!(updates, [(4, 0), (5, b.len()), (6, 0)]);
        assert_eq!(core.event(epoch, Some(StreamEvent::Batch(batch)), false), [Effect::Pause]);
        assert_eq!(core.resume(), Some(StartPosition::After(Cursor { generation: 1, sequence: 6 })));
        // A subscription after a pause is no replay behind a join: a revocation in it closes the socket at once.
        let epoch = core.subscribed();
        acl.link = Level::View;
        let shut = keyed(b"acl", &Acl::new("ben").encode());
        let batch = SequencedBatch::new(1, 7, vec![keyed(b"acl", &acl.encode()), shut]).unwrap();
        let effects = core.event(epoch, Some(StreamEvent::Batch(batch)), true);
        assert!(effects.contains(&Effect::Close(close::FORBIDDEN)), "revoked: {effects:?}");
    }

    /// The trash and the restore replay to a client that saw neither: only the sharing at the end of the replay
    /// counts, so a restored document opens.
    #[test]
    fn a_replay_closes_out_only_for_the_sharing_it_ends_with() {
        let acl = Acl::new("ana");
        let mut trash = acl.clone();
        trash.trashed_ms = Some(9);
        let trashed = keyed(b"acl", &trash.encode());
        let replay = |last: Bytes| {
            let mut core = SessionCore::new(7, ana(), "#fff".into());
            joined(&mut core, Some((1, 2)), 1);
            let epoch = core.subscribed();
            let mut effects = Vec::new();
            for (seq, record) in [(3, trashed.clone()), (4, last)] {
                let batch = SequencedBatch::new(1, seq, vec![record]).unwrap();
                effects.extend(core.event(epoch, Some(StreamEvent::Batch(batch)), true));
            }
            assert!(!effects.contains(&Effect::Close(close::FORBIDDEN)), "not before the replay ends: {effects:?}");
            let caught_up = StreamEvent::CaughtUp { through: Cursor { generation: 1, sequence: 4 } };
            core.event(epoch, Some(caught_up), true)
        };
        assert_eq!(replay(keyed(b"acl", &acl.encode())), [], "restored");
        assert_eq!(replay(trashed.clone()), [Effect::Close(close::FORBIDDEN)], "still in the trash");
    }

    #[test]
    fn the_client_is_told_the_viewers_of_every_web_node_when_their_sum_changes() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        joined(&mut core, None, 1);
        let epoch = core.presence_subscribed();
        let count = |node: &str, n: u64| {
            let mut w = crate::wire::Writer::new();
            w.uint(n);
            keyed(format!("v/{node}").as_bytes(), &w.into_vec())
        };
        let told = |effects: Vec<Effect>| -> Vec<u64> {
            sends(&effects)
                .iter()
                .filter_map(|f| match f {
                    ServerFrame::Notice(Notice::Viewers { count }) => Some(*count),
                    _ => None,
                })
                .collect()
        };
        let snapshot = Snapshot::new(Cursor { generation: 1, sequence: 2 }, vec![count("w1", 3), count("w2", 2)]);
        assert_eq!(told(core.presence_event(epoch, Some(StreamEvent::Snapshot(snapshot)), true)), [5]);
        let batch = SequencedBatch::new(1, 3, vec![count("w1", 3)]).unwrap();
        assert!(told(core.presence_event(epoch, Some(StreamEvent::Batch(batch)), true)).is_empty(), "no change");
        let batch = SequencedBatch::new(1, 4, vec![keyed(b"v/w2", b"")]).unwrap();
        assert_eq!(told(core.presence_event(epoch, Some(StreamEvent::Batch(batch)), true)), [3], "w2 has none");
        let epoch = core.presence_subscribed();
        let snapshot = Snapshot::new(Cursor { generation: 2, sequence: 1 }, vec![]);
        assert_eq!(told(core.presence_event(epoch, Some(StreamEvent::Snapshot(snapshot)), true)), [0], "a new state");
        // A client that rejoins keeps the count its last socket was told until a new session tells it, 0 too.
        let mut core = SessionCore::new(8, ana(), "#fff".into());
        joined(&mut core, None, 1);
        let epoch = core.presence_subscribed();
        let snapshot = Snapshot::new(Cursor { generation: 2, sequence: 1 }, vec![]);
        assert_eq!(told(core.presence_event(epoch, Some(StreamEvent::Snapshot(snapshot)), true)), [0], "rejoined");
    }

    #[test]
    fn every_end_subscribes_again_and_a_stale_epoch_is_ignored() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        joined(&mut core, None, 1);
        let old = core.subscribed();
        let epoch = core.subscribed();
        assert!(core.event(old, None, true).is_empty());
        for (event, start) in [
            (StreamEvent::End(EndReason::StreamClosed), StartPosition::SnapshotThenLive),
            (StreamEvent::ResetRequired(ResetReason::GenerationChanged), StartPosition::SnapshotThenLive),
            (StreamEvent::ResetRequired(ResetReason::Gap), StartPosition::SnapshotThenLive),
        ] {
            assert!(core.event(epoch, Some(event), true).is_empty());
            assert_eq!(core.event(epoch, None, true), [Effect::Subscribe(start)]);
        }
        core.cursor = Some(Cursor { generation: 1, sequence: 9 });
        core.event(epoch, Some(StreamEvent::ResetRequired(ResetReason::SlowConsumer)), true);
        let after = StartPosition::After(Cursor { generation: 1, sequence: 9 });
        assert_eq!(core.event(epoch, None, true), [Effect::Subscribe(after)]);
    }

    #[test]
    fn edits_go_one_at_a_time_and_a_viewer_is_closed_out() {
        let mut core = SessionCore::new(7, ana(), "#fff".into());
        assert!(core.client(ClientFrame::Edit { seq: 1, offline: 0, update: vec![0, 0] }).is_empty(), "not joined");
        joined(&mut core, None, 1);
        let edit = |seq| ClientFrame::Edit { seq, offline: 0, update: vec![0, 0] };
        assert!(matches!(core.client(edit(1))[..], [Effect::Call(DocReq::Edit { .. })]));
        assert!(core.client(edit(2)).is_empty(), "one out");
        assert!(matches!(core.failed()[..], [Effect::Call(DocReq::Edit { .. })]), "asked again");
        let effects = core.answered(DocAnswer::Saved { size: 1 });
        assert_eq!(effects[0], Effect::Send(ServerFrame::Saved { seq: 1 }));
        assert!(matches!(effects[1], Effect::Call(DocReq::Edit { .. })));
        let effects = core.answered(DocAnswer::Busy { retry_after_ms: 200 });
        assert_eq!(effects, [Effect::Send(ServerFrame::Notice(Notice::Busy { retry_after_ms: 200 }))]);
        let mut viewer = SessionCore::new(8, ana(), "#fff".into());
        viewer.client(ClientFrame::Hello { resume: None });
        viewer.answered(DocAnswer::Joined { generation: 1, level: Level::View, size: 1, max_size: 2 });
        assert_eq!(viewer.client(edit(1)), [Effect::Close(close::FORBIDDEN)]);
    }

    /// The client's half: a gap rejoins, a state makes it send what the server lacks, a save acks the marks.
    #[test]
    fn the_client_checks_positions_and_sends_what_the_server_lacks() {
        let server = crdt::doc(SERVER);
        crdt::insert(&server, 0, "hi");
        let mut client = ClientCore::new(5);
        client.connect();
        let welcome = Welcome {
            level: Level::Edit,
            user: "ana".into(),
            colour: String::new(),
            max_update: MAX_UPDATE,
            size: 0,
            max_size: MAX_STATE,
            resumed: false,
            generation: 1,
        };
        assert!(client.frame(ServerFrame::Welcome(welcome)).is_empty());
        let local = crdt::insert(&client.doc, 0, "x");
        assert_eq!(client.local(local, 1), None, "waits for the state");
        client.frame(ServerFrame::Step2(crdt::state(&server)));
        let acts = client.frame(ServerFrame::Step1(crdt::state_vector(&server)));
        let [ClientAct::Send(ClientFrame::Edit { seq: 2, update, .. })] = &acts[..] else { panic!("{acts:?}") };
        crdt::apply(&server, update).unwrap();
        assert_eq!(crdt::text(&server), crdt::text(&client.doc));
        assert!(client.frame(ServerFrame::Update { generation: 1, seq: 5, update: vec![] }).is_empty());
        assert_eq!(client.frame(ServerFrame::Saved { seq: 2 }), [ClientAct::Saved(vec![1])]);
        assert!(client.frame(ServerFrame::Update { generation: 1, seq: 6, update: vec![] }).is_empty());
        assert_eq!(client.frame(ServerFrame::Update { generation: 1, seq: 8, update: vec![] }), [ClientAct::Rejoin]);
        assert_eq!(client.frame(ServerFrame::Update { generation: 2, seq: 7, update: vec![] }), [ClientAct::Rejoin]);
        assert_eq!(client.connect(), ClientFrame::Hello { resume: Some((1, 6)) });
    }

    /// A busy answer holds every edit for its wait: a live update or a local change meanwhile sends nothing.
    #[test]
    fn busy_holds_the_edits_until_the_retry() {
        let mut client = ClientCore::new(5);
        client.connect();
        let welcome = Welcome {
            level: Level::Edit,
            user: "ana".into(),
            colour: String::new(),
            max_update: MAX_UPDATE,
            size: 0,
            max_size: MAX_STATE,
            resumed: false,
            generation: 1,
        };
        client.frame(ServerFrame::Welcome(welcome));
        client.frame(ServerFrame::Step1(crdt::state_vector(&crdt::doc(SERVER))));
        let edit = client.local(crdt::insert(&client.doc, 0, "x"), 1);
        assert!(matches!(edit, Some(ClientFrame::Edit { seq: 1, .. })));
        let busy = ServerFrame::Notice(Notice::Busy { retry_after_ms: 200 });
        assert_eq!(client.frame(busy), [ClientAct::RetryAfter(200)]);
        assert!(client.frame(ServerFrame::Update { generation: 1, seq: 1, update: vec![] }).is_empty());
        assert_eq!(client.local(crdt::insert(&client.doc, 1, "y"), 2), None);
        let Some(ClientFrame::Edit { seq: 1, .. }) = client.retry() else { panic!("the edit goes after the wait") };
    }
}
