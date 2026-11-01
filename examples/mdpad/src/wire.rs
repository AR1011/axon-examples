//! Bytes on the wire: the websocket frames (design §4.3) and the `DocP` contract between a web node and a document,
//! both in lib0's variable-length integers, as y-protocols writes them.

use axon_cluster::{CallError, WireProtocol};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::core::acl::{Acl, Change, Level, Principal};
use crate::core::versions::Version;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

impl std::fmt::Display for Malformed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("malformed frame")
    }
}

impl std::error::Error for Malformed {}

#[derive(Default)]
pub struct Writer(Vec<u8>);

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn u8(&mut self, b: u8) -> &mut Self {
        self.0.push(b);
        self
    }

    /// lib0 `writeVarUint`: seven bits a byte, low first.
    pub fn uint(&mut self, mut n: u64) -> &mut Self {
        while n >= 0x80 {
            self.0.push(n as u8 | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
        self
    }

    /// lib0 `writeVarUint8Array`.
    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.uint(b.len() as u64);
        self.0.extend_from_slice(b);
        self
    }

    pub fn str(&mut self, s: &str) -> &mut Self {
        self.bytes(s.as_bytes())
    }

    pub fn raw(&mut self, b: &[u8]) -> &mut Self {
        self.0.extend_from_slice(b);
        self
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self(b)
    }

    pub fn u8(&mut self) -> Result<u8, Malformed> {
        let (&b, rest) = self.0.split_first().ok_or(Malformed)?;
        self.0 = rest;
        Ok(b)
    }

    pub fn uint(&mut self) -> Result<u64, Malformed> {
        let mut n = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            n |= u64::from(b & 0x7F).checked_shl(shift).ok_or(Malformed)?;
            if b < 0x80 {
                return Ok(n);
            }
        }
        Err(Malformed)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8], Malformed> {
        let len = usize::try_from(self.uint()?).map_err(|_| Malformed)?;
        if len > self.0.len() {
            return Err(Malformed);
        }
        let (b, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(b)
    }

    pub fn string(&mut self) -> Result<String, Malformed> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| Malformed)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    pub fn end(&self) -> Result<(), Malformed> {
        if self.0.is_empty() { Ok(()) } else { Err(Malformed) }
    }
}

/// Close codes (design §4.3): the client reconnects after 4429 and 4503 only.
pub mod close {
    pub const UNAUTHORIZED: u16 = 4401;
    pub const FORBIDDEN: u16 = 4403;
    pub const NOT_FOUND: u16 = 4404;
    pub const TOO_LARGE: u16 = 4413;
    pub const LIMITED: u16 = 4429;
    pub const UNAVAILABLE: u16 = 4503;
}

pub const SYNC: u8 = 0;
pub const AWARENESS: u8 = 1;
pub const HELLO: u8 = 16;
pub const WELCOME: u8 = 17;
pub const EDIT: u8 = 18;
pub const SAVED: u8 = 19;
pub const UPDATE: u8 = 20;
pub const NOTICE: u8 = 21;
/// y-protocols sync sub-types.
pub const STEP1: u8 = 0;
pub const STEP2: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientFrame {
    /// The first frame: where the client's copy stands, if it was here before.
    Hello { resume: Option<(u64, u64)> },
    /// `offline` counts the client's own changes the update carries from while it was away, 0 for a live edit.
    Edit { seq: u64, offline: u64, update: Vec<u8> },
    /// A y-protocols awareness update: this client's cursor.
    Awareness(Vec<u8>),
}

impl ClientFrame {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            Self::Hello { resume: None } => w.u8(HELLO).u8(0),
            Self::Hello { resume: Some((generation, seq)) } => w.u8(HELLO).u8(1).uint(*generation).uint(*seq),
            Self::Edit { seq, offline, update } => w.u8(EDIT).uint(*seq).uint(*offline).bytes(update),
            Self::Awareness(update) => w.u8(AWARENESS).bytes(update),
        };
        w.into_vec()
    }

    pub fn decode(frame: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(frame);
        let frame = match r.u8()? {
            HELLO => match r.u8()? {
                0 => Self::Hello { resume: None },
                1 => Self::Hello { resume: Some((r.uint()?, r.uint()?)) },
                _ => return Err(Malformed),
            },
            EDIT => Self::Edit { seq: r.uint()?, offline: r.uint()?, update: r.bytes()?.to_vec() },
            AWARENESS => Self::Awareness(r.bytes()?.to_vec()),
            _ => return Err(Malformed),
        };
        r.end()?;
        Ok(frame)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    pub level: Level,
    pub user: String,
    pub colour: String,
    pub max_update: usize,
    pub size: usize,
    pub max_size: usize,
    /// The server went on from the client's position: it sends its unsaved edits again.
    pub resumed: bool,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Notice {
    /// The edit in flight was not taken: send it again after this.
    Busy {
        retry_after_ms: u64,
    },
    Size {
        bytes: usize,
        max: usize,
    },
    /// The edit in flight was refused: the document is at its size cap. Deletes are still taken.
    Full,
    /// The home cannot write its disk: the edit in flight was not taken, and a `busy` follows.
    CannotSave,
    /// The edit in flight needed something the server lacks: the state follows, then send what it lacks.
    Resync,
    /// The document's home is out of reach: keep the edits, the server joins again.
    Unreachable,
    /// The home is back.
    Reachable,
    /// How many viewers and guests have the document open, on every web node: they show no cursor.
    Viewers {
        count: u64,
    },
    /// Someone restored version `version`: the text changed as one edit.
    Restored {
        version: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerFrame {
    /// The document's state during a join.
    Step2(Vec<u8>),
    /// The server's state vector: the client sends what it lacks as edits.
    Step1(Vec<u8>),
    Welcome(Welcome),
    Saved {
        seq: u64,
    },
    /// A live record at `seq` of `generation`, empty for one the client needs only the position of.
    Update {
        generation: u64,
        seq: u64,
        update: Vec<u8>,
    },
    Notice(Notice),
    /// A y-protocols awareness update: the others' cursors, with the names and colours the server set.
    Awareness(Vec<u8>),
}

impl ServerFrame {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            Self::Step2(update) => w.u8(SYNC).u8(STEP2).bytes(update),
            Self::Step1(sv) => w.u8(SYNC).u8(STEP1).bytes(sv),
            Self::Welcome(welcome) => w.u8(WELCOME).str(&serde_json::to_string(welcome).expect("plain struct")),
            Self::Saved { seq } => w.u8(SAVED).uint(*seq),
            Self::Update { generation, seq, update } => w.u8(UPDATE).uint(*generation).uint(*seq).bytes(update),
            Self::Notice(notice) => w.u8(NOTICE).str(&serde_json::to_string(notice).expect("plain enum")),
            Self::Awareness(update) => w.u8(AWARENESS).bytes(update),
        };
        w.into_vec()
    }

    pub fn decode(frame: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(frame);
        let json = |r: &mut Reader<'_>| r.bytes().map(<[u8]>::to_vec);
        let frame = match r.u8()? {
            SYNC => match r.u8()? {
                STEP1 => Self::Step1(r.bytes()?.to_vec()),
                STEP2 => Self::Step2(r.bytes()?.to_vec()),
                _ => return Err(Malformed),
            },
            WELCOME => Self::Welcome(serde_json::from_slice(&json(&mut r)?).map_err(|_| Malformed)?),
            SAVED => Self::Saved { seq: r.uint()? },
            UPDATE => Self::Update { generation: r.uint()?, seq: r.uint()?, update: r.bytes()?.to_vec() },
            NOTICE => Self::Notice(serde_json::from_slice(&json(&mut r)?).map_err(|_| Malformed)?),
            AWARENESS => Self::Awareness(r.bytes()?.to_vec()),
            _ => return Err(Malformed),
        };
        r.end()?;
        Ok(frame)
    }
}

/// One client's entry of a y-protocols awareness update. `state` is its JSON, `None` for one that left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aware {
    pub client: u64,
    pub clock: u64,
    pub state: Option<String>,
}

impl Aware {
    pub fn write(&self, w: &mut Writer) {
        w.uint(self.client).uint(self.clock).str(self.state.as_deref().unwrap_or("null"));
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        let (client, clock, state) = (r.uint()?, r.uint()?, r.string()?);
        Ok(Self { client, clock, state: (state != "null").then_some(state) })
    }

    /// y-protocols `encodeAwarenessUpdate`.
    pub fn encode_update(entries: &[Aware]) -> Vec<u8> {
        let mut w = Writer::new();
        w.uint(entries.len() as u64);
        for entry in entries {
            entry.write(&mut w);
        }
        w.into_vec()
    }

    pub fn decode_update(update: &[u8]) -> Result<Vec<Aware>, Malformed> {
        let mut r = Reader::new(update);
        let n = r.uint()?;
        // Each entry takes at least three bytes: a count past that is not one.
        if n > update.len() as u64 / 3 {
            return Err(Malformed);
        }
        let entries = (0..n).map(|_| Self::read(&mut r)).collect::<Result<_, _>>()?;
        r.end()?;
        Ok(entries)
    }
}

/// What a web node asks a document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocReq {
    /// Starts the document if it is not running, and says where its stream stands and what `who` may do.
    Join {
        who: Principal,
    },
    Edit {
        session: u64,
        who: Principal,
        update: Vec<u8>,
        offline: bool,
    },
    /// Gives a new document its first state. Answered `Done` and changes nothing when it has one.
    Create {
        owner: String,
        markdown: String,
    },
    Text {
        who: Principal,
    },
    Info {
        who: Principal,
    },
    Share {
        who: Principal,
        change: Change,
    },
    /// Deletes a trashed document for good.
    Purge {
        who: Principal,
    },
    /// A told presence of a session on web node `node`: an editor's cursor entry, or `None` for a viewer, counted.
    /// Sent again every 15 s while the session lasts.
    Aware {
        session: u64,
        node: String,
        entry: Option<Aware>,
    },
    /// Told when a session ends.
    Leave {
        session: u64,
    },
    /// The list of versions.
    Versions {
        who: Principal,
    },
    /// One version, with its Markdown.
    Version {
        who: Principal,
        n: u64,
    },
    /// Saves the text as it stands as a version with a name.
    NameVersion {
        who: Principal,
        name: String,
    },
    /// Makes the text version `n`'s, as a forward edit, after a "before restore" version.
    Restore {
        who: Principal,
        n: u64,
    },
    /// Makes the text `markdown`, as a forward edit, after a "before import" version.
    Replace {
        who: Principal,
        markdown: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Forbidden,
    Trashed,
    NotFound,
    /// The update needs something the document lacks, or is not one.
    Resync,
    /// At the size cap.
    Full,
    TooLarge,
}

impl Refusal {
    const ALL: [Self; 6] = [Self::Forbidden, Self::Trashed, Self::NotFound, Self::Resync, Self::Full, Self::TooLarge];
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocInfo {
    pub acl: Acl,
    pub level: Level,
    pub title: String,
    pub size: usize,
    pub max_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocAnswer {
    Joined {
        generation: u64,
        level: Level,
        size: usize,
        max_size: usize,
    },
    /// On disk and published.
    Saved {
        size: usize,
    },
    Busy {
        retry_after_ms: u64,
    },
    Refused(Refusal),
    Text(String),
    Info(Box<DocInfo>),
    Done,
    /// The home cannot write its disk: the edit was not taken. Send it again after the wait.
    CannotSave {
        retry_after_ms: u64,
    },
    Versions(Vec<Version>),
    /// A version, with its Markdown; empty for a version just saved.
    Version(Box<Version>, String),
}

impl DocReq {
    pub fn write(&self, w: &mut Writer) {
        match self {
            Self::Join { who } => {
                w.u8(0);
                who.write(w);
            }
            Self::Edit { session, who, update, offline } => {
                w.u8(1).uint(*session);
                who.write(w);
                w.u8(*offline as u8).bytes(update);
            }
            Self::Create { owner, markdown } => {
                w.u8(2).str(owner).str(markdown);
            }
            Self::Text { who } => {
                w.u8(3);
                who.write(w);
            }
            Self::Info { who } => {
                w.u8(4);
                who.write(w);
            }
            Self::Share { who, change } => {
                w.u8(5);
                who.write(w);
                change.write(w);
            }
            Self::Purge { who } => {
                w.u8(6);
                who.write(w);
            }
            Self::Aware { session, node, entry } => {
                w.u8(7).uint(*session).str(node);
                match entry {
                    None => w.u8(0),
                    Some(entry) => {
                        w.u8(1);
                        entry.write(w);
                        w
                    }
                };
            }
            Self::Leave { session } => {
                w.u8(8).uint(*session);
            }
            Self::Versions { who } => {
                w.u8(9);
                who.write(w);
            }
            Self::Version { who, n } => {
                w.u8(10);
                who.write(w);
                w.uint(*n);
            }
            Self::NameVersion { who, name } => {
                w.u8(11);
                who.write(w);
                w.str(name);
            }
            Self::Restore { who, n } => {
                w.u8(12);
                who.write(w);
                w.uint(*n);
            }
            Self::Replace { who, markdown } => {
                w.u8(13);
                who.write(w);
                w.str(markdown);
            }
        }
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        Ok(match r.u8()? {
            0 => Self::Join { who: Principal::read(r)? },
            1 => {
                let session = r.uint()?;
                let who = Principal::read(r)?;
                let offline = r.u8()? != 0;
                Self::Edit { session, who, offline, update: r.bytes()?.to_vec() }
            }
            2 => Self::Create { owner: r.string()?, markdown: r.string()? },
            3 => Self::Text { who: Principal::read(r)? },
            4 => Self::Info { who: Principal::read(r)? },
            5 => Self::Share { who: Principal::read(r)?, change: Change::read(r)? },
            6 => Self::Purge { who: Principal::read(r)? },
            7 => Self::Aware {
                session: r.uint()?,
                node: r.string()?,
                entry: match r.u8()? {
                    0 => None,
                    1 => Some(Aware::read(r)?),
                    _ => return Err(Malformed),
                },
            },
            8 => Self::Leave { session: r.uint()? },
            9 => Self::Versions { who: Principal::read(r)? },
            10 => Self::Version { who: Principal::read(r)?, n: r.uint()? },
            11 => Self::NameVersion { who: Principal::read(r)?, name: r.string()? },
            12 => Self::Restore { who: Principal::read(r)?, n: r.uint()? },
            13 => Self::Replace { who: Principal::read(r)?, markdown: r.string()? },
            _ => return Err(Malformed),
        })
    }

    pub fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        self.write(&mut w);
        w.into_vec().into()
    }

    pub fn decode(body: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(body);
        let req = Self::read(&mut r)?;
        r.end()?;
        Ok(req)
    }
}

impl DocAnswer {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            Self::Joined { generation, level, size, max_size } => {
                w.u8(0).uint(*generation).u8(*level as u8).uint(*size as u64).uint(*max_size as u64)
            }
            Self::Saved { size } => w.u8(1).uint(*size as u64),
            Self::Busy { retry_after_ms } => w.u8(2).uint(*retry_after_ms),
            Self::Refused(why) => w.u8(3).u8(*why as u8),
            Self::Text(text) => w.u8(4).str(text),
            Self::Info(info) => {
                w.u8(5);
                info.acl.write(&mut w);
                w.u8(info.level as u8).str(&info.title).uint(info.size as u64).uint(info.max_size as u64)
            }
            Self::Done => w.u8(6),
            Self::CannotSave { retry_after_ms } => w.u8(7).uint(*retry_after_ms),
            Self::Versions(list) => {
                w.u8(8).uint(list.len() as u64);
                for v in list {
                    v.write(&mut w);
                }
                &mut w
            }
            Self::Version(v, markdown) => {
                w.u8(9);
                v.write(&mut w);
                w.str(markdown)
            }
        };
        w.into_vec().into()
    }

    pub fn decode(body: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(body);
        let answer = match r.u8()? {
            0 => Self::Joined {
                generation: r.uint()?,
                level: Level::from_u8(r.u8()?)?,
                size: r.uint()? as usize,
                max_size: r.uint()? as usize,
            },
            1 => Self::Saved { size: r.uint()? as usize },
            2 => Self::Busy { retry_after_ms: r.uint()? },
            3 => Self::Refused(*Refusal::ALL.get(r.u8()? as usize).ok_or(Malformed)?),
            4 => Self::Text(r.string()?),
            5 => Self::Info(Box::new(DocInfo {
                acl: Acl::read(&mut r)?,
                level: Level::from_u8(r.u8()?)?,
                title: r.string()?,
                size: r.uint()? as usize,
                max_size: r.uint()? as usize,
            })),
            6 => Self::Done,
            7 => Self::CannotSave { retry_after_ms: r.uint()? },
            8 => {
                let count = r.uint()?;
                Self::Versions((0..count).map(|_| Version::read(&mut r)).collect::<Result<_, _>>()?)
            }
            9 => Self::Version(Box::new(Version::read(&mut r)?), r.string()?),
            _ => return Err(Malformed),
        };
        r.end()?;
        Ok(answer)
    }
}

/// A web node's calls to a document. The `ActorProtocol` half is in `doc.rs`, beside the actor's mailbox.
pub struct DocP;

impl WireProtocol for DocP {
    type Message = DocReq;
    type Reply = DocAnswer;
    const ID: u64 = 0x4D44_0001;
    // 3: a reader and an operator ask, and publish and a new owner are changes of the sharing.
    const SCHEMA: u16 = 3;

    fn encode(message: DocReq) -> Result<Bytes, CallError> {
        Ok(message.encode())
    }

    fn decode_reply(body: Bytes) -> Result<DocAnswer, CallError> {
        DocAnswer::decode(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_are_lib0s() {
        let mut w = Writer::new();
        w.uint(0).uint(127).uint(128).uint(300).uint(u64::MAX);
        let bytes = w.into_vec();
        assert_eq!(&bytes[..6], [0x00, 0x7F, 0x80, 0x01, 0xAC, 0x02]);
        let mut r = Reader::new(&bytes);
        for n in [0, 127, 128, 300, u64::MAX] {
            assert_eq!(r.uint().unwrap(), n);
        }
        r.end().unwrap();
        assert!(Reader::new(&[0x80]).uint().is_err(), "a cut varint");
        assert!(Reader::new(&[0xFF; 11]).uint().is_err(), "past 64 bits");
        assert!(Reader::new(&[5, 1, 2]).bytes().is_err(), "a length past the end");
    }

    /// The bytes `provider.ts` reads and writes: change them only with it.
    #[test]
    fn frames_are_pinned() {
        assert_eq!(ClientFrame::Hello { resume: None }.encode(), [16, 0]);
        assert_eq!(ClientFrame::Hello { resume: Some((2, 300)) }.encode(), [16, 1, 2, 0xAC, 0x02]);
        assert_eq!(ClientFrame::Edit { seq: 7, offline: 0, update: vec![9, 9] }.encode(), [18, 7, 0, 2, 9, 9]);
        assert_eq!(ServerFrame::Step2(vec![1]).encode(), [0, 1, 1, 1]);
        assert_eq!(ServerFrame::Step1(vec![0]).encode(), [0, 0, 1, 0]);
        assert_eq!(ServerFrame::Saved { seq: 3 }.encode(), [19, 3]);
        let update = ServerFrame::Update { generation: 1, seq: 2, update: vec![] };
        assert_eq!(update.encode(), [20, 1, 2, 0]);
        let busy = ServerFrame::Notice(Notice::Busy { retry_after_ms: 200 }).encode();
        assert_eq!(&busy[2..], br#"{"kind":"busy","retry_after_ms":200}"#);
        for frame in [
            ServerFrame::Welcome(Welcome {
                level: Level::Edit,
                user: "ana".into(),
                colour: "#e0b252".into(),
                max_update: 1,
                size: 2,
                max_size: 3,
                resumed: true,
                generation: 4,
            }),
            ServerFrame::Notice(Notice::Full),
            update,
        ] {
            assert_eq!(ServerFrame::decode(&frame.encode()).unwrap(), frame);
        }
        // y-protocols awareness, as `encodeAwarenessUpdate` writes it: a count, then client, clock and JSON.
        let entries =
            vec![Aware { client: 300, clock: 1, state: Some("{}".into()) }, Aware { client: 2, clock: 5, state: None }];
        let update = Aware::encode_update(&entries);
        assert_eq!(update, [2, 0xAC, 0x02, 1, 2, b'{', b'}', 2, 5, 4, b'n', b'u', b'l', b'l']);
        assert_eq!(Aware::decode_update(&update).unwrap(), entries);
        assert_eq!(ClientFrame::Awareness(update.clone()).encode()[..2], [1, 14]);
        assert_eq!(
            ServerFrame::decode(&ServerFrame::Awareness(update.clone()).encode()).unwrap(),
            ServerFrame::Awareness(update)
        );
        assert!(Aware::decode_update(&[200, 1]).is_err(), "a count past the bytes");
        let restored = ServerFrame::Notice(Notice::Restored { version: 4 }).encode();
        assert_eq!(&restored[2..], br#"{"kind":"restored","version":4}"#);
        let unreachable = ServerFrame::Notice(Notice::Unreachable).encode();
        assert_eq!(&unreachable[2..], br#"{"kind":"unreachable"}"#);
        assert!(ClientFrame::decode(&[18, 1, 0, 5, 1]).is_err());
        assert!(ClientFrame::decode(&[16, 0, 0]).is_err(), "trailing bytes");
    }

    #[test]
    fn doc_calls_are_pinned_and_round_trip() {
        let ana = Principal::User("ana".into());
        let edit = DocReq::Edit { session: 1, who: ana.clone(), update: vec![5], offline: true };
        assert_eq!(edit.encode().as_ref(), [1, 1, 0, 3, b'a', b'n', b'a', 1, 1, 5]);
        let guest = Principal::Guest { name: "eve".into(), link_version: 2 };
        for req in [
            edit,
            DocReq::Join { who: guest.clone() },
            DocReq::Create { owner: "ana".into(), markdown: "# hi".into() },
            DocReq::Text { who: ana.clone() },
            DocReq::Info { who: guest },
            DocReq::Share { who: ana.clone(), change: Change::Member("ben".into(), Level::View) },
            DocReq::Share { who: Principal::Operator("op".into()), change: Change::Owner("ben".into()) },
            DocReq::Share { who: ana.clone(), change: Change::Publish(true) },
            DocReq::Text { who: Principal::Reader },
            DocReq::Purge { who: ana.clone() },
            DocReq::Aware { session: 9, node: "w1".into(), entry: None },
            DocReq::Aware {
                session: 9,
                node: "w1".into(),
                entry: Some(Aware { client: 4, clock: 2, state: Some("{}".into()) }),
            },
            DocReq::Leave { session: 9 },
            DocReq::Versions { who: ana.clone() },
            DocReq::Version { who: ana.clone(), n: 3 },
            DocReq::NameVersion { who: ana.clone(), name: "draft".into() },
            DocReq::Restore { who: ana.clone(), n: 300 },
            DocReq::Replace { who: ana.clone(), markdown: "# new".into() },
        ] {
            assert_eq!(DocReq::decode(&req.encode()).unwrap(), req);
        }
        assert_eq!(DocAnswer::Saved { size: 300 }.encode().as_ref(), [1, 0xAC, 0x02]);
        assert_eq!(DocReq::Restore { who: ana, n: 2 }.encode().as_ref(), [12, 0, 3, b'a', b'n', b'a', 2]);
        assert_eq!(DocReq::Text { who: Principal::Reader }.encode().as_ref(), [3, 2]);
        let publish = DocReq::Share { who: Principal::Operator("o".into()), change: Change::Publish(true) };
        assert_eq!(publish.encode().as_ref(), [5, 3, 1, b'o', 8, 1]);
        let version = Version {
            n: 2,
            at_ms: 5,
            kind: crate::core::versions::Kind::BeforeRestore,
            name: String::new(),
            authors: vec!["ben".into()],
            bytes: 7,
            crc: 9,
        };
        assert_eq!(
            DocAnswer::Version(Box::new(version.clone()), "x".into()).encode().as_ref(),
            [9, 2, 5, 2, 0, 1, 3, b'b', b'e', b'n', 7, 9, 1, b'x']
        );
        for answer in [
            DocAnswer::Joined { generation: 3, level: Level::View, size: 1, max_size: 2 },
            DocAnswer::Busy { retry_after_ms: 200 },
            DocAnswer::Refused(Refusal::TooLarge),
            DocAnswer::Text("a😀b".into()),
            DocAnswer::Info(Box::new(DocInfo {
                acl: Acl::new("ana"),
                level: Level::Edit,
                title: "t".into(),
                size: 1,
                max_size: 2,
            })),
            DocAnswer::Done,
            DocAnswer::CannotSave { retry_after_ms: 500 },
            DocAnswer::Versions(vec![version.clone(), version]),
            DocAnswer::Versions(Vec::new()),
        ] {
            assert_eq!(DocAnswer::decode(&answer.encode()).unwrap(), answer);
        }
    }
}
