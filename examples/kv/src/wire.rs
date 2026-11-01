//! The shard and Log contracts, encoded by hand: fixed ids, `SCHEMA` 1, bytes pinned in a test.

use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::log::{LogMsg, To};
use crate::repl::codec::{get_from_log, get_to_log, put_from_log, put_to_log};
use crate::repl::{FromLog, ToLog};

/// A call to one shard. Every key of a call is in that shard's slots.
#[derive(Clone, Debug, PartialEq)]
pub enum Call {
    Get(Bytes),
    Set(Bytes, Bytes),
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    Len,
    Clear,
    MGet(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
    /// The call itself comes as the body of an upload, encoded: one larger than a frame.
    Upload,
    /// The answer to the call inside, in parts of [`Answer::Chunk`]: one larger than a frame.
    Parts(Box<Call>),
}

impl Call {
    /// Changes the shard: never sent again after an error that says it may have run.
    pub fn is_write(&self) -> bool {
        match self {
            Call::Set(..) | Call::Del(_) | Call::Clear | Call::MSet(_) | Call::Upload => true,
            Call::Parts(inner) => inner.is_write(),
            Call::Get(_) | Call::Exists(_) | Call::Len | Call::MGet(_) => false,
        }
    }

    /// The bytes of `encode()`, without encoding. Also its weight in the shard's mailbox.
    pub fn encoded_len(&self) -> usize {
        let keys = |ks: &[Bytes]| 5 + ks.iter().map(|k| 4 + k.len()).sum::<usize>();
        match self {
            Call::Get(k) => 5 + k.len(),
            Call::Set(k, v) => 9 + k.len() + v.len(),
            Call::Del(ks) | Call::Exists(ks) | Call::MGet(ks) => keys(ks),
            Call::MSet(pairs) => 5 + pairs.iter().map(|(k, v)| 8 + k.len() + v.len()).sum::<usize>(),
            Call::Len | Call::Clear | Call::Upload => 1,
            Call::Parts(inner) => 1 + inner.encoded_len(),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(self.encoded_len());
        self.put(&mut out);
        out.freeze()
    }

    fn put(&self, out: &mut BytesMut) {
        match self {
            Call::Get(k) => {
                out.put_u8(0);
                put_bytes(out, k);
            }
            Call::Set(k, v) => {
                out.put_u8(1);
                put_bytes(out, k);
                put_bytes(out, v);
            }
            Call::Del(ks) => put_keys(out, 2, ks),
            Call::Exists(ks) => put_keys(out, 3, ks),
            Call::Len => out.put_u8(4),
            Call::Clear => out.put_u8(5),
            Call::MGet(ks) => put_keys(out, 6, ks),
            Call::MSet(pairs) => {
                out.put_u8(7);
                out.put_u32(pairs.len() as u32);
                for (k, v) in pairs {
                    put_bytes(out, k);
                    put_bytes(out, v);
                }
            }
            Call::Upload => out.put_u8(8),
            Call::Parts(inner) => {
                out.put_u8(9);
                inner.put(out);
            }
        }
    }

    pub fn decode(body: &mut Bytes) -> Option<Call> {
        Some(match body.try_get_u8().ok()? {
            0 => Call::Get(get_bytes(body)?),
            1 => Call::Set(get_bytes(body)?, get_bytes(body)?),
            2 => Call::Del(get_keys(body)?),
            3 => Call::Exists(get_keys(body)?),
            4 => Call::Len,
            5 => Call::Clear,
            6 => Call::MGet(get_keys(body)?),
            7 => {
                let n = body.try_get_u32().ok()? as usize;
                let mut pairs = Vec::with_capacity(n.min(body.remaining() / 8));
                for _ in 0..n {
                    pairs.push((get_bytes(body)?, get_bytes(body)?));
                }
                Call::MSet(pairs)
            }
            8 => Call::Upload,
            9 => Call::Parts(Box::new(Call::decode(body)?)),
            _ => return None,
        })
    }
}

/// A shard's answer.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    Ok,
    Value(Option<Bytes>),
    Count(u64),
    Values(Vec<Option<Bytes>>),
    /// The answer is this many bytes encoded, more than a frame: ask again with [`Call::Parts`].
    TooLarge(u64),
    /// One part of an answer asked for with [`Call::Parts`]: the parts together are the answer, encoded.
    Chunk(Bytes),
    /// The shard refused the call, with the RESP error to give the client.
    Refused(String),
}

impl Answer {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        match self {
            Answer::Ok => out.put_u8(0),
            Answer::Value(v) => {
                out.reserve(v.as_ref().map_or(0, Bytes::len) + 8);
                out.put_u8(1);
                put_maybe(&mut out, v);
            }
            Answer::Count(n) => {
                out.put_u8(2);
                out.put_u64(*n);
            }
            Answer::Values(vs) => {
                out.reserve(vs.iter().map(|v| v.as_ref().map_or(0, Bytes::len) + 5).sum::<usize>() + 5);
                out.put_u8(3);
                out.put_u32(vs.len() as u32);
                for v in vs {
                    put_maybe(&mut out, v);
                }
            }
            Answer::TooLarge(n) => {
                out.put_u8(4);
                out.put_u64(*n);
            }
            Answer::Chunk(b) => {
                out.reserve(b.len() + 1);
                out.put_u8(5);
                out.put_slice(b);
            }
            Answer::Refused(text) => {
                out.put_u8(6);
                out.put_slice(text.as_bytes());
            }
        }
        out.freeze()
    }

    pub fn decode(mut body: Bytes) -> Option<Answer> {
        let body = &mut body;
        Some(match body.try_get_u8().ok()? {
            0 => Answer::Ok,
            1 => Answer::Value(get_maybe(body)?),
            2 => Answer::Count(body.try_get_u64().ok()?),
            3 => {
                let n = body.try_get_u32().ok()? as usize;
                let mut vs = Vec::with_capacity(n.min(body.remaining()));
                for _ in 0..n {
                    vs.push(get_maybe(body)?);
                }
                Answer::Values(vs)
            }
            4 => Answer::TooLarge(body.try_get_u64().ok()?),
            5 => Answer::Chunk(std::mem::take(body)),
            6 => Answer::Refused(String::from_utf8(std::mem::take(body).to_vec()).ok()?),
            _ => return None,
        })
    }
}

fn put_bytes(out: &mut BytesMut, b: &[u8]) {
    out.put_u32(b.len() as u32);
    out.put_slice(b);
}

fn put_keys(out: &mut BytesMut, tag: u8, keys: &[Bytes]) {
    out.put_u8(tag);
    out.put_u32(keys.len() as u32);
    for k in keys {
        put_bytes(out, k);
    }
}

fn put_maybe(out: &mut BytesMut, v: &Option<Bytes>) {
    match v {
        None => out.put_u8(0),
        Some(v) => {
            out.put_u8(1);
            put_bytes(out, v);
        }
    }
}

fn get_bytes(body: &mut Bytes) -> Option<Bytes> {
    let n = body.try_get_u32().ok()? as usize;
    (body.remaining() >= n).then(|| body.split_to(n))
}

fn get_keys(body: &mut Bytes) -> Option<Vec<Bytes>> {
    let n = body.try_get_u32().ok()? as usize;
    let mut keys = Vec::with_capacity(n.min(body.remaining() / 4));
    for _ in 0..n {
        keys.push(get_bytes(body)?);
    }
    Some(keys)
}

fn get_maybe(body: &mut Bytes) -> Option<Option<Bytes>> {
    match body.try_get_u8().ok()? {
        0 => Some(None),
        1 => get_bytes(body).map(Some),
        _ => None,
    }
}

/// What a shard's mailbox holds.
pub enum ShardMsg {
    Call(Call, Option<Reply<Answer>>),
    /// The body of an upload, read whole by a job, and the reply it waits for.
    Uploaded(u64, Result<Bytes, String>),
    /// A reply in parts ended: the caller has been told.
    Sent,
    /// A Log's answer to a durable shard's owner, by the Log's place in the storage set and the `seq` of the Append
    /// or Install it answers, or why none came.
    Logged(usize, Option<u64>, Result<FromLog, String>),
    /// A durable shard's owner sends again what went unanswered.
    Tick,
}

pub struct ShardApi;

impl WireProtocol for ShardApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0x4B56_0001;
    const SCHEMA: u16 = 1;
    fn encode(call: Call) -> Result<Bytes, CallError> {
        Ok(call.encode())
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        Answer::decode(body).ok_or_else(|| CallError::Codec("a shard answer".into()))
    }
}

impl ActorProtocol for ShardApi {
    type Mailbox = ShardMsg;
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        Ok(answer.encode())
    }
    fn decode(mut body: Bytes, reply: Option<Reply<Answer>>) -> Result<ShardMsg, Rejection> {
        let call = Call::decode(&mut body).ok_or(Rejection::InvalidMessage)?;
        if body.has_remaining() {
            return Err(Rejection::InvalidMessage);
        }
        Ok(ShardMsg::Call(call, reply))
    }
}

/// A shard's owner to a Log on another node. Each holds [`log_body`]: the shard, then its `ToLog`.
#[derive(Clone, Debug, PartialEq)]
pub enum LogCall {
    Plain(Bytes),
    /// The body comes as an upload: an Append or Install larger than a frame.
    Upload,
    /// Answered in parts of the encoded `FromLog`: a `Fetch`, whose answer may hold the shard's whole map.
    Parts(Bytes),
}

pub fn log_body(shard: u16, msg: &ToLog) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u16(shard);
    put_to_log(&mut out, msg);
    out.freeze()
}

pub fn read_log_body(mut body: Bytes) -> Option<(u16, ToLog)> {
    let shard = body.try_get_u16().ok()?;
    let msg = get_to_log(&mut body)?;
    (!body.has_remaining()).then_some((shard, msg))
}

pub fn from_log_bytes(msg: &FromLog) -> Bytes {
    let mut out = BytesMut::new();
    put_from_log(&mut out, msg);
    out.freeze()
}

pub fn read_from_log(mut body: Bytes) -> Option<FromLog> {
    let msg = get_from_log(&mut body)?;
    (!body.has_remaining()).then_some(msg)
}

/// The Log, published as `kv.log`. Its reply is the encoded `FromLog`, whole or in parts.
pub struct LogApi;

impl WireProtocol for LogApi {
    type Message = LogCall;
    type Reply = Bytes;
    const ID: u64 = 0x4B56_0002;
    const SCHEMA: u16 = 1;
    fn encode(call: LogCall) -> Result<Bytes, CallError> {
        let (tag, body) = match call {
            LogCall::Plain(body) => (0, body),
            LogCall::Upload => (1, Bytes::new()),
            LogCall::Parts(body) => (2, body),
        };
        let mut out = BytesMut::with_capacity(1 + body.len());
        out.put_u8(tag);
        out.put_slice(&body);
        Ok(out.freeze())
    }
    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for LogApi {
    type Mailbox = LogMsg;
    fn encode_reply(answer: Bytes) -> Result<Bytes, CallError> {
        Ok(answer)
    }
    fn decode(mut body: Bytes, reply: Option<Reply<Bytes>>) -> Result<LogMsg, Rejection> {
        // An owner always waits for the answer: a tell carries nothing a Log could act on.
        let reply = reply.ok_or(Rejection::InvalidMessage)?;
        let tag = body.try_get_u8().map_err(|_| Rejection::InvalidMessage)?;
        let call = |body| read_log_body(body).ok_or(Rejection::InvalidMessage);
        Ok(match tag {
            0 => {
                let (shard, msg) = call(body)?;
                LogMsg::Call(shard, msg, To::Remote(reply))
            }
            1 if !body.has_remaining() => LogMsg::Upload(reply),
            2 => {
                let (shard, msg) = call(body)?;
                LogMsg::Call(shard, msg, To::Parts(reply))
            }
            _ => return Err(Rejection::InvalidMessage),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::{Ballot, Entry, Map, Op, Opened, Pos, Snapshot};

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn pinned_bytes() {
        let calls = [
            (Call::Get(b("k")), "00000000016b"),
            (Call::Set(b("k"), b("v")), "01000000016b0000000176"),
            (Call::Del(vec![b("a"), b("b")]), "020000000200000001610000000162"),
            (Call::Len, "04"),
            (Call::Clear, "05"),
            (Call::MSet(vec![(b("k"), b("v"))]), "0700000001000000016b0000000176"),
            (Call::Upload, "08"),
            (Call::Parts(Box::new(Call::MGet(vec![b("k")]))), "090600000001000000016b"),
        ];
        for (call, hex) in calls {
            let encoded: String = call.encode().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(encoded, hex, "{call:?}");
            assert_eq!(call.encoded_len(), hex.len() / 2, "{call:?}");
            assert_eq!(Call::decode(&mut call.encode()), Some(call));
        }
        let answers = [
            (Answer::Ok, "00"),
            (Answer::Value(None), "0100"),
            (Answer::Value(Some(b("v"))), "01010000000176"),
            (Answer::Count(2), "020000000000000002"),
            (Answer::Values(vec![None, Some(b("v"))]), "030000000200010000000176"),
            (Answer::TooLarge(5), "040000000000000005"),
            (Answer::Chunk(b("ab")), "056162"),
            (Answer::Refused("ERR x".into()), "064552522078"),
        ];
        for (answer, hex) in answers {
            let encoded: String = answer.encode().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(encoded, hex, "{answer:?}");
            assert_eq!(Answer::decode(answer.encode()), Some(answer));
        }
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn log_messages_pinned_and_read_back() {
        let ballot = Ballot { round: 2, node: 1, boot: 3, start: 4 };
        let ballot_hex = "00000000000000020100000000000000030000000000000004";
        let pos = Pos { ballot, index: 5 };
        let pos_hex = format!("{ballot_hex}0000000000000005");
        let entry = Entry { ballot, op: Op::Set(vec![(b("k"), b("v"))]) };
        let entry_hex = format!("{ballot_hex}0100000001000000016b0000000176");
        let snap = Snapshot { pos, map: [(b("k"), b("v"))].into_iter().collect() };
        let snap_hex = format!("{pos_hex}00000001000000016b0000000176");
        let to_logs = [
            (ToLog::Open { ballot, state: false }, format!("00{ballot_hex}00")),
            (
                ToLog::Append { ballot, seq: 7, prev: pos, entries: vec![entry.clone()], committed: 5 },
                format!("01{ballot_hex}0000000000000007{pos_hex}00000001{entry_hex}0000000000000005"),
            ),
            (
                ToLog::Install { ballot, seq: 7, snap: snap.clone() },
                format!("02{ballot_hex}0000000000000007{snap_hex}"),
            ),
            (ToLog::Fetch { ballot, from: 6 }, format!("03{ballot_hex}0000000000000006")),
        ];
        for (msg, want) in to_logs {
            let body = log_body(9, &msg);
            assert_eq!(hex(&body), format!("0009{want}"), "{msg:?}");
            assert_eq!(read_log_body(body.clone()), Some((9, msg.clone())));
            for cut in 0..body.len() {
                assert_eq!(read_log_body(body.slice(..cut)), None, "{msg:?} cut at {cut}");
            }
        }
        let opened = Opened { last: pos, committed: pos, runs: vec![(ballot, 5)], state: None };
        let from_logs = [
            (
                FromLog::Opened { ballot, opened: opened.clone() },
                format!("00{ballot_hex}{pos_hex}{pos_hex}00000001{ballot_hex}000000000000000500"),
            ),
            (FromLog::Refused { ballot, promised: ballot }, format!("01{ballot_hex}{ballot_hex}")),
            (
                FromLog::Appended { ballot, seq: 7, matched: 5 },
                format!("02{ballot_hex}00000000000000070000000000000005"),
            ),
            (
                FromLog::Fenced { ballot, seq: 7, promised: ballot },
                format!("03{ballot_hex}0000000000000007{ballot_hex}"),
            ),
            (
                FromLog::Behind { ballot, seq: 7, committed: pos, runs: vec![] },
                format!("04{ballot_hex}0000000000000007{pos_hex}00000000"),
            ),
            (
                FromLog::Fetched { ballot, snap: Some(snap), from: 6, entries: vec![entry] },
                format!("05{ballot_hex}01{snap_hex}000000000000000600000001{entry_hex}"),
            ),
        ];
        for (msg, want) in from_logs {
            let body = from_log_bytes(&msg);
            assert_eq!(hex(&body), want, "{msg:?}");
            assert_eq!(read_from_log(body.clone()), Some(msg.clone()));
            for cut in 0..body.len() {
                assert_eq!(read_from_log(body.slice(..cut)), None, "{msg:?} cut at {cut}");
            }
        }
        let with_state = FromLog::Opened { ballot, opened: Opened { state: Some((Map::new(), vec![])), ..opened } };
        assert_eq!(read_from_log(from_log_bytes(&with_state)), Some(with_state));
        let call = |c| hex(&LogApi::encode(c).unwrap());
        assert_eq!(call(LogCall::Plain(b("ab"))), "006162");
        assert_eq!(call(LogCall::Upload), "01");
        assert_eq!(call(LogCall::Parts(b("ab"))), "026162");
    }

    #[test]
    fn truncated_bodies_are_refused_not_misread() {
        let body = Call::MSet(vec![(b("key"), b("value"))]).encode();
        for cut in 0..body.len() {
            assert_eq!(Call::decode(&mut body.slice(..cut)), None, "cut at {cut}");
        }
        assert!(ShardApi::decode_reply(Bytes::from_static(&[9])).is_err());
    }
}
