//! A replica's records and state as bytes, for the Log's WAL and its snapshot files, and the messages between an
//! owner and a Log on another node. Big-endian, lengths as u32, as `wire.rs`. Decoding takes a `Bytes` and gives `None` for anything short or unknown.

use std::collections::VecDeque;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::core::{Ballot, Chain, Entry, FromLog, Map, Op, Opened, Pos, Rec, Snapshot, State, ToLog};

/// Shorter values are copied out of the buffer they are read from, so a value kept does not hold a whole read.
const COPY_BELOW: usize = 1 << 20;

fn put_bytes(out: &mut BytesMut, b: &[u8]) {
    out.put_u32(b.len() as u32);
    out.put_slice(b);
}

fn get_bytes(b: &mut Bytes) -> Option<Bytes> {
    let len = get_u32(b)? as usize;
    if b.remaining() < len {
        return None;
    }
    let v = b.split_to(len);
    Some(if len < COPY_BELOW { Bytes::copy_from_slice(&v) } else { v })
}

fn get_u8(b: &mut Bytes) -> Option<u8> {
    b.has_remaining().then(|| b.get_u8())
}

fn get_u32(b: &mut Bytes) -> Option<u32> {
    (b.remaining() >= 4).then(|| b.get_u32())
}

fn get_u64(b: &mut Bytes) -> Option<u64> {
    (b.remaining() >= 8).then(|| b.get_u64())
}

fn put_ballot(out: &mut BytesMut, b: &Ballot) {
    out.put_u64(b.round);
    out.put_u8(b.node);
    out.put_u64(b.boot);
    out.put_u64(b.start);
}

fn get_ballot(b: &mut Bytes) -> Option<Ballot> {
    Some(Ballot { round: get_u64(b)?, node: get_u8(b)?, boot: get_u64(b)?, start: get_u64(b)? })
}

fn put_pos(out: &mut BytesMut, p: &Pos) {
    put_ballot(out, &p.ballot);
    out.put_u64(p.index);
}

fn get_pos(b: &mut Bytes) -> Option<Pos> {
    Some(Pos { ballot: get_ballot(b)?, index: get_u64(b)? })
}

fn put_op(out: &mut BytesMut, op: &Op) {
    match op {
        Op::Begin => out.put_u8(0),
        Op::Set(pairs) => {
            out.put_u8(1);
            out.put_u32(pairs.len() as u32);
            for (k, v) in pairs {
                put_bytes(out, k);
                put_bytes(out, v);
            }
        }
        Op::Del(keys) => {
            out.put_u8(2);
            out.put_u32(keys.len() as u32);
            for k in keys {
                put_bytes(out, k);
            }
        }
        Op::Clear => out.put_u8(3),
    }
}

/// At most what the bytes left could hold, so a bad count does not reserve memory it cannot fill.
fn count(b: &mut Bytes, least: usize) -> Option<usize> {
    let n = get_u32(b)? as usize;
    (n.saturating_mul(least) <= b.remaining()).then_some(n)
}

fn get_op(b: &mut Bytes) -> Option<Op> {
    Some(match get_u8(b)? {
        0 => Op::Begin,
        1 => {
            let n = count(b, 8)?;
            let mut pairs = Vec::with_capacity(n);
            for _ in 0..n {
                pairs.push((get_bytes(b)?, get_bytes(b)?));
            }
            Op::Set(pairs)
        }
        2 => {
            let n = count(b, 4)?;
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                keys.push(get_bytes(b)?);
            }
            Op::Del(keys)
        }
        3 => Op::Clear,
        _ => return None,
    })
}

fn put_entries<'a>(out: &mut BytesMut, entries: impl ExactSizeIterator<Item = &'a Entry>) {
    out.put_u32(entries.len() as u32);
    for e in entries {
        put_ballot(out, &e.ballot);
        put_op(out, &e.op);
    }
}

fn get_entries(b: &mut Bytes) -> Option<Vec<Entry>> {
    let n = count(b, 26)?;
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        entries.push(Entry { ballot: get_ballot(b)?, op: get_op(b)? });
    }
    Some(entries)
}

fn put_map(out: &mut BytesMut, map: &Map) {
    out.put_u32(map.len() as u32);
    for (k, v) in map {
        put_bytes(out, k);
        put_bytes(out, v);
    }
}

fn get_map(b: &mut Bytes) -> Option<Map> {
    let n = count(b, 8)?;
    let mut map = Map::new();
    for _ in 0..n {
        map.insert(get_bytes(b)?, get_bytes(b)?);
    }
    Some(map)
}

pub fn put_state(out: &mut BytesMut, s: &State) {
    put_ballot(out, &s.promised);
    put_pos(out, &s.chain.base);
    put_entries(out, s.chain.entries.iter());
    out.put_u64(s.committed);
    put_map(out, &s.applied);
}

pub fn get_state(b: &mut Bytes) -> Option<State> {
    let promised = get_ballot(b)?;
    let base = get_pos(b)?;
    let entries = VecDeque::from(get_entries(b)?);
    let committed = get_u64(b)?;
    let applied = get_map(b)?;
    let chain = Chain { base, entries };
    // What `State::apply` keeps: the base at or below the committed index, which is at or below the last.
    (base.index <= committed && committed <= chain.last().index).then_some(State {
        promised,
        chain,
        committed,
        applied,
    })
}

pub fn put_rec(out: &mut BytesMut, rec: &Rec) {
    match rec {
        Rec::Promise(b) => {
            out.put_u8(1);
            put_ballot(out, b);
        }
        Rec::Entries { after, entries } => {
            out.put_u8(2);
            out.put_u64(*after);
            put_entries(out, entries.iter());
        }
        Rec::Commit(c) => {
            out.put_u8(3);
            out.put_u64(*c);
        }
        Rec::Install(snap) => {
            out.put_u8(4);
            put_snap(out, snap);
        }
        Rec::Checkpoint(state) => {
            out.put_u8(5);
            put_state(out, state);
        }
    }
}

pub fn get_rec(b: &mut Bytes) -> Option<Rec> {
    Some(match get_u8(b)? {
        1 => Rec::Promise(get_ballot(b)?),
        2 => Rec::Entries { after: get_u64(b)?, entries: get_entries(b)? },
        3 => Rec::Commit(get_u64(b)?),
        4 => Rec::Install(get_snap(b)?),
        5 => Rec::Checkpoint(Box::new(get_state(b)?)),
        _ => return None,
    })
}

/// About the bytes `put_rec` gives for these entries: what the Log reserves before it takes an Append.
pub fn entries_len(entries: &[Entry]) -> usize {
    entries.iter().map(|e| 64 + e.op.size() + 8 * op_items(&e.op)).sum()
}

pub fn map_len(map: &Map) -> usize {
    map.iter().map(|(k, v)| 8 + k.len() + v.len()).sum()
}

fn op_items(op: &Op) -> usize {
    match op {
        Op::Begin | Op::Clear => 0,
        Op::Set(pairs) => pairs.len(),
        Op::Del(keys) => keys.len(),
    }
}

fn put_runs(out: &mut BytesMut, runs: &[(Ballot, u64)]) {
    out.put_u32(runs.len() as u32);
    for (b, last) in runs {
        put_ballot(out, b);
        out.put_u64(*last);
    }
}

fn get_runs(b: &mut Bytes) -> Option<Vec<(Ballot, u64)>> {
    let n = count(b, 33)?;
    let mut runs = Vec::with_capacity(n);
    for _ in 0..n {
        runs.push((get_ballot(b)?, get_u64(b)?));
    }
    Some(runs)
}

fn put_snap(out: &mut BytesMut, snap: &Snapshot) {
    put_pos(out, &snap.pos);
    put_map(out, &snap.map);
}

fn get_snap(b: &mut Bytes) -> Option<Snapshot> {
    Some(Snapshot { pos: get_pos(b)?, map: get_map(b)? })
}

/// An owner's message to a Log on another node.
pub fn put_to_log(out: &mut BytesMut, msg: &ToLog) {
    match msg {
        ToLog::Open { ballot, state } => {
            out.put_u8(0);
            put_ballot(out, ballot);
            out.put_u8(u8::from(*state));
        }
        ToLog::Append { ballot, seq, prev, entries, committed } => {
            out.reserve(entries_len(entries) + 64);
            out.put_u8(1);
            put_ballot(out, ballot);
            out.put_u64(*seq);
            put_pos(out, prev);
            put_entries(out, entries.iter());
            out.put_u64(*committed);
        }
        ToLog::Install { ballot, seq, snap } => {
            out.reserve(map_len(&snap.map) + 64);
            out.put_u8(2);
            put_ballot(out, ballot);
            out.put_u64(*seq);
            put_snap(out, snap);
        }
        ToLog::Fetch { ballot, from } => {
            out.put_u8(3);
            put_ballot(out, ballot);
            out.put_u64(*from);
        }
    }
}

pub fn get_to_log(b: &mut Bytes) -> Option<ToLog> {
    Some(match get_u8(b)? {
        0 => ToLog::Open {
            ballot: get_ballot(b)?,
            state: match get_u8(b)? {
                0 => false,
                1 => true,
                _ => return None,
            },
        },
        1 => ToLog::Append {
            ballot: get_ballot(b)?,
            seq: get_u64(b)?,
            prev: get_pos(b)?,
            entries: get_entries(b)?,
            committed: get_u64(b)?,
        },
        2 => ToLog::Install { ballot: get_ballot(b)?, seq: get_u64(b)?, snap: get_snap(b)? },
        3 => ToLog::Fetch { ballot: get_ballot(b)?, from: get_u64(b)? },
        _ => return None,
    })
}

/// A Log's answer to an owner on another node.
pub fn put_from_log(out: &mut BytesMut, msg: &FromLog) {
    match msg {
        FromLog::Opened { ballot, opened } => {
            out.put_u8(0);
            put_ballot(out, ballot);
            put_pos(out, &opened.last);
            put_pos(out, &opened.committed);
            put_runs(out, &opened.runs);
            match &opened.state {
                None => out.put_u8(0),
                Some((map, entries)) => {
                    out.put_u8(1);
                    put_map(out, map);
                    put_entries(out, entries.iter());
                }
            }
        }
        FromLog::Refused { ballot, promised } => {
            out.put_u8(1);
            put_ballot(out, ballot);
            put_ballot(out, promised);
        }
        FromLog::Appended { ballot, seq, matched } => {
            out.put_u8(2);
            put_ballot(out, ballot);
            out.put_u64(*seq);
            out.put_u64(*matched);
        }
        FromLog::Fenced { ballot, seq, promised } => {
            out.put_u8(3);
            put_ballot(out, ballot);
            out.put_u64(*seq);
            put_ballot(out, promised);
        }
        FromLog::Behind { ballot, seq, committed, runs } => {
            out.put_u8(4);
            put_ballot(out, ballot);
            out.put_u64(*seq);
            put_pos(out, committed);
            put_runs(out, runs);
        }
        FromLog::Fetched { ballot, snap, from, entries } => {
            out.reserve(snap.as_ref().map_or(0, |s| map_len(&s.map)) + entries_len(entries) + 64);
            out.put_u8(5);
            put_ballot(out, ballot);
            match snap {
                None => out.put_u8(0),
                Some(snap) => {
                    out.put_u8(1);
                    put_snap(out, snap);
                }
            }
            out.put_u64(*from);
            put_entries(out, entries.iter());
        }
    }
}

pub fn get_from_log(b: &mut Bytes) -> Option<FromLog> {
    Some(match get_u8(b)? {
        0 => {
            let ballot = get_ballot(b)?;
            let (last, committed, runs) = (get_pos(b)?, get_pos(b)?, get_runs(b)?);
            let state = match get_u8(b)? {
                0 => None,
                1 => Some((get_map(b)?, get_entries(b)?)),
                _ => return None,
            };
            FromLog::Opened { ballot, opened: Opened { last, committed, runs, state } }
        }
        1 => FromLog::Refused { ballot: get_ballot(b)?, promised: get_ballot(b)? },
        2 => FromLog::Appended { ballot: get_ballot(b)?, seq: get_u64(b)?, matched: get_u64(b)? },
        3 => FromLog::Fenced { ballot: get_ballot(b)?, seq: get_u64(b)?, promised: get_ballot(b)? },
        4 => FromLog::Behind { ballot: get_ballot(b)?, seq: get_u64(b)?, committed: get_pos(b)?, runs: get_runs(b)? },
        5 => {
            let ballot = get_ballot(b)?;
            let snap = match get_u8(b)? {
                0 => None,
                1 => Some(get_snap(b)?),
                _ => return None,
            };
            FromLog::Fetched { ballot, snap, from: get_u64(b)?, entries: get_entries(b)? }
        }
        _ => return None,
    })
}
