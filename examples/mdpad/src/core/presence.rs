//! Who is in a document now, with no I/O: each editor's cursor entry, bound to the session that owns its client id,
//! and a count of viewers per web node. The document publishes what changed on its presence stream, as keyed records
//! `<session>/<client>` and `v/<node>`; nothing of it reaches the disk (I8). An entry goes on a clean leave, after
//! [`EXPIRE`] with no renewal, or when its web node leaves the cluster.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use axon_stream::keyed;
use bytes::Bytes;

use crate::wire::{Aware, Reader, Writer};

/// An entry's JSON, at most.
pub const MAX_ENTRY: usize = 256;
/// Editors' entries of one document, at most: past it a new one is not shown.
pub const MAX_ENTRIES: usize = 400;
/// An entry not renewed for this long goes: its session ended with no word, on a node still in the cluster.
pub const EXPIRE: Duration = Duration::from_secs(30);

struct Entry {
    node: String,
    clock: u64,
    state: String,
    renewed: Duration,
}

#[derive(Default)]
pub struct Presence {
    /// By (session, client).
    entries: BTreeMap<(u64, u64), Entry>,
    /// Which session speaks for a client id: another one's entries for it are dropped.
    owners: BTreeMap<u64, u64>,
    /// Viewers by session: their node and when they were last renewed.
    viewers: BTreeMap<u64, (String, Duration)>,
    /// Keys to publish: an entry's, or a node's viewer count.
    changed: BTreeSet<Key>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Entry(u64, u64),
    Viewers(String),
}

impl Presence {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.viewers.is_empty()
    }

    /// A session's presence, told again every 15 s while it lasts: an editor's entry, or `None` for a viewer.
    pub fn aware(&mut self, session: u64, node: &str, entry: Option<Aware>, now: Duration) {
        let Some(Aware { client, clock, state }) = entry else {
            if self.viewers.insert(session, (node.to_string(), now)).is_none() {
                self.changed.insert(Key::Viewers(node.to_string()));
            }
            return;
        };
        if self.owners.get(&client).is_some_and(|owner| *owner != session) {
            return; // another session's client id
        }
        let key = (session, client);
        let full = self.entries.len() >= MAX_ENTRIES;
        match state {
            None => {
                if self.entries.remove(&key).is_some() {
                    self.changed.insert(Key::Entry(session, client));
                }
            }
            Some(state) if state.len() > MAX_ENTRY => {}
            Some(state) => match self.entries.get_mut(&key) {
                Some(entry) => {
                    entry.renewed = now;
                    if entry.state != state || entry.clock != clock {
                        (entry.state, entry.clock) = (state, clock);
                        self.changed.insert(Key::Entry(session, client));
                    }
                }
                None if full => {}
                None => {
                    self.owners.insert(client, session);
                    self.entries.insert(key, Entry { node: node.to_string(), clock, state, renewed: now });
                    self.changed.insert(Key::Entry(session, client));
                }
            },
        }
    }

    pub fn leave(&mut self, session: u64) {
        self.remove(|s, _| s == session);
    }

    /// A web node left the cluster: its sessions are gone with it.
    pub fn left(&mut self, node: &str) {
        self.remove(|_, n| n == node);
    }

    pub fn expire(&mut self, now: Duration) {
        let stale = |renewed: Duration| now.saturating_sub(renewed) >= EXPIRE;
        let gone: BTreeSet<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| stale(e.renewed))
            .map(|((s, _), _)| *s)
            .chain(self.viewers.iter().filter(|(_, (_, at))| stale(*at)).map(|(s, _)| *s))
            .collect();
        self.remove(|s, _| gone.contains(&s));
    }

    fn remove(&mut self, gone: impl Fn(u64, &str) -> bool) {
        let changed = &mut self.changed;
        self.entries.retain(|(session, client), e| {
            let keep = !gone(*session, &e.node);
            if !keep {
                changed.insert(Key::Entry(*session, *client));
            }
            keep
        });
        self.viewers.retain(|session, (node, _)| {
            let keep = !gone(*session, node);
            if !keep {
                changed.insert(Key::Viewers(node.clone()));
            }
            keep
        });
        let sessions: BTreeSet<u64> = self.entries.keys().map(|(s, _)| *s).collect();
        self.owners.retain(|_, session| sessions.contains(session));
    }

    /// The records of what changed since the last call: an entry's, empty for one that went, and each changed
    /// node's viewer count, empty at none.
    pub fn take(&mut self) -> Vec<Bytes> {
        let changed = std::mem::take(&mut self.changed);
        changed
            .into_iter()
            .map(|key| match key {
                Key::Entry(session, client) => {
                    let value = self.entries.get(&(session, client)).map(|e| {
                        let mut w = Writer::new();
                        Aware { client, clock: e.clock, state: Some(e.state.clone()) }.write(&mut w);
                        w.into_vec()
                    });
                    keyed(entry_key(session, client).as_bytes(), &value.unwrap_or_default())
                }
                Key::Viewers(node) => {
                    let count = self.viewers.values().filter(|(n, _)| *n == node).count();
                    let mut w = Writer::new();
                    if count > 0 {
                        w.uint(count as u64);
                    }
                    let value = w.into_vec();
                    keyed(format!("v/{node}").as_bytes(), &value)
                }
            })
            .collect()
    }
}

pub fn entry_key(session: u64, client: u64) -> String {
    format!("{session:016x}/{client}")
}

/// A presence record, as a session reads it.
#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    /// `entry` is `None` for one that went.
    Entry {
        session: u64,
        client: u64,
        entry: Option<Aware>,
    },
    Viewers {
        node: String,
        count: u64,
    },
}

pub fn read(key: &[u8], value: &[u8]) -> Option<Record> {
    let key = std::str::from_utf8(key).ok()?;
    if let Some(node) = key.strip_prefix("v/") {
        let count = if value.is_empty() { 0 } else { Reader::new(value).uint().ok()? };
        return Some(Record::Viewers { node: node.to_string(), count });
    }
    let (session, client) = key.split_once('/')?;
    let (session, client) = (u64::from_str_radix(session, 16).ok()?, client.parse().ok()?);
    let entry = if value.is_empty() { None } else { Some(Aware::read(&mut Reader::new(value)).ok()?) };
    Some(Record::Entry { session, client, entry })
}

#[cfg(test)]
mod tests {
    use axon_stream::split_keyed;

    use super::*;

    fn editor(client: u64, clock: u64, state: &str) -> Option<Aware> {
        Some(Aware { client, clock, state: Some(state.into()) })
    }

    fn records(p: &mut Presence) -> Vec<Record> {
        p.take().iter().map(|r| split_keyed(r).unwrap()).map(|(k, v)| read(&k, &v).unwrap()).collect()
    }

    #[test]
    fn entries_are_bound_to_their_session_and_go_with_it() {
        let mut p = Presence::default();
        let at = Duration::ZERO;
        p.aware(1, "w1", editor(7, 1, "{}"), at);
        p.aware(2, "w2", editor(7, 2, r#"{"spoof":1}"#), at);
        p.aware(2, "w2", None, at);
        assert_eq!(
            records(&mut p),
            [
                Record::Entry { session: 1, client: 7, entry: editor(7, 1, "{}") },
                Record::Viewers { node: "w2".into(), count: 1 },
            ],
            "another session's client id is dropped"
        );
        p.aware(1, "w1", editor(7, 1, "{}"), Duration::from_secs(1));
        assert!(records(&mut p).is_empty(), "a renewal changes nothing");
        p.aware(1, "w1", editor(7, 2, &"x".repeat(MAX_ENTRY + 1)), at);
        assert!(records(&mut p).is_empty(), "too large");
        p.leave(1);
        assert_eq!(records(&mut p), [Record::Entry { session: 1, client: 7, entry: None }]);
        p.aware(2, "w2", editor(7, 3, "{}"), at);
        assert_eq!(records(&mut p).len(), 1, "a client id is free once its session went");
    }

    #[test]
    fn a_node_that_leaves_takes_its_sessions_and_silence_expires() {
        let mut p = Presence::default();
        p.aware(1, "w1", editor(1, 1, "{}"), Duration::ZERO);
        p.aware(2, "w1", None, Duration::ZERO);
        p.aware(3, "w2", editor(3, 1, "{}"), Duration::from_secs(20));
        records(&mut p);
        p.left("w1");
        assert_eq!(
            records(&mut p),
            [Record::Entry { session: 1, client: 1, entry: None }, Record::Viewers { node: "w1".into(), count: 0 }]
        );
        p.expire(EXPIRE);
        assert!(records(&mut p).is_empty(), "renewed 10 s ago");
        p.expire(Duration::from_secs(20) + EXPIRE);
        assert_eq!(records(&mut p), [Record::Entry { session: 3, client: 3, entry: None }]);
        assert!(p.is_empty());
    }

    #[test]
    fn past_the_cap_a_new_entry_is_not_shown() {
        let mut p = Presence::default();
        for s in 0..MAX_ENTRIES as u64 + 1 {
            p.aware(s, "w1", editor(s, 1, "{}"), Duration::ZERO);
        }
        assert_eq!(records(&mut p).len(), MAX_ENTRIES);
    }
}
