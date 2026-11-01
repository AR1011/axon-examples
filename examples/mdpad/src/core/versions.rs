//! A document's versions (design §5.2): its Markdown at a moment, with who wrote it since the version before, when,
//! and why it was kept. A version holds no CRDT state: a restore works from the Markdown, as a forward edit.
//!
//! The node's log holds each version's Markdown under a key of its own, `v/<id>/<n>`, and the list under `vi/<id>`.
//! A save writes the Markdown, then the list, then removes what thinning let go, in one write: a list that is
//! durable never names Markdown that is not.

use std::time::Duration;

use serde::Serialize;

use crate::wire::{Malformed, Reader, Writer};

/// A version is saved this long after the last edit, when anything changed.
pub const IDLE: Duration = Duration::from_secs(10 * 60);
/// And at least this often while edits go on.
pub const EVERY: Duration = Duration::from_secs(30 * 60);
/// An offline merge larger than this saves a version first.
pub const OFFLINE_BYTES: usize = 2 << 10;
/// Named versions past this many are thinned like the rest, oldest first.
pub const MAX_NAMED: usize = 100;
pub const MAX_NAME: usize = 80;
/// A version names at most this many authors, those who edited last.
pub const MAX_AUTHORS: usize = 64;

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Auto,
    Named,
    BeforeRestore,
    BeforeOffline,
    BeforeImport,
}

impl Kind {
    const ALL: [Self; 5] = [Self::Auto, Self::Named, Self::BeforeRestore, Self::BeforeOffline, Self::BeforeImport];
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Version {
    pub n: u64,
    pub at_ms: u64,
    pub kind: Kind,
    /// A named version's name; empty for the others.
    pub name: String,
    /// Who changed the text since the version before, in the order they first did.
    pub authors: Vec<String>,
    /// The Markdown's length in bytes.
    pub bytes: u64,
    /// The Markdown's CRC-32: an automatic save is skipped when the text is the newest version's.
    #[serde(skip)]
    pub crc: u32,
}

impl Version {
    pub fn write(&self, w: &mut Writer) {
        w.uint(self.n).uint(self.at_ms).u8(self.kind as u8).str(&self.name).uint(self.authors.len() as u64);
        for author in &self.authors {
            w.str(author);
        }
        w.uint(self.bytes).uint(u64::from(self.crc));
    }

    pub fn read(r: &mut Reader<'_>) -> Result<Self, Malformed> {
        let (n, at_ms) = (r.uint()?, r.uint()?);
        let kind = *Kind::ALL.get(r.u8()? as usize).ok_or(Malformed)?;
        let name = r.string()?;
        let count = r.uint()?;
        if count > MAX_AUTHORS as u64 {
            return Err(Malformed);
        }
        let authors = (0..count).map(|_| r.string()).collect::<Result<_, _>>()?;
        let bytes = r.uint()?;
        let crc = u32::try_from(r.uint()?).map_err(|_| Malformed)?;
        Ok(Self { n, at_ms, kind, name, authors, bytes, crc })
    }
}

/// The key of a version's Markdown in the node's log.
pub fn body_key(id: &str, n: u64) -> bytes::Bytes {
    format!("v/{id}/{n}").into()
}

pub fn index_key(id: &str) -> bytes::Bytes {
    format!("vi/{id}").into()
}

/// A document's list of versions, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Versions {
    list: Vec<Version>,
    /// The next version's number: never one a removed version had, so a link to one never shows another.
    next: u64,
}

impl Versions {
    pub fn decode(body: &[u8]) -> Result<Self, Malformed> {
        let mut r = Reader::new(body);
        let next = r.uint()?;
        let count = r.uint()?;
        let mut list = Vec::new();
        for _ in 0..count {
            list.push(Version::read(&mut r)?);
        }
        r.end()?;
        Ok(Self { list, next })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.uint(self.next).uint(self.list.len() as u64);
        for v in &self.list {
            v.write(&mut w);
        }
        w.into_vec()
    }

    pub fn list(&self) -> &[Version] {
        &self.list
    }

    pub fn get(&self, n: u64) -> Option<&Version> {
        self.list.iter().find(|v| v.n == n)
    }

    pub fn newest(&self) -> Option<&Version> {
        self.list.last()
    }

    /// Adds a version numbered after every one before it, and returns the numbers thinning lets go.
    pub fn add(&mut self, mut version: Version) -> (Version, Vec<u64>) {
        version.n = self.next.max(self.list.last().map_or(0, |v| v.n)) + 1;
        version.authors.drain(..version.authors.len().saturating_sub(MAX_AUTHORS));
        self.next = version.n;
        self.list.push(version.clone());
        let gone = thin(&self.list, version.at_ms);
        self.list.retain(|v| !gone.contains(&v.n));
        (version, gone)
    }
}

/// The versions to let go at `now_ms`: every named version stays (the newest `MAX_NAMED`), and the newest version,
/// and all of the last 24 hours. Older ones keep the newest of each day to 30 days, then the newest of each 30 days.
/// So 30 days of editing keep about 17 + 29 versions, and a year about 17 + 29 + 11.
pub fn thin(list: &[Version], now_ms: u64) -> Vec<u64> {
    let named: Vec<u64> = list.iter().filter(|v| v.kind == Kind::Named).map(|v| v.n).collect();
    let keep_named = &named[named.len().saturating_sub(MAX_NAMED)..];
    let mut gone = Vec::new();
    let mut last_bucket = None;
    // Newest first: the first version seen in a bucket is the one it keeps.
    for (i, v) in list.iter().enumerate().rev() {
        let age = now_ms.saturating_sub(v.at_ms);
        if i + 1 == list.len() || keep_named.contains(&v.n) || age < DAY_MS {
            continue;
        }
        let bucket = match age < 30 * DAY_MS {
            true => (0, v.at_ms / DAY_MS),
            false => (1, v.at_ms / (30 * DAY_MS)),
        };
        if last_bucket == Some(bucket) {
            gone.push(v.n);
        }
        last_bucket = Some(bucket);
    }
    gone.sort_unstable();
    gone
}

/// Whether an automatic version is due: something changed, and either the edits stopped `IDLE` ago or the last
/// version is `EVERY` old.
pub fn due(now_ms: u64, last_edit_ms: u64, last_version_ms: u64) -> bool {
    now_ms.saturating_sub(last_edit_ms) >= IDLE.as_millis() as u64
        || now_ms.saturating_sub(last_version_ms) >= EVERY.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(at_ms: u64, kind: Kind, bytes: u64) -> Version {
        Version { n: 0, at_ms, kind, name: String::new(), authors: vec!["ana".into()], bytes, crc: 0 }
    }

    #[test]
    fn the_list_round_trips_and_numbers_only_grow() {
        let mut versions = Versions::default();
        let (a, _) = versions.add(version(1, Kind::Auto, 3));
        let (b, _) = versions.add(Version { name: "draft".into(), ..version(2, Kind::Named, 4) });
        assert_eq!((a.n, b.n), (1, 2));
        let again = Versions::decode(&versions.encode()).unwrap();
        assert_eq!(again, versions);
        assert_eq!(again.get(2).unwrap().name, "draft");
        assert!(Versions::decode(&[1, 1, 1]).is_err());
        // A crowd of authors: the list still reads back, with those who edited last.
        let crowd = (0..MAX_AUTHORS + 6).map(|i| format!("u{i}")).collect();
        let (c, _) = versions.add(Version { authors: crowd, ..version(3, Kind::Auto, 5) });
        assert_eq!((c.authors.len(), c.authors[0].as_str()), (MAX_AUTHORS, "u6"));
        assert_eq!(Versions::decode(&versions.encode()).unwrap(), versions);
    }

    #[test]
    fn auto_versions_come_after_an_idle_spell_or_every_half_hour() {
        let min = 60_000;
        assert!(!due(5 * min, 4 * min, 0), "still editing, 5 minutes since the last version");
        assert!(due(14 * min, 4 * min, 0), "10 minutes idle");
        assert!(due(31 * min, 30 * min, 0), "30 minutes of edits");
    }

    #[test]
    fn thinning_keeps_named_versions_the_last_day_and_one_a_day() {
        let mut versions = Versions::default();
        let named = versions.add(Version { name: "keep".into(), ..version(0, Kind::Named, 1) }).0.n;
        for h in 0..24 * 3 {
            versions.add(version(h * HOUR_MS + 1, Kind::Auto, 1));
        }
        let now = versions.newest().unwrap().at_ms;
        let ns: Vec<u64> = versions.list().iter().map(|v| v.n).collect();
        assert!(ns.contains(&named));
        let recent = versions.list().iter().filter(|v| now - v.at_ms < DAY_MS).count();
        assert_eq!(recent, 24, "all of the last day");
        let older = versions.list().iter().filter(|v| v.kind == Kind::Auto && now - v.at_ms >= DAY_MS).count();
        assert_eq!(older, 2, "one a day before that");
    }

    /// Design §7: after 30 days of modelled edits, 8 hours a day, the versions hold under 60 times the text.
    #[test]
    fn thirty_days_of_edits_keep_under_sixty_texts() {
        let text = 20_000u64;
        let mut versions = Versions::default();
        let (mut most, mut month) = (0, 0);
        for day in 0..365u64 {
            // From 09:00 to 17:00 a version every 30 minutes while edits go on, and one 10 minutes after they stop.
            let start = day * DAY_MS + 9 * HOUR_MS;
            for half in 0..=16 {
                versions.add(version(start + half * 30 * 60_000, Kind::Auto, text));
            }
            versions.add(version(start + 8 * HOUR_MS + 10 * 60_000, Kind::Auto, text));
            let bytes: u64 = versions.list().iter().map(|v| v.bytes).sum();
            if day == 29 {
                month = bytes / text;
            }
            most = most.max(bytes / text);
        }
        println!("versions: {month} texts after 30 days, {most} at most in a year");
        assert!(month < 60, "{month} texts after 30 days");
        assert!(most < 80, "{most} texts at most in a year: bounded (I10)");
    }
}
