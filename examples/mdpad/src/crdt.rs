//! The Yjs document the server keeps: yrs with UTF-16 offsets, as the browser counts them.

use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{ClientID, Doc, GetString, OffsetKind, Options, ReadTxn, StateVector, Text, Transact, Update, WriteTxn};

/// The name of the document's one `Y.Text`, the same in the browser.
pub const TEXT: &str = "md";

pub fn doc(client_id: u64) -> Doc {
    // Yjs draws 32-bit client ids, and a larger one does not round-trip through a JavaScript number.
    let client_id = ClientID::new(client_id & 0xFFFF_FFFF);
    Doc::with_options(Options { client_id, offset_kind: OffsetKind::Utf16, ..Options::default() })
}

/// The whole document as one v1 update.
pub fn state(doc: &Doc) -> Vec<u8> {
    doc.transact().encode_state_as_update_v1(&StateVector::default())
}

pub fn state_vector(doc: &Doc) -> Vec<u8> {
    doc.transact().state_vector().encode_v1()
}

/// What `doc` holds that a peer at `state_vector` lacks, as one v1 update; `None` for bytes that are not one.
pub fn diff(doc: &Doc, state_vector: &[u8]) -> Option<Vec<u8>> {
    let sv = StateVector::decode_v1(state_vector).ok()?;
    Some(doc.transact().encode_diff_v1(&sv))
}

pub fn text(doc: &Doc) -> String {
    let text = doc.get_or_insert_text(TEXT);
    let txn = doc.transact();
    text.get_string(&txn)
}

/// Inserts `s` at UTF-16 offset `at`, as this document's own edit, and returns the update.
pub fn insert(doc: &Doc, at: u32, s: &str) -> Vec<u8> {
    let text = doc.get_or_insert_text(TEXT);
    let mut txn = doc.transact_mut();
    text.insert(&mut txn, at, s);
    txn.encode_update_v1()
}

/// Removes `len` UTF-16 units from `at`, and returns the update.
pub fn remove(doc: &Doc, at: u32, len: u32) -> Vec<u8> {
    let text = doc.get_or_insert_text(TEXT);
    let mut txn = doc.transact_mut();
    text.remove_range(&mut txn, at, len);
    txn.encode_update_v1()
}

/// The text's length in UTF-16 units.
pub fn len(doc: &Doc) -> u32 {
    let text = doc.get_or_insert_text(TEXT);
    let txn = doc.transact();
    text.len(&txn)
}

/// Updates in any order as one update, with its state vector: a stream snapshot's `base` and `u/*` records come in
/// key order, not in the order they were made.
pub fn merge(updates: &[&[u8]]) -> Option<(Vec<u8>, Vec<u8>)> {
    let merged = yrs::merge_updates_v1(updates).ok()?;
    let sv = yrs::encode_state_vector_from_update_v1(&merged).ok()?;
    Some((merged, sv))
}

/// Applies an update as Yjs does: a part that lacks a dependency waits in the document until it comes. For a client,
/// which takes what the server sends.
pub fn apply_any(doc: &Doc, update: &[u8]) {
    if let Ok(update) = Update::decode_v1(update) {
        drop(doc.transact_mut().apply_update(update));
    }
}

/// Whether `doc` holds an insert a peer at `state_vector` lacks.
pub fn ahead(doc: &Doc, state_vector: &[u8]) -> bool {
    let Ok(theirs) = StateVector::decode_v1(state_vector) else { return true };
    let ours = doc.transact().state_vector();
    ours.iter().any(|(client, clock)| theirs.get(client) < *clock)
}

/// A v1 update, or `None` for bytes that are not one.
pub fn decode(update: &[u8]) -> Option<Update> {
    Update::decode_v1(update).ok()
}

/// Whether an update adds anything: `false` for one that only deletes.
pub fn inserts(update: &Update) -> bool {
    !update.insertions(false).is_empty()
}

/// Why an update was refused with nothing applied.
#[derive(Debug, PartialEq, Eq)]
pub enum Invalid {
    /// It is not a v1 update, or yrs could not apply any of it.
    Decode,
    /// It starts past what the document holds of one of its clients, or skips clocks of one.
    MissingDependency,
}

/// What an update changed.
#[derive(Debug, Default)]
pub struct Applied {
    /// The new part of the update, as a v1 update: empty when the document had all of it.
    pub diff: Vec<u8>,
    /// A part of it was dropped: it needed something the document lacks, or yrs failed on it after applying the rest.
    /// Nothing waits as pending, and the sender must resync. `diff` is what did apply, and is kept like any other.
    pub missing: bool,
}

impl Applied {
    pub fn is_empty(&self) -> bool {
        self.diff.is_empty()
    }
}

/// Applies a client's update. One that cannot apply whole is refused before it changes anything where that can be
/// seen from its clients' clocks: one that starts past a client's clock, or leaves a hole in one, which yrs would
/// otherwise fill with a skip and then drop the edits that belong there. A dependency hidden deeper (an origin of
/// another client, a delete of an item the document lacks) or a failure part way is found after, and only what
/// applied stays, in `diff`, so the document holds no more than its diffs.
pub fn apply(doc: &Doc, update: &[u8]) -> Result<Applied, Invalid> {
    apply_update(doc, decode(update).ok_or(Invalid::Decode)?)
}

/// [`apply`] of an update already decoded.
pub fn apply_update(doc: &Doc, update: Update) -> Result<Applied, Invalid> {
    let mut txn = doc.transact_mut();
    let have = txn.state_vector();
    let gap = update.insertions(true).iter().any(|(client, ranges)| {
        let mut ranges = ranges.iter();
        ranges.next().is_some_and(|first| first.start > have.get(client)) || ranges.next().is_some()
    });
    if gap {
        return Err(Invalid::MissingDependency);
    }
    let failed = txn.apply_update(update).is_err();
    let missing = txn.prune_pending().is_some() || failed;
    let changed = !txn.insert_set().is_empty() || !txn.delete_set().is_empty();
    if failed && !changed {
        return Err(Invalid::Decode);
    }
    let diff = if changed { txn.encode_update_v1() } else { Vec::new() };
    Ok(Applied { diff, missing })
}

/// Makes the text `to` as this document's own edit: the fewest deletes and inserts, from a character diff, so text a
/// peer adds meanwhile inside a removed range stays, and what is removed never comes back (design §5.2, journey 7).
/// Returns the update, empty when the text is `to` already.
pub fn replace(doc: &Doc, to: &str) -> Vec<u8> {
    let from = text(doc);
    if from == to {
        return Vec::new();
    }
    let text = doc.get_or_insert_text(TEXT);
    let mut txn = doc.transact_mut();
    let mut at = 0u32;
    for chunk in dissimilar::diff(&from, to) {
        match chunk {
            dissimilar::Chunk::Equal(s) => at += utf16(s),
            dissimilar::Chunk::Delete(s) => text.remove_range(&mut txn, at, utf16(s)),
            dissimilar::Chunk::Insert(s) => {
                text.insert(&mut txn, at, s);
                at += utf16(s);
            }
        }
    }
    txn.encode_update_v1()
}

fn utf16(s: &str) -> u32 {
    s.encode_utf16().count() as u32
}
