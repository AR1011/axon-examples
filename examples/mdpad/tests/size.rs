//! How the encoded state (`encode_state_as_update_v1`, what `base` carries) grows with the characters typed and with
//! the edits made, against the 256 KiB cap of the design (README § Document size). It prints a table; the asserts
//! hold the cap's claim: about 100,000 characters typed, fewer for a long-edited text.
//!
//! The defaults are a twentieth of the sizes, for a debug build. `MDPAD_SIZE_FULL=1` and a release build give the run the
//! README quotes: `cargo test --release -p mdpad --test size -- --nocapture --test-threads 1`.

use mdpad::crdt::{self, TEXT};
use yrs::{ReadTxn, StateVector, Text, Transact, WriteTxn};

const CAP: usize = 256 << 10;

/// xorshift64: the same edits on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u32) -> u32 {
        u32::try_from(self.next() % u64::from(n.max(1))).expect("below n")
    }
}

fn size(doc: &yrs::Doc) -> usize {
    doc.transact().encode_state_as_update_v1(&StateVector::default()).len()
}

fn len(doc: &yrs::Doc) -> u32 {
    let mut txn = doc.transact_mut();
    let text = txn.get_or_insert_text(TEXT);
    text.len(&txn)
}

/// One keystroke, one transaction, as the editor sends them.
fn key(doc: &yrs::Doc, at: u32, c: &str) {
    let mut txn = doc.transact_mut();
    let text = txn.get_or_insert_text(TEXT);
    text.insert(&mut txn, at, c);
}

fn delete(doc: &yrs::Doc, at: u32, n: u32) {
    let mut txn = doc.transact_mut();
    let text = txn.get_or_insert_text(TEXT);
    text.remove_range(&mut txn, at, n);
}

/// A plausible character: mostly letters, a space every few, a newline every line.
fn char_at(i: u32) -> &'static str {
    match i % 61 {
        60 => "\n",
        n if n % 6 == 5 => " ",
        n => ["e", "t", "a", "o", "i", "n"][(n % 6) as usize],
    }
}

fn row(what: &str, chars: u32, bytes: usize) {
    let per = bytes as f64 / f64::from(chars.max(1));
    println!(
        "| {what} | {chars} | {:.1} KiB | {per:.2} | {:.0} % |",
        bytes as f64 / 1024.0,
        100.0 * bytes as f64 / CAP as f64
    );
}

/// The long run's sizes with `MDPAD_SIZE_FULL=1`, a twentieth of them without.
fn scale(full: u32) -> u32 {
    if std::env::var_os("MDPAD_SIZE_FULL").is_some() { full } else { full / 20 }
}

#[test]
fn state_size_against_characters_typed() {
    let n = scale(100_000);
    println!("| typed | characters | state | bytes per character | of the cap |\n|---|---|---|---|---|");
    // One writer at the end of the text, a key at a time.
    let doc = crdt::doc(1);
    for i in 0..n {
        key(&doc, i, char_at(i));
    }
    let at_end = size(&doc);
    row("at the end, one writer", n, at_end);

    // Ten writers taking turns, a line each, at the end.
    let docs: Vec<_> = (1..=10).map(crdt::doc).collect();
    let shared = crdt::doc(99);
    for i in 0..n {
        let writer = &docs[(i / 61) as usize % 10];
        let at = len(writer);
        let update = {
            let mut txn = writer.transact_mut();
            let text = txn.get_or_insert_text(TEXT);
            text.insert(&mut txn, at, char_at(i));
            txn.encode_update_v1()
        };
        for other in docs.iter().chain([&shared]).filter(|d| !std::ptr::eq(*d, writer)) {
            crdt::apply(other, &update).unwrap();
        }
    }
    row("at the end, ten writers by turns", n, size(&shared));

    // Each key at a place of its own: the worst case for typing without deletes.
    let doc = crdt::doc(1);
    let mut rng = Rng(0x5eed);
    for i in 0..n {
        key(&doc, rng.below(i + 1), char_at(i));
    }
    row("each key at a random place", n, size(&doc));

    assert!(at_end * 100_000 / n as usize <= CAP / 2, "100,000 characters typed fit the cap with room: {at_end}");
}

/// A text of 30,000 characters, then `edits` edits: a word typed (1 to 8 keys) or 1 to 8 characters deleted, about
/// evenly, so the text stays near its size. `near` is the share of edits within 200 characters of the last one, as
/// when someone works through a paragraph; the rest go anywhere. Returns the edits after which the state passed the
/// cap, and the edits made and the state's size at the last reading. Stops at twice the cap.
fn long_edit(what: &str, near: u32, edits: u32) -> (Option<u32>, u32, usize) {
    let doc = crdt::doc(1);
    for i in 0..30_000 {
        key(&doc, i, char_at(i));
    }
    let mut rng = Rng(0xed17);
    let (mut full, mut last, mut reading) = (None, 0, (0, size(&doc)));
    for edit in 1..=edits {
        let n = len(&doc);
        let at = if rng.below(100) < near {
            (last + rng.below(400)).saturating_sub(200).min(n.saturating_sub(8))
        } else {
            rng.below(n - 8)
        };
        last = at;
        if rng.below(2) == 0 && n < 32_000 || n < 28_000 {
            for k in 0..1 + rng.below(8) {
                key(&doc, at + k, char_at(rng.below(61)));
            }
        } else {
            delete(&doc, at, 1 + rng.below(8));
        }
        if edit % 1000 == 0 || edit == edits {
            let bytes = size(&doc);
            reading = (edit, bytes);
            if [1_000, 5_000, 10_000, 20_000].contains(&edit) || edit == edits || bytes >= 2 * CAP {
                row(&format!("{what}, {edit} edits"), len(&doc), bytes);
            }
            if bytes >= CAP && full.is_none() {
                full = Some(edit);
                row(&format!("{what}, full at {edit} edits"), len(&doc), bytes);
            }
            if bytes >= 2 * CAP {
                break;
            }
        }
    }
    (full, reading.0, reading.1)
}

#[test]
fn state_size_of_a_long_edited_text() {
    println!(
        "| 30,000 characters, then | characters | state | bytes per character | of the cap |\n|---|---|---|---|---|"
    );
    let edits = scale(100_000);
    for (what, near) in [("anywhere", 0), ("near the last edit, 9 in 10", 90)] {
        let (full, made, bytes) = long_edit(what, near, edits);
        println!("full after: {what} {full:?}");
        // The README's reading: each edit leaves about 12 bytes of split structure, wherever it lands.
        let per_edit = bytes.saturating_sub(30_000) / made as usize;
        assert!((8..=16).contains(&per_edit), "{what}: {per_edit} bytes an edit");
    }
}
