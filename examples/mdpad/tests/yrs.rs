//! yrs against Yjs: the updates the pinned Yjs makes (`ui/tests-node/fixtures.mjs`, kept in `tests/fixtures/yjs.json`) apply in
//! yrs at the same UTF-16 offsets, and an update yrs makes (`tests/fixtures/yrs.json`) applies in Yjs (`ui/tests-node`,
//! `npm test`). Then the server's refusal of an update that depends on something the document lacks.
//!
//! `MDPAD_BLESS=1` writes `yrs.json` again, after a change of yrs.

use std::path::PathBuf;

use mdpad::crdt::{self, Invalid, TEXT};
use serde_json::Value;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{
    GetString, ReadTxn, StateVector, Text, Transact, Update, WriteTxn, diff_updates_v1,
    encode_state_vector_from_update_v1, merge_updates_v1,
};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn bytes(hex: &Value) -> Vec<u8> {
    let hex = hex.as_str().expect("a hex string");
    (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex")).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn yjs() -> Value {
    serde_json::from_str(&std::fs::read_to_string(fixture("yjs.json")).expect("yjs.json")).expect("json")
}

fn text_of(doc: &yrs::Doc) -> (String, u32) {
    let mut txn = doc.transact_mut();
    let text = txn.get_or_insert_text(TEXT);
    (text.get_string(&txn), text.len(&txn))
}

/// UTF-16 units, as the browser and `OffsetKind::Utf16` count them.
fn units(s: &str) -> u32 {
    u32::try_from(s.encode_utf16().count()).expect("short")
}

#[test]
fn yjs_updates_apply_in_yrs_one_by_one_and_whole() {
    let js = yjs();
    let updates: Vec<Vec<u8>> = js["updates"].as_array().expect("updates").iter().map(bytes).collect();
    let want = (js["text"].as_str().expect("text").to_string(), u32::try_from(js["length"].as_u64().unwrap()).unwrap());

    let doc = crdt::doc(2);
    for update in &updates {
        let applied = crdt::apply(&doc, update).expect("a Yjs update applies");
        assert!(!applied.missing && !applied.is_empty());
    }
    assert_eq!(text_of(&doc), want, "the same text and the same UTF-16 length");

    let whole = crdt::doc(3);
    crdt::apply(&whole, &bytes(&js["state"])).expect("the whole state applies");
    assert_eq!(text_of(&whole), want);
    assert_eq!(doc.transact().state_vector(), StateVector::decode_v1(&bytes(&js["state_vector"])).unwrap());
}

#[test]
fn the_update_functions_the_server_needs_exist_and_agree_with_yjs() {
    let js = yjs();
    let updates: Vec<Vec<u8>> = js["updates"].as_array().unwrap().iter().map(bytes).collect();
    let state = bytes(&js["state"]);

    // merge_updates_v1: a backlog of edits sent as one.
    let merged = merge_updates_v1(updates.iter().map(Vec::as_slice)).expect("merge");
    let doc = crdt::doc(2);
    crdt::apply(&doc, &merged).unwrap();
    assert_eq!(text_of(&doc).0, js["text"].as_str().unwrap());

    // encode_state_vector_from_update_v1: the state vector of a stored state, without a document.
    let sv = encode_state_vector_from_update_v1(&state).expect("state vector");
    assert_eq!(StateVector::decode_v1(&sv).unwrap(), StateVector::decode_v1(&bytes(&js["state_vector"])).unwrap());

    // diff_updates_v1: what a client that holds the first edits lacks, from the stored state alone.
    let partial = usize::try_from(js["partial"].as_u64().unwrap()).unwrap();
    let behind = crdt::doc(4);
    for update in &updates[..partial] {
        crdt::apply(&behind, update).unwrap();
    }
    let missing = diff_updates_v1(&state, &behind.transact().state_vector().encode_v1()).expect("diff");
    assert!(missing.len() < state.len());
    crdt::apply(&behind, &missing).unwrap();
    assert_eq!(text_of(&behind).0, js["text"].as_str().unwrap());
}

#[test]
fn an_update_yrs_makes_at_utf16_offsets_is_the_one_yjs_checks() {
    let js = yjs();
    let doc = crdt::doc(2);
    crdt::apply(&doc, &bytes(&js["state"])).unwrap();
    let before = js["text"].as_str().unwrap();

    // Right after the family emoji (8 UTF-16 units, three people and two joiners), then the first CJK character.
    let family = "👨‍👩‍👧";
    let after_family = units(&before[..before.find(family).unwrap() + family.len()]);
    let cjk = units(&before[..before.find('中').unwrap()]);
    let update = {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text(TEXT);
        text.insert(&mut txn, after_family, "Y");
        text.remove_range(&mut txn, cjk, 1);
        txn.encode_update_v1()
    };
    let want = before.replacen(family, &format!("{family}Y"), 1).replacen('中', "", 1);
    let (text, length) = text_of(&doc);
    assert_eq!(text, want);
    assert_eq!(length, units(&want));

    let state = doc.transact().encode_state_as_update_v1(&StateVector::default());
    let made = serde_json::json!({ "text": text, "length": length, "update": hex(&update), "state": hex(&state) });
    if std::env::var_os("MDPAD_BLESS").is_some() {
        std::fs::write(fixture("yrs.json"), serde_json::to_string_pretty(&made).unwrap() + "\n").unwrap();
    }
    let kept: Value = serde_json::from_str(&std::fs::read_to_string(fixture("yrs.json")).expect("yrs.json")).unwrap();
    for field in ["text", "length", "update"] {
        assert_eq!(kept[field], made[field], "{field}: yrs makes other bytes than yrs.json holds (MDPAD_BLESS=1)");
    }
    // The state in yrs.json is checked in Yjs; here, that it holds the same text.
    let whole = crdt::doc(5);
    crdt::apply(&whole, &bytes(&kept["state"])).unwrap();
    assert_eq!(text_of(&whole).0, want);
}

/// Client 1 types "abc", then "d" after it.
fn typed() -> (yrs::Doc, Vec<u8>, Vec<u8>) {
    let doc = crdt::doc(1);
    let edit = |at: u32, s: &str| {
        let mut txn = doc.transact_mut();
        let text = txn.get_or_insert_text(TEXT);
        text.insert(&mut txn, at, s);
        txn.encode_update_v1()
    };
    let (first, second) = (edit(0, "abc"), edit(3, "d"));
    (doc, first, second)
}

fn pending(doc: &yrs::Doc) -> bool {
    doc.transact().has_missing_updates()
}

#[test]
fn an_update_past_a_clients_clock_is_refused_and_changes_nothing() {
    let (_, first, second) = typed();
    let server = crdt::doc(9);
    assert_eq!(crdt::apply(&server, &second).unwrap_err(), Invalid::MissingDependency);
    assert!(!pending(&server));
    assert_eq!(text_of(&server).0, "");
    assert_eq!(server.transact().state_vector(), StateVector::default());

    // Still usable: the edits in order apply.
    crdt::apply(&server, &first).unwrap();
    crdt::apply(&server, &second).unwrap();
    assert_eq!(text_of(&server).0, "abcd");
}

#[test]
fn an_update_whose_origin_is_missing_is_dropped_and_leaves_nothing_pending() {
    let (_, first, _) = typed();
    // Client 3 holds "abc" and types after "a": its item's origin is client 1's, which the server lacks. Its own
    // clock starts at 0, so only applying finds it.
    let other = crdt::doc(3);
    crdt::apply(&other, &first).unwrap();
    let third = {
        let mut txn = other.transact_mut();
        let text = txn.get_or_insert_text(TEXT);
        text.insert(&mut txn, 1, "Z");
        txn.encode_update_v1()
    };
    let server = crdt::doc(9);
    let applied = crdt::apply(&server, &third).expect("decodes");
    assert!(applied.missing && applied.is_empty(), "{applied:?}");
    assert!(!pending(&server), "nothing waits for the missing origin");
    assert_eq!(text_of(&server).0, "");

    crdt::apply(&server, &first).unwrap();
    let again = crdt::apply(&server, &third).unwrap();
    assert!(!again.missing && !again.is_empty());
    assert_eq!(text_of(&server).0, "aZbc");
}

#[test]
fn a_delete_of_an_item_the_document_lacks_is_dropped() {
    let (writer, first, _) = typed();
    let delete = {
        let mut txn = writer.transact_mut();
        let text = txn.get_or_insert_text(TEXT);
        text.remove_range(&mut txn, 1, 1);
        txn.encode_update_v1()
    };
    let server = crdt::doc(9);
    let applied = crdt::apply(&server, &delete).unwrap();
    assert!(applied.missing && applied.is_empty(), "{applied:?}");
    assert!(!pending(&server));
    crdt::apply(&server, &first).unwrap();
    assert_eq!(text_of(&server).0, "abc", "the dropped delete does not wait to apply later");
    crdt::apply(&server, &delete).unwrap();
    assert_eq!(text_of(&server).0, "ac");
}

#[test]
fn an_update_with_a_hole_in_a_clients_clock_is_refused() {
    // Client 1's "abc" and an "e" typed before it later, merged without the "d" between: clocks 0..3 and 4, a hole at 3.
    let (writer, first, second) = typed();
    let later = {
        let mut txn = writer.transact_mut();
        let text = txn.get_or_insert_text(TEXT);
        text.insert(&mut txn, 0, "e");
        txn.encode_update_v1()
    };
    let holed = merge_updates_v1([first.as_slice(), later.as_slice()]).unwrap();
    let server = crdt::doc(9);
    assert_eq!(crdt::apply(&server, &holed).unwrap_err(), Invalid::MissingDependency);
    assert_eq!(server.transact().state_vector(), StateVector::default(), "nothing claims the hole");

    for update in [&first, &second, &later] {
        crdt::apply(&server, update).unwrap();
    }
    assert_eq!(text_of(&server).0, "eabcd", "the edit in the hole is not lost");
}

#[test]
fn an_update_yrs_fails_part_way_through_reports_what_it_changed() {
    let (_, first, _) = typed();
    let server = crdt::doc(9);
    crdt::apply(&server, &first).unwrap();
    // By hand, two clients: 3 types "Q" with no neighbours (it sorts after client 1's "abc"), 2 types "Z" into
    // client 1's "abc", which is a string and not a type. yrs integrates client 3's item, then fails on client 2's.
    let update = [
        0x02, // clients
        0x01, 0x03, 0x00, 0x04, 0x01, 0x02, b'm', b'd', 0x01, b'Q', // client 3: a string item, parent named "md"
        0x01, 0x02, 0x00, 0x04, 0x00, 0x01, 0x00, 0x01, b'Z', // client 2: a string item, parent the item (1, 0)
        0x00, // no deletes
    ];
    let applied = crdt::apply(&server, &update).expect("client 3's item applied, so not refused");
    assert!(applied.missing && !applied.is_empty(), "{applied:?}");
    assert!(!pending(&server), "nothing waits after a failed apply");
    assert_eq!(text_of(&server).0, "abcQ");
    // The diff is all that changed: a peer that applies it holds the same state.
    let peer = crdt::doc(10);
    crdt::apply(&peer, &first).unwrap();
    crdt::apply(&peer, &applied.diff).unwrap();
    assert_eq!(text_of(&peer), text_of(&server));
    assert_eq!(peer.transact().state_vector(), server.transact().state_vector());
    crdt::apply(&server, &typed().2).unwrap();
    assert_eq!(text_of(&server).0, "abcdQ", "still usable");
}

#[test]
fn bytes_that_are_not_an_update_are_refused() {
    let server = crdt::doc(9);
    for garbage in [&[0xffu8, 0xff, 0xff][..], &[0x01, 0x05], b"not an update"] {
        assert_eq!(crdt::apply(&server, garbage).unwrap_err(), Invalid::Decode, "{garbage:?}");
    }
    let (_, first, _) = typed();
    crdt::apply(&server, &first).unwrap();
    assert_eq!(text_of(&server).0, "abc");
}

#[test]
fn the_diff_is_only_what_was_new() {
    let (_, first, second) = typed();
    let server = crdt::doc(9);
    crdt::apply(&server, &first).unwrap();
    assert!(crdt::apply(&server, &first).unwrap().is_empty(), "a retry changes nothing");

    // A client's backlog, merged, of which the server holds the first part.
    let backlog = merge_updates_v1([first.as_slice(), second.as_slice()]).unwrap();
    let diff = crdt::apply(&server, &backlog).unwrap().diff;
    let fresh = crdt::doc(10);
    crdt::apply(&fresh, &first).unwrap();
    crdt::apply(&fresh, &diff).unwrap();
    assert_eq!(text_of(&fresh).0, "abcd");
    assert_eq!(
        Update::decode_v1(&diff).unwrap().state_vector_lower().get(&yrs::ClientID::new(1)),
        3,
        "the new part only"
    );
}
