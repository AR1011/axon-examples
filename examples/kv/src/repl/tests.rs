use bytes::Bytes;

use super::*;

fn b(round: u64, node: u8) -> Ballot {
    Ballot { round, node, boot: 7, start: 1 }
}

fn set(k: &str, v: &str) -> Op {
    Op::Set(vec![(Bytes::copy_from_slice(k.as_bytes()), Bytes::copy_from_slice(v.as_bytes()))])
}

fn e(ballot: Ballot, k: &str, v: &str) -> Entry {
    Entry { ballot, op: set(k, v) }
}

fn append(ballot: Ballot, prev: Pos, entries: Vec<Entry>, committed: u64) -> ToLog {
    ToLog::Append { ballot, seq: 1, prev, entries, committed }
}

fn pos(ballot: Ballot, index: u64) -> Pos {
    Pos { ballot, index }
}

/// Every message the owner sends delivered at once, every record on disk at once, until nothing moves.
fn pump(owner: &mut Owner, logs: &mut [Replica], now: u64) -> Vec<Out> {
    let mut answers = Vec::new();
    loop {
        let out = owner.take_out();
        if out.is_empty() {
            return answers;
        }
        for o in out {
            match o {
                Out::Send(log, msg) => {
                    let (reply, _) = logs[log].handle(msg);
                    owner.on_reply(log, reply, now);
                }
                other => answers.push(other),
            }
        }
    }
}

#[test]
fn a_ballot_orders_by_round_then_node_boot_and_start() {
    let base = Ballot { round: 5, node: 1, boot: 9, start: 3 };
    assert!(Ballot { round: 6, node: 0, boot: 0, start: 0 } > base);
    assert!(Ballot { node: 2, boot: 0, start: 0, ..base } > base);
    assert!(Ballot { boot: 10, start: 0, ..base } > base);
    assert!(Ballot { start: 4, ..base } > base);
    assert!(pos(b(2, 0), 1) > pos(b(1, 0), 9), "the ballot of the last entry counts before its index");
}

#[test]
fn an_append_needs_the_entry_before_it_and_cuts_what_conflicts() {
    let mut r = Replica::default();
    let (a, recs) = r.handle(append(b(1, 0), Pos::default(), vec![e(b(1, 0), "a", "1"), e(b(1, 0), "b", "1")], 0));
    assert_eq!(a, FromLog::Appended { ballot: b(1, 0), seq: 1, matched: 2 });
    assert!(matches!(recs[..], [Rec::Promise(_), Rec::Entries { after: 0, .. }]));

    let (a, recs) = r.handle(append(b(1, 0), pos(b(1, 0), 5), vec![e(b(1, 0), "c", "1")], 0));
    assert!(matches!(a, FromLog::Behind { .. }), "a gap: {a:?}");
    assert!(recs.is_empty());
    let (a, _) = r.handle(append(b(1, 0), pos(b(9, 9), 2), vec![], 0));
    assert!(matches!(a, FromLog::Behind { .. }), "another ballot at prev: {a:?}");

    // A duplicate writes nothing; a newer owner's entry at 2 cuts the old one.
    let (_, recs) = r.handle(append(b(1, 0), pos(b(1, 0), 1), vec![e(b(1, 0), "b", "1")], 0));
    assert!(recs.is_empty(), "{recs:?}");
    let (a, recs) = r.handle(append(b(2, 1), pos(b(1, 0), 1), vec![e(b(2, 1), "x", "2")], 0));
    assert_eq!(a, FromLog::Appended { ballot: b(2, 1), seq: 1, matched: 2 });
    assert!(matches!(&recs[..], [Rec::Promise(p), Rec::Entries { after: 1, .. }] if *p == b(2, 1)));
    assert_eq!(r.entry(2).unwrap().ballot, b(2, 1));

    let (a, _) = r.handle(append(b(1, 0), pos(b(1, 0), 2), vec![], 0));
    assert_eq!(a, FromLog::Fenced { ballot: b(1, 0), seq: 1, promised: b(2, 1) });
}

#[test]
fn a_log_commits_only_what_the_append_matched() {
    // The log holds an old owner's entries at 1 and 2. The new owner committed its own entry at 1 elsewhere and
    // sends an Append that matches nothing past index 0: the log must not commit its stale entries.
    let mut r = Replica::default();
    r.handle(append(b(1, 0), Pos::default(), vec![e(b(1, 0), "a", "old"), e(b(1, 0), "b", "old")], 0));
    let (a, recs) = r.handle(append(b(2, 1), Pos::default(), vec![], 2));
    assert_eq!(a, FromLog::Appended { ballot: b(2, 1), seq: 1, matched: 0 });
    assert_eq!(r.committed(), 0, "{recs:?}");
    // Once it matched the new entry at 1, it commits 1, and not its old 2.
    r.handle(append(b(2, 1), Pos::default(), vec![e(b(2, 1), "a", "new")], 2));
    assert_eq!(r.committed(), 1);
    assert_eq!(r.applied().get(&b"a"[..]).unwrap(), &b"new"[..]);
}

#[test]
fn replaying_the_records_gives_the_replica_back() {
    let mut r = Replica::default();
    let mut disk = Vec::new();
    let mut step = |r: &mut Replica, msg| {
        let (_, recs) = r.handle(msg);
        disk.extend(recs);
        assert_eq!(Replica::replay(&disk, Weak::default()).state(), r.state());
    };
    step(&mut r, ToLog::Open { ballot: b(1, 0), state: false });
    step(&mut r, append(b(1, 0), Pos::default(), vec![e(b(1, 0), "a", "1"), e(b(1, 0), "b", "1")], 1));
    step(&mut r, append(b(2, 1), pos(b(1, 0), 1), vec![e(b(2, 1), "b", "2"), e(b(2, 1), "c", "2")], 3));
    step(&mut r, ToLog::Install { ballot: b(3, 2), seq: 1, snap: Snapshot { pos: pos(b(3, 2), 9), map: Map::new() } });
    assert_eq!(r.last(), pos(b(3, 2), 9));
    assert_eq!(r.committed(), 9);
    let cp = r.checkpoint();
    disk = vec![cp];
    assert_eq!(Replica::replay(&disk, Weak::default()).state(), r.state());
}

#[test]
fn a_checkpoint_holds_the_promise() {
    let mut r = Replica::default();
    r.handle(append(b(1, 0), Pos::default(), vec![e(b(1, 0), "a", "1")], 1));
    r.handle(ToLog::Open { ballot: b(5, 2), state: false });
    let cp = r.checkpoint();
    let back = Replica::replay([&cp], Weak::default());
    assert_eq!(back.promised(), b(5, 2));
    assert_eq!(back.base(), pos(b(1, 0), 1));
    assert_eq!(r.base(), Pos::default(), "the replica keeps its committed entry; the checkpoint does not");
    let weak = Weak { checkpoint_drops_promise: true, ..Weak::default() };
    let cp = Replica::replay([&cp], weak).checkpoint();
    assert_eq!(Replica::replay([&cp], weak).promised(), Ballot::default(), "the weakened line loses it");
}

#[test]
fn an_install_keeps_a_matching_suffix_and_never_goes_back() {
    let mut r = Replica::default();
    r.handle(append(b(1, 0), Pos::default(), (0..4).map(|i| e(b(1, 0), "k", &i.to_string())).collect(), 1));
    let mut map = Map::new();
    map.insert(Bytes::from_static(b"k"), Bytes::from_static(b"1"));
    let snap = Snapshot { pos: pos(b(1, 0), 2), map };
    let (a, _) = r.handle(ToLog::Install { ballot: b(1, 0), seq: 4, snap: snap.clone() });
    assert_eq!(a, FromLog::Appended { ballot: b(1, 0), seq: 4, matched: 2 });
    assert_eq!((r.committed(), r.last()), (2, pos(b(1, 0), 4)), "the entries after it are kept");
    r.handle(append(b(1, 0), pos(b(1, 0), 4), vec![], 4));
    assert_eq!(r.committed(), 4);
    r.handle(ToLog::Install { ballot: b(1, 0), seq: 5, snap });
    assert_eq!(r.committed(), 4, "an older map is not installed");
    assert_eq!(r.applied().get(&b"k"[..]).unwrap(), &b"3"[..]);
}

#[test]
fn an_owner_goes_on_from_the_highest_last_ballot_not_the_longest() {
    // Log 0 is longest with an old owner's entries; log 1 holds a later owner's entry, which was committed with
    // log 2 before log 2 crashed and lost nothing it acked. The owner hears logs 0 and 1.
    let mut logs = [Replica::default(), Replica::default(), Replica::default()];
    let old: Vec<Entry> = (0..3).map(|i| e(b(1, 0), "a", &format!("old{i}"))).collect();
    logs[0].handle(append(b(1, 0), Pos::default(), old, 0));
    for log in [1, 2] {
        logs[log].handle(append(b(2, 1), Pos::default(), vec![e(b(2, 1), "a", "acked")], 0));
    }
    let mut owner = Owner::start(b(3, 0), Config::new(3, Some(0)), 0);
    let mut out = owner.take_out();
    out.retain(|o| !matches!(o, Out::Send(2, _)));
    for o in out {
        let Out::Send(log, msg) = o else { unreachable!() };
        let (reply, _) = logs[log].handle(msg);
        owner.on_reply(log, reply, 0);
    }
    // Log 2 is down from here on: drop what goes to it.
    loop {
        let out = owner.take_out();
        if out.is_empty() {
            break;
        }
        for o in out {
            if let Out::Send(log @ 0..=1, msg) = o {
                let (reply, _) = logs[log].handle(msg);
                owner.on_reply(log, reply, 0);
            }
        }
    }
    assert_eq!(owner.status(), Status::Serving);
    assert_eq!(owner.map().get(&b"a"[..]).unwrap(), &b"acked"[..]);
    assert_eq!(logs[0].applied().get(&b"a"[..]).unwrap(), &b"acked"[..], "log 0's old entries were cut");
}

#[test]
fn an_owner_serves_only_once_a_majority_holds_begin() {
    let mut logs = [Replica::default(), Replica::default(), Replica::default()];
    let mut owner = Owner::start(b(1, 0), Config::new(3, Some(0)), 0);
    for o in owner.take_out() {
        let Out::Send(log, msg) = o else { unreachable!() };
        let (reply, _) = logs[log].handle(msg);
        owner.on_reply(log, reply, 0);
    }
    assert_eq!(owner.status(), Status::Beginning);
    assert!(owner.write(set("a", "1"), 1).is_err());
    let out = owner.take_out();
    assert_eq!(out.len(), 3, "an Append with Begin to each log: {out:?}");
    // Only log 0 answers: no majority.
    let Out::Send(0, msg) = out[0].clone() else { unreachable!() };
    owner.on_reply(0, logs[0].handle(msg).0, 0);
    assert_eq!(owner.status(), Status::Beginning);
    let Out::Send(1, msg) = out[1].clone() else { unreachable!() };
    owner.on_reply(1, logs[1].handle(msg).0, 0);
    assert_eq!(owner.status(), Status::Serving);
    assert_eq!(owner.committed(), 1);
}

#[test]
fn a_refused_start_opens_again_above_the_promise_three_times() {
    let mut log = Replica::default();
    log.handle(ToLog::Open { ballot: b(10, 2), state: false });
    let mut owner = Owner::start(b(4, 0), Config::new(1, Some(0)), 0);
    pump(&mut owner, std::slice::from_mut(&mut log), 0);
    assert_eq!(owner.status(), Status::Serving);
    assert_eq!(owner.ballot().round, 11);

    let mut cfg = Config::new(1, Some(0));
    cfg.reopens = 0;
    let mut owner = Owner::start(b(4, 0), cfg, 0);
    pump(&mut owner, std::slice::from_mut(&mut log), 0);
    assert_eq!(owner.status(), Status::Stopped(Stop::Deposed(b(11, 0))));
}

#[test]
fn one_log_serves_through_the_same_path() {
    let mut logs = vec![Replica::default()];
    let mut owner = Owner::start(b(1, 0), Config::new(1, Some(0)), 0);
    pump(&mut owner, &mut logs, 0);
    assert_eq!(owner.status(), Status::Serving);
    owner.write(set("a", "1"), 7).unwrap();
    owner.write(Op::Del(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]), 8).unwrap();
    owner.flush(0);
    let out = pump(&mut owner, &mut logs, 0);
    assert_eq!(out, vec![Out::Acked { token: 7, index: 2, result: 0 }, Out::Acked { token: 8, index: 3, result: 1 }]);
    assert_eq!(logs[0].committed(), 3, "the Append after the acks carries the commit");
}

#[test]
fn a_quorum_read_waits_for_a_round_sent_after_it() {
    let mut logs = [Replica::default(), Replica::default(), Replica::default()];
    let mut owner = Owner::start(b(1, 0), Config::new(3, Some(0)), 0);
    pump(&mut owner, &mut logs, 0);
    owner.write(set("a", "1"), 1).unwrap();
    owner.flush(0);
    pump(&mut owner, &mut logs, 0);
    assert!(owner.read(b"a", 2));
    assert!(owner.take_out().is_empty(), "nothing before flush");
    owner.flush(0);
    let out = owner.take_out();
    assert_eq!(out.iter().filter(|o| matches!(o, Out::Send(..))).count(), 3);
    for o in out {
        let Out::Send(log, msg) = o else { unreachable!() };
        owner.on_reply(log, logs[log].handle(msg).0, 0);
    }
    let out = pump(&mut owner, &mut logs, 0);
    assert_eq!(out, vec![Out::Read { token: 2, value: Some(Bytes::from_static(b"1")) }]);
}

#[test]
fn an_append_still_running_is_not_sent_again_and_one_that_failed_is() {
    let mut logs = [Replica::default(), Replica::default(), Replica::default()];
    let mut owner = Owner::start(b(1, 0), Config::new(3, Some(0)), 0);
    pump(&mut owner, &mut logs, 0);
    let big = Op::Set(vec![(Bytes::from_static(b"a"), Bytes::from(vec![7; 10 << 20]))]);
    owner.write(big, 1).unwrap();
    owner.flush(0);
    // Messages are counted, not printed: each holds 10 MiB.
    let sent = owner.take_out();
    assert_eq!(sent.len(), 3);
    // 10 MiB to each log, unanswered for a minute: going again would send it twice while the first still runs.
    owner.tick(60_000);
    assert_eq!(owner.take_out().len(), 0, "nothing goes again while the calls run");
    let Out::Send(2, ToLog::Append { seq, .. }) = &sent[2] else { unreachable!() };
    owner.failed(2, *seq);
    owner.tick(60_000);
    let again = owner.take_out();
    assert!(matches!(&again[..], [Out::Send(2, ToLog::Append { entries, .. })] if entries.len() == 1));
    // The answers to the first calls still count.
    for o in sent {
        let Out::Send(log, msg) = o else { unreachable!() };
        owner.on_reply(log, logs[log].handle(msg).0, 60_000);
    }
    assert!(owner.take_out().contains(&Out::Acked { token: 1, index: 2, result: 0 }));
}

/// An owner keeps the newest committed entries that fit in `keep_bytes`, for a log a few entries behind; a log behind
/// them gets the map. Kept by count alone (4096), the bench's 10 MB values were every write since the shard started,
/// and a node passed 21 GB.
#[test]
fn an_owner_keeps_committed_entries_within_keep_bytes() {
    let mut logs = [Replica::default(), Replica::default(), Replica::default()];
    let mut cfg = Config::new(3, Some(0));
    cfg.keep_bytes = 4 << 10;
    let mut owner = Owner::start(b(1, 0), cfg, 0);
    pump(&mut owner, &mut logs, 0);
    // Log 2 does not answer: logs 0 and 1 commit each write.
    let mut lost = None;
    for token in 0..20 {
        owner.write(set("k", &"v".repeat(1000)), token).unwrap();
        owner.flush(0);
        loop {
            let out = owner.take_out();
            if out.is_empty() {
                break;
            }
            for o in out {
                match o {
                    Out::Send(2, msg) => lost = lost.or(msg.seq()),
                    Out::Send(log, msg) => owner.on_reply(log, logs[log].handle(msg).0, 0),
                    _ => {}
                }
            }
        }
        assert!(owner.kept() <= 4 << 10, "{} bytes kept after write {token}", owner.kept());
    }
    assert_eq!(owner.committed(), 21);
    assert_eq!(owner.kept(), 4 * 1001, "the newest four of 1,001 bytes");
    // Log 2's Append failed: it goes again from Begin, which the owner no longer keeps.
    owner.failed(2, lost.expect("an Append to log 2"));
    owner.tick(1000);
    let out = owner.take_out();
    let to_2 = out.iter().find(|o| matches!(o, Out::Send(2, _)));
    assert!(matches!(to_2, Some(Out::Send(2, ToLog::Install { snap, .. })) if snap.pos.index == 21), "{to_2:?}");
}

fn encoded(rec: &Rec) -> Bytes {
    let mut out = bytes::BytesMut::new();
    codec::put_rec(&mut out, rec);
    out.freeze()
}

#[test]
fn records_read_back_as_written_and_a_cut_one_reads_as_none() {
    let mut r = Replica::default();
    let mut recs = Vec::new();
    for msg in [
        ToLog::Open { ballot: b(1, 0), state: false },
        append(b(1, 0), Pos::default(), vec![e(b(1, 0), "a", "1"), Entry { ballot: b(1, 0), op: Op::Begin }], 1),
        append(
            b(2, 1),
            pos(b(1, 0), 1),
            vec![Entry { ballot: b(2, 1), op: Op::Del(vec![Bytes::from_static(b"a")]) }],
            2,
        ),
        append(b(2, 1), pos(b(2, 1), 2), vec![Entry { ballot: b(2, 1), op: Op::Clear }], 2),
        ToLog::Install { ballot: b(3, 2), seq: 1, snap: Snapshot { pos: pos(b(3, 2), 9), map: Map::new() } },
    ] {
        recs.extend(r.handle(msg).1);
    }
    recs.push(r.checkpoint());
    let kinds: Vec<u8> = recs.iter().map(|rec| encoded(rec)[0]).collect();
    for kind in 1..=5 {
        assert!(kinds.contains(&kind), "record kind {kind} is covered: {recs:?}");
    }
    for rec in &recs {
        let bytes = encoded(rec);
        assert_eq!(codec::get_rec(&mut bytes.clone()), Some(rec.clone()));
        for cut in 0..bytes.len() {
            assert_eq!(codec::get_rec(&mut bytes.slice(..cut)), None, "{rec:?} cut at {cut}");
        }
    }
    assert_eq!(codec::get_rec(&mut Bytes::from_static(&[9])), None, "an unknown kind");
}

#[test]
fn a_state_reads_back_whole_or_not_at_all() {
    let mut r = Replica::default();
    r.handle(append(b(1, 0), Pos::default(), (0..5).map(|i| e(b(1, 0), &format!("k{i}"), "v")).collect(), 3));
    r.handle(ToLog::Open { ballot: b(4, 1), state: false });
    let mut out = bytes::BytesMut::new();
    codec::put_state(&mut out, r.state());
    let bytes = out.freeze();
    let back = codec::get_state(&mut bytes.clone()).expect("a state");
    assert_eq!(&back, r.state());
    assert_eq!(Replica::restore(back, Weak::default()).promised(), b(4, 1));
    // A committed index past the last entry is not a state `State::apply` could give.
    let mut bad = bytes.to_vec();
    let at = 25 + 33 + 4 + 5 * (26 + 4 + 8 + 2 + 1);
    bad[at..at + 8].copy_from_slice(&99u64.to_be_bytes());
    assert_eq!(codec::get_state(&mut Bytes::from(bad)), None);
}
