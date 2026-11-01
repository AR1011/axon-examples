use std::sync::Mutex;

use axon::files::{FileOp, MemoryFiles};

use super::*;
use crate::ready;

/// Small segments, so that a test rolls them.
fn small(preallocate: bool) -> LogConfig {
    LogConfig {
        name: "journal".into(),
        segment_bytes: 80_000,
        preallocate,
        max_record: 4096,
        max_unwritten_bytes: 1 << 20,
        max_write_bytes: 70_000,
        ..LogConfig::default()
    }
}

fn cfg() -> WriterConfig {
    WriterConfig::new(3)
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// Three replicas on `MemoryFiles`, and a network that delivers at once to the ones up.
struct Net {
    replicas: Vec<Replica<usize>>,
    down: Vec<bool>,
    /// Reads to these replicas are lost.
    lose_reads: Vec<bool>,
}

impl Net {
    fn new() -> Net {
        let replicas =
            (0..3).map(|_| ready(Replica::open(Arc::new(MemoryFiles::new()), small(true), None, 0)).unwrap()).collect();
        Net { replicas, down: vec![false; 3], lose_reads: vec![false; 3] }
    }

    /// Carries the writer's requests and the answers, writing each replica's log after each round, until nothing
    /// moves. What the writer says besides sending.
    fn run(&mut self, w: &mut Writer) -> Vec<Out> {
        let mut said = Vec::new();
        loop {
            let mut answers = Vec::new();
            for out in w.take_out() {
                let Out::Send(r, request) = out else {
                    said.push(out);
                    continue;
                };
                if self.down[r] || self.lose_reads[r] && matches!(request, Request::Read { .. }) {
                    continue;
                }
                match self.replicas[r].handle(request, r) {
                    Handled::Now(r, a) => answers.push((r, a)),
                    Handled::Queued => {}
                    Handled::Read(r, read) => answers.push((r, ready(read).unwrap())),
                }
            }
            for (r, replica) in self.replicas.iter_mut().enumerate() {
                if let Some(write) = replica.take_write(0) {
                    let result = ready(write);
                    answers.extend(replica.written(result).into_iter().filter(|_| !self.down[r]));
                }
            }
            if answers.is_empty() {
                return said;
            }
            for (r, a) in answers {
                w.answer(r, a, 0);
            }
        }
    }

    fn set_down(&mut self, down: &[usize]) {
        self.down = (0..3).map(|r| down.contains(&r)).collect();
    }
}

fn opened(said: &[Out]) -> Vec<Bytes> {
    said.iter()
        .find_map(|o| match o {
            Out::Opened(journal) => Some(journal.clone()),
            _ => None,
        })
        .expect("opened")
}

fn last_acked(said: &[Out]) -> Option<u64> {
    said.iter().rev().find_map(|o| match o {
        Out::Acked(n) => Some(*n),
        _ => None,
    })
}

fn own_of(body: &Bytes) -> Own {
    match Entry::decode(body) {
        Ok(Entry::Own(own)) => own,
        other => panic!("not an Own: {other:?}"),
    }
}

#[test]
fn a_new_run_opens_empty_and_acks_on_a_majority() {
    let mut net = Net::new();
    let mut w = Writer::open(b("r1"), 1, 0, cfg(), 0);
    let said = net.run(&mut w);
    assert_eq!(opened(&said), Vec::<Bytes>::new());
    assert_eq!(last_acked(&said), Some(1), "its own Own");
    assert_eq!(w.append(b("a"), 0), Ok(1));
    assert_eq!(last_acked(&net.run(&mut w)), Some(2));
    let past = Bytes::from(vec![0; cfg().max_batch_bytes + 1]);
    assert_eq!(w.append(past, 0), Err(AppendError::TooLarge), "it would not fit a frame");
    net.set_down(&[2]);
    w.append(b("b"), 0).unwrap();
    assert_eq!(last_acked(&net.run(&mut w)), Some(3), "two of three ack");
    net.set_down(&[1, 2]);
    w.append(b("c"), 0).unwrap();
    assert_eq!(last_acked(&net.run(&mut w)), None, "one of three does not");
}

#[test]
fn a_newer_owner_reads_every_acked_record_and_fences_the_older() {
    let mut net = Net::new();
    let mut w1 = Writer::open(b("r1"), 1, 0, cfg(), 0);
    net.run(&mut w1);
    w1.append(b("a"), 0).unwrap();
    net.run(&mut w1);
    let mut w2 = Writer::open(b("r1"), 2, 0, cfg(), 0);
    let said = net.run(&mut w2);
    let journal = opened(&said);
    assert_eq!(journal.len(), 2);
    assert_eq!(own_of(&journal[0]), Own::default());
    assert_eq!(journal[1], b("a"));
    w1.append(b("late"), 0).unwrap();
    assert_eq!(net.run(&mut w1), vec![Out::Fenced(2 << 8)]);
    w2.append(b("b"), 0).unwrap();
    assert_eq!(last_acked(&net.run(&mut w2)), Some(4));
}

#[test]
fn the_newest_segment_counts_to_the_longest_answer_and_the_carry_keeps_its_tail() {
    let mut net = Net::new();
    let mut w1 = Writer::open(b("r1"), 1, 0, cfg(), 0);
    net.run(&mut w1);
    // "x" reaches r0 only: never acked.
    net.set_down(&[1, 2]);
    w1.append(b("x"), 0).unwrap();
    assert_eq!(last_acked(&net.run(&mut w1)), None);
    // w2 seals r0 and r1: it counts "x", the longest, and carries it, past the shortest.
    net.set_down(&[2]);
    let mut w2 = Writer::open(b("r1"), 2, 0, cfg(), 0);
    let said = net.run(&mut w2);
    assert_eq!(opened(&said)[1], b("x"));
    let own = own_of(&net_record(&net, 0, 2 << 8, 1));
    assert_eq!(own.prev, Some(Address { epoch: 1 << 8, seq: 2 }));
    assert_eq!(own.carry, vec![(Address { epoch: 1 << 8, seq: 2 }, b("x"))]);
    // w3 seals r1 and r2, which never held "x": it reads it from the carry.
    net.set_down(&[0]);
    let mut w3 = Writer::open(b("r1"), 3, 0, cfg(), 0);
    let journal = opened(&net.run(&mut w3));
    assert_eq!(journal.len(), 3);
    assert_eq!(journal[1], b("x"));
}

#[test]
fn without_the_carry_a_tail_counted_on_one_replica_is_lost() {
    let mut net = Net::new();
    let mut w1 = Writer::open(b("r1"), 1, 0, cfg(), 0);
    net.run(&mut w1);
    net.set_down(&[1, 2]);
    w1.append(b("x"), 0).unwrap();
    net.run(&mut w1);
    net.set_down(&[2]);
    let mut w2 = Writer::open(b("r1"), 2, 0, WriterConfig { mutation: Mutation::NoCarry, ..cfg() }, 0);
    net.run(&mut w2);
    net.set_down(&[0]);
    let mut w3 = Writer::open(b("r1"), 3, 0, cfg(), 0);
    let said = net.run(&mut w3);
    assert!(matches!(&said[..], [Out::Failed(why)] if why.contains("on no replica")), "{said:?}");
}

/// A record as one replica holds it.
fn net_record(net: &Net, replica: usize, epoch: u64, seq: u64) -> Bytes {
    let read = net.replicas[replica].log().read(b"r1", Address { epoch, seq }, usize::MAX);
    let entries = ready(read).unwrap();
    match entries.into_iter().next() {
        Some(LogEntry::Record { at, body }) if at == (Address { epoch, seq }) => body,
        other => panic!("no record ({epoch}, {seq}) on replica {replica}: {other:?}"),
    }
}

#[test]
fn a_start_under_the_same_lease_takes_the_next_epoch() {
    let mut net = Net::new();
    let mut w1 = Writer::open(b("r1"), 5, 0, cfg(), 0);
    net.run(&mut w1);
    w1.append(b("a"), 0).unwrap();
    net.run(&mut w1);
    drop(w1);
    let mut w2 = Writer::open(b("r1"), 5, 1, cfg(), 0);
    let said = net.run(&mut w2);
    assert_eq!(w2.epoch(), (5 << 8) + 1);
    assert_eq!(opened(&said).len(), 2);
}

#[test]
fn a_read_with_no_answer_goes_to_another_replica_and_back() {
    let mut net = Net::new();
    let mut w1 = Writer::open(b("r1"), 1, 0, cfg(), 0);
    net.run(&mut w1);
    w1.append(b("a"), 0).unwrap();
    net.run(&mut w1);
    // w2 seals r0 and r1, and its read of r0 is lost.
    net.set_down(&[2]);
    net.lose_reads = vec![true, true, false];
    let mut w2 = Writer::open(b("r1"), 2, 0, cfg(), 0);
    assert!(last_acked(&net.run(&mut w2)).is_none());
    let resend = cfg().resend_ms;
    // Its read of r1 is lost too, and r1 stays silent: the next goes to r0 again.
    w2.tick(resend);
    assert!(net.run(&mut w2).is_empty());
    net.lose_reads = vec![false, true, false];
    w2.tick(2 * resend);
    assert_eq!(opened(&net.run(&mut w2)).len(), 2);
}

#[test]
fn a_replica_holds_a_prefix_and_answers_gaps_and_sends_again() {
    let files = MemoryFiles::new();
    let mut r: Replica<()> = ready(Replica::open(Arc::new(files), small(true), None, 0)).unwrap();
    let append = |seq, records: &[&str]| Request::Append {
        run: b("r1"),
        epoch: 10,
        seq,
        records: records.iter().map(|s| b(s)).collect(),
    };
    let write = |r: &mut Replica<()>| {
        let write = r.take_write(0).expect("a write");
        r.written(ready(write)).into_iter().map(|(_, a)| a).collect::<Vec<_>>()
    };
    assert!(matches!(r.handle(append(3, &["c"]), ()), Handled::Now((), Answer::Gap { epoch: 10, have: 0 })));
    assert!(matches!(r.handle(append(1, &["a", "b"]), ()), Handled::Queued));
    assert_eq!(write(&mut r), vec![Answer::Acked { epoch: 10, have: 2 }]);
    // Sent again with one more: only "c" goes in.
    assert!(matches!(r.handle(append(1, &["a", "b", "c"]), ()), Handled::Queued));
    assert_eq!(write(&mut r), vec![Answer::Acked { epoch: 10, have: 3 }]);
    // Sent again whole: nothing new, answered at once.
    assert!(matches!(r.handle(append(2, &["b"]), ()), Handled::Now((), Answer::Acked { epoch: 10, have: 3 })));
    // A seal waits for its fence to reach disk, and refuses the older epoch at once.
    assert!(matches!(r.handle(Request::Seal { run: b("r1"), epoch: 11, token: 1 }, ()), Handled::Queued));
    assert!(matches!(r.handle(append(4, &["d"]), ()), Handled::Now((), Answer::Fenced { epoch: 10, fence: 11 })));
    let failed = r.take_write(0).expect("the fence");
    drop(failed);
    assert_eq!(r.written(Err(io::ErrorKind::Other.into())), vec![], "not durable: not answered");
    assert_eq!(write(&mut r), vec![Answer::Sealed { epoch: 11, segments: vec![(10, 3)] }]);
    assert!(matches!(
        r.handle(Request::Seal { run: b("r1"), epoch: 11, token: 2 }, ()),
        Handled::Now((), Answer::Fenced { .. })
    ));
    match r.handle(Request::Read { run: b("r1"), epoch: 10, from: 2, to: 9 }, ()) {
        Handled::Read((), read) => {
            assert_eq!(ready(read).unwrap(), Answer::Records { epoch: 10, from: 2, records: vec![b("b"), b("c")] })
        }
        _ => panic!("a read"),
    }
}

/// Files that keep the ops of a write instead of applying them.
#[derive(Clone)]
struct Capture {
    inner: MemoryFiles,
    ops: Arc<Mutex<Vec<FileOp>>>,
}

impl Files for Capture {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.inner.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.inner.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        *self.ops.lock().unwrap() = ops;
        Box::pin(std::future::ready(Ok(())))
    }
}

#[test]
fn a_crash_anywhere_inside_a_write_keeps_every_acked_record_and_a_prefix() {
    for preallocate in [false, true] {
        let inner = MemoryFiles::new();
        let mut r: Replica<()> = ready(Replica::open(Arc::new(inner.clone()), small(preallocate), None, 0)).unwrap();
        let append = |seq, n: u64| Request::Append {
            run: b("r1"),
            epoch: 7,
            seq,
            records: (seq..seq + n).map(|i| Bytes::from(format!("record {i}"))).collect(),
        };
        r.handle(append(1, 3), ());
        r.handle(Request::Append { run: b("r2"), epoch: 7, seq: 1, records: vec![b("other")] }, ());
        let write = r.take_write(0).unwrap();
        assert_eq!(r.written(ready(write)).len(), 2);
        // The next write, captured, then every crash inside it.
        let capture = Capture { inner: inner.clone(), ops: Arc::default() };
        let mut r: Replica<()> = ready(Replica::open(Arc::new(capture.clone()), small(preallocate), None, 0)).unwrap();
        r.handle(append(4, 4), ());
        r.handle(Request::Seal { run: b("r2"), epoch: 9, token: 1 }, ());
        let write = r.take_write(0).unwrap();
        ready(write).unwrap();
        let ops = capture.ops.lock().unwrap().clone();
        let mut ends = 0;
        for (n, files) in axon_testkit::crashes(&inner, &ops) {
            let mut r: Replica<()> = ready(Replica::open(Arc::new(files), small(preallocate), None, 0))
                .unwrap_or_else(|e| panic!("end {n} does not open: {e}"));
            let have = r.length(b"r1", 7);
            assert!((3..=7).contains(&have), "end {n}: {have} records");
            match r.handle(Request::Read { run: b("r1"), epoch: 7, from: 1, to: 99 }, ()) {
                Handled::Read((), read) => {
                    let Answer::Records { records, .. } = ready(read).unwrap() else { panic!("records") };
                    let want: Vec<Bytes> = (1..=have).map(|i| Bytes::from(format!("record {i}"))).collect();
                    assert_eq!(records, want, "end {n}");
                }
                _ => panic!("a read"),
            }
            assert_eq!(r.length(b"r2", 7), 1, "end {n}");
            // It takes the next record after what it kept.
            assert!(matches!(r.handle(append(have + 1, 1), ()), Handled::Queued), "end {n}");
            let write = r.take_write(0).unwrap();
            assert_eq!(r.written(ready(write)), vec![((), Answer::Acked { epoch: 7, have: have + 1 })]);
            ends += 1;
        }
        assert!(ends >= 2, "{ends} ends");
        if !preallocate {
            assert!(ends > 100, "every byte of an append ends a write: {ends} ends");
        }
    }
}

#[test]
fn a_seal_sent_again_is_answered_again_and_another_writer_s_is_fenced() {
    let mut r: Replica<()> = ready(Replica::open(Arc::new(MemoryFiles::new()), small(true), None, 0)).unwrap();
    let seal = |token| Request::Seal { run: b("r1"), epoch: 9, token };
    assert!(matches!(r.handle(seal(1), ()), Handled::Queued));
    // Sent again before the fence is durable: answered with the first, not refused.
    assert!(matches!(r.handle(seal(1), ()), Handled::Queued));
    let write = r.take_write(0).unwrap();
    assert_eq!(r.written(ready(write)).len(), 2);
    assert!(matches!(r.handle(seal(1), ()), Handled::Now((), Answer::Sealed { epoch: 9, .. })));
    assert!(matches!(r.handle(seal(2), ()), Handled::Now((), Answer::Fenced { epoch: 9, fence: 9 })));
}

#[test]
fn a_carry_past_its_bound_waits_for_every_replica_and_drops_what_was_never_acked() {
    let mut net = Net::new();
    let cfg = WriterConfig { max_carry_bytes: 8, ..cfg() };
    let mut w1 = Writer::open(b("r1"), 1, 0, cfg, 0);
    net.run(&mut w1);
    w1.append(b("acked"), 0).unwrap();
    net.run(&mut w1);
    // On r0 only: never acked, and past the carry's bound.
    net.set_down(&[1, 2]);
    w1.append(b("never acked"), 0).unwrap();
    net.run(&mut w1);
    net.set_down(&[2]);
    let mut w2 = Writer::open(b("r1"), 2, 1, cfg, 0);
    let said = net.run(&mut w2);
    assert!(!said.iter().any(|o| matches!(o, Out::Opened(_))), "it waits for r2: {said:?}");
    net.set_down(&[]);
    w2.tick(cfg.resend_ms);
    let journal = opened(&net.run(&mut w2));
    assert_eq!(journal[1..], [b("acked")]);
    let own = own_of(&net_record(&net, 2, 2 << 8, 1));
    assert_eq!((own.prev, own.carry), (Some(Address { epoch: 1 << 8, seq: 2 }), vec![]));
    assert_eq!(own.summary, Summary::of(&journal));
}

#[test]
fn a_replica_left_behind_is_repaired_from_a_peer_and_taken_back() {
    let mut net = Net::new();
    let cfg = WriterConfig { max_kept_bytes: 64, ..cfg() };
    let mut w = Writer::open(b("r1"), 1, 0, cfg, 0);
    net.run(&mut w);
    net.set_down(&[2]);
    for i in 0..10 {
        w.append(Bytes::from(format!("record {i:02}")), 0).unwrap();
        net.run(&mut w);
    }
    net.set_down(&[]);
    // Left behind: the writer no longer keeps what r2 lacks, and sends it nothing.
    w.append(b("after"), 0).unwrap();
    net.run(&mut w);
    assert_eq!(net.replicas[2].log().key(b"r1").map(|k| k.epochs), Some(vec![(1 << 8, 1)]));
    // Repair: r2 compares a page of r0's, and appends what r0 reads it.
    let Handled::Now(_, Answer::Pages { runs, more: false }) =
        net.replicas[0].handle(Request::Pages { after: None, limit: 10 }, 0)
    else {
        panic!("a page")
    };
    let lacks = net.replicas[2].lacks(&runs);
    assert_eq!(lacks, vec![(b("r1"), 1 << 8, 2, 12)]);
    for (run, epoch, from, to) in lacks {
        let Handled::Read(_, read) = net.replicas[0].handle(Request::Read { run: run.clone(), epoch, from, to }, 0)
        else {
            panic!("a read")
        };
        let Answer::Records { records, .. } = ready(read).unwrap() else { panic!("records") };
        net.replicas[2].handle(Request::Append { run, epoch, seq: from, records }, 2);
    }
    // The writer's probe finds it caught up, and sends it the next.
    w.tick(cfg.resend_ms);
    net.run(&mut w);
    w.append(b("last"), cfg.resend_ms).unwrap();
    net.run(&mut w);
    assert_eq!(net.replicas[2].log().key(b"r1").map(|k| k.epochs), Some(vec![(1 << 8, 13)]));
    // Its index followed the records in order.
    assert_eq!(net.replicas[2].index().get(b"r1").map(|ix| ix.len), Some(13));
}

#[test]
fn a_run_past_half_its_retention_is_not_taken_back_by_a_replica_that_removed_it() {
    let mut net = Net::new();
    for r in &mut net.replicas {
        r.set_retention(Retention { keep_us: 100, failed_us: 100, ..Retention::default() });
    }
    let mut w = Writer::open(b("r1"), 1, 0, cfg(), 0);
    net.run(&mut w);
    // r1 is behind, holding part of the run when it ends.
    w.append(b("a"), 0).unwrap();
    net.run(&mut w);
    net.set_down(&[1]);
    w.append(Entry::Completed { output: Bytes::new() }.encode(), 0).unwrap();
    net.run(&mut w);
    let page = |net: &mut Net, now_us| {
        net.replicas[0].now_us = now_us;
        let Handled::Now(_, Answer::Pages { runs, .. }) =
            net.replicas[0].handle(Request::Pages { after: None, limit: 9 }, 0)
        else {
            panic!("a page")
        };
        runs
    };
    // r2 removes its copy at the end of its retention; r0 has yet to.
    net.replicas[2].now_us = 101;
    assert_eq!(net.replicas[2].expire(10), 1);
    assert!(net.replicas[2].log().key(b"r1").is_none());
    // Before half its retention, a replica that lacks the end takes it, and so would one that removed it.
    let fresh = page(&mut net, 49);
    assert!(!fresh[0].2);
    assert_eq!(net.replicas[1].lacks(&fresh), vec![(b("r1"), 1 << 8, 3, 3)]);
    // Past half of it: the replica holding part still takes the rest, the one that removed it does not.
    let going = page(&mut net, 50);
    assert!(going[0].2);
    assert_eq!(net.replicas[1].lacks(&going), vec![(b("r1"), 1 << 8, 3, 3)]);
    assert_eq!(net.replicas[2].lacks(&going), vec![]);
}
