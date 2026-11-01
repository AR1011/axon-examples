//! The World stories of design §8: three homes, two web nodes and twelve bots on three documents, through a crash of
//! a home or a web node, a cut between them, a home cut off from all, a client that does not read and a full disk.
//! Each story ends with every bot settled and the invariants checked: nothing acked or seen is lost (I1, I2) and
//! every copy is the server's (I3). Story 7 is journey 7: a restore while a client is offline (I7).

#[path = "support/world.rs"]
mod world;

use std::collections::BTreeSet;
use std::time::Duration;

use axon_cluster::KEYED_ENDS;
use axon_testkit::world::Crash;
use axon_testkit::{check_seeds, seeds};
use mdpad::core::acl::Principal;
use mdpad::core::versions::Kind;
use mdpad::crdt;
use mdpad::wire::{DocAnswer, DocReq};
use world::Story;

const S: fn(u64) -> Duration = Duration::from_secs;

fn saved(bots: &[world::Shared]) -> usize {
    bots.iter().map(|b| b.lock().unwrap().saved.len()).sum()
}

fn end(mut s: Story) {
    s.join_bots();
    s.settle();
    s.check();
}

/// Story 1: a home crashes under load and comes back. What it acked or published was on its disk: the text it loads
/// holds every token its clients were told is saved and every token they saw, before any client sends its copy
/// again (I1, I2). Before a machine crash its disk refuses writes for a second, so what it applied in that second is
/// not on disk when it goes.
fn home_crash(seed: u64, how: Crash) {
    let mut s = Story::new(seed);
    s.run_bots(S(25));
    let at = Duration::from_millis(s.w.pick(5000..7000));
    s.w.run_for(at);
    let bots = s.home_bots("h2");
    let disk = s.w.disk("h2");
    if matches!(how, Crash::Machine) {
        disk.set_capacity(Some(u64::MAX));
        disk.set_capacity(Some(u64::MAX - disk.free()));
        s.w.run_for(S(1));
    }
    // Its clients leave first: once it is back, the text it loads is all there is.
    s.hold(&bots, true);
    s.w.run_for(Duration::from_millis(300));
    let kept = s.kept(&bots);
    s.w.crash("h2", how);
    disk.set_capacity(None);
    s.w.run_for(S(3));
    s.w.restart("h2");
    let doc = bots[0].lock().unwrap().doc.clone();
    let text = s.text(&doc);
    let held: BTreeSet<&str> = text.split_whitespace().collect();
    let lost: Vec<_> = kept.iter().filter(|t| !held.contains(t.as_str())).collect();
    assert!(lost.is_empty(), "seed {seed}: saved or seen, then lost in the crash: {lost:?}");
    s.hold(&bots, false);
    let before = saved(&bots);
    s.w.run_for(S(14));
    assert!(saved(&bots) > before, "seed {seed}: no edit of h2's document saved after the restart");
    end(s);
}

#[test]
fn home_crash_process() {
    check_seeds(seeds(0..64), |seed| home_crash(seed, Crash::Process)).unwrap();
}

#[test]
fn home_crash_machine() {
    check_seeds(seeds(0..64), |seed| home_crash(seed, Crash::Machine)).unwrap();
}

/// Story 2 (I8): a web node crashes. Its clients' cursors leave the others' views once the homes see it leave, not
/// at the 30 s expiry: within 5 s of a process crash (its sessions close at once), within 13 s of a machine crash and
/// not before 3 s (the seeds must miss its heartbeats). Its clients join again through the other web node with their
/// unsaved edits.
fn web_crash(seed: u64, how: Crash, within: Duration) -> Duration {
    let mut s = Story::new(seed);
    s.run_bots(S(30));
    s.w.run_for(S(6));
    let gone: BTreeSet<u64> = s.bots_on("w1").iter().map(|b| s.client(b)).collect();
    let watchers = s.bots_on("w2");
    let shows = |s: &Story| watchers.iter().any(|b| !s.shown(b).is_disjoint(&gone));
    assert!(shows(&s), "seed {seed}: no cursor of w1's clients reached w2's");
    s.w.crash("w1", how);
    let start = s.w.now();
    while shows(&s) {
        assert!(s.w.now() - start < within, "seed {seed}: w1's cursors still shown after {within:?}");
        s.w.run_for(Duration::from_millis(100));
    }
    let left = s.w.now() - start;
    for bot in s.bots_on("w1") {
        let mut b = bot.lock().unwrap();
        b.core.disconnected();
        b.web = "w2".into();
        drop(b);
        s.run_bot(bot, S(10));
    }
    s.w.restart("w1");
    end(s);
    left
}

#[test]
fn web_crash_process() {
    check_seeds(seeds(0..64), |seed| {
        let left = web_crash(seed, Crash::Process, S(5));
        println!("process crash, seed {seed}: cursors gone after {:.1} s", left.as_secs_f64());
    })
    .unwrap();
}

#[test]
fn web_crash_machine() {
    check_seeds(seeds(0..64), |seed| {
        let left = web_crash(seed, Crash::Machine, S(13));
        assert!(left >= S(3), "seed {seed}: left at {left:?}, before the seeds could know");
        println!("machine crash, seed {seed}: cursors gone after {:.1} s", left.as_secs_f64());
    })
    .unwrap();
}

/// Story 3: a web node cannot reach one home for 10 s. Its clients of that home's document wait and keep their
/// edits; the other web node's clients go on; after the heal everything converges.
#[test]
fn partition_web_home() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed);
        s.run_bots(S(35));
        s.w.run_for(S(6));
        let others: Vec<_> = s.home_bots("h2").into_iter().filter(|b| b.lock().unwrap().web == "w2").collect();
        let before = saved(&others);
        s.w.partition(&["w1"], &["h2"]);
        s.w.run_for(S(10));
        s.w.heal();
        assert!(saved(&others) > before, "seed {seed}: w2's clients of h2 saved nothing during the cut");
        end(s);
    })
    .unwrap();
}

/// Story 4: a home cut off from every node. Phase 0 measured that a cut must pass about 6 s for the home to call
/// itself isolated and stop its documents at a heartbeat grace of 1 s; a cut of 2 s stops nothing. Either way the
/// document answers again soon after the heal, and only on its home.
fn isolate_home(seed: u64, cut: Duration) -> (bool, Duration) {
    let mut s = Story::new(seed);
    s.run_bots(S(40));
    s.w.run_for(S(6));
    let isolated = |s: &Story| {
        let at = KEYED_ENDS.iter().position(|e| *e == "isolated").expect("an end");
        s.w.read("h3", |n| n.get().cluster.hosted(10).kinds.iter().map(|k| k.ended[at]).sum::<u64>()).expect("h3 is up")
    };
    let ended = isolated(&s);
    let bots = s.home_bots("h3");
    s.w.partition(&["h3"], &["s", "h1", "h2", "w1", "w2"]);
    s.w.run_for(cut);
    s.w.heal();
    let healed = s.w.now();
    let before = saved(&bots);
    while saved(&bots) == before {
        assert!(s.w.now() - healed < S(15), "seed {seed}: h3's document saved nothing within 15 s of the heal");
        s.w.run_for(Duration::from_millis(100));
    }
    let back = s.w.now() - healed;
    let stopped = isolated(&s) > ended;
    end(s);
    (stopped, back)
}

#[test]
fn isolate_home_2s() {
    check_seeds(seeds(0..64), |seed| {
        let (stopped, back) = isolate_home(seed, S(2));
        assert!(!stopped, "seed {seed}: a 2 s cut stopped the document");
        println!("cut 2 s, seed {seed}: saving again {:.1} s after the heal", back.as_secs_f64());
    })
    .unwrap();
}

#[test]
fn isolate_home_8s() {
    check_seeds(seeds(0..64), |seed| {
        let (stopped, back) = isolate_home(seed, S(8));
        println!("cut 8 s, seed {seed}: stopped {stopped}, saving again {:.1} s after the heal", back.as_secs_f64());
    })
    .unwrap();
}

/// Story 5: a web node starved and one client that stops reading for 8 s. The session holds what it can and pauses
/// that client's subscription; the others on the node go on, and the slow one catches up after.
#[test]
fn slow_client() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed);
        s.run_bots(S(30));
        s.w.run_for(S(6));
        let slow = s.bots_on("w2").into_iter().find(|b| !b.lock().unwrap().editor).expect("a viewer on w2");
        let others: Vec<_> = s.bots_on("w2").into_iter().filter(|b| b.lock().unwrap().editor).collect();
        let before = saved(&others);
        // The bot reads the World's clock in its task: the stall is set from inside w2.
        let stall = slow.clone();
        s.w.block_on("w2", move |_| async move {
            stall.lock().unwrap().stall_until = Some(tokio::time::Instant::now() + S(8));
        })
        .expect("w2 is up");
        s.w.starve("w2", 3);
        s.w.run_for(S(8));
        s.w.starve("w2", 1);
        assert!(saved(&others) > before, "seed {seed}: the other clients on w2 saved nothing during the stall");
        end(s);
    })
    .unwrap();
}

/// Story 6: a home's disk fills. Its document answers "cannot save" and takes nothing it cannot write; once there is
/// room again the clients' kept edits are saved.
#[test]
fn disk_full() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed);
        s.run_bots(S(30));
        s.w.run_for(S(6));
        let disk = s.w.disk("h3");
        disk.set_capacity(Some(u64::MAX));
        let used = u64::MAX - disk.free();
        disk.set_capacity(Some(used + 512));
        s.w.run_for(S(6));
        disk.set_capacity(None);
        let bots = s.home_bots("h3");
        let refused: u64 = bots.iter().map(|b| b.lock().unwrap().notices("cannot_save")).sum();
        assert!(refused > 0, "seed {seed}: a full disk refused nothing");
        let before = saved(&bots);
        s.w.run_for(S(8));
        assert!(saved(&bots) > before, "seed {seed}: nothing saved once the disk had room");
        end(s);
    })
    .unwrap();
}

/// Story 7, journey 7 (I7): the text is named at "10:40", Ana adds a draft, Ben goes offline and types after it, and
/// Ana restores "10:40". Ana, online, sees the restored text at once. Ben comes back and the text is exactly the
/// restored one with his sentence: his new characters stay, the restored-away ones never come back. On some seeds the
/// home crashes between the restore and Ben's return. The version list only grew.
#[test]
fn restore_offline() {
    check_seeds(seeds(0..64), |seed| {
        let mut s = Story::new(seed);
        let doc = s.docs[s.w.pick(0..3) as usize].clone();
        let home = mdpad::doc::home_of(&doc).to_string();
        let ann = || Principal::User(world::USER.into());
        let set = |s: &mut Story, markdown: &str| {
            let answer = s.ask(&doc, DocReq::Replace { who: ann(), markdown: markdown.into() });
            assert!(matches!(answer, DocAnswer::Version(..)), "seed {seed}: {answer:?}");
        };
        let versions = |s: &mut Story| match s.ask(&doc, DocReq::Versions { who: ann() }) {
            DocAnswer::Versions(list) => list,
            other => panic!("seed {seed}: {other:?}"),
        };
        set(&mut s, "# Plan\n\nIntro.\n");
        let DocAnswer::Version(named, _) = s.ask(&doc, DocReq::NameVersion { who: ann(), name: "10:40".into() }) else {
            panic!("seed {seed}: no named version");
        };
        let draft = "# Plan\n\nIntro.\n## Draft\nOld idea.\n";
        set(&mut s, draft);

        let of_doc: Vec<_> = s.bots.iter().filter(|b| b.lock().unwrap().doc == doc).cloned().collect();
        let (ana, ben) = (of_doc[0].clone(), of_doc[1].clone());
        let synced = s.w.spawn_on("w2", {
            let ben = ben.clone();
            move |n| world::settle(n, ben, S(30))
        });
        assert!(s.w.join(synced).expect("w2 is up"), "seed {seed}: Ben did not sync");
        {
            let mut b = ben.lock().unwrap();
            assert_eq!(b.text(), draft);
            let at = crdt::len(&b.core.doc) - 1;
            let update = crdt::insert(&b.core.doc, at, " Better idea.");
            b.tokens.push("Better".into());
            assert!(b.core.local(update, 0).is_none(), "offline: nothing sent");
        }
        let online = s.w.spawn_on("w1", {
            let ana = ana.clone();
            move |n| world::drive(n, ana, None, S(8))
        });
        s.w.run_for(S(2));

        let before = versions(&mut s);
        let answer = s.ask(&doc, DocReq::Restore { who: ann(), n: named.n });
        assert!(
            matches!(&answer, DocAnswer::Version(v, _) if v.kind == Kind::BeforeRestore),
            "seed {seed}: {answer:?}"
        );
        s.w.run_for(S(1));
        let restored = "# Plan\n\nIntro.\n";
        assert_eq!(s.text(&doc), restored, "seed {seed}");
        {
            let a = ana.lock().unwrap();
            assert_eq!(a.text(), restored, "seed {seed}: Ana online sees the restore at once");
            assert_eq!(a.notices("restored"), 1, "seed {seed}");
        }
        let _ = s.w.join(online);
        if s.w.pick(0..2) == 1 {
            s.w.crash(&home, Crash::Process);
            s.w.run_for(S(2));
            s.w.restart(&home);
        }

        let back = s.w.spawn_on("w2", {
            let ben = ben.clone();
            move |n| world::settle(n, ben, S(60))
        });
        assert!(s.w.join(back).expect("w2 is up"), "seed {seed}: Ben did not come back");
        let want = "# Plan\n\nIntro.\n Better idea.";
        assert_eq!(s.text(&doc), want, "seed {seed}: Ben's sentence stays, the draft does not come back");
        assert_eq!(ben.lock().unwrap().text(), want, "seed {seed}");
        assert_eq!(ben.lock().unwrap().saved, BTreeSet::from([0]), "seed {seed}: Ben's sentence saved");

        let after = versions(&mut s);
        let ns = |list: &[mdpad::core::versions::Version]| list.iter().map(|v| v.n).collect::<BTreeSet<_>>();
        assert!(ns(&after).is_superset(&ns(&before)) && after.len() == before.len() + 1, "seed {seed}: {after:?}");
        let kinds: Vec<Kind> = after.iter().map(|v| v.kind).collect();
        assert_eq!(kinds, [Kind::BeforeImport, Kind::Named, Kind::BeforeImport, Kind::BeforeRestore], "seed {seed}");
        let restore_from = after.iter().find(|v| v.n == named.n).expect("the named version stays");
        assert_eq!((restore_from.name.as_str(), restore_from.bytes), ("10:40", restored.len() as u64));
    })
    .unwrap();
}
