use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ledger::demo::HOSTS;
use ledger::store::{self, Event, StoreError};

struct Kill(Child);

impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Lines from a child's stdout, tagged with who printed them.
fn lines(child: &mut Kill, who: &'static str, tx: mpsc::SyncSender<(&'static str, String)>) {
    let stdout = child.0.stdout.take().expect("stdout is piped");
    std::thread::spawn(move || {
        BufReader::new(stdout).lines().map_while(Result::ok).try_for_each(|l| tx.send((who, l)))
    });
}

/// Whether every thread of `pid` is stopped, or has exited.
fn stopped(pid: u32) -> bool {
    let threads = std::fs::read_dir(format!("/proc/{pid}/task")).unwrap();
    // The state is the first field after the thread's name.
    let halted = |stat: String| stat.rsplit_once(')').is_some_and(|(_, s)| s.trim_start().starts_with(['T', 'Z', 'X']));
    threads.filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("stat")).ok()).all(halted)
}

/// The binary as the README runs it, three hosts and a load. The test takes the audit's lock, and one host is paused
/// (SIGSTOP) once it has writes waiting for it, then woken once the other two run every account: each of those writes
/// is refused, and the audit, held back until they are, passes over their bytes.
#[test]
#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), ignore = "reads the threads' states from /proc")]
fn a_paused_host_is_fenced_when_it_wakes() {
    // Seeds need fixed ports. Below the ephemeral range, so that the other test's `127.0.0.1:0` cannot take one
    // between this check and the host's bind.
    let base = 20_000 + (std::process::id() % 1000) as u16 * 10;
    let free = (base..base + 10).filter(|p| std::net::TcpListener::bind(("127.0.0.1", *p)).is_ok());
    let seeds: Vec<u16> = free.take(3).collect();
    assert_eq!(seeds.len(), 3, "three free ports from {base}");
    let dir = std::env::temp_dir().join(format!("ledger-procs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let seed_list = seeds.iter().map(|p| format!("127.0.0.1:{p}")).collect::<Vec<_>>().join(",");
    let run = |role: &str, envs: &[(&str, String)]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ledger"));
        command
            .args([role, "--plaintext"])
            .env("SEEDS", &seed_list)
            .env("DIR", &dir)
            .env("LEASE_SECS", "1")
            .envs(envs.iter().cloned());
        Kill(command.stdout(Stdio::piped()).spawn().unwrap())
    };
    let (tx, rx) = mpsc::sync_channel(1024);
    let mut hosts = Vec::new();
    for (i, id) in HOSTS.into_iter().enumerate() {
        let mut host =
            run("host", &[("NODE_ID", id.into()), ("PORT", "0".into()), ("SEED_PORT", seeds[i].to_string())]);
        lines(&mut host, id, tx.clone());
        hosts.push(host);
    }
    // Few, so the test's length does not follow the runner's speed: the lock is taken as the load starts, and the
    // writes ledger-b is paused with hold the load until its accounts move.
    let mut load = run("load", &[("ACCOUNTS", "100".into()), ("COMMANDS", "2000".into())]);
    lines(&mut load, "load", tx);

    let signal =
        |sig: &str| assert!(Command::new("kill").args([sig, &hosts[1].0.id().to_string()]).status().unwrap().success());
    // "<id>: <n> accounts, <m> fenced, <k> waiting for an audit", printed by a host whenever one changes.
    let counts = |line: &str| {
        let mut words = line.split_whitespace().skip(1);
        let mut number = |skip| words.nth(skip)?.parse::<u64>().ok();
        Some((number(0)?, number(1)?, number(1)?))
    };
    let deadline = Instant::now() + Duration::from_secs(90);
    let (mut audit, mut paused, mut moved, mut hosted) = (None, None, None, [0; 3]);
    let (mut held, mut fenced, mut passed) = (0, 0, false);
    // Every write ledger-b was paused with is refused.
    while !passed || fenced < held {
        let left = deadline.saturating_duration_since(Instant::now());
        let (who, line) = rx.recv_timeout(left).unwrap_or_else(|_| {
            panic!("in 90 s: audit passed {passed}, woken {moved:?} after the pause, {fenced} of {held} writes refused")
        });
        println!("{who}: {line}");
        if who == "load" && line.ends_with("accounts open") {
            let cut = File::options().create(true).truncate(false).write(true).open(dir.join("cut.lock")).unwrap();
            cut.lock().unwrap(); // as the audit's `store::cut` does: every write waits, before it writes
            audit = Some(cut);
        }
        if let Some((n, m, waiting)) = counts(&line) {
            let i = HOSTS.iter().position(|h| *h == who).unwrap();
            hosted[i] = n;
            // Only this test's lock makes a write wait, and it is held until every thread has stopped (`kill`
            // returns before they have): those writes have not looked for a newer owner yet, and will find one.
            if i == 1 && waiting > 0 && paused.is_none() {
                signal("-STOP");
                while !stopped(hosts[1].0.id()) {
                    std::thread::yield_now();
                }
                // Shared, as a write takes it (flock converts it): the writes go, and the load's audit waits.
                audit.as_ref().expect("taken as the load started").lock_shared().unwrap();
                (paused, held) = (Some(Instant::now()), waiting);
            }
            fenced = if i == 1 { m } else { fenced };
            if paused.is_some() && fenced >= held {
                audit = None; // the audit comes after every write ledger-b was paused with
            }
        }
        if let (Some(at), None) = (paused, moved)
            && hosted[0] + hosted[2] > 100
        {
            // Every account and the ghost recovered on the two that run, each in a segment newer than ledger-b's.
            moved = Some(at.elapsed());
            println!("moved {:.1?} after the pause", at.elapsed());
            signal("-CONT");
        }
        if who == "load" && line.starts_with("audit passed") {
            assert!(!line.contains(", 0 bytes fenced"), "the audit did not see ledger-b's refused writes");
            passed = true;
        }
    }
    assert!(load.0.wait().unwrap().success());
    let moved = moved.expect("the accounts moved while ledger-b was paused");
    assert!(moved > Duration::from_secs(1), "not before the lease ran out: {moved:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The whole story with a 1 s lease. Every step it prints is checked, and it returns an error when one does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn the_demo_holds_at_every_step() {
    let dir = std::env::temp_dir().join(format!("ledger-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let report = ledger::demo(&dir, 200, 3000, Duration::from_secs(1)).await.unwrap();
    assert!(report.moved > 0 && report.audit.owners > report.accounts, "{report:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// An owner that is still alive after its account moved is turned away at its next write, which nobody reads; one
/// with an older epoch cannot take the account back; and a writer stuck in the middle of an append (here: holding
/// the shared lock) does not keep the next owner out.
#[test]
fn a_second_owner_fences_the_first() {
    let dir = std::env::temp_dir().join(format!("ledger-fence-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut old, _) = store::recover(&dir, "a", 10, 2).unwrap();
    old.append(&[Event::Open("k".into()), Event::Deposit("d1".into(), 7)], None).unwrap();
    let stuck = std::fs::File::open(dir.join("cut.lock")).unwrap();
    stuck.lock_shared().unwrap();
    let (mut new, state) = store::recover(&dir, "a", 20, 2).unwrap();
    assert_eq!((state.open, state.balance), (true, 7));
    assert!(matches!(old.append(&[Event::Deposit("d2".into(), 5)], None), Err(StoreError::Fenced)));
    for stale in [10, 5] {
        assert!(matches!(store::recover(&dir, "a", stale, 2), Err(StoreError::Fenced)));
    }
    drop(stuck);
    let mut state = state;
    for (i, event) in [Event::Deposit("d2".into(), 5), Event::Withdraw("w".into(), 3)].into_iter().enumerate() {
        state.apply(&event);
        new.append(&[event], (i == 1).then_some(&state)).unwrap(); // the second write is due a snapshot
    }
    drop(new);
    let (_, recovered) = store::recover(&dir, "a", 30, 2).unwrap();
    assert_eq!(recovered, state);
    let cut = store::cut(&dir).unwrap();
    let epochs: Vec<u64> = cut["a"].segments.iter().map(|s| s.epoch).collect();
    assert_eq!(epochs, [5, 10, 20, 30]); // 5 is the stale owner that gave up before writing
    let audit = ledger::audit::audit(&cut).unwrap();
    assert!(audit.fenced_bytes > 0, "{audit:?}");
    assert_eq!((audit.balances, audit.deposits, audit.withdrawals, audit.owners), (9, 12, 3, 3));
    std::fs::remove_dir_all(&dir).unwrap();
}
