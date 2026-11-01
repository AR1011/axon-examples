use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The demo with a short lease. Each step it prints is also checked, and it returns an error when one does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn the_demo_holds_at_every_step() {
    paxos::demo(std::time::Duration::from_secs(1)).await.unwrap();
}

struct Kill(Child);

impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The binary as the README runs it: an app finds the acceptor through its seed server and places a key there.
#[test]
fn an_acceptor_and_an_app_as_processes_place_a_key() {
    let free = || std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let seed = free();
    let run = |role: &str, id: &str, stdout: Stdio| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_paxos"));
        command.args([role, "--plaintext"]).envs([
            ("NODE_ID", id),
            ("ACCEPTORS", "lease-0"),
            ("LEASE_SECS", "1"),
            ("KEYS", "1"),
        ]);
        command.env("PORT", free().to_string()).env("SEED_PORT", seed.to_string());
        Kill(command.env("SEEDS", format!("127.0.0.1:{seed}")).stdout(stdout).spawn().unwrap())
    };
    let _acceptor = run("acceptor", "lease-0", Stdio::null());
    let mut app = run("app", "app-a", Stdio::piped());

    let (tx, rx) = mpsc::sync_channel(16);
    let stdout = app.0.stdout.take().unwrap();
    std::thread::spawn(move || BufReader::new(stdout).lines().map_while(Result::ok).try_for_each(|l| tx.send(l)));
    let (deadline, mut seen) = (Instant::now() + Duration::from_secs(20), Vec::new());
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = rx.recv_timeout(left).unwrap_or_else(|_| panic!("k0 not placed in 20 s: {seen:?}"));
        if line.starts_with("k0 on app-a at epoch") {
            break;
        }
        seen.push(line);
    }
}
