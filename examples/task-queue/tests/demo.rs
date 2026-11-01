use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use task_queue::Answer;
use task_queue::demo::{Params, demo};

/// The demo as the README shows it. Each step it prints is also checked, and it returns an error when one does not
/// hold.
#[tokio::test(flavor = "multi_thread")]
async fn the_demo_holds_at_every_step() {
    let report = demo(Params::default()).await.unwrap();
    assert!(report.redelivered > 0, "{report:?}");
}

struct Kill(Child);

impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The binary as the README runs it: a queue node and a worker as processes, a producer and a status query as
/// clients that come and go.
#[test]
fn a_queue_and_a_worker_as_processes_run_what_a_producer_enqueues() {
    let seed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port().to_string();
    let command = |role: &str| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_task-queue"));
        command.args([role, "--plaintext"]).env("SEED_PORT", &seed).env("SEEDS", format!("127.0.0.1:{seed}"));
        command.env("VISIBILITY_MS", "1000");
        command
    };
    let _queue = Kill(command("queue").stdout(Stdio::null()).spawn().unwrap());
    let _worker = Kill(command("worker").stdout(Stdio::null()).spawn().unwrap());

    let produced = command("produce").arg("200").output().unwrap();
    assert!(produced.status.success(), "{}", String::from_utf8_lossy(&produced.stderr));

    let (deadline, mut last) = (Instant::now() + Duration::from_secs(30), None);
    while Instant::now() < deadline {
        let out = command("status").output().unwrap();
        if let Ok(Answer::Status(status)) = serde_json::from_slice(&out.stdout) {
            let t = status.total();
            if t.done + t.dead == 200 {
                assert_eq!((t.done, t.dead), (200, 0), "{status:?}");
                assert_eq!(status.workers["worker"].done, 200);
                return;
            }
            last = Some(status);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("200 jobs not done in 30 s: {last:?}");
}
