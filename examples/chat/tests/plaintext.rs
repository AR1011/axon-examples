//! chat has no TLS between nodes, so a node starts only when `--plaintext` says its cluster traffic may go
//! unencrypted.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn a_node_without_plaintext_does_not_start() {
    for mode in ["seed", "node"] {
        let mut node = Command::new(env!("CARGO_BIN_EXE_chat"))
            .arg(mode)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("chat starts");
        let until = Instant::now() + Duration::from_secs(10);
        while node.try_wait().expect("its state").is_none() {
            if Instant::now() > until {
                node.kill().expect("kill it");
                panic!("{mode} without --plaintext was still running after 10 s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let out = node.wait_with_output().expect("its output");
        assert!(!out.status.success(), "{mode}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("add --plaintext"), "{mode}: {err}");
    }
}
