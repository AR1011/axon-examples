//! kv has no TLS between nodes, so a node starts only when `--plaintext` says its cluster traffic may go unencrypted.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn a_node_without_plaintext_does_not_start() {
    let dir = std::env::temp_dir().join(format!("kv-plaintext-{}", std::process::id()));
    let mut node = Command::new(env!("CARGO_BIN_EXE_kv"))
        .args(["node", "--resp", "127.0.0.1:0", "--bind", "127.0.0.1:0", "--seed-bind", "127.0.0.1:0", "--dir"])
        .arg(&dir)
        .env("KV_SECRET", "0123456789abcdef0123456789abcdef")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("kv starts");
    let until = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = node.try_wait().expect("kv's state") {
            break status;
        }
        if Instant::now() > until {
            node.kill().expect("kill kv");
            let _ = std::fs::remove_dir_all(&dir);
            panic!("a node without --plaintext was still running after 10 s");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let out = node.wait_with_output().expect("kv's output");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("add --plaintext"), "{err}");
}
