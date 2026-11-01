//! The README's first journey, as written: `workflows dev`, then `wf login`, `wf start`, `wf signal` and
//! `wf get --wait` against it, as a user types them.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::Value;

/// The dev node, stopped with SIGTERM when the test ends.
struct Dev(Child);

impl Drop for Dev {
    fn drop(&mut self) {
        let term = Command::new("kill").args(["-TERM", &self.0.id().to_string()]).status();
        if term.is_err() {
            drop(self.0.kill());
        }
        drop(self.0.wait());
    }
}

fn wf(config: &Path, args: &[&str]) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_wf"))
        .args(args)
        .env("WF_CONFIG", config)
        .env_remove("WF_URL")
        .env_remove("WF_TOKEN")
        .env_remove("WF_PASSWORD")
        .output()
        .expect("wf runs");
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "wf {args:?} failed:\n{stdout}\n{stderr}");
    out
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// `workflows dev` in `dir`, once it prints its line: the node, its URL and its password.
fn dev(dir: &Path) -> (Dev, String, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_workflows"))
        .arg("dev")
        .arg(format!("dir={}", dir.join("wf").display()))
        .stdout(Stdio::piped())
        .spawn()
        .expect("the dev node starts");
    let out = child.stdout.take().expect("its stdout");
    let dev = Dev(child);
    // The node prints its line once its lone acceptor grants leases: a little over a lease after it starts.
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        drop(BufReader::new(out).read_line(&mut line));
        drop(tx.send(line));
    });
    let line = rx.recv_timeout(Duration::from_secs(60)).expect("the dev node prints its URL");
    let said: Value = serde_json::from_str(&line).expect("one JSON line");
    let text = |key: &str| said[key].as_str().unwrap_or_else(|| panic!("no {key} in {said}")).to_owned();
    (dev, text("url"), text("password"))
}

#[test]
fn journey_one() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let config = dir.path().join("credentials.json");
    let (dev, url, password) = dev(dir.path());
    let (url, password) = (url.as_str(), password.as_str());

    wf(&config, &["login", "--url", url, "--password", password]);
    let input = r#"{"sku":"tea","qty":2,"account":"a1","parcels":[{"to":"Oslo"}],"ship_after_s":10}"#;
    let id = stdout(&wf(&config, &["start", "checkout", "--input", input])).trim().to_owned();
    assert!(id.starts_with("run-"), "{id}");
    wf(&config, &["signal", &id, "approve", "--signal-id", "ok-1", "{}"]);
    let got = stdout(&wf(&config, &["get", &id, "--wait", "--timeout", "60"]));

    let lines: Vec<&str> = got.lines().collect();
    assert_eq!(
        lines[..lines.len() - 1],
        [
            "step 1 activity reserve: done",
            "step 2 activity charge: done",
            "step 3 signal approve: done",
            "step 4 sleep: done",
            "step 5 activity ship: done",
        ],
        "{got}"
    );
    let receipt = lines.last().and_then(|l| l.strip_prefix("completed: ")).expect("the receipt");
    let receipt: Value = serde_json::from_str(receipt).expect("JSON");
    assert_eq!(receipt["paid"]["cents"], 500, "{receipt}");
    assert_eq!(receipt["labels"][0]["to"], "Oslo", "{receipt}");
    assert_eq!(receipt["labels"][0]["tracking"], format!("trk-{id}/5"), "{receipt}");
    drop(dev);
}

/// The bulk actions: each run the filter lists, done, skipped where it is not in that state.
#[test]
fn bulk_actions_take_a_filter() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let config = dir.path().join("credentials.json");
    let (dev, url, password) = dev(dir.path());
    wf(&config, &["login", "--url", &url, "--password", &password]);
    let declined = r#"{"sku":"tea","qty":1,"account":"declined","parcels":[]}"#;
    for n in 0..3 {
        let id = format!("bulk-{n}");
        wf(&config, &["start", "checkout", "--id", &id, "--input", declined]);
        let out = Command::new(env!("CARGO_BIN_EXE_wf"))
            .args(["get", &id, "--wait", "--timeout", "60"])
            .env("WF_CONFIG", &config)
            .output()
            .expect("wf runs");
        assert!(String::from_utf8_lossy(&out.stderr).contains("failed"), "{out:?}");
    }
    let listed = stdout(&wf(&config, &["list", "--filter", "workflow=checkout,status=failed"]));
    assert_eq!(listed.lines().count(), 3, "{listed}");

    let filter = "workflow=checkout,status=failed,from=2020-01-01T00:00";
    let resumed = stdout(&wf(&config, &["resume", "--filter", filter]));
    assert_eq!(resumed.trim(), "resume: 0 done, 3 skipped, 0 refused");
    let retried = stdout(&wf(&config, &["retry", "--filter", filter]));
    assert_eq!(retried.trim(), "retry: 3 done, 0 skipped, 0 refused");
    let history = stdout(&wf(&config, &["history", "bulk-0"]));
    assert!(history.contains("Redriven"), "{history}");

    wf(&config, &["logout"]);
    let out =
        Command::new(env!("CARGO_BIN_EXE_wf")).args(["list"]).env("WF_CONFIG", &config).output().expect("wf runs");
    assert!(!out.status.success(), "signed out, a list is refused: {out:?}");
    drop(dev);
}
