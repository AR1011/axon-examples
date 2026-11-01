use std::process::Command;

use device_twins::demo::{DemoConfig, demo};

/// Three twin nodes, 4 000 devices, an idle stop and a fourth node joining; every step checks what must hold.
/// The binary's 20 000 are more readings than a debug build on CI's four cores applies inside a round's deadline.
#[tokio::test(flavor = "multi_thread")]
async fn the_demo_holds_at_every_step() {
    demo(DemoConfig { devices: 4_000, ..DemoConfig::default() }).await.unwrap();
}

/// The binary as the README runs it, small: separate processes that find each other through the alerts node's
/// seed server, lease twins from its acceptor, and take a fourth node mid-run without losing a report or an alert.
#[test]
fn the_scale_run_as_processes_loses_nothing() {
    let run = Command::new(env!("CARGO_BIN_EXE_device-twins")).args(["scale", "2000", "12", "500"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(run.status.success(), "{stdout}\n{}", String::from_utf8_lossy(&run.stderr));
    assert!(stdout.contains("twin-4 serves"), "{stdout}");
}
