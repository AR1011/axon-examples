//! I10 for a client that does not read: its session pauses the subscription instead of queueing for it, the client
//! catches up once it reads again, and the session counters return to zero. Alone in its binary, as the counters are
//! the process's.

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use mdpad::client::Client;
use mdpad::crdt;
use mdpad::session::{OPEN, PAUSES};
use support::{CutFiles, settle, start, user};

const SETTLE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_does_not_read_is_paused_then_catches_up() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Stall\n").await.unwrap();
    let mut stalled = Client::new(node.http(), &id, api.credential());
    settle(&mut stalled, SETTLE).await;
    let mut writer = Client::new(node.http(), &id, api.credential());
    settle(&mut writer, SETTLE).await;
    assert_eq!(OPEN.load(Ordering::Relaxed), 2);

    // 64 KiB in and out, 400 times: far more than the socket buffers hold, all of it for a client that reads nothing.
    let chunk = "s".repeat(64 << 10);
    for mark in 0..400u64 {
        writer.insert(0, &chunk, 2 * mark).await.unwrap();
        settle(&mut writer, SETTLE).await;
        let delete = crdt::remove(&writer.core.doc, 0, chunk.len() as u32);
        let frame = writer.core.local(delete, 2 * mark + 1).expect("connected");
        writer.send(frame).await.unwrap();
        settle(&mut writer, SETTLE).await;
    }
    writer.insert(0, "done ", 1000).await.unwrap();
    settle(&mut writer, SETTLE).await;
    assert!(PAUSES.load(Ordering::Relaxed) > 0, "never paused");

    let text = api.markdown(&id).await.unwrap();
    assert_eq!(text, "done # Stall\n");
    let caught_up = stalled.run_until(SETTLE, |c| c.text() == text).await;
    assert_eq!(caught_up, Ok(()), "{} bytes", stalled.text().len());
    stalled.disconnect().await;
    writer.disconnect().await;
    axon_testkit::eventually("every session ends", async || OPEN.load(Ordering::Relaxed) == 0).await;
    node.shutdown().await;
}
