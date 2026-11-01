//! Order entry from any node: what a refusal means, and resending an order whose outcome was lost.

use std::time::{Duration, Instant};

use axon_cluster::{CallError, Cluster, ClusterError, KeyedRef, Placement, Rejection, Retry};
use bytes::Bytes;

use crate::book::Command;
use crate::wire::{Answer, BookApi, Call, KIND, Snapshot};

/// Why an order has no answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The book's mailbox is full: the order was refused, not queued. Nothing happened; send it later or drop it.
    Busy,
    /// The book is moving to another node, or starting there. The order was not delivered.
    Moving(String),
    /// Sent, but the answer was lost: it may have been matched. Resend it with the same id to find out.
    Unknown(String),
    Failed(String),
}

pub fn classify(e: &ClusterError) -> Refused {
    match e {
        ClusterError::Call(CallError::Rejected(Rejection::Full)) => Refused::Busy,
        ClusterError::Spawn(..) | ClusterError::NoCandidate => Refused::Moving(format!("{e:?}")),
        e => match e.retry() {
            // This node's own limits: too many calls in flight, or a full session outbox.
            Retry::Later => Refused::Busy,
            Retry::Resolve => Refused::Moving(format!("{e:?}")),
            Retry::IfIdempotent => Refused::Unknown(format!("{e:?}")),
            Retry::Never => Refused::Failed(format!("{e:?}")),
        },
    }
}

pub fn placement() -> Placement {
    Placement::default().require("role", "engine")
}

pub fn book(cluster: &Cluster, symbol: &str) -> KeyedRef<BookApi> {
    cluster.keyed::<BookApi>(KIND, symbol, Bytes::new(), &placement())
}

/// One try: the answer, or why there is none.
pub async fn submit(book: &KeyedRef<BookApi>, command: Command) -> Result<Answer, Refused> {
    book.request(Call::Submit(command)).await.map_err(|e| classify(&e))
}

/// Sends until the book answers or `within` passes, resending only what was not delivered or whose answer was lost.
/// Order ids make that safe: a resent order the book had matched comes back `Rejected(Duplicate)`.
/// `Busy` is returned at once: that refusal is the book saying it cannot keep up. Returns the tries it took.
pub async fn submit_known(
    book: &KeyedRef<BookApi>,
    command: Command,
    within: Duration,
) -> (Result<Answer, Refused>, u32) {
    let deadline = Instant::now() + within;
    let mut pause = Duration::from_millis(20);
    let mut tries = 0;
    loop {
        tries += 1;
        match submit(book, command).await {
            Err(Refused::Moving(_) | Refused::Unknown(_)) if Instant::now() + pause < deadline => {
                // The lease wait is seconds, so a capped backoff (`cluster.watch_key` would say when it has an owner).
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_millis(500));
            }
            done => return (done, tries),
        }
    }
}

/// Asks until the book answers or `within` passes, as a book that is starting or moving answers late.
pub async fn snapshot(book: &KeyedRef<BookApi>, depth: usize, within: Duration) -> Result<Snapshot, Refused> {
    let deadline = tokio::time::Instant::now() + within;
    match book.request_until(Call::Snapshot { depth }, deadline).await.map_err(|e| classify(&e))? {
        Answer::Snapshot(snapshot) => Ok(snapshot),
        other => Err(Refused::Failed(format!("unexpected answer {other:?}"))),
    }
}
