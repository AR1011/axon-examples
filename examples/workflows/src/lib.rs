//! A durable workflow engine on axon: workflows are Rust `async fn`s whose steps go into a journal replicated on
//! three history nodes before the workflow sees them. The design is in the README.
//!
//! - [`workflow`]: what a workflow's author writes against.
//! - [`run`]: a run's actor and its pure core, the replay.
//! - [`journal`]: the replicated journal; [`check`]: the checker every test ends with.
//! - [`queue`], [`worker`]: task queues, and the workers that pull activity attempts from them.
//! - [`node`], [`api`], [`auth`], [`client`]: a node's parts, the HTTP API and its Rust client; [`metrics`] and
//!   [`observer`]: the engine's series and the console's numbers.
//! - [`samples`]: the workflows the binaries run.

pub mod api;
pub mod auth;
pub mod check;
pub mod client;
pub mod journal;
pub mod metrics;
pub mod node;
pub mod observer;
pub mod queue;
pub mod run;
pub mod samples;
pub mod worker;
pub mod workflow;

/// The weakened rule of a run or a node that `scripts/mutations.sh` runs (`RUN_MUTATION`), or empty: read once.
#[doc(hidden)]
pub fn mutation() -> &'static str {
    static MUTATION: std::sync::LazyLock<String> =
        std::sync::LazyLock::new(|| std::env::var("RUN_MUTATION").unwrap_or_default());
    &MUTATION
}

/// A `MemoryFiles` call's result: ready at once.
#[cfg(test)]
fn ready<T>(mut future: axon::files::FileFuture<T>) -> std::io::Result<T> {
    use std::task::{Context, Poll, Waker};
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("files in memory waited"),
    }
}
