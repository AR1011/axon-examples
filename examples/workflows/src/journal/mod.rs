//! The journal: what a run did, step by step, on a majority of the history replicas before the run acts on it.
//!
//! - [`entry`]: the journal's entries and their bytes.
//! - [`core`]: a replica over [`axon::files::Log`] and the writer that seals, recovers and appends with a quorum, with
//!   no actor, clock or network, so the simulation and the actors drive the same code.
//! - [`index`]: each run's summary on a replica, the indexes runs are listed by, and the due timers.
//! - [`service`]: the `Journal` and `Waker` actors on a history node.
//! - [`client`]: an owner's side over the network, and a stub owner that the World stories and the bench drive.
//! - [`local`]: one replica in this process, for the embedded tests.

pub mod client;
pub mod core;
pub mod entry;
pub mod index;
pub mod local;
pub mod service;
pub mod wire;

pub use self::core::{
    Answer, AppendError, Handled, Mutation, Out, Replica, Request, Writer, WriterConfig, journal_epoch, replica_config,
};
pub use client::{JournalClient, Probe, Transport};
pub use entry::{Entry, Own, Policy, ValueKind};
pub use index::{Index, Indexed, Query, Retention, Status, Summary};
pub use local::LocalJournal;
