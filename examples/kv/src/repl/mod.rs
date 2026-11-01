//! Durable mode's replication: each shard's entries on a majority of the storage set's logs before a write is
//! answered. The core has no I/O, no clock and no randomness, so `tests/repl_sim.rs` drives it through dropped,
//! duplicated and reordered messages, crashes, zombie owners and ballots that go back.

pub mod codec;
mod core;

pub use self::core::{
    Ballot, Config, Entry, FromLog, Map, Op, Opened, Out, Owner, Pos, Rec, Replica, Snapshot, State, Status, Stop,
    ToLog, Weak,
};

#[cfg(test)]
mod tests;
