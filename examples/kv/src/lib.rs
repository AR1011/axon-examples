//! kv: a sharded key-value store on axon that speaks RESP2. Keys hash to the 16384 Redis Cluster slots, the slots
//! form 256 shards, and each shard is one keyed actor of the eager kind `shard` under `PaxosLeases`. See the README.

pub mod check;
pub mod client;
pub mod console;
pub mod front;
pub mod log;
pub mod node;
pub mod repl;
pub mod resp;
pub mod shard;
pub mod slot;
pub mod wire;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axon::AtomicHistogram;
use std::time::Duration;

/// The bounds a node keeps.
#[derive(Clone, Debug)]
pub struct Limits {
    pub key: usize,
    pub value: usize,
    /// One command, and so one argument, as it arrives.
    pub command: usize,
    /// One answer, as it leaves: `MGET` of several large values.
    pub reply: usize,
    /// The largest call or answer body sent in one frame; larger ones go in parts. `max_frame` less room for the
    /// call's own fields.
    pub frame_body: usize,
    /// Bytes of unparsed input and of large answers in flight, for every connection of the node together. Each
    /// connection also has `BASE_INPUT` outside it.
    pub input_budget: usize,
    /// How long a connection waits for the budget before it is told so and closed.
    pub input_wait: Duration,
    pub connections: usize,
}

/// Input a connection holds without asking the node's budget.
pub const BASE_INPUT: usize = 64 << 10;

impl Default for Limits {
    fn default() -> Self {
        Self {
            key: 1 << 10,
            value: 32 << 20,
            command: 33 << 20,
            reply: 33 << 20,
            frame_body: (1 << 20) - (4 << 10),
            input_budget: 1 << 30,
            input_wait: Duration::from_secs(30),
            connections: 1024,
        }
    }
}

/// A command kv serves, in Redis 5's six fields of `COMMAND`: name, arity, flags, first key, last key, step.
pub type Command = (&'static str, i64, &'static [&'static str], i64, i64, i64);

/// The commands kv serves: `COMMAND` gives them as Redis 7.4 does, and `INFO commandstats` counts them in this order.
pub const COMMANDS: [Command; 21] = [
    ("get", 2, &["readonly", "fast"], 1, 1, 1),
    ("set", -3, &["write", "denyoom"], 1, 1, 1),
    ("del", -2, &["write"], 1, -1, 1),
    ("exists", -2, &["readonly", "fast"], 1, -1, 1),
    ("mget", -2, &["readonly", "fast"], 1, -1, 1),
    ("mset", -3, &["write", "denyoom"], 1, -1, 2),
    ("dbsize", 1, &["readonly", "fast"], 0, 0, 0),
    ("flushdb", -1, &["write"], 0, 0, 0),
    ("flushall", -1, &["write"], 0, 0, 0),
    ("ping", -1, &["fast"], 0, 0, 0),
    ("echo", 2, &["fast"], 0, 0, 0),
    ("quit", -1, &["fast"], 0, 0, 0),
    ("select", 2, &["fast"], 0, 0, 0),
    ("auth", -2, &["fast"], 0, 0, 0),
    ("client", -2, &[], 0, 0, 0),
    ("command", -1, &[], 0, 0, 0),
    ("config", -2, &[], 0, 0, 0),
    ("info", -1, &[], 0, 0, 0),
    ("cluster", -2, &[], 0, 0, 0),
    ("hello", -1, &["fast"], 0, 0, 0),
    ("readonly", 1, &["fast"], 0, 0, 0),
];

/// Where a command's name is in [`COMMANDS`], in any case.
pub fn command_index(name: &[u8]) -> Option<usize> {
    COMMANDS.iter().position(|c| name.eq_ignore_ascii_case(c.0.as_bytes()))
}

/// A node's counters, for `INFO`.
#[derive(Default)]
pub struct Stats {
    pub calls: [AtomicU64; COMMANDS.len()],
    pub usec: [AtomicU64; COMMANDS.len()],
    pub failed: [AtomicU64; COMMANDS.len()],
    /// Shards started on this node. In memory mode each starts empty.
    pub shard_starts: AtomicU64,
    pub connections: AtomicU64,
    pub connections_total: AtomicU64,
    pub connections_refused: AtomicU64,
    /// Values that went to a shard in an upload, and answers that came back in parts.
    pub uploads: AtomicU64,
    pub parts_replies: AtomicU64,
    /// The Log's, in durable mode.
    pub wal: WalStats,
    /// Durable mode: messages to another node's Log sent as uploads, larger than a frame.
    pub log_uploads: AtomicU64,
    /// Durable mode: each shard start's time until its owner served, its recovery.
    pub recovery: AtomicHistogram,
    pub recovery_max_ms: AtomicU64,
    /// Durable mode: an owner's Append to another node's Log until its answer.
    pub append_rtt: AtomicHistogram,
    /// Durable mode: bytes of the committed entries this node's shard owners keep for a Log that is behind.
    pub owner_kept_bytes: AtomicU64,
    /// Durable mode: the `Install`s those owners sent to a Log behind them.
    pub owner_installs: AtomicU64,
}

/// What the Log last saw of its WAL, for `INFO kv`.
#[derive(Default)]
pub struct WalStats {
    pub writes: AtomicU64,
    pub failed_writes: AtomicU64,
    /// Entries of the shards' writes made durable: per write, the entries one sync covered.
    pub entries: AtomicU64,
    pub bytes: AtomicU64,
    pub segments: AtomicU64,
    pub snaps: AtomicU64,
    /// The bytes of the snapshot files there are now: with the floor, what the WAL may grow to before snapshots.
    pub snap_bytes: AtomicU64,
}

impl Stats {
    pub fn count(&self, command: usize, usec: u64, failed: bool) {
        self.calls[command].fetch_add(1, Relaxed);
        self.usec[command].fetch_add(usec, Relaxed);
        if failed {
            self.failed[command].fetch_add(1, Relaxed);
        }
    }

    /// `INFO commandstats`, in Redis's format.
    pub fn commandstats(&self) -> String {
        let mut out = String::from("# Commandstats\r\n");
        for (i, (name, ..)) in COMMANDS.iter().enumerate() {
            let calls = self.calls[i].load(Relaxed);
            if calls == 0 {
                continue;
            }
            let usec = self.usec[i].load(Relaxed);
            out.push_str(&format!(
                "cmdstat_{name}:calls={calls},usec={usec},usec_per_call={:.2},rejected_calls=0,failed_calls={}\r\n",
                usec as f64 / calls as f64,
                self.failed[i].load(Relaxed)
            ));
        }
        out
    }
}
