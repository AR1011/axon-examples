//! An exchange matching engine on axon: one keyed order-book actor per symbol under a lease, price-time priority,
//! a journal of accepted commands per owner epoch, market data on a stream per symbol.

pub mod book;
pub mod client;
pub mod engine;
pub mod journal;
pub mod md;
pub mod wire;

use std::collections::VecDeque;

use book::{Book, Command, Outcome};

/// Replays a journal into a fresh book, as a new owner does. Returns the book and the commands it accepted.
pub fn replay(commands: &[Command], max_resting: usize, max_seen: usize) -> (Book, u64) {
    let mut book = Book::new(max_resting, max_seen);
    let mut levels = Vec::new();
    // What each command changed is not wanted here: cleared each time, so a long journal holds none of it.
    let accepted = commands
        .iter()
        .filter(|c| {
            levels.clear();
            !matches!(book.apply(**c, &mut levels), Outcome::Rejected(_))
        })
        .count();
    (book, accepted as u64)
}

/// p50, p99 and max of latencies in microseconds.
pub fn percentiles(samples: &mut [u32]) -> (u32, u32, u32) {
    if samples.is_empty() {
        return (0, 0, 0);
    }
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    (at(0.50), at(0.99), samples[samples.len() - 1])
}

/// A bounded sample of latencies: past `cap` it keeps every other one and halves its rate, so a long run stays fair.
pub struct Samples {
    kept: VecDeque<u32>,
    cap: usize,
    every: u64,
    seen: u64,
}

impl Samples {
    pub fn new(cap: usize) -> Self {
        Self { kept: VecDeque::new(), cap, every: 1, seen: 0 }
    }
    pub fn add(&mut self, micros: u32) {
        self.seen += 1;
        if !self.seen.is_multiple_of(self.every) {
            return;
        }
        if self.kept.len() == self.cap {
            let mut i = 0;
            self.kept.retain(|_| {
                i += 1;
                i % 2 == 0
            });
            self.every *= 2;
        }
        self.kept.push_back(micros);
    }
    pub fn into_vec(self) -> Vec<u32> {
        self.kept.into()
    }
}
