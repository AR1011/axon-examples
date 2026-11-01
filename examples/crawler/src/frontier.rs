//! One domain's frontier with no I/O: which URLs it has seen, which wait, and when the next fetch may start.
//! The actor feeds it `now`, so the simulation drives the same code on virtual time.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

/// A fetch that failed transiently is tried this many times in all.
pub const MAX_ATTEMPTS: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub url: String,
    pub attempts: u8,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    Fetch(Pending),
    /// Work waits, but not before this time.
    At(Duration),
    /// Nothing to start until a URL arrives or a fetch returns.
    Idle,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub seen: u64,
    pub queued: u64,
    pub in_flight: u64,
    pub finished: u64,
    /// URLs refused because the domain already holds `max_urls`.
    pub over_cap: u64,
}

pub struct Frontier {
    seen: HashSet<String>,
    queue: VecDeque<Pending>,
    in_flight: usize,
    next_start: Duration,
    interval: Duration,
    max_in_flight: usize,
    max_urls: usize,
    finished: u64,
    over_cap: u64,
}

impl Frontier {
    /// At most one fetch starts per `interval`, and at most `max_in_flight` run at once.
    pub fn new(interval: Duration, max_in_flight: usize, max_urls: usize) -> Self {
        Self {
            seen: HashSet::new(),
            queue: VecDeque::new(),
            in_flight: 0,
            next_start: Duration::ZERO,
            interval,
            max_in_flight: max_in_flight.max(1),
            max_urls,
            finished: 0,
            over_cap: 0,
        }
    }

    /// State read back from the log: every URL discovered, and those finished. The first fetch waits one interval,
    /// since a previous owner may have just made one.
    pub fn restore(&mut self, discovered: Vec<String>, finished: &HashSet<String>, now: Duration) {
        for url in discovered {
            if self.seen.len() < self.max_urls && self.seen.insert(url.clone()) {
                if finished.contains(&url) {
                    self.finished += 1;
                } else {
                    self.queue.push_back(Pending { url, attempts: 0 });
                }
            }
        }
        self.next_start = now + self.interval;
    }

    /// Marks the URLs seen and returns the ones that are new, to be logged before they are queued.
    pub fn discover(&mut self, urls: impl IntoIterator<Item = String>) -> Vec<String> {
        let mut new = Vec::new();
        for url in urls {
            if self.seen.contains(&url) {
                continue;
            }
            if self.seen.len() >= self.max_urls {
                self.over_cap += 1;
                continue;
            }
            self.seen.insert(url.clone());
            new.push(url);
        }
        new
    }

    /// The URLs `discover` returned are in the log now.
    pub fn logged(&mut self, urls: impl IntoIterator<Item = String>) {
        self.queue.extend(urls.into_iter().map(|url| Pending { url, attempts: 0 }));
    }

    pub fn next(&mut self, now: Duration) -> Next {
        if self.queue.is_empty() || self.in_flight >= self.max_in_flight {
            return Next::Idle;
        }
        if now < self.next_start {
            return Next::At(self.next_start);
        }
        let pending = self.queue.pop_front().expect("queue checked non-empty");
        self.in_flight += 1;
        self.next_start = now + self.interval;
        Next::Fetch(pending)
    }

    /// When `next` would start a fetch, if one waits and a slot is free.
    pub fn wake_at(&self) -> Option<Duration> {
        (!self.queue.is_empty() && self.in_flight < self.max_in_flight).then_some(self.next_start)
    }

    /// A fetch could not start after all; it goes back to the front.
    pub fn unstarted(&mut self, pending: Pending) {
        self.in_flight -= 1;
        self.queue.push_front(pending);
    }

    /// A fetch ended. A transient failure goes back in the queue until `MAX_ATTEMPTS`; true means the URL is
    /// finished and its end must be logged.
    pub fn returned(&mut self, mut pending: Pending, transient: bool) -> bool {
        self.in_flight -= 1;
        pending.attempts += 1;
        if transient && pending.attempts < MAX_ATTEMPTS {
            self.queue.push_back(pending);
            return false;
        }
        true
    }

    /// A finished URL is in the log.
    pub fn finished(&mut self) {
        self.finished += 1;
    }

    pub fn counts(&self) -> Counts {
        Counts {
            seen: self.seen.len() as u64,
            queued: self.queue.len() as u64,
            in_flight: self.in_flight as u64,
            finished: self.finished,
            over_cap: self.over_cap,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn fetches_are_spaced_by_the_interval_and_bounded_in_flight() {
        let mut f = Frontier::new(100 * MS, 2, 10);
        let new = f.discover(["a".into(), "b".into(), "c".into(), "a".into()]);
        assert_eq!(new, ["a", "b", "c"]);
        assert_eq!(f.next(Duration::ZERO), Next::Idle, "nothing is queued before it is logged");
        f.logged(new);
        let Next::Fetch(a) = f.next(Duration::ZERO) else { panic!() };
        assert_eq!(f.next(50 * MS), Next::At(100 * MS));
        let Next::Fetch(b) = f.next(100 * MS) else { panic!() };
        assert_eq!(f.next(500 * MS), Next::Idle, "two in flight");
        assert!(!f.returned(a, true), "a transient failure is queued again");
        assert!(f.returned(b, false));
        assert!(matches!(f.next(500 * MS), Next::Fetch(p) if p.url == "c"));
        assert!(matches!(f.next(600 * MS), Next::Fetch(p) if p.url == "a" && p.attempts == 1));
    }

    #[test]
    fn a_transient_failure_gives_up_after_max_attempts() {
        let mut f = Frontier::new(Duration::ZERO, 1, 10);
        let new = f.discover(["a".into()]);
        f.logged(new);
        for attempt in 1..=MAX_ATTEMPTS {
            let Next::Fetch(p) = f.next(Duration::ZERO) else { panic!() };
            assert_eq!(f.returned(p, true), attempt == MAX_ATTEMPTS);
        }
        assert_eq!(f.next(Duration::ZERO), Next::Idle);
    }

    #[test]
    fn restore_queues_what_was_not_finished_and_waits_an_interval() {
        let mut f = Frontier::new(100 * MS, 1, 2);
        let done: HashSet<String> = ["a".to_string()].into();
        f.restore(vec!["a".into(), "b".into(), "c".into()], &done, 5 * MS);
        assert_eq!(f.counts(), Counts { seen: 2, queued: 1, in_flight: 0, finished: 1, over_cap: 0 });
        assert_eq!(f.next(5 * MS), Next::At(105 * MS));
        assert!(f.discover(["d".into()]).is_empty(), "the cap holds");
        assert_eq!(f.counts().over_cap, 1);
    }
}
