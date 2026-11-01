//! The pure parts: schedules, what a scheduler owes at a wall time, and the run log that fences fires by epoch.
//! No I/O and no clock of their own, so the actors, the embedded test and the simulation all run this same code.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A job fires once per tick. Tick `t` of a job starts at wall time `t * every_ms + offset_ms` (ms since the Unix
/// epoch), so every node numbers ticks the same way without talking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub name: String,
    pub every_ms: u64,
    pub offset_ms: u64,
    /// The missed-tick policy. After an outage, at most this many runs catch up: the latest `max_runs - 1` missed
    /// ticks fire one by one, and every older one is folded into the first run (its `from`). 1 fires once for the
    /// whole outage; a large value fires every tick late.
    pub max_runs: u64,
}

/// Most runs one job may owe at once, and so the size of one batch per job.
pub const MAX_RUNS: u64 = 1024;

impl Job {
    pub fn check(&self) -> Result<(), String> {
        let named = !self.name.is_empty() && self.name.len() <= 64;
        if !named || self.every_ms == 0 || self.offset_ms >= self.every_ms || !(1..=MAX_RUNS).contains(&self.max_runs) {
            return Err(format!(
                "job {:?}: name 1..=64 bytes, every_ms > offset_ms, max_runs 1..={MAX_RUNS}",
                self.name
            ));
        }
        Ok(())
    }

    /// The tick whose slot holds wall time `ms`, `None` before the first.
    pub fn tick_at(&self, ms: u64) -> Option<u64> {
        ms.checked_sub(self.offset_ms).map(|d| d / self.every_ms)
    }

    pub fn starts_at(&self, tick: u64) -> u64 {
        tick.saturating_mul(self.every_ms).saturating_add(self.offset_ms)
    }
}

/// One fire of `job`, standing for ticks `from..=tick`: `from < tick` when the policy folded missed ticks into it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fire {
    pub job: String,
    pub from: u64,
    pub tick: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// The side effect happened, now, once, for ticks `from..=tick`. `from` is the fire's own unless some of its
    /// ticks were fired already: a resend after a lost answer may fold missed ticks differently from the original.
    Fired { from: u64 },
    /// This tick was fired already; nothing happened. A resend after a lost reply lands here.
    Duplicate,
    /// A newer owner has claimed the log; nothing happened.
    Fenced,
    /// `from` is past the next tick the log expects: the fire would skip ticks. Nothing happened.
    Gap,
}

/// What must never happen twice lives here: the downstream system that runs the jobs. It knows the newest epoch
/// that claimed it and the last tick fired per job, and refuses anything older.
#[derive(Debug, Default)]
pub struct RunLog {
    epoch: u64,
    last: BTreeMap<String, u64>,
}

impl RunLog {
    /// A new owner claims before it fires, so a fire from any older owner is refused from then on. Returns the last
    /// tick fired per job, which is where the new owner picks up.
    pub fn claim(&mut self, epoch: u64) -> Result<BTreeMap<String, u64>, u64> {
        if epoch < self.epoch {
            return Err(self.epoch);
        }
        self.epoch = epoch;
        Ok(self.last.clone())
    }

    pub fn fire(&mut self, epoch: u64, fire: &Fire) -> Outcome {
        if epoch < self.epoch {
            return Outcome::Fenced;
        }
        self.epoch = epoch;
        let last = self.last.get(&fire.job).copied();
        if last.is_some_and(|last| fire.tick <= last) {
            return Outcome::Duplicate;
        }
        if last.is_some_and(|last| fire.from > last + 1) || fire.from > fire.tick {
            return Outcome::Gap;
        }
        self.last.insert(fire.job.clone(), fire.tick);
        Outcome::Fired { from: last.map_or(fire.from, |last| fire.from.max(last + 1)) }
    }
}

/// A scheduler's view: its jobs and the first tick of each not yet confirmed by the run log. `None` until it has
/// claimed.
#[derive(Debug)]
pub struct Plan {
    jobs: Vec<Job>,
    next: Option<BTreeMap<String, u64>>,
}

impl Plan {
    pub fn new(jobs: Vec<Job>) -> Self {
        Self { jobs, next: None }
    }

    /// `last` is the run log's last fired tick per job, as the claim returned it. A job the log has never fired
    /// starts at the tick `now` is in, fixed here: a fire sent again after a lost answer must ask for the same ticks,
    /// not for whatever tick the clock has reached by then.
    pub fn claimed(&mut self, last: BTreeMap<String, u64>, now: u64) {
        let next = self.jobs.iter().map(|job| {
            let first = last.get(&job.name).map_or(job.tick_at(now).unwrap_or(0), |l| l + 1);
            (job.name.clone(), first)
        });
        self.next = Some(next.collect());
    }

    pub fn is_claimed(&self) -> bool {
        self.next.is_some()
    }

    /// The first tick of `job` not yet confirmed.
    pub fn next(&self, job: &str) -> Option<u64> {
        self.next.as_ref()?.get(job).copied()
    }

    /// Every fire owed at wall time `now`, oldest tick first per job. A clock that stepped back owes nothing until it
    /// passes the last fired tick again.
    pub fn owed(&self, now: u64) -> Vec<Fire> {
        let Some(next) = &self.next else { return Vec::new() };
        let mut owed = Vec::new();
        for job in &self.jobs {
            let (Some(current), Some(&first)) = (job.tick_at(now), next.get(&job.name)) else { continue };
            if first > current {
                continue;
            }
            let runs = (current - first + 1).min(job.max_runs);
            let split = current + 1 - runs;
            owed.extend((split..=current).map(|tick| Fire {
                job: job.name.clone(),
                from: if tick == split { first } else { tick },
                tick,
            }));
        }
        owed
    }

    /// Records what the log said about `fires`. `Err` on the first refusal that means this scheduler must stop.
    pub fn settle(&mut self, fires: &[Fire], outcomes: &[Outcome]) -> Result<(), Outcome> {
        let next = self.next.as_mut().ok_or(Outcome::Fenced)?;
        if fires.len() != outcomes.len() {
            return Err(Outcome::Gap);
        }
        for (fire, outcome) in fires.iter().zip(outcomes) {
            match outcome {
                Outcome::Fired { .. } | Outcome::Duplicate => {
                    let first = next.entry(fire.job.clone()).or_default();
                    *first = (*first).max(fire.tick + 1);
                }
                refused => return Err(*refused),
            }
        }
        Ok(())
    }

    /// Wall time of the earliest tick not yet fired, if claimed.
    pub fn next_due(&self) -> Option<u64> {
        let next = self.next.as_ref()?;
        self.jobs.iter().filter_map(|job| next.get(&job.name).map(|&t| job.starts_at(t))).min()
    }
}

/// Checks a run log's history: per job, the ranges `from..=tick` of every fire it accepted must follow each other
/// with no overlap (no tick fired twice) and no hole (no tick skipped). Returns the ticks covered per job.
pub fn audit<'a>(fired: impl IntoIterator<Item = &'a Fire>) -> Result<BTreeMap<String, (u64, u64)>, String> {
    let mut covered: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for fire in fired {
        match covered.get_mut(&fire.job) {
            None => drop(covered.insert(fire.job.clone(), (fire.from, fire.tick))),
            Some((_, end)) if fire.from == *end + 1 && fire.tick >= fire.from => *end = fire.tick,
            Some((start, end)) => {
                let what = if fire.from <= *end { "fired twice" } else { "skipped" };
                return Err(format!("{}: {start}..={end} then {}..={}: {what}", fire.job, fire.from, fire.tick));
            }
        }
    }
    Ok(covered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(max_runs: u64) -> Job {
        Job { name: "j".into(), every_ms: 1000, offset_ms: 0, max_runs }
    }

    #[test]
    fn missed_ticks_fold_into_the_first_run_past_max_runs() {
        let mut plan = Plan::new(vec![job(3)]);
        plan.claimed([("j".to_string(), 10)].into(), 10_500);
        let owed: Vec<_> = plan.owed(20_500).into_iter().map(|f| (f.from, f.tick)).collect();
        assert_eq!(owed, [(11, 18), (19, 19), (20, 20)]);
        assert_eq!(plan.owed(10_999), []); // a clock behind the last fire owes nothing
        assert_eq!(plan.next_due(), Some(11_000));
    }

    #[test]
    fn a_job_never_fired_starts_where_the_claim_found_it_even_when_its_first_answer_is_lost() {
        let (mut plan, mut log) = (Plan::new(vec![job(1024)]), RunLog::default());
        plan.claimed(log.claim(1).unwrap(), 5_500);
        let first = plan.owed(5_600);
        assert_eq!(log.fire(1, &first[0]), Outcome::Fired { from: 5 }); // its answer is lost
        let again = plan.owed(7_200); // the resend, two ticks later
        let outcomes: Vec<_> = again.iter().map(|f| log.fire(1, f)).collect();
        assert_eq!(outcomes, [Outcome::Duplicate, Outcome::Fired { from: 6 }, Outcome::Fired { from: 7 }]);
        assert_eq!(plan.settle(&again, &outcomes), Ok(()));
        assert_eq!(plan.next("j"), Some(8));
    }

    #[test]
    fn the_log_refuses_old_epochs_repeats_and_holes() {
        let mut log = RunLog::default();
        let fire = |from, tick| Fire { job: "j".into(), from, tick };
        assert!(log.claim(5).is_ok());
        assert_eq!(log.fire(5, &fire(1, 1)), Outcome::Fired { from: 1 });
        assert_eq!(log.fire(5, &fire(1, 2)), Outcome::Fired { from: 2 }); // a resend that folded tick 1 in again
        assert_eq!(log.fire(5, &fire(1, 1)), Outcome::Duplicate);
        assert_eq!(log.fire(5, &fire(4, 4)), Outcome::Gap);
        assert_eq!(log.claim(4), Err(5));
        assert_eq!(log.claim(7).map(|l| l["j"]), Ok(2));
        assert_eq!(log.fire(5, &fire(3, 3)), Outcome::Fenced);
        assert!(audit(&[fire(1, 1), fire(2, 4), fire(5, 5)]).is_ok());
        assert!(audit(&[fire(1, 2), fire(2, 3)]).unwrap_err().contains("twice"));
        assert!(audit(&[fire(1, 2), fire(4, 4)]).unwrap_err().contains("skipped"));
    }
}
