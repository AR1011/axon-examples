//! Checks the logs on disk against every invariant the ledger promises, and says which record broke one.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::store::{Event, History, State};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Audit {
    pub accounts: usize,
    pub records: usize,
    /// Segments with an `Own` record: how many times an account was taken over, first starts included.
    pub owners: usize,
    /// Bytes past a seal, which nobody reads: writes a newer owner had fenced off, or a crash cut short.
    pub fenced_bytes: u64,
    pub balances: i64,
    pub deposits: u64,
    pub withdrawals: u64,
    /// Debited and neither credited nor refunded yet.
    pub in_flight: u64,
    /// Debited and not yet marked settled or refunded at the source, credited or not.
    pub pending: usize,
    pub settled: usize,
    pub refunded: usize,
    pub balance: BTreeMap<String, i64>,
}

struct Out {
    to: String,
    amount: u64,
    end: Option<bool>, // Some(true) settled, Some(false) refunded
}

/// Per account: each owner's segment starts with its one `Own`, whose seals match what every later owner read, no key
/// is applied twice, no balance goes below zero. Across accounts: every credit has its debit, a settled debit its
/// credit, a refund its rejection and no credit, and the balances plus what is in flight add up to deposits minus
/// withdrawals.
pub fn audit(logs: &BTreeMap<String, History>) -> Result<Audit, String> {
    let mut a = Audit::default();
    let mut outs: HashMap<(String, String), Out> = HashMap::new();
    let mut ins: HashMap<(String, String), (String, Option<u64>)> = HashMap::new();
    for (id, history) in logs {
        let (mut keys, mut state) = (HashSet::new(), State::default());
        a.fenced_bytes += history.fenced_bytes;
        for (n, segment) in history.segments.iter().enumerate() {
            let events = segment.events_from(0).map_err(|e| format!("{id} segment {}: {e}", segment.epoch))?;
            a.records += events.len();
            for (i, event) in events.iter().enumerate() {
                let at = format!("{id} segment {} record {i} ({event:?})", segment.epoch);
                if (i == 0) != matches!(event, Event::Own { .. }) {
                    return Err(format!("{at}: an owner's segment starts with its one Own"));
                }
                match event {
                    Event::Own { sealed } => {
                        a.owners += 1;
                        for older in &history.segments[..n] {
                            let read = sealed.get(&older.epoch).copied().unwrap_or(0);
                            if read != older.len() {
                                return Err(format!("{at}: sealed {} at {read}, it is {}", older.epoch, older.len()));
                            }
                        }
                    }
                    Event::Open(key) | Event::Refused(key, _) => once(&mut keys, key, &at)?,
                    Event::Deposit(key, amount) => {
                        once(&mut keys, key, &at)?;
                        a.deposits += amount;
                    }
                    Event::Withdraw(key, amount) => {
                        once(&mut keys, key, &at)?;
                        a.withdrawals += amount;
                    }
                    Event::Out { tid, to, amount } => {
                        once(&mut keys, tid, &at)?;
                        outs.insert((id.clone(), tid.clone()), Out { to: to.clone(), amount: *amount, end: None });
                    }
                    Event::Settled(tid) | Event::Refund(tid) => {
                        let out = outs.get_mut(&(id.clone(), tid.clone())).filter(|o| o.end.is_none());
                        let out = out.ok_or_else(|| format!("{at}: no open debit to end"))?;
                        out.end = Some(matches!(event, Event::Settled(_)));
                    }
                    Event::In { from, tid, amount } => credit(&mut ins, (id, from, tid), Some(*amount), &at)?,
                    Event::Reject { from, tid } => credit(&mut ins, (id, from, tid), None, &at)?,
                }
                state.apply(event);
                if state.balance < 0 {
                    return Err(format!("{at}: balance {} below zero", state.balance));
                }
            }
        }
        a.accounts += usize::from(state.open);
        a.balances += state.balance;
        a.balance.insert(id.clone(), state.balance);
    }
    for ((from, tid), (dest, amount)) in &ins {
        let out = outs.get(&(from.clone(), tid.clone())).filter(|o| &o.to == dest);
        let out = out.ok_or_else(|| format!("{dest} has {from}/{tid} and {from} never debited it"))?;
        if amount.is_some_and(|amount| amount != out.amount) {
            return Err(format!("{from}/{tid}: debited {}, credited {amount:?}", out.amount));
        }
    }
    for ((from, tid), out) in &outs {
        let credit = ins.get(&(from.clone(), tid.clone())).map(|(_, amount)| amount.is_some());
        match (out.end, credit) {
            (Some(true), Some(true)) => a.settled += 1,
            (Some(false), Some(false)) => a.refunded += 1,
            (None, credited) => {
                a.pending += 1;
                if credited != Some(true) {
                    a.in_flight += out.amount;
                }
            }
            (end, credit) => {
                return Err(format!("{from}/{tid}: one side only (source end {end:?}, credit {credit:?})"));
            }
        }
    }
    if a.balances + a.in_flight as i64 != a.deposits as i64 - a.withdrawals as i64 {
        return Err(format!("money is not conserved: {a:?}"));
    }
    Ok(a)
}

fn once(keys: &mut HashSet<String>, key: &str, at: &str) -> Result<(), String> {
    match keys.insert(key.to_owned()) {
        true => Ok(()),
        false => Err(format!("{at}: key {key} applied twice")),
    }
}

type Credits = HashMap<(String, String), (String, Option<u64>)>;

fn credit(
    ins: &mut Credits,
    (dest, from, tid): (&str, &str, &str),
    amount: Option<u64>,
    at: &str,
) -> Result<(), String> {
    match ins.insert((from.to_owned(), tid.to_owned()), (dest.to_owned(), amount)) {
        None => Ok(()),
        Some(_) => Err(format!("{at}: {from}/{tid} credited twice")),
    }
}
