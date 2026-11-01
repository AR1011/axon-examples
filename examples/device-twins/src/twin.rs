//! The twin: one keyed actor per device, holding its last report, the last `WINDOW` readings and the alert rule.
//!
//! A twin that hears nothing for the idle time drains and parks its `State` on its node. Whoever starts the
//! device's twin next takes the state back: from its own node's lot, or from another twin node's when the device
//! moved. The lease authority makes the old twin's exit, and so its `on_stop`, come before the new twin's start.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, AtomicHistogram, Context, StopReason};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WeakCluster, WireProtocol};
use axon_stream::{StreamKey, StreamNode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::wire::{Report, now_us};

pub const KIND: &str = "twin";
pub const WINDOW: usize = 16;
pub const THRESHOLD: f32 = 80.0;
/// Readings in a row on the other side of the threshold that raise or clear an alert.
pub const STREAK: u8 = 3;
/// The raw endpoint every twin node serves its parked states on.
pub const PARKED: &str = "twins.parked";
pub const PARKED_PROTOCOL: u64 = 0xD7_0001;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Event {
    Raised,
    Cleared,
}

/// Everything a twin knows. It is what survives an idle stop and a move.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub last_seq: u32,
    pub reports: u64,
    pub duplicates: u64,
    /// Sequence numbers skipped: reports acked to the device that this state never saw. Zero unless state was lost.
    pub gaps: u64,
    pub temperature: f32,
    window: [f32; WINDOW],
    filled: u8,
    next: u8,
    pub alert: bool,
    streak: u8,
    pub raised: u32,
    pub cleared: u32,
}

impl State {
    /// `None` for a sequence number already applied: a device resending a report whose ack it missed.
    pub fn apply(&mut self, seq: u32, temperature: f32) -> Option<Option<Event>> {
        if seq <= self.last_seq {
            self.duplicates += 1;
            return None;
        }
        self.gaps += u64::from(seq - self.last_seq - 1);
        (self.last_seq, self.temperature) = (seq, temperature);
        self.reports += 1;
        self.window[self.next as usize] = temperature;
        self.next = (self.next + 1) % WINDOW as u8;
        self.filled = (self.filled + 1).min(WINDOW as u8);
        Some(self.rule(temperature))
    }

    fn rule(&mut self, temperature: f32) -> Option<Event> {
        let hot = temperature > THRESHOLD;
        if hot == self.alert {
            self.streak = 0;
            return None;
        }
        self.streak += 1;
        if self.streak < STREAK {
            return None;
        }
        (self.alert, self.streak) = (hot, 0);
        if hot {
            self.raised += 1;
            Some(Event::Raised)
        } else {
            self.cleared += 1;
            Some(Event::Cleared)
        }
    }

    /// Min, max and mean of the last `WINDOW` readings; all 0 before the first, as JSON has no infinity.
    pub fn stats(&self) -> (f32, f32, f32) {
        let window = &self.window[..self.filled as usize];
        let min = window.iter().copied().reduce(f32::min).unwrap_or(0.0);
        let max = window.iter().copied().reduce(f32::max).unwrap_or(0.0);
        (min, max, window.iter().sum::<f32>() / window.len().max(1) as f32)
    }
}

/// What `GET /devices/<id>` answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub device: u64,
    pub node: String,
    pub epoch: u64,
    pub last_seq: u32,
    pub reports: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub temperature: f32,
    pub min: f32,
    pub max: f32,
    pub mean: f32,
    pub readings: usize,
    pub alert: bool,
    pub raised: u32,
    pub cleared: u32,
}

/// One record of the `alerts/<node>` stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    pub device: u64,
    pub seq: u32,
    pub event: Event,
    pub temperature: f32,
    pub node: String,
}

pub fn alerts_key(node: &str) -> StreamKey {
    StreamKey::new("alerts", node)
}

/// Parked states of stopped twins. Bounded: past `max`, a stopping twin's state is dropped and counted.
pub struct Parking {
    states: Mutex<HashMap<u64, State>>,
    /// The most states parked at once.
    max: usize,
    pub dropped: AtomicU64,
}

impl Parking {
    pub fn new(max: usize) -> Self {
        Self { states: Mutex::new(HashMap::new()), max, dropped: AtomicU64::new(0) }
    }

    fn states(&self) -> std::sync::MutexGuard<'_, HashMap<u64, State>> {
        self.states.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn park(&self, device: u64, state: State) {
        let mut states = self.states();
        if states.len() < self.max || states.contains_key(&device) {
            states.insert(device, state);
        } else {
            self.dropped.fetch_add(1, Relaxed);
        }
    }

    pub fn take(&self, device: u64) -> Option<State> {
        self.states().remove(&device)
    }

    pub fn len(&self) -> usize {
        self.states().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A node's twin counters: `Counters` while they count, `TwinCounters` as read at one moment.
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TwinCounters<C = u64> {
    pub started: C,
    pub restored_local: C,
    pub restored_remote: C,
    /// Starts that found no parked state anywhere: new devices, or state lost with a node.
    pub fresh: C,
    pub restore_errors: C,
    /// Twins that ended: an idle stop, a move, or their node leaving.
    pub stopped: C,
    pub reports: C,
    pub duplicates: C,
    pub gaps: C,
    pub alerts: C,
    pub publish_errors: C,
}

pub type Counters = TwinCounters<AtomicU64>;

impl Counters {
    pub fn read(&self) -> TwinCounters {
        let get = |c: &AtomicU64| c.load(Relaxed);
        TwinCounters {
            started: get(&self.started),
            restored_local: get(&self.restored_local),
            restored_remote: get(&self.restored_remote),
            fresh: get(&self.fresh),
            restore_errors: get(&self.restore_errors),
            stopped: get(&self.stopped),
            reports: get(&self.reports),
            duplicates: get(&self.duplicates),
            gaps: get(&self.gaps),
            alerts: get(&self.alerts),
            publish_errors: get(&self.publish_errors),
        }
    }
}

fn bump(counter: &AtomicU64, by: u64) {
    counter.fetch_add(by, Relaxed);
}

/// What every twin on a node shares.
pub struct Shared {
    pub node: String,
    pub streams: StreamNode,
    pub alerts: StreamKey,
    pub parking: Parking,
    pub counters: Counters,
    /// Device send to twin handling, wall clock.
    pub latency: AtomicHistogram,
}

/// Where a request's reply goes: `None` answers a report, a `View` a get.
type Answer = Option<Reply<Option<View>>>;

pub enum Msg {
    /// Answered once applied: the ingest acks the device only then.
    Report(Report, Answer),
    Get(Answer),
    /// The fetch is over; whatever it found is in this node's parking lot.
    Restored,
}

pub struct Twin {
    device: u64,
    epoch: u64,
    shared: Arc<Shared>,
    cluster: WeakCluster,
    state: State,
    /// The fetch is back or was not needed: `state` is the device's, even if empty.
    known: bool,
}

impl Twin {
    pub fn new(device: u64, epoch: u64, shared: Arc<Shared>, cluster: WeakCluster) -> Self {
        Self { device, epoch, shared, cluster, state: State::default(), known: false }
    }

    fn apply(&mut self, report: Report, reply: Answer) {
        drop(reply.map(|r| r.send(None)));
        let c = &self.shared.counters;
        let gaps = self.state.gaps;
        let Some(event) = self.state.apply(report.seq, report.temperature) else {
            return bump(&c.duplicates, 1);
        };
        bump(&c.reports, 1);
        bump(&c.gaps, self.state.gaps - gaps);
        let late = now_us().saturating_sub(report.sent_us);
        self.shared.latency.record(Duration::from_micros(late));
        let Some(event) = event else { return };
        bump(&c.alerts, 1);
        let alert = Alert {
            device: self.device,
            seq: report.seq,
            event,
            temperature: report.temperature,
            node: self.shared.node.clone(),
        };
        let published = serde_json::to_vec(&alert)
            .map(|record| self.shared.streams.publish(&self.shared.alerts, vec![Bytes::from(record)]));
        if !matches!(published, Ok(Ok(_))) {
            bump(&c.publish_errors, 1);
        }
    }
}

/// Takes the device's parked state from the first other twin node that has it.
async fn fetch(shared: Arc<Shared>, cluster: WeakCluster, device: u64) -> Msg {
    let Some(cluster) = cluster.upgrade() else { return Msg::Restored };
    let view = cluster.view();
    let peers = view.members.iter().filter(|m| m.reachable && m.record.has_kind(KIND) && m.record.id != shared.node);
    let body = Bytes::copy_from_slice(&device.to_be_bytes());
    for peer in peers {
        // A take whose answer is lost loses the state with it; the counter says how often.
        for attempt in 0..5u64 {
            match cluster.request(&peer.record.id, PARKED, PARKED_PROTOCOL, 1, body.clone()).await {
                Ok(state) if state.is_empty() => break,
                Ok(state) => match serde_json::from_slice(&state) {
                    // Parked here before the twin hears of it, so a stop in between does not lose it.
                    Ok(state) => {
                        shared.parking.park(device, state);
                        return Msg::Restored;
                    }
                    Err(_) => {
                        bump(&shared.counters.restore_errors, 1);
                        break;
                    }
                },
                Err(_) => {
                    bump(&shared.counters.restore_errors, 1);
                    tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
                }
            }
        }
    }
    Msg::Restored
}

impl Actor for Twin {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let c = &self.shared.counters;
        bump(&c.started, 1);
        if let Some(state) = self.shared.parking.take(self.device) {
            (self.state, self.known) = (state, true);
            bump(&c.restored_local, 1);
            return Ok(());
        }
        // Reports wait in the mailbox until the fetch is back; its bound holds the ingest back.
        let fetch = cx
            .submit_future(fetch(self.shared.clone(), self.cluster.clone(), self.device))
            .map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&fetch).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Report(report, reply) => self.apply(report, reply),
            Msg::Restored => {
                self.known = true;
                let c = &self.shared.counters;
                match self.shared.parking.take(self.device) {
                    Some(state) => (self.state, _) = (state, bump(&c.restored_remote, 1)),
                    None => bump(&c.fresh, 1),
                }
            }
            Msg::Get(reply) => {
                let (min, max, mean) = self.state.stats();
                let s = &self.state;
                let view = View {
                    device: self.device,
                    node: self.shared.node.clone(),
                    epoch: self.epoch,
                    last_seq: s.last_seq,
                    reports: s.reports,
                    duplicates: s.duplicates,
                    gaps: s.gaps,
                    temperature: s.temperature,
                    min,
                    max,
                    mean,
                    readings: s.filled as usize,
                    alert: s.alert,
                    raised: s.raised,
                    cleared: s.cleared,
                };
                drop(reply.map(|r| r.send(Some(view))));
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        bump(&self.shared.counters.stopped, 1);
        // Stopped while fetching, it has no state yet: whatever the fetch found is in the lot already. The stop drops
        // the reports that waited, and with them their replies, so the ingest does not ack them. A known state parks
        // even before its first report, so the next owner does not start fresh a second time.
        if self.known {
            self.shared.parking.park(self.device, self.state);
        }
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Report(..) => "report",
            Msg::Get(_) => "get",
            Msg::Restored => "restored",
        }
    }
}

#[derive(Clone, Copy)]
pub enum Op {
    Report(Report),
    Get,
}

/// The twin's wire contract: a report is the device's own datagram, a `G` asks for the `View` as JSON.
pub struct TwinApi;

impl WireProtocol for TwinApi {
    type Message = Op;
    /// `None` answers a report.
    type Reply = Option<View>;
    const ID: u64 = 0xD7;
    const SCHEMA: u16 = 1;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        Ok(match op {
            Op::Report(report) => Bytes::copy_from_slice(&report.encode()),
            Op::Get => Bytes::from_static(b"G"),
        })
    }

    fn decode_reply(body: Bytes) -> Result<Option<View>, CallError> {
        if body.is_empty() {
            return Ok(None);
        }
        serde_json::from_slice(&body).map(Some).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for TwinApi {
    type Mailbox = Msg;

    fn encode_reply(view: Option<View>) -> Result<Bytes, CallError> {
        let Some(view) = view else { return Ok(Bytes::new()) };
        serde_json::to_vec(&view).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
    }

    fn decode(body: Bytes, reply: Answer) -> Result<Msg, Rejection> {
        if &body[..] == b"G" {
            return Ok(Msg::Get(reply));
        }
        Report::decode(&body).map(|report| Msg::Report(report, reply)).ok_or(Rejection::InvalidMessage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::temperature;

    fn events(device: u64, seqs: std::ops::RangeInclusive<u32>) -> Vec<(u32, Event)> {
        let mut state = State::default();
        seqs.filter_map(|seq| state.apply(seq, temperature(device, seq)).flatten().map(|e| (seq, e))).collect()
    }

    #[test]
    fn three_in_a_row_raise_and_clear() {
        // Device 0 reads CYCLE from position 1: H C H H H H C C C C H H C C C H | H C H H ...
        assert_eq!(
            events(0, 1..=32),
            [(5, Event::Raised), (9, Event::Cleared), (21, Event::Raised), (25, Event::Cleared)]
        );
        assert!(events(1, 1..=100).is_empty(), "cool devices never alert");
        assert!(events(8, 1..=32).len() == 4, "another phase, the same cycle");
    }

    #[test]
    fn duplicates_are_dropped_and_gaps_counted() {
        let mut state = State::default();
        assert_eq!(state.apply(1, 90.0), Some(None));
        assert_eq!(state.apply(1, 90.0), None);
        assert_eq!(state.apply(4, 90.0), Some(None));
        assert_eq!((state.reports, state.duplicates, state.gaps, state.last_seq), (2, 1, 2, 4));
        assert_eq!(state.apply(5, 10.0), Some(None), "two hot readings are not three");
        assert!(!state.alert);
        for seq in 6..=20 {
            state.apply(seq, 10.0 + seq as f32);
        }
        // The window keeps the last 16 of 90, 90, 10, 16..=30.
        assert_eq!(state.stats(), (10.0, 30.0, (10.0 + (16..=30).sum::<i32>() as f32) / 16.0));
        assert_eq!(state.filled as usize, WINDOW);
    }

    #[test]
    fn stats_without_readings_survive_json() {
        // The `View` of a device that never reported goes as JSON, where an infinity becomes a null that fails it.
        let stats = State::default().stats();
        let json = serde_json::to_string(&stats).unwrap();
        assert_eq!(serde_json::from_str::<(f32, f32, f32)>(&json).unwrap(), stats);
    }
}
