//! Simulated devices. Each reports every `period`, keeps one report in flight and sends it again until it is
//! acked, so the next reading goes out only after its twin applied the last one.
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::AtomicHistogram;
use tokio::net::UdpSocket;

use crate::Result;
use crate::wire::{Report, decode_ack, now_us, temperature};

#[derive(Clone, Copy, Default)]
struct Device {
    acked: u32,
    /// The report in flight: its seq, when it was first sent (wall clock and monotonic).
    flying: Option<(u32, u64, Instant)>,
}

#[derive(Default)]
pub struct LoadStats {
    pub sent: AtomicU64,
    pub resent: AtomicU64,
    pub acked: AtomicU64,
    /// Readings skipped because the previous one was still unacked when the next was due.
    pub late: AtomicU64,
    pub send_errors: AtomicU64,
    /// First send to ack, resends included.
    pub rtt: AtomicHistogram,
}

#[derive(Clone)]
pub struct Run {
    pub targets: Vec<SocketAddr>,
    pub period: Duration,
    /// New readings each device sends in this run; `None` runs until `stop` is set.
    pub reports: Option<u32>,
    pub stop: Arc<AtomicBool>,
    /// Each reading goes to the next target in turn; otherwise a device keeps to one.
    pub rotate: bool,
    pub resend_after: Duration,
    /// A run with `reports` that is not done by then fails.
    pub deadline: Duration,
}

impl Run {
    pub fn new(targets: Vec<SocketAddr>, period: Duration, reports: Option<u32>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        Self {
            targets,
            period,
            reports,
            stop,
            rotate: true,
            resend_after: Duration::from_secs(1),
            deadline: Duration::from_secs(60),
        }
    }
}

/// Device `id` lives in shard `id % shards` at index `id / shards`; each shard is one task and one socket.
pub struct Fleet {
    shards: Vec<Vec<Device>>,
    pub stats: Arc<LoadStats>,
}

impl Fleet {
    pub fn new(devices: u64, shards: usize) -> Self {
        let shards = (0..shards as u64)
            .map(|k| vec![Device::default(); devices.saturating_sub(k).div_ceil(shards as u64) as usize])
            .collect();
        Self { shards, stats: Arc::default() }
    }

    /// Every device with the last seq it had acked.
    pub fn acked(&self) -> impl Iterator<Item = (u64, u32)> + '_ {
        let n = self.shards.len();
        (self.shards.iter().enumerate())
            .flat_map(move |(k, shard)| shard.iter().enumerate().map(move |(i, d)| ((k + i * n) as u64, d.acked)))
    }

    pub async fn run(&mut self, run: &Run) -> Result<()> {
        let n = self.shards.len();
        let tasks: Vec<_> = (self.shards.iter_mut().enumerate())
            .map(|(k, shard)| tokio::spawn(drive(k, n, std::mem::take(shard), run.clone(), self.stats.clone())))
            .collect();
        let mut failed = Vec::new();
        for (k, task) in tasks.into_iter().enumerate() {
            let (shard, result) = task.await?;
            self.shards[k] = shard;
            failed.extend(result.err().map(|e| e.to_string()));
        }
        if failed.is_empty() { Ok(()) } else { Err(failed.join("; ").into()) }
    }
}

async fn drive(
    k: usize,
    shards: usize,
    mut devices: Vec<Device>,
    run: Run,
    stats: Arc<LoadStats>,
) -> (Vec<Device>, Result<()>) {
    let result = pump(k, shards, &mut devices, &run, &stats).await;
    (devices, result)
}

async fn pump(k: usize, shards: usize, devices: &mut [Device], run: &Run, stats: &LoadStats) -> Result<()> {
    let n = devices.len();
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await?;
    let goal: Vec<u32> = devices.iter().map(|d| run.reports.map_or(u32::MAX, |r| d.acked + r)).collect();
    let mut done = 0;
    let id = |i: usize| (k + i * shards) as u64;
    let start = Instant::now();
    let due = |round: u64, i: usize| start + run.period.mul_f64(round as f64 + i as f64 / n.max(1) as f64);
    // A report left in flight by the last run goes out again first.
    let mut resend: VecDeque<(Instant, usize, u32)> =
        devices.iter().enumerate().filter_map(|(i, d)| d.flying.map(|(seq, ..)| (start, i, seq))).collect();
    let (mut round, mut next) = (0u64, 0usize);
    let mut tick = tokio::time::interval(Duration::from_millis(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut buf = [0u8; 64];
    loop {
        let finished = if run.reports.is_some() { done == n } else { run.stop.load(Relaxed) };
        if finished {
            return Ok(());
        }
        if start.elapsed() > run.deadline {
            return Err(format!("shard {k}: {} of {n} devices unfinished after {:?}", n - done, run.deadline).into());
        }
        let first = tokio::select! {
            // An error is a port that refused an earlier datagram; the resend covers it.
            got = socket.recv_from(&mut buf) => got.ok().map(|(len, _)| len),
            _ = tick.tick() => None,
        };
        let mut got = first;
        while let Some(len) = got {
            if let Some((device, seq)) = decode_ack(&buf[..len]).filter(|(d, _)| *d as usize % shards == k) {
                let i = device as usize / shards;
                if let Some(d) = devices.get_mut(i).filter(|d| d.flying.is_some_and(|f| f.0 == seq)) {
                    stats.rtt.record(d.flying.map_or(Duration::ZERO, |f| f.2.elapsed()));
                    (d.acked, d.flying) = (seq, None);
                    stats.acked.fetch_add(1, Relaxed);
                    done += (seq == goal[i]) as usize;
                }
            }
            got = socket.try_recv_from(&mut buf).ok().map(|(len, _)| len);
        }

        let now = Instant::now();
        while due(round, next) <= now {
            let i = next;
            (next, round) = if next + 1 == n { (0, round + 1) } else { (next + 1, round) };
            let d = &mut devices[i];
            if d.acked >= goal[i] {
                continue;
            }
            if d.flying.is_some() {
                stats.late.fetch_add(1, Relaxed);
                continue;
            }
            let (seq, sent_us) = (d.acked + 1, now_us());
            d.flying = Some((seq, sent_us, now));
            send(&socket, run, id(i), seq, sent_us, stats).await;
            stats.sent.fetch_add(1, Relaxed);
            resend.push_back((now + run.resend_after, i, seq));
        }
        while resend.front().is_some_and(|(at, ..)| *at <= now) {
            let Some((_, i, seq)) = resend.pop_front() else { break };
            let Some((_, sent_us, _)) = devices[i].flying.filter(|f| f.0 == seq) else { continue };
            send(&socket, run, id(i), seq, sent_us, stats).await;
            stats.resent.fetch_add(1, Relaxed);
            resend.push_back((now + run.resend_after, i, seq));
        }
    }
}

async fn send(socket: &UdpSocket, run: &Run, device: u64, seq: u32, sent_us: u64, stats: &LoadStats) {
    let turn = if run.rotate { u64::from(seq) } else { 0 };
    let target = run.targets[((device + turn) % run.targets.len() as u64) as usize];
    let report = Report { device, seq, sent_us, temperature: temperature(device, seq) };
    if socket.send_to(&report.encode(), target).await.is_err() {
        stats.send_errors.fetch_add(1, Relaxed);
    }
}
