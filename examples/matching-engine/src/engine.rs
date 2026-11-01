//! The order-book actor: one per symbol, a keyed actor under a lease. It matches in the handler, journals what it
//! accepted once per turn (group commit, one write in flight), and only after that write answers the callers and
//! publishes the trades and level changes on `md/<symbol>`.

use std::fs::File;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axon::{Actor, Context, JobHandle, MailboxLimits, StopReason};
use axon_cluster::{Cluster, ClusterError, Reply, SpawnContext};
use axon_stream::{DeliveryPolicy, StreamError, StreamKey, StreamNode};
use bytes::Bytes;

use crate::book::{Book, Command, Level, Outcome, Side};
use crate::journal;
use crate::wire::{Answer, BookApi, Call, KIND, MARKET_DATA, MarketData, Msg, Snapshot};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub dir: PathBuf,
    /// Orders one book keeps resting.
    pub max_resting: usize,
    /// Recent order ids refused as duplicates, so a client may resend an order whose outcome it did not learn.
    pub max_seen: usize,
    /// Commands a book matches ahead of its journal; past this it pauses and its mailbox fills.
    pub max_unwritten: usize,
    /// The book's mailbox. A full one refuses an order at once (`Rejected(Full)`), never queues it.
    pub mailbox: usize,
}

impl EngineConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into(), max_resting: 100_000, max_seen: 100_000, max_unwritten: 4096, mailbox: 1024 }
    }
}

/// What every book on this node has counted.
#[derive(Default, Debug)]
pub struct EngineStats {
    pub commands: AtomicU64,
    pub writes: AtomicU64,
    pub pauses: AtomicU64,
    /// Market data the stream refused; the book's state is unaffected.
    pub md_refused: AtomicU64,
}

pub fn market_data(symbol: &str) -> StreamKey {
    StreamKey::new(MARKET_DATA, symbol)
}

/// Makes this node a candidate owner for books.
pub async fn serve(
    cluster: &Cluster,
    streams: StreamNode,
    cfg: EngineConfig,
) -> Result<Arc<EngineStats>, ClusterError> {
    let stats = Arc::new(EngineStats::default());
    let limits = MailboxLimits { messages: cfg.mailbox, ..Default::default() };
    let (cfg, shared, node) = (Arc::new(cfg), stats.clone(), Arc::<str>::from(cluster.identity().node_id.as_str()));
    cluster
        .kind::<BookApi>(KIND)
        .mailbox(limits)
        .register(move |cx: &SpawnContext, _args: Bytes| {
            let symbol = cx.key.clone().ok_or("a book needs a symbol")?;
            // Without a lease authority there is no epoch; the start time still orders owners on one clock.
            let epoch = cx.epoch.unwrap_or_else(axon::trace::unix_micros);
            Ok(BookActor::new(symbol, node.clone(), epoch, cfg.clone(), streams.clone(), shared.clone()))
        })
        .await?;
    Ok(stats)
}

struct Pending {
    reply: Option<Reply<Answer>>,
    answer: Answer,
}

pub struct BookActor {
    symbol: String,
    node: Arc<str>,
    epoch: u64,
    cfg: Arc<EngineConfig>,
    streams: StreamNode,
    stats: Arc<EngineStats>,
    key: StreamKey,
    book: Book,
    seq: u64,
    log: Option<Arc<File>>,
    /// Matched since the last write started: its journal lines, answers and market data.
    lines: Vec<u8>,
    pending: Vec<Pending>,
    md: Vec<Bytes>,
    /// The write in flight and what waits on it.
    writing: Option<(JobHandle, Vec<Pending>, Vec<Bytes>)>,
    levels: Vec<Level>,
}

impl BookActor {
    pub fn new(
        symbol: String,
        node: Arc<str>,
        epoch: u64,
        cfg: Arc<EngineConfig>,
        streams: StreamNode,
        stats: Arc<EngineStats>,
    ) -> Self {
        let key = market_data(&symbol);
        let book = Book::new(cfg.max_resting, cfg.max_seen);
        Self {
            symbol,
            node,
            epoch,
            cfg,
            streams,
            stats,
            key,
            book,
            seq: 0,
            log: None,
            lines: Vec::new(),
            pending: Vec::new(),
            md: Vec::new(),
            writing: None,
            levels: Vec::new(),
        }
    }

    fn record(&mut self, data: &MarketData) {
        if let Ok(bytes) = serde_json::to_vec(data) {
            self.md.push(bytes.into());
        }
    }

    fn submit(&mut self, command: Command) -> Result<Answer, String> {
        self.levels.clear();
        let outcome = self.book.apply(command, &mut self.levels);
        if !matches!(outcome, Outcome::Rejected(_)) {
            self.seq += 1;
            serde_json::to_writer(&mut self.lines, &command).map_err(|e| e.to_string())?;
            self.lines.push(b'\n');
            let seq = self.seq;
            if let Outcome::Accepted { fills, .. } = &outcome {
                for fill in fills.clone() {
                    self.record(&MarketData::Trade { seq, fill });
                }
            }
            for level in mem::take(&mut self.levels) {
                self.record(&MarketData::Level { seq, level });
            }
        }
        Ok(Answer::Done { seq: self.seq, outcome })
    }

    fn snapshot(&self, depth: usize) -> Snapshot {
        let (bids, asks) = self.book.depth(depth);
        let (seq, epoch, digest, resting) = (self.seq, self.epoch, self.book.digest(), self.book.resting());
        Snapshot { node: self.node.to_string(), seq, epoch, digest, resting, bids, asks }
    }

    fn publish(&self, records: Vec<Bytes>) {
        if records.is_empty() {
            return;
        }
        if self.streams.publish(&self.key, records).is_err() {
            self.stats.md_refused.fetch_add(1, Relaxed);
        }
    }

    fn answer(&self, done: Vec<Pending>, md: Vec<Bytes>) {
        self.publish(md);
        for Pending { reply, answer } in done {
            // A caller that gave up is not an error of the book's.
            drop(reply.map(|reply| reply.send(answer)));
        }
    }

    /// Opens the market data stream in a job the book pauses for.
    fn open_stream(&self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (streams, key, epoch) = (self.streams.clone(), self.key.clone(), self.epoch);
        let open = self.streams.open_stream(&self.key, self.epoch, DeliveryPolicy::ReplayRequired);
        let open = cx.submit_future(async move {
            Msg::Opened(match open.await {
                // This node owned the symbol before: its stream is still open under the older epoch.
                Err(StreamError::AlreadyExists) => streams.restart_stream(&key, epoch),
                other => other,
            })
        });
        cx.pause_until(&open.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }
}

impl Actor for BookActor {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (dir, symbol, epoch) = (self.cfg.dir.clone(), self.symbol.clone(), self.epoch);
        let load =
            cx.submit_io(move || Msg::Loaded(journal::load(&dir, &symbol, epoch))).map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&load).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let loaded = loaded?;
                (self.book, self.seq) = crate::replay(&loaded.commands, self.cfg.max_resting, self.cfg.max_seen);
                self.log = Some(loaded.log);
                self.open_stream(cx)?;
            }
            Msg::Opened(opened) => {
                opened.map_err(|e| format!("open md/{}: {e}", self.symbol))?;
                // The stream starts over with each owner: its first records are the whole book.
                let (bids, asks) = self.book.depth(usize::MAX);
                let seq = self.seq;
                let levels = bids.iter().map(|l| (Side::Buy, l)).chain(asks.iter().map(|l| (Side::Sell, l)));
                let book: Vec<_> = levels
                    .filter_map(|(side, &(price, qty))| {
                        serde_json::to_vec(&MarketData::Level { seq, level: Level { side, price, qty } }).ok()
                    })
                    .map(Bytes::from)
                    .collect();
                self.publish(book);
            }
            Msg::Call(Call::Submit(command), reply) => {
                self.stats.commands.fetch_add(1, Relaxed);
                let answer = self.submit(command)?;
                self.pending.push(Pending { reply, answer });
            }
            Msg::Call(Call::Snapshot { depth }, reply) => {
                let answer = Answer::Snapshot(self.snapshot(depth));
                self.pending.push(Pending { reply, answer });
            }
            Msg::Written(written) => {
                written?;
                if let Some((_, done, md)) = self.writing.take() {
                    self.answer(done, md);
                }
            }
        }
        Ok(())
    }

    /// One journal write per turn at most, and one in flight: the next turn's commands wait for it.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if let Some((job, ..)) = &self.writing {
            if self.pending.len() >= self.cfg.max_unwritten {
                self.stats.pauses.fetch_add(1, Relaxed);
                cx.pause_until(job).map_err(|e| format!("{e:?}"))?;
            }
            return Ok(());
        }
        if self.pending.is_empty() {
            return Ok(());
        }
        let (done, md) = (mem::take(&mut self.pending), mem::take(&mut self.md));
        let lines = mem::take(&mut self.lines);
        if lines.is_empty() {
            // Only queries and refusals: nothing to make durable.
            self.answer(done, md);
            return Ok(());
        }
        let log = self.log.clone().ok_or("no journal")?;
        let job = cx.submit_io(move || Msg::Written(journal::append(&log, &lines))).map_err(|e| format!("{e:?}"))?;
        self.stats.writes.fetch_add(1, Relaxed);
        self.writing = Some((job, done, md));
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        // The next owner opens the stream again under its own epoch, possibly here.
        let _ = self.streams.close_stream(&self.key);
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Call(Call::Submit(Command::Cancel { .. }), _) => "cancel",
            Msg::Call(Call::Submit(_), _) => "order",
            Msg::Call(Call::Snapshot { .. }, _) => "snapshot",
            Msg::Loaded(_) => "loaded",
            Msg::Written(_) => "written",
            Msg::Opened(_) => "opened",
        }
    }
}
