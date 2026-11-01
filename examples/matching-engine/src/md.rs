//! A market-data view of one book: its levels, rebuilt from `md/<symbol>` on any node.
//!
//! A live subscription follows the key across owners, and a new generation arrives from its record 1 while a ring on
//! the path holds it. A first join, or one past what the rings hold, starts later. So the view does what market-data
//! feeds do: at each new generation it asks the book for a snapshot, and applies the records numbered after it.

use std::collections::BTreeMap;
use std::time::Duration;

use axon::{Actor, Context, ReplyTo};
use axon_cluster::KeyedRef;
use axon_stream::{StreamEvent, StreamNode, SubscribeOptions};

use crate::book::{Price, Qty, Side};
use crate::client::{self, Refused};
use crate::engine::market_data;
use crate::wire::{BookApi, MarketData, Snapshot};

/// Records held while a snapshot is on its way; past this the view asks for another.
const MAX_BUFFERED: usize = 65_536;
/// One snapshot's wait, as long as a request's default timeout; a view that gets none asks again.
const SNAPSHOT_WITHIN: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
    /// The book sequence the view reflects.
    pub seq: u64,
    pub trades: u64,
    /// Snapshots taken: one per owner the book had while the view watched, plus overflows.
    pub snapshots: u32,
    pub synced: bool,
}

pub enum Msg {
    Event(Option<StreamEvent>),
    Snapshot(u64, Result<Snapshot, Refused>),
    Resync(u64),
    Get(ReplyTo<View>),
}

pub struct MarketDataView {
    symbol: String,
    streams: StreamNode,
    book: KeyedRef<BookApi>,
    generation: u64,
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
    seq: u64,
    trades: u64,
    snapshots: u32,
    synced: bool,
    buffered: Vec<MarketData>,
}

impl MarketDataView {
    pub fn new(symbol: &str, streams: StreamNode, book: KeyedRef<BookApi>) -> Self {
        Self {
            symbol: symbol.into(),
            streams,
            book,
            generation: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            seq: 0,
            trades: 0,
            snapshots: 0,
            synced: false,
            buffered: Vec::new(),
        }
    }

    fn resync(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.synced = false;
        self.buffered.clear();
        let (book, generation) = (self.book.clone(), self.generation);
        let snapshot =
            async move { Msg::Snapshot(generation, client::snapshot(&book, usize::MAX, SNAPSHOT_WITHIN).await) };
        cx.submit_future(snapshot).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn apply(&mut self, record: MarketData) {
        match record {
            MarketData::Trade { seq, .. } => {
                self.trades += 1;
                self.seq = seq;
            }
            MarketData::Level { seq, level } => {
                let side = match level.side {
                    Side::Buy => &mut self.bids,
                    Side::Sell => &mut self.asks,
                };
                if level.qty == 0 {
                    side.remove(&level.price);
                } else {
                    side.insert(level.price, level.qty);
                }
                self.seq = seq;
            }
        }
    }
}

impl Actor for MarketDataView {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let subscription = self.streams.subscribe(&market_data(&self.symbol), SubscribeOptions::default());
        let subscription = subscription.map_err(|e| format!("subscribe md/{}: {e}", self.symbol))?;
        cx.attach(subscription, Msg::Event).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Event(Some(StreamEvent::Ready { start: Some(cursor) })) if cursor.generation != self.generation => {
                self.generation = cursor.generation;
                self.resync(cx)?;
            }
            Msg::Event(Some(StreamEvent::Batch(batch))) => {
                for record in batch.records() {
                    let record: MarketData = serde_json::from_slice(record).map_err(|e| e.to_string())?;
                    if self.synced {
                        self.apply(record);
                    } else if self.buffered.len() < MAX_BUFFERED {
                        self.buffered.push(record);
                    } else {
                        self.resync(cx)?;
                    }
                }
            }
            Msg::Event(Some(event)) if event.is_terminal() => {
                return Err(format!("md/{} ended: {event:?}", self.symbol));
            }
            Msg::Event(None) => return Err(format!("md/{} subscription ended", self.symbol)),
            Msg::Event(Some(_)) => {}
            Msg::Snapshot(generation, _) if generation != self.generation => {}
            Msg::Snapshot(_, Ok(snapshot)) => {
                self.snapshots += 1;
                self.bids = snapshot.bids.into_iter().collect();
                self.asks = snapshot.asks.into_iter().collect();
                self.seq = snapshot.seq;
                for record in std::mem::take(&mut self.buffered) {
                    let seq = match &record {
                        MarketData::Trade { seq, .. } | MarketData::Level { seq, .. } => *seq,
                    };
                    if seq > snapshot.seq {
                        self.apply(record);
                    }
                }
                self.synced = true;
            }
            // The book is moving or busy: ask again shortly.
            Msg::Snapshot(generation, Err(_)) => {
                cx.schedule_after(Duration::from_millis(200), Msg::Resync(generation)).map_err(|_| "no timer")?;
            }
            Msg::Resync(generation) if generation == self.generation => self.resync(cx)?,
            Msg::Resync(_) => {}
            Msg::Get(reply) => {
                let view = View {
                    bids: self.bids.iter().rev().map(|(p, q)| (*p, *q)).collect(),
                    asks: self.asks.iter().map(|(p, q)| (*p, *q)).collect(),
                    seq: self.seq,
                    trades: self.trades,
                    snapshots: self.snapshots,
                    synced: self.synced,
                };
                drop(reply.reply(view));
            }
        }
        Ok(())
    }
}
