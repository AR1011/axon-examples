//! The processor node hosts the keyed kind `candles`. A ws session asks for `<symbol>:<timeframe>` with
//! `cluster.keyed(..)`; the one sampler for that key subscribes to `trades/<symbol>` and publishes
//! `candles/<symbol>:<timeframe>` while the candle changes, and once more when it closes.
use std::error::Error;
use std::time::Duration;

use axon::{Actor, Context, RestartPolicy};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use axon_stream::{DeliveryPolicy, StreamError, StreamEvent, StreamKey, StreamNode, SubscribeOptions};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{Trade, now_ms, trades_key};

/// How often a changed candle is published. A closed candle goes out at once.
const PUBLISH_EVERY: Duration = Duration::from_millis(250);
/// How long after a candle's end a quiet market waits for late trades before the candle is closed on time.
const CLOSE_GRACE: u64 = 1000;

pub const KIND: &str = "candles";

pub fn candles_key(symbol: &str, tf: &str) -> StreamKey {
    StreamKey::new("candles", format!("{symbol}:{tf}"))
}

/// `1s` .. `1y`. Calendar units are fixed spans here: a month is 30 days, a year 365.
pub fn parse_timeframe(tf: &str) -> Option<u64> {
    let (n, unit) = tf.split_at(tf.len().checked_sub(1)?);
    let n: u64 = n.parse().ok().filter(|n| *n > 0)?;
    let unit = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        "w" => 7 * 86_400,
        "M" => 30 * 86_400,
        "y" => 365 * 86_400,
        _ => return None,
    };
    let secs = n.checked_mul(unit)?;
    (secs <= 365 * 86_400).then_some(secs * 1000)
}

pub async fn run(node: &crate::Node) -> Result<(), Box<dyn Error>> {
    let streams = node.streams.clone();
    // A sampler that fails (a trade that does not parse) is rebuilt in place; its clients keep their reference.
    node.cluster
        .kind::<CandlesApi>(KIND)
        .policy(RestartPolicy::default())
        .register(move |cx, _args| {
            let key = cx.key.clone().ok_or("candles need a key")?;
            let (symbol, tf) = key.split_once(':').ok_or("key is <symbol>:<timeframe>")?;
            let tf_ms = parse_timeframe(tf).ok_or_else(|| format!("bad timeframe {tf}"))?;
            Ok(Sampler {
                symbol: symbol.to_owned(),
                tf: tf.to_owned(),
                tf_ms,
                key: candles_key(symbol, tf),
                streams: streams.clone(),
                candle: None,
                dirty: false,
            })
        })
        .await?;
    Ok(())
}

/// One record of a `candles/<symbol>:<tf>` stream. `x` is set on the last record of a candle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    pub s: String,
    pub tf: String,
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
    pub n: u64,
    pub x: bool,
}

/// Folds one trade into the current candle. Returns the candle it closed, if the trade started a new one
/// and the current one was still open. A trade for a candle that is closed is dropped: nothing rewrites history.
pub fn fold(open: &mut Option<Candle>, trade: &Trade, tf: &str, tf_ms: u64) -> Option<Candle> {
    let t = trade.t - trade.t % tf_ms;
    let closed = match open {
        Some(c) if t < c.t || (t == c.t && c.x) => return None,
        Some(c) if t == c.t => {
            c.h = c.h.max(trade.p);
            c.l = c.l.min(trade.p);
            c.c = trade.p;
            c.v += trade.q;
            c.n += 1;
            return None;
        }
        Some(c) if !c.x => Some(Candle { x: true, ..c.clone() }),
        _ => None,
    };
    let (p, s, tf) = (trade.p, trade.s.clone(), tf.to_owned());
    *open = Some(Candle { s, tf, t, o: p, h: p, l: p, c: p, v: trade.q, n: 1, x: false });
    closed
}

pub enum Msg {
    Opened(Result<(), StreamError>),
    Trades(Option<StreamEvent>),
    Tick,
    Latest(Option<Reply>),
}

struct Sampler {
    symbol: String,
    tf: String,
    tf_ms: u64,
    key: StreamKey,
    streams: StreamNode,
    candle: Option<Candle>,
    dirty: bool,
}

impl Actor for Sampler {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let open = self.streams.open_stream(&self.key, now_ms(), DeliveryPolicy::ReplayRequired);
        let open = cx.submit_future(async move { Msg::Opened(open.await) }).map_err(|e| format!("{:?}", e.reason))?;
        cx.pause_until(&open).map_err(|e| format!("{e:?}"))?;
        cx.schedule_every(PUBLISH_EVERY, || Msg::Tick).map_err(|e| format!("{e:?}"))?;
        println!("processor: sampling {}", self.key.name());
        let sub = self.streams.subscribe(&trades_key(&self.symbol), SubscribeOptions::default());
        let sub = sub.map_err(|e| format!("{e:?}"))?;
        cx.attach(sub, Msg::Trades).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Opened(opened) => opened.map_err(|e| format!("{e:?}"))?,
            Msg::Trades(Some(StreamEvent::Batch(batch))) => {
                for record in batch.records() {
                    let trade: Trade = serde_json::from_slice(record).map_err(|e| e.to_string())?;
                    if let Some(closed) = fold(&mut self.candle, &trade, &self.tf, self.tf_ms) {
                        self.publish(&closed)?;
                    }
                    self.dirty = true;
                }
            }
            // A live subscription follows the key through the consumer's restarts; ending is a fault.
            Msg::Trades(Some(event)) if event.is_terminal() => return Err(format!("trades ended: {event:?}")),
            Msg::Trades(None) => return Err("trades subscription dropped".into()),
            Msg::Trades(_) => {}
            Msg::Tick => {
                // A quiet market still closes candles on time; the next trade opens the next one.
                let due = |c: &&mut Candle| !c.x && now_ms() >= c.t + self.tf_ms + CLOSE_GRACE;
                if let Some(c) = self.candle.as_mut().filter(due) {
                    c.x = true;
                    self.dirty = true;
                }
                if let Some(c) = self.candle.as_ref().filter(|_| self.dirty) {
                    self.publish(c)?;
                    self.dirty = false;
                }
            }
            Msg::Latest(reply) => {
                let body = serde_json::to_vec(&self.candle).map_err(|e| e.to_string())?;
                drop(reply.map(|r| r.send(Bytes::from(body))));
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, reason: &axon::StopReason) {
        eprintln!("processor: {} stopped: {reason:?}", self.key.name());
        drop(self.streams.close_stream(&self.key));
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Trades(_) => "trades",
            Msg::Tick => "tick",
            Msg::Latest(_) => "latest",
            Msg::Opened(_) => "opened",
        }
    }
}

impl Sampler {
    fn publish(&self, candle: &Candle) -> Result<(), String> {
        let record = Bytes::from(serde_json::to_vec(candle).map_err(|e| e.to_string())?);
        self.streams.publish(&self.key, vec![record]).map(drop).map_err(|e| format!("{e:?}"))
    }
}

/// The wire contract of a sampler: one request, answering the open candle as JSON (`null` before the first trade).
pub struct CandlesApi;

impl WireProtocol for CandlesApi {
    type Message = ();
    type Reply = Bytes;
    const ID: u64 = 0xCA;
    const SCHEMA: u16 = 1;

    fn encode((): ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for CandlesApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(_body: Bytes, reply: Option<Reply>) -> Result<Msg, Rejection> {
        Ok(Msg::Latest(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(t: u64, p: f64, q: f64) -> Trade {
        Trade { s: "BTCUSDT".into(), t, p, q, b: true }
    }

    #[test]
    fn timeframes() {
        assert_eq!(parse_timeframe("1s"), Some(1000));
        assert_eq!(parse_timeframe("5m"), Some(300_000));
        assert_eq!(parse_timeframe("1y"), Some(365 * 86_400_000));
        assert_eq!(parse_timeframe("2y"), None);
        assert_eq!(parse_timeframe("0m"), None);
        assert_eq!(parse_timeframe("m"), None);
        assert_eq!(parse_timeframe(""), None);
    }

    #[test]
    fn folds_trades_into_candles() {
        let mut open = None;
        assert_eq!(fold(&mut open, &trade(1_000, 10.0, 1.0), "5s", 5_000), None);
        assert_eq!(fold(&mut open, &trade(4_999, 12.0, 2.0), "5s", 5_000), None);
        assert_eq!(fold(&mut open, &trade(3_000, 9.0, 1.0), "5s", 5_000), None);
        let closed = fold(&mut open, &trade(5_000, 11.0, 1.0), "5s", 5_000).expect("closes at 5 s");
        assert_eq!(
            (closed.t, closed.o, closed.h, closed.l, closed.c, closed.v, closed.n, closed.x),
            (0, 10.0, 12.0, 9.0, 9.0, 4.0, 3, true)
        );
        let next = open.clone().expect("opened the next one");
        assert_eq!((next.t, next.o, next.n, next.x), (5_000, 11.0, 1, false));
        assert_eq!(fold(&mut open, &trade(4_000, 1.0, 1.0), "5s", 5_000), None, "late trade is dropped");
        assert_eq!(open.as_ref().map(|c| c.n), Some(1));

        open.as_mut().expect("open").x = true; // closed on time by the tick
        assert_eq!(fold(&mut open, &trade(6_000, 1.0, 1.0), "5s", 5_000), None, "a closed candle stays closed");
        assert_eq!(open.as_ref().map(|c| c.n), Some(1));
        assert_eq!(fold(&mut open, &trade(10_000, 2.0, 1.0), "5s", 5_000), None, "closed once, not again");
        assert_eq!(open.as_ref().map(|c| (c.t, c.x)), Some((10_000, false)));
    }
}
