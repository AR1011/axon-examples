//! The ws node: one actor per client connection. A session subscribes to `trades/<symbol>` straight from the
//! stream node, and to candles by first asking the cluster for the one sampler of `<symbol>:<tf>` (spawning it
//! on a processor if it does not exist yet), then subscribing to what that sampler publishes.
//!
//! Client → server: {"op":"subscribe"|"unsubscribe","stream":"trades"|"candles","symbol":"BTCUSDT","tf":"5m"}
//! Server → client: stream records as they are, plus {"ok":..} and {"error":..} for requests.
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::time::Duration;

use axon::{Actor, Context, JobHandle, RejectReason, SinkEvent, SinkHandle, StopReason};
use axon_cluster::Placement;
use axon_stream::{StreamEvent, SubscribeOptions, Subscription};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{self, Message, Utf8Bytes};

use crate::processor::{CandlesApi, KIND, candles_key, parse_timeframe};
use crate::{Node, SYMBOLS, trades_key};

/// Records waiting for the socket. A client further behind than this is dropped rather than buffered.
const OUTBOX: usize = 1024;
/// Records handed to the socket's writer at a time; the rest wait in the outbox.
const IN_FLIGHT: usize = 64;
/// How long a client waits for its sampler. A processor that died without leaving holds its leases for Consul's lock
/// delay, which can be longer than the cluster's operation timeout, so this covers it.
const FIND: Duration = Duration::from_secs(30);

pub async fn run(node: &Node, port: u16) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("ws: listening on ws://127.0.0.1:{port}");
    let node = node.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((tcp, _)) => drop(tokio::spawn(session(node.clone(), tcp))),
                Err(e) => eprintln!("ws: accept: {e}"),
            }
        }
    });
    Ok(())
}

/// After the handshake the socket is the session's: `on_start` attaches its two halves.
async fn session(node: Node, tcp: TcpStream) {
    let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else { return };
    let session =
        Session { node: node.clone(), ws: Some(ws), tx: None, outbox: VecDeque::new(), subs: BTreeMap::new() };
    drop(node.system.spawn(session).start());
}

#[derive(Deserialize)]
struct Request {
    op: String,
    stream: String,
    symbol: String,
    tf: Option<String>,
}

enum Msg {
    /// A frame from the client; `None` or an error ends the session.
    Client(Option<Result<Message, tungstenite::Error>>),
    Socket(SinkEvent<tungstenite::Error>),
    /// A sampler was found or started: its current candle and the subscription to what it publishes.
    Candles(String, Result<(Bytes, Subscription), String>),
    Stream(String, Option<StreamEvent>),
}

struct Session {
    node: Node,
    ws: Option<WebSocketStream<TcpStream>>,
    tx: Option<SinkHandle<Message>>,
    outbox: VecDeque<Message>,
    /// Stream name → the job that feeds it. Cancelling the job drops the subscription.
    subs: BTreeMap<String, JobHandle>,
}

impl Actor for Session {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (write, read) = self.ws.take().ok_or("started twice")?.split();
        cx.attach(read, Msg::Client).map_err(|e| format!("{e:?}"))?;
        self.tx = Some(cx.attach_sink(write, IN_FLIGHT, Msg::Socket).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Client(Some(Ok(Message::Text(text)))) => {
                let answer = match serde_json::from_str::<Request>(&text) {
                    Ok(req) => self.handle(req, cx),
                    Err(e) => Err(format!("bad request: {e}")),
                };
                let line = match answer {
                    Ok(name) => json!({ "ok": name }),
                    Err(e) => json!({ "error": e }),
                };
                self.push(Bytes::from(line.to_string()), cx);
            }
            // tungstenite answers pings itself.
            Msg::Client(Some(Ok(_))) | Msg::Socket(SinkEvent::Ready) => {}
            Msg::Client(_) | Msg::Socket(SinkEvent::Closed(_)) => cx.stop(),
            Msg::Candles(name, found) => {
                if !self.subs.contains_key(&name) {
                    return Ok(()); // unsubscribed while the sampler was being found
                }
                match found {
                    Ok((latest, sub)) => {
                        self.push(latest, cx);
                        let job = self.attach(&name, sub, cx)?;
                        self.subs.entry(name).and_modify(|entry| *entry = job);
                    }
                    Err(e) => self.ended(name, e, cx),
                }
            }
            Msg::Stream(_, Some(StreamEvent::Batch(batch))) => {
                for record in batch.records() {
                    self.push(record.clone(), cx);
                }
            }
            Msg::Stream(name, Some(event)) if event.is_terminal() => self.ended(name, format!("{event:?}"), cx),
            Msg::Stream(name, None) => self.ended(name, "subscription dropped".into(), cx),
            Msg::Stream(..) => {}
        }
        Ok(())
    }

    /// Everything this turn queued goes to the socket's writer at once; what it refuses waits for `Ready`.
    fn on_turn_end(&mut self, _: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(tx) = &self.tx else { return Ok(()) };
        while let Some(frame) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(frame) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message);
                }
                break;
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, _: &StopReason) {
        self.subs.values().for_each(JobHandle::cancel);
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Client(_) => "client",
            Msg::Socket(_) => "socket",
            Msg::Candles(..) => "candles",
            Msg::Stream(..) => "stream",
        }
    }
}

impl Session {
    fn push(&mut self, record: Bytes, cx: &mut Context<'_, Self>) {
        if self.outbox.len() >= OUTBOX {
            eprintln!("ws: dropping a slow client");
            cx.stop();
        } else if let Ok(text) = Utf8Bytes::try_from(record) {
            self.outbox.push_back(Message::Text(text));
        }
    }

    /// The subscription becomes the session's: its events are messages, and the handle in `subs` drops it.
    fn attach(&self, name: &str, sub: Subscription, cx: &mut Context<'_, Self>) -> Result<JobHandle, String> {
        let tag = name.to_owned();
        cx.attach(sub, move |event| Msg::Stream(tag.clone(), event)).map_err(|e| format!("{e:?}"))
    }

    /// A live subscription follows its key through restarts, so an end is this session's own doing (it fell
    /// too far behind): the client hears about it and decides whether to subscribe again.
    fn ended(&mut self, name: String, why: String, cx: &mut Context<'_, Self>) {
        if self.subs.remove(&name).is_some() {
            self.push(Bytes::from(json!({ "error": format!("{name}: ended ({why})") }).to_string()), cx);
        }
    }

    fn handle(&mut self, req: Request, cx: &mut Context<'_, Self>) -> Result<String, String> {
        if !SYMBOLS.contains(&req.symbol.as_str()) {
            return Err(format!("unknown symbol {}", req.symbol));
        }
        let name = match (req.stream.as_str(), &req.tf) {
            ("trades", _) => format!("trades:{}", req.symbol),
            ("candles", Some(tf)) if parse_timeframe(tf).is_some() => format!("candles:{}:{tf}", req.symbol),
            ("candles", _) => return Err("candles need a tf between 1s and 1y".into()),
            (other, _) => return Err(format!("unknown stream {other}")),
        };
        match req.op.as_str() {
            "unsubscribe" => {
                self.subs.remove(&name).ok_or("not subscribed")?.cancel();
            }
            "subscribe" if self.subs.contains_key(&name) => return Err(format!("already subscribed to {name}")),
            "subscribe" => {
                let (node, key) = (self.node.clone(), (req.symbol.clone(), req.tf.clone().unwrap_or_default()));
                if req.stream == "trades" {
                    let sub = self.node.streams.subscribe(&trades_key(&key.0), SubscribeOptions::default());
                    let job = self.attach(&name, sub.map_err(|e| format!("{e:?}"))?, cx)?;
                    self.subs.insert(name.clone(), job);
                } else {
                    let found = {
                        let name = name.clone();
                        async move { Msg::Candles(name, candles(&node, &key.0, &key.1).await) }
                    };
                    let job = cx.submit_future(found).map_err(|e| format!("{e:?}"))?;
                    self.subs.insert(name.clone(), job);
                }
            }
            other => return Err(format!("unknown op {other}")),
        }
        Ok(name)
    }
}

/// The sampler for this symbol and timeframe, wherever it runs, started if it is not. Its open candle
/// comes back as the first record, so the client is not left with nothing until the next trade.
async fn candles(node: &Node, symbol: &str, tf: &str) -> Result<(Bytes, Subscription), String> {
    let placement = Placement::default().require("role", "processor");
    let key = format!("{symbol}:{tf}");
    let latest = node
        .cluster
        .keyed::<CandlesApi>(KIND, &key, Bytes::new(), &placement)
        .request_until((), tokio::time::Instant::now() + FIND)
        .await
        .map_err(|e| format!("sampler {key}: {e}"))?;
    let sub =
        node.streams.subscribe(&candles_key(symbol, tf), SubscribeOptions::default()).map_err(|e| format!("{e:?}"))?;
    Ok((latest, sub))
}
