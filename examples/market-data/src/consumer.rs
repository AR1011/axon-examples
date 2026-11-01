//! The consumer node: one actor owns the Binance socket and publishes each trade to `trades/<symbol>`.
//! Connecting is a job; the connected socket's halves are attached as a source and a sink, so the actor owns
//! it, its frames arrive as messages and it can ping.
use std::error::Error;
use std::time::Duration;

use axon::{Actor, Context, JobHandle, SinkEvent, SinkHandle};
use axon_stream::{DeliveryPolicy, StreamNode};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{SYMBOLS, Trade, now_ms, trades_key};

const RECONNECT: Duration = Duration::from_secs(2);
/// Keeps frames coming when the market is quiet: Binance answers each ping with a pong.
const PING: Duration = Duration::from_secs(10);
/// Nothing received for this long means the connection is dead. TCP may never say so: a socket that is only
/// read from, after the network changed under it, stays established and silent.
const DEADLINE: Duration = Duration::from_secs(30);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Frame = Result<Message, tungstenite::Error>;

pub async fn run(node: &crate::Node) -> Result<(), Box<dyn Error>> {
    for symbol in SYMBOLS {
        node.streams.open_stream(&trades_key(symbol), now_ms(), DeliveryPolicy::ReplayRequired).await?;
    }
    node.system.spawn(Binance { streams: node.streams.clone(), socket: None }).name("binance").start()?;
    Ok(())
}

enum Msg {
    Connected(Result<Box<Socket>, String>),
    Frame(Option<Frame>),
    Socket(SinkEvent<tungstenite::Error>),
    Tick,
    Connect,
}

struct Binance {
    streams: StreamNode,
    /// The read half's job, the write half, and when a frame last arrived.
    socket: Option<(JobHandle, SinkHandle<Message>, Duration)>,
}

impl Actor for Binance {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        cx.schedule_every(PING, || Msg::Tick).map_err(|e| format!("{e:?}"))?;
        self.connect(cx)
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if let (Msg::Frame(Some(Ok(_))), Some((_, _, last))) = (&msg, &mut self.socket) {
            *last = cx.now();
        }
        match msg {
            Msg::Connected(Ok(socket)) => {
                println!("consumer: connected to binance for {}", SYMBOLS.join(" "));
                let (write, read) = socket.split();
                let read = cx.attach(read, Msg::Frame).map_err(|e| format!("{e:?}"))?;
                let write = cx.attach_sink(write, 1, Msg::Socket).map_err(|e| format!("{e:?}"))?;
                self.socket = Some((read, write, cx.now()));
            }
            Msg::Connected(Err(why)) => self.retry(format!("connect failed ({why})"), cx)?,
            Msg::Frame(Some(Ok(Message::Text(text)))) => {
                let Ok(Envelope { data }) = serde_json::from_str::<Envelope>(&text) else { return Ok(()) };
                let (Ok(p), Ok(q)) = (data.p.parse(), data.q.parse()) else { return Ok(()) };
                let trade = Trade { s: data.s, t: data.t, p, q, b: !data.m };
                let record = Bytes::from(serde_json::to_vec(&trade).map_err(|e| e.to_string())?);
                if let Err(e) = self.streams.publish(&trades_key(&trade.s), vec![record]) {
                    eprintln!("consumer: publish {}: {e:?}", trade.s);
                }
            }
            // Pings are answered by the socket itself while it is read.
            Msg::Frame(Some(Ok(Message::Close(_)))) | Msg::Frame(None) => self.lost("closed by binance".into(), cx)?,
            Msg::Frame(Some(Err(e))) => self.lost(e.to_string(), cx)?,
            Msg::Socket(SinkEvent::Closed(Err(e))) => self.lost(e.to_string(), cx)?,
            // `Closed(Ok)` only follows a close, and the only one is the shutdown's drain.
            Msg::Frame(Some(Ok(_))) | Msg::Socket(_) => {}
            Msg::Tick => {
                let Some((_, write, last)) = &self.socket else { return Ok(()) };
                if cx.now().saturating_sub(*last) > DEADLINE {
                    self.lost(format!("nothing received in {DEADLINE:?}"), cx)?;
                } else {
                    // Full: the last ping is still unwritten, which the deadline covers.
                    drop(write.try_send(Message::Ping(Bytes::new())));
                }
            }
            Msg::Connect => self.connect(cx)?,
        }
        Ok(())
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Connected(_) => "connected",
            Msg::Frame(_) => "frame",
            Msg::Socket(_) => "socket",
            Msg::Tick => "tick",
            Msg::Connect => "connect",
        }
    }
}

impl Binance {
    fn connect(&self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let connected =
            async { Msg::Connected(tokio::time::timeout(DEADLINE, open()).await.unwrap_or(Err("timed out".into()))) };
        cx.submit_future(connected).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn lost(&mut self, why: String, cx: &mut Context<'_, Self>) -> Result<(), String> {
        // A close frame and the end of the source both report the same loss; reconnect once.
        let Some((read, write, _)) = self.socket.take() else { return Ok(()) };
        read.cancel();
        write.cancel();
        self.retry(format!("disconnected ({why})"), cx)
    }

    fn retry(&self, why: String, cx: &mut Context<'_, Self>) -> Result<(), String> {
        eprintln!("consumer: binance {why}, reconnecting in {RECONNECT:?}");
        cx.schedule_after(RECONNECT, Msg::Connect).map(drop).map_err(|e| format!("{e:?}"))
    }
}

#[derive(Deserialize)]
struct Envelope {
    data: AggTrade,
}

// {"e":"aggTrade","E":1738661434388,"s":"BTCUSDT","a":1,"p":"97000.10","q":"0.010","f":1,"l":1,"T":1738661434380,"m":false}
#[derive(Deserialize)]
struct AggTrade {
    s: String,
    p: String,
    q: String,
    #[serde(rename = "T")]
    t: u64,
    m: bool,
}

/// A connected, subscribed socket. The documented `/stream?streams=` form connected but delivered nothing
/// when this was written; the path the production consumer uses, with an explicit SUBSCRIBE, does.
async fn open() -> Result<Box<Socket>, String> {
    let (mut ws, _) =
        tokio_tungstenite::connect_async("wss://fstream.binance.com/market/stream").await.map_err(|e| e.to_string())?;
    let streams: Vec<String> = SYMBOLS.iter().map(|s| format!("{}@aggTrade", s.to_lowercase())).collect();
    let subscribe = serde_json::json!({ "method": "SUBSCRIBE", "params": streams, "id": 1 });
    ws.send(Message::text(subscribe.to_string())).await.map_err(|e| e.to_string())?;
    Ok(Box::new(ws))
}
