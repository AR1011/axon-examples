//! A websocket client, and what it checks: every message once, in order, or an explicit break.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::proto::{ClientFrame, ServerFrame};

pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    pub async fn connect(addr: SocketAddr, user: &str) -> Result<Self, String> {
        let url = format!("ws://{addr}/ws?user={user}");
        let (ws, _) = tokio_tungstenite::connect_async(url).await.map_err(|e| e.to_string())?;
        Ok(Self { ws })
    }

    pub async fn send(&mut self, frame: &ClientFrame) -> Result<(), String> {
        let text = serde_json::to_string(frame).map_err(|e| e.to_string())?;
        self.ws.send(Message::text(text)).await.map_err(|e| e.to_string())
    }

    /// The next frame; `None` once the connection is gone.
    pub async fn next(&mut self) -> Option<ServerFrame> {
        loop {
            match self.ws.next().await? {
                Ok(Message::Text(text)) => match serde_json::from_str(&text) {
                    Ok(frame) => return Some(frame),
                    Err(e) => return Some(ServerFrame::Error { why: format!("unparsable frame: {e}") }),
                },
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }
}

/// What one client saw of one room.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomLog {
    /// The last message held, as `(generation, sequence)`: what a reconnect resumes after.
    pub cursor: Option<(u64, u64)>,
    /// Contiguous runs received: `(generation, first, last)`.
    pub runs: Vec<(u64, u64, u64)>,
    pub received: u64,
    /// A message at or before the cursor: must stay 0.
    pub duplicates: u64,
    /// A message past the cursor + 1 in the same generation with no reset between: must stay 0.
    pub gaps: u64,
    /// Explicit breaks: `reset` frames, and a `ready` or message in a new generation.
    pub breaks: u64,
}

impl RoomLog {
    pub fn holds(&self, generation: u64, sequence: u64) -> bool {
        self.runs.iter().any(|&(g, first, last)| g == generation && (first..=last).contains(&sequence))
    }

    fn take(&mut self, generation: u64, sequence: u64) {
        match self.cursor {
            Some((g, s)) if g == generation && sequence <= s => return self.duplicates += 1,
            Some((g, s)) if g == generation && sequence > s + 1 => self.gaps += 1,
            Some((g, _)) if g != generation => self.breaks += 1,
            _ => {}
        }
        match self.runs.last_mut() {
            Some((g, _, last)) if *g == generation && *last + 1 == sequence => *last = sequence,
            _ => self.runs.push((generation, sequence, sequence)),
        }
        self.cursor = Some((generation, sequence));
        self.received += 1;
    }
}

#[derive(Debug, Default)]
pub struct Tracker {
    pub rooms: BTreeMap<String, RoomLog>,
}

impl Tracker {
    pub fn apply(&mut self, frame: &ServerFrame) {
        match frame {
            ServerFrame::Ready { room, at } => {
                let log = self.rooms.entry(room.clone()).or_default();
                match (log.cursor, at) {
                    (None, _) => log.cursor = *at,
                    (Some((g, _)), Some((generation, _))) if g != *generation => {
                        log.breaks += 1;
                        log.cursor = *at;
                    }
                    _ => {}
                }
            }
            ServerFrame::Msg { room, generation, first, m } => {
                let log = self.rooms.entry(room.clone()).or_default();
                for sequence in *first..*first + m.len() as u64 {
                    log.take(*generation, sequence);
                }
            }
            ServerFrame::Reset { room, .. } => {
                let log = self.rooms.entry(room.clone()).or_default();
                log.breaks += 1;
                log.cursor = None;
            }
            _ => {}
        }
    }
}
