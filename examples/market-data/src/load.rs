//! Demo load: `sessions` clients, each on one symbol's trades and one timeframe's candles. Prints the
//! message rate every second and the newest record the first session received.
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use crate::SYMBOLS;

const TIMEFRAMES: [&str; 6] = ["1s", "5s", "1m", "5m", "1h", "1d"];

pub async fn run(url: &str, sessions: usize) -> Result<(), Box<dyn Error>> {
    let received = Arc::new(AtomicU64::new(0));
    let latest = Arc::new(Mutex::new(String::new()));
    for i in 0..sessions {
        let (symbol, tf) = (SYMBOLS[i % SYMBOLS.len()], TIMEFRAMES[i % TIMEFRAMES.len()]);
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
        for req in [
            format!("{{\"op\":\"subscribe\",\"stream\":\"trades\",\"symbol\":\"{symbol}\"}}"),
            format!("{{\"op\":\"subscribe\",\"stream\":\"candles\",\"symbol\":\"{symbol}\",\"tf\":\"{tf}\"}}"),
        ] {
            ws.send(Message::text(req)).await?;
        }
        println!("session {i}: trades:{symbol} and candles:{symbol}:{tf}");
        let (received, latest) = (received.clone(), latest.clone());
        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws.next().await {
                if let Message::Text(text) = msg {
                    received.fetch_add(1, Ordering::Relaxed);
                    if text.starts_with("{\"ok\"") || text.starts_with("{\"error\"") {
                        println!("session {i}: {text}");
                    } else if i == 0 {
                        *latest.lock().unwrap_or_else(|e| e.into_inner()) = text.to_string();
                    }
                }
            }
            eprintln!("session {i}: closed");
        });
    }
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    let mut last = 0;
    loop {
        ticks.tick().await;
        let total = received.load(Ordering::Relaxed);
        println!(
            "{sessions} sessions, {}/s, session 0 last saw: {}",
            total - last,
            latest.lock().unwrap_or_else(|e| e.into_inner())
        );
        last = total;
    }
}
