//! A small RESP client: one connection, commands written and replies read in order, so a caller can pipeline.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};

use crate::resp::{Value, command, parse_reply};
use crate::slot::{SHARDS, tag_for};

pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream, buf: BytesMut::with_capacity(16 << 10) })
    }

    /// Writes one command; its reply comes from a later `read`.
    pub async fn send(&mut self, args: &[&[u8]]) -> io::Result<()> {
        self.stream.write_all(&command(args)).await
    }

    /// Writes bytes as they are: a test's malformed input.
    pub async fn raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await
    }

    /// The next reply. An error if the server closed the connection first.
    pub async fn read(&mut self) -> io::Result<Value> {
        loop {
            match parse_reply(&mut self.buf) {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {}
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.0)),
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }

    pub async fn call(&mut self, args: &[&[u8]]) -> io::Result<Value> {
        self.send(args).await?;
        self.read().await
    }
}

/// Asks every shard once, through `addrs` in turn, until each answers: after this a cluster that has just started
/// has every shard running. How long it took, or the first shard that did not answer within `within`.
pub async fn warm(addrs: &[SocketAddr], within: Duration) -> Result<Duration, String> {
    if addrs.is_empty() {
        return Err("warm needs an address".into());
    }
    let started = Instant::now();
    let deadline = started + within;
    let mut client: Option<Client> = None;
    // Moves on to the next node after each failed connection or call, so one node down does not stop it.
    let mut next = 0;
    for shard in 0..SHARDS {
        let key = format!("kvw:{{{}}}", tag_for(shard));
        let mut last = String::new();
        loop {
            if client.is_none() {
                client = Client::connect(addrs[next % addrs.len()]).await.ok();
                next += 1;
            }
            if let Some(c) = client.as_mut() {
                match timeout(Duration::from_secs(3), c.call(&[b"EXISTS", key.as_bytes()])).await {
                    Ok(Ok(Value::Int(_))) => break,
                    Ok(Ok(other)) => last = format!("{other:?}"),
                    other => {
                        last = format!("{other:?}");
                        client = None;
                    }
                }
            }
            if Instant::now() > deadline {
                return Err(format!("shard {shard} did not answer within {within:?}: {last}"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    Ok(started.elapsed())
}
