//! A small HTTP/1.1 GET client, enough for the generated site: one request per connection, a bounded body.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const MAX_BODY: u64 = 256 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum Fetch {
    /// A 2xx answer, with the links found in its body.
    Page(Vec<String>),
    /// An answer that will not change if asked again: 4xx, or a body that is not a page.
    Gone(u16),
    /// A 5xx, a timeout or a connection error: worth asking again later.
    Transient(String),
}

/// `host` and `path` of an `http://` URL.
pub fn split(url: &str) -> Option<(&str, &str)> {
    let rest = url.strip_prefix("http://")?;
    Some(match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    })
}

pub fn links(body: &str) -> Vec<String> {
    body.split("href=\"").skip(1).filter_map(|s| s.split('"').next()).map(str::to_owned).collect()
}

/// Every host resolves to `server`, as if a hosts file pointed the site's names at it.
pub async fn get(server: SocketAddr, url: &str, deadline: Duration) -> Fetch {
    match tokio::time::timeout(deadline, exchange(server, url)).await {
        Ok(Ok(fetch)) => fetch,
        Ok(Err(e)) => Fetch::Transient(e.to_string()),
        Err(_) => Fetch::Transient("timed out".into()),
    }
}

async fn exchange(server: SocketAddr, url: &str) -> std::io::Result<Fetch> {
    let Some((host, path)) = split(url) else { return Ok(Fetch::Gone(0)) };
    let mut stream = TcpStream::connect(server).await?;
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut raw = Vec::new();
    stream.take(MAX_BODY).read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    let status = text.split(' ').nth(1).and_then(|s| s.parse::<u16>().ok()).unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    Ok(match status {
        200..=299 => Fetch::Page(links(body)),
        500..=599 => Fetch::Transient(format!("status {status}")),
        _ => Fetch::Gone(status),
    })
}
