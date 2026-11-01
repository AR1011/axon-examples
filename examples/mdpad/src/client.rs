//! A Rust client of the websocket protocol, running [`ClientCore`]: what `mdpad load` and the integration tests drive.
//! It holds one connection; the caller joins again after a close or a gap, and the core keeps the unsaved edits.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::crdt;
use crate::follow::{ClientAct, ClientCore};
use crate::wire::{ClientFrame, ServerFrame};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Who the client is: a signed-in user's token, or a guest's.
#[derive(Clone, Debug)]
pub enum Credential {
    User(String),
    Guest(String),
}

/// Why `run_until` returned without its condition.
#[derive(Debug, PartialEq, Eq)]
pub enum Stop {
    /// The server closed with this code, or the connection broke (1006).
    Closed(u16),
    /// A gap or a new generation it did not follow: join again.
    Rejoin,
    Timeout,
}

pub struct Client {
    pub core: ClientCore,
    /// The node it joins: a test moves it to a restarted one.
    pub http: SocketAddr,
    id: String,
    credential: Credential,
    ws: Option<Ws>,
    /// The marks the server said are saved.
    pub saved: BTreeSet<u64>,
    retry_at: Option<Instant>,
    /// Bytes this client received and sent, frames included.
    pub received: u64,
    pub sent: u64,
}

impl Client {
    pub fn new(http: SocketAddr, id: &str, credential: Credential) -> Self {
        let client_id = axon::trace::random_u64() & 0xFFFF_FFFE;
        Self {
            core: ClientCore::new(client_id),
            http,
            id: id.into(),
            credential,
            ws: None,
            saved: BTreeSet::new(),
            retry_at: None,
            received: 0,
            sent: 0,
        }
    }

    /// Opens a socket and says hello; the state comes in later frames.
    pub async fn connect(&mut self) -> Result<(), String> {
        let (name, value) = match &self.credential {
            Credential::User(token) => ("token", token),
            Credential::Guest(token) => ("guest", token),
        };
        let url = format!("ws://{}/api/docs/{}/ws?{name}={value}", self.http, self.id);
        // true: TCP_NODELAY, as on the server's side (`serve_http`).
        let (ws, _) = tokio_tungstenite::connect_async_with_config(url, None, true).await.map_err(|e| e.to_string())?;
        (self.ws, self.retry_at) = (Some(ws), None);
        let hello = self.core.connect();
        self.send(hello).await
    }

    pub async fn disconnect(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
        self.core.disconnected();
    }

    pub async fn send(&mut self, frame: ClientFrame) -> Result<(), String> {
        self.send_raw(frame.encode()).await
    }

    /// Any bytes as one binary frame: a test's malformed or forbidden frame.
    pub async fn send_raw(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        let ws = self.ws.as_mut().ok_or("not connected")?;
        self.sent += bytes.len() as u64;
        ws.send(Message::Binary(bytes.into())).await.map_err(|e| e.to_string())
    }

    /// Inserts `text` at `at` here, to be saved under `mark`.
    pub async fn insert(&mut self, at: u32, text: &str, mark: u64) -> Result<(), String> {
        let update = crdt::insert(&self.core.doc, at.min(crdt::len(&self.core.doc)), text);
        self.edit(update, mark).await
    }

    /// Sends an update made here, to be saved under `mark`.
    pub async fn edit(&mut self, update: Vec<u8>, mark: u64) -> Result<(), String> {
        match self.core.local(update, mark) {
            Some(frame) if self.ws.is_some() => self.send(frame).await,
            _ => Ok(()),
        }
    }

    pub fn text(&self) -> String {
        crdt::text(&self.core.doc)
    }

    /// Reads frames and acts on them until `done` holds, the connection ends, or `within` passes.
    pub async fn run_until(&mut self, within: Duration, mut done: impl FnMut(&Self) -> bool) -> Result<(), Stop> {
        let deadline = Instant::now() + within;
        loop {
            if done(self) {
                return Ok(());
            }
            self.step(deadline).await?;
        }
    }

    /// One frame, or a due retry, or the deadline.
    pub async fn step(&mut self, deadline: Instant) -> Result<(), Stop> {
        let wake = self.retry_at.map_or(deadline, |at| at.min(deadline));
        let ws = self.ws.as_mut().ok_or(Stop::Closed(1006))?;
        let message = match tokio::time::timeout_at(wake, ws.next()).await {
            Err(_) if self.retry_at.is_some_and(|at| at <= Instant::now()) => {
                self.retry_at = None;
                return match self.core.retry() {
                    Some(frame) => self.send(frame).await.map_err(|_| self.broken()),
                    None => Ok(()),
                };
            }
            Err(_) => return Err(Stop::Timeout),
            Ok(None) | Ok(Some(Err(_))) => return Err(self.broken()),
            Ok(Some(Ok(message))) => message,
        };
        match message {
            Message::Binary(bytes) => {
                self.received += bytes.len() as u64;
                let frame = ServerFrame::decode(&bytes).map_err(|_| self.broken())?;
                for act in self.core.frame(frame) {
                    match act {
                        ClientAct::Send(frame) => self.send(frame).await.map_err(|_| self.broken())?,
                        ClientAct::Rejoin => {
                            self.disconnect().await;
                            return Err(Stop::Rejoin);
                        }
                        ClientAct::RetryAfter(ms) => self.retry_at = Some(Instant::now() + Duration::from_millis(ms)),
                        ClientAct::Saved(marks) => self.saved.extend(marks),
                    }
                }
                Ok(())
            }
            Message::Close(frame) => {
                let code = frame.map_or(1005, |CloseFrame { code, .. }| code.into());
                self.ws = None;
                self.core.disconnected();
                Err(Stop::Closed(code))
            }
            _ => Ok(()),
        }
    }

    fn broken(&mut self) -> Stop {
        self.ws = None;
        self.core.disconnected();
        Stop::Closed(1006)
    }

    /// Joined, the state applied, and nothing unsaved.
    pub fn synced(&self) -> bool {
        self.core.welcome.is_some() && self.core.position.is_some() && self.core.unsaved() == 0
    }
}

/// An HTTP client of the API: what the command line and the tests use.
pub struct Api {
    pub base: String,
    pub http: reqwest::Client,
    pub token: Option<String>,
}

impl Api {
    pub fn new(addr: SocketAddr) -> Self {
        Self { base: format!("http://{addr}"), http: reqwest::Client::new(), token: None }
    }

    pub async fn login(addr: SocketAddr, name: &str, password: &str) -> Result<Self, String> {
        let mut api = Self::new(addr);
        let body = serde_json::json!({ "name": name, "password": password, "token": true });
        let answer = api.call(reqwest::Method::POST, "/api/login", Some(body)).await?;
        api.token = answer["token"].as_str().map(str::to_string);
        api.token.as_ref().ok_or("no token")?;
        Ok(api)
    }

    /// The status and the body's text, whatever the status.
    async fn text(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<(u16, String), String> {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status().as_u16();
        Ok((status, response.text().await.map_err(|e| e.to_string())?))
    }

    /// The status and the JSON body, whatever the status.
    pub async fn raw(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<(u16, serde_json::Value), String> {
        let (status, text) = self.text(method, path, body).await?;
        Ok((status, serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))))
    }

    /// The JSON body of a 2xx answer, or the error's message.
    pub async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        match self.raw(method, path, body).await? {
            (200..=299, body) => Ok(body),
            (status, body) => {
                let (code, message) = (body["error"].as_str().unwrap_or("?"), body["message"].as_str().unwrap_or(""));
                Err(format!("{status} {code} {message}").trim_end().to_string())
            }
        }
    }

    pub async fn create(&self, markdown: &str) -> Result<String, String> {
        let body = serde_json::json!({ "markdown": markdown });
        let answer = self.call(reqwest::Method::POST, "/api/docs", Some(body)).await?;
        answer["id"].as_str().map(str::to_string).ok_or_else(|| "no id".into())
    }

    pub async fn markdown(&self, id: &str) -> Result<String, String> {
        match self.text(reqwest::Method::GET, &format!("/api/docs/{id}/markdown"), None).await? {
            (200, text) => Ok(text),
            (status, text) => Err(format!("{status} {text}")),
        }
    }

    pub fn credential(&self) -> Credential {
        Credential::User(self.token.clone().unwrap_or_default())
    }
}
