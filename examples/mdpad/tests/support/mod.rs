//! What the tests share. `CutFiles` stands for a disk that loses what was not synced at a crash: `MemoryFiles` alone
//! makes each write durable as it is called.
#![allow(dead_code)]

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{FileFuture, FileOp, Files, MemoryFiles};

#[derive(Default)]
struct Cut {
    /// The process crashed: nothing it starts or has in flight lands.
    dead: bool,
    /// Each write lands this long after it starts, so a crash can come between (tokio tests only).
    delay: Option<Duration>,
    failing: bool,
    in_flight: Vec<(u64, Vec<FileOp>)>,
    next: u64,
}

/// Files whose writes land only when their future is polled to the end, and a crash loses those in flight: one of them
/// may land torn.
#[derive(Clone, Default)]
pub struct CutFiles {
    inner: MemoryFiles,
    cut: Arc<Mutex<Cut>>,
}

impl CutFiles {
    pub fn new(delay: Option<Duration>) -> Self {
        let files = Self::default();
        files.cut.lock().unwrap().delay = delay;
        files
    }

    /// The process stops: what is in flight is lost, but for the first write's first op, which lands cut in half when
    /// `tear` is set. Nothing this handle writes lands after.
    pub fn crash(&self, tear: bool) {
        let mut cut = self.cut.lock().unwrap();
        cut.dead = true;
        let torn = std::mem::take(&mut cut.in_flight).into_iter().next().and_then(|(_, ops)| ops.into_iter().next());
        drop(cut);
        let half = |b: &bytes::Bytes| b.slice(..b.len() / 2);
        let op = match torn.filter(|_| tear) {
            Some(FileOp::Append(name, b)) => FileOp::Append(name, half(&b)),
            Some(FileOp::Write(name, offset, b)) => FileOp::Write(name, offset, half(&b)),
            _ => return,
        };
        // MemoryFiles applies as it is called.
        drop(self.inner.apply(vec![op]));
    }

    /// The same files for the next process.
    pub fn reopen(&self) -> Self {
        let next = Self { inner: self.inner.clone(), cut: Default::default() };
        next.cut.lock().unwrap().delay = self.cut.lock().unwrap().delay;
        next
    }

    /// Each write lands `delay` after it starts from now on.
    pub fn set_delay(&self, delay: Option<Duration>) {
        self.cut.lock().unwrap().delay = delay;
    }

    /// Every write fails until this is set back.
    pub fn set_failing(&self, failing: bool) {
        self.cut.lock().unwrap().failing = failing;
    }
}

impl Files for CutFiles {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.inner.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.inner.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let (inner, cut) = (self.inner.clone(), self.cut.clone());
        Box::pin(async move {
            let (id, delay) = {
                let mut c = cut.lock().unwrap();
                if c.failing || c.dead {
                    return Err(io::Error::other("the disk fails"));
                }
                let id = c.next;
                c.next += 1;
                c.in_flight.push((id, ops.clone()));
                (id, c.delay)
            };
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let dead = {
                let mut c = cut.lock().unwrap();
                c.in_flight.retain(|(i, _)| *i != id);
                c.dead
            };
            if dead {
                return Err(io::Error::other("lost in a crash"));
            }
            inner.apply(ops).await
        })
    }
}

/// The signing key of every test node, so a restarted node takes the tokens of the one before.
pub const KEY: [u8; 32] = [7; 32];

/// A node on these files, with `tweak` applied to its config.
pub async fn start(
    docs: &CutFiles,
    accounts: &CutFiles,
    tweak: impl FnOnce(&mut mdpad::node::NodeConfig),
) -> mdpad::node::Node {
    let mut cfg = mdpad::node::NodeConfig::new(Arc::new(docs.clone()), Arc::new(accounts.clone()), KEY.to_vec());
    tweak(&mut cfg);
    mdpad::node::Node::start(cfg).await.expect("the node starts")
}

/// A user with the password `pw-<name>`, signed in.
pub async fn user(node: &mdpad::node::Node, name: &str) -> mdpad::client::Api {
    use mdpad::{accounts::Request, core::accounts::Event, keeper::Msg};
    let event = Event::Add { name: name.into(), hash: mdpad::auth::hash_password(&format!("pw-{name}")).unwrap() };
    let recorded = node.accounts().ask(|reply| Msg::Request(Request::Record { event: event.clone(), reply })).await;
    recorded.expect("the accounts answer").expect("a new user");
    signed_in(node, name).await
}

pub async fn signed_in(node: &mdpad::node::Node, name: &str) -> mdpad::client::Api {
    mdpad::client::Api::login(node.http(), name, &format!("pw-{name}")).await.expect("signs in")
}

/// The close code the client gets within `within`, reading and acting on what comes before it.
pub async fn closed(client: &mut mdpad::client::Client, within: Duration) -> u16 {
    use mdpad::client::Stop;
    match client.run_until(within, |_| false).await {
        Err(Stop::Closed(code)) => code,
        other => panic!("not closed within {within:?}: {other:?}"),
    }
}

/// Joins as a client does, again after a close it may retry, until nothing is unsaved and the state is the server's.
/// A 4404 is retried too: a socket can open before its document's first state is written.
pub async fn settle(client: &mut mdpad::client::Client, within: Duration) {
    use mdpad::client::Stop;
    use mdpad::wire::close;
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!left.is_zero(), "not settled within {within:?}");
        if !client.core.is_connected() && client.connect().await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        match client.run_until(left, mdpad::client::Client::synced).await {
            Ok(()) => return,
            Err(Stop::Rejoin | Stop::Closed(1006 | close::UNAVAILABLE | close::NOT_FOUND)) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(other) => panic!("{other:?}"),
        }
    }
}
