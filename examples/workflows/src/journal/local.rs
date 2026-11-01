//! One replica in this process, called without a network: the embedded tests' journal, R=1.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use axon::files::{FileFuture, Files, LogConfig};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use super::client::{Probe, Tally, Transport};
use super::core::{Answer, Handled, Replica, Request};

type Waiter = oneshot::Sender<Answer>;

#[derive(Clone)]
pub struct LocalJournal {
    replica: Arc<Mutex<Replica<Waiter>>>,
}

fn lock(replica: &Mutex<Replica<Waiter>>) -> MutexGuard<'_, Replica<Waiter>> {
    replica.lock().unwrap_or_else(PoisonError::into_inner)
}

impl LocalJournal {
    pub fn open(files: Arc<dyn Files>, cfg: LogConfig) -> FileFuture<LocalJournal> {
        let open = Replica::open(files, cfg, None, axon::trace::unix_micros());
        Box::pin(async move { Ok(LocalJournal { replica: Arc::new(Mutex::new(open.await?)) }) })
    }
}

impl Transport for LocalJournal {
    fn replicas(&self) -> usize {
        1
    }

    /// Handles the request, then writes until nothing waits: a call that finds a write out leaves its own to the
    /// call that made that write, which writes again once it lands.
    fn call(&self, _: usize, request: Request) -> BoxFuture<'static, Result<Answer, String>> {
        let replica = self.replica.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            let handled = lock(&replica).handle(request, tx);
            match handled {
                Handled::Now(tx, answer) => drop(tx.send(answer)),
                Handled::Queued => {}
                Handled::Read(tx, read) => drop(tx.send(read.await.map_err(|e| e.to_string())?)),
            }
            loop {
                let write = lock(&replica).take_write(axon::trace::unix_micros() / 1_000);
                let Some(write) = write else { break };
                let written = write.await;
                for (tx, answer) in lock(&replica).written(written) {
                    drop(tx.send(answer));
                }
            }
            rx.await.map_err(|_| "the replica dropped the call".to_string())
        })
    }

    fn probe(&self, run: Bytes) -> BoxFuture<'static, Result<Probe, String>> {
        let call = self.call(0, Request::Probe { run });
        Box::pin(async move { Tally::new(1).add(Some(call.await?)).unwrap_or_else(|| Err("no answer".into())) })
    }
}
