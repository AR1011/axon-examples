//! The actor that keeps a [`Book`] in a key of the node's journal: it loads the key before any request, applies each
//! request's events at once, writes them one write at a time, and answers once they are durable. A refused write ends
//! it, and its supervisor starts it again from what the journal holds.

use std::time::Duration;

use axon::files::LogError;
use axon::{Actor, ActorHandle, Backoff, Context, RequestError, RestartPolicy};
use bytes::Bytes;

use crate::core::book::{Book, Kept};
use crate::journal::{Loaded, ToJournal, job_error};

/// Called once what the request applied is durable.
pub type Answer = Box<dyn FnOnce() + Send>;

pub trait Keep: Send + 'static {
    type State: Book;
    type Request: Send + 'static;
    const KEY: &'static str;
    /// The state as the journal held it, before any request.
    fn loaded(&mut self, _state: &Self::State) {}
    fn handle(&mut self, kept: &mut Kept<Self::State, Answer>, request: Self::Request);
}

pub enum Msg<R> {
    Loaded(Result<Loaded, RequestError>),
    Request(R),
    Written(Result<Result<(), LogError>, RequestError>),
}

pub struct Keeper<K: Keep> {
    keep: K,
    journal: ActorHandle<ToJournal>,
    kept: Option<Kept<K::State, Answer>>,
}

impl<K: Keep> Keeper<K> {
    pub fn new(keep: K, journal: ActorHandle<ToJournal>) -> Self {
        Self { keep, journal, kept: None }
    }
}

/// Three restarts a minute: a disk that refuses for longer leaves the keeper stopped, and its callers refused.
pub fn policy() -> RestartPolicy {
    RestartPolicy {
        max_restarts: 3,
        within: Duration::from_secs(60),
        backoff: Backoff { initial: Duration::from_millis(50), max: Duration::from_secs(1), jitter: 0.2 },
        ..RestartPolicy::default()
    }
}

impl<K: Keep> Actor for Keeper<K> {
    type Message = Msg<K::Request>;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let journal = self.journal.clone();
        let load = async move {
            Msg::Loaded(journal.ask(|r| ToJournal::Load(Bytes::from_static(K::KEY.as_bytes()), r)).await)
        };
        let job = cx.submit_future(load).map_err(job_error)?;
        cx.pause_until(&job).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Self::Message, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let (epoch, entries) = loaded.map_err(|e| format!("{e:?}"))?.map_err(|e| e.to_string())?;
                let kept = Kept::load(epoch, entries)?;
                self.keep.loaded(&kept.state);
                self.kept = Some(kept);
            }
            Msg::Request(request) => {
                let kept = self.kept.as_mut().ok_or("a request before the load")?;
                self.keep.handle(kept, request);
            }
            Msg::Written(Ok(Ok(()))) => {
                let kept = self.kept.as_mut().ok_or("a write before the load")?;
                for answer in kept.written() {
                    answer();
                }
            }
            Msg::Written(failed) => return Err(format!("{} write: {failed:?}", K::KEY)),
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(kept) = self.kept.as_mut() else { return Ok(()) };
        // Nothing while a write is out.
        let Some(ops) = kept.take() else { return Ok(()) };
        let (journal, epoch) = (self.journal.clone(), kept.epoch);
        let key = Bytes::from_static(K::KEY.as_bytes());
        // No deadline: a disk that fails holds the write until it lands, and the journal tries it again.
        let write = journal.address.request(None, |r| ToJournal::Write(key, epoch, ops, r));
        let write = async move { Msg::Written(write.await) };
        cx.submit_future(write).map_err(job_error)?;
        Ok(())
    }
}
