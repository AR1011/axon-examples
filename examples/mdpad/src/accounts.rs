//! The accounts actor: the users in the `accounts` key of their own journal. The command line writes the same key
//! while the server is stopped (the directory's lock says so), through [`offline`].

use std::sync::Arc;

use axon::files::{Address, Files, Log};
use axon::{ReplyTo, SupervisedHandle};

use crate::core::accounts::{Accounts, Event, Listed, User};
use crate::core::book::Kept;
use crate::core::pipeline;
use crate::journal;
use crate::keeper::{Answer, Keep, Msg};

pub enum Request {
    Get {
        name: String,
        reply: ReplyTo<Option<User>>,
    },
    List {
        reply: ReplyTo<Vec<Listed>>,
    },
    /// Answered once durable, or with why it may not be applied.
    Record {
        event: Event,
        reply: ReplyTo<Result<(), String>>,
    },
}

pub type Handle = SupervisedHandle<Msg<Request>>;

pub struct AccountsKeep;

pub const KEY: &str = "accounts";

impl Keep for AccountsKeep {
    type State = Accounts;
    type Request = Request;
    const KEY: &'static str = KEY;

    fn handle(&mut self, kept: &mut Kept<Accounts, Answer>, request: Request) {
        match request {
            Request::Get { name, reply } => drop(reply.reply(kept.state.users.get(&name).cloned())),
            Request::List { reply } => {
                let users = kept.state.users.iter();
                let listed =
                    users.map(|(name, u)| Listed { name: name.clone(), disabled: u.disabled, operator: u.operator });
                drop(reply.reply(listed.collect()))
            }
            Request::Record { event, reply } => match kept.state.check(&event) {
                Ok(()) => kept.record(event, Some(Box::new(move || drop(reply.reply(Ok(())))))),
                Err(why) => drop(reply.reply(Err(why))),
            },
        }
    }
}

/// Reads the accounts and applies `event`, if any, with no server running: it holds the directory's lock meanwhile.
pub async fn offline(files: Arc<dyn Files>, event: Option<Event>) -> Result<Accounts, String> {
    let mut log = Log::open(files, journal::small_config(KEY)).await.map_err(|e| format!("accounts: {e}"))?;
    let epoch = log.key(KEY.as_bytes()).map_or(1, |k| k.fence + 1);
    log.fence(KEY.as_bytes(), epoch).map_err(|e| e.to_string())?;
    let entries = log.read(KEY.as_bytes(), Address::default(), usize::MAX).await.map_err(|e| e.to_string())?;
    let mut kept = Kept::<Accounts, ()>::load(epoch, entries)?;
    if let Some(event) = event {
        kept.state.check(&event)?;
        kept.record(event, None);
        let ops = kept.take().expect("a record");
        pipeline::append(&mut log, KEY.as_bytes(), epoch, ops).map_err(|e| e.to_string())?;
    }
    while let Some(write) = log.take_write(axon::trace::unix_micros() / 1000) {
        let written = write.await;
        let failed = written.as_ref().err().map(|e| e.to_string());
        log.written(written);
        if let Some(e) = failed {
            return Err(format!("accounts: {e}"));
        }
    }
    Ok(kept.state)
}
