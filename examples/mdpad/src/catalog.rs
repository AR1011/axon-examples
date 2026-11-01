//! The home's catalog actor: its rows in the journal's `catalog` key, and the id set the `doc` factory reads.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axon::{ReplyTo, SupervisedHandle};
use ring::rand::{SecureRandom, SystemRandom};

use crate::core::book::Kept;
use crate::core::catalog::{Catalog, Event, Row, RowState, Tab};
use crate::keeper::{Answer, Keep, Msg};

/// The most documents a home keeps. The catalog's checkpoint is one record of the documents' log
/// (`journal::docs_config`): rows of up to 400 bytes on average fit, where a row of the load's is 234. A home at the
/// limit drops the ask for a new one, and the web node asks the next home.
pub const MAX_ROWS: usize = 120_000;

/// Every document id of this home and its row's state: the `doc` factory refuses an id that is not here.
pub type Ids = Arc<Mutex<BTreeMap<String, RowState>>>;

pub enum Request {
    /// A new id, answered once its row is durable.
    Create { owner: String, reply: ReplyTo<String> },
    /// Told by a document.
    Doc(Event),
    List {
        user: String,
        tab: Tab,
        /// The last row's edit time and id.
        after: Option<(u64, String)>,
        limit: usize,
        reply: ReplyTo<Vec<(String, Row)>>,
    },
    /// Removes a shared document from the user's list. `false` when it is not shared with them.
    Hide { id: String, user: String, reply: ReplyTo<bool> },
    /// The rows of these ids that are there, and how many documents the home keeps: the admin page's open documents.
    Rows { ids: Vec<String>, reply: ReplyTo<(Vec<(String, Row)>, usize)> },
}

pub type Handle = SupervisedHandle<Msg<Request>>;

pub struct CatalogKeep {
    pub home: String,
    pub ids: Ids,
}

/// `<home>-<26 base32 characters>`: 128 random bits.
pub fn new_id(home: &str) -> String {
    let mut bytes = [0u8; 16];
    SystemRandom::new().fill(&mut bytes).expect("randomness for an id");
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let bits = u128::from_be_bytes(bytes);
    let chars: String = (0..26).map(|i| ALPHABET[((bits >> (i * 5)) & 31) as usize] as char).collect();
    format!("{home}-{chars}")
}

/// An id this example could have made: the CLI and the routes check before they ask anyone.
pub fn valid_id(id: &str) -> bool {
    id.split_once('-').is_some_and(|(home, rest)| {
        (1..=16).contains(&home.len())
            && home.bytes().all(|b| b.is_ascii_alphanumeric())
            && rest.len() == 26
            && rest.bytes().all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
    })
}

impl CatalogKeep {
    fn set(&self, event: &Event, state: &Catalog) {
        let mut ids = self.ids.lock().expect("the id set");
        match state.rows.get(event.id()).and_then(|row| row.state) {
            Some(row) => ids.insert(event.id().to_string(), row),
            None => ids.remove(event.id()),
        };
    }
}

impl Keep for CatalogKeep {
    type State = Catalog;
    type Request = Request;
    const KEY: &'static str = "catalog";

    fn loaded(&mut self, state: &Catalog) {
        let rows = state.rows.iter().filter_map(|(id, row)| Some((id.clone(), row.state?)));
        *self.ids.lock().expect("the id set") = rows.collect();
    }

    fn handle(&mut self, kept: &mut Kept<Catalog, Answer>, request: Request) {
        match request {
            Request::Create { .. } if kept.state.rows.len() >= MAX_ROWS => {}
            Request::Create { owner, reply } => {
                let id = new_id(&self.home);
                let event = Event::Create { id: id.clone(), owner, ms: axon::trace::unix_micros() / 1000 };
                kept.record(event.clone(), Some(Box::new(move || drop(reply.reply(id)))));
                self.set(&event, &kept.state);
            }
            Request::Doc(event) => {
                if kept.state.changes(&event) {
                    kept.record(event.clone(), None);
                    self.set(&event, &kept.state);
                }
            }
            Request::List { user, tab, after, limit, reply } => {
                drop(reply.reply(kept.state.list(
                    &user,
                    tab,
                    after.as_ref().map(|(ms, id)| (*ms, id.as_str())),
                    limit,
                )));
            }
            Request::Rows { ids, reply } => {
                let rows = ids.into_iter().filter_map(|id| kept.state.rows.get(&id).cloned().map(|row| (id, row)));
                drop(reply.reply((rows.collect(), self.ids.lock().expect("the id set").len())));
            }
            Request::Hide { id, user, reply } => {
                let shared = kept.state.rows.get(&id).is_some_and(|row| row.members.contains_key(&user));
                if !shared {
                    let _ = reply.reply(false);
                    return;
                }
                let answer: Answer = Box::new(move || {
                    let _ = reply.reply(true);
                });
                let event = Event::Hide { id, user };
                match kept.state.changes(&event) {
                    true => kept.record(event, Some(answer)),
                    false => kept.after(answer).unwrap_or_else(|answer| answer()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::book::Book;

    #[test]
    fn ids_are_unique_and_valid() {
        let (a, b) = (new_id("h1"), new_id("h1"));
        assert!(valid_id(&a) && valid_id(&b) && a != b, "{a} {b}");
        assert_eq!(a.len(), "h1-".len() + 26);
        for bad in ["", "h1", "h1-", "h1-abc", "h1-ABCDEFGHIJKLMNOPQRSTUVWXYZ", "../x-abcdefghijklmnopqrstuvwxyz"] {
            assert!(!valid_id(bad), "{bad}");
        }
    }

    /// `scripts/measure.sh stored` keeps 100,000 documents a home: the catalog's checkpoint is one record of the
    /// documents' log. It was 1 MiB, about 4,800 rows, and past it the catalog failed at each start and stopped.
    #[test]
    fn a_catalog_of_the_most_rows_checkpoints_in_one_record() {
        let mut catalog = Catalog::default();
        let ms = 1_791_686_152_008;
        for d in 0..MAX_ROWS {
            let id = new_id("h1");
            let (owner, by) = ("load".to_string(), "load".to_string());
            catalog.apply(&Event::Create { id: id.clone(), owner, ms });
            catalog.apply(&Event::Touched { id, title: format!("Stored document number {d}"), size: 2_000, ms, by });
        }
        let state = serde_json::to_vec(&catalog).unwrap();
        let max = crate::journal::docs_config().max_record;
        println!("{MAX_ROWS} rows: {} bytes, {} a row, of {max}", state.len(), state.len() / MAX_ROWS);
        assert!(state.len() < max * 3 / 4, "{} of {max}", state.len());
    }
}
