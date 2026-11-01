//! A home's list of its documents. The documents are the authority for their own sharing and text; the catalog is an
//! index of them, told by each document when it changes, so a lost tell heals the next time the document loads.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::core::acl::{Acl, Level};
use crate::core::book::Book;

/// A row is `Creating` from the catalog's answer until the document's first state is durable. A document whose row
/// is `Ready` and whose log holds nothing fails its load instead of opening empty (I4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowState {
    Creating,
    Ready,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub state: Option<RowState>,
    pub owner: String,
    pub created_ms: u64,
    pub title: String,
    pub edited_ms: u64,
    /// Who edited last, as the document last told.
    #[serde(default)]
    pub edited_by: String,
    pub size: usize,
    pub members: BTreeMap<String, Level>,
    pub hidden_by: BTreeSet<String>,
    pub trashed_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "e", rename_all = "snake_case")]
pub enum Event {
    Create {
        id: String,
        owner: String,
        ms: u64,
    },
    /// The document's first state is durable.
    Ready {
        id: String,
    },
    Sharing {
        id: String,
        owner: String,
        members: BTreeMap<String, Level>,
        trashed_ms: Option<u64>,
    },
    Touched {
        id: String,
        title: String,
        size: usize,
        ms: u64,
        #[serde(default)]
        by: String,
    },
    Hide {
        id: String,
        user: String,
    },
    Purged {
        id: String,
    },
}

impl Event {
    pub fn sharing(id: &str, acl: &Acl) -> Self {
        Self::Sharing {
            id: id.into(),
            owner: acl.owner.clone(),
            members: acl.members.iter().map(|(name, m)| (name.clone(), m.level)).collect(),
            trashed_ms: acl.trashed_ms,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Create { id, .. }
            | Self::Ready { id }
            | Self::Sharing { id, .. }
            | Self::Touched { id, .. }
            | Self::Hide { id, .. }
            | Self::Purged { id } => id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tab {
    Mine,
    Shared,
    Trash,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Catalog {
    pub rows: BTreeMap<String, Row>,
}

impl Book for Catalog {
    type Event = Event;

    fn apply(&mut self, event: &Event) {
        if let Event::Purged { id } = event {
            self.rows.remove(id);
            return;
        }
        let row = self.rows.entry(event.id().to_string()).or_default();
        match event {
            Event::Create { owner, ms, .. } => {
                (row.owner, row.created_ms, row.edited_ms) = (owner.clone(), *ms, *ms);
                row.state.get_or_insert(RowState::Creating);
            }
            Event::Ready { .. } => row.state = Some(RowState::Ready),
            Event::Sharing { owner, members, trashed_ms, .. } => {
                // Told by a document that loaded: it exists, whatever the catalog lost.
                row.state = Some(RowState::Ready);
                (row.owner, row.members, row.trashed_ms) = (owner.clone(), members.clone(), *trashed_ms);
                row.hidden_by.retain(|user| members.contains_key(user));
            }
            Event::Touched { title, size, ms, by, .. } => {
                row.state = Some(RowState::Ready);
                (row.title, row.size, row.edited_ms) = (title.clone(), *size, (*ms).max(row.edited_ms));
                if !by.is_empty() {
                    row.edited_by = by.clone();
                }
            }
            Event::Hide { user, .. } => {
                row.hidden_by.insert(user.clone());
            }
            Event::Purged { .. } => unreachable!("handled above"),
        }
    }
}

impl Catalog {
    /// Would the event change anything? The documents tell the same sharing at each load.
    pub fn changes(&self, event: &Event) -> bool {
        let before = self.rows.get(event.id());
        let mut one = Catalog { rows: before.map(|row| (event.id().to_string(), row.clone())).into_iter().collect() };
        one.apply(event);
        one.rows.get(event.id()) != before
    }

    /// A user's rows in one tab, newest edit first, after `after`: the last row of the page before, by its edit time
    /// and id, so pages from several homes merge in one order.
    pub fn list(&self, user: &str, tab: Tab, after: Option<(u64, &str)>, limit: usize) -> Vec<(String, Row)> {
        let shown = |row: &Row| match tab {
            Tab::Mine => row.owner == user && row.trashed_ms.is_none(),
            Tab::Shared => row.members.contains_key(user) && row.trashed_ms.is_none() && !row.hidden_by.contains(user),
            Tab::Trash => row.owner == user && row.trashed_ms.is_some(),
        };
        let mut rows: Vec<(&String, &Row)> =
            self.rows.iter().filter(|(_, r)| r.state == Some(RowState::Ready) && shown(r)).collect();
        rows.retain(|(id, row)| after.is_none_or(|after| order(row.edited_ms, id) > order(after.0, after.1)));
        rows.sort_by(|(a, ra), (b, rb)| order(ra.edited_ms, a).cmp(&order(rb.edited_ms, b)));
        rows.into_iter().take(limit).map(|(id, row)| (id.clone(), row.clone())).collect()
    }
}

/// The list's order: newest edit first, then by id.
pub fn order(edited_ms: u64, id: &str) -> (std::cmp::Reverse<u64>, &str) {
    (std::cmp::Reverse(edited_ms), id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::acl::{Change, Principal};

    #[test]
    fn rows_list_by_tab_and_heal_from_the_document() {
        let mut catalog = Catalog::default();
        let create = |id: &str, ms| Event::Create { id: id.into(), owner: "ana".into(), ms };
        catalog.apply(&create("a", 1));
        assert!(catalog.list("ana", Tab::Mine, None, 10).is_empty(), "creating rows are not listed");
        catalog.apply(&Event::Ready { id: "a".into() });
        catalog.apply(&create("b", 2));
        catalog.apply(&Event::Ready { id: "b".into() });
        let ids = |rows: Vec<(String, Row)>| rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        assert_eq!(ids(catalog.list("ana", Tab::Mine, None, 10)), ["b", "a"]);
        assert_eq!(ids(catalog.list("ana", Tab::Mine, Some((2, "b")), 10)), ["a"]);

        let mut acl = Acl::new("ana");
        acl = Change::Member("bob".into(), Level::View).apply(&acl, &Principal::User("ana".into())).unwrap();
        let sharing = Event::sharing("a", &acl);
        assert!(catalog.changes(&sharing));
        catalog.apply(&sharing);
        assert!(!catalog.changes(&sharing), "told again at the next load");
        assert_eq!(ids(catalog.list("bob", Tab::Shared, None, 10)), ["a"]);
        catalog.apply(&Event::Hide { id: "a".into(), user: "bob".into() });
        assert!(catalog.list("bob", Tab::Shared, None, 10).is_empty());

        acl.trashed_ms = Some(9);
        catalog.apply(&Event::sharing("a", &acl));
        assert_eq!(ids(catalog.list("ana", Tab::Trash, None, 10)), ["a"]);
        assert_eq!(ids(catalog.list("ana", Tab::Mine, None, 10)), ["b"]);

        // A row the catalog lost comes back when its document tells it.
        catalog.apply(&Event::Purged { id: "b".into() });
        catalog.apply(&Event::sharing("b", &Acl::new("ana")));
        assert_eq!(catalog.rows["b"].state, Some(RowState::Ready));
    }
}
