//! The users: made by an operator, each with an argon2id hash and a version that a new password or a disable raises,
//! so the sessions signed at an older version end. An operator may also use the admin routes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::core::book::Book;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub hash: String,
    pub disabled: bool,
    pub version: u64,
    #[serde(default)]
    pub operator: bool,
}

/// A row of the admin page's user list: no hash leaves the accounts node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listed {
    pub name: String,
    pub disabled: bool,
    pub operator: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "e", rename_all = "snake_case")]
pub enum Event {
    Add { name: String, hash: String },
    Password { name: String, hash: String },
    Disable { name: String },
    Enable { name: String },
    Operator { name: String, operator: bool },
}

#[derive(Default, Serialize, Deserialize)]
pub struct Accounts {
    pub users: BTreeMap<String, User>,
}

/// A user name: what a cookie, a member list and a URL carry.
pub fn valid_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

impl Book for Accounts {
    type Event = Event;

    fn apply(&mut self, event: &Event) {
        match event {
            Event::Add { name, hash } => {
                let user = User { hash: hash.clone(), disabled: false, version: 1, operator: false };
                self.users.insert(name.clone(), user);
            }
            Event::Password { name, hash } => {
                if let Some(user) = self.users.get_mut(name) {
                    (user.hash, user.version) = (hash.clone(), user.version + 1);
                }
            }
            Event::Disable { name } | Event::Enable { name } => {
                if let Some(user) = self.users.get_mut(name) {
                    user.disabled = matches!(event, Event::Disable { .. });
                    user.version += 1;
                }
            }
            Event::Operator { name, operator } => {
                if let Some(user) = self.users.get_mut(name) {
                    user.operator = *operator;
                }
            }
        }
    }
}

impl Accounts {
    /// Why the event may not be applied.
    pub fn check(&self, event: &Event) -> Result<(), String> {
        match event {
            Event::Add { name, .. } if !valid_name(name) => {
                Err(format!("{name:?} is not a user name: 1 to 32 of a-z, 0-9, - and _"))
            }
            Event::Add { name, .. } if self.users.contains_key(name) => Err(format!("{name} exists")),
            Event::Add { .. } => Ok(()),
            Event::Password { name, .. }
            | Event::Disable { name }
            | Event::Enable { name }
            | Event::Operator { name, .. } => self.users.get(name).map(drop).ok_or_else(|| format!("no user {name}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_password_or_a_disable_raises_the_version() {
        let mut accounts = Accounts::default();
        let add = Event::Add { name: "ana".into(), hash: "h1".into() };
        accounts.check(&add).unwrap();
        accounts.apply(&add);
        assert!(accounts.check(&add).is_err(), "twice");
        assert!(accounts.check(&Event::Add { name: "Ana!".into(), hash: "h".into() }).is_err());
        assert!(accounts.check(&Event::Disable { name: "bob".into() }).is_err());
        accounts.apply(&Event::Password { name: "ana".into(), hash: "h2".into() });
        accounts.apply(&Event::Disable { name: "ana".into() });
        let ana = &accounts.users["ana"];
        assert_eq!((ana.hash.as_str(), ana.disabled, ana.version), ("h2", true, 3));
        accounts.apply(&Event::Operator { name: "ana".into(), operator: true });
        assert!(accounts.users["ana"].operator);
        assert_eq!(accounts.users["ana"].version, 3, "the sign-ins go on");
        let before: User = serde_json::from_str(r#"{"hash":"h","disabled":false,"version":1}"#).unwrap();
        assert!(!before.operator, "a user kept before there were operators");
    }
}
