//! Passwords as argon2id hashes, and the signed values that stand for a sign-in, a share link and a guest: each is
//! fields joined by dots and an HMAC-SHA256 over them and their kind, so one kind cannot pass as another. They are
//! checked with the key alone; the version a sign-in carries is checked against the user's.

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

use crate::core::acl::Principal;

pub fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0; 16];
    SystemRandom::new().fill(&mut salt).map_err(|_| "no randomness for the salt")?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| e.to_string())?;
    Argon2::default().hash_password(password.as_bytes(), &salt).map(|hash| hash.to_string()).map_err(|e| e.to_string())
}

/// Tens of milliseconds of CPU: off an async thread.
pub fn verify_password(hash: &str, password: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|hash| Argon2::default().verify_password(password.as_bytes(), &hash).is_ok())
}

pub fn random_key() -> Result<Vec<u8>, String> {
    let mut key = vec![0; 32];
    SystemRandom::new().fill(&mut key).map_err(|_| "no randomness for a key")?;
    Ok(key)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex(text: &str) -> Option<Vec<u8>> {
    let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let pair = |pair: &[u8]| Some(digit(pair[0])? << 4 | digit(pair[1])?);
    text.len().is_multiple_of(2).then(|| text.as_bytes().chunks(2).map(pair).collect())?
}

/// A signed-in user, at the version of their account when they signed in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedIn {
    pub user: String,
    pub version: u64,
    pub expires_ms: u64,
}

/// A guest of a document's view link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Guest {
    pub doc: String,
    pub link_version: u64,
    pub name: String,
    pub expires_ms: u64,
}

impl Guest {
    pub fn principal(&self) -> Principal {
        Principal::Guest { name: self.name.clone(), link_version: self.link_version }
    }
}

/// An invite to set a password: for a new account at version 0, or for an account at that version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invite {
    pub name: String,
    pub version: u64,
    pub expires_ms: u64,
}

pub struct Auth {
    key: hmac::Key,
}

impl Auth {
    pub fn new(key: &[u8]) -> Result<Self, String> {
        if key.len() < 32 {
            return Err("a signing key needs 32 bytes or more".into());
        }
        Ok(Self { key: hmac::Key::new(hmac::HMAC_SHA256, key) })
    }

    fn sign(&self, kind: &str, fields: &[&str]) -> String {
        let body = fields.join(".");
        let mac = hmac::sign(&self.key, format!("{kind}:{body}").as_bytes());
        format!("{body}.{}", hex(mac.as_ref()))
    }

    fn open<'t>(&self, kind: &str, token: &'t str, fields: usize) -> Option<Vec<&'t str>> {
        let (body, mac) = token.rsplit_once('.')?;
        hmac::verify(&self.key, format!("{kind}:{body}").as_bytes(), &unhex(mac)?).ok()?;
        let parts: Vec<&str> = body.split('.').collect();
        (parts.len() == fields).then_some(parts)
    }

    /// A user name has no dots (`accounts::valid_name`).
    pub fn sign_in(&self, s: &SignedIn) -> String {
        self.sign("user", &[&s.user, &s.version.to_string(), &s.expires_ms.to_string()])
    }

    pub fn signed_in(&self, token: &str, now_ms: u64) -> Option<SignedIn> {
        let parts = self.open("user", token, 3)?;
        let signed =
            SignedIn { user: parts[0].into(), version: parts[1].parse().ok()?, expires_ms: parts[2].parse().ok()? };
        (signed.expires_ms > now_ms).then_some(signed)
    }

    /// A document's share link at a link version. It says nothing of the level: the document's sharing does.
    pub fn link(&self, doc: &str, link_version: u64) -> String {
        self.sign("link", &[doc, &link_version.to_string()])
    }

    pub fn open_link(&self, token: &str) -> Option<(String, u64)> {
        let parts = self.open("link", token, 2)?;
        Some((parts[0].into(), parts[1].parse().ok()?))
    }

    /// An operator's invite: `version` 0 makes the account `name`, and a user's current version sets a new password
    /// for it. Each holds once: the account then exists, or its version moved on.
    pub fn invite(&self, i: &Invite) -> String {
        self.sign("invite", &[&i.name, &i.version.to_string(), &i.expires_ms.to_string()])
    }

    pub fn open_invite(&self, token: &str, now_ms: u64) -> Option<Invite> {
        let parts = self.open("invite", token, 3)?;
        let invite =
            Invite { name: parts[0].into(), version: parts[1].parse().ok()?, expires_ms: parts[2].parse().ok()? };
        (invite.expires_ms > now_ms).then_some(invite)
    }

    pub fn guest(&self, g: &Guest) -> String {
        let name = hex(g.name.as_bytes());
        self.sign("guest", &[&g.doc, &g.link_version.to_string(), &name, &g.expires_ms.to_string()])
    }

    pub fn open_guest(&self, token: &str, now_ms: u64) -> Option<Guest> {
        let parts = self.open("guest", token, 4)?;
        let name = String::from_utf8(unhex(parts[2])?).ok()?;
        let guest = Guest {
            doc: parts[0].into(),
            link_version: parts[1].parse().ok()?,
            name,
            expires_ms: parts[3].parse().ok()?,
        };
        (guest.expires_ms > now_ms).then_some(guest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_hold_only_under_their_key_kind_and_time() {
        let auth = Auth::new(&[7; 32]).unwrap();
        let ana = SignedIn { user: "ana".into(), version: 2, expires_ms: 5000 };
        let token = auth.sign_in(&ana);
        assert_eq!(auth.signed_in(&token, 4999), Some(ana.clone()));
        assert_eq!(auth.signed_in(&token, 5000), None, "expired");
        assert_eq!(Auth::new(&[8; 32]).unwrap().signed_in(&token, 0), None, "another key");
        assert_eq!(auth.signed_in(&token.replacen("ana", "bob", 1), 0), None);
        for junk in ["", ".", "ana.2.5000", "ana.2.5000.", "ana.2.5000.zz"] {
            assert_eq!(auth.signed_in(junk, 0), None, "{junk:?}");
        }

        let link = auth.link("h1-x", 3);
        assert_eq!(auth.open_link(&link), Some(("h1-x".into(), 3)));
        assert_eq!(auth.signed_in(&link, 0), None, "a link is not a sign-in");

        let guest = Guest { doc: "h1-x".into(), link_version: 3, name: "Ana B. 中".into(), expires_ms: 9 };
        let token = auth.guest(&guest);
        assert_eq!(auth.open_guest(&token, 0), Some(guest));
        assert_eq!(auth.open_link(&token), None);
        assert!(Auth::new(&[1; 8]).is_err());
        let invite = Invite { name: "ben".into(), version: 0, expires_ms: 9 };
        let token = auth.invite(&invite);
        assert_eq!(auth.open_invite(&token, 8), Some(invite));
        assert_eq!(auth.open_invite(&token, 9), None, "expired");
        assert_eq!(auth.signed_in(&token, 0), None, "an invite is not a sign-in");
        assert!(verify_password(&hash_password("pw").unwrap(), "pw"));
    }
}
