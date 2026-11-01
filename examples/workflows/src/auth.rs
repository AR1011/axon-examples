//! Who calls the API, and what they may do. The admin signs in with one password, kept as an argon2id hash, and gets
//! a signed session `expiry.name.scope.mac`: a browser holds it as a cookie, the command line as a bearer token.
//! Programs use named tokens with a scope, listed in `tokens.toml` by the SHA-256 of each token
//! ([`Tokens::parse`]); `workflows token` makes one.

use std::collections::BTreeMap;
use std::fmt;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use ring::rand::{SecureRandom, SystemRandom};
use ring::{digest, hmac};

/// What a caller may do, each scope all that the one before it may: read runs, signal them, start them, and act on
/// them as an operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    Read,
    Signal,
    Start,
    Admin,
}

impl Scope {
    pub fn parse(text: &str) -> Option<Scope> {
        Some(match text {
            "read" => Scope::Read,
            "signal" => Scope::Signal,
            "start" => Scope::Start,
            "admin" => Scope::Admin,
            _ => return None,
        })
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scope::Read => "read",
            Scope::Signal => "signal",
            Scope::Start => "start",
            Scope::Admin => "admin",
        })
    }
}

/// The name a password sign-in has: no token in the file may take it.
pub const ADMIN: &str = "admin";

/// A caller: a token's name, or [`ADMIN`], and its scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    pub name: String,
    pub scope: Scope,
}

/// A token's name and its scope, as `tokens.toml` and `workflows token` take them.
fn check_token(name: &str, scope: &str) -> Result<Scope, String> {
    let valid = (1..=64).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && name != ADMIN;
    if !valid {
        return Err(format!("a name is 1 to 64 of A-Z a-z 0-9 - _, and not {ADMIN}"));
    }
    Scope::parse(scope).ok_or_else(|| "scope is read, signal, start or admin".to_owned())
}

/// The named tokens, by the SHA-256 of each.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tokens(BTreeMap<String, Caller>);

impl Tokens {
    /// `tokens.toml`: a `[[token]]` table for each, with `name`, `scope` and `sha256` (in hex) as strings.
    /// Comments and blank lines are skipped; anything else is an error that names its line.
    pub fn parse(text: &str) -> Result<Tokens, String> {
        let mut tables: Vec<BTreeMap<String, String>> = Vec::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or_default().trim();
            let at = || format!("line {}", n + 1);
            if line.is_empty() {
                continue;
            }
            if line == "[[token]]" {
                tables.push(BTreeMap::new());
                continue;
            }
            let (key, value) = line.split_once('=').ok_or_else(|| format!("{}: not key = \"value\"", at()))?;
            let value = value.trim();
            let value = value.strip_prefix('"').and_then(|v| v.strip_suffix('"'));
            let value = value.ok_or_else(|| format!("{}: a value is a string in quotes", at()))?;
            let table = tables.last_mut().ok_or_else(|| format!("{}: outside a [[token]]", at()))?;
            table.insert(key.trim().to_owned(), value.to_owned());
        }
        let mut tokens = BTreeMap::new();
        for (i, mut table) in tables.into_iter().enumerate() {
            let mut field = |key: &str| table.remove(key).ok_or(format!("token {}: no {key}", i + 1));
            let (name, scope, sha256) = (field("name")?, field("scope")?, field("sha256")?);
            let scope = check_token(&name, &scope).map_err(|e| format!("token {name:?}: {e}"))?;
            if sha256.len() != 64 || unhex(&sha256).is_none() {
                return Err(format!("token {name}: sha256 is 64 hex digits"));
            }
            if tokens.values().any(|c: &Caller| c.name == name) {
                return Err(format!("token {name}: named twice"));
            }
            tokens.insert(sha256.to_ascii_lowercase(), Caller { name, scope });
        }
        Ok(Tokens(tokens))
    }

    pub fn caller(&self, token: &str) -> Option<&Caller> {
        self.0.get(&sha256_hex(token))
    }

    pub fn has(&self, name: &str) -> bool {
        self.0.values().any(|c| c.name == name)
    }
}

fn sha256_hex(text: &str) -> String {
    hex(digest::digest(&digest::SHA256, text.as_bytes()).as_ref())
}

/// A new token for `name` with `scope`, and its `[[token]]` table for `tokens.toml`.
pub fn new_token(name: &str, scope: &str) -> Result<(String, String), String> {
    let parsed = check_token(name, scope)?;
    let token = format!("wft_{}", hex(&random_key()?[..16]));
    let entry = format!("[[token]]\nname = \"{name}\"\nscope = \"{parsed}\"\nsha256 = \"{}\"\n", sha256_hex(&token));
    Ok((token, entry))
}

/// An argon2id PHC string for `the api's password hash`, with a random salt.
pub fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0; 16];
    SystemRandom::new().fill(&mut salt).map_err(|_| "no randomness for the salt")?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| e.to_string())?;
    Argon2::default().hash_password(password.as_bytes(), &salt).map(|hash| hash.to_string()).map_err(|e| e.to_string())
}

/// 32 random bytes, for a node to keep as its signing key.
pub fn random_key() -> Result<Vec<u8>, String> {
    let mut key = vec![0; 32];
    SystemRandom::new().fill(&mut key).map_err(|_| "no randomness for a key")?;
    Ok(key)
}

pub struct Auth {
    hash: String,
    key: hmac::Key,
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn unhex(text: &str) -> Option<Vec<u8>> {
    let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let pair = |pair: &[u8]| Some(digit(pair[0])? << 4 | digit(pair[1])?);
    text.len().is_multiple_of(2).then(|| text.as_bytes().chunks(2).map(pair).collect())?
}

impl Auth {
    /// `key` signs the sessions: without one a random key is made, and every session ends with the process.
    pub fn new(hash: &str, key: Option<&[u8]>) -> Result<Self, String> {
        PasswordHash::new(hash).map_err(|e| format!("the admin password hash is not a PHC string: {e}"))?;
        let key = match key {
            Some(key) if key.len() < 32 => return Err("a session key needs 32 bytes or more".into()),
            Some(key) => key.to_vec(),
            None => random_key()?,
        };
        Ok(Self { hash: hash.to_owned(), key: hmac::Key::new(hmac::HMAC_SHA256, &key) })
    }

    /// Tens of milliseconds of CPU: off an async thread.
    pub fn verify(&self, password: &str) -> bool {
        PasswordHash::new(&self.hash)
            .is_ok_and(|hash| Argon2::default().verify_password(password.as_bytes(), &hash).is_ok())
    }

    /// `expires.name.scope.mac`: the time in Unix milliseconds, the caller, and an HMAC-SHA256 over them.
    pub fn seal(&self, expires_ms: u64, caller: &Caller) -> String {
        let body = format!("{expires_ms}.{}.{}", caller.name, caller.scope);
        format!("{body}.{}", hex(hmac::sign(&self.key, body.as_bytes()).as_ref()))
    }

    /// The caller and when the session ends, if a node with this key signed it and it has not ended.
    pub fn check(&self, token: &str, now_ms: u64) -> Option<(Caller, u64)> {
        let (body, mac) = token.rsplit_once('.')?;
        hmac::verify(&self.key, body.as_bytes(), &unhex(mac)?).ok()?;
        let mut parts = body.split('.');
        let (expires, name, scope) = (parts.next()?.parse().ok()?, parts.next()?, Scope::parse(parts.next()?)?);
        (parts.next().is_none() && expires > now_ms).then(|| (Caller { name: name.to_owned(), scope }, expires))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_holds_until_it_expires_and_only_under_the_key_that_signed_it() {
        // Argon2 at its lowest cost: the test checks the wiring, not the cost.
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let params = argon2::Params::new(8, 1, 1, None).unwrap();
        let cheap = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let hash = cheap.hash_password(b"open sesame", &salt).unwrap().to_string();
        let auth = Auth::new(&hash, Some(&[7; 32])).unwrap();
        assert!(auth.verify("open sesame") && !auth.verify("open sesame ") && !auth.verify(""));

        let admin = Caller { name: ADMIN.into(), scope: Scope::Admin };
        let token = auth.seal(5000, &admin);
        assert_eq!(auth.check(&token, 4999), Some((admin.clone(), 5000)));
        assert_eq!(auth.check(&token, 5000), None, "expired");
        // A later expiry or another scope under the old signature, a cut signature, another node's key.
        let (_, mac) = token.rsplit_once('.').unwrap();
        assert_eq!(auth.check(&format!("9000.admin.admin.{mac}"), 0), None);
        assert_eq!(auth.check(&format!("5000.admin.read.{mac}"), 0), None);
        assert_eq!(auth.check(&token[..token.len() - 2], 0), None);
        for junk in ["", ".", "5000", "5000.", "5000.zz", "x.00", "5000.admin.admin"] {
            assert_eq!(auth.check(junk, 0), None, "{junk:?}");
        }
        let other = Auth::new(&hash, Some(&[8; 32])).unwrap();
        assert_eq!(other.check(&token, 0), None);
        assert_eq!(Auth::new(&hash, None).unwrap().check(&token, 0), None, "a random key is no other node's");

        assert!(Auth::new("not a hash", None).is_err());
        assert!(Auth::new(&hash, Some(&[1; 8])).is_err());
        let hashed = hash_password("pw").unwrap();
        assert!(Auth::new(&hashed, None).unwrap().verify("pw"));
    }

    #[test]
    fn tokens_read_from_their_file_by_hash_and_scope() {
        let (token, entry) = new_token("carrier", "signal").unwrap();
        let text = format!(
            "# the carrier's webhook\n{entry}\n[[token]]\nname = \"ops\" # on call\nscope = \"admin\"\nsha256 = \"{}\"\n",
            "ab".repeat(32)
        );
        let tokens = Tokens::parse(&text).unwrap();
        assert_eq!(tokens.caller(&token), Some(&Caller { name: "carrier".into(), scope: Scope::Signal }));
        assert_eq!(tokens.caller("wft_other"), None);
        assert!(tokens.has("ops") && !tokens.has("admin"));
        assert!(Scope::Read < Scope::Signal && Scope::Signal < Scope::Start && Scope::Start < Scope::Admin);
        for bad in [
            "name = \"x\"",
            "[[token]]\nname = x",
            "[[token]]\nname = \"x\"\nscope = \"read\"",
            "[[token]]\nname = \"admin\"\nscope = \"read\"\nsha256 = \"00\"",
            "[[token]]\nname = \"x\"\nscope = \"root\"\nsha256 = \"00\"",
            "[[token]]\nname = \"x\"\nscope = \"read\"\nsha256 = \"00\"",
        ] {
            assert!(Tokens::parse(bad).is_err(), "{bad}");
        }
        let twice = format!("{entry}{entry}");
        assert!(Tokens::parse(&twice).is_err(), "a name twice");
        assert!(new_token("admin", "read").is_err() && new_token("x", "root").is_err());
    }
}
