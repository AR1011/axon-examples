//! The admin login: one password kept as an argon2id hash, and a signed value that says until when its bearer is
//! signed in. A browser holds it as a cookie, the command line as a bearer token.

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

/// An argon2id PHC string for `AdminConfig::password_hash`, with a random salt.
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
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

    /// `expires.mac`: the time in Unix milliseconds, and an HMAC-SHA256 over it.
    pub fn seal(&self, expires_ms: u64) -> String {
        let body = expires_ms.to_string();
        format!("{body}.{}", hex(hmac::sign(&self.key, body.as_bytes()).as_ref()))
    }

    /// When the session ends, if this node signed it and it has not ended.
    pub fn check(&self, token: &str, now_ms: u64) -> Option<u64> {
        let (body, mac) = token.split_once('.')?;
        hmac::verify(&self.key, body.as_bytes(), &unhex(mac)?).ok()?;
        body.parse().ok().filter(|expires| *expires > now_ms)
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

        let token = auth.seal(5000);
        assert_eq!(auth.check(&token, 4999), Some(5000));
        assert_eq!(auth.check(&token, 5000), None, "expired");
        // A later expiry under the old signature, a cut signature, another node's key.
        let (_, mac) = token.split_once('.').unwrap();
        assert_eq!(auth.check(&format!("9000.{mac}"), 0), None);
        assert_eq!(auth.check(&token[..token.len() - 2], 0), None);
        for junk in ["", ".", "5000", "5000.", "5000.zz", "x.00"] {
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
}
