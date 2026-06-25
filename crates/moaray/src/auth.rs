//! Inbound bearer authentication and per-key model allowlist.
//!
//! Auth is intentionally simple in v1: a bearer token is matched against the
//! configured keys (either by sha256 digest or by env-resolved plaintext,
//! constant-time compared). The matched key's `id` and allowlist are attached to
//! the request via [`AuthContext`]. The token itself is never logged or stored
//! past the comparison.

use moaray_config::{KeyConfig, KeySecret};
use moaray_core::error::{Error, Result};

/// What a successful authentication yields: the non-secret key id and the set of
/// models the key may call.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub key_id: String,
    pub allow_models: Vec<String>,
}

impl AuthContext {
    /// Whether this caller may use `model`.
    pub fn allows(&self, model: &str) -> bool {
        self.allow_models.iter().any(|m| m == model)
    }
}

fn sha256_hex(input: &[u8]) -> String {
    // Small dependency-free SHA-256 to avoid pulling a crypto crate for one use.
    sha256::digest(input)
}

/// Constant-time-ish equality for short secrets (avoid early-exit on mismatch).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Authenticate a presented bearer token against the configured keys.
pub fn authenticate(keys: &[KeyConfig], presented: &str) -> Result<AuthContext> {
    let presented = presented.trim();
    if presented.is_empty() {
        return Err(Error::InvalidApiKey);
    }
    let presented_hash = sha256_hex(presented.as_bytes());
    for k in keys {
        let matched = match &k.secret {
            KeySecret::Plain(p) => !p.is_empty() && ct_eq(p.as_bytes(), presented.as_bytes()),
            KeySecret::Sha256(h) => ct_eq(h.as_bytes(), presented_hash.as_bytes()),
        };
        if matched {
            return Ok(AuthContext {
                key_id: k.id.clone(),
                allow_models: k.allow_models.clone(),
            });
        }
    }
    Err(Error::InvalidApiKey)
}

/// Extract a bearer token from an `Authorization` header value.
pub fn parse_bearer(header: Option<&str>) -> Result<&str> {
    let h = header.ok_or(Error::InvalidApiKey)?;
    h.strip_prefix("Bearer ")
        .or_else(|| h.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or(Error::InvalidApiKey)
}

/// Minimal SHA-256 (FIPS 180-4) — used only to compare a presented token to a
/// configured digest. Not performance-critical.
mod sha256 {
    pub fn digest(data: &[u8]) -> String {
        let mut h: [u32; 8] = [
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
            0x5be0cd19,
        ];
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let bitlen = (data.len() as u64) * 8;
        let mut msg = data.to_vec();
        msg.push(0x80);
        while msg.len() % 64 != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bitlen.to_be_bytes());

        for chunk in msg.chunks(64) {
            let mut w = [0u32; 64];
            for (i, wv) in w.iter_mut().enumerate().take(16) {
                let j = i * 4;
                *wv = u32::from_be_bytes([chunk[j], chunk[j + 1], chunk[j + 2], chunk[j + 3]]);
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }
            let mut v = h;
            for i in 0..64 {
                let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
                let ch = (v[4] & v[5]) ^ ((!v[4]) & v[6]);
                let t1 = v[7]
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
                let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
                let t2 = s0.wrapping_add(maj);
                v[7] = v[6];
                v[6] = v[5];
                v[5] = v[4];
                v[4] = v[3].wrapping_add(t1);
                v[3] = v[2];
                v[2] = v[1];
                v[1] = v[0];
                v[0] = t1.wrapping_add(t2);
            }
            for i in 0..8 {
                h[i] = h[i].wrapping_add(v[i]);
            }
        }
        let mut out = String::with_capacity(64);
        for word in h {
            out.push_str(&format!("{word:08x}"));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<KeyConfig> {
        vec![
            KeyConfig {
                id: "plain-key".into(),
                secret: KeySecret::Plain("sk-secret".into()),
                allow_models: vec!["gpt".into()],
                rate_limit: None,
            },
            KeyConfig {
                id: "hash-key".into(),
                // sha256("sk-hashed")
                secret: KeySecret::Sha256(sha256::digest(b"sk-hashed")),
                allow_models: vec!["opus".into()],
                rate_limit: None,
            },
        ]
    }

    #[test]
    fn sha256_matches_known_vector() {
        // sha256("abc")
        assert_eq!(
            sha256::digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn parse_bearer_extracts_token() {
        assert_eq!(parse_bearer(Some("Bearer abc")).unwrap(), "abc");
        assert!(parse_bearer(None).is_err());
        assert!(parse_bearer(Some("Basic abc")).is_err());
        assert!(parse_bearer(Some("Bearer ")).is_err());
    }

    #[test]
    fn authenticate_plain_and_hashed() {
        let k = keys();
        assert_eq!(authenticate(&k, "sk-secret").unwrap().key_id, "plain-key");
        assert_eq!(authenticate(&k, "sk-hashed").unwrap().key_id, "hash-key");
        assert!(authenticate(&k, "wrong").is_err());
    }
}
