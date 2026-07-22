//! Small shared crypto helpers (hex codecs + keyed/unkeyed SHA hashing) used
//! by the pipeline transform crypto functions (`$hmac`/`$hmacVerify`) and by
//! outbound credential injection (HMAC/AWS SigV4 signing). Kept in one place so
//! the two callers don't drift on algorithm support or hex formatting.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};

/// Lowercase-hex encode bytes.
pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Decode lowercase/uppercase hex; `None` on odd length or non-hex digits.
pub fn from_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.is_empty() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// HMAC of `message` under `key` for `sha256`/`sha512`; `None` on unknown algo.
pub fn hmac_bytes(algorithm: &str, key: &[u8], message: &[u8]) -> Option<Vec<u8>> {
    match algorithm {
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
            mac.update(message);
            Some(mac.finalize().into_bytes().to_vec())
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).ok()?;
            mac.update(message);
            Some(mac.finalize().into_bytes().to_vec())
        }
        _ => None,
    }
}

/// Constant-time HMAC verification against a hex signature. Any malformed input
/// (unknown algorithm, non-hex signature) → `false`.
pub fn hmac_verify(algorithm: &str, key: &[u8], message: &[u8], signature_hex: &str) -> bool {
    let Some(provided) = from_hex(signature_hex) else {
        return false;
    };
    match algorithm {
        "sha256" => match Hmac::<Sha256>::new_from_slice(key) {
            Ok(mut mac) => {
                mac.update(message);
                mac.verify_slice(&provided).is_ok()
            }
            Err(_) => false,
        },
        "sha512" => match Hmac::<Sha512>::new_from_slice(key) {
            Ok(mut mac) => {
                mac.update(message);
                mac.verify_slice(&provided).is_ok()
            }
            Err(_) => false,
        },
        _ => false,
    }
}

/// HMAC-SHA256 raw bytes (the SigV4 signing primitive).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// SHA-256 digest, lowercase hex (SigV4 payload/canonical-request hashing).
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    to_hex(&hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?".
    #[test]
    fn hmac_sha256_known_vector() {
        let mac = hmac_bytes("sha256", b"Jefe", b"what do ya want for nothing?").unwrap();
        assert_eq!(
            to_hex(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn hex_roundtrips() {
        let bytes = vec![0u8, 1, 15, 16, 255];
        assert_eq!(from_hex(&to_hex(&bytes)).unwrap(), bytes);
        assert!(from_hex("xyz").is_none());
        assert!(from_hex("abc").is_none()); // odd length
    }

    // NIST FIPS 180-2: SHA-256 of "abc".
    #[test]
    fn sha256_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
