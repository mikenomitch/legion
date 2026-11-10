//! The request digest: a SHA-256 over a canonical form of a gRPC request
//! message, computed from the wire bytes without knowing the message type.
//!
//! Protobuf encoding is not canonical — prost encodes `map` fields from a
//! `HashMap`, whose iteration order differs between processes, so the same
//! `legion` command run twice can send different bytes. The digest therefore
//! parses the wire format and sorts every message's fields (recursively,
//! for any length-delimited value that parses as a message), so map entries
//! in any order produce one digest.
//!
//! The price of being type-blind: a length-delimited value that happens to
//! parse as a message is canonicalized as one, so two such values that are
//! permutations of the same fields digest alike, and reordering a repeated
//! field does not change the digest. Approvals bind the digest together with
//! the operation and the requester, so the residual ambiguity lets a caller
//! reorder its own approved request's list fields, nothing more
//! (docs/approvals-and-rate-limits.md, "What an approval binds").

use sha2::{Digest, Sha256};

/// Deepest nesting the canonicalizer follows; deeper values digest as bytes.
const MAX_DEPTH: usize = 24;

/// The digest of a framed unary gRPC request body (`flag, len, message`),
/// hex. A compressed or malformed frame digests its raw bytes.
pub fn request_digest(body: &[u8]) -> String {
    let message = match body {
        [0, a, b, c, d, rest @ ..] => {
            let len = u32::from_be_bytes([*a, *b, *c, *d]) as usize;
            if rest.len() == len {
                Some(rest)
            } else {
                None
            }
        }
        _ => None,
    };
    let canonical = message
        .and_then(|message| canonical(message, 0))
        .unwrap_or_else(|| {
            let mut raw = b"raw:".to_vec();
            raw.extend_from_slice(body);
            raw
        });
    hex(&Sha256::digest(&canonical))
}

/// The digest of an already-unframed message (tests and in-process callers).
pub fn message_digest(message: &[u8]) -> String {
    let mut framed = Vec::with_capacity(message.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    request_digest(&framed)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// The canonical form of `bytes` parsed as a message, or `None` when they
/// are not one.
fn canonical(bytes: &[u8], depth: usize) -> Option<Vec<u8>> {
    let mut entries: Vec<Vec<u8>> = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let key = read_varint(bytes, &mut pos)?;
        let field = key >> 3;
        let wire = (key & 7) as u8;
        if field == 0 {
            return None;
        }
        let mut entry = Vec::new();
        push_varint(&mut entry, field);
        entry.push(wire);
        match wire {
            0 => {
                let value = read_varint(bytes, &mut pos)?;
                push_varint(&mut entry, value);
            }
            1 => {
                let value = bytes.get(pos..pos + 8)?;
                pos += 8;
                entry.extend_from_slice(value);
            }
            5 => {
                let value = bytes.get(pos..pos + 4)?;
                pos += 4;
                entry.extend_from_slice(value);
            }
            2 => {
                let len = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
                let value = bytes.get(pos..pos.checked_add(len)?)?;
                pos += len;
                let nested = (depth < MAX_DEPTH && !value.is_empty())
                    .then(|| canonical(value, depth + 1))
                    .flatten();
                let (tag, body) = match nested {
                    Some(nested) => (b'M', nested),
                    None => (b'B', value.to_vec()),
                };
                entry.push(tag);
                push_varint(&mut entry, body.len() as u64);
                entry.extend_from_slice(&body);
            }
            _ => return None,
        }
        entries.push(entry);
    }
    entries.sort();
    Some(entries.concat())
}

#[cfg(test)]
mod digest_test {
    use super::*;
    use prost::Message;

    #[test]
    fn map_order_does_not_change_the_digest_test() {
        // One map entry per encoding; concatenated, the two orders are the
        // same `Namespace` with both entries (repeated fields append).
        let entry = |key: &str, value: &str| {
            crate::structs::Namespace {
                meta: std::collections::HashMap::from([(key.to_string(), value.to_string())]),
                ..Default::default()
            }
            .encode_to_vec()
        };
        let (a, b) = (entry("alpha", "1"), entry("beta", "2"));
        let forward = [a.clone(), b.clone()].concat();
        let backward = [b, a].concat();
        assert_ne!(forward, backward, "the wire bytes differ");
        let decoded = crate::structs::Namespace::decode(forward.as_slice()).unwrap();
        assert_eq!(decoded.meta.len(), 2);
        assert_eq!(
            decoded,
            crate::structs::Namespace::decode(backward.as_slice()).unwrap()
        );
        assert_eq!(message_digest(&forward), message_digest(&backward));

        // A different value changes it.
        let other = [entry("alpha", "1"), entry("beta", "3")].concat();
        assert_ne!(message_digest(&forward), message_digest(&other));
    }

    #[test]
    fn field_order_and_framing_test() {
        // field 1 varint 7, field 2 string "x" — and the reverse order.
        let forward = [0x08, 0x07, 0x12, 0x01, b'x'];
        let backward = [0x12, 0x01, b'x', 0x08, 0x07];
        assert_eq!(message_digest(&forward), message_digest(&backward));
        // A truncated frame falls back to the raw bytes, deterministically.
        assert_eq!(request_digest(&[0, 0, 0]), request_digest(&[0, 0, 0]));
        assert_ne!(request_digest(&[0, 0, 0]), request_digest(&[0, 0, 1]));
        // The empty message has a stable digest.
        assert_eq!(message_digest(&[]), message_digest(&[]));
    }
}
