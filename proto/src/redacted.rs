//! Hand-written `Debug` for the messages that carry key material.
//! `build.rs` turns off the generated `Debug` for each of them, and the
//! orphan rule puts the replacement in the crate that defines the type.

use std::fmt;

use crate::{CheckpointChunkKey, CheckpointPartKey, SessionSealKey};

/// Never prints the key bytes.
impl fmt::Debug for CheckpointPartKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointPartKey")
            .field("digest", &self.digest)
            .field("key_id", &self.key_id)
            .field("key", &format_args!("<{} bytes redacted>", self.key.len()))
            .finish()
    }
}

/// Never prints the key bytes.
impl fmt::Debug for SessionSealKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionSealKey")
            .field("key_id", &self.key_id)
            .field("key", &format_args!("<{} bytes redacted>", self.key.len()))
            .finish()
    }
}

/// Never prints the key bytes.
impl fmt::Debug for CheckpointChunkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointChunkKey")
            .field("key_id", &self.key_id)
            .field("key", &format_args!("<{} bytes redacted>", self.key.len()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_debug_never_prints_the_key_bytes_test() {
        let part = CheckpointPartKey {
            digest: "sha256:abc".into(),
            key_id: "ck_1".into(),
            key: vec![0x41; 32],
        };
        let seal = SessionSealKey {
            key_id: "sk_1".into(),
            key: vec![0x41; 32],
        };
        let chunk = CheckpointChunkKey {
            key_id: "cc_1".into(),
            key: vec![0x41; 32],
        };
        for printed in [
            format!("{part:?}"),
            format!("{seal:?}"),
            format!("{chunk:?}"),
        ] {
            assert!(printed.contains("<32 bytes redacted>"), "{printed}");
            assert!(!printed.contains("65, 65"), "{printed}");
        }
    }

    #[test]
    fn key_bytes_never_serialize_test() {
        let seal = SessionSealKey {
            key_id: "sk_1".into(),
            key: vec![0x41; 32],
        };
        let json = serde_json::to_value(&seal).expect("serialize");
        assert_eq!(json, serde_json::json!({ "keyId": "sk_1" }));
    }
}
