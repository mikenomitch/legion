//! A hibernated session isolate's checkpoint (brief item 15).
//!
//! Hibernation keeps what a guest put in `legion:isolate/state` and the
//! session's metadata, not linear memory: wasmtime 50 offers no cheap way to
//! read or restore a component's memories and handle tables. The body is
//! opaque to the worker and versioned here:
//!
//! ```text
//! "LISC"  u16 version (1)  u32 meta length  meta (JSON)
//! u32 entry count  { u32 key length, key, u32 value length, value }*
//! ```
//!
//! All integers little-endian. The state map is engine-independent, so a
//! checkpoint written under another engine version or module still
//! restores at the state level.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 4] = b"LISC";
pub const VERSION: u16 = 1;

/// Session metadata carried in a checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SessionMeta {
    pub session_id: String,
    pub job_id: String,
    pub task_name: String,
    pub tenant: String,
    pub invocations: u64,
    /// The session's own ceiling and reservation when it was resized
    /// (`ResizeSession`); 0 = the task's.
    pub memory_max_bytes: u64,
    pub memory_reservation_bytes: u64,
    pub hibernations: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    pub meta: SessionMeta,
    pub state: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unreadable session checkpoint: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

impl Checkpoint {
    pub fn encode(&self) -> Vec<u8> {
        let meta = serde_json::to_vec(&self.meta).expect("metadata serializes");
        let mut out = Vec::with_capacity(
            14 + meta.len()
                + self
                    .state
                    .iter()
                    .map(|(k, v)| 8 + k.len() + v.len())
                    .sum::<usize>(),
        );
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
        out.extend_from_slice(&meta);
        out.extend_from_slice(&(self.state.len() as u32).to_le_bytes());
        for (k, v) in &self.state {
            out.extend_from_slice(&(k.len() as u32).to_le_bytes());
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader(bytes);
        if r.take(4)? != MAGIC {
            return Err(DecodeError("not an isolate checkpoint".into()));
        }
        let version = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
        if version != VERSION {
            return Err(DecodeError(format!(
                "format version {version}, this host reads {VERSION}"
            )));
        }
        let mlen = r.u32()? as usize;
        let meta: SessionMeta = serde_json::from_slice(r.take(mlen)?)
            .map_err(|e| DecodeError(format!("metadata: {e}")))?;
        let n = r.u32()?;
        let mut state = BTreeMap::new();
        for _ in 0..n {
            let klen = r.u32()? as usize;
            let key = String::from_utf8(r.take(klen)?.to_vec())
                .map_err(|_| DecodeError("a state key is not UTF-8".into()))?;
            let vlen = r.u32()? as usize;
            state.insert(key, r.take(vlen)?.to_vec());
        }
        if !r.0.is_empty() {
            return Err(DecodeError("trailing bytes".into()));
        }
        Ok(Self { meta, state })
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError("truncated".into()));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod checkpoint_test {
    use super::*;

    #[test]
    fn round_trips_and_refuses_damage_test() {
        let c = Checkpoint {
            meta: SessionMeta {
                session_id: "s-1".into(),
                job_id: "j".into(),
                task_name: "t".into(),
                tenant: "acme".into(),
                invocations: 7,
                memory_max_bytes: 64 << 20,
                ..Default::default()
            },
            state: BTreeMap::from([
                ("a".to_string(), vec![1, 2, 3]),
                ("empty".to_string(), vec![]),
            ]),
        };
        let bytes = c.encode();
        assert_eq!(Checkpoint::decode(&bytes).unwrap(), c);
        assert!(Checkpoint::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut bad = bytes.clone();
        bad[4] = 9;
        assert!(Checkpoint::decode(&bad)
            .unwrap_err()
            .0
            .contains("format version 9"));
        assert!(Checkpoint::decode(b"nope").is_err());
    }
}
