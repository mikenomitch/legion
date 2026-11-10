//! openraft type configuration for Legion's replicated store (phase A-1 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2).
//!
//! One `TypeConfig` serves every raft group ("lane"): the groups differ only
//! in which tables their changesets touch, not in their protocol types, so
//! the transport, log store, and state machine are written once and
//! instantiated per lane.

use std::io::Cursor;

use crate::storage::raft::{StoreCommand, StoreResponse};

openraft::declare_raft_types!(
    /// The single openraft type configuration used by every Legion raft
    /// group.
    ///
    /// - `D = StoreCommand`: one captured SQLite session changeset plus lane
    ///   and op-name metadata (plan JC-1 — effects, not commands).
    /// - `R = StoreResponse`: empty; return values never cross raft (the
    ///   proposer computed them at capture time and only waits for commit).
    /// - `NodeId = u64`: derived deterministically from the manager's string
    ///   id via [`fnv1a64`], so any node can compute any peer's raft id
    ///   without coordination.
    /// - `Node = openraft::BasicNode` with `addr = "name@host:port"` (see
    ///   [`encode_node_addr`]). Encoding the manager string id into the
    ///   address — rather than keeping a separate id↔name map — was chosen
    ///   deliberately: membership (and therefore the mapping) is itself
    ///   replicated through raft, so every node always has a consistent
    ///   name/addr view with zero extra state to persist, migrate, or let
    ///   drift.
    ///
    /// The remaining associated types keep openraft's defaults:
    /// `Entry<TypeConfig>` log entries (serde-encoded on the wire and in the
    /// sqlite log store), `Cursor<Vec<u8>>` snapshot data (whole-DB `VACUUM
    /// INTO` images, plan §A5), the tokio runtime, and oneshot responders.
    pub(crate) TypeConfig:
        D = StoreCommand,
        R = StoreResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
);

/// 64-bit FNV-1a hash of a manager's string id, used as its raft
/// `NodeId`.
///
/// Deterministic and dependency-free so every manager derives the same raft
/// id for a peer from its string id alone (join tokens and configs carry
/// string ids; membership carries the derived u64). FNV-1a is not
/// collision-proof in general, but cluster sizes here are single digits and
/// the join path rejects a joiner whose derived id collides with an existing
/// member under a different name.
///
/// Reference: <http://www.isthe.com/chongo/tech/comp/fnv/> (offset basis
/// `0xcbf29ce484222325`, prime `0x100000001b3`).
pub(crate) fn fnv1a64(name: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Builds the `BasicNode.addr` value for a member: `"name@host:port"`.
///
/// `name` is the manager's string id, `host_port` the advertise address of
/// its gRPC listener. The transport dials only the part after the last `@`
/// ([`node_addr_host_port`]); the name rides along so membership listings
/// can show operator-meaningful identities (see `RaftMemberInfo` in
/// `proto/legion.proto`).
pub(crate) fn encode_node_addr(node_id: &str, host_port: &str) -> String {
    format!("{node_id}@{host_port}")
}

/// Splits a `BasicNode.addr` produced by [`encode_node_addr`] into
/// `(name, host_port)`.
///
/// Splits on the *last* `@` so names containing `@` still resolve; a bare
/// `"host:port"` (no `@`) yields an empty name, which keeps pre-A-2 configs
/// that list plain addresses dialable.
pub(crate) fn split_node_addr(addr: &str) -> (&str, &str) {
    match addr.rsplit_once('@') {
        Some((name, host_port)) => (name, host_port),
        None => ("", addr),
    }
}

/// The `"host:port"` part of a `BasicNode.addr` — what the transport dials.
pub(crate) fn node_addr_host_port(addr: &str) -> &str {
    split_node_addr(addr).1
}

/// serde adapter that encodes a `Vec<u8>` as base64 in human-readable
/// formats (serde_json — the raft wire *and* log-store encoding) and as
/// plain bytes in binary formats.
///
/// Why this exists (adversarial finding, 2026-07-24): every binary raft
/// payload — a captured SQLite session changeset inside `StoreCommand`, a
/// snapshot chunk inside `InstallSnapshotRequest` — travels through
/// `serde_json`, where a `Vec<u8>` renders as a JSON *array of numbers*
/// (`[137,80,78,...]`), up to 4 bytes of JSON per raw byte. That blowup is
/// what made large snapshots and big append-entries batches exceed the
/// transport's message ceiling (and bloated `log.sqlite` by the same
/// factor). Base64 is 4/3 — a ~3x reduction — and keeps the encoding
/// self-describing text, so the payloads stay debuggable.
///
/// This is a wire/disk format: both sides of an RPC and the on-disk log
/// must agree. Mixed-version manager planes are not supported (JC-6), so
/// the change rides the normal upgrade path.
pub(crate) mod wire_bytes {
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error> {
        if ser.is_human_readable() {
            ser.serialize_str(&BASE64.encode(bytes))
        } else {
            bytes.serialize(ser)
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        if de.is_human_readable() {
            let text = String::deserialize(de)?;
            BASE64.decode(&text).map_err(serde::de::Error::custom)
        } else {
            Vec::<u8>::deserialize(de)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_bytes_round_trips_and_is_compact_in_json() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Payload {
            #[serde(with = "wire_bytes")]
            data: Vec<u8>,
        }

        // Every byte value, so no encoding edge case is missed.
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let payload = Payload { data: data.clone() };
        let json = serde_json::to_vec(&payload).expect("encode");
        assert_eq!(
            serde_json::from_slice::<Payload>(&json).expect("decode"),
            payload
        );

        // The point of the adapter: JSON stays within base64's 4/3 (plus a
        // few bytes of field name / quotes), NOT the ~4x of a number array.
        assert!(
            (json.len() as f64) < data.len() as f64 * 1.5,
            "base64 payload must not blow up: {} json bytes for {} raw bytes",
            json.len(),
            data.len()
        );
    }

    #[test]
    fn fnv1a64_matches_reference_vectors() {
        // Published FNV-1a 64 test vectors (isthe.com/chongo/tech/comp/fnv).
        assert_eq!(fnv1a64(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64("foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn fnv1a64_is_deterministic_and_name_sensitive() {
        assert_eq!(fnv1a64("manager-1"), fnv1a64("manager-1"));
        assert_ne!(fnv1a64("manager-1"), fnv1a64("manager-2"));
    }

    #[test]
    fn node_addr_round_trips() {
        let addr = encode_node_addr("manager-1", "10.0.0.5:4646");
        assert_eq!(addr, "manager-1@10.0.0.5:4646");
        assert_eq!(split_node_addr(&addr), ("manager-1", "10.0.0.5:4646"));
        assert_eq!(node_addr_host_port(&addr), "10.0.0.5:4646");
    }

    #[test]
    fn node_addr_tolerates_bare_and_at_sign_names() {
        // Bare host:port (no name) stays dialable.
        assert_eq!(split_node_addr("10.0.0.5:4646"), ("", "10.0.0.5:4646"));
        // A name containing '@' splits on the last one.
        assert_eq!(
            split_node_addr("weird@name@10.0.0.5:4646"),
            ("weird@name", "10.0.0.5:4646")
        );
    }
}
