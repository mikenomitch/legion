//! Sealing published checkpoint parts (docs/suspend-and-resume.md,
//! "Security"; internal_docs/plans/checkpoint-encryption-plan.md).
//!
//! A checkpoint part is the guest's memory, so a published part holds every
//! secret the workload had loaded. Every part a worker publishes to its
//! `contentStore` is sealed first, and every part a worker fetches back is
//! opened and checked against its plaintext digest before it enters the
//! node content store.
//!
//! # Keys
//!
//! ```text
//! checkpoint key ck_…   lives in the manager keyring, wrapped by the root key
//!   │ HKDF-SHA256(info = "sha256:<hex>" of the part's plaintext)
//!   ▼
//! part key              one per digest; the manager sends it to the one
//!   │                   worker publishing, warming, or restoring that part
//!   ├─ HKDF("aes-256-gcm-stream") → the AEAD key
//!   └─ HKDF("object-name")        → the object's name in the store
//! ```
//!
//! The part key is a keyed convergent key: the same bytes always seal to the
//! same object, so publishing is still idempotent and parts still dedupe in
//! the bucket, but nobody without the checkpoint key can derive the key or
//! the name from a guessed plaintext. Workers never see the checkpoint key.
//!
//! A published part is stored as chunks plus an index
//! ([`crate::checkpoint_chunks`]), and two more keys hang off the tree:
//!
//! ```text
//! checkpoint key ck_…
//!   │ HKDF(info = job)             (the session sealing key, below)
//!   ▼
//! job key ─ HKDF("chunks") ─▶ chunk key    sent with a publish, or derived
//!   │                           │          by a session's node
//!   │                           │ HKDF(info = "<window secs>/<window>")
//!   │                           ▼
//!   │                         chunk scope ─ HKDF(chunk digest) ─▶ chunk part key
//!   ▼
//! part key ─ HKDF(part ‖ scope, chunk size) ─▶ index name
//!          └ HKDF(index name)               ─▶ index key
//! ```
//!
//! Chunk keys are keyed convergent too, scoped to one job and one dedup
//! window, so a repeat checkpoint (and every sandbox of the job) reuses the
//! chunks already uploaded in the window. The index lists each chunk's
//! digest and key and is sealed under a key only the part key derives, so a
//! restoring node needs nothing beyond the part key it already receives.
//!
//! # Object format
//!
//! ```text
//! header   "LGNSEAL1" | chunk size (u32 BE) | plaintext length (u64 BE)
//! chunk i  AES-256-GCM(key, nonce_i, aad = header, plaintext chunk i) ‖ tag
//! nonce_i  0x000000 | i (u64 BE) | 0x01 for the last chunk, else 0x00
//! ```
//!
//! This is the STREAM construction age uses: chunks are sealed and opened one
//! at a time, so a multi-GiB part never sits in memory, and reordering,
//! truncation, or a changed header fails authentication. Counter nonces are
//! safe because each key seals exactly one plaintext, the one its digest
//! names. An empty part is one empty, last chunk.

use std::collections::HashMap;
use std::io::{self, Read};

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hkdf::{Salt, HKDF_SHA256};
use zeroize::Zeroizing;

use crate::content_store::ContentDigest;
use crate::prelude::*;
use crate::structs::CheckpointPartKey;

/// First bytes of every sealed object. The trailing `1` is the format
/// version: a new format is a new magic, never a reinterpretation.
pub const SEAL_MAGIC: &[u8; 8] = b"LGNSEAL1";
/// Magic, chunk size, plaintext length.
pub const HEADER_BYTES: usize = 8 + 4 + 8;
/// Plaintext bytes per chunk. One chunk (plus its tag) is the most a sealer
/// or opener holds in memory.
pub const CHUNK_BYTES: usize = 1024 * 1024;
/// The largest chunk size an opener accepts, so a hostile header cannot ask
/// for a huge buffer.
const MAX_CHUNK_BYTES: u32 = 64 * 1024 * 1024;
const TAG_BYTES: usize = 16;
/// Bytes in a checkpoint key and in a part key.
pub const KEY_BYTES: usize = 32;
/// Published objects live under `<contentStore.url>/<OBJECT_PREFIX>/`.
pub const OBJECT_PREFIX: &str = "sealed";
/// `keyring_key.algorithm` of a checkpoint key: it is never used as a
/// cipher key itself, only to derive part keys.
pub const CHECKPOINT_KEY_ALGORITHM: &str = "hkdf-sha256";

const PART_KEY_SALT: &[u8] = b"legion-checkpoint-part-v1";
const PART_SUBKEY_SALT: &[u8] = b"legion-checkpoint-part-subkeys-v1";
const CHUNK_KEY_SALT: &[u8] = b"legion-checkpoint-chunks-v1";
const CHUNK_SCOPE_SALT: &[u8] = b"legion-checkpoint-chunk-scope-v1";
const INDEX_SALT: &[u8] = b"legion-checkpoint-index-v1";
const AEAD_INFO: &[u8] = b"aes-256-gcm-stream";
const NAME_INFO: &[u8] = b"object-name";

fn hkdf(salt: &[u8], ikm: &[u8], info: &[u8]) -> R<Zeroizing<[u8; KEY_BYTES]>> {
    let prk = Salt::new(HKDF_SHA256, salt).extract(ikm);
    let info = [info];
    let okm = prk
        .expand(&info, HKDF_SHA256)
        .map_err(|_| anyhow!("hkdf expand failed"))?;
    let mut out = Zeroizing::new([0u8; KEY_BYTES]);
    okm.fill(out.as_mut())
        .map_err(|_| anyhow!("hkdf fill failed"))?;
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The key for one published part. Its `Debug` never shows the key.
#[derive(Clone)]
pub struct PartKey {
    key_id: String,
    digest: ContentDigest,
    secret: Zeroizing<[u8; KEY_BYTES]>,
}

impl std::fmt::Debug for PartKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartKey")
            .field("key_id", &self.key_id)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl PartKey {
    /// Derive the part key for `digest` from the checkpoint key `key_id`
    /// (the manager side).
    pub fn derive(checkpoint_key: &[u8], key_id: &str, digest: &ContentDigest) -> R<Self> {
        if checkpoint_key.len() != KEY_BYTES {
            bail!(
                "checkpoint key {key_id} is {} bytes, expected {KEY_BYTES}",
                checkpoint_key.len()
            );
        }
        Ok(Self {
            key_id: key_id.to_string(),
            digest: digest.clone(),
            secret: hkdf(PART_KEY_SALT, checkpoint_key, digest.as_str().as_bytes())?,
        })
    }

    /// A part key received from the manager (the worker side).
    pub fn from_proto(wire: &CheckpointPartKey) -> R<Self> {
        let digest = ContentDigest::parse(&wire.digest)?;
        if wire.key.len() != KEY_BYTES {
            bail!(
                "the key for part {digest} is {} bytes, expected {KEY_BYTES}",
                wire.key.len()
            );
        }
        let mut secret = Zeroizing::new([0u8; KEY_BYTES]);
        secret.copy_from_slice(&wire.key);
        Ok(Self {
            key_id: wire.key_id.clone(),
            digest,
            secret,
        })
    }

    pub fn to_proto(&self) -> CheckpointPartKey {
        CheckpointPartKey {
            digest: self.digest.to_string(),
            key_id: self.key_id.clone(),
            key: self.secret.to_vec(),
        }
    }

    /// The checkpoint key this part key comes from (`ck_…`). Recorded in the
    /// manifest as the part's `keyId`.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn digest(&self) -> &ContentDigest {
        &self.digest
    }

    /// The object's name in the content store: 64 hex characters derived
    /// from the key, so it reveals nothing about the plaintext digest.
    pub fn object_name(&self) -> String {
        hex(&self.object_id())
    }

    /// [`Self::object_name`] as raw bytes.
    pub fn object_id(&self) -> [u8; KEY_BYTES] {
        *hkdf(PART_SUBKEY_SALT, self.secret.as_ref(), NAME_INFO).expect("hkdf of a fixed-size key")
    }

    /// A chunk key read back from a part index
    /// ([`crate::checkpoint_chunks`]): the key bytes the publisher recorded
    /// for the chunk whose plaintext hashes to `digest`.
    pub fn from_secret(key_id: &str, digest: ContentDigest, secret: &[u8]) -> R<Self> {
        if secret.len() != KEY_BYTES {
            bail!(
                "the key for chunk {digest} is {} bytes, expected {KEY_BYTES}",
                secret.len()
            );
        }
        let mut bytes = Zeroizing::new([0u8; KEY_BYTES]);
        bytes.copy_from_slice(secret);
        Ok(Self {
            key_id: key_id.to_string(),
            digest,
            secret: bytes,
        })
    }

    /// The raw key, for recording a chunk key inside a sealed part index.
    pub(crate) fn secret(&self) -> &[u8; KEY_BYTES] {
        &self.secret
    }

    /// The name of this part's index object when its chunks are keyed in
    /// `scope` and cut at `chunk_size` bytes ([`crate::checkpoint_chunks`]).
    /// The index lists those chunk keys, so its bytes differ per scope and
    /// chunk size, and so must its name and key: each index key seals
    /// exactly one plaintext.
    pub fn index_object_name(&self, scope: &ChunkScope, chunk_size: u32) -> String {
        let mut ikm = Zeroizing::new([0u8; 2 * KEY_BYTES]);
        ikm[..KEY_BYTES].copy_from_slice(self.secret.as_ref());
        ikm[KEY_BYTES..].copy_from_slice(scope.secret.as_ref());
        let info = format!("index-name:{chunk_size}");
        hex(hkdf(INDEX_SALT, ikm.as_ref(), info.as_bytes())
            .expect("hkdf of fixed-size keys")
            .as_ref())
    }

    /// The key the index object named `object_name` is sealed under. A
    /// reader derives it from the part key and the name in the part's URL,
    /// so the manifest needs no field beyond `url` and `keyId`.
    pub fn index_key(&self, object_name: &str) -> R<PartKey> {
        let info = format!("index-key:{object_name}");
        Ok(PartKey {
            key_id: self.key_id.clone(),
            digest: self.digest.clone(),
            secret: hkdf(INDEX_SALT, self.secret.as_ref(), info.as_bytes())?,
        })
    }

    fn aead_key(&self) -> R<LessSafeKey> {
        let bytes = hkdf(PART_SUBKEY_SALT, self.secret.as_ref(), AEAD_INFO)?;
        let unbound = UnboundKey::new(&AES_256_GCM, bytes.as_ref())
            .map_err(|_| anyhow!("invalid AES-256-GCM key"))?;
        Ok(LessSafeKey::new(unbound))
    }
}

/// The part keys a worker received with one piece of work, by digest, and
/// the manager's reason when it could not send them.
#[derive(Debug, Clone, Default)]
pub struct PartKeys {
    keys: HashMap<ContentDigest, PartKey>,
    /// Why keys are missing: the manager's `part_key_error`, plus any key
    /// that arrived malformed.
    error: Option<String>,
}

impl PartKeys {
    pub fn from_proto(keys: &[CheckpointPartKey], manager_error: &str) -> Self {
        let mut out = PartKeys::default();
        let mut errors = Vec::new();
        if !manager_error.trim().is_empty() {
            errors.push(manager_error.trim().to_string());
        }
        for wire in keys {
            match PartKey::from_proto(wire) {
                Ok(key) => {
                    out.keys.insert(key.digest.clone(), key);
                }
                Err(err) => errors.push(format!("{err:#}")),
            }
        }
        if !errors.is_empty() {
            out.error = Some(errors.join("; "));
        }
        out
    }

    pub fn from_keys(keys: impl IntoIterator<Item = PartKey>) -> Self {
        PartKeys {
            keys: keys
                .into_iter()
                .map(|key| (key.digest.clone(), key))
                .collect(),
            error: None,
        }
    }

    /// The key for `digest`, or an error saying why the manager sent none.
    pub fn for_part(&self, digest: &ContentDigest) -> R<&PartKey> {
        if let Some(key) = self.keys.get(digest) {
            return Ok(key);
        }
        match self.error.as_deref() {
            Some(reason) => bail!("the manager sent no key for checkpoint part {digest}: {reason}"),
            None => bail!(
                "the manager sent no key for checkpoint part {digest} (a manager older than \
                 this worker sends none; upgrade the managers first)"
            ),
        }
    }
}

const SESSION_KEY_SALT: &[u8] = b"legion-session-seal-v1";

/// The id of a job's session sealing key derived from checkpoint key
/// `checkpoint_key_id`: recorded as the `keyId` of every part sealed under
/// it.
pub fn session_seal_key_id(checkpoint_key_id: &str, job_id: &str) -> String {
    format!("{checkpoint_key_id}/session/{job_id}")
}

/// Derive a job's session sealing key from a checkpoint key (the manager
/// side; session durability plan §6.6). An unpromoted `store` session
/// publishes with no manager on the path, so its node derives part keys
/// from this per-job key instead of receiving one per digest: the same
/// HKDF step the manager uses for keyed parts, one level down.
pub fn derive_session_seal_key(
    checkpoint_key: &[u8],
    checkpoint_key_id: &str,
    job_id: &str,
) -> R<crate::structs::SessionSealKey> {
    if checkpoint_key.len() != KEY_BYTES {
        bail!(
            "checkpoint key {checkpoint_key_id} is {} bytes, expected {KEY_BYTES}",
            checkpoint_key.len()
        );
    }
    let secret = hkdf(SESSION_KEY_SALT, checkpoint_key, job_id.as_bytes())?;
    Ok(crate::structs::SessionSealKey {
        key_id: session_seal_key_id(checkpoint_key_id, job_id),
        key: secret.to_vec(),
    })
}

/// The session sealing keys a node holds for one job, the current one
/// first. Its `Debug` never shows a key.
#[derive(Clone, Default)]
pub struct SessionSealKeys {
    keys: Vec<(String, Zeroizing<[u8; KEY_BYTES]>)>,
}

impl std::fmt::Debug for SessionSealKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.keys.iter().map(|(id, _)| id))
            .finish()
    }
}

impl SessionSealKeys {
    /// The keys an envelope payload carried. Malformed ones are skipped.
    pub fn from_proto(keys: &[crate::structs::SessionSealKey]) -> Self {
        let keys = keys
            .iter()
            .filter(|key| key.key.len() == KEY_BYTES && !key.key_id.is_empty())
            .map(|key| {
                let mut secret = Zeroizing::new([0u8; KEY_BYTES]);
                secret.copy_from_slice(&key.key);
                (key.key_id.clone(), secret)
            })
            .collect();
        SessionSealKeys { keys }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The id of the key new parts are sealed under.
    pub fn current_key_id(&self) -> Option<&str> {
        self.keys.first().map(|(id, _)| id.as_str())
    }

    /// The part key to seal `digest` under: derived from the current key.
    pub fn seal_key(&self, digest: &ContentDigest) -> R<PartKey> {
        let (key_id, secret) = self.keys.first().context(
            "this node holds no session sealing key for the job (its envelope payload carried \
             none: the manager's keyring has no checkpoint key, or is locked)",
        )?;
        PartKey::derive(secret.as_ref(), key_id, digest)
    }

    /// The part key to open a part sealed under `key_id`.
    pub fn open_key(&self, key_id: &str, digest: &ContentDigest) -> R<PartKey> {
        let (_, secret) = self
            .keys
            .iter()
            .find(|(id, _)| id == key_id)
            .with_context(|| {
                format!(
                    "this node holds no session sealing key {key_id} (the checkpoint key it \
                     derives from left the keyring, or the job's envelope payload has not \
                     reached this node)"
                )
            })?;
        PartKey::derive(secret.as_ref(), key_id, digest)
    }

    /// The part keys that open every published memory part of `manifest`,
    /// in the shape a restore carries, with why any is missing.
    pub fn part_keys_for(
        &self,
        manifest: &crate::structs::checkpoint::SnapshotManifest,
    ) -> (Vec<CheckpointPartKey>, String) {
        let mut keys = Vec::new();
        let mut errors = Vec::new();
        for part in &manifest.memory {
            let Some(key_id) = part.key_id.as_deref() else {
                continue;
            };
            match ContentDigest::parse(&part.digest)
                .and_then(|digest| self.open_key(key_id, &digest))
            {
                Ok(key) => keys.push(key.to_proto()),
                Err(err) => errors.push(format!("{err:#}")),
            }
        }
        errors.sort();
        errors.dedup();
        (keys, errors.join("; "))
    }

    /// The job's chunk key, derived from the current session sealing key:
    /// the same key the manager derives for the job's keyed snapshots, so
    /// session and snapshot checkpoints of one job share chunks.
    pub fn chunk_key(&self) -> R<ChunkKey> {
        let (key_id, secret) = self.keys.first().context(
            "this node holds no session sealing key for the job (its envelope payload carried \
             none: the manager's keyring has no checkpoint key, or is locked)",
        )?;
        ChunkKey::from_job_secret(key_id, secret.as_ref())
    }

    /// Back to the wire shape.
    pub fn to_proto(&self) -> Vec<crate::structs::SessionSealKey> {
        self.keys
            .iter()
            .map(|(key_id, secret)| crate::structs::SessionSealKey {
                key_id: key_id.clone(),
                key: secret.to_vec(),
            })
            .collect()
    }
}

/// A job's chunk key: what a publishing node derives the key of every chunk
/// of the job's checkpoints from ([`crate::checkpoint_chunks`]). It is one
/// HKDF step below the job's session sealing key, which is one below the
/// checkpoint key, so the manager (for a keyed snapshot) and a session's
/// node (with no manager on the path) derive the same one. A node holding
/// it can name and seal chunks of that job, and nothing else: it cannot
/// open another checkpoint's index, whose key comes from the part key.
/// Its `Debug` never shows the key.
#[derive(Clone)]
pub struct ChunkKey {
    key_id: String,
    secret: Zeroizing<[u8; KEY_BYTES]>,
}

impl std::fmt::Debug for ChunkKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl ChunkKey {
    /// Derive job `job_id`'s chunk key from checkpoint key
    /// `checkpoint_key_id` (the manager side).
    pub fn derive(checkpoint_key: &[u8], checkpoint_key_id: &str, job_id: &str) -> R<Self> {
        let session = derive_session_seal_key(checkpoint_key, checkpoint_key_id, job_id)?;
        Self::from_job_secret(&session.key_id, &session.key)
    }

    /// From a job's session sealing key (`<ck>/session/<job>`).
    fn from_job_secret(session_key_id: &str, secret: &[u8]) -> R<Self> {
        Ok(Self {
            key_id: format!("{session_key_id}/chunks"),
            secret: hkdf(CHUNK_KEY_SALT, secret, b"chunks")?,
        })
    }

    /// A chunk key received from the manager with a publish.
    pub fn from_proto(wire: &crate::structs::CheckpointChunkKey) -> R<Self> {
        if wire.key.len() != KEY_BYTES || wire.key_id.is_empty() {
            bail!(
                "the chunk key {:?} is {} bytes, expected {KEY_BYTES}",
                wire.key_id,
                wire.key.len()
            );
        }
        let mut secret = Zeroizing::new([0u8; KEY_BYTES]);
        secret.copy_from_slice(&wire.key);
        Ok(Self {
            key_id: wire.key_id.clone(),
            secret,
        })
    }

    pub fn to_proto(&self) -> crate::structs::CheckpointChunkKey {
        crate::structs::CheckpointChunkKey {
            key_id: self.key_id.clone(),
            key: self.secret.to_vec(),
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The scope chunks are keyed in during dedup window `window` of
    /// `window_secs` seconds. Chunks dedupe only inside one scope, so no
    /// chunk is reused by a publish more than one window after it was
    /// uploaded (docs/suspend-and-resume.md, "How published parts are
    /// stored").
    pub fn scope(&self, window_secs: u64, window: u64) -> R<ChunkScope> {
        let info = format!("{window_secs}/{window}");
        Ok(ChunkScope {
            label: format!("{}@{info}", self.key_id),
            secret: hkdf(CHUNK_SCOPE_SALT, self.secret.as_ref(), info.as_bytes())?,
        })
    }
}

/// The keys of one dedup window of one job's chunks. Its `Debug` never shows
/// the key.
pub struct ChunkScope {
    /// `<chunk key id>@<window secs>/<window>`, for logs.
    label: String,
    secret: Zeroizing<[u8; KEY_BYTES]>,
}

impl std::fmt::Debug for ChunkScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkScope")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl ChunkScope {
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The key of the chunk whose plaintext hashes to `digest`: a keyed
    /// convergent key, like a part key one level down, so the same chunk
    /// in the same scope seals to the same object.
    pub fn chunk_key(&self, digest: &ContentDigest) -> R<PartKey> {
        PartKey::derive(self.secret.as_ref(), "chunk", digest)
    }
}

/// Seal `plaintext` (a chunk or a part index, at most a few MiB) under `key`
/// in memory.
pub fn seal_bytes(key: &PartKey, plaintext: &[u8]) -> R<Vec<u8>> {
    let mut reader = SealingReader::new(key, plaintext, plaintext.len() as u64)?;
    let mut out = Vec::with_capacity(sealed_len(plaintext.len() as u64) as usize);
    reader.write_all_to(&mut out)?;
    Ok(out)
}

/// Open a sealed object held in memory. The caller checks the plaintext
/// against its digest.
pub fn open_bytes(key: &PartKey, sealed: &[u8]) -> R<Vec<u8>> {
    let actual = sealed.len() as u64;
    let have = sealed.len().min(HEADER_BYTES);
    let (head, chunk, plaintext_len, chunks) = parse_header(&sealed[..have], actual)?;
    let key = key.aead_key()?;
    let mut out = Vec::with_capacity(plaintext_len as usize);
    let mut buf = Zeroizing::new(Vec::with_capacity(chunk as usize + TAG_BYTES));
    let mut read_at = HEADER_BYTES;
    for index in 0..chunks {
        let len = chunk_len(plaintext_len, chunk, index) + TAG_BYTES;
        buf.clear();
        buf.extend_from_slice(&sealed[read_at..read_at + len]);
        let plain = key
            .open_in_place(nonce(index, index + 1 == chunks), Aad::from(head), &mut buf)
            .map_err(|_| SealError::Authentication { chunk: index })?;
        out.extend_from_slice(plain);
        read_at += len;
    }
    Ok(out)
}

/// Check a sealed object's header against its total size `actual`. Returns
/// the header, the chunk size, the plaintext length, and the chunk count.
fn parse_header(head: &[u8], actual: u64) -> R<([u8; HEADER_BYTES], u64, u64, u64)> {
    let have = head.len();
    if have < SEAL_MAGIC.len() || &head[..SEAL_MAGIC.len()] != SEAL_MAGIC {
        return Err(SealError::NotSealed.into());
    }
    if have < HEADER_BYTES {
        return Err(SealError::Truncated {
            expected: HEADER_BYTES as u64,
            actual,
        }
        .into());
    }
    let head: [u8; HEADER_BYTES] = head[..HEADER_BYTES].try_into().expect("header bytes");
    let chunk = u32::from_be_bytes(head[8..12].try_into().expect("4 bytes"));
    if chunk == 0 || chunk > MAX_CHUNK_BYTES {
        return Err(SealError::Unsupported(format!("chunk size {chunk}")).into());
    }
    let plaintext_len = u64::from_be_bytes(head[12..20].try_into().expect("8 bytes"));
    let chunk = u64::from(chunk);
    let chunks = chunk_count(plaintext_len, chunk);
    let expected = (HEADER_BYTES as u64)
        .checked_add(plaintext_len)
        .and_then(|n| n.checked_add(chunks * TAG_BYTES as u64));
    if expected != Some(actual) {
        return Err(SealError::Truncated {
            expected: expected.unwrap_or(u64::MAX),
            actual,
        }
        .into());
    }
    Ok((head, chunk, plaintext_len, chunks))
}

/// Why a sealed object could not be opened. Each one names what happened,
/// so an operator can tell tampering from a plaintext leftover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// No `LGNSEAL1` header: a plaintext object, or not a part at all.
    NotSealed,
    /// A header this build does not read (a chunk size out of range).
    Unsupported(String),
    /// The object is shorter or longer than its header says.
    Truncated { expected: u64, actual: u64 },
    /// A chunk failed AEAD authentication: modified, reordered, or sealed
    /// under a different key.
    Authentication { chunk: u64 },
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SealError::NotSealed => write!(
                f,
                "the object is not a sealed checkpoint part (no LGNSEAL1 header); it may have \
                 been published before checkpoint encryption, which Legion no longer reads"
            ),
            SealError::Unsupported(what) => {
                write!(
                    f,
                    "the sealed checkpoint part has an unsupported header: {what}"
                )
            }
            SealError::Truncated { expected, actual } => write!(
                f,
                "the sealed checkpoint part is truncated or has trailing bytes ({actual} bytes, \
                 its header says {expected})"
            ),
            SealError::Authentication { chunk } => write!(
                f,
                "the sealed checkpoint part failed authentication at chunk {chunk}: it was \
                 modified, or sealed under a different key"
            ),
        }
    }
}

impl std::error::Error for SealError {}

fn chunk_count(plaintext_len: u64, chunk: u64) -> u64 {
    plaintext_len.div_ceil(chunk).max(1)
}

/// Plaintext bytes in chunk `index`.
fn chunk_len(plaintext_len: u64, chunk: u64, index: u64) -> usize {
    plaintext_len.saturating_sub(index * chunk).min(chunk) as usize
}

fn nonce(index: u64, last: bool) -> Nonce {
    let mut bytes = [0u8; 12];
    bytes[3..11].copy_from_slice(&index.to_be_bytes());
    bytes[11] = u8::from(last);
    Nonce::assume_unique_for_key(bytes)
}

fn header(chunk: u32, plaintext_len: u64) -> [u8; HEADER_BYTES] {
    let mut out = [0u8; HEADER_BYTES];
    out[..8].copy_from_slice(SEAL_MAGIC);
    out[8..12].copy_from_slice(&chunk.to_be_bytes());
    out[12..].copy_from_slice(&plaintext_len.to_be_bytes());
    out
}

/// The size of the sealed object for a plaintext of `plaintext_len` bytes.
pub fn sealed_len(plaintext_len: u64) -> u64 {
    HEADER_BYTES as u64
        + plaintext_len
        + chunk_count(plaintext_len, CHUNK_BYTES as u64) * TAG_BYTES as u64
}

/// Seals a plaintext stream as it is read: yields the header, then each
/// sealed chunk. Hashes the plaintext on the way through, so the publisher
/// learns whether the bytes it sealed are the bytes the digest names without
/// a separate pass ([`SealingReader::finish`]).
pub struct SealingReader<S: Read> {
    inner: S,
    key: LessSafeKey,
    header: [u8; HEADER_BYTES],
    plaintext_len: u64,
    chunks: u64,
    next_chunk: u64,
    /// Sealed bytes ready to hand out, and how many were handed out.
    out: Vec<u8>,
    pos: usize,
    /// `ring`'s SHA-256 uses the CPU's SHA instructions, which is several
    /// times faster than the `sha2` crate here, and this hash runs over
    /// every published byte.
    hasher: ring::digest::Context,
    finished: bool,
}

impl<S: Read> SealingReader<S> {
    /// `plaintext_len` must be exactly what `inner` yields; a source that
    /// ends early or runs long fails the read.
    pub fn new(key: &PartKey, inner: S, plaintext_len: u64) -> R<Self> {
        let header = header(CHUNK_BYTES as u32, plaintext_len);
        Ok(Self {
            inner,
            key: key.aead_key()?,
            header,
            plaintext_len,
            chunks: chunk_count(plaintext_len, CHUNK_BYTES as u64),
            next_chunk: 0,
            out: header.to_vec(),
            pos: 0,
            hasher: ring::digest::Context::new(&ring::digest::SHA256),
            finished: false,
        })
    }

    /// Seal the next chunk into `out`.
    fn fill(&mut self) -> io::Result<()> {
        let index = self.next_chunk;
        let len = chunk_len(self.plaintext_len, CHUNK_BYTES as u64, index);
        self.out.clear();
        self.out.resize(len, 0);
        self.inner.read_exact(&mut self.out).map_err(|err| {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the part is shorter than its size; it changed while being sealed",
                )
            } else {
                err
            }
        })?;
        self.hasher.update(&self.out);
        let last = index + 1 == self.chunks;
        if last {
            let mut probe = [0u8; 1];
            if self.inner.read(&mut probe)? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the part is longer than its size; it changed while being sealed",
                ));
            }
        }
        self.key
            .seal_in_place_append_tag(nonce(index, last), Aad::from(self.header), &mut self.out)
            .map_err(|_| io::Error::other("AES-256-GCM seal failed"))?;
        self.pos = 0;
        self.next_chunk += 1;
        self.finished = last;
        Ok(())
    }

    /// The digest of the plaintext that was sealed. Call after the reader
    /// returned 0 (end of stream).
    pub fn finish(self) -> R<ContentDigest> {
        if !self.finished || self.pos < self.out.len() {
            bail!("the sealed stream was not read to its end");
        }
        ContentDigest::parse(&format!("sha256:{}", hex(self.hasher.finish().as_ref())))
    }
}

impl<S: Read> SealingReader<S> {
    /// The next run of sealed bytes, as large as one whole sealed chunk (or
    /// the header), or `None` at the end. Writing these straight to the
    /// destination avoids the small reads `io::copy` would make, which cost
    /// more than the cipher on a multi-GiB part.
    pub fn next_sealed(&mut self) -> io::Result<Option<&[u8]>> {
        if self.pos == self.out.len() {
            if self.finished {
                return Ok(None);
            }
            self.fill()?;
        }
        let start = self.pos;
        self.pos = self.out.len();
        Ok(Some(&self.out[start..]))
    }

    /// Seal the whole stream into `out`. Returns the sealed bytes written.
    pub fn write_all_to(&mut self, out: &mut impl io::Write) -> io::Result<u64> {
        let mut written = 0u64;
        while let Some(sealed) = self.next_sealed()? {
            out.write_all(sealed)?;
            written += sealed.len() as u64;
        }
        Ok(written)
    }
}

impl<S: Read> Read for SealingReader<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.out.len() {
            if self.finished {
                return Ok(0);
            }
            self.fill()?;
        }
        let n = buf.len().min(self.out.len() - self.pos);
        buf[..n].copy_from_slice(&self.out[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn checkpoint_key(byte: u8) -> [u8; KEY_BYTES] {
        [byte; KEY_BYTES]
    }

    fn part_key_for(bytes: &[u8]) -> PartKey {
        PartKey::derive(
            &checkpoint_key(7),
            "ck_test",
            &ContentDigest::of_bytes(bytes),
        )
        .unwrap()
    }

    fn seal(key: &PartKey, bytes: &[u8]) -> R<Vec<u8>> {
        let mut reader = SealingReader::new(key, bytes, bytes.len() as u64)?;
        let mut out = Vec::new();
        reader.read_to_end(&mut out)?;
        assert_eq!(reader.finish()?, ContentDigest::of_bytes(bytes));
        Ok(out)
    }

    fn open(key: &PartKey, sealed: &[u8]) -> R<Vec<u8>> {
        open_bytes(key, sealed)
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 + i / 7) as u8).collect()
    }

    fn seal_error(err: &anyhow::Error) -> Option<SealError> {
        err.downcast_ref::<SealError>().cloned()
    }

    #[test]
    fn round_trips_across_chunk_boundaries() -> R<()> {
        for len in [
            0,
            1,
            CHUNK_BYTES - 1,
            CHUNK_BYTES,
            CHUNK_BYTES + 1,
            3 * CHUNK_BYTES,
            3 * CHUNK_BYTES + 17,
        ] {
            let bytes = pattern(len);
            let key = part_key_for(&bytes);
            let sealed = seal(&key, &bytes)?;
            assert_eq!(sealed.len() as u64, sealed_len(len as u64), "len {len}");
            assert_eq!(&sealed[..8], SEAL_MAGIC);
            assert_eq!(open(&key, &sealed)?, bytes, "len {len}");
        }
        Ok(())
    }

    #[test]
    fn sealed_bytes_hide_the_plaintext_and_are_deterministic() -> R<()> {
        let bytes = b"AWS_SECRET_ACCESS_KEY=hunter2 ".repeat(4096);
        let key = part_key_for(&bytes);
        let sealed = seal(&key, &bytes)?;
        assert!(
            !sealed
                .windows(29)
                .any(|w| w == b"AWS_SECRET_ACCESS_KEY=hunter2"),
            "no plaintext in the object"
        );
        // Same key, same bytes: the same object, so a re-publish is a no-op.
        assert_eq!(seal(&key, &bytes)?, sealed);
        assert_eq!(part_key_for(&bytes).object_name(), key.object_name());
        // The object name is not the plaintext digest.
        assert_ne!(key.object_name(), ContentDigest::of_bytes(&bytes).hex());
        assert_eq!(key.object_name().len(), 64);
        Ok(())
    }

    #[test]
    fn keys_depend_on_the_checkpoint_key_and_the_digest() -> R<()> {
        let digest = ContentDigest::of_bytes(b"x");
        let a = PartKey::derive(&checkpoint_key(1), "ck_a", &digest)?;
        let b = PartKey::derive(&checkpoint_key(2), "ck_b", &digest)?;
        let c = PartKey::derive(&checkpoint_key(1), "ck_a", &ContentDigest::of_bytes(b"y"))?;
        assert_ne!(a.object_name(), b.object_name());
        assert_ne!(a.object_name(), c.object_name());
        // The wire form round trips, and Debug never shows the key.
        let wire = a.to_proto();
        let back = PartKey::from_proto(&wire)?;
        assert_eq!(back.object_name(), a.object_name());
        assert_eq!(back.key_id(), "ck_a");
        let shown = format!("{a:?} {wire:?}");
        assert!(!shown.contains(&format!("{:?}", wire.key)), "{shown}");
        assert!(!shown.contains(&hex(&wire.key)), "{shown}");
        Ok(())
    }

    #[test]
    fn tampering_anywhere_fails_authentication_by_name() -> R<()> {
        let bytes = pattern(2 * CHUNK_BYTES + 100);
        let key = part_key_for(&bytes);
        let sealed = seal(&key, &bytes)?;
        // A flipped bit in the first chunk, a later chunk, and a tag.
        for (offset, chunk) in [
            (HEADER_BYTES + 5, 0),
            (HEADER_BYTES + CHUNK_BYTES + TAG_BYTES + 9, 1),
            (sealed.len() - 1, 2),
        ] {
            let mut bad = sealed.clone();
            bad[offset] ^= 0x01;
            let err = open(&key, &bad).unwrap_err();
            assert_eq!(seal_error(&err), Some(SealError::Authentication { chunk }));
            assert!(format!("{err:#}").contains("modified"), "{err:#}");
        }
        // Swapping two full chunks fails at the first one moved.
        let mut swapped = sealed.clone();
        let stride = CHUNK_BYTES + TAG_BYTES;
        let (first, second) = (HEADER_BYTES, HEADER_BYTES + stride);
        let chunk0 = sealed[first..first + stride].to_vec();
        swapped.copy_within(second..second + stride, first);
        swapped[second..second + stride].copy_from_slice(&chunk0);
        assert_eq!(
            seal_error(&open(&key, &swapped).unwrap_err()),
            Some(SealError::Authentication { chunk: 0 })
        );
        // A header claiming a different length with matching padding still
        // fails: the header is every chunk's associated data.
        let mut relabeled = sealed.clone();
        relabeled[12..20].copy_from_slice(&(bytes.len() as u64 + 1).to_be_bytes());
        relabeled.push(0);
        assert_eq!(
            seal_error(&open(&key, &relabeled).unwrap_err()),
            Some(SealError::Authentication { chunk: 0 })
        );
        Ok(())
    }

    #[test]
    fn truncation_at_and_inside_a_chunk_boundary_is_named() -> R<()> {
        let bytes = pattern(2 * CHUNK_BYTES);
        let key = part_key_for(&bytes);
        let sealed = seal(&key, &bytes)?;
        for cut in [
            sealed.len() - 1,
            HEADER_BYTES + CHUNK_BYTES + TAG_BYTES,
            HEADER_BYTES,
            12,
        ] {
            let err = open(&key, &sealed[..cut]).unwrap_err();
            assert!(
                matches!(seal_error(&err), Some(SealError::Truncated { .. })),
                "cut {cut}: {err:#}"
            );
        }
        let mut longer = sealed.clone();
        longer.extend_from_slice(b"extra");
        assert!(matches!(
            seal_error(&open(&key, &longer).unwrap_err()),
            Some(SealError::Truncated { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_wrong_key_and_a_plaintext_object_fail_by_name() -> R<()> {
        let bytes = pattern(1000);
        let key = part_key_for(&bytes);
        let sealed = seal(&key, &bytes)?;
        let other = PartKey::derive(&checkpoint_key(9), "ck_other", key.digest())?;
        let err = open(&other, &sealed).unwrap_err();
        assert_eq!(
            seal_error(&err),
            Some(SealError::Authentication { chunk: 0 })
        );

        let err = open(&key, &bytes).unwrap_err();
        assert_eq!(seal_error(&err), Some(SealError::NotSealed));
        assert!(
            format!("{err:#}").contains("published before checkpoint encryption"),
            "{err:#}"
        );
        let err = open(&key, b"").unwrap_err();
        assert_eq!(seal_error(&err), Some(SealError::NotSealed));
        Ok(())
    }

    #[test]
    fn a_source_that_changes_size_while_sealing_fails() -> R<()> {
        let bytes = pattern(CHUNK_BYTES + 10);
        let key = part_key_for(&bytes);
        let mut short = SealingReader::new(&key, &bytes[..100], bytes.len() as u64)?;
        assert!(io::copy(&mut short, &mut io::sink()).is_err());
        let mut long = SealingReader::new(&key, &bytes[..], 100)?;
        assert!(io::copy(&mut long, &mut io::sink()).is_err());
        Ok(())
    }

    /// A 64 MiB part streams through a file in and a file out with one
    /// chunk in memory, and opens in place.
    #[test]
    fn a_large_part_streams_through_files() -> R<()> {
        let dir = tempfile::tempdir()?;
        let plain_path = dir.path().join("plain");
        {
            let mut out = io::BufWriter::new(fs::File::create(&plain_path)?);
            for i in 0..64u32 {
                io::Write::write_all(&mut out, &pattern(CHUNK_BYTES))?;
                io::Write::write_all(&mut out, &i.to_be_bytes())?;
            }
        }
        let len = fs::metadata(&plain_path)?.len();
        let digest = ContentDigest::of_file(&plain_path)?;
        let key = PartKey::derive(&checkpoint_key(3), "ck_big", &digest)?;
        let sealed_path = dir.path().join("sealed");
        let mut reader = SealingReader::new(&key, fs::File::open(&plain_path)?, len)?;
        let written = reader.write_all_to(&mut fs::File::create(&sealed_path)?)?;
        assert_eq!(written, sealed_len(len));
        assert_eq!(reader.finish()?, digest);
        assert_eq!(fs::metadata(&sealed_path)?.len(), sealed_len(len));
        let plain = open_bytes(&key, &fs::read(&sealed_path)?)?;
        assert_eq!(plain.len() as u64, len);
        assert_eq!(ContentDigest::of_bytes(&plain), digest);
        Ok(())
    }

    #[test]
    fn missing_keys_say_why() {
        let digest = ContentDigest::of_bytes(b"part");
        let keys = PartKeys::from_proto(&[], "keyring locked: root key rk_1 is missing");
        let err = keys.for_part(&digest).unwrap_err();
        assert!(format!("{err:#}").contains("keyring locked"), "{err:#}");
        let keys = PartKeys::from_proto(
            &[CheckpointPartKey {
                digest: digest.to_string(),
                key_id: "ck_x".into(),
                key: vec![1, 2, 3],
            }],
            "",
        );
        let err = keys.for_part(&digest).unwrap_err();
        assert!(format!("{err:#}").contains("3 bytes"), "{err:#}");
    }
}
