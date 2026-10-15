//! KV encryption at rest — the keyring (docs/kv-encryption.md).
//!
//! Two tiers of keys, three names used everywhere (config, CLI, errors):
//!
//! - **root key** — the key-encryption key. Lives OUTSIDE the store: a file
//!   on each manager (`file`, the default — minted on first start), an
//!   environment variable (`env`), or an external KMS reached through a
//!   command (`exec`). Never written to the database, a changeset, a
//!   snapshot, or a backup.
//! - **data keys** — 256-bit AES keys that encrypt KV values. Live INSIDE
//!   the store, wrapped by the root key, in the replicated `keyring_key`
//!   table. Exactly one is `active`; older ones stay `retired` until every
//!   value has been rewrapped, then are garbage-collected.
//! - **keyring** — the data keys plus the provider that unwraps them.
//!
//! A stored value is `base64(nonce ‖ ciphertext ‖ tag)` under AES-256-GCM
//! with the KV key path as additional authenticated data, and
//! `kv_entry.enc_key_id` names the data key. A `NULL` `enc_key_id` is a
//! plaintext row (written before the feature, or with encryption off).
//!
//! This module owns the cryptography and the in-memory key state. The
//! `keyring_key` table, the leader-only initialization/rotation/rewrap
//! writes, and the RPC surface live in `store.rs` / `server.rs` / the daemon.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::structs::config::{EncryptionConfigFull, RootKeyConfigFull};

/// The one value cipher this release writes. A future cipher is a new key
/// id whose `keyring_key.algorithm` names it — never a new envelope format.
pub const DATA_KEY_ALGORITHM: &str = "aes-256-gcm";
/// Directory under `storePath` holding the file provider's root key.
pub const ROOT_KEY_DIR: &str = "keys";
/// File name of the file provider's root key.
pub const ROOT_KEY_FILE: &str = "root.key";
/// Environment variable the `env` provider reads by default.
pub const DEF_ROOT_KEY_ENV: &str = "LEGION_ROOT_KEY";
/// Raw key length for root and data keys (AES-256).
pub const KEY_BYTES: usize = 32;
/// Encryptions under one data key after which the leader rotates it on its
/// own: half of the 2^32 random-IV bound in NIST SP 800-38D §8.3. Counted in
/// memory per process; time-based rotation (`dataKeyRotationAge`) is what
/// actually fires in practice, this is the safety net.
pub const DATA_KEY_ENCRYPTION_LIMIT: u64 = 1 << 31;

const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const ROOT_KEY_ID_PREFIX: &str = "rk_";
const DATA_KEY_ID_PREFIX: &str = "dk_";
const CHECKPOINT_KEY_ID_PREFIX: &str = "ck_";
/// Header written into a freshly minted root key file, so the file explains
/// itself to whoever finds it in a backup rotation or a config repo.
const ROOT_KEY_FILE_HEADER: &str = "# Legion KV root key (docs/kv-encryption.md).\n\
# One base64 key per line: the FIRST line wraps new data keys, EVERY line can unwrap.\n\
# Keep this file with your backups and on every manager. Never commit it.\n";

/// Provider kinds accepted by `encryption.rootKey.provider`.
pub const PROVIDER_FILE: &str = "file";
pub const PROVIDER_ENV: &str = "env";
pub const PROVIDER_EXEC: &str = "exec";

/// Count one root key provider call for `legion_keyring_provider_calls_total`.
fn count_provider_call(op: &str, ok: bool) {
    crate::manager::telemetry::metrics()
        .keyring_provider_calls_total
        .inc(&[op, if ok { "ok" } else { "error" }]);
}

// ── ids and randomness ───────────────────────────────────────────────────

fn random_bytes(len: usize) -> R<Vec<u8>> {
    let mut buf = vec![0u8; len];
    SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| anyhow!("system randomness unavailable"))?;
    Ok(buf)
}

/// A root key's public id: a fingerprint of the key bytes, so a key file
/// needs no metadata and two managers holding the same bytes agree on the
/// name without coordination.
pub fn root_key_id(key: &[u8]) -> String {
    let digest = Sha256::digest(key);
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("{ROOT_KEY_ID_PREFIX}{}", &hex[..16])
}

/// A fresh data key id (`dk_` + 16 hex chars of randomness).
pub fn new_data_key_id() -> R<String> {
    new_key_id(DATA_KEY_ID_PREFIX)
}

fn new_key_id(prefix: &str) -> R<String> {
    let bytes = random_bytes(8)?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("{prefix}{hex}"))
}

/// One new root key line, ready to be placed FIRST in a root key file or in
/// `LEGION_ROOT_KEY` (`legion keyring root-key generate`).
pub fn generate_root_key_line() -> R<String> {
    Ok(B64.encode(random_bytes(KEY_BYTES)?))
}

// ── AES-256-GCM primitives ───────────────────────────────────────────────

fn aead_key(key: &[u8]) -> R<LessSafeKey> {
    let unbound = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|_| anyhow!("key material is not {KEY_BYTES} bytes"))?;
    Ok(LessSafeKey::new(unbound))
}

/// `nonce ‖ ciphertext ‖ tag` under `key`, bound to `aad`.
fn seal_bytes(key: &[u8], aad: &[u8], plaintext: &[u8]) -> R<Vec<u8>> {
    let key = aead_key(key)?;
    let nonce_bytes = random_bytes(NONCE_BYTES)?;
    let nonce =
        Nonce::try_assume_unique_for_key(&nonce_bytes).map_err(|_| anyhow!("bad nonce length"))?;
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::from(aad), &mut in_out)
        .map_err(|_| anyhow!("encryption failed"))?;
    let mut out = Vec::with_capacity(NONCE_BYTES + in_out.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&in_out);
    Ok(out)
}

/// Inverse of [`seal_bytes`]; fails on a wrong key, a wrong `aad`, or any
/// modification of the ciphertext.
fn open_bytes(key: &[u8], aad: &[u8], sealed: &[u8]) -> R<Zeroizing<Vec<u8>>> {
    if sealed.len() < NONCE_BYTES + TAG_BYTES {
        bail!("ciphertext is too short to be an AES-256-GCM envelope");
    }
    let key = aead_key(key)?;
    let (nonce_bytes, body) = sealed.split_at(NONCE_BYTES);
    let nonce =
        Nonce::try_assume_unique_for_key(nonce_bytes).map_err(|_| anyhow!("bad nonce length"))?;
    let mut in_out = Zeroizing::new(body.to_vec());
    let len = key
        .open_in_place(nonce, Aad::from(aad), &mut in_out)
        .map_err(|_| anyhow!("authentication failed (wrong key, wrong path, or tampered)"))?
        .len();
    in_out.truncate(len);
    Ok(in_out)
}

// ── root key material shared by the file and env providers ──────────────

/// Parsed root key text: one base64 key per line, `#` comments and blank
/// lines ignored. The first key wraps; every key unwraps.
struct LocalRootKeys {
    keys: Vec<Zeroizing<Vec<u8>>>,
    ids: Vec<String>,
}

impl std::fmt::Debug for LocalRootKeys {
    /// Ids only — key bytes never reach a log or a panic message.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalRootKeys")
            .field("ids", &self.ids)
            .finish()
    }
}

impl LocalRootKeys {
    fn parse(text: &str, source: &str) -> R<Self> {
        let mut keys = Vec::new();
        let mut ids = Vec::new();
        for (line_no, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let bytes = B64
                .decode(line)
                .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(line))
                .with_context(|| format!("{source} line {}: not a base64 root key", line_no + 1))?;
            if bytes.len() != KEY_BYTES {
                bail!(
                    "{source} line {}: root key must be {KEY_BYTES} bytes, got {}",
                    line_no + 1,
                    bytes.len()
                );
            }
            ids.push(root_key_id(&bytes));
            keys.push(Zeroizing::new(bytes));
        }
        if keys.is_empty() {
            bail!("{source} holds no root key (one base64 line of 32 random bytes is required)");
        }
        Ok(Self { keys, ids })
    }

    fn wrapping(&self) -> (&[u8], &str) {
        (&self.keys[0], &self.ids[0])
    }

    fn by_id(&self, id: &str) -> Option<&[u8]> {
        self.ids
            .iter()
            .position(|candidate| candidate == id)
            .map(|index| self.keys[index].as_slice())
    }
}

/// Wrap a data key under a local root key: AES-256-GCM, bound to the data
/// key's id so a wrapped blob cannot be moved between keyring rows.
fn local_wrap(root_key: &[u8], key_id: &str, dek: &[u8]) -> R<Vec<u8>> {
    seal_bytes(root_key, key_id.as_bytes(), dek)
}

fn local_unwrap(root_key: &[u8], key_id: &str, wrapped: &[u8]) -> R<Zeroizing<Vec<u8>>> {
    open_bytes(root_key, key_id.as_bytes(), wrapped)
}

// ── the provider contract ────────────────────────────────────────────────

/// Where the root key lives and how data keys are wrapped with it. Called
/// only at start, on keyring changes, and on rotation — never on a KV read
/// or write, so a slow or unavailable provider cannot touch KV traffic.
pub trait RootKeyProvider: Send + Sync {
    /// `file` | `env` | `exec`.
    fn kind(&self) -> &'static str;
    /// Where the key is, for status output and error messages (a path, a
    /// variable name, a command).
    fn location(&self) -> String;
    /// The id of the root key `wrap` would use right now, `None` when no
    /// material is available yet (file absent, variable unset), or an error
    /// when material exists but is unusable.
    fn active_id(&self) -> R<Option<String>>;
    /// Create root key material when none exists. Only the file provider
    /// can; the others answer `Ok(None)`.
    fn mint(&self) -> R<Option<String>>;
    /// Wrap `dek` (the data key `key_id`); returns the wrapped bytes and the
    /// id of the root key used.
    fn wrap(&self, key_id: &str, dek: &[u8]) -> R<(Vec<u8>, String)>;
    /// Unwrap `wrapped` (the data key `key_id`) with root key `root_key_id`.
    fn unwrap(&self, key_id: &str, wrapped: &[u8], root_key_id: &str) -> R<Zeroizing<Vec<u8>>>;
    /// Root key material a leader may hand a joining manager (file/env),
    /// `None` when the key never leaves its store (`exec`).
    fn export_material(&self) -> Option<Zeroizing<Vec<u8>>>;
    /// Persist material delivered by a leader. `Ok(true)` when written,
    /// `Ok(false)` when this provider already has material or cannot store
    /// any.
    fn import_material(&self, material: &[u8]) -> R<bool>;
}

/// `file`: one base64 key per line at `path`, minted on first start.
pub struct FileRootKey {
    path: PathBuf,
}

impl FileRootKey {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn load(&self) -> R<Option<LocalRootKeys>> {
        match std::fs::read_to_string(&self.path) {
            Ok(text) => Ok(Some(LocalRootKeys::parse(
                &text,
                &self.path.display().to_string(),
            )?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("failed reading {}", self.path.display())),
        }
    }

    fn write_new(&self, contents: &str) -> R<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("failed creating {}", dir.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
            }
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&self.path)
            .with_context(|| format!("failed creating {}", self.path.display()))?;
        use std::io::Write;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

impl RootKeyProvider for FileRootKey {
    fn kind(&self) -> &'static str {
        PROVIDER_FILE
    }

    fn location(&self) -> String {
        self.path.display().to_string()
    }

    fn active_id(&self) -> R<Option<String>> {
        Ok(self.load()?.map(|keys| keys.wrapping().1.to_string()))
    }

    fn mint(&self) -> R<Option<String>> {
        if let Some(existing) = self.load()? {
            return Ok(Some(existing.wrapping().1.to_string()));
        }
        let line = generate_root_key_line()?;
        self.write_new(&format!("{ROOT_KEY_FILE_HEADER}{line}\n"))?;
        let keys = self
            .load()?
            .context("freshly minted root key file could not be read back")?;
        Ok(Some(keys.wrapping().1.to_string()))
    }

    fn wrap(&self, key_id: &str, dek: &[u8]) -> R<(Vec<u8>, String)> {
        let keys = self.load()?.with_context(|| {
            format!(
                "root key file {} does not exist (run the manager once to mint it, or place one)",
                self.path.display()
            )
        })?;
        let (root, id) = keys.wrapping();
        Ok((local_wrap(root, key_id, dek)?, id.to_string()))
    }

    fn unwrap(&self, key_id: &str, wrapped: &[u8], root_key_id: &str) -> R<Zeroizing<Vec<u8>>> {
        let keys = self
            .load()?
            .with_context(|| format!("root key file {} does not exist", self.path.display()))?;
        let root = keys.by_id(root_key_id).with_context(|| {
            format!(
                "root key {root_key_id} is not in {} (keys present: {})",
                self.path.display(),
                keys.ids.join(", ")
            )
        })?;
        local_unwrap(root, key_id, wrapped)
    }

    fn export_material(&self) -> Option<Zeroizing<Vec<u8>>> {
        std::fs::read(&self.path).ok().map(Zeroizing::new)
    }

    fn import_material(&self, material: &[u8]) -> R<bool> {
        if self.path.exists() {
            return Ok(false);
        }
        let text = std::str::from_utf8(material).context("delivered root key is not utf-8")?;
        LocalRootKeys::parse(text, "delivered root key")?;
        self.write_new(text)?;
        Ok(true)
    }
}

/// `env`: the same text as the file, in one environment variable.
pub struct EnvRootKey {
    var: String,
}

impl EnvRootKey {
    pub fn new(var: String) -> Self {
        Self { var }
    }

    fn load(&self) -> R<Option<LocalRootKeys>> {
        match std::env::var(&self.var) {
            Ok(text) if !text.trim().is_empty() => Ok(Some(LocalRootKeys::parse(
                &text.replace(',', "\n"),
                &format!("${}", self.var),
            )?)),
            _ => Ok(None),
        }
    }
}

impl RootKeyProvider for EnvRootKey {
    fn kind(&self) -> &'static str {
        PROVIDER_ENV
    }

    fn location(&self) -> String {
        format!("${}", self.var)
    }

    fn active_id(&self) -> R<Option<String>> {
        Ok(self.load()?.map(|keys| keys.wrapping().1.to_string()))
    }

    fn mint(&self) -> R<Option<String>> {
        Ok(None)
    }

    fn wrap(&self, key_id: &str, dek: &[u8]) -> R<(Vec<u8>, String)> {
        let keys = self.load()?.with_context(|| {
            format!(
                "${} is not set (generate a key with `legion keyring root-key generate`)",
                self.var
            )
        })?;
        let (root, id) = keys.wrapping();
        Ok((local_wrap(root, key_id, dek)?, id.to_string()))
    }

    fn unwrap(&self, key_id: &str, wrapped: &[u8], root_key_id: &str) -> R<Zeroizing<Vec<u8>>> {
        let keys = self
            .load()?
            .with_context(|| format!("${} is not set", self.var))?;
        let root = keys.by_id(root_key_id).with_context(|| {
            format!(
                "root key {root_key_id} is not in ${} (keys present: {})",
                self.var,
                keys.ids.join(", ")
            )
        })?;
        local_unwrap(root, key_id, wrapped)
    }

    fn export_material(&self) -> Option<Zeroizing<Vec<u8>>> {
        std::env::var(&self.var)
            .ok()
            .filter(|text| !text.trim().is_empty())
            .map(|text| Zeroizing::new(text.replace(',', "\n").into_bytes()))
    }

    fn import_material(&self, _material: &[u8]) -> R<bool> {
        Ok(false)
    }
}

// ── exec provider ────────────────────────────────────────────────────────

/// `exec`: wrap/unwrap delegated to a command over JSON on stdin/stdout.
///
/// Requests, one JSON document on stdin per invocation:
///
/// ```json
/// {"op":"describe"}
/// {"op":"wrap","key_id":"dk_…","plaintext":"<base64>"}
/// {"op":"unwrap","key_id":"dk_…","ciphertext":"<base64>","root_key_id":"…"}
/// ```
///
/// Replies, one JSON document on stdout:
///
/// ```json
/// {"key_id":"<root key id>"}
/// {"ciphertext":"<base64>","key_id":"<root key id>"}
/// {"plaintext":"<base64>"}
/// ```
///
/// A non-zero exit, a timeout, or malformed output is a provider error. The
/// command is never on a KV read or write path.
pub struct ExecRootKey {
    command: Vec<String>,
    timeout: Duration,
}

impl ExecRootKey {
    pub fn new(command: Vec<String>, timeout: Duration) -> Self {
        Self { command, timeout }
    }

    fn call(&self, request: &Value) -> R<Value> {
        run_provider_command(&self.command, request, self.timeout)
    }
}

/// Run `command` once with `request` on stdin and parse its stdout as JSON,
/// killing it after `timeout`. Shared by the provider and the conformance
/// harness (`legion keyring provider test`).
pub fn run_provider_command(command: &[String], request: &Value, timeout: Duration) -> R<Value> {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    let program = command
        .first()
        .context("exec root key provider has an empty command")?;
    let mut child = Command::new(program)
        .args(&command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed starting root key provider `{program}`"))?;
    {
        let mut stdin = child.stdin.take().context("provider stdin unavailable")?;
        // A provider that exits without reading its request closes the pipe
        // first; its exit status below is the error worth reporting, not
        // the broken pipe.
        let written = stdin
            .write_all(request.to_string().as_bytes())
            .and_then(|()| stdin.write_all(b"\n"));
        match written {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(err) => return Err(err.into()),
        }
    }
    let mut stdout = child.stdout.take().context("provider stdout unavailable")?;
    let mut stderr = child.stderr.take().context("provider stderr unavailable")?;
    let out_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let err_reader = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = stderr.read_to_string(&mut buf);
        buf
    });
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "root key provider `{program}` did not answer within {}s",
                timeout.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    if !status.success() {
        bail!(
            "root key provider `{program}` exited with {status}: {}",
            stderr.trim()
        );
    }
    let reply: Value = serde_json::from_slice(&stdout).with_context(|| {
        format!(
            "root key provider `{program}` printed something other than a JSON object: {}",
            String::from_utf8_lossy(&stdout).trim()
        )
    })?;
    Ok(reply)
}

fn reply_field<'a>(reply: &'a Value, field: &str, op: &str) -> R<&'a str> {
    reply
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("root key provider `{op}` reply is missing `{field}`"))
}

impl RootKeyProvider for ExecRootKey {
    fn kind(&self) -> &'static str {
        PROVIDER_EXEC
    }

    fn location(&self) -> String {
        self.command.join(" ")
    }

    fn active_id(&self) -> R<Option<String>> {
        let reply = self.call(&json!({ "op": "describe" }))?;
        Ok(Some(reply_field(&reply, "key_id", "describe")?.to_string()))
    }

    fn mint(&self) -> R<Option<String>> {
        Ok(None)
    }

    fn wrap(&self, key_id: &str, dek: &[u8]) -> R<(Vec<u8>, String)> {
        let reply = self.call(&json!({
            "op": "wrap",
            "key_id": key_id,
            "plaintext": B64.encode(dek),
        }))?;
        let ciphertext = B64
            .decode(reply_field(&reply, "ciphertext", "wrap")?)
            .context("root key provider `wrap` reply: ciphertext is not base64")?;
        let root_key_id = reply_field(&reply, "key_id", "wrap")?.to_string();
        Ok((ciphertext, root_key_id))
    }

    fn unwrap(&self, key_id: &str, wrapped: &[u8], root_key_id: &str) -> R<Zeroizing<Vec<u8>>> {
        let reply = self.call(&json!({
            "op": "unwrap",
            "key_id": key_id,
            "ciphertext": B64.encode(wrapped),
            "root_key_id": root_key_id,
        }))?;
        let plaintext = B64
            .decode(reply_field(&reply, "plaintext", "unwrap")?)
            .context("root key provider `unwrap` reply: plaintext is not base64")?;
        if plaintext.len() != KEY_BYTES {
            bail!(
                "root key provider `unwrap` returned {} bytes, expected {KEY_BYTES}",
                plaintext.len()
            );
        }
        Ok(Zeroizing::new(plaintext))
    }

    fn export_material(&self) -> Option<Zeroizing<Vec<u8>>> {
        None
    }

    fn import_material(&self, _material: &[u8]) -> R<bool> {
        Ok(false)
    }
}

/// Build the provider named by the resolved `encryption.rootKey` block.
pub fn provider_from_config(
    root_key: &RootKeyConfigFull,
    store_path: &str,
) -> R<Arc<dyn RootKeyProvider>> {
    match root_key.provider.as_str() {
        PROVIDER_FILE => {
            let path = match &root_key.path {
                Some(path) if !path.trim().is_empty() => PathBuf::from(path),
                _ => Path::new(store_path).join(ROOT_KEY_DIR).join(ROOT_KEY_FILE),
            };
            Ok(Arc::new(FileRootKey::new(path)))
        }
        PROVIDER_ENV => Ok(Arc::new(EnvRootKey::new(root_key.var.clone()))),
        PROVIDER_EXEC => {
            if root_key.command.is_empty() {
                bail!("encryption.rootKey.provider = \"exec\" requires a non-empty `command`");
            }
            let timeout = humantime::parse_duration(&root_key.timeout).with_context(|| {
                format!(
                    "encryption.rootKey.timeout `{}` is not a duration",
                    root_key.timeout
                )
            })?;
            Ok(Arc::new(ExecRootKey::new(root_key.command.clone(), timeout)))
        }
        other => bail!(
            "encryption.rootKey.provider `{other}` is not one of file, env, exec (docs/kv-encryption.md)"
        ),
    }
}

// ── keyring rows and in-memory state ─────────────────────────────────────

/// One `keyring_key` row: a data key as stored (wrapped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyringRow {
    pub key_id: String,
    pub algorithm: String,
    pub wrapped_key: Vec<u8>,
    pub root_key_id: String,
    pub root_provider: String,
    /// `active` | `retired` for a data key, `checkpoint` for a checkpoint
    /// key.
    pub state: String,
    pub created_at: i64,
    pub retired_at: Option<i64>,
}

pub const KEY_STATE_ACTIVE: &str = "active";
pub const KEY_STATE_RETIRED: &str = "retired";
/// A checkpoint key (`ck_…`): the key published checkpoint parts are sealed
/// under (docs/suspend-and-resume.md, "Security"). It is wrapped by the root
/// key like a data key, so a root-key rewrap covers it, but it never seals
/// a KV value, never rotates with the data keys, and is never collected.
pub const KEY_STATE_CHECKPOINT: &str = "checkpoint";

struct LoadedKey {
    key: Zeroizing<Vec<u8>>,
}

/// A row this manager cannot unwrap, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedKey {
    pub key_id: String,
    pub root_key_id: String,
    pub reason: String,
}

#[derive(Default)]
struct KeyringState {
    keys: HashMap<String, LoadedKey>,
    active: Option<String>,
    rows: Vec<KeyringRow>,
    locked: Vec<LockedKey>,
    /// The provider's current wrapping id, when it has material.
    root_key_id: Option<String>,
    /// Why the leader is not initializing yet (mixed-version cluster), if so.
    pending_upgrade: Option<String>,
    provider_error: Option<String>,
    /// Whether a rewrap is converting values right now (the daemon loop or
    /// an operator RPC), and the last rewrap failure, for status output.
    rewrap_running: bool,
    rewrap_error: Option<String>,
}

/// The whole keyring as one readable snapshot (status RPC, CLI, UI).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyringSnapshot {
    pub enabled: bool,
    /// `disabled` | `uninitialized` | `pending_upgrade` | `unlocked` | `locked`
    pub state: String,
    pub provider: String,
    pub location: String,
    pub root_key_id: Option<String>,
    pub active_key_id: Option<String>,
    pub rows: Vec<KeyringRow>,
    pub locked: Vec<LockedKey>,
    pub pending_upgrade: Option<String>,
    pub provider_error: Option<String>,
    pub rotation_age: Duration,
    pub encryptions_since_load: u64,
    pub rewrap_running: bool,
    pub rewrap_error: Option<String>,
}

impl KeyringSnapshot {
    /// True when the active key is usable for writes.
    pub fn can_encrypt(&self) -> bool {
        self.state == "unlocked"
    }
}

struct Inner {
    enabled: bool,
    provider: Arc<dyn RootKeyProvider>,
    rotation_age: Duration,
    state: RwLock<KeyringState>,
    encryptions: AtomicU64,
    wake: tokio::sync::Notify,
}

/// Shared handle to the keyring; clones share state.
#[derive(Clone)]
pub struct Keyring {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Keyring {
    /// Provider kind, location, and key ids — never key material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("Keyring")
            .field("enabled", &snapshot.enabled)
            .field("state", &snapshot.state)
            .field("provider", &snapshot.provider)
            .field("location", &snapshot.location)
            .field("active_key_id", &snapshot.active_key_id)
            .field("keys", &snapshot.rows.len())
            .finish()
    }
}

impl Keyring {
    /// Keyring for the resolved `encryption` block; `store_path` anchors the
    /// file provider's default location.
    pub fn from_config(config: &EncryptionConfigFull, store_path: &str) -> R<Self> {
        let provider = provider_from_config(&config.root_key, store_path)?;
        let rotation_age =
            humantime::parse_duration(&config.data_key_rotation_age).with_context(|| {
                format!(
                    "encryption.dataKeyRotationAge `{}` is not a duration",
                    config.data_key_rotation_age
                )
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                enabled: config.enabled,
                provider,
                rotation_age,
                state: RwLock::new(KeyringState::default()),
                encryptions: AtomicU64::new(0),
                wake: tokio::sync::Notify::new(),
            }),
        })
    }

    /// A keyring with an explicit provider (tests, embedding).
    pub fn with_provider(
        enabled: bool,
        provider: Arc<dyn RootKeyProvider>,
        rotation_age: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                enabled,
                provider,
                rotation_age,
                state: RwLock::new(KeyringState::default()),
                encryptions: AtomicU64::new(0),
                wake: tokio::sync::Notify::new(),
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    pub fn provider(&self) -> &Arc<dyn RootKeyProvider> {
        &self.inner.provider
    }

    pub fn rotation_age(&self) -> Duration {
        self.inner.rotation_age
    }

    /// Wake whoever waits on [`Keyring::wait_for_change`] (the maintenance
    /// loop) — called when a replicated `keyring_key` change lands.
    pub fn wake(&self) {
        self.inner.wake.notify_one();
    }

    pub async fn wait_for_change(&self) {
        self.inner.wake.notified().await
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, KeyringState> {
        self.inner
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, KeyringState> {
        self.inner
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Replace the in-memory key set from the stored rows, unwrapping each
    /// with the provider. Keys already unwrapped are kept without asking the
    /// provider again (rotation on the leader installs the new key
    /// directly). Returns the ids that were newly unlocked.
    pub fn load_rows(&self, rows: Vec<KeyringRow>) -> Vec<String> {
        let provider = &self.inner.provider;
        let mut state = self.write();
        let mut keys: HashMap<String, LoadedKey> = HashMap::new();
        let mut locked = Vec::new();
        let mut newly_unlocked = Vec::new();
        let mut provider_error = None;
        let describe = provider.active_id();
        count_provider_call("describe", describe.is_ok());
        let root_key_id = match describe {
            Ok(id) => id,
            Err(err) => {
                provider_error = Some(format!("{err:#}"));
                None
            }
        };
        for row in &rows {
            if let Some(existing) = state.keys.remove(&row.key_id) {
                keys.insert(row.key_id.clone(), existing);
                continue;
            }
            let algorithm_ok = if row.state == KEY_STATE_CHECKPOINT {
                row.algorithm == crate::checkpoint_seal::CHECKPOINT_KEY_ALGORITHM
            } else {
                row.algorithm == DATA_KEY_ALGORITHM
            };
            if !algorithm_ok {
                locked.push(LockedKey {
                    key_id: row.key_id.clone(),
                    root_key_id: row.root_key_id.clone(),
                    reason: format!(
                        "algorithm `{}` is not supported by this manager (upgrade it)",
                        row.algorithm
                    ),
                });
                continue;
            }
            let unwrapped = provider.unwrap(&row.key_id, &row.wrapped_key, &row.root_key_id);
            count_provider_call("unwrap", unwrapped.is_ok());
            match unwrapped {
                Ok(key) => {
                    newly_unlocked.push(row.key_id.clone());
                    keys.insert(row.key_id.clone(), LoadedKey { key });
                }
                Err(err) => locked.push(LockedKey {
                    key_id: row.key_id.clone(),
                    root_key_id: row.root_key_id.clone(),
                    reason: format!("{err:#}"),
                }),
            }
        }
        let active = rows
            .iter()
            .find(|row| row.state == KEY_STATE_ACTIVE)
            .map(|row| row.key_id.clone());
        if state.active != active {
            self.inner.encryptions.store(0, Ordering::Relaxed);
        }
        state.keys = keys;
        state.active = active;
        state.rows = rows;
        state.locked = locked;
        state.root_key_id = root_key_id;
        state.provider_error = provider_error;
        newly_unlocked
    }

    /// Install a data key this process generated (the leader, at
    /// initialization or rotation) without a provider round trip.
    pub fn install_generated(&self, row: KeyringRow, key: Zeroizing<Vec<u8>>) {
        let mut state = self.write();
        if row.state == KEY_STATE_ACTIVE {
            for existing in state.rows.iter_mut() {
                if existing.state == KEY_STATE_ACTIVE {
                    existing.state = KEY_STATE_RETIRED.to_string();
                    existing.retired_at = Some(row.created_at);
                }
            }
            state.active = Some(row.key_id.clone());
            self.inner.encryptions.store(0, Ordering::Relaxed);
        }
        state.keys.insert(row.key_id.clone(), LoadedKey { key });
        state.locked.retain(|locked| locked.key_id != row.key_id);
        state.rows.retain(|existing| existing.key_id != row.key_id);
        state.rows.push(row);
    }

    /// Generate a fresh active data key wrapped by the provider's current
    /// root key. The caller persists the row (replicated) and then calls
    /// [`Keyring::install_generated`].
    pub fn generate_data_key(&self, now_unix: i64) -> R<(KeyringRow, Zeroizing<Vec<u8>>)> {
        let key = Zeroizing::new(random_bytes(KEY_BYTES)?);
        let key_id = new_data_key_id()?;
        let wrapped = self.inner.provider.wrap(&key_id, &key);
        count_provider_call("wrap", wrapped.is_ok());
        let (wrapped_key, root_key_id) = wrapped?;
        let row = KeyringRow {
            key_id,
            algorithm: DATA_KEY_ALGORITHM.to_string(),
            wrapped_key,
            root_key_id,
            root_provider: self.inner.provider.kind().to_string(),
            state: KEY_STATE_ACTIVE.to_string(),
            created_at: now_unix,
            retired_at: None,
        };
        Ok((row, key))
    }

    /// Generate a checkpoint key wrapped by the provider's current root
    /// key. The caller persists the row (replicated) and then calls
    /// [`Keyring::install_generated`].
    pub fn generate_checkpoint_key(&self, now_unix: i64) -> R<(KeyringRow, Zeroizing<Vec<u8>>)> {
        let key = Zeroizing::new(random_bytes(KEY_BYTES)?);
        let key_id = new_key_id(CHECKPOINT_KEY_ID_PREFIX)?;
        let wrapped = self.inner.provider.wrap(&key_id, &key);
        count_provider_call("wrap", wrapped.is_ok());
        let (wrapped_key, root_key_id) = wrapped?;
        let row = KeyringRow {
            key_id,
            algorithm: crate::checkpoint_seal::CHECKPOINT_KEY_ALGORITHM.to_string(),
            wrapped_key,
            root_key_id,
            root_provider: self.inner.provider.kind().to_string(),
            state: KEY_STATE_CHECKPOINT.to_string(),
            created_at: now_unix,
            retired_at: None,
        };
        Ok((row, key))
    }

    /// Whether the keyring has a checkpoint key row (loaded or not).
    pub fn has_checkpoint_key(&self) -> bool {
        self.read()
            .rows
            .iter()
            .any(|row| row.state == KEY_STATE_CHECKPOINT)
    }

    /// The key a published checkpoint part with plaintext `digest` is sealed
    /// under. `key_id` is the part's recorded `keyId`; `None` asks for the
    /// current checkpoint key, for a new publish. Never logs key material.
    pub fn checkpoint_part_key(
        &self,
        key_id: Option<&str>,
        digest: &crate::content_store::ContentDigest,
    ) -> R<crate::checkpoint_seal::PartKey> {
        self.with_checkpoint_key(key_id, |key, key_id| {
            crate::checkpoint_seal::PartKey::derive(key, key_id, digest)
        })
    }

    /// Job `job_id`'s chunk key under the current checkpoint key, which a
    /// worker publishing one of the job's snapshots derives every chunk's
    /// key from ([`crate::checkpoint_seal::ChunkKey`]). Never logs key
    /// material.
    pub fn checkpoint_chunk_key(&self, job_id: &str) -> R<crate::checkpoint_seal::ChunkKey> {
        self.with_checkpoint_key(None, |key, key_id| {
            crate::checkpoint_seal::ChunkKey::derive(key, key_id, job_id)
        })
    }

    /// Run `derive` with the checkpoint key `key_id` names (`None`: the
    /// current one) and its id, or fail naming why it is unavailable.
    fn with_checkpoint_key<T>(
        &self,
        key_id: Option<&str>,
        derive: impl FnOnce(&[u8], &str) -> R<T>,
    ) -> R<T> {
        let state = self.read();
        let row = match key_id {
            Some(id) => state
                .rows
                .iter()
                .find(|row| row.key_id == id && row.state == KEY_STATE_CHECKPOINT)
                .with_context(|| {
                    format!(
                        "checkpoint key {id} is not in this cluster's keyring (a `legion keyring                          reset` dropped it, or the snapshot was published by another cluster),                          so parts sealed under it cannot be read"
                    )
                })?,
            None => state
                .rows
                .iter()
                .filter(|row| row.state == KEY_STATE_CHECKPOINT)
                .max_by_key(|row| (row.created_at, row.key_id.clone()))
                .with_context(|| {
                    if self.inner.enabled {
                        "this cluster has no checkpoint key yet; the keyring leader mints one                          within seconds of initializing (check `legion keyring status`)"
                            .to_string()
                    } else {
                        "this cluster has no checkpoint key, because the keyring is off                          (encryption.enabled = false on the manager); published checkpoints are                          always sealed, so publishing needs the keyring (docs/kv-encryption.md)"
                            .to_string()
                    }
                })?,
        };
        let Some(loaded) = state.keys.get(&row.key_id) else {
            let reason = state
                .locked
                .iter()
                .find(|locked| locked.key_id == row.key_id)
                .map(|locked| locked.reason.clone())
                .unwrap_or_else(|| "checkpoint key not loaded yet".to_string());
            let provider = &self.inner.provider;
            return Err(anyhow::Error::new(StorageError::KeyringLocked {
                key_id: row.key_id.clone(),
                message: format!(
                    "keyring locked: checkpoint key {} cannot be unwrapped — {reason}. Root key \
                     provider `{}` at {}. Copy the root key from a manager that has it (or set \
                     {DEF_ROOT_KEY_ENV}), then check `legion keyring status`",
                    row.key_id,
                    provider.kind(),
                    provider.location()
                ),
            }));
        };
        derive(&loaded.key, &row.key_id)
    }

    /// A job's session sealing keys (session durability plan §6.6): one per
    /// loaded checkpoint key, the newest first. Empty when the keyring has
    /// no loaded checkpoint key. Never logs key material.
    pub fn session_seal_keys(&self, job_id: &str) -> Vec<crate::structs::SessionSealKey> {
        let state = self.read();
        let mut rows: Vec<&KeyringRow> = state
            .rows
            .iter()
            .filter(|row| row.state == KEY_STATE_CHECKPOINT)
            .collect();
        rows.sort_by_key(|row| std::cmp::Reverse((row.created_at, row.key_id.clone())));
        rows.into_iter()
            .filter_map(|row| {
                let loaded = state.keys.get(&row.key_id)?;
                crate::checkpoint_seal::derive_session_seal_key(&loaded.key, &row.key_id, job_id)
                    .ok()
            })
            .collect()
    }

    /// Re-wrap every loaded data key under the provider's CURRENT root key
    /// (`legion keyring root-key rewrap`). Returns the rewritten rows; the
    /// caller persists them in one replicated write. Locked keys cannot be
    /// rewrapped and are reported by id.
    pub fn rewrap_rows_under_current_root(&self) -> R<(Vec<KeyringRow>, Vec<String>)> {
        let state = self.read();
        let mut rewritten = Vec::new();
        let mut skipped = Vec::new();
        for row in &state.rows {
            let Some(loaded) = state.keys.get(&row.key_id) else {
                skipped.push(row.key_id.clone());
                continue;
            };
            let wrapped = self.inner.provider.wrap(&row.key_id, &loaded.key);
            count_provider_call("wrap", wrapped.is_ok());
            let (wrapped_key, root_key_id) = wrapped?;
            let mut next = row.clone();
            next.wrapped_key = wrapped_key;
            next.root_key_id = root_key_id;
            next.root_provider = self.inner.provider.kind().to_string();
            rewritten.push(next);
        }
        Ok((rewritten, skipped))
    }

    /// Encrypt a KV value for storage. Returns the stored form and the data
    /// key id — or the plaintext and `None` while encryption is disabled or
    /// the keyring has no active key yet (an upgraded cluster before its
    /// keyring initializes writes exactly as it always did).
    pub fn seal(&self, key_path: &str, plaintext: &str) -> R<(String, Option<String>)> {
        if !self.inner.enabled {
            return Ok((plaintext.to_string(), None));
        }
        let state = self.read();
        let Some(active) = state.active.as_deref() else {
            return Ok((plaintext.to_string(), None));
        };
        let Some(loaded) = state.keys.get(active) else {
            let reason = state
                .locked
                .iter()
                .find(|locked| locked.key_id == active)
                .map(|locked| locked.reason.clone())
                .unwrap_or_else(|| "data key not loaded".to_string());
            return Err(anyhow::Error::new(StorageError::KeyringLocked {
                key_id: active.to_string(),
                message: self.locked_message(active, &reason),
            }));
        };
        let sealed = seal_bytes(&loaded.key, key_path.as_bytes(), plaintext.as_bytes())?;
        self.inner.encryptions.fetch_add(1, Ordering::Relaxed);
        Ok((B64.encode(sealed), Some(active.to_string())))
    }

    /// Decrypt a stored KV value. A `None` key id is a plaintext row.
    pub fn open(&self, key_path: &str, stored: &str, enc_key_id: Option<&str>) -> R<String> {
        let Some(key_id) = enc_key_id else {
            return Ok(stored.to_string());
        };
        let state = self.read();
        let Some(loaded) = state.keys.get(key_id) else {
            let reason = state
                .locked
                .iter()
                .find(|locked| locked.key_id == key_id)
                .map(|locked| locked.reason.clone())
                .unwrap_or_else(|| {
                    if state.rows.iter().any(|row| row.key_id == key_id) {
                        "data key not loaded yet".to_string()
                    } else {
                        "no such data key in the keyring".to_string()
                    }
                });
            return Err(anyhow::Error::new(StorageError::KeyringLocked {
                key_id: key_id.to_string(),
                message: self.locked_message(key_id, &reason),
            }));
        };
        let sealed = B64.decode(stored).map_err(|err| {
            anyhow::Error::new(StorageError::KvCiphertextInvalid {
                key: key_path.to_string(),
                message: format!("stored value is not base64: {err}"),
            })
        })?;
        let plaintext = open_bytes(&loaded.key, key_path.as_bytes(), &sealed).map_err(|err| {
            anyhow::Error::new(StorageError::KvCiphertextInvalid {
                key: key_path.to_string(),
                message: format!("{err:#}"),
            })
        })?;
        String::from_utf8(plaintext.to_vec()).map_err(|_| {
            anyhow::Error::new(StorageError::KvCiphertextInvalid {
                key: key_path.to_string(),
                message: "decrypted value is not utf-8".to_string(),
            })
        })
    }

    fn locked_message(&self, key_id: &str, reason: &str) -> String {
        let provider = &self.inner.provider;
        format!(
            "keyring locked: data key {key_id} cannot be unwrapped — {reason}. Root key provider \
             `{}` at {}. Copy the root key from a manager that has it (or set {DEF_ROOT_KEY_ENV}), \
             then check `legion keyring status`",
            provider.kind(),
            provider.location()
        )
    }

    /// Encryptions performed under the active key since it was loaded.
    pub fn encryptions_since_load(&self) -> u64 {
        self.inner.encryptions.load(Ordering::Relaxed)
    }

    pub fn set_pending_upgrade(&self, reason: Option<String>) {
        self.write().pending_upgrade = reason;
    }

    /// The active data key's id and creation time, when unlocked.
    pub fn active_key(&self) -> Option<(String, i64)> {
        let state = self.read();
        let active = state.active.as_deref()?;
        if !state.keys.contains_key(active) {
            return None;
        }
        state
            .rows
            .iter()
            .find(|row| row.key_id == active)
            .map(|row| (row.key_id.clone(), row.created_at))
    }

    /// Whether the active key is past `dataKeyRotationAge` or the encryption
    /// count guard, so the leader should rotate.
    pub fn rotation_due(&self, now_unix: i64) -> Option<String> {
        let (key_id, created_at) = self.active_key()?;
        let age = now_unix.saturating_sub(created_at).max(0) as u64;
        if age >= self.inner.rotation_age.as_secs() {
            return Some(format!(
                "data key {key_id} is {} old (dataKeyRotationAge {})",
                humantime::format_duration(Duration::from_secs(age)),
                humantime::format_duration(self.inner.rotation_age)
            ));
        }
        let count = self.encryptions_since_load();
        if count >= DATA_KEY_ENCRYPTION_LIMIT {
            return Some(format!(
                "data key {key_id} performed {count} encryptions since load (limit {DATA_KEY_ENCRYPTION_LIMIT})"
            ));
        }
        None
    }

    /// Data keys that are unwrapped in memory.
    pub fn loaded_key_ids(&self) -> Vec<String> {
        let state = self.read();
        let mut ids: Vec<String> = state.keys.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Whether every stored row is unwrapped.
    pub fn fully_unlocked(&self) -> bool {
        let state = self.read();
        state.locked.is_empty() && state.provider_error.is_none()
    }

    pub fn snapshot(&self) -> KeyringSnapshot {
        let state = self.read();
        let provider = &self.inner.provider;
        let state_name = if !self.inner.enabled {
            "disabled"
        } else if state.rows.is_empty() {
            if state.pending_upgrade.is_some() {
                "pending_upgrade"
            } else {
                "uninitialized"
            }
        } else if let Some(active) = state.active.as_deref() {
            if state.keys.contains_key(active) && state.locked.is_empty() {
                "unlocked"
            } else {
                "locked"
            }
        } else {
            "locked"
        };
        KeyringSnapshot {
            enabled: self.inner.enabled,
            state: state_name.to_string(),
            provider: provider.kind().to_string(),
            location: provider.location(),
            root_key_id: state.root_key_id.clone(),
            active_key_id: state.active.clone(),
            rows: state.rows.clone(),
            locked: state.locked.clone(),
            pending_upgrade: state.pending_upgrade.clone(),
            provider_error: state.provider_error.clone(),
            rotation_age: self.inner.rotation_age,
            encryptions_since_load: self.encryptions_since_load(),
            rewrap_running: state.rewrap_running,
            rewrap_error: state.rewrap_error.clone(),
        }
    }

    /// Mark a rewrap as running (true) or finished (false, with the error
    /// it ended on, if any). Concurrent rewraps are refused by the caller
    /// that sees `rewrap_running`; this is bookkeeping for status output.
    pub fn set_rewrap_running(&self, running: bool, error: Option<String>) {
        let mut state = self.write();
        state.rewrap_running = running;
        if running {
            state.rewrap_error = None;
        } else {
            state.rewrap_error = error;
        }
    }

    pub fn rewrap_running(&self) -> bool {
        self.read().rewrap_running
    }

    /// Ids of rows this manager cannot unwrap.
    pub fn locked_key_ids(&self) -> Vec<String> {
        self.read()
            .locked
            .iter()
            .map(|locked| locked.key_id.clone())
            .collect()
    }

    /// The value counts per data key id, paired with the rows, for status
    /// rendering. `counts` maps `enc_key_id` (`None` = plaintext) to a row
    /// count.
    pub fn describe_keys(&self, counts: &BTreeMap<Option<String>, i64>) -> Vec<DescribedKey> {
        let state = self.read();
        state
            .rows
            .iter()
            .map(|row| DescribedKey {
                row: row.clone(),
                value_count: counts.get(&Some(row.key_id.clone())).copied().unwrap_or(0),
                unwrapped: state.keys.contains_key(&row.key_id),
                error: state
                    .locked
                    .iter()
                    .find(|locked| locked.key_id == row.key_id)
                    .map(|locked| locked.reason.clone()),
            })
            .collect()
    }
}

/// A keyring row with its live status, for the status RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedKey {
    pub row: KeyringRow,
    pub value_count: i64,
    pub unwrapped: bool,
    pub error: Option<String>,
}

// ── join delivery (root key to a joining manager) ────────────────────────

const JOIN_BUNDLE_SALT: &[u8] = b"legion-keyring-join";
const JOIN_BUNDLE_INFO: &[u8] = b"root-key-bundle-v1";

fn join_bundle_key(cluster_secret: &str) -> R<Zeroizing<Vec<u8>>> {
    use ring::hkdf::{Salt, HKDF_SHA256};
    let prk = Salt::new(HKDF_SHA256, JOIN_BUNDLE_SALT).extract(cluster_secret.as_bytes());
    let okm = prk
        .expand(&[JOIN_BUNDLE_INFO], HKDF_SHA256)
        .map_err(|_| anyhow!("hkdf expand failed"))?;
    let mut key = Zeroizing::new(vec![0u8; KEY_BYTES]);
    okm.fill(&mut key)
        .map_err(|_| anyhow!("hkdf fill failed"))?;
    Ok(key)
}

/// Encrypt root key material for the raft join reply: AES-256-GCM under a
/// key derived from the cluster secret, bound to the cluster id. Whoever
/// holds the cluster secret can already join as a manager and receive the
/// whole database; this keeps the key off the wire in the clear for anyone
/// who does not.
pub fn seal_root_bundle(cluster_secret: &str, cluster_id: &str, material: &[u8]) -> R<Vec<u8>> {
    let key = join_bundle_key(cluster_secret)?;
    seal_bytes(&key, cluster_id.as_bytes(), material)
}

/// Inverse of [`seal_root_bundle`].
pub fn open_root_bundle(
    cluster_secret: &str,
    cluster_id: &str,
    bundle: &[u8],
) -> R<Zeroizing<Vec<u8>>> {
    let key = join_bundle_key(cluster_secret)?;
    open_bytes(&key, cluster_id.as_bytes(), bundle)
}

// ── conformance harness for exec providers ───────────────────────────────

/// One check of `legion keyring provider test`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// Exercise a candidate `exec` provider command the way the manager will:
/// describe, a wrap/unwrap round trip, a tampered ciphertext must be
/// refused, and a second wrap must not repeat the first (no deterministic
/// ciphertext). Every check runs even after a failure so the report is
/// complete.
pub fn test_exec_provider(command: &[String], timeout: Duration) -> Vec<ProviderCheck> {
    let provider = ExecRootKey::new(command.to_vec(), timeout);
    let mut checks = Vec::new();

    let describe = provider.active_id();
    checks.push(match &describe {
        Ok(Some(id)) => ProviderCheck {
            name: "describe",
            passed: true,
            detail: format!("root key id {id}"),
        },
        Ok(None) => ProviderCheck {
            name: "describe",
            passed: false,
            detail: "no key_id in the describe reply".to_string(),
        },
        Err(err) => ProviderCheck {
            name: "describe",
            passed: false,
            detail: format!("{err:#}"),
        },
    });

    let dek: Vec<u8> = (0..KEY_BYTES as u8).collect();
    let wrapped = provider.wrap("dk_conformance", &dek);
    checks.push(match &wrapped {
        Ok((bytes, id)) => ProviderCheck {
            name: "wrap",
            passed: true,
            detail: format!("{} bytes under {id}", bytes.len()),
        },
        Err(err) => ProviderCheck {
            name: "wrap",
            passed: false,
            detail: format!("{err:#}"),
        },
    });

    if let Ok((bytes, id)) = &wrapped {
        let round_trip = provider.unwrap("dk_conformance", bytes, id);
        checks.push(match &round_trip {
            Ok(plain) if plain.as_slice() == dek.as_slice() => ProviderCheck {
                name: "unwrap round trip",
                passed: true,
                detail: "plaintext matches".to_string(),
            },
            Ok(_) => ProviderCheck {
                name: "unwrap round trip",
                passed: false,
                detail: "plaintext differs from what was wrapped".to_string(),
            },
            Err(err) => ProviderCheck {
                name: "unwrap round trip",
                passed: false,
                detail: format!("{err:#}"),
            },
        });

        let mut tampered = bytes.clone();
        if let Some(last) = tampered.last_mut() {
            *last ^= 0x55;
        }
        let tamper = provider.unwrap("dk_conformance", &tampered, id);
        checks.push(match tamper {
            Err(_) => ProviderCheck {
                name: "tampered ciphertext refused",
                passed: true,
                detail: "unwrap failed as it must".to_string(),
            },
            Ok(plain) if plain.as_slice() != dek.as_slice() => ProviderCheck {
                name: "tampered ciphertext refused",
                passed: false,
                detail: "unwrap returned garbage instead of failing (no authentication)"
                    .to_string(),
            },
            Ok(_) => ProviderCheck {
                name: "tampered ciphertext refused",
                passed: false,
                detail: "unwrap of a modified ciphertext succeeded (no authentication)".to_string(),
            },
        });

        let again = provider.wrap("dk_conformance", &dek);
        checks.push(match again {
            Ok((second, _)) if &second != bytes => ProviderCheck {
                name: "wrap is randomized",
                passed: true,
                detail: "two wraps of one key differ".to_string(),
            },
            Ok(_) => ProviderCheck {
                name: "wrap is randomized",
                passed: false,
                detail: "two wraps of one key are identical (deterministic wrapping)".to_string(),
            },
            Err(err) => ProviderCheck {
                name: "wrap is randomized",
                passed: false,
                detail: format!("{err:#}"),
            },
        });
    }

    checks
}

/// Test support: one root key shared by every store a test process starts.
///
/// Stores built by the crate's unit tests stand in for replicas of ONE
/// cluster (a changeset captured on one is applied on another, a snapshot
/// built on one is installed on another), and replicas of one cluster share
/// a root key — a joiner receives it with the join. Pointing every test
/// store at this file gives them that relationship without each test
/// copying key files around; a test that wants a manager with a *different*
/// key sets `encryption.rootKey.path` itself.
#[cfg(test)]
pub(crate) mod test_support {
    use super::RootKeyProvider;
    use std::sync::OnceLock;

    /// Path of the process-wide shared root key file, minted on first use.
    pub(crate) fn shared_root_key_path() -> String {
        static PATH: OnceLock<String> = OnceLock::new();
        PATH.get_or_init(|| {
            let dir = tempfile::Builder::new()
                .prefix("legion-test-root-key-")
                .tempdir()
                .expect("tempdir for the shared test root key")
                .keep();
            let provider = super::FileRootKey::new(dir.join(super::ROOT_KEY_FILE));
            provider.mint().expect("mint the shared test root key");
            provider.path().display().to_string()
        })
        .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("legion-keyring-")
            .tempdir()
            .expect("tempdir")
    }

    fn file_provider(dir: &Path) -> FileRootKey {
        FileRootKey::new(dir.join(ROOT_KEY_DIR).join(ROOT_KEY_FILE))
    }

    fn unlocked_keyring(dir: &Path) -> Keyring {
        let provider = file_provider(dir);
        provider.mint().expect("mint");
        let keyring = Keyring::with_provider(true, Arc::new(provider), Duration::from_secs(3600));
        let (row, key) = keyring.generate_data_key(1_000).expect("data key");
        keyring.install_generated(row, key);
        keyring
    }

    #[test]
    fn value_round_trips_and_is_bound_to_its_path() -> R<()> {
        let dir = temp_dir();
        let keyring = unlocked_keyring(dir.path());
        let (stored, key_id) = keyring.seal("prod/db/password", "hunter2")?;
        assert!(key_id.as_deref().unwrap().starts_with("dk_"));
        assert_ne!(stored, "hunter2");
        assert_eq!(
            keyring.open("prod/db/password", &stored, key_id.as_deref())?,
            "hunter2"
        );
        // Transplanting the ciphertext to another path fails authentication.
        let err = keyring
            .open("dev/scratch", &stored, key_id.as_deref())
            .unwrap_err();
        assert!(
            matches!(
                StorageError::from_anyhow(&err),
                Some(StorageError::KvCiphertextInvalid { .. })
            ),
            "{err:#}"
        );
        // A flipped byte fails too.
        let mut bytes = B64.decode(&stored)?;
        bytes[NONCE_BYTES + 1] ^= 0x01;
        let tampered = B64.encode(bytes);
        assert!(keyring
            .open("prod/db/password", &tampered, key_id.as_deref())
            .is_err());
        Ok(())
    }

    #[test]
    fn plaintext_rows_pass_through_and_disabled_writes_plaintext() -> R<()> {
        let dir = temp_dir();
        let keyring = unlocked_keyring(dir.path());
        assert_eq!(keyring.open("k", "plain", None)?, "plain");

        let disabled = Keyring::with_provider(
            false,
            Arc::new(file_provider(dir.path())),
            Duration::from_secs(1),
        );
        assert_eq!(disabled.seal("k", "v")?, ("v".to_string(), None));
        assert_eq!(disabled.snapshot().state, "disabled");
        Ok(())
    }

    #[test]
    fn uninitialized_keyring_writes_plaintext_until_a_key_exists() -> R<()> {
        let dir = temp_dir();
        let keyring = Keyring::with_provider(
            true,
            Arc::new(file_provider(dir.path())),
            Duration::from_secs(1),
        );
        assert_eq!(keyring.seal("k", "v")?, ("v".to_string(), None));
        assert_eq!(keyring.snapshot().state, "uninitialized");
        keyring.set_pending_upgrade(Some("waiting for mgr-b".into()));
        assert_eq!(keyring.snapshot().state, "pending_upgrade");
        Ok(())
    }

    #[test]
    fn root_key_file_first_line_wraps_every_line_unwraps() -> R<()> {
        let dir = temp_dir();
        let provider = file_provider(dir.path());
        assert_eq!(provider.active_id()?, None);
        let first = provider.mint()?.expect("minted");
        assert_eq!(provider.mint()?, Some(first.clone()), "mint is idempotent");
        let contents = std::fs::read_to_string(provider.path())?;
        assert!(contents.starts_with("# Legion KV root key"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(provider.path())?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        let dek = vec![7u8; KEY_BYTES];
        let (wrapped, id) = provider.wrap("dk_one", &dek)?;
        assert_eq!(id, first);
        assert_eq!(
            provider.unwrap("dk_one", &wrapped, &id)?.as_slice(),
            &dek[..]
        );
        // The wrapped blob is bound to its data key id.
        assert!(provider.unwrap("dk_two", &wrapped, &id).is_err());

        // Prepend a new key: it wraps, the old line still unwraps.
        let new_line = generate_root_key_line()?;
        let rotated = format!("{new_line}\n{contents}");
        std::fs::write(provider.path(), rotated)?;
        let (wrapped2, id2) = provider.wrap("dk_one", &dek)?;
        assert_ne!(id2, first);
        assert_eq!(provider.active_id()?, Some(id2.clone()));
        assert_eq!(
            provider.unwrap("dk_one", &wrapped, &first)?.as_slice(),
            &dek[..]
        );
        assert_eq!(
            provider.unwrap("dk_one", &wrapped2, &id2)?.as_slice(),
            &dek[..]
        );

        // Drop the old line: the old blob locks with the missing id named.
        std::fs::write(provider.path(), format!("{new_line}\n"))?;
        let err = provider.unwrap("dk_one", &wrapped, &first).unwrap_err();
        assert!(format!("{err:#}").contains(&first), "{err:#}");
        Ok(())
    }

    #[test]
    fn root_key_text_rejects_bad_lines() {
        assert!(LocalRootKeys::parse("", "t").is_err());
        assert!(LocalRootKeys::parse("# only a comment\n", "t").is_err());
        assert!(LocalRootKeys::parse("not base64!!", "t").is_err());
        let short = B64.encode([1u8; 16]);
        let err = LocalRootKeys::parse(&short, "t").unwrap_err();
        assert!(format!("{err:#}").contains("32 bytes"), "{err:#}");
        let ok = B64.encode([9u8; KEY_BYTES]);
        let parsed = LocalRootKeys::parse(&format!("# c\n\n{ok}\n"), "t").expect("parses");
        assert_eq!(parsed.ids.len(), 1);
        assert_eq!(parsed.ids[0], root_key_id(&[9u8; KEY_BYTES]));
        assert!(parsed.ids[0].starts_with("rk_") && parsed.ids[0].len() == 19);
    }

    #[test]
    fn env_provider_reads_the_variable_and_never_mints() -> R<()> {
        let var = format!("LEGION_TEST_ROOT_KEY_{}", std::process::id());
        let provider = EnvRootKey::new(var.clone());
        assert_eq!(provider.active_id()?, None);
        assert_eq!(provider.mint()?, None);
        assert!(provider.wrap("dk", &[1u8; KEY_BYTES]).is_err());
        let line = generate_root_key_line()?;
        std::env::set_var(&var, &line);
        let id = provider.active_id()?.expect("set");
        let (wrapped, used) = provider.wrap("dk", &[3u8; KEY_BYTES])?;
        assert_eq!(used, id);
        assert_eq!(
            provider.unwrap("dk", &wrapped, &id)?.as_slice(),
            &[3u8; KEY_BYTES][..]
        );
        assert!(provider.export_material().is_some());
        std::env::remove_var(&var);
        Ok(())
    }

    #[test]
    fn load_rows_locks_what_it_cannot_unwrap_and_keeps_what_it_can() -> R<()> {
        let dir = temp_dir();
        let keyring = unlocked_keyring(dir.path());
        let (active_id, _) = keyring.active_key().expect("active");
        let mut rows = keyring.snapshot().rows;
        // A row wrapped under a root key this manager does not have.
        rows.push(KeyringRow {
            key_id: "dk_foreign".into(),
            algorithm: DATA_KEY_ALGORITHM.into(),
            wrapped_key: vec![0u8; 60],
            root_key_id: "rk_0000000000000000".into(),
            root_provider: "file".into(),
            state: KEY_STATE_RETIRED.into(),
            created_at: 1,
            retired_at: Some(2),
        });
        let unlocked = keyring.load_rows(rows);
        assert!(
            unlocked.is_empty(),
            "already-loaded keys are not re-unwrapped"
        );
        let snapshot = keyring.snapshot();
        assert_eq!(snapshot.state, "locked");
        assert_eq!(snapshot.locked.len(), 1);
        assert_eq!(snapshot.locked[0].key_id, "dk_foreign");
        assert!(snapshot.locked[0].reason.contains("rk_0000000000000000"));
        // The active key still encrypts; the foreign one reports locked.
        assert!(keyring.seal("k", "v")?.1.as_deref() == Some(active_id.as_str()));
        let err = keyring.open("k", "AAAA", Some("dk_foreign")).unwrap_err();
        match StorageError::from_anyhow(&err) {
            Some(StorageError::KeyringLocked { key_id, message }) => {
                assert_eq!(key_id, "dk_foreign");
                assert!(message.contains("legion keyring status"), "{message}");
            }
            other => panic!("unexpected {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn reload_from_rows_after_restart_unwraps_with_the_provider() -> R<()> {
        let dir = temp_dir();
        let keyring = unlocked_keyring(dir.path());
        let rows = keyring.snapshot().rows;
        let (stored, key_id) = keyring.seal("k", "secret")?;
        // A "restarted" keyring: same provider, empty memory, rows from the DB.
        let restarted = Keyring::with_provider(
            true,
            Arc::new(file_provider(dir.path())),
            Duration::from_secs(1),
        );
        let unlocked = restarted.load_rows(rows);
        assert_eq!(unlocked.len(), 1);
        assert_eq!(restarted.snapshot().state, "unlocked");
        assert_eq!(restarted.open("k", &stored, key_id.as_deref())?, "secret");
        Ok(())
    }

    #[test]
    fn rotation_due_by_age_and_by_count() -> R<()> {
        let dir = temp_dir();
        let provider = file_provider(dir.path());
        provider.mint()?;
        let keyring = Keyring::with_provider(true, Arc::new(provider), Duration::from_secs(100));
        let (row, key) = keyring.generate_data_key(1_000)?;
        keyring.install_generated(row, key);
        assert!(keyring.rotation_due(1_050).is_none());
        assert!(keyring.rotation_due(1_100).is_some());
        keyring
            .inner
            .encryptions
            .store(DATA_KEY_ENCRYPTION_LIMIT, Ordering::Relaxed);
        assert!(keyring.rotation_due(1_001).unwrap().contains("encryptions"));
        Ok(())
    }

    #[test]
    fn rewrap_under_current_root_uses_the_first_line() -> R<()> {
        let dir = temp_dir();
        let keyring = unlocked_keyring(dir.path());
        let provider = file_provider(dir.path());
        let old_root = provider.active_id()?.unwrap();
        let contents = std::fs::read_to_string(provider.path())?;
        std::fs::write(
            provider.path(),
            format!("{}\n{contents}", generate_root_key_line()?),
        )?;
        let new_root = provider.active_id()?.unwrap();
        let (rows, skipped) = keyring.rewrap_rows_under_current_root()?;
        assert!(skipped.is_empty());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].root_key_id, new_root);
        assert_ne!(rows[0].root_key_id, old_root);
        // A fresh manager holding ONLY the new line unwraps the rewrapped row.
        let new_line = std::fs::read_to_string(provider.path())?
            .lines()
            .next()
            .unwrap_or_default()
            .to_string();
        std::fs::write(provider.path(), format!("{new_line}\n"))?;
        let fresh = Keyring::with_provider(
            true,
            Arc::new(file_provider(dir.path())),
            Duration::from_secs(1),
        );
        assert_eq!(fresh.load_rows(rows).len(), 1);
        assert_eq!(fresh.snapshot().state, "unlocked");
        Ok(())
    }

    #[test]
    fn join_bundle_round_trips_and_is_bound_to_the_cluster() -> R<()> {
        let material = b"# header\nAAAA\n";
        let bundle = seal_root_bundle("secret-1", "legion-raft-abc", material)?;
        assert_eq!(
            open_root_bundle("secret-1", "legion-raft-abc", &bundle)?.as_slice(),
            &material[..]
        );
        assert!(open_root_bundle("secret-2", "legion-raft-abc", &bundle).is_err());
        assert!(open_root_bundle("secret-1", "legion-raft-xyz", &bundle).is_err());
        Ok(())
    }

    #[test]
    fn import_material_only_fills_an_empty_file_provider() -> R<()> {
        let dir = temp_dir();
        let provider = file_provider(dir.path());
        let line = generate_root_key_line()?;
        assert!(provider.import_material(b"garbage").is_err());
        assert!(provider.import_material(format!("{line}\n").as_bytes())?);
        assert_eq!(
            provider.active_id()?,
            Some(root_key_id(&B64.decode(&line)?))
        );
        assert!(!provider.import_material(b"anything")?, "never overwrites");
        Ok(())
    }

    /// A fake provider script: a local AES wrap via python is more than a
    /// test needs, so the script XORs with a fixed pad and appends a checksum
    /// byte — enough to exercise the JSON contract, the timeout, the tamper
    /// check, and the randomization check (which this deliberately weak
    /// fake fails, proving the harness catches it).
    #[cfg(unix)]
    fn fake_provider_script(dir: &Path, deterministic: bool) -> String {
        let path = dir.join("fake-kms.sh");
        let nonce = if deterministic {
            "nonce=0000"
        } else {
            "nonce=$(od -An -N2 -tx1 /dev/urandom | tr -d ' \\n')"
        };
        let script = format!(
            r#"#!/bin/sh
set -eu
req=$(cat)
op=$(printf '%s' "$req" | sed -n 's/.*"op":"\([a-z]*\)".*/\1/p')
case "$op" in
  describe) printf '{{"key_id":"fake-1"}}' ;;
  wrap)
    pt=$(printf '%s' "$req" | sed -n 's/.*"plaintext":"\([^"]*\)".*/\1/p')
    {nonce}
    sum=$(printf '%s' "$pt" | cksum | cut -d' ' -f1)
    printf '{{"ciphertext":"%s","key_id":"fake-1"}}' "$(printf '%s:%s:%s' "$nonce" "$pt" "$sum" | base64 | tr -d '\n')" ;;
  unwrap)
    ct=$(printf '%s' "$req" | sed -n 's/.*"ciphertext":"\([^"]*\)".*/\1/p')
    body=$(printf '%s' "$ct" | base64 -d 2>/dev/null) || {{ echo undecodable >&2; exit 1; }}
    rest=${{body#*:}}
    pt=${{rest%:*}}
    sum=${{rest##*:}}
    # "authenticate": the checksum must match, so any modification is refused.
    check=$(printf '%s' "$pt" | cksum | cut -d' ' -f1)
    [ "$sum" = "$check" ] || {{ echo tampered >&2; exit 1; }}
    printf '{{"plaintext":"%s"}}' "$pt" ;;
  *) echo "unknown op" >&2; exit 2 ;;
esac
"#
        );
        std::fs::write(&path, script).expect("write script");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        path.display().to_string()
    }

    #[cfg(unix)]
    #[test]
    fn exec_provider_contract_round_trips_and_times_out() -> R<()> {
        let dir = temp_dir();
        let script = fake_provider_script(dir.path(), false);
        let provider = ExecRootKey::new(vec![script.clone()], Duration::from_secs(5));
        assert_eq!(provider.active_id()?, Some("fake-1".into()));
        let dek = vec![5u8; KEY_BYTES];
        let (wrapped, id) = provider.wrap("dk_x", &dek)?;
        assert_eq!(id, "fake-1");
        assert_eq!(provider.unwrap("dk_x", &wrapped, &id)?.as_slice(), &dek[..]);
        let bad = ExecRootKey::new(
            vec!["/bin/sh".into(), "-c".into(), "exit 3".into()],
            Duration::from_secs(5),
        );
        let err = bad.active_id().unwrap_err();
        assert!(format!("{err:#}").contains("exited"), "{err:#}");
        let slow = ExecRootKey::new(
            vec!["/bin/sh".into(), "-c".into(), "sleep 5".into()],
            Duration::from_millis(200),
        );
        let err = slow.active_id().unwrap_err();
        assert!(format!("{err:#}").contains("did not answer"), "{err:#}");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn conformance_harness_reports_every_check() -> R<()> {
        let dir = temp_dir();
        let script = fake_provider_script(dir.path(), true);
        let checks = test_exec_provider(&[script], Duration::from_secs(5));
        let by_name: BTreeMap<&str, &ProviderCheck> =
            checks.iter().map(|check| (check.name, check)).collect();
        assert!(by_name["describe"].passed, "{:?}", by_name["describe"]);
        assert!(by_name["wrap"].passed);
        assert!(by_name["unwrap round trip"].passed);
        // The fake's "authentication" is a shape check, so a flipped last
        // byte of the base64 payload is caught as corrupt.
        assert!(
            by_name["tampered ciphertext refused"].passed,
            "{:?}",
            by_name["tampered ciphertext refused"]
        );
        // The deterministic fake fails the randomization check — the harness
        // must say so rather than pass it.
        assert!(!by_name["wrap is randomized"].passed);
        Ok(())
    }
}
