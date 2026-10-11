//! KV encryption keyring operations on the store (docs/kv-encryption.md):
//! loading the data keys, first-start initialization behind the raft
//! upgrade gate, rotation, background rewrap, root-key rewrap, and the
//! break-glass reset. Every write here is a replicated control-lane op like
//! any other store mutation; the keyring's in-memory state is refreshed on
//! every node from the committed `keyring_key` rows.

use super::*;
use crate::storage::keyring::{
    Keyring, KeyringRow, DEF_ROOT_KEY_ENV, KEY_STATE_ACTIVE, KEY_STATE_CHECKPOINT,
    KEY_STATE_RETIRED, PROVIDER_EXEC,
};
use crate::storage::migrations::KEYRING_SCHEMA_VERSION;

const KEYRING_COLUMNS: &str =
    "key_id, algorithm, wrapped_key, root_key_id, root_provider, state, created_at, retired_at";

/// Rows re-encrypted per replicated write during a rewrap. Small enough
/// that a batch commits in milliseconds, large enough that a KV of a few
/// thousand values converges in seconds.
pub const REWRAP_BATCH_ROWS: usize = 256;
/// Pause between rewrap batches so a long rewrap never monopolizes the
/// control lane.
const REWRAP_BATCH_PAUSE: Duration = Duration::from_millis(100);

/// Outcome of [`Store::run_rewrap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewrapRun {
    pub rewrapped: i64,
    /// Non-zero only when a batch made no progress (reported, never spun on).
    pub remaining: i64,
    /// Retired data keys removed once nothing referenced them.
    pub collected_keys: Vec<String>,
}

/// Outcome of [`Store::ensure_keyring_initialized`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyringInit {
    /// `encryption.enabled = false`: nothing to initialize.
    Disabled,
    /// A data key already exists.
    AlreadyInitialized,
    /// This call created the first data key (and, for the file provider,
    /// possibly the root key itself).
    Initialized {
        key_id: String,
        root_key_id: String,
        minted_root_key: bool,
    },
    /// Raft: a voter cannot apply the keyring's schema yet; writes stay
    /// plaintext until every voter is upgraded (the reason names them).
    PendingUpgrade(String),
    /// Raft: this manager does not lead the control lane; the leader
    /// initializes and this node receives the rows through replication.
    NotLeader,
    /// The provider has no root key and cannot mint one (`env`/`exec`).
    NoRootKey(String),
}

/// Progress of one rewrap batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewrapBatch {
    /// Rows re-encrypted (or decrypted, with `to_plaintext`) by this batch.
    pub rewrapped: usize,
    /// Rows still needing the same treatment after this batch.
    pub remaining: i64,
}

/// Outcome of [`Store::rewrap_root_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootKeyRewrap {
    /// Data keys now wrapped under `root_key_id`.
    pub rows: usize,
    pub root_key_id: String,
    /// Data keys that could not be rewrapped because this manager cannot
    /// unwrap them (locked); they keep their old wrapping.
    pub skipped: Vec<String>,
}

/// Outcome of [`Store::reset_keyring`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyringReset {
    /// KV key paths whose values were undecryptable and are now deleted.
    pub deleted_values: Vec<String>,
    /// Data keys dropped from the keyring.
    pub dropped_keys: Vec<String>,
}

impl Store {
    /// The keyring handle (state shared across store clones).
    pub fn keyring(&self) -> &Keyring {
        &self.keyring
    }

    fn keyring_row_from_row(row: &Row<'_>) -> Result<KeyringRow, rusqlite::Error> {
        Ok(KeyringRow {
            key_id: row.get(0)?,
            algorithm: row.get(1)?,
            wrapped_key: row.get(2)?,
            root_key_id: row.get(3)?,
            root_provider: row.get(4)?,
            state: row.get(5)?,
            created_at: row.get(6)?,
            retired_at: row.get(7)?,
        })
    }

    /// Every `keyring_key` row, active first, then newest first.
    pub fn list_keyring_rows(&self) -> R<Vec<KeyringRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {KEYRING_COLUMNS} FROM keyring_key ORDER BY (state = 'active') DESC, created_at DESC, key_id ASC;"
        ))?;
        let rows = stmt.query_map([], Self::keyring_row_from_row)?;
        Self::collect_rows(rows)
    }

    /// Reload the in-memory data keys from the stored rows, unwrapping new
    /// ones with the provider. Returns the ids newly unlocked.
    pub fn load_keyring(&self) -> R<Vec<String>> {
        let rows = self.list_keyring_rows()?;
        Ok(self.keyring.load_rows(rows))
    }

    /// Startup load: never fails the manager; a locked keyring is logged
    /// with the operator-facing reason and shows in `legion keyring status`.
    /// Runs with encryption disabled too, so values encrypted earlier stay
    /// readable (and `rewrap --plaintext` can convert them).
    pub(crate) fn load_keyring_at_start(&self) {
        match self.load_keyring() {
            Ok(_) => {
                let snapshot = self.keyring.snapshot();
                if !snapshot.enabled {
                    let encrypted_rows = self
                        .kv_encryption_counts()
                        .map(|counts| {
                            counts
                                .iter()
                                .filter(|(key, _)| key.is_some())
                                .map(|(_, count)| *count)
                                .sum::<i64>()
                        })
                        .unwrap_or(0);
                    info!(
                        encrypted_rows,
                        "KV encryption at rest is disabled (encryption.enabled = false); new \
                         values are stored as plaintext rows"
                    );
                    return;
                }
                match snapshot.state.as_str() {
                    "unlocked" => info!(
                        provider = %snapshot.provider,
                        location = %snapshot.location,
                        keys = snapshot.rows.len(),
                        active = snapshot.active_key_id.as_deref().unwrap_or("-"),
                        "KV keyring unlocked"
                    ),
                    "locked" => {
                        for locked in &snapshot.locked {
                            warn!(
                                key_id = %locked.key_id,
                                root_key_id = %locked.root_key_id,
                                "KV keyring LOCKED: {}. KV reads and writes under this key fail \
                                 until the root key is available here (`legion keyring status`)",
                                locked.reason
                            );
                        }
                        if let Some(err) = &snapshot.provider_error {
                            warn!("KV root key provider error: {err}");
                        }
                    }
                    _ => {}
                }
            }
            Err(err) => warn!(error = ?err, "Failed loading the KV keyring at start"),
        }
    }

    /// The replicated `keyring_key` table changed on this node (a rotation,
    /// a root-key rewrap, a reset, a snapshot install): unwrap what is new.
    /// Local providers (file/env) reload inline — a file read — while an
    /// `exec` provider is only woken, so a KMS call never runs on the raft
    /// apply thread; the daemon's keyring loop performs that reload.
    pub(crate) fn keyring_rows_changed(&self) {
        if self.keyring.provider().kind() != PROVIDER_EXEC {
            match self.load_keyring() {
                Ok(unlocked) if !unlocked.is_empty() => {
                    info!(keys = ?unlocked, "Unwrapped replicated KV data keys")
                }
                Ok(_) => {}
                Err(err) => warn!(error = ?err, "Failed reloading the KV keyring"),
            }
        }
        self.keyring.wake();
    }

    /// First-start initialization, best effort: mint the root key (file
    /// provider) and write the first data key when this node may. Logs each
    /// outcome; never fails startup.
    pub(crate) fn initialize_keyring_at_start(&self) {
        match self.ensure_keyring_initialized() {
            Ok(KeyringInit::Initialized {
                key_id,
                root_key_id,
                minted_root_key,
            }) => {
                if minted_root_key {
                    info!(
                        root_key_id = %root_key_id,
                        path = %self.keyring.provider().location(),
                        "Minted the KV root key — keep this file with your backups and on every manager"
                    );
                }
                info!(key_id = %key_id, root_key_id = %root_key_id, "Initialized the KV keyring");
            }
            Ok(KeyringInit::PendingUpgrade(reason)) => {
                info!("KV encryption not initialized yet: {reason}")
            }
            Ok(KeyringInit::NoRootKey(reason)) => {
                warn!("KV encryption cannot start: {reason}")
            }
            Ok(
                KeyringInit::AlreadyInitialized | KeyringInit::Disabled | KeyringInit::NotLeader,
            ) => {}
            Err(err) => {
                // A raft follower that raced the election, or a transient
                // pipeline error: the daemon's keyring loop retries.
                if err.chain().any(|cause| {
                    cause
                        .downcast_ref::<crate::storage::raft::driver::NotRaftLeader>()
                        .is_some()
                }) {
                    debug!("KV keyring initialization deferred to the control-lane leader");
                } else {
                    warn!(error = ?err, "Failed initializing the KV keyring at start");
                }
            }
        }
    }

    /// Fetch the cluster's KV root key from another raft member when this
    /// manager's `file` provider has none (R3). Members that were in the
    /// cluster before KV encryption arrived never re-join, so the join
    /// reply never reached them. Returns the member the key came from, or
    /// `None` when nothing was needed or no member had a key to give. A
    /// no-op outside raft mode, with encryption off, and for `env`/`exec`
    /// providers, whose key the operator places on every manager.
    pub fn fetch_root_key_if_missing(&self) -> R<Option<String>> {
        if !self.raft_mode() || !self.keyring.enabled() {
            return Ok(None);
        }
        let provider = self.keyring.provider();
        if provider.kind() != crate::storage::keyring::PROVIDER_FILE
            || provider.export_material().is_some()
        {
            return Ok(None);
        }
        let Some(driver) = self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
        else {
            return Ok(None);
        };
        let Some(from) = driver.fetch_root_key_from_members(&self.keyring)? else {
            return Ok(None);
        };
        info!(
            from = %from,
            location = %provider.location(),
            "Received the cluster's KV root key from another manager and stored it"
        );
        self.keyring_rows_changed();
        Ok(Some(from))
    }

    /// Test hook for the upgrade gate: forget the schema version manager
    /// `peer` (by name) advertised, as if it ran a build without the header.
    /// Errors outside raft mode.
    pub fn raft_forget_peer_schema_version(&self, peer: &str) -> R<()> {
        let driver = self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .context("not a raft store")?;
        driver.forget_peer_schema_version(crate::storage::raft::types::fnv1a64(peer));
        Ok(())
    }

    /// Why the leader must not create the keyring yet, or `None` when it
    /// may: in raft mode every voter must advertise a schema version that
    /// includes `keyring_key`, because a manager below it cannot apply the
    /// changeset (docs/upgrades.md). Never closed outside raft mode.
    pub fn keyring_upgrade_gate(&self) -> Option<String> {
        if !self.raft_mode() {
            return None;
        }
        let driver = self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()?;
        let behind = driver.voters_missing_schema_version(KEYRING_SCHEMA_VERSION);
        if behind.is_empty() {
            return None;
        }
        Some(format!(
            "waiting for manager(s) {} to run a build with the KV keyring (schema version \
             {KEYRING_SCHEMA_VERSION}); KV values are stored exactly as before until every voter \
             is upgraded",
            behind.join(", ")
        ))
    }

    /// Create the first data key if none exists — and, for the file
    /// provider, the root key itself. Idempotent; the write refuses to add a
    /// second active key if two callers race.
    pub fn ensure_keyring_initialized(&self) -> R<KeyringInit> {
        if !self.keyring.enabled() {
            return Ok(KeyringInit::Disabled);
        }
        if self
            .list_keyring_rows()?
            .iter()
            .any(|row| row.state != KEY_STATE_CHECKPOINT)
        {
            self.keyring.set_pending_upgrade(None);
            self.ensure_checkpoint_key()?;
            return Ok(KeyringInit::AlreadyInitialized);
        }
        // Raft followers and streaming standbys leave this to the writer.
        if !self.is_raft_leader(Lane::Control) {
            return Ok(KeyringInit::NotLeader);
        }
        if let Some(reason) = self.keyring_upgrade_gate() {
            self.keyring.set_pending_upgrade(Some(reason.clone()));
            return Ok(KeyringInit::PendingUpgrade(reason));
        }
        self.keyring.set_pending_upgrade(None);

        let provider = self.keyring.provider();
        let mut minted_root_key = false;
        let root_key_id = match provider.active_id()? {
            Some(id) => id,
            None => match provider.mint()? {
                Some(id) => {
                    minted_root_key = true;
                    id
                }
                None => {
                    return Ok(KeyringInit::NoRootKey(format!(
                        "root key provider `{}` has no key at {} — set it (`legion keyring \
                         root-key generate` prints one; the `env` provider reads \
                         {DEF_ROOT_KEY_ENV}) and restart, or switch to the `file` provider, \
                         which mints its own",
                        provider.kind(),
                        provider.location()
                    )))
                }
            },
        };

        let now = Self::now_unix();
        let (row, key) = self.keyring.generate_data_key(now)?;
        let to_insert = row.clone();
        let inserted = self.replicated(Lane::Control, "initialize_keyring", move |s| {
            s.insert_first_keyring_row_apply(&to_insert)
        })?;
        if !inserted {
            // Lost the race to another initializer on this leader.
            self.load_keyring()?;
            return Ok(KeyringInit::AlreadyInitialized);
        }
        let key_id = row.key_id.clone();
        self.keyring.install_generated(row, key);
        self.ensure_checkpoint_key()?;
        // Deliberately a log line and not a scheduler event: every fresh
        // store initializes once at start, and an event there would put a
        // row in every new cluster's journal before any real work happened.
        // Rotation, rewrap, lock/unlock, and reset — the transitions an
        // operator acts on — do emit events.
        Ok(KeyringInit::Initialized {
            key_id,
            root_key_id,
            minted_root_key,
        })
    }

    /// Mint the cluster's checkpoint key when the keyring has none: the key
    /// published checkpoint parts are sealed under
    /// (internal_docs/plans/checkpoint-encryption-plan.md §6.1). Runs on the
    /// control-lane leader from keyring initialization and the daemon's
    /// keyring loop, so a cluster whose keyring predates checkpoint keys
    /// gets one on the next tick. Returns the minted key id, or `None` when
    /// one exists or this node may not mint (a follower, a closed upgrade
    /// gate, no root key).
    pub fn ensure_checkpoint_key(&self) -> R<Option<String>> {
        if !self.keyring.enabled()
            || self
                .list_keyring_rows()?
                .iter()
                .any(|row| row.state == KEY_STATE_CHECKPOINT)
            || !self.is_raft_leader(Lane::Control)
            || self.keyring_upgrade_gate().is_some()
            || self.keyring.provider().active_id()?.is_none()
        {
            return Ok(None);
        }
        let (row, key) = self.keyring.generate_checkpoint_key(Self::now_unix())?;
        let to_insert = row.clone();
        let inserted = self.replicated(Lane::Control, "mint_checkpoint_key", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let existing: i64 = tx.query_row(
                "SELECT count(*) FROM keyring_key WHERE state = ?1;",
                [KEY_STATE_CHECKPOINT],
                |r| r.get(0),
            )?;
            if existing > 0 {
                return Ok(false);
            }
            Self::insert_keyring_row_in(&tx, &to_insert)?;
            tx.commit()?;
            Ok(true)
        })?;
        if !inserted {
            self.load_keyring()?;
            return Ok(None);
        }
        let key_id = row.key_id.clone();
        self.keyring.install_generated(row, key);
        info!(key_id = %key_id, "Minted the checkpoint key for published checkpoints");
        Ok(Some(key_id))
    }

    /// Test helper: give this store the same `keyring_key` rows as `other`,
    /// as a replica that applied the same `initialize_keyring` entry has.
    /// Two stores started independently each mint their own data key, so a
    /// changeset captured on one only decrypts on the other after this
    /// (both must also share the root key — see
    /// `keyring::test_support::shared_root_key_path`).
    #[cfg(test)]
    pub(crate) fn adopt_keyring_from(&self, other: &Store) -> R<()> {
        let rows = other.list_keyring_rows()?;
        {
            let mut conn = self.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            tx.execute("DELETE FROM keyring_key;", [])?;
            for row in &rows {
                Self::insert_keyring_row_in(&tx, row)?;
            }
            tx.commit()?;
        }
        self.keyring_rows_changed();
        Ok(())
    }

    /// Insert the first row only if the table is still empty (idempotency
    /// under a race). Returns whether the insert happened.
    fn insert_first_keyring_row_apply(&self, row: &KeyringRow) -> R<bool> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let existing: i64 = tx.query_row(
            "SELECT count(*) FROM keyring_key WHERE state != ?1;",
            [KEY_STATE_CHECKPOINT],
            |r| r.get(0),
        )?;
        if existing > 0 {
            return Ok(false);
        }
        Self::insert_keyring_row_in(&tx, row)?;
        tx.commit()?;
        Ok(true)
    }

    fn insert_keyring_row_in(conn: &Connection, row: &KeyringRow) -> R<()> {
        conn.execute(
            &format!(
                "INSERT INTO keyring_key ({KEYRING_COLUMNS}) VALUES (:key_id, :algorithm, \
                 :wrapped_key, :root_key_id, :root_provider, :state, :created_at, :retired_at);"
            ),
            named_params! {
                ":key_id": row.key_id,
                ":algorithm": row.algorithm,
                ":wrapped_key": row.wrapped_key,
                ":root_key_id": row.root_key_id,
                ":root_provider": row.root_provider,
                ":state": row.state,
                ":created_at": row.created_at,
                ":retired_at": row.retired_at,
            },
        )?;
        Ok(())
    }

    /// Rotate: a new active data key in one replicated write (the old one
    /// retires). Values already stored stay readable under the retired key;
    /// the background rewrap converges them (docs/kv-encryption.md).
    pub fn rotate_data_key(&self, reason: &str) -> R<KeyringRow> {
        if !self.keyring.enabled() {
            bail!("KV encryption is disabled (encryption.enabled = false); nothing to rotate");
        }
        if self.list_keyring_rows()?.is_empty() {
            bail!(
                "the KV keyring is not initialized on this cluster yet (see `legion keyring \
                 status`); there is no key to rotate"
            );
        }
        let now = Self::now_unix();
        let (row, key) = self.keyring.generate_data_key(now)?;
        let to_insert = row.clone();
        self.replicated(Lane::Control, "rotate_data_key", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            tx.execute(
                "UPDATE keyring_key SET state = :retired, retired_at = :now WHERE state = :active;",
                named_params! {
                    ":retired": KEY_STATE_RETIRED,
                    ":now": now,
                    ":active": KEY_STATE_ACTIVE,
                },
            )?;
            Self::insert_keyring_row_in(&tx, &to_insert)?;
            tx.commit()?;
            Ok(())
        })?;
        self.keyring.install_generated(row.clone(), key);
        info!(key_id = %row.key_id, reason, "Rotated the KV data key");
        self.emit_event(
            EventDraft::info(
                "keyring.rotated",
                format!("KV data key rotated to {} ({reason})", row.key_id),
            )
            .meta(json!({ "keyId": row.key_id, "reason": reason })),
        );
        Ok(row)
    }

    /// Per-data-key value counts (`None` = plaintext rows).
    pub fn kv_encryption_counts(&self) -> R<BTreeMap<Option<String>, i64>> {
        let conn = self.read_connection()?;
        let mut stmt =
            conn.prepare("SELECT enc_key_id, count(*) FROM kv_entry GROUP BY enc_key_id;")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut counts = BTreeMap::new();
        for row in rows {
            let (key_id, count) = row?;
            counts.insert(key_id, count);
        }
        Ok(counts)
    }

    /// Rows a rewrap still has to touch: every row not under the active key
    /// (plaintext included), or — with `to_plaintext` — every encrypted row.
    pub fn kv_rows_pending_rewrap(&self, to_plaintext: bool) -> R<i64> {
        let conn = self.read_connection()?;
        Self::count_pending_rewrap(&conn, &self.keyring, to_plaintext)
    }

    fn count_pending_rewrap(conn: &Connection, keyring: &Keyring, to_plaintext: bool) -> R<i64> {
        let count = if to_plaintext {
            conn.query_row(
                "SELECT count(*) FROM kv_entry WHERE enc_key_id IS NOT NULL;",
                [],
                |r| r.get(0),
            )?
        } else {
            let Some((active, _)) = keyring.active_key() else {
                return Ok(0);
            };
            conn.query_row(
                "SELECT count(*) FROM kv_entry WHERE enc_key_id IS NULL OR enc_key_id != ?1;",
                [active],
                |r| r.get(0),
            )?
        };
        Ok(count)
    }

    /// Re-encrypt up to [`REWRAP_BATCH_ROWS`] values under the active data
    /// key (or decrypt them to plaintext rows with `to_plaintext`), in one
    /// replicated write. Versions and timestamps are untouched — the values
    /// did not change — and nothing on the reactive path fires. Call until
    /// `remaining` is 0.
    pub fn rewrap_kv_batch(&self, to_plaintext: bool) -> R<RewrapBatch> {
        if !to_plaintext {
            if !self.keyring.enabled() {
                bail!(
                    "KV encryption is disabled (encryption.enabled = false); use `rewrap \
                     --plaintext` to decrypt the remaining rows, or enable encryption first"
                );
            }
            if self.keyring.active_key().is_none() {
                let snapshot = self.keyring.snapshot();
                bail!(
                    "no usable active data key (keyring state `{}`); see `legion keyring status`",
                    snapshot.state
                );
            }
        }
        let keyring = self.keyring.clone();
        self.replicated(Lane::Control, "rewrap_kv_batch", move |s| {
            s.rewrap_kv_batch_apply(&keyring, to_plaintext)
        })
    }

    fn rewrap_kv_batch_apply(&self, keyring: &Keyring, to_plaintext: bool) -> R<RewrapBatch> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let raws: Vec<KvRaw> = {
            let (sql, params): (String, Vec<String>) = if to_plaintext {
                (
                    format!("{KV_ENTRY_SELECT} WHERE enc_key_id IS NOT NULL ORDER BY key ASC LIMIT {REWRAP_BATCH_ROWS};"),
                    Vec::new(),
                )
            } else {
                let (active, _) = keyring
                    .active_key()
                    .context("no active data key to rewrap under")?;
                (
                    format!("{KV_ENTRY_SELECT} WHERE enc_key_id IS NULL OR enc_key_id != ?1 ORDER BY key ASC LIMIT {REWRAP_BATCH_ROWS};"),
                    vec![active],
                )
            };
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
                self.kv_raw_from_row(row)
            })?;
            Self::collect_rows(rows)?
        };
        let mut rewrapped = 0usize;
        for raw in raws {
            let key_path = raw.entry.key.clone();
            let version = raw.entry.version;
            let plaintext = keyring.open(&key_path, &raw.entry.value, raw.enc_key_id.as_deref())?;
            let (stored, enc_key_id) = if to_plaintext {
                (plaintext, None)
            } else {
                keyring.seal(&key_path, &plaintext)?
            };
            rewrapped += tx.execute(
                "UPDATE kv_entry SET value = :value, enc_key_id = :enc_key_id WHERE key = :key AND version = :version;",
                named_params! {
                    ":value": stored,
                    ":enc_key_id": enc_key_id,
                    ":key": key_path,
                    ":version": version,
                },
            )?;
        }
        let remaining = Self::count_pending_rewrap(&tx, keyring, to_plaintext)?;
        tx.commit()?;
        Ok(RewrapBatch {
            rewrapped,
            remaining,
        })
    }

    /// Rewrap to completion: batch after batch until nothing is pending,
    /// then garbage-collect the data keys nothing references. Shared by the
    /// `RewrapKv` RPC and the daemon's keyring loop; refuses to run twice at
    /// once on one manager.
    pub fn run_rewrap(&self, to_plaintext: bool) -> R<RewrapRun> {
        if self.keyring.rewrap_running() {
            bail!("a rewrap is already running on this manager (`legion keyring status` shows its progress)");
        }
        self.keyring.set_rewrap_running(true, None);
        let started = Instant::now();
        let result = (|| -> R<(i64, i64)> {
            let mut total = 0i64;
            loop {
                let batch = self.rewrap_kv_batch(to_plaintext)?;
                total += batch.rewrapped as i64;
                if batch.remaining == 0 || batch.rewrapped == 0 {
                    return Ok((total, batch.remaining));
                }
                // Let ordinary writes through between batches.
                std::thread::sleep(REWRAP_BATCH_PAUSE);
            }
        })();
        match result {
            Ok((rewrapped, remaining)) => {
                self.keyring.set_rewrap_running(false, None);
                let collected_keys = if to_plaintext {
                    Vec::new()
                } else {
                    self.gc_retired_keys()?
                };
                if rewrapped > 0 {
                    info!(
                        rewrapped,
                        remaining,
                        to_plaintext,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "KV rewrap completed"
                    );
                    self.emit_event(
                        EventDraft::info(
                            "keyring.rewrap_completed",
                            if to_plaintext {
                                format!("{rewrapped} KV value(s) converted to plaintext rows")
                            } else {
                                format!("{rewrapped} KV value(s) re-encrypted under the active data key")
                            },
                        )
                        .meta(json!({
                            "rewrapped": rewrapped,
                            "remaining": remaining,
                            "plaintext": to_plaintext,
                            "collectedKeys": collected_keys,
                        })),
                    );
                }
                Ok(RewrapRun {
                    rewrapped,
                    remaining,
                    collected_keys,
                })
            }
            Err(err) => {
                self.keyring
                    .set_rewrap_running(false, Some(format!("{err:#}")));
                Err(err)
            }
        }
    }

    /// Refuse to serve a restored store whose KV values this manager cannot
    /// decrypt: `--restore-from` is an explicit operator action with the
    /// operator present, so failing fast beats a cluster that is up with
    /// unreadable secrets (docs/kv-encryption.md, docs/upgrades.md).
    pub fn verify_keyring_after_restore(&self, backup: &str) -> R<()> {
        let snapshot = self.keyring.snapshot();
        if snapshot.state != "locked" {
            return Ok(());
        }
        let root_key_ids: BTreeSet<String> = snapshot
            .locked
            .iter()
            .map(|locked| locked.root_key_id.clone())
            .collect();
        bail!(
            "the store restored from {backup} holds KV values encrypted under root key(s) {} \
             that are not available on this manager (provider `{}` at {}). Copy the original \
             manager's root key into place (or set {DEF_ROOT_KEY_ENV}) and start again. If \
             that root key is gone for good, start WITHOUT --restore-from (the restored store \
             is already in place) and run `legion keyring reset \
             --i-understand-encrypted-values-will-be-lost`, which deletes the unreadable \
             values and starts a fresh keyring.",
            root_key_ids.into_iter().collect::<Vec<_>>().join(", "),
            snapshot.provider,
            snapshot.location
        )
    }

    /// Delete retired data keys no value references any more. Returns the
    /// ids removed. The active key is never a candidate.
    pub fn gc_retired_keys(&self) -> R<Vec<String>> {
        let counts = self.kv_encryption_counts()?;
        let candidates: Vec<String> = self
            .list_keyring_rows()?
            .into_iter()
            .filter(|row| row.state == KEY_STATE_RETIRED)
            .filter(|row| counts.get(&Some(row.key_id.clone())).copied().unwrap_or(0) == 0)
            .map(|row| row.key_id)
            .collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let ids = candidates.clone();
        self.replicated(Lane::Control, "gc_retired_keys", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            for id in &ids {
                // Re-check inside the write: a value may have been rewrapped
                // back under this key between the scan and now (it cannot —
                // only the active key encrypts — but the guard is free).
                let referenced: i64 = tx.query_row(
                    "SELECT count(*) FROM kv_entry WHERE enc_key_id = ?1;",
                    [id],
                    |r| r.get(0),
                )?;
                if referenced == 0 {
                    tx.execute(
                        "DELETE FROM keyring_key WHERE key_id = ?1 AND state = ?2;",
                        rusqlite::params![id, KEY_STATE_RETIRED],
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        })?;
        info!(keys = ?candidates, "Garbage-collected retired KV data keys");
        Ok(candidates)
    }

    /// Re-wrap every data key under the provider's current root key
    /// (`legion keyring root-key rewrap`): one replicated write, no value
    /// touched. After it, root keys that wrapped nothing can be deleted.
    pub fn rewrap_root_key(&self) -> R<RootKeyRewrap> {
        if !self.keyring.enabled() {
            bail!("KV encryption is disabled (encryption.enabled = false)");
        }
        // Re-read the provider (a new first line in the key file) before wrapping.
        self.load_keyring()?;
        let (rows, skipped) = self.keyring.rewrap_rows_under_current_root()?;
        let root_key_id = rows
            .first()
            .map(|row| row.root_key_id.clone())
            .or_else(|| self.keyring.provider().active_id().ok().flatten())
            .unwrap_or_default();
        if rows.is_empty() {
            return Ok(RootKeyRewrap {
                rows: 0,
                root_key_id,
                skipped,
            });
        }
        let to_write = rows.clone();
        self.replicated(Lane::Control, "rewrap_root_key", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            for row in &to_write {
                tx.execute(
                    "UPDATE keyring_key SET wrapped_key = :wrapped_key, root_key_id = :root_key_id, root_provider = :root_provider WHERE key_id = :key_id;",
                    named_params! {
                        ":wrapped_key": row.wrapped_key,
                        ":root_key_id": row.root_key_id,
                        ":root_provider": row.root_provider,
                        ":key_id": row.key_id,
                    },
                )?;
            }
            tx.commit()?;
            Ok(())
        })?;
        info!(root_key_id = %root_key_id, rows = rows.len(), "Rewrapped the KV keyring under the current root key");
        self.emit_event(
            EventDraft::info(
                "keyring.root_key_rewrapped",
                format!(
                    "{} KV data key(s) rewrapped under root key {root_key_id}",
                    rows.len()
                ),
            )
            .meta(json!({ "rootKeyId": root_key_id, "keys": rows.len(), "skipped": skipped })),
        );
        Ok(RootKeyRewrap {
            rows: rows.len(),
            root_key_id,
            skipped,
        })
    }

    /// Break-glass: the root key is gone for good. Deletes every KV value
    /// this manager cannot decrypt, drops the data keys it cannot unwrap,
    /// and initializes a fresh keyring. Returns what was lost so the caller
    /// can say it and fail the jobs that depended on those keys.
    pub fn reset_keyring(&self) -> R<KeyringReset> {
        let loaded = self.keyring.loaded_key_ids();
        let unusable: Vec<String> = self
            .list_keyring_rows()?
            .into_iter()
            .filter(|row| !loaded.contains(&row.key_id))
            .map(|row| row.key_id)
            .collect();
        let ids = unusable.clone();
        let deleted_values = self.replicated(Lane::Control, "reset_keyring", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let mut deleted = Vec::new();
            for id in &ids {
                let mut stmt =
                    tx.prepare("SELECT key FROM kv_entry WHERE enc_key_id = ?1 ORDER BY key ASC;")?;
                let keys = stmt.query_map([id], |row| row.get::<_, String>(0))?;
                let keys = Self::collect_rows(keys)?;
                drop(stmt);
                tx.execute("DELETE FROM kv_entry WHERE enc_key_id = ?1;", [id])?;
                tx.execute("DELETE FROM keyring_key WHERE key_id = ?1;", [id])?;
                deleted.extend(keys);
            }
            tx.commit()?;
            Ok(deleted)
        })?;
        self.keyring_rows_changed();
        if self.keyring.enabled() && self.keyring.active_key().is_none() {
            // Every key was unusable: start over with a fresh one.
            self.ensure_keyring_initialized()?;
        }
        // A dropped checkpoint key leaves every part sealed under it
        // unreadable; new publishes need a new one.
        self.ensure_checkpoint_key()?;
        warn!(
            values = deleted_values.len(),
            keys = ?unusable,
            "KV keyring RESET: undecryptable values deleted and their data keys dropped"
        );
        self.emit_event(
            EventDraft::warn(
                "keyring.reset",
                format!(
                    "KV keyring reset: {} undecryptable value(s) deleted, {} data key(s) dropped",
                    deleted_values.len(),
                    unusable.len()
                ),
            )
            .meta(json!({ "deletedValues": deleted_values, "droppedKeys": unusable })),
        );
        Ok(KeyringReset {
            deleted_values,
            dropped_keys: unusable,
        })
    }

    /// A raw look at how a KV row is stored (tests, the status surface): the
    /// on-disk `value` and `enc_key_id`, without decrypting.
    pub fn kv_stored_form(&self, key: &str) -> R<Option<(String, Option<String>)>> {
        let conn = self.read_connection()?;
        let row = conn
            .query_row(
                "SELECT value, enc_key_id FROM kv_entry WHERE key = ?1;",
                [key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        Ok(row)
    }

    /// Refresh the keyring gauges (`legion_keyring_*`, `legion_kv_rows`).
    pub fn refresh_keyring_gauges(&self) {
        let telemetry = crate::manager::telemetry::metrics();
        let snapshot = self.keyring.snapshot();
        telemetry
            .keyring_unlocked
            .set(i64::from(snapshot.state == "unlocked"));
        let count = |state: &str| {
            snapshot
                .rows
                .iter()
                .filter(|row| row.state == state)
                .count() as i64
        };
        telemetry.keyring_keys.set_all(&[
            (vec!["active".to_string()], count(KEY_STATE_ACTIVE)),
            (vec!["retired".to_string()], count(KEY_STATE_RETIRED)),
            (vec!["checkpoint".to_string()], count(KEY_STATE_CHECKPOINT)),
        ]);
        let age = self
            .keyring
            .active_key()
            .map(|(_, created)| Self::now_unix().saturating_sub(created).max(0))
            .unwrap_or(0);
        telemetry.keyring_active_key_age_seconds.set(age);
        if let Ok(counts) = self.kv_encryption_counts() {
            let plaintext = counts.get(&None).copied().unwrap_or(0);
            let encrypted: i64 = counts
                .iter()
                .filter(|(key, _)| key.is_some())
                .map(|(_, count)| *count)
                .sum();
            telemetry.kv_rows.set_all(&[
                (vec!["encrypted".to_string()], encrypted),
                (vec!["plaintext".to_string()], plaintext),
            ]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::keyring::{generate_root_key_line, ROOT_KEY_DIR, ROOT_KEY_FILE};
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::KvValueType;
    use std::path::PathBuf;

    fn store_dir() -> PathBuf {
        tempfile::Builder::new()
            .prefix("legion-keyring-store-")
            .tempdir()
            .expect("tempdir")
            .keep()
    }

    fn config_at(dir: &Path, enabled: bool) -> ManagerConfigFull {
        let mut config = ManagerConfigFull {
            store_path: dir.to_str().expect("utf-8 path").to_string(),
            ..Default::default()
        };
        config.encryption.enabled = enabled;
        config
    }

    fn start(dir: &Path) -> R<Store> {
        Store::start_storage(&config_at(dir, true))
    }

    fn root_key_path(dir: &Path) -> PathBuf {
        dir.join(ROOT_KEY_DIR).join(ROOT_KEY_FILE)
    }

    fn put(store: &Store, key: &str, value: &str) -> R<KvEntry> {
        store.put_kv(key, value, KvValueType::String, None, None)
    }

    #[test]
    fn values_are_encrypted_on_disk_and_readable_through_the_api() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        assert_eq!(store.keyring().snapshot().state, "unlocked");
        assert!(
            root_key_path(&dir).exists(),
            "root key minted on first start"
        );

        let entry = put(&store, "prod/db/password", "hunter2")?;
        assert!(entry.encrypted);
        assert_eq!(entry.value, "hunter2");
        let (stored, key_id) = store.kv_stored_form("prod/db/password")?.expect("row");
        assert_ne!(stored, "hunter2");
        assert!(!stored.contains("hunter2"));
        assert!(key_id.expect("enc_key_id").starts_with("dk_"));
        let read = store.get_kv("prod/db/password")?.expect("entry");
        assert_eq!(read.value, "hunter2");
        assert!(read.encrypted);
        assert_eq!(store.list_kv("prod/")?[0].value, "hunter2");

        // JSON patch decrypts, patches, re-encrypts.
        put(&store, "cfg/json", r#"{"a":1}"#)?;
        store.put_kv("cfg/json", r#"{"a":1}"#, KvValueType::Json, None, None)?;
        let patched = store.patch_kv(
            "cfg/json",
            r#"[{"op":"replace","path":"/a","value":2}]"#,
            None,
            None,
        )?;
        assert_eq!(patched.value, r#"{"a":2}"#);
        assert!(patched.encrypted);
        let (stored, _) = store.kv_stored_form("cfg/json")?.expect("row");
        assert!(!stored.contains("\"a\""));
        Ok(())
    }

    #[test]
    fn plaintext_rows_stay_readable_until_a_rewrap_converts_them() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        {
            // A row from before the feature: plaintext, no key id.
            let conn = store.write_connection()?;
            conn.execute(
                "INSERT INTO kv_entry (key, value, value_type, version, created_at, updated_at) \
                 VALUES ('legacy/flag', 'on', 1, 7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');",
                [],
            )?;
        }
        let legacy = store.get_kv("legacy/flag")?.expect("legacy row");
        assert_eq!(legacy.value, "on");
        assert!(!legacy.encrypted);
        assert_eq!(store.kv_rows_pending_rewrap(false)?, 1);
        assert_eq!(store.kv_encryption_counts()?.get(&None), Some(&1));

        let run = store.run_rewrap(false)?;
        assert_eq!(run.rewrapped, 1);
        assert_eq!(run.remaining, 0);
        let converted = store.get_kv("legacy/flag")?.expect("row");
        assert_eq!(converted.value, "on");
        assert!(converted.encrypted);
        assert_eq!(converted.version, 7, "a rewrap never bumps the version");
        assert_eq!(converted.updated_at, "2026-01-01T00:00:00Z");
        assert_eq!(store.kv_rows_pending_rewrap(false)?, 0);
        Ok(())
    }

    #[test]
    fn a_missing_root_key_locks_reads_and_writes_but_not_deletes() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        put(&store, "a/secret", "one")?;
        put(&store, "a/other", "two")?;
        let original = std::fs::read_to_string(root_key_path(&dir))?;

        // The wrong root key on this manager at (re)start: every data key
        // locks. (Keys already unwrapped in memory stay usable until a
        // restart — losing the file mid-flight never takes running reads
        // down; the next start is where a missing key bites.)
        drop(store);
        std::fs::write(
            root_key_path(&dir),
            format!("{}\n", generate_root_key_line()?),
        )?;
        let store = start(&dir)?;
        let snapshot = store.keyring().snapshot();
        assert_eq!(snapshot.state, "locked");
        // The data key and the checkpoint key.
        assert_eq!(snapshot.locked.len(), 2);

        let err = store.get_kv("a/secret").unwrap_err();
        match StorageError::from_anyhow(&err) {
            Some(StorageError::KeyringLocked { message, .. }) => {
                assert!(message.contains("legion keyring status"), "{message}");
                assert!(message.contains(&root_key_path(&dir).display().to_string()));
            }
            other => panic!("expected KeyringLocked, got {other:?}"),
        }
        let err = put(&store, "a/new", "three").unwrap_err();
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::KeyringLocked { .. })
        ));
        // Deleting never needs the value.
        store.delete_kv("a/other", None, None)?;
        assert!(store.kv_stored_form("a/other")?.is_none());

        // The key comes back: the next read heals itself, no restart.
        std::fs::write(root_key_path(&dir), original)?;
        assert_eq!(store.get_kv("a/secret")?.expect("row").value, "one");
        assert_eq!(store.keyring().snapshot().state, "unlocked");
        Ok(())
    }

    #[test]
    fn rotation_keeps_old_values_readable_and_rewrap_converges_without_version_bumps() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        let (first_key, _) = store.keyring().active_key().expect("active");
        for i in 0..3 {
            put(&store, &format!("k/{i}"), &format!("v{i}"))?;
        }
        let before: Vec<KvEntry> = store.list_kv("k/")?;

        let checkpoint_key = store
            .list_keyring_rows()?
            .into_iter()
            .find(|row| row.state == KEY_STATE_CHECKPOINT)
            .expect("checkpoint key minted with the keyring");
        let rotated = store.rotate_data_key("test")?;
        assert_ne!(rotated.key_id, first_key);
        let data_keys = |store: &Store| -> R<Vec<KeyringRow>> {
            Ok(store
                .list_keyring_rows()?
                .into_iter()
                .filter(|row| row.state != KEY_STATE_CHECKPOINT)
                .collect())
        };
        let rows = data_keys(&store)?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key_id, rotated.key_id);
        assert_eq!(rows[0].state, KEY_STATE_ACTIVE);
        assert_eq!(rows[1].state, KEY_STATE_RETIRED);
        // Old values still read under the retired key; new writes use the new one.
        assert_eq!(store.get_kv("k/0")?.expect("row").value, "v0");
        let (_, key_id) = store.kv_stored_form("k/0")?.expect("row");
        assert_eq!(key_id.as_deref(), Some(first_key.as_str()));
        put(&store, "k/new", "fresh")?;
        let (_, key_id) = store.kv_stored_form("k/new")?.expect("row");
        assert_eq!(key_id.as_deref(), Some(rotated.key_id.as_str()));
        assert_eq!(store.kv_rows_pending_rewrap(false)?, 3);

        let run = store.run_rewrap(false)?;
        assert_eq!(run.rewrapped, 3);
        assert_eq!(run.remaining, 0);
        assert_eq!(run.collected_keys, vec![first_key.clone()]);
        assert_eq!(data_keys(&store)?.len(), 1, "retired key collected");
        // Rotation and collection never touch the checkpoint key: published
        // checkpoints sealed under it stay readable.
        assert!(store.list_keyring_rows()?.contains(&checkpoint_key));
        let after: Vec<KvEntry> = store.list_kv("k/")?;
        for (old, new) in before.iter().zip(after.iter().filter(|e| e.key != "k/new")) {
            assert_eq!(old.key, new.key);
            assert_eq!(old.value, new.value);
            assert_eq!(old.version, new.version, "rewrap must not bump versions");
            assert_eq!(old.updated_at, new.updated_at);
        }
        for entry in &after {
            let (_, key_id) = store.kv_stored_form(&entry.key)?.expect("row");
            assert_eq!(key_id.as_deref(), Some(rotated.key_id.as_str()));
        }
        Ok(())
    }

    #[test]
    fn root_key_rotation_is_prepend_rewrap_then_delete() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        put(&store, "s/1", "one")?;
        let old_root = store.keyring().provider().active_id()?.expect("root");
        let contents = std::fs::read_to_string(root_key_path(&dir))?;
        let new_line = generate_root_key_line()?;
        std::fs::write(root_key_path(&dir), format!("{new_line}\n{contents}"))?;

        let outcome = store.rewrap_root_key()?;
        // The data key and the checkpoint key.
        assert_eq!(outcome.rows, 2);
        assert_ne!(outcome.root_key_id, old_root);
        assert!(outcome.skipped.is_empty());
        assert_eq!(
            store.list_keyring_rows()?[0].root_key_id,
            outcome.root_key_id
        );

        // Delete the old line: a fresh manager with only the new key reads everything.
        std::fs::write(root_key_path(&dir), format!("{new_line}\n"))?;
        drop(store);
        let reopened = start(&dir)?;
        assert_eq!(reopened.keyring().snapshot().state, "unlocked");
        assert_eq!(reopened.get_kv("s/1")?.expect("row").value, "one");

        // Deleting the old line BEFORE rewrapping locks the next start,
        // naming the missing id.
        put(&reopened, "s/2", "two")?;
        drop(reopened);
        let newer = generate_root_key_line()?;
        std::fs::write(root_key_path(&dir), format!("{newer}\n"))?;
        let locked = start(&dir)?;
        let snapshot = locked.keyring().snapshot();
        assert_eq!(snapshot.state, "locked");
        assert!(snapshot.locked[0].reason.contains(&outcome.root_key_id));
        assert!(
            !root_key_path(&dir).exists() || {
                // The wrong key is still the file's only line: nothing was minted over it.
                std::fs::read_to_string(root_key_path(&dir))?.trim() == newer
            }
        );
        Ok(())
    }

    #[test]
    fn reset_deletes_undecryptable_values_and_starts_a_fresh_keyring() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        put(&store, "lost/one", "x")?;
        put(&store, "lost/two", "y")?;
        put(&store, "kept/plain", "z")?;
        {
            // A plaintext row survives a reset untouched.
            let conn = store.write_connection()?;
            conn.execute(
                "UPDATE kv_entry SET value = 'z', enc_key_id = NULL WHERE key = 'kept/plain';",
                [],
            )?;
        }
        drop(store);
        std::fs::write(
            root_key_path(&dir),
            format!("{}\n", generate_root_key_line()?),
        )?;
        let store = start(&dir)?;
        assert_eq!(store.keyring().snapshot().state, "locked");

        let old_checkpoint_key = store
            .list_keyring_rows()?
            .into_iter()
            .find(|row| row.state == KEY_STATE_CHECKPOINT)
            .expect("checkpoint key")
            .key_id;
        let reset = store.reset_keyring()?;
        assert_eq!(
            store
                .get_kv("kept/plain")?
                .expect("plaintext row kept")
                .value,
            "z"
        );
        assert_eq!(
            reset.deleted_values,
            vec!["lost/one".to_string(), "lost/two".to_string()]
        );
        // The data key and the checkpoint key it shared a root key with.
        assert_eq!(reset.dropped_keys.len(), 2);
        assert!(reset.dropped_keys.contains(&old_checkpoint_key));
        assert_eq!(store.keyring().snapshot().state, "unlocked");
        assert!(store.get_kv("lost/one")?.is_none());
        put(&store, "fresh/one", "z")?;
        assert_eq!(store.get_kv("fresh/one")?.expect("row").value, "z");
        // A fresh data key and a fresh checkpoint key.
        let rows = store.list_keyring_rows()?;
        assert_eq!(rows.len(), 2);
        let fresh = rows
            .iter()
            .find(|row| row.state == KEY_STATE_CHECKPOINT)
            .expect("a new checkpoint key");
        assert_ne!(fresh.key_id, old_checkpoint_key);
        let digest = crate::content_store::ContentDigest::of_bytes(b"part");
        let err = store
            .keyring()
            .checkpoint_part_key(Some(&old_checkpoint_key), &digest)
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("is not in this cluster's keyring"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn rewrap_to_plaintext_then_disabled_config_serves_and_writes_plaintext() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        put(&store, "p/1", "one")?;
        let run = store.run_rewrap(true)?;
        assert_eq!(run.rewrapped, 1);
        assert_eq!(
            store.kv_stored_form("p/1")?.expect("row"),
            ("one".to_string(), None)
        );
        drop(store);

        let disabled = Store::start_storage(&config_at(&dir, false))?;
        assert_eq!(disabled.keyring().snapshot().state, "disabled");
        assert_eq!(disabled.get_kv("p/1")?.expect("row").value, "one");
        let entry = put(&disabled, "p/2", "two")?;
        assert!(!entry.encrypted);
        assert_eq!(
            disabled.kv_stored_form("p/2")?.expect("row"),
            ("two".to_string(), None)
        );
        // Encrypted rows written earlier still decrypt while disabled.
        drop(disabled);
        let enabled = start(&dir)?;
        put(&enabled, "p/3", "three")?;
        drop(enabled);
        let disabled = Store::start_storage(&config_at(&dir, false))?;
        assert_eq!(disabled.get_kv("p/3")?.expect("row").value, "three");
        Ok(())
    }

    #[test]
    fn restart_unwraps_keys_from_the_store_with_the_file() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        put(&store, "r/1", "one")?;
        let rows = store.list_keyring_rows()?;
        drop(store);
        let reopened = start(&dir)?;
        assert_eq!(reopened.list_keyring_rows()?, rows, "no second key minted");
        assert_eq!(reopened.keyring().snapshot().state, "unlocked");
        assert_eq!(reopened.get_kv("r/1")?.expect("row").value, "one");
        assert_eq!(
            reopened.ensure_keyring_initialized()?,
            KeyringInit::AlreadyInitialized
        );
        Ok(())
    }

    #[test]
    fn restore_without_the_root_key_is_refused_with_the_path_named() -> R<()> {
        let source = store_dir();
        let store = start(&source)?;
        put(&store, "b/1", "one")?;
        let backup = source.join("backup.sqlite");
        store.state_backup(Some(&backup))?;
        drop(store);

        // A fresh host: the backup restored, no root key anywhere.
        let target = store_dir();
        Store::restore_from_backup(target.to_str().unwrap(), &backup, false)?;
        let restored = start(&target)?;
        let err = restored
            .verify_keyring_after_restore(backup.to_str().unwrap())
            .unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains(&root_key_path(&target).display().to_string()),
            "{message}"
        );
        assert!(message.contains("rk_"), "{message}");
        assert!(message.contains("legion keyring reset"), "{message}");
        // No root key was minted on the restored host: a lost key is not a
        // fresh cluster.
        assert!(!root_key_path(&target).exists());

        // Copy the key over: the same store now serves.
        std::fs::create_dir_all(target.join(ROOT_KEY_DIR))?;
        std::fs::copy(root_key_path(&source), root_key_path(&target))?;
        restored.load_keyring()?;
        restored.verify_keyring_after_restore(backup.to_str().unwrap())?;
        assert_eq!(restored.get_kv("b/1")?.expect("row").value, "one");
        Ok(())
    }

    #[test]
    fn env_provider_needs_the_variable_before_first_start() -> R<()> {
        let dir = store_dir();
        let var = format!("LEGION_TEST_KEYRING_{}", std::process::id());
        let mut config = config_at(&dir, true);
        config.encryption.root_key.provider = "env".into();
        config.encryption.root_key.var = var.clone();
        let store = Store::start_storage(&config)?;
        // No variable: nothing to wrap with, nothing minted, writes plaintext.
        assert_eq!(store.keyring().snapshot().state, "uninitialized");
        assert!(matches!(
            store.ensure_keyring_initialized()?,
            KeyringInit::NoRootKey(_)
        ));
        let entry = put(&store, "e/1", "plain")?;
        assert!(!entry.encrypted);

        std::env::set_var(&var, generate_root_key_line()?);
        assert!(matches!(
            store.ensure_keyring_initialized()?,
            KeyringInit::Initialized {
                minted_root_key: false,
                ..
            }
        ));
        let entry = put(&store, "e/2", "secret")?;
        assert!(entry.encrypted);
        std::env::remove_var(&var);
        Ok(())
    }
    /// The checkpoint key (docs/suspend-and-resume.md, "Published checkpoint
    /// encryption") is minted with the keyring, derives the same part key on
    /// every start, survives a root-key rewrap, and names its problems.
    #[test]
    fn the_checkpoint_key_is_minted_stable_and_named_when_missing() -> R<()> {
        let dir = store_dir();
        let store = start(&dir)?;
        let digest = crate::content_store::ContentDigest::of_bytes(b"guest memory");
        let current = store.keyring().checkpoint_part_key(None, &digest)?;
        assert!(current.key_id().starts_with("ck_"));
        let by_id = store
            .keyring()
            .checkpoint_part_key(Some(current.key_id()), &digest)?;
        assert_eq!(by_id.object_name(), current.object_name());
        let other = store.keyring().checkpoint_part_key(
            None,
            &crate::content_store::ContentDigest::of_bytes(b"other"),
        )?;
        assert_ne!(other.object_name(), current.object_name());
        // Never shown in Debug.
        assert!(!format!("{current:?}").contains(&format!("{:?}", current.to_proto().key)));

        // Idempotent: a second ensure mints nothing.
        assert_eq!(store.ensure_checkpoint_key()?, None);

        // A root-key rewrap and a restart keep the same part keys.
        let contents = std::fs::read_to_string(root_key_path(&dir))?;
        let new_line = generate_root_key_line()?;
        std::fs::write(root_key_path(&dir), format!("{new_line}\n{contents}"))?;
        store.rewrap_root_key()?;
        std::fs::write(root_key_path(&dir), format!("{new_line}\n"))?;
        drop(store);
        let reopened = start(&dir)?;
        let again = reopened.keyring().checkpoint_part_key(None, &digest)?;
        assert_eq!(again.key_id(), current.key_id());
        assert_eq!(again.object_name(), current.object_name());

        // Without the root key the checkpoint key is locked, by name.
        drop(reopened);
        std::fs::write(
            root_key_path(&dir),
            format!("{}\n", generate_root_key_line()?),
        )?;
        let locked = start(&dir)?;
        let err = locked
            .keyring()
            .checkpoint_part_key(Some(current.key_id()), &digest)
            .unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("keyring locked: checkpoint key ck_"),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn a_disabled_keyring_has_no_checkpoint_key_and_says_why() -> R<()> {
        let dir = store_dir();
        let store = Store::start_storage(&config_at(&dir, false))?;
        let err = store
            .keyring()
            .checkpoint_part_key(None, &crate::content_store::ContentDigest::of_bytes(b"x"))
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("encryption.enabled = false"),
            "{err:#}"
        );
        Ok(())
    }
}
