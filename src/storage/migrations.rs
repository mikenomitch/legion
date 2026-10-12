use std::sync::{Arc, Mutex};

use rusqlite_migration::{Migrations, M};

/// The schema version (migration count) at which the KV encryption keyring
/// landed (`keyring_key` + `kv_entry.enc_key_id`, docs/kv-encryption.md).
/// A raft leader will not initialize the keyring — and therefore never
/// writes an encrypted row or a keyring row — until every voter advertises
/// at least this version, because a manager below it cannot apply such a
/// changeset. Pinned by `keyring_schema_version_matches_the_migration`.
pub const KEYRING_SCHEMA_VERSION: u32 = 129;

/// The schema version this binary migrates a store to: the number of
/// migrations it knows. Advertised to raft peers as
/// `x-legion-schema-version` so version-gated features can tell whether
/// every voter can apply what they are about to write.
pub fn schema_version() -> u32 {
    static VERSION: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *VERSION.get_or_init(|| migration_list().len() as u32)
}

/// Primary-key flags per column of every table, as a store migrated to
/// `version` lays them out. The raft leader checks a captured changeset
/// against the schema of each peer that advertises an older version, so it
/// refuses a write that peer could not apply (R2, docs/upgrades.md). Built
/// once per version by migrating an in-memory database, then cached.
pub(crate) fn table_shapes_at(
    version: u32,
) -> anyhow::Result<Arc<std::collections::BTreeMap<String, Vec<bool>>>> {
    type Shapes = Arc<std::collections::BTreeMap<String, Vec<bool>>>;
    static CACHE: std::sync::OnceLock<Mutex<std::collections::HashMap<u32, Shapes>>> =
        std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(shapes) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&version)
    {
        return Ok(shapes.clone());
    }
    let mut conn = rusqlite::Connection::open_in_memory()?;
    // Version 0 (a peer that predates the version header) has no tables.
    if version > 0 {
        get_migrations().to_version(&mut conn, version.min(schema_version()) as usize)?;
    }
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table';")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let mut shapes = std::collections::BTreeMap::new();
    for table in tables {
        if let Some(flags) = crate::storage::raft::capture::table_pk_flags(&conn, &table)? {
            shapes.insert(table, flags);
        }
    }
    let shapes = Arc::new(shapes);
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(version, shapes.clone());
    Ok(shapes)
}

pub fn get_migrations() -> Migrations<'static> {
    Migrations::new(migration_list())
}

fn migration_list() -> Vec<M<'static>> {
    vec![
        M::up(
            "CREATE TABLE IF NOT EXISTS node (
            id STRING PRIMARY KEY,
            pool STRING NOT NULL,
            status STRING NOT NULL,
            availability STRING NOT NULL,
            config_attrs  JSONB NOT NULL,
            fingerprint_attrs  JSONB NOT NULL,
            dynamic_attrs JSONB NOT NULL,
            health_checks JSONB NOT NULL,
            last_heartbeat TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS node;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS worker_type (
            name STRING PRIMARY KEY,
            pool STRING NOT NULL,
            additional_node_checks Text NOT NULL,
            attributes JSONB NOT NULL,
            name_prefix STRING
        )",
        )
        .down("DROP TABLE IF EXISTS worker_type;"),
        M::up("ALTER TABLE worker_type ADD COLUMN resources JSONB NOT NULL DEFAULT '{}';")
            .down(""),
        M::up(
            "ALTER TABLE worker_type ADD COLUMN reserved_resources JSONB NOT NULL DEFAULT '{}';",
        )
        .down(""),
        M::up("ALTER TABLE worker_type ADD COLUMN max_allocations INTEGER;").down(""),
        M::up(
            "CREATE TABLE IF NOT EXISTS job (
            id STRING PRIMARY KEY,
            name STRING NOT NULL,
            status STRING NOT NULL,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS job;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job_config (
            id STRING PRIMARY KEY,
            name STRING NOT NULL,
            type STRING NOT NULL,
            meta JSONB NOT NULL,
            version INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            job_id STRING NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS job_config;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS submission (
            hash STRING PRIMARY KEY,
            content TEXT NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS submission;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job_config_submission (
        id INTEGER PRIMARY KEY,
        path STRING NOT NULL,
        is_primary INTEGER NOT NULL,
        job_config_id STRING NOT NULL,
        submission_hash STRING NOT NULL,
        FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
        FOREIGN KEY(submission_hash) REFERENCES submission(hash) ON DELETE CASCADE
    )",
        )
        .down("DROP TABLE IF EXISTS job_config_submission;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS task_config (
            id INTEGER PRIMARY KEY,
            count INTEGER NOT NULL,
            name STRING NOT NULL,
            kind STRING NOT NULL,
            config JSONB NOT NULL,
            resources JSONB NOT NULL,
            constraints JSONB NOT NULL,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS task_config;"),
        M::up("ALTER TABLE task_config ADD COLUMN affinities JSONB NOT NULL DEFAULT '{}';")
            .down(""),
        M::up("ALTER TABLE task_config ADD COLUMN permit JSONB NOT NULL DEFAULT '{}';")
            .down(""),
        M::up("ALTER TABLE task_config ADD COLUMN checks JSONB NOT NULL DEFAULT '[]';").down(""),
        M::up("ALTER TABLE task_config ADD COLUMN on_unhealthy JSONB NOT NULL DEFAULT '{}';")
            .down(""),
        M::up("ALTER TABLE task_config ADD COLUMN count_per STRING;").down(""),
        M::up(
            "ALTER TABLE task_config ADD COLUMN group_name STRING NOT NULL DEFAULT 'default';",
        )
        .down(""),
        M::up(
            "ALTER TABLE task_config ADD COLUMN lifecycle STRING NOT NULL DEFAULT 'service';",
        )
        .down(""),
        M::up("ALTER TABLE task_config ADD COLUMN schedule STRING;").down(""),
        M::up("ALTER TABLE task_config ADD COLUMN depends_on JSONB NOT NULL DEFAULT '[]';")
            .down(""),
        M::up("ALTER TABLE task_config ADD COLUMN kv_injections JSONB;").down(""),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_task_config_job_config_id ON task_config(job_config_id);",
        )
        .down("DROP INDEX IF EXISTS idx_task_config_job_config_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS task_status (
            task_config_id INTEGER PRIMARY KEY,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            status STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY(task_config_id) REFERENCES task_config(id) ON DELETE CASCADE,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS task_status;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_task_status_job_config_id ON task_status(job_config_id);",
        )
        .down("DROP INDEX IF EXISTS idx_task_status_job_config_id;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_task_status_task_config_id ON task_status(task_config_id);",
        )
        .down("DROP INDEX IF EXISTS idx_task_status_task_config_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job_update (
            id STRING PRIMARY KEY,
            status STRING NOT NULL,
            job_id STRING NOT NULL,
            to_job_config_id STRING NOT NULL,
            from_job_config_id STRING,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(to_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(from_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS job_update;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation (
            id INTEGER PRIMARY KEY,
            status STRING NOT NULL,
            resources JSONB NOT NULL,
            node_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            task_config_id INTEGER NOT NULL,
            FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(task_config_id) REFERENCES task_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS allocation;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_allocation_task_config_id ON allocation(task_config_id);")
            .down("DROP INDEX IF EXISTS idx_allocation_task_config_id;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_allocation_job_config_id ON allocation(job_config_id);")
            .down("DROP INDEX IF EXISTS idx_allocation_job_config_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS eval (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status STRING NOT NULL,
            type STRING NOT NULL,
            job_id STRING,
            job_config_id STRING,
            node_id STRING,
            priority INTEGER NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS eval;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_eval_status_priority_id ON eval(status, priority DESC, id ASC);",
        )
        .down("DROP INDEX IF EXISTS idx_eval_status_priority_id;"),
        M::up("ALTER TABLE eval ADD COLUMN ready_at TIMESTAMP NOT NULL DEFAULT '1970-01-01T00:00:00Z';")
            .down(""),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_eval_status_ready_priority_id ON eval(status, ready_at, priority DESC, id ASC);",
        )
        .down("DROP INDEX IF EXISTS idx_eval_status_ready_priority_id;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_eval_status_job_id ON eval(status, job_id);")
            .down("DROP INDEX IF EXISTS idx_eval_status_job_id;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_eval_status_node_id ON eval(status, node_id);")
            .down("DROP INDEX IF EXISTS idx_eval_status_node_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS manager_config (
            node_id STRING PRIMARY KEY,
            config JSONB NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS manager_config;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS kv_entry (
            key STRING PRIMARY KEY,
            value TEXT NOT NULL,
            value_type INTEGER NOT NULL,
            version INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS kv_entry;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS kv_lock (
            key STRING PRIMARY KEY,
            lock_id STRING NOT NULL,
            owner STRING,
            acquired_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS kv_lock;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job_kv_reference (
            id INTEGER PRIMARY KEY,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            kv_key STRING NOT NULL,
            json_pointer STRING NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS job_kv_reference;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_job_kv_reference_key ON job_kv_reference(kv_key);",
        )
        .down("DROP INDEX IF EXISTS idx_job_kv_reference_key;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_job_kv_reference_job ON job_kv_reference(job_id);",
        )
        .down("DROP INDEX IF EXISTS idx_job_kv_reference_job;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation_epoch (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            epoch INTEGER NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS allocation_epoch;"),
        M::up(
            "INSERT OR IGNORE INTO allocation_epoch(singleton, epoch) VALUES (1, 0);",
        )
        .down(""),
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation_epoch_partition (
            scope_type STRING NOT NULL,
            scope_id STRING NOT NULL,
            epoch INTEGER NOT NULL,
            PRIMARY KEY(scope_type, scope_id)
        )",
        )
        .down("DROP TABLE IF EXISTS allocation_epoch_partition;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_allocation_epoch_partition_scope ON allocation_epoch_partition(scope_type, scope_id);",
        )
        .down("DROP INDEX IF EXISTS idx_allocation_epoch_partition_scope;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS plan_submission (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_config_id STRING NOT NULL,
            mode STRING NOT NULL,
            placements JSONB NOT NULL,
            expected_epoch INTEGER NOT NULL,
            expected_version_vector JSONB NOT NULL DEFAULT '{}',
            status STRING NOT NULL,
            rejection_reason STRING,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS plan_submission;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_plan_submission_job_config_status ON plan_submission(job_config_id, status);",
        )
        .down("DROP INDEX IF EXISTS idx_plan_submission_job_config_status;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS rollout (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id STRING NOT NULL,
            from_job_config_id STRING,
            to_job_config_id STRING NOT NULL,
            status STRING NOT NULL,
            incomplete INTEGER NOT NULL DEFAULT 0,
            message STRING NOT NULL DEFAULT '',
            strategy STRING,
            progression STRING,
            replacement STRING,
            in_place INTEGER,
            config_json JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(from_job_config_id) REFERENCES job_config(id) ON DELETE SET NULL,
            FOREIGN KEY(to_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS rollout;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_rollout_job_id ON rollout(job_id);")
            .down("DROP INDEX IF EXISTS idx_rollout_job_id;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_rollout_status ON rollout(status);")
            .down("DROP INDEX IF EXISTS idx_rollout_status;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_rollout_job_status_created ON rollout(job_id, status, created_at DESC);",
        )
        .down("DROP INDEX IF EXISTS idx_rollout_job_status_created;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation_health (
            allocation_id INTEGER PRIMARY KEY,
            node_id STRING NOT NULL,
            healthy INTEGER NOT NULL,
            status STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY(allocation_id) REFERENCES allocation(id) ON DELETE CASCADE,
            FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS allocation_health;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_allocation_health_node_id ON allocation_health(node_id);",
        )
        .down("DROP INDEX IF EXISTS idx_allocation_health_node_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS rollout_event (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rollout_id INTEGER NOT NULL,
            job_id STRING NOT NULL,
            group_name STRING,
            level STRING NOT NULL,
            message STRING NOT NULL,
            metadata_json JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL,
            FOREIGN KEY(rollout_id) REFERENCES rollout(id) ON DELETE CASCADE,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS rollout_event;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_rollout_event_rollout_created ON rollout_event(rollout_id, created_at ASC, id ASC);",
        )
        .down("DROP INDEX IF EXISTS idx_rollout_event_rollout_created;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_rollout_event_job_created ON rollout_event(job_id, created_at DESC, id DESC);",
        )
        .down("DROP INDEX IF EXISTS idx_rollout_event_job_created;"),
        M::up("ALTER TABLE task_config ADD COLUMN network JSONB;").down(""),
        M::up("ALTER TABLE task_config ADD COLUMN service JSONB;").down(""),
        M::up("ALTER TABLE allocation ADD COLUMN network JSONB;").down(""),
        M::up("CREATE INDEX IF NOT EXISTS idx_allocation_node_id ON allocation(node_id);")
            .down("DROP INDEX IF EXISTS idx_allocation_node_id;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS saved_state_query (
            name STRING PRIMARY KEY,
            sql TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS saved_state_query;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS idempotency_record (
            key TEXT PRIMARY KEY,
            operation TEXT NOT NULL,
            response_message TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS idempotency_record;"),
        // Workload identities declared on a task (workload-identity OIDC plan).
        // JSON-encoded Vec<WorkloadIdentity>; NULL/absent == no identities.
        M::up("ALTER TABLE task_config ADD COLUMN identities JSONB;").down(""),
        // Stateful block declared on a task (stateful-workloads plan).
        // JSON-encoded StatefulConfig; NULL/absent == stateless task.
        M::up("ALTER TABLE task_config ADD COLUMN stateful JSONB;").down(""),
        // Stable instances for stateful workloads (stateful-workloads plan).
        // One row per logical instance; allocations reference instance_id and
        // churn underneath it. Terminated rows are retained: slot rows so a
        // scale-out re-materializes the same id/storage, keyed rows forever so
        // ids are never recycled.
        M::up(
            "CREATE TABLE IF NOT EXISTS instance (
            instance_id STRING PRIMARY KEY,
            job_id STRING NOT NULL,
            group_name STRING NOT NULL,
            task_name STRING NOT NULL,
            mode STRING NOT NULL,
            state STRING NOT NULL,
            slot_index INTEGER,
            current_alloc_id INTEGER,
            storage_handle STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS instance;"),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_instance_job_group ON instance(job_id, group_name);",
        )
        .down("DROP INDEX IF EXISTS idx_instance_job_group;"),
        // The stable instance an allocation currently fulfills (NULL for
        // stateless tasks). No FK: instance rows outlive allocations and the
        // column is assigned inside the allocation-insert transaction.
        M::up("ALTER TABLE allocation ADD COLUMN instance_id STRING;").down(""),
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_allocation_instance_id ON allocation(instance_id);",
        )
        .down("DROP INDEX IF EXISTS idx_allocation_instance_id;"),
        // OIDC signing keys for workload identity (workload-identity plan).
        // Persisting the keypair is what keeps externally-held tokens
        // verifiable across manager restarts: the JWKS must keep publishing
        // the same public keys, and the active private key must keep signing.
        // The private key PEM lives here in the manager's SQLite — the same
        // trust domain as the CA material; whoever can read the store can
        // mint identities. state ∈ {active, retiring, expired}; expired rows
        // are retained for audit and dropped from the JWKS.
        M::up(
            "CREATE TABLE IF NOT EXISTS signing_key (
            kid STRING PRIMARY KEY,
            alg STRING NOT NULL,
            private_key_pem TEXT NOT NULL,
            public_jwk_x STRING NOT NULL,
            public_jwk_y STRING NOT NULL,
            created_at INTEGER NOT NULL,
            not_after INTEGER,
            state STRING NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS signing_key;"),
        // When the node was marked lost (RFC3339); NULL while active/ready.
        // Set once by mark_node_lost/mark_nodes_as_lost (not refreshed while
        // the node stays lost) and cleared when the node heartbeats again or
        // re-registers. Drives the stateful node-loss confirmation gate: a
        // stateful allocation on a lost node is only freed for replacement
        // once `now - lost_at > stateful.onLost.replaceAfter` (see
        // docs/stateful-workloads.md "Node loss and fencing").
        M::up("ALTER TABLE node ADD COLUMN lost_at TIMESTAMP;").down(""),
        // Scheduler event journal backing ListEvents/StreamEvents.
        //
        // No foreign keys: events must outlive the jobs/nodes they describe
        // (e.g. a job.destroyed event survives the job row's deletion).
        //
        // Deliberately no secondary indexes and no AUTOINCREMENT: events are
        // inserted inside scheduler write transactions (the eval critical
        // path), where every extra b-tree update lengthens the serialized
        // critical section. The table is pruned to ~10k rows, so filtered
        // list queries scan it in ~1ms, and cursor queries use the rowid PK.
        // Ids are assigned explicitly by `Store::next_scheduler_event_id`
        // (unix-ms high bits + per-process sequence + the minting raft
        // lane's stride residue in the low bits) so they are globally
        // unique across raft lanes/nodes (the JC-28 commutative-class
        // requirement, made structural by JC-42) while still sorting
        // ~chronologically for the id-cursor list/stream/prune paths.
        M::up(
            "CREATE TABLE IF NOT EXISTS scheduler_event (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            level TEXT NOT NULL,
            job_id TEXT,
            node_id TEXT,
            eval_id TEXT,
            allocation_id INTEGER,
            rollout_id INTEGER,
            message TEXT NOT NULL,
            metadata_json JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS scheduler_event;"),
        // Artifacts declared on a task (artifact system plan). JSON-encoded
        // Vec<ArtifactSpec> in normalized (post-admission) form; NULL/absent
        // == no artifacts. Ordered after main's scheduler_event migration so
        // databases migrated on main upgrade cleanly.
        M::up("ALTER TABLE task_config ADD COLUMN artifacts JSONB;").down(""),
        // Worker-type artifact fetch settings (JSON WorkerArtifactsConfig;
        // empty string == worker built-in defaults).
        M::up("ALTER TABLE worker_type ADD COLUMN artifacts TEXT NOT NULL DEFAULT '';")
            .down(""),
        // Per-artifact staging states from the latest worker heartbeat
        // (JSON Vec<ArtifactStatus>); NULL == task declares no artifacts.
        M::up("ALTER TABLE allocation_health ADD COLUMN artifact_states TEXT;").down(""),
        // ACL policies: named rule documents (see internal_docs/plans/auth-acl-plan.md).
        M::up(
            "CREATE TABLE IF NOT EXISTS acl_policy (
            name STRING PRIMARY KEY,
            description STRING NOT NULL DEFAULT '',
            rules JSONB NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS acl_policy;"),
        // ACL tokens. Only a SHA-256 hash of the secret is stored; the
        // accessor id is the public, log-safe handle.
        M::up(
            "CREATE TABLE IF NOT EXISTS acl_token (
            accessor_id STRING PRIMARY KEY,
            secret_hash STRING NOT NULL UNIQUE,
            name STRING NOT NULL,
            token_type STRING NOT NULL,
            policies JSONB NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS acl_token;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_acl_token_secret_hash ON acl_token(secret_hash);")
            .down("DROP INDEX IF EXISTS idx_acl_token_secret_hash;"),
        // Username/password users for the built-in userpass auth method.
        // password_hash is an argon2id PHC string.
        M::up(
            "CREATE TABLE IF NOT EXISTS acl_user (
            username STRING PRIMARY KEY,
            password_hash STRING NOT NULL,
            policies JSONB NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS acl_user;"),
        // Worker join tokens. Only a SHA-256 hash of the secret is stored;
        // the id is the public handle for listing/revocation.
        M::up(
            "CREATE TABLE IF NOT EXISTS join_token (
            id STRING PRIMARY KEY,
            secret_hash STRING NOT NULL UNIQUE,
            worker_type STRING NOT NULL,
            description STRING NOT NULL DEFAULT '',
            expires_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS join_token;"),
        // Declared actions on a task (exec & actions plan). JSON-encoded
        // Vec<ActionSpec>; NULL/absent == no actions. Appended last so
        // databases created before this feature migrate cleanly.
        M::up("ALTER TABLE task_config ADD COLUMN actions JSONB;").down(""),
        // Volume claims declared on a task (volume driver system). JSON-encoded
        // Vec<VolumeSpec>; NULL/absent == no claims. The per-instance volume
        // from `stateful.storage` is NOT here — it lives in the stateful
        // column and materializes through the instance's storage handle.
        M::up("ALTER TABLE task_config ADD COLUMN volumes JSONB;").down(""),
        // Exit-driven restart policy for a task (host process supervision
        // plan). JSON-encoded TaskRestartConfig; NULL/absent == the lifecycle
        // default (`always` for service/sidecar, `onFailure` for batch).
        M::up("ALTER TABLE task_config ADD COLUMN restart JSONB;").down(""),
        // Volumes: the manager's record of storage a plugin has (or will)
        // materialize. See docs/volumes.md.
        //
        // - `volume_id` is the second half of an instance's
        //   `{plugin}/{volume_id}` storage handle, so a handle always has a row.
        // - `node_id` is the pin: set the first time a node-local volume
        //   mounts, and the constraint every later placement of its instance
        //   must satisfy.
        // - `state` is the two-phase destroy machine: pending -> created ->
        //   destroying -> destroyed. A crash mid-destroy retries; nothing leaks.
        // - No FK to `instance`: a volume deliberately OUTLIVES its instance
        //   row through the destroying/destroyed states, which is what lets the
        //   reaper finish reclaiming storage after a job is destroyed.
        M::up(
            "CREATE TABLE IF NOT EXISTS volume (
            volume_id STRING PRIMARY KEY,
            name STRING NOT NULL DEFAULT 'data',
            driver STRING NOT NULL,
            job_id STRING,
            instance_id STRING,
            task_name STRING,
            state STRING NOT NULL,
            node_id STRING,
            node_local INTEGER NOT NULL DEFAULT 1,
            read_only INTEGER NOT NULL DEFAULT 0,
            params JSONB NOT NULL DEFAULT '{}',
            attrs JSONB NOT NULL DEFAULT '{}',
            mount_path STRING,
            last_error STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS volume;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_volume_instance ON volume(instance_id);")
            .down("DROP INDEX IF EXISTS idx_volume_instance;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_volume_node ON volume(node_id);")
            .down("DROP INDEX IF EXISTS idx_volume_node;"),
        M::up("CREATE INDEX IF NOT EXISTS idx_volume_state ON volume(state);")
            .down("DROP INDEX IF EXISTS idx_volume_state;"),
        // Host paths a worker type offers through the `host` volume driver
        // (JSON map of name -> {path, readOnly, create}); empty == none.
        M::up("ALTER TABLE worker_type ADD COLUMN host_volumes TEXT NOT NULL DEFAULT '';")
            .down(""),
        // Per-worker resource enforcement (host process supervision plan).
        // JSON-encoded WorkerResourceEnforcementConfig; empty == off, which is
        // what every existing worker type gets.
        M::up(
            "ALTER TABLE worker_type ADD COLUMN resource_enforcement TEXT NOT NULL DEFAULT '';",
        )
        .down(""),
        // How a volume is handed to its consumer: `filesystem` (a directory) or
        // `block` (a device/disk image). Recorded rather than re-derived so a
        // future restore/clone knows the shape without re-probing the plugin.
        M::up(
            "ALTER TABLE volume ADD COLUMN volume_mode STRING NOT NULL DEFAULT 'filesystem';",
        )
        .down(""),
        // Block mode: the filesystem the device carries, so a consumer knows
        // what to mount without probing.
        M::up("ALTER TABLE volume ADD COLUMN fs STRING;").down(""),
        // The task-execution axis was renamed `kind` -> `runtime` (see
        // internal_docs/plans/task-runtime-rename-plan.md). Column rename only;
        // values are unchanged, so existing clusters migrate on manager start
        // with no operator action.
        M::up("ALTER TABLE task_config RENAME COLUMN kind TO runtime;")
            .down("ALTER TABLE task_config RENAME COLUMN runtime TO kind;"),
        // Raft applied-state marker (durability plan §A5, phase A-1): one row
        // per lane recording the last applied log id and the last applied
        // membership (both serde_json blobs of the openraft types). Written
        // by the raft state machine INSIDE the same transaction as the
        // changeset it describes, so applied index and data commit atomically
        // and can never diverge — this is what makes crash recovery resume
        // from exactly the right log position. Absent row == blank slate
        // (applied_state None/default). Never touched by capture-mode
        // mutations, so it never rides a changeset itself.
        M::up(
            "CREATE TABLE IF NOT EXISTS raft_applied (
            lane TEXT PRIMARY KEY,
            last_applied BLOB,
            membership BLOB
        )",
        )
        .down("DROP TABLE IF EXISTS raft_applied;"),
        // Node-local raft cluster identity stamp: written once at first raft
        // engine start (key `cluster_id`), checked on every later start so a
        // manager whose raft data_dir (cluster secret included) was wiped
        // cannot silently bootstrap a NEW cluster over an EXISTING cluster's
        // state DB and split-brain against its surviving members. Every
        // member independently writes the same derived id, so replicas stay
        // content-identical; never touched by capture-mode mutations.
        M::up(
            "CREATE TABLE IF NOT EXISTS raft_cluster (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS raft_cluster;"),
        // Rebuild `eval` WITHOUT its foreign keys to `job` / `job_config` /
        // `node` (plan §7-C2). Originally this was because JC-27 made the
        // queue node-local; JC-41 put it back in raft on the SCHEDULER lane
        // and the FKs still stay gone, for the JC-32 reason: those three
        // parents are CONTROL-lane tables, so a `DELETE FROM job` would
        // cascade scheduler-lane eval rows into a control changeset — a
        // single-writer ownership violation the capture-time guard rejects,
        // which would make `job destroy` fail outright.
        //
        // Evals therefore reference job/job_config/node ADVISORILY. Stale
        // references are absorbed without a retry loop: `prune_stale_evals`
        // drops rows whose config is superseded or gone at leadership
        // acquisition, `claim_eval` sweeps superseded same-scope siblings,
        // and anything left simply fails in its handler — and a failed eval
        // is terminal (status `failed`, never re-claimed), so a dangling
        // reference cannot spin. (insert_eval historically toggled
        // foreign_keys OFF around its insert precisely because these FKs
        // were already too strict — e.g. node evals for not-yet-registered
        // nodes.)
        M::up(
            "CREATE TABLE eval_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status STRING NOT NULL,
            type STRING NOT NULL,
            job_id STRING,
            job_config_id STRING,
            node_id STRING,
            priority INTEGER NOT NULL,
            ready_at TIMESTAMP NOT NULL DEFAULT '1970-01-01T00:00:00Z'
        );
        INSERT INTO eval_local (id, status, type, job_id, job_config_id, node_id, priority, ready_at)
            SELECT id, status, type, job_id, job_config_id, node_id, priority, ready_at FROM eval;
        DROP TABLE eval;
        ALTER TABLE eval_local RENAME TO eval;
        CREATE INDEX IF NOT EXISTS idx_eval_status_priority_id ON eval(status, priority DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_eval_status_ready_priority_id ON eval(status, ready_at, priority DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_eval_status_job_id ON eval(status, job_id);
        CREATE INDEX IF NOT EXISTS idx_eval_status_node_id ON eval(status, node_id);",
        )
        .down(""),
        // The node/node_health split (plan §7-C2, phase C-1): liveness —
        // worker-reported `status`, `last_heartbeat`, `lost_at`,
        // `health_checks` — moves to `node_health`, single-written by the
        // STATUS raft lane, while `node` keeps identity/pool/attrs plus
        // `availability` (the operator/control axis), single-written by the
        // CONTROL lane. This is what makes per-lane snapshots sound: every
        // table now has exactly one writing lane.
        //
        // Deliberately NO foreign key from node_health to node: a control-
        // lane `DELETE FROM node` must never cascade status-lane rows into
        // its changeset. A node_health row orphaned by node removal is
        // harmless (all hydration LEFT JOINs from `node`) and is swept by
        // the status lane (`prune_node_health_orphans`).
        //
        // Existing liveness values are copied over, then the moved columns
        // are dropped (bundled SQLite is 3.46; ALTER ... DROP COLUMN works
        // and none of the dropped columns is indexed or referenced).
        M::up(
            "CREATE TABLE IF NOT EXISTS node_health (
            node_id STRING PRIMARY KEY,
            status STRING NOT NULL,
            last_heartbeat TIMESTAMP NOT NULL,
            lost_at TIMESTAMP,
            health_checks JSONB NOT NULL
        );
        INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
            SELECT id, status, last_heartbeat, lost_at, health_checks FROM node;
        ALTER TABLE node DROP COLUMN status;
        ALTER TABLE node DROP COLUMN last_heartbeat;
        ALTER TABLE node DROP COLUMN lost_at;
        ALTER TABLE node DROP COLUMN health_checks;",
        )
        .down(
            "ALTER TABLE node ADD COLUMN status STRING NOT NULL DEFAULT 'active';
        ALTER TABLE node ADD COLUMN last_heartbeat TIMESTAMP NOT NULL DEFAULT '';
        ALTER TABLE node ADD COLUMN lost_at TIMESTAMP;
        ALTER TABLE node ADD COLUMN health_checks JSONB NOT NULL DEFAULT '{}';
        UPDATE node SET
            status = COALESCE((SELECT h.status FROM node_health h WHERE h.node_id = node.id), 'active'),
            last_heartbeat = COALESCE((SELECT h.last_heartbeat FROM node_health h WHERE h.node_id = node.id), ''),
            lost_at = (SELECT h.lost_at FROM node_health h WHERE h.node_id = node.id),
            health_checks = COALESCE((SELECT h.health_checks FROM node_health h WHERE h.node_id = node.id), '{}');
        DROP TABLE IF EXISTS node_health;",
        ),
        // Cross-lane FK removal (plan §7-C2, phase C-2): with single-writer
        // lane ownership, a replicated op's changeset must touch only its
        // own lane's tables — but `ON DELETE CASCADE` from a control-lane
        // parent (job/job_config/task_config/node) into scheduler/status
        // children (allocation/instance/rollout/task_status/
        // allocation_health/rollout_event) captured foreign-lane rows into
        // control changesets (and allocation deletes cascaded status-lane
        // allocation_health rows into scheduler changesets). Same fix shape
        // as the C-0 eval rebuild and the C-1 node_health table: rebuild the
        // six tables WITHOUT cross-lane foreign keys. Deletion becomes
        // explicit, performed by each table's owning lane (destroy_job's
        // status/scheduler cleanup ops, prune_rollouts' rollout_event
        // delete, remove_node's remove_allocs_for_node pass) plus orphan
        // sweeps (`prune_allocation_health_orphans`, mirroring
        // `prune_node_health_orphans`). Referential integrity that inserts
        // relied on is preserved by reads: every insert path resolves its
        // parent row first (task_config/job_config lookups, EXISTS guards).
        //
        // Rebuild order matters under `foreign_keys = ON`: children whose
        // FKs point at other rebuilt tables (allocation_health -> allocation,
        // rollout_event -> rollout) go FIRST, so the later `DROP TABLE` of
        // their old parents finds no FK-bearing children (SQLite's implicit
        // DELETE-on-DROP would otherwise cascade into — and wipe — them).
        //
        // Empty down, like the eval rebuild: the original creations'
        // `DROP TABLE IF EXISTS` downs remove the rebuilt tables on the way
        // to version 0.
        M::up(
            "CREATE TABLE allocation_health_ownerlane (
            allocation_id INTEGER PRIMARY KEY,
            node_id STRING NOT NULL,
            healthy INTEGER NOT NULL,
            status STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            artifact_states TEXT
        );
        INSERT INTO allocation_health_ownerlane (allocation_id, node_id, healthy, status, updated_at, artifact_states)
            SELECT allocation_id, node_id, healthy, status, updated_at, artifact_states FROM allocation_health;
        DROP TABLE allocation_health;
        ALTER TABLE allocation_health_ownerlane RENAME TO allocation_health;
        CREATE INDEX IF NOT EXISTS idx_allocation_health_node_id ON allocation_health(node_id);

        CREATE TABLE rollout_event_ownerlane (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rollout_id INTEGER NOT NULL,
            job_id STRING NOT NULL,
            group_name STRING,
            level STRING NOT NULL,
            message STRING NOT NULL,
            metadata_json JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL
        );
        INSERT INTO rollout_event_ownerlane (id, rollout_id, job_id, group_name, level, message, metadata_json, created_at)
            SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at FROM rollout_event;
        DROP TABLE rollout_event;
        ALTER TABLE rollout_event_ownerlane RENAME TO rollout_event;
        CREATE INDEX IF NOT EXISTS idx_rollout_event_rollout_created ON rollout_event(rollout_id, created_at ASC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_rollout_event_job_created ON rollout_event(job_id, created_at DESC, id DESC);

        CREATE TABLE task_status_ownerlane (
            task_config_id INTEGER PRIMARY KEY,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            status STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        INSERT INTO task_status_ownerlane (task_config_id, job_id, job_config_id, status, updated_at)
            SELECT task_config_id, job_id, job_config_id, status, updated_at FROM task_status;
        DROP TABLE task_status;
        ALTER TABLE task_status_ownerlane RENAME TO task_status;
        CREATE INDEX IF NOT EXISTS idx_task_status_job_config_id ON task_status(job_config_id);
        CREATE INDEX IF NOT EXISTS idx_task_status_task_config_id ON task_status(task_config_id);

        CREATE TABLE instance_ownerlane (
            instance_id STRING PRIMARY KEY,
            job_id STRING NOT NULL,
            group_name STRING NOT NULL,
            task_name STRING NOT NULL,
            mode STRING NOT NULL,
            state STRING NOT NULL,
            slot_index INTEGER,
            current_alloc_id INTEGER,
            storage_handle STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        INSERT INTO instance_ownerlane (instance_id, job_id, group_name, task_name, mode, state, slot_index, current_alloc_id, storage_handle, created_at, updated_at)
            SELECT instance_id, job_id, group_name, task_name, mode, state, slot_index, current_alloc_id, storage_handle, created_at, updated_at FROM instance;
        DROP TABLE instance;
        ALTER TABLE instance_ownerlane RENAME TO instance;
        CREATE INDEX IF NOT EXISTS idx_instance_job_group ON instance(job_id, group_name);

        CREATE TABLE rollout_ownerlane (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id STRING NOT NULL,
            from_job_config_id STRING,
            to_job_config_id STRING NOT NULL,
            status STRING NOT NULL,
            incomplete INTEGER NOT NULL DEFAULT 0,
            message STRING NOT NULL DEFAULT '',
            strategy STRING,
            progression STRING,
            replacement STRING,
            in_place INTEGER,
            config_json JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP
        );
        INSERT INTO rollout_ownerlane (id, job_id, from_job_config_id, to_job_config_id, status, incomplete, message, strategy, progression, replacement, in_place, config_json, created_at, updated_at, completed_at)
            SELECT id, job_id, from_job_config_id, to_job_config_id, status, incomplete, message, strategy, progression, replacement, in_place, config_json, created_at, updated_at, completed_at FROM rollout;
        DROP TABLE rollout;
        ALTER TABLE rollout_ownerlane RENAME TO rollout;
        CREATE INDEX IF NOT EXISTS idx_rollout_job_id ON rollout(job_id);
        CREATE INDEX IF NOT EXISTS idx_rollout_status ON rollout(status);
        CREATE INDEX IF NOT EXISTS idx_rollout_job_status_created ON rollout(job_id, status, created_at DESC);

        CREATE TABLE allocation_ownerlane (
            id INTEGER PRIMARY KEY,
            status STRING NOT NULL,
            resources JSONB NOT NULL,
            node_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            task_config_id INTEGER NOT NULL,
            network JSONB,
            instance_id STRING
        );
        INSERT INTO allocation_ownerlane (id, status, resources, node_id, job_config_id, task_config_id, network, instance_id)
            SELECT id, status, resources, node_id, job_config_id, task_config_id, network, instance_id FROM allocation;
        DROP TABLE allocation;
        ALTER TABLE allocation_ownerlane RENAME TO allocation;
        CREATE INDEX IF NOT EXISTS idx_allocation_task_config_id ON allocation(task_config_id);
        CREATE INDEX IF NOT EXISTS idx_allocation_job_config_id ON allocation(job_config_id);
        CREATE INDEX IF NOT EXISTS idx_allocation_node_id ON allocation(node_id);
        CREATE INDEX IF NOT EXISTS idx_allocation_instance_id ON allocation(instance_id);",
        )
        .down(""),
        // Replicated restart intent (JC-27 failover regeneration): a
        // KV-triggered restart of an UNCHANGED job config leaves no trace in
        // replicated state (the restart itself is a leader-local job_update
        // eval), so a failover silently dropped it. The marker is stamped by
        // the control-lane `request_job_restart` op when the restart is
        // acknowledged and cleared by the job_update eval before it
        // executes; `regenerate_scheduling_evals` re-enqueues job_update for
        // any job whose marker is still set.
        M::up("ALTER TABLE job ADD COLUMN restart_requested_at TIMESTAMP;").down(""),
        // Replicated drain-eviction intent (JC-27 failover regeneration):
        // `node drain --evict` was only distinguishable from a plain cordon
        // by the leader-local node_drain eval, so failover regeneration
        // evicted EVERY drained node. Set by `set_node_availability` when
        // the operator asked for eviction; cleared whenever availability
        // leaves 'drained'.
        M::up("ALTER TABLE node ADD COLUMN drain_evict INTEGER NOT NULL DEFAULT 0;").down(""),
        // Job templates (job-templates plan): name + latest-version pointer,
        // with immutable versions in job_template_version. variables is the
        // JSON-encoded Vec<JobTemplateVariableRecord> parsed from the
        // manifest at upsert time so list/get never re-evaluate Nickel.
        M::up(
            "CREATE TABLE IF NOT EXISTS job_template (
            name STRING PRIMARY KEY,
            latest_version INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS job_template;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job_template_version (
            template_name STRING NOT NULL,
            version INTEGER NOT NULL,
            description STRING,
            source_nickel TEXT NOT NULL,
            variables JSONB NOT NULL DEFAULT '[]',
            created_by STRING NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY(template_name, version),
            FOREIGN KEY(template_name) REFERENCES job_template(name) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE IF EXISTS job_template_version;"),
        // Upstream services a task connects to (service-discovery connect
        // layer). JSON-encoded ConnectConfig; NULL/absent == the task declares
        // no upstreams and gets no forwarders. Appended last so databases
        // created before this feature migrate cleanly.
        M::up("ALTER TABLE task_config ADD COLUMN connect JSONB;").down(""),
        // Service-name lookup for the discovery catalog. `Store::service_endpoints`
        // filters on `COALESCE(json_extract(service,'$.name'), name)` — exactly
        // `ServiceConfig::service_name` — and the connect upstream feed runs
        // that lookup on every node's heartbeat (every 2s), so it has to be a
        // seek rather than a cluster-wide scan. Partial on `json_valid` so a
        // hand-corrupted `service` value can never fail this migration, nor
        // block later writes to the table.
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_task_config_service_name
             ON task_config(COALESCE(json_extract(service, '$.name'), name))
             WHERE service IS NOT NULL AND json_valid(service);",
        )
        .down("DROP INDEX IF EXISTS idx_task_config_service_name;"),
        // Integer heartbeat clock for the liveness scan (heartbeat-scale
        // plan §4): the manager's 2s liveness tick compares every node's
        // last heartbeat against a cutoff, and parsing the RFC3339 text
        // column per node per tick is 10k parses every 2s at 10k workers.
        // Heartbeats now write both columns; the scan compares integers and
        // falls back to parsing the text only for rows written before this
        // migration (NULL unix), which age out on their next heartbeat. The
        // text column stays — it is the human/API-facing representation.
        M::up("ALTER TABLE node_health ADD COLUMN last_heartbeat_unix INTEGER;").down(""),
        // The per-heartbeat volume-destroy feed (`volumes_to_destroy_for_node`)
        // filters on state = 'destroying' but its node disjunction defeats
        // any full-column index; a partial index keeps the per-heartbeat
        // query O(destroying rows) — almost always zero — instead of a
        // volume-table scan per heartbeat (heartbeat-scale plan §3).
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_volume_destroying
             ON volume(state) WHERE state = 'destroying';",
        )
        .down("DROP INDEX IF EXISTS idx_volume_destroying;"),
        // Worker-type heartbeat cadence (heartbeat-scale plan §5): NULL =
        // the built-in default (2s). Served to workers at pre-registration.
        M::up("ALTER TABLE worker_type ADD COLUMN heartbeat_seconds INTEGER;").down(""),
        // Job mtime (job-updated-at plan §2). The job row only ever carried
        // `created_at`, so every surface that wanted "when did this job last
        // change" — the UI's Updated column most visibly — showed the
        // submission time instead, and a job stopped or resubmitted months
        // later still read as untouched. Unix seconds, matching
        // `job.created_at`; NULL only until the backfill below runs.
        M::up("ALTER TABLE job ADD COLUMN updated_at INTEGER;").down(""),
        // Backfill: pre-migration rows have no mtime to recover, and their
        // creation time is the only honest lower bound. Guarded on
        // `typeof(created_at)` so a row whose created_at was written as text
        // (only the pre-rename migration fixtures do this) leaves updated_at
        // NULL rather than poisoning an INTEGER column with a string —
        // `job_from_row` falls back to created_at for those.
        M::up(
            "UPDATE job SET updated_at = created_at
             WHERE updated_at IS NULL AND typeof(created_at) = 'integer';",
        )
        .down(""),
        // Stop causes (preemption-and-eviction plan § Phase 1a). Written in
        // the same transaction that removes an allocation, so a notice can
        // never claim a stop that did not happen, and read by the heartbeat
        // that tells the owning node its allocation is gone.
        //
        // Ordered after the job-mtime pair above deliberately: those shipped
        // to `main` first, so any store that has run that code already counts
        // them as applied. `rusqlite_migration` tracks the applied *count*,
        // so inserting these ahead of them would make such a store skip these
        // two and re-run the job-mtime pair.
        //
        // No foreign key to `allocation`: the row this describes is deleted
        // by the very transaction that inserts the notice. Rows are pruned by
        // age (`STOP_NOTICE_RETENTION_SECS`) rather than by acknowledgement —
        // the data is advisory, so bounded staleness beats a delivery
        // protocol.
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation_stop_notice (
            allocation_id INTEGER PRIMARY KEY,
            node_id STRING NOT NULL,
            cause STRING NOT NULL,
            created_at TIMESTAMP NOT NULL,
            created_at_unix INTEGER NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS allocation_stop_notice;"),
        // The per-heartbeat notice feed selects by node; without this every
        // heartbeat scans the table.
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_allocation_stop_notice_node
             ON allocation_stop_notice(node_id);",
        )
        .down("DROP INDEX IF EXISTS idx_allocation_stop_notice_node;"),
        // Count overrides (`legion job scale`, autoscaler plan §6.1): a
        // task's effective desired count, layered over the spec without a
        // new job config version. Keyed by the version-stable task tuple
        // (the same handle slot retirement and keyed instances use), NOT
        // the version-scoped task_config id, so overrides survive
        // resubmits. `base_count` records the spec count the override was
        // laid over — the resubmit-ownership arbiter (docs/autoscaling.md).
        // `pending_count` holds a scale-in recorded during an active
        // rollout, promoted to `count` when the rollout finishes; `count`
        // is NULL while only a pending scale-in exists.
        //
        // Kept LAST for the same reason the stop-notice pair above gives:
        // `rusqlite_migration` tracks the applied count, so a migration that
        // has already shipped to `main` must keep its position. Anything
        // added on a branch goes after everything already on `main`.
        M::up(
            "CREATE TABLE IF NOT EXISTS task_scale (
            job_id STRING NOT NULL,
            task_group STRING NOT NULL,
            task_name STRING NOT NULL,
            count INTEGER,
            pending_count INTEGER,
            base_count INTEGER NOT NULL,
            source STRING NOT NULL,
            reason STRING NOT NULL DEFAULT '',
            principal STRING NOT NULL DEFAULT '',
            updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (job_id, task_group, task_name)
        )",
        )
        .down("DROP TABLE IF EXISTS task_scale;"),
        // ---------------------------------------------------------------
        // Preemption and eviction, phases 1b-3. Everything below is
        // additive and appended in the order it must apply. Per the note on
        // `task_scale` above: `rusqlite_migration` tracks the applied COUNT,
        // so new migrations always go at the end.
        // ---------------------------------------------------------------
        //
        // Graceful stop (phase 1b): the ladder a task is stopped with, and
        // the floor voluntary disruption may not cross. JSON columns rather
        // than scalar ones because both blocks are small structs that will
        // grow fields (a future `signal` per cause, a `maxUnavailable`)
        // without wanting a migration each time — the same shape `restart`
        // and `on_unhealthy` already use.
        M::up("ALTER TABLE task_config ADD COLUMN stop JSONB;").down(""),
        M::up("ALTER TABLE task_config ADD COLUMN disruption JSONB;").down(""),
        // Who displaced a preempted allocation, carried on the stop notice so
        // the heartbeat can hand the task a preemptor link in its on-disk
        // notice. NULL for every other cause.
        M::up("ALTER TABLE allocation_stop_notice ADD COLUMN preemptor_job STRING;").down(""),
        M::up("ALTER TABLE allocation_stop_notice ADD COLUMN preemptor_band STRING;").down(""),
        // Priority bands (phase 2). A column rather than a `meta` key: the
        // preemption pass reads bands for every allocation on a candidate
        // node, which is a join, not a JSON walk. Existing configs migrate to
        // `normal`, which is exactly what absent means.
        M::up("ALTER TABLE job_config ADD COLUMN priority STRING NOT NULL DEFAULT 'normal';")
            .down(""),
        // Allocation age (phase 3), for `preemption.victimExemptAge` — the
        // cheapest thrash guard there is: an allocation that just started is
        // not a victim, so a preemption cannot immediately undo the placement
        // a previous one made. NULL on pre-migration rows and read as
        // "older than any exempt window", which is true by construction.
        M::up("ALTER TABLE allocation ADD COLUMN created_at_unix INTEGER;").down(""),
        // Evictions ride the plan (phase 3). The plan format could only ever
        // ADD allocations; this column is what lets one express a removal, so
        // victims and the placements they pay for commit in a single
        // transaction under a single epoch compare-and-set.
        M::up("ALTER TABLE plan_submission ADD COLUMN evictions JSONB NOT NULL DEFAULT '[]';")
            .down(""),
        // ---------------------------------------------------------------
        // Edge boot (docs/edge-boot.md). Appended last, per the note on
        // `task_scale` above.
        // ---------------------------------------------------------------
        //
        // How far a node may get on a start for this task before the
        // placement is durable. A JSON column for the same reason `stop` and
        // `disruption` are: it is a small block that will grow fields
        // (a per-gate timeout, a per-runtime override) without wanting a
        // migration each time. NULL — which every existing row gets — reads
        // as `mode: 'off'`, so an upgraded cluster behaves byte for byte as
        // it did.
        M::up("ALTER TABLE task_config ADD COLUMN speculative_start JSONB;").down(""),
        // Per-worker-type edge boot settings, as JSON like `artifacts` and
        // `resource_enforcement`. Empty on every existing row, which reads as
        // "this node accepts no edge starts".
        M::up("ALTER TABLE worker_type ADD COLUMN edge_boot TEXT NOT NULL DEFAULT '';").down(""),
        // Boot tickets: the capabilities the cluster has handed out. The
        // token bytes are NEVER stored — only the metadata an operator needs
        // to audit a start and the manager needs to enforce single use.
        // Rows are pruned once they expire, because an expired ticket can
        // never be redeemed and its row has no further job.
        M::up(
            "CREATE TABLE IF NOT EXISTS boot_ticket (
            jti STRING PRIMARY KEY,
            job STRING NOT NULL,
            task STRING NOT NULL,
            job_config_id STRING NOT NULL,
            instance_id STRING NOT NULL DEFAULT '',
            instance_prefix STRING NOT NULL DEFAULT '',
            pool STRING NOT NULL,
            gate STRING NOT NULL,
            uses INTEGER NOT NULL DEFAULT 1,
            uses_spent INTEGER NOT NULL DEFAULT 0,
            expires_at_unix INTEGER NOT NULL,
            accessor_id STRING NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS boot_ticket;"),
        M::up("CREATE INDEX IF NOT EXISTS boot_ticket_job_idx ON boot_ticket (job);")
            .down("DROP INDEX IF EXISTS boot_ticket_job_idx;"),
        // A ratification names the ONE keyed instance it is racing for, so
        // the apply binds that sandbox or refuses. Empty on every ordinary
        // plan, which keeps the "any unbound instance" binding unchanged.
        M::up("ALTER TABLE plan_submission ADD COLUMN target_instance STRING NOT NULL DEFAULT '';")
            .down(""),
        // Per-node lost window (heartbeat-cadence plan §6.4). The lost
        // threshold used to be one global constant compared against every
        // node, which made `heartbeatSeconds` a knob that relieved manager
        // load and destroyed detection margin with the same turn: at 13s
        // cadence against a fixed 10s window, one missed beat was a dead node.
        //
        // The window is now `missedBeatsBeforeLost x that node's own
        // interval`, recorded here. NULL falls back to DEF_LOST_NODE_AT_SEC,
        // so a row written before this column behaves exactly as it did and
        // ages out on its next registration. Additive with no backfill: an
        // older manager binary ignores the column entirely, so a manager
        // rollback is safe.
        M::up("ALTER TABLE node_health ADD COLUMN lost_after_seconds INTEGER;").down(""),
        // Per-node cached allocation usage for the plan-apply capacity
        // recheck (`Store::free_pool_for_node_tx`).
        //
        // Rebuilding a node's free pool used to read EVERY allocation row on
        // that node and parse every `resources` blob, inside the serialized
        // plan-apply commit. That made the cost of committing a plan scale
        // with how full the cluster is while the work per plan stayed
        // constant: at 5,000 nodes an eval touching ~44 nodes read ~440 rows
        // with 50k allocations live and ~1,760 with 200k, for the same 44
        // placements. Measured throughput fell 736 -> 457 -> 273 alloc/s
        // across those three fills — roughly 1.6x per doubling.
        //
        // This table caches the aggregate ("everything on the node adds up
        // to this much") next to a FINGERPRINT of the rows it was derived
        // from: the node's allocation epoch, the row count, and the sum of
        // the allocation ids. The apply path recomputes that fingerprint
        // (index-only — no row bodies, no JSON) and only trusts the cached
        // aggregate when it still matches.
        //
        // It is therefore a CACHE, not a maintained invariant: nothing has
        // to remember to update it. Any writer that adds or removes an
        // allocation by any path invalidates it implicitly, and the worst a
        // stale row can do is cost one rebuild. A wrong free-capacity number
        // overcommits a node, so "validated against the rows" is the only
        // form of this that is safe to ship.
        //
        // Purely additive, no backfill: an empty table means every node
        // rebuilds once. An older manager binary neither reads nor writes
        // it, so a mixed-version cluster (and a rollback) is safe in both
        // directions — a new leader taking over after an old one placed
        // allocations simply finds the fingerprints no longer match.
        //
        // NOTE for a future migration that REBUILDS `allocation` (the
        // ownerlane pattern above): the fingerprint is computed from live
        // rows, so a rebuild that preserves ids preserves validity. One that
        // renumbers ids must `DELETE FROM node_alloc_usage` in the same
        // migration.
        M::up(
            "CREATE TABLE IF NOT EXISTS node_alloc_usage (
            node_id STRING PRIMARY KEY,
            alloc_epoch INTEGER NOT NULL,
            alloc_count INTEGER NOT NULL,
            alloc_id_sum INTEGER NOT NULL,
            usage_json JSONB NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS node_alloc_usage;"),
        // `node_alloc_usage.policy_fingerprint`: the resource policy the
        // cached aggregate was normalized under (adversarial finding,
        // 2026-08-06). The row-set fingerprint above (epoch, count, id sum)
        // is pure row IDENTITY — it cannot see the manager's
        // `taskResourceDefaults`/`customResources` changing across a
        // restart, and an aggregate normalized under the OLD defaults would
        // keep validating forever on any node whose rows don't move.
        // Raising a default would then understate cached usage on every
        // quiet node: silent overcommit. The read path only serves a row
        // whose fingerprint equals the running policy's
        // (`ResourcePolicy::fingerprint`), so a policy change costs one
        // rebuild per node — the same as the cache's cold start.
        //
        // Nullable, no backfill, and rollback-safe in both directions: rows
        // written before this column (or by an older binary after a
        // rollback, whose INSERT names only the original columns) carry
        // NULL, which never equals a fingerprint, so they are rebuilt
        // rather than trusted.
        M::up("ALTER TABLE node_alloc_usage ADD COLUMN policy_fingerprint TEXT;").down(""),
        // JSONB -> TEXT affinity (jsonb-text-affinity plan). SQLite derives
        // column affinity from the declared type name by substring rules, and
        // `JSONB` matches none of the keywords — so every JSON column in the
        // schema had NUMERIC affinity, and every INSERT/UPDATE first ran
        // `applyNumericAffinity`/`sqlite3AtoF` over the whole bound blob
        // trying to store it as a number (~4-5% of a sampled manager's CPU;
        // a local A/B measures the numeric-affinity twin ~5-10% slower on
        // the raw JSON insert path). The parse always fails on JSON
        // objects/arrays, so the work was pure waste — sitting inside the
        // serialized plan-apply commit (allocation.resources,
        // plan_submission.placements, node_alloc_usage.usage_json), the
        // job-intake path (21 columns on task_config), and the event stream.
        //
        // Every JSONB-carrying table is rebuilt with the identical schema
        // text except ` JSONB` -> ` TEXT`. Stored content is untouched (the
        // numeric conversion never succeeded on JSON, so the values were
        // already TEXT); only future writes skip the parse.
        //
        // The rebuild deliberately preserves COLUMN ORDER, COLUMN COUNT, and
        // ROWIDS exactly — that is the load-bearing property, not a nicety.
        // Raft session changesets identify columns BY POSITION, so a
        // migration that reordered columns (e.g. the cheaper
        // add/copy/drop/rename column surgery) would let a mixed-version
        // manager pair silently write values into the wrong columns during a
        // rolling upgrade. A twin-table rebuild is positionally identical to
        // a pre-migration peer; the only cross-version difference is
        // declared type names, which changesets never carry. Preserved
        // rowids also keep `node_alloc_usage` fingerprints valid, per the
        // NOTE on that table's migration.
        //
        // `job_config` is the one rebuilt table that is a CASCADE parent
        // (task_config, job_config_submission, job_kv_reference, job_update
        // all reference it ON DELETE CASCADE), and migrations run with
        // `foreign_keys = ON` where `DROP TABLE` performs an implicit
        // `DELETE FROM` that fires cascades (the C-2 rule above). So it gets
        // a rename-first dance: rename the old table aside (children's FK
        // clauses follow the rename), create + fill the new `job_config`,
        // rebuild all four children (their fresh `REFERENCES job_config`
        // clauses bind to the new parent — three of them carry no JSONB and
        // are rebuilt with identical schemas purely to re-point their FKs),
        // and only then drop the renamed original, which by that point has
        // no FK-bearing children left.
        //
        // The three AUTOINCREMENT tables (plan_submission, rollout,
        // rollout_event) lose their sqlite_sequence rows at DROP and re-seed
        // to max(id) via the explicit-id copies — an id below the old
        // high-water mark can be reused if the largest-id row had been
        // deleted, the same acceptance the C-2 rebuild made.
        //
        // Empty down, like the other rebuilds: the original creations'
        // `DROP TABLE IF EXISTS` downs remove the rebuilt tables on the way
        // to version 0.
        M::up(
            "ALTER TABLE job_config RENAME TO job_config__jsonb;
        CREATE TABLE job_config (
            id STRING PRIMARY KEY,
            name STRING NOT NULL,
            type STRING NOT NULL,
            meta TEXT NOT NULL,
            version INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            job_id STRING NOT NULL, priority STRING NOT NULL DEFAULT 'normal',
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE
        );
        INSERT INTO job_config (id, name, type, meta, version, created_at, job_id, priority) SELECT id, name, type, meta, version, created_at, job_id, priority FROM job_config__jsonb;

        CREATE TABLE task_config__textaff (
            id INTEGER PRIMARY KEY,
            count INTEGER NOT NULL,
            name STRING NOT NULL,
            runtime STRING NOT NULL,
            config TEXT NOT NULL,
            resources TEXT NOT NULL,
            constraints TEXT NOT NULL,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL, affinities TEXT NOT NULL DEFAULT '{}', permit TEXT NOT NULL DEFAULT '{}', checks TEXT NOT NULL DEFAULT '[]', on_unhealthy TEXT NOT NULL DEFAULT '{}', count_per STRING, group_name STRING NOT NULL DEFAULT 'default', lifecycle STRING NOT NULL DEFAULT 'service', schedule STRING, depends_on TEXT NOT NULL DEFAULT '[]', kv_injections TEXT, network TEXT, service TEXT, identities TEXT, stateful TEXT, artifacts TEXT, actions TEXT, volumes TEXT, restart TEXT, connect TEXT, stop TEXT, disruption TEXT, speculative_start TEXT,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        );
        INSERT INTO task_config__textaff (id, count, name, runtime, config, resources, constraints, job_id, job_config_id, affinities, permit, checks, on_unhealthy, count_per, group_name, lifecycle, schedule, depends_on, kv_injections, network, service, identities, stateful, artifacts, actions, volumes, restart, connect, stop, disruption, speculative_start) SELECT id, count, name, runtime, config, resources, constraints, job_id, job_config_id, affinities, permit, checks, on_unhealthy, count_per, group_name, lifecycle, schedule, depends_on, kv_injections, network, service, identities, stateful, artifacts, actions, volumes, restart, connect, stop, disruption, speculative_start FROM task_config;
        DROP TABLE task_config;
        ALTER TABLE task_config__textaff RENAME TO task_config;
        CREATE INDEX idx_task_config_job_config_id ON task_config(job_config_id);
        CREATE INDEX idx_task_config_service_name
             ON task_config(COALESCE(json_extract(service, '$.name'), name))
             WHERE service IS NOT NULL AND json_valid(service);

        CREATE TABLE job_config_submission__textaff (
            id INTEGER PRIMARY KEY,
            path STRING NOT NULL,
            is_primary INTEGER NOT NULL,
            job_config_id STRING NOT NULL,
            submission_hash STRING NOT NULL,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(submission_hash) REFERENCES submission(hash) ON DELETE CASCADE
        );
        INSERT INTO job_config_submission__textaff (id, path, is_primary, job_config_id, submission_hash) SELECT id, path, is_primary, job_config_id, submission_hash FROM job_config_submission;
        DROP TABLE job_config_submission;
        ALTER TABLE job_config_submission__textaff RENAME TO job_config_submission;

        CREATE TABLE job_kv_reference__textaff (
            id INTEGER PRIMARY KEY,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            kv_key STRING NOT NULL,
            json_pointer STRING NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        );
        INSERT INTO job_kv_reference__textaff (id, job_id, job_config_id, kv_key, json_pointer) SELECT id, job_id, job_config_id, kv_key, json_pointer FROM job_kv_reference;
        DROP TABLE job_kv_reference;
        ALTER TABLE job_kv_reference__textaff RENAME TO job_kv_reference;
        CREATE INDEX idx_job_kv_reference_key ON job_kv_reference(kv_key);
        CREATE INDEX idx_job_kv_reference_job ON job_kv_reference(job_id);

        CREATE TABLE job_update__textaff (
            id STRING PRIMARY KEY,
            status STRING NOT NULL,
            job_id STRING NOT NULL,
            to_job_config_id STRING NOT NULL,
            from_job_config_id STRING,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(to_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(from_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        );
        INSERT INTO job_update__textaff (id, status, job_id, to_job_config_id, from_job_config_id) SELECT id, status, job_id, to_job_config_id, from_job_config_id FROM job_update;
        DROP TABLE job_update;
        ALTER TABLE job_update__textaff RENAME TO job_update;

        DROP TABLE job_config__jsonb;

        CREATE TABLE acl_policy__textaff (
            name STRING PRIMARY KEY,
            description STRING NOT NULL DEFAULT '',
            rules TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        INSERT INTO acl_policy__textaff (name, description, rules, created_at, updated_at) SELECT name, description, rules, created_at, updated_at FROM acl_policy;
        DROP TABLE acl_policy;
        ALTER TABLE acl_policy__textaff RENAME TO acl_policy;

        CREATE TABLE acl_token__textaff (
            accessor_id STRING PRIMARY KEY,
            secret_hash STRING NOT NULL UNIQUE,
            name STRING NOT NULL,
            token_type STRING NOT NULL,
            policies TEXT NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL
        );
        INSERT INTO acl_token__textaff (accessor_id, secret_hash, name, token_type, policies, expires_at, created_at) SELECT accessor_id, secret_hash, name, token_type, policies, expires_at, created_at FROM acl_token;
        DROP TABLE acl_token;
        ALTER TABLE acl_token__textaff RENAME TO acl_token;
        CREATE INDEX idx_acl_token_secret_hash ON acl_token(secret_hash);

        CREATE TABLE acl_user__textaff (
            username STRING PRIMARY KEY,
            password_hash STRING NOT NULL,
            policies TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        INSERT INTO acl_user__textaff (username, password_hash, policies, created_at, updated_at) SELECT username, password_hash, policies, created_at, updated_at FROM acl_user;
        DROP TABLE acl_user;
        ALTER TABLE acl_user__textaff RENAME TO acl_user;

        CREATE TABLE allocation__textaff (
            id INTEGER PRIMARY KEY,
            status STRING NOT NULL,
            resources TEXT NOT NULL,
            node_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            task_config_id INTEGER NOT NULL,
            network TEXT,
            instance_id STRING
        , created_at_unix INTEGER);
        INSERT INTO allocation__textaff (id, status, resources, node_id, job_config_id, task_config_id, network, instance_id, created_at_unix) SELECT id, status, resources, node_id, job_config_id, task_config_id, network, instance_id, created_at_unix FROM allocation;
        DROP TABLE allocation;
        ALTER TABLE allocation__textaff RENAME TO allocation;
        CREATE INDEX idx_allocation_task_config_id ON allocation(task_config_id);
        CREATE INDEX idx_allocation_job_config_id ON allocation(job_config_id);
        CREATE INDEX idx_allocation_node_id ON allocation(node_id);
        CREATE INDEX idx_allocation_instance_id ON allocation(instance_id);

        CREATE TABLE job_template_version__textaff (
            template_name STRING NOT NULL,
            version INTEGER NOT NULL,
            description STRING,
            source_nickel TEXT NOT NULL,
            variables TEXT NOT NULL DEFAULT '[]',
            created_by STRING NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY(template_name, version),
            FOREIGN KEY(template_name) REFERENCES job_template(name) ON DELETE CASCADE
        );
        INSERT INTO job_template_version__textaff (template_name, version, description, source_nickel, variables, created_by, created_at) SELECT template_name, version, description, source_nickel, variables, created_by, created_at FROM job_template_version;
        DROP TABLE job_template_version;
        ALTER TABLE job_template_version__textaff RENAME TO job_template_version;

        CREATE TABLE manager_config__textaff (
            node_id STRING PRIMARY KEY,
            config TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        INSERT INTO manager_config__textaff (node_id, config, updated_at) SELECT node_id, config, updated_at FROM manager_config;
        DROP TABLE manager_config;
        ALTER TABLE manager_config__textaff RENAME TO manager_config;

        CREATE TABLE node__textaff (
            id STRING PRIMARY KEY,
            pool STRING NOT NULL,
            availability STRING NOT NULL,
            config_attrs  TEXT NOT NULL,
            fingerprint_attrs  TEXT NOT NULL,
            dynamic_attrs TEXT NOT NULL, drain_evict INTEGER NOT NULL DEFAULT 0);
        INSERT INTO node__textaff (id, pool, availability, config_attrs, fingerprint_attrs, dynamic_attrs, drain_evict) SELECT id, pool, availability, config_attrs, fingerprint_attrs, dynamic_attrs, drain_evict FROM node;
        DROP TABLE node;
        ALTER TABLE node__textaff RENAME TO node;

        CREATE TABLE node_alloc_usage__textaff (
            node_id STRING PRIMARY KEY,
            alloc_epoch INTEGER NOT NULL,
            alloc_count INTEGER NOT NULL,
            alloc_id_sum INTEGER NOT NULL,
            usage_json TEXT NOT NULL
        , policy_fingerprint TEXT);
        INSERT INTO node_alloc_usage__textaff (node_id, alloc_epoch, alloc_count, alloc_id_sum, usage_json, policy_fingerprint) SELECT node_id, alloc_epoch, alloc_count, alloc_id_sum, usage_json, policy_fingerprint FROM node_alloc_usage;
        DROP TABLE node_alloc_usage;
        ALTER TABLE node_alloc_usage__textaff RENAME TO node_alloc_usage;

        CREATE TABLE node_health__textaff (
            node_id STRING PRIMARY KEY,
            status STRING NOT NULL,
            last_heartbeat TIMESTAMP NOT NULL,
            lost_at TIMESTAMP,
            health_checks TEXT NOT NULL
        , last_heartbeat_unix INTEGER, lost_after_seconds INTEGER);
        INSERT INTO node_health__textaff (node_id, status, last_heartbeat, lost_at, health_checks, last_heartbeat_unix, lost_after_seconds) SELECT node_id, status, last_heartbeat, lost_at, health_checks, last_heartbeat_unix, lost_after_seconds FROM node_health;
        DROP TABLE node_health;
        ALTER TABLE node_health__textaff RENAME TO node_health;

        CREATE TABLE plan_submission__textaff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_config_id STRING NOT NULL,
            mode STRING NOT NULL,
            placements TEXT NOT NULL,
            expected_epoch INTEGER NOT NULL,
            expected_version_vector TEXT NOT NULL DEFAULT '{}',
            status STRING NOT NULL,
            rejection_reason STRING,
            created_at TIMESTAMP NOT NULL
        , evictions TEXT NOT NULL DEFAULT '[]', target_instance STRING NOT NULL DEFAULT '');
        INSERT INTO plan_submission__textaff (id, job_config_id, mode, placements, expected_epoch, expected_version_vector, status, rejection_reason, created_at, evictions, target_instance) SELECT id, job_config_id, mode, placements, expected_epoch, expected_version_vector, status, rejection_reason, created_at, evictions, target_instance FROM plan_submission;
        DROP TABLE plan_submission;
        ALTER TABLE plan_submission__textaff RENAME TO plan_submission;
        CREATE INDEX idx_plan_submission_job_config_status ON plan_submission(job_config_id, status);

        CREATE TABLE rollout__textaff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id STRING NOT NULL,
            from_job_config_id STRING,
            to_job_config_id STRING NOT NULL,
            status STRING NOT NULL,
            incomplete INTEGER NOT NULL DEFAULT 0,
            message STRING NOT NULL DEFAULT '',
            strategy STRING,
            progression STRING,
            replacement STRING,
            in_place INTEGER,
            config_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP
        );
        INSERT INTO rollout__textaff (id, job_id, from_job_config_id, to_job_config_id, status, incomplete, message, strategy, progression, replacement, in_place, config_json, created_at, updated_at, completed_at) SELECT id, job_id, from_job_config_id, to_job_config_id, status, incomplete, message, strategy, progression, replacement, in_place, config_json, created_at, updated_at, completed_at FROM rollout;
        DROP TABLE rollout;
        ALTER TABLE rollout__textaff RENAME TO rollout;
        CREATE INDEX idx_rollout_job_id ON rollout(job_id);
        CREATE INDEX idx_rollout_status ON rollout(status);
        CREATE INDEX idx_rollout_job_status_created ON rollout(job_id, status, created_at DESC);

        CREATE TABLE rollout_event__textaff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rollout_id INTEGER NOT NULL,
            job_id STRING NOT NULL,
            group_name STRING,
            level STRING NOT NULL,
            message STRING NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL
        );
        INSERT INTO rollout_event__textaff (id, rollout_id, job_id, group_name, level, message, metadata_json, created_at) SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at FROM rollout_event;
        DROP TABLE rollout_event;
        ALTER TABLE rollout_event__textaff RENAME TO rollout_event;
        CREATE INDEX idx_rollout_event_rollout_created ON rollout_event(rollout_id, created_at ASC, id ASC);
        CREATE INDEX idx_rollout_event_job_created ON rollout_event(job_id, created_at DESC, id DESC);

        CREATE TABLE scheduler_event__textaff (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            level TEXT NOT NULL,
            job_id TEXT,
            node_id TEXT,
            eval_id TEXT,
            allocation_id INTEGER,
            rollout_id INTEGER,
            message TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL
        );
        INSERT INTO scheduler_event__textaff (id, kind, level, job_id, node_id, eval_id, allocation_id, rollout_id, message, metadata_json, created_at) SELECT id, kind, level, job_id, node_id, eval_id, allocation_id, rollout_id, message, metadata_json, created_at FROM scheduler_event;
        DROP TABLE scheduler_event;
        ALTER TABLE scheduler_event__textaff RENAME TO scheduler_event;

        CREATE TABLE volume__textaff (
            volume_id STRING PRIMARY KEY,
            name STRING NOT NULL DEFAULT 'data',
            driver STRING NOT NULL,
            job_id STRING,
            instance_id STRING,
            task_name STRING,
            state STRING NOT NULL,
            node_id STRING,
            node_local INTEGER NOT NULL DEFAULT 1,
            read_only INTEGER NOT NULL DEFAULT 0,
            params TEXT NOT NULL DEFAULT '{}',
            attrs TEXT NOT NULL DEFAULT '{}',
            mount_path STRING,
            last_error STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        , volume_mode STRING NOT NULL DEFAULT 'filesystem', fs STRING);
        INSERT INTO volume__textaff (volume_id, name, driver, job_id, instance_id, task_name, state, node_id, node_local, read_only, params, attrs, mount_path, last_error, created_at, updated_at, volume_mode, fs) SELECT volume_id, name, driver, job_id, instance_id, task_name, state, node_id, node_local, read_only, params, attrs, mount_path, last_error, created_at, updated_at, volume_mode, fs FROM volume;
        DROP TABLE volume;
        ALTER TABLE volume__textaff RENAME TO volume;
        CREATE INDEX idx_volume_instance ON volume(instance_id);
        CREATE INDEX idx_volume_node ON volume(node_id);
        CREATE INDEX idx_volume_state ON volume(state);
        CREATE INDEX idx_volume_destroying
             ON volume(state) WHERE state = 'destroying';

        CREATE TABLE worker_type__textaff (
            name STRING PRIMARY KEY,
            pool STRING NOT NULL,
            additional_node_checks Text NOT NULL,
            attributes TEXT NOT NULL,
            name_prefix STRING
        , resources TEXT NOT NULL DEFAULT '{}', reserved_resources TEXT NOT NULL DEFAULT '{}', max_allocations INTEGER, artifacts TEXT NOT NULL DEFAULT '', host_volumes TEXT NOT NULL DEFAULT '', resource_enforcement TEXT NOT NULL DEFAULT '', heartbeat_seconds INTEGER, edge_boot TEXT NOT NULL DEFAULT '');
        INSERT INTO worker_type__textaff (name, pool, additional_node_checks, attributes, name_prefix, resources, reserved_resources, max_allocations, artifacts, host_volumes, resource_enforcement, heartbeat_seconds, edge_boot) SELECT name, pool, additional_node_checks, attributes, name_prefix, resources, reserved_resources, max_allocations, artifacts, host_volumes, resource_enforcement, heartbeat_seconds, edge_boot FROM worker_type;
        DROP TABLE worker_type;
        ALTER TABLE worker_type__textaff RENAME TO worker_type;",
        )
        .down(""),
        // Network policy objects (`legion net policy`, docs/network-handlers.md):
        // the canonical JSON of a `NetPolicyObject` plus the columns the list
        // view orders by. `net_policy_meta.generation` is bumped on every
        // write so the heartbeat builder can skip the merge on clusters where
        // nothing changed (and skip it entirely where nothing exists).
        M::up(
            "CREATE TABLE IF NOT EXISTS net_policy (
            name STRING PRIMARY KEY,
            priority INTEGER NOT NULL DEFAULT 0,
            enforcement STRING NOT NULL DEFAULT 'default',
            body TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS net_policy_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO net_policy_meta (id, generation) VALUES (1, 0);",
        )
        .down("DROP TABLE IF EXISTS net_policy; DROP TABLE IF EXISTS net_policy_meta;"),
        // The mesh CA behind `certFrom = "legion"` (docs/network-handlers.md):
        // one row, created on the first IssueNodeCertificate, replicated
        // with the control lane so every manager signs with the same key.
        M::up(
            "CREATE TABLE IF NOT EXISTS net_mesh_ca (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cert_pem TEXT NOT NULL,
            key_pem TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        );",
        )
        .down("DROP TABLE IF EXISTS net_mesh_ca;"),
        // KV encryption at rest (docs/kv-encryption.md). `enc_key_id` names
        // the data key a value is encrypted under; NULL is a plaintext row
        // (written before this migration, or with encryption disabled) and
        // stays readable until a rewrap converts it. `keyring_key` holds the
        // data keys WRAPPED by the root key — the root key itself never
        // enters the store, so this table (and every backup, snapshot, and
        // changeset carrying it) is useless without the key file, env var,
        // or KMS that wraps it. Control lane, with kv_entry. Bumping this
        // pair must keep KEYRING_SCHEMA_VERSION in step (see its test).
        M::up("ALTER TABLE kv_entry ADD COLUMN enc_key_id STRING;").down(""),
        M::up(
            "CREATE TABLE IF NOT EXISTS keyring_key (
            key_id STRING PRIMARY KEY,
            algorithm STRING NOT NULL,
            wrapped_key BLOB NOT NULL,
            root_key_id STRING NOT NULL,
            root_provider STRING NOT NULL,
            state STRING NOT NULL,
            created_at INTEGER NOT NULL,
            retired_at INTEGER
        )",
        )
        .down("DROP TABLE IF EXISTS keyring_key;"),
        // Per-job config lookups. "This job's latest config" is the most
        // common job_config read (`LATEST_CONFIG_FOR_JOB` and every
        // `WHERE job_id = ? ORDER BY created_at DESC` / `version DESC`), and
        // without an index each one scanned the whole table. As a correlated
        // subquery that made `jobs_with_unrolled_latest_config` O(jobs^2):
        // ~60s per node-status-checker tick at 16.7k jobs, so the checker ran
        // it back to back and held a core on the leader (AWS 20k-node run,
        // 2026-10-10). `rollout.to_job_config_id` is the same pass's
        // NOT EXISTS probe.
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_job_config_job_id_created ON job_config(job_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_job_config_job_id_version ON job_config(job_id, version DESC);
        CREATE INDEX IF NOT EXISTS idx_rollout_to_job_config_id ON rollout(to_job_config_id);",
        )
        .down(
            "DROP INDEX IF EXISTS idx_job_config_job_id_created;
        DROP INDEX IF EXISTS idx_job_config_job_id_version;
        DROP INDEX IF EXISTS idx_rollout_to_job_config_id;",
        ),
        // Two more whole-table scans on the submit and eval paths
        // (plan-path-fleet-scaling plan, Phase 4): `job_update` by
        // `to_job_config_id` is how every job-submit eval (and every RunJob)
        // finds the config it replaces (`Store::previous_job_config_id`),
        // and `job_config` by `name` is how every RunJob loads the job's
        // current config (`GET_JOB_CONFIG`). Each cost O(jobs) per use.
        M::up(
            "CREATE INDEX IF NOT EXISTS idx_job_update_to_job_config_id ON job_update(to_job_config_id);
        CREATE INDEX IF NOT EXISTS idx_job_config_name_version ON job_config(name, version DESC);",
        )
        .down(
            "DROP INDEX IF EXISTS idx_job_update_to_job_config_id;
        DROP INDEX IF EXISTS idx_job_config_name_version;",
        ),
        // Snapshots (suspend plan §7, docs/suspend-and-resume.md). A
        // `snapshot` row is the manager's record of a capture whose bytes
        // live on `node_id`: its state machine (capturing -> local | failed,
        // -> deleting -> removed, -> lost) drives the heartbeat checkpoint
        // channel, which re-sends work for `capturing` and `deleting` rows
        // until the node reports. `snapshot_alias` holds operator names
        // (`--name`), unique per job. `instance.restore_from` names the
        // snapshot an instance is restoring from until its node reports the
        // outcome. `task_config.snapshots` is the task's `snapshots` block.
        // Scheduler lane, with `instance` and `volume`.
        M::up(
            "CREATE TABLE IF NOT EXISTS snapshot (
            snapshot_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            task_name TEXT NOT NULL,
            origin_instance_id TEXT,
            name TEXT,
            kind TEXT NOT NULL,
            purpose TEXT NOT NULL,
            state TEXT NOT NULL,
            node_id TEXT,
            allocation_id INTEGER,
            manifest_json TEXT NOT NULL DEFAULT '',
            total_bytes INTEGER NOT NULL DEFAULT 0,
            duration_ms INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at INTEGER NOT NULL,
            expires_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS snapshot_node_state ON snapshot (node_id, state);
        CREATE INDEX IF NOT EXISTS snapshot_job ON snapshot (job_id, created_at);
        CREATE TABLE IF NOT EXISTS snapshot_alias (
            job_id TEXT NOT NULL,
            name TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (job_id, name)
        );
        ALTER TABLE instance ADD COLUMN restore_from TEXT;
        ALTER TABLE task_config ADD COLUMN snapshots TEXT;",
        )
        .down(
            "DROP TABLE IF EXISTS snapshot; DROP TABLE IF EXISTS snapshot_alias;",
        ),
        // Suspended instances (suspend plan §7, docs/suspend-and-resume.md).
        // `suspended` is a new value of `instance.state`. `checkpoint_id`
        // names the suspend snapshot a resume restores from; `pending_op` is
        // `suspend` while a capture is outstanding and `resume` while a
        // restore has not reported yet. The rest is what `instance get`
        // shows about the last suspend and restore. `task_config.suspend` is
        // the task's `suspend` block. Scheduler lane, like `instance`.
        M::up(
            "ALTER TABLE instance ADD COLUMN checkpoint_id TEXT;
        ALTER TABLE instance ADD COLUMN suspend_reason TEXT;
        ALTER TABLE instance ADD COLUMN suspended_at INTEGER;
        ALTER TABLE instance ADD COLUMN pending_op TEXT;
        ALTER TABLE instance ADD COLUMN restored_from_snapshot_id TEXT;
        ALTER TABLE instance ADD COLUMN last_suspend_ms INTEGER;
        ALTER TABLE instance ADD COLUMN last_restore_ms INTEGER;
        ALTER TABLE instance ADD COLUMN resume_requested_at INTEGER;
        ALTER TABLE instance ADD COLUMN lifecycle_error TEXT;
        ALTER TABLE task_config ADD COLUMN suspend TEXT;",
        )
        .down(""),
        // Allocation ids are never reused. `INTEGER PRIMARY KEY` alone hands
        // the next insert `MAX(id) + 1`, so deleting the newest allocation
        // and placing another (a suspend then a resume, a restore, any stop
        // then start) gave the new allocation the old one's id. A restored
        // guest's workload-identity token must name the NEW allocation, and
        // a worker must never mistake a replacement for the task it already
        // runs, so the table is rebuilt with AUTOINCREMENT. The copy keeps
        // every id and seeds sqlite_sequence to the current maximum (an id
        // deleted before this migration can still be reused once, the same
        // acceptance the earlier rebuilds made). No foreign keys point at
        // `allocation` any more (the ownerlane rebuild removed them).
        M::up(
            "CREATE TABLE allocation__monotonic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status STRING NOT NULL,
            resources TEXT NOT NULL,
            node_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            task_config_id INTEGER NOT NULL,
            network TEXT,
            instance_id STRING,
            created_at_unix INTEGER
        );
        INSERT INTO allocation__monotonic (id, status, resources, node_id, job_config_id, task_config_id, network, instance_id, created_at_unix) SELECT id, status, resources, node_id, job_config_id, task_config_id, network, instance_id, created_at_unix FROM allocation;
        DROP TABLE allocation;
        ALTER TABLE allocation__monotonic RENAME TO allocation;
        CREATE INDEX idx_allocation_task_config_id ON allocation(task_config_id);
        CREATE INDEX idx_allocation_job_config_id ON allocation(job_config_id);
        CREATE INDEX idx_allocation_node_id ON allocation(node_id);
        CREATE INDEX idx_allocation_instance_id ON allocation(instance_id);",
        )
        .down(""),
        // Phase 6 (fork, docs/suspend-and-resume.md): how long a live capture
        // paused the workload, which ForkInstance reports as `pause_ms`. A
        // fork waiting for its checkpoint is `instance.pending_op = 'fork'`,
        // an existing column, so the instance table does not change.
        M::up("ALTER TABLE snapshot ADD COLUMN pause_ms INTEGER;").down(""),
        // Published snapshots (suspend plan §6.8, Phase 7b;
        // docs/suspend-and-resume.md). `durability` is `local` or `store`: a
        // `store` row is published to its node's contentStore once captured,
        // and moves `local -> published` when the upload reports
        // (`published_at`). The manifest then records each part's URL.
        // Scheduler lane, like the rest of `snapshot`.
        M::up(
            "ALTER TABLE snapshot ADD COLUMN durability TEXT NOT NULL DEFAULT 'local';
        ALTER TABLE snapshot ADD COLUMN published_at INTEGER;",
        )
        .down(""),
        // A failed publish is retried by the manager (docs/suspend-and-resume.md,
        // "Durability"): `publish_attempts` counts the reported failures, and
        // `publish_retry_at` (unix seconds) is when the next `op = publish`
        // goes out. NULL once the snapshot is published or out of attempts.
        M::up(
            "ALTER TABLE snapshot ADD COLUMN publish_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE snapshot ADD COLUMN publish_retry_at INTEGER;",
        )
        .down(""),
        // Suspend/resume UI plan §7.3. `last_stop` is JSON
        // ([`crate::structs::instance::InstanceLastStop`]): why the
        // instance's last allocation stopped, written in the transaction that
        // removes it. `forked_from` names a fork's origin instance, so lineage
        // outlives the fork checkpoint. Scheduler lane, like `instance`.
        M::up(
            "ALTER TABLE instance ADD COLUMN last_stop TEXT;
        ALTER TABLE instance ADD COLUMN forked_from TEXT;",
        )
        .down(""),
        // A resume or restore that booted cold in place of a failed restore
        // (`onRestoreFailure = "cold"`), as JSON
        // ([`crate::structs::suspend::InstanceColdBoot`]). Kept until the
        // next resume or restore starts, so `instance get`, `instance list`,
        // and the UI can say the running state was lost. Scheduler lane,
        // like `instance`.
        M::up("ALTER TABLE instance ADD COLUMN cold_boot TEXT;").down(""),
        // The instance's workload-identity epoch (docs/workload-identity.md,
        // "Forks and snapshots"): stamped into its tokens as
        // `legion.io/epoch` and raised when a fork or snapshot of it is
        // captured, so Legion refuses the tokens copied into the
        // checkpoint. Scheduler lane, like `instance`.
        M::up("ALTER TABLE instance ADD COLUMN identity_epoch INTEGER NOT NULL DEFAULT 0;")
            .down(""),
        // Removed allocations, remembered for `ALLOCATION_HISTORY_RETENTION_SECS`
        // so `legion job logs` can still find where a removed allocation ran
        // (docs/logging.md, "Logs of removed allocations"). Written by every
        // allocation-removal op in the transaction that deletes the row, and
        // expired by those same transactions, so it is a scheduler-lane
        // table like `allocation_stop_notice`. No foreign keys: the rows it
        // describes are gone, and a destroyed job's history outlives it.
        M::up(
            "CREATE TABLE IF NOT EXISTS allocation_history (
            allocation_id INTEGER PRIMARY KEY,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            task_config_id INTEGER NOT NULL,
            task_name STRING NOT NULL DEFAULT '',
            node_id STRING NOT NULL,
            status STRING NOT NULL,
            cause STRING NOT NULL DEFAULT '',
            removed_at_unix INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_allocation_history_job
            ON allocation_history(job_id, removed_at_unix);",
        )
        .down(
            "DROP INDEX IF EXISTS idx_allocation_history_job;
        DROP TABLE IF EXISTS allocation_history;",
        ),
    ]
}

#[cfg(test)]
mod migrations_test {
    use super::*;
    use rusqlite::Connection;

    /// `KEYRING_SCHEMA_VERSION` is what the raft upgrade gate compares peer
    /// versions against, so it must be exactly the migration that creates
    /// `keyring_key` — walk the list and find it, rather than trust a number.
    #[test]
    fn keyring_schema_version_matches_the_migration() -> anyhow::Result<()> {
        let migrations = get_migrations();
        let total = schema_version() as usize;
        assert!(total >= KEYRING_SCHEMA_VERSION as usize);
        let has_keyring = |conn: &Connection| -> anyhow::Result<bool> {
            let count: i64 = conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'keyring_key';",
                [],
                |row| row.get(0),
            )?;
            Ok(count == 1)
        };
        let mut conn = Connection::open_in_memory()?;
        migrations.to_version(&mut conn, KEYRING_SCHEMA_VERSION as usize - 1)?;
        assert!(
            !has_keyring(&conn)?,
            "keyring_key exists one version BEFORE KEYRING_SCHEMA_VERSION"
        );
        let has_column = |conn: &Connection| -> anyhow::Result<bool> {
            let mut stmt = conn.prepare("PRAGMA table_info(kv_entry);")?;
            let names = stmt
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(names.iter().any(|name| name == "enc_key_id"))
        };
        migrations.to_version(&mut conn, KEYRING_SCHEMA_VERSION as usize)?;
        assert!(
            has_keyring(&conn)? && has_column(&conn)?,
            "KEYRING_SCHEMA_VERSION must be the migration that creates keyring_key \
             (the column migration directly precedes it)"
        );
        Ok(())
    }

    /// The mixed-version write guard reads an older peer's table layout
    /// from here, so it must match what migrating to that version produces.
    #[test]
    fn table_shapes_at_reflects_the_schema_of_that_version() -> anyhow::Result<()> {
        let latest = table_shapes_at(schema_version())?;
        let before_keyring = table_shapes_at(KEYRING_SCHEMA_VERSION - 2)?;
        assert!(latest.contains_key("keyring_key"));
        assert!(!before_keyring.contains_key("keyring_key"));
        let kv_now = latest.get("kv_entry").expect("kv_entry exists");
        let kv_then = before_keyring.get("kv_entry").expect("kv_entry existed");
        assert!(kv_now.len() > kv_then.len(), "kv_entry gained enc_key_id");
        assert!(table_shapes_at(0)?.is_empty());
        Ok(())
    }

    /// A cluster created before the `kind` -> `runtime` rename must migrate its
    /// existing task rows in place, with the values preserved.
    #[test]
    fn task_config_kind_column_migrates_to_runtime() -> anyhow::Result<()> {
        let migrations = get_migrations();
        let mut conn = Connection::open_in_memory()?;
        migrations.to_latest(&mut conn)?;

        // Reconstruct a pre-rename database by migrating UP to the last
        // version that still exposes `kind`, on a fresh connection. Stepping
        // DOWN is not viable: additive `ALTER TABLE` migrations carry no-op
        // `.down("")`, so a down-then-up walk re-runs them and fails with
        // "duplicate column". Searching for the rename (instead of assuming
        // it is the last migration) keeps this test valid as migrations are
        // appended after it.
        let latest: u32 = conn.query_row("PRAGMA user_version;", [], |row| row.get(0))?;
        let has_kind_column = |conn: &Connection| -> anyhow::Result<bool> {
            let mut stmt = conn.prepare("PRAGMA table_info(task_config);")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                if row.get::<_, String>(1)? == "kind" {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let mut pre_rename_version = None;
        for version in 1..=latest as usize {
            let mut probe = Connection::open_in_memory()?;
            migrations.to_version(&mut probe, version)?;
            if has_kind_column(&probe)? {
                pre_rename_version = Some(version);
            }
        }
        let pre_rename_version =
            pre_rename_version.expect("no migration version exposes the pre-rename `kind` column");
        let mut conn = Connection::open_in_memory()?;
        migrations.to_version(&mut conn, pre_rename_version)?;

        conn.execute(
            "INSERT INTO job (id, name, status, created_at)
             VALUES ('job-1', 'demo', 'active', '2026-01-01T00:00:00Z');",
            [],
        )?;
        conn.execute(
            "INSERT INTO job_config (id, name, type, meta, version, created_at, job_id)
             VALUES ('jc-1', 'demo', 'service', '{}', 1, '2026-01-01T00:00:00Z', 'job-1');",
            [],
        )?;
        conn.execute(
            "INSERT INTO task_config (id, count, name, kind, config, resources, constraints, job_id, job_config_id)
             VALUES (1, 1, 'web', 'container', '{}', '{}', '{}', 'job-1', 'jc-1');",
            [],
        )?;

        migrations.to_latest(&mut conn)?;

        let runtime: String =
            conn.query_row("SELECT runtime FROM task_config WHERE id = 1;", [], |row| {
                row.get(0)
            })?;
        assert_eq!(runtime, "container");

        // The old spelling must be gone, not shadowed by a leftover column.
        let has_kind = conn
            .prepare("SELECT kind FROM task_config;")
            .err()
            .is_some();
        assert!(has_kind, "`kind` column should no longer exist");

        Ok(())
    }

    /// The JSONB -> TEXT affinity rebuild must be invisible except to a
    /// profiler: same rows, same column order (raft changesets bind by
    /// position), same indexes, same FK behavior — under `foreign_keys = ON`,
    /// which is how production runs migrations and what makes the job_config
    /// rename dance necessary in the first place.
    #[test]
    fn jsonb_affinity_rebuild_preserves_rows_order_indexes_and_fks() -> anyhow::Result<()> {
        let migrations = get_migrations();
        let mut probe = Connection::open_in_memory()?;
        migrations.to_latest(&mut probe)?;
        let latest: usize = probe.query_row("PRAGMA user_version;", [], |row| row.get(0))?;

        let jsonb_count = |conn: &Connection| -> anyhow::Result<i64> {
            Ok(conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND sql LIKE '%JSONB%';",
                [],
                |row| row.get(0),
            )?)
        };

        // The newest version that still declares JSONB anywhere is the
        // pre-rebuild schema. Searching (like the rename tests above) keeps
        // this valid as migrations are appended after the rebuild.
        let mut pre_version = None;
        for version in 1..=latest {
            let mut walk = Connection::open_in_memory()?;
            migrations.to_version(&mut walk, version)?;
            if jsonb_count(&walk)? > 0 {
                pre_version = Some(version);
            }
        }
        let pre_version = pre_version.expect("no migration version declares a JSONB column");

        let mut conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        migrations.to_version(&mut conn, pre_version)?;

        // One row in every rebuilt table (and the parents they reference),
        // JSON payloads included, explicit ids on the AUTOINCREMENT tables.
        conn.execute_batch(r#"
            INSERT INTO job (id, name, status, created_at) VALUES ('job-1','demo','active','2026-01-01T00:00:00Z');
            INSERT INTO job_config (id, name, type, meta, version, created_at, job_id) VALUES ('jc-1','demo','service','{"team":"platform"}',1,'2026-01-01T00:00:00Z','job-1');
            INSERT INTO submission (hash, content) VALUES ('h1','{}');
            INSERT INTO job_config_submission (id, path, is_primary, job_config_id, submission_hash) VALUES (1,'main.ncl',1,'jc-1','h1');
            INSERT INTO task_config (id, count, name, runtime, config, resources, constraints, job_id, job_config_id, service) VALUES (1,2,'web','noop','{}','{"cpu":500}','{"class":"web"}','job-1','jc-1','{"name":"web-svc"}');
            INSERT INTO job_kv_reference (id, job_id, job_config_id, kv_key, json_pointer) VALUES (1,'job-1','jc-1','cfg/key','/a/b');
            INSERT INTO job_update (id, status, job_id, to_job_config_id) VALUES ('ju-1','complete','job-1','jc-1');
            INSERT INTO node (id, pool, availability, config_attrs, fingerprint_attrs, dynamic_attrs) VALUES ('n1','default','active','{}','{"memory":1}','{}');
            INSERT INTO node_health (node_id, status, last_heartbeat, health_checks) VALUES ('n1','active','2026-01-01T00:00:00Z','{}');
            INSERT INTO allocation (id, status, resources, node_id, job_config_id, task_config_id) VALUES (7,'running','{"cpu":500}','n1','jc-1',1);
            INSERT INTO node_alloc_usage (node_id, alloc_epoch, alloc_count, alloc_id_sum, usage_json) VALUES ('n1',3,1,7,'{"cpu":500}');
            INSERT INTO plan_submission (id, job_config_id, mode, placements, expected_epoch, status, created_at) VALUES (5,'jc-1','fresh','[{"node":"n1"}]',1,'applied','2026-01-01T00:00:00Z');
            INSERT INTO rollout (id, job_id, to_job_config_id, status, created_at, updated_at) VALUES (9,'job-1','jc-1','complete','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
            INSERT INTO rollout_event (id, rollout_id, job_id, level, message, metadata_json, created_at) VALUES (11,9,'job-1','info','step','{"pct":25}','2026-01-01T00:00:00Z');
            INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at) VALUES (1,'placed','info','ok','{"n":1}','2026-01-01T00:00:00Z');
            INSERT INTO volume (volume_id, driver, state, created_at, updated_at) VALUES ('v1','host','ready','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
            INSERT INTO worker_type (name, pool, additional_node_checks, attributes) VALUES ('wt1','default','','{"os":"linux"}');
            INSERT INTO manager_config (node_id, config, updated_at) VALUES ('m1','{"evalThreads":4}','2026-01-01T00:00:00Z');
            INSERT INTO acl_policy (name, rules, created_at, updated_at) VALUES ('p1','[{"resource":"job"}]','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
            INSERT INTO acl_token (accessor_id, secret_hash, name, token_type, policies, created_at) VALUES ('a1','s1','root','management','[]','2026-01-01T00:00:00Z');
            INSERT INTO acl_user (username, password_hash, policies, created_at, updated_at) VALUES ('u1','h','[]','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
            INSERT INTO job_template (name, latest_version, created_at, updated_at) VALUES ('t1',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
            INSERT INTO job_template_version (template_name, version, source_nickel, variables, created_at) VALUES ('t1',1,'{}','[{"name":"x"}]','2026-01-01T00:00:00Z');
        "#)?;

        let rebuilt = [
            "job_config",
            "task_config",
            "job_config_submission",
            "job_kv_reference",
            "job_update",
            "acl_policy",
            "acl_token",
            "acl_user",
            "allocation",
            "job_template_version",
            "manager_config",
            "node",
            "node_alloc_usage",
            "node_health",
            "plan_submission",
            "rollout",
            "rollout_event",
            "scheduler_event",
            "volume",
            "worker_type",
        ];
        let column_names = |conn: &Connection, table: &str| -> anyhow::Result<Vec<String>> {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({table});"))?;
            let names = stmt
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(names)
        };
        let index_names = |conn: &Connection, table: &str| -> anyhow::Result<Vec<String>> {
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL ORDER BY name;",
            )?;
            let names = stmt
                .query_map([table], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(names)
        };
        let mut pre_columns = Vec::new();
        let mut pre_indexes = Vec::new();
        for table in rebuilt {
            pre_columns.push(column_names(&conn, table)?);
            pre_indexes.push(index_names(&conn, table)?);
        }

        // Exactly the rebuild, not `to_latest`: later migrations may add
        // indexes to these tables, and that is not the rebuild changing them.
        migrations.to_version(&mut conn, pre_version + 1)?;

        // No live table declares JSONB anymore — this is also the guard that
        // keeps future migrations from re-introducing a NUMERIC-affinity
        // JSON column.
        assert_eq!(jsonb_count(&conn)?, 0, "a live table still declares JSONB");

        // Positional layout and index sets are exactly what they were. Later
        // migrations may append columns (`ADD COLUMN` keeps every existing
        // position), so the pre-rebuild columns must be an exact prefix.
        for (i, table) in rebuilt.iter().enumerate() {
            let columns = column_names(&conn, table)?;
            assert_eq!(
                columns[..pre_columns[i].len().min(columns.len())],
                pre_columns[i][..],
                "column order changed on {table} — raft changesets bind by position"
            );
            assert_eq!(
                index_names(&conn, table)?,
                pre_indexes[i],
                "index set changed on {table}"
            );
        }

        // Rows and JSON payloads came through byte for byte.
        for (table, expected) in [("job_config", 1i64), ("task_config", 1), ("allocation", 1)] {
            let count: i64 =
                conn.query_row(&format!("SELECT count(*) FROM {table};"), [], |r| r.get(0))?;
            assert_eq!(count, expected, "{table} row count");
        }
        let meta: String =
            conn.query_row("SELECT meta FROM job_config WHERE id='jc-1';", [], |r| {
                r.get(0)
            })?;
        assert_eq!(meta, r#"{"team":"platform"}"#);
        let service: String =
            conn.query_row("SELECT service FROM task_config WHERE id=1;", [], |r| {
                r.get(0)
            })?;
        assert_eq!(service, r#"{"name":"web-svc"}"#);
        let resources: String =
            conn.query_row("SELECT resources FROM allocation WHERE id=7;", [], |r| {
                r.get(0)
            })?;
        assert_eq!(resources, r#"{"cpu":500}"#);
        let usage: String = conn.query_row(
            "SELECT usage_json FROM node_alloc_usage WHERE node_id='n1';",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(usage, r#"{"cpu":500}"#);
        let placements: String = conn.query_row(
            "SELECT placements FROM plan_submission WHERE id=5;",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(placements, r#"[{"node":"n1"}]"#);

        // Nothing was orphaned by the rebuild.
        let violations: i64 = conn.query_row(
            "SELECT count(*) FROM pragma_foreign_key_check();",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(violations, 0, "foreign_key_check found violations");

        // TEXT affinity in practice: numeric-looking text now STAYS text
        // (under NUMERIC affinity this stored as an INTEGER).
        conn.execute(
            "INSERT INTO node_alloc_usage (node_id, alloc_epoch, alloc_count, alloc_id_sum, usage_json) VALUES ('n2',1,0,0,'123');",
            [],
        )?;
        let stored_type: String = conn.query_row(
            "SELECT typeof(usage_json) FROM node_alloc_usage WHERE node_id='n2';",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(stored_type, "text", "JSON columns must have TEXT affinity");

        // AUTOINCREMENT resumed above the copied ids.
        conn.execute(
            "INSERT INTO plan_submission (job_config_id, mode, placements, expected_epoch, status, created_at) VALUES ('jc-1','fresh','[]',1,'applied','2026-01-01T00:00:00Z');",
            [],
        )?;
        let next_id: i64 =
            conn.query_row("SELECT max(id) FROM plan_submission;", [], |r| r.get(0))?;
        assert!(
            next_id > 5,
            "plan_submission AUTOINCREMENT regressed: {next_id}"
        );

        // The FKs still enforce, and still cascade, against the new parent.
        assert!(
            conn.execute(
                "INSERT INTO task_config (id, count, name, runtime, config, resources, constraints, job_id, job_config_id) VALUES (2,1,'x','noop','{}','{}','{}','job-1','nope');",
                [],
            )
            .is_err(),
            "FK on task_config.job_config_id no longer enforces"
        );
        conn.execute("DELETE FROM job_config WHERE id='jc-1';", [])?;
        for table in [
            "task_config",
            "job_config_submission",
            "job_kv_reference",
            "job_update",
        ] {
            let count: i64 =
                conn.query_row(&format!("SELECT count(*) FROM {table};"), [], |r| r.get(0))?;
            assert_eq!(
                count, 0,
                "job_config delete no longer cascades into {table}"
            );
        }

        Ok(())
    }

    /// A cluster created before `job.updated_at` existed must come out of the
    /// migration with a usable mtime rather than a NULL or an epoch-zero
    /// timestamp — the UI's Updated column reads it directly, and "Jan 1
    /// 1970" is a worse lie than "never changed since submission".
    #[test]
    fn job_updated_at_backfills_from_created_at() -> anyhow::Result<()> {
        let migrations = get_migrations();

        // Walk to the newest version that predates the column, rather than
        // hard-coding an index, so appending migrations cannot silently turn
        // this into a test of nothing.
        let mut probe = Connection::open_in_memory()?;
        migrations.to_latest(&mut probe)?;
        let latest: usize = probe.query_row("PRAGMA user_version;", [], |row| row.get(0))?;
        let has_updated_at = |conn: &Connection| -> anyhow::Result<bool> {
            let mut stmt = conn.prepare("PRAGMA table_info(job);")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                if row.get::<_, String>(1)? == "updated_at" {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let mut pre_column_version = None;
        for version in 1..=latest {
            let mut probe = Connection::open_in_memory()?;
            migrations.to_version(&mut probe, version)?;
            if !has_updated_at(&probe)? {
                pre_column_version = Some(version);
            }
        }
        let pre_column_version =
            pre_column_version.expect("no migration version predates `job.updated_at`");

        let mut conn = Connection::open_in_memory()?;
        migrations.to_version(&mut conn, pre_column_version)?;
        // Unix seconds, the form `submit_job_config` has always written.
        conn.execute(
            "INSERT INTO job (id, name, status, created_at)
             VALUES ('legacy', 'legacy', 'done', 1735689600);",
            [],
        )?;

        migrations.to_latest(&mut conn)?;

        let (created_at, updated_at): (i64, Option<i64>) = conn.query_row(
            "SELECT created_at, updated_at FROM job WHERE id = 'legacy';",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(created_at, 1_735_689_600, "created_at is left untouched");
        assert_eq!(
            updated_at,
            Some(1_735_689_600),
            "a pre-column job's mtime backfills to its creation time"
        );

        Ok(())
    }
}
