use rusqlite_migration::{Migrations, M};

pub fn get_migrations() -> Migrations<'static> {
    Migrations::new(vec![
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
        // manifest at upsert time so list/get never re-evaluate Jsonnet.
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
            source_jsonnet TEXT NOT NULL,
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
    ])
}

#[cfg(test)]
mod migrations_test {
    use super::*;
    use rusqlite::Connection;

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
