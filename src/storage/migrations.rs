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
            "CREATE TABLE IF NOT EXISTS plan_submission (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_config_id STRING NOT NULL,
            mode STRING NOT NULL,
            placements JSONB NOT NULL,
            expected_epoch INTEGER NOT NULL,
            status STRING NOT NULL,
            rejection_reason STRING,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE IF EXISTS plan_submission;"),
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
    ])
}
