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
        .down("DROP TABLE node;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS worker_type (
            name STRING PRIMARY KEY,
            pool STRING NOT NULL,
            additional_node_checks Text NOT NULL,
            attributes JSONB NOT NULL,
            name_prefix STRING
        )",
        )
        .down("DROP TABLE worker_type;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS job (
            id STRING PRIMARY KEY,
            name STRING NOT NULL,
            status STRING NOT NULL,
            created_at TIMESTAMP NOT NULL
        )",
        )
        .down("DROP TABLE job;"),
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
        .down("DROP TABLE job_config;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS submission (
            hash STRING PRIMARY KEY,
            content TEXT NOT NULL
        )",
        )
        .down("DROP TABLE submission;"),
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
        .down("DROP TABLE job_config_submission;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS task_config (
            id INTEGER PRIMARY KEY,
            count INTEGER NOT NULL,
            name STRING NOT NULL,
            driver STRING NOT NULL,
            config JSONB NOT NULL,
            resources JSONB NOT NULL,
            constraints JSONB NOT NULL,
            job_id STRING NOT NULL,
            job_config_id STRING NOT NULL,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE task_config;"),
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
        .down("DROP TABLE job_update;"),
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
        .down("DROP TABLE allocation;"),
        M::up(
            "CREATE TABLE IF NOT EXISTS eval (
            id INTEGER PRIMARY KEY,
            status STRING NOT NULL,
            type STRING NOT NULL,
            job_id STRING,
            job_config_id STRING,
            node_id STRING,
            FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
            FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
            FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE
        )",
        )
        .down("DROP TABLE eval;"),
    ])
}
