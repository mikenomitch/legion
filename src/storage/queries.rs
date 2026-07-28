use lazy_static::lazy_static;

//
// NODES
//

// The node/node_health split (plan §7-C2, phase C-1): `node` carries
// identity/pool/attrs/`availability` (control lane); `node_health` carries
// liveness — `status`, `last_heartbeat`, `lost_at`, `health_checks` (status
// lane). Every place a `Node` struct is hydrated reads this LEFT JOIN so the
// struct's public shape is unchanged for all callers.
//
// A node without a `node_health` row (registered but not yet heartbeated,
// or a crash between registration's control and status ops) hydrates the
// same liveness registration used to set before the split: status
// `'active'`, empty `last_heartbeat`, no `lost_at`, `'{}'` health checks.
pub const NODE_JOINED_ATTRS: &str = r#"
node.id, node.pool, COALESCE(node_health.status, 'active'), node.availability, node.config_attrs, node.fingerprint_attrs, node.dynamic_attrs, COALESCE(node_health.health_checks, '{}'), COALESCE(node_health.last_heartbeat, '')
"#;

pub const NODE_JOINED_FROM: &str = r#"
FROM node LEFT JOIN node_health ON node_health.node_id = node.id
"#;

lazy_static! {
    pub static ref GET_NODE: String = format!(
        r#"
    SELECT {NODE_JOINED_ATTRS}
    {NODE_JOINED_FROM}
    WHERE node.id = ?
    "#
    );
    pub static ref GET_NODES: String = format!(
        r#"
    SELECT {NODE_JOINED_ATTRS}
    {NODE_JOINED_FROM};
    "#
    );
}

/// Control-lane registration upsert: identity/pool/attrs/availability only —
/// liveness lives in `node_health`, seeded by the status lane (see
/// `Store::insert_node`).
pub const INSERT_NODE: &str = r#"
INSERT INTO node (id, pool, availability, config_attrs, fingerprint_attrs, dynamic_attrs)
VALUES (:id, :pool, :availability, :config_attrs, :fingerprint_attrs, :dynamic_attrs)
ON CONFLICT(id) DO UPDATE SET pool = :pool, availability = :availability, config_attrs = :config_attrs, fingerprint_attrs = :fingerprint_attrs, dynamic_attrs = :dynamic_attrs;
"#;

/// Status-lane registration seed (INSERT OR REPLACE = wholesale reset of the
/// liveness row): a (re-)registering node is by definition not lost, so
/// `lost_at` resets to NULL and the stateful node-loss confirmation clock
/// stops (see mark_node_lost).
pub const SEED_NODE_HEALTH: &str = r#"
INSERT OR REPLACE INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
VALUES (:id, :status, :last_heartbeat, NULL, :health_checks);
"#;

//
// ALLOCATION
//

pub const ALLOCATION_ATTRS: &str = r#"
id, status, resources, node_id, job_config_id, task_config_id, instance_id
"#;

pub const ALLOCATION_WITH_JOB_ATTRS: &str = r#"
allocation.id, allocation.status, allocation.resources, allocation.node_id, allocation.job_config_id, allocation.task_config_id, job_config.job_id, allocation.instance_id, allocation.network
"#;

pub const ALLOCATION_SELECT_ATTRS: &str = r#"
allocation.id, allocation.status, allocation.resources, allocation.node_id, allocation.job_config_id, allocation.task_config_id, task_config.job_id, allocation.instance_id, allocation.network
"#;

pub const ALLOCATION_RETURNING_ATTRS: &str = r#"
id, status, resources, node_id, job_config_id, task_config_id,
    (SELECT job_id FROM job_config WHERE job_config.id = allocation.job_config_id) as job_id,
    instance_id,
    network
"#;

pub const ALLOCATION_INSERT_ATTRS: &str = r#"
status, resources, node_id, job_config_id, task_config_id, instance_id, network
"#;

pub const ALLOCATION_ATTRS_VALS: &str = r#"
:status, :resources, :node_id, :job_config_id, :task_config_id, :instance_id, :network
"#;

lazy_static! {
    pub static ref GET_ALLOCS: String = format!(
        r#"
    SELECT {ALLOCATION_WITH_JOB_ATTRS}
    FROM allocation
    INNER JOIN job_config ON job_config.id = allocation.job_config_id;
    "#,
    );
    pub static ref GET_ALLOCATION_BY_ID: String = format!(
        r#"
    SELECT {ALLOCATION_WITH_JOB_ATTRS}
    FROM allocation
    INNER JOIN job_config ON job_config.id = allocation.job_config_id
    WHERE allocation.id = ?;
    "#,
    );
    pub static ref GET_ALLOCS_FOR_NODE: String = format!(
        r#"
    SELECT {ALLOCATION_WITH_JOB_ATTRS}
    FROM allocation
    INNER JOIN job_config ON job_config.id = allocation.job_config_id
    WHERE allocation.node_id = ?;
    "#,
    );
    pub static ref GET_ALLOCS_FOR_JOB_CONFIG: String = format!(
        r#"
    SELECT {ALLOCATION_WITH_JOB_ATTRS}
    FROM allocation
    INNER JOIN job_config ON job_config.id = allocation.job_config_id
    WHERE allocation.job_config_id = ?;
    "#,
    );
    pub static ref GET_ALLOCS_FOR_JOB: String = format!(
        r#"
    SELECT {ALLOCATION_SELECT_ATTRS}
    FROM allocation
    INNER JOIN task_config ON task_config.id = allocation.task_config_id
    WHERE task_config.job_id = ?;
    "#,
    );
    pub static ref INSERT_ALLOCATION: String = format!(
        r#"
    INSERT INTO allocation ({ALLOCATION_INSERT_ATTRS})
    VALUES ({ALLOCATION_ATTRS_VALS})
    RETURNING {ALLOCATION_RETURNING_ATTRS};
    "#,
    );
}

//
// INSTANCE
//

pub const INSTANCE_ATTRS: &str = r#"
instance_id, job_id, group_name, task_name, mode, state, slot_index, current_alloc_id, storage_handle, created_at, updated_at
"#;

pub const INSTANCE_ATTRS_VALS: &str = r#"
:instance_id, :job_id, :group_name, :task_name, :mode, :state, :slot_index, :current_alloc_id, :storage_handle, :created_at, :updated_at
"#;

pub const INSTANCE_PARAM_SET: &str = r#"
job_id = :job_id, group_name = :group_name, task_name = :task_name, mode = :mode, state = :state, slot_index = :slot_index, current_alloc_id = :current_alloc_id, storage_handle = :storage_handle, updated_at = :updated_at
"#;

lazy_static! {
    pub static ref GET_INSTANCE: String = format!(
        r#"
    SELECT {INSTANCE_ATTRS}
    FROM instance
    WHERE instance_id = ?;
    "#
    );
    pub static ref GET_INSTANCES_FOR_JOB: String = format!(
        r#"
    SELECT {INSTANCE_ATTRS}
    FROM instance
    WHERE job_id = ?
    ORDER BY group_name ASC, slot_index ASC, instance_id ASC;
    "#
    );
    pub static ref UPSERT_INSTANCE: String = format!(
        r#"
    INSERT INTO instance ({INSTANCE_ATTRS})
    VALUES ({INSTANCE_ATTRS_VALS})
    ON CONFLICT(instance_id) DO UPDATE SET {INSTANCE_PARAM_SET}
    RETURNING {INSTANCE_ATTRS};
    "#
    );
}

//
// WORKER_TYPE
//

pub const WORKER_TYPE_ATTRS: &str = r#"
name, pool, additional_node_checks, attributes, name_prefix, resources, reserved_resources, max_allocations, artifacts, host_volumes
"#;

pub const WORKER_TYPE_PARAM_SET: &str = r#"
pool = :pool, additional_node_checks = :additional_node_checks, attributes = :attributes, name_prefix = :name_prefix, resources = :resources, reserved_resources = :reserved_resources, max_allocations = :max_allocations, artifacts = :artifacts, host_volumes = :host_volumes
"#;

pub const WORKER_TYPE_ATTRS_VALS: &str = r#"
:name, :pool, :additional_node_checks, :attributes, :name_prefix, :resources, :reserved_resources, :max_allocations, :artifacts, :host_volumes
"#;

lazy_static! {
    pub static ref GET_WORKER_TYPE: String = format!(
        r#"
    SELECT {WORKER_TYPE_ATTRS}
    FROM worker_type
    WHERE name = ?
    "#
    );
    pub static ref GET_WORKER_TYPES: String = format!(
        r#"
    SELECT {WORKER_TYPE_ATTRS}
    FROM worker_type;
    "#
    );
    pub static ref INSERT_WORKER_TYPE: String = format!(
        r#"
    INSERT INTO worker_type ({WORKER_TYPE_ATTRS})
    VALUES ({WORKER_TYPE_ATTRS_VALS})
    ON CONFLICT(name) DO UPDATE SET {WORKER_TYPE_PARAM_SET}
    RETURNING {WORKER_TYPE_ATTRS};
    "#
    );
}

//
// JOB
//

pub const JOB_ATTRS: &str = r#"
id, name, status, created_at
"#;

pub const JOB_PARAM_SET: &str = r#"
name = :name, status = :status, created_at = :created_at
"#;

pub const JOB_ATTRS_VALS: &str = r#"
:id, :name, :status, :created_at
"#;

lazy_static! {
    pub static ref GET_JOB: String = format!(
        r#"
    SELECT {JOB_ATTRS}
    FROM job
    WHERE name = ?
    "#
    );
    pub static ref GET_JOBS: String = format!(
        r#"
    SELECT {JOB_ATTRS}
    FROM job;
    "#
    );
    pub static ref INSERT_JOB: String = format!(
        r#"
    INSERT INTO job ({JOB_ATTRS})
    VALUES ({JOB_ATTRS_VALS})
    ON CONFLICT(id) DO UPDATE SET {JOB_PARAM_SET}
    RETURNING {JOB_ATTRS};
    "#
    );
}

//
// JOB CONFIG
//

pub const JOB_CONFIG_ATTRS: &str = r#"
id, name, type, meta, version, created_at, job_id
"#;

// pub const JOB_CONFIG_PARAM_SET: &str = r#"
// name = :name, type = :type, meta = :meta, version = :version, created_at = :created_at, job_id = :job_id
// "#;

pub const JOB_CONFIG_ATTRS_VALS: &str = r#"
:id, :name, :type, :meta, :version, :created_at, :job_id
"#;

lazy_static! {
    pub static ref GET_JOB_CONFIG: String = format!(
        r#"
    SELECT {JOB_CONFIG_ATTRS}
    FROM job_config
    WHERE name = ?
    ORDER BY version DESC
    LIMIT 1;
    "#
    );
    pub static ref GET_JOB_CONFIG_BY_ID: String = format!(
        r#"
    SELECT {JOB_CONFIG_ATTRS}
    FROM job_config
    WHERE id = ?
    LIMIT 1;
    "#
    );
    pub static ref GET_JOB_CONFIGS: String = format!(
        r#"
    SELECT {JOB_CONFIG_ATTRS}
    FROM job_config;
    "#
    );
    pub static ref INSERT_JOB_CONFIG: String = format!(
        r#"
    INSERT INTO job_config ({JOB_CONFIG_ATTRS})
    VALUES ({JOB_CONFIG_ATTRS_VALS})
    RETURNING {JOB_CONFIG_ATTRS};
    "#
    );
}

//
// TASK CONFIG
//

pub const TASK_CONFIG_ATTRS: &str = r#"
id, name, runtime, count, count_per, group_name, lifecycle, schedule, depends_on, config, resources, constraints, permit, affinities, checks, on_unhealthy, kv_injections, job_id, job_config_id, identities, stateful, artifacts, network, service, actions, volumes
"#;

pub const TASK_CONFIG_INSERT_ATTRS: &str = r#"
name, runtime, count, count_per, group_name, lifecycle, schedule, depends_on, config, resources, constraints, permit, affinities, checks, on_unhealthy, kv_injections, job_id, job_config_id, identities, stateful, artifacts, network, service, actions, volumes
"#;

// pub const TASK_CONFIG_PARAM_SET: &str = r#"
// count = :count, name = :name, runtime = :runtime, config = :config, resources = :resources, constraints = :constraints, job_id = :job_id, job_config_id = :job_config_id
// "#;

pub const TASK_CONFIG_ATTRS_VALS: &str = r#"
:name, :runtime, :count, :count_per, :group_name, :lifecycle, :schedule, :depends_on, :config, :resources, :constraints, :permit, :affinities, :checks, :on_unhealthy, :kv_injections, :job_id, :job_config_id, :identities, :stateful, :artifacts, :network, :service, :actions, :volumes
"#;

lazy_static! {
    pub static ref GET_TASK: String = format!(
        r#"
    SELECT {TASK_CONFIG_ATTRS}
    FROM task_config
    WHERE name = ?
    "#
    );
    pub static ref GET_TASK_FOR_JOB_CONIFG: String = format!(
        r#"
    SELECT {TASK_CONFIG_ATTRS}
    FROM task_config
    WHERE job_config_id = ?;
    "#
    );
    pub static ref GET_TASKS: String = format!(
        r#"
    SELECT {TASK_CONFIG_ATTRS}
    FROM task_config;
    "#
    );
    pub static ref INSERT_TASK: String = format!(
        r#"
    INSERT INTO task_config ({TASK_CONFIG_INSERT_ATTRS})
    VALUES ({TASK_CONFIG_ATTRS_VALS})
    RETURNING id;
    "#
    );
}

//
// SUBMISSION
//

pub const SUBMISSION_ATTRS: &str = r#"
content, hash
"#;

// pub const SUBMISSION_PARAM_SET: &str = r#"
// content = :content, hash = :hash
// "#;

pub const SUBMISSION_ATTRS_VALS: &str = r#"
:content, :hash
"#;

lazy_static! {
    pub static ref GET_SUBMISSION: String = format!(
        r#"
    SELECT {SUBMISSION_ATTRS}
    FROM submission
    WHERE id = ?
    "#
    );
    pub static ref GET_SUBMISSIONS: String = format!(
        r#"
    SELECT {SUBMISSION_ATTRS}
    FROM submission;
    "#
    );
    pub static ref INSERT_SUBMISSION: String = format!(
        r#"
        INSERT INTO submission ({SUBMISSION_ATTRS})
        VALUES ({SUBMISSION_ATTRS_VALS})
        ON CONFLICT(hash) DO NOTHING;
    "#
    );
}

//
// JOB CONFIG SUBMISSION
//

pub const JOB_CONFIG_SUBMISSION_INSERT_ATTRS: &str = r#"
path, is_primary, job_config_id, submission_hash
"#;

pub const JOB_CONFIG_SUBMISSION_ATTRS: &str = r#"
id, path, is_primary, job_config_id, submission_hash
"#;

// pub const JOB_CONFIG_SUBMISSION_PARAM_SET: &str = r#"
// path = :path, is_primary = :is_primary, job_config_id = :job_config_id, submission_hash = :submission_hash
// "#;

pub const JOB_CONFIG_SUBMISSION_ATTRS_VALS: &str = r#"
:path, :is_primary, :job_config_id, :submission_hash
"#;

lazy_static! {
    pub static ref GET_JOB_CONFIG_SUBMISSION: String = format!(
        r#"
    SELECT {JOB_CONFIG_SUBMISSION_ATTRS}
    FROM job_config_submission
    WHERE id = ?
    "#
    );
    pub static ref GET_JOB_CONFIG_SUBMISSIONS: String = format!(
        r#"
    SELECT {JOB_CONFIG_SUBMISSION_ATTRS}
    FROM job_config_submission;
    "#
    );
    pub static ref INSERT_JOB_CONFIG_SUBMISSION: String = format!(
        r#"
    INSERT INTO job_config_submission ({JOB_CONFIG_SUBMISSION_INSERT_ATTRS})
    VALUES ({JOB_CONFIG_SUBMISSION_ATTRS_VALS})
    "#
    );
}

//
// JOB UPDATE
//

pub const JOB_UPDATE_ATTRS: &str = r#"
id, status, job_id, to_job_config_id, from_job_config_id
"#;

pub const JOB_UPDATE_ATTRS_VALS: &str = r#"
:id, :status, :job_id, :to_job_config_id, :from_job_config_id
"#;

// pub const JOB_UPDATE_PARAM_SET: &str = r#"
// status = :status, job_id = :job_id, to_job_config_id = :to_job_config_id, from_job_config_id = :from_job_config_id
// "#;

lazy_static! {
    pub static ref GET_JOB_UPDATE: String = format!(
        r#"
    SELECT {JOB_UPDATE_ATTRS}
    FROM job_update
    WHERE id = ?
    "#
    );
    pub static ref GET_JOB_UPDATE_FOR_CONFIG: String = format!(
        r#"
    SELECT {JOB_UPDATE_ATTRS}
    FROM job_update
    WHERE to_job_config_id = ?
    ORDER BY rowid DESC
    LIMIT 1;
    "#
    );
    pub static ref GET_JOB_UPDATES: String = format!(
        r#"
    SELECT {JOB_UPDATE_ATTRS}
    FROM job_update;
    "#
    );
    pub static ref INSERT_JOB_UPDATE: String = format!(
        r#"
    INSERT INTO job_update ({JOB_UPDATE_ATTRS})
    VALUES ({JOB_UPDATE_ATTRS_VALS});
    "#
    );
}

//
// EVAL
//

pub const EVAL_ATTRS: &str = r#"
id, status, type, job_id, job_config_id, node_id, priority, ready_at
"#;

pub const EVAL_ATTRS_VALS: &str = r#"
:id, :status, :type, :job_id, :job_config_id, :node_id, :priority, :ready_at
"#;

// pub const EVAL_PARAM_SET: &str = r#"
// status = :status, type = :type, job_id = :job_id, job_config_id = :job_config_id, node_id = :node_id
// "#;

lazy_static! {
    pub static ref GET_NEXT_EVAL: String = format!(
        r#"
    SELECT {EVAL_ATTRS}
    FROM eval
    WHERE status = 'pending'
      AND ready_at <= ?
    ORDER BY priority DESC, id ASC
    LIMIT 1;
    "#
    );
    pub static ref GET_EVALS: String = format!(
        r#"
    SELECT {EVAL_ATTRS}
    FROM eval;
    "#
    );
    pub static ref INSERT_EVAL: String = format!(
        r#"
    INSERT INTO eval ({EVAL_ATTRS})
    VALUES ({EVAL_ATTRS_VALS});
    "#
    );
}

//
// MANAGER CONFIG
//

pub const MANAGER_CONFIG_ATTRS: &str = r#"
node_id, config, updated_at
"#;

pub const MANAGER_CONFIG_ATTRS_VALS: &str = r#"
:node_id, :config, :updated_at
"#;

pub const MANAGER_CONFIG_PARAM_SET: &str = r#"
config = :config, updated_at = :updated_at
"#;

lazy_static! {
    pub static ref UPSERT_MANAGER_CONFIG: String = format!(
        r#"
    INSERT INTO manager_config ({MANAGER_CONFIG_ATTRS})
    VALUES ({MANAGER_CONFIG_ATTRS_VALS})
    ON CONFLICT(node_id) DO UPDATE SET {MANAGER_CONFIG_PARAM_SET};
    "#,
    );
    pub static ref GET_MANAGER_CONFIGS: String = format!(
        r#"
    SELECT {MANAGER_CONFIG_ATTRS}
    FROM manager_config;
    "#,
    );
}

//
// SIGNING KEYS (workload identity OIDC issuer; see docs/workload-identity.md)
//

pub const SIGNING_KEY_ATTRS: &str = r#"
kid, alg, private_key_pem, public_jwk_x, public_jwk_y, created_at, not_after, state
"#;

pub const SIGNING_KEY_ATTRS_VALS: &str = r#"
:kid, :alg, :private_key_pem, :public_jwk_x, :public_jwk_y, :created_at, :not_after, :state
"#;

lazy_static! {
    pub static ref INSERT_SIGNING_KEY: String = format!(
        r#"
    INSERT INTO signing_key ({SIGNING_KEY_ATTRS})
    VALUES ({SIGNING_KEY_ATTRS_VALS});
    "#
    );
    pub static ref GET_SIGNING_KEYS: String = format!(
        r#"
    SELECT {SIGNING_KEY_ATTRS}
    FROM signing_key
    ORDER BY created_at DESC, kid ASC;
    "#
    );
    // COALESCE keeps the recorded overlap window when a key is expired
    // (callers pass NULL to mean "leave not_after as-is").
    pub static ref UPDATE_SIGNING_KEY_STATE: String = r#"
    UPDATE signing_key
    SET state = :state, not_after = COALESCE(:not_after, not_after)
    WHERE kid = :kid;
    "#
    .to_string();
    pub static ref DELETE_SIGNING_KEY: String = r#"
    DELETE FROM signing_key
    WHERE kid = :kid;
    "#
    .to_string();
}
