//! Regression: replicated ops whose SQL relies on `ON DELETE CASCADE`
//! (e.g. `destroy_job`, `remove_node`) must survive the raft
//! capture → propose → apply path.
//!
//! The changeset capture records BOTH the parent delete and every cascaded
//! child delete. At apply time `sqlite3changeset_apply` executes the parent
//! DELETE with `foreign_keys = ON`, which re-fires the cascade in the target
//! DB; the changeset's child DELETE records then find no row, raise
//! `SQLITE_CHANGESET_NOTFOUND`, and the conflict handler aborts the apply.
//! The entry is already quorum-committed, so the state machine can never
//! apply it: every later write fails on the same poisoned log entry and a
//! restart replays into the same failure — the store is permanently bricked
//! by a routine `legion job destroy`.
//!
//! Found by the adversarial review pass (plan §4); FAILS until fixed.

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{KvValueType, Node, NodeMsg, Submission};

fn raft_config(root: &std::path::Path) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-a".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

#[test]
fn cascade_reliant_deletes_survive_raft_apply() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path())?)?;

    store.insert_node(NodeMsg::from(test_node("node-1")))?;
    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "cascade-job".to_string(),
        tasks: vec![TaskConfig::new("svc", "noop")],
        version: None,
    };
    let outcome = store.handle_job_submit(
        &job,
        &[Submission {
            path: "cascade-job.ncl".into(),
            content: "job cascade-job".into(),
            is_primary: true,
        }],
    )?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = tasks.first().context("job must have a task config")?;
    // An allocation gives the job delete FK children to cascade into.
    store.place_alloc("node-1", *task_id)?;
    store.stop_job("cascade-job")?;

    // The cascade-reliant delete must replicate and apply.
    store
        .destroy_job("cascade-job")
        .context("destroy_job must survive the raft apply path")?;
    assert!(
        store.get_job("cascade-job").is_err(),
        "destroyed job must be gone"
    );

    // And the store must still accept writes afterwards (a failed apply
    // poisons the committed log entry: every later write dies on it).
    let kv = store
        .put_kv("post-destroy", "alive", KvValueType::String, None, None)
        .context("store must keep accepting writes after a cascade delete")?;
    assert_eq!(kv.version, 1);

    // Same shape for node removal (node -> allocation cascade).
    store
        .remove_node("node-1")
        .context("remove_node must survive the raft apply path")?;
    Ok(())
}
