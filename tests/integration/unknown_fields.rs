//! Strict jobspec fields through the manager's RPCs
//! (docs/jobs-tasks-and-scheduling.md, "Unknown fields"): an unknown key is
//! an INVALID_ARGUMENT naming its path and a suggestion, and
//! `allow_unknown_fields` keeps extension-point keys, drops the rest, warns,
//! and records both on the version `GetJobInfo` reports.
//!
//! The specs use the built-in `exec` runtime: `noop` is a dylib plugin,
//! built only with LEGION_BUILD_PLUGINS=1.

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, manager_server::Manager, GetJobInfoRequest, NodeMsg, PlanJobRequest,
    RunJobRequest, Submission, UnknownJobspecField,
};
use tempfile::Builder;
use tokio::runtime::Runtime;
use tonic::{Code, Request};

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-unknown-fields-it-")
        .tempdir_in(crate::support::scratch_root())
        .context("error creating temp store directory")?
        .keep();
    let config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("error parsing store path")?
            .to_string(),
        ..Default::default()
    };
    Store::start_storage(&config)
}

/// A node for `PlanJob` to place onto.
fn insert_node(store: &Store, id: &str) -> R<()> {
    store.insert_node(NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: json!({ "worker": "pass" }).to_string(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

fn run_request(jobspec: &Value, allow_unknown_fields: bool) -> RunJobRequest {
    RunJobRequest {
        parsed_jobspec: jobspec.to_string(),
        submissions: vec![Submission {
            path: "orders-db.ncl".into(),
            content: "orders-db".into(),
            is_primary: true,
        }],
        allow_unknown_fields,
        ..Default::default()
    }
}

/// The plan's example: a job with keys for apps that are not installed, plus
/// one typo inside a core block.
fn spec_with_unknown_keys() -> Value {
    json!({
        "name": "orders-db",
        "uptime": { "url": "https://orders.acme.dev/healthz" },
        "tasks": {
            "postgres": {
                "runtime": "exec",
                "config": { "command": ["true"] },
                "stop": { "signall": "SIGINT" },
                "volumes": [{ "name": "data", "backup": { "schedule": "0 3 * * *" } }],
                "network": { "ports": [{ "name": "http", "dns": { "hostname": "orders" } }] },
            }
        }
    })
}

fn field(scope: &str, path: &str, point: &str) -> UnknownJobspecField {
    UnknownJobspecField {
        scope: scope.into(),
        path: path.into(),
        point: point.into(),
    }
}

#[test]
fn an_unknown_key_fails_the_submit_with_its_path_and_a_suggestion() -> R<()> {
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let status = runtime
        .block_on(manager.run_job(Request::new(run_request(&spec_with_unknown_keys(), false))))
        .expect_err("a strict submit refuses unknown keys");
    assert_eq!(status.code(), Code::InvalidArgument);
    let message = status.message();
    for line in [
        "orders-db: unknown field `uptime`",
        "orders-db: task postgres: unknown field `stop.signall` (did you mean `stop.signal`?)",
        "orders-db: task postgres: volume data: unknown field `backup`",
        "orders-db: task postgres: port http: unknown field `dns`",
        "\nHint: ",
    ] {
        assert!(message.contains(line), "missing {line:?} in:\n{message}");
    }

    // Nothing was stored.
    assert!(store.get_job_config_optional("orders-db")?.is_none());

    // A plan refuses what the submit would.
    let status = runtime
        .block_on(manager.plan_job(Request::new(PlanJobRequest {
            parsed_jobspec: spec_with_unknown_keys().to_string(),
            ..Default::default()
        })))
        .expect_err("a strict plan refuses unknown keys");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("did you mean `stop.signal`?"));
    Ok(())
}

#[test]
fn allow_unknown_fields_keeps_pending_drops_the_rest_and_records_both() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let reply = runtime
        .block_on(manager.run_job(Request::new(run_request(&spec_with_unknown_keys(), true))))?
        .into_inner();
    assert_eq!(
        reply.warnings[..4],
        [
            "task postgres: dropped unknown field `stop.signall`".to_string(),
            "kept `uptime` as pending (no installed app declares it)".to_string(),
            "task postgres: port http: kept `dns` as pending (no installed app declares it)"
                .to_string(),
            "task postgres: volume data: kept `backup` as pending (no installed app declares it)"
                .to_string(),
        ]
    );
    assert!(
        reply
            .message
            .contains("\nwarning: task postgres: dropped unknown field `stop.signall`"),
        "{}",
        reply.message
    );

    // Stored with the version, as written.
    let config = store.get_job_config("orders-db")?;
    assert_eq!(
        config.pending_extensions["uptime"],
        json!({ "url": "https://orders.acme.dev/healthz" })
    );
    let task = &config.tasks[0];
    assert_eq!(
        task.volumes[0].pending_extensions["backup"],
        json!({ "schedule": "0 3 * * *" })
    );
    assert_eq!(
        task.network.as_ref().expect("network").ports[0].pending_extensions["dns"],
        json!({ "hostname": "orders" })
    );
    assert_eq!(task.stop.as_ref().expect("stop").signal, "SIGTERM");

    // And recorded on it, for `legion job info`.
    let info = runtime
        .block_on(manager.get_job_info(Request::new(GetJobInfoRequest {
            name: "orders-db".into(),
        })))?
        .into_inner();
    let record = info.unknown_fields.expect("the version records the flag");
    assert_eq!(
        record.dropped,
        vec![field("task postgres", "stop.signall", "")]
    );
    assert_eq!(
        record.pending,
        vec![
            field("", "uptime", "job"),
            field("task postgres: port http", "dns", "port"),
            field("task postgres: volume data", "backup", "volume"),
        ]
    );

    // The same submit again is unchanged and keeps the record.
    let again = runtime
        .block_on(manager.run_job(Request::new(run_request(&spec_with_unknown_keys(), true))))?
        .into_inner();
    assert!(
        again.message.starts_with("Job config unchanged"),
        "{}",
        again.message
    );
    assert_eq!(store.get_job_config("orders-db")?.version, Some(1));

    // A pending key on a task is kept too, and a plan with the flag reports
    // what the submit would keep and drop.
    let mut with_task_key = spec_with_unknown_keys();
    with_task_key["tasks"]["postgres"]["autoscael"] = json!(true);
    let plan = runtime
        .block_on(manager.plan_job(Request::new(PlanJobRequest {
            parsed_jobspec: with_task_key.to_string(),
            allow_unknown_fields: true,
            ..Default::default()
        })))?
        .into_inner();
    assert!(
        plan.warnings.contains(
            &"task postgres: kept `autoscael` as pending (no installed app declares it)"
                .to_string()
        ),
        "{:?}",
        plan.warnings
    );

    // A clean, strict resubmit is a new version with no record.
    let clean = json!({
        "name": "orders-db",
        "tasks": {
            "postgres": {
                "runtime": "exec",
                "config": { "command": ["true"] },
                "volumes": [{ "name": "data" }],
                "network": { "ports": [{ "name": "http" }] },
            }
        }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(&clean, false))))?;
    let config = store.get_job_config("orders-db")?;
    assert_eq!(config.version, Some(2));
    assert!(config.pending_extensions.is_empty());
    assert!(config.tasks[0].volumes[0].pending_extensions.is_empty());
    let info = runtime
        .block_on(manager.get_job_info(Request::new(GetJobInfoRequest {
            name: "orders-db".into(),
        })))?
        .into_inner();
    assert_eq!(info.unknown_fields, None);
    Ok(())
}

#[test]
fn a_task_key_is_kept_on_the_task_and_survives_the_store() -> R<()> {
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let spec = json!({
        "name": "web",
        "task": { "name": "web", "runtime": "exec", "config": { "command": ["true"] }, "autoscael": true }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(&spec, true))))?;
    let config = store.get_job_config("web")?;
    assert_eq!(config.tasks[0].pending_extensions["autoscael"], json!(true));
    // The read model serves the same task, pending map included.
    let tasks = store.get_task_configs_for_job_config(&config.id())?;
    assert_eq!(tasks[0].1.pending_extensions["autoscael"], json!(true));
    Ok(())
}

#[test]
fn a_known_but_wrong_value_fails_even_with_the_flag() -> R<()> {
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store);
    let runtime = Runtime::new()?;

    let spec = json!({
        "name": "web",
        "task": { "runtime": "exec", "count": "three", "autoscael": true }
    });
    let status = runtime
        .block_on(manager.run_job(Request::new(run_request(&spec, true))))
        .expect_err("a bad count is an error with or without the flag");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("count must be a whole number"),
        "{}",
        status.message()
    );
    Ok(())
}
