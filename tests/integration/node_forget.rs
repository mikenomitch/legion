//! `ForgetNode` end to end (autoscaler v2 plan §6.11 and §6.13;
//! docs/architecture-managers-workers.md, "Forgetting a node"):
//!
//! - the guard refuses a live, schedulable node, a node with allocations
//!   (cordoned or not), and a node holding node-local volumes, each with
//!   what to do instead;
//! - a cordoned, empty node is forgotten without `--force`;
//! - `--force` removes the node and its allocations, retry-safe with an
//!   idempotency key;
//! - the node's token is revoked in the same write, so the worker cannot
//!   heartbeat, and a node group's spent single-use join token refuses the
//!   forgotten node's re-join.

use crate::support::{acl_service, authed};
use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::manager_server::Manager;
use legion::structs::token::JoinToken;
use legion::structs::{
    CreateJoinTokenRequest, ForgetNodeReply, ForgetNodeRequest, HeartbeatRequest,
    QueryStateStoreRequest, RunJobRequest, SetNodeAvailabilityRequest, WorkerConfig,
    WorkerRegisterRequest,
};
use tonic::Code;

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

/// The eval worker, until it has nothing left.
fn settle(store: &Store) -> R<()> {
    let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
    }
    Ok(())
}

fn service() -> R<(ManagerDaemonService, String)> {
    let (svc, admin) = acl_service("legion-node-forget-")?;
    svc.store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    Ok((svc, admin))
}

/// Mint a join token (`max_uses`) and register `name` with it, with room
/// for a few small tasks. Returns (join secret, node token).
async fn join(
    svc: &ManagerDaemonService,
    admin: &str,
    name: &str,
    max_uses: Option<u32>,
) -> R<(String, String)> {
    let created = svc
        .create_join_token(authed(
            CreateJoinTokenRequest {
                worker_type: "default".into(),
                ttl: Some("1h".into()),
                max_uses,
                ..Default::default()
            },
            Some(admin),
        ))
        .await?
        .into_inner();
    let secret = JoinToken::decode(&created.token)?.secret;
    let node_token = register(svc, name, &secret).await?;
    Ok((secret, node_token.context("a node token")?))
}

async fn register(
    svc: &ManagerDaemonService,
    name: &str,
    secret: &str,
) -> Result<Option<String>, tonic::Status> {
    svc.worker_register(authed(
        WorkerRegisterRequest {
            name: name.into(),
            pool: "default".into(),
            config_attrs: json!({ "maxAllocations": 10 }).to_string(),
            fingerprint_attrs: json!({
                "memory_total": "4294967296",
                "cpus": { "cpu-1": { "frequency": 2400.0 } }
            })
            .to_string(),
            health_checks: "[]".into(),
            join_token: Some(secret.to_string()),
            ..Default::default()
        },
        None,
    ))
    .await
    .map(|reply| reply.into_inner().node_token)
}

async fn cordon(svc: &ManagerDaemonService, admin: &str, node: &str) -> R<()> {
    svc.set_node_availability(authed(
        SetNodeAvailabilityRequest {
            node_id: node.into(),
            availability: "drained".into(),
            evict: false,
            idempotency_key: None,
        },
        Some(admin),
    ))
    .await?;
    Ok(())
}

async fn forget(
    svc: &ManagerDaemonService,
    admin: &str,
    node: &str,
    force: bool,
    key: Option<&str>,
) -> Result<ForgetNodeReply, tonic::Status> {
    svc.forget_node(authed(
        ForgetNodeRequest {
            node_id: node.into(),
            force,
            idempotency_key: key.map(str::to_string),
        },
        Some(admin),
    ))
    .await
    .map(|reply| reply.into_inner())
}

#[tokio::test]
async fn forget_guards_then_revokes_the_token_and_a_spent_token_refuses_a_rejoin_test() -> R<()> {
    let (svc, admin) = service()?;
    let (secret, node_token) = join(&svc, &admin, "n1", Some(1)).await?;

    let live = forget(&svc, &admin, "n1", false, None)
        .await
        .expect_err("a live, schedulable node is refused");
    assert_eq!(live.code(), Code::FailedPrecondition);
    assert!(
        live.message().contains("live and schedulable"),
        "{}",
        live.message()
    );

    // Work on it: refused even once cordoned.
    svc.run_job(authed(
        RunJobRequest {
            namespace: Some("default".into()),
            parsed_jobspec: json!({ "name": "web", "tasks": [{
                "name": "web", "runtime": "exec", "lifecycle": "service",
                "config": { "command": ["sleep", "600"], "user": "nobody" },
                "resources": { "memory": 64, "cpu": 100 },
            }] })
            .to_string(),
            ..Default::default()
        },
        Some(&admin),
    ))
    .await?;
    settle(&svc.store)?;
    assert_eq!(
        svc.store.allocs_for_node("n1")?.len(),
        1,
        "web placed on n1"
    );
    cordon(&svc, &admin, "n1").await?;
    let busy = forget(&svc, &admin, "n1", false, None)
        .await
        .expect_err("a node with allocations is refused");
    assert_eq!(busy.code(), Code::FailedPrecondition);
    assert!(
        busy.message()
            .contains("still has 1 allocation(s) (jobs web)"),
        "{}",
        busy.message()
    );

    // --force, retry-safe.
    let forced = forget(&svc, &admin, "n1", true, Some("forget-n1")).await?;
    assert_eq!(forced.message, "Forgot node n1");
    assert!(!forced.deduplicated);
    assert_eq!(forced.allocations_removed, 1);
    assert_eq!(forced.tokens_revoked, 1);
    assert!(svc.store.get_node("n1").is_err());
    assert!(svc.store.allocs_for_node("n1")?.is_empty());
    let again = forget(&svc, &admin, "n1", true, Some("forget-n1")).await?;
    assert!(again.deduplicated);
    assert_eq!(again.message, "Forgot node n1");
    let gone = forget(&svc, &admin, "n1", true, None)
        .await
        .expect_err("without a key, a repeat finds nothing");
    assert_eq!(gone.code(), Code::NotFound);

    // The node token no longer authenticates.
    let denied = svc
        .heartbeat(authed(
            HeartbeatRequest {
                id: "n1".into(),
                ..Default::default()
            },
            Some(&node_token),
        ))
        .await
        .expect_err("a forgotten node's token is revoked");
    assert_eq!(denied.code(), Code::Unauthenticated, "{}", denied.message());

    // The node group's single-use token is spent: the forgotten node is a
    // new node now, and cannot come back with it.
    let rejoin = register(&svc, "n1", &secret)
        .await
        .expect_err("a spent token refuses the forgotten node");
    assert_eq!(rejoin.code(), Code::PermissionDenied);
    assert!(rejoin.message().contains("spent"), "{}", rejoin.message());
    Ok(())
}

#[tokio::test]
async fn a_cordoned_empty_node_goes_and_node_local_volumes_need_force_test() -> R<()> {
    let (svc, admin) = service()?;
    join(&svc, &admin, "n2", None).await?;
    cordon(&svc, &admin, "n2").await?;
    let reply = forget(&svc, &admin, "n2", false, None).await?;
    assert_eq!(reply.allocations_removed, 0);
    assert_eq!(reply.tokens_revoked, 1);
    assert!(svc.store.get_node("n2").is_err());

    join(&svc, &admin, "n3", None).await?;
    cordon(&svc, &admin, "n3").await?;
    svc.query_state_store(authed(
        QueryStateStoreRequest {
            sql: "INSERT INTO volume (volume_id, driver, state, node_id, node_local, \
                  created_at, updated_at) VALUES ('vol-n3', 'host', 'created', 'n3', 1, \
                  '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
                .into(),
            allow_write: true,
        },
        Some(&admin),
    ))
    .await?;
    let refused = forget(&svc, &admin, "n3", false, None)
        .await
        .expect_err("node-local data needs --force");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains("1 node-local volume(s)"),
        "{}",
        refused.message()
    );
    let forced = forget(&svc, &admin, "n3", true, None).await?;
    assert_eq!(forced.volumes_orphaned, 1);
    Ok(())
}
