//! Join tokens for node groups (autoscaler v2 plan §6.11;
//! docs/auth-and-acls.md, "Node attributes from the join"):
//!
//! - `max_uses` admits that many new nodes, and a node re-registering under
//!   its own name is not a new use;
//! - a token's `attributes` are stamped on every node that joins with it,
//!   win over the host's `--attr` and the worker type's attributes, and
//!   survive restarts that re-register with the node token;
//! - reserved `legion.*` keys cannot be spoofed: `--attr` may set only the
//!   per-host ones, and a key the worker reports that nothing vouches for is
//!   dropped;
//! - with ACLs off, a presented token still stamps and still counts.

use crate::support::{acl_service, authed, temp_store};
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::structs::manager_server::Manager;
use legion::structs::token::JoinToken;
use legion::structs::{
    CreateJoinTokenRequest, ListJoinTokensRequest, WorkerConfig, WorkerRegisterReply,
    WorkerRegisterRequest,
};
use std::collections::HashMap;
use tonic::Code;

fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// Mint a token through the RPC and return (id, secret).
async fn mint(
    svc: &ManagerDaemonService,
    admin: Option<&str>,
    worker_type: &str,
    max_uses: Option<u32>,
    attributes: HashMap<String, String>,
) -> Result<(String, String), tonic::Status> {
    let reply = svc
        .create_join_token(authed(
            CreateJoinTokenRequest {
                worker_type: worker_type.into(),
                ttl: Some("1h".into()),
                description: "node group test".into(),
                max_uses,
                attributes,
            },
            admin,
        ))
        .await?
        .into_inner();
    let secret = JoinToken::decode(&reply.token)
        .map_err(|err| tonic::Status::internal(err.to_string()))?
        .secret;
    Ok((reply.info.unwrap_or_default().id, secret))
}

/// Register `name` the way a worker does: the join secret in the body, or
/// (`node_token`) the node's own token as the bearer credential.
async fn register(
    svc: &ManagerDaemonService,
    name: &str,
    pool: &str,
    join_secret: Option<&str>,
    node_token: Option<&str>,
    reported: Value,
    host: HashMap<String, String>,
) -> Result<WorkerRegisterReply, tonic::Status> {
    svc.worker_register(authed(
        WorkerRegisterRequest {
            name: name.into(),
            pool: pool.into(),
            config_attrs: reported.to_string(),
            fingerprint_attrs: "{}".into(),
            health_checks: "[]".into(),
            join_token: join_secret.map(str::to_string),
            join_attributes: host,
            ..Default::default()
        },
        node_token,
    ))
    .await
    .map(|reply| reply.into_inner())
}

fn node_attrs(svc: &ManagerDaemonService, name: &str) -> R<Value> {
    Ok(svc.store.get_node(name)?.config_attrs)
}

#[tokio::test]
async fn max_uses_admits_that_many_new_nodes_test() -> R<()> {
    let (svc, admin) = acl_service("legion-join-tokens-")?;
    svc.store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    let (id, secret) = mint(&svc, Some(&admin), "default", Some(2), HashMap::new()).await?;

    register(
        &svc,
        "n1",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await?;
    let second = register(
        &svc,
        "n2",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await?;
    let spent = register(
        &svc,
        "n3",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await
    .expect_err("a third node is past max_uses");
    assert_eq!(spent.code(), Code::PermissionDenied);
    assert!(
        spent.message().contains("is spent: 2 of its 2 uses"),
        "{}",
        spent.message()
    );
    assert!(svc.store.get_node("n3").is_err(), "nothing was registered");

    // A node that went lost and rejoins under its own name with the token
    // (a worker that lost its disk) is the node the token introduced, not
    // a new use; a restart with the node token never touches the token.
    svc.store.mark_nodes_as_lost(&["n1".to_string()])?;
    register(
        &svc,
        "n1",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await?;
    let node_token = second.node_token.context("a node token")?;
    register(
        &svc,
        "n2",
        "default",
        None,
        Some(&node_token),
        json!({}),
        HashMap::new(),
    )
    .await?;

    let tokens = svc
        .list_join_tokens(authed(ListJoinTokensRequest {}, Some(&admin)))
        .await?
        .into_inner()
        .tokens;
    let info = tokens.iter().find(|t| t.id == id).context("the token")?;
    assert_eq!((info.uses, info.max_uses), (2, Some(2)));

    let zero = mint(&svc, Some(&admin), "default", Some(0), HashMap::new())
        .await
        .expect_err("max_uses 0 is refused");
    assert_eq!(zero.code(), Code::InvalidArgument);
    Ok(())
}

#[tokio::test]
async fn token_attributes_win_and_reserved_keys_cannot_be_spoofed_test() -> R<()> {
    let (svc, admin) = acl_service("legion-join-tokens-")?;
    svc.store.insert_worker_type(&WorkerConfig {
        name: "group".into(),
        pool: "default".into(),
        attributes: [
            ("team", "type-team"),
            ("rack", "type-rack"),
            ("zone", "type-zone"),
            ("legion.scale-in", "never"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
        ..Default::default()
    })?;
    let (id, secret) = mint(
        &svc,
        Some(&admin),
        "group",
        None,
        attrs(&[
            ("legion.nodegroup", "workers-general"),
            ("team", "token-team"),
        ]),
    )
    .await?;

    // The worker reports its type's attributes, plus two it made up.
    let reported = json!({
        "team": "type-team", "rack": "type-rack", "zone": "type-zone",
        "legion.scale-in": "never",
        "legion.nodegroup": "gpu-expensive",
        "legion.made-up": "x",
    });
    let host = attrs(&[
        ("rack", "r12"),
        ("team", "host-team"),
        ("legion.provider-id", "i-0abc"),
    ]);
    let reply = register(
        &svc,
        "w1",
        "default",
        Some(&secret),
        None,
        reported,
        host.clone(),
    )
    .await?;
    let node = node_attrs(&svc, "w1")?;
    assert_eq!(
        node["legion.nodegroup"], "workers-general",
        "the token wins: {node}"
    );
    assert_eq!(node["team"], "token-team", "token over --attr: {node}");
    assert_eq!(node["rack"], "r12", "--attr over the worker type: {node}");
    assert_eq!(node["zone"], "type-zone", "{node}");
    assert_eq!(
        node["legion.provider-id"], "i-0abc",
        "a per-host key: {node}"
    );
    assert_eq!(
        node["legion.scale-in"], "never",
        "the worker type vouches: {node}"
    );
    assert!(
        node.get("legion.made-up").is_none(),
        "nothing vouches: {node}"
    );
    assert_eq!(node["legion.join"]["token"], id.as_str(), "{node}");
    assert_eq!(
        node["legion.join"]["attributes"]["legion.nodegroup"],
        "workers-general"
    );

    // A restart with the node token, reporting a forged group: what the
    // token stamped survives, the forgery does not.
    let node_token = reply.node_token.context("a node token")?;
    register(
        &svc,
        "w1",
        "default",
        None,
        Some(&node_token),
        json!({ "team": "type-team", "legion.nodegroup": "gpu-expensive" }),
        host,
    )
    .await?;
    let node = node_attrs(&svc, "w1")?;
    assert_eq!(node["legion.nodegroup"], "workers-general", "{node}");
    assert_eq!(node["team"], "token-team", "{node}");
    assert_eq!(node["legion.provider-id"], "i-0abc", "{node}");

    // `--attr` may not set a reserved key, or structure.
    for (key, value) in [
        ("legion.nodegroup", "gpu-expensive"),
        ("legion.scale-in", "never"),
        ("namespaces", "*"),
    ] {
        let refused = register(
            &svc,
            "w2",
            "default",
            Some(&secret),
            None,
            json!({}),
            attrs(&[(key, value)]),
        )
        .await
        .expect_err("a reserved --attr is refused");
        assert_eq!(refused.code(), Code::InvalidArgument, "{key}");
    }
    // Nor may a token set structure or Legion's own records.
    for (key, value) in [
        ("namespaces", "*"),
        ("legion.role", "manager"),
        ("legion.join", "{}"),
    ] {
        let refused = mint(&svc, Some(&admin), "group", None, attrs(&[(key, value)]))
            .await
            .expect_err("a structural token attribute is refused");
        assert_eq!(refused.code(), Code::InvalidArgument, "{key}");
    }
    Ok(())
}

#[tokio::test]
async fn with_acls_off_a_presented_token_still_stamps_and_counts_test() -> R<()> {
    let svc = ManagerDaemonService::new(temp_store("legion-join-tokens-")?);
    svc.store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    let (_, secret) = mint(
        &svc,
        None,
        "default",
        Some(1),
        attrs(&[("legion.nodegroup", "edge")]),
    )
    .await?;
    register(
        &svc,
        "e1",
        "default",
        Some(&secret),
        None,
        json!({}),
        attrs(&[("rack", "r1")]),
    )
    .await?;
    let node = node_attrs(&svc, "e1")?;
    assert_eq!(node["legion.nodegroup"], "edge", "{node}");
    assert_eq!(node["rack"], "r1", "{node}");
    // A worker restarting without node tokens (ACLs off) presents the join
    // token again: the same node, not a new use.
    register(
        &svc,
        "e1",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await?;
    let spent = register(
        &svc,
        "e2",
        "default",
        Some(&secret),
        None,
        json!({}),
        HashMap::new(),
    )
    .await
    .expect_err("max_uses holds with ACLs off too");
    assert_eq!(spent.code(), Code::PermissionDenied);
    // A registration presenting no token is not limited (ACLs off).
    register(&svc, "e3", "default", None, None, json!({}), HashMap::new()).await?;
    Ok(())
}
