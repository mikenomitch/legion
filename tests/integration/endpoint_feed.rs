//! The endpoint feed end to end (mesh-control-path plan §6.1;
//! docs/service-discovery.md, "How endpoints reach a node"): a heartbeat
//! carries a node's endpoint sets as a delta against what it holds, a steady
//! beat carries none, and a backend that goes unhealthy reaches the node's
//! `StreamWork` stream as a push-only message without any heartbeat. The
//! backend node advertises an IPv6 address, so the set carries it unbracketed
//! for the worker to bracket.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::ManagerServer;
use legion::structs::{
    stream_work_request, AllocationHealth, EndpointFeed, EndpointFeedState, EndpointSetVersion,
    HeartbeatRequest, Node, NodeMsg, StreamWorkOpen, StreamWorkReply, StreamWorkRequest,
    Submission,
};

fn node(id: &str, worker_address: &str) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": id,
            "maxAllocations": 16,
            "resources": {},
            "workerAddress": worker_address,
        }),
        fingerprint_attrs: json!({
            "memory_total": (4u64 << 30).to_string(),
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

/// Submit a one-task job and return its task config id.
fn submit(store: &Store, name: &str, task: TaskConfig) -> R<i64> {
    store.handle_job_submit(
        &JobConfig {
            priority: Default::default(),
            name: name.to_string(),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        },
        &[Submission {
            path: format!("{name}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    let job_config_id = store.get_job_config(name)?.id();
    Ok(store.get_task_configs_for_job_config(&job_config_id)?[0].0)
}

fn health(allocation_id: i64, healthy: bool) -> AllocationHealth {
    AllocationHealth {
        allocation_id,
        healthy,
        status: if healthy { "healthy" } else { "failing" }.into(),
        artifact_states: None,
    }
}

/// What a worker that applied `feed` reports holding.
fn held_after(feed: &EndpointFeed) -> EndpointFeedState {
    EndpointFeedState {
        bindings_digest: feed.bindings_digest.clone(),
        sets: feed
            .sets
            .iter()
            .map(|set| EndpointSetVersion {
                service: set.service.clone(),
                consumer: set.consumer.clone(),
                epoch: feed.epoch,
                version: set.version,
                digest: set.digest,
            })
            .collect(),
    }
}

#[test]
fn a_backend_change_reaches_a_streaming_worker_without_a_heartbeat() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    store.insert_node(NodeMsg::from(node("w-web", "http://10.0.0.8:4848")))?;
    store.insert_node(NodeMsg::from(node("w-db", "http://[fd00::9]:4848")))?;

    let mut db = TaskConfig::new("db", "noop");
    db.service = Some(serde_json::from_value(json!({ "port": 15432 }))?);
    let db_task = submit(&store, "db", db)?;
    let backend = store.place_alloc("w-db", db_task)?;
    store.upsert_allocation_health_snapshots("w-db", &[health(backend.id, true)])?;
    let mut web = TaskConfig::new("web", "noop");
    web.connect = Some(serde_json::from_value(
        json!({ "upstreams": [{ "to": "db" }] }),
    )?);
    let web_task = submit(&store, "web", web)?;
    store.place_alloc("w-web", web_task)?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let addr: SocketAddr = listener.local_addr()?;
    let service = ManagerServer::new(ManagerDaemonService::new(store.clone()));
    rt.spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    // What `run_manager` runs on every manager.
    rt.spawn(legion::manager::server::endpoint_feed::run_endpoint_refresher(store.clone()));
    let client = rt.block_on(ManagerClient::connect(format!("http://{addr}")))?;
    let beat = |held: Option<EndpointFeedState>| -> R<EndpointFeed> {
        rt.block_on(client.clone().heartbeat(HeartbeatRequest {
            id: "w-web".into(),
            endpoint_feed: held,
            ..Default::default()
        }))?
        .into_inner()
        .endpoint_feed
        .context("the reply carries a feed")
    };

    // The stream the pushes arrive on.
    let (requests, rx) = tokio::sync::mpsc::channel::<StreamWorkRequest>(8);
    rt.block_on(requests.send(StreamWorkRequest {
        frame: Some(stream_work_request::Frame::Open(StreamWorkOpen {
            node_id: "w-web".into(),
            last_epoch: 0,
            last_seq: 0,
        })),
    }))?;
    let mut inbound = rt
        .block_on(
            client
                .clone()
                .stream_work(tokio_stream::wrappers::ReceiverStream::new(rx)),
        )?
        .into_inner();
    let (forward, received) = std::sync::mpsc::channel::<StreamWorkReply>();
    rt.spawn(async move {
        while let Ok(Some(message)) = inbound.message().await {
            if forward.send(message).is_err() {
                break;
            }
        }
    });

    // The first beat: the binding and the one set, with the backend's IPv6
    // address as the catalog holds it.
    let first = beat(None)?;
    assert!(!first.bindings_unchanged);
    assert_eq!(first.bindings.len(), 1);
    assert_eq!(first.bindings[0].upstream, "db");
    assert_eq!(first.sets.len(), 1);
    let set = &first.sets[0];
    assert_eq!((set.service.as_str(), set.consumer.as_str()), ("db", "web"));
    assert_eq!(set.endpoints.len(), 1);
    assert_eq!(set.endpoints[0].address, "fd00::9");
    assert_eq!(set.endpoints[0].port, 15432);

    // A steady beat ships nothing.
    let steady = beat(Some(held_after(&first)))?;
    assert!(steady.bindings_unchanged && steady.bindings.is_empty());
    assert!(steady.sets.is_empty(), "{steady:?}");

    // The backend fails its check. No heartbeat runs from here: the change
    // reaches the stream as a push.
    let failed_at = Instant::now();
    store.upsert_allocation_health_snapshots("w-db", &[health(backend.id, false)])?;
    let push = loop {
        let message = received
            .recv_timeout(Duration::from_secs(10))
            .context("a push before the timeout")?;
        if message.push_only {
            break message;
        }
    };
    let feed = push.endpoint_feed.context("a push carries a feed")?;
    assert!(feed.bindings_unchanged);
    assert_eq!(feed.sets.len(), 1);
    assert_eq!(feed.sets[0].service, "db");
    assert!(feed.sets[0].endpoints.is_empty(), "the failed backend left");
    assert!(feed.sets[0].version > set.version);
    assert!(
        failed_at.elapsed() < Duration::from_secs(5),
        "pushed in {:?}",
        failed_at.elapsed()
    );

    // The heartbeat stays the record: a worker that missed the push gets the
    // same set on its next beat.
    let record = beat(Some(held_after(&first)))?;
    assert_eq!(record.sets.len(), 1);
    assert_eq!(record.sets[0].digest, feed.sets[0].digest);
    Ok(())
}
