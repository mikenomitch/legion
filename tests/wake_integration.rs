//! Wake on request end to end against a real manager (docs/gateway.md): a
//! keyed instance with `suspend.wake` is suspended, a request reaches the
//! gateway, the gateway resumes it through StartInstance, and the held
//! request is answered by the resumed instance.
//!
//! The heartbeat stands in for the worker, as in `suspend_integration.rs`:
//! after the resume is placed, a background task reports the restore and
//! binds the instance's advertised port, which is when the gateway's dial
//! succeeds.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Duration;

use legion::gateway::{Control, Gateway, InstanceView, WakeView};
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::network::{NetworkConfig, PortConfig};
use legion::structs::suspend::{SuspendConfig, WakeConfig};
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::StatefulStorageConfig,
    job::TaskConfig, GetInstanceRequest, HeartbeatRequest, InstanceInfo, InstanceMode, Node,
    NodeMsg, SnapshotReport, StartInstanceRequest, Submission, SuspendInstanceRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Request, Status};

const NODE: &str = "wake-node";
const JOB: &str = "workspaces";
const TASK: &str = "workspace";
const ID: &str = "user-7";

struct Cluster {
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-wake-it-")
            .tempdir()?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            svc: ManagerDaemonService::new(store),
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self) -> R<()> {
        self.store().insert_node(NodeMsg::from(Node {
            id: NODE.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": NODE,
                "maxAllocations": 20,
                "resources": { "memory": 65536, "cpu": 40000 },
                "workerAddress": "http://127.0.0.1:4749",
            }),
            fingerprint_attrs: json!({
                "arch": "x86_64",
                "memory_total": "68719476736",
                "disk_total_space": "214748364800",
                "disk_available_space": "107374182400",
                "cpus": { "cpu-1": { "frequency": 2000.0 } },
                "volumeDrivers": {
                    "local-dir": {
                        "apiVersion": 1, "name": "local-dir", "nodeLocal": true,
                        "ephemeral": false, "accessModes": ["single-writer"], "available": true,
                        "ops": ["snapshot", "restore", "clone", "snapshot-delete"]
                    }
                }
            }),
            dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": ["checkpoint"] }),
            health_checks: json!({}),
        }))?;
        Ok(())
    }

    fn submit(&self, config: JobConfig) -> R<()> {
        self.store().handle_job_submit(
            &config,
            &[Submission {
                path: format!("{}.json", config.name),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    fn reschedule(&self) -> R<()> {
        let config_id = self.store().get_job_config(JOB)?.id();
        self.store()
            .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        while let Some(eval) = self.store().get_next_eval()? {
            let outcome = eval.handle(self.store());
            self.store().set_eval_status(
                &eval.id,
                if outcome.is_ok() {
                    "complete"
                } else {
                    "failed"
                },
            )?;
        }
        Ok(())
    }

    async fn report(&self, report: SnapshotReport) -> R<()> {
        self.svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: NODE.into(),
                snapshot_reports: vec![report],
                ..Default::default()
            }))
            .await?;
        Ok(())
    }

    async fn info(&self) -> R<InstanceInfo> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: ID.into(),
            }))
            .await?
            .into_inner()
            .instance
            .context("instance")
    }
}

fn job() -> JobConfig {
    let task = TaskConfig {
        config: json!({ "command": ["sleep", "60"] }),
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            require_id: false,
            initial_instances: None,
            on_lost: None,
            storage: Some(StatefulStorageConfig {
                path: "/workspace".into(),
                driver: Some("local-dir".into()),
                mode: None,
                params: None,
                retain_on_scale_in: None,
                retain_on_stop: None,
            }),
            envelope: None,
            promote: None,
            placement: None,
            fence_after: None,
        }),
        network: Some(NetworkConfig {
            ports: vec![PortConfig {
                name: "http".into(),
                static_port: None,
                to: None,
            }],
            ..Default::default()
        }),
        suspend: Some(SuspendConfig {
            wake: Some(WakeConfig {
                port: "http".into(),
                timeout: Some("15s".into()),
            }),
            ..Default::default()
        }),
        ..TaskConfig::new(TASK, "exec")
    };
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn manifest(snapshot_id: &str, allocation_id: i64) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originInstance": ID,
        "originAllocId": allocation_id,
        "runtime": "exec",
        "kind": "disk",
        "createdAt": "2026-10-12T00:00:00Z",
    })
    .to_string()
}

/// The gateway's view of the manager, called in process. `start` also
/// plays the worker: it places the resume, reports the restore, and binds
/// the advertised port with an origin that echoes the request line.
struct InProcess {
    cluster: Arc<Cluster>,
}

#[tonic::async_trait]
impl Control for InProcess {
    async fn get(&self, instance_id: &str) -> Result<Option<InstanceView>, Status> {
        let info = match self
            .cluster
            .svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: instance_id.into(),
            }))
            .await
        {
            Ok(reply) => reply.into_inner().instance,
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        Ok(info.map(|info| InstanceView {
            state: info.state,
            wake: info.wake.map(|wake| WakeView {
                port: wake.port,
                timeout: Duration::from_millis(wake.timeout_ms),
            }),
            endpoints: info
                .endpoints
                .into_iter()
                .map(|endpoint| (endpoint.port, endpoint.address, endpoint.number as u16))
                .collect(),
        }))
    }

    async fn start(&self, instance_id: &str) -> Result<(), Status> {
        let reply = self
            .cluster
            .svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: instance_id.into(),
                idempotency_key: None,
                cold: false,
                force_move: false,
            }))
            .await?
            .into_inner();
        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            // The resume takes a moment to place and restore.
            tokio::time::sleep(Duration::from_millis(300)).await;
            cluster.reschedule().expect("placed");
            let info = cluster.info().await.expect("info");
            let allocation_id = info.current_alloc_id.expect("resumed allocation");
            cluster
                .report(SnapshotReport {
                    snapshot_id: reply.resuming_from.clone(),
                    op: "restore".into(),
                    ok: true,
                    duration_ms: 90,
                    allocation_id,
                    ..Default::default()
                })
                .await
                .expect("restore report");
            let info = cluster.info().await.expect("info");
            let port = info
                .endpoints
                .iter()
                .find(|endpoint| endpoint.port == "http")
                .expect("an http endpoint once active")
                .number;
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port as u16))
                .await
                .expect("bind the advertised port");
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1024];
                    let read = stream.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..read]).to_string();
                    let line = head.lines().next().unwrap_or_default().to_string();
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{line}",
                                line.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_resumes_a_suspended_instance_and_is_answered() -> R<()> {
    let cluster = Arc::new(Cluster::new()?);
    cluster.add_node()?;
    cluster.submit(job())?;
    cluster
        .store()
        .create_keyed_instance(JOB, "default", TASK, Some(ID))?;
    cluster.reschedule()?;

    // Running: GetInstance names the wake port and where it is served.
    let running = cluster.info().await?;
    let wake = running.wake.clone().context("wake block")?;
    assert_eq!(wake.port, "http");
    assert_eq!(wake.timeout_ms, 15_000);
    let endpoint = running
        .endpoints
        .iter()
        .find(|endpoint| endpoint.port == "http")
        .context("http endpoint while running")?;
    assert_eq!(endpoint.address, "127.0.0.1");
    assert!(endpoint.number > 0);

    // Suspend it, as the idle policy would.
    let allocation_id = running.current_alloc_id.context("allocation")?;
    let suspend = cluster
        .svc
        .suspend_instance(Request::new(SuspendInstanceRequest {
            instance_id: ID.into(),
            checkpoint: None,
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    cluster
        .report(SnapshotReport {
            snapshot_id: suspend.snapshot_id.clone(),
            op: "capture".into(),
            ok: true,
            manifest_json: manifest(&suspend.snapshot_id, allocation_id),
            duration_ms: 120,
            allocation_id,
            ..Default::default()
        })
        .await?;
    let suspended = cluster.info().await?;
    assert_eq!(suspended.state, "suspended");
    assert!(
        suspended.endpoints.is_empty(),
        "nothing serves while suspended"
    );
    assert!(suspended.wake.is_some(), "still routable: it can be woken");

    // A request arrives at the gateway: it resumes the instance, holds the
    // request, and hands back a connection to the resumed port.
    let gateway = Gateway::new(
        InProcess {
            cluster: cluster.clone(),
        },
        Some("gw.test".into()),
    );
    let (mut stream, resolved) = gateway
        .connect(ID, None)
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
    assert!(resolved.woke);
    assert_eq!(resolved.port, "http");
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: user-7.gw.test\r\n\r\n")
        .await?;
    let mut response = vec![0u8; 1024];
    let read = stream.read(&mut response).await?;
    let response = String::from_utf8_lossy(&response[..read]);
    assert!(response.ends_with("GET /hello HTTP/1.1"), "{response}");

    let resumed = cluster.info().await?;
    assert_eq!(resumed.state, "active");
    assert_eq!(
        resumed.restored_from_snapshot_id.as_deref(),
        Some(suspend.snapshot_id.as_str())
    );
    Ok(())
}
