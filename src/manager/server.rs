use crate::cmd::structs::{ManagerConfig, WorkerConfig};
use crate::constants::{DEFAULT_HOSTNAME, DEFAULT_PORT, NODE_CHECK_DURATION};
use crate::storage::store::Store;
use crate::util::R;

use jsonnet::JsonnetVm;

use log::{error, warn};
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;

use futures::join;
use serde_json::json;

use crate::manager::join_server::get_insecure_manager_service;
use crate::structs::manager_server::{Manager, ManagerServer};

use crate::structs::{
    GetJobStatusReply, GetJobStatusRequest, GetNodeReply, GetNodeRequest, GetNodesReply,
    GetNodesRequest, HeartbeatReply, HeartbeatRequest, Node, RunJobReply, RunJobRequest,
    UpdateNodeDynamicReply, UpdateNodeDynamicRequest, WorkerConfig as WorkerConfigMsg,
    WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterReply, WorkerRegisterRequest, HealthCheckRequest, HealthCheckReply,
};

use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

#[allow(dead_code)]
// #[derive(Default)]
pub struct ManagerAgentService {
    store: Store,
}

#[tonic::async_trait]
impl Manager for ManagerAgentService {
    async fn run_job(
        &self,
        _request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {
        let reply = RunJobReply {
            message: format!("Hello {}!", "jorb"),
        };

        Ok(Response::new(reply))
    }

    async fn get_job_status(
        &self,
        _request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusReply>, Status> {
        let reply = GetJobStatusReply {
            message: format!("Hello {}!", "status"),
        };

        Ok(Response::new(reply))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let id = request.into_inner().id;

        self.store.update_node_heartbeat(id.clone()).map_err(|e| {
            error!("Error updading node heartbeat. Node ID: {}: {:?}", id, e);
            Status::internal("Error updading node heartbeat")
        })?;

        let reply = HeartbeatReply {
            message: format!("ack"),
        };

        Ok(Response::new(reply))
    }

    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        let req = request.into_inner();
        let id = req.id;

        self.store.update_node_health_checks(id.clone(), req.health_checks).map_err(|e| {
            error!("Error updading node health checks. Node ID: {}: {:?}", id, e);
            Status::internal("Error updading node health checks")
        })?;

        let reply = HealthCheckReply {
            message: format!("ack"),
        };

        Ok(Response::new(reply))
    }

    async fn worker_pre_register(
        &self,
        request: Request<WorkerPreRegisterRequest>,
    ) -> Result<Response<WorkerPreRegisterReply>, Status> {
        let request = request.into_inner();
        let worker_type = request.worker_type;

        let worker_config = self
            .store
            .get_worker_type(worker_type.clone())
            .map_err(|e| {
                error!("Error getting worker type {}: {:?}", worker_type.clone(), e);
                Status::internal("Error getting worker type")
            })?;

        let mut attrs = HashMap::new();
        for (key, value) in worker_config.attributes.iter() {
            match serde_json::to_string(value).ok() {
                Some(val_str) => {
                    attrs.insert(key.to_string(), val_str);
                }
                None => {
                    error!("Error serializing value for key: {}", key);
                }
            }
        }

        let config = WorkerConfigMsg {
            name: worker_type,
            pool: worker_config.pool,
            additional_node_checks: worker_config.additional_node_checks,
            attributes: worker_config.attributes,
            name_prefix: worker_config.name_prefix,
        };

        let reply = WorkerPreRegisterReply {
            config: Some(config),
        };

        Ok(Response::new(reply))
    }

    async fn worker_register(
        &self,
        request: Request<WorkerRegisterRequest>,
    ) -> Result<Response<WorkerRegisterReply>, Status> {
        let request = request.into_inner();
        let dynamic_attrs = json!({}).to_string();

        let node = Node {
            id: request.name,
            fingerprint_attrs: request.fingerprint_attrs,
            config_attrs: request.config_attrs,
            dynamic_attrs: dynamic_attrs,
            status: "active".to_string(),
            availability: "availabile".to_string(),
            pool: request.pool,
            last_heartbeat: "".to_string(),
            health_checks: request.health_checks,
        };

        self.store.insert_node(node.clone()).map_err(|e| {
            error!("Error registering worker: {:?}", e);
            Status::internal("Error registering worker")
        })?;

        let reply = WorkerRegisterReply {
            message: format!("You are in!"),
        };

        Ok(Response::new(reply))
    }

    async fn get_nodes(
        &self,
        _request: Request<GetNodesRequest>,
    ) -> Result<Response<GetNodesReply>, Status> {
        let nodes = self.store.get_nodes().map_err(|e| {
            error!("Error getting nodes: {:?}", e);
            Status::internal("Error getting nodes")
        })?;

        let reply = GetNodesReply { nodes: nodes };
        Ok(Response::new(reply))
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeReply>, Status> {
        let id = request.into_inner().id;
        let node = self
            .store
            .get_node(id)
            .expect("Could not get node with given id");

        let reply = GetNodeReply { node: Some(node) };
        Ok(Response::new(reply))
    }

    async fn update_node_dynamic(
        &self,
        request: Request<UpdateNodeDynamicRequest>,
    ) -> Result<Response<UpdateNodeDynamicReply>, Status> {
        let r = request.into_inner();
        let id = r.id;
        let dynamic_diff = r.dynamic_diff;
        let node = self
            .store
            .update_node_dyamic(id, dynamic_diff)
            .expect("Can't find or update node");
        let reply = UpdateNodeDynamicReply { node: Some(node) };

        Ok(Response::new(reply))
    }
}

pub async fn run_manager(config: ManagerConfig) -> R<()> {
    let addr = format!("{}:{}", DEFAULT_HOSTNAME, DEFAULT_PORT).parse()?;
    let cloned_config = config.clone();
    let store = Store::start_storage()?;

    load_initial_data(&store, &config).await?;

    // Spawn a separate grpc server for non-TLS calls
    let join_server_addr = format!(
        "{}:{}",
        cloned_config.server.address, cloned_config.server.insecure_port
    )
    .parse()?;

    let insecure_manager_service = get_insecure_manager_service();
    let join_server_fut = Server::builder()
        .add_service(insecure_manager_service)
        .serve(join_server_addr);

    let s2 = Store {
        conn: store.conn.clone(),
    };

    let manager_node = ManagerAgentService { store: store };

    let data_dir = std::path::PathBuf::from_str("/Users/mike/code/legion/configs/tls/server")?;
    let cert = std::fs::read_to_string(data_dir.join("server.pem"))?;
    let key = std::fs::read_to_string(data_dir.join("server.key"))?;
    let identity = Identity::from_pem(cert, key);
    let _tls = ServerTlsConfig::new().identity(identity);

    let start_server_fut = Server::builder()
        // .tls_config(tls)?
        .add_service(ManagerServer::new(manager_node))
        .serve(addr);

    println!("Join server starting on {}", join_server_addr);
    println!("Manager node starting on {}", addr);

    let node_checker_fut = start_node_status_checker(s2);

    let _res = join!(join_server_fut, start_server_fut, node_checker_fut);

    Ok(())
}

async fn start_node_status_checker(store: Store) -> R<()> {
    loop {
        let node_ids_to_update = store
            .get_node_ids_and_heartbeats()?
            .into_iter()
            .filter(
                |(_id, heartbeat)| match Node::last_heartbeat_too_slow(heartbeat) {
                    Ok(too_slow) => too_slow,
                    Err(e) => {
                        warn!("Error checking heartbeat - marking lost: {:?}", e);
                        true
                    }
                },
            )
            .map(|(id, _heartbeat)| id)
            .collect::<Vec<String>>();

        store.mark_nodes_as_lost(node_ids_to_update)?;
        tokio::time::sleep(tokio::time::Duration::from_secs(NODE_CHECK_DURATION)).await;
    }
}

fn parse_worker_config(config_path: &str) -> R<WorkerConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: WorkerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(e) => {
            error!("Error parsing worker config: {:?}", e);
            Err(e.to_string().into())
        },
    }
}

async fn load_initial_data(store: &Store, config: &ManagerConfig) -> R<()> {
    let paths = fs::read_dir(config.worker_config_path.clone())?;

    for path in paths {
        let path = path.unwrap().path();
        let path_str = path.to_str().unwrap();
        let wcp = parse_worker_config(path_str)?;

        let af = wcp.additional_node_checks.unwrap_or([].to_vec());
        let attrs = wcp.attributes.unwrap_or(HashMap::new());

        let wc = WorkerConfigMsg {
            name: wcp.worker_type.clone(),
            pool: wcp.pool.unwrap_or("default".into()),
            additional_node_checks: af,
            attributes: attrs,
            name_prefix: wcp.name_prefix,
        };

        store.insert_worker_type(wc)?;
    }

    Ok(())
}
