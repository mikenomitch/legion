use crate::cmd::structs::{AgentConfig, JoinTokenClaims};
use crate::constants::{DEFAULT_HOSTNAME, DEFAULT_PORT, TMP_SECRET_KEY, NODE_CHECK_DURATION};
use crate::storage::store::Store;
use crate::util::R;

use std::str::FromStr;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde_json::json;
use futures::join;

use crate::manager::join_server::get_insecure_manager_service;
use crate::structs::manager_server::{Manager, ManagerServer};

use crate::structs::{
    GetJobStatusReply, GetJobStatusRequest, GetNodesReply, GetNodesRequest, HeartbeatReply,
    HeartbeatRequest,RunJobReply, RunJobRequest,
    UpdateNodeDynamicReply, UpdateNodeDynamicRequest, WorkerJoinReply, WorkerJoinRequest,
    WorkerRegisterReply, WorkerRegisterRequest, GetNodeReply, GetNodeRequest, Node
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

    // NOTE: Make this work for servers and workers
    async fn worker_join(
        &self,
        request: Request<WorkerJoinRequest>,
    ) -> Result<Response<WorkerJoinReply>, Status> {
        let token = request.into_inner().token;
        let token_data = match decode::<JoinTokenClaims>(
            &token,
            &DecodingKey::from_secret(&TMP_SECRET_KEY[..]),
            &Validation::new(Algorithm::HS512),
        ) {
            Ok(c) => c,
            Err(err) => match *err.kind() {
                ErrorKind::InvalidToken => panic!(), // Example on how to handle a specific error
                _ => panic!(),
            },
        };

        let reply = WorkerJoinReply {
            message: format!("Joined Yay?: {:?}!", token_data.claims),
        };

        Ok(Response::new(reply))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let id = request.into_inner().id;

        self.store.update_node_heartbeat(id).unwrap();

        let reply = HeartbeatReply {
            message: format!("ack"),
        };
        Ok(Response::new(reply))
    }

    async fn worker_register(
        &self,
        request: Request<WorkerRegisterRequest>,
    ) -> Result<Response<WorkerRegisterReply>, Status> {
        let request = request.into_inner();
        let name = request.name;
        let attrs = request.attributes;
        let dynamic = json!({}).to_string(); //

        let node = Node {
            id: name,
            attrs: attrs.clone(),
            dynamic: dynamic.clone(),
            status: "active".to_string(),
            last_heartbeat: "".to_string(),
        };

        self.store.insert_node(node.clone()).unwrap();
        let nodes = self.store.get_nodes().unwrap();

        for node in nodes {
            println!("Found node {:?}", node.id);
            println!("With attrs: {:?}", node.attrs);
        }

        let reply = WorkerRegisterReply {
            message: format!("You are in!"),
        };

        Ok(Response::new(reply))
    }

    async fn get_nodes(
        &self,
        _request: Request<GetNodesRequest>,
    ) -> Result<Response<GetNodesReply>, Status> {
        let nodes = self.store.get_nodes().unwrap();
        let reply = GetNodesReply { nodes: nodes };
        Ok(Response::new(reply))
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeReply>, Status> {
        let id = request.into_inner().id;
        let node = self.store.get_node(id)
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
        let node = self.store.update_node_dyamic(id, dynamic_diff).expect("Can't find or update node");
        let reply = UpdateNodeDynamicReply { node: Some(node) };

        Ok(Response::new(reply))
    }
}

pub async fn run_manager(config: AgentConfig) -> R<()> {
    let addr = format!("{}:{}", DEFAULT_HOSTNAME, DEFAULT_PORT).parse()?;
    let cloned_config = config.clone();
    let store = Store::start_storage()?;

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

    let s2 = Store{
        conn: store.conn.clone()
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
        let node_ids_to_update = store.get_node_ids_and_heartbeats()?
            .into_iter()
            .filter( |(_id, heartbeat)| {
                match Node::last_heartbeat_too_slow(heartbeat) {
                    Ok(too_slow) => too_slow,
                    Err(e) => {
                        println!("Error checking heartbeat - marking lost: {:?}", e);
                        true
                    }
                }
            })
            .map(|(id, _heartbeat)| id)
            .collect::<Vec<String>>();

        store.mark_nodes_as_lost(node_ids_to_update)?;
        tokio::time::sleep(tokio::time::Duration::from_secs(NODE_CHECK_DURATION)).await;
    }
}
