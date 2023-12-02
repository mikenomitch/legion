use crate::cmd::structs::{AgentConfig, JoinTokenClaims};
use crate::constants::{DEFAULT_HOSTNAME, DEFAULT_PORT, TMP_SECRET_KEY};
use crate::storage::node::{start_storage, Msg};
use crate::util::R;

use log::debug;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, Sender};

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use std::sync::{Arc, Mutex};

use futures::join;

pub mod legion {
    tonic::include_proto!("legion");
}

use crate::manager::join_server::get_insecure_manager_service;
use legion::manager_client::ManagerClient;
use legion::manager_server::{Manager, ManagerServer};

use legion::{
    GetJobStatusReply, GetJobStatusRequest, HeartbeatReply, HeartbeatRequest, ManagerJoinReply,
    ManagerJoinRequest, RunJobReply, RunJobRequest, WorkerJoinReply, WorkerJoinRequest,
};

use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

#[allow(dead_code)]
// #[derive(Default)]
pub struct ManagerAgentService {
    sender_to_store: Arc<Mutex<Sender<Msg>>>,
    receiver_from_store: Arc<Mutex<Receiver<String>>>,
}

#[tonic::async_trait]
impl Manager for ManagerAgentService {
    // NOTE: Deprecate this once the join token is used for everything
    async fn manager_join(
        &self,
        request: Request<ManagerJoinRequest>,
    ) -> Result<Response<ManagerJoinReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        // let to_store = self.sender_to_store.lock()?;
        // to_store.send(Msg::Content("LET ME IN".to_string()))?;

        let reply = ManagerJoinReply {
            message: format!("Hello {}!", request.into_inner().address),
        };

        Ok(Response::new(reply))
    }

    async fn run_job(
        &self,
        _request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {
        // let to_store = self.sender_to_store.lock()?;
        // to_store.send(Msg::Content("ADD THE JOB".to_string()))?;

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
        println!("{:?}", token_data.claims);
        println!("{:?}", token_data.header);

        let reply = WorkerJoinReply {
            message: format!("Joined Yay?: {:?}!", token_data.claims),
        };

        Ok(Response::new(reply))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let reply = HeartbeatReply {
            message: format!("Thunky thunk"),
        };

        Ok(Response::new(reply))
    }
}

pub async fn run_leader(config: AgentConfig) -> R<()> {
    let addr = format!("{}:{}", DEFAULT_HOSTNAME, DEFAULT_PORT).parse()?;

    let (sender_to_store, receiver_from_server) = mpsc::channel();
    let sender_to_store = Arc::new(Mutex::new(sender_to_store));

    let (sender_to_server, receiver_from_store) = mpsc::channel();
    let sender_to_server = Arc::new(Mutex::new(sender_to_server));
    let receiver_from_store = Arc::new(Mutex::new(receiver_from_store));

    let cloned_config = config.clone();

    start_storage(receiver_from_server, sender_to_server, config);

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

    let manager_node = ManagerAgentService {
        sender_to_store: sender_to_store,
        receiver_from_store: receiver_from_store,
    };

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

    let _res = join!(join_server_fut, start_server_fut);

    Ok(())
}

// =====================
// =====================
// ===== FOR LATER =====
// =====================
// =====================

pub async fn run_follower(config: AgentConfig) -> R<()> {
    let leader_addr = config.manager.leader_address.clone();

    let addr = format!("{}:{}", DEFAULT_HOSTNAME, 4848).parse()?;

    let mut client = ManagerClient::connect(leader_addr).await?;

    let leader_addr = config.manager.leader_address.clone();
    let request = tonic::Request::new(ManagerJoinRequest {
        address: leader_addr,
    });

    let response = client.manager_join(request).await?;
    debug!("RESPONSE={:?}", response);

    let (sender_to_store, receiver_from_server) = mpsc::channel();
    let sender_to_store = Arc::new(Mutex::new(sender_to_store));

    let (sender_to_server, receiver_from_store) = mpsc::channel();
    let sender_to_server = Arc::new(Mutex::new(sender_to_server));
    let receiver_from_store = Arc::new(Mutex::new(receiver_from_store));

    start_storage(receiver_from_server, sender_to_server, config);

    let manager_node = ManagerAgentService {
        sender_to_store: sender_to_store,
        receiver_from_store: receiver_from_store,
    };

    debug!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerServer::new(manager_node))
        .serve(addr)
        .await?;

    debug!("Cleaning Up");

    Ok(())
}
