use crate::cmd::structs::{AgentConfig, WorkerConfig, JoinTokenClaims};
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT, TMP_SECRET_KEY, DEFAULT_WORKER_CONFIG_PATH};
use crate::storage::node::{start_storage, Msg};
use log::debug;
use std::error;
use std::sync::mpsc::{self, Sender, Receiver};
// use std::thread;
// use std::time::Duration;
use jsonnet::JsonnetVm;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use std::sync::{Arc, Mutex};

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}

use legion::manager_node_client::ManagerNodeClient;
use legion::manager_node_server::{ManagerNode, ManagerNodeServer};
use legion::{
    ManagerJoinReply,
    ManagerJoinRequest,
    WorkerJoinReply,
    WorkerJoinRequest,
    RunJobRequest,
    RunJobReply,
    GetJobStatusReply,
    GetJobStatusRequest,
    GetJoinTokenReply,
    GetJoinTokenRequest,
};

use tonic::{transport::Server, Request, Response, Status};

#[allow(dead_code)]
// #[derive(Default)]
pub struct ManagerAgentService {
    sender_to_store: Arc<Mutex<Sender<Msg>>>,
    receiver_from_store: Arc<Mutex<Receiver<String>>>,
}

#[tonic::async_trait]
impl ManagerNode for ManagerAgentService {
    // TODO: Deprecate this once the join token is used for everything
    async fn manager_join(
        &self,
        request: Request<ManagerJoinRequest>,
    ) -> Result<Response<ManagerJoinReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        // let to_store = self.sender_to_store.lock().unwrap();

        // to_store
        //     .send(Msg::Content("LET ME IN".to_string()))
        //     .unwrap();

        let reply = ManagerJoinReply {
            message: format!("Hello {}!", request.into_inner().address),
        };

        Ok(Response::new(reply))
    }

    async fn run_job(
        &self,
        _request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {

        // let to_store = self.sender_to_store.lock().unwrap();

        // to_store
        //     .send(Msg::Content("ADD THE JOB".to_string()))
        //     .unwrap();

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

    async fn get_join_token(
        &self,
        _request: Request<GetJoinTokenRequest>,
    ) -> Result<Response<GetJoinTokenReply>, Status> {
        // TODO: Mint a signed JWT containing:
        // - the node type (server or worker)
        // - the node address & port
        // - the node's public key (maybe?)
        // - a cert to establish TLS
        // - an expiration

        let token = generate_join_token();

        let reply = GetJoinTokenReply {
            token: token,
        };

        Ok(Response::new(reply))
    }

    // TODO: Make this work for servers and workers
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
}

pub async fn run_leader(config: AgentConfig) -> Result<(), Box<dyn error::Error>> {
    let addr = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    let (sender_to_store, receiver_from_server) = mpsc::channel();
    let sender_to_store = Arc::new(Mutex::new(sender_to_store));

    let (sender_to_server, receiver_from_store) = mpsc::channel();
    let sender_to_server = Arc::new(Mutex::new(sender_to_server));
    let receiver_from_store = Arc::new(Mutex::new(receiver_from_store));

    start_storage(receiver_from_server, sender_to_server, config);

    let manager_node = ManagerAgentService {
        sender_to_store: sender_to_store,
        receiver_from_store: receiver_from_store
    };

    debug!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    debug!("Cleaning Up");

    Ok(())
}

pub async fn run_follower(config: AgentConfig) -> Result<(), Box<dyn error::Error>> {
    let leader_addr = config.manager.leader_address.clone();

    let addr = format!("{}:{}", DEFAULT_ADDR, 4848)
    .parse()
    .unwrap();

    let mut client = ManagerNodeClient::connect(leader_addr).await?;

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
        receiver_from_store: receiver_from_store
    };

    debug!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    debug!("Cleaning Up");

    Ok(())
}

// add generate_join_token_function
fn generate_join_token() -> String {
    let config = parse_worker_config(DEFAULT_WORKER_CONFIG_PATH).unwrap();

    let claims = JoinTokenClaims {
        sub: "b@b.com".to_owned(),
        exp: 10000000000,
        address: "http://localhost:4747".to_owned(),
        config: config,
    };

    let header = Header { kid: Some("signing_key".to_owned()), alg: Algorithm::HS512, ..Default::default() };

    let token = match encode(&header, &claims, &EncodingKey::from_secret(&TMP_SECRET_KEY[..])) {
        Ok(t) => t,
        Err(_) => panic!(), // in practice you would return the error
    };

    token
}

fn parse_worker_config(config_path: &str) -> Result<WorkerConfig, Box<dyn error::Error>> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: WorkerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}
