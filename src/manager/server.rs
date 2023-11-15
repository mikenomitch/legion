use crate::cmd::structs::AgentConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};
use crate::storage::node::{start_storage, Msg};
use log::debug;
use std::error;
use std::sync::mpsc::{self, Sender, Receiver};
// use std::thread;
// use std::time::Duration;

use std::sync::{Arc, Mutex};

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}

use legion::manager_node_client::ManagerNodeClient;
use legion::manager_node_server::{ManagerNode, ManagerNodeServer};
use legion::{
    JoinClusterReply,
    JoinClusterRequest,
    RunJobRequest,
    RunJobReply,
    GetJobStatusReply,
    GetJobStatusRequest
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
    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        // let to_store = self.sender_to_store.lock().unwrap();

        // to_store
        //     .send(Msg::Content("LET ME IN".to_string()))
        //     .unwrap();

        let reply = JoinClusterReply {
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
    let request = tonic::Request::new(JoinClusterRequest {
        address: leader_addr,
    });

    let response = client.join_cluster(request).await?;
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
