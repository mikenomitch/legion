use crate::cmd::structs::AgentConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};
use crate::raft::node::{start_raft, Msg};
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

// #[derive(Default)]
pub struct ManagerAgentService {
    sender_to_raft: Arc<Mutex<Sender<Msg>>>,
    _receiver_from_raft: Arc<Mutex<Receiver<String>>>,
}

#[tonic::async_trait]
impl ManagerNode for ManagerAgentService {
    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        let to_raft = self.sender_to_raft.lock().unwrap();

        to_raft
            .send(Msg::Propose {
                id: 1,
                message: "JOIN".to_string(),
                cb: Box::new(move || debug!("wat")),
            })
            .unwrap();

        let reply = JoinClusterReply {
            message: format!("Hello {}!", request.into_inner().address),
        };

        Ok(Response::new(reply))
    }

    async fn run_job(
        &self,
        _request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {

        let to_raft = self.sender_to_raft.lock().unwrap();

        to_raft
            .send(Msg::Propose {
                id: 2,
                message: "JOB".to_string(),
                cb: Box::new(move || debug!("applied job")),
            })
            .unwrap();


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

    let (sender_to_raft, receiver_from_server) = mpsc::channel();
    let sender_to_raft = Arc::new(Mutex::new(sender_to_raft));

    let (sender_to_server, receiver_from_raft) = mpsc::channel();
    let sender_to_server = Arc::new(Mutex::new(sender_to_server));
    let receiver_from_raft = Arc::new(Mutex::new(receiver_from_raft));

    start_raft(receiver_from_server, sender_to_server, config);

    let manager_node = ManagerAgentService {
        sender_to_raft: sender_to_raft,
        receiver_from_raft: receiver_from_raft
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

    let (sender_to_raft, receiver_from_server) = mpsc::channel();
    let sender_to_raft = Arc::new(Mutex::new(sender_to_raft));

    let (sender_to_server, receiver_from_raft) = mpsc::channel();
    let sender_to_server = Arc::new(Mutex::new(sender_to_server));
    let receiver_from_raft = Arc::new(Mutex::new(receiver_from_raft));

    start_raft(receiver_from_server, sender_to_server, config);

    let manager_node = ManagerAgentService {
        sender_to_raft: sender_to_raft,
        receiver_from_raft: receiver_from_raft
    };

    debug!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    debug!("Cleaning Up");

    Ok(())
}

// fn send_propose(sender: mpsc::Sender<Msg>) {
//     thread::spawn(move || {
//         // Wait some time and send the request to the Raft.
//         thread::sleep(Duration::from_secs(10));

//         let (s1, r1) = mpsc::channel::<u8>();

//         debug!("propose a request");

//         // Send a command to the Raft, wait for the Raft to apply it
//         // and get the result.
//         sender
//             .send(Msg::Propose {
//                 id: 1,
//                 cb: Box::new(move || {
//                     s1.send(0).unwrap();
//                 }),
//             })
//             .unwrap();

//         let n = r1.recv().unwrap();
//         assert_eq!(n, 0);

//         debug!("receive the propose callback");
//     });
// }

// I need to...
// run a long term process in Raft
// run a long term server process
// I need to send messages from the Server to the Raft stuff
// must have a "sender in the server"

// I need to pass messages from raft to the server?
// must have a "receiver in the server
// I.E. raft dies and should kill the server?
