use crate::cmd::structs::AgentConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};
use crate::raft::node::{start_raft, Msg};
use crate::types::Res;

use std::sync::mpsc::{self, Sender};
// use std::thread;
// use std::time::Duration;

use std::sync::{Arc, Mutex};

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
    outbox: Arc<Mutex<Sender<Msg>>>,
}

#[tonic::async_trait]
impl ManagerNode for ManagerAgentService {
    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let outbox = self.outbox.lock().unwrap();

        outbox
            .send(Msg::Propose {
                id: 1,
                cb: Box::new(move || println!("wat")),
            })
            .unwrap();

        let reply = JoinClusterReply {
            message: format!("Hello {}!", request.into_inner().address),
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

    async fn run_job(
        &self,
        _request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {

        let reply = RunJobReply {
            message: format!("Hello {}!", "jorb"),
        };

        Ok(Response::new(reply))
    }
}

pub async fn run_leader(config: AgentConfig) -> Res<()> {
    let addr = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    let (sender, receiver) = mpsc::channel();
    let sender = Arc::new(Mutex::new(sender));

    start_raft(receiver, config);

    let manager_node = ManagerAgentService { outbox: sender };
    println!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    println!("Cleaning Up");

    Ok(())
}

pub async fn run_follower(config: AgentConfig, leader_address: String) -> Res<()> {
    let leader_addr_clone = leader_address.clone();

    // TODO: Parameterize port
    let addr = format!("{}:{}", DEFAULT_ADDR, 4848)
        .parse()
        .unwrap();

    let mut client = ManagerNodeClient::connect(leader_address).await?;
    let request = tonic::Request::new(JoinClusterRequest {
        address: leader_addr_clone,
    });

    let response = client.join_cluster(request).await?;
    println!("RESPONSE={:?}", response);

    let (sender, receiver) = mpsc::channel();
    let sender = Arc::new(Mutex::new(sender));

    start_raft(receiver, config);

    let manager_node = ManagerAgentService { outbox: sender };

    println!("Manager node starting on {}", addr);
    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    println!("Cleaning Up");

    Ok(())
}

// fn send_propose(sender: mpsc::Sender<Msg>) {
//     thread::spawn(move || {
//         // Wait some time and send the request to the Raft.
//         thread::sleep(Duration::from_secs(10));

// let (s1, r1) = mpsc::channel::<u8>();

//         println!("propose a request");

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

//         println!("receive the propose callback");
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
