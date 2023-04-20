use tonic::{transport::Server, Request, Response, Status};

use legion::manager_node_client::ManagerNodeClient;
use legion::manager_node_server::{ManagerNode, ManagerNodeServer};
use legion::{JoinClusterReply, JoinClusterRequest};

use crate::cmd::structs::AgentConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT, DEFAULT_PROTOCOL};
use crate::raft::node::start_raft;
use crate::types::Res;

use jsonnet::JsonnetVm;

pub mod legion {
    tonic::include_proto!("legion");
}

#[derive(Default)]
pub struct ManagerAgentService {}

#[tonic::async_trait]
impl ManagerNode for ManagerAgentService {
    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = JoinClusterReply {
            message: format!("Hello {}!", request.into_inner().address),
        };

        Ok(Response::new(reply))
    }
}

pub async fn run_leader(config_path: &str) -> Res<()> {
    let config = parse_config(config_path)?;
    let addr = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    let manager_node = ManagerAgentService::default();
    println!("Manager node listening on {}", addr);
    start_raft(config);
    println!("Started Raft");

    Server::builder()
        .add_service(ManagerNodeServer::new(manager_node))
        .serve(addr)
        .await?;

    println!("Cleaning Up");

    Ok(())
}

pub async fn run_follower(config_path: &str, port: &str) -> Res<()> {
    let config = parse_config(config_path)?;

    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_ADDR, port);
    let cloned_addr = addr.clone();

    let mut client = ManagerNodeClient::connect(addr).await?;

    let request = tonic::Request::new(JoinClusterRequest {
        address: cloned_addr,
    });

    let response = client.join_cluster(request).await?;

    println!("RESPONSE={:?}", response);

    start_raft(config);

    println!("Started Raft");

    Ok(())
}

fn parse_config(config_path: &str) -> Res<AgentConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: AgentConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}
