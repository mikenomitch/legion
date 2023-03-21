use clap::{arg, Command};
use tonic::{transport::Server, Request, Response, Status};

use legion::control_node_client::ControlNodeClient;
use legion::control_node_server::{ControlNode, ControlNodeServer};
use legion::{JoinClusterReply, JoinClusterRequest};

use crate::types::Res;

pub mod legion {
    tonic::include_proto!("legion");
}

#[derive(Default)]
pub struct ControlAgentService {}

const DEFAULT_PROTOCOL: &str = "http";
const DEFAULT_PORT: &str = "4747";
const DEFAULT_ADDR: &str = "127.0.0.1";

#[tonic::async_trait]
impl ControlNode for ControlAgentService {
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

pub fn config() -> Command {
    let leader_cmd = Command::new("leader").about("Start a leader manager node");
    let follower_cmd = Command::new("follower")
        .about("Start a follower manager node")
        .arg(arg!(port: [PORT]));

    Command::new("start")
        .about("Start an agent")
        .subcommand(leader_cmd)
        .subcommand(follower_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Res<()> {
    match sub_matches.subcommand() {
        Some(("leader", _)) => run_leader().await,
        Some(("follower", sms)) => {
            let port = sms.get_one::<String>("port").map(|s| s.as_str());

            match port {
                Some(port) => run_follower(port).await,
                None => run_follower(DEFAULT_PORT).await,
            }
        }
        _ => run_leader().await,
    }
}

async fn run_leader() -> Res<()> {
    let addr = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();
    let control_node = ControlAgentService::default();

    println!("Control node listening on {}", addr);

    Server::builder()
        .add_service(ControlNodeServer::new(control_node))
        .serve(addr)
        .await?;

    println!("Cleaning Up");

    Ok(())
}

async fn run_follower(port: &str) -> Res<()> {
    // interpolate port into string with localhost
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_ADDR, port);
    let cloned_addr = addr.clone();

    let mut client = ControlNodeClient::connect(addr).await?;

    let request = tonic::Request::new(JoinClusterRequest {
        address: cloned_addr,
    });

    let response = client.join_cluster(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
