use clap::Command;
use tonic::{transport::Server, Request, Response, Status};

use legion::greeter_server::{Greeter, GreeterServer};
use legion::{HelloReply, HelloRequest};

pub mod legion {
    tonic::include_proto!("legion");
}

#[derive(Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = legion::HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(Response::new(reply))
    }
}

pub fn config() -> Command {
    Command::new("start").about("Start an agent (manager or worker)")
}

pub async fn handle(_sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    run_server().await
}

// Logic

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50052".parse().unwrap();
    let greeter = MyGreeter::default();

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    println!("Cleaning Up");

    Ok(())
}
