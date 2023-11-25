use crate::cmd::structs::JoinTokenClaims;
use crate::constants::TMP_SECRET_KEY;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, Validation, DecodingKey};

use crate::worker::fingerprint::fingerprint_node;

use tonic::{transport::Server, transport::Certificate, Request, Response, Status, transport::ClientTlsConfig, transport::Channel};

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}
use legion::worker_server::{Worker, WorkerServer};
use legion::manager_client::ManagerClient;
use legion::{
    PostWorkReply,
    PostWorkRequest,
    RunJobRequest,
};

use log::debug;
use std::error;

pub struct WorkerAgentService {}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        let reply = PostWorkReply {
            message: "I got work yay!".to_string(),
        };

        Ok(Response::new(reply))
    }
}

pub async fn run_worker(token: &str) -> Result<(), Box<dyn error::Error>> {
    // let addr: String = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
    //     .parse()
    //     .unwrap();

    println!("Starting worker node with token: {}", token);

    let token_data = match decode::<JoinTokenClaims>(
        &token,
        &DecodingKey::from_secret(&TMP_SECRET_KEY[..]),
        &Validation::new(Algorithm::HS512),
    ) {
        Ok(c) => c,
        Err(err) => {
            // print error message
            println!("Error: {}", err);

            match *err.kind() {
            ErrorKind::InvalidToken => panic!(), // Example on how to handle a specific error
            _ => panic!(),
        }
    },
    };
    println!("{:?}", token_data.claims.address);
    println!("{:?}", token_data.claims.config);

    println!("cert {:?}", token_data.claims.cert);
    println!("key {:?}", token_data.claims.key);

    let fingerprint_data = fingerprint_node(&token_data.claims.config);
    let os = fingerprint_data.get("os").unwrap().as_str().unwrap();
    println!("OS fingerprinted as: {:?}", os);

    // TODO: ESTABLISH TLS CONNECTION
    // TODO: SEND REAL REGISTRATION
    // TODO: GET FROM TOKEN

    // TODO: Remove once testing is done
    // ---- START TEST -----
    // Temporarily estabilish a connection to the manager
    let ca = Certificate::from_pem(token_data.claims.cert);

    let tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("legion");

    let manager_addr = token_data.claims.address;
    let channel = Channel::from_shared(manager_addr)?
        .tls_config(tls)?
        .connect()
        .await?;

    let mut client = ManagerClient::new(channel);
    let request = tonic::Request::new(RunJobRequest {
        name: "Mike".into(),
    });
    let response = client.run_job(request).await?;
    debug!("RESPONSE={:?}", response);
    // ---- END TEST -----




    let worker_addr = format!("{}:{}", "127.0.0.1", 4848)
        .parse()
        .unwrap();
    println!("Worker node starting on {}", worker_addr);

    // TODO: ADD YOUR OWN TLS TO THIS!
    // TODO: START HEARTBEAT LOOP
    let worker_node = WorkerAgentService {};
    Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(worker_addr)
        .await?;

    Ok(())
}
