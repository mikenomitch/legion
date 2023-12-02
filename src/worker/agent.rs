use crate::cmd::structs::JoinTokenClaims;
use crate::constants::{DEFAULT_HOSTNAME, TMP_SECRET_KEY};
use crate::util::R;
use crate::worker::fingerprint::fingerprint_node;

use jsonwebtoken::{decode, errors::ErrorKind, Algorithm, DecodingKey, Validation};
use log::debug;

use tonic::{
    transport::Certificate, transport::Channel, transport::ClientTlsConfig, transport::Server,
    Request, Response, Status,
};

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_client::ManagerClient;
use legion::worker_server::{Worker, WorkerServer};
use legion::{PostWorkReply, PostWorkRequest, RunJobRequest};

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

pub async fn run_worker(token: &str) -> R<()> {
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
        }
    };

    println!("{:?}", token_data.claims.address);
    println!("{:?}", token_data.claims.config);

    println!("cert {:?}", token_data.claims.ca_cert);

    let fingerprint_data = fingerprint_node(&token_data.claims.config);

    let os = fingerprint_data
        .get("os")
        .ok_or("OS fingerprinter failed to return an OS")?
        .as_str()
        .ok_or("OS fingerprinter returned a non-string value")?;

    println!("OS fingerprinted as: {:?}", os);

    // TODO: ESTABLISH TLS CONNECTION
    // TODO: SEND REAL REGISTRATION
    // TODO: GET FROM TOKEN

    // TODO: Remove once testing is done
    // ---- START TEST -----
    // Temporarily estabilish a connection to the manager

    let ca_cert = Certificate::from_pem(token_data.claims.ca_cert);
    let tls = ClientTlsConfig::new().ca_certificate(ca_cert);

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

    let worker_addr = format!("{}:{}", DEFAULT_HOSTNAME, 4848).parse()?;
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
