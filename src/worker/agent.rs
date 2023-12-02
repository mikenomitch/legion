use crate::cmd::structs::JoinTokenClaims;
use crate::constants::{DEFAULT_HOSTNAME, TMP_SECRET_KEY, WORKER_HEARTBEAT_DURATION};
use crate::util::R;
use crate::worker::fingerprint::fingerprint_node;

use jsonwebtoken::{decode, errors::ErrorKind, Algorithm, DecodingKey, Validation};
use log::debug;
use futures::join;

use tonic::{
    transport::Certificate, transport::Channel, transport::ClientTlsConfig, transport::Server,
    Request, Response, Status,
};

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_client::ManagerClient;
use legion::worker_server::{Worker, WorkerServer};
use legion::{PostWorkReply, PostWorkRequest, HeartbeatRequest};

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

    // println!("{:?}", token_data.claims.address);
    // println!("{:?}", token_data.claims.config);

    // TODO: ESTABLISH TLS CONNECTION PROPERLY
    // println!("cert {:?}", token_data.claims.ca_cert);

    let fingerprint_data = fingerprint_node(&token_data.claims.config);

    let os = fingerprint_data
        .get("os")
        .ok_or("OS fingerprinter failed to return an OS")?
        .as_str()
        .ok_or("OS fingerprinter returned a non-string value")?;

    println!("OS fingerprinted as: {:?}", os);
    // TODO: SEND THE REAL RE-REGISTRATION WITH FINGERPRINT DATA

    let addr = format!("{}:{}", DEFAULT_HOSTNAME, 4848).parse()?;
    let worker_node = WorkerAgentService {};
    let worker_server_fut = Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(addr);

    let heartbeat_fut = start_heartbeats(token_data.claims.address, token_data.claims.ca_cert);

    let _res = join!(heartbeat_fut, worker_server_fut);

    Ok(())
}


async fn start_heartbeats(manager_addr: String, ca_cert: String) -> R<()> {
    let ca_cert = Certificate::from_pem(ca_cert);
    let tls = ClientTlsConfig::new().ca_certificate(ca_cert);
    let channel = Channel::from_shared(manager_addr)?
        .tls_config(tls.clone())?
        .connect()
        .await?;
    let mut client = ManagerClient::new(channel);

    loop {
        let req = tonic::Request::new(HeartbeatRequest {
            name: "worker-a".into(),
        });

        let response = client.heartbeat(req).await?;
        debug!("RESPONSE={:?}", response);

        tokio::time::sleep(tokio::time::Duration::from_secs(WORKER_HEARTBEAT_DURATION)).await;
    }
}
