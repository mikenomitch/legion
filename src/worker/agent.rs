use crate::cmd::structs::JoinTokenClaims;
use crate::constants::{DEFAULT_HOSTNAME, TMP_SECRET_KEY, WORKER_HEARTBEAT_DURATION};
use crate::util::R;
use crate::worker::fingerprint::fingerprint_node;

use jsonwebtoken::{decode, errors::ErrorKind, Algorithm, DecodingKey, Validation};
use log::debug;
use futures::join;
use serde_json::{Map, Value};

use tonic::{
    transport::Certificate, transport::Channel, transport::ClientTlsConfig, transport::Server,
    Request, Response, Status,
};

use crate::structs::manager_client::ManagerClient;
use crate::structs::worker_server::{Worker, WorkerServer};
use crate::structs::{PostWorkReply, PostWorkRequest, HeartbeatRequest, WorkerRegisterRequest};

pub struct WorkerAgentService {}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());

        let reply = PostWorkReply {
            message: "I got work yay!".into(),
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

    // TODO: ESTABLISH TLS CONNECTION PROPERLY

    let fingerprint_data = fingerprint_node(&token_data.claims.config);

    // TODO: SEND THE REAL RE-REGISTRATION WITH FINGERPRINT DATA

    let addr = format!("{}:{}", DEFAULT_HOSTNAME, 4848).parse()?;
    let worker_node = WorkerAgentService {};
    let worker_server_fut = Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(addr);

    let client = create_manager_client(
        token_data.claims.address,
        token_data.claims.ca_cert,
    ).await?;

    register_self(client.clone(), fingerprint_data).await?;

    let heartbeat_fut = start_heartbeats(client);
    let _res = join!(heartbeat_fut, worker_server_fut);

    Ok(())
}

async fn create_manager_client(manager_addr: String, ca_cert: String) -> R<ManagerClient<Channel>> {
    let ca_cert = Certificate::from_pem(ca_cert);
    let tls = ClientTlsConfig::new().ca_certificate(ca_cert);
    let channel = Channel::from_shared(manager_addr)?.tls_config(tls)?.connect().await?;
    let client = ManagerClient::new(channel);

    Ok(client)
}


async fn start_heartbeats(client: ManagerClient<Channel>) -> R<()> {
    let mut c2 = client.clone();
    loop {
        let req = tonic::Request::new(HeartbeatRequest {
            id: "worker".into(),
        });

        let response = c2.heartbeat(req).await?;
        debug!("RESPONSE={:?}", response);

        tokio::time::sleep(tokio::time::Duration::from_secs(WORKER_HEARTBEAT_DURATION)).await;
    }
}

async fn register_self(client: ManagerClient<Channel>, fingerprint_data: Map<String, Value>) -> R<()> {
    let mut c2 = client.clone();
    let serialized_data = serde_json::to_string(&fingerprint_data)?;


    let req = tonic::Request::new(WorkerRegisterRequest {
        name: "worker".into(),
        attributes: serialized_data,
    });

    let response = c2.worker_register(req).await?;
    debug!("RESPONSE={:?}", response);

    Ok(())
}