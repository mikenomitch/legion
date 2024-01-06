use crate::prelude::*;

use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, HealthCheckRequest,
    HeartbeatRequest, JoinTokenClaims, WorkerPreRegisterReply, WorkerPreRegisterRequest,
    WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    node_check::{fingerprint_node, run_health_checks},
    server::WorkerAgentService,
};

use futures::join;
use jsonwebtoken::{decode, errors::ErrorKind, Algorithm, DecodingKey, Validation};
use tonic::{transport::Channel, transport::Server, Response};
// use tonic::transport::Certificate, transport::ClientTlsConfig,

pub async fn run_worker(token: &str) -> R<()> {
    let mut val = Validation::new(Algorithm::HS512);
    val.validate_exp = false;

    let token_data = match decode::<JoinTokenClaims>(
        token,
        &DecodingKey::from_secret(&TMP_SECRET_KEY[..]),
        &val,
    ) {
        Ok(c) => c,
        Err(err) => {
            // print error message
            println!("Error: {err}");

            // TODO: Return a nicer error to the CLI instead of a panic
            match *err.kind() {
                ErrorKind::InvalidToken => panic!(), // Example on how to handle a specific error
                _ => panic!(),
            }
        }
    };

    // Reusing the validation code from above just to
    // check for expiry. Not beautiful but it works.
    if !token_data.claims.skip_expiry_validation {
        debug!("Validating expiry on join token");
        val.validate_exp = true;
        match decode::<JoinTokenClaims>(token, &DecodingKey::from_secret(&TMP_SECRET_KEY[..]), &val)
        {
            Ok(_c) => (),
            Err(err) => {
                // TODO: Return a nicer error to the CLI instead of a panic
                error!("Error: {}", err);
                panic!()
            }
        }
    } else {
        debug!("Skipping expiry validation check on join token");
    }

    let addr = format!("{DEF_MANAGER_HOSTNAME}:4848").parse()?;
    let worker_node = WorkerAgentService {};
    let worker_server_fut = Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(addr);

    let client = create_manager_client(
        token_data.claims.address,
        String::new(),
        // token_data.claims.ca_cert,
    )
    .await?;

    // TODO: Establish TLS connection properly
    let pre_register_res = pre_register_self(client.clone(), token_data.claims.worker_type).await?;
    let config = pre_register_res
        .into_inner()
        .config
        .ok_or("No config found")?;

    let name_core = random_string(8);
    let name = match config.name_prefix {
        Some(n) => n + "-" + &name_core,
        None => name_core,
    };

    let fingerprint_data = fingerprint_node(config.additional_node_checks.clone());
    let health_checks = run_health_checks(config.additional_node_checks.clone());

    register_self(
        client.clone(),
        name.clone(),
        config.pool,
        config.attributes,
        fingerprint_data,
        health_checks,
    )
    .await?;

    let heartbeat_fut = start_heartbeats(name.clone(), client.clone());
    let checks_fut = start_node_checks(name.clone(), config.additional_node_checks.clone(), client);
    let _res = join!(heartbeat_fut, checks_fut, worker_server_fut);

    Ok(())
}

async fn create_manager_client(
    manager_addr: String,
    _ca_cert: String,
) -> R<ManagerClient<Channel>> {
    // // let ca_cert = Certificate::from_pem(ca_cert);
    // // let tls = ClientTlsConfig::new().ca_certificate(ca_cert);
    // let channel = Channel::from_shared(manager_addr)?.tls_config(tls)?.connect().await?;
    let channel = Channel::from_shared(manager_addr)?.connect().await?;
    let client = ManagerClient::new(channel);

    Ok(client)
}

async fn start_heartbeats(name: String, client: ManagerClient<Channel>) -> R<()> {
    let mut client_clone = client.clone();

    loop {
        let req = tonic::Request::new(HeartbeatRequest { id: name.clone() });

        let response = client_clone.heartbeat(req).await?;
        debug!("RESPONSE={:?}", response);

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_HEARTBEAT_DURATION_SEC,
        ))
        .await;
    }
}

async fn start_node_checks(
    name: String,
    additional_node_checks: Vec<String>,
    client: ManagerClient<Channel>,
) -> R<()> {
    let mut client_clone = client.clone();

    let checks_data = run_health_checks(additional_node_checks);
    let serialized_health_check_data = serde_json::to_string(&checks_data)?;

    loop {
        let req = tonic::Request::new(HealthCheckRequest {
            id: name.clone(),
            health_checks: serialized_health_check_data.clone(),
        });

        let response = client_clone.health_check(req).await?;
        debug!("RESPONSE={:?}", response);

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_NODE_CHECK_DURATION_SEC,
        ))
        .await;
    }
}

async fn register_self(
    client: ManagerClient<Channel>,
    name: String,
    pool: String,
    config_attrs: HashMap<String, String>,
    fingerprint_attrs: Map<String, Value>,
    health_checks: Map<String, Value>,
) -> R<()> {
    let mut c2 = client.clone();
    let serialized_config_data = serde_json::to_string(&config_attrs)?;
    let serialized_fp_data = serde_json::to_string(&fingerprint_attrs)?;
    let serialized_health_check_data = serde_json::to_string(&health_checks)?;

    let req = tonic::Request::new(WorkerRegisterRequest {
        name,
        config_attrs: serialized_config_data,
        fingerprint_attrs: serialized_fp_data,
        health_checks: serialized_health_check_data,
        pool,
    });

    let response = c2.worker_register(req).await?;
    debug!("RESPONSE={:?}", response);

    Ok(())
}

async fn pre_register_self(
    client: ManagerClient<Channel>,
    worker_type: String,
) -> R<Response<WorkerPreRegisterReply>> {
    let mut c2 = client.clone();

    let req = tonic::Request::new(WorkerPreRegisterRequest { worker_type });

    let response = c2.worker_pre_register(req).await?;

    Ok(response)
}
