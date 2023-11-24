use crate::cmd::structs::{WorkerConfig, JoinTokenClaims};
use crate::constants::{TMP_SECRET_KEY, DEFAULT_WORKER_CONFIG_PATH};
use tonic::{Request, Response, Status};

use jsonnet::JsonnetVm;
use std::error;

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}
use legion::insecure_manager_server::{InsecureManager, InsecureManagerServer};
use legion::{
    GetJoinTokenReply,
    GetJoinTokenRequest,
};

use jsonwebtoken::{encode, Header, Algorithm, EncodingKey};

pub struct InsecureManagerService {}
#[tonic::async_trait]
impl InsecureManager for InsecureManagerService {
    async fn get_join_token(
        &self,
        _request: Request<GetJoinTokenRequest>,
    ) -> Result<Response<GetJoinTokenReply>, Status> {

        // TODO: Mint a signed JWT containing:

        // - the node type (server or worker)
        // - the node address & port
        // - the node's public key (maybe?)
        // - a cert to establish TLS
        // - an expiration

        let token = generate_join_token();

        let reply = GetJoinTokenReply {
            token: token,
        };

        Ok(Response::new(reply))
    }
}

pub fn get_insecure_manager_service() -> InsecureManagerServer<InsecureManagerService> {
    let insecure_manager = InsecureManagerService {};
    InsecureManagerServer::new(insecure_manager)
}

// add generate_join_token_function
pub fn generate_join_token() -> String {
    let config = parse_worker_config(DEFAULT_WORKER_CONFIG_PATH).unwrap();

    let claims = JoinTokenClaims {
        sub: "legion".to_owned(),
        exp: 10000000000,
        address: "http://localhost:4747".to_owned(),
        config: config,
    };

    let header = Header { kid: Some("signing_key".to_owned()), alg: Algorithm::HS512, ..Default::default() };

    let token = match encode(&header, &claims, &EncodingKey::from_secret(&TMP_SECRET_KEY[..])) {
        Ok(t) => t,
        Err(_) => panic!(), // in practice you would return the error
    };

    token
}


fn parse_worker_config(config_path: &str) -> Result<WorkerConfig, Box<dyn error::Error>> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: WorkerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}