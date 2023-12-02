use crate::cmd::structs::{WorkerConfig, JoinTokenClaims};
use crate::util::R;
use crate::certs::{signed_cert_with_ca, read_root_cert};
use crate::constants::{TMP_SECRET_KEY, DEFAULT_WORKER_CONFIG_PATH, DEFAULT_ADDR};
use tonic::{Request, Response, Status};

use jsonnet::JsonnetVm;

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::insecure_manager_server::{InsecureManager, InsecureManagerServer};
use legion::{
    GetJoinTokenReply,
    GetJoinTokenRequest,
};

use jsonwebtoken::{encode, Header, Algorithm, EncodingKey};

// use tonic::transport::{Certificate, Channel, ClientTlsConfig};
// use rustls::Certificate;
// use rustls::PrivateKey;

pub struct InsecureManagerService {}
#[tonic::async_trait]
impl InsecureManager for InsecureManagerService {
    async fn get_join_token(
        &self,
        _request: Request<GetJoinTokenRequest>,
    ) -> Result<Response<GetJoinTokenReply>, Status> {
        let token = generate_join_token().
            map_err(|e| Status::new(tonic::Code::Internal, e.to_string()))?;

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
pub fn generate_join_token() -> R<String> {
    let config = parse_worker_config(DEFAULT_WORKER_CONFIG_PATH)?;

    // NOTE:
    // I think what I have to do is:
    // - When the client connects, it will use this leaf cert

    let ca_pem_path: String = "/Users/mike/code/legion/configs/tls/rootca.pem".into();
    let ca_key_path: String = "/Users/mike/code/legion/configs/tls/rootca.key".into();
    let cert = read_root_cert(ca_pem_path, ca_key_path)?;
    let (new_cert, new_key, ca_cert_again) = signed_cert_with_ca(cert, "legion".into())?;
    // let ca_as_string = std::fs::read_to_string(ca_pem_path.clone())?;
    let ca_cert = ca_cert_again.serialize_pem()?;

    let claims = JoinTokenClaims {
        sub: "legion".to_owned(),
        exp: 10000000000,
        address: DEFAULT_ADDR.to_owned(),
        cert: new_cert,
        key: new_key,
        ca_cert: ca_cert,
        config: config,
    };

    let header = Header { kid: Some("signing_key".to_owned()), alg: Algorithm::HS512, ..Default::default() };

    let encoded_token = encode(&header, &claims, &EncodingKey::from_secret(&TMP_SECRET_KEY[..])).map_err(|e| e.into());
    encoded_token
}


fn parse_worker_config(config_path: &str) -> R<WorkerConfig> {
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
