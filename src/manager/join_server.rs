use std::time::Duration;

use crate::cmd::structs::{ManagerConfig, JoinTokenClaims};
use crate::util::R;
use crate::constants::{TMP_SECRET_KEY, DEFAULT_CONFIG_PATH, DEFAULT_ADDR};

use tonic::{Request, Response, Status};
use jsonnet::JsonnetVm;

use crate::structs::insecure_manager_server::{InsecureManager, InsecureManagerServer};
use crate::structs::{GetJoinTokenReply, GetJoinTokenRequest};

use jsonwebtoken::{encode, Header, Algorithm, EncodingKey};

// use crate::certs::{signed_cert_with_ca, read_root_cert};
// use tonic::transport::{Certificate, Channel, ClientTlsConfig};
// use rustls::Certificate;
// use rustls::PrivateKey;


pub struct InsecureManagerService {}
#[tonic::async_trait]
impl InsecureManager for InsecureManagerService {
    async fn get_join_token(
        &self,
        request: Request<GetJoinTokenRequest>,
    ) -> Result<Response<GetJoinTokenReply>, Status> {
        let r = request.into_inner();
        let ttl = r.clone().ttl;
        let wt = r.clone().worker_type;

        let worker_type = match wt {
            Some(wt) => wt,
            None => "default".into(),
        };

        let token = generate_join_token(worker_type, ttl).
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
pub fn generate_join_token(worker_type: String, ttl: Option<String>) -> R<String> {
    let config = parse_manager_config(DEFAULT_CONFIG_PATH)?;

    let jwt_exp: usize;
    match ttl {
        Some(ttl) => {
            // TODO: If this errors it probably means they did "1" not "1m" or "1s" etc
            // handle this error nicely!
            let parsed_ttl: Duration = ttl.parse::<humantime::Duration>().unwrap().into();
            let now = std::time::SystemTime::now();
            let expiry = now + parsed_ttl;
            jwt_exp = expiry.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as usize;
        }
        None => {
            jwt_exp = 0;
        }
    }

    // NOTE:
    // I think what I have to do is:
    // - When the client connects, it will use this leaf cert
    // let ca_pem_path: String = "/Users/mike/code/legion/configs/tls/rootca.pem".into();
    // let ca_key_path: String = "/Users/mike/code/legion/configs/tls/rootca.key".into();
    // let cert = read_root_cert(ca_pem_path, ca_key_path)?;
    // let (new_cert, new_key, ca_cert_again) = signed_cert_with_ca(cert, "legion".into())?;
    // // let ca_as_string = std::fs::read_to_string(ca_pem_path.clone())?;
    // let ca_cert = ca_cert_again.serialize_pem()?;

    let claims = JoinTokenClaims {
        sub: "legion".to_owned(),
        exp: jwt_exp,
        address: DEFAULT_ADDR.to_owned(),
        // cert: new_cert,
        // key: new_key,
        // ca_cert: ca_cert,
        worker_type: worker_type,
        server_config: config.server,
        skip_expiry_validation: jwt_exp == 0,
    };

    let header = Header { kid: Some("signing_key".to_owned()), alg: Algorithm::HS512, ..Default::default() };

    let encoded_token = encode(&header, &claims, &EncodingKey::from_secret(&TMP_SECRET_KEY[..])).map_err(|e| e.into());
    encoded_token
}


fn parse_manager_config(config_path: &str) -> R<ManagerConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: ManagerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}
