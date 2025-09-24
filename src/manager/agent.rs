use crate::prelude::*;

use crate::manager::join_server::get_insecure_manager_service;
use crate::manager::server::ManagerAgentService;
use crate::storage::store::Store;
use crate::structs::{
    manager_server::ManagerServer, ManagerConfig, Node, WorkerConfig as WorkerConfigMsg,
    WorkerConfigAlt,
};

use futures::join;
use jsonnet::JsonnetVm;
use std::fs;
use std::str::FromStr;
use tonic::transport::{Identity, Server, ServerTlsConfig};

#[instrument(err, skip_all)]
pub async fn run_manager(config: ManagerConfig) -> R<()> {
    info!("Starting manager node");

    let full_config = config.with_defaults();
    let store = Store::start_storage(&full_config)?;
    load_initial_data(&store, &config).await?;

    // Spawn a separate grpc server for non-TLS calls
    let join_server_addr = format!(
        "{}:{}",
        full_config.server.host, full_config.server.insecure_port
    )
    .parse()?;

    let insecure_manager_service = get_insecure_manager_service();
    let join_server_fut = Server::builder()
        .add_service(insecure_manager_service)
        .serve(join_server_addr);

    let store_copy_a = Store {
        conn: store.conn.clone(),
    };

    let store_copy_b = Store {
        conn: store.conn.clone(),
    };

    let manager_node = ManagerAgentService { store };

    let data_dir = std::path::PathBuf::from_str("/Users/mike/code/legion/configs/tls/server")?;
    let cert = std::fs::read_to_string(data_dir.join("server.pem"))?;
    let key = std::fs::read_to_string(data_dir.join("server.key"))?;
    let identity = Identity::from_pem(cert, key);
    let _tls = ServerTlsConfig::new().identity(identity);

    let addr = format!("{}:{}", full_config.server.host, full_config.server.port).parse()?;
    let start_server_fut = Server::builder()
        // .tls_config(tls)?
        .add_service(ManagerServer::new(manager_node))
        .serve(addr);

    println!("Join server starting on {join_server_addr}");
    println!("Manager node starting on {addr}");

    let node_checker_fut =
        start_node_status_checker(store_copy_a, full_config.worker_check_seconds);
    let eval_loop_fut = start_eval_loop(store_copy_b);
    let _res = join!(
        join_server_fut,
        start_server_fut,
        node_checker_fut,
        eval_loop_fut
    );

    Ok(())
}

#[instrument(err, skip_all)]
async fn start_node_status_checker(store: Store, check_seconds: u64) -> R<()> {
    info!("Beginning node status checker loop");
    loop {
        let node_ids_to_update = store
            .get_node_ids_and_heartbeats()?
            .into_iter()
            .filter(
                |(id, heartbeat)| match Node::last_heartbeat_too_slow(heartbeat) {
                    Ok(too_slow) => {
                        if too_slow {
                            warn!("Node {} heartbeat too slow - marking as lost", id);
                        }
                        too_slow
                    }
                    Err(e) => {
                        warn!(
                            "Error checking heartbeat - marking node {} lost: {:?}",
                            id, e
                        );
                        true
                    }
                },
            )
            .map(|(id, _heartbeat)| id)
            .collect::<Vec<String>>();

        store.mark_nodes_as_lost(&node_ids_to_update)?;
        tokio::time::sleep(tokio::time::Duration::from_secs(check_seconds)).await;
    }
}

#[instrument(err, skip_all)]
async fn start_eval_loop(store: Store) -> R<()> {
    info!("Beginning eval loop");

    loop {
        match store.get_next_eval() {
            Ok(eval) => match eval.handle(&store) {
                Ok(message) => {
                    info!("Eval {} handled successfully: {}", eval.id, message);
                }
                Err(e) => {
                    error!("Error handling eval {}: {:?}", eval.id, e);
                }
            },
            Err(e) => {
                error!("Error getting next eval: {:?}", e);
            }
        }

        // Note: do I want a sleep here?
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

#[instrument(err, skip_all)]
fn parse_worker_config(config_path: &str) -> R<WorkerConfigAlt> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: WorkerConfigAlt = serde_json::from_str(&s)
                .map_err(|e| format!("Error parsing worker config at path {config_path}: {e}"))?;
            Ok(config)
        }
        Err(e) => {
            let msg = format!("Error parsing worker config at path {config_path}: {e}");
            error!("{}", &msg);
            Err(msg.into())
        }
    }
}

#[instrument(err, skip_all)]
async fn load_initial_data(store: &Store, config: &ManagerConfig) -> R<()> {
    let worker_conf_path = config.with_defaults().worker_config_path.clone();
    let sub_paths = fs::read_dir(worker_conf_path)?;

    for path in sub_paths {
        let path = path.unwrap().path();
        let path_str = path.to_str().unwrap();
        let wcp = parse_worker_config(path_str)?;

        let af = wcp.additional_node_checks.unwrap_or([].to_vec());
        let attrs = wcp.attributes.unwrap_or(HashMap::new());

        let wc = WorkerConfigMsg {
            name: wcp.worker_type.clone(),
            pool: wcp.pool.unwrap_or("default".into()),
            additional_node_checks: af,
            attributes: attrs,
            name_prefix: wcp.name_prefix,
        };

        store.insert_worker_type(&wc)?;
    }

    Ok(())
}
