use crate::prelude::*;

use crate::manager::join_server::get_insecure_manager_service;
use crate::manager::server::ManagerAgentService;
use crate::storage::store::Store;
use crate::structs::{
    manager_server::ManagerServer, ManagerConfig, Node, WorkerConfig as WorkerConfigMsg,
    WorkerConfigAlt,
};

use futures::join;
use http::Method;
use jsonnet::JsonnetVm;
use std::fs;
use std::time::Duration;
use tokio::task::JoinSet;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{Any, CorsLayer};

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
        scheduling_policy: store.scheduling_policy.clone(),
        default_eval_priority: store.default_eval_priority,
        scheduler_settings: store.scheduler_settings.clone(),
    };

    let store_copy_b = Store {
        conn: store.conn.clone(),
        scheduling_policy: store.scheduling_policy.clone(),
        default_eval_priority: store.default_eval_priority,
        scheduler_settings: store.scheduler_settings.clone(),
    };

    let manager_node = ManagerAgentService { store };

    let addr = format!("{}:{}", full_config.server.host, full_config.server.port).parse()?;

    let tls_identity = load_server_tls_identity()?;
    let server_builder = match tls_identity {
        Some(identity) => {
            info!("Starting manager with TLS enabled");
            Server::builder().tls_config(ServerTlsConfig::new().identity(identity))?
        }
        None => {
            info!("Starting manager without TLS");
            Server::builder()
        }
    };

    let manager_service = ManagerServer::new(manager_node);
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .expose_headers([
            http::header::HeaderName::from_static("grpc-status"),
            http::header::HeaderName::from_static("grpc-message"),
            http::header::HeaderName::from_static("grpc-encoding"),
        ]);

    let start_server_fut = server_builder
        .accept_http1(true)
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .add_service(manager_service)
        .serve(addr);

    println!("Join server starting on {join_server_addr}");
    println!("Manager node starting on {addr}");

    let node_checker_fut =
        start_node_status_checker(store_copy_a, full_config.worker_check_seconds);
    let eval_loop_fut = start_eval_loop(store_copy_b, full_config.eval_threads);
    let _res = join!(
        join_server_fut,
        start_server_fut,
        node_checker_fut,
        eval_loop_fut
    );

    Ok(())
}

fn load_server_tls_identity() -> R<Option<Identity>> {
    let tls_dir = std::env::var("LEGION_TLS_SERVER_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("configs/tls/server"));

    let cert_path = tls_dir.join("server.pem");
    let key_path = tls_dir.join("server.key");

    if !(cert_path.exists() && key_path.exists()) {
        info!(
            ?tls_dir,
            "TLS credentials not found; proceeding without TLS"
        );
        return Ok(None);
    }

    // Temporarily don't do any TLS
    return Ok(None);

    // let cert = std::fs::read_to_string(cert_path)?;
    // let key = std::fs::read_to_string(key_path)?;

    // Ok(Some(Identity::from_pem(cert, key)))
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
async fn start_eval_loop(store: Store, max_threads: usize) -> R<()> {
    let max_threads = max_threads.max(1);
    info!(max_threads, "Beginning eval loop");

    let mut join_set = JoinSet::new();
    for worker_id in 0..max_threads {
        spawn_eval_worker(&mut join_set, &store, worker_id);
    }

    let mut next_worker_id = max_threads;
    loop {
        match join_set.join_next().await {
            Some(Ok(())) => {
                warn!("Eval worker exited unexpectedly; restarting");
            }
            Some(Err(e)) => {
                error!("Eval worker crashed; restarting: {:?}", e);
            }
            None => {
                error!("Eval worker join set drained unexpectedly");
                break;
            }
        }

        let worker_id = next_worker_id;
        next_worker_id = next_worker_id.saturating_add(1);
        spawn_eval_worker(&mut join_set, &store, worker_id);
    }

    Ok(())
}

fn spawn_eval_worker(join_set: &mut JoinSet<()>, store: &Store, worker_id: usize) {
    let store_for_eval = Store {
        conn: store.conn.clone(),
    };

    join_set.spawn_blocking(move || eval_worker_loop(store_for_eval, worker_id));
}

fn eval_worker_loop(store: Store, worker_id: usize) {
    info!(worker_id, "Eval worker started");

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        match store.claim_eval() {
            Ok(Some(eval)) => {
                backoff = Duration::from_secs(1);

                let result = eval.handle(&store);

                match result {
                    Ok(message) => {
                        info!(
                            worker_id,
                            "Eval {} handled successfully: {}", eval.id, message
                        );
                        if let Err(e) = store.complete_eval(&eval.id, true) {
                            error!(worker_id, "Error completing eval {}: {:?}", eval.id, e);
                        }
                    }
                    Err(e) => {
                        error!(worker_id, "Error handling eval {}: {:?}", eval.id, e);
                        if let Err(complete_err) = store.complete_eval(&eval.id, false) {
                            error!(
                                worker_id,
                                "Error marking eval {} as failed after handling error: {:?}",
                                eval.id,
                                complete_err
                            );
                        }
                    }
                }

                std::thread::sleep(Duration::from_millis(200));
            }
            Ok(None) => {
                info!(worker_id, "No evals to handle; backing off");
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
            Err(e) => {
                error!(worker_id, "Error claiming next eval: {:?}", e);
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }
}

#[instrument(err, skip_all)]
fn parse_worker_config(config_path: &str) -> R<WorkerConfigAlt> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let rendered = vm
        .evaluate_file(config_path)
        .map_err(|e| {
            let err = anyhow!("Error parsing worker config at path {config_path}: {e}");
            error!("{err}");
            err
        })?
        .to_string();

    let config: WorkerConfigAlt = serde_json::from_str(&rendered)
        .with_context(|| format!("Error parsing worker config at path {config_path}"))?;

    Ok(config)
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
