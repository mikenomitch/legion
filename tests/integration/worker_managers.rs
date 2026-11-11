//! A worker's manager endpoints against real managers: its clients follow a
//! failover, and a learned manager is reachable by address (docs/architecture-
//! managers-workers.md).

use legion::prelude::*;
use legion::worker::managers::*;

/// The channel really follows a failover: a call through a client built
/// BEFORE the switch reaches the manager that is active AFTER it.
#[tokio::test]
async fn clients_follow_the_active_manager() -> R<()> {
    use legion::structs::manager_client::ManagerClient;
    use legion::structs::manager_server::ManagerServer;

    // Manager B: answers WhoAmI over the manager's TLS listener. Manager
    // A: an address nothing listens on. The worker trusts the cluster CA
    // through the token's pin, recorded for A only: it covers B too.
    let store = legion::storage::store::Store::start_storage(
        &legion::structs::config::ManagerConfigFull {
            store_path: tempfile::tempdir_in(crate::support::scratch_root())?
                .keep()
                .display()
                .to_string(),
            ..Default::default()
        },
    )?;
    let pin = store.ensure_mesh_ca()?.pin()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let live = format!("http://{}", listener.local_addr()?);
    let incoming = legion::manager::daemon::manager_incoming(&store, listener, None)?;
    let service = legion::manager::server::ManagerDaemonService::new(store);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ManagerServer::new(service))
            .serve_with_incoming(incoming),
    );
    let dead = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        format!("http://{}", probe.local_addr()?)
    };
    legion::worker::plane::trust_manager(&dead, &pin)?;

    let managers = ManagerEndpoints::new(&dead, vec![live.clone()])?;
    let mut client = ManagerClient::new(managers.channel());
    let first = client
        .who_am_i(legion::structs::WhoAmIRequest::default())
        .await
        .expect_err("the token's manager is down");
    assert!(is_unreachable(&first), "{first:?}");
    managers.record_failure(&first);
    managers.record_failure(&first);
    assert_eq!(managers.active(), live);
    client
        .who_am_i(legion::structs::WhoAmIRequest::default())
        .await
        .expect("the same client now reaches the live manager");
    Ok(())
}

/// The spot-check regression: after a failover, calls that dial the
/// active manager by address (artifact credentials, node identity
/// tokens) failed with "no cluster CA pin for the manager at ..."
/// because only the token's address had a recorded trust. A manager the
/// cluster reports shares it.
#[tokio::test]
async fn a_learned_manager_is_reachable_by_address_test() -> R<()> {
    use legion::structs::manager_client::ManagerClient;
    use legion::structs::manager_server::ManagerServer;

    let store = legion::storage::store::Store::start_storage(
        &legion::structs::config::ManagerConfigFull {
            store_path: tempfile::tempdir_in(crate::support::scratch_root())?
                .keep()
                .display()
                .to_string(),
            ..Default::default()
        },
    )?;
    let pin = store.ensure_mesh_ca()?.pin()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let live = format!("http://{}", listener.local_addr()?);
    let incoming = legion::manager::daemon::manager_incoming(&store, listener, None)?;
    let service = legion::manager::server::ManagerDaemonService::new(store);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ManagerServer::new(service))
            .serve_with_incoming(incoming),
    );
    let seed = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        format!("http://{}", probe.local_addr()?)
    };
    legion::worker::plane::trust_manager(&seed, &pin)?;
    let managers = ManagerEndpoints::new(&seed, Vec::new())?;
    let before = legion::worker::plane::manager_channel(&live)
        .await
        .expect_err("not reported yet");
    assert!(
        format!("{before:#}").contains("no cluster CA pin"),
        "{before:#}"
    );

    managers.learn(std::slice::from_ref(&live));
    let channel = legion::worker::plane::manager_channel(&live).await?;
    ManagerClient::new(channel)
        .who_am_i(legion::structs::WhoAmIRequest::default())
        .await
        .expect("the learned manager answers over the cluster's trust");
    Ok(())
}
