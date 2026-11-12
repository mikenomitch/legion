pub mod acl_store;
pub mod app_store;
pub mod boot_ticket_store;
pub(crate) mod coalesce;
pub mod content_index;
pub(crate) mod data_migrations;
pub mod device_quota;
pub mod endpoint_index;
pub mod errors;
pub mod events;
pub mod governance_store;
pub mod keyring;
pub mod liveness;
pub mod migrations;
pub mod namespace_policy_store;
pub mod namespace_store;
pub mod netplugin_store;
pub mod netpolicy_store;
pub mod persistence;
pub mod queries;
pub mod raft;
pub mod revisions;
pub mod runtime_state_store;
pub mod session_index;
pub mod snapshot_warm;
pub mod store;
pub mod work_log;

pub(crate) use raft::Lane;

/// Runs `work`, which blocks the calling thread (a quorum wait, a commit),
/// without parking a tokio worker when the caller is one.
///
/// On a multi-thread runtime's worker this is `block_in_place`: the worker
/// hands its queue to another thread for the duration, so a burst of
/// blocking calls cannot starve the eval loop's `spawn_blocking` resumes or
/// the heartbeat handlers. On a `spawn_blocking` thread, a plain OS thread,
/// or a current-thread runtime (tests) it runs inline: `block_in_place`
/// panics on a current-thread runtime and is a no-op off a worker.
pub(crate) fn block_off_runtime<T>(work: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        _ => work(),
    }
}

#[cfg(test)]
mod block_off_runtime_test {
    use super::block_off_runtime;
    use std::sync::mpsc;
    use std::time::Duration;

    /// One worker thread: a task that blocks on a reply only another task
    /// can send would deadlock if it parked the worker. Off the runtime, the
    /// second task still runs.
    #[test]
    fn a_blocked_caller_does_not_hold_the_only_worker_test() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let got = runtime.block_on(async {
            let (tx, rx) = mpsc::channel::<u32>();
            let waiter = tokio::spawn(async move {
                block_off_runtime(|| rx.recv_timeout(Duration::from_secs(10)))
            });
            tokio::task::yield_now().await;
            let sender = tokio::spawn(async move { tx.send(7).unwrap() });
            sender.await.unwrap();
            waiter.await.unwrap()
        });
        assert_eq!(got.unwrap(), 7);
    }

    #[test]
    fn current_thread_runtime_runs_inline_test() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(async { block_off_runtime(|| 3) }), 3);
    }
}
