//! Unit tests for revisions on replies, blocking reads, `Watch`, `RunJob`
//! compare-and-set, `PlanJob` diffs, and the heartbeat offload
//! (revisions-and-watches plan), against a `local_disk` manager.

use super::*;
use crate::storage::revisions::Revision;
use crate::structs::ManagerConfigFull;
use crate::structs::{
    AllocationHealth, HeartbeatRequest, NodeMsg, PlanJobRequest, PutKvRequest, WatchKind,
    WatchRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn setup() -> R<(Store, ManagerDaemonService)> {
    let dir = tempfile::Builder::new()
        .prefix("legion-revisions-")
        .tempdir_in(crate::test_scratch::root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    let manager = ManagerDaemonService::new(store.clone());
    Ok((store, manager))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

async fn put(manager: &ManagerDaemonService, key: &str, value: &str) -> String {
    manager
        .put_kv(Request::new(PutKvRequest {
            key: key.into(),
            value: value.into(),
            value_type: KvValueType::String as i32,
            expected_version: None,
            lock_id: None,
        }))
        .await
        .expect("put")
        .into_inner()
        .revision
}

fn blocking<T>(inner: T, min_revision: &str, wait_seconds: u32) -> Request<T> {
    let mut request = Request::new(inner);
    request
        .metadata_mut()
        .insert(MIN_REVISION_HEADER, min_revision.parse().unwrap());
    request
        .metadata_mut()
        .insert(WAIT_HEADER, wait_seconds.to_string().parse().unwrap());
    request
}

fn get_request(key: &str) -> GetKvRequest {
    GetKvRequest {
        key: key.into(),
        metadata_only: false,
    }
}

#[test]
fn writes_move_the_revision_and_reads_report_it() -> R<()> {
    let (_store, manager) = setup()?;
    let rt = runtime();
    rt.block_on(async {
        let first = put(&manager, "app/a", "1").await;
        let second = put(&manager, "app/a", "2").await;
        let first = Revision::parse(&first).expect("a revision");
        let second = Revision::parse(&second).expect("a revision");
        assert!(
            second.covers(&first) && second != first,
            "{first} -> {second}"
        );
        let read = manager
            .get_kv(Request::new(get_request("app/a")))
            .await
            .expect("get")
            .into_inner();
        assert_eq!(Revision::parse(&read.revision).unwrap(), second);
        assert_eq!(read.entry.expect("entry").value, "2");
    });
    Ok(())
}

#[test]
fn a_blocking_read_returns_on_a_change_and_ignores_other_keys() -> R<()> {
    let (_store, manager) = setup()?;
    let manager = Arc::new(manager);
    let rt = runtime();
    rt.block_on(async {
        let at = put(&manager, "app/watched", "1").await;
        let started = Instant::now();
        let waiter = {
            let manager = Arc::clone(&manager);
            let at = at.clone();
            tokio::spawn(async move {
                manager
                    .get_kv(blocking(get_request("app/watched"), &at, 30))
                    .await
                    .expect("blocking get")
                    .into_inner()
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        // A write to another key does not wake it.
        put(&manager, "app/other", "x").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !waiter.is_finished(),
            "a change to another key woke the read"
        );
        put(&manager, "app/watched", "2").await;
        let reply = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("the read returned after the change")
            .expect("task");
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(reply.entry.expect("entry").value, "2");
        assert!(Revision::parse(&reply.revision)
            .unwrap()
            .covers(&Revision::parse(&at).unwrap()));
    });
    Ok(())
}

#[test]
fn a_blocking_read_times_out_with_the_current_state() -> R<()> {
    let (_store, manager) = setup()?;
    let rt = runtime();
    rt.block_on(async {
        let at = put(&manager, "quiet", "1").await;
        let started = Instant::now();
        let reply = manager
            .get_kv(blocking(get_request("quiet"), &at, 1))
            .await
            .expect("get")
            .into_inner();
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(900), "{waited:?}");
        assert!(waited < Duration::from_secs(10), "{waited:?}");
        assert!(
            Revision::parse(&reply.revision)
                .unwrap()
                .covers(&Revision::parse(&at).unwrap()),
            "a timeout answers with the current revision"
        );
        assert_eq!(reply.entry.expect("entry").value, "1");

        // A malformed revision is the caller's mistake.
        let err = manager
            .get_kv(blocking(get_request("quiet"), "not-a-revision", 1))
            .await
            .expect_err("rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    });
    Ok(())
}

async fn next_event(
    stream: &mut ReceiverStream<Result<WatchEvent, Status>>,
) -> crate::structs::WatchEvent {
    use tokio_stream::StreamExt;
    tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("a watch message in time")
        .expect("the stream is open")
        .expect("not an error")
}

use crate::structs::WatchEvent;

fn kv_watch(prefix: &str, from: &str) -> Request<WatchRequest> {
    Request::new(WatchRequest {
        kind: WatchKind::Kv as i32,
        prefix: prefix.into(),
        from_revision: from.into(),
        ..Default::default()
    })
}

#[test]
fn a_watch_streams_changes_and_resumes_without_resending() -> R<()> {
    let (_store, manager) = setup()?;
    let rt = runtime();
    rt.block_on(async {
        let mut stream = manager
            .watch(kv_watch("app/", ""))
            .await
            .expect("watch")
            .into_inner();
        let opening = next_event(&mut stream).await;
        assert!(opening.progress, "the first message says where it starts");

        put(&manager, "other/x", "ignored").await;
        put(&manager, "app/a", "1").await;
        let event = next_event(&mut stream).await;
        assert!(!event.progress);
        assert_eq!(event.changes.len(), 1, "{event:?}");
        let change = &event.changes[0];
        assert_eq!(change.key, "app/a");
        assert!(!change.deleted);
        assert_eq!(change.kv.as_ref().expect("the entry").value, "1");
        let resume_at = event.revision.clone();
        drop(stream);

        // Changes while disconnected: one update and one delete.
        put(&manager, "app/b", "2").await;
        manager
            .delete_kv(Request::new(DeleteKvRequest {
                key: "app/a".into(),
                expected_version: None,
                lock_id: None,
            }))
            .await
            .expect("delete");

        let mut resumed = manager
            .watch(kv_watch("app/", &resume_at))
            .await
            .expect("resume")
            .into_inner();
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let event = next_event(&mut resumed).await;
            for change in event.changes {
                seen.push((change.key.clone(), change.deleted));
            }
        }
        seen.sort();
        assert_eq!(
            seen,
            vec![("app/a".to_string(), true), ("app/b".to_string(), false)],
            "everything after the resume revision, nothing before it"
        );
    });
    Ok(())
}

#[test]
fn a_watch_from_before_the_journal_must_resync() -> R<()> {
    let (store, manager) = setup()?;
    let rt = runtime();
    rt.block_on(async {
        let old = put(&manager, "app/a", "1").await;
        put(&manager, "app/a", "2").await;
        // What a snapshot install does to a manager's journal.
        store.reset_revisions();
        let mut stream = manager
            .watch(kv_watch("app/", &old))
            .await
            .expect("watch")
            .into_inner();
        let event = next_event(&mut stream).await;
        assert!(event.resync_required, "{event:?}");
        assert_eq!(
            Revision::parse(&event.oldest_revision).unwrap(),
            store.current_revision().unwrap()
        );
        use tokio_stream::StreamExt;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("the stream ends")
                .is_none(),
            "nothing follows a resync-required answer"
        );
    });
    Ok(())
}

fn spec(count: u32) -> String {
    json!({
        "name": "cas",
        "task": {
            "name": "web",
            "runtime": "exec",
            "count": count,
            "config": { "command": ["echo", "hi"] }
        }
    })
    .to_string()
}

fn run(spec: String, expected_version: Option<i64>) -> Request<RunJobRequest> {
    Request::new(RunJobRequest {
        parsed_jobspec: spec,
        expected_version,
        ..Default::default()
    })
}

#[test]
fn run_job_compare_and_set_wins_loses_and_creates_only() -> R<()> {
    let (_store, manager) = setup()?;
    let rt = runtime();
    rt.block_on(async {
        // Must not exist: creates version 1.
        let created = manager
            .run_job(run(spec(1), Some(0)))
            .await
            .expect("create")
            .into_inner();
        assert_eq!(created.version, 1);
        assert!(!created.revision.is_empty());

        // Must not exist, again: refused, naming the version.
        let err = manager
            .run_job(run(spec(2), Some(0)))
            .await
            .expect_err("the job exists");
        assert_eq!(err.code(), tonic::Code::Aborted, "{err:?}");
        assert!(
            err.message().contains("already exists (version 1)"),
            "{err:?}"
        );

        // At the right version: version 2.
        let updated = manager
            .run_job(run(spec(2), Some(1)))
            .await
            .expect("update")
            .into_inner();
        assert_eq!(updated.version, 2);

        // A writer still holding version 1 loses, and nothing is written.
        let err = manager
            .run_job(run(spec(3), Some(1)))
            .await
            .expect_err("stale");
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert!(err.message().contains("is at version 2, not 1"), "{err:?}");
        let info = manager
            .get_job_info(Request::new(GetJobInfoRequest { name: "cas".into() }))
            .await
            .expect("info")
            .into_inner();
        assert_eq!(
            info.job.expect("job").version,
            2,
            "the losing write wrote nothing"
        );

        // An unchanged resubmit at the right version is fine.
        let unchanged = manager
            .run_job(run(spec(2), Some(2)))
            .await
            .expect("unchanged")
            .into_inner();
        assert_eq!(unchanged.version, 2);

        // A version for a job that does not exist.
        let err = manager
            .run_job(run(spec(1).replace("\"cas\"", "\"absent\""), Some(4)))
            .await
            .expect_err("no such job");
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert!(err.message().contains("does not exist"), "{err:?}");

        // No expected version: as before.
        let plain = manager
            .run_job(run(spec(5), None))
            .await
            .expect("plain")
            .into_inner();
        assert_eq!(plain.version, 3);
    });
    Ok(())
}

#[test]
fn plan_job_lists_the_fields_a_submit_would_change() -> R<()> {
    let (store, manager) = setup()?;
    // A node to plan onto: a plan with nowhere to place is an error.
    store.insert_node(NodeMsg {
        id: "plan-node".into(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        config_attrs: json!({ "node_id": "plan-node", "maxAllocations": 50, "resources": {} })
            .to_string(),
        fingerprint_attrs: json!({
            "memory_total": (8u64 * 1024 * 1024 * 1024).to_string(),
            "cpus": { "cpu-1": { "frequency": 2000.0 }, "cpu-2": { "frequency": 2000.0 } },
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    })?;
    let rt = runtime();
    rt.block_on(async {
        manager.run_job(run(spec(1), None)).await.expect("create");
        let plan = |count| PlanJobRequest {
            parsed_jobspec: spec(count),
            ..Default::default()
        };
        let changed = manager
            .plan_job(Request::new(plan(3)))
            .await
            .expect("plan")
            .into_inner();
        assert!(!changed.same_content);
        let paths: Vec<&str> = changed.diff.iter().map(|c| c.path.as_str()).collect();
        assert!(
            changed
                .diff
                .iter()
                .any(|c| c.path.starts_with("tasks.web.count") && c.change == "changed"),
            "{paths:?}"
        );
        assert!(
            changed.diff.iter().all(|c| !c.path.starts_with("version")),
            "the stored version is not a change: {paths:?}"
        );
        let same = manager
            .plan_job(Request::new(plan(1)))
            .await
            .expect("plan")
            .into_inner();
        assert!(same.same_content);
        assert!(same.diff.is_empty());
    });
    Ok(())
}

/// ST5: the heartbeat's report writes run on the blocking pool. With the
/// write connection held elsewhere, a beat that reports allocation health
/// waits for it — and a ticker on the same single-threaded runtime keeps
/// ticking the whole time, which an inline write would have stopped.
#[test]
fn the_heartbeat_does_not_block_the_runtime_on_its_report_writes() -> R<()> {
    let (store, manager) = setup()?;
    store.insert_node(NodeMsg {
        id: "hb-node".into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    })?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (held, holding) = std::sync::mpsc::channel();
    let hold = Duration::from_millis(600);
    let holder = {
        let store = store.clone();
        std::thread::spawn(move || {
            let guard = store.write_connection().expect("write connection");
            held.send(()).expect("signal");
            std::thread::sleep(hold);
            drop(guard);
        })
    };
    holding.recv()?;
    let started = Instant::now();
    let (reply, ticks) = rt.block_on(async {
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = {
            let ticks = Arc::clone(&ticks);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    ticks.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let reply = manager
            .heartbeat(Request::new(HeartbeatRequest {
                id: "hb-node".into(),
                allocation_health: vec![AllocationHealth {
                    allocation_id: 42,
                    healthy: true,
                    status: "running".into(),
                    artifact_states: None,
                }],
                ..Default::default()
            }))
            .await;
        ticker.abort();
        (reply, ticks.load(Ordering::Relaxed))
    });
    let took = started.elapsed();
    holder.join().expect("holder");
    reply.map_err(|status| anyhow!("heartbeat failed: {status:?}"))?;
    assert!(
        took >= Duration::from_millis(400),
        "the heartbeat's health write never waited for the held connection ({took:?}), so \
         this test proved nothing"
    );
    assert!(
        ticks >= 20,
        "the runtime stalled while the heartbeat's writes waited: {ticks} ticks in {hold:?}"
    );
    Ok(())
}
