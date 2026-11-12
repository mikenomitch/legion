//! Runner guests across lifetimes: reuse, tenant isolation, state,
//! sessions and their hibernation, reclaim, drain, resize, control, vm.

mod common;

use std::time::Duration;

use common::*;
use legion_proto::isolate_host::*;

fn runner_spec(task: &str, life: Lifetime) -> TaskSpec {
    let mut s = spec("j", task, "runner");
    s.lifetime = life as i32;
    s
}

fn host_with(spec: TaskSpec) -> TestHost {
    let h = start(config());
    h.task(spec);
    h
}

async fn call(h: &TestHost, task: &str, tenant: &str, session: &str, args: &[&str]) -> InvokeReply {
    h.run_with("j", task, tenant, session, run(args))
        .await
        .unwrap_or_else(|e| panic!("{args:?}: {e}"))
}

fn result(r: &InvokeReply) -> String {
    outcome(r).result.clone()
}

async fn open(h: &TestHost, task: &str, session: &str) {
    h.host
        .open_session(OpenSessionRequest {
            job_id: "j".into(),
            task_name: task.into(),
            session_id: session.into(),
            tenant: "acme".into(),
            ..Default::default()
        })
        .unwrap();
}

/// Wait until `session` (parked) has hibernated: its checkpoint is saved
/// and the host has let it go. Hibernation runs after the call returns.
async fn wait_hibernated(h: &TestHost, session: &str) {
    let done = wait_for(
        || {
            h.worker.checkpoints.lock().unwrap().contains_key(session)
                && h.host.get_session(session).is_err()
        },
        |done| *done,
    )
    .await;
    assert!(done, "{session} did not hibernate");
}

/// Wait until `session` has a call running.
async fn wait_running(h: &TestHost, session: &str) {
    let active = wait_for(
        || {
            h.host
                .get_session(session)
                .map(|i| i.active)
                .unwrap_or(false)
        },
        |a| *a,
    )
    .await;
    assert!(active, "no call of {session} started");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_lifetime_never_reuses_an_instance_test() {
    let h = host_with(runner_spec("r", Lifetime::Request));
    for _ in 0..2 {
        let r = call(&h, "r", "t", "", &["count"]).await;
        assert_eq!(result(&r), "{\"count\":1}");
        assert_eq!(r.start, "fresh");
    }
    // A request isolate has no state.
    let r = call(&h, "r", "t", "", &["set", "k", "v"]).await;
    assert_eq!(exit(&r), Some(2));
    assert!(String::from_utf8_lossy(&r.stderr).contains("NotAvailable"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_lifetime_reuses_per_tenant_and_isolates_tenants_test() {
    let h = host_with(runner_spec("t", Lifetime::Tenant));
    assert_eq!(
        result(&call(&h, "t", "a", "", &["count"]).await),
        "{\"count\":1}"
    );
    let r = call(&h, "t", "a", "", &["count"]).await;
    assert_eq!(result(&r), "{\"count\":2}");
    assert_eq!(r.start, "warm");
    assert_eq!(r.instantiate_micros, 0);
    // Another tenant: its own instance and its own state.
    assert_eq!(
        result(&call(&h, "t", "b", "", &["count"]).await),
        "{\"count\":1}"
    );
    call(&h, "t", "a", "", &["set", "k", "from-a"]).await;
    assert_eq!(
        result(&call(&h, "t", "a", "", &["get", "k"]).await),
        "\"from-a\""
    );
    assert_eq!(result(&call(&h, "t", "b", "", &["get", "k"]).await), "\"\"");
    let info = result(&call(&h, "t", "b", "", &["info"]).await);
    assert!(info.contains("\"tenant\":\"b\""), "{info}");
    assert!(info.contains("Tenant"), "{info}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_tenant_instances_are_dropped_test() {
    let mut s = runner_spec("t", Lifetime::Tenant);
    s.tenant_idle_millis = 100;
    let h = host_with(s);
    call(&h, "t", "a", "", &["count"]).await;
    // Dropped once idle past 100 ms: wait for it rather than sleep.
    let resident = wait_for(|| h.host.stats().tasks[0].resident, |n| *n == 0).await;
    assert_eq!(resident, 0, "the idle tenant instance was dropped");
    let r = call(&h, "t", "a", "", &["count"]).await;
    assert_eq!(result(&r), "{\"count\":1}");
    assert_eq!(r.start, "fresh");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_quota_is_enforced_test() {
    let h = host_with(runner_spec("t", Lifetime::Tenant));
    // Quota is 1 KiB in `spec`.
    let big = "x".repeat(2000);
    let r = call(&h, "t", "a", "", &["set", "k", &big]).await;
    assert_eq!(exit(&r), Some(2));
    assert!(String::from_utf8_lossy(&r.stderr).contains("Quota(1024)"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_run_dir_is_fresh_per_call_and_entry_input_reach_the_runner_test() {
    let mut s = runner_spec("t", Lifetime::Tenant);
    s.entry = Some(File {
        path: "main.py".into(),
        content: b"print(1)".to_vec(),
        ..Default::default()
    });
    let h = host_with(s);
    for _ in 0..2 {
        let mut c = run(&["entry"]);
        c.input = Some("{\"n\":2}".into());
        let r = h.run_with("j", "t", "a", "", c).await.unwrap();
        let res = result(&r);
        assert!(res.contains("\"entry\":\"main.py\""), "{res}");
        assert!(res.contains("\"body\":\"print(1)\""), "{res}");
        assert!(res.contains("\"input\":\"{\\\"n\\\":2}\""), "{res}");
        assert!(res.contains("\"leftover\":false"), "{res}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runner_exit_status_and_unsupported_test() {
    let h = host_with(runner_spec("t", Lifetime::Tenant));
    let r = call(&h, "t", "a", "", &["exit", "7"]).await;
    assert_eq!(exit(&r), Some(7));
    let r = call(&h, "t", "a", "", &["unsupported"]).await;
    let (reason, _, detail) = stopped(&r);
    assert_eq!(reason, "unsupported");
    assert_eq!(detail, "subprocess.run: an isolate cannot start processes");
    assert_eq!(stdout(&r), "ran unsupported\n");
    // The stopped instance was dropped: the next call is fresh.
    let r = call(&h, "t", "a", "", &["count"]).await;
    assert_eq!(
        (result(&r).as_str(), r.start.as_str()),
        ("{\"count\":1}", "fresh")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_hibernates_on_park_and_wakes_with_its_state_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    open(&h, "s", "sess-1").await;
    let info = h.host.get_session("sess-1").unwrap();
    assert_eq!(info.tier, "empty");
    call(&h, "s", "acme", "sess-1", &["set", "k", "kept"]).await;
    let r = call(&h, "s", "acme", "sess-1", &["count"]).await;
    assert_eq!(
        (result(&r).as_str(), r.start.as_str()),
        ("{\"count\":1}", "warm")
    );
    let info = h.host.get_session("sess-1").unwrap();
    assert_eq!((info.tier.as_str(), info.invocations), ("hot", 2));
    assert_eq!(info.state_bytes, 5);
    call(&h, "s", "acme", "sess-1", &["park"]).await;
    // Hibernation happens after the call returns. Done when the
    // checkpoint is saved and the host has let go.
    wait_for(
        || {
            h.worker.checkpoints.lock().unwrap().contains_key("sess-1")
                && h.host.get_session("sess-1").is_err()
        },
        |done| *done,
    )
    .await;
    let (header, _) = h.worker.checkpoints.lock().unwrap()["sess-1"].clone();
    assert_eq!(header.module_id, "runner");
    assert!(header.engine_version.starts_with("wasmtime-"));
    assert!(header.origin.as_ref().unwrap().session);
    assert_eq!(
        h.host.get_session("sess-1").unwrap_err().code(),
        tonic::Code::NotFound,
        "the host forgets a hibernated session"
    );
    let r = call(&h, "s", "acme", "sess-1", &["get", "k"]).await;
    assert_eq!(result(&r), "\"kept\"");
    assert_eq!(r.start, "hibernated");
    // Linear memory did not survive; `resumed` is true on the first call
    // after waking only.
    let r = call(&h, "s", "acme", "sess-1", &["count"]).await;
    assert_eq!(result(&r), "{\"count\":1}");
    let mut r = call(&h, "s", "acme", "sess-1", &["info"]).await;
    assert!(result(&r).contains("\"resumed\":false"), "{}", result(&r));
    assert!(result(&r).contains("\"session\":\"sess-1\""));
    // A fresh wake reports resumed.
    call(&h, "s", "acme", "sess-1", &["park"]).await;
    wait_hibernated(&h, "sess-1").await;
    r = call(&h, "s", "acme", "sess-1", &["info"]).await;
    assert!(result(&r).contains("\"resumed\":true"), "{}", result(&r));
    assert_eq!(h.host.stats().tasks[0].hibernations, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_session_hibernates_after_its_idle_time_test() {
    let mut s = runner_spec("s", Lifetime::Session);
    s.hibernate_after_millis = 100;
    let h = host_with(s);
    open(&h, "s", "sess-idle").await;
    call(&h, "s", "acme", "sess-idle", &["set", "a", "1"]).await;
    // Hibernated once idle past 100 ms: wait for the checkpoint.
    assert!(
        wait_for(
            || h.worker
                .checkpoints
                .lock()
                .unwrap()
                .contains_key("sess-idle"),
            |saved| *saved,
        )
        .await
    );
    let r = call(&h, "s", "acme", "sess-idle", &["get", "a"]).await;
    assert_eq!(
        (result(&r).as_str(), r.start.as_str()),
        ("\"1\"", "hibernated")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_during_hibernation_wakes_from_the_checkpoint_test() {
    use std::sync::atomic::Ordering;
    let mut s = runner_spec("s", Lifetime::Session);
    s.hibernate_after_millis = 100;
    let h = host_with(s);
    // The checkpoint takes a while to keep, so the call below lands while
    // the session is between "checkpoint asked for" and "let go".
    *h.worker.save_delay.lock().unwrap() = Some(Duration::from_millis(500));
    open(&h, "s", "sess-mid").await;
    call(&h, "s", "acme", "sess-mid", &["set", "a", "1"]).await;
    assert_eq!(
        wait_for(|| h.worker.saves_started.load(Ordering::SeqCst), |n| *n > 0).await,
        1
    );
    assert!(!h
        .worker
        .checkpoints
        .lock()
        .unwrap()
        .contains_key("sess-mid"));
    // The call waits for the hibernation, then wakes the session from its
    // checkpoint: its state is there, not a fresh instance's.
    let r = call(&h, "s", "acme", "sess-mid", &["get", "a"]).await;
    assert_eq!(
        (result(&r).as_str(), r.start.as_str()),
        ("\"1\"", "hibernated")
    );
    assert_eq!(h.host.stats().tasks[0].hibernations, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_hibernates_sessions_and_drops_tenants_coldest_first_test() {
    let mut c = config();
    c.shards = 2;
    let h = start(c);
    h.task(runner_spec("s", Lifetime::Session));
    h.host
        .configure_task(runner_spec("t", Lifetime::Tenant))
        .unwrap();
    open(&h, "s", "s1").await;
    call(&h, "s", "acme", "s1", &["set", "x", "y"]).await;
    call(&h, "t", "a", "", &["count"]).await;
    let r = h
        .host
        .reclaim(ReclaimRequest {
            level: ReclaimLevel::ReclaimHibernate as i32,
            target_bytes: 0,
            min_idle_millis: 0,
            ..Default::default()
        })
        .await;
    assert_eq!((r.hibernated, r.dropped), (1, 1));
    assert!(r.bytes > 0);
    let r = call(&h, "t", "a", "", &["count"]).await;
    assert_eq!(r.start, "fresh");
    let r = call(&h, "s", "acme", "s1", &["get", "x"]).await;
    assert_eq!(
        (result(&r).as_str(), r.start.as_str()),
        ("\"y\"", "hibernated")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_out_marks_idle_instances_and_reports_paged_starts_test() {
    let h = host_with(runner_spec("t", Lifetime::Tenant));
    call(&h, "t", "a", "", &["count"]).await;
    call(&h, "t", "b", "", &["count"]).await;
    sleep_ms(50).await;
    let r = h
        .host
        .reclaim(ReclaimRequest {
            level: ReclaimLevel::ReclaimPageOut as i32,
            target_bytes: 1,
            min_idle_millis: 10,
            ..Default::default()
        })
        .await;
    // One instance already meets the 1-byte target.
    assert_eq!(r.paged_out, 1);
    assert!(r.bytes > 0);
    assert_eq!(h.host.stats().tasks[0].paged_out, 1);
    // Paging out keeps the instance: its counter survives.
    let ra = call(&h, "t", "a", "", &["count"]).await;
    let rb = call(&h, "t", "b", "", &["count"]).await;
    assert_eq!(result(&ra), "{\"count\":2}");
    assert_eq!(result(&rb), "{\"count\":2}");
    let starts = [ra.start.as_str(), rb.start.as_str()];
    assert!(
        starts.contains(&"paged") && starts.contains(&"warm"),
        "{starts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_hibernates_every_session_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    for id in ["d1", "d2"] {
        open(&h, "s", id).await;
        call(&h, "s", "acme", id, &["set", "k", id]).await;
    }
    let r = h
        .host
        .drain(DrainRequest {
            hibernate: true,
            grace_millis: 100,
        })
        .await;
    assert_eq!((r.hibernated, r.cut), (2, 0));
    assert_eq!(h.worker.checkpoints.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_cuts_what_outlives_the_grace_test() {
    let mut s = runner_spec("s", Lifetime::Session);
    s.limits.as_mut().unwrap().cpu_micros = 10_000_000;
    let h = host_with(s);
    open(&h, "s", "busy").await;
    let host = h.host.clone();
    let spin = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "s".into(),
            session_id: "busy".into(),
            call: Some(invoke_request::Call::Run(run(&["spin"]))),
            ..Default::default()
        })
        .await
    });
    wait_running(&h, "busy").await;
    let r = h
        .host
        .drain(DrainRequest {
            hibernate: false,
            grace_millis: 50,
        })
        .await;
    assert_eq!(r.cut, 1);
    let r = spin.await.unwrap().unwrap();
    let (reason, _, detail) = stopped(&r);
    assert_eq!(reason, "ended");
    assert!(detail.contains("drained"), "{detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_session_ends_it_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    open(&h, "s", "c1").await;
    call(&h, "s", "acme", "c1", &["count"]).await;
    assert!(h.host.close_session("c1", "done").await);
    assert!(!h.host.close_session("c1", "done").await);
    let e = h
        .run_with("j", "s", "acme", "c1", run(&["count"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_a_session_cuts_its_running_call_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    open(&h, "s", "c2").await;
    let host = h.host.clone();
    let spin = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "s".into(),
            session_id: "c2".into(),
            call: Some(invoke_request::Call::Run(run(&["spin"]))),
            ..Default::default()
        })
        .await
    });
    wait_running(&h, "c2").await;
    assert!(h.host.close_session("c2", "user asked").await);
    let r = spin.await.unwrap().unwrap();
    let (reason, _, detail) = stopped(&r);
    assert_eq!(reason, "ended");
    assert_eq!(detail, "its session was closed: user asked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_session_without_a_checkpoint_is_not_found_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    let e = h
        .run_with("j", "s", "", "nope", run(&["count"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resize_session_grows_shrinks_and_never_takes_memory_back_test() {
    let mut s = runner_spec("s", Lifetime::Session);
    s.memory_max_bytes = 32 << 20;
    s.memory_reservation_bytes = 1 << 20;
    s.max_resident = 3;
    s.queue_timeout_millis = 100;
    let h = host_with(s);
    open(&h, "s", "rz").await;
    call(&h, "s", "acme", "rz", &["count"]).await;
    let resize = |max: u64, res: u64| ResizeSessionRequest {
        session_id: "rz".into(),
        memory_max_bytes: max,
        memory_reservation_bytes: res,
    };
    // Grow.
    let r = h.host.resize_session(resize(96 << 20, 0)).await.unwrap();
    assert_eq!(
        (r.memory_max_bytes, r.memory_reservation_bytes),
        (96 << 20, 1 << 20)
    );
    // Shrink, still above what it uses.
    let r = h.host.resize_session(resize(8 << 20, 0)).await.unwrap();
    assert_eq!(r.memory_max_bytes, 8 << 20);
    // Below what it uses: refused.
    let e = h
        .host
        .resize_session(resize(64 << 10, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("never"), "{}", e.message());
    // Above the slot: refused.
    let e = h.host.resize_session(resize(1 << 40, 0)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let e = h
        .host
        .resize_session(ResizeSessionRequest {
            session_id: "ghost".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    // Resident accounting uses the session's reservation: 3 MiB of room,
    // this session now holds 2, so a second session (1 MiB) fits and a
    // third does not.
    h.host.resize_session(resize(0, 2 << 20)).await.unwrap();
    open(&h, "s", "rz2").await;
    open(&h, "s", "rz3").await;
    call(&h, "s", "acme", "rz2", &["count"]).await;
    let e = h
        .run_with("j", "s", "acme", "rz3", run(&["count"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
    // The size survives hibernation through the checkpoint.
    call(&h, "s", "acme", "rz", &["park"]).await;
    wait_hibernated(&h, "rz").await;
    call(&h, "s", "acme", "rz", &["count"]).await;
    let e = h
        .host
        .resize_session(resize(64 << 10, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    let r = h.host.resize_session(resize(0, 0)).await.unwrap();
    assert_eq!(
        (r.memory_max_bytes, r.memory_reservation_bytes),
        (8 << 20, 2 << 20)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_session_takes_its_own_size_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    h.host
        .open_session(OpenSessionRequest {
            job_id: "j".into(),
            task_name: "s".into(),
            session_id: "own".into(),
            memory_max_bytes: 4 << 20,
            memory_reservation_bytes: 3 << 20,
            ..Default::default()
        })
        .unwrap();
    call(&h, "s", "", "own", &["count"]).await;
    let r = h
        .host
        .resize_session(ResizeSessionRequest {
            session_id: "own".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        (r.memory_max_bytes, r.memory_reservation_bytes),
        (4 << 20, 3 << 20)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vm_calls_go_to_the_worker_with_the_origin_test() {
    let mut s = runner_spec("t", Lifetime::Tenant);
    s.has_vm = true;
    let h = host_with(s);
    let r = call(&h, "t", "acme", "", &["vm"]).await;
    assert_eq!(
        result(&r),
        "{\"id\":\"child-1\",\"stdout\":\"ran echo hi\"}"
    );
    let seen = h.worker.vm_seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    let o = seen[0].origin.as_ref().unwrap();
    assert_eq!(
        (o.job_id.as_str(), o.task_name.as_str(), o.tenant.as_str()),
        ("j", "t", "acme")
    );
    match &seen[1].op {
        Some(vm_request::Op::Exec(e)) => {
            assert_eq!(e.id, "child-1");
            assert_eq!(e.argv, ["echo", "hi"]);
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vm_is_not_configured_without_a_vm_task_test() {
    let h = host_with(runner_spec("t", Lifetime::Tenant));
    let r = call(&h, "t", "acme", "", &["vm"]).await;
    assert_eq!(exit(&r), Some(3));
    assert!(String::from_utf8_lossy(&r.stderr).contains("NotConfigured"));
    assert!(h.worker.vm_seen.lock().unwrap().is_empty());
    let _ = Duration::ZERO;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_can_name_sessions_and_skips_a_busy_one_test() {
    let mut s = runner_spec("s", Lifetime::Session);
    s.limits.as_mut().unwrap().cpu_micros = 10_000_000;
    let h = host_with(s);
    for id in ["n1", "n2", "n3", "busy"] {
        open(&h, "s", id).await;
        call(&h, "s", "acme", id, &["set", "k", id]).await;
    }
    let host = h.host.clone();
    let spin = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "s".into(),
            session_id: "busy".into(),
            call: Some(invoke_request::Call::Run(run(&["spin"]))),
            ..Default::default()
        })
        .await
    });
    wait_running(&h, "busy").await;
    let r = h
        .host
        .reclaim(ReclaimRequest {
            level: ReclaimLevel::ReclaimHibernate as i32,
            // Named sessions ignore the idle threshold.
            min_idle_millis: 60_000,
            session_ids: vec!["n1".into(), "n3".into(), "busy".into()],
            ..Default::default()
        })
        .await;
    assert_eq!(
        r.hibernated, 2,
        "n1 and n3; busy is skipped and not counted"
    );
    let saved = h.worker.checkpoints.lock().unwrap().clone();
    assert!(saved.contains_key("n1") && saved.contains_key("n3"));
    assert!(!saved.contains_key("n2") && !saved.contains_key("busy"));
    assert_eq!(h.host.get_session("n2").unwrap().tier, "hot");
    assert!(h.host.get_session("busy").unwrap().active);
    h.host.close_session("busy", "").await;
    let _ = spin.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_can_be_limited_to_one_job_or_task_test() {
    let h = start(config());
    h.load("runner");
    for (job, task) in [("j", "t"), ("j", "u"), ("k", "t")] {
        let mut s = runner_spec(task, Lifetime::Tenant);
        s.job_id = job.into();
        h.host.configure_task(s).unwrap();
        h.run_with(job, task, "a", "", run(&["count"]))
            .await
            .unwrap();
    }
    let reclaim = |job: &str, task: &str| ReclaimRequest {
        level: ReclaimLevel::ReclaimHibernate as i32,
        job_id: job.into(),
        task_name: task.into(),
        ..Default::default()
    };
    let r = h.host.reclaim(reclaim("j", "t")).await;
    assert_eq!(r.dropped, 1);
    let r = h.host.reclaim(reclaim("j", "")).await;
    assert_eq!(r.dropped, 1, "only j/u was left in job j");
    // Job k's instance is untouched: still warm.
    let r = h
        .run_with("k", "t", "a", "", run(&["count"]))
        .await
        .unwrap();
    assert_eq!(r.start, "warm");
    let r = h
        .run_with("j", "t", "a", "", run(&["count"]))
        .await
        .unwrap();
    assert_eq!(r.start, "fresh");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_keeps_its_own_tenant_whatever_a_call_names_test() {
    let h = host_with(runner_spec("s", Lifetime::Session));
    open(&h, "s", "sess-t").await; // opened for tenant "acme"
                                   // A caller who knows the session id names another tenant: the call
                                   // runs (routing is by session), attributed to the session's tenant.
    let r = call(&h, "s", "evil", "sess-t", &["info"]).await;
    assert!(result(&r).contains("\"tenant\":\"acme\""), "{}", result(&r));
    // Also after the instance is made again from a checkpoint.
    call(&h, "s", "evil", "sess-t", &["park"]).await;
    wait_hibernated(&h, "sess-t").await;
    let r = call(&h, "s", "evil", "sess-t", &["info"]).await;
    assert!(result(&r).contains("\"tenant\":\"acme\""), "{}", result(&r));
    assert!(result(&r).contains("\"resumed\":true"), "{}", result(&r));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifetime_refusal_names_the_jobspec_word_test() {
    let h = start(config());
    h.load("cmd");
    let mut s = spec("j", "c", "cmd");
    s.lifetime = Lifetime::Tenant as i32;
    let e = h.host.configure_task(s).unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(
        e.message().contains("lifetime \"tenant\""),
        "{}",
        e.message()
    );
    assert!(!e.message().contains("LIFETIME_"), "{}", e.message());
}
