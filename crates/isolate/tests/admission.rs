//! Admission (max_active, max_resident, queue timeout), time slicing across
//! tenants on one shard, and stats.

mod common;

use std::time::{Duration, Instant};

use common::*;
use legion_proto::isolate_host::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_active_refuses_after_the_queue_timeout_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.max_active = 1;
    s.queue_timeout_millis = 100;
    h.task(s);
    let host = h.host.clone();
    let first = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "cmd".into(),
            invocation_id: "a".into(),
            call: Some(invoke_request::Call::Run(run(&["sleep", "800"]))),
            ..Default::default()
        })
        .await
    });
    sleep_ms(150).await;
    let t = Instant::now();
    let e = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
    assert!(e.message().contains("maxActive 1"), "{}", e.message());
    assert!(t.elapsed() >= Duration::from_millis(90));
    let r = first.await.unwrap().unwrap();
    assert_eq!(stdout(&r), "slept\n");
    let st = h.host.stats();
    assert_eq!(st.tasks[0].refusals, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_invocation_runs_when_room_frees_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.max_active = 1;
    s.queue_timeout_millis = 3000;
    h.task(s);
    let host = h.host.clone();
    let first = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "cmd".into(),
            call: Some(invoke_request::Call::Run(run(&["sleep", "300"]))),
            ..Default::default()
        })
        .await
    });
    sleep_ms(50).await;
    let r = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap();
    assert_eq!(exit(&r), Some(0));
    assert!(r.queue_micros >= 150_000, "{}", r.queue_micros);
    first.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_resident_bounds_live_instances_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.max_resident = 1;
    s.queue_timeout_millis = 100;
    h.task(s);
    let host = h.host.clone();
    let first = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "cmd".into(),
            call: Some(invoke_request::Call::Run(run(&["sleep", "600"]))),
            ..Default::default()
        })
        .await
    });
    sleep_ms(150).await;
    let e = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
    assert!(e.message().contains("maxResident 1"), "{}", e.message());
    first.await.unwrap().unwrap();
    // Room again once the first instance is gone.
    let r = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap();
    assert_eq!(exit(&r), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_slots_bound_instances_across_tasks_test() {
    let mut c = config();
    c.slots = 1;
    let h = start(c);
    let mut s = spec("j", "a", "cmd");
    s.queue_timeout_millis = 100;
    h.task(s.clone());
    s.task_name = "b".into();
    h.task(s);
    let host = h.host.clone();
    let first = tokio::spawn(async move {
        host.invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "a".into(),
            call: Some(invoke_request::Call::Run(run(&["sleep", "500"]))),
            ..Default::default()
        })
        .await
    });
    sleep_ms(150).await;
    let e = h
        .run_with("j", "b", "", "", run(&["echo"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
    first.await.unwrap().unwrap();
}

/// A tenant spinning on the shard does not starve another tenant: the
/// spinner's turns are cut at the slice and DRR alternates flows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_slicing_keeps_a_second_tenant_responsive_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    let l = s.limits.as_mut().unwrap();
    l.cpu_micros = 1_500_000;
    l.wall_micros = 5_000_000;
    h.task(s);
    let mut hogs = Vec::new();
    for i in 0..3 {
        let host = h.host.clone();
        hogs.push(tokio::spawn(async move {
            host.invoke(InvokeRequest {
                job_id: "j".into(),
                task_name: "cmd".into(),
                tenant: "hog".into(),
                invocation_id: format!("hog-{i}"),
                call: Some(invoke_request::Call::Run(run(&["spin"]))),
                ..Default::default()
            })
            .await
        }));
    }
    sleep_ms(200).await;
    let mut worst = Duration::ZERO;
    for _ in 0..5 {
        let t = Instant::now();
        let r = h
            .run_with("j", "cmd", "small", "", run(&["echo"]))
            .await
            .unwrap();
        assert_eq!(exit(&r), Some(0));
        worst = worst.max(t.elapsed());
    }
    // One shard, three spinning invocations: the small tenant still gets a
    // turn within a few slices (5 ms each), not after the hogs' 1.5 s.
    assert!(
        worst < Duration::from_millis(150),
        "worst latency {worst:?}"
    );
    for hog in hogs {
        let r = hog.await.unwrap().unwrap();
        assert_eq!(stopped(&r).1, "cpu");
        assert!(r.wall_micros > 1_500_000, "the hogs shared the shard");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_count_invocations_stops_and_starts_test() {
    let mut c = config();
    c.shards = 2;
    let h = start(c);
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().cpu_micros = 50_000;
    h.task(s);
    h.run("j", "cmd", &["echo"]).await;
    h.run("j", "cmd", &["trap"]).await;
    h.run("j", "cmd", &["spin"]).await;
    let st = h.host.stats();
    assert_eq!(st.shards, 2);
    assert_eq!(st.slots, 64);
    assert_eq!(st.slots_used, 0, "request instances are gone");
    assert!(st.rss_bytes > 0);
    assert!(st.started_unix_ms > 0);
    let t = &st.tasks[0];
    assert_eq!(t.invocations, 3);
    assert_eq!(t.instantiations, 3);
    assert!(t.instantiate_micros > 0);
    assert!(t.cpu_micros >= 50_000);
    assert!(t.turns >= 3);
    assert_eq!(t.starts.get("fresh"), Some(&3));
    assert_eq!(t.stopped.get("trap"), Some(&1));
    assert_eq!(t.stopped.get("limits"), Some(&1));
    assert_eq!(t.limits.get("cpu"), Some(&1));
    assert_eq!((t.active, t.queued, t.resident), (0, 0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_task_answers_not_found_after_test() {
    let h = start(config());
    h.task(spec("j", "cmd", "cmd"));
    h.run("j", "cmd", &["echo"]).await;
    h.host
        .remove_task(RemoveTaskRequest {
            job_id: "j".into(),
            task_name: "cmd".into(),
        })
        .await
        .unwrap();
    let e = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    // The module is unused now and can go.
    assert!(
        h.host
            .unload_module(UnloadModuleRequest {
                module_id: "cmd".into()
            })
            .unwrap()
            .unloaded
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_host_refuses_new_invocations_test() {
    let h = start(config());
    h.task(spec("j", "cmd", "cmd"));
    let d = h
        .host
        .drain(DrainRequest {
            hibernate: false,
            grace_millis: 100,
        })
        .await;
    assert_eq!(d.cut, 0);
    let e = h
        .run_with("j", "cmd", "", "", run(&["echo"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
}
