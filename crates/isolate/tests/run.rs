//! Runs of a `wasi:cli/run` command in an in-process host: files, input,
//! results, exit codes, every limit, clocks, randomness, and loading.

mod common;

use std::time::{Duration, Instant};

use common::*;
use legion_proto::isolate_host::*;

fn cmd_task() -> TestHost {
    let h = start(config());
    h.task(spec("j", "cmd", "cmd"));
    h
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_sees_args_env_input_and_the_run_dir_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.env.insert("FOO".into(), "bar".into());
    h.task(s);
    let mut call = run(&["echo", "x"]);
    call.input = Some("{\"a\":1}".into());
    let r = h.run_with("j", "cmd", "", "", call).await.unwrap();
    let out = stdout(&r);
    assert_eq!(exit(&r), Some(0), "{out}");
    assert!(out.contains("args=echo,x"), "{out}");
    assert!(out.contains("env FOO=bar"), "{out}");
    assert!(out.contains("env LEGION_RUN_DIR=/legion/run"), "{out}");
    assert!(
        out.contains("env LEGION_RUN_INPUT=/legion/run/input.json"),
        "{out}"
    );
    assert!(out.contains("cwd=/legion/run"), "{out}");
    assert!(out.contains("input={\"a\":1}"), "{out}");
    assert_eq!(r.stderr, b"to stderr\n");
    assert_eq!(r.start, "fresh");
    assert!(r.instantiate_micros > 0);
    assert!(r.memory_peak_bytes > 0);
    assert!(r.stopped.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_in_result_json_and_get_files_out_test() {
    let h = cmd_task();
    let mut call = run(&["files"]);
    call.files.push(File {
        path: "in.txt".into(),
        content: b"hello".to_vec(),
        ..Default::default()
    });
    call.get = vec!["data/out.txt".into(), "missing.txt".into()];
    let r = h.run_with("j", "cmd", "", "", call).await.unwrap();
    let o = outcome(&r);
    assert_eq!(o.exit, Some(0), "{:?}", String::from_utf8_lossy(&r.stderr));
    assert_eq!(o.result, "{\"len\":5}");
    assert_eq!(o.files[0].content, b"HELLO");
    assert!(!o.files[0].missing);
    assert!(o.files[1].missing);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_json_that_is_not_json_is_dropped_test() {
    let h = cmd_task();
    let r = h.run("j", "cmd", &["bad-result"]).await;
    assert_eq!(outcome(&r).result, "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returned_files_share_the_output_budget_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().output_bytes = 1000;
    h.task(s);
    let mut call = run(&["files"]);
    call.files.push(File {
        path: "in.txt".into(),
        content: b"hi".to_vec(),
        ..Default::default()
    });
    call.get = vec!["big.bin".into()];
    let r = h.run_with("j", "cmd", "", "", call).await.unwrap();
    let f = &outcome(&r).files[0];
    // "wrote\n" took 6 bytes of the 1000.
    assert_eq!(f.content.len(), 994);
    assert!(f.truncated);
    assert!(r.truncated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_codes_are_reported_test() {
    let h = cmd_task();
    // WASI 0.2's `exit` carries only ok or error, and Rust's std uses it for
    // every nonzero code, so 3 and 42 both arrive as 1.
    for (code, seen) in [(0, 0), (3, 1), (42, 1)] {
        let r = h.run("j", "cmd", &["exit", &code.to_string()]).await;
        assert_eq!(exit(&r), Some(seen));
        assert_eq!(stdout(&r), "bye\n");
        assert!(r.stopped.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpu_limit_stops_a_spinning_guest_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().cpu_micros = 100_000;
    h.task(s);
    let t = Instant::now();
    let r = h.run("j", "cmd", &["spin"]).await;
    let (reason, limit, detail) = stopped(&r);
    assert_eq!((reason, limit), ("limits", "cpu"), "{detail}");
    assert!(
        detail.starts_with("exceeded its 100ms CPU limit"),
        "{detail}"
    );
    assert!(
        r.cpu_micros >= 100_000 && r.cpu_micros < 400_000,
        "{}",
        r.cpu_micros
    );
    assert!(t.elapsed() < Duration::from_secs(3));
    assert_eq!(exit(&r), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wall_limit_stops_a_guest_waiting_on_a_timer_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().wall_micros = 200_000;
    h.task(s);
    let t = Instant::now();
    let r = h.run("j", "cmd", &["sleep", "10000"]).await;
    let (reason, limit, detail) = stopped(&r);
    assert_eq!((reason, limit), ("limits", "wall"), "{detail}");
    assert_eq!(detail, "exceeded its 200ms wall-clock limit");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    // Waiting costs no CPU.
    assert!(r.cpu_micros < 50_000, "{}", r.cpu_micros);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wall_limit_also_stops_a_running_guest_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    let l = s.limits.as_mut().unwrap();
    l.wall_micros = 150_000;
    l.cpu_micros = 10_000_000;
    h.task(s);
    let r = h.run("j", "cmd", &["spin"]).await;
    assert_eq!(stopped(&r).1, "wall");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_limit_stops_a_growing_guest_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.memory_max_bytes = 16 << 20;
    h.task(s);
    let r = h.run("j", "cmd", &["alloc"]).await;
    let (reason, limit, detail) = stopped(&r);
    assert_eq!((reason, limit), ("limits", "memory"), "{detail}");
    assert_eq!(detail, "exceeded its 16MiB memory limit");
    assert!(r.memory_peak_bytes <= 16 << 20);
    // Within the limit, the same guest runs.
    let r = h.run("j", "cmd", &["alloc-mib", "4"]).await;
    assert_eq!(exit(&r), Some(0), "{:?}", r.stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_past_the_budget_truncates_without_stopping_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().output_bytes = 4096;
    h.task(s);
    let r = h.run("j", "cmd", &["print", "100000"]).await;
    assert_eq!(exit(&r), Some(0));
    assert!(r.stopped.is_none());
    assert!(r.truncated);
    assert_eq!(r.stdout.len(), 4096);
    // stdout used the whole budget, so stderr got nothing.
    assert!(r.stderr.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trap_is_reported_without_host_internals_test() {
    let h = cmd_task();
    let r = h.run("j", "cmd", &["trap"]).await;
    let (reason, _, detail) = stopped(&r);
    assert_eq!(reason, "trap");
    assert!(detail.contains("unreachable"), "{detail}");
    assert!(!detail.contains("0x"), "no addresses: {detail}");
    assert!(!detail.contains('\n'), "one line: {detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clocks_are_frozen_within_a_turn_and_coarse_test() {
    let h = cmd_task();
    let r = h.run("j", "cmd", &["clocks"]).await;
    let out = stdout(&r);
    let words: Vec<&str> = out.split_whitespace().collect();
    // wall w1 w2 w3 mono_same=.. mono_advanced_ns=..
    let w: Vec<u128> = words[1..4].iter().map(|x| x.parse().unwrap()).collect();
    assert_eq!(w[0], w[1], "wall clock frozen in a turn: {out}");
    assert!(w[2] > w[0], "advances across an await: {out}");
    assert!(
        w.iter().all(|x| x.is_multiple_of(1_000_000)),
        "1ms resolution: {out}"
    );
    assert_eq!(words[4], "mono_same=true", "{out}");
    let adv: u128 = words[5]
        .trim_start_matches("mono_advanced_ns=")
        .parse()
        .unwrap();
    assert!(adv >= 5_000_000 && adv.is_multiple_of(1_000_000), "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_hygiene_two_instances_never_share_rng_output_test() {
    let h = cmd_task();
    let a = stdout(&h.run("j", "cmd", &["random"]).await);
    let b = stdout(&h.run("j", "cmd", &["random"]).await);
    assert_eq!(a.trim().len(), 32);
    assert_ne!(a, b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sockets_are_linked_but_deny_everything_test() {
    let h = cmd_task();
    let r = h.run("j", "cmd", &["socket"]).await;
    let out = stdout(&r);
    assert!(out.starts_with("socket error"), "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_module_refuses_a_path_outside_read_dirs_test() {
    let h = start(config());
    let other = tempfile::tempdir().unwrap();
    let outside = other.path().join("cmd.cwasm");
    std::fs::copy(artifact("cmd"), &outside).unwrap();
    let e = h
        .host
        .load_module(LoadModuleRequest {
            module_id: "m".into(),
            artifact_path: outside.display().to_string(),
        })
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    // A link from inside the read dir to outside is resolved and refused too.
    let link = artifacts().join("escape.cwasm");
    let _ = std::fs::remove_file(&link);
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let e = h
        .host
        .load_module(LoadModuleRequest {
            module_id: "m".into(),
            artifact_path: link.display().to_string(),
        })
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_module_reports_exports_and_is_idempotent_test() {
    let h = start(config());
    let req = LoadModuleRequest {
        module_id: "cmd".into(),
        artifact_path: artifact("cmd").display().to_string(),
    };
    let r = h.host.load_module(req.clone()).unwrap();
    assert!(r.exports_cli_run && !r.exports_runner && !r.exports_http_handler);
    assert!(r.initial_memory_bytes > 0);
    assert!(!r.already_loaded);
    assert!(h.host.load_module(req).unwrap().already_loaded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_tasks_and_wrong_calls_are_refused_test() {
    let h = cmd_task();
    let e = h.run_with("j", "nope", "", "", run(&[])).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
    let e = h
        .host
        .invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: "cmd".into(),
            call: Some(invoke_request::Call::Http(HttpCall::default())),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    // A command cannot be a tenant isolate.
    let mut s = spec("j", "cmd2", "cmd");
    s.lifetime = Lifetime::Tenant as i32;
    let e = h.host.configure_task(s).unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("single-shot"), "{}", e.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_version_mismatch_is_refused_test() {
    let mut c = config();
    c.protocol_version = PROTOCOL_VERSION + 1;
    let e = legion_isolate::host::Host::new(legion_isolate::host::HostSetup {
        config: c,
        worker: std::sync::Arc::new(legion_isolate::worker_api::NoWorker),
        rt: tokio::runtime::Handle::current(),
        confinement: Default::default(),
        statm: None,
    })
    .err()
    .unwrap();
    assert!(e.to_string().contains("protocol"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compile_writes_a_loadable_artifact_and_reports_the_component_test() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("x.cwasm");
    let r = legion_isolate::compile::compile_file(
        &legion_isolate::engine::EngineParams::default(),
        &fixture("cmd"),
        &out,
    )
    .unwrap();
    assert!(r.exports.cli_run);
    assert!(r
        .imports
        .iter()
        .any(|i| i.starts_with("wasi:cli/environment@")));
    assert_eq!(r.bytes, std::fs::metadata(&out).unwrap().len());
    // No temporary file is left beside it.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    let mut c = config();
    c.read_dirs = vec![dir.path().display().to_string()];
    let h = start(c);
    h.host
        .load_module(LoadModuleRequest {
            module_id: "x".into(),
            artifact_path: out.display().to_string(),
        })
        .unwrap();
}

/// The MPK layout end to end, where the CPU has protection keys: many live
/// instances in one host, each isolated by its key stripe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mpk_layout_runs_many_instances_when_the_cpu_has_pku_test() {
    use legion_isolate::engine::{mpk_available, EngineParams};
    if !mpk_available() {
        eprintln!("MPK is not available on this CPU; SLOT_LAYOUT_MPK is not exercised");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let params = EngineParams {
        layout: SlotLayout::Mpk,
        ..Default::default()
    };
    let art = dir.path().join("cmd.cwasm");
    legion_isolate::compile::compile_file(&params, &fixture("cmd"), &art).unwrap();
    let mut c = config();
    c.layout = SlotLayout::Mpk as i32;
    c.slots = 512;
    c.shards = 2;
    c.read_dirs = vec![dir.path().display().to_string()];
    let h = start(c);
    assert_ne!(
        h.host.compat_key(),
        legion_isolate::engine::compat_key(&EngineParams::default()).unwrap(),
        "an MPK host has its own compat key"
    );
    h.host
        .load_module(LoadModuleRequest {
            module_id: "cmd".into(),
            artifact_path: art.display().to_string(),
        })
        .unwrap();
    let mut s = spec("j", "cmd", "cmd");
    s.queue_timeout_millis = 10_000;
    h.host.configure_task(s).unwrap();
    // 64 concurrent instances, each sleeping so they are live together.
    let t = std::time::Instant::now();
    let mut all = Vec::new();
    for i in 0..64 {
        let host = h.host.clone();
        all.push(tokio::spawn(async move {
            host.invoke(InvokeRequest {
                job_id: "j".into(),
                task_name: "cmd".into(),
                invocation_id: format!("mpk-{i}"),
                call: Some(invoke_request::Call::Run(run(&["sleep", "200"]))),
                ..Default::default()
            })
            .await
        }));
    }
    let mut inst_us = Vec::new();
    for a in all {
        let r = a.await.unwrap().unwrap();
        assert_eq!(stdout(&r), "slept\n", "{:?}", r.stopped);
        inst_us.push(r.instantiate_micros);
    }
    inst_us.sort();
    eprintln!(
        "MPK: 64 concurrent instances in {:?}; instantiate p50 {}µs p99 {}µs",
        t.elapsed(),
        inst_us[32],
        inst_us[63]
    );
    // The memory limit still holds under MPK.
    let r = h.run("j", "cmd", &["alloc"]).await;
    assert_eq!(stopped(&r).1, "memory");
}
