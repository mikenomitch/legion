//! HTTP into a `wasi:http/incoming-handler`, outbound requests through the
//! worker's egress (from a handler and from a command), and log forwarding.

mod common;

use common::*;
use legion_proto::isolate_host::*;

fn handler_task(h: &TestHost, task: &str, life: Lifetime) {
    let mut s = spec("j", task, "handler");
    s.lifetime = life as i32;
    h.task(s);
}

async fn get(h: &TestHost, task: &str, tenant: &str, call: HttpCall) -> InvokeReply {
    h.host
        .invoke(InvokeRequest {
            job_id: "j".into(),
            task_name: task.into(),
            invocation_id: next_id(),
            tenant: tenant.into(),
            call: Some(invoke_request::Call::Http(call)),
            ..Default::default()
        })
        .await
        .unwrap()
}

fn http(method: &str, path: &str) -> HttpCall {
    HttpCall {
        method: method.into(),
        path: path.into(),
        authority: "app.example".into(),
        scheme: "http".into(),
        ..Default::default()
    }
}

fn resp(r: &InvokeReply) -> &HttpOutcome {
    match &r.outcome {
        Some(invoke_reply::Outcome::Http(o)) => o,
        other => panic!("no http outcome: {other:?} stopped {:?}", r.stopped),
    }
}

fn body(r: &InvokeReply) -> String {
    String::from_utf8_lossy(&resp(r).body).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_answers_a_request_test() {
    let h = start(config());
    handler_task(&h, "web", Lifetime::Request);
    let mut c = http("POST", "/echo?x=1");
    c.headers.push(Header {
        name: "x-test".into(),
        value: b"v".to_vec(),
    });
    c.body = b"payload".to_vec();
    let r = get(&h, "web", "", c).await;
    let o = resp(&r);
    assert_eq!(o.status, 200);
    assert!(o
        .headers
        .iter()
        .any(|x| x.name == "content-type" && x.value == b"text/plain"));
    let b = body(&r);
    assert!(b.starts_with("Method::Post /echo?x=1 ["), "{b}");
    assert!(b.contains("x-test=v"), "{b}");
    assert!(b.ends_with("] payload"), "{b}");
    assert_eq!(stdout(&r), "echo /echo\n");
    // Handler output also goes to the task's log stream.
    for _ in 0..50 {
        if !h.worker.logs.lock().unwrap().is_empty() {
            break;
        }
        sleep_ms(10).await;
    }
    let logs = h.worker.logs.lock().unwrap().clone();
    assert_eq!(logs[0].lines[0].line, b"echo /echo");
    assert_eq!(logs[0].origin.as_ref().unwrap().task_name, "web");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_handler_keeps_its_globals_and_a_request_one_does_not_test() {
    let h = start(config());
    handler_task(&h, "web", Lifetime::Request);
    let mut s = spec("j", "warm", "handler");
    s.lifetime = Lifetime::Tenant as i32;
    h.host.configure_task(s).unwrap();
    for want in ["1", "1"] {
        assert_eq!(
            body(&get(&h, "web", "a", http("GET", "/count")).await),
            want
        );
    }
    for want in ["1", "2", "3"] {
        assert_eq!(
            body(&get(&h, "warm", "a", http("GET", "/count")).await),
            want
        );
    }
    assert_eq!(
        body(&get(&h, "warm", "b", http("GET", "/count")).await),
        "1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_streamed_body_is_read_while_the_handler_runs_test() {
    let h = start(config());
    handler_task(&h, "web", Lifetime::Request);
    let r = get(&h, "web", "", http("GET", "/big?500000")).await;
    assert_eq!(resp(&r).body.len(), 500_000);
    // Past the host's body limit (1 MiB in tests) the call fails.
    let r = get(&h, "web", "", http("GET", "/big?2000000")).await;
    let (reason, _, detail) = stopped(&r);
    assert_eq!(reason, "trap");
    assert!(detail.contains("larger than"), "{detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trapping_handler_is_stopped_test() {
    let h = start(config());
    handler_task(&h, "web", Lifetime::Request);
    let r = get(&h, "web", "", http("GET", "/trap")).await;
    assert_eq!(stopped(&r).0, "trap");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn egress_from_a_handler_goes_through_the_worker_with_its_origin_test() {
    let h = start(config());
    handler_task(&h, "web", Lifetime::Request);
    let r = get(
        &h,
        "web",
        "acme",
        http("GET", "/fetch?http://api.internal/v1/thing"),
    )
    .await;
    let b = body(&r);
    assert!(b.starts_with("upstream 200\n"), "{b}");
    assert!(b.contains("x-upstream=yes"), "{b}");
    assert!(
        b.ends_with("hello from http://api.internal/v1/thing"),
        "{b}"
    );
    // The credential the worker injects on its side never reaches the guest.
    assert!(!b.to_lowercase().contains("authorization"), "{b}");
    let seen = h.worker.egress_seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let e = &seen[0];
    assert_eq!(e.method, "GET");
    assert_eq!(e.url, "http://api.internal/v1/thing");
    let o = e.origin.as_ref().unwrap();
    assert_eq!(
        (
            o.job_id.as_str(),
            o.task_name.as_str(),
            o.tenant.as_str(),
            o.session
        ),
        ("j", "web", "acme", false)
    );
    assert!(o.isolate_id.starts_with("inv-"));
    assert!(e.timeout_millis > 0 && e.timeout_millis <= 5000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn egress_from_a_command_and_its_refusals_test() {
    let h = start(config());
    h.task(spec("j", "cmd", "cmd"));
    let r = h.run("j", "cmd", &["fetch", "http://up/x"]).await;
    let out = stdout(&r);
    assert!(out.contains("status=200"), "{out}");
    assert!(out.contains("body=hello from http://up/x"), "{out}");
    let seen = h.worker.egress_seen.lock().unwrap()[0].clone();
    assert!(seen
        .headers
        .iter()
        .any(|x| x.name == "x-from-guest" && x.value == b"1"));
    // A worker policy refusal is a typed error for the guest.
    *h.worker.egress_reply.lock().unwrap() = Some(EgressReply {
        error: Some(EgressError {
            kind: EgressErrorKind::EgressDenied as i32,
            message: "not allowed".into(),
        }),
        ..Default::default()
    });
    let r = h.run("j", "cmd", &["fetch", "http://up/x"]).await;
    assert_eq!(exit(&r), Some(1));
    assert!(
        stdout(&r).contains("error=ErrorCode::HttpRequestDenied"),
        "{}",
        stdout(&r)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn egress_is_refused_locally_when_the_task_has_none_test() {
    let h = start(config());
    let mut s = spec("j", "cmd", "cmd");
    s.egress = false;
    h.task(s);
    let r = h.run("j", "cmd", &["fetch", "http://up/x"]).await;
    assert!(
        stdout(&r).contains("error=ErrorCode::HttpRequestDenied"),
        "{}",
        stdout(&r)
    );
    assert!(h.worker.egress_seen.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// What one guest can make the host and the worker hold (review finding 6).
// ---------------------------------------------------------------------------

fn in_flight(h: &TestHost) -> (usize, usize, usize) {
    use std::sync::atomic::Ordering;
    (
        h.worker.egress_in_flight.load(Ordering::SeqCst),
        h.worker.egress_max_in_flight.load(Ordering::SeqCst),
        h.worker.egress_cancelled.load(Ordering::SeqCst),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_invocation_has_at_most_eight_outgoing_requests_in_flight_test() {
    let h = start(config());
    h.task(spec("j", "cmd", "cmd"));
    *h.worker.egress_delay.lock().unwrap() = Some(std::time::Duration::from_millis(100));
    let r = h
        .run("j", "cmd", &["fetch-many", "20", "http://up/x"])
        .await;
    // Every request is answered: the ones past the cap waited their turn.
    assert_eq!(stdout(&r).trim(), "ok=20", "{r:?}");
    let (now, max, cancelled) = in_flight(&h);
    assert_eq!(
        max,
        legion_isolate::guest::MAX_EGRESS_IN_FLIGHT,
        "never more than the cap in flight at the worker"
    );
    assert_eq!((now, cancelled), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_requests_end_with_the_invocation_test() {
    let h = start(config());
    *h.worker.egress_delay.lock().unwrap() = Some(std::time::Duration::from_secs(30));
    // A wall limit cuts a command waiting on three slow requests.
    let mut s = spec("j", "cmd", "cmd");
    s.limits.as_mut().unwrap().wall_micros = 300_000;
    h.task(s);
    let r = h
        .run("j", "cmd", &["fetch-many", "3", "http://slow/x"])
        .await;
    assert_eq!(stopped(&r).1, "wall");
    let wait_cancelled = |n: usize| async move {
        for _ in 0..100 {
            if in_flight(&h).2 >= n {
                break;
            }
            sleep_ms(10).await;
        }
        in_flight(&h)
    };
    let (now, _, cancelled) = wait_cancelled(3).await;
    assert_eq!((now, cancelled), (0, 3), "the worker calls were dropped");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_warm_instance_cannot_leave_a_request_running_after_its_call_test() {
    let h = start(config());
    *h.worker.egress_delay.lock().unwrap() = Some(std::time::Duration::from_secs(30));
    let mut s = spec("j", "t", "runner");
    s.lifetime = Lifetime::Tenant as i32;
    h.task(s);
    let r = h
        .run_with("j", "t", "acme", "", run(&["fetch-leave", "http://slow/x"]))
        .await
        .unwrap();
    assert_eq!(outcome(&r).result, "\"left\"", "{r:?}");
    for _ in 0..100 {
        if in_flight(&h).2 >= 1 {
            break;
        }
        sleep_ms(10).await;
    }
    assert_eq!(in_flight(&h), (0, 1, 1), "cancelled when the call returned");
    // The instance is still warm and usable.
    let r = h
        .run_with("j", "t", "acme", "", run(&["count"]))
        .await
        .unwrap();
    assert_eq!(r.start, "warm");
}
