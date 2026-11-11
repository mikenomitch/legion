//! `run(request)`: the first argument picks the behavior.

wit_bindgen::generate!({ world: "runner", path: "wit", generate_all });

use std::sync::atomic::{AtomicU64, Ordering};

use exports::legion::isolate::runner::{Guest, Outcome, Request};
use legion::isolate::{control, state, vm};

static COUNT: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Outgoing requests left pending across calls (`fetch-leave`).
    static LEFT: std::cell::RefCell<Vec<wasi::http::types::FutureIncomingResponse>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

struct Runner;

fn ok(result: String) -> Outcome {
    Outcome { exit: 0, result: Some(result) }
}

impl Guest for Runner {
    fn run(req: Request) -> Outcome {
        if let Ok(dir) = std::env::var("LEGION_RUN_DIR") {
            let _ = std::env::set_current_dir(dir);
        }
        let arg = |i: usize| req.args.get(i).cloned().unwrap_or_default();
        println!("ran {}", arg(0));
        match arg(0).as_str() {
            // A global proves whether the instance was reused.
            "count" => {
                let n = COUNT.fetch_add(1, Ordering::SeqCst) + 1;
                ok(format!("{{\"count\":{n}}}"))
            }
            "info" => {
                let i = control::info();
                ok(format!(
                    "{{\"job\":\"{}\",\"task\":\"{}\",\"lifetime\":\"{:?}\",\"tenant\":{:?},\"session\":{:?},\"resumed\":{}}}",
                    i.job, i.task, i.lifetime, i.tenant.unwrap_or_default(), i.session.unwrap_or_default(), i.resumed
                ))
            }
            "set" => match state::set(&arg(1), arg(2).as_bytes()) {
                Ok(()) => ok("\"set\"".into()),
                Err(e) => {
                    eprintln!("state error: {e:?}");
                    Outcome { exit: 2, result: None }
                }
            },
            "get" => match state::get(&arg(1)) {
                Ok(v) => ok(format!(
                    "{:?}",
                    v.map(|v| String::from_utf8_lossy(&v).into_owned()).unwrap_or_default()
                )),
                Err(e) => {
                    eprintln!("state error: {e:?}");
                    Outcome { exit: 2, result: None }
                }
            },
            "keys" => match state::keys() {
                Ok(k) => ok(format!("{:?}", k.join(","))),
                Err(e) => {
                    eprintln!("state error: {e:?}");
                    Outcome { exit: 2, result: None }
                }
            },
            "unsupported" => {
                control::unsupported("subprocess.run: an isolate cannot start processes");
                ok("\"unreachable\"".into())
            }
            "park" => {
                control::park();
                ok("\"parked\"".into())
            }
            "vm" => match vm::acquire() {
                Ok(m) => match m.exec(&["echo".to_string(), "hi".to_string()], b"", 1000) {
                    Ok(r) => ok(format!(
                        "{{\"id\":\"{}\",\"stdout\":{:?}}}",
                        m.id(),
                        String::from_utf8_lossy(&r.stdout)
                    )),
                    Err(e) => {
                        eprintln!("exec: {e:?}");
                        Outcome { exit: 2, result: None }
                    }
                },
                Err(e) => {
                    eprintln!("acquire: {e:?}");
                    Outcome { exit: 3, result: None }
                }
            },
            "exit" => Outcome { exit: arg(1).parse().unwrap_or(0), result: None },
            "spin" => {
                let mut x: u64 = 1;
                loop {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if x == 0 {
                        break;
                    }
                }
                ok(format!("{x}"))
            }
            // The entry file and input, as the runner sees them.
            "entry" => {
                let entry = req.entry.clone().unwrap_or_default();
                let body = std::fs::read_to_string(&entry).unwrap_or_else(|e| format!("<{e}>"));
                let input = req.input.clone().unwrap_or_default();
                let leftover = std::fs::metadata("leftover.txt").is_ok();
                std::fs::write("leftover.txt", "x").unwrap();
                std::fs::write("result.json", format!("{{\"entry\":{entry:?},\"body\":{body:?},\"input\":{input:?},\"leftover\":{leftover}}}")).unwrap();
                Outcome { exit: 0, result: None }
            }
            // Send a request and return without waiting for it: the
            // future stays in this (warm) instance's resource table.
            "fetch-leave" => {
                use wasi::http::types::{Fields, Method, OutgoingRequest, Scheme};
                let url = arg(1);
                let rest = url.strip_prefix("http://").unwrap_or(&url);
                let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
                let req = OutgoingRequest::new(Fields::new());
                req.set_method(&Method::Get).unwrap();
                req.set_scheme(Some(&Scheme::Http)).unwrap();
                req.set_authority(Some(authority)).unwrap();
                req.set_path_with_query(Some(&format!("/{path}"))).unwrap();
                match wasi::http::outgoing_handler::handle(req, None) {
                    Ok(fut) => {
                        LEFT.with(|l| l.borrow_mut().push(fut));
                        ok("\"left\"".into())
                    }
                    Err(e) => ok(format!("\"{e:?}\"")),
                }
            }
            other => {
                eprintln!("unknown mode {other}");
                Outcome { exit: 64, result: None }
            }
        }
    }
}

export!(Runner);
