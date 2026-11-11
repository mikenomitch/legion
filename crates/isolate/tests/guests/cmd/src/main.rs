//! `cmd <mode> [arg]`: one behavior per mode, for the host's run tests.

wit_bindgen::generate!({ world: "cmd", path: "wit", generate_all });

use std::io::{Read, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wasi::http::types::{ErrorCode, Fields, Method, OutgoingRequest, Scheme};
use wasi::io::streams::StreamError;

fn main() {
    // wasi-libc starts in `/` whatever `initial-cwd` says; the host names
    // the run directory in LEGION_RUN_DIR.
    if let Ok(dir) = std::env::var("LEGION_RUN_DIR") {
        std::env::set_current_dir(dir).expect("chdir to the run directory");
    }
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("echo");
    let arg = args.get(2).cloned().unwrap_or_default();
    match mode {
        // Arguments, LEGION_* and FOO env, and input.json if there is one.
        "echo" => {
            println!("args={}", args[1..].join(","));
            let mut env: Vec<(String, String)> = std::env::vars()
                .filter(|(k, _)| k.starts_with("LEGION_") || k == "FOO")
                .collect();
            env.sort();
            for (k, v) in env {
                println!("env {k}={v}");
            }
            println!("cwd={}", std::env::current_dir().unwrap().display());
            if let Ok(input) = std::fs::read_to_string("input.json") {
                println!("input={input}");
            }
            eprintln!("to stderr");
        }
        // Read in.txt, write OUT.txt uppercased and result.json.
        "files" => {
            let text = std::fs::read_to_string("in.txt").expect("in.txt");
            std::fs::create_dir_all("data").unwrap();
            std::fs::write("data/out.txt", text.to_uppercase()).unwrap();
            std::fs::write("result.json", format!("{{\"len\":{}}}", text.len())).unwrap();
            std::fs::write("big.bin", vec![b'z'; 4096]).unwrap();
            println!("wrote");
        }
        "bad-result" => {
            std::fs::write("result.json", "not json {").unwrap();
        }
        "exit" => {
            println!("bye");
            std::process::exit(arg.parse().unwrap_or(0));
        }
        "spin" => {
            let mut x: u64 = 1;
            loop {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                if x == 0 {
                    break;
                }
            }
            println!("{x}");
        }
        // Grow the heap 1 MiB at a time and keep it.
        "alloc" => {
            let mut keep = Vec::new();
            loop {
                let mut v = vec![0u8; 1 << 20];
                v[0] = 1;
                keep.push(v);
            }
        }
        // Allocate this many MiB, then exit 0.
        "alloc-mib" => {
            let n: usize = arg.parse().unwrap_or(1);
            let v: Vec<Vec<u8>> = (0..n).map(|_| vec![1u8; 1 << 20]).collect();
            println!("held {} MiB", v.len());
        }
        "print" => {
            let n: usize = arg.parse().unwrap_or(0);
            let out = std::io::stdout();
            let mut out = out.lock();
            let chunk = [b'x'; 1024];
            let mut left = n;
            while left > 0 {
                let k = left.min(chunk.len());
                out.write_all(&chunk[..k]).unwrap();
                left -= k;
            }
            out.flush().unwrap();
            eprint!("err-tail");
        }
        "sleep" => {
            std::thread::sleep(Duration::from_millis(arg.parse().unwrap_or(1000)));
            println!("slept");
        }
        "trap" => {
            core::arch::wasm32::unreachable();
        }
        // Clocks twice in one turn, then again after an await (a sleep).
        "clocks" => {
            let w1 = wall_ns();
            let m1 = Instant::now();
            let w2 = wall_ns();
            let m2 = Instant::now();
            std::thread::sleep(Duration::from_millis(5));
            let w3 = wall_ns();
            let m3 = Instant::now();
            println!(
                "wall {} {} {} mono_same={} mono_advanced_ns={}",
                w1,
                w2,
                w3,
                m1 == m2,
                (m3 - m1).as_nanos()
            );
        }
        "random" => {
            let b = wasi::random::random::get_random_bytes(16);
            println!("{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>());
        }
        "fetch" => match fetch(&arg) {
            Ok((status, headers, body)) => {
                println!("status={status}");
                for (k, v) in headers {
                    println!("header {k}={v}");
                }
                println!("body={}", String::from_utf8_lossy(&body));
            }
            Err(e) => {
                println!("error={e}");
                std::process::exit(3);
            }
        },
        // `fetch-many <n> <url>`: start n requests at once, then wait for
        // each; prints how many succeeded.
        "fetch-many" => {
            let n: usize = arg.parse().unwrap_or(1);
            let url = args.get(3).cloned().unwrap_or_default();
            let futs: Vec<_> = (0..n).map(|_| start(&url)).collect();
            let mut ok = 0;
            for f in futs {
                let Ok(f) = f else { continue };
                let r = loop {
                    match f.get() {
                        Some(r) => break r,
                        None => f.subscribe().block(),
                    }
                };
                if matches!(r, Ok(Ok(_))) {
                    ok += 1;
                }
            }
            println!("ok={ok}");
        }
        "stdin" => {
            let mut s = String::new();
            let _ = std::io::stdin().read_to_string(&mut s);
            println!("stdin={s:?}");
        }
        "socket" => {
            // The host links sockets but denies everything.
            match std::net::TcpStream::connect("127.0.0.1:80") {
                Ok(_) => println!("connected"),
                Err(e) => println!("socket error: {e}"),
            }
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(64);
        }
    }
}

fn wall_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

type Fetched = (u16, Vec<(String, String)>, Vec<u8>);

fn fetch(url: &str) -> Result<Fetched, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or("only http:// URLs")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let headers = Fields::new();
    headers
        .append("x-from-guest", b"1")
        .map_err(|e| format!("{e:?}"))?;
    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Get).unwrap();
    req.set_scheme(Some(&Scheme::Http)).unwrap();
    req.set_authority(Some(authority)).unwrap();
    req.set_path_with_query(Some(path)).unwrap();
    let fut = wasi::http::outgoing_handler::handle(req, None).map_err(code)?;
    let resp = loop {
        match fut.get() {
            Some(r) => break r,
            None => fut.subscribe().block(),
        }
    };
    let resp = resp.map_err(|_| "response taken twice".to_string())?.map_err(code)?;
    let status = resp.status();
    let headers = resp
        .headers()
        .entries()
        .into_iter()
        .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
        .collect();
    let body = resp.consume().map_err(|_| "no body")?;
    let stream = body.stream().map_err(|_| "no body stream")?;
    let mut out = Vec::new();
    loop {
        match stream.blocking_read(64 << 10) {
            Ok(b) => out.extend_from_slice(&b),
            Err(StreamError::Closed) => break,
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    Ok((status, headers, out))
}

fn start(url: &str) -> Result<wasi::http::types::FutureIncomingResponse, String> {
    let rest = url.strip_prefix("http://").ok_or("only http:// URLs")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let req = OutgoingRequest::new(Fields::new());
    req.set_method(&Method::Get).unwrap();
    req.set_scheme(Some(&Scheme::Http)).unwrap();
    req.set_authority(Some(authority)).unwrap();
    req.set_path_with_query(Some(path)).unwrap();
    wasi::http::outgoing_handler::handle(req, None).map_err(code)
}

fn code(e: ErrorCode) -> String {
    format!("{e:?}")
}
