//! A `wasi:http/incoming-handler` for the host's HTTP tests.

wit_bindgen::generate!({ world: "handler", path: "wit", generate_all });

use std::sync::atomic::{AtomicU64, Ordering};

use exports::wasi::http::incoming_handler::Guest;
use wasi::http::types::{
    ErrorCode, Fields, IncomingRequest, Method, OutgoingBody, OutgoingRequest, OutgoingResponse,
    ResponseOutparam, Scheme,
};
use wasi::io::streams::StreamError;

static COUNT: AtomicU64 = AtomicU64::new(0);

struct Handler;

fn respond(out: ResponseOutparam, status: u16, headers: &[(&str, &[u8])], body: &[u8]) {
    let h = Fields::new();
    for (k, v) in headers {
        h.append(k, v).unwrap();
    }
    let resp = OutgoingResponse::new(h);
    resp.set_status_code(status).unwrap();
    let ob = resp.body().unwrap();
    ResponseOutparam::set(out, Ok(resp));
    let s = ob.write().unwrap();
    for chunk in body.chunks(4096) {
        s.blocking_write_and_flush(chunk).unwrap();
    }
    drop(s);
    OutgoingBody::finish(ob, None).unwrap();
}

fn read_body(req: &IncomingRequest) -> Vec<u8> {
    let body = req.consume().unwrap();
    let s = body.stream().unwrap();
    let mut out = Vec::new();
    loop {
        match s.blocking_read(65536) {
            Ok(b) => out.extend_from_slice(&b),
            Err(StreamError::Closed) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    out
}

fn fetch(url: &str) -> Result<(u16, Vec<(String, Vec<u8>)>, Vec<u8>), String> {
    let rest = url.strip_prefix("http://").ok_or("http only")?;
    let (auth, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let req = OutgoingRequest::new(Fields::new());
    req.set_method(&Method::Get).unwrap();
    req.set_scheme(Some(&Scheme::Http)).unwrap();
    req.set_authority(Some(auth)).unwrap();
    req.set_path_with_query(Some(path)).unwrap();
    let fut = wasi::http::outgoing_handler::handle(req, None).map_err(|e: ErrorCode| format!("{e:?}"))?;
    let resp = loop {
        match fut.get() {
            Some(r) => break r,
            None => fut.subscribe().block(),
        }
    };
    let resp = resp.unwrap().map_err(|e| format!("{e:?}"))?;
    let status = resp.status();
    let headers = resp.headers().entries();
    let body = resp.consume().unwrap();
    let s = body.stream().unwrap();
    let mut out = Vec::new();
    loop {
        match s.blocking_read(65536) {
            Ok(b) => out.extend_from_slice(&b),
            Err(StreamError::Closed) => break,
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    Ok((status, headers, out))
}

impl Guest for Handler {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        let pq = req.path_with_query().unwrap_or_default();
        let (path, query) = match pq.find('?') {
            Some(i) => (pq[..i].to_string(), pq[i + 1..].to_string()),
            None => (pq.clone(), String::new()),
        };
        match path.as_str() {
            "/echo" => {
                let method = format!("{:?}", req.method());
                let mut hs: Vec<String> = req
                    .headers()
                    .entries()
                    .into_iter()
                    .map(|(k, v)| format!("{k}={}", String::from_utf8_lossy(&v)))
                    .collect();
                hs.sort();
                let body = read_body(&req);
                println!("echo {path}");
                let text = format!(
                    "{method} {pq} [{}] {}",
                    hs.join(";"),
                    String::from_utf8_lossy(&body)
                );
                respond(out, 200, &[("content-type", b"text/plain")], text.as_bytes());
            }
            "/count" => {
                let n = COUNT.fetch_add(1, Ordering::SeqCst) + 1;
                respond(out, 200, &[], n.to_string().as_bytes());
            }
            "/fetch" => match fetch(&query) {
                Ok((status, headers, body)) => {
                    let mut text = format!("upstream {status}\n");
                    for (k, v) in headers {
                        text.push_str(&format!("{k}={}\n", String::from_utf8_lossy(&v)));
                    }
                    text.push_str(&String::from_utf8_lossy(&body));
                    respond(out, 200, &[], text.as_bytes());
                }
                Err(e) => respond(out, 502, &[], e.as_bytes()),
            },
            "/big" => {
                let n: usize = query.parse().unwrap_or(1 << 20);
                respond(out, 200, &[], &vec![b'b'; n]);
            }
            "/trap" => core::arch::wasm32::unreachable(),
            _ => respond(out, 404, &[], b"not found"),
        }
    }
}

export!(Handler);
