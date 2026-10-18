// The conformance module behind Legion's plugin host tests
// (src/worker/netd/handlers/plugin.rs, `plugin_host_test`). One module,
// many behaviors: the plugin configuration's `mode` picks one, so each test
// is a reference with a different config.
//
// Modes: noop, rewrite, meter, body, response, sse, connect, trap, spin, hog,
// sign, call, reject.
use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use std::time::Duration;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Debug);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(Root { mode: String::new(), config: Vec::new() }) });
}}

struct Root {
    mode: String,
    config: Vec<u8>,
}

fn field<'a>(config: &'a str, key: &str) -> Option<&'a str> {
    // A tiny reader for {"key":"value"} without pulling in serde.
    let needle = format!("\"{key}\"");
    let at = config.find(&needle)? + needle.len();
    let rest = config[at..].trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    Some(&rest[..rest.find('"')?])
}

impl Context for Root {}
impl RootContext for Root {
    fn on_configure(&mut self, _: usize) -> bool {
        self.config = self.get_plugin_configuration().unwrap_or_default();
        let text = String::from_utf8_lossy(&self.config).into_owned();
        self.mode = field(&text, "mode").unwrap_or("noop").to_string();
        // A plugin may refuse its configuration.
        self.mode != "reject"
    }

    fn create_http_context(&self, _: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(Stream {
            mode: self.mode.clone(),
            config: String::from_utf8_lossy(&self.config).into_owned(),
            seen: 0,
        }))
    }

    fn create_stream_context(&self, _: u32) -> Option<Box<dyn StreamContext>> {
        Some(Box::new(Connection { mode: self.mode.clone() }))
    }

    fn get_type(&self) -> Option<ContextType> {
        Some(if self.mode == "connect" {
            ContextType::StreamContext
        } else {
            ContextType::HttpContext
        })
    }
}

struct Connection {
    mode: String,
}

impl Context for Connection {}
impl StreamContext for Connection {
    fn on_new_connection(&mut self) -> Action {
        let destination = self.get_property(vec!["legion", "destination"]).unwrap_or_default();
        if self.mode == "connect" && destination.ends_with(b":25") {
            self.close_downstream();
        }
        Action::Continue
    }
}

struct Stream {
    mode: String,
    config: String,
    seen: usize,
}

impl Context for Stream {
    fn on_http_call_response(&mut self, _: u32, _: usize, body_size: usize, _: usize) {
        let status = self.get_http_call_response_header(":status").unwrap_or_default();
        let body = self.get_http_call_response_body(0, body_size).unwrap_or_default();
        if status == "200" && body == b"yes" {
            self.set_http_request_header("x-quota", Some("granted"));
            self.resume_http_request();
        } else {
            self.send_http_response(429, vec![("x-quota", "denied")], Some(b"over quota"));
        }
    }
}

impl HttpContext for Stream {
    fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
        match self.mode.as_str() {
            "rewrite" => {
                let job = self.get_property(vec!["legion", "job"]).unwrap_or_default();
                let tier = self.get_property(vec!["legion", "labels", "tier"]).unwrap_or_default();
                self.set_http_request_header("x-legion-job", Some(&String::from_utf8_lossy(&job)));
                self.set_http_request_header("x-tier", Some(&String::from_utf8_lossy(&tier)));
                self.set_http_request_header("x-remove-me", None);
                self.add_http_request_header("x-added", "1");
                let path = self.get_http_request_header(":path").unwrap_or_default();
                if let Some(rest) = path.strip_prefix("/old") {
                    self.set_http_request_header(":path", Some(&format!("/new{rest}")));
                }
                proxy_wasm::hostcalls::log(LogLevel::Info, "rewrote a request").ok();
                Action::Continue
            }
            "meter" => {
                // Count requests per scope in shared data; the third is over
                // budget.
                let (value, cas) = self.get_shared_data("count");
                let count = value
                    .map(|v| String::from_utf8_lossy(&v).parse::<u32>().unwrap_or(0))
                    .unwrap_or(0)
                    + 1;
                let _ = self.set_shared_data("count", Some(count.to_string().as_bytes()), cas);
                if let Ok(id) = proxy_wasm::hostcalls::define_metric(MetricType::Counter, "requests") {
                    let _ = proxy_wasm::hostcalls::increment_metric(id, 1);
                }
                if count > 2 {
                    self.send_http_response(429, vec![], Some(b"budget spent"));
                    return Action::Pause;
                }
                Action::Continue
            }
            "trap" => panic!("this plugin always traps"),
            "spin" => {
                let mut x: u64 = 1;
                loop {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if x == 0 {
                        break;
                    }
                }
                Action::Continue
            }
            "hog" => {
                let mut held: Vec<Vec<u8>> = Vec::new();
                loop {
                    held.push(vec![1u8; 1 << 20]);
                    if held.len() > 100_000 {
                        break;
                    }
                }
                Action::Continue
            }
            "sign" => {
                let args = format!(
                    "{{\"credential\":\"key\",\"algorithm\":\"hmac-sha256\",\"data\":\"{}\"}}",
                    self.get_http_request_header(":path").unwrap_or_default()
                );
                match proxy_wasm::hostcalls::call_foreign_function("legion_sign", Some(args.as_bytes())) {
                    Ok(Some(mac)) => {
                        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
                        self.set_http_request_header("x-signature", Some(&hex));
                    }
                    _ => {
                        self.send_http_response(500, vec![], Some(b"no signature"));
                        return Action::Pause;
                    }
                }
                Action::Continue
            }
            "call" => {
                let path = field(&self.config, "path").unwrap_or("/quota").to_string();
                match self.dispatch_http_call(
                    "quota",
                    vec![(":method", "GET"), (":path", &path), (":authority", "quota")],
                    None,
                    vec![],
                    Duration::from_secs(2),
                ) {
                    Ok(_) => Action::Pause,
                    Err(_) => {
                        self.send_http_response(502, vec![], Some(b"call refused"));
                        Action::Pause
                    }
                }
            }
            _ => Action::Continue,
        }
    }

    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if self.mode != "body" || !end_of_stream {
            return Action::Continue;
        }
        let body = self.get_http_request_body(0, body_size).unwrap_or_default();
        if body.windows(6).any(|w| w == b"secret") {
            self.send_http_response(403, vec![], Some(b"no secrets"));
            return Action::Pause;
        }
        // Redact digits.
        let redacted: Vec<u8> = body.iter().map(|b| if b.is_ascii_digit() { b'#' } else { *b }).collect();
        self.set_http_request_body(0, body_size, &redacted);
        Action::Continue
    }

    fn on_http_response_headers(&mut self, _: usize, _: bool) -> Action {
        if self.mode == "response" || self.mode == "sse" {
            self.set_http_response_header("x-plugin", Some("seen"));
            if self.get_http_response_header(":status").as_deref() == Some("404") {
                self.set_http_response_header(":status", Some("410"));
            }
        }
        Action::Continue
    }

    fn on_http_response_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        match self.mode.as_str() {
            "response" => {
                // Buffer the whole body, then uppercase it.
                if !end_of_stream {
                    return Action::Pause;
                }
                let body = self.get_http_response_body(0, body_size).unwrap_or_default();
                self.set_http_response_body(0, body_size, &body.to_ascii_uppercase());
                Action::Continue
            }
            "sse" => {
                // Stream: count events chunk by chunk, tagging each.
                let chunk = self.get_http_response_body(0, body_size).unwrap_or_default();
                self.seen += chunk.windows(2).filter(|w| w == b"\n\n").count();
                let tagged = String::from_utf8_lossy(&chunk).replace("data: ", "data: [seen] ");
                self.set_http_response_body(0, body_size, tagged.as_bytes());
                Action::Continue
            }
            _ => Action::Continue,
        }
    }

    fn on_log(&mut self) {
        let code = self.get_property(vec!["response", "code"]).unwrap_or_default();
        let mut bytes = [0u8; 8];
        bytes[..code.len().min(8)].copy_from_slice(&code[..code.len().min(8)]);
        let status = u64::from_le_bytes(bytes);
        let events = self.seen;
        proxy_wasm::hostcalls::log(LogLevel::Info, &format!("done status={status} events={events}")).ok();
        let (value, cas) = self.get_shared_data("done");
        let count = value
            .map(|v| String::from_utf8_lossy(&v).parse::<u32>().unwrap_or(0))
            .unwrap_or(0)
            + 1;
        let _ = self.set_shared_data("done", Some(count.to_string().as_bytes()), cas);
        let _ = self.set_shared_data("last_status", Some(status.to_string().as_bytes()), None);
    }
}
