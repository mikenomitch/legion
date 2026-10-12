//! Control-plane clients for the two microVM monitors, over their HTTP API
//! sockets.
//!
//! Both Firecracker and cloud-hypervisor serve a small REST API on a Unix
//! socket. Checkpoint and restore are API calls (pause, snapshot, load,
//! resume), so the sandbox runtime needs a client for each. The task-runtime
//! trait is synchronous, and every call here is one short request on a local
//! socket, so this is a deliberately small blocking HTTP/1.1 client rather
//! than an async stack: one request per connection, `Content-Length` bodies,
//! no chunked encoding (neither VMM sends it).
//!
//! References:
//! - Firecracker: `src/firecracker/swagger/firecracker.yaml` (`PATCH /vm`,
//!   `PUT /snapshot/create`, `PUT /snapshot/load`, `PATCH /drives/{id}`,
//!   `GET`/`PATCH /balloon`) and `docs/ballooning.md`.
//! - cloud-hypervisor: `docs/api.md` (`/api/v1/vm.pause`, `vm.resume`,
//!   `vm.snapshot`, `vm.restore`, `vmm.ping`, `vm.info`, `vm.resize` with
//!   `desired_balloon`) and `docs/balloon.md`.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::prelude::*;

/// Pause, resume, and drive updates are near-instant.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// Writing or loading guest memory scales with its size and the disk.
/// Generous, because a timeout mid-snapshot leaves a paused guest.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(300);

/// One HTTP response from a VMM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApiResponse {
    pub status: u16,
    pub body: String,
}

impl ApiResponse {
    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    fn json(&self) -> R<Value> {
        serde_json::from_str(&self.body)
            .with_context(|| format!("VMM returned non-JSON body: {}", self.body))
    }
}

/// A blocking HTTP/1.1 client bound to one Unix socket.
#[derive(Debug, Clone)]
pub(crate) struct UnixHttp {
    socket: PathBuf,
}

impl UnixHttp {
    pub(crate) fn new(socket: impl Into<PathBuf>) -> Self {
        UnixHttp {
            socket: socket.into(),
        }
    }

    /// Send one request and read the whole response.
    pub(crate) fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> R<ApiResponse> {
        let mut stream = UnixStream::connect(&self.socket).with_context(|| {
            format!(
                "failed to connect to VMM API socket {}",
                self.socket.display()
            )
        })?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;

        let mut head =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n");
        let payload = body.map(|body| body.to_string());
        if let Some(payload) = payload.as_deref() {
            head.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                payload.len()
            ));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes())?;
        if let Some(payload) = payload.as_deref() {
            stream.write_all(payload.as_bytes())?;
        }
        stream.flush()?;

        read_response(BufReader::new(stream))
            .with_context(|| format!("{method} {path} on {}", self.socket.display()))
    }
}

/// Parse a status line, headers, and a `Content-Length` (or read-to-close)
/// body.
fn read_response(mut reader: impl BufRead) -> R<ApiResponse> {
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .with_context(|| format!("malformed HTTP status line {status_line:?}"))?;

    let mut content_length: Option<usize> = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().ok();
            }
        }
    }

    let mut body = Vec::new();
    match content_length {
        Some(len) => {
            body.resize(len, 0);
            reader.read_exact(&mut body)?;
        }
        // 204 and 1xx carry no body by definition; anything else without a
        // length runs to connection close.
        None if status == 204 || status < 200 => {}
        None => {
            reader.read_to_end(&mut body)?;
        }
    }
    Ok(ApiResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

/// Wait for a freshly spawned VMM to accept connections on its API socket.
pub(crate) fn wait_for_api_socket(socket: &Path, timeout: Duration) -> R<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if UnixStream::connect(socket).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "VMM API socket {} did not come up within {}s",
                socket.display(),
                timeout.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Turn a non-2xx response into an error naming the VMM's own message.
fn check(vmm: &str, what: &str, response: ApiResponse) -> R<ApiResponse> {
    if response.ok() {
        return Ok(response);
    }
    // Firecracker wraps errors as {"fault_message": "..."}; cloud-hypervisor
    // returns plain text.
    let message = serde_json::from_str::<Value>(&response.body)
        .ok()
        .and_then(|body| {
            body.get("fault_message")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| response.body.trim().to_string());
    bail!("{vmm} {what} failed (HTTP {}): {message}", response.status)
}

/// Firecracker's API (`--api-sock`).
#[derive(Debug, Clone)]
pub(crate) struct FirecrackerApi {
    http: UnixHttp,
}

impl FirecrackerApi {
    pub(crate) fn new(socket: impl Into<PathBuf>) -> Self {
        FirecrackerApi {
            http: UnixHttp::new(socket),
        }
    }

    fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> R<ApiResponse> {
        let response = self.http.request(method, path, body.as_ref(), timeout)?;
        check("firecracker", &format!("{method} {path}"), response)
    }

    /// `GET /version` → `firecracker_version`.
    pub(crate) fn version(&self) -> R<String> {
        let body = self
            .call("GET", "/version", None, CONTROL_TIMEOUT)?
            .json()?;
        body.get("firecracker_version")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("firecracker /version reply has no firecracker_version")
    }

    pub(crate) fn pause(&self) -> R<()> {
        self.call(
            "PATCH",
            "/vm",
            Some(json!({"state": "Paused"})),
            CONTROL_TIMEOUT,
        )
        .map(|_| ())
    }

    pub(crate) fn resume(&self) -> R<()> {
        self.call(
            "PATCH",
            "/vm",
            Some(json!({"state": "Resumed"})),
            CONTROL_TIMEOUT,
        )
        .map(|_| ())
    }

    /// Full snapshot of a paused VM: device state to `state`, guest memory
    /// to `memory`.
    pub(crate) fn create_snapshot(&self, state: &Path, memory: &Path) -> R<()> {
        self.call(
            "PUT",
            "/snapshot/create",
            Some(json!({
                "snapshot_type": "Full",
                "snapshot_path": state.display().to_string(),
                "mem_file_path": memory.display().to_string(),
            })),
            SNAPSHOT_TIMEOUT,
        )
        .map(|_| ())
    }

    /// Load a snapshot into a fresh, unconfigured Firecracker. With
    /// `resume: false` the VM stays paused so drives can be repointed first.
    /// `network_overrides` (Phase 6, Firecracker 1.12+) moves each named
    /// NIC (`iface_id`) to another host TAP device (`host_dev_name`), which
    /// is how a fork stops sharing its origin's TAP. Empty sends none, so
    /// older Firecrackers see the request they always did.
    pub(crate) fn load_snapshot(
        &self,
        state: &Path,
        memory: &Path,
        resume: bool,
        network_overrides: &[(&str, &str)],
    ) -> R<()> {
        let mut body = json!({
            "snapshot_path": state.display().to_string(),
            "mem_backend": {
                "backend_type": "File",
                "backend_path": memory.display().to_string(),
            },
            "resume_vm": resume,
        });
        if !network_overrides.is_empty() {
            body["network_overrides"] = network_overrides
                .iter()
                .map(|(iface_id, host_dev_name)| {
                    json!({ "iface_id": iface_id, "host_dev_name": host_dev_name })
                })
                .collect();
        }
        self.call("PUT", "/snapshot/load", Some(body), SNAPSHOT_TIMEOUT)
            .map(|_| ())
    }

    /// The balloon's current target, in MiB (`GET /balloon`). Errors when
    /// the VM was booted without a balloon device.
    pub(crate) fn balloon_mib(&self) -> R<u64> {
        let body = self
            .call("GET", "/balloon", None, CONTROL_TIMEOUT)?
            .json()?;
        body.get("amount_mib")
            .and_then(Value::as_u64)
            .context("firecracker /balloon reply has no amount_mib")
    }

    /// Set the balloon's target (`PATCH /balloon`). Inflating asks the guest
    /// driver to hand that much memory back to the host; 0 deflates it.
    pub(crate) fn set_balloon_mib(&self, amount_mib: u64) -> R<()> {
        self.call(
            "PATCH",
            "/balloon",
            Some(json!({ "amount_mib": amount_mib })),
            CONTROL_TIMEOUT,
        )
        .map(|_| ())
    }

    /// Point an existing drive at a new backing file. Works on a restored,
    /// still-paused VM, which is how a fork gets its own identity disk: a
    /// restored Firecracker VM cannot hotplug a drive, but it can repoint one.
    pub(crate) fn update_drive(&self, drive_id: &str, path: &Path) -> R<()> {
        self.call(
            "PATCH",
            &format!("/drives/{drive_id}"),
            Some(json!({
                "drive_id": drive_id,
                "path_on_host": path.display().to_string(),
            })),
            CONTROL_TIMEOUT,
        )
        .map(|_| ())
    }
}

/// cloud-hypervisor's API (`--api-socket`).
#[derive(Debug, Clone)]
pub(crate) struct CloudHypervisorApi {
    http: UnixHttp,
}

impl CloudHypervisorApi {
    pub(crate) fn new(socket: impl Into<PathBuf>) -> Self {
        CloudHypervisorApi {
            http: UnixHttp::new(socket),
        }
    }

    fn call(
        &self,
        method: &str,
        action: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> R<ApiResponse> {
        let path = format!("/api/v1/{action}");
        let response = self.http.request(method, &path, body.as_ref(), timeout)?;
        check("cloud-hypervisor", action, response)
    }

    /// `GET /api/v1/vmm.ping` → `version`.
    pub(crate) fn version(&self) -> R<String> {
        let body = self
            .call("GET", "vmm.ping", None, CONTROL_TIMEOUT)?
            .json()?;
        body.get("version")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("cloud-hypervisor vmm.ping reply has no version")
    }

    pub(crate) fn pause(&self) -> R<()> {
        self.call("PUT", "vm.pause", None, CONTROL_TIMEOUT)
            .map(|_| ())
    }

    pub(crate) fn resume(&self) -> R<()> {
        self.call("PUT", "vm.resume", None, CONTROL_TIMEOUT)
            .map(|_| ())
    }

    /// Snapshot a paused VM into `dir` (`config.json`, `state.json`, and the
    /// memory ranges). The directory must exist.
    pub(crate) fn snapshot(&self, dir: &Path) -> R<()> {
        self.call(
            "PUT",
            "vm.snapshot",
            Some(json!({ "destination_url": file_url(dir) })),
            SNAPSHOT_TIMEOUT,
        )
        .map(|_| ())
    }

    /// The balloon's current size in bytes, from `vm.info`
    /// (`config.balloon.size`). Errors when the VM has no balloon device.
    pub(crate) fn balloon_bytes(&self) -> R<u64> {
        let body = self.call("GET", "vm.info", None, CONTROL_TIMEOUT)?.json()?;
        body.pointer("/config/balloon/size")
            .and_then(Value::as_u64)
            .context("cloud-hypervisor vm.info has no balloon (booted without --balloon)")
    }

    /// Resize the balloon (`vm.resize` with `desired_balloon`, in bytes).
    /// Inflating asks the guest to hand that much memory back; 0 deflates.
    pub(crate) fn set_balloon_bytes(&self, bytes: u64) -> R<()> {
        self.call(
            "PUT",
            "vm.resize",
            Some(json!({ "desired_balloon": bytes })),
            CONTROL_TIMEOUT,
        )
        .map(|_| ())
    }

    /// Restore into a VMM started with only `--api-socket`. The sandbox
    /// runtime restores with `--restore` on the command line instead (one
    /// process launch, no window where an empty VMM is listening). This stays
    /// for the API-driven path a pre-started VMM pool would use.
    #[allow(dead_code)]
    pub(crate) fn restore(&self, dir: &Path) -> R<()> {
        self.call(
            "PUT",
            "vm.restore",
            Some(json!({ "source_url": file_url(dir) })),
            SNAPSHOT_TIMEOUT,
        )
        .map(|_| ())
    }
}

/// `file://` URL for a local directory, the form both CH snapshot calls take.
pub(crate) fn file_url(dir: &Path) -> String {
    format!("file://{}", dir.display())
}

#[cfg(test)]
pub(crate) mod fake {
    //! A fake VMM: a Unix-socket HTTP server that records every request and
    //! answers from a closure. Enough to exercise both clients end to end
    //! without a hypervisor.

    use super::*;
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Seen {
        pub method: String,
        pub path: String,
        pub body: Option<Value>,
        pub content_length_header: bool,
    }

    pub(crate) struct FakeVmm {
        pub socket: PathBuf,
        seen: mpsc::Receiver<Seen>,
        _dir: Option<tempfile::TempDir>,
    }

    impl FakeVmm {
        /// Serve until the test ends. `respond` maps a request to
        /// `(status, body)`.
        pub(crate) fn start(respond: impl Fn(&Seen) -> (u16, String) + Send + 'static) -> FakeVmm {
            let dir = tempfile::tempdir().expect("tempdir");
            let socket = dir.path().join("api.sock");
            let mut fake = Self::start_at(socket, false, respond);
            fake._dir = Some(dir);
            fake
        }

        /// Serve on `socket`. With `after_removal`, wait for whatever is at
        /// that path to be removed first, standing in for a VMM that the
        /// code under test spawns after clearing a stale socket.
        pub(crate) fn start_at(
            socket: PathBuf,
            after_removal: bool,
            respond: impl Fn(&Seen) -> (u16, String) + Send + 'static,
        ) -> FakeVmm {
            let (tx, rx) = mpsc::channel();
            let (bound_tx, bound_rx) = mpsc::channel();
            let path = socket.clone();
            std::thread::spawn(move || {
                if after_removal {
                    while path.exists() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                let listener = UnixListener::bind(&path).expect("bind fake VMM socket");
                let _ = bound_tx.send(());
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
                        continue;
                    }
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or_default().to_string();
                    let path = parts.next().unwrap_or_default().to_string();
                    let mut length = None;
                    loop {
                        let mut line = String::new();
                        reader.read_line(&mut line).expect("header");
                        let line = line.trim_end();
                        if line.is_empty() {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse::<usize>().ok();
                            }
                        }
                    }
                    let body = length.map(|len| {
                        let mut buf = vec![0; len];
                        reader.read_exact(&mut buf).expect("body");
                        serde_json::from_slice(&buf).expect("JSON body")
                    });
                    let seen = Seen {
                        method,
                        path,
                        body,
                        content_length_header: length.is_some(),
                    };
                    let (status, reply) = respond(&seen);
                    let _ = tx.send(seen);
                    let mut stream = stream;
                    let head = if reply.is_empty() {
                        format!("HTTP/1.1 {status} X\r\n\r\n")
                    } else {
                        format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\n\r\n",
                            reply.len()
                        )
                    };
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(reply.as_bytes());
                }
            });
            if !after_removal {
                bound_rx.recv().expect("fake VMM bound");
            }
            FakeVmm {
                socket,
                seen: rx,
                _dir: None,
            }
        }

        /// Every request received so far, in order.
        pub(crate) fn drain(&self) -> Vec<Seen> {
            self.seen.try_iter().collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeVmm;
    use super::*;

    #[test]
    fn firecracker_client_speaks_the_documented_endpoints() -> R<()> {
        let vmm = FakeVmm::start(|seen| match seen.path.as_str() {
            "/version" => (200, r#"{"firecracker_version":"1.7.0"}"#.into()),
            _ => (204, String::new()),
        });
        wait_for_api_socket(&vmm.socket, Duration::from_secs(5))?;
        let api = FirecrackerApi::new(&vmm.socket);

        assert_eq!(api.version()?, "1.7.0");
        api.pause()?;
        api.create_snapshot(Path::new("/c/vm-state"), Path::new("/c/memory"))?;
        api.resume()?;
        api.load_snapshot(Path::new("/c/vm-state"), Path::new("/c/memory"), false, &[])?;
        api.update_drive("identity", Path::new("/a/identity.img"))?;
        // Phase 6: a fork's load moves the NIC to its own TAP.
        api.load_snapshot(
            Path::new("/c/vm-state"),
            Path::new("/c/memory"),
            false,
            &[("eth0", "lgf0123456789ab")],
        )?;

        let seen = vmm.drain();
        let calls: Vec<(&str, &str)> = seen
            .iter()
            .map(|seen| (seen.method.as_str(), seen.path.as_str()))
            .collect();
        assert_eq!(
            calls,
            vec![
                ("GET", "/version"),
                ("PATCH", "/vm"),
                ("PUT", "/snapshot/create"),
                ("PATCH", "/vm"),
                ("PUT", "/snapshot/load"),
                ("PATCH", "/drives/identity"),
                ("PUT", "/snapshot/load"),
            ]
        );
        assert_eq!(seen[1].body, Some(json!({"state": "Paused"})));
        assert_eq!(
            seen[2].body,
            Some(json!({
                "snapshot_type": "Full",
                "snapshot_path": "/c/vm-state",
                "mem_file_path": "/c/memory",
            }))
        );
        assert_eq!(seen[3].body, Some(json!({"state": "Resumed"})));
        let load = seen[4].body.as_ref().unwrap();
        assert_eq!(load["mem_backend"]["backend_type"], "File");
        assert_eq!(load["mem_backend"]["backend_path"], "/c/memory");
        assert_eq!(load["resume_vm"], false);
        assert!(
            load.get("network_overrides").is_none(),
            "a plain restore sends no overrides, so older Firecrackers accept it"
        );
        assert_eq!(
            seen[6].body.as_ref().unwrap()["network_overrides"],
            json!([{ "iface_id": "eth0", "host_dev_name": "lgf0123456789ab" }])
        );
        assert_eq!(
            seen[5].body.as_ref().unwrap()["path_on_host"],
            "/a/identity.img"
        );
        assert!(!seen[0].content_length_header, "GET sends no body");
        Ok(())
    }

    #[test]
    fn firecracker_errors_carry_the_fault_message() {
        let vmm = FakeVmm::start(|_| {
            (
                400,
                r#"{"fault_message":"Cannot create snapshot: VM is not paused"}"#.into(),
            )
        });
        let api = FirecrackerApi::new(&vmm.socket);
        let err = api
            .create_snapshot(Path::new("/s"), Path::new("/m"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("VM is not paused"), "{err}");
        assert!(err.contains("HTTP 400"), "{err}");
    }

    #[test]
    fn cloud_hypervisor_client_speaks_the_documented_endpoints() -> R<()> {
        let vmm = FakeVmm::start(|seen| match seen.path.as_str() {
            "/api/v1/vmm.ping" => (200, r#"{"version":"v41.0"}"#.into()),
            _ => (204, String::new()),
        });
        let api = CloudHypervisorApi::new(&vmm.socket);
        assert_eq!(api.version()?, "v41.0");
        api.pause()?;
        api.snapshot(Path::new("/c/ch"))?;
        api.resume()?;
        api.restore(Path::new("/c/ch"))?;

        let seen = vmm.drain();
        let calls: Vec<(&str, &str)> = seen
            .iter()
            .map(|seen| (seen.method.as_str(), seen.path.as_str()))
            .collect();
        assert_eq!(
            calls,
            vec![
                ("GET", "/api/v1/vmm.ping"),
                ("PUT", "/api/v1/vm.pause"),
                ("PUT", "/api/v1/vm.snapshot"),
                ("PUT", "/api/v1/vm.resume"),
                ("PUT", "/api/v1/vm.restore"),
            ]
        );
        // ch-remote sends bodiless PUTs for pause/resume; match it.
        assert!(!seen[1].content_length_header);
        assert_eq!(
            seen[2].body,
            Some(json!({"destination_url": "file:///c/ch"}))
        );
        assert_eq!(seen[4].body, Some(json!({"source_url": "file:///c/ch"})));
        Ok(())
    }

    #[test]
    fn firecracker_balloon_calls_match_the_documented_api() -> R<()> {
        let vmm = FakeVmm::start(|seen| match (seen.method.as_str(), seen.path.as_str()) {
            ("GET", "/balloon") => (
                200,
                r#"{"amount_mib":128,"deflate_on_oom":true,"stats_polling_interval_s":0}"#.into(),
            ),
            _ => (204, String::new()),
        });
        let api = FirecrackerApi::new(&vmm.socket);
        assert_eq!(api.balloon_mib()?, 128);
        api.set_balloon_mib(512)?;
        api.set_balloon_mib(0)?;
        let seen = vmm.drain();
        assert_eq!(
            seen.iter()
                .map(|seen| (seen.method.as_str(), seen.path.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("GET", "/balloon"),
                ("PATCH", "/balloon"),
                ("PATCH", "/balloon")
            ]
        );
        assert_eq!(seen[1].body, Some(json!({ "amount_mib": 512 })));
        assert_eq!(seen[2].body, Some(json!({ "amount_mib": 0 })));
        Ok(())
    }

    #[test]
    fn firecracker_without_a_balloon_says_so() {
        let vmm = FakeVmm::start(|_| {
            (
                400,
                r#"{"fault_message":"Invalid request method and/or path: GET /balloon."}"#.into(),
            )
        });
        let err = FirecrackerApi::new(&vmm.socket)
            .balloon_mib()
            .unwrap_err()
            .to_string();
        assert!(err.contains("/balloon"), "{err}");
    }

    #[test]
    fn cloud_hypervisor_balloon_calls_match_the_documented_api() -> R<()> {
        let vmm = FakeVmm::start(|seen| {
            match seen.path.as_str() {
            "/api/v1/vm.info" => (
                200,
                r#"{"config":{"balloon":{"size":268435456,"deflate_on_oom":true,"free_page_reporting":true}},"state":"Running"}"#
                    .into(),
            ),
            _ => (204, String::new()),
        }
        });
        let api = CloudHypervisorApi::new(&vmm.socket);
        assert_eq!(api.balloon_bytes()?, 256 * 1024 * 1024);
        api.set_balloon_bytes(1 << 30)?;
        let seen = vmm.drain();
        assert_eq!(seen[0].method, "GET");
        assert_eq!(seen[1].path, "/api/v1/vm.resize");
        assert_eq!(seen[1].method, "PUT");
        assert_eq!(seen[1].body, Some(json!({ "desired_balloon": 1u64 << 30 })));

        let bare = FakeVmm::start(|_| (200, r#"{"config":{},"state":"Running"}"#.into()));
        let err = CloudHypervisorApi::new(&bare.socket)
            .balloon_bytes()
            .unwrap_err()
            .to_string();
        assert!(err.contains("--balloon"), "{err}");
        Ok(())
    }

    #[test]
    fn cloud_hypervisor_errors_carry_the_plain_text_body() {
        let vmm = FakeVmm::start(|_| (500, "\"Error pausing VM\"".into()));
        let err = CloudHypervisorApi::new(&vmm.socket)
            .pause()
            .unwrap_err()
            .to_string();
        assert!(err.contains("Error pausing VM"), "{err}");
    }

    #[test]
    fn a_missing_socket_is_a_clear_error() {
        let err = FirecrackerApi::new("/nonexistent/legion-api.sock")
            .pause()
            .unwrap_err();
        assert!(format!("{err:#}").contains("API socket"), "{err:#}");
        let err = wait_for_api_socket(Path::new("/nonexistent/x.sock"), Duration::from_millis(30))
            .unwrap_err()
            .to_string();
        assert!(err.contains("did not come up"), "{err}");
    }

    #[test]
    fn responses_without_a_length_read_to_close() -> R<()> {
        let raw = b"HTTP/1.1 200 OK\r\nServer: x\r\n\r\n{\"a\":1}";
        let response = read_response(BufReader::new(&raw[..]))?;
        assert_eq!(response.status, 200);
        assert_eq!(response.json()?, json!({"a": 1}));
        Ok(())
    }
}
