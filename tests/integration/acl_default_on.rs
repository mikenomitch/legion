//! ACLs are on by default (worker-plane-auth plan, Phase 0).
//!
//! Runs the real binary with a config that never mentions `acl`, on an
//! ephemeral port, with the data dir and CLI token file inside a temp dir so
//! it never touches a developer's `~/.legion/tokens/`. It checks the plan's
//! acceptance criteria end to end:
//!
//! - the manager mints the initial management token at first start and
//!   writes it to `<dataDir>/bootstrap-token` and the CLI token file, both
//!   mode 0600;
//! - a call without a token is refused with `Unauthenticated`;
//! - the CLI on the same machine works with no flags or env vars beyond the
//!   token file the manager wrote;
//! - a restart mints nothing new.

#![cfg(unix)]

use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

struct Paths {
    config: PathBuf,
    data_dir: PathBuf,
    cli_token: PathBuf,
    home: PathBuf,
}

fn start_manager(paths: &Paths, port: u16) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_legion"))
        .args(["start", "manager", "-c"])
        .arg(&paths.config)
        .env("LEGION_DATA_DIR", &paths.data_dir)
        .env("LEGION_TOKEN_FILE", &paths.cli_token)
        .env("HOME", &paths.home)
        .env_remove("LEGION_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn legion manager");
    let started = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(status) = child.try_wait().expect("poll manager") {
            panic!("manager exited before serving: {status}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "manager never started listening on {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    child
}

fn stop_manager(mut child: Child) {
    // SAFETY: plain kill(2) on our own child's pid.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    let signalled = Instant::now();
    while child.try_wait().expect("poll manager").is_none() {
        if signalled.elapsed() > Duration::from_secs(40) {
            let _ = child.kill();
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Runs the CLI with `--json` and returns the parsed envelope. `token_file`
/// is the only credential source the child sees.
fn cli(port: u16, home: &Path, token_file: &Path, args: &[&str]) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_legion"))
        .args(args)
        .arg("-j")
        .env("LEGION_ADDR", format!("http://127.0.0.1:{port}"))
        .env("LEGION_TOKEN_FILE", token_file)
        .env("HOME", home)
        .env_remove("LEGION_TOKEN")
        .stdin(Stdio::null())
        .output()
        .expect("run legion");
    let text = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(text.trim()).unwrap_or_else(|err| {
        panic!(
            "legion {args:?} printed no JSON envelope ({err}): stdout={text} stderr={}",
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .unwrap_or_else(|err| panic!("stat {}: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

fn wait_for(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn default_config_requires_a_token_and_mints_one_at_first_start_test() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let paths = Paths {
        config: dir.path().join("manager.json"),
        data_dir: dir.path().join("data"),
        cli_token: home.join(".legion").join("token"),
        home: home.clone(),
    };
    // No `acl` key: the default is what is under test.
    let config = serde_json::json!({
        "server": { "host": "127.0.0.1", "port": port },
        "storePath": dir.path().join("store").to_str().expect("utf-8 path"),
        "exec": { "enabled": false },
        "telemetry": { "enabled": false },
        // No system worker: it would bind a worker port nobody reserved
        // (tests/integration/system_worker.rs covers it).
        "systemWorker": { "enabled": false },
    });
    std::fs::write(&paths.config, config.to_string()).expect("write config");

    let manager = start_manager(&paths, port);
    let bootstrap = paths.data_dir.join("bootstrap-token");
    wait_for(&bootstrap);
    wait_for(&paths.cli_token);
    assert_eq!(mode(&bootstrap), 0o600, "bootstrap-token must be 0600");
    assert_eq!(mode(&paths.cli_token), 0o600, "CLI token file must be 0600");
    let secret = std::fs::read_to_string(&bootstrap).expect("read bootstrap-token");
    assert!(secret.trim().starts_with("lgn_"), "unexpected token shape");
    assert_eq!(
        secret,
        std::fs::read_to_string(&paths.cli_token).expect("read CLI token"),
        "both files hold the same secret"
    );

    // No credential: refused as unauthenticated, with a hint naming the file.
    let no_token = dir.path().join("no-such-token");
    let denied = cli(port, &home, &no_token, &["job", "list"]);
    assert_eq!(denied["ok"], false, "{denied}");
    assert_eq!(denied["error"]["code"], "Unauthenticated", "{denied}");
    assert!(
        denied["error"]["hint"]
            .as_str()
            .unwrap_or_default()
            .contains("bootstrap-token"),
        "{denied}"
    );

    // The CLI on the manager's machine works with the file the manager wrote.
    let listed = cli(port, &home, &paths.cli_token, &["job", "list"]);
    assert_eq!(listed["ok"], true, "{listed}");
    let whoami = cli(port, &home, &paths.cli_token, &["acl", "whoami"]);
    assert_eq!(whoami["ok"], true, "{whoami}");
    assert_eq!(whoami["data"]["aclEnabled"], true, "{whoami}");

    // A restart mints nothing new.
    stop_manager(manager);
    let manager = start_manager(&paths, port);
    let tokens = cli(port, &home, &paths.cli_token, &["acl", "token", "list"]);
    assert_eq!(tokens["ok"], true, "{tokens}");
    let management = tokens["data"]
        .as_array()
        .or_else(|| tokens["data"]["tokens"].as_array())
        .expect("token list")
        .iter()
        .filter(|token| token["tokenType"] == "management")
        .count();
    assert_eq!(
        management, 1,
        "a restart must not mint a second token: {tokens}"
    );
    assert_eq!(
        secret,
        std::fs::read_to_string(&bootstrap).expect("read bootstrap-token"),
        "a restart must not rewrite the bootstrap token"
    );
    stop_manager(manager);
}
