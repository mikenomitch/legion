//! The guest side of identity refresh after a restore (suspend plan §6.9,
//! §6.12; docs/suspend-and-resume.md, "Identity after a resume").
//!
//! A memory restore resumes a guest mid-instruction: nothing inside it knows
//! it was frozen, moved to a new allocation, or that hours passed. So every
//! sandbox launched for memory checkpoints runs this handler (the guest
//! binary, copied into the rootfs, started by the generated init): as
//! `guest-resume`, or inside the gated datapath's agent as `guest-agent
//! --resume-watch`, which also closes the agent's relays on a restore
//! ([`Watcher`]). It polls
//! the identity disk named by `legion.identity=` on the kernel cmdline. The
//! worker writes a fresh identity there on every restore, so a change in the
//! disk's `alloc_id` or `host_time_unix_ms` means "you were just restored".
//! The handler then, in order:
//!
//! 1. reseeds the kernel RNG from the identity's fresh entropy (forks of one
//!    snapshot must never share RNG state; VMGenID reseeds too where the VMM
//!    exposes it, and this is the explicit reseed beside it),
//! 2. steps the clock to the host's time,
//! 3. sets the hostname, the NIC's MAC when the restore assigned one, and,
//!    on a TAP-wired guest, the address Legion leased it (flushing whatever
//!    address the captured guest had, see [`crate::netconf`]),
//! 4. writes the refreshed identity where the workload can read it:
//!    `/legion/identity.json` (mode 0600), `/legion/resume.env` (the
//!    refreshed env, including `LEGION_ALLOC_ID` and every env-delivered
//!    workload-identity token), and `/legion/identity/<ENV>` per token,
//! 5. runs `suspend.onResume` with that env, after everything above.
//!
//! The same disk carries workload-identity token refreshes with no restore.
//! The worker rewrites it in place with the same allocation and host time
//! and new tokens, and the handler then only rewrites the token files
//! ([`apply_tokens`]): no reseed, no clock step, no `onResume`. It also
//! writes the tokens the disk carries at boot, so `/legion/identity/` is
//! populated from the start (docs/workload-identity.md, "Sandboxes").
//!
//! The logic is a pure function over a [`GuestHost`], so it is tested with a
//! fake on any platform. The real host is Linux-only, like the guest.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result as R};

use crate::identity::ResumeIdentity;

/// Where the refreshed identity lands inside the guest.
pub const GUEST_IDENTITY_JSON: &str = "/legion/identity.json";
/// The refreshed env, in the same `export KEY='value'` form as `/legion/env`.
pub const GUEST_RESUME_ENV: &str = "/legion/resume.env";
/// One file per env-delivered workload-identity token.
pub const GUEST_TOKEN_DIR: &str = "/legion/identity";

/// How long `suspend.onResume` may run before the handler kills it.
pub const ON_RESUME_TIMEOUT: Duration = Duration::from_secs(30);

/// What the resume handler does to the guest. The Linux implementation
/// talks to the kernel; tests substitute a recorder.
pub trait GuestHost {
    /// Mix `entropy` into the kernel RNG and credit it.
    fn reseed(&self, entropy: &[u8]) -> R<()>;
    /// Step the wall clock to `unix_ms`.
    fn set_clock(&self, unix_ms: i64) -> R<()>;
    fn set_hostname(&self, hostname: &str) -> R<()>;
    /// Give the guest NIC a new MAC address.
    fn set_mac(&self, mac: &str) -> R<()>;
    /// Re-address the guest NIC with a leased address, its default route,
    /// and its resolvers.
    fn set_address(&self, addr: &crate::identity::GuestAddr) -> R<()>;
    /// Write a file atomically with `mode`.
    fn write_file(&self, path: &Path, contents: &[u8], mode: u32) -> R<()>;
    /// Run `argv` with `env` added, wait up to `timeout`, and return its
    /// exit code (killing it at the timeout).
    fn run(&self, argv: &[String], env: &BTreeMap<String, String>, timeout: Duration) -> R<i32>;
}

/// What one refresh did, for the guest's console log.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResumeOutcome {
    /// Steps that worked, in order.
    pub done: Vec<String>,
    /// Steps that failed, with why. A failed step never stops the later
    /// ones: a guest with a stale clock is still better off with a new token.
    pub failed: Vec<String>,
    /// `onResume`'s exit code, when it ran.
    pub on_resume_exit: Option<i32>,
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).ok())
        .collect()
}

/// Apply a freshly written identity to the guest (see the module docs for
/// the order and why).
pub fn apply_resume(identity: &ResumeIdentity, host: &dyn GuestHost) -> ResumeOutcome {
    let mut outcome = ResumeOutcome::default();
    let mut step = |name: &str, result: R<()>| match result {
        Ok(()) => outcome.done.push(name.to_string()),
        Err(err) => outcome.failed.push(format!("{name}: {err:#}")),
    };

    match decode_hex(&identity.entropy).filter(|bytes| !bytes.is_empty()) {
        Some(entropy) => step("reseed", host.reseed(&entropy)),
        None => step("reseed", Err(anyhow!("the identity carries no entropy"))),
    }
    if identity.host_time_unix_ms > 0 {
        step("clock", host.set_clock(identity.host_time_unix_ms));
    }
    if !identity.hostname.is_empty() {
        step("hostname", host.set_hostname(&identity.hostname));
    }
    if let Some(mac) = identity.mac.as_deref() {
        step("nic", host.set_mac(mac));
    }
    if let Some(addr) = identity.guest_addr.as_ref() {
        step("address", host.set_address(addr));
    }

    let tokens = apply_tokens(identity, host);
    outcome.done.extend(tokens.done);
    outcome.failed.extend(tokens.failed);
    let env = resume_env(identity);

    if !identity.on_resume.is_empty() {
        match host.run(&identity.on_resume, &env, ON_RESUME_TIMEOUT) {
            Ok(code) => {
                outcome.on_resume_exit = Some(code);
                if code == 0 {
                    outcome.done.push("onResume".into());
                } else {
                    outcome
                        .failed
                        .push(format!("onResume: exited with status {code}"));
                }
            }
            Err(err) => outcome.failed.push(format!("onResume: {err:#}")),
        }
    }
    outcome
}

/// The env the handler publishes in `/legion/resume.env` and hands to
/// `onResume`: the identity's env plus `LEGION_ALLOC_ID`,
/// `LEGION_INSTANCE_ID`, and a TAP guest's `LEGION_GUEST_ADDR`.
fn resume_env(identity: &ResumeIdentity) -> BTreeMap<String, String> {
    let mut env = identity.env.clone();
    env.insert("LEGION_ALLOC_ID".into(), identity.alloc_id.to_string());
    if let Some(instance) = identity.instance_id.as_deref() {
        env.insert("LEGION_INSTANCE_ID".into(), instance.to_string());
    }
    if let Some(addr) = identity.guest_addr.as_ref() {
        env.insert("LEGION_GUEST_ADDR".into(), addr.address.clone());
    }
    env
}

/// Write what the identity says the workload reads, without touching
/// anything else in the guest: `/legion/identity.json`, `/legion/resume.env`,
/// one `/legion/identity/<ENV>` per env-delivered token, and each
/// file-delivered token at its own path and mode. A restore runs this as one
/// step of [`apply_resume`]; a token refresh runs only this.
pub fn apply_tokens(identity: &ResumeIdentity, host: &dyn GuestHost) -> ResumeOutcome {
    let mut outcome = ResumeOutcome::default();
    let mut step = |name: &str, result: R<()>| match result {
        Ok(()) => outcome.done.push(name.to_string()),
        Err(err) => outcome.failed.push(format!("{name}: {err:#}")),
    };
    let json = serde_json::to_vec_pretty(identity).unwrap_or_default();
    step(
        "identity.json",
        host.write_file(Path::new(GUEST_IDENTITY_JSON), &json, 0o600),
    );
    step(
        "resume.env",
        host.write_file(
            Path::new(GUEST_RESUME_ENV),
            crate::env::render_env_file(&resume_env(identity)).as_bytes(),
            0o600,
        ),
    );
    for (key, value) in &identity.env {
        if key.starts_with("LEGION_") {
            continue;
        }
        let path = PathBuf::from(GUEST_TOKEN_DIR).join(key);
        step(
            &format!("token {key}"),
            host.write_file(&path, value.as_bytes(), 0o400),
        );
    }
    for file in &identity.files {
        // The worker only names paths under /legion/identity (checked at
        // submit and again when it builds the list). Refuse anything else
        // here too, so a bad disk can never write elsewhere in the guest.
        let path = Path::new(&file.path);
        if !path.starts_with(GUEST_TOKEN_DIR)
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            step(
                &format!("token file {}", file.path),
                Err(anyhow!("outside {GUEST_TOKEN_DIR}")),
            );
            continue;
        }
        step(
            &format!("token file {}", file.path),
            host.write_file(path, file.token.as_bytes(), file.mode),
        );
    }
    outcome
}

/// Whether `current` refreshes only tokens relative to `seen`: the same
/// allocation and host time (no restore), different tokens.
pub fn is_token_refresh(seen: &ResumeIdentity, current: &ResumeIdentity) -> bool {
    !is_new_identity(seen, current) && seen.tokens_differ(current)
}

/// Whether `current` is a new identity relative to `seen`: a restore names a
/// new allocation, or at least rewrites the host time.
pub fn is_new_identity(seen: &ResumeIdentity, current: &ResumeIdentity) -> bool {
    seen.alloc_id != current.alloc_id || seen.host_time_unix_ms != current.host_time_unix_ms
}

/// The identity device named on the kernel cmdline (`legion.identity=`).
pub fn identity_device_from_cmdline(cmdline: &str) -> Option<PathBuf> {
    cmdline
        .split_whitespace()
        .find_map(|arg| arg.strip_prefix("legion.identity="))
        .filter(|device| !device.is_empty())
        .map(PathBuf::from)
}

/// Read the identity disk, bypassing the guest's block cache: after a
/// restore, the page cache still holds what the disk said at capture.
pub fn read_identity_fresh(device: &Path) -> R<ResumeIdentity> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // BLKFLSBUF: flush and drop the device's buffer cache.
        const BLKFLSBUF: libc::c_ulong = 0x1261;
        if let Ok(file) = std::fs::File::open(device) {
            // SAFETY: a plain ioctl on an fd we own, with no argument.
            unsafe {
                libc::ioctl(file.as_raw_fd(), BLKFLSBUF as _, 0);
            }
        }
    }
    crate::identity::read_identity_disk(device)
}

/// What one read of the identity disk meant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    /// Nothing new: the identity the watcher already applied.
    Unchanged,
    /// The first identity the watcher saw (the boot identity). Its tokens,
    /// if any, were written.
    Boot,
    /// The same allocation with new workload-identity tokens, written.
    Tokens,
    /// A restore: the guest now belongs to `alloc_id`, and the identity was
    /// applied with this outcome.
    Restored {
        alloc_id: i64,
        outcome: ResumeOutcome,
    },
}

/// The resume watch's state: the identity it last applied. Each read of the
/// disk goes through [`Watcher::observe`], which decides what changed and
/// applies it. `on_restore` runs first on a restore, before anything else is
/// refreshed: the guest agent uses it to close the relays the capture left
/// dead, so clients reconnect through the restoring allocation's listener.
pub struct Watcher<'a> {
    seen: Option<ResumeIdentity>,
    host: &'a dyn GuestHost,
    on_restore: &'a (dyn Fn() + Sync),
}

impl<'a> Watcher<'a> {
    pub fn new(host: &'a dyn GuestHost, on_restore: &'a (dyn Fn() + Sync)) -> Self {
        Watcher {
            seen: None,
            host,
            on_restore,
        }
    }

    pub fn observe(&mut self, current: ResumeIdentity) -> Observed {
        let Some(seen) = self.seen.as_ref() else {
            // Tokens the disk carries at boot land in /legion/identity
            // right away.
            if current.tokens_differ(&ResumeIdentity::default()) {
                log_tokens("boot", &apply_tokens(&current, self.host));
            }
            self.seen = Some(current);
            return Observed::Boot;
        };
        if is_token_refresh(seen, &current) {
            log_tokens("refresh", &apply_tokens(&current, self.host));
            self.seen = Some(current);
            return Observed::Tokens;
        }
        if !is_new_identity(seen, &current) {
            return Observed::Unchanged;
        }
        eprintln!(
            "legion-guest-resume: restored as allocation {}; refreshing identity",
            current.alloc_id
        );
        (self.on_restore)();
        let outcome = apply_resume(&current, self.host);
        let alloc_id = current.alloc_id;
        self.seen = Some(current);
        Observed::Restored { alloc_id, outcome }
    }
}

/// The watch loop the resume handler runs: remember the boot identity, then
/// poll for a new one and apply it. Never returns.
pub fn watch(device: &Path, host: &dyn GuestHost, poll: Duration) -> ! {
    watch_with(device, host, poll, &|| {})
}

/// [`watch`], running `on_restore` first on every restore (see
/// [`Watcher`]).
pub fn watch_with(
    device: &Path,
    host: &dyn GuestHost,
    poll: Duration,
    on_restore: &(dyn Fn() + Sync),
) -> ! {
    let mut watcher = Watcher::new(host, on_restore);
    if let Ok(boot) = read_identity_fresh(device) {
        watcher.observe(boot);
    }
    loop {
        std::thread::sleep(poll);
        let Ok(current) = read_identity_fresh(device) else {
            continue;
        };
        if let Observed::Restored { outcome, .. } = watcher.observe(current) {
            eprintln!(
                "legion-guest-resume: refreshed {}{}",
                outcome.done.join(", "),
                if outcome.failed.is_empty() {
                    String::new()
                } else {
                    format!("; FAILED {}", outcome.failed.join("; "))
                }
            );
        }
    }
}

/// One console line for a token write that was not part of a restore.
fn log_tokens(why: &str, outcome: &ResumeOutcome) {
    if outcome.failed.is_empty() {
        eprintln!("legion-guest-resume: {why}: wrote identity tokens");
    } else {
        eprintln!(
            "legion-guest-resume: {why}: FAILED {}",
            outcome.failed.join("; ")
        );
    }
}

/// The real guest: Linux syscalls and files.
#[cfg(target_os = "linux")]
pub struct LinuxGuest;

#[cfg(target_os = "linux")]
impl GuestHost for LinuxGuest {
    fn reseed(&self, entropy: &[u8]) -> R<()> {
        use std::io::Write;
        use std::os::unix::io::AsRawFd;
        let mut random = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/urandom")
            .context("open /dev/urandom")?;
        // Writing mixes the bytes in without crediting them; RNDADDENTROPY
        // also credits them, which unblocks a pool a restore left stale.
        random.write_all(entropy)?;
        // struct rand_pool_info { int entropy_count; int buf_size; __u32 buf[]; }
        let mut info = Vec::with_capacity(8 + entropy.len());
        info.extend_from_slice(&((entropy.len() * 8) as i32).to_ne_bytes());
        info.extend_from_slice(&(entropy.len() as i32).to_ne_bytes());
        info.extend_from_slice(entropy);
        // RNDADDENTROPY = _IOW('R', 0x03, int[2])
        const RNDADDENTROPY: libc::c_ulong = 0x4008_5203;
        // SAFETY: `info` is a valid rand_pool_info for its stated length.
        let rc = unsafe { libc::ioctl(random.as_raw_fd(), RNDADDENTROPY as _, info.as_ptr()) };
        if rc != 0 {
            bail!("RNDADDENTROPY failed: {}", std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn set_clock(&self, unix_ms: i64) -> R<()> {
        // `as _`: the field types differ across Linux targets.
        let spec = libc::timespec {
            tv_sec: unix_ms.div_euclid(1000) as _,
            tv_nsec: (unix_ms.rem_euclid(1000) * 1_000_000) as _,
        };
        // SAFETY: a valid timespec for CLOCK_REALTIME.
        let rc = unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &spec) };
        if rc != 0 {
            bail!("clock_settime failed: {}", std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn set_hostname(&self, hostname: &str) -> R<()> {
        // SAFETY: a valid buffer and its length.
        let rc = unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) };
        if rc != 0 {
            bail!("sethostname failed: {}", std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn set_mac(&self, mac: &str) -> R<()> {
        // The guest's one NIC, through netlink (no `ip` tool in the image
        // needed). The link bounces so peers relearn it.
        crate::netconf::set_mac(crate::netconf::GUEST_NIC, crate::netconf::parse_mac(mac)?)
    }

    fn set_address(&self, addr: &crate::identity::GuestAddr) -> R<()> {
        let parsed = crate::netconf::parse(addr)?;
        crate::netconf::apply(crate::netconf::GUEST_NIC, &parsed)?;
        if !parsed.dns.is_empty() {
            self.write_file(
                Path::new("/etc/resolv.conf"),
                crate::netconf::resolv_conf(&parsed.dns).as_bytes(),
                0o644,
            )?;
        }
        Ok(())
    }

    fn write_file(&self, path: &Path, contents: &[u8], mode: u32) -> R<()> {
        use std::os::unix::fs::PermissionsExt;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let staging = path.with_extension("new");
        std::fs::write(&staging, contents)?;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(mode))?;
        std::fs::rename(&staging, path)?;
        Ok(())
    }

    fn run(&self, argv: &[String], env: &BTreeMap<String, String>, timeout: Duration) -> R<i32> {
        let (program, args) = argv.split_first().context("onResume is empty")?;
        let mut child = std::process::Command::new(program)
            .args(args)
            .envs(env)
            .spawn()
            .with_context(|| format!("start onResume {program}"))?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status.code().unwrap_or(-1));
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("timed out after {}s and was killed", timeout.as_secs());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<String>>,
        files: Mutex<BTreeMap<PathBuf, (Vec<u8>, u32)>>,
        fail_clock: bool,
        on_resume_exit: i32,
    }

    impl GuestHost for Recorder {
        fn reseed(&self, entropy: &[u8]) -> R<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("reseed {}", entropy.len()));
            Ok(())
        }
        fn set_clock(&self, unix_ms: i64) -> R<()> {
            if self.fail_clock {
                bail!("EPERM");
            }
            self.calls.lock().unwrap().push(format!("clock {unix_ms}"));
            Ok(())
        }
        fn set_hostname(&self, hostname: &str) -> R<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("hostname {hostname}"));
            Ok(())
        }
        fn set_mac(&self, mac: &str) -> R<()> {
            self.calls.lock().unwrap().push(format!("mac {mac}"));
            Ok(())
        }
        fn set_address(&self, addr: &crate::identity::GuestAddr) -> R<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("address {}/{}", addr.address, addr.prefix));
            Ok(())
        }
        fn write_file(&self, path: &Path, contents: &[u8], mode: u32) -> R<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("write {}", path.display()));
            self.files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), (contents.to_vec(), mode));
            Ok(())
        }
        fn run(
            &self,
            argv: &[String],
            env: &BTreeMap<String, String>,
            _timeout: Duration,
        ) -> R<i32> {
            self.calls.lock().unwrap().push(format!(
                "run {} alloc={}",
                argv.join(" "),
                env.get("LEGION_ALLOC_ID").cloned().unwrap_or_default()
            ));
            Ok(self.on_resume_exit)
        }
    }

    fn restored(alloc_id: i64) -> ResumeIdentity {
        ResumeIdentity {
            alloc_id,
            instance_id: Some("user-42".into()),
            job_id: "agents".into(),
            task_name: "workspace".into(),
            hostname: format!("legion-sbx-{alloc_id}"),
            token: Some(format!("token-for-{alloc_id}")),
            mac: None,
            env: [("ACME_TOKEN".to_string(), format!("token-for-{alloc_id}"))].into(),
            entropy: "00".repeat(32),
            host_time_unix_ms: 1_760_000_000_123,
            on_resume: vec!["/usr/local/bin/reconnect".into()],
            fork: false,
            files: vec![crate::identity::GuestTokenFile {
                path: "/legion/identity/vault".into(),
                mode: 0o440,
                token: format!("vault-for-{alloc_id}"),
            }],
            guest_addr: None,
        }
    }

    /// The order matters: entropy and clock before anything the workload
    /// reads, and `onResume` last, with the new identity in its env.
    #[test]
    fn a_restore_reseeds_resyncs_rewrites_identity_then_runs_on_resume() {
        let host = Recorder::default();
        let outcome = apply_resume(&restored(9), &host);
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        assert_eq!(outcome.on_resume_exit, Some(0));
        let calls = host.calls.lock().unwrap().clone();
        assert_eq!(calls[0], "reseed 32");
        assert_eq!(calls[1], "clock 1760000000123");
        assert_eq!(calls[2], "hostname legion-sbx-9");
        assert_eq!(
            calls.last().unwrap(),
            "run /usr/local/bin/reconnect alloc=9",
            "onResume runs last, with the new allocation id"
        );
        let files = host.files.lock().unwrap();
        let (token, mode) = &files[&PathBuf::from("/legion/identity/ACME_TOKEN")];
        assert_eq!(token, b"token-for-9");
        assert_eq!(*mode, 0o400);
        let (env, _) = &files[&PathBuf::from(GUEST_RESUME_ENV)];
        let env = String::from_utf8_lossy(env);
        assert!(env.contains("export LEGION_ALLOC_ID='9'"), "{env}");
        assert!(env.contains("export ACME_TOKEN='token-for-9'"), "{env}");
        let (json, mode) = &files[&PathBuf::from(GUEST_IDENTITY_JSON)];
        assert_eq!(*mode, 0o600);
        let written: ResumeIdentity = serde_json::from_slice(json).unwrap();
        assert_eq!(written.alloc_id, 9);
    }

    /// Gated plan §6.7: a TAP guest restored into a fork gets its own MAC,
    /// then its own leased address, and the workload can read the address
    /// from `LEGION_GUEST_ADDR`.
    #[test]
    fn a_tap_restore_applies_the_leased_address_after_the_mac() {
        let host = Recorder::default();
        let identity = ResumeIdentity {
            mac: Some("02:4c:00:00:00:09".into()),
            guest_addr: Some(crate::identity::GuestAddr {
                address: "10.88.128.9".into(),
                prefix: 16,
                gateway: Some("10.88.0.1".into()),
                dns: vec!["10.88.0.1".into()],
                bridge: "br0".into(),
            }),
            ..restored(9)
        };
        let outcome = apply_resume(&identity, &host);
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        let calls = host.calls.lock().unwrap().clone();
        let mac_at = calls.iter().position(|c| c.starts_with("mac ")).unwrap();
        let addr_at = calls
            .iter()
            .position(|c| c == "address 10.88.128.9/16")
            .expect("re-addressed");
        assert!(mac_at < addr_at, "{calls:?}");
        let files = host.files.lock().unwrap();
        let env = String::from_utf8_lossy(&files[&PathBuf::from(GUEST_RESUME_ENV)].0).into_owned();
        assert!(
            env.contains("export LEGION_GUEST_ADDR='10.88.128.9'"),
            "{env}"
        );
    }

    #[test]
    fn a_failed_step_does_not_stop_the_rest() {
        let host = Recorder {
            fail_clock: true,
            on_resume_exit: 3,
            ..Default::default()
        };
        let outcome = apply_resume(&restored(9), &host);
        assert!(outcome.failed.iter().any(|f| f.starts_with("clock: EPERM")));
        assert!(outcome
            .failed
            .iter()
            .any(|f| f == "onResume: exited with status 3"));
        assert!(outcome.done.contains(&"identity.json".to_string()));
    }

    #[test]
    fn a_new_allocation_or_host_time_is_a_restore() {
        let boot = restored(7);
        assert!(!is_new_identity(&boot, &boot.clone()));
        assert!(is_new_identity(&boot, &restored(9)));
        let rewritten = ResumeIdentity {
            host_time_unix_ms: boot.host_time_unix_ms + 1,
            ..boot.clone()
        };
        assert!(is_new_identity(&boot, &rewritten));
    }

    /// A refreshed token rewrites the disk with the same allocation and
    /// host time: that is a token refresh, which writes the token files and
    /// nothing else (no reseed, no clock step, no `onResume`).
    #[test]
    fn a_token_refresh_only_rewrites_the_token_files() {
        let boot = restored(7);
        let refreshed = ResumeIdentity {
            env: [("ACME_TOKEN".to_string(), "fresh".to_string())].into(),
            files: vec![crate::identity::GuestTokenFile {
                token: "fresh-vault".into(),
                ..boot.files[0].clone()
            }],
            ..boot.clone()
        };
        assert!(is_token_refresh(&boot, &refreshed));
        assert!(!is_new_identity(&boot, &refreshed));
        assert!(!is_token_refresh(&boot, &boot.clone()));
        assert!(
            !is_token_refresh(&boot, &restored(9)),
            "a restore is not a refresh"
        );
        // LEGION_* env is not a token.
        let relabeled = ResumeIdentity {
            env: boot
                .env
                .clone()
                .into_iter()
                .chain([("LEGION_X".to_string(), "1".to_string())])
                .collect(),
            ..boot.clone()
        };
        assert!(!is_token_refresh(&boot, &relabeled));

        let host = Recorder::default();
        let outcome = apply_tokens(&refreshed, &host);
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        let calls = host.calls.lock().unwrap().clone();
        assert!(
            calls.iter().all(|call| !call.starts_with("reseed")
                && !call.starts_with("clock")
                && !call.starts_with("run")),
            "{calls:?}"
        );
        let files = host.files.lock().unwrap();
        assert_eq!(
            files[&PathBuf::from("/legion/identity/ACME_TOKEN")].0,
            b"fresh"
        );
        let (vault, mode) = &files[&PathBuf::from("/legion/identity/vault")];
        assert_eq!(vault, b"fresh-vault");
        assert_eq!(*mode, 0o440);
    }

    #[test]
    fn a_token_file_outside_the_identity_dir_is_refused() {
        let mut identity = restored(7);
        identity.files[0].path = "/legion/identity/../etc/passwd".into();
        identity.files.push(crate::identity::GuestTokenFile {
            path: "/etc/shadow".into(),
            mode: 0o400,
            token: "x".into(),
        });
        let host = Recorder::default();
        let outcome = apply_tokens(&identity, &host);
        assert_eq!(outcome.failed.len(), 2, "{:?}", outcome.failed);
        assert!(!host
            .files
            .lock()
            .unwrap()
            .keys()
            .any(|path| path.starts_with("/etc") || path.to_string_lossy().contains("..")));
    }

    #[test]
    fn the_identity_device_comes_from_the_cmdline() {
        assert_eq!(
            identity_device_from_cmdline(
                "console=ttyS0 init=/legion/init legion.identity=/dev/vdc"
            ),
            Some(PathBuf::from("/dev/vdc"))
        );
        assert_eq!(identity_device_from_cmdline("console=ttyS0"), None);
    }

    /// The watcher reads the same disk format the worker writes.
    #[test]
    fn the_identity_disk_round_trips_through_the_fresh_read() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("identity.img");
        crate::identity::write_identity_disk(&path, &restored(11))?;
        assert_eq!(read_identity_fresh(&path)?.alloc_id, 11);
        Ok(())
    }

    /// The watcher applies the boot identity's tokens, a token refresh, and
    /// a restore, each once, and runs the restore hook only on a restore.
    #[test]
    fn the_watcher_runs_the_restore_hook_only_on_a_restore() {
        let host = Recorder::default();
        let hooks = std::sync::atomic::AtomicUsize::new(0);
        let hook = || {
            hooks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        };
        let mut watcher = Watcher::new(&host, &hook);
        let boot = restored(7);
        assert_eq!(watcher.observe(boot.clone()), Observed::Boot);
        assert_eq!(watcher.observe(boot.clone()), Observed::Unchanged);
        let refreshed = ResumeIdentity {
            env: [("ACME_TOKEN".to_string(), "fresh".to_string())].into(),
            ..boot.clone()
        };
        assert_eq!(watcher.observe(refreshed), Observed::Tokens);
        assert_eq!(hooks.load(std::sync::atomic::Ordering::SeqCst), 0);
        match watcher.observe(restored(9)) {
            Observed::Restored { alloc_id, outcome } => {
                assert_eq!(alloc_id, 9);
                assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
            }
            other => panic!("expected a restore, got {other:?}"),
        }
        assert_eq!(hooks.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(watcher.observe(restored(9)), Observed::Unchanged);
    }

    /// Gated plan §6.3: a relay the capture left dead (here, a host stream
    /// that never answers, the half-open case an old cloud-hypervisor can
    /// leave) is closed the moment the agent's watch sees a new allocation,
    /// so the guest client gets a prompt close instead of a hang. A new
    /// connection afterwards dials a fresh host stream.
    #[test]
    fn the_agent_closes_its_relays_when_the_guest_is_restored() -> R<()> {
        use crate::agent::{self, AgentConfig, HostDial, UnixDial, PROXY_PORT};
        use std::io::Read as _;
        use std::io::Write as _;
        use std::sync::Arc;

        let dir = tempfile::tempdir()?;
        let base = dir.path().join("vsock.sock");
        // The host end: accept and hold every stream, never reply.
        let listener =
            std::os::unix::net::UnixListener::bind(agent::host_socket_path(&base, PROXY_PORT))?;
        let held = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let held = held.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    held.lock().unwrap().push(stream);
                }
            });
        }
        let dial: Arc<dyn HostDial> = Arc::new(UnixDial { base });
        let started = agent::start(
            AgentConfig {
                proxy_listen: "127.0.0.1:0".parse().unwrap(),
                dns_listen: None,
                ..Default::default()
            },
            dial,
        )?;
        let mut client = std::net::TcpStream::connect(started.proxy_addr)?;
        client.write_all(b"CONNECT api.example.com:443 HTTP/1.1\r\n\r\n")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while started.relays.open_count() == 0 {
            assert!(std::time::Instant::now() < deadline, "relay never opened");
            std::thread::sleep(Duration::from_millis(10));
        }

        let host = Recorder::default();
        let relays = started.relays.clone();
        let hook = move || {
            relays.close_all();
        };
        let mut watcher = Watcher::new(&host, &hook);
        watcher.observe(restored(7));
        assert!(matches!(
            watcher.observe(restored(8)),
            Observed::Restored { alloc_id: 8, .. }
        ));

        // The client sees its connection end at once, not at a timeout.
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        let started_read = std::time::Instant::now();
        let mut buf = [0u8; 16];
        let read = client.read(&mut buf);
        assert!(
            started_read.elapsed() < Duration::from_secs(4),
            "the relay was not closed: {read:?}"
        );
        assert!(!matches!(read, Ok(n) if n > 0), "{read:?}");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while started.relays.open_count() != 0 {
            assert!(std::time::Instant::now() < deadline, "relay never finished");
            std::thread::sleep(Duration::from_millis(10));
        }

        // A new guest connection dials a fresh host stream.
        let before = held.lock().unwrap().len();
        let mut again = std::net::TcpStream::connect(started.proxy_addr)?;
        again.write_all(b"x")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while held.lock().unwrap().len() == before {
            assert!(std::time::Instant::now() < deadline, "no fresh host stream");
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}
