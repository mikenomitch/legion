//! Measured usage: what each governed allocation and the node as a whole
//! actually use, read from cgroup v2 and `/proc`.
//!
//! The worker samples every allocation that runs in a worker-owned cgroup
//! (see `src/worker/resources.rs`) once per [`USAGE_REPORT_INTERVAL`] and
//! sends the result on the heartbeat that falls due
//! (`HeartbeatRequest.usage`). The manager keeps the latest report in memory
//! and serves it through `GetNodeUsage` (`src/manager/usage.rs`).
//!
//! Everything that touches the kernel is a thin read of one file, and every
//! parse is a pure function over the file's text, so the arithmetic is tested
//! against fixture files on any machine. See `docs/resource-enforcement.md`,
//! "Measuring usage".

use crate::structs::{AllocationUsage, NodeMemoryUsage, UsageReport};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How often the worker samples and reports usage. Fixed rather than
/// configurable: usage is a display and a later input to the pressure
/// response, both of which want seconds, not milliseconds, and a knob nobody
/// needs is one more thing to document.
pub const USAGE_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// CPU use, in thousandths of a core, at or above which an interval counts as
/// activity. One percent of a core: enough to ignore a kernel thread waking
/// up, low enough that any real request handling registers.
pub const ACTIVE_CPU_FLOOR_MILLICORES: u64 = 10;

/// One allocation's cgroup at a point in time.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CgroupSample {
    pub memory_current_bytes: u64,
    /// `memory.peak`, when the kernel has it (5.19+).
    pub memory_peak_bytes: Option<u64>,
    pub memory_anon_bytes: u64,
    pub cpu_usage_usec: u64,
    pub psi_memory_some_avg10: f64,
    /// Cumulative bytes the guest sent on its own NIC, for an allocation
    /// whose traffic never crosses the node datapath: a sandbox's TAP device
    /// (its `rx_bytes`, since the host receives what the guest sends) or an
    /// Apple container's interface. A rise between samples is network
    /// activity. Only the guest's sends count: what arrives on a bridge
    /// includes broadcast noise a quiet guest never asked for.
    pub guest_sent_bytes: Option<u64>,
}

/// Where an allocation's CPU and memory are read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageSource {
    /// A worker-owned cgroup's absolute directory (Linux, with resource
    /// enforcement on).
    Cgroup(PathBuf),
    /// A dev-mode sandbox on Apple `container` (macOS), by container name:
    /// `container stats` reports its CPU, memory, and network, since there
    /// is no cgroup to read.
    AppleContainer(String),
}

/// What to measure: an allocation id as the manager knows it, where its
/// usage is read from, and what the memory pressure response needs to place
/// it in order (`src/worker/pressure.rs`).
#[derive(Debug, Clone, PartialEq)]
pub struct UsageTarget {
    pub allocation_id: i64,
    pub source: UsageSource,
    /// The host TAP device a microVM sandbox's NIC is wired to, whose
    /// counters are the guest's network activity.
    pub tap_device: Option<String>,
    pub pressure: crate::worker::pressure::PressureMeta,
    /// The task's idle policy (`suspend.idle`), when it has one.
    pub idle: Option<crate::worker::idle::IdlePolicy>,
}

impl UsageTarget {
    /// The cgroup directory, for a cgroup-governed allocation.
    pub fn cgroup_dir(&self) -> Option<&Path> {
        match &self.source {
            UsageSource::Cgroup(dir) => Some(dir),
            UsageSource::AppleContainer(_) => None,
        }
    }
}

/// A TAP device's byte counters, behind a seam so the sampler is testable
/// without Linux.
pub trait NicCounters {
    /// Bytes the guest behind `tap` has sent since the device was created.
    fn guest_sent_bytes(&self, tap: &str) -> Option<u64>;
}

/// The real counters: `/sys/class/net/<tap>/statistics/rx_bytes`. On a TAP
/// device the host's receive side is what the guest transmitted.
pub struct SysfsNicCounters;

/// [`SysfsNicCounters`] against any `/sys/class/net`-shaped tree.
pub fn read_tap_guest_sent_bytes(class_net: &Path, tap: &str) -> Option<u64> {
    if tap.is_empty() || tap.contains('/') {
        return None;
    }
    read_u64(&class_net.join(tap).join("statistics").join("rx_bytes"))
}

impl NicCounters for SysfsNicCounters {
    fn guest_sent_bytes(&self, tap: &str) -> Option<u64> {
        if cfg!(target_os = "linux") {
            read_tap_guest_sent_bytes(Path::new("/sys/class/net"), tap)
        } else {
            None
        }
    }
}

/// One Apple container's `container stats --format json` entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppleContainerStats {
    pub id: String,
    #[serde(default)]
    pub cpu_usage_usec: u64,
    #[serde(default)]
    pub memory_usage_bytes: u64,
    #[serde(default)]
    pub network_rx_bytes: u64,
    #[serde(default)]
    pub network_tx_bytes: u64,
}

impl AppleContainerStats {
    /// The sample the sampler reads. Apple's VM reports no anon split, peak,
    /// or pressure stall, so those stay empty, and the container's transmit
    /// side is what the guest sent.
    pub fn sample(&self) -> CgroupSample {
        CgroupSample {
            memory_current_bytes: self.memory_usage_bytes,
            memory_peak_bytes: None,
            memory_anon_bytes: 0,
            cpu_usage_usec: self.cpu_usage_usec,
            psi_memory_some_avg10: 0.0,
            guest_sent_bytes: Some(self.network_tx_bytes),
        }
    }
}

/// Parse `container stats --format json --no-stream` (a JSON array, one
/// object per running container) into stats by container name.
pub fn parse_apple_container_stats(json: &str) -> HashMap<String, AppleContainerStats> {
    serde_json::from_str::<Vec<AppleContainerStats>>(json)
        .unwrap_or_default()
        .into_iter()
        .map(|stats| (stats.id.clone(), stats))
        .collect()
}

/// Every running Apple container's stats, from one `container stats` call.
/// Empty when the CLI is missing or its service is down: the allocations
/// are then left out of the report, the way an unreadable cgroup is.
///
/// One call for all containers, never one per name: `container stats`
/// fails the whole call when any named container is not running.
pub fn read_apple_container_stats() -> HashMap<String, AppleContainerStats> {
    let output = std::process::Command::new("container")
        .args(["stats", "--format", "json", "--no-stream"])
        .stdin(std::process::Stdio::null())
        .output();
    match output {
        Ok(output) if output.status.success() => {
            parse_apple_container_stats(&String::from_utf8_lossy(&output.stdout))
        }
        Ok(output) => {
            tracing::debug!(
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "container stats failed; Apple container sandboxes go unmeasured this report"
            );
            HashMap::new()
        }
        Err(err) => {
            tracing::debug!(error = %err, "container CLI unavailable; Apple container sandboxes go unmeasured");
            HashMap::new()
        }
    }
}

/// Read one target: its cgroup (plus its TAP's counters), or its Apple
/// container's stats from `apple` (one `container stats` call per report).
pub fn read_target_sample(
    target: &UsageTarget,
    apple: &HashMap<String, AppleContainerStats>,
    nics: &dyn NicCounters,
) -> Option<CgroupSample> {
    let mut sample = match &target.source {
        UsageSource::Cgroup(dir) => read_cgroup_sample(dir)?,
        UsageSource::AppleContainer(name) => apple.get(name)?.sample(),
    };
    if let Some(tap) = target.tap_device.as_deref() {
        sample.guest_sent_bytes = nics.guest_sent_bytes(tap).or(sample.guest_sent_bytes);
    }
    Some(sample)
}

/// Activity the sampler cannot read from a cgroup: what the datapath and the
/// exec server saw. Read fresh for every report.
#[derive(Debug, Clone, Default)]
pub struct ActivityInputs {
    /// Open datapath connections (ingress, and egress through a gated
    /// allocation's proxy) and last-bytes times per allocation
    /// (`src/worker/netd/activity.rs`).
    pub datapath: HashMap<i64, crate::worker::netd::activity::DatapathActivity>,
    /// Exec sessions open per allocation (`src/worker/exec.rs`).
    pub exec_sessions: HashMap<i64, u32>,
}

/// The value of `key` in a flat-keyed cgroup file (`memory.stat`,
/// `cpu.stat`: one `key value` pair per line).
pub fn parse_flat_keyed(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().parse::<u64>().ok())?
    })
}

/// `avg10` from the `some` line of a PSI file (`memory.pressure`,
/// `/proc/pressure/memory`):
///
/// ```text
/// some avg10=1.53 avg60=0.40 avg300=0.08 total=123456
/// full avg10=0.00 avg60=0.00 avg300=0.00 total=0
/// ```
pub fn parse_psi_some_avg10(text: &str) -> Option<f64> {
    let line = text.lines().find(|line| line.starts_with("some "))?;
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("avg10="))
        .and_then(|value| value.parse::<f64>().ok())
}

/// `(MemTotal, MemAvailable)` in bytes from `/proc/meminfo`, whose values are
/// in kB.
pub fn parse_meminfo(text: &str) -> Option<(u64, u64)> {
    let field = |name: &str| {
        text.lines().find_map(|line| {
            let rest = line.strip_prefix(name)?.strip_prefix(':')?;
            let kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            Some(kib * 1024)
        })
    };
    Some((field("MemTotal")?, field("MemAvailable")?))
}

fn read_u64(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Sample one cgroup directory. `None` when the cgroup is gone or has no
/// memory controller (`memory.current` unreadable), which happens in the
/// window between an allocation's teardown and the next sample.
pub fn read_cgroup_sample(dir: &Path) -> Option<CgroupSample> {
    let memory_current_bytes = read_u64(&dir.join("memory.current"))?;
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap_or_default();
    Some(CgroupSample {
        memory_current_bytes,
        memory_peak_bytes: read_u64(&dir.join("memory.peak")),
        memory_anon_bytes: parse_flat_keyed(&read("memory.stat"), "anon").unwrap_or(0),
        cpu_usage_usec: parse_flat_keyed(&read("cpu.stat"), "usage_usec").unwrap_or(0),
        psi_memory_some_avg10: parse_psi_some_avg10(&read("memory.pressure")).unwrap_or(0.0),
        guest_sent_bytes: None,
    })
}

/// Node-wide memory from a `/proc`-shaped directory (`meminfo`,
/// `pressure/memory`). Split from [`read_node_memory`] so tests can point it
/// at a fixture tree.
pub fn read_node_memory_from(proc_root: &Path) -> Option<NodeMemoryUsage> {
    let meminfo = std::fs::read_to_string(proc_root.join("meminfo")).ok()?;
    let (total_bytes, available_bytes) = parse_meminfo(&meminfo)?;
    let psi = std::fs::read_to_string(proc_root.join("pressure").join("memory"))
        .ok()
        .and_then(|text| parse_psi_some_avg10(&text));
    Some(NodeMemoryUsage {
        total_bytes,
        available_bytes,
        psi_some_avg10: psi.unwrap_or(0.0),
        psi_available: psi.is_some(),
    })
}

/// Node-wide memory on this machine. `None` off Linux, where there is no
/// `/proc/meminfo` to read.
pub fn read_node_memory() -> Option<NodeMemoryUsage> {
    if cfg!(target_os = "linux") {
        read_node_memory_from(Path::new("/proc"))
    } else {
        None
    }
}

/// What the sampler remembers about one allocation between reports.
#[derive(Debug, Clone)]
struct AllocationState {
    cpu_usage_usec: u64,
    sampled_at_unix_ms: i64,
    peak_bytes: u64,
    last_activity_unix: i64,
    guest_sent_bytes: Option<u64>,
}

/// Turns periodic cgroup samples into [`UsageReport`]s: decides when a
/// report is due, derives CPU rates and activity from consecutive samples,
/// and tracks peaks on kernels without `memory.peak`.
#[derive(Debug, Default)]
pub struct UsageSampler {
    last_report: Option<Instant>,
    allocations: HashMap<i64, AllocationState>,
    /// Allocations the datapath saw traffic for since the last report.
    network_activity: HashSet<i64>,
    /// Idle-since per allocation with an idle policy.
    idle: crate::worker::idle::IdleTracker,
}

impl UsageSampler {
    /// Whether a report is due at `now`: always on the first call, then once
    /// per [`USAGE_REPORT_INTERVAL`].
    pub fn due(&self, now: Instant) -> bool {
        self.last_report
            .is_none_or(|last| now.duration_since(last) >= USAGE_REPORT_INTERVAL)
    }

    /// Record that these allocations moved traffic through the datapath.
    /// Called every heartbeat, so activity between reports is not lost.
    pub fn note_network_activity(&mut self, allocation_ids: impl IntoIterator<Item = i64>) {
        self.network_activity.extend(allocation_ids);
    }

    /// Take one report. `read` samples one target (the real reader is
    /// [`read_target_sample`]); targets it cannot read are left out.
    pub fn sample(
        &mut self,
        now: Instant,
        now_unix_ms: i64,
        targets: &[UsageTarget],
        read: impl Fn(&UsageTarget) -> Option<CgroupSample>,
        node_memory: Option<NodeMemoryUsage>,
    ) -> UsageReport {
        self.sample_with(
            now,
            now_unix_ms,
            targets,
            read,
            node_memory,
            &ActivityInputs::default(),
        )
    }

    /// [`Self::sample`], with what the datapath and exec server saw. Each
    /// allocation whose task has an idle policy also gets its idle-since
    /// time, from the signals that policy counts.
    pub fn sample_with(
        &mut self,
        now: Instant,
        now_unix_ms: i64,
        targets: &[UsageTarget],
        read: impl Fn(&UsageTarget) -> Option<CgroupSample>,
        node_memory: Option<NodeMemoryUsage>,
        inputs: &ActivityInputs,
    ) -> UsageReport {
        self.last_report = Some(now);
        let now_unix = now_unix_ms / 1000;
        let mut allocations = Vec::with_capacity(targets.len());
        let mut seen = HashSet::with_capacity(targets.len());

        for target in targets {
            let Some(sample) = read(target) else {
                continue;
            };
            seen.insert(target.allocation_id);
            let previous = self.allocations.get(&target.allocation_id);

            // usec of CPU per ms of wall time is exactly thousandths of a
            // core. A counter that went backwards means the cgroup was
            // recreated (an in-place restart), which restarts the rate too.
            let cpu_millicores = previous
                .and_then(|previous| {
                    let elapsed_ms = now_unix_ms.checked_sub(previous.sampled_at_unix_ms)?;
                    let used = sample.cpu_usage_usec.checked_sub(previous.cpu_usage_usec)?;
                    (elapsed_ms > 0).then(|| used / elapsed_ms as u64)
                })
                .unwrap_or(0);
            let datapath = inputs
                .datapath
                .get(&target.allocation_id)
                .copied()
                .unwrap_or_default();
            // Bytes through a datapath connection (ingress, or a gated
            // allocation's egress proxy) since the previous sample, stamped
            // as they move (in whole seconds, so a stamp in the same second
            // counts).
            let datapath_bytes = datapath.last_bytes_unix > 0
                && previous.is_none_or(|previous| {
                    datapath.last_bytes_unix >= previous.sampled_at_unix_ms / 1000
                });
            // Bytes the guest sent on its own NIC (a TAP, an Apple
            // container) since the previous sample. The first sample has
            // nothing to compare with, and a counter that went backwards
            // (the device was recreated) restarts the count.
            let guest_sent = matches!(
                (previous.and_then(|previous| previous.guest_sent_bytes), sample.guest_sent_bytes),
                (Some(before), Some(now)) if now > before
            );
            let interval = crate::worker::idle::IntervalActivity {
                cpu_millicores,
                network_bytes: self.network_activity.contains(&target.allocation_id)
                    || datapath_bytes
                    || guest_sent,
                open_connections: datapath.open_connections,
                exec_sessions: inputs
                    .exec_sessions
                    .get(&target.allocation_id)
                    .copied()
                    .unwrap_or(0),
            };
            let active = cpu_millicores >= ACTIVE_CPU_FLOOR_MILLICORES
                || interval.network_bytes
                || interval.exec_sessions > 0;
            let (idle_since_unix, idle_after_seconds) = match target.idle.as_ref() {
                Some(policy) => (
                    self.idle
                        .observe(target.allocation_id, now_unix, policy, &interval),
                    policy.after_seconds,
                ),
                None => (0, 0),
            };
            let last_activity_unix = match previous {
                Some(previous) if !active => previous.last_activity_unix,
                _ => now_unix,
            };
            let peak_bytes = sample
                .memory_peak_bytes
                .unwrap_or(0)
                .max(sample.memory_current_bytes)
                .max(previous.map(|previous| previous.peak_bytes).unwrap_or(0));

            self.allocations.insert(
                target.allocation_id,
                AllocationState {
                    cpu_usage_usec: sample.cpu_usage_usec,
                    sampled_at_unix_ms: now_unix_ms,
                    peak_bytes,
                    last_activity_unix,
                    guest_sent_bytes: sample.guest_sent_bytes,
                },
            );
            allocations.push(AllocationUsage {
                allocation_id: target.allocation_id,
                memory_current_bytes: sample.memory_current_bytes,
                memory_peak_bytes: peak_bytes,
                memory_anon_bytes: sample.memory_anon_bytes,
                cpu_usage_usec: sample.cpu_usage_usec,
                cpu_millicores,
                psi_memory_some_avg10: sample.psi_memory_some_avg10,
                last_activity_unix,
                // Filled by `pressure::annotate` once the whole report exists:
                // a rank is a position among every allocation on the node.
                pressure_rank: 0,
                pressure_steps: Vec::new(),
                idle_since_unix,
                idle_after_seconds,
            });
        }

        // Forget allocations that are gone, so the state stays the live set.
        self.allocations.retain(|id, _| seen.contains(id));
        self.idle.retain(&seen);
        self.network_activity.clear();
        allocations.sort_by_key(|usage| usage.allocation_id);

        UsageReport {
            sampled_at_unix_ms: now_unix_ms,
            allocations,
            node_memory,
            pressure: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMORY_STAT: &str = "anon 104857600\nfile 52428800\nkernel 1048576\n\
                               anon_thp 0\nfile_mapped 4096\n";
    const CPU_STAT: &str = "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n\
                            nr_periods 0\nnr_throttled 0\nthrottled_usec 0\n";
    const PRESSURE: &str = "some avg10=1.53 avg60=0.40 avg300=0.08 total=123456\n\
                            full avg10=0.20 avg60=0.00 avg300=0.00 total=789\n";
    const MEMINFO: &str = "MemTotal:       16384000 kB\nMemFree:         1024000 kB\n\
                           MemAvailable:    8192000 kB\nBuffers:          102400 kB\n";

    #[test]
    fn flat_keyed_files_parse_by_exact_key() {
        assert_eq!(parse_flat_keyed(MEMORY_STAT, "anon"), Some(104_857_600));
        // `anon_thp` must not match `anon`.
        assert_eq!(parse_flat_keyed("anon_thp 5\nanon 7\n", "anon"), Some(7));
        assert_eq!(parse_flat_keyed(CPU_STAT, "usage_usec"), Some(2_500_000));
        assert_eq!(parse_flat_keyed(CPU_STAT, "missing"), None);
        assert_eq!(parse_flat_keyed("", "anon"), None);
    }

    #[test]
    fn psi_reads_the_some_line_only() {
        assert_eq!(parse_psi_some_avg10(PRESSURE), Some(1.53));
        assert_eq!(
            parse_psi_some_avg10("full avg10=9.00 avg60=0 avg300=0 total=0\n"),
            None
        );
        assert_eq!(parse_psi_some_avg10(""), None);
    }

    #[test]
    fn meminfo_converts_kib_to_bytes() {
        assert_eq!(
            parse_meminfo(MEMINFO),
            Some((16_384_000 * 1024, 8_192_000 * 1024))
        );
        // An old kernel without MemAvailable reports nothing rather than a
        // wrong number.
        assert_eq!(parse_meminfo("MemTotal: 100 kB\nMemFree: 50 kB\n"), None);
    }

    fn fixture_cgroup(dir: &Path, current: u64, peak: Option<u64>, usage_usec: u64) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
        if let Some(peak) = peak {
            std::fs::write(dir.join("memory.peak"), format!("{peak}\n")).unwrap();
        }
        std::fs::write(dir.join("memory.stat"), MEMORY_STAT).unwrap();
        std::fs::write(
            dir.join("cpu.stat"),
            format!("usage_usec {usage_usec}\nuser_usec 0\n"),
        )
        .unwrap();
        std::fs::write(dir.join("memory.pressure"), PRESSURE).unwrap();
    }

    #[test]
    fn a_cgroup_directory_reads_into_a_sample() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup = dir.path().join("legion-7");
        fixture_cgroup(&cgroup, 314_572_800, Some(400_000_000), 2_500_000);

        let sample = read_cgroup_sample(&cgroup).expect("readable");
        assert_eq!(sample.memory_current_bytes, 314_572_800);
        assert_eq!(sample.memory_peak_bytes, Some(400_000_000));
        assert_eq!(sample.memory_anon_bytes, 104_857_600);
        assert_eq!(sample.cpu_usage_usec, 2_500_000);
        assert_eq!(sample.psi_memory_some_avg10, 1.53);

        // A cgroup that is already gone reads as nothing, not zeros.
        assert_eq!(read_cgroup_sample(&dir.path().join("legion-8")), None);
    }

    #[test]
    fn node_memory_reads_from_a_proc_tree() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("meminfo"), MEMINFO).unwrap();
        let without_psi = read_node_memory_from(dir.path()).expect("meminfo present");
        assert_eq!(without_psi.available_bytes, 8_192_000 * 1024);
        assert!(!without_psi.psi_available);

        std::fs::create_dir_all(dir.path().join("pressure")).unwrap();
        std::fs::write(dir.path().join("pressure").join("memory"), PRESSURE).unwrap();
        let with_psi = read_node_memory_from(dir.path()).expect("meminfo present");
        assert!(with_psi.psi_available);
        assert_eq!(with_psi.psi_some_avg10, 1.53);
    }

    fn sample_of(current: u64, usage_usec: u64) -> CgroupSample {
        CgroupSample {
            memory_current_bytes: current,
            memory_peak_bytes: None,
            memory_anon_bytes: current / 2,
            cpu_usage_usec: usage_usec,
            psi_memory_some_avg10: 0.0,
            guest_sent_bytes: None,
        }
    }

    fn target(id: i64) -> UsageTarget {
        UsageTarget {
            allocation_id: id,
            source: UsageSource::Cgroup(PathBuf::from(format!("/fake/legion-{id}"))),
            tap_device: None,
            pressure: Default::default(),
            idle: None,
        }
    }

    /// With an idle policy, the report carries idle-since from the signals
    /// that policy counts: an open exec session keeps it awake, a quiet open
    /// datapath connection does not (without `connections`), and bytes
    /// through one do.
    #[test]
    fn idle_since_follows_the_policys_signals() {
        use crate::structs::suspend::IdleSignal;
        use crate::worker::netd::activity::DatapathActivity;
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        let mut idle_target = target(1);
        idle_target.idle = Some(crate::worker::idle::IdlePolicy {
            after_seconds: 60,
            signals: IdleSignal::DEFAULTS.to_vec(),
            cpu_active_millicores: 20,
        });
        let targets = [idle_target, target(2)];
        let quiet = |_: &UsageTarget| Some(sample_of(10, 0));
        let at = |n: u32| start + USAGE_REPORT_INTERVAL * n;

        let first = sampler.sample(start, 100_000, &targets, quiet, None);
        assert_eq!(first.allocations[0].idle_since_unix, 100);
        assert_eq!(first.allocations[0].idle_after_seconds, 60);
        assert_eq!(
            first.allocations[1].idle_since_unix, 0,
            "no policy, no idle"
        );

        // A quiet open connection: still idle since 100.
        let mut inputs = ActivityInputs::default();
        inputs.datapath.insert(
            1,
            DatapathActivity {
                open_connections: 1,
                last_bytes_unix: 95,
            },
        );
        let held = sampler.sample_with(at(1), 110_000, &targets, quiet, None, &inputs);
        assert_eq!(held.allocations[0].idle_since_unix, 100);

        // Bytes through it: awake at 120.
        inputs.datapath.get_mut(&1).unwrap().last_bytes_unix = 118;
        let talked = sampler.sample_with(at(2), 120_000, &targets, quiet, None, &inputs);
        assert_eq!(talked.allocations[0].idle_since_unix, 120);

        // An exec session: awake at 130.
        inputs.datapath.clear();
        inputs.exec_sessions.insert(1, 1);
        let shell = sampler.sample_with(at(3), 130_000, &targets, quiet, None, &inputs);
        assert_eq!(shell.allocations[0].idle_since_unix, 130);
        assert_eq!(shell.allocations[0].last_activity_unix, 130);

        // Session closed: quiet since 130.
        inputs.exec_sessions.clear();
        let after = sampler.sample_with(at(4), 140_000, &targets, quiet, None, &inputs);
        assert_eq!(after.allocations[0].idle_since_unix, 130);
    }

    /// A fake TAP: fixed counters per device.
    struct FakeNics(HashMap<String, u64>);

    impl NicCounters for FakeNics {
        fn guest_sent_bytes(&self, tap: &str) -> Option<u64> {
            self.0.get(tap).copied()
        }
    }

    #[test]
    fn tap_counters_read_the_hosts_receive_side() {
        let dir = tempfile::tempdir().unwrap();
        let stats = dir.path().join("lgf0123456789ab").join("statistics");
        std::fs::create_dir_all(&stats).unwrap();
        std::fs::write(stats.join("rx_bytes"), "4096\n").unwrap();
        std::fs::write(stats.join("tx_bytes"), "999999\n").unwrap();
        assert_eq!(
            read_tap_guest_sent_bytes(dir.path(), "lgf0123456789ab"),
            Some(4096)
        );
        assert_eq!(read_tap_guest_sent_bytes(dir.path(), "missing"), None);
        assert_eq!(read_tap_guest_sent_bytes(dir.path(), "../etc"), None);
    }

    /// `container stats --format json --no-stream`, as Apple `container`
    /// 1.1.0 prints it.
    #[test]
    fn apple_container_stats_parse_into_samples() {
        let json = r#"[{"blockReadBytes":1744896,"blockWriteBytes":0,"cpuUsageUsec":129924,
            "id":"legion-sbx-7","memoryLimitBytes":1073741824,"memoryUsageBytes":2408448,
            "networkRxBytes":14464,"networkTxBytes":602,"numProcesses":1}]"#;
        let stats = parse_apple_container_stats(json);
        let sample = stats["legion-sbx-7"].sample();
        assert_eq!(sample.cpu_usage_usec, 129_924);
        assert_eq!(sample.memory_current_bytes, 2_408_448);
        assert_eq!(sample.guest_sent_bytes, Some(602));
        assert!(parse_apple_container_stats("not json").is_empty());
        assert!(parse_apple_container_stats("[]").is_empty());
    }

    #[test]
    fn a_target_reads_from_its_source_and_its_tap() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup = dir.path().join("legion-7");
        fixture_cgroup(&cgroup, 100, None, 0);
        let nics = FakeNics([("lgf7".to_string(), 512)].into());
        let mut microvm = target(7);
        microvm.source = UsageSource::Cgroup(cgroup);
        microvm.tap_device = Some("lgf7".into());
        let sample = read_target_sample(&microvm, &HashMap::new(), &nics).expect("readable");
        assert_eq!(sample.guest_sent_bytes, Some(512));

        let mut apple = target(8);
        apple.source = UsageSource::AppleContainer("legion-sbx-8".into());
        let stats = parse_apple_container_stats(
            r#"[{"id":"legion-sbx-8","cpuUsageUsec":5,"memoryUsageBytes":10,"networkTxBytes":3}]"#,
        );
        let sample = read_target_sample(&apple, &stats, &nics).expect("listed");
        assert_eq!(sample.memory_current_bytes, 10);
        assert_eq!(sample.guest_sent_bytes, Some(3));
        // Not running (not listed): left out, like a vanished cgroup.
        assert_eq!(read_target_sample(&apple, &HashMap::new(), &nics), None);
        assert_eq!(apple.cgroup_dir(), None);
    }

    /// A microVM's guest traffic never crosses the datapath. Bytes it sends
    /// on its TAP count as network activity for its idle policy; a counter
    /// that holds still is quiet, and the first sample has no baseline.
    #[test]
    fn bytes_the_guest_sends_on_its_nic_keep_it_awake() {
        use crate::structs::suspend::IdleSignal;
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        let mut sandbox = target(1);
        sandbox.idle = Some(crate::worker::idle::IdlePolicy {
            after_seconds: 60,
            signals: vec![IdleSignal::Network],
            cpu_active_millicores: 20,
        });
        let targets = [sandbox];
        let sent = |bytes: u64| {
            move |_: &UsageTarget| {
                Some(CgroupSample {
                    guest_sent_bytes: Some(bytes),
                    ..sample_of(10, 0)
                })
            }
        };
        let at = |n: u32| start + USAGE_REPORT_INTERVAL * n;
        let first = sampler.sample(at(0), 100_000, &targets, sent(1000), None);
        assert_eq!(first.allocations[0].idle_since_unix, 100);
        let held = sampler.sample(at(1), 110_000, &targets, sent(1000), None);
        assert_eq!(held.allocations[0].idle_since_unix, 100, "no new bytes");
        let talked = sampler.sample(at(2), 120_000, &targets, sent(1400), None);
        assert_eq!(talked.allocations[0].idle_since_unix, 120);
        assert_eq!(talked.allocations[0].last_activity_unix, 120);
        // Recreated device: the counter restarts, which is not activity.
        let reset = sampler.sample(at(3), 130_000, &targets, sent(10), None);
        assert_eq!(reset.allocations[0].idle_since_unix, 120);
    }

    #[test]
    fn reports_are_due_once_per_interval() {
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        assert!(sampler.due(start), "the first report goes out immediately");
        sampler.sample(start, 0, &[], |_| None, None);
        assert!(!sampler.due(start + Duration::from_secs(9)));
        assert!(sampler.due(start + USAGE_REPORT_INTERVAL));
    }

    /// CPU rate comes from consecutive samples; the first has none. Idle
    /// intervals keep the old activity time, busy ones move it forward.
    #[test]
    fn cpu_rate_and_activity_come_from_consecutive_samples() {
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        let targets = [target(1)];

        let first = sampler.sample(start, 100_000, &targets, |_| Some(sample_of(10, 0)), None);
        assert_eq!(first.allocations[0].cpu_millicores, 0);
        assert_eq!(first.allocations[0].last_activity_unix, 100);

        // 10s later, 5s of CPU used: half a core.
        let busy = sampler.sample(
            start + USAGE_REPORT_INTERVAL,
            110_000,
            &targets,
            |_| Some(sample_of(10, 5_000_000)),
            None,
        );
        assert_eq!(busy.allocations[0].cpu_millicores, 500);
        assert_eq!(busy.allocations[0].last_activity_unix, 110);

        // 10s later, 1ms of CPU: below the floor, so still idle since 110.
        let idle = sampler.sample(
            start + USAGE_REPORT_INTERVAL * 2,
            120_000,
            &targets,
            |_| Some(sample_of(10, 5_001_000)),
            None,
        );
        assert_eq!(idle.allocations[0].cpu_millicores, 0);
        assert_eq!(idle.allocations[0].last_activity_unix, 110);

        // Datapath traffic counts as activity even with no CPU.
        sampler.note_network_activity([1]);
        let talked = sampler.sample(
            start + USAGE_REPORT_INTERVAL * 3,
            130_000,
            &targets,
            |_| Some(sample_of(10, 5_001_000)),
            None,
        );
        assert_eq!(talked.allocations[0].last_activity_unix, 130);
    }

    /// Without `memory.peak` the sampler keeps the highest value it saw, and
    /// with it the kernel's number wins when larger.
    #[test]
    fn peak_is_tracked_when_the_kernel_has_no_memory_peak() {
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        let targets = [target(1)];
        sampler.sample(start, 0, &targets, |_| Some(sample_of(500, 0)), None);
        let after = sampler.sample(
            start + USAGE_REPORT_INTERVAL,
            10_000,
            &targets,
            |_| Some(sample_of(200, 0)),
            None,
        );
        assert_eq!(after.allocations[0].memory_current_bytes, 200);
        assert_eq!(after.allocations[0].memory_peak_bytes, 500);

        let kernel_peak = sampler.sample(
            start + USAGE_REPORT_INTERVAL * 2,
            20_000,
            &targets,
            |_| {
                Some(CgroupSample {
                    memory_peak_bytes: Some(900),
                    ..sample_of(200, 0)
                })
            },
            None,
        );
        assert_eq!(kernel_peak.allocations[0].memory_peak_bytes, 900);
    }

    /// Unreadable cgroups are left out, and allocations that vanish are
    /// forgotten so a reused id starts fresh.
    #[test]
    fn gone_allocations_are_dropped_from_reports_and_state() {
        let mut sampler = UsageSampler::default();
        let start = Instant::now();
        let report = sampler.sample(
            start,
            0,
            &[target(1), target(2)],
            |target| (!target.cgroup_dir().unwrap().ends_with("legion-2")).then(|| sample_of(1, 0)),
            None,
        );
        assert_eq!(
            report
                .allocations
                .iter()
                .map(|usage| usage.allocation_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        sampler.sample(start, 10_000, &[], |_| None, None);
        assert!(sampler.allocations.is_empty());
    }

    /// The real `/proc` on a Linux machine parses: total is nonzero and
    /// available never exceeds it. Runs in Linux CI.
    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_machines_proc_meminfo() {
        let memory = read_node_memory().expect("/proc/meminfo is readable on Linux");
        assert!(memory.total_bytes > 0);
        assert!(memory.available_bytes <= memory.total_bytes);
    }

    /// The test process's own cgroup reads as a sample. Needs the cgroup v2
    /// unified hierarchy with the memory controller enabled for that cgroup.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs cgroup v2 with the memory controller on the test's own cgroup"]
    fn reads_this_processs_own_cgroup() {
        let relative = std::fs::read_to_string("/proc/self/cgroup").expect("cgroup membership");
        let path = relative
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .expect("a cgroup v2 entry");
        let dir = Path::new(crate::task_plugins::CGROUP_ROOT).join(path.trim_start_matches('/'));
        let sample = read_cgroup_sample(&dir).expect("memory.current is readable");
        assert!(sample.memory_current_bytes > 0, "{sample:?}");
        assert!(sample.cpu_usage_usec > 0, "{sample:?}");
    }
}
