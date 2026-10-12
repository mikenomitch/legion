//! Fingerprint attributes a memory checkpoint's `compat` block is matched
//! against: the sandbox VMM and its version, and the CPU vendor, model, and
//! feature set. See `crate::structs::checkpoint::compat_matches`.

use sha2::{Digest, Sha256};

use crate::prelude::*;
use crate::structs::checkpoint::{ATTR_CPU_FEATURES, ATTR_CPU_MODEL, ATTR_CPU_VENDOR};
use crate::worker::node_check::NodeChecker;

pub struct CheckpointCompatNodeChecker {}

impl NodeChecker for CheckpointCompatNodeChecker {
    fn name(&self) -> &'static str {
        "checkpoint-compat"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = local_cpu_attrs();
        // `sandbox.vmm`, `sandbox.vmm.version`, and one
        // `sandbox.vmm.<name>.version` per installed VMM. The sandbox runtime
        // owns VMM discovery, so it computes these.
        map.extend(crate::task_plugins::sandbox_vmm_attrs());
        map
    }
}

/// This node's `cpu.*` attributes alone, for the `compat` block a memory
/// checkpoint records (`SnapshotCompat::with_cpu_from_attrs`).
pub(crate) fn local_cpu_attrs() -> Map<String, Value> {
    let mut map = Map::new();
    let cpu = local_cpu_identity();
    if let Some(vendor) = cpu.vendor {
        map.insert(ATTR_CPU_VENDOR.into(), json!(vendor));
    }
    if let Some(model) = cpu.model {
        map.insert(ATTR_CPU_MODEL.into(), json!(model));
    }
    if let Some(features) = cpu.features {
        map.insert(ATTR_CPU_FEATURES.into(), json!(features));
    }
    map
}

/// What this node's CPU looks like to a guest.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CpuIdentity {
    pub vendor: Option<String>,
    pub model: Option<String>,
    /// Short digest of the sorted feature flags.
    pub features: Option<String>,
}

fn local_cpu_identity() -> CpuIdentity {
    #[cfg(target_os = "linux")]
    if let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") {
        return parse_cpuinfo(&text);
    }
    // Elsewhere (macOS dev workers) sysinfo's vendor and brand are the best
    // available, and there is no feature list. Nothing restores memory
    // checkpoints there anyway: no Linux VMM runs on it.
    let mut system = sysinfo::System::new();
    system.refresh_cpu_all();
    let cpu = system.cpus().first();
    CpuIdentity {
        vendor: cpu
            .map(|cpu| cpu.vendor_id().trim().to_string())
            .filter(|value| !value.is_empty()),
        model: cpu
            .map(|cpu| cpu.brand().trim().to_string())
            .filter(|value| !value.is_empty()),
        features: None,
    }
}

/// Parse the first processor block of `/proc/cpuinfo`.
///
/// x86 reports `vendor_id`, `cpu family`, `model`, `stepping`, and `flags`,
/// so the model is `family:model:stepping` (the old plan's `6:106:6`). ARM
/// reports `CPU implementer`, `CPU variant`, `CPU part`, `CPU revision`, and
/// `Features`, so the vendor is the implementer code and the model is
/// `implementer:part:variant:revision`.
// Only the Linux fingerprint path reads /proc/cpuinfo; tests run it everywhere.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn parse_cpuinfo(text: &str) -> CpuIdentity {
    let mut fields: HashMap<String, String> = HashMap::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !fields.is_empty() {
                break; // first processor only
            }
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            fields
                .entry(key.trim().to_string())
                .or_insert_with(|| value.trim().to_string());
        }
    }
    let get = |key: &str| fields.get(key).filter(|value| !value.is_empty()).cloned();

    if let Some(vendor) = get("vendor_id") {
        let model = match (get("cpu family"), get("model"), get("stepping")) {
            (Some(family), Some(model), Some(stepping)) => {
                Some(format!("{family}:{model}:{stepping}"))
            }
            _ => None,
        };
        return CpuIdentity {
            vendor: Some(vendor),
            model,
            features: get("flags").map(|flags| features_digest(&flags)),
        };
    }
    if let Some(implementer) = get("CPU implementer") {
        let model = match (get("CPU part"), get("CPU variant"), get("CPU revision")) {
            (Some(part), Some(variant), Some(revision)) => {
                Some(format!("{implementer}:{part}:{variant}:{revision}"))
            }
            _ => None,
        };
        return CpuIdentity {
            vendor: Some(implementer),
            model,
            features: get("Features").map(|flags| features_digest(&flags)),
        };
    }
    CpuIdentity::default()
}

/// First 16 hex chars of SHA-256 over the sorted, de-duplicated flag list.
/// Order-insensitive, so two kernels listing the same flags differently
/// agree.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn features_digest(flags: &str) -> String {
    let mut list: Vec<&str> = flags.split_whitespace().collect();
    list.sort_unstable();
    list.dedup();
    let digest = Sha256::digest(list.join(" ").as_bytes());
    format!("{digest:x}")[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const X86: &str = "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 106\nmodel name\t: Intel(R) Xeon(R)\nstepping\t: 6\nflags\t\t: fpu vme avx2 avx512f\n\nprocessor\t: 1\nvendor_id\t: AuthenticAMD\n";
    const ARM: &str = "processor\t: 0\nBogoMIPS\t: 243.75\nFeatures\t: fp asimd evtstrm aes\nCPU implementer\t: 0x41\nCPU architecture: 8\nCPU variant\t: 0x1\nCPU part\t: 0xd0c\nCPU revision\t: 1\n";

    #[test]
    fn x86_cpuinfo_yields_vendor_family_model_stepping() {
        let cpu = parse_cpuinfo(X86);
        assert_eq!(cpu.vendor.as_deref(), Some("GenuineIntel"));
        assert_eq!(cpu.model.as_deref(), Some("6:106:6"));
        assert_eq!(cpu.features.as_deref().map(str::len), Some(16));
    }

    #[test]
    fn arm_cpuinfo_yields_implementer_and_part() {
        let cpu = parse_cpuinfo(ARM);
        assert_eq!(cpu.vendor.as_deref(), Some("0x41"));
        assert_eq!(cpu.model.as_deref(), Some("0x41:0xd0c:0x1:1"));
        assert!(cpu.features.is_some());
    }

    #[test]
    fn feature_digest_ignores_order_and_duplicates_but_not_content() {
        assert_eq!(
            features_digest("avx2 fpu sse"),
            features_digest("sse fpu avx2 fpu")
        );
        assert_ne!(
            features_digest("avx2 fpu"),
            features_digest("avx2 fpu avx512f")
        );
    }

    #[test]
    fn unknown_cpuinfo_reports_nothing_rather_than_guessing() {
        assert_eq!(parse_cpuinfo("garbage"), CpuIdentity::default());
    }

    #[test]
    fn the_checker_reports_cpu_attributes_on_this_machine() {
        let attrs = CheckpointCompatNodeChecker {}.fingerprint();
        // Every dev and CI machine has a CPU vendor; VMM attributes depend
        // on what is installed.
        assert!(attrs.contains_key(ATTR_CPU_VENDOR), "{attrs:?}");
    }
}
