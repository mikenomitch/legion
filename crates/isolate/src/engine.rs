//! The one place a `wasmtime::Config` is built, for both roles.
//!
//! A compiled artifact loads only into an engine whose code-generation
//! settings match the ones that compiled it (target and ISA flags, Cranelift
//! settings, Wasm features, memory reservation and guard sizes, epoch
//! interruption, concurrency support). So the compiler process and the host
//! process build their engines from the same [`EngineParams`] through
//! [`base_config`], and [`compat_key`] names that configuration: the worker
//! keys compiled modules by it, and a host refuses an artifact compiled under
//! another key before wasmtime ever sees it.

use std::hash::{Hash, Hasher};

use legion_proto::isolate_host::{HostConfig, SlotLayout};
use sha2::{Digest, Sha256};
use wasmtime::{Config, Enabled, Engine, InstanceAllocationStrategy, PoolingAllocationConfig};

/// `wasmtime-<version>`: what `HelloReply.engine_version` and checkpoints
/// carry.
pub const ENGINE_VERSION: &str = "wasmtime-50.0.1";

/// The target this binary was built for (and compiles for: the compiler
/// never cross-compiles).
pub const TARGET: &str = env!("LEGION_ISOLATE_TARGET");

/// Wasm pages are 64 KiB (custom page sizes are off).
pub const WASM_PAGE: u64 = 64 << 10;

/// Core instances one component may hold. The componentize-py image needs
/// more than 8 (the spike measured 24 working); the pool's per-instance
/// metadata is virtual until touched, so a generous bound costs little.
const CORE_INSTANCES_PER_COMPONENT: u32 = 32;

/// The settings that decide whether an artifact is loadable. Everything
/// else about an engine (pool sizes, stack sizes) is the host's business
/// and never changes generated code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineParams {
    pub layout: SlotLayout,
    /// The largest linear memory one isolate may have. Generated code
    /// depends on it only with `SlotLayout::Small` (it is the reservation
    /// bounds checks are emitted against).
    pub slot_bytes: u64,
    /// Spectre mitigations on heap and table accesses in generated code.
    pub spectre: bool,
}

impl Default for EngineParams {
    fn default() -> Self {
        Self {
            layout: SlotLayout::GuardPages,
            slot_bytes: 128 << 20,
            spectre: true,
        }
    }
}

impl EngineParams {
    /// The parameters a host process runs with.
    pub fn from_host_config(c: &HostConfig) -> Self {
        Self {
            layout: SlotLayout::try_from(c.layout).unwrap_or(SlotLayout::GuardPages),
            slot_bytes: if c.slot_bytes == 0 {
                EngineParams::default().slot_bytes
            } else {
                c.slot_bytes
            },
            spectre: c.require_spectre_mitigations,
        }
    }

    /// The slot size rounded up to whole Wasm pages.
    pub fn slot_bytes_rounded(&self) -> u64 {
        self.slot_bytes.div_ceil(WASM_PAGE).max(1) * WASM_PAGE
    }

    /// The name used on the command line and in JSON.
    pub fn layout_name(&self) -> &'static str {
        layout_name(self.layout)
    }
}

pub fn layout_name(layout: SlotLayout) -> &'static str {
    match layout {
        SlotLayout::GuardPages => "guard",
        SlotLayout::Mpk => "mpk",
        SlotLayout::Small => "small",
    }
}

pub fn parse_layout(s: &str) -> Result<SlotLayout, String> {
    match s {
        "guard" | "guard-pages" => Ok(SlotLayout::GuardPages),
        "mpk" => Ok(SlotLayout::Mpk),
        "small" => Ok(SlotLayout::Small),
        other => Err(format!(
            "unknown slot layout {other:?}; use \"guard\", \"mpk\", or \"small\""
        )),
    }
}

/// The configuration both roles must agree on. The host adds its pooling
/// allocator and stack sizes on top (see [`host_engine`]); none of those
/// change generated code.
pub fn base_config(p: &EngineParams) -> anyhow::Result<Config> {
    let mut c = Config::new();
    // Cranelift is the only compiler (no Winch, no Pulley: plan §6.5).
    c.strategy(wasmtime::Strategy::Cranelift);
    c.wasm_component_model(true);
    // Preemption and the wall/CPU limits are epoch deadlines.
    c.epoch_interruption(true);
    // Fibers for every guest; no Component Model async (brief item 14).
    // Also part of the artifact compatibility check.
    c.concurrency_support(false);
    // No threads and no shared memory (plan §6.10 layer 4): the engine is
    // built without the `threads` feature, and the compiler's validator
    // refuses both before Cranelift sees a module.
    c.memory_init_cow(true);
    // Isolates never touch the host's native-code debug interfaces.
    c.debug_info(false);
    c.native_unwind_info(true);
    match p.layout {
        SlotLayout::GuardPages | SlotLayout::Mpk => {}
        SlotLayout::Small => {
            // The slot is the whole reservation: Cranelift emits explicit
            // bounds checks (and Spectre guards on them), no guard region.
            c.memory_reservation(p.slot_bytes_rounded());
            c.memory_guard_size(0);
            c.memory_reservation_for_growth(0);
            c.guard_before_linear_memory(false);
        }
    }
    if !p.spectre {
        // SAFETY: these are documented Cranelift settings; turning them off
        // only removes speculative-execution hardening, which the operator
        // asked for with `require_spectre_mitigations = false`.
        unsafe {
            c.cranelift_flag_set("enable_heap_access_spectre_mitigation", "false");
            c.cranelift_flag_set("enable_table_access_spectre_mitigation", "false");
        }
    }
    Ok(c)
}

/// An engine with no allocator tuning: what the compiler uses, and what
/// `engine-info` hashes.
pub fn plain_engine(p: &EngineParams) -> anyhow::Result<Engine> {
    let c = base_config(p)?;
    Engine::new(&c).map_err(|e| anyhow::anyhow!("{e:#}"))
}

/// Sizes for the host's pooling allocator.
#[derive(Debug, Clone, Copy)]
pub struct PoolSize {
    /// Linear-memory slots (`HostConfig.slots`); also the number of live
    /// component instances and of fiber stacks.
    pub slots: u32,
    /// Fiber stack bytes (`HostConfig.async_stack_bytes`).
    pub async_stack_bytes: u64,
}

/// Whether this machine can do MPK striping.
pub fn mpk_available() -> bool {
    PoolingAllocationConfig::are_memory_protection_keys_available()
}

/// The host's engine: the shared configuration plus a pooling allocator
/// sized from the slot budget and small fiber stacks.
pub fn host_engine(p: &EngineParams, size: PoolSize) -> anyhow::Result<Engine> {
    let mut c = base_config(p)?;
    let slots = size.slots.max(1);
    let stack = size.async_stack_bytes.max(64 << 10) as usize;
    // A fiber stack must be larger than the deepest Wasm stack allowed on
    // it, with room for host frames below.
    c.async_stack_size(stack);
    c.max_wasm_stack(stack * 3 / 4);
    let mut pc = PoolingAllocationConfig::new();
    pc.total_component_instances(slots)
        .total_memories(slots)
        .total_stacks(slots)
        .total_core_instances(slots.saturating_mul(CORE_INSTANCES_PER_COMPONENT))
        .total_tables(slots.saturating_mul(4))
        .max_core_instances_per_component(CORE_INSTANCES_PER_COMPONENT)
        .max_memories_per_component(4)
        .max_tables_per_component(CORE_INSTANCES_PER_COMPONENT)
        .max_memory_size(p.slot_bytes_rounded() as usize)
        .table_elements(200_000)
        .max_core_instance_size(4 << 20)
        .max_component_instance_size(1 << 20)
        // Slot affinity: keep recently used slots warm for their module
        // rather than decommitting them (plan §6.4).
        .max_unused_warm_slots(slots.max(100))
        .linear_memory_keep_resident(64 << 10)
        .table_keep_resident(16 << 10);
    if p.layout == SlotLayout::Mpk {
        pc.memory_protection_keys(if mpk_available() {
            Enabled::Yes
        } else {
            Enabled::No
        });
    }
    c.allocation_strategy(InstanceAllocationStrategy::Pooling(pc));
    Engine::new(&c).map_err(|e| anyhow::anyhow!("building the isolate engine: {e:#}"))
}

/// Feeds `Hash` output into SHA-256, so the key is stable across processes
/// and Rust versions (`DefaultHasher` promises neither).
struct ShaHasher(Sha256);

impl Hasher for ShaHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

/// The artifact compatibility key for these parameters: wasmtime's own
/// compatibility hash (target, ISA flags, Cranelift settings, features,
/// tunables) plus the settings this crate chooses, so a change to any of
/// them gives a different key.
pub fn compat_key(p: &EngineParams) -> anyhow::Result<String> {
    let engine = plain_engine(p)?;
    Ok(compat_key_for(&engine, p))
}

/// [`compat_key`] for an engine already built from `p` (pool settings do
/// not change it).
pub fn compat_key_for(engine: &Engine, p: &EngineParams) -> String {
    let mut h = ShaHasher(Sha256::new());
    engine.precompile_compatibility_hash().hash(&mut h);
    let mut sha = h.0;
    sha.update(ENGINE_VERSION.as_bytes());
    sha.update(TARGET.as_bytes());
    sha.update(p.layout_name().as_bytes());
    if p.layout == SlotLayout::Small {
        sha.update(p.slot_bytes_rounded().to_le_bytes());
    }
    sha.update([
        p.spectre as u8,
        1, /* epochs */
        0, /* concurrency */
    ]);
    let digest = sha.finalize();
    digest[..16].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod engine_test {
    use super::*;

    #[test]
    fn compat_key_is_stable_and_changes_with_each_setting_test() {
        let base = EngineParams::default();
        let k = compat_key(&base).unwrap();
        assert_eq!(k, compat_key(&base).unwrap());
        assert_eq!(k.len(), 32);
        let small = EngineParams {
            layout: SlotLayout::Small,
            ..base
        };
        let ks = compat_key(&small).unwrap();
        assert_ne!(k, ks, "the layout is part of the key");
        let small_big = EngineParams {
            slot_bytes: 256 << 20,
            ..small
        };
        assert_ne!(
            ks,
            compat_key(&small_big).unwrap(),
            "a small slot's size is"
        );
        let mpk = EngineParams {
            layout: SlotLayout::Mpk,
            ..base
        };
        assert_ne!(k, compat_key(&mpk).unwrap());
        let nospectre = EngineParams {
            spectre: false,
            ..base
        };
        assert_ne!(k, compat_key(&nospectre).unwrap(), "spectre settings are");
        // A guard-page slot's size does not change generated code.
        let guard_big = EngineParams {
            slot_bytes: 1 << 30,
            ..base
        };
        assert_eq!(k, compat_key(&guard_big).unwrap());
    }
}
