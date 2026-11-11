//! A rollout strategy that panics on every `resolve`: the test fixture for
//! panic containment (docs/plugin-system.md, "Panics and quarantine").
//! `manager::rollout_strategy`'s tests load it when it has been built
//! (`LEGION_BUILD_PLUGINS=1 cargo build`) and check that the manager gets
//! an error, quarantines the plugin, and keeps running. Never install it.

use legion::manager::rollout_strategy::RolloutStrategyPlugin;
use legion::prelude::R;
use legion::structs::RolloutConfig;

struct Panicking;

impl RolloutStrategyPlugin for Panicking {
    fn name(&self) -> &'static str {
        "panicking"
    }

    fn resolve(&self, group: &str, _desired: u32, _config: &RolloutConfig) -> R<RolloutConfig> {
        panic!("panicking strategy asked to resolve group {group}")
    }
}

legion::export_rollout_strategy!(Panicking);
