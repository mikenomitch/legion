#![crate_type = "cdylib"]

use legion::manager::rollout_strategy::RolloutStrategyPlugin;
use legion::prelude::R;
use legion::structs::RolloutConfig;

#[derive(Default)]
struct SlowStartStrategy;

impl RolloutStrategyPlugin for SlowStartStrategy {
    fn name(&self) -> &'static str {
        "slow_start"
    }

    fn resolve(
        &self,
        _group: &str,
        _desired_count: u32,
        config: &RolloutConfig,
    ) -> R<RolloutConfig> {
        let mut resolved = config.clone();
        let has_step_mode = resolved.percent_each_step.is_some()
            || resolved.count_each_step.is_some()
            || resolved.percent_steps.is_some()
            || resolved.count_steps.is_some();

        if !has_step_mode {
            resolved.percent_steps = Some(vec![5, 20, 50, 100]);
        }

        resolved.strategy = Some(self.name().to_string());
        Ok(resolved)
    }
}

// Exports the ABI fingerprint (the host refuses a dylib built with a
// different rustc or legion version), the panic probe, and the contained
// constructor: every method runs under this plugin's own `catch_unwind`, so
// a panic quarantines the plugin instead of taking the manager down.
legion::export_rollout_strategy!(SlowStartStrategy);
