use libloading::Library;

use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::{bail, instrument, Context, R};
use crate::structs::RolloutConfig;
use crate::util::plugin_root_dir;

pub trait RolloutStrategyPlugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn resolve(&self, group: &str, desired_count: u32, config: &RolloutConfig) -> R<RolloutConfig>;
}

#[derive(Default)]
struct DefaultRolloutStrategy;

impl RolloutStrategyPlugin for DefaultRolloutStrategy {
    fn name(&self) -> &'static str {
        "default"
    }

    fn resolve(
        &self,
        _group: &str,
        _desired_count: u32,
        config: &RolloutConfig,
    ) -> R<RolloutConfig> {
        Ok(config.clone())
    }
}

// A dylib strategy's methods run under its own `catch_unwind` (PLG1;
// docs/plugin-system.md, "Panics and quarantine"). Generic, so this impl is
// compiled into the plugin by `legion::export_rollout_strategy!`.
impl<T: RolloutStrategyPlugin> RolloutStrategyPlugin
    for crate::plugin_runtime::containment::Contained<T>
{
    fn name(&self) -> &'static str {
        crate::plugin_runtime::containment::contained("name", || "panicked", || self.0.name())
    }

    fn resolve(&self, group: &str, desired_count: u32, config: &RolloutConfig) -> R<RolloutConfig> {
        crate::plugin_runtime::containment::contained(
            "resolve",
            || Err(anyhow::anyhow!("the rollout strategy panicked")),
            || self.0.resolve(group, desired_count, config),
        )
    }
}

pub struct LoadedRolloutStrategy {
    pub strategy: Box<dyn RolloutStrategyPlugin>,
    /// The dylib's panic probe; `None` for the built-in strategy.
    probe: Option<crate::plugin_runtime::containment::PanicProbe>,
    _lib: Option<Library>,
}

impl LoadedRolloutStrategy {
    fn new(
        strategy: Box<dyn RolloutStrategyPlugin>,
        probe: Option<crate::plugin_runtime::containment::PanicProbe>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            strategy,
            probe,
            _lib: lib,
        }
    }
}

pub fn validate_rollout_strategy_name(name: &str) -> R<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        bail!("rollout.strategy cannot be empty");
    }

    let _ = load_rollout_strategy(trimmed)
        .with_context(|| format!("rollout.strategy {} is not available", trimmed))?;
    Ok(())
}

pub fn resolve_rollout_strategy(
    config: RolloutConfig,
    group: &str,
    desired_count: u32,
) -> R<RolloutConfig> {
    let strategy_name = config
        .strategy
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let loaded = load_rollout_strategy(&strategy_name)
        .with_context(|| format!("failed to load rollout strategy {}", strategy_name))?;
    // A panicking strategy fails this resolve and is quarantined, instead
    // of taking the manager down.
    let mut resolved = crate::plugin_runtime::containment::guard_r(
        "rollouts",
        &strategy_name,
        "resolve",
        loaded.probe.as_ref(),
        || loaded.strategy.resolve(group, desired_count, &config),
    )
    .with_context(|| {
        format!(
            "rollout strategy {} failed to resolve group {}",
            strategy_name, group
        )
    })?;
    if resolved.strategy.is_none() {
        resolved.strategy = Some(strategy_name);
    }
    Ok(resolved)
}

#[instrument(err, skip_all)]
pub fn load_rollout_strategy(name: &str) -> R<LoadedRolloutStrategy> {
    match name {
        "default" => Ok(LoadedRolloutStrategy::new(
            Box::new(DefaultRolloutStrategy),
            None,
            None,
        )),
        other => {
            // A quarantined strategy is not even loaded again.
            crate::plugin_runtime::containment::check_quarantine("rollouts", other)?;
            let plugin_root = plugin_root_dir();
            let paths = plugin_paths("rollouts", other, &plugin_root);
            validate_manifest("rollouts", other, &paths.root).with_context(|| {
                format!("invalid rollout strategy plugin manifest for {}", other)
            })?;
            unsafe {
                let lib =
                    crate::plugin_runtime::open_plugin_library("rollouts", other, &paths.dylib)?;
                let (strategy, probe) = crate::plugin_runtime::contained_constructor::<
                    dyn RolloutStrategyPlugin,
                >("rollouts", other, &lib)?;
                Ok(LoadedRolloutStrategy::new(strategy, Some(probe), Some(lib)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_strategy_resolves_without_changes() -> R<()> {
        let config = RolloutConfig {
            percent_each_step: Some(20),
            ..Default::default()
        };
        let resolved = resolve_rollout_strategy(config.clone(), "api", 10)?;
        assert_eq!(resolved.percent_each_step, Some(20));
        assert_eq!(resolved.strategy.as_deref(), Some("default"));
        Ok(())
    }

    #[test]
    fn empty_strategy_name_is_invalid() {
        let err = validate_rollout_strategy_name(" ").expect_err("expected validation failure");
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn unknown_strategy_fails_to_load() {
        let result = load_rollout_strategy("missing_strategy");
        assert!(result.is_err());
        let err = result.err().expect("expected strategy load error");
        assert!(err
            .to_string()
            .contains("invalid rollout strategy plugin manifest"));
    }

    /// The deliberately panicking dylib (`plugins/rollouts/panicking`): the
    /// manager gets an error, not an abort, and the plugin is quarantined.
    /// Runs only when the plugin has been built (`LEGION_BUILD_PLUGINS=1`).
    #[test]
    fn a_panicking_strategy_dylib_is_contained_and_quarantined_test() -> R<()> {
        let paths = plugin_paths("rollouts", "panicking", &plugin_root_dir());
        if !paths.dylib.exists() {
            eprintln!(
                "skipping: {} is not built (LEGION_BUILD_PLUGINS=1)",
                paths.dylib.display()
            );
            return Ok(());
        }
        let config = RolloutConfig {
            strategy: Some("panicking".into()),
            ..Default::default()
        };
        let err = resolve_rollout_strategy(config.clone(), "api", 4).unwrap_err();
        assert!(format!("{err:#}").contains("panicked"), "{err:#}");
        let err = resolve_rollout_strategy(config, "api", 4).unwrap_err();
        assert!(format!("{err:#}").contains("quarantined"), "{err:#}");
        crate::plugin_runtime::containment::release_quarantine("rollouts", "panicking");
        Ok(())
    }
}
