use libloading::{Library, Symbol};

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

pub struct LoadedRolloutStrategy {
    pub strategy: Box<dyn RolloutStrategyPlugin>,
    _lib: Option<Library>,
}

impl LoadedRolloutStrategy {
    fn new(strategy: Box<dyn RolloutStrategyPlugin>, lib: Option<Library>) -> Self {
        Self {
            strategy,
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
    let mut resolved = loaded
        .strategy
        .resolve(group, desired_count, &config)
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
        )),
        other => {
            let path = rollout_strategy_name_to_path(other);
            unsafe {
                let lib = Library::new(&path)
                    .with_context(|| format!("failed opening rollout strategy plugin {}", path))?;
                let strategy = {
                    let func: Symbol<unsafe extern "C" fn() -> Box<dyn RolloutStrategyPlugin>> =
                        lib.get(b"get").with_context(|| {
                            format!("rollout strategy plugin {} missing get symbol", other)
                        })?;
                    func()
                };
                Ok(LoadedRolloutStrategy::new(strategy, Some(lib)))
            }
        }
    }
}

fn rollout_strategy_name_to_path(name: &str) -> String {
    let mut path = plugin_root_dir();
    path.push("rollouts");
    path.push(name);
    path.push("target");
    path.push("release");
    path.push(format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        name,
        std::env::consts::DLL_SUFFIX
    ));

    path.to_string_lossy().into_owned()
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
            .contains("failed opening rollout strategy plugin"));
    }
}
