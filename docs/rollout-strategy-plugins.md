# Rollout Strategy Plugins

Rollout strategies now use a plugin contract in `src/manager/rollout_strategy.rs`.

## Contract

- Trait: `RolloutStrategyPlugin`
- Entry point symbol: `get`
- Dynamic plugin path: `plugins/rollouts/<strategy>/target/release/lib<strategy>.<ext>`
- Built-in strategy: `default`

The strategy receives `(group, desired_count, RolloutConfig)` and returns an effective `RolloutConfig` used by the rollout engine.

## Example plugin

`plugins/rollouts/slow_start/src/lib.rs` provides an example plugin:

- name: `slow_start`
- behavior: if no step mode is configured, injects `percentSteps = [5, 20, 50, 100]`
- keeps all other rollout settings unchanged

## Example jobspec

`configs/jobs/rollout-plugin-slow-start.ncl` demonstrates usage:

```nickel
rollout = {
  strategy = "slow_start",
  replacement = "createFirst",
}
```

## Notes

- Validation rejects unknown strategy names at jobspec submit time.
- `strategy` defaults to `"default"` when omitted.
