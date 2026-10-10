# Rollout Strategy Plugins

A rollout strategy decides how a job's instances are replaced during a rollout,
for example how many go at each step. You can swap in your own strategy as a
plugin. The plugin contract is defined in `src/manager/rollout_strategy.rs`.

## Contract

- Trait: `RolloutStrategyPlugin`
- Entry point symbol: `get`
- Dynamic plugin path: `plugins/rollouts/<strategy>/target/release/lib<strategy>.<ext>`
- Built-in strategy: `default`

The strategy receives `(group, desired_count, RolloutConfig)` and returns the
`RolloutConfig` that the rollout engine then uses.

## Example plugin

`plugins/rollouts/slow_start/src/lib.rs` provides an example plugin:

- name: `slow_start`
- behavior: if no step mode is configured, injects `percentSteps = [5, 20, 50, 100]`
- keeps all other rollout settings unchanged

## Example jobspec

`configs/jobs/rollout-plugin-slow-start.ncl` shows how to use it:

```nickel
rollout = {
  strategy = "slow_start",
  replacement = "createFirst",
}
```

## Notes

- Validation rejects unknown strategy names at jobspec submit time.
- `strategy` defaults to `"default"` when omitted.
