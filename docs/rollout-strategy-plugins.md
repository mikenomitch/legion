# Rollout Strategy Plugins

A rollout strategy decides how a job's instances are replaced during a rollout,
for example how many go at each step. You can swap in your own strategy as a
plugin. The plugin contract is defined in `crates/worker/src/rollout_strategy.rs`.

## Contract

- Trait: `RolloutStrategyPlugin` (`name`, `resolve`)
- Export: `legion::export_rollout_strategy!(MyStrategy);`, which writes the
  build fingerprint, the panic probe, and the contained constructor
  (`legion_contained_get`). A plugin that exports only the old `get` is
  refused with that fix in the error.
- Dynamic plugin path: `plugins/rollouts/<strategy>/target/release/lib<strategy>.<ext>`
- Built-in strategy: `default`

The strategy receives `(group, desired_count, RolloutConfig)` and returns the
`RolloutConfig` that the rollout engine then uses.

Strategies run inside the manager. A strategy that panics does not take the
manager down: the resolve fails with `rollouts plugin <name> panicked in
\`resolve\` (...)`, and the strategy is quarantined until the manager
restarts, so every later rollout that names it fails with `... is
quarantined`. Fix or remove the plugin and restart the manager
([docs/plugin-system.md](plugin-system.md#panics-and-quarantine)).
`plugins/rollouts/panicking` is a deliberately panicking strategy used to
test this; never install it.

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

## Limitations

- **One hook.** A strategy shapes the rollout's config up front
  (`resolve`). There are no step, analysis, or abort hooks: a strategy
  cannot pause on a metric, run an analysis between steps, or abort a
  rollout, the way Argo Rollouts and Flagger can.
- The strategy library is loaded on every resolve.
