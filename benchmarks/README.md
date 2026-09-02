# Benchmarks

Every Legion benchmark lives under this directory — the Rust micro-benchmarks,
the scenario-driven scheduler harness, and the AWS comparison rig.

| Path | What it is | How it runs |
| --- | --- | --- |
| [`micro/`](micro) | Criterion micro-benchmarks compiled into the crate (`scheduler_bench`, `eval_critical_path_bench`). Declared as `[[bench]]` targets in `Cargo.toml` with an explicit `path`, so they live here rather than in Cargo's default `benches/`. | `cargo bench --bench scheduler_bench` |
| [`scheduler/`](scheduler) | Scenario definitions (`scenarios/*.json`) and SLO thresholds (`slo.json`) for the whole-plan scheduler runs in `tests/scheduler_benchmark_slo.rs` and `tests/scheduler_scale_benchmark.rs`. Its README is also where recorded before/after result tables live. | `cargo test --test scheduler_benchmark_slo -- --ignored --nocapture` |
| [`aws/`](aws) | The comparative rig: Terraform, configs, load driver, and experiment scripts for measuring Legion against Nomad and Kubernetes on identical AWS hardware. | See [`aws/README.md`](aws/README.md) |
| [`loadgen/`](loadgen) | `legion-loadgen`: the rig's native Legion job submitter — one process, pooled gRPC connections, many concurrent `RunJob` calls. A `[[bin]]` behind the `loadgen` feature so ordinary builds skip it. **Legion-only**, so read the submission-path rule in [`aws/README.md`](aws/README.md) § Methodology before comparing a cell built with it against another system. | `cargo build --release --features loadgen --bin legion-loadgen` |

Scheduler changes ship with before/after numbers from the relevant
`micro/scheduler_bench.rs` groups, recorded in
[`scheduler/README.md`](scheduler/README.md). The full picture — what each
target measures, how to read Criterion's noise floor, and the persistence
backend matrix — is in
[`docs/testing-and-benchmarks.md`](../docs/testing-and-benchmarks.md).
