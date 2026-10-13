# Pre-launch spot check

A small real cluster on AWS, a jobspec per feature area, and the scripts to
stand it up, test it, and tear it down. It checks that Legion's headline
features work end to end on real machines before we share it. It is not a
benchmark (that is [`benchmarks/aws/`](../../benchmarks/aws/README.md)).

- **[RUNBOOK.md](RUNBOOK.md)** is the step-by-step plan for a human tester.
- **`smoke.sh`** runs the same scenarios unattended and prints PASS or FAIL.
- Findings from the first run live in
  [`internal_docs/launch-spot-check-findings.md`](../../internal_docs/launch-spot-check-findings.md).

## The rig

| Box | Type | Pricing | Runs |
| --- | --- | --- | --- |
| `sc-mgr-1..3` | t3.small | on-demand | Legion managers, one raft group, ACLs on |
| `sc-wkr-1..2` | m7i.large | spot, nested virtualization | Legion workers: runc, cloud-hypervisor, a 20 GB EBS data disk at `/data` |

About $0.20/hour while up (about $4.60/day): instances, 160 GB of gp3, and
five public IPv4 addresses. Workers use
[nested virtualization](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html),
so microVM sandboxes get `/dev/kvm` without bare metal.

**Temporary shim.** The rig needs one not-for-merge source patch, kept in
[`shims.patch`](shims.patch) rather than in `src/`. Apply it before building
the Linux binary, and only while the real fix is missing (`git apply
--check` fails once the code it touches has changed, which is the cue to
check whether it is still needed). The hunk is marked `SPOT-CHECK SHIM`:

- **Health checks after artifact staging** (`src/worker/task_manager.rs`):
  the staging thread enters the Tokio runtime, so tasks whose images go
  through the artifact pipeline get health checks instead of a panic.

The worker address shim is gone: workers bind `LEGION_WORKER_HOST` and
advertise `LEGION_WORKER_ADVERTISE_ADDR` natively, and managers reach them
over mutual TLS
([architecture-managers-workers.md](../../docs/architecture-managers-workers.md#ports-and-who-dials-whom)).
The rig still opens port 4848 only to the managers' security group.

## Layout

| Path | What |
| --- | --- |
| `infra/` | `up.sh`, `install.sh`, `down.sh`, plus `cli-env.sh`, `ssh.sh`, `refresh-ecr.sh`, cloud-init, and config templates |
| `images/` | `web`, `api`, `frontend`, and `agent` images, and `build-push.sh` (amd64 and arm64, to the rig's ECR) |
| `jobs/` | One jobspec per area, numbered in runbook order. `09`-`13` are the sandbox and session jobs for RUNBOOK section 12: `09-sandboxes` (keyed gated microVMs with egress, injection, and suspend), `10-sandbox-store` (store durability), `11-sessions` (session mode), `12-mem-hog` (memory pressure), `13-band` (preemption). `jobs/mac/` holds the macOS variants. |
| `launcher/` | The agent-sandbox launcher: a stdlib Python server plus one HTML page |
| `mac/` | Manager and worker config for local development, and `build-local.sh` |

## Quick start

```bash
# 1. a Linux x86_64 binary (about an hour under Rosetta; faster on Linux)
git apply tests/spot-check/shims.patch           # only while still needed
docker run --rm --platform linux/amd64 -v "$PWD":/src:ro -v "$PWD/target-linux-amd64":/target \
  -e CARGO_TARGET_DIR=/target -w /src rust:1.90-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq protobuf-compiler clang libclang-dev >/dev/null && cargo build --release --locked --bin legion'
cargo build --bin legion                         # the laptop CLI
# the static guest agent, for microVM exec and gated networking on the
# Alpine agent image (RUNBOOK 12.0); install.sh ships it when present
docker run --rm --platform linux/amd64 -v "$PWD":/src:ro -v "$PWD/target-linux-amd64":/target \
  -e CARGO_TARGET_DIR=/target -w /src rust:1.90-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq protobuf-compiler clang libclang-dev musl-tools >/dev/null && rustup target add x86_64-unknown-linux-musl && cargo build -p legion-guest --profile dist --locked --target x86_64-unknown-linux-musl'

# 2. the rig, the images, the cluster
tests/spot-check/infra/up.sh
tests/spot-check/images/build-push.sh
tests/spot-check/infra/install.sh

# 3. test, then tear down
tests/spot-check/infra/cli-env.sh --fish | source
tests/spot-check/smoke.sh                        # or: smoke.sh sandboxes
tests/spot-check/infra/down.sh
```

Everything the rig creates is tagged `Project=legion-spot-check` in the
`legion-bench` AWS profile, and `down.sh` deletes exactly that.
