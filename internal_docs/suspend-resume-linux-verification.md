# Suspend, Resume, and Oversubscription: Linux Verification

Each phase of the
[suspend/resume/oversubscription plan](plans/suspend-resume-and-oversubscription-plan.md)
was built on macOS, which has no KVM, no cgroup v2, and no `/proc`. The
Linux-only code is written to mirror existing patterns and is unit-tested
against fixture files, but some of it has never run against a real kernel.
This doc lists what still has to be checked on Linux, phase by phase, so the
checks don't get lost. Each item gives prereqs, the commands to run, and what
counts as a pass.

Record results (date, kernel version, VMM version, pass or fail) under each
item when you run it. Open a TODO.md item for every failure.

**Runs so far.** 2026-10-12, on a Linux 6.18 cloud VM without `/dev/kvm` and
with the cgroup memory controller on v1: 1.1 (partial), 3a.1, 3a.2 (after a
message fix), and 3b.5 (reflink on a loop XFS) ran, with results under each.
Nothing that needs KVM (3b to 6, Track 1, [Gated sandboxes](#gated-sandboxes))
or cgroup v2 (1.2 to 1.8, Phase 2, Phase 5's balloon) could run there, and
neither could the MinIO items (7a, 7b). The container items (4.7, 6.5) wait
on the OCI runtime tests in TODO.md.

## Phase 1

What shipped: sandbox VMMs in the worker cgroup, per-allocation usage on the
heartbeat, `GetNodeUsage` and `legion node usage`. See
[docs/resource-enforcement.md](../docs/resource-enforcement.md#measuring-usage).

### Shared setup

Prereqs for every item below:

- A Linux host (x86_64 or aarch64) running as root, with the cgroup v2
  unified hierarchy. Check with `test -f /sys/fs/cgroup/cgroup.controllers`.
- `memory`, `cpu`, and `cpuset` in `/sys/fs/cgroup/cgroup.controllers`.
- Kernel 4.20 or later for PSI, ideally 5.19 or later for `memory.peak`.
  Record `uname -r`, since 5.14 to 5.18 exercises the sampler's own peak
  tracking instead of `memory.peak`.
- `protoc` and a Rust toolchain, then `cargo build` in the repo.

Start a single-node cluster with enforcement on:

```bash
# configs/workers/default.ncl: set resourceEnforcement.enabled = true
# and leave sandbox.vmmOverheadMiB = 64 (the default).
cargo run -- start manager -c configs/manager.ncl &
TOKEN=$(cargo run -q -- join-token create --type default -j | jq -r '.data.token')
sudo -E cargo run -- join "$TOKEN" &
cargo run -q -- node list -j | jq -r '.data[0].id'   # NODE below
```

The worker must log `Resource enforcement enabled` at startup. If it logs
`no cgroup v2 unified hierarchy` instead, stop: the host doesn't meet the
prereqs.

### 1.1 Cgroup and `/proc` readers on a real kernel

Prereqs: shared setup prereqs only (no cluster needed).

```bash
# Runs in Linux CI too: parses this machine's /proc/meminfo.
cargo test --lib worker::usage::tests::reads_this_machines_proc_meminfo

# Needs cgroup v2 with the memory controller on the test process's cgroup.
# Run it from a shell that is in a delegated cgroup, for example:
sudo systemd-run --scope -p MemoryAccounting=yes \
  cargo test --lib worker::usage::tests::reads_this_processs_own_cgroup -- --ignored

# Fixture-based parser tests (also run on macOS):
cargo test --lib worker::usage
```

Then compare the readers with the kernel by hand:

```bash
grep -E 'MemTotal|MemAvailable' /proc/meminfo
cat /proc/pressure/memory
```

Pass:

- Both tests pass.
- `/proc/pressure/memory` has a `some avg10=` line. If the file is missing
  (PSI disabled, `psi=0` on the kernel command line), `legion node usage`
  must show `free N MiB` with no `stall` figure, and `-j` must show
  `"pressure_available": false`. Either outcome passes as long as it matches
  the kernel.

Fail: a test fails, or the reader reports PSI as available on a kernel
without `/proc/pressure/memory`.

**Results.**

- 2026-10-12, kernel 6.18.44 (x86_64, a cloud VM with no `/dev/kvm`, cgroup v1 hybrid with the memory controller on v1): **partial.** `reads_this_machines_proc_meminfo` and
  the `worker::usage` fixture tests pass, and `/proc/pressure/memory` has a
  `some avg10=` line. `reads_this_processs_own_cgroup` cannot run here
  (`memory.current is readable` fails: the memory controller is bound to the
  v1 hierarchy, so the v2 mount at `/sys/fs/cgroup/unified` has none). Rerun
  on a cgroup v2 host.

### 1.2 Usage for an `exec` task within 10%

Prereqs: shared setup, plus `python3` on the host.

```bash
cat > /tmp/mem-300.ncl <<'EOF'
{
  name = "mem-300",
  tasks = [{
    name = "hog",
    runtime = "exec",
    resources = { memory = 512, cpu = 500 },
    config = {
      command = ["python3", "-c",
        "b = bytearray(300 * 1024 * 1024); import time; time.sleep(3600)"],
    },
  }],
}
EOF
cargo run -q -- job run /tmp/mem-300.ncl
sleep 25   # two report intervals
cargo run -q -- node usage "$NODE"
cargo run -q -- node usage "$NODE" -j | jq '.data.allocations[] | select(.job_id=="mem-300")'
```

Compare with the kernel directly. `ALLOC` is the allocation id from the
output above:

```bash
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.current
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.max     # 536870912
grep anon /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.stat
```

Pass:

- The allocation shows `USED MiB` between 300 and 345 (python's own runtime
  adds some), within 10% of `memory.current` / 1 MiB.
- `memory_anon_bytes` is at least 300 MiB.
- `memory.max` is `536870912` (512 MiB). A worker-cgroup task gets its
  declared `memory`.
- `usage sampled Ns ago` stays at or below about 12 across a few reruns.
- `IDLE` shows a time after about 20s, because `sleep` uses no CPU.

Fail: no usage row after 25s, usage off by more than 10%, or a growing
sample age.

Repeat with `runtime = "container"` (an `alpine` image running
`sh -c 'head -c 300m /dev/zero | tail; sleep 3600'`) to cover the
`RuntimeApplied` path. Same pass criteria. The cgroup is the same
`legion/share/legion-$ALLOC` path, created by the OCI runtime.

### 1.3 A sandbox's VMM is in its allocation cgroup

Prereqs: shared setup, plus `/dev/kvm`, `cloud-hypervisor` or `firecracker`
on `PATH`, `e2fsprogs`, and an uncompressed guest kernel exported as
`LEGION_SANDBOX_KERNEL` in the worker's environment. First confirm the
microVM path itself boots:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
  sandbox_microvm_boots -- --ignored --nocapture
```

Then run a sandbox with a known guest size and no `cores`:

```bash
cat > /tmp/sandbox-1g.ncl <<'EOF'
{
  name = "sb-1g",
  tasks = [{
    name = "vm",
    runtime = "sandbox",
    resources = { memory = 1100, cpu = 1000 },
    config = {
      image = "alpine:latest",
      command = ["sh", "-c", "sleep 3600"],
      memory_mib = 1024,
      hypervisor = "auto",
    },
  }],
}
EOF
cargo run -q -- job run /tmp/sandbox-1g.ncl
VMM_PID=$(pgrep -f 'cloud-hypervisor|firecracker' | head -1)
cat /proc/$VMM_PID/cgroup
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/cgroup.procs
```

Pass:

- `/proc/$VMM_PID/cgroup` reads `0::/legion/share/legion-$ALLOC`.
- That pid is listed in the cgroup's `cgroup.procs`.

Fail: the VMM is in the worker's own cgroup or the root cgroup. That means
`join_assignment_cgroup` did not run in `start_microvm`.

Run once with each VMM (`hypervisor = "cloud-hypervisor"`, then
`"firecracker"`), because they are launched differently.

### 1.4 `memory.max` is guest plus overhead

Prereqs: item 1.3's running sandbox.

```bash
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.max
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.swap.max
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/cpu.weight
```

Pass:

- `memory.max` is `(1024 + 64) * 1048576 = 1140850688`.
- `memory.swap.max` is `0`.
- `cpu.weight` is `39` (1000 MHz through the shares-to-weight mapping).

Then change the overhead and check that it follows. Set
`sandbox = { vmmOverheadMiB = 128 }` in `configs/workers/default.ncl`,
restart the manager and the worker, and rerun the job. Pass:
`memory.max` is `1207959552`.

Also check that `vmmOverheadMiB = 0` is refused: the manager must fail to
start with `sandbox.vmmOverheadMiB must be at least 1`.

Fail: any value differs. A `memory.max` of `1153433600` (1100 MiB) means
the override was ignored and `resources.memory` was used.

### 1.5 The guest fills its memory without the VMM being OOM-killed

Prereqs: item 1.3 with this command instead:
`["sh", "-c", "head -c 900m /dev/zero > /dev/shm/fill; sleep 3600"]`.

```bash
sleep 25
cargo run -q -- node usage "$NODE"
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.events
```

Pass:

- The allocation stays running.
- `memory.events` shows `oom_kill 0`.
- `USED MiB` is at least 900 and below 1088 (the ceiling).

Fail: an OOM kill means 64 MiB is too little for that VMM. Record the VMM's
RSS (`ps -o rss= -p $VMM_PID`) above guest size, and raise the default in
`DEF_SANDBOX_VMM_OVERHEAD_MIB` and the docs.

### 1.6 A dev-mode sandbox lands in the worker's cgroup

Prereqs: shared setup, a Linux host **without** `/dev/kvm` (or set
`hypervisor = "dev"`), and `runc` or `crun` on `PATH`.

Run item 1.3's job with `hypervisor = "dev"`, then:

```bash
CPID=$(cat /sys/fs/cgroup/legion/share/legion-$ALLOC/cgroup.procs | head -1)
cat /proc/$CPID/cgroup
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.max
```

Pass:

- The container's processes are in `legion/share/legion-$ALLOC`.
- `memory.max` is the declared `resources.memory`, 1100 MiB =
  `1153433600`. Dev mode gets no VMM overhead.

Fail: the OCI runtime rejects the pre-created cgroup, or the container lands
elsewhere. Note the `runc` or `crun` version.

### 1.7 Teardown reaps the cgroup

Prereqs: any of the jobs above, running.

```bash
cargo run -q -- job stop sb-1g
sleep 5
ls /sys/fs/cgroup/legion/share/ | grep "legion-$ALLOC" || echo gone
pgrep -f 'cloud-hypervisor|firecracker' || echo no-vmm
```

Pass: both print `gone` and `no-vmm`, and within 2 minutes the next
`legion node usage` no longer lists the allocation.

### 1.8 Metrics on a live node

Prereqs: item 1.2's job, running.

```bash
curl -s http://127.0.0.1:4747/metrics | grep -E '^legion_node_(memory|cpu)_'
```

Use the manager's metrics address if it is configured separately (see
[docs/metrics.md](../docs/metrics.md#endpoints)).

Pass:

- One series per gauge for `node="$NODE"`.
- `legion_node_memory_physical_bytes` equals `MemTotal` × 1024.
- `legion_node_memory_used_bytes` is within 10% of the sum of the governed
  allocations' `memory.current`.

Then stop the worker and wait 3 minutes. Pass: the node's series read `0`.

## Phase 2

What shipped: `resources.memoryMax` (`memory.max = memoryMax`,
`memory.low = memory`), sandbox guest size derived from `resources`, worker
`resourceEnforcement.oversubscribe` ratios, the memory pressure response
(`reclaim`, `evict-bursting`, `evict-reserved`), `cause = node_pressure`, and
the "UNDER PRESSURE, IN ORDER" preview in `legion node usage`. See
[docs/resource-enforcement.md](../docs/resource-enforcement.md#burstable-memory-memorymax).

What was tested on macOS: the ordering, the step runner, and the triggers
against a fake node (`cargo test --lib worker::pressure`), capacity math and
config validation (`resources::tests`, `structs::config::tests`), the manager
half end to end (`cargo test --test node_pressure`), and the CLI rendering.
Nothing has written `memory.low` or `memory.reclaim` to a real kernel, and no
real task has been evicted.

**Note on Phase 1 item 1.4.** Its sandbox sets `memory_mib = 1024` and
`resources.memory = 1100`. Phase 2 still boots 1024 MiB in that case (an
explicit `memory_mib` wins this release) and `job run` now warns about the
mismatch, so 1.4's expected values are unchanged. To check the derived size
instead, drop `memory_mib` and expect `memory.max` = `(1100 + 64) * 1048576
= 1220542464`.

### Shared setup

Phase 1's shared setup, plus:

- Kernel 5.19 or later for `memory.reclaim` (record `uname -r`). On an
  older kernel item 2.3 expects `reclaim` to free nothing and the response to
  go straight to eviction.
- Check how cgroup v2 is mounted: `grep cgroup2 /proc/mounts`. Record
  whether `memory_recursiveprot` is in the options. Without it, item 2.1's
  `memory.low` is written but may not protect anything (see the failure
  table in the public doc).
- `python3` and `stress-ng` (or python alone) on the host.

### 2.1 `memory.max = memoryMax`, `memory.low = memory`

Prereqs: shared setup.

```bash
sudo -E cargo test --lib worker::resources::tests::burstable_limits_land_in_a_real_cgroup \
  -- --ignored --nocapture
```

Then with a real task:

```bash
cat > /tmp/burst.ncl <<'EOF2'
{
  name = "burst",
  tasks = [{
    name = "hog",
    runtime = "exec",
    resources = { memory = 128, memoryMax = 512, cpu = 200 },
    config = { command = ["sleep", "3600"] },
  }],
}
EOF2
cargo run -q -- job run /tmp/burst.ncl
ALLOC=$(cargo run -q -- node usage "$NODE" -j | jq '.data.allocations[] | select(.job_id=="burst") | .allocation_id')
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.max   # 536870912
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.low   # 134217728
cat /sys/fs/cgroup/legion/memory.low /sys/fs/cgroup/legion/share/memory.low   # max, max
```

Repeat with `runtime = "container"` (alpine, `sleep 3600`) and check the
same two files: the OCI runtime writes `memory.reservation` as `memory.low`.

Pass: the ignored test passes, and the files hold the values in the
comments for both runtimes. `legion node usage` shows `128/512` in the
`RES/MAX MiB` column.

Fail: `memory.low` is `0` on the leaf (not written), or `memory.max` is
`134217728` (the reservation used as the ceiling).

### 2.2 Oversubscription admits the ratio

Prereqs: shared setup. Set in `configs/workers/default.ncl`:

```nickel
resourceEnforcement = { enabled = true, oversubscribe = { memory = 1.5 } },
```

Restart the manager and worker. The worker must not log `cannot enforce`.

```bash
cargo run -q -- node usage "$NODE" | head -5
cargo run -q -- node usage "$NODE" -j | jq '.data.memory | {capacity, oversubscribe_ratio}'
```

Pass: the memory row's capacity reads `<physical - reserved> MiB ×1.5`, and
`capacity` in JSON is 1.5 times the value with the ratio removed. Submit a job
whose `count x memory` is about 1.3x physical memory (for example
`memory = 1024`, `count` sized to fit) using `sleep` so nothing is actually
used: every allocation places. At ratio 1.0 (remove the block, restart), the
same job leaves some allocations unplaced.

Also check that `oversubscribe = { memory = 1.5 }` with `enabled = false`
makes the manager refuse to start with `need resourceEnforcement.enabled =
true`.

### 2.3 Two bursting `exec` tasks on a small watermark (the integration test)

This is the Phase 2 acceptance scenario on a real kernel: two tasks burst
past their reservation, the node crosses its threshold, and the lower band is
evicted with `cause = node_pressure` while the higher band survives.

Prereqs: shared setup. Pick `minFree` so the two tasks can cross it: read
`MemAvailable` from `/proc/meminfo` (call it `A` MiB) and set `minFree` to
`A - 1500` MiB, so 1.5 GiB of new use triggers the response. Worker config:

```nickel
resourceEnforcement = {
    enabled = true,
    memoryPressure = { minFree = "<A - 1500>MiB", settle = "3s" },
},
```

Restart the worker, which must log `Memory pressure response armed` with
that `min_free_bytes`.

```bash
cat > /tmp/pressure.ncl <<'EOF2'
{
  name = "batch",
  priority = "low",
  tasks = [{
    name = "hog",
    runtime = "exec",
    resources = { memory = 128, memoryMax = 2048, cpu = 200 },
    config = { command = ["python3", "-c",
      "import time; time.sleep(20); b = bytearray(1000 * 1024 * 1024); time.sleep(3600)"] },
  }],
}
EOF2
sed -e 's/"batch"/"api"/' -e 's/"low"/"high"/' /tmp/pressure.ncl > /tmp/pressure-api.ncl
cargo run -q -- job run /tmp/pressure.ncl
cargo run -q -- job run /tmp/pressure-api.ncl
sleep 12
cargo run -q -- node usage "$NODE"        # before the burst: preview order
sleep 30
cargo run -q -- events list --kind node.pressure
cargo run -q -- node usage "$NODE"
cargo run -q -- job info batch
cargo run -q -- job info api
```

Pass:

- Before the burst, both rows may be absent from `UNDER PRESSURE, IN ORDER`
  (neither is above its reservation yet). Once both have allocated (about
  20s in), the preview lists `batch` first (`low`) and `api` second, each
  with `reclaim, then evict-bursting`.
- `events list` shows `info` `node.pressure` events for `reclaim` first,
  then one `warn` event `Allocation N: evicted by evict-bursting on <node>:
  using ~1.0 GiB of a 128 MiB reservation while free memory was ...`, for
  the `batch` allocation only. Its metadata (`-j`) has `cause:
  node_pressure` and a `hint`.
- The batch task's stop notice (`cat <alloc dir>/stop-notice.json` on the
  worker) says `"cause": "node_pressure"`.
- `api` is still running with the same allocation id. `batch` has a new
  allocation (the replacement), which may land on this node again (a known
  limitation).
- `curl -s localhost:<worker metrics port>/metrics | grep
  legion_worker_pressure_actions_total` and the manager's
  `legion_node_pressure_actions_total{node=...,step="evict-bursting"}` are
  both at least 1.

Fail: `api` is evicted, the kernel OOM killer acts first (`dmesg | grep -i
oom`), an evicted `batch` comes back with the same allocation id before the
manager removed it, or no event appears.

### 2.4 Within the reservation is never evicted at ratio 1.0

Prereqs: item 2.3's worker config. Submit `batch` with `memory = 1100,
memoryMax = 1100` (no burst) and the same python command, plus a low-band
`noisy` job that bursts.

Pass: only `noisy` is ever evicted, and `batch` never appears in the preview.
Then set `oversubscribe = { memory = 1.5 }`, restart, and repeat with only
`batch` running and `minFree` set so that `batch` alone crosses it: the
preview shows `batch` with `evict-reserved`, and it is evicted with that step.

### 2.5 `memory.reclaim` frees page cache

Prereqs: kernel 5.19 or later. A task that fills page cache above its
reservation, for example `exec` with `memory = 128, memoryMax = 1024` running
`sh -c 'head -c 800m /dev/urandom > /tmp/f; cat /tmp/f > /dev/null; sleep 3600'`
with a private tmp, and a `minFree` it crosses.

Pass: a `node.pressure` `reclaim` event with `freed_bytes` well above zero,
`memory.current` of the task falls toward 128 MiB, and the task keeps
running.

### 2.6 Sandbox guest size from `resources`

Prereqs: Phase 1 item 1.3's KVM setup. Run a sandbox with no `memory_mib`
and `resources = { memory = 512, memoryMax = 1024 }`.

```bash
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.max   # (1024 + 64) MiB = 1140850688
cat /sys/fs/cgroup/legion/share/legion-$ALLOC/memory.low   # 536870912
cargo run -q -- job logs <job> | grep -i 'Memory:'          # guest sees ~1 GiB
```

Then set `memory_mib = 768` in the config and resubmit: `job run` prints the
`config.memory_mib = 768 ... resources size the guest at 1024 MiB` warning,
and the guest boots with 768 MiB.

Also run a sandbox with no `memory` and no `memory_mib` on a manager whose
`taskResourceDefaults.memory` is 128: `legion node usage` shows a 512 MiB
reservation, the guest boots 512 MiB, and `memory.max` is
`(512 + 64) * 1048576 = 603979776`.

Fail: the guest boots with 512 MiB (the reservation) when `memoryMax` is
1024, or the undeclared sandbox boots or reserves 128 MiB.
## Phase 3a

The node content store is platform independent and its unit tests ran on
macOS (`cargo test --lib content_store`). Nothing in it is Linux-only, but it
has not served artifacts on a Linux worker, where the state dir usually sits
on ext4 or XFS and `statvfs` reports a different filesystem than a dev laptop.

### Prereqs

- A Linux worker with the content store enabled (the default) and a
  `content.minFreeDisk` you can cross, for example on a small loopback
  filesystem: `truncate -s 2G /tmp/state.img && mkfs.ext4 /tmp/state.img &&
  mount -o loop /tmp/state.img /var/lib/legion`.

### 3a.1 A digest-pinned artifact is served from the store the second time

Run a job whose task has an `artifacts` entry with a `checksum` (any file of a
few MiB over HTTPS), then stop it and run it again.

```bash
ls -l <worker state>/content/sha256/
sqlite3 <worker state>/content/index.sqlite "select digest, class, size from entry;"
cargo run -q -- job rollout status --id <job> --json | jq '.. | .artifact_states? // empty'
```

- **Pass:** one entry, class `artifacts`, mode `0444`. The first allocation's
  status row has `"cache": "miss"`, the second's `"cache": "hit"`.
  The task's copy is writable and is not a hardlink (`stat -c %h` is 1).

**Results.**

- 2026-10-12, kernel 6.18.44 (x86_64, a cloud VM with no `/dev/kvm`, cgroup v1 hybrid with the memory controller on v1), an isolated manager (`:5947`) and worker (`:5958`)
  with the worker state on a 1.2 GB ext4 loop filesystem, a 4 MiB artifact
  over plain HTTP (`allowedSchemes` opted in): **pass.** One entry
  (`sha256:e923…`, class `artifacts`, mode `0444`). The origin served it
  once. The second allocation logged `Staged artifact from node content
  store` and its rollout status row shows `"cache": "hit"`. The task's copy
  is `0644` with one link. A stopped-then-rerun job with an unchanged spec
  is not re-placed (`Job config unchanged`), so the rerun needs a new
  version, and a first placement has no rollout, so the `miss` row only
  shows in the worker log.

### 3a.2 `minFreeDisk` on a real filesystem

Fill the loopback filesystem to within `minFreeDisk` of full
(`fallocate -l <size> /var/lib/legion/filler`) and run a job with a new
digest-pinned artifact.

- **Pass:** the store evicts unpinned entries first, then refuses the new
  entry, and the artifact is staged straight from origin. The worker logs the
  refusal with the free bytes `statvfs` reported, which match `df -B1`.

**Results.**

- 2026-10-12, kernel 6.18.44 (x86_64, a cloud VM with no `/dev/kvm`, cgroup v1 hybrid with the memory controller on v1), same cluster, `minFreeDisk = "700MiB"` on the 1.2 GB
  loop filesystem filled to about 704 MiB free: **pass after a fix.** The
  store evicted the unpinned `blob.bin` (`Evicted content (minFreeDisk)`),
  refused the new 6 MiB entry, and staged it from origin. The refusal named
  only the floor, not the free bytes, so it now reads `node disk is below
  content.minFreeDisk (723292160 bytes free, floor 734003200 bytes) ...`;
  `df -B1` read 735875072 a moment later, after the stopped allocation's
  copy was removed. Budgets on so small a disk are clamped above the disk's
  size (`artifacts_budget_bytes: 1073741824`, `snapshots: 2147483648` on
  1.2 GB), which `minFreeDisk` covers.

## Phase 3b

Runtime checkpoint and restore: the `TaskPlugin` checkpoint methods, the
Firecracker and cloud-hypervisor API clients, the identity disk, the compat
fingerprint attributes, and the `local-dir` snapshot op. On macOS these ran
against fake VMM API servers. The items below need real KVM.

### Prereqs

- x86_64 or aarch64 Linux with `/dev/kvm` readable and writable by the user
  running the tests (`ls -l /dev/kvm`, add the user to the `kvm` group).
- `e2fsprogs` (`mkfs.ext4`, `debugfs`) on `PATH` or in `/sbin`.
- Firecracker 1.x (tested shape: `PUT /snapshot/load` with `mem_backend`,
  which needs 1.2 or newer) and cloud-hypervisor v35 or newer, as
  `firecracker` and `cloud-hypervisor` on `PATH`, or set
  `LEGION_FIRECRACKER_BIN` / `LEGION_CLOUD_HYPERVISOR_BIN`.
- An uncompressed guest kernel with virtio-blk and a serial console, for
  example Firecracker's CI `vmlinux-6.1`. Export
  `LEGION_SANDBOX_KERNEL=/path/to/vmlinux`.
- Network access for the `alpine:latest` pull, or set
  `LEGION_SANDBOX_TEST_IMAGE` to an image the node can reach.

### 3b.1 Memory checkpoint and restore keeps a RAM-only counter

The guest runs `i=0; while true; do i=$((i+1)); echo counter=$i; sleep 0.2;
done`, so the counter exists only in guest memory. The test boots it with
`checkpoint_launch = true`, waits for `counter=10`, checkpoints with
`AfterCheckpoint::Stop`, restores into a new allocation, and checks that the
restored console continues counting instead of starting at 1.

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
  sandbox_memory_checkpoint_round_trip_firecracker -- --ignored --nocapture
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
  sandbox_memory_checkpoint_round_trip_cloud_hypervisor -- --ignored --nocapture
```

- **Pass:** both tests pass. `--nocapture` prints `restore returned in …`.
  Record that number for each VMM (the Phase 4 target is under 300 ms for a
  1 GiB guest on reference hardware).
- **Fail, counter restarts at 1:** the restore booted fresh instead of
  loading memory. Check the VMM's stderr in the test temp dir's
  `logs8/stderr.log`.
- **Fail, `snapshot/load` error naming a drive path:** a device link was not
  in place. Inspect `<tmp>/allocs/.vmdev/<key>/`: every link must exist and
  point into `allocs/8/.sandbox/` or the volume dir.
- **Fail on cloud-hypervisor `--restore`:** check the version's flag syntax
  (`--restore source_url=file:///dir`) and whether it needs `resume=true`
  instead of the follow-up `vm.resume` call.

### 3b.2 Firecracker launch flags

Run a sandbox job on Firecracker twice, once as-is and once with a
`snapshots = {}` block on the task (Phase 3c wired the block to
`TaskInstance.checkpoint_launch`).

```bash
ps -o args= -C firecracker
```

- **Pass:** a normal sandbox shows `--no-api --config-file …`. A
  checkpointable one shows `--api-sock <alloc>/.sandbox/vmm.sock --config-file
  …`, and its config JSON (`<alloc>/.sandbox/firecracker.json`) has a fourth
  drive `identity` whose `path_on_host` is under `allocs/.vmdev/`.

### 3b.3 Identity disk inside the guest

In the checkpointable guest from item 2 (or by adding a `dd` to the counter
test's command):

```bash
cat /proc/cmdline                      # contains legion.identity=/dev/vdX
dd if=/dev/vdX bs=1 count=8 2>/dev/null   # prints LEGIONID
```

- **Pass:** the device named on the cmdline starts with `LEGIONID`, and the
  JSON after the 16-byte header names the allocation id. After a restore, the
  host-side `allocs/8/.sandbox/identity.img` names allocation 8 (the guest
  re-reading it is Phase 4).

### 3b.4 Compat fingerprint attributes

Start a worker and read its node attributes:

```bash
cargo run -- node info <node-id> -j
```

Look at the fingerprint attributes (a JSON string in the envelope; pipe it
through `jq 'fromjson'` if needed) for keys starting `cpu.` and `sandbox.`.

- **Pass:** `cpu.vendor`, `cpu.model` (`family:model:stepping` on x86),
  `cpu.features` (16 hex chars), `sandbox.vmm` (`cloud-hypervisor` when both
  are installed), `sandbox.vmm.version`, and one
  `sandbox.vmm.<name>.version` per installed VMM.
- Two identical machines report identical values. A machine with a different
  CPU generation reports a different `cpu.model` or `cpu.features`.

### 3b.5 `local-dir` snapshots reflink on btrfs or XFS

On a worker whose state dir is on btrfs or XFS (`mkfs.xfs -m reflink=1`):

```bash
cargo test --lib local_dir_filesystem_snapshot_round_trip local_dir_block_snapshot_round_trip
legion volume driver test local-dir --mode block -p size=256M
```

Then snapshot a large block volume through the conformance suite and check
the snapshot ref's `attrs.method`:

- **Pass:** tests pass, the conformance run shows `snapshot`, `restore`,
  `clone`, and `snapshot-delete` checks passing, and on btrfs/XFS the
  `snapshot` detail is instant for a multi-GiB image (`method = reflink`). On
  ext4 it reports `copy` and `du -h` of the snapshot shows only written
  blocks.

**Results.**

- 2026-10-12, kernel 6.18.44 (x86_64, a cloud VM with no `/dev/kvm`, cgroup v1 hybrid with the memory controller on v1), a 2 GiB loop XFS (`mkfs.xfs -m reflink=1`) used as
  `TMPDIR` and `--dir`: **pass, timing not measured.** The local-dir tests
  pass, and the block-mode conformance run passes every check with
  `snapshot ... (268435456 bytes, reflink)`. On the host's ext4 it reports
  `copy`. A 4 GiB run did not finish in 20 minutes in a debug build (the
  suite fingerprints the whole image several times), so the multi-GiB
  timing needs a release build.

## Phase 3c

The manager side of snapshots: `legion snapshot create|list|get|delete|restore`,
the heartbeat checkpoint channel, restore placement, and the worker wiring
that captures into the node content store and restores at start. On macOS
the `exec` disk round trip ran for real
(`an_exec_disk_snapshot_round_trips_through_capture_and_restore`), the
manager side ran against a simulated worker
(`tests/snapshot_integration.rs`), and TAP handling ran against a fake host.
The items below need a Linux worker, and the memory items need KVM.

### Prereqs

- Everything in [Phase 1 shared setup](#shared-setup) and
  [Phase 3b prereqs](#prereqs).
- `iproute2` (`ip`) on `PATH`, and the worker running as root (it creates
  TAP devices for 3c.4).
- The worker's content store enabled (the default). Check
  `ls <worker state>/content/sha256` exists after the worker starts.

Use this jobspec for every item (save as `/tmp/workspace.ncl`):

```nickel
{
  name = "workspaces",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 600, cpu = 1000 },
    stateful = {
      mode = "keyed",
      storage = { path = "/workspace", mode = "block", params = { size = "256M" } },
    },
    snapshots = { ttl = "1h", keep = 3 },
    config = {
      image = "alpine:latest",
      command = ["sh", "-c", "i=0; while true; do i=$((i+1)); echo counter=$i; echo $i > /workspace/counter; sleep 0.2; done"],
      memory_mib = 512,
      hypervisor = "firecracker",
    },
  }],
}
```

```bash
cargo run -q -- job run /tmp/workspace.ncl
cargo run -q -- instance create workspaces --id user-42
sleep 10
cargo run -q -- job logs workspaces | tail -2     # counter=N, climbing
```

### 3c.1 Memory snapshot and restore bring back a RAM-only counter

This is the Phase 3 acceptance check.

```bash
cargo run -q -- snapshot create user-42 --memory --name ckpt --wait -j | jq .data.snapshot
cargo run -q -- job logs workspaces | tail -1     # note counter=N
sleep 5                                           # counter moves on
cargo run -q -- snapshot restore user-42 ckpt --wait -j
sleep 2
cargo run -q -- job logs workspaces | tail -3
```

- **Pass:** `create` reports `"state": "local"`, `kind` `memory`, and a
  `durationMs`. After the restore, the new allocation's log continues from
  about N (the value at capture), not from 1 and not from the later value.
  `cat /workspace/counter` inside the guest (or `debugfs` on the volume
  image) agrees with the log within a few ticks.
- **Fail, counter restarts at 1:** the restore booted cold. Check
  `legion events list --kind snapshot -j` for `snapshot.restore_failed` and
  its `error`.
- **Fail, `permission denied` opening the memory or state file:** restore
  hardlinks parts out of the content store, whose blobs are mode `0444`. If a
  VMM opens them for writing, `materialize_parts` in
  `src/worker/checkpoints.rs` must copy instead of hardlink (or the store
  must hand out a writable clone). Record which VMM and version.
- Record the restore time (`snapshot.restored` has no duration yet, so time
  `restore --wait` and note it). Repeat with `hypervisor =
  "cloud-hypervisor"`.

### 3c.2 Parts land in the content store and are pinned

After 3c.1:

```bash
ls -l <worker state>/content/sha256/
sqlite3 <worker state>/content/index.sqlite "select digest, class, size from entry; select * from pin;"
```

- **Pass:** one entry per manifest part (`memory`, `vm-state`, and `rootfs`
  for Firecracker), class `snapshots`, each pinned by `snapshot:<id>`, files
  mode `0444`. `legion snapshot get <id> -j | jq '.data.manifest.memory'`
  lists the same digests.
- Then `legion snapshot delete <id>` and wait two heartbeats. **Pass:** the
  pins and the blobs are gone, the volume snapshot directory
  `<worker state>/volumes/local-dir-snapshots/<id>` is gone, and
  `legion snapshot get <id>` is `NotFound`.

### 3c.3 The live capture keeps memory and disk consistent

During 3c.1's capture, the guest is paused across the volume snapshot.

```bash
cargo run -q -- snapshot create user-42 --memory --wait -j | jq .data.snapshot.durationMs
```

- **Pass:** after restoring that snapshot, the volume's `/workspace/counter`
  and the console counter agree (they were captured at one instant). The
  guest's filesystem mounts cleanly after restore (`dmesg` in the guest shows
  no ext4 journal replay errors).

### 3c.4 TAP restore behavior

Change the jobspec to attach a TAP device (gated plan Phase 4: every TAP is
per instance, on a bridge with a pool in the worker config, see [Gated
sandboxes](#gated-sandboxes) prereqs):

```nickel
network = { mode = "tap", bridge = "br0" },
```

Restart the instance, note its device (`cat <alloc dir>/.sandbox/tap-device`,
`lgf...`, below as `$TAP`), then take a memory snapshot.

1. Restore with the TAP present and free. **Pass:** `snapshot.restored`, and
   `ip -d link show $TAP` shows the VMM attached (carrier up).
2. Stop the instance (destroy deletes `$TAP`), start it again and restore.
   **Pass:** the worker logs `Created the TAP device a sandbox restore needs`,
   `ip link show $TAP` exists, is UP, and is `master br0`, and after `legion
   instance destroy` the device is gone again.
3. Hold the TAP with another process (`ip tuntap add` a second time is not
   enough: start a second Firecracker on it, or
   `python3 -c "import fcntl,os,struct;f=os.open('/dev/net/tun',os.O_RDWR);fcntl.ioctl(f,0x400454ca,struct.pack('16sH','$TAP'.encode(),0x1002));input()"`)
   and restore. **Pass:** `snapshot.restore_failed` with
   `restore needs TAP device lgf...: it exists and another process holds it`,
   and the instance then boots cold (no VMM started on the busy device).
4. Run the worker as a non-root user without `CAP_NET_ADMIN` and restore
   with the TAP missing. **Pass:** `restore needs TAP device lgf...: creating
   it failed: ...` naming the `ip` error.

### 3c.5 Superseded: gated sandboxes take memory checkpoints

Gated sandboxes take memory checkpoints since the gated sandbox checkpoints
plan, so the submit refusal this item checked is gone. Verify the gated
paths with [Gated sandboxes](#gated-sandboxes) G1.1 to G1.5 instead.

### 3c.6 Restored VMM is governed

With `resourceEnforcement.enabled = true` on the worker, after 3c.1:

```bash
VMM_PID=$(pgrep -f 'firecracker --id legion-sbx' | tail -1)
cat /proc/$VMM_PID/cgroup
```

- **Pass:** the restored VMM is in `legion/share/legion-<new alloc>`, the same
  as a booted one (Phase 1 item 1.3).

## Phase 4

Suspend and resume: `legion instance suspend`, resume through `legion
instance start`, `onRestoreFailure`, `reserve`, and the guest's resume
handler (`legion guest-resume`). On macOS the manager side ran end to end
against a simulated worker (`cargo test --test suspend_integration`), the
`exec` disk suspend ran for real
(`an_exec_disk_suspend_cold_boots_on_its_retained_volume`), and the resume
handler ran against a fake guest (`cargo test --lib guest_resume`). Nothing
has paused, stored, and killed a real VMM for a suspend, and the resume
handler has never run inside a guest. The Linux-only parts of the handler
(`RNDADDENTROPY`, `clock_settime`, `sethostname`, `BLKFLSBUF`) have only been
type-checked by reading, since the dev machine cannot build for Linux.

### Prereqs

- Everything in [Phase 3c prereqs](#phase-3c), including KVM, both VMMs, and
  the guest kernel.
- A **glibc** guest image for the identity items: the resume handler is the
  worker's own binary copied into the guest, so it must be able to run there.
  Use `debian:bookworm-slim` (it has `/bin/sh`). With `alpine:latest`, item
  4.5 is the expected result.
- Record the reference hardware (CPU model, NVMe model, `uname -r`) next to
  every latency number.

Use this jobspec (save as `/tmp/agents.ncl`). It attaches no TAP device, so
the restore needs no network setup:

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 1024, cpu = 1000 },
    stateful = {
      mode = "keyed",
      storage = { path = "/workspace", mode = "block", params = { size = "256M" } },
    },
    identities = [{ name = "acme", audience = ["acme-api"], env = "ACME_TOKEN" }],
    suspend = {
      checkpoint = "memory",
      onResume = ["/bin/sh", "-c", "date +%s > /workspace/resumed-at; hostname >> /workspace/resumed-at"],
    },
    config = {
      image = "debian:bookworm-slim",
      command = ["/bin/sh", "-c", "head -c 900M /dev/urandom > /dev/shm/fill; i=0; while true; do i=$((i+1)); echo counter=$i; sleep 0.2; done"],
      hypervisor = "firecracker",
    },
  }],
}
```

The command touches about 900 MiB of guest memory, so the checkpoint is close
to the plan's 1 GiB reference. The manager needs `workloadIdentity.enabled =
true` for the `identities` block.

```bash
cargo run -q -- job run /tmp/agents.ncl
cargo run -q -- instance create agents --id user-42
sleep 20
cargo run -q -- job logs agents | tail -1          # counter=N, climbing
```

### 4.1 Suspend frees the reservation, resume restores under 300 ms

This is the Phase 4 acceptance check.

```bash
cargo run -q -- node usage "$NODE" | grep -i reserved     # note reserved memory
cargo run -q -- instance suspend user-42 --wait -j | tee /tmp/suspend.json
cargo run -q -- node usage "$NODE" | grep -i reserved     # 1024 MiB less
cargo run -q -- instance list agents                      # CHECKPOINT: snap-... (memory, local <node>)
sleep 30
cargo run -q -- instance start user-42 --wait -j | tee /tmp/resume.json
cargo run -q -- job logs agents | tail -3
curl -s localhost:<manager metrics port>/metrics | grep -E 'legion_instance_(suspend|resume)_seconds'
```

- **Pass:** `suspend.json` has `state: suspended`, `kind: memory`, `bytes`
  around 1 GiB, and `duration_ms` under 1500. Reserved memory drops by the
  task's 1024 MiB while suspended. `resume.json` has `state: active`,
  `resumed_from` equal to the checkpoint, and `restore_ms` under 300. The log
  continues from the counter value at suspend, not from 1. Record
  `duration_ms` and `restore_ms` for both VMMs (repeat with `hypervisor =
  "cloud-hypervisor"`) and put them in `docs/testing-and-benchmarks.md`.
- **Fail, counter restarts at 1:** the resume booted cold. Check
  `legion events list --kind instance.restore_failed -j` for the reason.
- **Fail, `restore_ms` well over 300:** check where the content store lives
  (it must be local NVMe) and whether the memory part was copied instead of
  hardlinked (`ls -li` the part in the new allocation's `.sandbox/` against
  `<worker state>/content/sha256/`).

### 4.2 The guest is never resumed past the capture, and a failed store keeps it running

During 4.1's suspend, watch the VMM:

```bash
VMM_PID=$(pgrep -f 'firecracker --id legion-sbx' | tail -1)
cargo run -q -- instance suspend user-42 &
while kill -0 $VMM_PID 2>/dev/null; do grep State /proc/$VMM_PID/status; sleep 0.05; done
```

Then make the store fail: set `content.budgets.snapshots = "100MiB"` on the
worker, restart it, start the instance again, and suspend it.

- **Pass:** the first run shows the VMM alive until the checkpoint is stored,
  then gone (no window where the guest runs after the pause: the console log
  has no counter lines after the last one before the suspend). The second run
  reports `instance.suspend_failed` naming the budget, `instance get` shows
  `state: active` with the error in `detail`, and the log keeps counting.
- **Fail:** the counter advances after the suspend began, or the instance is
  stopped after a failed capture.

### 4.3 Identity refresh inside the guest

After a resume (4.1), read the guest's view. There is no exec into a
sandbox, so read it through `onResume`'s output on the volume and the
console:

```bash
cargo run -q -- job logs agents | grep legion-guest-resume
# legion-guest-resume: restored as allocation <new id>; refreshing identity
# legion-guest-resume: refreshed reseed, clock, hostname, identity.json, resume.env, token ACME_TOKEN, onResume
cargo run -q -- instance stop user-42
debugfs -R 'cat /resumed-at' <worker state>/volumes/local-dir/<volume>/disk.img
```

Also capture the token: add `cat /legion/identity/ACME_TOKEN` to the
`onResume` command and decode the JWT payload (`cut -d. -f2 | base64 -d`).

- **Pass:** the console shows both lines with no `FAILED`. `resumed-at`
  holds a Unix time within a few seconds of the host's at resume (not the
  time of the suspend, 30 seconds earlier) and the hostname
  `legion-sbx-<new allocation id>`. The token's `legion.io/alloc` claim is the
  new allocation id from `instance get`, not the suspended one.
- **Fail, `reseed: RNDADDENTROPY failed`:** the ioctl number or the
  `rand_pool_info` layout is wrong for this architecture. Record `uname -m`.
- **Fail, `clock` missing or the time is the suspend's:** the handler did
  not run, or `clock_settime` failed (it needs `CAP_SYS_TIME`, which the
  guest's root has).
- **Fail, no `legion-guest-resume` lines at all:** the handler never
  noticed the restore. Check that `/proc/cmdline` in the guest has
  `legion.identity=` and that the handler started at boot (`legion-guest-resume:
  watching /dev/vdX` on the first boot's console).

### 4.4 RNG divergence

Suspend, then resume twice from equivalent state: take a memory snapshot
(`legion snapshot create user-42 --memory --wait`), restore it twice in a
row, and have `onResume` record `head -c 16 /dev/urandom | od -An -tx1` to
the volume each time.

- **Pass:** the two recorded byte strings differ. (Without the reseed, two
  restores of one snapshot can produce the same bytes.)

### 4.5 A musl guest restores without the refresh, and says so

Repeat 4.1 with `image = "alpine:latest"`.

- **Pass:** the resume itself works (the counter continues), and the
  console shows the handler failing to start at boot (`legion: resume handler
  missing` or an exec error from the dynamic loader). Record the exact text so
  the public doc's limitation can quote it.

### 4.6 `onRestoreFailure` on a real node

1. Suspend, then remove the checkpoint's memory blob from the content store
   (`rm <worker state>/content/sha256/<hex of the memory part>`, found with
   `legion snapshot get <checkpoint> -j`), and `instance start`.
   **Pass:** `instance.restore_failed` with `outcome: cold` and an error
   naming the missing part, and the instance runs again from a cold boot on
   its volume (the counter restarts at 1, `/workspace` keeps its files).
2. Set `suspend.onRestoreFailure = "fail"`, `job run` again, and repeat.
   **Pass:** the instance is `suspended` with the error in `detail`, and
   `legion instance start user-42 --cold` boots it.
3. Upgrade (or swap) the node's Firecracker binary to another version,
   restart the worker, and resume a checkpoint taken before. **Pass:**
   placement refuses with `sandbox.vmm.firecracker.version is ...`, and after
   `resumeTimeout` (30s) the instance boots cold (`instance.restore_failed`,
   `outcome: cold`).

### 4.7 Disk suspend of a `container` task

With a `container` task (alpine, `sleep 3600` writing a file to its
`stateful.storage` volume), run `instance suspend --wait` and `instance
start --wait`.

- **Pass:** `kind: disk`, `bytes: 0`, the container is gone while suspended
  (`ctr task ls` or the runtime's equivalent), and the resumed container is a
  fresh process that sees the file on its volume.

### Ignored tests

None new. 4.1 through 4.7 are the real-hardware checks, and the KVM tests
from Phase 3b still apply:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib sandbox_memory_checkpoint_round_trip -- --ignored --nocapture
```

## Phase 5

Automatic suspend: the idle policy (worker detection, the manager's idle
sweep), the `suspend` memory pressure step with local suspensions, and the
sandbox balloon for `reclaim`. On macOS the manager side ran end to end with
fake usage reports and simulated local-suspension reports (`cargo test
--test suspend_integration`), the idle detector and sampler ran against
fixture activity (`cargo test --lib worker::idle worker::usage`), the
pressure ordering and step runner against a fake node (`cargo test --lib
worker::pressure`), a pressure suspend of a real `exec` task ran through the
worker (`a_pressure_suspend_reports_a_local_suspension_and_holds_the_allocation`),
and the balloon calls ran against fake Firecracker and cloud-hypervisor API
servers (`cargo test --lib balloon reclaim_drives`). Nothing has measured a
real allocation going idle (macOS has no cgroups, so no usage reports and no
idle tracking), no real node has run short of memory with an opted-in
instance, and no real guest has had its balloon inflated.

### Prereqs

- Everything in [Phase 4 prereqs](#phase-4), including the `agents` jobspec
  and a glibc guest image.
- The worker type with `resourceEnforcement = { enabled = true }` (the idle
  policy reads the usage reports enforcement produces).
- Firecracker 1.2+ and cloud-hypervisor v35+. Record both versions. For 5.5
  record the guest kernel's `CONFIG_PAGE_REPORTING` (`zcat /proc/config.gz |
  grep PAGE_REPORTING` inside the guest, or the kernel build config).
- `stress-ng` (or any memory hog) on the host for 5.3 and 5.4.

Add the Phase 5 fields to `/tmp/agents.ncl`'s task:

```nickel
resources = { memory = 512, memoryMax = 1024, cpu = 1000 },
suspend = {
  checkpoint = "memory",
  idle = { after = "1m" },
  onPressure = true,
},
```

and change the command to one that goes quiet: `["/bin/sh", "-c", "head -c
300M /dev/urandom > /dev/shm/fill; sleep infinity"]`.

### 5.1 An idle sandbox suspends within `after` + 30s

This is the Phase 5 idle acceptance check.

```bash
cargo run -q -- job run /tmp/agents.ncl
cargo run -q -- instance create agents --id user-42
cargo run -q -- instance create agents --id user-17
# Keep user-17 busy: an open exec session counts as activity.
cargo run -q -- alloc exec <user-17 alloc> -- /bin/sh -c 'sleep 600' &
T0=$(date +%s)
watch -n 5 'cargo run -q -- instance get user-42 | grep -E "state|idle since|last suspend"'
```

- **Pass:** `instance get user-42` shows `idle since: <T0 + a few seconds>
  (suspends after 1m0s)` while running, then `state: suspended` and `last
  suspend: ... (idle)` no later than `T0 + 90s` (boot time plus `after` plus
  30s). `legion events list --kind instance.suspended` shows `reason: idle`.
  `user-17` stays `active` for the whole exec session, and suspends within
  90s of the session ending.
- **Fail:** no `idle since` line (check `legion node usage` reports usage for
  the node, and that `detail` is not `idle-unmeasured`), or the suspend comes
  later than 90s after the guest went quiet.

### 5.2 Quiet open connection, with and without `connections`

Give the task an ingress chain so the datapath fronts its port (see
docs/network-handlers.md), and have the guest listen on it
(`nc -lk -p 8080`). From another host, open a connection and send nothing:
`nc <node> <port>` and leave it open.

- **Pass:** with the default signals, the instance suspends `after` past the
  last byte despite the open connection. Add `"connections"` to
  `idle.signals`, resubmit, recreate, and repeat: it stays `active` while the
  connection is open and suspends `after` past closing it.

### 5.3 A pressured node suspends opted-in instances before evicting

This is the Phase 5 pressure acceptance check. Run two allocations on one
node: the `agents` instance (opted in, bursting to about 900 MiB of its 512
MiB reservation) and a plain `exec` job bursting past its reservation
(`resources = { memory = 128, memoryMax = 2048 }`, command `stress-ng --vm 1
--vm-bytes 1500M --vm-keep`). Set the worker's `memoryPressure.minFree` a
little under the node's current `MemAvailable`, then push it below with a
host-side `stress-ng --vm 1 --vm-bytes <N>M --vm-keep`.

```bash
cargo run -q -- node usage "$NODE"           # UNDER PRESSURE, IN ORDER: agents row reads "reclaim, then suspend"
cargo run -q -- events list --kind node.pressure
cargo run -q -- instance get user-42
```

- **Pass:** before the pressure, the order preview lists the `agents`
  allocation as `reclaim, then suspend`. Under pressure, `node.pressure`
  events show `reclaim`, then `suspend` for `user-42` (metadata `cause:
  suspended`, a `snapshot_id`), and only after that, if free memory is still
  short, `evict-bursting` for the `exec` job. `instance get user-42` shows
  `suspended`, `last suspend: ... (pressure)`, and the `snapshot_id` as its
  checkpoint. `instance start user-42 --wait` restores it with its
  `/dev/shm/fill` intact.
- **Fail:** any `evict-*` event before the `suspend` event, or the instance
  ends `stopped` rather than `suspended`.

### 5.4 Local suspension survives a lost heartbeat

Repeat 5.3, but block the worker's manager traffic just before the pressure
hits (`iptables -A OUTPUT -p tcp --dport 4747 -j DROP` on the worker), wait
for the `suspend` (worker log: `Suspended for memory pressure`), wait 30s,
then remove the rule.

- **Pass:** within a heartbeat of reconnecting, `instance get user-42` shows
  `suspended (pressure)` with the checkpoint the worker minted, and the
  allocation is not restarted on the node in between.
- **Also:** repeat with the outage longer than the node-lost window and the
  instance stopped meanwhile (`instance stop user-42` while the node is
  unreachable). On reconnect the snapshot goes `deleting`, then disappears,
  and the content store's `snapshots` class no longer holds its parts.

### 5.5 Balloon and free page reporting

With the `agents` instance running (memoryMax above memory, so it has a
balloon):

```bash
# Firecracker
curl -s --unix-socket <alloc dir>/.sandbox/vmm.sock http://localhost/balloon
# cloud-hypervisor
ch-remote --api-socket <alloc dir>/.sandbox/vmm.sock info | jq .config.balloon
cat <alloc dir>/.sandbox/balloon.json
```

- **Pass:** Firecracker reports `amount_mib: 0, deflate_on_oom: true`;
  cloud-hypervisor reports `size: 0, deflate_on_oom: true,
  free_page_reporting: true`; `balloon.json` holds the guest size and the
  reservation. Under pressure (5.3), the `reclaim` event for the sandbox has
  `freed_bytes` > 0, the balloon target grows by at most `guest - reserved`,
  and `memory.current` of the allocation cgroup drops. After the pressure
  clears, the balloon reads 0 again.
- **Free page reporting (cloud-hypervisor):** with a guest kernel that has
  `CONFIG_PAGE_REPORTING=y`, write and delete 300 MiB in the guest's
  `/dev/shm` and watch the allocation cgroup's `memory.current` drop back
  within a few seconds with no pressure at all. Record the result either way.
- **Firecracker version check:** confirm whether the pinned Firecracker
  accepts `"free_page_reporting": true` in the `balloon` config. If it does,
  open a TODO to turn it on there too (it is off today because older
  supported releases reject the field and fail the boot).
- **Restore:** suspend and resume `user-42`, then repeat the `GET /balloon`
  against the restored VMM. The balloon is still there and `balloon.json` was
  rewritten in the new allocation's directory.

### 5.6 A sandbox without a balloon keeps `--no-api`

Run a sandbox task with no `memoryMax` (or `memoryMax = memory`) and no
`suspend` or `snapshots` block on Firecracker.

- **Pass:** `ps -o args -p <vmm pid>` shows `--no-api`, its config has no
  `balloon` key, and its directory has no `balloon.json`.

### Ignored tests

None new. 5.1 through 5.6 are the real-hardware checks. The Phase 3b KVM test
still applies, and now also covers a guest booted with a balloon:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib sandbox_memory_checkpoint_round_trip -- --ignored --nocapture
```

## Phase 6

Fork and create-from-snapshot: `legion instance fork`, `legion instance
create --from-snapshot`, and named snapshots (`legion snapshot alias`). On
macOS the manager side ran end to end against a simulated worker (`cargo test
--test fork_integration`), a disk fork of an `exec` task ran for real with
three forks on real `local-dir` clones
(`an_exec_disk_fork_gives_every_fork_its_own_copy_of_the_volume`), and the
per-fork identity, MAC, and TAP handling ran against fakes
(`three_forks_get_divergent_identities`,
`forks_of_a_tap_wired_checkpoint_get_their_own_taps_and_macs`). Nothing has
restored three real guests from one checkpoint, moved a restored NIC with
Firecracker's `network_overrides`, or created a TAP device for a fork. The
Linux-only `master` and `set_master` TAP calls were never compiled on Linux by
this phase.

### Prereqs

- Everything in [Phase 4 prereqs](#phase-4), including KVM, both VMMs, the
  guest kernel, and a glibc guest image.
- **Firecracker 1.12 or later** for the TAP items (6.3), since forks move the
  restored NIC with `network_overrides`. Record `firecracker --version`.
- A Linux bridge for 6.3: `ip link add br0 type bridge && ip addr add
  10.77.0.1/24 dev br0 && ip link set br0 up`, and its pool in the worker
  config (`sandbox.bridges.br0`, see [Gated sandboxes](#gated-sandboxes)).

Use the Phase 4 jobspec with a `snapshots` block added (so `snapshot create
--memory` works for item 6.4), and change the command so every line shows
what diverges (save as `/tmp/fork.ncl`):

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 1024, cpu = 1000 },
    stateful = {
      mode = "keyed",
      storage = { path = "/workspace", mode = "block", params = { size = "256M" } },
    },
    identities = [{ name = "acme", audience = ["acme-api"], env = "ACME_TOKEN" }],
    snapshots = {},
    suspend = {
      checkpoint = "memory",
      onResume = ["/bin/sh", "-c", "cat /legion/identity/ACME_TOKEN > /workspace/token; hostname > /workspace/host; cat /sys/class/net/eth0/address > /workspace/mac 2>/dev/null; head -c 16 /dev/urandom | od -An -tx1 > /workspace/rand"],
    },
    config = {
      image = "debian:bookworm-slim",
      command = ["/bin/sh", "-c", "echo origin > /workspace/marker; head -c 900M /dev/urandom > /dev/shm/fill; while true; do echo rand=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \\n') host=$(hostname); sleep 1; done"],
      hypervisor = "firecracker",
    },
  }],
}
```

```bash
cargo run -q -- job run /tmp/fork.ncl
cargo run -q -- instance create agents --id run-17
sleep 20
```

### 6.1 Three memory forks: ids, hostnames, RNG, and the origin keeps running

This is the Phase 6 acceptance check (plan §13).

```bash
cargo run -q -- instance fork run-17 --count 3 --wait -j | tee /tmp/fork.json
cargo run -q -- instance list agents
cargo run -q -- job logs agents | grep -E 'rand=' | tail -20
```

- **Pass:** `fork.json` has `instances: ["run-17-f1","run-17-f2","run-17-f3"]`
  in one envelope, `kind: memory`, `checkpoint_state: local`, and a
  `pause_ms` around 0.5 to 1 second per GiB. `instance list` shows the origin
  `active` on its original allocation and three forks `active` on the same
  node, each with its own allocation id. The logs show four streams of
  `rand=` lines with different values and hostnames `legion-sbx-<alloc>`, one
  per allocation, and the origin's stream has no gap longer than `pause_ms`.
- **Fail, the forks stay `pending (forking)`:** the capture never reported.
  Check `legion snapshot get <snapshot> -j` and the worker log.
- **Fail, two forks print the same `rand=` value:** the reseed did not run in
  one of them. Check its console for `legion-guest-resume` lines (Phase 4,
  item 4.3).

Then run the ignored KVM test, which checks the same divergence without a
cluster:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux LEGION_SANDBOX_TEST_IMAGE=debian:bookworm-slim \
  cargo test --lib sandbox_fork_divergence -- --ignored --nocapture
```

- **Pass:** both `sandbox_fork_divergence_firecracker` and
  `sandbox_fork_divergence_cloud_hypervisor` pass.

### 6.2 Identity, volume, and marker divergence

After 6.1, stop each fork and read its volume (as in item 4.3):

```bash
for f in run-17-f1 run-17-f2 run-17-f3; do
  cargo run -q -- instance stop $f
  vol=$(cargo run -q -- instance get $f -j | jq -r '.data.storage_handle' | cut -d/ -f2)
  img=<worker state>/volumes/local-dir/$vol/disk.img
  for file in marker token host mac rand; do echo "$f $file: $(debugfs -R "cat /$file" $img 2>/dev/null)"; done
done
```

Decode each token's payload (`cut -d. -f2 | base64 -d`).

- **Pass:** every fork's `marker` is `origin` (written before the fork). The
  three `token` claims `legion.io/alloc` name three different allocations, and
  `sub` names the fork's own instance. `host`, `mac` (`02:4c:...`), and
  `rand` all differ between forks. The three volume ids differ from each
  other and from the origin's.
- **Then:** start `run-17-f1`, have it write a file (add `touch
  /workspace/only-f1` to a one-off `onResume`, or use a disk fork as in 6.5),
  and confirm the file is missing from `run-17-f2`'s and the origin's
  volumes.

### 6.3 Each fork gets its own TAP on the origin's bridge

Add `network = { mode = "tap", bridge = "br0" }` to the task's `config`
(with the [Gated sandboxes](#gated-sandboxes) pool on the worker), rerun the
job, recreate `run-17`, and fork it twice.

```bash
cargo run -q -- instance fork run-17 --count 2 --wait -j
ip -br link | grep lgf
bridge link show | grep lgf
```

- **Pass:** two new `lgf<12 hex>` TAP devices exist beside the origin's,
  both `master br0`, both up, and both held by a Firecracker process (`ls -l
  /proc/<pid>/fd | grep tun`). The origin's device is still held by its VMM.
  Destroying a fork (`legion instance destroy run-17-f1`) removes its
  `lgf...` device. Each fork's address is G4.1's.
- **Fail, `restore needs TAP device lgf...`:** read the rest of the message.
  `creating it failed` points at `ip tuntap` permissions. `attaching it to
  bridge br0 failed` points at the bridge.
- **Fail, the fork's load fails with an unknown field:** Firecracker is older
  than 1.12. The manager should have refused the fork first: check the
  node's `sandbox.vmm.version` fingerprint.
- Repeat with `hypervisor = "cloud-hypervisor"`. **Pass:** same devices.
  **Fail, the fork comes up on the origin's device or fails to open it:** cloud-hypervisor
  did not take the TAP name from the rewritten `config.json` on restore. Record
  the cloud-hypervisor version, and switch that path to `net_fds` on restore.
- With Firecracker 1.11 on the node, `legion instance fork run-17
  --checkpoint memory` must fail with `cannot move a restored NIC to a new TAP
  device`.

### 6.4 Memory sharing between forks

With 6.1's three forks running, compare their resident memory to what three
cold guests would use:

```bash
for pid in $(pgrep -f 'firecracker --id legion-sbx'); do
  grep -E '^(Rss|Pss|Shared_Clean)' /proc/$pid/smaps_rollup | tr '\n' ' '; echo
done
ls -li <worker state>/allocs/*/.sandbox/memory
```

- **Pass:** the `memory` files in the three forks' scratch dirs share one
  inode number (hard links into the content store). Each fork's `Pss` is well
  below its `Rss` right after the fork, and `Shared_Clean` is most of the
  guest. Record the numbers.
- **Expected for cloud-hypervisor:** no sharing (it copies memory on
  restore). Record it anyway.

### 6.5 Disk fork of a `container` task

With a `container` task (alpine, a `stateful.storage` volume on `local-dir`,
a command that writes `/data/marker` once and then sleeps), run `legion
instance fork <id> --count 2 --wait -j`.

- **Pass:** `kind: disk`, `pause_ms: 0`, and two fresh containers that see
  the marker on their own volumes. A file written in one fork's volume is not
  in the other's. On btrfs or XFS, `filefrag -v` on a large file in a fork's
  volume shows shared extents with the snapshot (reflink).

### 6.6 Create from a named snapshot

```bash
cargo run -q -- snapshot create run-17 --memory --name agent-golden --wait
cargo run -q -- instance create agents --from-snapshot agent-golden --count 5 -j
cargo run -q -- snapshot create run-17 --memory --wait -j | jq -r .data.snapshot.snapshot_id   # NEW
cargo run -q -- snapshot alias set agents agent-golden <NEW>
cargo run -q -- instance create agents --from-snapshot agent-golden -j
```

- **Pass:** the first create returns five ids with `restoringFrom` the golden
  snapshot, and all five run on its node with distinct hostnames and RNG
  output. The alias set prints `Moved agent-golden from ... to <NEW>`, the
  last create's `restoringFrom` is `<NEW>`, and the first snapshot is still
  `local`.

### 6.7 Spread forks across nodes (integration)

Needs the Phase 7b prereqs too: two KVM workers A and B with matching CPUs,
both with a `contentStore` on MinIO, and a third C for the waiting case. Use
`/tmp/fork.ncl` without `stateful.storage` (a spread fork carries no volume)
and with `network = { mode = "tap", bridge = "br0" }`, where every node has a
bridge `br0` and a `sandbox.bridges.br0` pool.

```bash
cargo run -q -- instance fork run-17 --count 3 --spread --wait -j | tee /tmp/spread.json
for f in run-17-f1 run-17-f2 run-17-f3; do cargo run -q -- instance get $f -j | jq -r '.data.node'; done
bridge link show | grep lgf    # on each node
```

- **Pass:** `durability: store` and `checkpoint_state: published` in the
  envelope. Without C, two forks run on two different nodes (one may share
  the origin's node), and the third stays `pending` with the scheduler log
  line `Spread fork waits: every eligible node runs a sibling`. Add C and it
  places there. Each fork's `lgf...` device is `master br0` on its node, and
  each fork prints its own `rand=` stream. After every fork restored,
  `snapshot get <checkpoint>` shows `deleting`, then the row is gone, and
  the MinIO objects are still there.
- **Also:** with `contentStore` removed from A's worker, `--spread` fails
  with `FailedPrecondition` naming `contentStore`. With `stateful.storage`
  back in the spec it fails naming the volume.
- **Fail:** two forks on one node, a fork placed before the publish lands,
  or a fork on B that comes up with no network and no error.

### 6.8 TAP forks that boot cold (integration)

On one node with `run-17` running on the TAP spec from 6.3:

- `legion instance fork run-17 --checkpoint disk --wait`. **Pass:** accepted
  (gated plan Phase 4 removed the disk-fork refusal), and each fork boots on
  its own `lgf...` device with its own address.
- Force a memory fork's restore to fail (delete the checkpoint's `memory`
  part from the node content store before the fork restores), with
  `onRestoreFailure = "cold"`. **Pass:** the fork boots cold on its own
  `lgf...` device, never the origin's.
- On a node B whose worker declares no `sandbox.bridges.br0`, a spread fork
  is never placed there (the implicit `sandbox.bridges contains "br0"`
  constraint).

### Ignored tests

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib sandbox_fork_divergence -- --ignored --nocapture
```

The Phase 3b and 4 KVM tests still apply.

## Phase 7a

Content locality is platform-independent and fully unit and integration
tested on macOS. What was not run is a real S3 upload: the `s3` publisher's
HEAD, multipart upload, and URL round trip were only exercised with the
`file` backend and URL-building tests. Any host with Docker or a MinIO
binary works, Linux or not.

### 7a.1 Publish to S3 (MinIO) and fetch it back by digest

Prereqs: MinIO on `localhost:9000` with a bucket `legion-content`, and an
AWS credential object in KV:

```bash
cargo run -q -- kv put legion/content-store \
  '{"type":"aws","accessKeyId":"minioadmin","secretAccessKey":"minioadmin","region":"us-east-1","endpoint":"http://localhost:9000"}'
```

Worker config:

```nickel
contentStore = {
    url = "s3://legion-content/test?endpoint=http://localhost:9000",
    auth = { kv = "legion/content-store" },
},
```

There is no CLI that publishes in 7a, so drive `ContentPublishing::publish`
from a scratch test against the running worker's state dir, or wait for 7b's
`durability = "store"`. Then:

```bash
mc ls local/legion-content/test/sha256/          # one object named by the hex digest
mc stat local/legion-content/test/sha256/<hex>   # size matches the blob
```

Publish the same digest again: nothing new is uploaded (MinIO's request log
shows a HEAD and no PUT). Fetch it from a second worker with an artifact
`{ source = "<published url>", checksum = "sha256:<hex>" }`.

Pass: one object, a HEAD-only second publish, and the second worker stages
the artifact with a verified checksum. Fail: a PUT on the second publish, or
a checksum mismatch on fetch.

### 7a.2 Ambient credentials on EC2

Prereqs: an EC2 worker whose instance profile can `s3:PutObject` and
`s3:GetObject` on a bucket, and `contentStore = { url = "s3://<bucket>/" }`
with no `auth`. Repeat 7a.1's publish.

Pass: the upload succeeds with no keys in config or KV. Fail: a credentials
error naming the instance metadata endpoint.

## Phase 7b

Durable checkpoints and cross-node resume: `suspend.durability = "store"`,
`snapshot create --publish`, the `publish` and `warm` checkpoint ops,
cross-node restore fetch, per-candidate `compat`, and `snapshot warm`. On
macOS the manager side ran end to end against simulated workers (`cargo test
--test published_resume_integration`), and the worker transport ran for real
with the `file` publisher between two worker task managers
(`a_published_snapshot_restores_on_a_node_that_never_held_it`,
`warm_fetches_published_parts_into_the_store_once`). That test carries its
part on a disk-kind manifest, because the `exec` runtime cannot load guest
memory. What has never run: an S3 (or MinIO) publish of a real checkpoint, a
memory restore of a guest on a node that did not capture it, and either on
two real machines.

### Prereqs

- Two Linux workers, **A** and **B**, each with everything in
  [Phase 3c prereqs](#phase-3c) (KVM, the same Firecracker version, the same
  guest kernel). A and B must have the same CPU vendor, model, and feature
  digest. Check with `legion node info <id> -j | jq '.data.fingerprint_attrs |
  {"cpu.vendor","cpu.model","cpu.features","sandbox.vmm.version"}'`. A third
  worker **C** with a different `cpu.model` is needed for 7b.4 only.
- MinIO reachable from both workers, a bucket `legion-content`, and the KV
  credential from [7a.1](#7a1-publish-to-s3-minio-and-fetch-it-back-by-digest).
  Turn on the MinIO request log (`mc admin trace -v local`) for 7b.2.
- Worker config on **both** A and B (B needs it only to fetch, through its
  ordinary `s3://` fetcher, but giving it a `contentStore` too is harmless):

  ```nickel
  contentStore = {
      url = "s3://legion-content/test?endpoint=http://<minio-host>:9000",
      auth = { kv = "legion/content-store" },
  },
  ```

  Check registration: `legion node info A -j | jq '.data.dynamic_attrs.capabilities'`
  lists `checkpoint`, `checkpoint-fetch`, and `content-publish`. B lists
  `checkpoint-fetch` too (added at the Phase 6 and 7b integration).
- Add `region = "lab"` to A's and B's worker `attributes` block for
  7b.5.

Jobspec (save as `/tmp/durable.ncl`). No `stateful.storage`: everything the
guest has is in its checkpoint, so it can move:

```nickel
{
  name = "durable",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 1024, cpu = 1000 },
    stateful = { mode = "keyed" },
    suspend = { checkpoint = "memory", durability = "store" },
    config = {
      image = "debian:bookworm-slim",
      command = ["/bin/sh", "-c", "head -c 900M /dev/urandom > /dev/shm/fill; i=0; while true; do i=$((i+1)); echo counter=$i; sleep 0.2; done"],
      hypervisor = "firecracker",
    },
  }],
}
```

```bash
cargo run -q -- job run /tmp/durable.ncl
cargo run -q -- instance create durable --id user-7
cargo run -q -- instance get user-7 -j | jq -r .data.node   # note it: call it A
```

### 7b.1 Submit refuses a node-local volume with store durability

Add `storage = { path = "/workspace", mode = "block" }` to the `stateful`
block and run `cargo run -q -- job validate /tmp/durable.ncl`.

Pass: exit non-zero with `sets suspend.durability = "store", but its
stateful.storage uses volume driver `local-dir`` and a `Hint:` naming `nfs`.
`job run` and `job plan` say the same. With `driver = "nfs"` (and an NFS
server), validation passes. Fail: the spec is accepted.

### 7b.2 Suspend publishes to MinIO

```bash
cargo run -q -- instance suspend user-7 --wait -j | jq .data
cargo run -q -- snapshot list --instance user-7
sleep 30   # one heartbeat for the publish request, then the upload
cargo run -q -- snapshot get <snap-id> -j | jq '.data | {state, durability, published_at}'
cargo run -q -- snapshot get <snap-id> -j | jq '.data.manifest.memory[] | {role, size, url}'
mc ls local/legion-content/test/sha256/
cargo run -q -- events list --kind snapshot.published -j | jq '.data[-1].metadata'
curl -s localhost:4747/metrics | grep legion_checkpoint_publish
```

Pass: the suspend returns `durability: "store"` and `state: suspended` right
after the capture (before any upload), the snapshot moves `local` ->
`published`, every memory part has an `s3://legion-content/test/sha256/<hex>`
URL, the objects exist with matching sizes, `snapshot.published` carries
`uploadedBytes` close to the checkpoint size, and
`legion_checkpoint_publish_total{outcome="published"}` is 1. Record
`publishMs` and the link speed. Suspend and resume the same guest a second
time without touching its memory: the trace shows HEADs and fewer PUTs.
Fail: the suspend waits for the upload, a part has no URL, or the state stays
`local` with no error.

### 7b.3 Resume on B after A is destroyed (the acceptance check)

Plan §13 Phase 7: "a `durability = "store"` instance resumes on another node
after its original node is destroyed."

```bash
# on A: stop the worker and destroy the host (or at least wipe its state dir)
sudo systemctl stop legion-worker && sudo rm -rf /var/lib/legion
# wait for the manager to mark A lost
cargo run -q -- node list
cargo run -q -- instance start user-7 --wait -j | jq .data
cargo run -q -- instance get user-7 -j | jq -r .data.node   # B
cargo run -q -- job logs durable | tail -3                  # counter=N continues
```

Pass: the start reply has no `node_id` and says `on any compatible node`,
the instance lands on B and is `active`, B's worker log shows `Fetched
published snapshot parts for restore` with the bytes and time, and the
counter continues from where it was suspended (it does not restart at 1).
`instance get` shows the last restore time: record it, and the fetch time
separately (worker log). Fail: the resume waits for A, a `snapshot.restore_failed`
event, or the counter restarts (a cold boot).

### 7b.4 A published resume skips an incompatible node

With A gone and only C (different `cpu.model`) available, suspend and
publish again on B, stop B's worker, and `instance start user-7`.

Pass: the instance stays `pending (resuming)` and the eval's failure names
C with `cpu.model is ..., the checkpoint needs ...`. Restart B's worker:
the resume lands on B. Fail: the instance is placed on C, or C's restore is
attempted at all.

### 7b.5 Warm, then resume from local bytes

With A and B both up and a published checkpoint captured on A:

```bash
cargo run -q -- snapshot warm <snap-id> --constraint region=lab
cargo run -q -- events list --kind snapshot.warmed -j | jq '.data[] | {node: .node_id, meta: .metadata}'
```

Pass: the reply lists B under `Fetching on` and A under `Already warm`,
`snapshot.warmed` arrives for B with `fetchedBytes` equal to the checkpoint,
and a second `snapshot warm` lists both as already warm. Drain A
(`legion node drain A`), resume: B's worker log shows no fetch, and the
restore time matches a local resume. Fail: a second fetch on B, or the
resume waits on a download.

### 7b.6 A worker without a contentStore

Remove `contentStore` from A's worker config and restart it. Run `instance
suspend user-7`.

Pass: `FailedPrecondition` naming A and `contentStore`, and the instance is
still `active`. Fail: the suspend runs.

### 7b.7 A failed publish is retried, then given up

Point A's `contentStore.auth` at a KV path with wrong credentials, restart A,
and suspend `user-7`.

```bash
cargo run -q -- instance suspend user-7 --wait
watch -n 10 'cargo run -q -- snapshot get <checkpoint> -j | jq -r .data.snapshot.error'
cargo run -q -- events list --kind snapshot.publish_failed
```

Pass: the error reads `publish attempt 1 of 3 failed, retrying in 60s: ...`,
then about a minute later `publish attempt 2 of 3 failed, retrying in 300s`,
then about five minutes later `publish failed after 3 attempts: ...`. Three
`snapshot.publish_failed` events, and the MinIO trace shows three rounds of
upload attempts. Repeat, fixing the credentials after the first failure:
the snapshot is `published` after the next attempt. Fail: a re-send before
the minute is up, a fourth attempt, or the snapshot leaving `local` on a
failure.

### 7b.8 An older worker is never sent a published restore

Run a worker built before the integration (it advertises `checkpoint` but not
`checkpoint-fetch`) as the only node besides A, publish `user-7`'s checkpoint,
drain A, and start `user-7`.

Pass: the placement fails naming the old node and `checkpoint-fetch`, and the
old worker never logs a restore. Fail: the old worker is sent the restore.

### Ignored tests

None new. The macOS-runnable tests above cover the transport. The KVM tests
from Phase 3b still cover a memory restore on one node:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib sandbox_memory_checkpoint_round_trip -- --ignored --nocapture
```

## Track 2

Resume guards: `suspend.reserve` as a ratio or fixed amounts, make-room suspends for a resume
pinned to a full node, and the `snapshots` content budget as a placement
constraint (docs/suspend-and-resume.md, "Capacity for resumes"). Nothing here
is Linux-only code: the hold math, make-room, and the budget filter all run on
the manager, and the worker's budget report reads its content store on any
OS. On macOS they ran end to end with the heartbeat standing in for the worker
(`cargo test --test suspend_integration`). What has never run is the whole
loop with real sandbox memory captures, whose timing (a capture takes about a
second) is what make-room's "wait for the in-flight suspend" rule depends on,
and a budget filled by real checkpoint files.

### Prereqs

- Everything in [Phase 4 prereqs](#phase-4), on **one** worker, with
  `resourceEnforcement.enabled = true` so it sends usage reports (make-room
  reads idle-since from them).
- Make the node small so three 1 GiB sandboxes fill it. Set the worker's
  `reservedResources.memory` to physical memory minus 3 GiB (in MiB), and
  shrink the checkpoint budget so three checkpoints fill it:

  ```nickel
  reservedResources = { memory = <physical MiB - 3072> },
  content = { budgets = { snapshots = "3GiB" } },
  ```

- Use the Phase 4 jobspec (`/tmp/agents.ncl`) with this `suspend` block:

  ```nickel
  suspend = {
    checkpoint = "memory",
    idle = { after = "30m" },   # long, so the idle sweep stays out of the way
    onPressure = true,
  },
  ```

### T2.1 A partial hold, end to end

Set `reserve = 0.25` in the jobspec, `job run` it, create `user-1`, and
suspend it.

```bash
cargo run -q -- instance create agents --id user-1
cargo run -q -- instance suspend user-1 --wait
cargo run -q -- instance get user-1
cargo run -q -- node usage <node>
cargo run -q -- instance start user-1 --wait
cargo run -q -- node usage <node>
```

Pass: `instance get` prints `held: 256 MiB and 250 MHz reserved on <node> for
the resume`. `node usage` shows `suspended: 1 instance, 256 MiB held and 250
MHz` and a row `user-1 (suspended, held)` reserving 256. After the start, the
row is gone, `RESERVED` is back to 1024 for `user-1`, and the guest kept its
processes (Phase 4.1). Fail: the held row reserves 1024, or remains after the
resume.

### T2.2 Make-room with real captures

Back to `reserve = false`. Create `user-1`, `user-2`, `user-3` (the node is
now full) and wait two minutes, so all three are past make-room's age rule.
Suspend `user-1`, then create `user-4` into its room. Leave `user-2` idle and
run a busy loop in `user-3`
(`cargo run -q -- alloc exec <alloc> -- sh -c 'while :; do :; done' &`).
Then resume `user-1`:

```bash
time cargo run -q -- instance start user-1 --wait --timeout 60
cargo run -q -- events list --kind instance.make_room
cargo run -q -- instance get user-2
curl -s localhost:4747/metrics | grep legion_make_room_suspends_total
```

Pass: within about 5 seconds of the start, `instance.make_room` names
`user-1` and `user-2` only. `user-2` is the quietest (`user-3` is eligible
through `onPressure` but busy, so it sorts after, and `user-4` became active
under two minutes ago). `user-2` is `suspended` with reason `make_room`, and
`user-1` restores within `resumeTimeout` (30s) with its memory intact. The
counter shows `outcome="started"` 1. Fail: `user-3` or `user-4` is
suspended, more than one round runs, or `user-1` boots cold.

Then resume `user-2` right away. Pass: `user-1` is never chosen (it resumed
under two minutes ago), so the round suspends `user-3` or `user-4` (whichever
is past two minutes and quieter), or none with `outcome="no_room"` if neither
is. Fail: `user-1` is suspended again (thrash).

### T2.3 A full checkpoint budget

With the 3 GiB budget, suspend three running 1 GiB sandboxes so their
checkpoints are pinned, then create one more instance.

```bash
cargo run -q -- node usage <node>
cargo run -q -- instance create agents --id user-9
cargo run -q -- job info agents
```

Pass: `node usage` shows `checkpoints: 3 GiB suspended here, snapshots budget
3 GiB pinned of 3 GiB (0 MiB free, FULL: no new memory-checkpoint instances
placed here)`, and `user-9` stays `pending` with the placement error `every
one's snapshots content budget is full: less than 1024 MiB free`. Resume one
of the three: its checkpoint is reclaimed, and the next evaluation of the job
places `user-9` (re-run `job run /tmp/agents.ncl` to trigger one, since a
capacity failure waits for the next evaluation like any other). Fail:
`user-9` places while the budget is full, and its first suspend fails with a
content-store budget error.

### Ignored tests

None new.

## Track 1

The agent-sandbox use case: per-instance TAPs, idle detection from a guest's TAP,
sandbox identity refresh through the identity disk, and the payload fix
that carries `suspend` and `snapshots` to the worker. On macOS the TAP
creation, counters, and identity disk rewrite ran against fakes and fixture
trees, and the payload fix and Apple-container idle loop ran end to end
(`cargo test --test apple_container_idle -- --ignored`). Nothing created a
real TAP on a bridge, read real `/sys/class/net` counters, or rewrote a
running VMM's identity disk.

### Prereqs

- Everything in [Phase 4 prereqs](#phase-4): KVM, Firecracker and
  cloud-hypervisor, and a guest kernel with `CONFIG_IP_PNP=y`. Any image
  works: Legion addresses the guest (gated plan Phase 4).
- A bridge, and its pool in the worker config:

  ```bash
  ip link add br0 type bridge && ip addr add 10.77.0.1/24 dev br0 && ip link set br0 up
  ```

  ```nickel
  sandbox = { bridges = { br0 = { subnet = "10.77.0.0/24", gateway = "10.77.0.1", range = "10.77.0.128/25" } } },
  ```

- A worker run as root (`CAP_NET_ADMIN`, `ip` on `PATH`) with
  `resourceEnforcement.enabled = true` and the content store on.
- A manager with `workloadIdentity.enabled = true` for T1.4.

Save as `/tmp/t1.ncl`:

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 512, memoryMax = 1024, cpu = 500 },
    stateful = { mode = "keyed", requireId = true },
    config = {
      image = "<your glibc image>",
      command = ["/bin/sh", "-c", "while true; do date; cat /legion/identity/platform 2>/dev/null | cut -c1-24; sleep 30; done"],
      network = { mode = "tap", bridge = "br0" },
    },
    suspend = {
      checkpoint = "memory",
      idle = { after = "1m", signals = ["network", "cpu"] },
    },
    identities = [{
      name = "platform",
      audience = ["api.example"],
      ttl = "2m",
      file = { path = "/legion/identity/platform" },
    }],
  }],
}
```

### T1.1 Many TAP sandboxes on one node

```bash
legion job run /tmp/t1.ncl
for i in 1 2 3 4 5; do legion instance create agents --id user-$i; done
ip -br link show master br0
```

Pass: five `lgf...` devices, all `UP`, all with `master br0`, five MACs
starting `02:4c:`, and five distinct `address:` lines in `legion instance
get user-$i`, each from `10.77.0.128/25` and each answering `ping` from the
host. Each allocation dir holds `.sandbox/tap-device` naming its device,
`.sandbox/restored-tap`, and `.sandbox/guest-addr.json` naming its address.
Fail: a boot error naming `sandbox needs TAP device lgf...`, or a guest with
no address (check the console for the kernel's `IP-Config` line).

### T1.2 Resume recreates the TAP on the bridge

```bash
legion instance suspend user-1 --wait
ip link show "$(cat <old alloc dir>/.sandbox/tap-device)"   # gone: destroy deleted it
legion instance start user-1 --wait
ip -br link show master br0 | grep lgf
```

Pass: the same `lgf` name is back, on `br0`, and the guest keeps its
address (`legion job logs` keeps printing). Fail: `restore needs TAP device
lgf...`, or the device without `master br0`.

### T1.3 Idle sees TAP traffic

Let `user-2` sit idle, then from the host `ping -c 200 -i 0.2 <user-2's IP>`.

```bash
legion instance get user-2   # idle since resets during the ping
cat /sys/class/net/<user-2's lgf>/statistics/rx_bytes   # rises while it replies
```

Pass: `idle since` moves forward while the guest replies, and the instance
suspends about a minute after the ping stops with `suspend_reason idle`.
Fail: it suspends during the ping, or never suspends while idle (broadcast
noise on `br0` should not keep it awake: check `rx_bytes` holds still).

### T1.4 A token refresh rewrites the guest file with no reboot

With `ttl = "2m"`, the worker refreshes at about one minute.

```bash
legion job logs agents --follow   # the token prefix changes each minute; `date` lines never restart
```

Pass: the printed token prefix changes about every minute, the guest's
uptime keeps growing (`legion job logs` shows no new boot), and the console
shows `legion-guest-resume: refresh: wrote identity tokens`. Fail: the
prefix never changes, or the guest reboots.

Then set `changeMode = "restart"` on the identity and run `legion job
validate /tmp/t1.ncl`: it prints the `reboots this sandbox's guest`
warning.

### T1.5 Forks of a TAP sandbox

```bash
legion instance fork user-3 --count 2 --wait
ip -br link show master br0 | grep lgf
```

Pass: two more `lgf` devices on `br0`, named for `user-3-f1` and
`user-3-f2`, and each fork answers on its own address (G4.1). Fail:
`restore needs TAP device lgf...`.

### Ignored tests

```bash
cargo test --test apple_container_idle -- --ignored   # macOS only
```

The Linux paths above have no ignored tests of their own: the TAP calls are
the Phase 3c/6 `HostTaps` calls, already covered by
`cargo test -- --ignored sandbox` on a KVM host.

## Gated sandboxes

The [gated sandbox checkpoints plan](plans/gated-sandbox-checkpoints-plan.md)
(Phases 0 to 4). Built on a Linux host **without KVM**: the vsock retarget,
the guest agent's relay drop, the datapath's attribution, the idle signals,
the gated default, and TAP addressing ran against fakes, fixture trees, Unix
sockets standing in for vsock, and (for the guest's netlink re-addressing) a
veth pair in a throwaway network namespace. No guest booted. Phase 0 is the
gate: until G0.1 passes on both VMMs, `SANDBOX_GATED_BY_DEFAULT` in
`src/structs/network.rs` stays `false`, and nothing here is claimed as
boot-tested.

Status: **pending** (no KVM host has run any item below). Record each run
with the date, `uname -r`, `firecracker --version`, `cloud-hypervisor
--version`, and the guest kernel's config.

### Prereqs

- x86_64 or aarch64 with `/dev/kvm`, and the worker run as root.
- **Firecracker 1.16.1 or later** (the `vsock_override` floor), and
  **cloud-hypervisor v52 or later**, plus one older cloud-hypervisor for the
  half-open check in G1.2. One Firecracker 1.15 binary for G1.5.
- A guest kernel with `CONFIG_VIRTIO_VSOCKETS=y` (gated) and
  `CONFIG_IP_PNP=y` (TAP addressing), at `/var/lib/legion/kernels/vmlinux` or
  `LEGION_SANDBOX_KERNEL`.
- The static guest binary beside the worker:

  ```bash
  rustup target add x86_64-unknown-linux-musl
  cargo build -p legion-guest --profile dist --target x86_64-unknown-linux-musl
  cp target/x86_64-unknown-linux-musl/dist/legion-guest "$(dirname "$(command -v legion)")/"
  # or, for the ignored tests:
  export LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest
  ```

- Outbound HTTP from the host (the datapath dials from the host), and the
  hosts the items name (`example.com`, `example.org`, `pypi.org`).
- For G4.1, a bridge with NAT and a pool:

  ```bash
  ip link add br0 type bridge && ip addr add 10.88.0.1/16 dev br0 && ip link set br0 up
  iptables -t nat -A POSTROUTING -s 10.88.0.0/16 ! -o br0 -j MASQUERADE
  ```

  ```nickel
  # worker config
  sandbox = { bridges = { br0 = { subnet = "10.88.0.0/16", gateway = "10.88.0.1", range = "10.88.128.0/17" } } },
  ```

- A local cluster on isolated ports (never 4747 or 4848), for example a
  manager on `127.0.0.1:5947` and a worker joined to it, with
  `resourceEnforcement.enabled = true` for G3.1.
- To exercise the Phase 2 default before the flip, start the manager with
  `LEGION_SANDBOX_GATED_DEFAULT=1` (verification only, not a supported
  setting).

### G0.1 Boot

```bash
LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux \
LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest \
cargo test --lib sandbox_gated_boots -- --ignored --nocapture --test-threads 1
```

The test binds the datapath (egress proxy and resolver) on the guest's vsock
host sockets with a policy allowing only `example.com` (override with
`LEGION_GATED_TEST_ALLOW` and `LEGION_GATED_TEST_DENY`), boots an
`alpine:latest` guest, and checks from inside it.

- **Pass:** both `sandbox_gated_boots_firecracker` and
  `sandbox_gated_boots_cloud_hypervisor` pass. The console shows `guest
  agent started`, `ALLOWED-OK` (wget through `HTTP_PROXY`), `DENIED-OK` (a
  `403` with `Legion-Denied-By`), `DNS-OK` (`nslookup` through the relay),
  and `RAW-BLOCKED` (`nc 1.1.1.1 443` with no proxy fails).
- **Fail, `guest agent missing`:** the guest binary did not reach the root
  disk, or does not run on the image (check `LEGION_GUEST_BINARY`).
- **Fail, the fetch hangs:** the VMM did not forward guest-initiated vsock
  to `<base>_1080`. Record the VMM version and the socket names under the
  allocation's `.sandbox/`.
- **Also record (spikes):** two gated guests on one node both use CID 3 with
  no conflict; the `sun_path` length of the default state path
  (`/var/lib/legion/allocs/<id>/.sandbox/vsock.sock_1080`) stays under 108.

### G0.2 Throughput

From inside a gated guest, and from a TAP guest on `br0`:

```bash
time wget -O /dev/null http://<a host serving a 1 GiB file>/1g.bin   # through HTTP_PROXY when gated
```

- **Pass:** a recorded number for each VMM, not a threshold. Record
  single-stream MB/s and the connect latency of `wget -O /dev/null
  http://example.com/` for both paths, so the public doc can say how much
  slower the proxy path is.

### G1.1 Resume keeps a RAM-only counter and its network

Jobspec: a keyed gated sandbox with `suspend.checkpoint = "memory"`,
`network = { mode = "gated", egress = { allow = ["example.com"] } }`, and a
command that counts in a shell variable and fetches `http://example.com/`
every 5 seconds, printing `counter=N fetch=<status>`.

```bash
legion job run /tmp/gated.ncl && legion instance create agents --id g-1 --wait
legion instance suspend g-1 --wait && legion instance start g-1 --wait
legion job logs agents --follow
legion net doctor -j | jq '.. | objects | select(.allocationId?)'
```

- **Pass:** the counter continues from where it stopped, the first fetch
  after the resume succeeds, and the access log and `net doctor` attribute
  it to the new allocation id.

### G1.2 Open connections reset, never hang

Hold a slow download through the proxy (`wget --limit-rate=10k` of a large
file), then take a live `legion snapshot create g-1 --memory --wait`.

- **Pass on Firecracker:** the snapshot prints the `resets its open network
  connections` note, the origin's download fails promptly, and a retry
  succeeds.
- **Pass on each cloud-hypervisor version:** the origin's download
  continues. Restore that snapshot (`legion instance create agents --id g-2
  --from-snapshot <id>`): the restored guest's client fails promptly (the
  agent closes its relays), never hangs. Record the version where the VMM
  starts resetting on its own (v52).

### G1.3 Policy at resume

Suspend `g-1`, apply a `require` policy denying `example.com/**`, resume.

- **Pass:** the first post-resume fetch is denied (`403`,
  `Legion-Denied-By: <policy>`), and the `net.denied` event names the new
  allocation.

### G1.4 Three forks

```bash
legion instance fork g-1 --count 3 --wait -j
legion instance fork g-1 --count 1 --spread --wait -j   # second node, published checkpoint
legion instance stop g-1
```

- **Pass:** each fork's fetches are attributed to its own allocation and
  instance, and stopping the origin does not affect them.

### G1.5 Version floor

On a node whose Firecracker is 1.15:

```bash
legion instance suspend g-1 --checkpoint memory
```

- **Pass:** refused before capture with `node <n> runs Firecracker 1.15.0,
  which cannot move a restored guest's vsock ... Hint: upgrade Firecracker on
  the node, or use --checkpoint disk`, and the guest keeps running.

### G2.1 Default

With the manager started with `LEGION_SANDBOX_GATED_DEFAULT=1`, submit a
sandbox with no network config whose command is `pip install requests`
(image `python:3.12-slim`).

- **Pass:** `legion net doctor` shows `mode gated`, the install fails with a
  `gated-default` denial for `pypi.org`, and succeeds after adding
  `network.egress.allow = ["pypi.org", "files.pythonhosted.org"]`. Repeat
  with `alpine:latest` (`apk add curl` with
  `dl-cdn.alpinelinux.org` allowed): the static agent runs on musl.
- When this and G0.1 pass, flip `SANDBOX_GATED_BY_DEFAULT` to `true` and
  update the plan's status line and TODO.md.

### G3.1 Model-call wake

`idle = { after = "1m", signals = ["connections", "cpu"] }` on a gated
sandbox. From the guest, hold a connection through the proxy that sends
nothing for two minutes (`nc` to an allowed host's port 80 with no input),
then close it.

- **Pass:** no suspend while it is open (`instance get` shows idle since
  holding still), then a suspend about a minute after it closes, with
  `suspend_reason idle`.

### G4.1 Re-addressing

A keyed TAP sandbox on `network = { mode = "tap", bridge = "br0" }` with the
pool above and `suspend.checkpoint = "memory"`:

```bash
legion instance create agents --id t-1 --wait
legion instance fork t-1 --count 3 --wait
for i in t-1 t-1-f1 t-1-f2 t-1-f3; do legion instance get $i -j | jq -r '.data.instance.guestAddress'; done
```

- **Pass:** four distinct addresses from `10.88.128.0/17`, each matching
  `instance get`'s `address:` line and the guest's `LEGION_GUEST_ADDR`, and
  each answering `ping` from the host. Suspend and resume `t-1` on the same
  node: it gets its old address back.
- **Repeat** with an image whose own init sets a static IP on `eth0` (or
  `kernel_args = "ip=10.88.200.5::10.88.0.1:255.255.0.0::eth0:off"` after
  Legion's argument, which the kernel honors last): after a fork, Legion's
  lease wins.
- **Also:** `legion job validate` refuses `network = { mode = "tap", tap =
  "tap0" }` with `config.network.tap is gone` and the bridge hint, and a
  pool with every address leased fails the next start with `bridge br0's
  sandbox range ... is full`.

### Ignored tests

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux LEGION_GUEST_BINARY=/path/to/legion-guest \
  cargo test --lib sandbox_gated_boots -- --ignored --nocapture --test-threads 1
```

Run without KVM, `cargo test -p legion-guest` runs the netlink
re-addressing test in a throwaway network namespace when it is root and has
the `ip` tool (it skips otherwise).
