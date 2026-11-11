# Judgement calls: sandbox clean stop

Calls made fixing the P0 data-loss bug from the 2026-10-12 AWS sandbox spot
check: writes to a microVM's block volume were lost across `legion instance
stop` + `start`. The worker SIGKILLed the VMM 1 ms after the stop, with the
guest's page cache (up to ~30s of writes) still in it, and unmounted the
volume 20 ms later. A bug fix, so there is no plan. The public behavior is in
[task-runtime-sandbox.md](../../docs/task-runtime-sandbox.md#stopping-a-sandbox).

**Entries are ranked by project impact, highest first. An entry's number is
its rank.**

## 1. A microVM is stopped from inside: the guest agent signals init, and the sandbox now honors `stop { signal, killAfter }`

**The call.** `SandboxTaskPlugin::stop_with` runs `sh -c 'kill -s "$1" 1'
legion-stop <SIG>` in the guest through the agent's exec listener (vsock port
1100), with the task's `stop.signal`. Init traps every `STOP_SIGNALS` name,
runs `sync`, forwards the signal to the command, waits for it, unmounts the
volumes, and powers off. `stop_with` returns `Ok(true)`, so the worker's
existing terminating set holds the allocation and its volumes until the VMM
exits or `killAfter` passes. This reverses the "no graceful stop for
sandboxes" call in `docs/graceful-stop.md`.

```nickel
stop = { signal = "SIGTERM", killAfter = "60s" },   # now meaningful on a microVM
```

**Alternatives.** (a) The VMM's own shutdown request: cloud-hypervisor's
`PUT /api/v1/vm.power-button`, Firecracker's `SendCtrlAltDel`. This is what
the bug report suggested, and what a VMM-centric design would reach for.
(b) Only flush (`sync` through the agent) and keep the immediate kill.
(c) Signal the VMM process and hope it drains.

**Trade-off.** It depends on the guest agent. A guest without one (an image
the fallback agent cannot run on, a guest restored from a pre-exec
checkpoint) gets no clean stop at all, and loses its unflushed writes. Stops
now take up to `killAfter` for sandboxes too, which is the same latency
every other runtime already pays.

**Why (simplicity).** The power button generates an ACPI event that does
nothing without a userspace handler (acpid, logind), and our init is a shell
script. Ctrl-Alt-Del needs Firecracker's API socket, which a sandbox
launched without checkpoints or a balloon boots without (`--no-api`). The
agent runs in every microVM on both VMMs, so one path covers both. (b) would
have fixed the reported loss but still killed the workload mid-write, and a
sandbox is usually a database or a dev workspace. Honoring the existing
`stop` block means no new knob. (c) does nothing: SIGTERM kills a VMM.

## 2. Every hard stop flushes the guest first, bounded at 10s under the task-manager lock

**The call.** `SandboxTaskPlugin::stop` runs `sync` in the guest (up to
`GUEST_FLUSH_TIMEOUT`, 10s) before it kills a VMM that is still running.
That covers the `killAfter` deadline, an in-place restart, a disk suspend
(which calls `stop` and then snapshots the volume), and a destroy. A memory
checkpoint runs the same flush before it pauses the guest.

**Alternatives.** (a) Run the full clean shutdown inside `stop`, waiting for
the VMM to exit. (b) No flush on these paths, since the graceful path already
covers the common stop.

**Trade-off.** `stop` runs under the worker's task-manager mutex, which the
heartbeat needs. A wedged guest can hold it for up to 10s per stop, plus 3s
for a failed stop request. A sync is not a clean unmount, so the next boot
replays the ext4 journal. Accepted: a 10s stall is far shorter than the
node-lost threshold, and journal replay is what ext4 is for. A memory
capture now pays one `sync` before every pause, including live snapshots and
forks.

**Why (simplicity).** (a) would hold the lock for the whole grace window,
which `TaskPlugin::stop_with` forbids for exactly this reason. (b) leaves the
disk suspend path with the same data loss as the original bug, because it
never goes through `stop_with`. A bounded `sync` makes "every write finished
before the stop is on the volume" true on every path that ends a VMM, which
is one sentence in the public doc.

## 3. Init runs the command in the background so it can trap the request, and accepts that `SIGINT`/`SIGQUIT` arrive ignored

**The call.** `/legion/init` runs the command as `legion_run <&3 3<&- &`
(fd 3 is the console, so stdin is unchanged) and loops on `wait`, which a
trapped signal interrupts. `exec` inside `legion_run` makes `$!` the command
itself.

**Alternatives.** (a) Keep the command in the foreground. A shell runs traps
only between commands, so the request would wait for the command to exit on
its own. (b) Start the command through a launcher that resets signal
dispositions (`/legion/legion` as a tiny exec wrapper, or `setsid`). (c)
`set -m` for job control, which would also give the command its own process
group.

**Trade-off.** POSIX shells start background jobs with `SIGINT` and
`SIGQUIT` ignored when job control is off. A program that installs its own
handler still gets them. A shell-script command cannot act on them.
Documented in the public doc, with the four signals that work.

**Why (simplicity).** (b) makes the command depend on the agent binary
running on the image, which is exactly the case that already fails on musl
images without `legion-guest`, and would turn "no exec" into "no workload".
(c) needs a controlling terminal, which PID 1 on `/dev/console` never has,
so dash and busybox turn it off with a warning. Two of six signals is a
documented edge, and `SIGTERM` (the default) works.

## 4. An unreachable agent means an immediate hard stop, not a wait for the deadline

**The call.** When the stop request fails (refused dial, no agent, 3s
timeout), `stop_with` returns `Ok(false)` and the worker stops the VMM at
once, through `stop`'s flush attempt.

**Alternatives.** Return `Ok(true)` anyway and let the guest run until
`killAfter`, in case its own writeback gets there.

**Trade-off.** A guest whose agent is merely slow (more than 3s to take an
exec) loses its chance to shut down cleanly.

**Why (simplicity).** A guest that was never told to stop will not stop, so
waiting buys at most whatever the kernel writes back on its own, at the cost
of a 30s stall on every such stop. The warning (`Could not deliver a stop
request into the sandbox guest; stopping it hard`) names the cause.

## 5. Init kills every remaining process with SIGKILL before it unmounts, and only when volumes are mounted

**The call.** After the command exits, init runs `kill -9 -1`, then unmounts
each mounted volume in reverse order (retrying once after 1s, falling back to
a read-only remount), then syncs and powers off. A guest with no mounted
volumes keeps its old shutdown.

**Alternatives.** A conventional `SIGTERM`, sleep, `SIGKILL` sweep, so strays
and exec sessions get a chance to exit cleanly.

**Trade-off.** Strays the command left behind, open exec sessions, and the
agent get no notice. They had none before either: `poweroff -f` kills
everything at once.

**Why (performance).** The command, which is what the stop signal was for,
has already exited. A TERM sweep adds a fixed sleep to every sandbox stop to
be polite to processes that were about to die anyway.
