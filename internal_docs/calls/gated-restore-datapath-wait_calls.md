# Judgement calls: the gated restore's datapath wait

Calls made while fixing the 5-second wait on every sandbox restore
(2026-10-15). The work has no plan of its own: it fixes the wait that
[gated-sandbox-checkpoints-plan.md](../plans/gated-sandbox-checkpoints-plan.md)
§6.3 introduced
([calls 34](gated-sandbox-checkpoints-plan_calls.md#34-a-restore-waits-briefly-for-the-datapaths-socket-then-resumes-fail-closed)),
which hid lazy against eager in the first KVM run of
[lazy-memory-restore-plan.md](../plans/lazy-memory-restore-plan.md) §9.
Entries are ranked by project impact, highest first, and the number is the
rank.

## 1. The launch path binds the egress proxy, for every delivery route

**Call.** `WorkerDaemonService::accept_work`, which every push and every
work-stream delta goes through, binds a gated sandbox's egress proxy on
`<alloc>/.sandbox/vsock.sock_1080` before it launches the task
(`daemon::bind_launch_datapath`, `EgressProxies::ensure`). The heartbeat
already bound it before its own reconcile. The spec is built by the same
function the heartbeat's `proxy_specs` uses, so the next sync sees the
listener as its own and keeps it. The bind also wakes the heartbeat.
Provisional starts return from `accept_work` before the bind, and that is
fine: session tasks refuse gated networking at submit, and an edge-started
sandbox gates on its TAP device, not on the vsock datapath.

**Alternatives.** (a) Shorten `DATAPATH_WAIT`, so a miss costs less. (b)
Have the restore wake the heartbeat and wait for the beat that binds the
socket. (c) Bind the whole gated datapath in the launch path: the proxy,
the resolver, the upstream listeners, and the policy.

**Trade-off.** Until the woken heartbeat lands, the restored guest's proxy
has no policy, so it resets each connection, and its resolver and upstream
sockets are not bound yet. The guest resumes with a working door that
refuses for about one heartbeat round trip, instead of no door at all for
5 seconds. The service now carries a handle on the node's egress proxies.

**Why (performance, simplicity).** The wait exists for a transient bind
error, and the restore should never be the thing waiting on a heartbeat
cadence. (a) keeps paying the wait on every streamed restore and only
trades it against a real bind error's grace. (b) ties a restore's latency
to the manager's round trip and to a loop that is not built to be waited
on. (c) duplicates the heartbeat's whole reconcile in a second place, with
a policy fetch on the start path. The proxy is the one socket the restore
blocks on, so it is the one the launch path owns, and waking the heartbeat
gets the rest there as soon as the manager answers.

## 2. The restore waits only when the allocation is gated

**Call.** `RestoreTarget` gains `gated`, set from the allocation's network
mode, and `await_datapath` returns at once without it. `RestoreTarget.vsock`
stays set for every microVM restore, because exec needs the guest moved onto
the new allocation's socket.

**Alternatives.** Derive the checkpoint's recorded `network` and
`RestoreTarget::network` from the network mode too, which the TODO asks for,
so that `check_restorable`'s gated-against-TAP refusal fires again.

**Trade-off.** Checkpoints still record `gated` for every microVM, and the
refusal still never fires. A TAP checkpoint restored into a gated task, or
the reverse, is not refused by name. It stays an open TODO item.

**Why (simplicity).** The rig's sandboxes are not gated (the gated default
is still off), so nothing ever binds their proxy socket, and this is the
cause of the measured 5 seconds. One field fixes it. Rederiving the
recorded network changes what old checkpoints mean and touches the
Firecracker vsock-move refusal, which keys off the same field and really
is about the device. That deserves its own change with its own tests.

## 3. The heartbeat stays the owner of the proxy set

**Call.** `EgressProxies::ensure` binds one listener and records nothing
else. The next `sync` keeps it when its desired set carries the same spec
and stops it otherwise.

**Alternatives.** Have `ensure` pin the allocation until a desired set
lists it, the way a ratified claim is exempt from the stop sweep.

**Trade-off.** On a node that takes pushes without the work stream, a
heartbeat reply computed before the manager committed the allocation can
arrive after the push and stop the proxy, until the next beat binds it
again. The guest fails closed for that beat.

**Why (simplicity).** A streaming worker's heartbeat reconciles from the
work-stream mirror, which already holds the allocation before the launch,
so the race needs the older push-only route and an unlucky reply order. A
pin is a second source of truth for a listener set the heartbeat already
reconciles.

## 4. The launch path rebinds a proxy whose socket file is gone, a sync does not

**Call.** `ensure` counts a vsock-backed listener as serving only while its
socket path exists, so a launch rebinds one that vanished. A start that
fails removes the allocation's directory, socket and all, after the launch
path bound it. `sync` still counts any running listener as serving.

**Alternatives.** (a) Make `sync` rebind a vanished socket too. (b) Unbind
the proxy when the launch fails.

**Trade-off.** A heartbeat-delivered start that follows a failed start of
the same allocation, with no push or stream launch in between, still finds
the old listener on an unlinked path and waits out the 5 seconds. A failed
allocation is normally replaced by a new one with a new id, so this needs a
retry in place.

**Why (simplicity).** (a) recreates `<alloc>/.sandbox/` for any allocation
the node tore down while the manager still lists it, and nothing sweeps
stray allocation directories. (b) covers only the launch failure, and
wires the proxy set into the task manager's failure path.
