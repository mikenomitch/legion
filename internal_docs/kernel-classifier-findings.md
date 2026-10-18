# Kernel Classifier: Phase A0 Findings

What phase A0 of the [mesh interfaces plan](plans/mesh-interfaces-plan.md)
(§6.A, §8) asked to measure before any design was committed: coverage of
the `connect()` hooks, restart survival, and what a classified connection
costs. Calls are in
[`calls/mesh-interfaces-plan_calls.md`](calls/mesh-interfaces-plan_calls.md)
(entries 27, 29, 30, 35, 36, 41, 45, and 51). The public page is
[docs/network-handlers.md, "The kernel classifier"](../docs/network-handlers.md#the-kernel-classifier).

**Where measured.** A Firecracker guest: kernel 6.18.44, 4 vCPUs (Xeon @
2.10 GHz), cgroup v2 on a hybrid host (`/sys/fs/cgroup/unified`), bpffs at
`/sys/fs/bpf`, no IPv6 in the kernel build. clang 18.1.3,
linux-libc-dev 6.8.0. Everything below is reproducible with the root-only
tests and the bench rows named in each section.

## Coverage

Every way a task can open a flow met the rule. One deny rule on port 25
and one on `2001:db8::/32`, from a process in an allocation leaf
(`every_way_to_open_a_flow_meets_the_rule_test`):

| Path | Hook | Result |
| --- | --- | --- |
| TCP `connect()` | `cgroup/connect4` | `EPERM` |
| TCP Fast Open (`sendto(MSG_FASTOPEN)`) | `cgroup/connect4` (the TFO path runs the connect hook) | `EPERM` |
| `io_uring` `IORING_OP_CONNECT` (`bpf/tests/uring_connect.c`) | `cgroup/connect4` (it goes through the socket's own connect) | `EPERM` |
| Connected UDP (`connect()` then `send()`) | `cgroup/connect4` | `EPERM` |
| Unconnected UDP (`sendto()`) | `cgroup/sendmsg4` | `EPERM` |
| An IPv4-mapped dial on a dual-stack socket (`::ffff:a.b.c.d`) | `cgroup/connect6`, judged as IPv4 | `EPERM` (on kernels with IPv6) |
| IPv6 | `cgroup/connect6` | `EPERM` (on kernels with IPv6) |
| `SOCK_RAW`, `AF_PACKET` | `cgroup/sock_create` | `EPERM` |

The guest kernel was built without IPv6 (`socket(AF_INET6)` fails with
`EAFNOSUPPORT`), so the two IPv6 rows are not measured here; the same test
covers them in CI's `kernel-tests` job, whose runner kernel has IPv6, and
treats an unsupported family as nothing to test. `sendmmsg` goes
through the same `sendmsg` hook per message.

**DNS capture.** A `sendto()` to `8.8.8.8:53` arrived at the allocation's
resolver, and the reply read as coming from `8.8.8.8:53`
(`dns_and_capture_redirect_on_a_real_kernel_test`).

**Original destination.** A redirected connect's original destination is
recovered from what the capture listener sees as the peer, keyed in
`sock_ops` at `TCP_CONNECT_CB` by (netns cookie, local address, local port)
(same test). A gated task's in-namespace listener recovers it the same way
(`gated_capture_is_transparent_test`).

## Restart survival

With bpffs, maps and links pinned under `/sys/fs/bpf/legion/abi-1/`
outlive the worker: a dropped `KernelNet` without a clean shutdown left the
rule enforced, and a second open re-adopted the maps, attached fresh
programs, and dropped the old links with no gap
(`pins_survive_a_restart_test`).

Two kernel facts the implementation had to learn:

- **bpffs refuses names with a dot.** A link staged as `connect4.new`
  fails `BPF_OBJ_PIN`; the staging suffix is `_new`.
- **A `bpf_link` cgroup attachment is always multi-attach**, and
  `BPF_LINK_CREATE` refuses the explicit `BPF_F_ALLOW_MULTI` flag
  (`EINVAL`). aya's `CgroupAttachMode::Single` sends no flag, which is what
  a link wants.

## Cost

`net_bench`'s `kernel_conn_rr` rows: a new TCP connection per request from
a child process in an allocation leaf to a backend on the node's own
address (not loopback, which the chain never judges), 100 bytes each way,
then the backend's close. 2000 iterations per row, three runs:

| Row | Run 1 | Run 2 | Run 3 |
| --- | --- | --- | --- |
| unclassified (no programs attached) | 67.9 µs | 71.4 µs | 72.8 µs |
| pass after the first rule | 70.8 µs | 73.6 µs | 72.1 µs |
| pass after 32 rules | 72.4 µs | 72.7 µs | 69.2 µs |
| through the forwarder (the netd hop a pass skips) | 180.6 µs | 214.8 µs | 170.1 µs |
| redirected through the capture listener | 198.4 µs | 219.1 µs | 163.4 µs |
| refused (`EPERM`, connect attempt only) | 2.8 µs | 2.7 µs | 2.8 µs |

- **A kernel pass is within noise of no classifier at all**: the
  differences range from −3.6 µs to +4.5 µs per connection across runs, all
  programs included (`connect4`, `sock_create`, `sock_ops`, and the
  `cgroup_skb` byte counters on every packet). The plan's estimate,
  "sub-microsecond for the lookups", holds for the lookups; the run-to-run
  noise of a new connection is larger than the programs.
- **The hop a pass flow skips costs 100–140 µs per new connection** here.
- **A redirect costs what the netd hop costs**, plus the peek: the capture
  listener waits for the client's first bytes (a full ClientHello or
  request head) before judging.
- **A refusal costs nothing on the wire**: the `connect()` fails in under
  3 µs, socket creation included, and no packet leaves.

## What this decided

- The kernel floor is features, not a version: the probe loads every
  program and attaches it to a scratch cgroup (`netKernel`). Every hook
  used exists since 5.8 (ring buffer); the link-based attachment needs 5.7.
- The programs attach once, to the cgroup parent, not per allocation leaf.
  One set of links serves every allocation, and the leaf is read with
  `bpf_get_current_ancestor_cgroup_id`.
- The checked-in object is C built with clang, not `aya-ebpf`: no nightly
  toolchain or `bpf-linker`, and `bpf/build.sh --check` makes the rebuild
  reproducible in CI.
