# Judgement calls: join token file and CLI timeouts

Calls made while fixing two findings from the AWS spot check: the worker's
join token sat in its argv (P1), and `legion job list` hung forever against a
cluster with no raft leader (P2). Both are bug fixes with no plan. Public
docs: [auth-and-acls.md](../../docs/auth-and-acls.md),
[architecture-managers-workers.md](../../docs/architecture-managers-workers.md#running-a-worker-under-a-service-manager),
and [agent-readiness.md](../../docs/agent-readiness.md#connecting).

**Entries are ranked by project impact, highest first; an entry's number is
its rank.**

## 1. The join token moves to `--token-file`; there is no `LEGION_JOIN_TOKEN`

**The call.** `legion join` and `legion start worker` take
`--token-file <PATH>` as the non-argv way to pass the join token. The
positional `TOKEN` becomes optional when the flag is given, and the two
conflict.

```ini
ExecStart=/usr/local/bin/legion start worker --token-file /etc/legion/join-token --name %H
```

**The alternative.** A `LEGION_JOIN_TOKEN` environment variable, with a
systemd `EnvironmentFile=` at `0600`. It is the obvious shape, matches
`LEGION_TOKEN` for the CLI, and is how containers usually get secrets.

**The trade-off.** A container platform that injects secrets only as
environment variables needs a line to write the value to a file first. Docker
and Kubernetes secrets mount as files, and systemd's `LoadCredential=` hands
the unit a file, so the common platforms are covered.

**Why.** Exec tasks inherit the worker's whole environment
(`src/task_plugins/exec.rs` adds to it and never clears it). A token in the
worker's environment reaches every host-mode exec task through `env`, which
is worse than argv, where a task had to go looking with `ps`. A root-only
`0600` file is readable only by root. This is a security boundary, so the
option without a known leak wins over the conventional one. The env
inheritance itself is a separate finding, now open in `TODO.md`.

## 2. Every manager call gets a 30s deadline, exempted by RPC name rather than by command

**The call.** The CLI's auth interceptor sets a `grpc-timeout` of
`LEGION_REQUEST_TIMEOUT` (default `30s`, `0` turns it off) on every manager
request, except the RPCs in `UNBOUNDED_METHODS`: the two streams the CLI opens
(`ExecAllocation`, `StreamEvents`) and two whole-store operations
(`GetStateBackup`, `RewrapKv`). Commands that wait by polling (`job wait`,
`job logs -f`, `--wait`) are not exempt: each poll is a short unary call and
fails at the deadline like any other.

**The alternatives.**

- A second constructor, `connect_manager_client_streaming()`, that the
  streaming commands call. This was the obvious shape, and the request named
  `job logs -f` and `job wait` as long-running.
- No default deadline, like `kubectl --request-timeout` (default `0`).

**The trade-off.** A unary call that is legitimately slower than 30s fails
with `DeadlineExceeded` until the caller raises `LEGION_REQUEST_TIMEOUT`, and
`UNBOUNDED_METHODS` has to grow when the CLI starts calling a new stream. The
interceptor reads tonic's `GrpcMethod` extension, a `#[doc(hidden)]` API that
generated clients set.

**Why.** The deadline belongs to the RPC's shape, not the command's. `exec`
calls a unary `resolve_allocation` before its stream, so a per-command client
would have left that call unbounded. Keeping the polling waits bounded is the
point: `job wait` against a leaderless cluster should report that, not wait
out its own 120s `--timeout` on a hung first poll. One interceptor also covers
the gateway and the MCP server with no change at their call sites. Defaulting
to no deadline would have kept the bug for agents, which retry on a typed
error but cannot tell a hang from a slow call.

## 3. The positional token stays, with a warning

**The call.** `legion join lgnjoin_...` keeps working. The worker logs a
warning when the token arrives on the command line, and another when the
token file is readable by group or others.

**The alternative.** Remove the positional token. AGENTS.md says to break
surfaces freely while there is no installed base.

**The trade-off.** The leak stays possible, and an interactive first run now
logs a warning.

**Why.** Pasting the token printed by `legion join-token create` (or the UI's
Access → Worker Join tab) is the whole onboarding story in the README, and a
throwaway first run is not where the leak bites. The long-lived service is
where it does, and every unit we document or ship now uses the file.

## 4. The manager connection opens lazily, so connect failures reach the envelope

**The call.** `connect_manager_client` uses `Endpoint::connect_lazy()`. A
manager that cannot be reached fails the first call with `Unavailable`
(retryable) instead of failing `connect_manager_client` with an `anyhow`
error.

**The alternative.** Keep the eager `connect()` and map its error into the
envelope at each call site.

**The trade-off.** The connect error surfaces at the first RPC rather than at
connect time, and its text changed from `failed to connect to manager at
<addr>` to `cannot connect to the manager: <cause>`, without the address.

**Why.** Before this, a manager that could not be reached printed
`Error: failed to connect...` to stderr in `--json` mode, with no envelope.
The lazy channel routes the failure through `output::resolve` at all ~150 call
sites with no change at any of them.

## 5. The client's own failures are restated before they reach the envelope

**The call.** `output::client_side_failure` walks the status's source chain.
tonic's `TimeoutExpired` becomes `DeadlineExceeded` with a hint that names a
missing raft leader and `legion raft status`. A `ConnectError` becomes
`Unavailable: cannot connect to the manager` with the `LEGION_ADDR` hint. A
`Cancelled: Timeout expired` from the manager (it enforces the `grpc-timeout`
it receives and can win the race) is treated as a deadline too.

**The alternative.** Leave tonic's `Cancelled: Timeout expired` as is.

**The trade-off.** It matches one tonic message string as a fallback, and
that string could change in a tonic upgrade. The source-chain check, which
covers the usual case, does not depend on it.

**Why.** `Cancelled` is not retryable in the envelope and reads as if someone
pressed Ctrl-C. The finding asked for a typed retryable error, and an agent
branches on `code` and `retryable`, not the message.

## 6. The worker still reads its join token at every start

**The call.** We did not persist the manager address, worker type, and CA pin
beside `node_token`, which would let `legion start worker` run with no token
after the first registration.

**The alternative.** Persist them, so the unit carries no token at all after
the first boot.

**The trade-off.** The token file has to stay on the host. Its secret is no
longer needed after the first registration (the worker re-registers with its
node token), so an expired or revoked token in the file still restarts the
worker.

**Why.** It is a new on-disk record and a change to how a worker starts,
which is more than a bug fix. With a 0600 file the remaining exposure is
root on that host, who can read `node_token` anyway. Open in `TODO.md`.
