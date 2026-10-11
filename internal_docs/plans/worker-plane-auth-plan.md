# Authenticated, Reachable Worker Plane

> **Judgement calls:** [worker-plane-auth-plan_calls.md](../calls/worker-plane-auth-plan_calls.md)

**Status: PLANNED.** Nothing here is built. Deliberately deferred: raft
transport encryption, TLS defaults for the user-facing API, external or
pluggable CAs, and workers behind NAT (see [Non-goals](#non-goals)).

## 1. Problem statement

Legion cannot run a worker on a different machine from its managers and keep
exec, logs, or the service mesh working. The README's "Add more machines" flow
(`legion join lgnjoin_...` on a second box) registers the node and runs its
work, but nothing that needs the manager to call the worker works.

We found this while preparing a pre-launch spot check on AWS: three raft
managers and two workers on separate EC2 instances. The cause is two lines:

- The worker serves its gRPC API on `127.0.0.1:4848` only
  (`src/worker/daemon.rs:137-138`).
- It registers `workerAddress = "http://127.0.0.1:4848"` no matter where it
  runs, hardcoded from `DEF_MANAGER_HOSTNAME` after the user attribute loop,
  so config cannot override it (`src/worker/daemon.rs:315-318`).

What that breaks on a multi-host cluster:

| Surface | What happens | Code |
| --- | --- | --- |
| `legion exec`, actions, the UI terminal | The manager dials `127.0.0.1:4848` on itself and fails | `src/manager/exec.rs:190`, `:258-260` |
| `legion job logs` | Same | `src/manager/server.rs:9112-9120` |
| `legion node restart` | Same | `src/manager/server.rs:6363-6366` |
| Service discovery | Every endpoint resolves to `127.0.0.1:<port>`, because the endpoint IP is the host part of `workerAddress` | `src/storage/store.rs:206-226` |
| Cross-node mesh (`connect`) | A caller's forwarder dials `127.0.0.1:<remote port>` on its own node: refused, or worse, a local process that owns that port | `src/worker/daemon.rs:2194` |
| Edge boot | `legion sandbox start --node http://10.0.1.7:4848` (the documented form, `docs/edge-boot.md:282`) cannot connect | `src/cmd/sandbox.rs:109` |
| Work dispatch | Still works, slower: the push fails and the next heartbeat reply carries the allocation | `src/manager/dispatcher.rs:137-146` |

The AWS benchmarks never hit this because they ran simulated fleets, which
take an explicit `--advertise-host` (`src/cmd/start.rs:110`).

**The bind is the only thing standing between the network and an
unauthenticated remote-exec endpoint**, so the fix is not "bind 0.0.0.0." The
`Worker` service (`proto/legion.proto:5195-5228`) has five RPCs, and none of
`PostWork`, `ExecTask`, `GetTaskLogs`, or `RestartWorker` checks who is calling
(`src/worker/server.rs`). Their proto comments say "called only by the
manager," and the loopback bind is what makes that true. Reachable as-is, any
host on the network could open a root shell in any task, read any log, or post
arbitrary work.

With ACLs off (the default), port 4747 already lets anyone do those things
through the manager. The worker port matters for every cluster that turned ACLs
on: it would bypass ACLs, the exec audit trail, and `exec = { enabled = false }`
entirely.

Two related gaps sit on the same channel:

- **The worker-to-manager channel is always plaintext.** `create_manager_client`
  ignores TLS with a `TODO: Temporarily doesn' do any TLS`
  (`src/worker/daemon.rs:585-598`), even when the manager serves TLS. Heartbeat
  replies carry work payloads with KV-rendered secrets, and
  `IssueNodeCertificate` returns the node's mesh *private key* over it
  (`src/manager/mesh_ca.rs:72-75`). `docs/auth-and-acls.md:320-321` claims
  "the listener-level TLS below already covers transport encryption," which is
  not true for workers.
- **A worker has no way to know it is talking to a real manager.** There is no
  trust anchor in the join token, so whoever answers at the token's address
  gets the join secret.

Today an operator who wants a multi-host cluster has no workaround short of
iptables DNAT and socat on every box, which also exposes the unauthenticated
port.

## 2. Prior art

Every comparable system authenticates control-plane-to-node traffic with mutual
TLS from a cluster CA, and encodes the peer's *role* in the certificate so
"signed by our CA" is not enough to act as a server.

- **Kubernetes.** The kubelet serves HTTPS on 10250. The API server presents a
  client certificate (`--kubelet-client-certificate`), and the kubelet
  authorizes it with a `SubjectAccessReview` webhook. Kubelet serving certs can
  come from the CSR API (`serverTLSBootstrap`), and `kubeadm join` pins the
  cluster CA with `--discovery-token-ca-cert-hash sha256:...`.
  - Take: role-bearing client certs, CSR-issued node certs, and **the CA hash
    pinned in the join command**. That is the cleanest trust-bootstrap story in
    the field.
  - Reject: the API server does not verify the kubelet's serving cert unless
    `--kubelet-certificate-authority` is set, and kubelet authz historically
    defaulted to `AlwaysAllow`. Both are long-running CIS findings. We verify
    both directions by default.
  - Also note: [Konnectivity](https://kubernetes.io/docs/tasks/extend-kubernetes/setup-konnectivity/)
    exists because some control planes cannot reach nodes. See calls entry 1 for
    why we do not copy it.
  - [Kubelet authn/authz](https://kubernetes.io/docs/reference/access-authn-authz/kubelet-authn-authz/),
    [TLS bootstrapping](https://kubernetes.io/docs/reference/access-authn-authz/kubelet-tls-bootstrapping/),
    [kubeadm join](https://kubernetes.io/docs/reference/setup-tools/kubeadm/kubeadm-join/).
- **Nomad.** Certificates carry role and region in their names
  (`server.global.nomad`, `client.global.nomad`), and `verify_server_hostname`
  is what makes the role check real. Without it, any CA-signed cert can act as a
  server. Nomad's docs recommend it plus `verify_https_client`.
  - Take: role in the name, checked on every connection.
  - Reject: Nomad's TLS is opt-in and certs are operator-generated
    (`nomad tls cert create`), which is why many clusters run without it.
  - [Nomad TLS](https://developer.hashicorp.com/nomad/docs/secure/traffic/tls).
- **Consul.** `auto_encrypt` has servers hand client agents their certificates
  automatically from the built-in CA, held in memory. HashiCorp recommends it
  over operator-managed certs because rotation needs no operator action.
  - Take: the cluster issues node certs itself, and rotation is automatic.
    Legion's mesh CA already does half of this.
  - [Consul built-in CA](https://developer.hashicorp.com/consul/docs/secure/encryption/tls/enable/new/builtin),
    [Consul agent mTLS](https://developer.hashicorp.com/consul/docs/secure/encryption/mtls).

What Legion already has that the others had to build: a cluster CA replicated
with the control lane (`src/manager/mesh_ca.rs`), short-lived node certificates
with `ServerAuth` and `ClientAuth`, refreshed on the heartbeat loop, and
authenticated per-node principals (`node:<id>`) on every worker-plane RPC when
ACLs are on.

## 3. Jobs to be done

**Cluster operator**
- When I add a second machine, I want `legion join` to just work, so I can grow
  past one box without learning a TLS toolchain.
- When I open a port between my managers and workers, I want to know that
  only my managers can drive the workers through it, so I can sleep at night.
- When a worker is misconfigured or firewalled, I want the cluster to tell me
  which node and which port, so I can fix it before someone needs a shell.

**On-call responder**
- When a task on some node is misbehaving, I want logs and a shell from any
  manager I happen to be pointed at, so I can debug without hunting for the
  node.

**Security / platform admin**
- When I turn on ACLs, I want every path to a running task to go through
  them, so the audit trail and the "no exec" switch mean something.
- When I hand out a join token, I want a stolen or spoofed manager address to
  be useless to an attacker, so joining is safe on an untrusted network.

**Job author**
- When my frontend and API land on different machines, I want the mesh to
  connect them, so placement is not my problem.

**Autonomous agent**
- When I call `legion_exec_run` or `legion_job_logs` over MCP, I want them to
  work on whichever node the task landed on, so my debugging loop does not
  depend on topology.

*Not in scope:* the developer on a single laptop. `legion start manager
--run-worker` must behave exactly as it does today, with nothing new to
configure.

### User scenarios

1. **Two-box join.** An operator runs `legion start manager` on box A and
   `legion join lgnjoin_...` on box B. They run a two-replica job, then
   `legion job logs`, `legion exec`, and a mesh client on A calling a service on
   B. All of it works with no flags, as long as A can reach B on 4848.
2. **Firewalled worker.** The same, but the security group blocks 4848. The
   worker joins and runs work, and within seconds `legion node info` says the
   manager cannot reach it, naming the address and the port. `legion exec`
   fails with the same hint, not a bare `transport error`.
3. **Attacker on the network.** Someone reaches a worker's 4848 directly. They
   can complete a TLS handshake but every privileged RPC returns
   `Unauthenticated`. They impersonate the manager's address to a joining
   worker. The worker refuses, because the CA does not match the pin in its
   join token.

## 4. Goals and non-goals

### Goals

- Workers on separate hosts support exec, actions, logs, restart, service
  discovery, the mesh, and edge boot, with no extra configuration.
- Only a manager can call `PostWork`, `ExecTask`, `GetTaskLogs`, and
  `RestartWorker`, regardless of ACL mode. This holds because the manager's
  private key never leaves the managers.
- A manager talks to a worker only after verifying that worker's identity
  matches the node it means to reach.
- A joining worker verifies the manager against a CA pinned in its join token.
- No node private key crosses the network.
- `--run-worker` and every single-box flow keep working unchanged.

### Non-goals

- **Raft transport encryption.** Peers already authenticate with the cluster
  secret (`src/storage/raft/driver.rs:1884`), but in plaintext. The manager
  certificates this plan introduces are the building block for that work. It
  gets its own plan.
- **TLS by default on the user-facing API (CLI, UI, MCP).** Phase 3 encrypts
  the worker's connection to the manager. What the CLI does is a separate,
  user-visible decision tracked in `internal_docs/TODO.md` ("TLS is
  autoconfigured and auto-rotated").
- **Workers behind NAT** that managers cannot dial. The mesh needs
  node-to-node reachability anyway, so a reverse tunnel would not make such a
  worker fully functional (calls entry 1).
- **External or pluggable CAs** (Vault PKI, ACME). The CA stays built in.
- **Registration through a raft follower.** `WorkerRegister` is not forwarded
  to the leader today, so a worker whose token names a follower fails to
  register. That is a separate bug, filed in `internal_docs/TODO.md`.

## 5. What exists today

- **Worker listener.** Plain gRPC on `127.0.0.1:<LEGION_WORKER_PORT|4848>`
  (`src/worker/daemon.rs:137-138`, `:278-280`). No TLS, no interceptor.
- **Advertised address.** `workerAddress` is hardcoded to loopback
  (`src/worker/daemon.rs:315-318`). The simulator alone takes
  `--advertise-host` (`src/cmd/start.rs:110-118`, `src/worker/simulate.rs:66-77`).
- **Manager-side clients.** Seven places build a `WorkerClient`, each with its
  own copy of `worker_address_for_node`:
  - dispatch (`src/manager/dispatcher.rs:40-54`, `:361-362`, `:463-464`)
  - exec (`src/manager/exec.rs:217-223`, `:258-260`)
  - logs and restart (`src/manager/server.rs:9045-9050`, `:9112-9120`,
    `:6363-6366`)
  - edge boot from the CLI (`src/cmd/sandbox.rs:109`)
- **Dispatch is best-effort by design.** A failed push costs one heartbeat
  interval, because the heartbeat reply carries the desired set
  (`src/manager/dispatcher.rs:137-146`).
- **The mesh CA.** One CA per cluster, generated on first use and replicated
  in the control lane (`net_mesh_ca`). It signs node certs with SANs
  `<node>.node.legion` and `legion://node/<id>`, valid 24 hours and refreshed
  after 8 (`src/manager/mesh_ca.rs`). The manager generates the key pair and
  returns both halves (`:72-75`).
- **Node cert issuance on the worker is lazy.** The worker asks for one only
  once something on the node needs mesh TLS (`src/worker/daemon.rs:1594-1640`).
- **Worker credentials.** With ACLs on, registration needs the join secret and
  mints a per-node token. Every worker-plane RPC on the manager checks that the
  token's node matches the request (`docs/auth-and-acls.md:285-310`).
- **The join token** is `lgnjoin_` plus base64url JSON
  `{v, addr, secret, workerType}` (`src/structs/token.rs:21-40`). It carries no
  trust anchor.
- **The upgrade contract.** Managers upgrade first. `PROTOCOL_VERSION = 2`
  (`src/constants.rs:13`), and the manager reads a node's advertised version to
  keep old behavior for old workers (`docs/upgrades.md:57-106`).
- **`StartClaim`** is authorized by the boot ticket alone and is meant to be
  called by an external router, not a manager (`proto/legion.proto:5210-5227`).
- **TLS dependencies** are already in the tree: `tonic` with `tls`, `rustls`,
  `rcgen`, and `x509-parser` (`Cargo.toml:43-78`).

## 6. Proposed design

### 6.1 The worker listener is mutual TLS, always

The worker serves the `Worker` service over TLS using its node certificate,
on every platform and in every mode, including `--run-worker`. There is no
plaintext worker listener and no knob to turn one on.

The listener requests a client certificate but does not require one at the
handshake, because `StartClaim` callers are routers holding boot tickets. Each
RPC declares who may call it:

| RPC | Caller | Check |
| --- | --- | --- |
| `PostWork`, `ExecTask`, `GetTaskLogs`, `RestartWorker` | a manager | client cert chains to the cluster CA and carries a `legion://manager/<id>` URI SAN |
| `StartClaim` | a router | the boot ticket, as today. A client cert is ignored. |

A node certificate is *not* a manager certificate. The CA signs both, so the
role SAN is what the worker checks, exactly like Nomad's
`verify_server_hostname`.

A direct call without a manager cert:

```console
$ grpcurl -cacert ca.pem 10.0.1.7:4848 legion.Worker/GetTaskLogs
ERROR:
  Code: Unauthenticated
  Message: GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate
```

A test parses `proto/legion.proto` and fails the build if a `Worker` RPC has no
caller class, the same way `tests/acl_endpoints.rs` guards the `Manager`
service.

### 6.2 Managers hold manager certificates

Each manager mints its own client certificate from the cluster CA at startup
and re-mints it before expiry. It carries:

- URI SAN `legion://manager/<manager id>` (the raft `nodeId`, or `local` for a
  single manager)
- `ClientAuth` only, valid 24 hours

The CA key lives in the replicated store already, so every manager can sign.
Nothing is distributed.

### 6.3 The manager verifies the worker it dials

All seven call sites go through one `WorkerPlane::client(node)` factory that:

- reads `workerAddress` from the node record (one function, not three copies)
- dials it with the cluster CA as the only root and the manager cert as client
  identity
- verifies the server certificate against `<node>.node.legion`, so a worker
  that registered a stolen or stale address cannot receive another node's
  work payloads
- caches channels per node, as the dispatcher does per address today

The CLI's edge-boot path (`legion sandbox start --node https://...`) verifies
the node cert the same way. It fetches the CA with the caller's token through
the existing `legion net ca show`.

### 6.4 Workers generate their own key

The worker generates its key pair locally and sends a CSR. The manager signs
the public key with SANs it chooses (`<node>.node.legion`,
`legion://node/<id>`), ignoring whatever the CSR requests. The private key
never crosses the network.

- `IssueNodeCertificateRequest` gains an optional `csr_pem`. Without it, the
  manager keeps today's behavior (manager-generated key), so old workers keep
  their mesh certificates.
- Issuance moves from lazy to eager. The worker asks for its certificate right
  after registration, because the listener needs it.
- The key and certificate persist under the worker's state dir, mode `0600`,
  so a worker restarted during a manager outage can still serve `StartClaim`
  (edge boot must work with no manager on the critical path). The mesh reuses
  the same identity.

### 6.5 The join token pins the CA

`legion join-token create` embeds the CA's SHA-256 in the token:

```json
{ "v": 1, "addr": "http://10.0.0.5:4747", "secret": "ljt_...", "workerType": "default",
  "caHash": "sha256:9c1e4f..." }
```

The worker checks every CA it receives from the manager against the pin. A
mismatch stops the join before the worker sends anything sensitive:

```console
$ legion join lgnjoin_eyJ2IjoxLC...
error: the manager at http://10.0.0.5:4747 presented cluster CA sha256:51aa07...,
but this join token pins sha256:9c1e4f...
hint: the address may not be your manager, or the CA was rotated after this
token was minted. Mint a new token with `legion join-token create`.
```

The token stays version 1 because the field is additive. A token without
`caHash` (minted before this change) joins with a warning, trusting the first CA
it sees, and the warning says to re-mint.

### 6.6 Reachable by default

The worker binds `0.0.0.0:4848` by default. This is safe only because 6.1 makes
every privileged RPC manager-only. Two env vars, mirroring the manager's
`server.host` and `server.advertiseAddr`, cover the rest:

| Env var | Default | Meaning |
| --- | --- | --- |
| `LEGION_WORKER_HOST` | `0.0.0.0` | Address the worker plane binds |
| `LEGION_WORKER_PORT` | `4848` | Existing. Port the worker plane binds and advertises |
| `LEGION_WORKER_ADVERTISE_ADDR` | auto | Host (or `host:port`) managers and routers dial |

`legion join` and `legion start worker` take `--advertise-addr` with the same
meaning.

**Auto-detection** picks the source address the worker uses to reach its
manager (a connected UDP socket's local address: no packets sent). On a VPC
that is the private IP, which is what managers on the same network dial. A
worker co-located with its manager gets `127.0.0.1`, exactly as today. Behind
NAT it is wrong, which is what the override is for.

The worker logs what it chose at startup:

```text
INFO Worker plane listening on 0.0.0.0:4848 (mTLS); advertised as https://10.0.1.7:4848
```

`workerAddress` becomes `https://10.0.1.7:4848`. Service-discovery endpoints,
the mesh, and edge boot all take the host from it, so fixing the address fixes
them with no further change.

### 6.7 Tell the operator when a manager cannot reach a worker

Dispatch survives an unreachable worker (heartbeats carry the work), so a
firewalled worker looks healthy until someone needs logs at 3am. After a worker
registers, the manager that accepted the registration calls a new
`Worker/Probe` RPC (manager-only, returns the node id). The result lands on the
node record and in `legion node info`:

```console
$ legion node info wkr-1
...
Worker plane   https://10.0.1.7:4848   unreachable from mgr-b: connection timed out
               Managers dial workers on port 4848. Allow it from the managers,
               or set LEGION_WORKER_ADVERTISE_ADDR if this address is wrong.
Certificate    wkr-1.node.legion, expires in 21h
```

Exec and logs failures carry the same hint:

```console
$ legion exec 412 -it -- sh
error: cannot reach node wkr-1 at https://10.0.1.7:4848 (connection timed out)
hint: managers dial workers on port 4848; see `legion node info wkr-1`
```

In `--json` output this is the standard error envelope with code
`WORKER_UNREACHABLE` and `retryable: true`.

The probe is informational. It does not feed node readiness, which gates
rollouts, because the node still runs work correctly (open question 2).

### 6.8 The worker-to-manager channel is encrypted

Phase 3 closes the plaintext channel workers use for heartbeats and
registration.

- When the manager serves operator TLS (`server.tls`), workers use it. This
  fixes the `TODO` in `create_manager_client`. The worker verifies the server
  cert against the system roots or the CA pinned in its token.
- When it does not, the manager answers TLS handshakes on the same port with a
  certificate from the cluster CA, and keeps answering plaintext for the CLI
  and UI. It tells them apart by the first byte of the connection: a TLS
  ClientHello starts with `0x16`, and HTTP/2 or HTTP/1 never do. Workers always
  speak TLS and pin the CA. Join tokens from such a manager advertise
  `https://`.

This keeps one port and one join command, and leaves the CLI's defaults to the
separate TLS-autoconfig work.

### 6.9 CA rotation keeps both CAs trusted for one cert lifetime

`legion net ca rotate` already exists and today drops the old CA outright.
Once the CA guards the control plane, that would cut every manager off from
every worker until each one refreshes, up to 8 hours later. Rotation becomes:

1. Generate the new CA. Publish a trust bundle of old plus new.
2. Keep signing with the old CA until every node has fetched the bundle on a
   refresh. The bundle rides on the existing certificate refresh.
3. Switch signing to the new CA. Drop the old one from the bundle after one
   node-cert lifetime (24 hours).

`legion net ca show` prints both CAs and which phase rotation is in. Join
tokens minted during the overlap pin the new CA, and workers accept a bundle
containing their pinned CA.

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto`
  - `IssueNodeCertificateRequest.csr_pem` (optional string, new field number).
    The reply leaves `key_pem` empty when a CSR was sent, and `ca_pem` becomes
    a bundle during rotation.
  - `Worker/Probe(WorkerProbeRequest) returns (WorkerProbeReply)`, carrying the
    node id. It is manager-only.
  - Proto comments on every `Worker` RPC name its caller class.
- `src/constants.rs`: `PROTOCOL_VERSION = 3`. Version 3 means "serves the
  mTLS worker plane and advertises a reachable address."
- `JoinToken` gets `ca_hash: Option<String>` (`caHash` on the wire).
- Node record: `workerAddress` changes scheme to `https`. New config attrs for
  the probe result (`workerPlaneProbe = { ok, by, at, error }`) are written by
  the manager, not reported by the worker.
- Store: the `net_mesh_ca` row gains a `next` CA and a rotation phase. Add a
  migration.
- Worker state dir: `identity/node.key` and `identity/node.pem`, mode `0600`.
- No job-config fields change, so `configs/jobs/maximal.ncl` is untouched.

## 8. Implementation phases

Each phase ships and is useful on its own.

**Phase 1: an authenticated, reachable worker plane.** 6.1 through 6.6 and 6.9,
plus the simulator.

- Manager certificates. The `WorkerPlane` client factory replaces all seven
  call sites.
- The mTLS worker listener with per-RPC caller classes and the classification
  test.
- CSR issuance, eager at registration, persisted.
- The CA pin in join tokens.
- Bind and advertise defaults and overrides.
- The simulator serves one listener for its whole fleet, so it registers its
  nodes with a shared fleet identity (open question 4).
- Two-step CA rotation, because Phase 1 makes the CA load-bearing.
- Docs: `architecture-managers-workers.md` (ports, what each side dials),
  `auth-and-acls.md` (remove the false transport-encryption claim, describe the
  worker plane), `edge-boot.md` (`https://` and the CA), `network-handlers.md`
  (rotation), `upgrades.md` (protocol 3), `data-directory.md`, and the
  README's "Add more machines."

After Phase 1, multi-host clusters work and the worker port is safe to open.

**Phase 2: reachability diagnostics.** 6.7.

- `Worker/Probe`, the node-record field, `legion node info` output, and the
  UI node page.
- `WORKER_UNREACHABLE` errors with hints on exec, logs, and restart. The same
  in MCP tool errors.
- `docs/agent-readiness.md` for the new error code.

**Phase 3: an encrypted worker-to-manager channel.** 6.8.

- Workers honor operator `server.tls`.
- TLS detection on 4747 with cluster-CA certs when no operator cert is set.
  Join tokens advertise `https://`.
- Docs: `auth-and-acls.md` TLS section, `architecture-managers-workers.md`
  "What Is Not Implemented Yet."

## 9. Testing plan

**Unit (Phase 1)**
- The worker verifier accepts a manager cert. It rejects a node cert, a cert
  from a foreign CA, an expired cert, and no cert, for each manager-only RPC.
- `StartClaim` with no client cert still reaches ticket verification.
- The manager's dial rejects a server cert for `node-2` at `node-1`'s address.
- CSR signing ignores requested SANs and extensions.
- The join flow refuses a CA that does not match the pin and accepts a bundle
  that contains it.
- Every `Worker` RPC has a caller class (parses the proto).
- Advertise detection returns the route's source address. The override wins.

**Integration (Phases 1-2)**
- Two workers in one test process on `127.0.0.2` and `127.0.0.3` (Linux routes
  all of `127/8` to loopback, so this needs no setup). Exec, logs, restart, and
  a cross-node `connect` upstream all work through the advertised addresses.
  Gate the test to Linux, since macOS needs `lo0` aliases.
- A direct `ExecTask` with no client cert returns `Unauthenticated`. With a node
  cert it is refused too.
- A worker whose port is closed registers, runs work through heartbeats, and
  shows `unreachable` in `legion node info` with the hint.
- Rotation: rotate the CA mid-test. Exec keeps working through every step.
- Upgrade skew: a protocol-2 worker co-located with a new manager keeps working
  over plaintext loopback.

**Regression and performance**
- `cargo test --all --locked`, `tests/acl_endpoints.rs`, and the simulator
  smoke at 1,000 nodes.
- Dispatch now runs over TLS. Run the AWS rig's 1,000-node cell before and
  after, and put both placement-throughput numbers in the PR. Channels are
  cached per node, so the handshake cost should amortize to nothing, but
  measure it.

**End to end**
- The paused AWS spot check is the acceptance test: three raft managers and two
  spot workers. Exec, logs, and cross-node mesh traffic work. Exec served by a
  follower works after a leader failover. A worker is unreachable on purpose
  (security group) and reported.

## 10. Rollout and backward compatibility

- **Upgrade order is unchanged:** managers first. A new manager dials a
  protocol-3 worker over mTLS and a protocol-2 or older worker the old way.
  Old workers advertise `http://127.0.0.1:4848`, so the old way only ever
  reached a co-located worker, and that keeps working.
- **New workers need new managers.** That is already enforced: a worker newer
  than its manager is rejected at registration.
- **Plaintext to a non-loopback worker is refused** with an error naming the
  protocol version. No supported configuration did that before.
- **Operators must allow 4848 from managers to workers**, and from routers if
  they use edge boot. This was never needed before because it never worked. The
  release note and the README say so.
- **Single-box flows** (`--run-worker`, `mise run join`) get mTLS over
  loopback with no configuration. Certificates come from the in-process
  manager.
- **Simulated fleets** keep working through the fleet identity. Benchmark
  commands do not change.
- **Existing join tokens** keep working without a pin, with a warning to
  re-mint.
- Remove the plaintext-loopback compatibility path one release after Phase 1.

## 11. Open questions

1. **One CA or two?** The mesh CA would now also guard the control plane, so a
   mesh rotation is a control-plane event. *Lean: one CA*, with roles in URI
   SANs and the two-step rotation in 6.9. A second CA doubles the concepts and
   the rotation procedures for blast-radius separation we do not have a
   concrete threat for. Calls entry 3.
2. **Should an unreachable worker plane fail node readiness?** *Lean: no.* The
   node runs work correctly, and readiness gates rollouts on every job. Revisit
   if operators ask for it.
3. **TLS detection on 4747, or a dedicated worker port on the manager?**
   *Lean: detection,* to keep one port and one address in the token. A
   dedicated port is simpler to reason about but is a second thing to open in
   every firewall.
4. **How does the simulator prove identity for thousands of nodes on one
   listener?** *Lean: a fleet certificate.* The simulator registers its nodes
   with one shared fleet name, and the manager verifies that name for them.
   Per-node SANs would make a 5,000-SAN certificate.
5. **Persist the node key, or keep it in memory like Consul?** *Lean:
   persist,* because edge boot must serve claims while managers are down.

## 12. Acceptance criteria

**Phase 1**
- On two hosts with only 4747 and 4848 open, `legion job logs`, `legion exec
  -it`, `legion node restart`, and a cross-node `connect` upstream all work
  with no flags beyond `legion join <token>`.
- `legion service resolve` returns the worker's private IP, not `127.0.0.1`.
- From a third host, every manager-only `Worker` RPC returns `Unauthenticated`.
- A worker given a token whose `addr` points at a host with a different CA
  refuses to join and prints the pin mismatch.
- The `every_worker_rpc_is_classified` test exists and passes.
- `legion net ca rotate` completes with no failed exec or logs call during the
  overlap.
- AWS 1,000-node placement throughput is within noise of the base branch.

**Phase 2**
- Blocking 4848 shows `unreachable` with the hint in `legion node info`
  within one probe, and `legion exec` fails with `WORKER_UNREACHABLE`.

**Phase 3**
- A packet capture of worker-to-manager traffic on 4747 shows only TLS. The CLI
  still works over plaintext against the same port.
