# Authenticated Worker Plane and Isolated Workloads

> **Judgement calls:** [worker-plane-auth-plan_calls.md](../calls/worker-plane-auth-plan_calls.md)

**Status: PLANNED.** Nothing here is built. The plan has two halves that
share one threat: something reaching a node's control plane that shouldn't.

- **The worker plane** (Phases 1, 4, and 8): mutual TLS on the worker
  listener, a reachable advertised address, an encrypted worker-to-manager
  channel, and reachability diagnostics. It also carries the
  [sandbox-exec plan's](sandbox-exec-plan.md) ticket caller class (§6.1).
- **Local workload isolation** (Phases 0, 2, 3, 5, 6, and 7): ACLs on by
  default, a host guard under every allocation's traffic, gated networking by
  default for Linux containers, a container privilege floor, and an honest
  trust story for `exec`.

Phases are numbered in the order they should ship, most dangerous gap first.
Each ships alone. Deliberately deferred: raft transport encryption, TLS
defaults for the user-facing API, external or pluggable CAs, workers behind
NAT, transparent capture in gated namespaces, LSM (AppArmor or SELinux)
profiles, and macOS as a host for untrusted code (see
[Non-goals](#non-goals)).

## 1. Problem statement

### 1.1 Workers on other hosts don't work

Legion cannot run a worker on a different machine from its managers and keep
exec, logs, or the service mesh working. The README's "Add more machines" flow
(`legion join lgnjoin_...` on a second box) registers the node and runs its
work, but nothing that needs the manager to call the worker works.

We found this while preparing a pre-launch spot check on AWS: three raft
managers and two workers on separate EC2 instances. The cause is two lines:

- The worker serves its gRPC API on `127.0.0.1:4848` only
  (`src/worker/daemon.rs:155`).
- It registers `workerAddress = "http://127.0.0.1:4848"` no matter where it
  runs, hardcoded from `DEF_MANAGER_HOSTNAME` after the user attribute loop,
  so config cannot override it (`src/worker/daemon.rs:376-379`).

What that breaks on a multi-host cluster:

| Surface | What happens | Code |
| --- | --- | --- |
| `legion exec`, actions, the UI terminal | The manager dials `127.0.0.1:4848` on itself and fails | `src/manager/exec.rs:217-223`, `:260` |
| `legion job logs` | Same | `src/manager/server.rs:9561`, `:9623-9644` |
| `legion node restart` | Same | `src/manager/server.rs:6879-6882` |
| Service discovery | Every endpoint resolves to `127.0.0.1:<port>`, because the endpoint IP is the host part of `workerAddress` | `src/storage/store.rs:216-226` |
| Cross-node mesh (`connect`) | A caller's forwarder dials `127.0.0.1:<remote port>` on its own node: refused, or worse, a local process that owns that port | `src/worker/daemon.rs:2684` |
| Edge boot | `legion sandbox start --node http://10.0.1.7:4848` (the documented form, `docs/edge-boot.md:283`) cannot connect | `src/cmd/sandbox.rs:109` |
| Work dispatch | Still works, slower: the push fails and the next heartbeat reply carries the allocation | `src/manager/dispatcher.rs:139-146` |

The AWS benchmarks never hit this because they ran simulated fleets, which
take an explicit `--advertise-host` (`src/cmd/start.rs:110`).

**The bind is the only thing standing between the network and an
unauthenticated remote-exec endpoint**, so the fix is not "bind 0.0.0.0." The
`Worker` service (`proto/legion.proto:7257-7290`) has five RPCs, and none of
`PostWork`, `ExecTask`, `GetTaskLogs`, or `RestartWorker` checks who is calling
(`src/worker/server.rs`). Their proto comments say "called only by the
manager," and the loopback bind is what makes that true. Reachable as-is, any
host on the network could open a root shell in any task, read any log, or post
arbitrary work.

Two related gaps sit on the same channel:

- **The worker-to-manager channel is always plaintext.** `create_manager_client`
  ignores TLS with a `TODO: Temporarily doesn' do any TLS`
  (`src/worker/daemon.rs:799-811`), even when the manager serves TLS. Heartbeat
  replies carry work payloads with KV-rendered secrets, and
  `IssueNodeCertificate` returns the node's mesh *private key* over it
  (`src/manager/mesh_ca.rs:72-75`). The edge-boot JWKS is fetched over the
  same plaintext address (`src/worker/daemon.rs:500-508`), so an on-path
  attacker can swap the keys that verify boot tickets.
  `docs/auth-and-acls.md:342-343` claims "the listener-level TLS below
  already covers transport encryption," which is not true for workers.
- **A worker has no way to know it is talking to a real manager.** There is no
  trust anchor in the join token, so whoever answers at the token's address
  gets the join secret.

Today an operator who wants a multi-host cluster has no workaround short of
iptables DNAT and socat on every box, which also exposes the unauthenticated
port.

### 1.2 Local workloads can reach the host's control plane

Closing port 4848 to the network is not enough, because the attacker we
most need to stop is already on the host. Linux `container` tasks share the
host's network by default (`docs/task-runtime-container.md:316`, `:392`), and
so do `exec` tasks (`docs/task-runtime-exec.md:89`). `network.mode = "auto"`
resolves to `host` for every runtime (`src/structs/network.rs:39-49`). Code in
any such task reaches everything the host reaches. We checked each target in
the code:

| Target | What a host-network task gets | Verified at |
| --- | --- | --- |
| **The manager on 4747**, when co-located (`--run-worker`, single-box setups) | Full control of the cluster with no token. ACLs are off by default (`docs/auth-and-acls.md:9`, `src/structs/config.rs:1129-1131`). The manager binds `127.0.0.1` by default (`src/constants.rs:73`, `configs/manager.ncl:4`), which a host-network task shares. The origin guard does not help: it ignores requests without an `Origin` header (`src/structs/config.rs:37-41`). | `src/manager/daemon.rs:432-433` |
| **The manager on another host**, in any multi-host cluster | The same, over the network. Workers must reach the manager, so `server.host` is a reachable address, and every task on every worker can reach it too. | `src/structs/config.rs:1633-1647` |
| **The open `BootstrapAcl` RPC**, even with ACLs on | Succeeds whenever no management token exists (`src/manager/server.rs:8752-8754`, `docs/auth-and-acls.md:371`). A task on the manager's host can race the operator to it after first start, or after a recovery. | `tests/acl_endpoints.rs:194` (`Plane::Open`) |
| **The exec WebSocket on 4748** and **metrics on 4749** | Both bind `server.host`. The WebSocket enforces ACLs, so with ACLs off it is another unauthenticated shell into every task. `/metrics` is unauthenticated, though its labels are bounded and low-cardinality, so it leaks cluster shape rather than secrets. | `src/manager/daemon.rs:436-477` |
| **The worker plane on 4848** | `ExecTask` into any allocation on the node, `GetTaskLogs`, `PostWork`, `RestartWorker`, with no credential. `StartClaim` is ticket-protected and fine. | `src/worker/daemon.rs:155`, `src/worker/server.rs` |
| **The cloud metadata service** (`169.254.169.254`, `fd00:ec2::254`) | The instance role's credentials. IMDSv2 does not help: a host-network task is zero hops from the instance. | (host networking) |
| **Other allocations' datapath listeners** | Every host-mode allocation's upstream ports (`127.0.0.1:<port>`), VIPs (`127.42.<b>.x`), and resolver (`127.42.<b>.1:53`) live on the host's loopback. The datapath attributes a connection to the allocation that owns the listener, not to the process that connected (`src/worker/netd/dns.rs:4-10`, `src/worker/forwarder.rs:1024-1029`). A neighbor dials through another allocation's egress policy and wears its mesh identity at the remote ingress. | `src/worker/forwarder.rs:1601` |
| **Any other host daemon** | sshd, node exporters, a Docker socket over TCP, anything on loopback or the node's IPs. | (host networking) |

**`gated` mode does not close this today, and the docs say it does.**
`docs/network-handlers.md:556` says gated "cannot be bypassed on Linux for
`container` and `exec`." The namespace itself holds, but its egress proxy is
a door to the host:

- The proxy dials from the worker's host namespace
  (`src/worker/netd/proxy.rs:20-21`), and nothing in the datapath refuses
  loopback, link-local, or node-local destinations. A grep for
  `is_loopback`, `is_link_local`, or `169.254` in `src/worker` finds nothing.
  `curl -x 127.0.0.1:1080 http://127.0.0.1:4747/` from a gated task reaches
  the host's manager.
- A gated allocation with no policy allows everything through the proxy
  (`docs/network-handlers.md:602-604`, `src/worker/netd/policy.rs:198-201`).
- CIDR rules never see hostnames. All three proxy paths set `ip` only when
  the target is an IP literal (`src/worker/netd/proxy.rs:512`, `:621`,
  SOCKS at `:879`), then let the host resolve the name at dial time. So the
  documented metadata rule (`{ cidr = "169.254.0.0/16", action = "deny" }`,
  `docs/network-handlers.md:118`) is bypassed by any name that resolves to
  `169.254.169.254`, by `localhost`, or by a decimal literal like
  `2130706433` that `host.parse::<IpAddr>()` rejects and `getaddrinfo`
  accepts.
- An `exec` task enters the gated namespace with `setns` while still holding
  the worker's privileges (`src/task_plugins/exec.rs:647-658`). Without a
  `user`, it stays root with every capability and can `setns` straight back
  to the host namespace. Gated `exec` is a boundary only for a non-root task.

**What an in-container compromise is worth** (separate from the network).
The Linux OCI bundle (`src/task_plugins/oci_bundle.rs:564-679`), used by
youki (the default), runc, and crun:

- It runs the image's `User`, which is root when the image says nothing
  (`oci_bundle.rs:329-336`), and there is **no user namespace**, so container
  root is host uid 0 (`oci_bundle.rs:613-621`).
- It keeps only `CAP_AUDIT_WRITE`, `CAP_KILL`, and `CAP_NET_BIND_SERVICE`, with
  `noNewPrivileges` (`oci_bundle.rs:646-652`). That is tighter than Docker's
  default of 14 capabilities, so capabilities need no work.
- It has **no seccomp profile** (no `linux.seccomp` in the spec, and OCI runtimes
  apply only what the spec asks for), no AppArmor or SELinux label, and no
  cgroup namespace.
- It has a writable rootfs, masks the usual `/proc` paths, and mounts `/sys`
  read-only.

So the whole kernel syscall surface is open to a process that is host root
in every way except its capability set. Apple `container` on macOS boots a
VM per container, so a compromise there lands in a guest kernel, not the
Mac. Its VMs sit on a NAT network and reach the Mac at the gateway
(`192.168.64.1`, `internal_docs/development.md:84`), which reaches anything
the Mac binds on all interfaces.

**`exec` tasks are host processes.** With no `user`, an `exec` task runs as
the worker's user, which is root on a real node. It can read the worker's
node token, the co-located manager's store (with the cluster CA key and KV
keyring), and `~/.legion/token`. `privateTmp`, capability drops, and
`noNewPrivileges` are opt-in and off by default
(`src/task_plugins/exec_isolation.rs:27-50`). The user switch calls
`setgid` and `setuid` but never `setgroups`, so a task keeps the worker's
supplementary groups (`src/task_plugins/exec.rs:679-687`). The ACL model
already calls `exec` "host-level" (`docs/task-runtime-exec.md:119-122`), but
anyone with `job:write` can submit one.

Put together: on any Legion cluster today, any task can take over the
cluster, and on any cloud host, any task can take the instance's role.

## 2. Prior art

### Control plane to node

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
    exists because some control planes cannot reach nodes. See calls entry 3 for
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

### API authentication by default

- **Nomad and Consul** ship ACLs off. Turning them on is a documented
  procedure, and `nomad acl bootstrap` is open until the first call succeeds,
  so the operator races anything else on the network to it.
  [Nomad ACL bootstrap](https://developer.hashicorp.com/nomad/commands/acl/bootstrap).
- **Kubernetes** has no "auth off" mode. `kubeadm init` writes an admin
  kubeconfig to `/etc/kubernetes/admin.conf`, and k3s writes
  `/etc/rancher/k3s/k3s.yaml` with mode `0600`.
  [k3s cluster access](https://docs.k3s.io/cluster-access).
  - Take: **mint the first admin credential at first start and write it to a
    root-only file.** No open bootstrap endpoint, no race, nothing extra to
    type on a single box.

### Workload network isolation

- **Kubernetes** gives every pod its own network namespace, and
  `hostNetwork: true` is the opt-out. The
  [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
  `baseline` profile forbids `hostNetwork`, `hostPID`, and privileged pods.
  `restricted` adds `runAsNonRoot`, dropping all capabilities, and the
  `RuntimeDefault` seccomp profile.
  - Take: a private namespace by default, the host network as an explicit
    opt-out that an admin can forbid.
- **Cloud metadata.** EKS recommends IMDSv2 with a hop limit of 1, so a
  pod's token request dies at the bridge hop
  ([EKS best practices](https://docs.aws.amazon.com/eks/latest/best-practices/identity-and-access-management.html),
  [IMDS options](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-IMDS-existing-instances.html)).
  GKE's [metadata concealment](https://cloud.google.com/kubernetes-engine/docs/how-to/protecting-cluster-metadata)
  intercepted pod traffic to the metadata server before Workload Identity
  replaced it. Both stop only pods on the pod network: a `hostNetwork` pod
  is zero hops away and talks to the real endpoint.
  - Take: block the metadata service in the datapath, by address, after name
    resolution. Reject the hop limit as the only defense, since it does
    nothing for host-network tasks or IMDSv1, and Legion does not own the
    instance's metadata options anyway. Recommend it in the public doc as
    defense in depth.
- **Nomad.** The group-level `network` block defaults to `mode = "host"`,
  and `bridge` is opt-in. The Docker driver uses Docker's own `bridge`
  network unless told otherwise. `raw_exec`, which runs commands as the
  agent's user with no isolation, is disabled by default, and the `exec`
  driver runs as `nobody` by default.
  [Nomad networking](https://developer.hashicorp.com/nomad/docs/networking),
  [raw_exec](https://developer.hashicorp.com/nomad/docs/drivers/raw_exec).
  - Take: an unisolated host-process runtime is a privilege, not a default
    right. Legion's equivalent of `raw_exec` being off is that host-level jobs
    need the `host` grant (§6.16).
  - Reject: `bridge` (a veth and NAT per allocation) as Legion's default. It
    keeps UDP and non-proxy-aware tools working, but egress policy stays
    cooperative and it is a second datapath. See calls entry 2.

### Container privileges

- **Docker** applies a [default seccomp profile](https://docs.docker.com/engine/security/seccomp/)
  that blocks about 44 syscalls, keeps 14 capabilities, and offers
  [`userns-remap`](https://docs.docker.com/engine/security/userns-remap/),
  which maps every container to one shared subordinate range and is off by
  default.
  - Take: Docker's default seccomp profile as is (Apache-2.0). It is the
    most widely tested profile there is.
  - Reject: one shared range for all containers. A container that escapes its
    mount namespace owns every other container's files.
- **Kubernetes** [user namespaces](https://kubernetes.io/docs/concepts/workloads/pods/user-namespaces/)
  (`hostUsers: false`) give each pod its own 65,536-id range, using idmapped
  mounts for volumes.
  - Take: per-allocation ranges and idmapped volume mounts.
- **gVisor** ([docs](https://gvisor.dev/docs/)) runs a user-space kernel as
  the OCI runtime (`runsc`), which cuts the host kernel surface to a small
  set of syscalls, at a cost in syscall-heavy workloads and compatibility.
  - Reject as the default. Legion's microVM `sandbox` runtime is the strong
    boundary for untrusted code. `LEGION_OCI_RUNTIME=runsc` may work with
    Legion's bundles, which is open question 8.

## 3. Jobs to be done

**Cluster operator**
- When I add a second machine, I want `legion join` to just work, so I can grow
  past one box without learning a TLS toolchain.
- When I open a port between my managers and workers, I want to know that
  only my managers can drive the workers through it, so I can sleep at night.
- When a worker is misconfigured or firewalled, I want the cluster to tell me
  which node and which port, so I can fix it before someone needs a shell.
- When I start a manager for the first time, I want it locked by default and
  the admin key handed to me, so a fresh cluster is never open to whoever
  finds it first.
- When I run jobs on a cloud instance, I want them unable to borrow the
  instance's cloud credentials, so one bad image can't reach my whole account.

**Developer on a laptop**
- When I start a manager with a worker on my laptop, I want the CLI to work
  without setting anything up, so the safer default costs me nothing.

**On-call responder**
- When a task on some node is misbehaving, I want logs and a shell from any
  manager I happen to be pointed at, so I can debug without hunting for the
  node.

**Security / platform admin**
- When I turn on ACLs, I want every path to a running task to go through
  them, so the audit trail and the "no exec" switch mean something.
- When I hand out a join token, I want a stolen or spoofed manager address to
  be useless to an attacker, so joining is safe on an untrusted network.
- When I let teams run containers, I want a compromised container to be stuck
  inside its own network and its own user ids, so one bad dependency can't
  reach the cluster, the host, or its neighbors.
- When someone needs a job that runs on the host itself, I want that to be a
  permission I grant on purpose, so "can deploy" doesn't quietly mean "is
  root on every node."

**Job author**
- When my frontend and API land on different machines, I want the mesh to
  connect them, so placement is not my problem.
- When my container is refused a connection, I want the error to say what
  refused it and what to add, so I can fix the job in one try.
- When my job really needs the host's network, I want one line that says so,
  so I'm not fighting the defaults.

**Autonomous agent**
- When I call `legion_exec_run` or `legion_job_logs` over MCP, I want them to
  work on whichever node the task landed on, so my debugging loop does not
  depend on topology.
- When I run code I just wrote inside a container, I want it unable to touch
  the cluster I'm driving, so a mistake in my code can't become a mistake in
  my credentials.

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
4. **A bad image on a single box.** A developer runs `legion start manager
   --run-worker` on an EC2 instance and deploys a container whose image was
   compromised. The image tries `127.0.0.1:4747`, `127.0.0.1:4848`, the
   proxy with `CONNECT 127.0.0.1:4747`, `169.254.169.254`, and a name that
   resolves to it. Every attempt fails, and each failure names its reason:
   no route out of the namespace, or a `403` from `host-guard`. The CLI on
   the box still works, using the token the manager wrote at first start.
5. **A platform runs agent code.** A platform runs untrusted, agent-written
   code in Linux containers on a shared pool. Without any jobspec work, the
   containers are gated with no egress, run as unprivileged host ids under
   seccomp, and cannot reach a neighbor's datapath listeners. The platform
   opens `pypi.org` with one `egress.allow` line. A team that submits a job
   with `network.mode = "host"` is refused unless its token carries the
   `host` grant.

## 4. Goals and non-goals

### Goals

- Workers on separate hosts support exec, actions, logs, restart, service
  discovery, the mesh, and edge boot, with no extra configuration.
- Only a manager can call `PostWork`, `GetTaskLogs`, and `RestartWorker`.
  `ExecTask` admits a manager or, where the worker opts in, a holder of a
  valid exec ticket. Both hold regardless of ACL mode, because the manager's
  private key never leaves the managers and tickets are signed by the
  cluster.
- A manager talks to a worker only after verifying that worker's identity
  matches the node it means to reach.
- A joining worker verifies the manager against a CA pinned in its join token.
- No node private key crosses the network.
- **A new cluster requires a credential for every API call.** The first
  management token is minted at first start and written to a root-only file.
  No open bootstrap endpoint exists.
- **No allocation reaches its own node's control plane, the cloud metadata
  service, or another allocation's datapath listeners** through Legion's
  datapath, and on Linux, not through the kernel either unless the task is
  root on the host.
- **Linux containers are network-isolated by default.** `host` networking is
  an explicit opt-in that needs the `host` grant.
- **A Linux container's root is not host root.** User namespaces, a default
  seccomp profile, and a cgroup namespace are the default.
- **`exec` is honest.** It stays a host-level runtime, the docs say what that
  means, gated `exec` refuses to run as root, and submitting it needs the
  `host` grant.
- A spot check that tries every path in §1.2 ships as a test and as a job an
  operator can run on a real cluster.
- `--run-worker` and every single-box flow work with nothing new to
  configure.

### Non-goals

- **Raft transport encryption.** Peers already authenticate with the cluster
  secret (`src/storage/raft/driver.rs:1884`, `src/storage/raft/server.rs:70`),
  but in plaintext. The manager certificates this plan introduces are the
  building block for that work. It gets its own plan.
- **TLS by default on the user-facing API (CLI, UI, MCP).** Phase 4 encrypts
  the worker's connection to the manager. What the CLI does is a separate,
  user-visible decision tracked in `internal_docs/TODO.md` ("TLS is
  autoconfigured and auto-rotated").
- **Workers behind NAT** that managers cannot dial. The mesh needs
  node-to-node reachability anyway, so a reverse tunnel would not make such a
  worker fully functional (calls entry 3).
- **External or pluggable CAs** (Vault PKI, ACME). The CA stays built in.
- **Registration through a raft follower.** `WorkerRegister` is not forwarded
  to the leader today, so a worker whose token names a follower fails to
  register. That is a separate bug, filed in `internal_docs/TODO.md`.
- **Transparent capture in gated namespaces.** Gated tasks keep using the
  proxy variables. The gated-sandbox plan defers the same work.
- **Stopping a root `exec` task.** A process that is root on the host can
  remove any guard Legion installs. The plan makes that explicit and makes
  submitting one a privilege. It does not pretend to contain it.
- **AppArmor and SELinux profiles.** Distro-specific, and seccomp plus user
  namespaces is the floor that works everywhere.
- **macOS as a host for untrusted code.** Apple `container` runs a VM per
  container, which is already a stronger boundary than a Linux container,
  and the manager binds loopback by default. The guard here is Linux-only
  (`nftables`), and macOS workers are documented as development hosts.
- **Hiding other allocations' advertised ports.** A gated task that reaches
  a neighbor's service on the node's IP goes through that service's ingress
  chain, the same as a caller from another node. That is service traffic,
  not a control-plane leak.

## 5. What exists today

### Worker plane

- **Worker listener.** Plain gRPC on `127.0.0.1:<LEGION_WORKER_PORT|4848>`
  (`src/worker/daemon.rs:75-80`, `:155`, `:318-320`). No TLS, no interceptor.
- **Advertised address.** `workerAddress` is hardcoded to loopback
  (`src/worker/daemon.rs:376-379`). The simulator alone takes
  `--advertise-host` (`src/cmd/start.rs:110-118`, `src/worker/simulate.rs:66-78`).
- **Manager-side clients.** Seven places build a `WorkerClient`, each with its
  own copy of `worker_address_for_node`:
  - dispatch (`src/manager/dispatcher.rs:40-54`, `:362`, `:464`, `:502`)
  - exec (`src/manager/exec.rs:217-223`, `:260`)
  - logs and restart (`src/manager/server.rs:9561`, `:9623-9644`,
    `:6879-6882`)
  - edge boot from the CLI (`src/cmd/sandbox.rs:109`)
- **Dispatch is best-effort by design.** A failed push costs one heartbeat
  interval, because the heartbeat reply carries the desired set
  (`src/manager/dispatcher.rs:139-146`).
- **The mesh CA.** One CA per cluster, generated on first use and replicated
  in the control lane (`net_mesh_ca`). It signs node certs with SANs
  `<node>.node.legion` and `legion://node/<id>`, valid 24 hours and refreshed
  after 8 (`src/manager/mesh_ca.rs`). The manager generates the key pair and
  returns both halves (`:72-75`).
- **Node cert issuance on the worker is lazy.** The worker asks for one only
  once something on the node needs mesh TLS (`src/worker/daemon.rs:1962-1990`).
- **Worker credentials.** With ACLs on, registration needs the join secret and
  mints a per-node token. Every worker-plane RPC on the manager checks that the
  token's node matches the request (`docs/auth-and-acls.md:312-325`).
- **The join token** is `lgnjoin_` plus base64url JSON
  `{v, addr, secret, workerType}` (`src/structs/token.rs:30-40`). It carries no
  trust anchor.
- **Edge boot's ticket verifier** fetches the JWKS from
  `<manager addr>/.well-known/jwks.json`, which is plain `http://` today, and
  is built only when `edgeBoot.acceptClaims` is on
  (`src/worker/daemon.rs:284-310`, `:488-519`).
- **`StartClaim`** is authorized by the boot ticket alone and is meant to be
  called by an external router, not a manager (`proto/legion.proto:7289`).
- **TLS dependencies** are already in the tree: `tonic` with `tls`, `rustls`,
  `rcgen`, and `x509-parser` (`Cargo.toml:43-78`).
- **The protocol version** is `PROTOCOL_VERSION = 2` (`src/constants.rs:13`).

### Local reachability

- **Manager listeners.** gRPC (and raft transport) on `server.host:4747`,
  the exec WebSocket on `+1`, telemetry on `+2`, all on `server.host`, which
  defaults to `127.0.0.1` (`src/manager/daemon.rs:432-477`,
  `src/structs/config.rs:1664-1679`). Workers on other hosts need a reachable
  `server.host`, so multi-host managers bind beyond loopback.
- **ACLs.** Off by default (`src/structs/config.rs:1020-1028`, `:1129-1131`).
  `--run-worker` with ACLs on already mints an in-process join secret
  (`src/manager/daemon.rs:350-360`). `BootstrapAcl` is open while no
  management token exists (`src/manager/server.rs:8746-8759`). The CLI reads
  `LEGION_TOKEN`, then `~/.legion/token` (`src/cmd/helpers.rs:50-62`).
  `catalog()` lists `acl.bootstrap` (`src/cmd/capabilities.rs:727-729`).
- **The ACL `host` flag** exists on `exec` rules only, marking exec into
  host-level runtimes (`src/structs/acl.rs:174-178`, `:224-226`,
  `docs/exec-and-actions.md:150-164`).
- **Network mode.** `auto` resolves to `host` for every runtime
  (`src/structs/network.rs:39-49`), at placement, in
  `effective_network_mode` (`src/storage/store.rs:12034-12053`), where a
  `require` policy can impose a mode.
- **Gated namespaces.** Created per allocation at
  `/var/run/netns/legion-<alloc>` with loopback only. Containers join by path
  (`src/task_plugins/oci_bundle.rs:619-621`, `src/task_plugins/container.rs:840-868`).
  `exec` tasks `setns` before the user switch
  (`src/task_plugins/exec.rs:647-658`). The worker binds listeners inside with
  `netns::within` (`src/worker/netd/netns.rs:219-250`).
- **The egress proxy** judges gate, then pending policy, then the egress
  chain (`src/worker/netd/proxy.rs:294-343`), and dials with
  `TcpStream::connect((host, port))` from the host namespace (`:544`, `:731`,
  `:913`).
- **Gated ingress.** Only a task with an ingress chain gets private twin ports
  and ingress listeners (`src/storage/store.rs:12182-12195`,
  `src/structs/network.rs:940-951`, `src/worker/daemon.rs:2417-2446`). A gated
  task without one binds its declared ports inside its namespace, where
  nothing outside can reach them.
- **Gated DNS.** A gated task gets a resolver only with a VIP block, which
  comes with upstreams (the gated-sandbox plan §6.5 records this gap).
- **cgroups.** Tasks get a `legion/{share,reserve}/legion-<alloc>` leaf only
  with `resourceEnforcement.enabled` (`src/worker/resources.rs:180-258`).
- **The OCI bundle** is generated by Legion from the image layout
  (`src/task_plugins/oci_bundle.rs:67-115`, `:564-679`), and the worker
  extracts layers itself (`:211-268`).
- **`exec` hardening** is opt-in (`src/task_plugins/exec_isolation.rs:27-50`),
  applied in privilege order around the user switch
  (`src/task_plugins/exec.rs:618-718`).

## 6. Proposed design

Sections 6.1 through 6.9 are the worker plane. Sections 6.10 through 6.17
are local workload isolation.

### 6.1 The worker listener is mutual TLS, always

The worker serves the `Worker` service over TLS using its node certificate,
on every platform and in every mode, including `--run-worker`. There is no
plaintext worker listener and no knob to turn one on.

The listener requests a client certificate but does not require one at the
handshake, because `StartClaim` callers and direct-exec clients hold tickets,
not certificates. Each RPC declares the caller classes it admits:

| RPC | Caller classes | Check |
| --- | --- | --- |
| `PostWork`, `GetTaskLogs`, `RestartWorker`, `Probe` | `manager` | client cert chains to the cluster CA and carries a `legion://manager/<id>` URI SAN |
| `ExecTask` | `manager` or `ticket` | `manager`: as above. `ticket`: no client certificate is required. The first frame carries an exec ticket (`aud = legion:exec`) verified offline against the cluster JWKS, as `StartClaim` verifies boot tickets. Admitted only when the worker sets `exec.direct = true`. |
| `StartClaim` | `ticket` | the boot ticket (`aud` for edge boot), as today. A client cert is ignored. |

A node certificate is *not* a manager certificate. The CA signs both, so the
role SAN is what the worker checks, exactly like Nomad's
`verify_server_hostname`.

The `ticket` class on `ExecTask` is the
[sandbox-exec plan's](sandbox-exec-plan.md) direct exec (its §6.3 and §6.4).
That plan owns the ticket's claims, minting, replay, and revocation. This plan
owns the rules the listener applies:

- **Two classes on one RPC.** `ExecTask` tries the `manager` class when a
  client certificate is present and the `ticket` class otherwise. A request
  with a certificate that is not a manager certificate is refused, never
  downgraded to the ticket path.
- **The JWKS verifier runs on every worker that admits tickets**, not only
  when `edgeBoot.acceptClaims` is on. It is built when either
  `edgeBoot.acceptClaims` or `exec.direct` is true. The cache, refresh, and
  clock-skew settings are shared, and the spent `jti` sets are per audience.
- **The JWKS must arrive over TLS that verifies the pinned cluster CA.**
  Today it is fetched over plain `http://` (§1.1), so an on-path attacker
  could swap the keys and forge tickets. From Phase 4 on, the JWKS fetch
  uses the TLS channel of §6.8, and a worker refuses to build a ticket
  verifier over a plaintext non-loopback URL, failing at start with:

  ```text
  error: exec.direct = true needs the cluster JWKS over TLS, but the manager
  address is http://10.0.0.5:4747
  hint: upgrade managers to a release with the encrypted worker channel
  ```

  This makes direct exec depend on Phase 4 as well as Phase 1 (calls entry
  13). Edge boot keeps today's exposure until Phase 4 lands, and its public
  doc says so.

A direct call without a manager cert:

```console
$ grpcurl -cacert ca.pem 10.0.1.7:4848 legion.Worker/GetTaskLogs
ERROR:
  Code: Unauthenticated
  Message: GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate
```

`ExecTask` with neither a manager certificate nor a ticket:

```console
ERROR:
  Code: Unauthenticated
  Message: ExecTask admits a manager certificate or an exec ticket; this
  request carried neither
```

**The classification test.** `every_worker_rpc_is_classified` parses
`proto/legion.proto` and fails the build if a `Worker` RPC has no caller
class, the same way `tests/acl_endpoints.rs` guards the `Manager` service. It
reads a table that allows more than one class per RPC:

```rust
("PostWork", &[Caller::Manager]),
("ExecTask", &[Caller::Manager, Caller::Ticket]),
("StartClaim", &[Caller::Ticket]),
```

An RPC missing from the table still fails the build. A test request per class
checks the table against the handlers: each listed class is admitted with a
valid credential, and each unlisted class is refused.

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
- refuses an `http://` address with an error naming the worker's protocol
  version. There is no plaintext path (calls entry 16).

The CLI's edge-boot path (`legion sandbox start --node https://...`) verifies
the node cert the same way. It fetches the CA with the caller's token through
the existing `legion net ca show`.

### 6.4 Workers generate their own key

The worker generates its key pair locally and sends a CSR. The manager signs
the public key with SANs it chooses (`<node>.node.legion`,
`legion://node/<id>`), ignoring whatever the CSR requests. The private key
never crosses the network.

- `IssueNodeCertificateRequest` gains a required `csr_pem`. The
  manager-generated key path is removed, along with the `key_pem` reply field.
- Issuance moves from lazy to eager. The worker asks for its certificate right
  after registration, because the listener needs it.
- The key and certificate persist under the worker's state dir, mode `0600`,
  so a worker restarted during a manager outage can still serve `StartClaim`
  (edge boot must work with no manager on the critical path). The mesh reuses
  the same identity.

### 6.5 The join token pins the CA

`legion join-token create` embeds the CA's SHA-256 in the token:

```json
{ "v": 2, "addr": "https://10.0.0.5:4747", "secret": "ljt_...", "workerType": "default",
  "caHash": "sha256:9c1e4f..." }
```

The worker checks every CA it receives from the manager against the pin. A
mismatch stops the join before the worker sends anything sensitive:

```console
$ legion join lgnjoin_eyJ2IjoyLC...
error: the manager at https://10.0.0.5:4747 presented cluster CA sha256:51aa07...,
but this join token pins sha256:9c1e4f...
hint: the address may not be your manager, or the CA was rotated after this
token was minted. Mint a new token with `legion join-token create`.
```

The token version goes to 2 and `caHash` is required. A version-1 token is
refused with a hint to mint a new one. There is no trust-on-first-use path.

### 6.6 Reachable by default

The worker binds `0.0.0.0:4848` by default. This is safe only because 6.1 makes
every privileged RPC need a manager certificate or a ticket. Two env vars,
mirroring the manager's `server.host` and `server.advertiseAddr`, cover the
rest:

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

Phase 4 closes the plaintext channel workers use for heartbeats,
registration, and the JWKS.

- When the manager serves operator TLS (`server.tls`), workers use it. This
  fixes the `TODO` in `create_manager_client`. The worker verifies the server
  cert against the system roots or the CA pinned in its token.
- When it does not, the manager answers TLS handshakes on the same port with a
  certificate from the cluster CA, and keeps answering plaintext for the CLI
  and UI. It tells them apart by the first byte of the connection: a TLS
  ClientHello starts with `0x16`, and HTTP/2 or HTTP/1 never do. Workers always
  speak TLS and pin the CA. Join tokens from such a manager advertise
  `https://`.
- The worker's JWKS source (edge boot and direct exec) uses the same pinned
  TLS channel (§6.1).

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

### 6.10 ACLs are on by default, and the first token is minted at first start

`acl.enabled` defaults to `true`. When the manager starts with ACLs on and no
management token exists, it mints one itself. In raft mode the first leader
does this once, as a replicated write. The secret goes to two places, both
mode `0600`:

- `<dataDir>/bootstrap-token`, always
- `~/.legion/token` for the user running the manager, only when that file
  does not exist, so the CLI on the same box works with nothing to set up

The manager prints where it wrote the token, once, and never logs the secret:

```text
INFO ACLs are on. Created the initial management token (accessor 7c1f...).
     Secret written to /var/lib/legion/bootstrap-token and ~/.legion/token (mode 0600).
     The CLI on this machine uses it automatically. Treat it like a root password.
```

On a laptop nothing changes: `legion start manager --run-worker`, then
`legion job run ...` works, because the CLI reads `~/.legion/token`. The UI's
login page takes the same secret.

**`BootstrapAcl` and `legion acl bootstrap` are removed.** An open endpoint
that works "whenever no management token exists" is a race any local task can
win. What it did moves to config and startup:

| Before | After |
| --- | --- |
| `legion acl bootstrap` | nothing to run: the token exists after first start |
| `legion acl bootstrap --secret <s>` (IaC pre-seed) | `acl = { bootstrapSecretFile = "/etc/legion/bootstrap-secret" }`, read once at first start |
| Recovery: delete the management rows, then `legion acl bootstrap` | delete the management rows, then restart the manager, which mints a fresh token |

**`acl.enabled = false` stays** as an explicit choice for a throwaway
cluster. The manager then logs a `WARN` at every start, the UI keeps its
banner, and `legion acl whoami` says so.

Workers already handle ACLs: `--run-worker` mints an in-process join secret,
and `legion join` uses a join token from the authenticated
`CreateJoinToken`.

Why this and not the other options (calls entry 1):

- **A Unix socket for local use** protects nothing in scope. Multi-host
  clusters need TCP, and a root `exec` task can open any socket.
- **A token only for non-loopback callers** protects nothing either. A
  host-network task is a loopback caller, and the gated proxy dials from the
  worker's own host namespace.
- **Denying allocation cgroups at the firewall** is in this plan (§6.13), but
  as defense in depth. It can't protect a remote manager, a node without
  `nftables`, or macOS, and root tasks can remove it.

### 6.11 The host guard: a floor under every allocation's egress

The host guard is a fixed set of destinations no allocation may reach on its
own: its node, the cloud's link-local services, and other allocations'
datapath listeners. It sits below policy. A job's own rules and stored
policy objects can't widen it. Only the node's operator can, in worker
config. The datapath enforces it for every connection Legion dials on an
allocation's behalf (this section), and the kernel enforces it for host-mode
tasks (§6.13).

**What it denies by default:**

| Destination | Why |
| --- | --- |
| loopback (`127.0.0.0/8`, `::1`) and unspecified (`0.0.0.0/8`, `::`) | the node's own services, including Legion's ports and other allocations' upstreams |
| every address on the node's interfaces | the same services by another name. Refreshed when interfaces change |
| link-local (`169.254.0.0/16`, `fe80::/10`) | AWS, GCP, Azure, OCI, and DigitalOcean metadata, ECS task metadata (`169.254.170.2`), the VPC resolver |
| `fd00:ec2::254`, `100.100.100.200` | AWS IPv6 metadata, Alibaba Cloud metadata |

IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are judged as the IPv4
address they map.

**Resolve, judge, then dial the address that was judged.** The proxy and the
external-upstream dialer resolve a hostname first, judge every resolved
address against the host guard and the allocation's CIDR rules, and dial
only an address that passed. A name rule still matches on the name. This
closes the hostname, `localhost`, decimal-literal, and DNS-rebinding bypasses
in §1.2, because there is no second resolution between the verdict and the
dial (calls entry 14).

**Order of checks** in `judge`: the boot gate, then the host guard, then
pending policy, then the egress chain. A host-guard denial is final.

**The surface a task sees** is the existing denial shape with a new source:

```console
$ curl -sS -x $HTTP_PROXY http://169.254.169.254/latest/meta-data/   # inside a gated task
curl: (56) CONNECT tunnel failed, response 403

HTTP/1.1 403 Forbidden
Legion-Denied-By: host-guard
Legion-Denied-Reason: 169.254.169.254 is link-local (cloud metadata); allocations may not reach it
```

The `net.denied` event and `legion net policy test` name `host-guard`:

```console
$ legion net policy test web --to http://instance-data.ec2.internal/
deny  host-guard  instance-data.ec2.internal resolves to 169.254.169.254 (link-local)
      Hint: the host guard is set on the worker (hostGuard.allow), not in job policy
```

**Operator overrides**, in worker config, for the rare host service an
allocation should reach on purpose:

```nickel
hostGuard = {
  # Opened on purpose: the node exporter, for a metrics-scraping job.
  allow = ["10.0.1.7:9100"],
  # Extra destinations to treat like metadata.
  deny = ["192.0.2.10/32"],
},
```

`allow` takes `ip:port` or `cidr:port` only, never a bare range, so an
override opens one service and not the node. The defaults can't be removed,
only punched through one port at a time.

### 6.12 Linux containers are gated by default

`network.mode = "auto"` (the default when unset) resolves at placement, as it
does now, with the node's OS as a new input:

| Task | Node | Resolves to |
| --- | --- | --- |
| `container` | Linux | `gated` |
| `container` | macOS | `host` (Apple `container` already gives each container a VM on its own network, and macOS has no network namespaces) |
| `exec` | any | `host` (§6.15) |
| `sandbox` | any | per the [gated-sandbox plan](gated-sandbox-checkpoints-plan.md) §6.5 |

`network.mode = "host"` is the explicit opt-in and needs the `host` grant
(§6.16). An explicit `gated` is never downgraded.

**Gated means deny by default.** A gated container that no task block and no
policy object covers denies all egress with the `gated-default` source. This
is the gated-sandbox plan's Phase 2 rule for every gated runtime. If that
phase hasn't shipped, Phase 3 here ships the `gated-default` verdict and the
lower-case proxy variables for all gated runtimes. Opening a host is one line:

```nickel
network = { egress = { allow = ["api.stripe.com:443"] } },
```

Today's "observe everything, block nothing" is also one line, and the host
guard still applies under it:

```nickel
network = { egress = { policy = "allow", accessLog = true } },
```

**Three gaps in gated mode close in the same phase**, because a default has
to work for an ordinary service:

- **Every gated allocation gets ingress listeners for its declared ports.**
  Today only a task with an ingress chain gets private twins (§5). A gated
  task without one gets twins and an allow-all ingress listener, so its
  service is reachable from other nodes and from its health checks.
- **Every gated allocation gets a resolver.** Inside a namespace the resolver
  binds a fixed address (`127.0.0.53`), so it spends no VIP block and does not
  count toward the node's 256 blocks.
- **`legion job validate` and `job plan` warn** when a gated task allows no
  egress, so an author and an agent see it before the first `403`:

  ```text
  warning: task "web" is gated (the default for containers on Linux) and allows no
  egress. Outbound connections will be refused with `gated-default`.
  hint: add hosts to network.egress.allow, or set network.mode = "host"
  (needs the host grant)
  ```

**What gated costs a container**, stated in the public doc: no raw UDP (DNS
goes through the resolver), no ICMP, IPv4 only inside the namespace, and
tools that ignore the proxy variables have no route. That includes Java's
default HTTP client and most database drivers. A database outside the
cluster is reached through an external upstream (`connect.upstreams` with
`to`), which the datapath already supports. Calls entry 2 weighs this
against the alternatives.

### 6.13 The host guard for host-mode tasks, in the kernel

A host-mode task never touches the datapath unless it wants to, so §6.11 does
nothing for it. On Linux, the worker installs an `nftables` table that
applies the same floor to sockets owned by allocations, matched by cgroup:

```text
table inet legion_host_guard {
  set control_ports { type inet_service; elements = { 4747, 4748, 4749, 4848 } }
  set metadata_v4   { type ipv4_addr; flags interval; elements = { 169.254.0.0/16, 100.100.100.200 } }
  set metadata_v6   { type ipv6_addr; flags interval; elements = { fe80::/10, fd00:ec2::254 } }

  chain output {
    type filter hook output priority filter; policy accept;
    socket cgroupv2 level 1 "legion" jump allocation
  }

  chain allocation {
    ip daddr @metadata_v4 counter drop
    ip6 daddr @metadata_v6 counter drop
    fib daddr type local tcp dport @control_ports counter drop
    # One pair per allocation with datapath listeners, reconciled from the
    # desired set: the owner may reach its own VIP block and upstream ports.
    ip daddr 127.42.7.0/24 socket cgroupv2 level 3 "legion/share/legion-88213" accept
    ip daddr 127.42.7.0/24 counter drop
    ip daddr 127.0.0.1 tcp dport 20417 socket cgroupv2 level 3 "legion/share/legion-88213" accept
    ip daddr 127.0.0.1 tcp dport 20417 counter drop
  }
}
```

- **Every task gets a cgroup leaf**, `legion/{share,reserve}/legion-<alloc>`,
  on any cgroup v2 node, whether or not `resourceEnforcement` is on. Limits
  are still written only with enforcement. The guard needs the leaf to know
  whose socket it is looking at.
- **The control ports come from config**: the manager's `server.port`, `+1`,
  and telemetry port when co-located, and the worker's own port. Operator
  `hostGuard.allow` entries become `accept` rules ahead of the drops.
- **A drop in the output hook fails fast.** The task's `connect()` returns
  `EPERM` ("Operation not permitted") at once, not a timeout. The worker reads
  the counters on each heartbeat and reports them per allocation, so
  `legion net doctor` names `host-guard` as the cause.
- **The worker owns the table.** It creates it at start, reconciles the
  per-allocation rules with the datapath listeners, and deletes it at clean
  shutdown. It shells out to `nft -j` like the container runtime shells out to
  its binaries, so there is no new dependency.
- **What it does not cover, stated in the public doc:**
  - A task that is root on the host with `CAP_NET_ADMIN`, which is any `exec`
    task without a `user`, can delete the table (§6.15).
  - Other host daemons on the node's IPs, except Legion's own ports. Host
    mode is host networking. The guard keeps Legion's control plane, the
    metadata service, and other allocations' listeners out of reach, not
    every service an operator chose to run.
  - Nodes without cgroup v2 or without the `nft` binary. The worker reports
    `hostGuard = unavailable (<reason>)` as a node attribute, logs a `WARN`
    at start, and runs host-mode tasks anyway, because host mode is the
    explicit trust opt-in.

```console
$ legion node info wkr-1
...
Host guard     active (nftables, 4 allocations, 2 drops in the last hour)
```

### 6.14 A Linux container's root is not host root

Three additions to the generated OCI bundle, default on for every Linux
container:

- **A default seccomp profile.** Docker's default profile, embedded as JSON
  with its Apache-2.0 attribution, goes into `linux.seccomp`. It blocks about
  44 syscalls, including `mount`, `unshare` of new user namespaces, `bpf`,
  `kexec_load`, and `keyctl`.
- **A cgroup namespace**, so the container sees its own cgroup as the root
  and not the host's tree.
- **A user namespace per allocation.** Container ids `0-65535` map to a
  65,536-id range on the host that belongs to this allocation alone, from a
  pool the worker owns. The slot is recorded in worker state, so an in-place
  restart keeps it.
  - The worker extracts image layers itself (`oci_bundle.rs:211-268`), so it
    writes each file's owner already shifted into the range. There is no
    separate chown pass.
  - The task directory (`/legion/task`) is chowned to the range's root, the
    way `exec` hands its tree to `user`.
  - Volumes mount with idmapped mounts, so a volume reattached to a new
    allocation (with a new range) keeps working. A node or runtime that
    can't do idmapped mounts fails a volume-backed start with a named reason.
  - The three capabilities and `noNewPrivileges` stay as they are. Inside a
    user namespace they apply to that namespace only.

The surface is a `security` block on the container config, shown with its
defaults:

```nickel
config = {
  image = "postgres:17",
  command = ["postgres"],
  security = {
    userNamespace = true,   # false runs container root as host root
    seccomp = "default",    # "unconfined" removes the filter
  },
},
```

Either opt-out needs the `host` grant (§6.16), and a validate warning names
the cost. Both fields go into `configs/jobs/maximal.ncl`.

**One ordering problem to settle first.** Mounting `sysfs` inside a user
namespace needs the network namespace to belong to that user namespace. A
gated namespace the worker created belongs to the host's user namespace.
Phase 6 starts by testing two fixes on youki, runc, and crun: the worker
creates the gated namespace inside the allocation's user namespace, or the
bundle bind-mounts `/sys` read-only instead of mounting `sysfs`. It keeps
whichever works on all three, and records the result in the calls doc.

Apple `container` needs nothing here. Each container is a VM with its own
kernel.

### 6.15 `exec` tasks are host-level, and the docs say what that means

`exec` stays `host` by default. An `exec` task is a process on the host with
host paths, host users, and host binaries. Putting it in a namespace by
default would break what it is for and still not contain a root process.
The changes make its trust explicit instead:

- **Gated `exec` refuses to run as root.** A root process with
  `CAP_SYS_ADMIN` can `setns` out of its namespace, so a gated `exec` task
  with no `user`, or with `user = "root"`, is refused at submit:

  ```text
  Error: task "worker" sets network.mode = "gated" but runs as root.
  A root exec task can leave its network namespace, so gated would not hold.
  Hint: set user = "nobody" (or another unprivileged user), or use runtime = "container"
  ```

- **The user switch drops supplementary groups.** `setgroups(0, NULL)` runs
  before `setgid`, so a task no longer inherits the worker's groups.
- **The host guard covers non-root `exec` tasks** (§6.13), and the public doc
  says plainly that a root `exec` task can remove it.
- **`docs/task-runtime-exec.md` gets a "Trust" section**: `exec` without
  `user` is root on the node and can read the worker's credentials, and on a
  co-located manager, the cluster's. Run untrusted code in `container` or
  `sandbox`, never `exec`.

### 6.16 Host-level jobs need the `host` grant

Today any `job:write` token can submit an `exec` task, which is root on every
node it lands on. The ACL vocabulary already has the word for this: `host`
on `exec` rules marks exec into host-level runtimes. This plan extends it to
`job` rules. Submitting or updating a job with a host-level task needs a
`job:write` rule with `host = true`:

| Task | Host-level when |
| --- | --- |
| `exec` | always |
| `container` | `network.mode = "host"`, `security.userNamespace = false`, or `security.seccomp = "unconfined"` |
| `sandbox` | `hypervisor = "dev"` on Linux (an OCI bundle standing in for a VM, which `docs/task-runtime-sandbox.md:88-92` already says is not a security boundary) |

```nickel
# Deploy containers and sandboxes, but nothing that runs on the host:
{ resources = ["job"], actions = ["write"], names = ["team-a-*"] },
# Platform admins may deploy host-level jobs too:
{ resources = ["job"], actions = ["write"], names = ["*"], host = true },
```

A refusal names the reason:

```text
Error: PermissionDenied: job "backup" has a host-level task ("dump" uses runtime "exec").
Submitting it needs a job:write rule with host = true.
```

Management tokens have every grant, so single-box flows and the bootstrap
token are unaffected. The check runs where the scope is built on the write
path, after parsing, with the two-step `authenticate_acl` and
`acl::enforce` form AGENTS.md describes. A `require` policy that imposes
`mode = "host"` counts as host-level for every job it matches, so imposing it
needs `net:write`, which is already an admin grant.

### 6.17 macOS

macOS is a development host in this plan. What holds there:

- The manager binds `127.0.0.1` by default, which Apple container VMs can't
  reach. A manager bound to `0.0.0.0`, as the Linux-workers dev flow needs
  (`internal_docs/development.md:84`), is reachable from every container at
  `192.168.64.1`, which ACLs on by default (§6.10) makes safe.
- The worker binds `0.0.0.0:4848` after Phase 1, reachable from container VMs
  at the gateway, and protected by mTLS.
- There is no host guard and no gated mode. `exec` tasks are host processes.

The public doc says macOS workers are for development, and that untrusted
code belongs on Linux.

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto`
  - `IssueNodeCertificateRequest.csr_pem` (required string, new field number).
    `IssueNodeCertificateReply.key_pem` is removed, and `ca_pem` becomes a
    bundle during rotation.
  - `Worker/Probe(WorkerProbeRequest) returns (WorkerProbeReply)`, carrying the
    node id. It is manager-only.
  - `Manager/BootstrapAcl`, `BootstrapAclRequest`, and `BootstrapAclReply`
    are removed.
  - Proto comments on every `Worker` RPC name its caller classes, including
    `ExecTask: manager | ticket` and when the ticket class is admitted.
  - `NetDenial` and the denial events gain `host-guard` as a documented
    source value.
- `src/constants.rs`: `PROTOCOL_VERSION = 3`. Version 3 means "serves the
  mTLS worker plane and advertises a reachable address."
- `JoinToken`: `v = 2`, with a required `ca_hash` (`caHash` on the wire).
- Node record: `workerAddress` changes scheme to `https`. New config attrs for
  the probe result (`workerPlaneProbe = { ok, by, at, error }`) are written by
  the manager, not reported by the worker. New fingerprint attr
  `hostGuard = { state, reason, drops }`, reported by the worker.
- Store: the `net_mesh_ca` row gains a `next` CA and a rotation phase. Add a
  migration.
- Worker state dir: `identity/node.key` and `identity/node.pem`, mode `0600`.
  User-namespace slots per allocation in worker state.
- Manager config:
  - `acl.enabled` defaults to `true`
  - new `acl.bootstrapSecretFile`
  - `<dataDir>/bootstrap-token` is written at first start
- Worker config:
  - new `hostGuard = { allow = [...], deny = [...] }`
  - new `userNamespaces = { idPool = "<first id>-<last id>" }`, with a
    default the worker validates against `/etc/subuid` (open question 7)
  - `exec.direct` (from the sandbox-exec plan) builds the JWKS verifier
- ACL: `host` becomes valid on `job` rules (`src/structs/acl.rs:174-178`).
  `tests/acl_endpoints.rs` drops `BootstrapAcl`. `catalog()` drops
  `acl.bootstrap`.
- Job config:
  - `container.config.security = { userNamespace, seccomp }`, both into
    `configs/jobs/maximal.ncl`
  - `network.mode = "auto"` changes meaning for `container` on Linux. The
    comment in `maximal.ncl` ("`auto` is host today") is updated in Phase 3.

## 8. Implementation phases

Each phase ships and is useful on its own. They are numbered in the order to
ship them, most dangerous gap first. Phase 1 keeps its number because the
sandbox-exec plan depends on it by name.

**Phase 0: ACLs on by default.** §6.10.

- `acl.enabled` defaults to `true`. First-start minting, the two token files,
  `acl.bootstrapSecretFile`, the startup log line.
- Remove `BootstrapAcl`, `legion acl bootstrap`, its catalog entry, and its
  `acl_endpoints` row.
- Update test harnesses that assume ACLs off to set `acl.enabled = false`
  explicitly or use the minted token. Update the AWS benchmark scripts to
  read `bootstrap-token`.
- Docs: `auth-and-acls.md` (quick start, recovery, the removed command), the
  README quickstart, `ui-architecture.md` (login), `agent-readiness.md` (MCP
  needs a token by default).

Closes: any task, on any node, owning the cluster through the manager API.

**Phase 1: an authenticated, reachable worker plane.** §6.1 through §6.6 and
§6.9, plus the simulator.

- Manager certificates. The `WorkerPlane` client factory replaces all seven
  call sites.
- The mTLS worker listener with per-RPC caller classes, the `ticket` class on
  `ExecTask` behind `exec.direct`, the JWKS verifier on every ticket-accepting
  worker, and the multi-class classification test.
- CSR issuance, eager at registration, persisted. The manager-generated key
  path is removed.
- The CA pin in join tokens (token v2).
- Bind and advertise defaults and overrides.
- The simulator serves one listener for its whole fleet, so it registers its
  nodes with a shared fleet identity (open question 4).
- Two-step CA rotation, because Phase 1 makes the CA load-bearing.
- Docs: `architecture-managers-workers.md` (ports, what each side dials),
  `auth-and-acls.md` (remove the false transport-encryption claim, describe the
  worker plane and its caller classes), `edge-boot.md` (`https://` and the
  CA), `network-handlers.md` (rotation), `upgrades.md` (protocol 3),
  `data-directory.md`, and the README's "Add more machines."

Closes: the multi-host launch blocker, and any process on a node driving
`ExecTask`, `GetTaskLogs`, `PostWork`, or `RestartWorker`.

**Phase 2: the host guard in the datapath.** §6.11.

- The guard's address classes, refreshed node addresses, and the worker's
  `hostGuard` config.
- Resolve-then-judge-then-dial on all three proxy paths and on external
  upstreams.
- The `host-guard` verdict source in events, `net policy test`, and
  `net doctor`.
- Docs: correct the "cannot be bypassed" claims in `network-handlers.md`,
  `task-runtime-container.md`, and `task-runtime-exec.md` to describe the
  guard. Add a "Cloud metadata" section that also recommends IMDSv2 with a hop
  limit of 1 as defense in depth.

Closes: gated tasks reaching the host's control plane, the metadata service,
and other allocations through the proxy, and every hostname bypass of CIDR
rules.

**Phase 3: gated by default for Linux containers.** §6.12.

- `auto` resolves to `gated` for `container` on Linux nodes, using the node's
  OS at placement.
- Twins and allow-all ingress listeners for every gated allocation's ports.
- A resolver for every gated allocation at a fixed in-namespace address.
- `gated-default` and the lower-case proxy variables for every gated runtime,
  if the gated-sandbox plan's Phase 2 hasn't shipped them.
- The validate and plan warning.
- Benchmark: container start latency (p50, p99) before and after, since every
  container now gets a namespace. Numbers in the PR.
- Docs: `task-runtime-container.md` (the default, the costs, the opt-out),
  `network-handlers.md` (the `network.mode` table), `maximal.ncl` comment.

Closes: host-network containers reaching anything on the host or the
metadata service by default.

**Phase 4: an encrypted worker-to-manager channel.** §6.8.

- Workers honor operator `server.tls`.
- TLS detection on 4747 with cluster-CA certs when no operator cert is set.
  Join tokens advertise `https://`.
- The JWKS fetch moves to the pinned TLS channel, and ticket verifiers refuse
  a plaintext non-loopback JWKS URL (§6.1).
- Docs: `auth-and-acls.md` TLS section, `architecture-managers-workers.md`
  "What Is Not Implemented Yet," `edge-boot.md`.

Closes: secrets in heartbeat replies on the wire, and forged tickets through
a swapped JWKS.

**Phase 5: the host guard in the kernel.** §6.13.

- A cgroup leaf for every task on cgroup v2 nodes.
- The `nftables` table, reconciled per allocation, with counters on the
  heartbeat and the `hostGuard` node attribute.
- Docs: a "Host guard" section in `network-handlers.md`, the node-info field
  in `node-liveness.md` or the CLI docs, and the limits (root `exec`, other
  daemons, nodes without `nft`).

Closes: non-root host-mode tasks reaching Legion's ports, the metadata
service, and other allocations' loopback listeners.

**Phase 6: the container privilege floor.** §6.14.

- First, settle the `sysfs` ordering question on youki, runc, and crun.
- The seccomp profile, the cgroup namespace, per-allocation user namespaces,
  shifted extraction, idmapped volume mounts, and the `security` block.
- Docs: a "Security" section in `task-runtime-container.md` with the
  defaults and opt-outs, and the kernel and runtime floors.

Closes: container root being host root, and the open syscall surface.

**Phase 7: exec trust and the host grant.** §6.15 and §6.16.

- The gated-root refusal at submit, `setgroups` in the user switch.
- `host` on `job` rules, the host-level classification per runtime, and the
  refusal.
- Docs: the "Trust" section in `task-runtime-exec.md`, `auth-and-acls.md`
  (the extended `host` flag), `exec-and-actions.md` (cross-reference).

Closes: "can deploy" quietly meaning "is root on every node."

**Phase 8: reachability diagnostics.** §6.7.

- `Worker/Probe`, the node-record field, `legion node info` output, and the
  UI node page.
- `WORKER_UNREACHABLE` errors with hints on exec, logs, and restart. The same
  in MCP tool errors.
- `docs/agent-readiness.md` for the new error code.

Not a security gap. It is last because it only improves how a misconfigured
cluster reports itself.

## 9. Testing plan

**Unit, runnable on macOS (all phases)**

- Phase 0: first start mints exactly one management token and writes both
  files with mode `0600`. A second start mints nothing. An existing
  `~/.legion/token` is left alone. `bootstrapSecretFile` seeds the secret.
  An unauthenticated `RunJob` against a default config returns
  `Unauthenticated`. `catalog()` has no `acl.bootstrap`.
- Phase 1:
  - The worker verifier accepts a manager cert. It rejects a node cert, a cert
    from a foreign CA, an expired cert, and no cert, for each manager-only RPC.
  - `ExecTask` admits a manager cert, admits a valid ticket only with
    `exec.direct = true`, and refuses a node cert even when a valid ticket is
    also present.
  - `StartClaim` with no client cert still reaches ticket verification.
  - The JWKS verifier is built when `exec.direct` is on and edge boot is off.
  - The manager's dial rejects a server cert for `node-2` at `node-1`'s
    address, and refuses an `http://` worker address.
  - CSR signing ignores requested SANs and extensions. A request without a
    CSR is refused.
  - The join flow refuses a CA that does not match the pin, accepts a bundle
    that contains it, and refuses a v1 token.
  - `every_worker_rpc_is_classified` (parses the proto, allows multiple
    classes, fails on a missing RPC).
  - Advertise detection returns the route's source address. The override wins.
- Phase 2, against the proxy in host-namespace mode (`ProxySpec { netns:
  None, listen: Some(127.0.0.1:0) }`, as the existing proxy tests run):
  - Each default class is denied on CONNECT, absolute-URI HTTP, and SOCKS5:
    `127.0.0.1`, `[::1]`, `[::ffff:127.0.0.1]`, `0.0.0.0`, a node address,
    `169.254.169.254`, `[fd00:ec2::254]`, `100.100.100.200`.
  - Names are judged by resolved address, with a fake resolver: `localhost`,
    a name that resolves to `169.254.169.254`, `2130706433`, and `0177.0.0.1`
    are all denied.
  - DNS rebinding: a resolver that answers a public address and then
    `127.0.0.1` can't get the second answer dialed.
  - A `hostGuard.allow` entry opens exactly its `ip:port` and nothing next
    to it.
  - A job `allow = ["*"]` and a `require` policy with `policy = "allow"`
    can't widen the guard.
  - The denial carries `Legion-Denied-By: host-guard` and records `net.denied`
    with that source.
- Phase 3: `auto` resolves to `gated` for `container` on a Linux node and to
  `host` on a macOS node and for `exec`. A gated task without an ingress chain
  gets twins. The validate warning fires for a gated task with no egress and
  not for one with an `allow`.
- Phase 5: the `nft` JSON the worker generates for a desired set (golden
  test), including the per-allocation pairs and `hostGuard.allow` accepts.
  Reconciling after an allocation leaves removes its pair.
- Phase 6: the generated spec has `linux.seccomp`, a `user` namespace with
  mappings in the allocation's slot, and a `cgroup` namespace. Shifted
  extraction writes owners inside the range. Slots are unique per node and
  stable across an in-place restart.
- Phase 7: submit refuses gated root `exec`. The ACL matrix shows `job:write`
  without `host` refused for each host-level row in §6.16 and admitted for
  everything else.

**Integration**

- Phase 1: two workers in one test process on `127.0.0.2` and `127.0.0.3`
  (Linux routes all of `127/8` to loopback, so this needs no setup). Exec,
  logs, restart, and a cross-node `connect` upstream all work through the
  advertised addresses. Gate the test to Linux, since macOS needs `lo0`
  aliases. A direct `ExecTask` with no client cert returns `Unauthenticated`.
  With a node cert it is refused too.
- Phase 1: rotate the CA mid-test. Exec keeps working through every step.
- Phase 1: a protocol-2 worker registers and heartbeats, and exec and logs
  against it fail with the protocol-version error, not a plaintext dial.
- Phase 8: a worker whose port is closed registers, runs work through
  heartbeats, and shows `unreachable` in `legion node info` with the hint.

**The spot check (Phases 2, 3, 5, 6)**

One script tries every path in §1.2 from inside a task and prints one line
per target, so the result is readable and greppable:

```text
PASS  127.0.0.1:4747          ECONNREFUSED (own loopback: nothing listens)
PASS  10.0.1.7:4848           ENETUNREACH (gated: no route)
PASS  proxy -> 127.0.0.1:4747 403 host-guard (loopback)
PASS  proxy -> 169.254.169.254:80 403 host-guard (link-local)
FAIL  ...                     connected
```

It ships two ways:

- `configs/jobs/host-guard-spot-check.ncl`: a batch job with one gated
  container task, one `network.mode = "host"` container task, and one
  non-root host-mode `exec` task. Each runs the script against the node's
  manager and worker ports, its node address, the metadata addresses
  (direct, through the proxy, and by a name that resolves to them), a
  neighbor allocation's upstream port, VIP, and resolver, and a sentinel
  listener. A non-zero exit means something was reached. An operator runs it
  on a real cluster, and the paused AWS spot check runs it as an acceptance
  test.
- `tests/host_guard_spot_check.rs`, `#[ignore]` on Linux, which starts a
  manager with `--run-worker`, a sentinel listener, and a neighbor job with
  upstreams, runs the job, and asserts every line and its reason.

The expected reason per target:

| Target | gated container | host-mode container or non-root `exec` |
| --- | --- | --- |
| `127.0.0.1:4747/4748/4749/4848` | `ECONNREFUSED`: its own loopback | `EPERM`: guard drop |
| node address `:4848` | `ENETUNREACH`: no route | `EPERM`: guard drop |
| proxy `CONNECT` to any of the above | `403 host-guard` | n/a |
| `169.254.169.254:80`, `[fd00:ec2::254]:80` | `ENETUNREACH` | `EPERM` |
| proxy to the metadata IP, its name, `2130706433` | `403 host-guard` | n/a |
| a neighbor's upstream `127.0.0.1:<port>`, VIP, resolver | `ECONNREFUSED` (own loopback) or `ENETUNREACH` | `EPERM`: per-allocation rule |
| sentinel on the node address | `403 host-guard` through the proxy | reached, documented (§6.13) |

Run the Linux tests as root on a cgroup v2 host with `nft` and the runtime
installed, or in the Linux worker container from
`internal_docs/development.md`:

```bash
# the datapath guard against real namespaces
sudo -E cargo test --lib netd -- --ignored --nocapture

# the spot check, once per OCI runtime
sudo -E LEGION_OCI_RUNTIME=youki cargo test --test host_guard_spot_check -- --ignored --nocapture
sudo -E LEGION_OCI_RUNTIME=runc  cargo test --test host_guard_spot_check -- --ignored --nocapture
sudo -E LEGION_OCI_RUNTIME=crun  cargo test --test host_guard_spot_check -- --ignored --nocapture

# the container privilege floor: seccomp blocks unshare(CLONE_NEWUSER),
# container root maps outside 0, and a volume keeps working across a reschedule
sudo -E LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_security -- --ignored --nocapture
```

**Regression and performance**

- `cargo test --all --locked`, `tests/acl_endpoints.rs`, and the simulator
  smoke at 1,000 nodes.
- Dispatch now runs over TLS. Run the AWS rig's 1,000-node cell before and
  after, and put both placement-throughput numbers in the PR. Channels are
  cached per node, so the handshake cost should amortize to nothing, but
  measure it.
- Phase 3 container start latency, and Phase 2 proxy connect latency (one
  extra resolution moves earlier, it is not added), before and after.

**End to end**

- The paused AWS spot check is the acceptance test: three raft managers and two
  spot workers. Exec, logs, and cross-node mesh traffic work. Exec served by a
  follower works after a leader failover. A worker is unreachable on purpose
  (security group) and reported. The spot-check job passes on every worker,
  including the metadata rows on real EC2.

## 10. Rollout and backward compatibility

Legion has no installed base to protect (AGENTS.md, "Upgrades and Breaking
Changes"), so nothing here keeps an old path alive. What breaks, so it is a
decision and not a surprise:

- **Every API call needs a token**, from Phase 0. The CLI on the manager's
  machine has one automatically. Scripts elsewhere need `LEGION_TOKEN`.
  `legion acl bootstrap` is gone.
- **Workers and managers move together**, from Phase 1. A new manager never
  dials plaintext. A protocol-2 worker keeps heartbeating and running work,
  but exec, logs, and restart against it fail with an error naming its
  protocol version, until it is upgraded. Upgrade managers first, as
  `docs/upgrades.md` already says.
- **Join tokens minted before Phase 1 are refused.** Mint new ones.
- **The node certificate API takes a CSR only.**
- **Operators must allow 4848 from managers to workers**, and from routers if
  they use edge boot. This was never needed before because it never worked. The
  release note and the README say so.
- **Linux containers are gated by default**, from Phase 3. A container that
  dials out needs `egress.allow`, or `network.mode = "host"` with the `host`
  grant. UDP, ICMP, and tools that ignore proxy variables need `host`.
  Running allocations keep their mode, because mode resolves at placement.
- **Container ids change**, from Phase 6. Files a container wrote into a
  host volume before Phase 6 are owned by host ids that idmapped mounts map
  differently. The public doc tells operators to re-own such volumes once.
- **Submitting `exec` jobs needs the `host` grant**, from Phase 7. Management
  tokens have it.
- **Single-box flows** (`--run-worker`, `mise run join`) get mTLS over
  loopback, gated containers, and the guard with no configuration.
  Certificates come from the in-process manager, and the token from first
  start.
- **Simulated fleets** keep working through the fleet identity. Benchmark
  commands do not change, but benchmark scripts read the bootstrap token.

## 11. Open questions

1. **One CA or two?** The mesh CA would now also guard the control plane, so a
   mesh rotation is a control-plane event. *Lean: one CA*, with roles in URI
   SANs and the two-step rotation in 6.9. A second CA doubles the concepts and
   the rotation procedures for blast-radius separation we do not have a
   concrete threat for. Calls entry 8.
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
6. **Should the UI log in automatically on the manager's machine?** *Lean:
   no.* Loopback is exactly where workloads live. The manager could print a
   one-time login link with the secret in the URL fragment, the way Jupyter
   does, if pasting the token proves to be real friction.
7. **Where does the user-namespace id pool come from?** *Lean: a `legion`
   entry in `/etc/subuid` and `/etc/subgid` when present, else a default
   range starting at `2^31` that the worker checks against both files and
   refuses to start on if it overlaps.* Kubernetes uses the kubelet's subuid
   entry the same way.
8. **Should `LEGION_OCI_RUNTIME=runsc` (gVisor) be supported?** *Lean: test it
   once in Phase 6 and document the result, without making it a goal.* The
   microVM `sandbox` runtime is the strong boundary Legion ships.
9. **Should `exec` default to an unprivileged user, as Nomad's `exec` driver
   does?** *Lean: no.* Legion's `exec` is closer to Nomad's `raw_exec`, which
   Nomad disables by default. The `host` grant (§6.16) is Legion's version of
   that switch, and changing the default user would break every `exec` job
   that writes outside its task directory. Revisit if the `host` grant proves
   too coarse.

## 12. Acceptance criteria

**Phase 0**
- A manager started with the default config refuses an unauthenticated
  `RunJob` with `Unauthenticated`, and `legion job run` on the same machine
  works with no flags or env vars.
- `<dataDir>/bootstrap-token` and `~/.legion/token` exist with mode `0600`
  after first start. A restart mints nothing new.
- `legion acl bootstrap` and `BootstrapAcl` no longer exist, and
  `tests/acl_endpoints.rs` passes.

**Phase 1**
- On two hosts with only 4747 and 4848 open, `legion job logs`, `legion exec
  -it`, `legion node restart`, and a cross-node `connect` upstream all work
  with no flags beyond `legion join <token>`.
- `legion service resolve` returns the worker's private IP, not `127.0.0.1`.
- From a third host, every manager-only `Worker` RPC returns `Unauthenticated`,
  and `ExecTask` without a manager cert or a valid ticket is refused.
- A worker given a token whose `addr` points at a host with a different CA
  refuses to join and prints the pin mismatch.
- The `every_worker_rpc_is_classified` test exists, lists `ExecTask` with two
  classes, and passes.
- `legion net ca rotate` completes with no failed exec or logs call during the
  overlap.
- AWS 1,000-node placement throughput is within noise of the base branch.

**Phase 2**
- From a gated task, `curl -x $HTTP_PROXY` to `127.0.0.1:4747`, the node's
  address on `:4848`, `169.254.169.254`, a name resolving to it, and
  `2130706433:4747` each return `403` with `Legion-Denied-By: host-guard`.
- `legion net policy test` explains each of those denials with `host-guard`.
- A `hostGuard.allow` entry on the worker opens exactly that `ip:port`.

**Phase 3**
- A `container` task with no `network` block on a Linux node runs gated,
  serves its declared port to another node and to its health check, and
  resolves names.
- `legion job validate` warns about a gated task with no egress.
- `network.mode = "host"` still works for a token with the `host` grant.
- Container start latency regression is in the PR.

**Phase 4**
- A packet capture of worker-to-manager traffic on 4747 shows only TLS. The CLI
  still works over plaintext against the same port.
- A worker with `exec.direct = true` fetches the JWKS over TLS and refuses to
  start against a plaintext non-loopback manager address.

**Phase 5**
- A non-root host-mode `exec` task and a host-mode container get `EPERM` on
  `127.0.0.1:4747`, the node address `:4848`, `169.254.169.254`, and a
  neighbor allocation's upstream port, and `legion net doctor` attributes the
  drops to `host-guard`.
- `legion node info` shows the guard as `active`, or `unavailable` with a
  reason on a node without `nft`.

**Phase 6**
- In a default Linux container, `id -u` is `0` and the process's host uid is
  inside the allocation's range. `unshare -U` fails under seccomp. A volume
  survives a reschedule to a new range.
- The spot check passes on youki, runc, and crun.

**Phase 7**
- A gated `exec` task without a non-root `user` is refused at submit with the
  hint.
- A `job:write` token without `host` can submit a container job and is refused
  for an `exec` job and for a container with `network.mode = "host"`.
- An `exec` task with `user` set has no supplementary groups beyond the
  user's own (`/proc/<pid>/status` `Groups:`).

**Phase 8**
- Blocking 4848 shows `unreachable` with the hint in `legion node info`
  within one probe, and `legion exec` fails with `WORKER_UNREACHABLE`.
