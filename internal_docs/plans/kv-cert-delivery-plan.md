# Plan: Live KV File Updates and KV-Backed Ingress Certificates

> **Judgement calls:** [`calls/kv-cert-delivery-plan_calls.md`](../calls/kv-cert-delivery-plan_calls.md)

## Status (2026-10-10): SHIPPED

Both phases are implemented, with public docs in `docs/kv-store.md` ("Live
file updates"), `docs/network-handlers.md` ("Certificates from KV"), and
`docs/guides/tls-certificates.md`, plus example jobs
`configs/jobs/cert-renewer.ncl` and `configs/jobs/tls-edge.ncl`. Where the
build diverged from this text, the calls doc says so (notably #3: `update`
migrates synchronously instead of through an eval). The open question below
became a TODO.md item.

Two small extensions to existing primitives, so a certificate (or any other
KV-held file) can rotate under a running task:

- **Phase 1:** `onChange = "update"` and `changeMode` on KV `files`
  injections. The file is rewritten in place under the running task.
- **Phase 2:** `certFrom = { kv = "<path>" }` on an ingress chain's `tls`.
  The node datapath serves a certificate held in KV and swaps it on change.

Deliberately deferred: an ACME client (built in or packaged), a
cluster-edge load balancer, `clientCa = { kv = ... }`, and KV-backed
material for egress `originate`.

## Problem statement

Legion has a KV store that is encrypted at rest and ACL-scoped by path,
which makes it a natural place to keep a TLS certificate. Getting that
certificate to a running workload is where it falls down.

- **KV `files` injections are written once, at task start.**
  `materialize_task_files` (`src/task_plugins/container.rs:226`) and the
  exec start path (`src/task_plugins/exec.rs:474`) are the only writers. A
  renewed certificate reaches the task only through `onChange = "restart"`
  (every allocation of the job is replaced at once) or `"rollout"`. Neither
  is what nginx or HAProxy want: rewrite the file, send `SIGHUP`.
- **`docs/kv-store.md` says otherwise.** It claims a re-render is an atomic
  rewrite the running container sees without a restart. The code has no
  such path.
- **Ingress TLS reads certificate files from the node's disk.**
  `tls = { mode = "server", certPath, keyPath }` makes the operator place
  and rotate files on every node a task might land on. The mesh
  certificate (`certFrom = "legion"`) rotates itself, but it is a private
  CA certificate and useless to a browser.

What a user has to do today to serve a Let's Encrypt certificate from a
Legion task: roll the job on every renewal, or stage the certificate onto
nodes by hand.

## Prior art

- **Nomad `template` blocks** ([docs](https://developer.hashicorp.com/nomad/docs/job-specification/template))
  render Consul KV and Vault secrets into files and apply
  `change_mode = noop | signal | restart | script`, with `splay`. The file
  is rewritten under the running task. We take the shape: `changeMode`,
  `changeSignal`, `splay`. Legion's `connect.templates` and workload
  `identities` already use the same vocabulary, so this brings KV files in
  line. We reject `script`.
- **Kubernetes Secret volumes** are updated in place by the kubelet
  (eventually, via an atomic symlink swap). The app is expected to notice.
  Env-sourced secrets never update. Same split we keep: env needs a
  restart, files can change live.
- **cert-manager** writes certificates into a `kubernetes.io/tls` Secret,
  and ingress controllers watch that Secret and hot-swap. Phase 2 is the
  datapath-side half of this: the node watches the KV key, and the task
  never holds the key.
- **Consul API gateway / Envoy SDS** stream certificates to the proxy over
  an API instead of files. Phase 2's fetch-by-version RPC is the same idea
  at Legion's scale.

## Jobs to be done

**Job author / app developer**
- When my certificate renews, I want nginx to pick it up without a
  redeploy, so a routine renewal is not a deploy event.
- When I put a config file in KV, I want my running service to see edits
  to it, so I can tune it without restarting everything.
- When I expose a plain HTTP service, I want it served over HTTPS with a
  real certificate, without adding a proxy or teaching my app TLS.

**Cluster operator**
- When certificates rotate, I want nothing to touch node disks, so I do
  not have to sync files across a fleet.

**Security / platform admin**
- When someone references a certificate in a job, I want them to need read
  access to that key, so job-submit rights do not become a way to read
  every private key in KV.

**Autonomous agent**
- When I rotate a certificate, I want one `kv put` to be the whole
  operation, with status that says whether the node picked it up.

## Goals and non-goals

Goals:
- A KV file can change under a running task, then the task gets nothing,
  a signal, or an in-place restart.
- The ingress datapath can serve a KV-held certificate and swap it without
  dropping connections.
- A job can only reference a KV certificate its submitter can read.

Non-goals:
- An ACME client. Renewal stays a job's concern (the guide shows a
  scheduled batch task running `lego`).
- Live env updates. A running process's environment cannot change.
- A cluster-edge load balancer. Ingress stays one listener per advertised
  port per allocation.

## Background: what exists today

- **KV injection specs:** `TaskKvFileInjection`
  (`src/structs/job.rs:1980`), policy resolution `resolve_with_defaults`
  (`:1947`), `KvInjectionAction` (`:1909`).
- **Manager reconcile:** `reconcile_kv_changes`
  (`src/manager/server.rs:2727`) re-renders into a new job config, then
  acts. Rendered files land in `task.config.files` as
  `{ path, mode, content }` (`apply_task_kv_injections`, `:1041`).
- **Allocations on a superseded config:** `migrate_allocations_to_job_config`
  (`src/storage/store.rs:11372`) repoints allocations at a new config
  without stopping them when `job_configs_schedule_equivalent` holds.
  `task_schedule_equivalent` (`:19001`) ignores `config`, so a files-only
  change qualifies.
- **Worker:** `reconcile_allocations` (`src/worker/task_manager.rs:1139`)
  ignores `task_config` changes for running allocations.
  `connect.templates` (`src/worker/templates.rs`, driven by `apply_templates`
  in `src/worker/daemon.rs`) is the existing live-rewrite engine with
  `changeMode`, `splay`, and `coalesce`.
- **Ingress TLS:** `TlsConfig` (`src/structs/netpolicy.rs:413`),
  validation at `:575`. The datapath builds a `rustls::ServerConfig` per
  (allocation, port) in `ForwarderContext::ingress_server_config`
  (`src/worker/forwarder.rs:429`), cached by a fingerprint. A new
  connection re-checks the fingerprint, so changing it is a hot swap.
- **Worker KV reads:** only `GetArtifactCredential`
  (`src/manager/server.rs:7661`), which serves any path to any worker (the
  "KV ACL caveat" in `docs/artifacts.md`).
- **The failover rollout pass:** `reconcile_missing_rollouts` creates a
  rollout for any job whose newest config has no allocations, no rollout,
  and an older config with allocations
  (`Store::jobs_with_unrolled_latest_config`).

## Proposed design

### 1. `onChange = "update"` and `changeMode` on KV files

```nickel
files = [
  {
    path = "tls/bundle.pem",
    mode = "0400",
    file = { template = "{{ kv \"certs/app.example.com\" }}" },
    kv = { watch = true, onChange = "update" },
    changeMode = "signal",       # noop (default) | signal | restart
    changeSignal = "SIGHUP",     # required by signal
    splay = "10s",               # random delay before acting, per allocation
  },
],
```

- `update` is a new `onChange` action: re-render, then repoint the job's
  allocations at the new config in place. Its strength sits between `none`
  and `restart` (`none < update < restart < rollout < fail`), so a batch
  that also needs a restart still restarts.
- The worker sees a running allocation's rendered `files` change on the
  heartbeat. It rewrites each changed file atomically (temp file, mode,
  fsync, rename), records the new content in its stored task config (so a
  later restart keeps it), then applies the file's `changeMode`.
- Validation at submit:
  - `update` is rejected on env entries (resolved through task defaults
    too) and on `onDelete`:
    `env FEATURE_FLAGS: onChange = "update" cannot change a running process's environment; use "restart" or "rollout"`.
  - `changeMode`, `changeSignal`, and `splay` are rejected unless the file
    resolves to `onChange = "update"`.
  - `signal` needs `changeSignal` and a runtime that supports signals.
- A job in `failed` from a missing KV dependency is recovered by
  escalating `update` to `restart`, since only a replacement clears the
  failure stamps.

### 2. `certFrom = { kv = "<path>" }` on ingress TLS

```nickel
network = {
  ports = [{ name = "https", static = 443 }],
  ingress = {
    tls = { mode = "server", certFrom = { kv = "certs/app.example.com" } },
  },
},
```

- The KV value is one PEM bundle: the certificate chain followed by the
  private key (`cat fullchain.pem privkey.pem`). One key keeps the pair
  atomic, so a reader never sees a new cert with an old key.
- Works on an inline chain and on a policy object.
- **Submit-time ACL.** `RunJob` (and template runs) and `UpsertNetPolicy`
  require `kv:read` on the referenced path, in addition to their own
  checks.
- **Delivery.** A new worker-plane RPC, `GetIngressCertificates`:

  ```text
  { node_id, known: [{ allocation_id, version }] }
    -> { certificates: [{ allocation_id, kv_path, version, bundle_pem, unchanged, error }] }
  ```

  The worker never names a path. The manager checks that each allocation
  is placed on the calling node, recomputes its effective ingress chain,
  reads the path from it, validates the bundle, and returns it. A version
  the worker already holds comes back `unchanged` with no material.
- **Worker.** One call per heartbeat, only when an allocation on the node
  has a KV-backed chain. Material is held in memory only, never in
  `netd_state.json`. The KV version is folded into the server-config
  fingerprint, so new connections get the new certificate and open ones
  are untouched. Fail-static: a failed fetch keeps the previous
  certificate.
- **Status.** A missing or invalid bundle reports `tls_ready = false` with
  `tls_error = "kv certs/app.example.com: ..."` on the allocation, and
  `legion net doctor` names it.

## Proto, struct, config changes

- `KvInjectionAction::Update`; `TaskKvFileInjection.change_mode`,
  `change_signal`, `splay`, copied into the rendered `config.files` entry.
- `TlsConfig.cert_from` becomes `CertFrom` (`"legion"` or `{ kv }`). The
  string form serializes identically, so existing policy hashes are stable.
- New RPC `GetIngressCertificates` with request and reply messages,
  classified `Plane::Worker` in `tests/acl_endpoints.rs`.
- `configs/jobs/maximal.ncl`: both surfaces.
- No storage migration.

## Implementation phases

1. **KV file live update.** Spec, validation, manager `update` action
   (synchronous migration), worker rewrite and change action. Ships alone:
   a certificate in KV can rotate under nginx with `SIGHUP`.
2. **KV-backed ingress TLS.** Spec, ACL, RPC, worker fetch and swap,
   status. Ships alone: any task gets HTTPS without a proxy.

## Testing plan

- Unit: validation of every new rejection; policy strength order; PEM
  bundle parsing; `CertFrom` serde round-trip (string form unchanged).
- Manager: an `update` put migrates allocations to the new config without
  replacing them and creates no rollout; a failed job escalates to restart.
- Worker: a changed `files` entry on a running allocation is rewritten
  atomically with its mode, the stored config is updated, and the change
  action fires (signal test with a real process, as `identity.rs` does).
- Datapath: an ingress listener with KV material completes a handshake,
  then presents the new certificate after the version changes.
- ACL: the new RPC in `endpoint_table`; a foreign node cannot fetch another
  node's allocation; submit without `kv:read` on the path is denied.

## Rollout and backward compatibility

- Both surfaces are opt-in. Existing jobs and policies are unchanged.
- An old worker ignores the new `files` keys and keeps start-only
  behavior. An old manager rejects `onChange = "update"` and
  `certFrom = { kv }` at submit.
- A new worker against an old manager gets `Unimplemented` from
  `GetIngressCertificates` and reports the chain not ready.

## Open questions

- Should `onChange = "none"` also migrate allocations? Today it leaves them
  on the superseded config, which the failover rollout pass then rolls out.
  Lean: yes, but as its own fix with its own test, not in this change.

## Acceptance criteria

- Phase 1: `legion kv put certs/x "$(cat new.pem)"` rewrites the file under
  a running task and delivers `SIGHUP`, with the allocation id unchanged.
- Phase 2: `curl --resolve` against the advertised port sees the KV
  certificate, then the new one after a `kv put`, with no task restart.
