# Plan: Pluggable Cluster CA Providers (`builtin`, `file`, `exec`)

> **Judgement calls:** [`calls/cluster-ca-provider-plan_calls.md`](../calls/cluster-ca-provider-plan_calls.md)

**Status: PLANNED** (2026-10-14).

This plan reverses one non-goal of the
[worker-plane-auth plan](worker-plane-auth-plan.md) ("External or pluggable
CAs (Vault PKI, ACME). The CA stays built in."). The cluster CA keeps its
built-in default, and gains two other places its signing key can live: a
CA certificate and key the operator puts on each manager (`file`), and an
external signer Legion talks to over JSON on stdin/stdout (`exec`, with a
reference helper for Vault PKI). The seam is modelled on the KV keyring's
root-key providers ([kv-encryption plan](kv-encryption-plan.md) §6.3,
`src/storage/keyring.rs:234`), so an operator who has configured one knows
the other.

---

## 1. Problem statement

One CA anchors everything Legion's processes present to each other: node
certificates (the worker plane listener and the mesh), the managers' own
client and server certificates, the pin every join token carries, and
`certFrom = "legion"` ingress. Today that CA is hard-wired:

- `src/manager/mesh_ca.rs` is plain `rcgen` code: `generate_ca`, `sign_csr`,
  `issue_manager_cert`, `cross_sign`.
- The CA certificate **and its private key** live as plaintext PEM in one
  replicated row, `net_mesh_ca` (`src/storage/netpolicy_store.rs:175-400`).
  So the key is in every raft log entry that touched it, every snapshot,
  every `legion state backup`, and every streamed replica. `QueryStateStore`
  redacts the column, but a copy of `store.sqlite` does not.
- An enterprise whose policy is "every certificate chains to our PKI" has
  no way to comply. It cannot hand Legion an intermediate, and it cannot
  point Legion at Vault PKI, step-ca, or AWS Private CA. What it does today
  is run Legion's CA as an unaudited island, or not run Legion.

Verification is already issuer-agnostic. `src/cluster_tls.rs` trusts a PEM
bundle (`SwappableRoots`), finds a peer's role in its URI SANs
(`legion://node/`, `legion://manager/`, `legion://fleet/`), and the worker's
pinned first contact finds the pinned CA among the certificates the manager
sends (`PinnedManagerVerifier`, `src/cluster_tls.rs:473-550`). Only the
*issuing* side and the places that read the store row directly need to
change.

## 2. Prior art

### 2.1 Consul Connect CA providers (the closest analogue)

Consul's mesh CA has a provider seam: `consul` (built in, the default),
`vault`, and `aws-pca`
([docs](https://developer.hashicorp.com/consul/docs/connect/ca)). The
provider interface (`GenerateRoot`, `GenerateIntermediateCSR`,
`ActiveIntermediate`, `Sign`, `CrossSignCA`) runs on the leader. The CA
configuration is **cluster state in Raft**, set with `consul connect ca
set-config`, so every server agrees by construction. Rotation with the
built-in provider cross-signs, and with Vault Consul mints a new
intermediate from the configured root mount.

*Take:* built-in default plus external providers; cross-signing stays the
built-in story. *Reject:* replicated CA configuration. Legion's managers
read every other secret-bearing setting (the keyring's root key provider,
`server.tls`, raft secrets) from their own config file, and a Vault token
in replicated state would land in every backup (calls entry 3).

### 2.2 Kubernetes

`kube-controller-manager --cluster-signing-cert-file/--cluster-signing-key-file`
is exactly the `file` provider: an operator-provided CA on the controller's
disk signs node and kubelet-serving CSRs. External signers plug in through
the CSR API's `signerName`, with approvers deciding *what* gets signed.

*Take:* the file shape and the split between "who decides the names" and
"who holds the key". *Reject:* a separate approval step. Legion's manager
already authenticates the caller and picks the names (`CsrSubject`).

### 2.3 Linkerd and Istio

Linkerd takes a **trust anchor** (the bundle every proxy trusts) separately
from an **issuer** certificate and key (what signs). Its documented rotation
is the procedure this plan adopts for `file`/`exec`: add the new anchor to
the bundle, wait for it to propagate, switch the issuer, and remove the old
anchor later. Istio's "plug-in CA certificates" (`ca-cert.pem`,
`ca-key.pem`, `root-cert.pem`, `cert-chain.pem`) is the same split.

*Take:* trust bundle separate from the signing CA, defaulting to the
signing CA. *Take:* the bundle-first rotation procedure.

### 2.4 SPIRE upstream authorities

SPIRE keeps leaf signing local and asks an `UpstreamAuthority` plugin
(disk, Vault, AWS PCA, cert-manager) for an **intermediate** it then signs
with. Leaves are cheap and never leave the process, and the external CA is
called once per intermediate lifetime.

*Reject for now:* Legion has no safe place for a locally held intermediate
key. It would go back into the replicated store, which is the thing this
plan removes. Kept as an open question (§11).

### 2.5 Distilled requirements

1. The built-in CA stays the zero-config default.
2. The signing key for `file`/`exec` never enters the replicated store.
3. Legion, not the provider, decides every name. The provider only signs,
   and Legion checks what comes back.
4. Rotation never strands a worker, whichever provider is active.
5. A manager configured differently from the rest of the cluster says so
   and refuses to sign, instead of splitting the fleet's trust.

## 3. Jobs to be done, by persona

**Security / platform admin**
- When our policy says every certificate must chain to the company PKI, I
  want Legion's internal certificates to come from our intermediate, so
  auditors see one trust hierarchy and I can rotate it from one place.
- When a database backup leaks, I want it to hold nothing that can mint an
  identity in my cluster, so a leaked backup is a data incident and not a
  takeover.
- When I hand a signing service to a tool, I want the tool to be unable to
  choose what it is signed as, so a compromised manager cannot ask for an
  arbitrary name and a buggy signer cannot hand out one.

**Cluster operator**
- When I have set up Vault PKI once, I want managers to get their
  certificates from it without me running a renewal job, so nothing expires
  at 3am.
- When I rotate our intermediate, I want to do it without breaking a
  connection or re-joining a worker, so rotation is routine, not an
  outage.
- When one manager has the wrong CA files, I want it to tell me which
  manager disagrees and why, instead of half my workers trusting one CA and
  half the other.
- Before I point a manager at a new signer, I want to test it, so I find out
  it hands out wrong names on my laptop, not in production.

**On-call responder**
- When workers cannot reach a manager after someone changed the CA, I want
  one command that says which CA this manager signs with, where it comes
  from, and what is wrong, so I do not have to read PEM files by hand.

**Autonomous agent**
- When I check a cluster's health, I want the CA provider and any problem
  in structured output, and a conformance check with a JSON verdict, so I
  can report a misconfigured CA without parsing prose.

*Not in scope:* the job author. Nothing in the jobspec changes;
`certFrom = "legion"` keeps meaning "the node's cluster certificate".

### User scenarios

1. **Bring an intermediate.** A platform team issues
   `legion-intermediate.pem` under their root, copies it and its key to the
   three managers, and sets `clusterCa = { provider = "file", certPath = …,
   keyPath = … }`. They restart the managers, mint join tokens, and workers
   join as before. `legion net ca show` says `Provider: file
   (/etc/legion/ca/cluster-ca.pem)`.
2. **Vault PKI.** An operator creates a PKI mount and a role for Legion,
   runs `legion net ca provider test -- legion-ca-vault --mount pki_legion
   --role legion`, sees every check pass, and pastes the printed config
   block into each manager's config.
3. **Rotate an intermediate.** The operator adds the new intermediate to
   `bundlePath` on every manager. Within a heartbeat every worker holds both.
   They then switch `certPath`/`keyPath` to the new one, and every node
   re-issues within a heartbeat. A day later they drop the old one from the
   bundle.
4. **One manager is wrong.** A third manager is started without the
   `clusterCa` block. It logs and reports `this manager uses the built-in CA,
   but the cluster's CA comes from the file provider (recorded by manager
   mgr-1)`, and refuses to sign or mint, while the other two keep serving.

## 4. Goals and non-goals

### Goals

- A `ClusterCaProvider` seam with `builtin` (default, unchanged behavior),
  `file`, and `exec`.
- Every reader of the CA (trust bundle, pin, chain, signing) goes through
  the seam. Nothing outside the builtin provider reads `net_mesh_ca`.
- Manager self-certificates are signed from a CSR the manager generates, so
  no provider needs to hand Legion a key.
- Post-sign verification for every non-builtin signature.
- Trust-bundle changes reach every worker within one heartbeat.
- Multi-manager agreement detection.
- A conformance harness (`legion net ca provider test`) and a reference Vault
  PKI helper.

### Non-goals

- **ACME.** Public CAs do not issue `legion://` URI SANs or `.legion`
  names, and they should not. Ingress certificates from ACME already work
  through `certFrom = { kv }`.
- **A native Vault (or AWS PCA) client linked into the binary.** `exec`
  covers every CA with a CLI; native providers are a follow-up if `exec`
  proves too slow or too awkward (same call as the keyring's).
- **Revocation (CRL/OCSP).** Lifetimes stay 24 hours; that is the
  revocation story, as today.
- **Separate CAs per plane** (worker plane vs mesh). Worker-plane calls
  entry 8 stands. One provider serves both.
- **Signing an intermediate for Legion through the provider** (SPIRE's
  upstream-authority shape). §11.
- **Changing provider on a running cluster without re-joining workers.** A
  worker trusts the old CA only, and nothing cross-signs between providers.
  Documented as a limitation; switching is "reconfigure every manager, then
  re-join workers".
- **The raft transport.** Still plaintext between managers, as before.

## 5. Background — what exists today

| Where | What it does with the CA |
| --- | --- |
| `src/manager/mesh_ca.rs` | `generate_ca`, `sign_csr(ca_cert, ca_key, csr, subject)`, `issue_manager_cert(ca_cert, ca_key, id, kind)` (generates the manager's key and signs it directly), `cross_sign`, `generate_csr`, `csr_for_key`, `issue_node_cert` (tests). |
| `src/storage/netpolicy_store.rs:175-400` | `MeshCa` (cert, key, previous, cross, rotated_at), `phase`, `bundle_pem`, `chain_suffix`, `server_chain_suffix`, `pin`; `Store::{mesh_ca, ensure_mesh_ca, rotate_mesh_ca}`. |
| `src/manager/worker_plane.rs:214-322` | `materials_with_pin` reads the row, keys material by the signing pin, mints the manager's client and server certificates from the CA key, re-mints after 8h. |
| `src/manager/acl.rs:529` | `mint_join_token` pins `ensure_mesh_ca().pin()`. |
| `src/manager/daemon.rs:449` | the `--run-worker` worker's pin. |
| `src/manager/daemon.rs:1350-1420` | `keep_manager_server_cert` and `manager_incoming` serve the minted server certificate for `manager.legion`. |
| `src/manager/exec.rs:339` | direct exec tickets carry the trust bundle. |
| `src/manager/server.rs:1856` | `cluster_ca_pin()` for `HeartbeatReply.cluster_ca_pin`, cached 5s. |
| `src/manager/server.rs:7269-7420` | `GetMeshCa`, `RotateMeshCa`, `IssueNodeCertificate` (signs with the row's key). |
| `src/worker/plane.rs:290` | `needs_refresh`: re-issue after 8h, under 1h left, or when the heartbeat's pin is not in the held bundle. |
| `src/cluster_tls.rs` | verification only: bundles, pins, URI-SAN roles, pinned first contact. Unchanged. |

The keyring seam this mirrors: `RootKeyProvider` (`src/storage/keyring.rs:234`),
`provider_from_config` (`:633`), `ExecRootKey` with
`run_provider_command` (`:475`), `test_exec_provider` (`:1387`),
`legion keyring provider test` (`src/cmd/keyring.rs:524`), helpers in
`plugins/keyring/`, docs in `docs/kv-encryption.md`.

## 6. Proposed design

### 6.1 The seam

```rust
pub trait ClusterCaProvider: Send + Sync {
    fn kind(&self) -> &'static str;             // builtin | file | exec
    fn location(&self) -> String;               // paths, or the command
    fn view(&self, store: &Store) -> Result<Option<CaView>, CaError>;
    fn ensure(&self, store: &Store) -> Result<CaView, CaError>;
    fn sign(&self, store: &Store, view: &CaView, csr_pem: &str, spec: &LeafSpec)
        -> Result<String, CaError>;
    fn rotate(&self, store: &Store) -> Result<CaView, CaError>;
}
```

`CaView` is everything a reader needs, so no reader needs the provider's
internals: the trust bundle (anchor first), the signing CA, the chain a leaf
carries after itself, the **anchor pin** (the bundle certificate the
signing CA chains to; join tokens carry it), and every bundle pin. A
`ClusterCa` wrapper, held by the `Store` and built from config like the
keyring, owns the shared behavior: the agreement check (§6.9), CSR
generation for manager certificates (§6.5), and verification of what a
provider returns (§6.4).

### 6.2 The providers

```nickel
clusterCa = {
  provider = "builtin",          # "builtin" (default) | "file" | "exec"

  # file: an operator-provided CA, typically an intermediate under your root
  certPath = "/etc/legion/ca/cluster-ca.pem",  # signing CA first, then intermediates
  keyPath = "/etc/legion/ca/cluster-ca.key",   # PKCS#8 ("BEGIN PRIVATE KEY")
  bundlePath = "/etc/legion/ca/trust.pem",     # optional; default: the signing CA

  # exec: an external signer over JSON on stdin/stdout
  command = ["legion-ca-vault", "--mount", "pki_legion", "--role", "legion"],
  timeout = "10s",           # per call
  refreshInterval = "1m",    # how often the trust bundle is re-read
}
```

- **`builtin`**: exactly today. Generated on first use by the control-lane
  leader, stored in `net_mesh_ca`, rotated with `legion net ca rotate`
  (cross-signed, 24h overlap).
- **`file`**: the signing CA and key are read from each manager's disk and
  re-read when a file's mtime changes. The key never enters the store. The
  trust bundle defaults to the signing CA itself (calls entry 4). A file
  that is broken at start stops the manager, naming the field; a file that
  breaks later keeps the last good CA in use and reports the problem.
  Loading runs a self-test: sign a probe certificate and verify it, which
  catches a CA name rcgen cannot reproduce byte for byte, name constraints
  that exclude `.legion`, and expired CAs before a worker sees them.
- **`exec`**: two operations, mirroring the keyring's conventions (one JSON
  document in, one out, non-zero exit or timeout is an error):

  ```json
  {"op":"describe"}
  → {"bundle_pem":"<every CA holders trust>","chain_pem":"<the signing CA, then intermediates toward the bundle>"}

  {"op":"sign","role":"node","csr_pem":"…","common_name":"node-1.node.legion",
   "dns_sans":["node-1.node.legion"],"uri_sans":["legion://node/node-1"],
   "ttl_seconds":86400,"usages":["server_auth","client_auth"]}
  → {"cert_pem":"<the leaf, optionally followed by its intermediates>"}
  ```

  `role` is `node`, `fleet`, `manager_server`, or `manager_client`, so a
  helper can map roles to separate CA roles if it wants. `describe` is
  cached for `refreshInterval` and re-run in the background
  (stale-while-revalidate), so no request path waits on it after the first
  call. A failed refresh keeps the last good answer and reports the
  problem.

### 6.3 Legion decides the names

The SANs still come from the authenticated caller (`CsrSubject` for nodes
and fleets; the manager's own id for manager certificates), exactly as
today. Common names become hostname-shaped (`node-1.node.legion`,
`manager.legion`, `mgr-1.manager.legion`) so a Vault role can be narrow
(calls entry 9).

### 6.4 Verification of what a provider returns

Every non-builtin signature is checked before anything is handed out. A
refusal names the provider and the mismatch:

```console
error: the exec cluster CA provider (legion-ca-vault --mount pki_legion --role legion)
returned a certificate Legion will not hand out: its URI SANs are
[legion://node/node-1, spiffe://corp/x], but Legion asked for exactly [legion://node/node-1]
```

Checks, each with its own message: at least one certificate; the leaf is
not a CA and its key usage cannot sign certificates; its public key is the
CSR's; DNS SANs and URI SANs are exactly the requested sets and there are
no other SAN types; the EKU extension is present, holds every requested
usage, and holds nothing but `serverAuth`/`clientAuth` (calls entry 7);
`notBefore` is at most 5 minutes in the future; the lifetime is at most the
requested TTL plus 10 minutes and at least 2 hours; the chain verifies
against the trust bundle with webpki, for every usage asked for (which also
enforces name constraints and path length). A chain failure from `exec`
re-reads `describe` once before refusing, so an issuer rotated inside the
external CA is picked up instead of refused.

### 6.5 Manager certificates from a CSR

`issue_manager_cert` signed with the CA key directly. Now the manager
generates its key and a CSR and asks the provider, with the same spec it
used before (`legion://manager/<id>`, plus `manager.legion` for the server
certificate). The builtin provider signs exactly as before. Re-minting is
at a third of the actual lifetime (or 8 hours, whichever is sooner), and
for `exec` it runs in the background while the current certificate keeps
serving.

### 6.6 Every reader goes through the provider

| Reader | Now reads |
| --- | --- |
| worker-plane materials, manager server cert | `view` + manager certs through the provider |
| join tokens, `--run-worker` pin | `ensure().pin` (the anchor) |
| direct exec tickets | `view.bundle_pem` |
| heartbeat | `view.bundle_pins` (§6.7) |
| `GetMeshCa` | the view plus provider, location, and any problem |
| `RotateMeshCa` | `provider.rotate` |
| `IssueNodeCertificate` | `ClusterCa::sign` (verified), `view.bundle_pem` |

### 6.7 Trust-bundle changes reach workers within a heartbeat

Today the heartbeat names only the signing CA's pin, and a worker re-issues
when its bundle lacks it. That is enough for builtin rotation (the pin
changes) but not for a `file`/`exec` bundle that gains a root while the
signing CA stays the same. `HeartbeatReply.cluster_ca_pin` becomes
`cluster_ca_pins`: every bundle certificate's pin, anchor first. A worker
re-issues when its held bundle's anchor differs from the first pin or its
set of pins differs. Re-issuing returns the new bundle in
`IssueNodeCertificateReply.ca_pem` and swaps the roots the worker trusts
managers and peers with, the same path a builtin rotation takes.

### 6.8 Rotation per provider

- **builtin:** `legion net ca rotate`, unchanged.
- **file:** `legion net ca rotate` fails with `FailedPrecondition`, naming
  the provider and the procedure:
  1. add the new CA to `bundlePath` on every manager (workers pick it up
     within a heartbeat);
  2. switch `certPath`/`keyPath` on every manager (every node re-issues
     within a heartbeat, because the anchor changed);
  3. after 24 hours, remove the old CA from the bundle.
  With a root in the bundle and a new intermediate under the same root, only
  step 2 is needed.
- **exec:** the same refusal, pointing at the external CA. The reference
  Vault helper reports every issuer in the mount as the bundle, so "create a
  new issuer, make it the default, delete the old one a day later" is the
  whole procedure.

### 6.9 Managers must agree

A replicated single-row record, `cluster_ca_record`, holds the provider
kind, the anchor pin, and the bundle pins **as the control-lane leader sees
them**, and which manager wrote them. The leader rewrites it whenever its
own view changes. Every other manager compares its view with it:

- a `builtin` manager agrees only with a `builtin` record (their views come
  from the same replicated row);
- a `file`/`exec` manager disagrees with a `builtin` record; with another
  `file`/`exec` record it agrees when the two are **trust-compatible**: its
  anchor is in the recorded bundle, and the recorded anchor is in its
  bundle. That admits the rotation procedure in §6.8 at every step and
  refuses a manager whose certificates the others would not trust.

A manager that disagrees refuses to sign node certificates, mint its own
certificates, or mint join tokens, with an error naming both views and the
recording manager. It keeps serving the certificates it already has.
`legion net ca show` reports the problem. No record (a fresh cluster)
means no check (calls entry 2).

### 6.10 CLI and RPC surface

```console
$ legion net ca show
Provider: file  (/etc/legion/ca/cluster-ca.pem)
Pin:      sha256:9c1e4f...
Bundle:   1 CA. Holders trust the signing CA itself.
-----BEGIN CERTIFICATE-----
…

$ legion net ca rotate
error: the cluster CA comes from the `file` provider (/etc/legion/ca/cluster-ca.pem);
Legion cannot rotate it (FAILED_PRECONDITION)
Hint: add the new CA to clusterCa.bundlePath on every manager, wait one heartbeat, switch
certPath/keyPath, and remove the old CA from the bundle a day later (docs/cluster-ca.md#rotation)

$ legion net ca provider test -- legion-ca-vault --mount pki_legion --role legion
Testing cluster CA provider: legion-ca-vault --mount pki_legion --role legion
  [ok] describe                     bundle of 1 CA, anchor sha256:9c1e4f...
  [ok] sign node                    node certificate verified (24h)
  [ok] sign manager server          manager.legion verified (24h)
  [ok] sign manager client          legion://manager/conformance verified (24h)
  [ok] requested names win          the CSR's own names were ignored

Provider is usable. Configure it with:
  clusterCa = { provider = "exec", command = ["legion-ca-vault", "--mount", "pki_legion", "--role", "legion"] }
```

`provider test` also takes `--cert-path/--key-path/--bundle-path` to test
`file` material before deploying it. Every form takes `-j`.

### 6.11 Failure modes

| What breaks | What happens |
| --- | --- |
| `file` invalid at start | the manager does not start; the error names the field and the file |
| `file` invalid later | the last good CA keeps signing; `net ca show` and logs report it |
| `exec` failing at start | the manager starts; signing returns `Unavailable` until `describe` works |
| `exec` `describe` failing later | the last good bundle is used; the problem is reported |
| `exec` `sign` failing | `IssueNodeCertificate` returns `Unavailable`; the worker keeps its certificate and retries |
| provider returns a bad certificate | refused with `FailedPrecondition` naming the check; counted |
| manager disagrees with the cluster | it refuses to sign and mint, with both views in the error |
| Legion asked to rotate `file`/`exec` | `FailedPrecondition` with the procedure |

### 6.12 Observability

`legion_cluster_ca_provider_calls_total{provider,op,outcome}` with `outcome`
of `ok`, `error`, or `refused` (Legion refused what the provider returned).

## 7. Proto, struct, config, and persistence changes

- `GetMeshCaReply`: add `provider`, `provider_location`, `chain_pem`,
  `problem`. `pin` is the anchor's pin (the signing CA's for builtin).
- `HeartbeatReply`: reserve 14 (`cluster_ca_pin`), add `repeated string
  cluster_ca_pins`.
- `RotateMeshCa`, `IssueNodeCertificate`, `CreateJoinToken`: comments
  describe the provider-dependent behavior and new error codes. No new RPC,
  so no ACL change.
- Manager config: `clusterCa { provider, certPath, keyPath, bundlePath,
  command, timeout, refreshInterval }` (`ClusterCaConfig` /
  `ClusterCaConfigFull`). No job-config change.
- Store: `cluster_ca_record` (one row, control lane), a migration and an
  ownership entry.

## 8. Implementation phases

1. **The seam with builtin behind it.** Trait, `ClusterCa`, every reader
   moved, manager certs from CSRs, heartbeat pins list. No behavior change
   for users beyond the hostname-shaped CNs and a re-issue when a builtin
   overlap ends.
2. **`file`** with verification, the agreement record, and `net ca show`
   reporting the provider. Useful on its own for "bring an intermediate".
3. **`exec`**, the conformance harness, the Vault PKI helper, and the
   metric.

All three land together in this change; each would ship on its own.

## 9. Testing plan

- builtin: the existing mesh/worker-plane/ACL suites unchanged, plus a
  `ClusterCa` round trip (sign, manager certs, rotation).
- file: an intermediate generated under a test root, with the bundle
  defaulted and explicit; mtime reload; invalid key/cert refused at start;
  a key that does not match the certificate; rotation refused.
- exec: a helper script that delegates signing to the test binary, so the
  real JSON-over-stdio contract runs; timeouts and non-zero exits; a cached
  `describe` that survives a failing helper.
- verification: one test per refusal.
- agreement: the pure trust-compatibility rule, and a store-level check that
  a non-leader with a foreign record refuses.
- worker refresh: `needs_refresh` on anchor change and on bundle-set change.
- harness: every check reported for a good helper and for a helper that
  signs the CSR's own names.

## 10. Rollout

No installed base (AGENTS.md). Builtin clusters see no configuration change.
Breaking: `HeartbeatReply.cluster_ca_pin` is replaced by `cluster_ca_pins`
(workers and managers upgrade together); certificate CNs change shape.

## 11. Open questions

- **Upstream-authority mode** (SPIRE): sign one intermediate through `exec`
  and leaves locally. Lean: only if per-certificate `exec` latency shows up
  at fleet scale; the key would need a home outside the store.
- **Per-manager agreement records** rather than the leader's view only.
  Lean: not until a cluster with followers that disagree with each other
  but not with the leader shows up.

## 12. Acceptance criteria

- With no `clusterCa` block, every existing test passes unchanged.
- With `file`, a worker joins, the manager dials it, the mesh verifies, and
  `store.sqlite` contains no private key.
- With `exec`, `legion net ca provider test` passes against the test helper
  and the Vault helper's contract, and a helper that returns the wrong SANs
  is refused with the mismatch named.
- A manager with a different CA than the leader refuses to sign, and
  `legion net ca show` says why.
- `legion net ca rotate` on `file`/`exec` returns `FailedPrecondition` with
  the procedure.
