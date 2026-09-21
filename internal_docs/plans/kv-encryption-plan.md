# Plan: KV Encryption at Rest — Envelope Encryption with a Replicated Keyring

> **Judgement calls:** [`calls/kv-encryption-plan_calls.md`](../calls/kv-encryption-plan_calls.md)

Status: **PLANNED** (nothing implemented; written 2026-09-21). Deliberately
deferred out of this plan and named as such: native cloud-KMS providers (the
`exec` provider covers them), a manual-unlock / passphrase mode, encrypting the
*rendered* copies of KV values in `job_config` / `task_config`, and any
worker-side at-rest protection. Each is a follow-up plan, not a hidden gap —
see §4 (non-goals) and §6.9 (what this does and does not close).

Legion's KV store is the cluster's secret store: artifact credentials
(`auth.kv`), `$path/key` references in jobspecs, and `{{ kv "…" }}` templates
rendered into task env and files all read from it. Every one of those values
is written to disk in plaintext today — in the state database, in every
backup, in the streamed WAL replica, in the raft log and every snapshot, and
readable through raw SQL by anyone holding `state:read`. This plan encrypts
KV values at rest with a key hierarchy that keeps the one key that matters
*out* of the store, so a copy of the database — a leaked backup, a bucket
listing, a snapshot on a decommissioned manager — is worthless on its own.

The user-facing page, when this ships, will be `docs/kv-encryption.md` with a
cross-reference from `docs/kv-store.md`, `docs/upgrades.md` (backups) and
`docs/auth-and-acls.md` (`state:read`). Until then, the plan is the design.

---

## 1. Problem statement

Concretely, today:

- `kv_entry.value` is a plaintext `TEXT` column
  (`src/storage/migrations.rs:207-214`). The store makes no distinction
  between a feature flag and a database password.
- That plaintext is copied everywhere the store goes:
  - `<storePath>/store.sqlite` and its WAL;
  - every `legion state backup` file (`VACUUM INTO`,
    `src/storage/store.rs:2229`) — "a plain SQLite database — copy it
    off-host with ordinary tooling" (`docs/upgrades.md`);
  - the streamed replica in object storage when `persistence.type =
    "streaming"` (`docs/persistence-streaming.md`, `replicaUrl`);
  - in raft mode, every committed changeset in the raft log (a changeset
    carries the row's new value, `src/storage/raft/capture.rs:1-12`), every
    whole-DB snapshot under `raft.dataDir/<lane>/snapshots/`, and the
    snapshot image shipped to a joining manager.
- `state:read` is an ACL bypass for KV. The `kv` resource scopes policies by
  key path (`paths = ["team-a/*"]`), but `state:read` "can read every table in
  the state store" (`docs/auth-and-acls.md:44-56`), so `legion state query
  "select value from kv_entry"` returns every team's secrets to any principal
  with raw-state access. The UI's State Explorer is the same surface.

What an operator has to do instead today: full-disk encryption on every
manager, server-side encryption on the replica bucket, treat backup files as
secrets with their own custody chain, and never grant `state:read`. None of
that helps once a backup leaves the host, and the `state:read` hole has no
mitigation short of not granting it. `internal_docs/systemd-and-chef-analysis.md`
already lists "no encrypted data bags (KV values are stored in the manager's
state; ACLs gate access)" as a gap against Chef.

The ask is the same one every comparable system eventually answered: values
encrypted before they touch storage, with keys the operator controls, without
turning the simplest scheduler to run into something that needs a key
ceremony to restart.

---

## 2. Prior art

The systems closest to Legion's shape, what each does, and what we take or
reject. This section drove most of the design and most of the judgement
calls.

### 2.1 Nomad Variables and the keyring (the closest analogue)

Nomad's Variables are the KV-as-secret-store feature Legion's KV most
resembles, and its keyring is the reference design for this plan.

- **Cipher and envelope.** Variables are encrypted with AES-256-GCM
  ([`nomad/encrypter.go`](https://github.com/hashicorp/nomad/blob/main/nomad/encrypter.go):
  `aead.WithAeadType(kms.AeadTypeAesGcm)`), each under a data encryption key
  (DEK); the DEK is wrapped by a key encryption key (KEK). The key ID is bound
  into the ciphertext as additional authenticated data
  (`kms.WithAad([]byte(keyID))`) and stored beside it.
- **Where keys live — the lesson.** Before 1.9 the DEKs lived in `.nks.json`
  files in a `keystore` subdirectory of the data directory, *separate from
  raft*. HashiCorp's own motivation for moving them
  ([PR #23977](https://github.com/hashicorp/nomad/pull/23977),
  [issue #23665](https://github.com/hashicorp/nomad/issues/23665)): "we've
  encountered multiple instances of the keystore files not getting backed up
  separately from the Raft snapshot, resulting in failure to restore clusters
  from backup." Nomad 1.9 stores the wrapped DEKs in raft, and "automatically
  migrates the key storage for all key material on the first
  `root_key_gc_interval` after all servers are upgraded to 1.9.0"
  ([key management](https://developer.hashicorp.com/nomad/docs/manage/key-management)).
- **The default is honest but weak.** With the default `aead` provider "the
  KEK is stored in cleartext in Raft" — the docs warn that this "may expose
  data in case of a breach". With an external provider (`awskms`,
  `azurekeyvault`, `gcpckms`, `transit` —
  [`keyring` block](https://developer.hashicorp.com/nomad/docs/configuration/keyring))
  "the KEK is securely stored outside of Nomad and no cleartext key material
  exists on disk". Only one `keyring` block can be `active` in CE; changing it
  affects future rotations only, so inactive providers must stay configured
  until their wrapped keys are garbage-collected.
- **Rotation.** `root_key_rotation_threshold` default `720h`, the replacement
  key prepublished at half that so external JWKS consumers can pick it up;
  `root_key_gc_threshold` `1h`; `root_key_gc_interval` `10m`
  ([server block](https://developer.hashicorp.com/nomad/docs/configuration/server)).
  `nomad operator root keyring rotate -full` marks old keys `rekeying` and
  "Nomad will asynchronously decrypt and re-encrypt all variables". New wrapped
  keys replicate through raft, and "as each server replicates the new key, the
  server starts a task to decrypt the key material".

**Take:** the two-tier envelope; wrapped DEKs replicated in raft so backups
carry them; time-based rotation with a 30-day default; asynchronous rewrap
after rotation; the *migration gate* ("after all servers are upgraded"); the
key ID stored beside each ciphertext. **Reject:** the KEK-in-raft default —
it makes a backup self-decrypting, which is the exact threat this plan
exists for; and overloading one root key for both encryption and
workload-identity signing (Legion already has its own `signing_key` table
and rotation loop).

### 2.2 Vault — the barrier, the keyring, and seal/unseal

- "The storage backends used by Vault are also untrusted by design." "The
  security barrier automatically encrypts all data leaving Vault using a
  256-bit Advanced Encryption Standard (AES) cipher in the Galois Counter
  Mode (GCM) with 96-bit nonces"
  ([security model](https://developer.hashicorp.com/vault/docs/internals/security)).
- The encryption keys sit in a keyring encrypted by the **root key**; the
  root key is protected by the **unseal key**, split with Shamir (5 shares,
  threshold 3 by default) or held by an auto-unseal provider (cloud KMS,
  HSM, transit). A sealed Vault "can access physical storage but cannot
  decrypt any of the data on it"
  ([seal concepts](https://developer.hashicorp.com/vault/docs/concepts/seal)).
- Rotation adds a key to the keyring; "Vault can encrypt new writes to the
  storage backend using the new key, but still decrypt entries written under
  the previous key". Vault also rotates automatically "before performing
  ~2³² encryptions" per NIST SP 800-38D, tracked by
  `vault.barrier.estimated_encryptions`
  ([rotation internals](https://developer.hashicorp.com/vault/docs/internals/rotation)).

**Take:** storage-is-untrusted as the threat model; a keyring where old keys
keep decrypting and rotation never forces a rewrite; the NIST encryption-count
guard; the *concept* of a "sealed" state where the process is up but the
secrets are not. **Reject:** manual unseal as a default. Vault itself ships
auto-unseal because a key ceremony on every restart is unbearable, and it
would contradict Legion's headline claim.

### 2.3 Kubernetes — EncryptionConfiguration and KMS v2

- "By default, the API server stores plain-text representations of resources
  into etcd, with no at-rest encryption." Providers: `identity` (none),
  `secretbox`, `aesgcm` ("must be rotated every 200k writes"), `aescbc` (a
  padding-oracle caveat), `kms v1` (deprecated), `kms v2` (recommended)
  ([encrypt data](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)).
  The static-key providers keep the key in a file on the API server's disk,
  which the docs flag as the weak point; rotation is "add the new key first
  in the list, then `kubectl get secrets --all-namespaces -o json | kubectl
  replace -f -`".
- KMS v2 ([kms provider](https://kubernetes.io/docs/tasks/administer-cluster/kms-provider/),
  [KEP-3299](https://github.com/kubernetes/enhancements/blob/master/keps/sig-auth/3299-kms-v2-improvements/README.md)):
  the KEK stays in the remote KMS; the API server holds a DEK *seed* wrapped
  by the KEK and derives a single-use AES-GCM key per write with a KDF over
  the seed plus 32 random bytes — "This allows us to use a purely per-write
  random 12 byte nonce for AES-GCM because each generated DEK+nonce
  combination is unique." Each stored object carries the ciphertext, the
  `key_id` of the KEK that wrapped its seed, and the wrapped seed. The API
  server treats the `key_id` from the plugin's `Status` call as authoritative
  and rewrites stale objects when it changes.

**Take:** an ordered key list for the file-backed root key ("the first key
wraps, every key unwraps"); rewrite-all as the way a rotation *completes*;
the key id carried with each ciphertext. **Reject:** off-by-default (the
K8s-secrets-are-base64 lesson has been learned expensively by a decade of
clusters), and per-write calls to a remote KMS (v1's mistake, which is why
the DEK is a local object in this plan).

### 2.4 Consul and etcd — no at-rest encryption

Consul encrypts gossip (a shared symmetric key) and RPC (mTLS) and nothing
at rest; its KV store is plaintext on disk
([Consul encryption](https://developer.hashicorp.com/consul/docs/secure/encryption)),
and its answer to secrets is "use Vault". etcd is the same: Kubernetes'
own docs open with the plaintext-in-etcd statement above, and the recommended
mitigations are disk encryption or client-side encryption.

**Take:** the lesson, not the design. "Use disk encryption" protects a
stolen disk and nothing else — not a backup, not a bucket, not `state:read`.
"Use Vault" is precisely the second system Legion's KV exists so an operator
does not need.

### 2.5 Docker Swarm — encrypted by default, autolock as the hardening step

"The Raft logs used by swarm managers are encrypted on disk by default";
both the raft key and the TLS key are loaded into memory from disk on a
normal restart. Autolock is opt-in: the raft key is encrypted with an unlock
key, `docker swarm unlock` is required after every restart, and
`docker swarm unlock-key --rotate` rotates it — with the warning "without it
you will not be able to restart the manager"
([manager locking](https://docs.docker.com/engine/swarm/swarm_manager_locking/)).

**Take:** the posture exactly — encrypted at rest by default with a local
key, and *hardening* (a key that is not on the disk) as an opt-in step,
never a prerequisite for running the thing at all.

### 2.6 CockroachDB, AWS Secrets Manager, SOPS, Sealed Secrets, systemd-creds

- **CockroachDB** encryption at rest: an operator-supplied *store key* file
  wraps *data keys* that rotate automatically (default: one week); changing
  the store key rewraps the data keys on the next start rather than
  rewriting data ([docs](https://docs.cockroachlabs.com/docs/stable/encryption)).
  **Take:** rotating the outer key is a rewrap, not a rewrite.
- **AWS Secrets Manager** requests a fresh 256-bit data key from KMS for every
  secret-value change, stores the wrapped data key in the secret's metadata,
  and binds `SecretARN` + `SecretVersionId` into the KMS encryption context
  ([docs](https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html)).
  **Take:** bind the ciphertext to the identity of the secret so it cannot
  be transplanted.
- **SOPS** encrypts each value with AES-256-GCM under a per-file data key
  wrapped by one or more master keys (KMS, age, PGP, Vault), uses the value's
  path as AAD, and rotates with `sops rotate` / `updatekeys`
  ([repo](https://github.com/getsops/sops)). **Take:** the AAD-is-the-path
  pattern, and the shape of the client-side alternative (§12, alternative D).
- **Sealed Secrets** is the asymmetric variant: anyone can encrypt to the
  controller's public key, only the controller decrypts; name and namespace
  are bound in; keys renew every 30 days and are themselves stored as
  ordinary Kubernetes secrets; "without that private key … you can't decrypt
  it" ([repo](https://github.com/bitnami-labs/sealed-secrets)). Informs
  alternative D and the honesty about key loss.
- **systemd-creds** binds credentials to a host key
  (`/var/lib/systemd/credential.secret`) and/or the TPM2, by default both, so
  they "cannot be prepared on a machine other than the local one"
  ([CREDENTIALS](https://systemd.io/CREDENTIALS/)). Informs a possible future
  host-bound provider; a named non-goal here.
- **NIST SP 800-38D §8.3** limits the random-IV construction of GCM to 2³²
  invocations per key ([SP 800-38D](https://csrc.nist.gov/pubs/sp/800/38/d/final)).
  This is where every "rotate before N writes" rule above comes from, and
  why §6.6 has a count guard.

### 2.7 Distilled requirements

1. Storage is untrusted. A copy of the database, a backup, a replica, a
   snapshot, or a raft log must not yield a single KV value.
2. The key that makes the copy useful must live *outside* the store — on the
   manager's disk by default, in a KMS when the operator wants it — and the
   keys that change often (data keys) must live *inside* the store, replicated,
   so backups are restorable and every manager sees rotations for free.
3. On by default, with zero configuration, on a single-node `local_disk`
   cluster. Hardening is a config change, not a rebuild.
4. Rotation is cheap and never blocks reads: old keys keep decrypting; a
   rewrap converges the data to the active key in the background.
5. Losing the outer key must be loud, precise, and survivable for the rest
   of the cluster — the manager does not become unbootable over KV.
6. The extension point for external key management must not drag cloud SDKs
   into the binary.
7. No change to the KV API, the reactive-update semantics, or who is allowed
   to read what. Encryption at rest is not authorization.

---

## 3. Jobs to be done, by persona

**Security / platform admin**
- When someone copies a backup or a replica bucket, I want it to contain no
  usable secret, so a leaked file is an incident about a file and not about
  every credential in the company.
- When my compliance checklist says "secrets encrypted at rest with
  customer-managed keys", I want to point the cluster at our KMS in one
  config block, so I can answer the question with a config diff.
- When an engineer with broad access leaves, I want to rotate the keys and
  know the old ones are gone, so their copy of anything is dead.
- When I grant an on-call engineer raw state access for debugging, I want
  that not to also hand them every team's secrets, so `state:read` stops
  being a KV back door.

**Cluster operator**
- When I upgrade, I want encryption to turn on without me generating,
  distributing, or even thinking about a key, so the default cluster is safe
  without a ceremony.
- When I restore a backup onto a new host, I want to be told *exactly* what
  file I need and where, before anything starts, so a 3am restore does not
  end in a half-working cluster with unreadable secrets.
- When a manager restarts, I want it to come back on its own, so encryption
  never turns a reboot into a paging event.
- When I add a manager, I want it to get whatever it needs to read secrets as
  part of joining, so "join the cluster" stays one command.

**On-call responder**
- When a job is failing because a secret cannot be read, I want one status
  command that says whether the keyring is locked and which key is missing,
  so I do not debug the KV store when the problem is a file.

**Job author / app developer**
- When I write `{{ kv "db/password" }}` or `auth = { kv = "secrets/pull" }`,
  I want nothing to change — same commands, same values, same restart
  behavior — so encryption at rest is invisible to the jobspec.

**Autonomous agent**
- When I automate a rotation or check cluster health, I want the keyring
  state as structured output (`-j`) and the rotation as an idempotent call,
  so I can put it in a runbook without parsing prose.

*Out of scope for this plan:* the job author who wants the *manager* never
to see plaintext (client-side encryption, alternative D) — a different
product with a different template model.

### User scenarios

**S1 — The backup leaks.** A `legion state backup` file lands in a
misconfigured bucket. With this plan the file holds ciphertext and a keyring
of *wrapped* data keys; the root key is on the managers' disks (or in the
KMS) and nowhere in the file. Nothing is recoverable from it.

**S2 — KMS-backed cluster.** A platform team sets
`encryption.rootKey = { provider = "exec", command = ["legion-kms-aws",
"--key-id", "arn:…"] }` on every manager. No key material is on disk; every
manager start and every root-key rewrap is a KMS `Decrypt`/`Encrypt` call
visible in CloudTrail. Data keys still live in raft, wrapped, so backups
restore anywhere the KMS is reachable.

**S3 — A new manager joins.** `legion raft join-token` → `legion start
manager --join …`. In Phase 1 the operator also places the root key file (or
sets the env var) on the new host and the manager says so plainly if it is
missing. In Phase 4 the join flow delivers the root key itself.

**S4 — Rotation after an offboarding.** `legion keyring rotate` mints a new
data key (one replicated write); the background rewrap re-encrypts every
value under it and retires the old key. `legion keyring root-key generate`
plus `legion keyring root-key rewrap` does the same one level up. The old
key material is then deletable everywhere it was.

**S5 — Restore on a fresh host.** `legion start manager --restore-from
backup.sqlite` copies the DB in, then refuses to serve with:
`keyring locked: root key rk_9f31c2a7e0d4b6a1 (file provider) not found at
/var/lib/legion/keys/root.key — copy it from the original manager or supply
it via LEGION_ROOT_KEY`. The operator copies one file; the restore proceeds.

---

## 4. Goals and non-goals

**Goals**

1. Every KV value written by a manager running this feature is stored, logged,
   snapshotted, replicated, and backed up as AES-256-GCM ciphertext.
2. The root key never enters the state store, the raft log, a snapshot, or a
   backup — in any provider.
3. On by default on new and upgraded clusters, zero-config on `local_disk`,
   with a lazy migration that leaves existing plaintext rows readable until a
   rewrap converts them.
4. Data-key rotation is a single replicated write; rewrap is background,
   batched, resumable, and does not bump KV versions or trigger reactive
   restarts.
5. A locked keyring (root key unavailable) degrades KV to a precise
   `FAILED_PRECONDITION` and a metric, never takes down scheduling, and is
   fail-fast on the explicit restore path.
6. `state:read` and the State Explorer see ciphertext. KV path ACLs become the
   only way to read a value.
7. External key management via a process-boundary `exec` provider with a
   conformance harness, and reference plugins for AWS KMS and Vault Transit.
8. Full observability: `legion keyring status`, metrics, events, a UI card.

**Non-goals** (each is a real alternative or a follow-up, and each is named in
the public doc as a limitation)

- **Encrypting the rendered copies.** `job_config` / `task_config` rows hold
  the *rendered* results of KV references and templates
  (`src/manager/server.rs:670-785`, `869-…`), so a KV secret that a task
  consumes also exists, rendered, in those columns and in the worker's state
  file (`src/worker/state.rs:115-116`). This plan encrypts the source of
  truth only. The keyring is designed to be reused by the follow-up that
  covers derived columns (or that moves rendering to the worker); §6.9 states
  the residual exposure plainly.
- **Encrypting other secret-bearing tables** (`signing_key.private_key_pem`,
  `acl_token` hashes are already hashes). Same follow-up.
- **Client-side / end-to-end encryption** where the manager never sees
  plaintext (alternative D). It breaks server-side rendering, JSON patch, and
  the reactive model.
- **Native cloud KMS providers** linked into the binary (alternative to the
  `exec` provider). Deliberately deferred; see the calls doc.
- **Manual unseal / passphrase mode** (Docker autolock, Vault Shamir). A future
  provider, not v1.
- **Encrypting KV keys (paths)**, lock rows, or `job_kv_reference` edges. Paths
  are addressing, not secrets, and ACLs scope them.
- **Worker-side at-rest protection** for task dirs and `worker_state.json`.
- **Any FIPS validation claim.** AES-256-GCM is a FIPS-approved algorithm; the
  implementation (`ring`) is not a validated module. Say so.
- **A change to who may read a KV value.** ACLs remain the authorization
  model; encryption is confidentiality of storage only.

---

## 5. Background — what exists today

**Schema.** `kv_entry(key, value TEXT, value_type, version, created_at,
updated_at)` (`src/storage/migrations.rs:207-214`); `kv_lock`
(`218-224`); `job_kv_reference` (`228-236`). All three are owned by the
control lane (`src/storage/raft/ownership.rs:148-150`); any new table must be
classified there or the coverage test fails (`ownership.rs:289-292, 555-563`).

**Write and read paths — one chokepoint each.** `Store::put_kv` wraps
`put_kv_apply` in `self.replicated(Lane::Control, …)`
(`src/storage/store.rs:15616-15730`); `get_kv` (`15733`), `list_kv` (`15748`),
`delete_kv` (`15759`), `patch_kv` (`15813-15903`, a read-modify-write of the
JSON value inside one transaction). Every row is hydrated by
`kv_entry_from_row` (`5943-5955`). So encryption on write lives in
`put_kv_apply` / `patch_kv_apply`, and decryption on read lives in one row
mapper — there is no second path to the column.

**Raft mechanics decide where encryption must happen.** In raft mode a
mutation's body runs inside a SQLite session capture on the leader; the
captured changeset (row images) is what gets proposed, committed, and applied
byte-identically on every manager (`src/storage/raft/capture.rs:1-12`).
Therefore: encrypt *inside* the `_apply` body, before capture, and only
ciphertext ever enters the log, the snapshots, or a follower. Conversely,
followers serve reads from their own replica and must decrypt locally —
**every manager needs the root key**, not just the leader.

**Every consumer of plaintext goes through `Store::get_kv`/`list_kv`:**
jobspec `$` references (`src/manager/server.rs:670-705`), re-materialization
on KV change (`746-785`), template rendering (`812-844`) and dependency
extraction (`851-867`), the KV RPCs (`6276`, `6286`), artifact credentials
(`GetArtifactCredential`, `6313`), the CLI (`src/cmd/kv.rs`), the UI
(`ui/src/pages/KeyValue.tsx`), and the capability catalog
(`src/cmd/capabilities.rs:506-530`; there are no KV MCP tools today).
Decrypting in the row mapper makes all of them correct at once.

**KV reconciliation is triggered by the RPC handlers, not by row changes.**
`reconcile_kv_changes` is called from the put/delete/patch handlers
(`src/manager/server.rs:6269, 6376, 6407`), so a store-level rewrite of a
row (a rewrap) does not fire the reactive machinery — which is exactly what a
rewrap needs.

**Precedents inside Legion this plan mirrors.**
- A bootstrap-minted secret persisted next to the data: the raft cluster
  secret is minted at bootstrap and written to `<dataDir>/cluster_secret`, or
  taken from config, or delivered by the join token
  (`src/storage/raft/driver.rs:795-831`, `JoinTokenPayload` at `556-564`).
- A replicated key table with a rotation loop: `signing_key` stores the
  workload-identity keypair *including the private PEM* in the store
  (`src/storage/migrations.rs:422-442` — "the same trust domain as the CA
  material; whoever can read the store can mint identities"), and
  `start_workload_identity_rotation` ticks on the primary and reloads on
  standbys (`src/manager/daemon.rs:492-522`), driven by
  `workloadIdentity.rotationAge`.
- Secrets hashed, never stored: ACL and join tokens (`docs/auth-and-acls.md`).
- A version-gated feature: edge boot fences pre-commit dispatch on the
  worker's advertised `protocolVersion` (`docs/upgrades.md`, "Edge boot under
  skew"); Nomad gates its keyring migration the same way.

**Crypto already linked.** `ring 0.17` is in the dependency graph via
`jsonwebtoken`, `rustls`, `rcgen`, and `object_store` (`cargo tree -i ring`).
`ring::aead::AES_256_GCM` costs no new dependency. `argon2`, `sha2`, `rand`,
`base64`, `zeroize`, and `subtle` are already present.

**Migration runner.** `rusqlite_migration` refuses to open a database whose
`user_version` is ahead of the binary's migration list
(`MigrationDefinitionError::DatabaseTooFarAhead`), so an older binary cannot
open a store containing encrypted rows and hand ciphertext to jobs as values.
That makes downgrade after this release the same as after any schema-adding
release: restore a pre-upgrade backup.

**Backups.** `GetStateBackupReply { path, bytes, applied_index }`
(`proto/legion.proto:3649-3658`); `legion state backup` prints them. The
restore path (`--restore-from`) copies the DB in before the store starts
(`docs/upgrades.md:233-245`).

**Manager config.** Top-level blocks on `ManagerConfigFull`
(`src/structs/config.rs:1373-1407`): `workloadIdentity`, `acl`, `edgeBoot`,
`heartbeat`, `liveness`, … — the new `encryption` block sits beside them.

---

## 6. Proposed design

### 6.1 One concept: the keyring

```
   root key  (KEK)  ── lives OUTSIDE the store: a file on each manager,
        │             an env var, or an external KMS via `exec`.
        │             Never written to the DB, the raft log, a snapshot,
        │             or a backup.
        ▼ wraps
   data keys (DEKs) ── live INSIDE the store, wrapped, in `keyring_key`.
        │             Replicated like any control-lane table; exactly one
        │             is `active`, older ones are `retired` until every
        │             value has been rewrapped, then garbage-collected.
        ▼ encrypt
   KV values        ── `kv_entry.value` = AES-256-GCM ciphertext,
                      `kv_entry.enc_key_id` = which data key.
```

Three names, used consistently in config, CLI, docs, and errors:

- **root key** — the key encryption key. Identified by a fingerprint
  `rk_<first 16 hex of SHA-256(key bytes)>` for `file`/`env`, or by the
  provider's own key id for `exec` (e.g. a KMS ARN). Legion never persists
  it.
- **data key** — a 256-bit AES key, `dk_<ULID>`, generated by the leader,
  wrapped by the root key, stored in `keyring_key`.
- **keyring** — the set of data keys plus the root-key provider that unwraps
  them. `legion keyring …` is the whole operator surface.

Why two tiers and not one (the single-key design is alternative B): the thing
that must be *identical on every manager* (the root key) changes rarely and
is small; the thing that *changes on every rotation* (the data key) is
replicated, so a rotation is one raft write and every manager picks it up
without touching a file. It is the shape Nomad 1.9 and Kubernetes KMS v2 both
converged on after trying the alternatives.

### 6.2 What a stored value looks like

| Column | Plaintext row (legacy or `enabled = false`) | Encrypted row |
| --- | --- | --- |
| `value` | the normalized value | `base64(nonce ‖ ciphertext ‖ tag)` |
| `enc_key_id` | `NULL` | `dk_01J8…` |

- **Cipher:** AES-256-GCM, 96-bit random nonce, 128-bit tag, via
  `ring::aead`. The algorithm is a property of the *key row*
  (`keyring_key.algorithm = "aes-256-gcm"`), so a future cipher is a new key
  id, not a new envelope format.
- **AAD = the key path** (`kv_entry.key`, UTF-8). A ciphertext is bound to
  its path: copying `prod/db-password`'s ciphertext into `dev/scratch` (where
  the attacker has `kv:read`) with `state:write` fails authentication. Nomad
  binds the key id, SOPS binds the path, AWS binds the ARN — same idea.
- **Plaintext** is the normalized value string exactly as `put_kv_apply`
  computes it today (`Self::normalize_kv_value`), so `value_type` validation
  and JSON patching are unchanged.
- **Decryption happens in one place** (`kv_entry_from_row`, given a keyring
  handle): `enc_key_id IS NULL` → return the column as-is; otherwise look up
  the data key, `open` with the path as AAD, return the plaintext. An unknown
  key id or a failed tag is a `StorageError::KeyringLocked { key_id }` /
  `StorageError::Corrupt`, never a silent empty string.

The wire (`KvEntry.value` in `PutKv`/`GetKv`) is unchanged — plaintext over
TLS, as today. A `KvEntry.encrypted` boolean is added to the reply so the CLI
and UI can show a lock icon; nothing else changes on the API.

### 6.3 Root-key providers

A provider is one small trait with three operations:

```rust
pub trait RootKeyProvider: Send + Sync {
    /// Stable id of the key that `wrap` will use (a fingerprint or a KMS key id).
    fn active_id(&self) -> R<String>;
    /// Wrap a data key. Returns (wrapped bytes, root key id used).
    fn wrap(&self, dek: &[u8; 32]) -> R<(Vec<u8>, String)>;
    /// Unwrap a data key wrapped under `root_key_id`.
    fn unwrap(&self, wrapped: &[u8], root_key_id: &str) -> R<Zeroizing<[u8; 32]>>;
}
```

| Provider | Where the root key is | Wrap | Phase |
| --- | --- | --- | --- |
| `file` (default) | `<storePath>/keys/root.key`, mode `0600`, minted on first run | AES-256-GCM under the file's first key | 1 |
| `env` | `LEGION_ROOT_KEY` (same format as the file's contents) | same | 1 |
| `exec` | Wherever the command keeps it (AWS KMS, GCP KMS, Azure Key Vault, Vault Transit, an HSM, `systemd-creds`) | delegated to the command | 3 |

**File format — "the first key wraps, every key unwraps."** One base64
32-byte key per line; `#` comments allowed. Rotating the root key is:
generate a new line, put it *first* on every manager, run
`legion keyring root-key rewrap`, then delete the old line. The file is read
at start and re-read on `root-key rewrap` and whenever an unwrap fails with an
unknown root key id, so adding a line does not need a restart. This is the
Kubernetes ordered-key-list pattern with a file that a human can read.

**Minting.** With `provider = "file"`, if the file is missing *and the
keyring table is empty*, the leader mints 32 random bytes from the OS RNG,
writes the file (`0600`, directory `0700`), and logs
`Minted KV root key rk_… at <path>` — the same behavior as the raft cluster
secret. If the file is missing and the keyring table is *not* empty, that is
a lost key, not a fresh cluster: the keyring locks (§6.7). The two cases are
never confused because the store, not the file system, says which one it is.

**`exec` contract (Phase 3).** The manager runs `command` with one JSON
document on stdin and expects one on stdout, with a timeout (default `10s`):

```json
{"op":"wrap","plaintext":"<base64>"}           → {"ciphertext":"<base64>","key_id":"arn:aws:kms:…"}
{"op":"unwrap","ciphertext":"<base64>","key_id":"arn:…"} → {"plaintext":"<base64>"}
{"op":"describe"}                              → {"key_id":"arn:…"}
```

Non-zero exit or malformed output is a provider error (the keyring locks if
it happens at start; a wrap failure fails the rotation and nothing else).
`legion keyring provider test <command…>` runs a wrap/unwrap round trip and a
tamper check against a candidate command — the same conformance-harness idea
as `legion volume plugin test`. Reference plugins ship as scripts under
`plugins/keyring/aws-kms/` (`aws kms encrypt`/`decrypt`) and
`plugins/keyring/vault-transit/` (`vault write transit/encrypt/…`), and are
what the docs show. Native providers linked into the binary are a deliberate
non-goal (see the calls doc).

### 6.4 Config surface

```nickel
{
  # KV encryption at rest (docs/kv-encryption.md). ON BY DEFAULT: with no
  # block at all, values are encrypted under a data key wrapped by a root
  # key that the manager mints at <storePath>/keys/root.key on first start.
  # Every manager in a raft cluster needs the same root key.
  encryption = {
    enabled = true,
    rootKey = {
      provider = "file",                         # "file" | "env" | "exec"
      path = "/var/lib/legion/keys/root.key",    # file: default <storePath>/keys/root.key
      # var = "LEGION_ROOT_KEY",                 # env: default LEGION_ROOT_KEY
      # command = ["legion-kms-aws", "--key-id", "arn:aws:kms:us-east-1:…:key/…"],
      # timeout = "10s",                         # exec only
    },
    # Age past which the active data key is rotated automatically (one
    # replicated write; values are rewrapped in the background afterwards).
    dataKeyRotationAge = "720h",
  },
}
```

Three knobs. Nothing is added to job config, so `configs/jobs/maximal.ncl` is
untouched; `configs/manager.ncl` gains the commented block above.

`enabled = false` means: new writes are stored as plaintext rows
(`enc_key_id NULL`), existing ciphertext rows are still decrypted on read
while the root key is available, and `legion keyring rewrap --plaintext`
converts everything back. It exists for the mixed-version upgrade window
(§10) and for clusters that use KV purely as a queryable config store and
want `state query` over the values back.

### 6.5 CLI surface

Every verb takes `-j` and emits the standard envelope; mutating verbs accept
`--idempotency-key`.

```console
$ legion keyring status
Encryption:   enabled (aes-256-gcm)
Root key:     file  rk_9f31c2a7e0d4b6a1  /var/lib/legion/keys/root.key
State:        unlocked
Data keys:
  dk_01J8ZQ3W7K5V9C2N4T6X8B0D2F   active    created 2026-09-21T10:02:11Z   age 3d   values 412
  dk_01J7A9P2R6M1E4H8K3W5Y7Z0B9   retired   created 2026-08-22T10:02:11Z   values 18   (rewrap in progress)
Plaintext rows: 0
Rewrap:       running  430/430 scanned, 18 pending

$ legion keyring rotate                 # new active data key; background rewrap follows
$ legion keyring rewrap                 # re-encrypt every value under the active key now
$ legion keyring rewrap --plaintext     # decrypt every value (before `enabled = false` or a downgrade)
$ legion keyring root-key generate      # prints one key line for the operator to place FIRST in root.key on every manager
$ legion keyring root-key rewrap        # rewrap every data key under the current root key (file: first line; exec: provider's active key)
$ legion keyring provider test -- legion-kms-aws --key-id arn:…
$ legion keyring reset --i-understand-encrypted-values-will-be-lost   # break-glass, Phase 2 (§6.7)
```

The locked case, which is the one the responder persona meets:

```console
$ legion kv get prod/db/password
error: keyring locked: root key rk_9f31c2a7e0d4b6a1 (file provider) is not
available at /var/lib/legion/keys/root.key on manager mgr-b. Copy it from a
manager that has it, or set LEGION_ROOT_KEY. `legion keyring status` shows
every key this manager cannot unwrap.  (FAILED_PRECONDITION, retryable: true)
```

ACLs: `status` is `operator:read`; `rotate`, `rewrap`, `root-key rewrap`,
`reset` are `operator:write` (raft, manager configs, and now keys — the
existing vocabulary, no new resource). `root-key generate` and
`provider test` are client-local and need no manager.

### 6.6 Rotation

**Data key (automatic and on demand).** A rotation is one control-lane
replicated write: insert a new `keyring_key` row as `active`, flip the old
one to `retired`. Every manager applies the row, unwraps the new key with
its root key, and starts encrypting new writes under it. Reads of values
still under the retired key keep working because the retired key stays in
the keyring. Rotation happens:

- at `dataKeyRotationAge` (default `720h`, Nomad's number), checked by a
  loop shaped exactly like `start_workload_identity_rotation`: ticks on the
  primary/leader, no-ops elsewhere;
- when the leader has performed 2³¹ encryptions under the active key since
  it loaded it — the NIST SP 800-38D random-IV bound at half strength. The
  counter is in-memory per process; a restart resets it, which only matters
  if a single key sees more than 2³¹ writes between two restarts *and* the
  30-day rotation has not fired, i.e. never for a KV whose writers are humans
  and agents. Documented as such;
- on `legion keyring rotate`.

**Rewrap (background, after every rotation, and on demand).** The leader
scans `kv_entry` for rows whose `enc_key_id` is not the active key (or is
`NULL`), and rewrites them in batches of 256 rows, one replicated write per
batch, throttled to one batch per 100 ms. A rewrap:

- does **not** change `version` or `updated_at` — the value did not change;
- does **not** fire `reconcile_kv_changes` — it is a store-level write, not
  an RPC (`§5`), so no job restarts;
- is resumable: progress is derivable from the rows themselves, so a
  leadership change simply restarts the scan on the new leader;
- retires a key for good (state `retired` → row deleted) only when no row
  references it. `legion keyring status` shows the count.

Two policies were on the table: Nomad's explicit `rotate -full` and the
automatic follow-up rewrap. Automatic won (calls doc): the keyring converges
to a single key on its own, and a retired key that lingers for months because
nobody ran the second command is the failure mode to design out.

**Root key.** For `file`/`env`: `root-key generate` → place first on every
manager → `root-key rewrap` (one replicated write rewrapping every
`keyring_key` row under the first-line key, recording its `root_key_id`) →
remove the old line. Until the old line is removed, the file's *other* lines
keep unwrapping rows they wrapped, so the sequence is safe in any order
except deleting the old line first (which locks). For `exec`: providers with
their own rotation (AWS KMS key rotation keeps old material internally) need
nothing; `root-key rewrap` re-wraps under whatever the provider's `describe`
reports as active, and is what completes a manual key change.

### 6.7 Startup, locking, and failure modes

At startup, after migrations, and on every applied change to `keyring_key`,
each manager (re)evaluates the keyring:

| Store says | Provider says | Result |
| --- | --- | --- |
| no `keyring_key` rows, `enabled` | `file` and no file | **fresh cluster**: mint root key, then (leader only) mint the first data key |
| no rows, `enabled` | key available | leader mints the first data key |
| rows exist | every `root_key_id` unwraps | **unlocked** |
| rows exist | some `root_key_id` cannot be unwrapped | **locked** for those keys (the active one → writes fail; a retired one → reads of values under it fail); `legion keyring status` names each |
| `enabled = false` | anything | no minting; existing ciphertext rows still decrypt if the key is present |

**Locked is a KV condition, not a manager condition.** The manager starts,
joins raft, schedules, serves every non-KV RPC, and reports
`legion_keyring_unlocked 0`. KV reads and writes return the
`FAILED_PRECONDITION` above; template re-rendering on KV change logs the
same error; job submits that reference KV fail with it. This is Vault's
"sealed but running" and the opposite of Docker's "will not start", chosen
because losing quorum over a missing file is a worse incident than failing
KV loudly (calls doc). The one exception is `--restore-from`: an operator is
present and the action is explicit, so the restore **refuses to proceed**
when the restored keyring cannot be unlocked, naming the root key id and the
path.

**Every manager needs the root key (raft).** Followers serve KV reads from
their own replica. In Phase 1 the operator copies the file or sets the env
var on each manager, exactly as `persistence.raft.clusterSecret` works today;
a manager without it runs locked and says so. Phase 4 makes the raft join
flow deliver the root key (§8).

**Lost root key.** If the root key is truly gone, the values encrypted under
data keys it wrapped are gone — that is the property being paid for, and the
docs say it in those words (Sealed Secrets and Docker say the same). The
break-glass path is `legion keyring reset
--i-understand-encrypted-values-will-be-lost` (Phase 2): it deletes every
undecryptable `kv_entry` row (reporting the keys, so the operator knows what
to re-put), drops the unusable keyring rows, and initializes a fresh keyring.
Jobs that referenced the deleted keys fail through the existing
`missing_kv_dependency` path (`docs/kv-store.md`), which is the correct
outcome: loudly, per job, recoverable by re-putting the value.

**Tampering.** A modified ciphertext or a transplanted one fails the GCM tag
(AAD = path) and reads as `StorageError::Corrupt` naming the key path — never
as a wrong value.

**Provider failures at runtime.** `exec` is called only at start, on keyring
changes, on rotation, and on `root-key rewrap` — never on a KV read or write
(data keys are cached in memory, zeroized on drop). A provider outage
therefore cannot slow or fail KV traffic; it can only delay a rotation, which
is logged and retried on the next tick.

### 6.8 Backups and restore

- `GetStateBackupReply` gains `encryption { enabled, root_key_provider,
  root_key_ids[] }`. `legion state backup` prints, after the path:
  `KV values in this backup are encrypted. Root key rk_9f31… (file provider,
  /var/lib/legion/keys/root.key) is NOT included — keep it with the backup.`
- `--restore-from` verifies the keyring before serving (§6.7).
- `docs/upgrades.md` "Backup and restore" gets a paragraph, and the backup
  capability summary in `src/cmd/capabilities.rs` says the same.

### 6.9 What this closes, and what it does not

**Closed.** A copy of `store.sqlite`, a backup file, the streamed replica, a
raft snapshot, the raft log, or a snapshot image in flight to a joiner
yields no KV value without the root key. `state:read` (raw SQL, the State
Explorer, saved queries) sees ciphertext, so KV path ACLs are the only read
path — a real ACL fix that comes for free.

**Not closed, stated in the public doc.**

1. **The manager's memory and the root key's own storage.** With the default
   `file` provider, an attacker with root on a manager host has both the DB
   and the key. That is the same posture as Docker Swarm's and Nomad's
   defaults; the `exec` provider with a KMS is the upgrade.
2. **Rendered copies.** A value consumed by a task is rendered into
   `job_config` / `task_config` rows (plaintext), shipped to the worker, and
   written to the worker's state file and task dir. Encrypting the source of
   truth does not encrypt its derivatives. Follow-up plan; §4.
3. **History.** Raft log entries and streamed WAL segments written *before*
   the upgrade contain plaintext until compaction / retention removes them;
   `rewrap` rewrites rows, not history. The doc tells operators to rotate
   bucket retention and let a snapshot compact the log after enabling.
4. **`GetArtifactCredential` still has no KV ACL** (`internal_docs/TODO.md`,
   "Auth, TLS & security"). Unchanged by this plan; still open.

### 6.10 Observability

- **Metrics** (`docs/metrics.md`): `legion_keyring_unlocked` (gauge, 1/0),
  `legion_keyring_keys{state="active"|"retired"}` (gauge),
  `legion_keyring_active_key_age_seconds` (gauge),
  `legion_kv_rows{encryption="encrypted"|"plaintext"}` (gauge, refreshed
  with the rewrap scan and on the metrics tick),
  `legion_kv_decrypt_failures_total{reason="locked"|"corrupt"}` (counter),
  `legion_keyring_provider_calls_total{op,outcome}` (counter, `exec`).
- **Events** (`docs/event-stream.md`): `keyring.initialized`,
  `keyring.rotated {key_id}`, `keyring.rewrap_completed {rows}`,
  `keyring.locked {root_key_id}`, `keyring.unlocked`. Never a key byte.
- **Logs**: key ids only. The root key file path is logged; its contents
  never are (AGENTS.md tracing guidance).
- **UI**: a card on the Manager page (`ui/src/pages/Manager.tsx`) showing
  provider, state, and the key list from `GetKeyringStatus`; a lock icon on
  encrypted rows in the KV page.

### 6.11 Performance

AES-256-GCM on values of a few hundred bytes is single-digit microseconds
with `ring`'s AES-NI path; `list_kv` decrypts N rows in the same pass that
hydrates them. Nothing on the scheduler or heartbeat hot path touches KV, so
no scheduler benchmark is affected (the "before/after numbers" rule in
AGENTS.md applies to scheduler changes; this is not one). Phase 1 still
reports `legion kv put/get` micro-numbers before and after, because the KV
reactive path renders templates on every reconciliation. Data keys are
unwrapped once and cached; the `exec` provider is never on a request path.

---

## 7. Proto, struct, config, and persistence changes

**Migration** (one, additive):

```sql
ALTER TABLE kv_entry ADD COLUMN enc_key_id STRING;          -- NULL = plaintext row
CREATE TABLE keyring_key (
    key_id       STRING PRIMARY KEY,   -- dk_<ULID>
    algorithm    STRING NOT NULL,      -- 'aes-256-gcm'
    wrapped_key  BLOB   NOT NULL,      -- DEK wrapped by the root key
    root_key_id  STRING NOT NULL,      -- rk_<fingerprint> or the provider's key id
    root_provider STRING NOT NULL,     -- 'file' | 'env' | 'exec'
    state        STRING NOT NULL,      -- 'active' | 'retired'
    created_at   INTEGER NOT NULL,
    retired_at   INTEGER
);
```

`keyring_key` is classified `Owner::Lane(Lane::Control)` in
`src/storage/raft/ownership.rs` — the lane that owns `kv_entry`, so a rewrap
batch and a key-state flip are one changeset. Adding a table to a lane is a
rolling-safe change (`docs/multi-raft.md`, "Table ownership is a cluster-wide
agreement"); the column addition is covered by §10.

**Proto** (`proto/legion.proto`, each RPC and field with public-facing
comments):

- `rpc GetKeyringStatus (GetKeyringStatusRequest) returns (GetKeyringStatusReply)`
  — `operator:read`. Reply: `enabled`, `algorithm`, `root_key_provider`,
  `root_key_id`, `state` (`unlocked|locked|pending_upgrade|disabled`),
  `keys[] {key_id, state, created_at, retired_at, root_key_id, value_count}`,
  `plaintext_rows`, `rewrap {running, scanned, pending}`, `locked_key_ids[]`.
- `rpc RotateDataKey`, `rpc RewrapKv {plaintext: bool}`, `rpc RewrapRootKey`,
  `rpc ResetKeyring {confirm: string}` — `operator:write`, each with
  `idempotency_key` and `deduplicated` (AGENTS.md, retry-safe mutations).
- `KvEntry.encrypted` (bool) on replies.
- `GetStateBackupReply.encryption` (message) as in §6.8.

**Structs / config**: `EncryptionConfig` / `EncryptionConfigFull` +
`RootKeyConfig` on `ManagerConfigFull`; the commented block in
`configs/manager.ncl`. No job-config change, so no `maximal.ncl` change
(stated so the rule is visibly checked, not skipped).

**Store**: a `Keyring` handle (`Arc<RwLock<KeyringState>>`) on `Store`,
constructed in `start_storage` from the config; `kv_entry_from_row` gains
access to it; `put_kv_apply` / `patch_kv_apply` encrypt; new
`rewrap_kv_batch`, `rotate_data_key`, `rewrap_root_key`, keyring load and
projection hooks (`project_changeset_side_effects` reloads the keyring when
a changeset touches `keyring_key`).

**CLI / catalog / MCP**: `src/cmd/keyring.rs`; catalog entries
`keyring.status` (read-only) and `keyring.rotate|rewrap|root_key_rewrap|reset`
(`mutates: true`, `read_only_safe: false`); MCP tool
`legion_keyring_status` (read-only) and `legion_keyring_rotate`
(`mutates: true`, behind `--allow-write`). `tests/acl_endpoints.rs` gains the
four RPCs — `every_manager_rpc_is_classified` fails the build otherwise.

**Docs**: new `docs/kv-encryption.md`; cross-links from `docs/kv-store.md`,
`docs/upgrades.md`, `docs/auth-and-acls.md` (`state:read` now sees
ciphertext), `docs/persistence-streaming.md` (history caveat),
`docs/metrics.md`, `docs/event-stream.md`, `docs/agent-readiness.md`; index
entries in `docs/README.md` and `README.md`.

---

## 8. Implementation phases

Each phase ships on its own and is worth shipping on its own.

**Phase 1 — Encrypt at rest (default on).** `keyring_key` + `enc_key_id`
migration; `file` and `env` providers; encrypt on put/patch, decrypt in the
row mapper; lazy migration (legacy rows readable); keyring initialization on
the leader with the upgrade gate (§10); locked mode with the precise error;
`legion keyring status` + `GetKeyringStatus`; the backup reply/CLI reminder
and the restore fail-fast; `legion_keyring_unlocked` and the row gauges; the
public doc. *Value:* every new write is encrypted; backups stop being
secrets. No rotation yet — the first data key is the only one.

**Phase 2 — Rotation and rewrap.** Automatic data-key rotation
(`dataKeyRotationAge`, the 2³¹ guard), `legion keyring rotate|rewrap
[--plaintext]`, background rewrap after rotation with GC of unreferenced
keys, `root-key generate|rewrap` for `file`/`env`, `reset` break-glass, the
remaining metrics and events, the UI card. *Value:* S4 (offboarding) and the
"finish the migration" story for upgraded clusters.

**Phase 3 — External key management.** The `exec` provider and its JSON
contract, `legion keyring provider test`, reference plugins for AWS KMS and
Vault Transit under `plugins/keyring/`, `root-key rewrap` for providers.
*Value:* S2; the compliance persona's checkbox with no SDK in the binary.

**Phase 4 — Root key delivery on raft join.** The leader's `Join` handler
returns the root key material encrypted under a key derived from the join
token's secret (HKDF-SHA256, info `legion-keyring-join-v1`) for the `file`
provider; the joiner writes it to its `root.key` before its store starts.
`env` and `exec` clusters need nothing. *Value:* S3 — "join the cluster"
stays one command.

**Follow-up plans, not phases of this one:** encrypting derived columns or
moving rendering to the worker; a `passphrase` / manual-unlock provider; a
host-bound (TPM2 / `systemd-creds`) provider.

---

## 9. Testing plan

**Unit (Phase 1).** Round trip; AAD mismatch (same ciphertext, different
path) fails; tampered tag fails; unknown `enc_key_id` → `KeyringLocked`;
`NULL enc_key_id` passes through; the `file` format parser (first line
wraps, every line unwraps, comments, trailing newline, wrong length);
fingerprint stability; `env` provider equivalence; zeroization on drop.

**Store (Phase 1).** `put/get/list/patch/delete` with encryption on; a
patch re-encrypts and bumps `version` exactly once; a legacy plaintext row
beside encrypted rows lists correctly; `enabled = false` writes plaintext
and still reads ciphertext; the fresh-vs-locked table in §6.7, row by row,
including "file missing but rows exist → locked, not minted".

**Raft (Phase 1, in `tests/`).** A `raft_apply_roundtrip`-style test that
captures a `put_kv` changeset and asserts the plaintext bytes do **not**
appear in the changeset blob or the snapshot image, and that a follower's
`get_kv` returns the plaintext; snapshot install to a joiner keeps
decryptability; a joiner *without* the root key reports locked and serves
non-KV RPCs; the keyring-initialization gate does not fire while any voter
reports an older protocol version.

**Rotation (Phase 2).** After `rotate`, old values still read; the rewrap
converges (`plaintext_rows == 0`, one active key, retired key GC'd) without
changing any `version` and without any `reconcile_kv_changes` call (spy on
the debounce queue); a leadership change mid-rewrap resumes; the 2³¹ guard
(counter injected); `root-key rewrap` then old-line removal keeps reading;
deleting the old line first locks with the right key id; `rewrap
--plaintext` then `enabled = false` round trip; `reset` reports exactly the
lost keys and jobs fail through `missing_kv_dependency`.

**Provider (Phase 3).** The conformance harness against a fake command
(round trip, tamper, timeout, non-zero exit, malformed JSON); a provider
outage during a rotation tick is retried and never touches KV traffic.

**Join (Phase 4).** A joiner with only a join token ends up unlocked; the
join token's secret alone does not decrypt anything in the store.

**ACL.** `tests/acl_endpoints.rs` entries for the four RPCs; `status` denied
without `operator:read`.

**Backup/restore.** The reply carries `encryption`; `--restore-from` with the
key proceeds, without it refuses with the path in the message.

**Micro-benchmark.** `legion kv put/get` p50/p99 before and after Phase 1, in
the PR.

---

## 10. Rollout and backward compatibility

**Upgrading a `local_disk` manager.** The migration adds the column and the
table; the keyring initializes on first start; new writes are encrypted;
existing rows stay plaintext and readable. `legion keyring status` shows
`Plaintext rows: N`; `legion keyring rewrap` (Phase 2) converts them. Nothing
an operator must do to stay working; one command to finish.

**Upgrading a raft cluster — the gate.** A changeset that inserts into
`keyring_key` or writes `kv_entry.enc_key_id` cannot be applied by a manager
that has not run this release's migration. That constraint exists for every
schema-adding release; what is new is that this feature would trigger such a
write *on its own*, at startup, on the first upgraded leader. So the leader
**does not initialize the keyring until every raft voter advertises a
manager protocol version that includes it** — Nomad's "after all servers are
upgraded to 1.9.0" rule, and the same fencing edge boot applies to workers.
Until then `legion keyring status` reports `pending-upgrade`, writes are
plaintext exactly as before, and a mixed-version cluster held for weeks is
merely un-encrypted, never broken. The manager-plane skew table in
`docs/upgrades.md` gets the row. (Phase 1 adds each voter's protocol version
to the raft membership view if the transport does not already expose it;
it is also useful on its own.)

**Downgrading.** `rusqlite_migration` refuses a database ahead of the binary
(`DatabaseTooFarAhead`), so an old binary cannot open a store with encrypted
rows and hand ciphertext to jobs. The path is the same as any schema-adding
release: `legion keyring rewrap --plaintext`, then restore a pre-upgrade
backup or run the previous binary against a `--restore-from` of it.
`docs/upgrades.md` says so.

**Turning it off.** `encryption.enabled = false` stops encrypting new writes
and keeps decrypting old ones while the key is present; `rewrap --plaintext`
finishes the job. The root key file can then be deleted.

**History.** Pre-upgrade raft log entries and streamed WAL segments keep
plaintext until compaction/retention removes them (§6.9). The doc tells the
operator to take a snapshot (`raft.snapshotEntries` or a restart of the
leader) and rotate bucket retention after enabling.

**Workers.** Nothing changes on the worker plane; `protocol_version` is not
bumped for this feature.

**What is default-on.** Encryption itself, with the `file` provider. Nothing
else: no automatic root-key rotation (that needs a human or a KMS), and the
`exec` provider is opt-in.

---

## 11. Alternatives considered

The recommended design (A) is what §6 describes. The others were genuinely on
the table; each is written so a reader can see what it would have felt like
to run.

### A. Envelope encryption with a replicated keyring and pluggable root-key providers (recommended)

Root key outside the store (`file` by default, `env`, `exec`→KMS); wrapped
data keys in raft; AES-256-GCM per value with the path as AAD; time-based
rotation plus background rewrap; locked-not-dead on a missing key.

- **Pros.** Backups and replicas are useless without a key they never
  contain, in every provider. Rotation is one replicated write and never
  needs a file touched on N managers. Every manager sees a rotation for free.
  KMS integration needs no SDK. Zero config on a single node. Closes the
  `state:read` hole. It is the design Nomad 1.9, Kubernetes KMS v2, AWS
  Secrets Manager, and CockroachDB all converged on.
- **Cons.** Two key tiers to explain instead of one. A new table and column,
  a new CLI group, four RPCs. The default `file` provider protects copies of
  the DB, not a compromised manager host. A lost root key loses the values —
  a new thing an operator can lose. The upgrade gate adds a state
  (`pending-upgrade`) to reason about.

### B. One symmetric key from config, values encrypted directly under it

`encryption = { key = "<base64>" }` or `LEGION_KV_KEY`; every value AES-GCM
under that key; rotation = new key in config on every manager, restart, and
re-encrypt everything. Kubernetes' `aesgcm`/`secretbox` providers and
pre-1.9 Nomad, roughly.

- **Pros.** Smallest possible implementation: one column, no keyring table,
  no RPCs beyond status. One concept. Fine for a single manager.
- **Cons.** Rotation is a rewrite of every value *and* a coordinated
  config change on every manager with a restart, during which managers
  disagree about the key and reads fail on some. No KMS story without
  handing the KMS-held key to the manager in plaintext (which is B again).
  Backups and the key are still separate things to lose. Every problem Nomad
  hit with the keystore directory, on purpose.

### C. No application-level encryption: rely on disk and bucket encryption

Document LUKS/dm-crypt on managers, SSE on the replica bucket, and "treat
backups as secrets". Consul's and etcd's answer.

- **Pros.** Zero code, zero new failure modes, zero keys for Legion to
  manage; a compromised host is not made worse.
- **Cons.** Protects a stolen disk and nothing else: a backup copied
  off-host is plaintext the moment it leaves the encrypted volume, and
  `state:read` still returns every secret to anyone who has it. Does not
  answer the compliance question ("customer-managed keys for secrets at
  rest") at all, so users who need it run Vault beside Legion — the second
  system the KV store exists to avoid.

### D. Client-side / end-to-end: the manager never sees plaintext

`legion kv put --sealed-for job:api …` encrypts to a per-job or per-task
identity key (Legion already issues workload identities); the manager stores
opaque ciphertext; the task's runtime decrypts at start. SOPS/age and Sealed
Secrets, applied to a scheduler.

- **Pros.** The strongest posture: a compromised manager, its DB, and its
  root key together yield nothing. Operators with `kv:read` cannot read
  secrets either — encryption *is* the authorization. Attractive for
  multi-tenant clusters.
- **Cons.** Breaks the product as it exists: server-side template rendering
  (`{{ kv "…" }}` into env/files), JSON patch, `$` references in jobspecs,
  and the reactive `onChange` model all require the manager to see values.
  Needs a key-distribution subsystem to tasks, per-identity re-encryption on
  identity rotation, and a worker-side decrypt in every task runtime plugin.
  A different, larger plan; the keyring from A is still the right storage
  layer underneath it if it ever ships.

### E. Delegate secrets to an external store and resolve references

`$vault:secret/data/db#password` or `$aws-sm:…` references resolved at
render time; Legion stores no secrets. Nomad's historical Vault integration.

- **Pros.** No key management in Legion; the secret store's audit, rotation,
  and lease model come for free; the compliance persona already has one.
- **Cons.** A hard runtime dependency on another service for every render
  and every KV-driven restart; the reactive `watch`/`onChange` semantics need
  a polling adapter per backend; rendered copies still land in `task_config`
  (the same residual as A); and it abandons the users who chose Legion's KV
  precisely so they would not have to run Vault. Worth doing *later* as a
  reference resolver on top of A, not instead of it.

### F. Vault-style seal: Shamir shares or a passphrase, manual unseal on start

The root key never rests on disk; a restart needs `legion keyring unlock`
with a threshold of shares (or one passphrase — Docker autolock).

- **Pros.** The strongest posture available without a KMS; two-person
  control over restarts.
- **Cons.** Every manager restart is a paging event until a human types
  something; a rolling upgrade becomes a ceremony; Vault itself ships
  auto-unseal because of this. Contradicts "simplest scheduler to run" as a
  default. As an *opt-in provider* it is compatible with A (a `passphrase`
  provider whose `unwrap` waits for `legion keyring unlock`) and is listed as
  a follow-up.

### Smaller forks inside A (each is an entry in the calls doc)

| Fork | Chosen | Rejected | One-line why |
| --- | --- | --- | --- |
| Default | on, `file` provider auto-minted | off until configured (K8s) | the default cluster must be safe without a ceremony; Docker/Nomad ship this posture |
| What is encrypted | every KV value | only entries flagged `--secret` / under `secrets/` | no "forgot to flag it" failure; SQL blind to *all* KV closes `state:read` fully |
| Root key at rest | outside the store | wrapped KEK in raft (Nomad default) | a backup must not be self-decrypting |
| Cipher | AES-256-GCM via `ring` (already linked) + rotation guard | XChaCha20-Poly1305 (no nonce bound, new crate); HKDF per-value keys (K8s v2) | zero new deps, FIPS-approved algorithm, guard is cheap |
| KMS | `exec` provider + reference scripts | native SDK providers | no cloud SDKs in a 26 MB binary; one process contract covers every KMS |
| Missing root key | KV locks, manager runs | manager refuses to start | losing quorum over a file is the worse incident |
| Rewrap after rotation | automatic, background | explicit `rotate -full` (Nomad) | a lingering retired key is the failure mode to design out |
| Storage format | base64 in existing `TEXT` + `enc_key_id` | BLOB column; magic prefix | `state query` output stays explainable; no ambiguity with plaintext |
| Upgrade safety | gate initialization on all voters' versions | "upgrade managers first" doc rule only | a doc rule cannot stop a startup write |
| Rendered copies | out of scope, named | encrypt `task_config`/`job_config` now | a separate plan with its own trade-offs (worker-side rendering may be the real answer) |

---

## 12. Open questions

1. **Should `enabled` default to `true` on *upgraded* clusters, or only on
   new ones?** Lean: `true` everywhere — the lazy migration means nothing
   breaks, the gate means nothing breaks mid-roll, and a two-tier default is
   not explainable at 3am. Revisit if the `state query` regression turns out
   to matter to real users of KV-as-config.
2. **Is a per-member manager protocol version already visible through the
   raft transport?** The transport versions payloads with `PROTOCOL_VERSION`
   and names mismatches in errors (`docs/upgrades.md`), but the plan needs
   each voter's version as data for the gate. Lean: add it to the membership
   view in Phase 1; it is useful on its own for `legion raft status`.
3. **Should the join flow (Phase 4) deliver the root key at all, or should
   multi-manager clusters be steered to `env`/`exec`?** Lean: deliver it —
   a joining manager receives the entire database anyway, so it does not
   widen the trust boundary, and "join is one command" is the promise.
4. **Where should KMS-backed clusters keep the *fingerprint* of the current
   provider key for `status`?** Lean: `describe` on the provider, cached;
   nothing persisted beyond `keyring_key.root_key_id`.
5. **Should `legion state query` grow a decrypting helper for operators with
   both `state:read` and `kv:read`?** Lean: no. It would re-open the hole
   by another door; `legion kv get` is the read path.
6. **Should the 2³¹ guard be persisted rather than in-memory?** Lean: no —
   the 30-day rotation dominates by orders of magnitude; document the
   assumption and revisit if a KV write-rate metric ever shows otherwise.
7. **Does the rendered-copies follow-up encrypt columns, or move rendering to
   the worker (Nomad's `template` + Variables model)?** Not this plan's to
   answer; noted because it decides whether the keyring must also be readable
   by workers.

---

## 13. Acceptance criteria

**Phase 1**
- A fresh `local_disk` manager with no `encryption` block starts, logs the
  minted root key path, and `legion kv put` followed by
  `sqlite3 store.sqlite "select value, enc_key_id from kv_entry"` shows
  base64 and a `dk_` id; `legion kv get` returns the value.
- `legion state query "select value from kv_entry"` returns ciphertext for
  every encrypted row.
- A `raft_*` integration test asserts the plaintext is absent from the
  changeset and the snapshot image, and present in a follower's `get_kv`.
- Deleting the root key file and restarting yields a running manager, the
  `keyring locked` error on `kv get` naming the key id and path,
  `legion_keyring_unlocked 0`, and a passing `/readyz`.
- `--restore-from` without the root key refuses with the path in the
  message; with it, proceeds.
- A three-manager cluster where one voter runs the previous release reports
  `pending-upgrade` and writes plaintext; upgrading the last voter flips it
  to `unlocked` and encrypted writes without a restart of the others.
- `legion state backup -j` carries `encryption.root_key_ids`; the human
  output prints the "NOT included" reminder.
- `docs/kv-encryption.md` exists, `cargo test --all --locked` and
  `cargo test --test acl_endpoints` pass, and the calls doc has its
  implementation-time entries.

**Phase 2**
- After `legion keyring rotate`, `status` shows two keys; within the rewrap
  window it shows one, `plaintext_rows 0`, and no KV `version` changed
  (asserted in a test) and no allocation restarted.
- The root-key sequence (generate → place first → `root-key rewrap` → delete
  old line) keeps every read working at every step; reversing the last two
  steps locks with the right id.
- `rewrap --plaintext` followed by `enabled = false` leaves a store the
  previous release can read after a restore.
- `reset` lists exactly the lost keys, and affected jobs show
  `failed:missing_kv_dependency`.

**Phase 3**
- `legion keyring provider test` passes against the AWS KMS and Vault
  Transit reference plugins and fails against a tampering fake with a
  message naming the failed check.
- With an `exec` provider, no `rk_`/root-key material exists anywhere under
  `storePath`, and a KMS outage during a rotation tick is logged and retried
  with KV reads and writes unaffected.

**Phase 4**
- `legion raft join-token` → `legion start manager --join` on a host with no
  root key file ends unlocked, and the join token's payload alone decrypts
  nothing in a copied store.
