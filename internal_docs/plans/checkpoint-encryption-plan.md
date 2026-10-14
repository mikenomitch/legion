# Plan: Encrypting Published Checkpoints

> **Judgement calls:** [`calls/checkpoint-encryption-plan_calls.md`](../calls/checkpoint-encryption-plan_calls.md)

Status: **SHIPPED** (2026-10-11, plan and code on the same branch). Public
pages: [`docs/suspend-and-resume.md`](../../docs/suspend-and-resume.md)
("Published checkpoint encryption") and
[`docs/artifacts.md`](../../docs/artifacts.md) ("Publishing content").
Shipped as designed, plus two performance fixes found while measuring (calls
entry 6): the sealer hashes with `ring` and writes whole chunks. Deliberately
deferred, each an open item in [`TODO.md`](../TODO.md): rotating the
checkpoint key by hand, parallel sealing inside one part, a streaming decrypt
that never lands ciphertext on disk, and moving the rest of the content
store's hashing to `ring`. Parallel sealing and the streaming decrypt later shipped with
[chunked-checkpoint-publish-plan.md](chunked-checkpoint-publish-plan.md),
which also replaced this plan's one-object-per-part layout (§6.2) with
chunks and a sealed index.

Every memory checkpoint Legion publishes is the guest's whole RAM, so it holds
every secret and token the workload had loaded. Today a publish uploads it to
the worker's `contentStore` as a plain object. This plan seals each published
part with a key that lives in the manager's keyring, so the bucket holds
nothing readable without the cluster's root key.

---

## 1. Problem statement

Three paths upload checkpoint parts to a remote store:

- `suspend.durability = "store"` publishes a suspend checkpoint once it is
  captured.
- `legion snapshot create --publish` publishes a manual snapshot.
- `legion instance fork --spread` publishes the fork checkpoint so the forks
  can land on other nodes.

Each one runs [`ContentPublishing::publish`](../../src/content_publish.rs),
which copies the part from the node content store to
`<contentStore.url>/sha256/<hex>` byte for byte. The public doc says so
plainly: "A published snapshot is in your bucket, unencrypted"
(`docs/suspend-and-resume.md`, Security). The mitigation it offers is bucket
policy plus the provider's server-side encryption. Both protect against a
stolen disk at the provider. Neither protects against anyone who can read the
bucket, which in practice is every worker credential, every CI job that
shares the bucket, and every operator with a broad IAM role.

Call 36 of the suspend plan
([calls doc](../calls/suspend-resume-and-oversubscription-plan_calls.md))
deferred this on purpose: envelope encryption of multi-GiB blobs needs a
streaming cipher on both sides, a key id in the manifest, and a rotation story
for objects that outlive keys. This plan is that follow-up.

## 2. Prior art

- **Firecracker and cloud-hypervisor** write snapshot memory files in the
  clear and say protecting them is the user's job
  ([Firecracker snapshot security](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md#security-and-uniqueness)).
  CRIU images are also plaintext. Legion inherits that from its runtimes and
  has to add the layer itself.
- **Kubernetes forensic checkpointing** writes a plaintext tar on the node
  ([kubelet checkpoint API](https://kubernetes.io/docs/reference/node/kubelet-checkpoint-api/))
  and leaves shipping it anywhere to the user. Nomad has no checkpointing.
- **Tahoe-LAFS convergent encryption** derives each file's key from its
  content plus a secret "convergence secret"
  ([docs](https://tahoe-lafs.readthedocs.io/en/latest/convergence-secret.html)).
  Identical files dedupe, and the secret stops an outsider from confirming a
  guessed file. This is the model we take.
- **Borg** names chunks with a keyed hash (`HMAC(id_key, chunk)`), so the
  repository dedupes without revealing plaintext hashes
  ([borg security internals](https://borgbackup.readthedocs.io/en/stable/internals/security.html)).
  We take the keyed object name.
- **restic** names blobs by plaintext SHA-256 and hides the names inside
  encrypted pack files and indexes
  ([restic design](https://restic.readthedocs.io/en/stable/100_references.html#design)).
  We reject the pack layer. It buys nothing for objects that are already
  whole parts.
- **age** seals payloads with the STREAM construction: fixed-size chunks,
  each an AEAD with a counter nonce and a last-chunk flag
  ([age v1 spec](https://age-encryption.org/v1),
  [STREAM paper](https://eprint.iacr.org/2015/189)). Chunks can be sealed and
  opened one at a time, and truncation and reordering fail authentication.
  We use the same framing with AES-256-GCM.
- **Legion's own KV keyring** ([kv-encryption plan](kv-encryption-plan.md))
  already has a root key held outside the store, wrapped keys in a replicated
  table, three root-key providers, root-key rotation by rewrap, and join-time
  delivery to new managers. We reuse all of it.

## 3. Jobs to be done, by persona

**Cluster operator**
- When I point workers at a shared bucket, I want what lands there to be
  unreadable on its own, so a leaked bucket credential is not a leaked set of
  customer secrets.
- When I rotate the cluster's root key, I want my published checkpoints to
  keep working without re-uploading terabytes.

**Security / platform admin**
- When someone can read the bucket but holds no Legion credential, I want
  them to learn nothing about what any sandbox had in memory, not even whether
  two sandboxes had the same file.
- When a resume fails because a key is missing, I want the error to say which
  key and why, so I can tell a lost root key from tampering.

**Job author**
- When I set `durability = "store"`, I want nothing else to configure, so
  durable suspends stay a one-line choice.

**On-call responder**
- When a resume fails with a decryption error, I want it to say whether the
  object was tampered with, was published before encryption existed, or needs
  a key the cluster no longer has, so I know whether to boot cold or escalate.

*Not in scope:* the autonomous agent persona gets no new surface. Agents see
the same structured errors on the existing snapshot and instance calls.

### User scenarios

1. An operator configures `contentStore = { url = "s3://acme-ckpt/usw2" }`.
   A sandbox suspends with `durability = "store"`. The bucket now holds
   `usw2/sealed/3f1c…` objects. Downloading one yields a 20-byte header and
   ciphertext. The instance resumes on another node with no extra step.
2. An operator rotates the root key with `legion keyring root-key rewrap`.
   Every published checkpoint still resumes. Nothing in the bucket changes.
3. A cluster upgraded to this build has a manifest published before it. Its
   resume on another node fails with `published before checkpoint encryption`,
   and `onRestoreFailure = "cold"` boots the instance fresh, as for any lost
   checkpoint.

## 4. Goals and non-goals

**Goals**

- Every part a worker publishes is sealed. There is no setting that turns
  sealing off.
- Workers never hold a cluster-wide key. Each worker gets one key per part,
  only for work the manager sent it.
- The node content store, the location index, and the `locality` score keep
  keying on the plaintext digest, unchanged.
- Publishing the same bytes twice still uploads nothing.
- Memory use stays bounded for any part size, and publishing and fetching
  never need twice the part's size in free disk.
- Every failure has a named error: tampered, truncated, wrong key, missing
  key, locked keyring, plaintext object.

**Non-goals**

- Encrypting checkpoint parts at rest on the node that captured them. They
  are `0444` files in the worker's own state directory, beside everything
  else the worker can read.
- Encrypting artifacts. Fetchers stay read-only and unchanged.
- Garbage collection of published parts (call 62 of the suspend plan).
- A plaintext opt-out. See call 1.
- Hiding part sizes. Object sizes stay visible to anyone who can list the
  bucket.

## 5. Background: what exists today

- **Publisher.** `src/content_publish.rs`. `ContentPublishing::publish`
  rehashes the local blob (`ContentStore::verify`), then a
  `ContentPublisher` writes it: `FileContentPublisher` copies and rehashes,
  `S3ContentPublisher` streams a 16 MiB multipart upload. The URL is
  `<url>/sha256/<hex>`, recorded per digest in the node content store index
  (`record_published`).
- **Publish op.** `src/worker/checkpoints.rs` `publish()` walks the
  manifest's memory parts and fills each part's `url`. Every publish is a
  manager `CheckpointRequest { op = "publish" }`, built in
  `build_checkpoint_requests` (`src/manager/server/snapshot_rpcs.rs`) for a
  snapshot in state `local`.
- **Fetch.** `fetch_published_parts` (`src/worker/checkpoints.rs`) runs on
  the staging thread for a restore and on a blocking thread for `warm`. It
  calls `ArtifactStager::fetch_into_store` (`src/artifacts/stage.rs`), which
  downloads through the `s3://` or `file://` fetcher, checks the bytes
  against the plaintext digest, and imports them into the node store.
- **Restore payload.** `snapshot_restore_payload` builds a `SnapshotRestore`
  for every allocation restoring from a snapshot: resumes, rollbacks, forks
  (including `--spread`), and `instance create --from-snapshot`. A worker
  holds it in memory only (`CheckpointState::restores`). It is not written to
  the worker's state file.
- **Keyring.** `src/storage/keyring.rs` and
  `src/storage/store/keyring_ops.rs`. The `keyring_key` table holds wrapped
  32-byte keys with a free-text `state` column (`active`, `retired`). Rows
  replicate, every manager unwraps them at load, and `legion keyring root-key
  rewrap` rewraps every row. Initialization and the daemon's keyring loop run
  `ensure_keyring_initialized` on the control-lane leader.

## 6. Proposed design

### 6.1 The key hierarchy

```
root key   ── outside the store (file, env, or KMS), as for KV
   │
   ▼ wraps
checkpoint key ck_…  ── a keyring_key row in state "checkpoint", replicated,
   │                    minted with the keyring, never rotated on its own
   ▼ HKDF-SHA256(info = "sha256:<hex>" of the part)
part key   ── one per plaintext digest, computed by the manager and sent
   │           only to the worker doing that part's publish, warm, or restore
   ├─▶ HKDF("aes-256-gcm-stream")  → the AEAD key for the object
   └─▶ HKDF("object-name")         → 32 bytes, hex: the object's name
```

The part key is a keyed convergent key: the same bytes get the same key and
the same object, so dedup survives, and nobody without the checkpoint key can
derive it from a guessed plaintext (call 2).

Data-key rotation never touches the checkpoint key. A root-key rewrap rewraps
it with the data keys. No published object is ever rewritten.

### 6.2 What the bucket holds

```
<contentStore.url>/sealed/<64 hex chars>
```

The name is derived from the part key, so it reveals nothing about the
plaintext digest. The object is:

```
header   "LGNSEAL1" | chunk size (u32 BE) | plaintext length (u64 BE)   20 bytes
chunk 0  AES-256-GCM(part AEAD key, nonce_0, aad = header, plaintext[0 .. 1 MiB]) ‖ tag
chunk 1  …
chunk n  the last chunk, flagged in its nonce (an empty part is one empty chunk)
nonce_i  = 0x000000 | i (u64 BE) | 0x01 if last else 0x00
```

The header is the AAD of every chunk, so changing the length or chunk size
fails every chunk. The last-chunk flag catches truncation at a chunk
boundary. Counter nonces are safe because each key seals exactly one
plaintext (the one its digest names).

### 6.3 How a worker gets a key

The manager attaches part keys to the work it already sends:

| Work | Where the keys ride | Authorized by |
| --- | --- | --- |
| publish | `CheckpointRequest.part_keys` (`op = "publish"`) | the suspend or `snapshot create --publish` that made the snapshot (`snapshot:write`) |
| warm | `CheckpointRequest.part_keys` (`op = "warm"`) | `WarmSnapshot` (`snapshot:read`) |
| restore | `SnapshotRestore.part_keys` | resume, `snapshot restore`, fork, or `instance create --from-snapshot` (each checks its ACL before it places the allocation) |

There is no new RPC. A worker cannot ask for a key. It receives keys only for
the parts of a manifest the manager holds, on work the manager chose to send
it. When the manager cannot attach keys (no checkpoint key yet, a locked
keyring, a manifest naming a key the cluster no longer has), it sets
`part_key_error` instead, and the worker fails the publish, warm, or fetch
with that message. A restore whose parts are all already on the node needs
no key and ignores the error.

Keys stay in memory on the worker. The proto type redacts them from `Debug`
and skips them in serde, so neither a log line nor a JSON dump of a request
carries one.

### 6.4 Publish

`ContentPublishing::publish(digest, part_key)` streams the local blob through
the sealer straight into the destination: a `.tmp` file renamed into place,
or an S3 multipart upload. The sealer hashes the plaintext as it reads, so the
old separate rehash pass goes away and corrupt bytes still never publish. A
mismatch aborts the upload and quarantines the local blob. The worker records
the part's `keyId` in the manifest beside its `url`.

### 6.5 Fetch

`ArtifactStager::fetch_sealed_into_store` downloads the object through the
same fetcher registry, schemes, credentials, limits, retries, and
single-flight as before, then opens it in place: each chunk is decrypted and
written back over the ciphertext behind the read cursor, and the file is
truncated to the plaintext length. The import then hashes the plaintext and
refuses anything that does not match the manifest's digest. Peak extra disk is
zero beyond the ciphertext itself.

### 6.6 Failure modes

| Situation | Error the user sees |
| --- | --- |
| Object modified, or sealed under another key | `sealed checkpoint part failed authentication at chunk N: it was modified, or sealed under a different key` |
| Object cut short or extended | `sealed checkpoint part is truncated or has trailing bytes` |
| Object has no `LGNSEAL1` header | `the object is not a sealed checkpoint part (no LGNSEAL1 header) …` |
| Manifest part has a `url` and no `keyId` | `snapshot … part … was published before checkpoint encryption, and Legion no longer reads unsealed parts …` |
| Keyring has no checkpoint key | `this cluster has no checkpoint key yet …` (with `encryption.enabled = false` named when that is why) |
| Checkpoint key locked on this manager | the keyring's own `keyring locked: …` message, naming the root key |
| Checkpoint key dropped by `keyring reset` | `checkpoint key ck_… is not in this cluster's keyring …` |
| Plaintext does not match the digest after decrypt | the content store's existing digest mismatch error |

Every one fails the publish (snapshot stays `local`, retried as today), the
warm (`snapshot.warm_failed`), or the restore (`onRestoreFailure` applies).

### 6.7 Rotation and loss

- **Root key rotation** rewraps the checkpoint key row. Objects are
  untouched.
- **Data key rotation** does not involve the checkpoint key.
- **Checkpoint key rotation** is deferred. Manifests already carry `keyId`
  per part, so a later `legion keyring checkpoint-key rotate` only has to
  mint a new current key and keep old ones while a manifest names them.
- **Root key loss** makes the checkpoint key, and so every published
  checkpoint, unreadable. That is the same contract as KV values.
  `legion keyring reset` drops the checkpoint key with the data keys and
  mints a new one. Old published snapshots then fail with the named
  `ck_… is not in this cluster's keyring` error.
- **Backups** carry the wrapped checkpoint key like any keyring row.

### 6.8 Performance

AES-256-GCM through `ring` runs about 2.6 GB/s on one Apple M-series core.
SHA-256 runs about 1.4 GB/s with the CPU's SHA instructions, and the
publisher already paid two SHA passes on the `file://` path (verify, then
rehash the copy) and one on the S3 path. The new path pays one SHA pass and
one AES pass on both. Publishing is already
asynchronous to the suspend, so none of it lands on the workload's pause.
Fetching adds one AES pass to the download. Measured numbers are in §9.

## 7. Proto, struct, config, and persistence changes

- `message CheckpointPartKey` with `digest`, `key_id`, and `key`,
  `skip_debug` with a redacting `Debug`, and `#[serde(skip)]` on `key`.
- `CheckpointRequest.part_keys` (8), `CheckpointRequest.part_key_error` (9),
  `SnapshotRestore.part_keys` (7), `SnapshotRestore.part_key_error` (8).
- `MemoryPart.keyId` in the manifest JSON. Set exactly when `url` is.
- `keyring_key` rows with `state = "checkpoint"`, `algorithm =
  "hkdf-sha256"`, ids `ck_…`. No schema migration: both columns are text.
- No config or jobspec change. `contentStore` stays as it is.

## 8. Implementation phases

One phase. Sealing without the key delivery, or delivery without sealing, is
worth nothing on its own, and the change is a few hundred lines.

1. Sealed object format and key derivation (`src/checkpoint_seal.rs`).
2. Checkpoint key in the keyring, minted with initialization and by the
   daemon loop on clusters that already have a keyring.
3. Proto fields, manager-side key attachment, worker-side publish and fetch.
4. Docs, TODO, calls.

## 9. Testing plan

- **Unit (`checkpoint_seal`)**: round trip across chunk boundaries (empty,
  one byte, one chunk exactly, one chunk plus one, several chunks), tamper
  detection in header, body, and tag, truncation at and inside a chunk
  boundary, wrong key, plaintext object, a streamed 64 MiB part with bounded
  buffers, determinism (same key and bytes, same object).
- **Unit (keyring)**: the checkpoint key is minted with the keyring, survives
  data-key rotation and root-key rewrap, and derives the same part key on a
  replica. A locked or missing key gives the named errors.
- **Worker (`task_manager` tests)**: publish writes a sealed object under
  `sealed/`, never the plaintext. A node that never held the part restores
  from it, and warm fetches it. A missing key, a wrong key, and a manifest without
  `keyId` each fail by name.
- **Integration (`tests/published_resume_integration.rs`)**: publish and warm
  requests and the restore payload carry a key for every part, the publish
  key and the restore key agree, and a part published before encryption gets
  no key.
- **Throughput**: an ignored test publishes a 2 GiB synthetic part through
  the `file://` publisher, run before and after the change.

Measured on an Apple M-series laptop (macOS, APFS, release build, one core,
median of three runs, 2 GiB part, the whole `ContentPublishing::publish`):

| Path | Before (plaintext) | After (sealed) |
| --- | --- | --- |
| publish (`file://`) | 8.4 to 8.5 s, about 240 MiB/s | 1.35 s, about 1.5 GiB/s |

The old path hashed the part twice with the `sha2` crate. The new one hashes
once with `ring` and adds one AES pass. Calls entry 6 has the breakdown.

## 10. Rollout and backward compatibility

Legion has no installed base, so there is no shim. A cluster that upgrades
mints its checkpoint key on the next keyring loop tick. Manifests published
before the upgrade cannot be fetched by another node and fail with the named
error. On the node that captured them the parts are usually still in the
content store, and a resume there needs no key.

## 11. Open questions

None blocking. Checkpoint key rotation by hand is the next step if an
operator asks for it.

## 12. Acceptance criteria

- No published object under `sealed/` contains a plaintext chunk of its part.
- A published checkpoint resumes on a node that never held it, and after a
  root-key rewrap.
- Each failure in §6.6 produces its named error in a test.
- `docs/suspend-and-resume.md` no longer says published parts are
  unencrypted, and documents the model, failure modes, and limits.
