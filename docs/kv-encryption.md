# KV Encryption at Rest

Legion encrypts every value in the [KV store](kv-store.md) before it reaches
disk. That covers the state database, every backup, the streamed replica, the
raft log, and every snapshot. The keys live outside the database. Encryption
is **on by default and needs no configuration**. A manager mints its own root
key on first start, a manager joining a raft cluster receives the cluster's
root key when it joins, and data keys rotate on their own.

For most clusters, this is all you need to know:

1. Do nothing. Encryption is on.
2. Keep the file `<storePath>/keys/root.key` with your backups. A backup
   cannot be restored without it.
3. If anything needs attention, `legion keyring status` tells you what and
   how to fix it.

The rest of this page covers the details.

## What it protects, and what it does not

**Protected.** Without the root key, none of these yield a KV value: a copy
of `store.sqlite`, a `legion state backup` file, the object-storage replica
of a `streaming` manager, a raft snapshot, the raft log, or a snapshot image
in flight to a joining manager. Raw state access (`legion state query`, the
State Explorer, saved queries, or anything else `state:read` grants) sees
ciphertext in the `value` column. A KV value can only be read through the KV
API, where [`kv` path policies](auth-and-acls.md) apply. Without encryption,
`state:read` could read every KV value regardless of `kv` path scoping.

**Not protected:**

- **The manager host itself.** With the default `file` provider, root on a
  manager has both the database and the key. This matches the defaults in
  Docker Swarm and Nomad. To go further, keep the root key in a KMS (the
  `exec` provider, below).
- **Rendered copies.** A value a task consumes is rendered into the job's
  stored task config (`{{ kv "…" }}` templates and `$key` references),
  shipped to the worker, and written to the worker's state file and task
  directory. Encrypting the source of truth does not encrypt those copies.
  That is a separate, planned change.
- **History written before encryption was on.** Raft log entries and
  streamed WAL segments from before the upgrade stay plaintext until log
  compaction (a snapshot) and bucket retention remove them. A rewrap
  rewrites rows, not history.
- **The `GetArtifactCredential` path** still has no KV path ACL of its own
  (see [Artifacts](artifacts.md)). Encryption does not change this.
- **FIPS.** AES-256-GCM is a FIPS-approved algorithm, but the implementation
  (`ring`) is not a validated module.

## How the keys work

These three names appear in config, the CLI, and error messages:

```
root key   ── lives OUTSIDE the store: a file on each manager (default),
   │           an environment variable, or a KMS reached through a command.
   │           Never written to the database, a changeset, a snapshot, or
   │           a backup.
   ▼ wraps
data keys  ── live INSIDE the store, wrapped, in the replicated keyring_key
   │           table. Exactly one is `active`; older ones are `retired`
   │           until every value has been rewrapped, then removed.
   ▼ encrypt
KV values  ── kv_entry.value = base64(nonce ‖ ciphertext ‖ tag) under
               AES-256-GCM, kv_entry.enc_key_id = which data key.
```

- A **root key** is 32 random bytes. Its id is a fingerprint of the bytes
  (`rk_` + 16 hex characters), so two managers holding the same key agree on
  its name with no coordination.
- A **data key** (`dk_…`) is a 256-bit AES key. Each value is encrypted with
  AES-256-GCM under the active data key, with a random 96-bit nonce and the
  KV key path as authenticated data. So a ciphertext moved from
  `prod/db/password` to `dev/scratch` fails authentication instead of
  decrypting.
- The **keyring** is the data keys plus the provider that unwraps them.
  The wrapped data keys are ordinary replicated rows, so backups carry
  them, and a rotation is one replicated write that every manager sees.

A row whose `enc_key_id` is `NULL` is a plaintext row. It was written before
encryption existed, or with `encryption.enabled = false`. It stays readable,
and a rewrap converts it.

## Configuration

We recommend leaving the block out entirely. It only exists for two choices:
where the root key lives, and whether encryption is on at all.

```nickel
{
  encryption = {
    enabled = true,                              # default
    rootKey = {
      provider = "file",                         # "file" | "env" | "exec"
      # path = "/var/lib/legion/keys/root.key",  # file: default <storePath>/keys/root.key
      # var = "LEGION_ROOT_KEY",                 # env: variable holding the key line(s)
      # command = ["legion-kms-aws", "--key-id", "arn:aws:kms:…:key/…"],  # exec
      # timeout = "10s",                         # exec: per-call limit
    },
    dataKeyRotationAge = "720h",                 # default: 30 days
  },
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Off: new writes are plaintext rows. Existing ciphertext still decrypts while the root key is present. |
| `rootKey.provider` | `file` | Where the root key lives (below). |
| `rootKey.path` | `<storePath>/keys/root.key` | `file` only. |
| `rootKey.var` | `LEGION_ROOT_KEY` | `env` only. Same text as the file. Commas may separate keys. |
| `rootKey.command` | none | `exec` only, required. The KMS helper to run. |
| `rootKey.timeout` | `10s` | `exec` only. |
| `dataKeyRotationAge` | `720h` | Age past which the active data key is rotated automatically. Minimum `1m`. |

A malformed block stops the manager at startup with the field named.

### Root key providers

**`file` (default).** One base64 key per line. `#` comments and blank lines
are ignored. **The first line wraps new data keys, and every line can
unwrap.** The manager creates the file on first start (`0600`, in a `0700`
directory) with an explanatory header. To rotate the root key, put a new
line first, run `legion keyring root-key rewrap`, then remove the old line.
The file is re-read on every rewrap and whenever the manager meets an
unknown root key, so adding a line needs no restart.

**`env`.** The same text in one environment variable. Use this when you
mount secrets as environment variables (a Kubernetes Secret, a systemd
credential) and want nothing on disk. Legion never creates this key for you.
Generate one with `legion keyring root-key generate` and set it before the
first start.

**`exec`.** A command that wraps and unwraps data keys through a KMS or an
HSM. The root key never leaves the KMS, and Legion stores only the wrapped
data keys. The manager runs the command with one JSON document on stdin and
reads one from stdout:

```json
{"op":"describe"}                                                          → {"key_id":"<root key id>"}
{"op":"wrap","key_id":"dk_…","plaintext":"<base64>"}                       → {"ciphertext":"<base64>","key_id":"<root key id>"}
{"op":"unwrap","key_id":"dk_…","ciphertext":"<base64>","root_key_id":"…"}  → {"plaintext":"<base64>"}
```

A non-zero exit, a timeout, or anything but a JSON object on stdout is a
provider error. The command runs at start, on keyring changes, and on
rotation. It **never runs on a KV read or write**, so a slow or unavailable
KMS can delay a rotation but cannot slow down or fail KV traffic. Reference
helpers for AWS KMS and HashiCorp Vault Transit live in
[`plugins/keyring/`](../plugins/keyring/README.md). Test any helper with the
conformance harness before pointing a manager at it:

```console
$ legion keyring provider test -- legion-kms-aws --key-id arn:aws:kms:us-east-1:123456789012:key/…
Testing root key provider: legion-kms-aws --key-id arn:…
  [ok] describe                     root key id arn:aws:kms:…
  [ok] wrap                         60 bytes under arn:aws:kms:…
  [ok] unwrap round trip            plaintext matches
  [ok] tampered ciphertext refused  unwrap failed as it must
  [ok] wrap is randomized           two wraps of one key differ

Provider is usable. Configure it with:
  encryption = { rootKey = { provider = "exec", command = ["legion-kms-aws", "--key-id", "arn:…"] } }
```

## Multi-manager (raft) clusters

Every manager needs the root key, because followers serve KV reads from
their own replica and decrypt locally.

- **`file`:** a manager joining with `legion raft join-token` **receives the
  root key from the leader as part of joining** and writes it to its own key
  file. Joining the cluster stays a single command. The key crosses the wire
  encrypted under a key derived from the cluster secret (the same secret the
  join token carries). If the leader has not minted its key yet (for
  example, when every manager of a fresh cluster starts at once), it mints
  one before answering the join, so the joiner always gets a key. A manager
  that already has a key file keeps it. You can also place the file by hand
  before starting the joiner.
- **`file`, existing members:** a manager that was already in the cluster
  when encryption arrived never joins again, so no join reply reaches it.
  Its keyring loop notices it has no key file and **fetches the root key
  from another member** (the leader first) over the same authenticated
  manager channel, sealed the same way. This runs within seconds of the
  first data key replicating, and every 15 seconds while the key is still
  missing. A member only hands over a key it already holds, never mints one
  for this, and only to a current member of the cluster. A manager that
  lost its key file recovers the same way.
- **`env` / `exec`:** set the variable, or install the helper and its
  credentials, on every manager. Nothing is delivered on join.

**Upgrading a cluster to a build with this feature.** The leader does not
create the keyring, and so never writes an encrypted row, until **every
voter** runs a build that can apply the keyring's schema. Until then,
`legion keyring status` reports `pending upgrade` and names the managers
still to upgrade, and KV values are stored as before. A mixed-version
cluster can run like this for weeks. It is unencrypted, but it still works.
The standard rolling manager upgrade ([Upgrades](upgrades.md)) turns
encryption on by itself, with no extra command. Once the last manager is
upgraded, the leader mints the root key and the first data key, and every
other manager fetches the root key from it (above). Check that every
manager reports `unlocked` in `legion keyring status` before you rely on
losing the original leader's disk: until the others have fetched it, the
root key exists only there. With `env` or `exec`, put the key on every
manager before the upgrade.

## The keyring commands

Most clusters never need these. Each accepts `-j` for the standard JSON
envelope, and the mutating ones accept `--idempotency-key`.

```console
$ legion keyring status
Encryption:   enabled (aes-256-gcm)
Root key:     file  rk_9f31c2a7e0d4b6a1  /var/lib/legion/.legion/store.sqlite/keys/root.key
State:        unlocked (every data key is available here)
Manager:      http://10.0.0.5:4747
Data keys:
  dk_1c2f8e3b7a9d4e60  active   created 2026-09-21T10:02:11Z  age 3d  values 412
Values:       412 encrypted, 0 plaintext
Rewrap:       nothing pending
Rotation:     every 30days (active key age 3d)
```

- **`legion keyring status`**: the state of this manager's keyring, plus a
  `Next step:` line whenever something needs doing. The root key is
  per-manager state, so check each manager when diagnosing a cluster.
  Requires `operator:read`.
- **`legion keyring rotate`**: create a new active data key now. The old key
  is retired but kept. The manager re-encrypts every value under the new key
  in the background and removes the retired key once nothing references it.
  Rotation happens automatically every `dataKeyRotationAge`. Run this
  command after offboarding someone or a suspected exposure. Requires
  `operator:write`.
- **`legion keyring rewrap`**: re-encrypt every value under the active key
  right now, including plaintext rows, and wait for it to finish.
  `--plaintext` does the reverse and turns every value into a plaintext row.
  Run it before setting `enabled = false` or downgrading. Values do not
  change (no version bump, no job restarts). Requires `operator:write`.
- **`legion keyring root-key generate`**: print one new root key line. This
  runs locally and changes nothing.
- **`legion keyring root-key rewrap`**: re-wrap every data key under the
  provider's current root key (the first line, or the KMS key it reports).
  This is one replicated write and touches no values. Requires
  `operator:write`.
- **`legion keyring provider test -- <command…>`**: the exec-provider
  conformance harness. Runs locally.
- **`legion keyring reset --i-understand-encrypted-values-will-be-lost`**:
  the break-glass reset, described below. Requires `operator:write`.

The same surface is available over gRPC (`GetKeyringStatus`,
`RotateDataKey`, `RewrapKv`, `RewrapRootKey`, `ResetKeyring`, documented in
`proto/legion.proto`) and, for agents, as the `legion_keyring_status` and
`legion_keyring_rotate` MCP tools ([Agent Readiness](agent-readiness.md)).

### Rotating the root key (`file` / `env`)

```console
$ legion keyring root-key generate
h3Q7…base64…=              # 1. put this line FIRST in root.key on EVERY manager
$ legion keyring root-key rewrap    # 2. one replicated write; data keys now wrapped under it
1 data key(s) now wrapped under root key rk_4b8e…
Older root keys wrap nothing now; remove them from every manager.
                                    # 3. delete the old line everywhere
```

Order matters for the last step. If you delete the old line before the
rewrap, the keyring locks (the error names the missing key id). Putting the
line back unlocks it.

## What happens when

**A value is written.** It is encrypted before the row exists. The changeset
that replicates the write, and every snapshot and backup after it, carries
only ciphertext. A KV write fails when the keyring is locked instead of
silently storing plaintext.

**The root key is missing on a manager** (a lost file, a fresh host without
`LEGION_ROOT_KEY`, a KMS the helper cannot reach). The manager starts,
joins raft, schedules, and serves everything except the KV values it cannot
decrypt:

```console
$ legion kv get prod/db/password
error: keyring locked: data key dk_1c2f8e3b7a9d4e60 cannot be unwrapped — root key
rk_9f31c2a7e0d4b6a1 is not in /var/lib/legion/.legion/store.sqlite/keys/root.key
(keys present: rk_77aa…). Root key provider `file` at …/keys/root.key. Copy the root
key from a manager that has it (or set LEGION_ROOT_KEY), then check `legion keyring
status`  (FAILED_PRECONDITION, retryable: true)
```

`legion keyring status` shows `LOCKED` with the same next step,
`legion_keyring_unlocked` reads `0`, and a `keyring.locked` event is
emitted. As soon as the key is in place, reads succeed. The file is re-read
on the next read that meets an unknown key, so no restart is needed. A
locked keyring only affects KV. The manager keeps running, because losing
quorum over a missing file would be a worse incident.

**A backup is taken.** `legion state backup` reports what the file does not
contain:

```console
$ legion state backup
Backup written on the manager host (point-in-time image of this node's applied state):
  path:          /var/lib/legion/.legion/store.sqlite/backups/store-20260921T100211Z.sqlite
  …
KV values in this backup are encrypted. Root key rk_9f31c2a7e0d4b6a1 (file provider,
/var/lib/legion/.legion/store.sqlite/keys/root.key) is NOT included — keep it with the backup.
```

**A backup is restored** onto a host without the root key.
`legion start manager --restore-from <backup>` refuses to serve and names
the root key id(s) and the path to put the key at. Someone is always present
for a restore, so failing fast is better than a running cluster with
unreadable secrets. Copy the key into place and start again.

**The root key is gone for good.** The values encrypted under data keys it
wrapped are also gone. That is the point of encryption. To recover, reset
the keyring:

```console
$ legion keyring reset --i-understand-encrypted-values-will-be-lost
Keyring reset: 3 value(s) deleted, 1 data key(s) dropped, active data key dk_…
Put these values back with `legion kv put`:
  prod/db/password
  prod/api/token
  secrets/ghcr-pull
```

Jobs that referenced a deleted value fail through the normal
`missing_kv_dependency` path ([KV Store](kv-store.md#fail-semantics-and-recovery))
and recover when the value is put back.

**A data key rotates** (on schedule, by count guard, or `legion keyring
rotate`). The rotation is one replicated write. The background rewrap then
converts every value in batches of 256 rows, one replicated write per batch,
with a pause between batches. After that, the retired key is removed.
Versions and timestamps are untouched, so nothing on the reactive path fires
and no task restarts.

**Encryption is turned off.** `enabled = false` stops encrypting new writes
and keeps decrypting old ones while the root key is present. Run
`legion keyring rewrap --plaintext` first if the store must be fully
plaintext. You need this to downgrade to a build without the keyring, since
its migration runner refuses a newer database outright.

**A tampered or transplanted ciphertext** fails authentication and reads as
an internal error naming the key path. It never reads as a wrong value.

## Observability

Metrics ([Metrics](metrics.md)): `legion_keyring_unlocked`,
`legion_keyring_keys{state}`, `legion_keyring_active_key_age_seconds`,
`legion_kv_rows{encryption}`, `legion_kv_decrypt_failures_total{reason}`,
`legion_keyring_provider_calls_total{op,outcome}`. Alert on
`legion_keyring_unlocked == 0` while encryption is enabled.

Events ([Event Stream](event-stream.md)): `keyring.rotated`,
`keyring.rewrap_completed`, `keyring.root_key_rewrapped`, `keyring.locked`,
`keyring.unlocked`, `keyring.reset`. Events never include key bytes.
First-start initialization is logged, not emitted as an event.

Logs name key ids and the root key's location, never its contents.

## Known limitations

- The rendered copies of KV values in task configs, worker state files, and
  task directories are not encrypted (see above).
- The 2³¹-encryptions-per-key guard that backs time-based rotation is
  counted in memory per process, so a restart resets it. With the default
  30-day rotation, you will not reach the bound in practice.
- `legion kv list` fails entirely when any listed value is under a data key
  this manager cannot unwrap. `legion keyring status` names the key.
- An `exec` provider's `wrap`/`unwrap` runs during rotation and root-key
  rewrap on the manager handling the request, with its configured timeout.
  A provider that is down delays those operations and nothing else.
