# Judgement Calls: Checkpoint Encryption Plan

Decisions made while planning and building
[`plans/checkpoint-encryption-plan.md`](../plans/checkpoint-encryption-plan.md).
**Entries are ranked by project impact, highest first. The number is the
rank.** Insert new entries at their rank and fix any citation by number.

## 1. Every published part is sealed, with no plaintext opt-out

**Call.** A worker seals every part it publishes. There is no
`contentStore.encrypt = false`, and no fallback to plaintext when the keyring
is unavailable. A publish that cannot get a key fails, and the checkpoint
stays `local`:

```text
publish attempt 1 of 3 failed, retrying in 60s: publishing part memory of snapshot
snap-5b10e2c7a9d04f31 needs its key: the manager sent no key for checkpoint part
sha256:9c1f…: this cluster has no checkpoint key, because the keyring is off
(encryption.enabled = false on the manager); published checkpoints are always
sealed, so publishing needs the keyring (docs/kv-encryption.md)
```

**Alternatives.** (a) A `contentStore.encryption = "none"` switch for
operators who trust their bucket and want to skip the AES pass. (b) Fall back
to plaintext when `encryption.enabled = false`, so turning off KV encryption
keeps durable suspends working.

**Trade-off.** A cluster that turned the keyring off cannot publish
checkpoints, so `durability = "store"` stops working there. The AES pass is
paid on every publish and fetch. Operators who already rely on bucket policy
and server-side encryption get a second layer they did not ask for.

**Why (simplicity, and the cost is small).** A memory checkpoint is every
secret the guest held. A switch that turns that into plaintext in a shared
bucket is a foot-gun that exists only to save about 1.6s of one core per
4 GiB, off the workload's critical path (§6.8 of the plan, and entry 6). The
only real reason for an opt-out would be a cluster without a keyring, and
`encryption.enabled = false` is itself a non-default choice an operator makes
on purpose. Saying "publishing needs the keyring" is one sentence. A
plaintext mode is a second code path, a second object layout, and a
permanent question in every security review.

## 2. Keyed convergent encryption: the part key derives from the plaintext digest

**Call.** Each part's key is `HKDF(checkpoint key, info = "sha256:<hex>")`.
The same bytes always produce the same key, the same object name, and the
same ciphertext. So publishing stays idempotent (a `HEAD` hit uploads
nothing), and identical parts across snapshots are stored once.

**Alternatives.** (a) A random data key per part or per snapshot, wrapped and
stored in the manifest, as envelope encryption usually works. (b) Plain
convergent encryption, keyed by the digest alone with no secret, as early
Tahoe-LAFS did.

**Trade-off.** Anyone who can list the bucket can see that two snapshots
share an object, which leaks that they had an identical part. And a worker
that holds a part's plaintext can derive nothing more than that part's key,
so a compromised worker that forges a capture report naming a digest it
learned elsewhere can get that digest's key. Digests travel only to workers
doing work on that snapshot, so this needs a second leak to matter.

**Why (performance and simplicity).** Random keys break dedup in the bucket
and turn every re-publish of an unchanged part (a suspend, a resume, a
suspend again) into a full upload. They also break the one-URL-per-digest
record in the node content store, which the publish fast path and the
location index rely on. Without the checkpoint key in the derivation, anyone
holding a candidate plaintext could confirm whether it is in the bucket
(Tahoe's "confirmation of a file" attack). Mixing in the checkpoint key closes
that, as Tahoe's convergence secret does. Equality is the residual leak, and
it is the price of dedup in every system that dedupes encrypted data.

## 3. The plaintext digest stays the part's identity, and the object name is derived from the key

**Call.** The manifest, the node content store, the location index, and the
`locality` score keep keying on the plaintext `sha256:<hex>`. The object in
the bucket lives at `sealed/<hex of HKDF(part key, "object-name")>`.
Integrity is AEAD per chunk plus the plaintext digest check the content store
import already runs.

**Alternatives.** (a) Name the object by its ciphertext digest and record
that in the manifest, so the existing digest-verifying fetch path works
unchanged. (b) Keep naming it by the plaintext digest (`sha256/<hex>`).

**Trade-off.** The fetch path needs a sealed variant
(`fetch_sealed_into_store`) instead of reusing the checksum check, and an
operator browsing the bucket cannot match an object to a manifest by eye.

**Why (simplicity).** A ciphertext digest would be a second identity for
every part, threaded through the manifest, the publish record, and the
location index, which all key on the plaintext digest today. Naming by the
plaintext digest would publish exactly the value the keyed derivation (entry
2) exists to hide. A derived name keeps one identity and reveals nothing.

## 4. The checkpoint key is its own keyring row, never rotated on its own

**Call.** The cluster has one checkpoint key (`ck_…`), a `keyring_key` row in
state `checkpoint`, wrapped by the root key, minted by the keyring leader at
initialization (or on the next keyring loop tick for a keyring that predates
it). Data-key rotation and collection skip it. A root-key rewrap rewraps it.

**Alternatives.** (a) Use the active KV data key directly, as the suspend
plan's open question 4 assumed. (b) Derive the checkpoint key from the root
key with HKDF. (c) Per-job or per-namespace checkpoint keys.

**Trade-off.** One key covers every published checkpoint, so its compromise
exposes all of them, and it does not age out. Rotating it by hand is deferred
(entry 8).

**Why (simplicity and the rotation story).** Data keys rotate every 30 days
and are deleted once no KV value references them. A published object can
outlive that by months, so (a) needs either a rewrap of multi-GiB objects on
every rotation or a second retention rule for data keys. That rotation story
is why call 36 of the suspend plan deferred the whole feature. (b) breaks
with the `exec` provider, whose root key never leaves the KMS, and changes on
every root-key rotation. (c) needs a job or namespace in every derivation and
a key lifecycle per job, and namespaces are not on this branch. Per-job keys
buy little here: a worker only ever receives part keys for work on that job's
snapshots, and a part key opens only the bytes its digest names. A
dedicated row reuses wrapping, replication, join delivery, backups, and
root-key rewrap with no schema change (the `state` column is text).

## 5. Keys ride the work the manager already sends, with no new RPC

**Call.** The manager attaches part keys to `CheckpointRequest` (`publish`,
`warm`) and `SnapshotRestore`, and puts the reason in `part_key_error` when
it cannot. A worker never asks for a key.

**Alternatives.** A `GetCheckpointPartKey` RPC that workers call with their
node identity, checked against the snapshot's job ACL.

**Trade-off.** A key is derived on every heartbeat that carries the work, not
once (an HKDF call, microseconds). Keys cross the heartbeat channel, which is
plaintext when manager TLS is off, as KV-rendered task config already does.

**Why (simplicity and least privilege).** The work itself is the
authorization. A publish goes only to the node that captured the snapshot, a
warm only to nodes `WarmSnapshot` chose under `snapshot:read`, and a restore
only to the node placed by an RPC that already checked its ACL. A pull RPC
would need its own ACL vocabulary for worker-plane callers, a test-table
entry, and a rule for which worker may ask for which digest, and it would
still end up checking "is this worker doing work on this snapshot", which the
push already guarantees.

## 6. Publishing hashes while it seals, with `ring`, replacing the separate verify pass

**Call.** `ContentPublishing::publish` no longer rehashes the local blob
before uploading. The sealer hashes the plaintext as it reads it, with
`ring`'s SHA-256, and a mismatch aborts the upload (or removes the temp file)
and quarantines the blob. Sealed chunks are written whole (1 MiB), not
through `io::copy`'s 8 KiB buffer.

**Alternatives.** Keep `ContentStore::verify` before sealing, as the
plaintext publisher did, and keep hashing with the `sha2` crate like the rest
of the content store.

**Trade-off.** A corrupt blob is detected at the end of the upload instead of
before it starts, so a corrupt multi-GiB part costs an aborted upload. And
the sealer hashes with a different implementation than the content store,
so the two have to agree on the `sha256:<hex>` form (they do, and the round
trip tests check it).

**Why (performance).** The AES pass is new work. Dropping a redundant SHA
pass pays for it, and on this machine the `sha2` crate runs well below the
CPU's SHA instructions, which `ring` uses. Measured with the ignored
`publish_throughput` test (release build, Apple M-series laptop, macOS, APFS,
a 2 GiB synthetic part, the whole `ContentPublishing::publish` through the
`file://` publisher, median of 3 runs, two rounds run back to back on an idle
machine):

| | Before (plaintext: verify, copy, rehash) | After (seal and hash in one pass) |
| --- | --- | --- |
| 2 GiB publish | 8.4 to 8.5 s (about 240 MiB/s) | 1.35 s (about 1.5 GiB/s) |
| Same, after only, hashing with `sha2` | | 4.6 s (about 445 MiB/s) |
| Same, after only, `sha2` and 8 KiB `io::copy` writes | | 5.4 s (about 376 MiB/s) |

The S3 path had one SHA pass before and has one SHA pass plus one AES pass
now, both far faster than any upload. Corruption of a `0444` blob in the
node's own store is rare, and it is still never published. Moving the rest
of the content store (imports, `verify`) to `ring` would speed up every
fetch and artifact stage too, and is a TODO item rather than part of this
change.

## 7. Fetch opens the object in place instead of streaming through the fetcher

> **Superseded** by
> [chunked-checkpoint-publish calls 7](chunked-checkpoint-publish-plan_calls.md#7-chunks-transfer-16-at-a-time-on-a-private-four-thread-runtime-per-part):
> a part is now fetched as chunks, each opened in memory, and
> `open_file_in_place` is gone. The entry stays as the record of the
> original call.

**Call.** The fetcher downloads the ciphertext to its scratch file as it
always did, then `open_file_in_place` decrypts each chunk and writes it back
behind the read position and truncates. The content store import then hashes
the plaintext.

**Alternatives.** Decrypt inside the fetcher's sink as bytes arrive, so the
ciphertext never lands on disk.

**Trade-off.** One extra read and write pass over the part on the fetching
node's disk.

**Why (simplicity and extensibility).** The fetchers are a plugin surface
that writes to a generic sink. Teaching every fetcher about sealed streams,
or wrapping the sink, would touch the artifact system for one caller. In
place needs no disk beyond the object itself, so the disk-room checks for
checkpoints stay correct. A streaming decrypt is a TODO item if fetch time on
large parts shows it matters.

## 8. Manual checkpoint key rotation is deferred, with `keyId` in the manifest now

**Call.** There is no `legion keyring checkpoint-key rotate`. Every published
part records the `keyId` it is sealed under, and the manager derives fetch
keys from that id, so a later rotation only has to mint a new current key and
keep old ones while a manifest names them.

**Alternatives.** Ship rotation now, with a reference count of manifests per
checkpoint key and collection when it reaches zero.

**Trade-off.** An operator who suspects the checkpoint key leaked can only
reset the keyring, which loses every published checkpoint and every KV value
under the same root key.

**Why (simplicity).** A root-key rewrap covers routine rotation, since the
checkpoint key never leaves the managers unwrapped. The per-manifest
reference count is the same problem as garbage-collecting published parts
(call 62 of the suspend plan), and both belong together.

## 9. Sealing errors that cannot change between attempts are not retried

**Call.** A fetch that fails authentication, finds no `LGNSEAL1` header, or
finds an unsupported header stops after one attempt. A truncated object is
still retried, like any download error.

**Alternatives.** Retry every failure up to `artifacts.fetch.attempts`, as
artifact staging does.

**Trade-off.** A transient corruption on the wire that happens to pass the
transport's own checks fails the fetch at once instead of on the third try.

**Why (performance).** A wrong key or a tampered object fails identically
every time, and each retry downloads the whole multi-GiB part again before
learning that.

## 10. An empty part is one empty, last chunk, and chunks are 1 MiB

**Call.** The STREAM framing always has at least one chunk, so an empty part
seals to a header and one tag, and every part's end is authenticated. Chunks
are 1 MiB of plaintext.

**Alternatives.** 64 KiB chunks, as age uses, or a variable chunk size
recorded in the header and chosen per part.

**Trade-off.** 1 MiB of buffer per concurrent seal or open, and a flipped bit
fails the whole chunk instead of a smaller span (which makes no practical
difference: any failure fails the part).

**Why (performance).** Overhead is 16 bytes per MiB, and per-call AEAD setup
is amortized well at 1 MiB. The header records the chunk size, so a reader
accepts any size up to 64 MiB and a future change needs no new magic.
