# Plan: Chunked, Deduplicated Checkpoint Publishing

> **Judgement calls:** [`calls/chunked-checkpoint-publish-plan_calls.md`](../calls/chunked-checkpoint-publish-plan_calls.md)

Status: **IN EXECUTION**. Phase 1 (chunked publish and fetch, zero-chunk
elision, per-job dedup inside a dedup window) is built and tested on the
`file://` store. Public page:
[`docs/suspend-and-resume.md`](../../docs/suspend-and-resume.md) ("How
published parts are stored"). Deferred and named: Phase 2 (a restore fetches
only the chunks its node lacks), Phase 3 (Firecracker dirty-page tracking to
skip hashing clean memory), a real-S3 run (MinIO and AWS), and a real-KVM
memory run. Builds on
[checkpoint-encryption-plan.md](checkpoint-encryption-plan.md), whose
"parallel sealing" and "streaming decrypt" deferrals this plan closes.

Every published checkpoint today re-uploads the whole guest: all of its
memory and all of its root disk, zeros included, every time. This plan cuts
each part into fixed-size, content-addressed, sealed chunks, so a repeat
checkpoint uploads only the chunks that changed, zero chunks are never
uploaded, and every sandbox of one job shares the chunks of its image.

---

## 1. Problem statement

Stateful sandboxes checkpoint often, and with `durability = "store"` every
checkpoint is published: an idle suspend, every `suspend.checkpointEvery`
tick, a checkpoint at worker stop, `snapshot create --publish`, and a
`--spread` fork. A sandbox checkpoint has four parts
(`src/task_plugins/sandbox_checkpoint.rs:655`): guest memory (the full guest
RAM size), device state, the VMM config, and the guest's root disk (the
image plus `rootfs_headroom_mib` of free space, 256 MiB by default).

Today each part is published as one sealed object named by a key derived
from the whole part's digest (`src/content_publish.rs`, `publish_sealed`).
So:

- **A repeat checkpoint re-uploads everything.** One dirtied page changes
  the memory part's digest, and one log line changes the root disk's. A
  512 MiB guest on a 600 MiB root disk uploads about 1.1 GiB per
  checkpoint whatever changed. With `checkpointEvery = "10m"` that is
  6.6 GiB an hour per session, and the bucket keeps every copy until a
  lifecycle rule removes it.
- **Zeros are uploaded.** Memory the guest never touched and the root
  disk's free space are zero, and sealing turns them into incompressible
  ciphertext. A young guest's memory part is mostly zero.
- **Sandboxes of one image do not share bytes.** Each sandbox's root disk
  diverges from the image after one write, so its digest is unique, and
  N sessions of a job upload the image N times.
- **One part uploads on one stream.** A 16 MiB-part multipart upload runs
  four parts in flight, and a fetch is one GET, so a large part is bound by
  one connection's throughput and lands its ciphertext on disk before it is
  opened.

Where the time goes: a publish is off the suspend path (capacity is freed
when the checkpoint is local), but a `store` session cannot move nodes, and
a `--spread` fork cannot place, until its publish lands. Bytes are also
money: S3 storage and, across regions or providers, transfer.

## 2. Prior art

- **AWS Lambda container loading and SnapStart**
  ([Brooker et al., ATC '23](https://www.usenix.org/conference/atc23/presentation/brooker))
  cut images and Firecracker snapshots into 512 KiB chunks, name them by
  content, encrypt each with a key derived from its content (convergent
  encryption) and a salt that is rotated, and fetch only missing chunks
  into a tiered cache. The salt is what bounds both the blast radius of a
  chunk and how long a chunk can be reused. We take all of it except
  lazy loading, which is another agent's work, and use the dedup window as
  the rotating salt.
- **restic and Borg** ([restic design](https://restic.readthedocs.io/en/stable/100_references.html#design),
  [Borg internals](https://borgbackup.readthedocs.io/en/stable/internals/data-structures.html))
  dedupe backups with content-defined chunking and keyed chunk ids, and GC
  unreferenced chunks with a mark and sweep over every snapshot index. We
  take keyed chunk names and reject content-defined chunking: memory and
  disk images are block-addressed, nothing shifts, and a fixed grid lines up
  with pages and filesystem blocks. We reject mark and sweep for now (§6.6).
- **Firecracker diff snapshots**
  ([docs](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md#creating-diff-snapshots))
  write only pages dirtied since the last snapshot, using KVM dirty-page
  tracking. They need the previous snapshot as a base, chain on restore,
  and cloud-hypervisor has no equivalent. We take plain hashing (works for
  every runtime and needs no chain) and keep dirty tracking as a later
  speedup of the hashing pass (Phase 3).
- **EBS snapshots** ([docs](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-snapshots.html))
  are incremental at a fixed block size, with each snapshot an index over
  shared blocks. Same shape as ours.
- **Kubernetes and Nomad** have no published checkpoints to compare
  against. K8s forensic checkpoints are local tarballs.

## 3. Jobs to be done, by persona

**Platform builder running sandboxes (job author)**
- When my agent sandboxes checkpoint every few minutes so they can move
  nodes, I want each checkpoint to cost what changed, not the whole guest,
  so durable sessions are affordable at thousands of sandboxes.
- When thousands of sessions run one image, I want the image's bytes
  stored once, so my bucket bill tracks my users' data, not my fleet size.

**Cluster operator**
- When I size the bucket, its lifecycle rule, and node bandwidth, I want
  one knob for how much is shared and one for how big the pieces are, with
  defaults I do not have to touch, so I can reason about cost.
- When I set a lifecycle rule to stop the bucket growing forever, I want a
  rule I can state in one sentence that never deletes something a
  checkpoint still needs.

**Security / platform admin**
- When published bytes are shared between sandboxes, I want sharing kept
  inside one job, so a tenant's job never learns anything about another
  job's memory, and the bucket alone still reveals nothing.

**On-call responder**
- When a restore fails because a piece is missing or damaged, I want the
  error to name the piece and the likely cause (a lifecycle rule, tampering,
  a wrong key), so I know whether to boot cold or fix the bucket.

*Not in scope:* the autonomous agent persona gets no new surface. The
publish report's byte count simply gets smaller.

### User scenarios

1. A session pool runs 2,000 sessions of one `python` image with
   `checkpointEvery = "10m"` and `durability = "store"`. The first session's
   first checkpoint uploads its non-zero memory and the image's root disk.
   Every other session's first checkpoint uploads its own memory and only
   the root-disk chunks it changed. Every later checkpoint uploads only what
   the guest touched in ten minutes.
2. An operator sets a bucket lifecycle rule: expire `sealed/` objects older
   than 3 days, because the longest-lived suspended session is kept for
   `keepFor = "48h"` and `contentStore.dedupWindow` is the default 24h. No
   restore ever finds a chunk missing.
3. A restore on another node fetches 16 chunks at a time, never downloads
   the zero chunks, and decrypts each chunk in memory, so the part lands on
   disk once, sparse.

## 4. Goals and non-goals

**Goals**

- A repeat checkpoint of one sandbox uploads only the chunks that changed.
- Zero chunks are never uploaded or downloaded.
- Sandboxes of one job share chunks. Jobs never share chunks.
- Every published object stays sealed. The bucket alone reveals no
  plaintext and no plaintext digest.
- A bucket lifecycle rule of "older than the longest-lived checkpoint plus
  one dedup window" is always safe.
- A restoring node needs exactly the keys it gets today. No new RPC, no new
  manifest field.
- Publishing and fetching run chunks in parallel, and a fetch never lands
  ciphertext on disk.
- Fixed-size chunks a later lazy-restore can page from.

**Non-goals**

- Lazy or streaming restore (a separate workstream). The index format
  allows it.
- Reference-counted garbage collection of the bucket (§6.6).
- Reusing chunks a restoring node already holds locally (Phase 2).
- Dirty-page tracking in the VMM (Phase 3).
- Deduplicating across jobs (§6.3, calls entry 2).
- Compression. Sealed chunks are incompressible, and compressing before
  sealing would leak content through sizes.

## 5. Background: what exists today

- **Capture** writes memory, device state, the VMM config, and a reflink of
  the root disk into a checkpoint dir, hashes each whole file
  (`CheckpointDir::seal`, `src/task_plugins/checkpoint.rs:163`), and imports
  them into the node content store pinned to the snapshot
  (`ingest_memory_parts`, `src/worker/checkpoints.rs:561`).
- **Publish** walks the manifest's memory parts and calls
  `ContentPublishing::publish(part key)` for each
  (`src/worker/checkpoints.rs:832` for snapshots,
  `publish_checkpoint_parts` in `src/worker/task_manager/session_store.rs`
  for `store` sessions). The publisher seals the whole file with the STREAM
  construction and uploads it as one object at
  `sealed/<hkdf(part key, "object-name")>` (S3 multipart, 16 MiB parts,
  4 in flight).
- **Keys.** The manager derives a part key per whole-part digest from the
  cluster checkpoint key and sends it with the publish, warm, or restore
  (`checkpoint_part_keys`, `src/manager/server/snapshot_rpcs.rs`). A `store`
  session's node derives part keys from the job's session sealing key
  (`SessionSealKeys`, `src/checkpoint_seal.rs`). So whole parts dedupe
  cluster-wide for snapshots and job-wide for sessions, but only when the
  whole part is identical, which almost never happens for memory or disk.
- **Fetch** downloads the object through the artifact fetcher, opens it in
  place on disk, and imports it with its digest checked
  (`ArtifactStager::fetch_sealed_into_store`).
- **The bucket is never cleaned by Legion.** The docs tell operators to
  expire `sealed/` objects with a lifecycle rule older than their
  longest-lived snapshot.

## 6. Proposed design

### 6.1 What the bucket holds

```text
<contentStore.url>/sealed/<index name>   one per published part and dedup window:
                                         the object the manifest's `url` names
<contentStore.url>/sealed/<chunk name>   one per distinct, non-zero chunk per
                                         job and dedup window
```

Every object is a `LGNSEAL1` sealed object, as before. The index plaintext:

```text
"LGNPIDX1" | chunk size (u32) | part length (u64) | chunk count (u64)
per chunk:  0x00 ‖ 64 zero bytes                 an all-zero chunk
            0x01 ‖ sha256 (32) ‖ chunk key (32)  a stored chunk
```

A 1 GiB part at 1 MiB chunks has a 66 KiB index.

### 6.2 Keys

```text
checkpoint key ck_…
  │ HKDF(job)
  ▼
job key (the session sealing key that exists today)
  ├─ HKDF("chunks") ─▶ chunk key ─ HKDF("<window secs>/<window>") ─▶ chunk scope
  │                                                                    │ HKDF(chunk digest)
  │                                                                    ▼
  │                                                                  chunk part key
  ▼                                                                  (AEAD key + name)
part key (unchanged: per whole-part digest)
  ├─ HKDF(part ‖ scope, chunk size) ─▶ index name
  └─ HKDF(index name)               ─▶ index key
```

- **Publishing needs the chunk key.** The manager derives the job's chunk
  key and sends it with a snapshot publish (`CheckpointRequest.chunk_key`).
  A `store` session's node derives the same key from the session sealing
  key it already holds. Snapshots and sessions of one job share chunks.
- **Restoring needs only the part key**, exactly as today. The index is
  sealed under a key the part key derives from the index name in the URL,
  and lists every chunk's key. No new manifest field.
- **Each key seals one plaintext.** A chunk key is a function of the chunk's
  digest, and an index key of the index name, which is a function of the
  scope and chunk size the index lists keys for.

### 6.3 Dedup scope: one job, one window

A chunk is shared only between publishes of one job in one dedup window
(`contentStore.dedupWindow`, default 24h, a wall-clock window). Two
reasons:

- **Isolation.** Keyed convergent encryption lets anyone holding the scope
  key test a guess ("did some checkpoint of this job contain this exact
  1 MiB?"). A per-job scope keeps that test inside one job, whose workers
  already hold its session sealing key. Cross-job dedup would need a
  cluster-wide key on every worker, which the encryption plan rules out.
- **Lifecycle safety.** A publish reuses a chunk only if that chunk was
  written during the same window, so a chunk a checkpoint needs is never
  more than one window older than the checkpoint. An age-based lifecycle
  rule of "longest-lived checkpoint plus one window" is therefore safe,
  with no reference counting.

The cost is one full upload of each sandbox's non-zero bytes per window.

### 6.4 Publish

For each part: compute the scope and index name. If the node already
recorded this part at this index URL, or the store already holds the index,
upload nothing. Otherwise read the part once, sequentially, hashing the
whole part and each chunk. Skip zero chunks. For each other chunk, derive
its key and name. A name the node uploaded or saw earlier in the window
(an in-memory set, capped at about a million names) is reused. Anything else gets a
`HEAD` and, when missing, a sealed `PUT`, 16 in flight on four threads.
Then check the whole digest (a mismatch publishes no index and drops the
corrupt blob, as today) and write the sealed index last, so a reader that
finds the index finds every chunk. A failed upload cancels the rest and
writes no index.

### 6.5 Fetch

`GET` the index, open it with the part key, and allocate a sparse file of
the part's length. Fetch each distinct stored chunk once, 16 at a time,
open each in memory, check its digest, and write it at every offset that
holds it. Zero chunks stay holes. Retries cover only chunks that failed.
Tampering, a wrong key, and a missing chunk fail at once by name, cancel
the part's other fetches, and remove the partial file (calls entry 9). The
import then checks the whole digest, as today.

### 6.6 Garbage collection

Legion still never deletes from the bucket. The lifecycle rule becomes
"longest-lived checkpoint plus `dedupWindow`". Reference-counted GC (mark
every chunk reachable from live snapshot rows, session manifests, and
archive manifests, then sweep) is deferred: it needs a full view of
manifests spread across raft and buckets, and a conditional-delete race
story with concurrent publishes. The window makes it unnecessary for
safety, and the cost of not having it is that unreferenced chunks live
until the lifecycle rule, as whole parts do today.

### 6.7 The surface

Worker config, both optional:

```nickel
contentStore = {
  url = "s3://legion-content-usw2/",
  chunkSize = "1MiB",     # 64KiB to 16MiB, a multiple of 4KiB
  dedupWindow = "24h",    # at least 1m
},
```

The publish report's `total_bytes`, the `snapshot.published` event's
`uploadedBytes`, and the `legion_snapshot_publish_bytes` metric now count
only the chunks uploaded. A restore's `fetched_bytes` log counts only the
chunks downloaded.

### 6.8 Failure modes

| Situation | Error |
| --- | --- |
| A chunk the index names was deleted | `chunk N (<name>) of the published part is missing from the content store: something deleted it, for example a bucket lifecycle rule shorter than the checkpoint's age plus contentStore.dedupWindow`. Not retried |
| The index itself is gone | `the published part's index <name> is not in the content store` |
| A chunk or index modified, or swapped for another | `failed authentication at chunk N`. Not retried |
| The manager sent no chunk key | `the manager sent no chunk key: <reason>` (the keyring is locked, or has no checkpoint key) |
| Bad `chunkSize` or `dedupWindow` | refused when the manager loads the worker config, naming the field and range |

## 7. Proto, struct, config, and persistence changes

- `message CheckpointChunkKey { key_id, key }`, redacted from `Debug` and
  skipped by serde. `CheckpointRequest.chunk_key` (12).
- `ContentStoreTarget.chunk_size` and `.dedup_window` (worker config
  `contentStore.chunkSize`, `contentStore.dedupWindow`).
- `MemoryPart.url` now names a part index. No new manifest field, no
  schema version bump: nothing published by an older build is readable
  anyway, and Legion has no installed base.
- No job-config field, so `configs/jobs/maximal.ncl` is unchanged.
- New module `src/checkpoint_chunks.rs`. `ContentPublisher::publish_sealed`
  is replaced by `sealed_objects()`. `open_file_in_place` is removed.

## 8. Implementation phases

1. **Chunked publish and fetch (this change).** Format, keys, manager key
   delivery, worker publish and fetch for snapshots and `store` sessions,
   config knobs, docs. Valuable alone: every repeat checkpoint and every
   sandbox of a job pays only for what differs.
2. **Fetch only what the node lacks.** Keep a node-local chunk index (chunk
   digest to blob and offset) for parts in the content store, and copy
   chunks the node already holds (an earlier checkpoint of the same
   session, another sandbox of the image) instead of fetching them. Pairs
   with keeping released checkpoint parts as unpinned, budget-evicted cache
   on the node instead of deleting them at once (`release_owner_pins`), so a
   session that moves back to a node it ran on fetches only what changed.
3. **Skip hashing clean memory.** On Firecracker, take diff-capable
   snapshots and use the dirty bitmap to reuse the previous checkpoint's
   chunk digests for clean chunks, cutting the publish's hashing pass to the
   dirty set. cloud-hypervisor keeps full hashing.
4. **Lazy restore** reads the index and pages chunks on fault. Not this
   plan's work, but the index is the input it needs.

## 9. Testing plan

- **Unit (`checkpoint_chunks`)**: round trip across chunk boundaries
  (empty, one byte, one chunk exactly, one chunk plus one, several), zero
  chunks never stored and fetched as holes, a repeat checkpoint uploading
  only changed chunks (with and without the node's known-name set), two
  sandboxes of a job sharing chunks and another job not, a new window
  uploading again, no plaintext and no plaintext digest in the store,
  tampered, swapped, and deleted chunks failing by name, the index opening
  only with its part key, a digest mismatch publishing no index, and index
  encoding. Parallel transfers run against a slow in-memory store that
  records the most calls in flight and injects failures: in-flight calls
  stay at or under the cap, a failed upload cancels the rest and writes no
  index, a transient fetch failure retries only its chunks, a missing chunk
  cancels the fetch and removes the partial file, and a repeated chunk is
  fetched once.
- **Unit (`content_publish`)**: the `file://` publisher's layout and
  idempotence across nodes, a corrupt blob refused and dropped, and config
  validation for both knobs.
- **Unit (`stage`)**: the credential a part fetch is signed with, and the
  hint for a store the node has no credential for.
- **Worker (`task_manager`)**: a snapshot published on one node and
  restored on another, unpinning after publish, every way a part fails to
  open.
- **Integration (`published_resume_integration`)**: a publish request
  carries a part key per part and the job's chunk key, and `Debug` redacts
  both.
- **Measurement**: `checkpoint_chunks::tests::chunked_publish_bench` and
  `chunk_transfer_parallelism_bench` (both ignored) and a real-guest measurement of how memory and disk change
  between checkpoints (§10).

## 10. Measurements

**How a real guest changes between checkpoints (measured).** A Debian
guest (6.1 cloud kernel) under QEMU TCG on aarch64, in a container on an
Apple-silicon Mac, with 512 MiB of RAM backed by a file and a 605 MiB ext4
root disk (349 MiB of Debian and Python, 256 MiB of headroom, as Legion
sizes it). Two guests boot from one image. Each runs a Python process that
holds a 48 MiB heap and every second rewrites 16 random 4 KiB pages of it,
inserts a row into SQLite on the root disk, and appends to a log, and every
ten seconds writes a 256 KiB file, runs `ls`, and makes a local HTTP
request, for about 160 s, then idles. Captures pause the VM and copy its
memory file and disk, a minute apart: `a` (30 s after boot), `b` and `c`
(active minutes), `d` (mostly idle), `e` (idle). MiB a content-addressed
publish uploads for each capture, given every earlier capture is in the
store ([`benchmarks/checkpoint-chunks/`](../../benchmarks/checkpoint-chunks/README.md)):

| Chunk | a | a, second guest | b | c | d | e |
| --- | --- | --- | --- | --- | --- | --- |
| whole part (before) | 1117 | 1117 | 1117 | 1117 | 1117 | 1117 |
| 64 KiB | 539 | 209 | 60 | 56 | 15 | 7 |
| 256 KiB | 541 | 215 | 98 | 94 | 45 | 17 |
| 512 KiB | 545 | 220 | 113 | 110 | 70 | 25 |
| 1 MiB | 549 | 227 | 131 | 127 | 105 | 38 |
| 2 MiB | 558 | 236 | 156 | 152 | 140 | 62 |
| 4 MiB | 572 | 252 | 188 | 188 | 176 | 96 |

At 1 MiB, 52% of the memory and 48% of the disk are zero chunks, the root
disk shares 97% of its non-zero chunks with the image, and the second
guest's memory shares only 11% of its non-zero chunks with the first's.
What is not measured: Firecracker or cloud-hypervisor memory files on KVM
(same guest-physical layout, but a different kernel and devices), longer
idle periods, and guests with large page caches.

**Publish and fetch through the code (measured, `file://`).** Release
build on an Apple M-series laptop shared with other builds,
`chunked_publish_bench` with `LEGION_CHUNK_BENCH_MIB=512` (half the part
zero, 2% of pages dirtied between rounds, 1 MiB chunks), timing only the
publish and the fetch: the first publish uploads 256 MiB in 0.68 s, each
later one uploads 70 to 73 MiB (the dirtied chunks) in 0.66 to 0.70 s,
since the publish still reads and hashes the whole part (Phase 3), and a
fetch of the 256 MiB of non-zero chunks takes 0.34 to 0.39 s.

**Serial against parallel transfers (measured, simulated S3).**
`chunk_transfer_parallelism_bench`, same machine, a 256 MiB part of
non-zero 1 MiB chunks through an in-memory store where every call costs
20 ms and each connection moves 80 MB/s:

| Chunks in flight | Publish | Fetch |
| --- | --- | --- |
| 1 (serial) | 15.49 s (17 MB/s) | 9.99 s (27 MB/s) |
| 16 (the default) | 1.07 s (251 MB/s) | 0.74 s (365 MB/s) |

A serial publish pays a `HEAD` and a `PUT` round trip per chunk, so 16 in
flight cut both directions about 14 times. Not measured against a real
bucket (an open item in TODO.md).

## 11. Rollout and backward compatibility

Legion has no installed base, so there is no shim. Parts published by an
older build fail to fetch with `failed authentication` (the URL names a
whole sealed part, which does not open as an index). On the node that
captured them they are still in the content store, and a resume there needs
no fetch. Operators with a lifecycle rule on `sealed/` add `dedupWindow` to
its age.

## 12. Open questions

- Should `dedupWindow` default longer for fleets that checkpoint rarely?
  Lean no: 24h matches `keepFor`, and a longer window only lengthens the
  lifecycle rule.

## 13. Acceptance criteria

- A second publish of a part with a small dirty set uploads only the changed
  chunks (unit test, and the bench).
- No zero chunk is ever stored.
- A publish in a new window or another job shares nothing.
- Every published object is sealed and named by a keyed function.
- A restore on another node reassembles the exact part, and every failure
  in §6.8 is named in a test.
