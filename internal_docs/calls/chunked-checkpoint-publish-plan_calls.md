# Judgement Calls: Chunked Checkpoint Publish Plan

Decisions made while planning and building
[`plans/chunked-checkpoint-publish-plan.md`](../plans/chunked-checkpoint-publish-plan.md).
**Entries are ranked by project impact, highest first. The number is the
rank.** Insert new entries at their rank and fix any citation by number.

## 1. Every published part is chunked, with no whole-object format left

**Call.** A published part is always an index plus fixed-size chunks. The
whole-object format (`publish_sealed`, `open_file_in_place`) is removed, and
a part's manifest `url` now names its index.

**Alternatives.** (a) Keep the whole-object format and add chunking behind
a `contentStore.chunking = true` switch. (b) Chunk only parts above a size
threshold, so a 4 KiB `vm-config` stays one object.

**Trade-off.** Small parts cost two objects (a one-chunk index and the
chunk) instead of one, which on S3 is one extra `PUT` per small part per
window. Parts published by an older build no longer open (`failed
authentication`), since their URL names a whole object, not an index.

**Why (simplicity, and the win is the common case).** Every sandbox
checkpoint is mostly memory and root disk, which is where chunking pays
(entry 4 has the numbers). A switch would keep two readers, two failure
tables, and two sets of tests forever to save one request per small part.
Legion has no installed base, so the old format has no one to protect
(AGENTS.md, "Upgrades and Breaking Changes").

## 2. Chunks dedupe per job and per dedup window, not cluster-wide

**Call.** A chunk's key, and so its name, is derived from the job's chunk
key and the current dedup window (`contentStore.dedupWindow`, default 24h).
Two checkpoints share a chunk only if they belong to one job and publish in
one window.

```nickel
contentStore = { url = "s3://legion-content-usw2/", dedupWindow = "24h" },
```

**Alternatives.** (a) Cluster-wide dedup under the checkpoint key, as whole
parts dedupe today, so every job running `python:3.12` shares one copy of
its root disk. (b) No window: share a chunk forever within a job. (c) Touch
reused chunks (an S3 self-copy) so an age-based lifecycle rule sees them as
fresh. (d) A reference-counted mark and sweep GC.

**Trade-off.** Two jobs on one image store it twice. Each sandbox uploads
its non-zero bytes in full once per window (about 549 MiB for the guest in
entry 4, once a day by default). The lifecycle rule grows by one window.

**Why (security first, then simplicity).** (a) needs a key that can derive
any chunk's key on every publishing worker, which the encryption plan rules
out ("workers never hold a cluster-wide key"), and it would let one tenant's
compromised worker test guesses against every other tenant's memory. A
job's workers already hold its session sealing key, so a per-job scope adds
no new key holder. (b) and (c) fail on lifecycle: with chunks reused
forever, any age-based rule eventually deletes a chunk a fresh checkpoint
still uses, and touching every reused chunk costs one `COPY` request per
chunk per checkpoint, which at 1,000 sessions checkpointing every ten
minutes, each reusing about 500 chunks, is about 70 million requests (about
$360) a day. (d) needs one view over raft
snapshot rows, session manifests, and archive manifests in every bucket,
plus a delete race story. The window turns lifecycle safety into one
sentence ("older than the longest-lived checkpoint plus one window") with no
new subsystem. This is the same move as the rotating salt in AWS Lambda's
chunk store.

## 3. Restorers get exactly the keys they get today: chunk keys ride in the sealed index

**Call.** The index lists each chunk's digest and key, and is sealed under
a key the part key derives from the index's name. A restoring or warming
node needs only the part key the manager already sends. Only a publishing
node gets the new job chunk key (`CheckpointRequest.chunk_key`).

**Alternatives.** (a) Send the job chunk key with every restore and warm
too, and let the reader derive chunk keys from it. (b) Keep the chunk list
in the manifest instead of a bucket object.

**Trade-off.** One extra `GET` (the index) before the chunk fetches start.
Chunk keys are stored in the bucket, protected only by the part key.

**Why (least privilege, simplicity).** With (a), every node that restores a
checkpoint would hold a key that can name and seal any chunk of the job,
which is more than it needs. (b) would put up to a MiB of chunk list per
part into raft rows and session manifests and still need the keys
somewhere. The index makes the manifest shape and the restore protocol
unchanged: no proto field on restore or warm, no manifest version bump.

## 4. The default chunk size is 1 MiB

**Call.** `contentStore.chunkSize` defaults to 1 MiB (64 KiB to 16 MiB, a
multiple of 4 KiB).

**Alternatives.** 512 KiB (AWS Lambda's chunk size), 256 KiB, or 4 MiB.

**Trade-off.** Larger chunks re-upload more clean bytes around each dirty
page. Measured on a real Linux guest (QEMU TCG on aarch64, Debian 6.1
kernel, 512 MiB RAM, 605 MiB root disk, a Python process that rewrites 16
random 4 KiB pages of a 48 MiB heap, inserts into SQLite, and appends to a
log every second, and writes a 256 KiB file and runs `ls` every ten
seconds), one checkpoint a minute, MiB uploaded per checkpoint:

| Chunk | First | First, second guest of the job | Active minute | Idle minute |
| --- | --- | --- | --- | --- |
| whole part (before) | 1117 | 1117 | 1117 | 1117 |
| 64 KiB | 539 | 209 | 60 | 7 |
| 256 KiB | 541 | 215 | 98 | 17 |
| 512 KiB | 545 | 220 | 113 | 25 |
| 1 MiB | 549 | 227 | 131 | 38 |
| 2 MiB | 558 | 236 | 156 | 62 |
| 4 MiB | 572 | 252 | 188 | 96 |

About half of each capture is zero chunks (52% of memory, 48% of the disk
at 1 MiB). The second guest booted from the same image shares 97% of its
non-zero root-disk chunks with the first, but only 11% of its memory
chunks.

**Why (performance per dollar, and fewer objects).** Bytes are not the only
cost. On S3 a `PUT` costs about what storing 3 MiB for two days does
($0.005 per 1,000 requests against $0.023 per GB-month), and checkpoints
behind a dedup window live days, not months. 256 KiB saves 25% of the
active minute's bytes over 1 MiB but makes three times the requests (392
against 131), so it costs more for the same checkpoint. 1 MiB also equals
the seal's internal block, so a chunk is one AEAD operation, and a 1 GiB
part is a 66 KiB index and at most 1,024 objects to fetch. An operator who
pays for bandwidth rather than requests turns the knob down.

## 5. Fixed-size chunks and plain hashing, not content-defined chunking or VMM dirty tracking

**Call.** Parts are cut on a fixed grid and every chunk is hashed on every
publish.

**Alternatives.** (a) Content-defined chunking (rolling hash boundaries),
as restic and Borg use. (b) Firecracker diff snapshots: KVM's dirty bitmap
says which pages changed, so only those are hashed and sent.

**Trade-off.** A publish reads and hashes the whole part every time, about
two SHA-256 passes over every byte, even when one page changed.

**Why (extensibility, simplicity).** (a) helps when insertions shift data,
which never happens in a memory image or a block device: offsets are
addresses. A fixed grid lines up with pages and filesystem blocks, and lets
a later lazy restore find a page's chunk by division. (b) works only on
Firecracker (cloud-hypervisor has no diff snapshots), chains each snapshot
to its parent, and still needs content addressing to dedupe across
sandboxes. Plain hashing works for every runtime with no state between
checkpoints. Dirty tracking stays as a later speedup of the hashing pass
(plan Phase 3), not a format.

## 6. The node remembers chunk names it uploaded this window, and asks the store about the rest

**Call.** A publishing node keeps an in-memory set of chunk names it
uploaded or found in the store during the current window (cleared when the
window changes, capped at about a million names). A chunk not in the set gets a `HEAD`,
and a `PUT` only when missing.

**Alternatives.** (a) `HEAD` every chunk on every publish. (b) A
conditional `PUT` (`If-None-Match: *`) for every chunk, skipping the `HEAD`.
(c) Persist the set in the content store's SQLite index.

**Trade-off.** If something deletes a chunk mid-window behind the node's
back, the node keeps naming it, and that checkpoint's restore fails naming
the missing chunk. A restarted worker asks the store again for every chunk
once.

**Why (performance).** A repeat checkpoint of the guest in entry 4 would
otherwise spend about 550 `HEAD`s to learn nothing. (b) sends the body even when
the object exists, which is the bandwidth chunking exists to save. (c)
saves only one round of `HEAD`s after a restart. The deletion risk is the
same one the per-digest `published_url` record already accepts, and a
lifecycle rule sized per entry 2 never causes it.

## 7. Chunks transfer 16 at a time on a private four-thread runtime per part

**Call.** Publish and fetch each run a multi-thread tokio runtime with four
workers and keep 16 chunk requests in flight. The calling thread reads (or
assembles) the part sequentially. Neither number is a knob.

**Alternatives.** (a) A `contentStore.transfers` knob. (b) One shared
runtime for the worker.

**Trade-off.** A node publishing several parts at once runs several small
runtimes. The fixed 16 may underuse a very fast link or crowd a slow one.

**Why (simplicity).** Publishing and fetching are blocking by contract (like
the artifact fetchers), and each already built a private runtime. 16
requests is enough to saturate a node's S3 bandwidth at 50 to 100 MB/s per
connection, and four threads seal or open faster than that. This closes the
encryption plan's "seal chunks of one large part in parallel" and "decrypt
as they stream in" deferrals ([checkpoint-encryption calls 6 and
7](checkpoint-encryption-plan_calls.md)) without a knob. A knob can follow a
measured need.

## 8. A restore still fetches every non-zero chunk

**Call.** Phase 1 changes what is uploaded and how a fetch runs, but a
restore fetches every non-zero chunk of a part the node lacks, even when
the node holds an earlier checkpoint of the same session.

**Alternatives.** Build a node-local chunk index (chunk digest to blob and
offset) now, and copy chunks the node already holds.

**Trade-off.** A session moving back to a node it ran on downloads its
memory again. The publish side's savings do not yet show on the fetch side,
apart from zero chunks.

**Why (scope).** The local chunk index touches the node content store, its
eviction, and the restore path that the lazy-restore work is also changing.
Publishing is where every checkpoint pays today, and it ships alone. The
plan's Phase 2 names the follow-up, together with keeping released parts as
evictable cache instead of deleting them at once.

## 9. A failed chunk cancels the part's other transfers, and a fetch downloads a repeated chunk once

Made while porting the branch onto `main` after PR #43, on the user's ask
that pushing and pulling chunks run in parallel with partial failure
handled.

**Call.** The branch already moved chunks 16 at a time (entry 7). The port
keeps that and tightens three edges. In-flight transfers live in a tokio
`JoinSet`, so any early return (a permanent chunk failure, an unreadable
source, a digest mismatch) aborts the rest at once instead of relying on the
private runtime's shutdown. A failed fetch removes the half-written part
file itself, whoever called it. And a fetch groups the index's entries by
chunk digest, so a chunk the part repeats is downloaded and opened once and
written at every offset that holds it, matching what the publish side
already did with its `queued` set. The in-flight cap stays a constant. Tests
and the parallelism benchmark reach a private `cap` argument, not a config
field.

**Alternatives.** (a) Leave cancellation to runtime shutdown, which does
abort the tasks, only implicitly. (b) A `contentStore.transfers` knob so
the benchmark could compare serial and parallel. (c) Skip fetch-side dedup,
since repeated non-zero chunks are rare outside all-0xff runs and
duplicated files.

**Trade-off.** One more `HashMap` per fetch (one entry per distinct chunk,
about 1,000 for a 1 GiB part). The cap is still not tunable by operators.

**Why (simplicity).** The cancellation and cleanup are visible where they
happen, so the next reader does not have to know how a dropped runtime
treats its tasks. Fetch dedup is a few lines and makes the restore side
honest about "each distinct chunk once". A knob would be surface with no
measured need behind it (entry 7).
