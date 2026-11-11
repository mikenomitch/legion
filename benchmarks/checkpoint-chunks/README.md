# Checkpoint chunk measurement

How much of a real Linux guest's memory and root disk changes between two
checkpoints, at chunk sizes from 64 KiB to 4 MiB. This is the evidence
behind `contentStore.chunkSize` and the chunked publish format
([docs](../../docs/suspend-and-resume.md#how-published-parts-are-stored),
[plan](../../internal_docs/plans/chunked-checkpoint-publish-plan.md)).

It needs no KVM: two Debian guests run under QEMU's TCG emulator in an
arm64 container, with guest RAM backed by a file so a capture is a copy of
it, like a Firecracker memory snapshot. Each guest runs `work.py` (a Python
process rewriting random pages of a 48 MiB heap, SQLite inserts, a log,
file writes, and subprocesses) for about 160 seconds, then idles.

```sh
docker build --platform linux/arm64 -t legion-checkpoint-chunks benchmarks/checkpoint-chunks
docker run --rm --platform linux/arm64 --privileged --shm-size=2g \
  -v "$PWD/benchmarks/checkpoint-chunks:/m" legion-checkpoint-chunks bash /m/run.sh
```

It takes about ten minutes and prints, per chunk size, the share of zero
chunks and the MiB a content-addressed publish uploads for each capture
given every earlier capture is in the store, then how much the root disk
shares with the image and how much a second guest shares with the first.
`GAP=30` changes the minute between captures.

What it does not cover: Firecracker or cloud-hypervisor on KVM (the memory
file has the same guest-physical layout, but the kernel and devices
differ), long idle periods, and guests with large page caches.
