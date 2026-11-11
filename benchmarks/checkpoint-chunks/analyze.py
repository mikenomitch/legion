"""Chunk-level change and dedup between checkpoint captures.

For each chunk size: the share of all-zero chunks, the bytes a content-
addressed publish would upload for each capture given what earlier captures
already put in the store, and cross-VM / base-image overlap.
"""
import hashlib, os, sys

out = sys.argv[1]
SIZES = [64 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20]
MIB = 1 << 20


def chunks(path, size):
    res = []
    with open(path, "rb") as f:
        while True:
            b = f.read(size)
            if not b:
                break
            if b.count(0) == len(b):
                res.append(None)
            else:
                res.append(hashlib.sha256(b).digest())
    return res


def p(name):
    return os.path.join(out, name)


files = {
    "mem": ["vm1-a.mem", "vm1-b.mem", "vm1-c.mem", "vm1-d.mem", "vm1-e.mem"],
    "disk": ["vm1-a.disk", "vm1-b.disk", "vm1-c.disk", "vm1-d.disk", "vm1-e.disk"],
}
total = {k: os.path.getsize(p(v[0])) for k, v in files.items()}
print(f"memory file {total['mem'] / MIB:.0f} MiB, disk image {total['disk'] / MIB:.0f} MiB")
print("today (whole-part publish) uploads both in full on every checkpoint: "
      f"{(total['mem'] + total['disk']) / MIB:.0f} MiB per checkpoint")
for size in SIZES:
    print(f"\n== chunk {size >> 10} KiB ==")
    for kind, names in files.items():
        seen = set()
        line = []
        for i, name in enumerate(names):
            if not os.path.exists(p(name)):
                continue
            cs = chunks(p(name), size)
            zero = sum(1 for c in cs if c is None)
            new = {c for c in cs if c is not None and c not in seen}
            line.append(f"{name.split('-')[1].split('.')[0]}: zero {100 * zero / len(cs):.0f}% "
                        f"upload {len(new) * size / MIB:.1f} MiB")
            seen |= {c for c in cs if c is not None}
        print(f"  {kind:4s} " + " | ".join(line))
    # Base image overlap for the disk.
    base = {c for c in chunks(p("base.img"), size) if c is not None}
    d = chunks(p("vm1-a.disk"), size)
    nz = [c for c in d if c is not None]
    print(f"  disk a vs base image: {100 * sum(1 for c in nz if c in base) / max(1, len(nz)):.0f}% "
          f"of non-zero chunks already in the base image; uploads "
          f"{len({c for c in nz if c not in base}) * size / MIB:.1f} MiB when the image is in the store")
    # Cross-VM: two guests from one image, captured at the same point.
    for kind, ext in (("mem", "mem"), ("disk", "disk")):
        a1 = {c for c in chunks(p(f"vm1-a.{ext}"), size) if c is not None}
        a2 = [c for c in chunks(p(f"vm2-a.{ext}"), size) if c is not None]
        shared = sum(1 for c in a2 if c in a1)
        new2 = len({c for c in a2 if c not in a1})
        print(f"  cross-VM {kind}: {100 * shared / max(1, len(a2)):.0f}% of vm2's non-zero chunks "
              f"are in vm1's capture; vm2 uploads {new2 * size / MIB:.1f} MiB after vm1")
