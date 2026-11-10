# Judgement calls: binary size

No plan: this is build configuration (Cargo profiles, dependency features, one
linker flag), not a new user-facing surface. The measurements behind these
calls, and how to repeat them, are in
[`development.md`](../development.md#building-a-shipping-binary---profile-dist).

All sizes are the `dist` binary on linux/amd64 (Ubuntu 24.04, glibc 2.39)
unless a line says otherwise. The work took it from 97.3 MB to 89.1 MB.
Per-change figures marked "before the split" were measured one at a time on
the single-crate tree, two days before Legion became five crates.

Entries are ranked by project impact, highest first; the number is the rank.

## 1. Packed relocations are switched on by the build script, only where the binary will load

**Call.** `build.rs` passes `-Wl,-z,pack-relative-relocs` to the `legion`
binary's link (2.9 MB) when the build is a native linux-gnu build on a host
with glibc 2.36 or newer. One variable overrides the check either way:

```sh
LEGION_PACK_RELATIVE_RELOCS=0 cargo build --profile dist   # off
LEGION_PACK_RELATIVE_RELOCS=1 cargo build --profile dist   # on (cross builds)
```

**Alternatives.** (a) `rustflags` in a `.cargo/config.toml`, unconditionally
for linux-gnu: three lines, and what most projects do. (b) Pass the flag only
in the release pipeline.

**Trade-off.** A binary built on a new distribution and copied to one with
glibc older than 2.36 (Ubuntu 22.04, RHEL 9, Debian 11) now refuses to start:

```
legion: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_ABI_DT_RELR' not found (required by legion)
```

Built on Ubuntu 24.04 the binary already needed glibc 2.39, so the release
build loses nothing; a build on Debian 12 (2.36) is the case that tightens.
The build script also gained an `unsafe` call to `gnu_get_libc_version`.

**Why (simplicity).** With (a), `cargo build` on Ubuntu 22.04 produces a binary
that does not start on the machine that built it, and `RUSTFLAGS` in the
environment silently drops the flag. With (b), anyone building from source gets
a different binary from the one shipped. The check costs 40 lines once and
needs no instructions: a build never asks for a newer glibc than its own host
has.

## 2. Config and rule evaluators are built for size in every optimized profile

**Call.** Nickel (`nickel-lang-core`, `nickel-lang-parser`), its bignum library
(`malachite-*`), and CEL (`cel`, `antlr4rust`) are compiled with
`opt-level = "s"` under `[profile.release.package.*]`, which `dist` and
`distfast` inherit (3.3 MB, before the split).

**Alternatives.** (a) Override only in `dist`, so `--release` stays at full
speed. (b) Leave CEL at `opt-level = 3`, since rules run on every app object
write. (c) `opt-level = "z"`, or size-optimizing the whole binary.

**Trade-off.** Those crates run slower. Measured:
`legion job validate configs/jobs/maximal.ncl` takes 26 ms against 25 ms. CEL
rule evaluation was not benchmarked on its own; it is bounded per write by the
rule limits in `object_rules.rs`. `"z"` and a whole-binary setting were not
measured.

**Why (performance).** Size is a feature for the edge, and these crates are the
largest code that is never on the scheduler, storage, or datapath: a jobspec is
parsed once per submission and rules run once per object write. Putting the
override in `release` keeps the benchmarks (`cargo bench` inherits `release`)
measuring the code that ships; (a) would have had them measure something
faster than the shipping binary.

## 3. A remote `$ref` in a JSON Schema fails the check; nothing is fetched

**Call.** `jsonschema` is built with `default-features = false`, which removes
its HTTP and file resolvers (1.9 MB with entry 4, before the split, most of
it a second HTTP stack: reqwest 0.11, hyper 0.14, h2 0.3). App and autoscaler schemas already
refused non-local refs through `LocalRefsOnly`. Network plugin `configSchema`
and task plugin schemas used the default resolver, so a schema like this made
the manager issue a GET at validation time:

```nickel
configSchema = { "$ref" = "http://169.254.169.254/latest/meta-data/" }
```

Now the ref does not resolve and the config is refused.

**Alternatives.** Keep the features and route the remaining call sites through
`compile_schema`'s resolver. That closes the fetch but keeps the HTTP stack in
the binary.

**Trade-off.** A plugin whose `configSchema` pointed at a hosted schema stops
validating. Nothing documented that it worked.

**Why (simplicity).** One rule for every schema Legion reads: local refs only.
The docs for app schemas already promise "a remote ref is refused before
anything is fetched"; this makes it true everywhere, and it is the same change
that shrinks the binary.

## 4. The shipped binary loads binary WebAssembly only

**Call.** wasmi's `wat` feature is off in the workspace dependency and on
only in `legion-worker`'s `[dev-dependencies]`. Tests still
write guest modules as text; `legion net plugin apply` and `test` accept
`.wasm` only.

**Alternatives.** Keep accepting text modules, which wasmi did as a side
effect of the feature being on.

**Trade-off.** A plugin author can no longer hand a `.wat` file straight to
`legion net plugin test`. They get `not a valid module`, and the fix is
`wat2wasm`.

**Why (simplicity).** Every documented example is a `.wasm` built by a
compiler, and the module checksum is over those bytes. A text parser in the
worker served nobody.

## 5. Identical-code folding is left off

**Call.** `-Wl,--icf=all` is not passed.

**Alternatives.** Pass it: 3.1 MB on a `--release` build, before the split.

**Trade-off.** 0.7 MB left on the table in `dist` (before the split), where
fat LTO has already merged most duplicates.

**Why (simplicity).** `--icf=all` also merges identical read-only data and
functions whose addresses are compared, and the Rust compiler emits none of the
address-significance tables that make the safe variant work (`--icf=safe`
saved nothing). 0.7 MB does not pay for a class of bug that only shows up in
the shipping profile.

## 6. Panics still unwind

**Call.** `panic = "abort"` is not set for `dist`.

**Alternatives.** Set it: 8.8 MB of unwind tables, plus the landing pads in
`.text`.

**Trade-off.** The single largest remaining lever is not taken.

**Why (extensibility).** About 20 `catch_unwind` sites contain a panic in a plugin,
a rule, or a task runtime to that one call. With `abort`, a bad plugin takes
the worker down. The plugin ABI fingerprint also hashes the panic strategy, so
host and plugins would have to switch together.
