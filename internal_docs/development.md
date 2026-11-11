# Development Notes

Assorted notes on how to develop Legion

### Build prerequisites on a bare Linux box

macOS and the GitHub Actions runners both satisfy these already, which is
why nothing else mentions them. A fresh Ubuntu machine does not, and the
failures are a long way from their cause:

```bash
sudo apt-get install -y build-essential pkg-config libssl-dev \
    protobuf-compiler clang libclang-dev
```

- **`protobuf-compiler`** — `proto/build.rs` (the `legion-proto` crate)
  compiles `proto/legion.proto` through `tonic-build`. CI installs this one
  explicitly.
- **`clang` + `libclang-dev`** — `rusqlite` is built with the `session`
  feature (raft persistence replicates write effects as SQLite session
  changesets, see `docs/multi-raft.md`), which forces `libsqlite3-sys` to
  run `bindgen` at build time. `bindgen` needs libclang *and* clang's own
  builtin headers. Without them the build dies deep in a dependency with
  `sqlite3/sqlite3.h:35:10: fatal error: 'stdarg.h' file not found`,
  which reads like a missing libc rather than a missing clang.
  `build-essential` does not cover it — that gives you gcc.

`benchmarks/aws/scripts/bootstrap-operator-box.sh` installs this set on
the box it builds, and verifies the toolchain is reachable over a
*non-interactive* SSH before it starts a campaign. That second part
matters: rustup adds `~/.cargo/bin` to the PATH via `~/.bashrc`, which
Ubuntu's `.bashrc` skips for non-interactive shells, so `ssh host "cargo
build"` fails with `command not found` on a box where an interactive
login finds cargo fine.

### Running locally

<!-- Run the manager -->
cargo run -- start manager -c configs/manager.ncl
<!-- Join the cluster as a worker node: `-a` mints a join token through the API, then joins with it -->
LEGION_ADDR=http://127.0.0.1:4747 cargo run -- join -a
<!-- Now you can make calls to legion -->
LEGION_ADDR=http://127.0.0.1:4747 cargo run -- ledger

### Running a second cluster on one machine

A worker serves its worker plane (mTLS gRPC) on `0.0.0.0:4848` and advertises
that port to the manager, so two workers on one host collide: the second one
cannot bind it, and its exec, logs, and push dispatch fail (the manager
verifies each node's name, so nothing is misrouted). Set `LEGION_WORKER_PORT`
to give a second worker its own. Point the manager at a free port, and give both
processes their own `LEGION_DATA_DIR` so they share no state with your main
cluster (see `docs/data-directory.md`):

```bash
LEGION_DATA_DIR=/tmp/legion-test \
  cargo run -- start manager -c /tmp/legion-test/manager.ncl  # server.port changed
LEGION_DATA_DIR=/tmp/legion-test LEGION_WORKER_PORT=4858 \
  cargo run -- start worker "$TOKEN"
```

The same variable is the quickest way around a local `.legion` store left
broken by another branch: start against a fresh `LEGION_DATA_DIR` instead of
deleting it.

### Linux worker containers for local manager testing

Use the mise helper to start worker(s) in Linux containers (via Apple's `container` tool) and connect them to your local manager:

```bash
container system start
```

Install and start Apple `container` first if needed: https://github.com/apple/container


```bash
# One Linux worker
mise run join --linux

# Three Linux workers
mise run join --linux --count 3
```

By default Linux workers connect to `http://192.168.64.1:4747` (the default `container` host gateway for the default network). If your setup differs, pass `--manager-addr`. Run `mise run join --help` for additional options (platform, custom manager address, container name prefix, startup log tail behavior, and forwarded join args).

### Keeping builds and test runs small

A full `cargo test --all` used to leave ~20+ GB in `target/` and another
gigabyte of SQLite stores in the system temp dir on every run. What keeps it
down now, and what to keep doing:

- **Memory, and the crates.** rustc compiles a crate in one process whose
  memory grows with the crate, so Legion is five crates in a chain
  (`legion-proto` → `legion-common` → `legion-worker` → `legion-manager` → the
  binary; [docs/codebase-conventions.md](../docs/codebase-conventions.md#crates)).
  Measured 2026-11-10 on 4 vCPU / 16 GB, dev profile
  ([the crate-split plan](plans/workspace-crate-split-plan.md#54-measured-before-and-after)):

  | | One crate | Five crates |
  | --- | ---: | ---: |
  | `cargo build`, clean: peak | 9.0 GB | 5.0 GB |
  | Largest single compile | 9.0 GB | 3.8 GB (`legion-manager`) |
  | `cargo test --all --no-run`: peak | OOM-killed at 13.5 GB | 9.6 GB |
  | Largest unit-test compile | 10.9 GB | 4.5 GB (`legion-manager`) |
  | `cargo check` after an edit | 30 s / 3.9 GB | 3–19 s / 0.7–1.4 GB |

  The 9.6 GB is everything cargo runs at once on four cores (two unit-test
  compiles and the test binaries' links). On a machine with less than about
  12 GB, pass `-j 2`: what a build needs at minimum is its largest single
  compile, 4.5 GB. `cargo test -p legion-worker` (or `-p legion-common`) never
  builds the manager at all.
- **Debuginfo.** `[profile.dev]` keeps line tables for Legion's crates and
  none for dependencies. Panics keep `file:line`. For a debugger session with
  variables, build with `CARGO_PROFILE_DEV_DEBUG=2`.
- **One integration binary.** Integration tests are modules of
  `tests/integration/main.rs` (see "Test Layout" in
  `docs/testing-and-benchmarks.md`). The 66 per-file binaries were ~200 MB
  each (12.9 GB together); now there are five test binaries totalling ~1.1 GB.
  New integration tests go in as modules, not as new `tests/*.rs` files.
- **Temp dirs.** Store helpers that `keep()` a `TempDir` create it under the
  per-process scratch root (`crates/common/src/test_scratch.rs`,
  `tests/integration/support.rs`), which is removed when the test process
  exits. A run killed with Ctrl-C leaves its one `legion-tests-*` directory in
  `$TMPDIR`; delete those by hand if they pile up.
- **Build scripts.** The root `build.rs` re-runs only when itself or the
  plugin env vars change; `proto/build.rs` only when `proto/` changes. It used
  to watch the whole repository, so editing a doc or a UI file rebuilt the
  crate. Editing `legion.proto` rebuilds `legion-proto` and then `legion`.
- **Incremental cache.** `target/debug/incremental` is usually the largest
  thing in `target/`: rustc keeps up to two sessions of ~1.7 GB for each way
  the `legion` crate gets built (library, library tests, clippy's check of
  each), and a variant you stop using is never collected.
- **One set of feature flags.** Features are part of a crate's build hash, so
  `cargo test --all` and CI's `cargo test --all --all-features` build every
  Legion target twice, with a separate incremental cache for each. Pick one
  and stick to it; `--all-features` only adds the `legion-loadgen` bench tool.
- **Habits.** Run the part you are changing (`cargo test -p legion-worker
  <filter>`, `cargo test --test integration <module>::`) rather than `--all`
  on every edit. A bare `cargo test --lib <filter>` works too, but builds
  every crate's unit tests to run the ones that match. Each git worktree has its own `target/`; pointing several at one
  `CARGO_TARGET_DIR` shares the dependency builds at the cost of serializing
  builds on Cargo's lock. `cargo clean` now and then drops stale artifacts.

Measured on a 4-core Linux container (2026-10-18), before and after these
changes:

| | Before | After |
| --- | --- | --- |
| Test binaries | 70 (66 integration) | 9 (5 integration) |
| Integration test binaries on disk | 12.9 GB (line tables only) | 1.1 GB |
| `target/` after a clean `cargo test --all --no-run` | 20 GB (line tables only) | 8.3 GB |
| Temp dir left behind per full run | > 1.1 GB | 0–44 KB |
| Library unit tests | 335 s | 208–225 s |
| Full suite, test execution only | 10 m 20 s | 7 m 21 s |
| No-op build after editing a doc | crate rebuild (44 s incremental) | 0.3 s |

The "before" column already includes the debuginfo change and the two
targets dropped from `Cargo.toml` (the library's doc-test pass and the
`legion` binary's empty unit-test binary), so the original tree was larger
and slower still; CI recorded ~23 GB at full debuginfo with only 29 test
binaries.

### CI caching

The Rust jobs restore a dependency cache (`Swatinem/rust-cache`) keyed on the
toolchain and `Cargo.lock`. Only runs on `main` save it, because a cache
written on a PR ref is visible to that PR alone and would crowd main's entry
out of the repo's 10 GB budget; CI runs on pushes to `main` for that reason.
The first run after a `Cargo.lock` change rebuilds every dependency. A new
push to a PR cancels the run in progress for it. `protoc` comes from a pinned
release download (`.github/actions/setup-protoc`), not `apt-get`.

### Building a shipping binary (`--profile dist`)

Use `--release` for local work and `--profile dist` for anything you hand to
someone else:

```bash
cargo build --profile dist
```

The binary lands in `target/dist/legion` rather than `target/release/legion`.

Legion ships as one binary that is manager, worker, CLI, and MCP server, so
size counts. The `dist` profile adds fat LTO, a single codegen unit, and
symbol stripping on top of `release`, which is worth about 70 MB (measured
2026-11-10 on linux/amd64, ~470 crates in the build, 4 vCPU / 16 GB):

| Build | Size | Clean build | Peak RSS | Largest single process |
| --- | --- | --- | --- | --- |
| `cargo build --release` | 159.6 MB | 11m 00s | 7.4 GB | 4.5 GB |
| `cargo build --profile dist` | **89.1 MB** | 23m 46s | 9.8 GB | 7.8 GB (the LTO link) |

A little over half of the saving is `strip` removing the symbol tables
(41 MB) and the rest is LTO collapsing dead code and duplicated generics
(loaded sections 118.7 MB → 89.1 MB). Of the 89 MB, `.text` is 71.6 MB and
unwind tables 8.8 MB. The sizes are from after the size work described
[below](#what-else-keeps-the-binary-small); the build times and memory are
from just before it.

As one crate, before the split into five
([the plan](plans/workspace-crate-split-plan.md#54-measured-before-and-after)),
the `dist` binary was 91.5 MB, and 97.3 MB after it: the split costs 5.8 MB,
most likely generic code now instantiated once per crate. Its build took
35 minutes and 11.1 GB.

An earlier version of this page quoted 43.7 MB and 26.0 MB. Those were macOS
arm64 numbers from an older, smaller tree, and the README's "~26 MB" came
from them; [Finding what grew](#finding-what-grew) has where the difference
came from. For
context, the same kind of measurement for other orchestrators: k3s is
81.5 MB, Nomad 150 MB (both embed a web UI, which Legion serves from disk),
and a Kubernetes control-plane node (apiserver, controller manager,
scheduler, kubelet, kube-proxy, etcd) is 341 MB.

Two things to know before reaching for it:

- **It is slow.** Each crate is one codegen unit and the fat-LTO link is
  single-threaded: ~24 minutes on four cores, 16 of them the link, where
  `--release` takes ~11. Don't put it in an edit-compile-test loop. It needs
  ~10 GB free at the peak, which comes while Legion's crates are in LLVM
  together, before the link.
- **Stripped binaries don't symbolize.** Backtraces from a `dist` build lose
  function names. Keep the unstripped artifact around if you need to debug a
  crash report from a shipped build.

The profile and the reasoning behind each setting live in `Cargo.toml`.

#### What else keeps the binary small

Three things besides the profile, worth 8.2 MB together on the `dist` binary
(97.3 MB without them, 89.1 MB with). The reasoning, and the levers that were
measured and left alone, are in
[`calls/binary-size_calls.md`](calls/binary-size_calls.md).

- **Packed relocations (2.9 MB).** `build.rs` links `legion` with
  `-z pack-relative-relocs` when the build is native linux-gnu and the host's
  glibc is 2.36 or newer, the first that can load the result. A binary built
  that way does not start on an older glibc (`version 'GLIBC_ABI_DT_RELR' not
  found`). Set `LEGION_PACK_RELATIVE_RELOCS=0` to build without it, or `=1` to
  force it for a cross build. `readelf -S target/dist/legion | grep relr`
  shows whether a binary has it.
- **Cold crates built for size (about 3.3 MB).** Nickel, its bignum library,
  and CEL are compiled with `opt-level = "s"` in every optimized profile
  (`[profile.release.package.*]` in `Cargo.toml`). They only run when a config
  file or a validation rule is read:
  `legion job validate configs/jobs/maximal.ncl` takes 26 ms against 25 ms.
  Add a crate there only if it is large and off the scheduler, storage, and
  network paths, and time what it does before and after.
- **Dependency features trimmed (about 1.9 MB).** `jsonschema` is built
  without its default features, which were a CLI and resolvers for remote and
  on-disk `$ref`s, and with them a second HTTP stack. wasmi's WebAssembly text
  parser (`wat`) is on only for `legion-worker`'s tests.

The last two were measured one at a time before the crate split; after it
they are worth 5.3 MB together.

#### Finding what grew

Sizes move a megabyte at a time with ordinary feature work: each RPC costs
40-100 KB of generated tonic, prost, and serde code. To see where a binary's
bytes are, build `--release` (it keeps symbols) and rank them:

```bash
cargo build --release
nm -S --size-sort -C target/release/legion | tail -50
```

To attribute growth between two commits, build each one, `strip` a copy, and
compare file sizes; then group `nm` output by the crate prefix of each symbol
to see which crate moved. Compare like with like: a `--release` delta is
roughly 1.3 times the `dist` delta for the same change.

Grouped that way, a `--release` build from 2026-11-08 (one crate, 94.6 MB of
symbols) is mostly Legion's own code and the generic code instantiated for
its types, not any one dependency:

| Owner of the symbol | Size |
| --- | --- |
| `legion` | 28.2 MB |
| `core` and `alloc` (generics instantiated for Legion's and other crates' types) | 20.9 MB |
| Nickel: `nickel_lang_parser`, `nickel_lang_core`, `malachite_nz` | 7.8 MB |
| `serde_json` | 5.2 MB |
| `tonic` | 4.3 MB |
| `tokio` | 2.9 MB |
| `h2` and `hyper` | 2.6 MB |
| `wasmi`, `wast`, `wasmparser` | 1.3 MB |
| `openraft` | 0.9 MB |
| `object_store`, `cel`, `oci_client`, `jsonschema` | 0.2-0.4 MB each |

The history, stripped `--release` on linux/amd64:

| When | Size | What moved it |
| --- | --- | --- |
| 2026-07-28 | 42.8 MB | the `dist` profile lands |
| 2026-08-03 | 62.7 MB | Nickel replaces Jsonnet, #188 (+15.8 MB) |
| 2026-09-28 | 68.4 MB | sandbox network handlers, #178 (+4.9 MB) |
| 2026-10-12 | 85.4 MB | about twenty feature PRs in two days (+14.7 MB); suspend/resume, #15, is the largest (+5.0 MB) |
| 2026-10-27 | 94.7 MB | the mesh surface, #51 (+4.9 MB) |
| 2026-11-08 | 120.9 MB | apps, app objects and the autoscaler, #60 (+16.5 MB); the architecture review branch, #68 (+6.3 MB) |

### The guest binary (`legion-guest`)

A release ships a second artifact per architecture: `legion-guest`, the
static build of the sandbox guest agent and resume handler (the `guest/`
workspace crate). Workers copy it into every sandbox guest that needs it, so
it must run on any image, and a musl static build does
(docs/task-runtime-sandbox.md, "The guest agent"):

```bash
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
cargo build -p legion-guest --profile dist --target x86_64-unknown-linux-musl
cargo build -p legion-guest --profile dist --target aarch64-unknown-linux-musl  # needs an aarch64 linker
```

The x86_64 build needs no C toolchain (the crate is pure Rust over `libc`):
`static-pie linked`, 750 KB with `--profile dist`, measured 2026-10-12. It
runs in an empty chroot with no libc at all, which the glibc `legion` binary
cannot. Install it beside the worker binary as `legion-guest`, or point the
worker's `LEGION_GUEST_BINARY` at it.

Publishing to crates.io: `legion-rs` 0.1.0 and `legion-guest` 0.1.0 were
published when Legion was one crate. `legion-rs` now depends by path and
version on five crates, so a release publishes them first, in this order:
`legion-proto`, `legion-isolate`, `legion-guest`, `legion-common`,
`legion-worker`, `legion-manager`, then `legion-rs`. `legion-isolate` is
not a dependency of `legion-rs` (the worker starts it as a separate
binary); it depends only on `legion-proto`, so it can go any time after
that. None of the new names (`legion-isolate` included) has been published
yet. Before the first such release: `legion-worker` embeds
`bpf/legion_net.bpf.o` from outside its package directory (it was outside
`legion-rs`'s `include` list too), so that object has to move into the
package or be generated by its build script for `cargo publish` to verify.

### The isolate binary (`legion-isolate`) and its images

The `isolate` runtime (docs/task-runtime-isolate.md) adds a third artifact
per architecture: `legion-isolate`, the only binary that links wasmtime. A
worker starts it as one confined host process per cordon replica and as a
short-lived compiler per module. It is x86-64 Linux only for now. The crate
is `crates/isolate`, a workspace member but not a default member (like
`guest/`), so a plain `cargo build` skips it:

```bash
cargo build -p legion-isolate --profile dist    # target/dist/legion-isolate
```

Install it beside the worker binary as `legion-isolate`, or point the
worker at it with `isolate.binary` or `LEGION_ISOLATE_BINARY` (that order:
config, beside the worker, env var). A worker that finds a working binary
advertises the `isolates` capability. No release ships it yet.

The worker runs `legion-isolate compile --input <file> --output <file>
--confine <isolate.confine>` once per module; `--confine` is `enforce`,
`best-effort`, or `off` and defaults to `enforce`, so running the compiler
by hand on a machine without unprivileged user namespaces or Landlock
needs `--confine best-effort` (it prints `{"ok":false,"kind":"confinement",
...}` and exits 3 otherwise).

**The language images** (`js`, `python`) are not checked in and CI does
not build them:

```bash
crates/isolate/images/build.sh                 # both images + manifest.json
crates/isolate/images/build.sh --out DIR js    # one image, elsewhere
crates/isolate/images/build.sh --no-manifest   # skip the measuring step
```

It needs `python3` with `venv` and `pip`, `node` 20+ and `npm`, and
`cargo`. The pinned toolchains (componentize-py 0.25.1 from
`requirements.txt` with hashes, componentize-js 0.23.0 from
`js/package-lock.json`) are installed into `target/isolate-images-tools`
on first use (`LEGION_IMAGES_TOOLS` moves it; `LEGION_COMPONENTIZE_PY` and
`LEGION_JS_NODE_MODULES` reuse existing installs). Output goes to
`target/isolate-images/` (`js.wasm`, `python.wasm`, `manifest.json`). The
manifest step runs `manifest_write_test` against the built images to
record each image's sha256, initial memory, the Python module list the
pre-flight check uses, and idle bytes per instance, so it compiles the
crate's test binary (minutes the first time). Builds are not
bit-reproducible: a release ships the artifact the manifest pins, never a
rebuild. Images ship as `isolate-images/` beside the binary (or
`isolate.images`, or `LEGION_ISOLATE_IMAGES`), each `<name>.wasm` plus the
`manifest.json`. Bumping a toolchain and the image internals are in
`crates/isolate/images/README.md`.

**Running the isolate tests locally:**

```bash
# The host library: run, admission, lifetimes, http, scratch, binary, escape corpus.
# With the feature, binary.rs also runs the confinement tests: forbidden
# syscalls, each layer alone (--test-skip), stray descriptors, the PID
# namespace, the compiler's --confine. Without it they are ignored.
cargo test -p legion-isolate -j 3 --features confinement-test

# Both images compiled and served by the real confined binary (ignored;
# build the images first)
cargo test -p legion-isolate -j 3 --features confinement-test --test binary \
  -- --ignored both_language_images_run_in_the_confined_binary_test

# The language images (ignored by default; build them first)
crates/isolate/images/build.sh
cargo test -p legion-isolate -j 3 --test images -- --ignored --nocapture
#   LEGION_ISOLATE_IMAGES=<dir> to use images other than target/isolate-images

# The worker side against the real binary, the whole stack through a
# manager (the `cluster` harness in isolate.rs), and the control plane
cargo test --test integration -j 3 isolate:: isolate_control::

# The images through manager -> worker -> host (ignored; build images first,
# and a release legion-isolate saves minutes compiling the Python image)
LEGION_ISOLATE_BINARY=$PWD/target/release/legion-isolate \
  cargo test --test integration -j 3 isolate_images:: -- --ignored --nocapture

# The worker's supervisor, datapath and child VMs (fake host)
cargo test -p legion-worker -j 3 --lib isolates::

# Benchmarks
cargo bench -p legion-isolate --bench isolate_bench
```

The integration tests find `legion-isolate` through
`LEGION_ISOLATE_BINARY`, else beside the test binary in the build profile's
directory, else they build it once with `cargo build -p legion-isolate`;
they fail, never skip, when they cannot get it. The VM-tier tests that
need a real sandbox session run are ignored ("needs /dev/kvm"); the Linux
dev sandbox backend takes no exec, so the others use an `exec` session task
stored straight into the store as a stand-in VM task. Hosts in tests confine
themselves with `best-effort`, which in the build container applied all
eight layers. `LEGION_ISOLATE_SECCOMP=log` turns the host's (and the
compiler's) seccomp kills into log entries when chasing a missing syscall;
the allowlists were found with `strace -f --decode-pids=pidns` over the
binary suite and both images. The test-only `--test-skip
seccomp,pidns,scopes,mount,landlock` flag on `host` and `compile` exists
only in a `--features confinement-test` build. The end-to-end timing
(`isolate_images::hello_world_end_to_end_timing`) prints its numbers with
`--nocapture`; with a debug `legion-isolate` it measures debug builds, and
the first Python run then compiles the image for about a minute.

**The guest fixtures** (`crates/isolate/tests/fixtures/{cmd,runner,handler,fs}.wasm`)
are checked in, because CI has no wasm target. Rebuild them after changing
a guest under `crates/isolate/tests/guests/`:

```bash
rustup target add wasm32-wasip2
crates/isolate/tests/guests/build.sh          # every guest
crates/isolate/tests/guests/build.sh runner   # just the named ones
```

### Testing GRPC endpoints

Can use this command to inspect them/test:

grpcui -proto proto/legion.proto -plaintext localhost:4747

Or use `grpcurl`

### CI lint baseline (2026-06)

CI runs `cargo clippy --all -- -D warnings` and will fail on lint errors that
predate your branch — for a while we carried a local "33-error baseline" and
held it steady. That baseline was cleared: auto-fixable lints were fixed
properly, and `too_many_arguments` plus the tonic-`Status` `result_large_err`
sites got targeted `#[allow]`s pending real refactors. Keep it at zero rather
than re-accumulating a baseline.

One CI gotcha worth remembering: a Scheduler Benchmark Gate failure was a
workflow-token permission problem (`paths-filter` needs `pull-requests: read`),
not a code regression. Check the workflow permissions before chasing a
benchmark diff.
