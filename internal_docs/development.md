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

- **`protobuf-compiler`** — `build.rs` compiles `proto/legion.proto`
  through `tonic-build`. CI installs this one explicitly.
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

- **Debuginfo.** `[profile.dev]` keeps line tables for Legion's crates and
  none for dependencies. Panics keep `file:line`. For a debugger session with
  variables, build with `CARGO_PROFILE_DEV_DEBUG=2`.
- **One integration binary.** Integration tests are modules of
  `tests/integration/main.rs` (see "Test Layout" in
  `docs/testing-and-benchmarks.md`). The 66 per-file binaries were ~200 MB
  each (12.9 GB together); now there are five test binaries totalling ~1.1 GB.
  New integration tests go in as modules, not as new `tests/*.rs` files.
- **Temp dirs.** Store helpers that `keep()` a `TempDir` create it under the
  per-process scratch root (`src/test_scratch.rs`,
  `tests/integration/support.rs`), which is removed when the test process
  exits. A run killed with Ctrl-C leaves its one `legion-tests-*` directory in
  `$TMPDIR`; delete those by hand if they pile up.
- **Build script.** `build.rs` re-runs only when `proto/`, `build.rs`, or the
  plugin env vars change. It used to watch the whole repository, so editing a
  doc or a UI file rebuilt the crate.
- **Incremental cache.** `target/debug/incremental` is usually the largest
  thing in `target/`: rustc keeps up to two sessions of ~1.7 GB for each way
  the `legion` crate gets built (library, library tests, clippy's check of
  each), and a variant you stop using is never collected.
- **One set of feature flags.** Features are part of a crate's build hash, so
  `cargo test --all` and CI's `cargo test --all --all-features` build every
  Legion target twice, with a separate incremental cache for each. Pick one
  and stick to it; `--all-features` only adds the `legion-loadgen` bench tool.
- **Habits.** Run the part you are changing (`cargo test --lib <filter>`,
  `cargo test --test integration <module>::`) rather than `--all` on every
  edit. Each git worktree has its own `target/`; pointing several at one
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

Legion ships as one binary that is manager, worker, CLI, and MCP server, and it
is meant to run on small and edge machines, so size counts. The `dist` profile
adds fat LTO, a single codegen unit, and symbol stripping on top of `release`,
which is worth about 18 MB (measured on macOS arm64, ~520 crates in the
lockfile):

| Build | Size |
| --- | --- |
| `cargo build --release` | 43.7 MB |
| `cargo build --profile dist` | **26.0 MB** |

About half the saving is `strip` removing symbol tables and half is LTO
collapsing dead code and duplicated generics across the dependency tree. For
context, the same measurement run against other orchestrators: k3s is 81.5 MB,
Nomad 150 MB, and a Kubernetes control-plane node (apiserver, controller
manager, scheduler, kubelet, kube-proxy, etcd) is 341 MB.

Two things to know before reaching for it:

- **It is slow.** Fat LTO with `codegen-units = 1` cannot parallelize the final
  link, so expect several minutes where `--release` takes about one. Don't put
  it in an edit-compile-test loop.
- **Stripped binaries don't symbolize.** Backtraces from a `dist` build lose
  function names. Keep the unstripped artifact around if you need to debug a
  crash report from a shipped build.

The profile and the reasoning behind each setting live in `Cargo.toml`.

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

Publishing to crates.io: `legion-rs` depends on `legion-guest` by path and
version, so publish `legion-guest` first.

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
