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

A worker serves its own gRPC endpoint on port 4848 and advertises that address
to the manager, so two workers on one host collide (the manager dispatches both
allocations to whichever process owns the port). Set `LEGION_WORKER_PORT` to
give a second worker its own. Point the manager at a free port, and give both
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
