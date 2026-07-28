# Development Notes

Assorted notes on how to develop Legion

### Running locally

<!-- Run the manager -->
cargo run -- start manager -c configs/manager.jsonnet
<!-- Join the cluster as a worker node (note the port and lack of tls) -->
LEGION_ADDR=http://127.0.0.1:4748 cargo run -- join -a
<!-- Now you can make calls to legion (note that the port is different, should be TLS in the future) -->
LEGION_ADDR=http://127.0.0.1:4747 cargo run -- ledger

### Running a second cluster on one machine

A worker serves its own gRPC endpoint on port 4848 and advertises that address
to the manager, so two workers on one host collide (the manager dispatches both
allocations to whichever process owns the port). Set `LEGION_WORKER_PORT` to
give a second worker its own, and point the manager at a free port and store
path:

```bash
LEGION_WORKER_PORT=4858 \
LEGION_WORKER_STATE_PATH=/tmp/legion-test/worker-state.json \
  cargo run -- start worker "$TOKEN" --name test-worker
```

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

By default Linux workers connect to `http://192.168.64.1:4748` (the default `container` host gateway for the default network). If your setup differs, pass `--manager-addr`. Run `mise run join --help` for additional options (platform, custom manager address, container name prefix, startup log tail behavior, and forwarded join args).

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
