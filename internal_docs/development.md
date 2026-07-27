# Development Notes

Assorted notes on how to develop Legion

### Running locally

<!-- Run the manager -->
cargo run -- start manager -c configs/manager.jsonnet
<!-- Join the cluster as a worker node (note the port and lack of tls) -->
LEGION_ADDR=http://127.0.0.1:4748 cargo run -- join -a
<!-- Now you can make calls to legion (note that the port is different, should be TLS in the future) -->
LEGION_ADDR=http://127.0.0.1:4747 cargo run -- ledger

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
