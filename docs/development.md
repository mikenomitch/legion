# Development Notes

Assorted notes on how to develop Legion

### Protobuffs

edit legion.proto
`cargo build`
change stuff in server.rs to have new functions

### Plugins

Run `cargo build --release` to make these

### Running locally

<!-- Run the manager -->
cargo run -- start manager -c configs/manager.jsonnet
<!-- Join the cluster as a worker node (note the port and lack of tls) -->
LEGION_ADDR=http://127.0.0.1:4748 cargo run -- join -a
<!-- Now you can make calls to legion (note that the port is different, should be TLS in the future) -->
LEGION_ADDR=http://127.0.0.1:4747 cargo run -- ledger

### Testing GRPC endpoints

Can use this command to inspect them/test:

grpcui -proto proto/legion.proto -plaintext localhost:4747

Or use `grpcurl`
