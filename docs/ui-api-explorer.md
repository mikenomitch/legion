# UI API Explorer library research

This document captures the library evaluation behind the in-app API Explorer.

## Candidate libraries

1. **ConnectRPC (`@connectrpc/connect`, `@connectrpc/connect-web`)**
   - Already used in Legion UI.
   - Popular in TypeScript gRPC-web ecosystems.
   - Strongly typed generated clients and active maintenance.
   - Supports browser transport cleanly without custom plumbing.

2. **Buf generated protobuf runtime (`@bufbuild/protobuf`)**
   - Already used in Legion UI.
   - Exposes message descriptors (`fields`) that can be used to render request/response type tables.
   - Lets the UI parse JSON payloads to typed protobuf messages (`fromJson`) and print typed responses (`toJson`).

3. **grpcui / grpc-web tooling options**
   - grpcui is excellent for standalone interactive gRPC UIs, but embedding into the existing Solid app would require running and proxying a separate server process.
   - Browser extensions and devtools are useful for debugging, but not suitable as a first-class in-product API section.

## Selected approach

Use **ConnectRPC + Buf runtime descriptors** directly inside the Legion UI.

Rationale:
- Keeps a single integrated UI surface.
- Avoids running another service just for API docs/testing.
- Uses mature, popular, already-adopted dependencies.
- Provides both interaction (invoke every Manager RPC) and type visibility (request/response field tables) from generated protobuf metadata.
