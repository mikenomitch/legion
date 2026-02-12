# Legion UI

A minimal Solid.js + Tailwind front-end for interacting with the Legion manager over gRPC.

## Getting started

```bash
npm install
npm run dev
```

The UI defaults to talking to the manager at `http://127.0.0.1:4747`. Set `VITE_MANAGER_ENDPOINT` to override:

```bash
VITE_MANAGER_ENDPOINT="http://192.168.1.25:4747" npm run dev
```

## Generating gRPC clients

The generated client stubs live in `src/gen` and can be recreated with:

```bash
npx protoc \
  -I ../proto \
  --plugin=protoc-gen-connect-es=./node_modules/.bin/protoc-gen-connect-es \
  --plugin=protoc-gen-es=./node_modules/.bin/protoc-gen-es \
  --connect-es_out=src/gen --connect-es_opt=target=ts \
  --es_out=src/gen --es_opt=target=ts \
  ../proto/legion.proto
```

## Available scripts

- `npm run dev` – start the dev server
- `npm run build` – type-check and produce a production build
- `npm run preview` – preview the production build locally

## API explorer implementation notes

The `/api` route uses these libraries so Legion does **not** need to hand-roll a custom gRPC console:

- **ConnectRPC (`@connectrpc/connect`, `@connectrpc/connect-web`)**: Maintained by Buf, already used by the UI, and gives strongly-typed, browser-native gRPC calls against generated service descriptors.
- **Buf Protobuf runtime (`@bufbuild/protobuf`)**: Exposes generated message/service descriptors so the API page can render request/response type metadata for every RPC method.
- **JSONEditor (`jsoneditor`)**: Mature and widely used JSON editor widget that provides a rich interactive request body editor instead of building editor UX from scratch.

Alternatives considered:

- **grpcui**: Excellent server-hosted explorer, but it is a separate Go service rather than an embeddable in-app browser component.
- **Postman/Insomnia/BloomRPC**: Great external tools, but they don't satisfy the requirement to provide an integrated API section directly inside Legion UI.
