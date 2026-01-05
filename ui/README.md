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
