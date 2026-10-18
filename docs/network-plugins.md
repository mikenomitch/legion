# Network Plugins

A network plugin is a WASM module that runs inside the node datapath as one
more [handler](network-handlers.md#handlers). It sees each request (and, if
it asks, the body and the response), and it can answer the request itself,
rewrite it, count it, or let it through. Plugins use the
[proxy-wasm](https://github.com/proxy-wasm/spec) ABI (versions 0.2.0 and
0.2.1), so a filter written against a proxy-wasm SDK runs on Legion without a
Legion-specific library.

Use a plugin when a decision needs to read the request and none of the
built-in handlers fits:

- An `authz` webhook costs a network round trip per new flow and can only
  answer allow or deny.
- A `proxy` hop costs a second proxy.
- A plugin runs in-process with no extra hop. It can also rewrite headers
  and bodies, meter usage across requests, and sign requests with a key it
  never holds.

The isolation comes from WASM:

- A trap is caught.
- Fuel and a memory cap bound every callback.
- A plugin that one job references never shares instances or data with
  another job's.

Native plugins (`cdylib`) are deliberately not offered: a panic in native
code would take down every tenant's datapath on the node.

## A complete plugin

This plugin makes the free tier read-only. It is written against the
proxy-wasm Rust SDK with no Legion crate:

```rust
use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_http_context(|_, _| -> Box<dyn HttpContext> { Box::new(ReadOnlyFreeTier) });
}}

struct ReadOnlyFreeTier;
impl Context for ReadOnlyFreeTier {}
impl HttpContext for ReadOnlyFreeTier {
    fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
        let tier = self.get_property(vec!["legion", "labels", "tier"]);
        let method = self.get_http_request_header(":method");
        if tier.as_deref() == Some(b"free".as_slice()) && method.as_deref() != Some("GET") {
            self.send_http_response(403, vec![("legion-denied-reason", "the free tier is read-only")], None);
            return Action::Pause;
        }
        Action::Continue
    }
}
```

Build it, try it locally, then apply it:

```console
$ legion net plugin new read-only-free-tier       # the scaffold above, plus its object file
$ cd read-only-free-tier && cargo build --release --target wasm32-wasip1
$ legion net plugin test target/wasm32-wasip1/release/read_only_free_tier.wasm \
    --request 'POST https://api.github.com/repos/x/issues' --label tier=free --stages request
✗ request: 403 answered by plugin read-only-free-tier — the free tier is read-only   (… µs, 57.0K fuel)
client gets 403
$ legion net plugin apply -f read-only-free-tier.ncl --dry-run
$ legion net plugin apply -f read-only-free-tier.ncl
```

The object names the module and what it may do:

```nickel
# read-only-free-tier.ncl
{
    name = "read-only-free-tier",
    module = {
        source = "https://artifacts.example.com/read-only-free-tier.wasm",
        checksum = "sha256:48dc46b14d621f9952f0a1ac439330b8408e243c52df01eaced37cb1d2cfdd83",
    },
    stages = ["request"],
    usableBy = "policies",
}
```

A policy object references it by name, like any other handler:

```nickel
{
    name = "agents-free-tier",
    enforcement = "require",
    selector = { labels = { tier = "free" } },
    egress = {
        protocol = "http",
        allow = ["api.github.com/**"],
        handlers = [ { plugin = { name = "read-only-free-tier" } } ],
    },
}
```

A refused request gets the plugin's own answer, plus the hop that gave it:

```http
HTTP/1.1 403 Forbidden
legion-denied-reason: the free tier is read-only
legion-denied-by: policy/agents-free-tier#0
```

## Stages

A plugin declares the stages it acts at. Each stage is a proxy-wasm
callback:

| Stage | Callback | The plugin sees | The plugin can |
| --- | --- | --- | --- |
| `connect` | `on_new_connection` | the flow's facts, as properties | refuse the connection (`close_downstream`, or `Pause`) |
| `request` | `on_http_request_headers` | `:method`, `:path`, `:authority`, `:scheme`, and the headers | answer instead of the upstream (`send_http_response`), rewrite `:path`, or add, set, or remove headers |
| `body` | `on_http_request_body` | the whole request body, buffered up to `limits.maxBodyBytes` | answer, or rewrite the body |
| `response` | `on_http_response_headers`, then `on_http_response_body` per chunk | `:status` and the headers, then the body as it streams | rewrite the status and headers, replace the response, or rewrite the body. Returning `Pause` from a body chunk holds it until the next chunk, up to `maxBodyBytes` |
| `done` | `on_log` | the whole exchange, with `response.code` | count, log |

The stages run on these paths:

- **An L4 path** (a `tcp` chain, an upstream that does not parse HTTP) runs
  only `connect`.
- **An HTTP path** (an HTTP-mode upstream, the gated proxy's absolute-URI
  requests, HTTP-mode ingress) runs `request`, `body`, `response`, and
  `done`.
- **No `connect` on HTTP paths.** An HTTP path does not run `connect`.
- **One stream context per HTTP exchange.** The same context lives from the
  request through `done`, so state a plugin keeps on its context carries
  across the stages.

A reference to a plugin whose stages none of its path runs is refused at
submit. One example is a `request`-only plugin on a `tcp` chain:

```
policy 'p' egress.handlers[0]: plugin 'read-only-free-tier' acts at stage(s) request,
none of which run here: egress.protocol is "tcp". Set protocol = "http" for a
plugin that acts on requests, or declare stage "connect"
```

A plugin that reads the response body can change its length. The response
then goes out chunked, without `content-length`. Streamed responses, such as
server-sent events, pass through one chunk at a time: a plugin sees and
forwards each event as it arrives.

## Facts are properties

The plugin reads facts about the flow with the SDK's `get_property`:

| Property | Value |
| --- | --- |
| `legion.job`, `legion.task`, `legion.group` | the calling task (egress) or the receiving one (ingress) |
| `legion.identity` | its workload identity, `<region>:<job>:<group>:<task>` |
| `legion.labels.<key>` | its `network.labels` |
| `legion.allocation` | the allocation id |
| `legion.direction` | `egress` or `ingress` |
| `legion.protocol` | `tcp` or `http` |
| `legion.destination` | `host:port`: the egress destination, or the ingress peer |
| `legion.upstream` | the upstream key, on an upstream's path |
| `legion.peer_identity` | the verified caller's identity, on mesh ingress |
| `legion.node`, `legion.plugin`, `legion.plugin_revision`, `legion.scope` | where it runs |
| `request.method`, `request.path`, `request.host`, `request.scheme` | the request head |
| `response.code` | the status, from the response stage on, as an 8-byte little-endian integer |
| `plugin_name` | the plugin's name |

Values are UTF-8 text except `response.code`.

The `legion.*` facts are read-only. A plugin may set its own properties
under `legion.tags.*`, or under any other path. Those last for the stream
context, so a `connect` or `request` callback can tag a flow for later
stages.

## What a plugin cannot reach

- **Credentials it signs with.**
  - **Injection runs after plugins.** Credential injection
    ([`connect.upstreams[].credentials`](network-handlers.md#credential-injection))
    happens after the plugins have run, so no plugin sees an injected value.
  - **Keys stay with netd.** A plugin may sign with a key its object
    declares under `credentials` by calling the `legion_sign` foreign
    function ([Signing](#signing-without-the-key)). netd holds the key, and
    the module only gets the signature.
- **Sockets and the filesystem.** There are no WASI sockets and no
  filesystem: no preopened directories, no environment, no arguments.
  `dispatch_http_call` reaches only the `targets` the object declares, and
  the calling allocation's egress policy and the host guard judge each call
  like any other flow.
- **Other tenants.** Instances, shared data (`get_shared_data`), and metrics
  are scoped to the reference:
  - **A policy object's reference** runs under the plugin's own scope
    (`policy`).
  - **A job's own reference** runs under that job (`job:<id>`), from its
    `network.egress` / `network.ingress` handlers or its upstreams'
    `handlers`.
  - **No crossing.** One job's plugin never reads another job's data or
    counters.

## Plugin objects

A plugin is stored like a [policy object](network-handlers.md#policy-objects),
in the replicated store, and applied with the CLI. What it is, which stages
it runs at, and who may use it live on the object. A reference only names it
and gives its `config`.

```nickel
{
    name = "llm-token-meter",
    description = "Meters LLM tokens per job",
    module = {
        source = "https://artifacts.example.com/llm-token-meter.wasm",
        checksum = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
    },
    stages = ["request", "response", "done"],
    usableBy = "jobs",                    # policies (default) | jobs
    nodePools = ["gpu-*"],                # where it may run; empty = every pool
    limits = { memory = "16Mi", deadline = "10ms", maxBodyBytes = "1Mi" },
    configSchema = {                      # JSON Schema every reference's config must satisfy
        type = "object",
        required = ["budget"],
        properties = { budget = { type = "integer" }, per = { type = "string" } },
    },
    targets = { quota = "http://quota.internal:8080" },   # dispatch_http_call targets
    credentials = { openai = { kv = "secrets/openai/hmac" } },  # legion_sign keys
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `name` | (required) | letters, digits, `-`, `_`, `.` |
| `module.source` | (required) | `https://`, `http://`, `s3://`, or `file://` URL of one `.wasm` file |
| `module.checksum` | (required) | `sha256:<hex>` of the module |
| `stages` | (required) | one or more of `connect`, `request`, `body`, `response`, `done` |
| `usableBy` | `policies` | `policies`: only stored policy objects may reference it. `jobs`: a job may reference it from its own chains and upstreams too, and policies still may |
| `nodePools` | every pool | node pool globs the plugin may run on. A policy that references a pool-scoped plugin must select pools within them |
| `limits.memory` | `16Mi` | the most linear memory an instance may grow to (64 KiB to 256 MiB) |
| `limits.deadline` | `10ms` | how long one callback may run (at most `1s`) |
| `limits.maxBodyBytes` | `1Mi` | the most of a body the plugin buffers (at most 64 MiB) |
| `configSchema` | any config | a JSON Schema every reference's `config` is checked against at submit |
| `targets` | none | `dispatch_http_call` targets by name, each `http(s)://host[:port]`. At most 16 |
| `credentials` | none | `legion_sign` keys by name, each `{ kv = "<path>" }`. At most 16 |

A reference:

```nickel
{ plugin = { name = "llm-token-meter", config = { budget = 100000, per = "1h" } }, onError = "deny", timeout = "2s" }
```

The plugin reads `config` as its plugin configuration (the JSON text) with
`get_plugin_configuration`. The submit-time checks are:

- the plugin exists
- the referencing side may use it (a job may reference only a `usableBy = "jobs"` plugin)
- its path runs at least one of its stages
- `config` satisfies `configSchema`

```
task main network.egress.handlers[0]: config does not match plugin 'llm-token-meter'
configSchema: /budget: "lots" is not of type "integer"
```

Applying a plugin re-checks every stored policy that uses it. A new schema
the policies' configs no longer satisfy, or a dropped stage they rely on, is
refused there and not discovered on the datapath. A plugin a policy still
references cannot be deleted.

Jobs already submitted are not re-checked when a plugin changes. If the new
version no longer suits a job, its hops keep running with the config the job
gave, or fail with `not on this node` after a delete.

### How a plugin reaches a node

A node fetches a plugin object only when one of its allocations references
the plugin. The object then reaches the node as datapath material
(`FetchDatapathMaterial`, kind `plugin`), versioned by its revision. Every
write takes a new cluster-wide revision, even across a delete and a
re-create.

The module is fetched by the node's [artifact fetchers](artifacts.md):
- It is checked against the checksum and kept in the node content store.
- It is compiled once per checksum, so a thousand allocations that use one
  plugin cost one fetch and one compile.
- The worker's `artifacts.fetch.allowedSchemes` applies, and so do
  credentials for private buckets.

A new revision replaces the node's instances for the next request, while
in-flight requests finish on the instances they hold. Shared data and
module metrics carry across revisions on a node, so a meter's counts survive
an update.

## Failure

A failed callback is a handler error and follows the hop's `onError`, which
defaults to `deny`. These are failures:
- a trap (a panic, `unreachable`, an out-of-bounds access)
- exhausted fuel
- a callback past its deadline
- a module over its memory limit
- a module that exits
- a module that will not start (refuses its configuration, or fails to
  instantiate)
- a plugin whose module has not loaded on the node yet
- a `Pause` nothing resumes

The client gets a `503` that names the hop and the failure:

```http
HTTP/1.1 503 Service Unavailable
legion-denied-by: policy/meter#0
legion-denied-reason: plugin llm-token-meter exceeded its 10ms deadline (12931 µs)
content-type: application/json

{"legion":{"error":"plugin llm-token-meter exceeded its 10ms deadline (12931 µs)","plugin":"llm-token-meter","source":"policy/meter#0"}}
```

With `onError = "allow"` the request goes on as if the hop were not there.

A trap, exhausted fuel, a memory limit, or an exit breaks the instance:
- The broken instance is not called again.
- The next request starts a fresh one.
- Requests whose stream contexts lived in the broken instance fail their
  remaining callbacks, by `onError`.
- A callback that only runs past its deadline does not break its instance,
  because its state is consistent.

**Deadlines and fuel.** wasmi has no clock-driven interruption, so the
deadline is enforced in two ways:
- **Fuel.** A callback gets fuel in proportion to its deadline (5 million
  units per millisecond). A loop that never returns stops when the fuel
  runs out: within about twice the deadline on a 2.1 GHz Xeon core, and
  sooner on faster cores.
- **The clock.** A callback that returns after its deadline counts as
  failed.

Starting an instance (the module's initializer and the root context's
`on_vm_start` and `on_configure`) runs on ten callbacks' worth of fuel and
is not held to the deadline.

The response stage can fail after the upstream has answered:
- **Response head.** A failure there becomes the `503`.
- **Response body, mid-stream.** A failure there aborts the response, so the
  client sees a truncated body and a reset connection, never a silently
  shortened one.

## Status and diagnostics

Every heartbeat reports each plugin a node holds:
- the revision
- the module state (`fetching`, `ready`, or `failed` and why)
- calls and errors
- the last error
- the p99 callback time over the last 1024 callbacks
- the live instances

```console
$ legion net plugin status llm-token-meter
llm-token-meter revision 7 (sha256:9f86d081884c)
┌────────┬──────────┬───────┬────────┬────────┬───────┬───────────┬────────────┐
│ NODE   │ REVISION │ STATE │ CALLS  │ ERRORS │ P99   │ INSTANCES │ LAST ERROR │
├────────┼──────────┼───────┼────────┼────────┼───────┼───────────┼────────────┤
│ node-a │ 7        │ ready │ 120331 │ 0      │ 38 µs │ 4         │            │
└────────┴──────────┴───────┴────────┴────────┴───────┴───────────┴────────────┘
```

Each plugin hop also reports its health per allocation, like `authz` and
`proxy` hops. `legion net doctor` and the UI's task Network section show it
with the other handlers.

Metrics on the worker registry:
- `legion_net_plugin_calls_total{plugin,stage,outcome}`
- `legion_net_plugin_callback_seconds`
- `legion_net_plugin_metric{plugin,scope,name}`: the current value of each
  metric a module defines with `define_metric`, at most 64 per scope.

Module log lines (`proxy_log`, and anything the module writes to standard
output or error) go to the worker log under the target `legion::net_plugin`,
at their own level. Each line carries the plugin, the scope, and the
context. Past 100 lines per second per plugin the rest are dropped and
counted.

The events are `net.plugin_applied` and `net.plugin_deleted`.

## Signing without the key

A plugin that declares `credentials` can sign with them through the
proxy-wasm foreign-function call. The key is a KV value that netd holds in
memory. The module never reads it:

```rust
let args = r#"{"credential":"openai","algorithm":"hmac-sha256","data":"POST\n/v1/chat"}"#;
let mac = proxy_wasm::hostcalls::call_foreign_function("legion_sign", Some(args.as_bytes()))?;
```

| `algorithm` | Arguments | Returns |
| --- | --- | --- |
| `hmac-sha256` | `data` (text) or `dataBase64` | the 32-byte MAC |
| `aws4-hmac-sha256` | `date` (`YYYYMMDD`), `region`, `service`, `stringToSign` | the AWS Signature Version 4 signature, hex: the signing key is derived from the secret, the date, the region, and the service |

A refused signing call returns `BadArgument` and logs why. Causes are a
credential the object does not declare, one that has not reached the node
yet, a bad argument, or an unknown algorithm.

Applying a plugin that names a `credentials` KV path needs `kv:read` on that
path. Every policy that uses the plugin can then sign with the key, which is
the point. A plugin's module can use the key as a signing oracle but cannot
exfiltrate it.

## Calling out

`dispatch_http_call` sends a request to one of the object's `targets`, by
name:

```rust
self.dispatch_http_call("quota", vec![(":method", "GET"), (":path", "/check")], None, vec![], Duration::from_secs(1))?;
Action::Pause   // resumed from on_http_call_response
```

The call:
- goes to the declared scheme and authority, with the module's `:path`
- is judged by the calling allocation's egress chain, at connect time, like
  any flow the task could make
- passes the [host guard](network-handlers.md#the-host-guard), and dials
  the address the guard checked

`on_http_call_response` gets the answer. A failed call comes back as a
`503` answer with the error as the body. A callback may make up to 8 calls,
and a request may go 8 rounds of calls. The module resumes the request with
`resume_http_request` or answers it with `send_http_response`.

## ABI coverage

Implemented:
- Logging, time, buffers, and request and response header maps.
- Properties, shared data (with compare-and-swap), and local responses.
- Stream continue and close, HTTP calls, foreign functions (`legion_sign`),
  and metrics.

Request and response trailers are not exposed: those maps read as not
found.

`set_tick_period` is accepted, but ticks never fire. A module that sets a
tick when it starts still loads, and nothing runs on a timer inside the
datapath.

These calls return `Unimplemented`:
- shared queues
- gRPC calls and streams
- foreign functions other than `legion_sign`

An SDK that unwraps one of those results traps, which fails that callback by
`onError`. The Rust SDK unwraps for shared queues.

Any other import a module declares is stubbed so the module instantiates.
Calling an unknown `env` function returns `Unimplemented`, and an unknown
WASI function returns `ENOSYS`. A call into an import from another module
traps.

WASI (`wasm32-wasip1`):
- **Stubbed:**
  - `fd_write` to standard output and error goes to the plugin log; to any
    other descriptor it returns `EBADF`.
  - The clocks, randomness, and `proc_exit`.
- **Empty:** the environment and the arguments.
- **Missing:** every other WASI call, including the whole filesystem,
  returns `ENOSYS`.

The engine is [wasmi](https://github.com/wasmi-labs/wasmi), an interpreter,
chosen over a compiler such as wasmtime for binary size: it adds about
1.8 MB to the worker, against about 10 MB. A compute-heavy plugin pays for
that in callback time. A header-stage plugin costs tens of microseconds
(see [Cost](#cost)).

## Cost

The `net_bench` rows below were measured on a 4-vCPU 2.1 GHz Xeon guest.
The plugin is the conformance module's no-op mode: its callbacks do nothing,
so the rows measure what the host adds.

`plugin_exchange` runs the host in-process, with no network:

| Row | Time |
| --- | --- |
| one `on_http_request_headers` callback on a live context | 0.44–0.47 µs |
| a stream context's life (`on_context_create`, `on_done`, `on_delete`) | 2.2–2.3 µs |
| a whole `request`-stage exchange: start the context, run the callback, end it | 6.4–6.9 µs |

`http_rr`: HTTP/1.1 GETs on one kept-open connection through an HTTP-mode
upstream:

| Row | Run 1 | Run 2 |
| --- | --- | --- |
| `http_mode`, no plugin | 129.3 µs | 126.4 µs |
| a plugin with no HTTP stage (the exchange runs, no callback does) | 129.7 µs | 145.3 µs |
| a no-op plugin at `request` | 156.0 µs | 196.3 µs |
| a no-op plugin at `request`, `response`, and `done` | 251.3 µs | 233.0 µs |

A stage the plugin does not declare costs nothing. The host's own work, the
in-process row, is under 7 µs per request. On the real path, a `request`
callback averages about 1.2 µs, but the `request` row adds 25–70 µs, more
than the in-process exchange accounts for. That difference is not yet
attributed, and the second run was noisy. The `response` stage costs the
most: it runs a callback per body chunk and sends the body chunked.

## CLI and MCP

```bash
legion net plugin new <name> [--lang rust] [--dir <dir>]        # scaffold a crate and its object file
legion net plugin test <module.wasm|name> --request 'METHOD URL' \
    [--header k=v] [--body <text|@file>] [--label k=v] [--job <id>] \
    [--config <json|@file>] [--object <file.ncl>] [--stages s1,s2] \
    [--response <status>] [--response-body <text|@file>]
legion net plugin apply -f <file.ncl|file.json> [--dry-run] [--idempotency-key <key>]
legion net plugin list
legion net plugin describe <name>
legion net plugin status <name>
legion net plugin delete <name>
```

Every verb takes `--json`/`-j`. `test` runs locally, with no cluster:
- **Inputs.** It takes a `.wasm` file, or a stored plugin's module
  (`file://` and `http(s)://` sources), checked against the checksum.
- **Runs.** It sends one synthetic request through the same pipeline the
  datapath uses. The response stage sees a synthetic upstream answer
  (`--response`, default `200`).
- **Reports.** Per stage, it gives what the plugin did, what it changed,
  and its callback time and fuel. The time does not include starting the
  instance.
- **Defaults.** A bare `.wasm` runs at `request`, `body`, `response`, and
  `done` unless `--stages` or `--object` says otherwise.

`test` and `apply --dry-run` are read-only.

The MCP tools are:
- **Read-only:** `legion_net_plugin_test`, `legion_net_plugin_list`,
  `legion_net_plugin_describe`, and `legion_net_plugin_status`.
- **Mutating:** `legion_net_plugin_apply`.

The RPCs are `UpsertNetPlugin` (with an `idempotency_key`), `GetNetPlugin`,
`ListNetPlugins`, `DeleteNetPlugin`, and `GetNetPluginStatus`
([`proto/legion.proto`](../proto/legion.proto)).

## ACLs

- `net:read` lists, describes, and reads the status of plugins.
- `net:write` applies and deletes them:
  - **Pool-scoped plugins.** A `usableBy = "policies"` plugin with
    `nodePools` needs `net:write` on each pool.
  - **Cluster-wide plugins.** One without pools, and every
    `usableBy = "jobs"` plugin (which every job may reference, so it is
    cluster-wide by nature), needs unscoped `net:write`.
- Naming a `credentials` KV path needs `kv:read` on it.

Referencing a plugin needs no extra right beyond writing the policy or
submitting the job. The plugin's `usableBy` and `nodePools` decide where it
may be referenced.

## Known limitations

- **Modules are single `.wasm` files over `https://`, `http://`, `s3://`, or
  `file://`.** `oci://` modules (a WASM module published as an OCI artifact)
  are not supported yet, because the node content store caches single files
  only.
- **Request bodies are buffered, not streamed.** The `body` stage sees the
  whole body (up to `maxBodyBytes`) in one callback, with
  `end_of_stream = true`. Response bodies stream.
- **A plugin cannot change where a request goes.** It may rewrite `:path`
  and headers, but a change to `:method`, `:authority`, or `:scheme` is
  ignored: those are the flow's facts, already judged by policy. A
  rewritten `host` header is replaced by the upstream's own name on an
  upstream path.
- **No HTTP/2, HTTP/3, or gRPC.** The datapath speaks HTTP/1.1. There are no
  gRPC calls from a plugin.
- **No ticks or shared queues**, so a plugin cannot run work on a timer or
  between instances (a tick is accepted and never fires). Shared data is per node, not cluster-wide. A
  cross-node budget needs a service the plugin calls with
  `dispatch_http_call`.
- **The `connect` stage sees facts, not bytes.** On an L4 path a plugin
  decides once when the flow opens. It gets no data callbacks.
- **Callbacks run on the datapath's threads.** A plugin's work is bounded by
  its deadline and fuel, but it is not preempted mid-callback.
- **Not re-checked on change.** Jobs submitted before a plugin changes are
  not re-checked against its new schema or stages.
