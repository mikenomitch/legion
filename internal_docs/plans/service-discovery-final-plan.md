# Service Discovery: Final Plan (v1 implementation + mesh follow-on)

> **Judgement calls:** [`calls/service-discovery-final-plan_calls.md`](../calls/service-discovery-final-plan_calls.md)

> **This is the rectified, authoritative plan.** It consolidates and supersedes the
> design in [service-discovery-plan.md](service-discovery-plan.md) (kept for rationale
> and the full mesh design), the [config cookbook / mesh extensions](service-discovery-mesh-and-config.md),
> and the [self-critique](service-discovery-critique.md). Where those documents
> disagree, this one wins. It adds a third research pass on **mesh dev-ex and
> ergonomics**, locks the v1 scope, and specifies implementation as concrete,
> independently verifiable steps (each with automated tests *and* manual verification),
> sized so steps can be delegated to subagents/workflows.

---

## 1. What the dev-ex research adds (and how it changed the plan)

A dedicated research pass on mesh developer experience produced three load-bearing
conclusions:

**a) Complexity is the adoption killer — measured, not vibes.** CNCF's 2022 mesh
micro-survey: top challenges were *shortage of engineering expertise* (47%), *complexity*
(41%), *lack of guidance* (36%). By the 2024 annual survey, mesh production use had
*fallen* 50%→42% with 100% more "no near-term plans," attributed by CNCF itself to
"complexity, cost, and performance… significant operational overhead"
([CNCF 2022](https://www.cncf.io/blog/2022/05/17/service-meshes-are-on-the-rise-but-greater-understanding-and-experience-are-required/),
[CNCF 2024](https://www.cncf.io/wp-content/uploads/2025/04/cncf_annual_survey24_031225a.pdf)).

**b) The named dev-ex sins are specific and avoidable.** The ones this plan designs out:
- **Config-ordering footguns**: Istio officially documents that applying a
  VirtualService before its DestinationRule yields 503s until you apply them in the
  right order; a DestinationRule can *instantly break working traffic* via its mTLS-mode
  default ([istio.io common-problems](https://istio.io/latest/docs/ops/common-problems/network-issues/)).
  → Legion: **one block, one document** — `network`/`service` live on the task; there is
  nothing to apply out of order, and submit-time validation rejects dangling port refs.
- **Silent failures**: subsets matching zero endpoints 503 silently; EnvoyFilter inserts
  that miss are "silently ignored… nothing in the error log"
  ([istio#37415](https://github.com/istio/istio/issues/37415)).
  → Legion: validation errors at submit; catalog shows `0/N healthy` loudly; no config
  is accepted that references something that doesn't exist.
- **Opaque debugging chains**: the canonical K8s "Debug Services" doc is a ~10-step
  manual ladder across DNS/EndpointSlices/kube-proxy; the classic Istio 503 hunt took
  metrics → Envoy stats → debug logs → packet captures to find a keepalive default
  ([Karl Stoney](https://karlstoney.com/istio-503s-ucs-and-tcp-fun-times/)).
  → Legion: **`legion service doctor`** (see §4) — one command that walks the layers
  *for* you and names the failing one, modeled on `linkerd check`'s praised
  verdict + cause + remediation contract and Tailscale's "name the mechanism" pattern.
- **Sidecar lifecycle pain**: jobs that never complete, app-before-proxy startup races,
  pod restarts on mesh upgrade ([istio ambient rationale](https://istio.io/latest/blog/2022/introducing-ambient-mesh/)).
  → Legion: already designed out — per-node forwarder, never a per-task sidecar.

**c) The ergonomic moves worth copying are converging industry-wide.**
Persona-split config (Gateway API), progressive disclosure (ambient's
no-mesh → L4 → L7 per namespace), "adoption = a label," and single-command diagnostics.
Legion's versions: progressive disclosure in the jobspec grammar (one-liner → blocks),
adoption = a `service:` key, and `doctor`/`resolve` CLI verbs.

**Rectifications locked in from the critique doc** (unchanged, restated as binding):
AP-read/CP-write; liveness-as-lease; self-preservation on correlated failure;
deregister-before-stop; no-UDP-over-NAT DNS + TCP fallback; coalesce/splay/fail-static
templates; per-resource partial apply; fail-open locality; hard v1 line; no per-task
sidecar; datapath as plugin with CNI shim.

---

## 2. Final scope

**v1 — Service Discovery (implemented by this plan):**
- `network` block: named ports, static + dynamic (20000–32000), assigned at placement.
- `service` block with the **int/`'dynamic'` port shorthand** (one-liner onboarding).
- `LEGION_*` env vars for a task's own ports/identity.
- **Catalog as projection**: services derived from `task_config × allocation ×
  allocation_health × node` — no registration RPC, no drift, only-healthy.
- Manager RPCs `ListServices` / `ResolveService`; CLI `legion service list | resolve |
  doctor`.
- Works identically on Linux and macOS (host datapath; no netns required).

**v2 — Mesh (planned, not implemented now):** node-local DNS + template functions
(Phase 3 of the core plan), datapath plugin ABI + bridge/CNI shim, per-node L4
forwarder + mTLS + intentions, L7 waypoint, WASM middleware, observability service
graph. The full design lives in the superseded docs and remains the blueprint; §8
summarizes the adoption path.

**Explicit non-goals for v1:** DNS resolver (v2), `connect.upstreams` (v2, needs the
forwarder), intentions (v2, needs workload identity + forwarder), cross-service env
injection (never — the K8s env-var E2BIG lesson), drain-before-SIGTERM (v2, rollout
integration).

---

## 3. Grounded integration map (from code recon)

| Change | Exact location | Mechanism |
| --- | --- | --- |
| Port/service types | new `src/structs/network.rs` | `NetworkConfig`, `PortConfig`, `ServiceConfig`, `AllocNetwork`, assignment + env helpers |
| Jobspec fields | `TaskConfig` ([src/structs/job.rs:815](../../src/structs/job.rs)) | `network: Option<NetworkConfig>`, `service: Option<ServiceConfig>` |
| Sugar + validation | `JobConfig` normalize + `validate_job_config` ([src/task_plugins/mod.rs:185](../../src/task_plugins/mod.rs)); called at [src/manager/server.rs:847](../../src/manager/server.rs) | int/`'dynamic'` `service.port` materializes a `network.ports` entry before storage |
| Schema | [src/storage/migrations.rs](../../src/storage/migrations.rs) | `task_config` +`network`,+`service`; `allocation` +`network`; index `allocation(node_id)` |
| Query consts | [src/storage/queries.rs](../../src/storage/queries.rs) | extend `TASK_CONFIG_*` and `ALLOCATION_*` attr lists |
| Port assignment | `Store::insert_allocation` ([src/storage/store.rs:2647](../../src/storage/store.rs)) | in-transaction: read task network + used ports on node → assign → persist on allocation row. Static conflict = placement error |
| Node address | `node.config_attrs.workerAddress` (set at [src/worker/agent.rs:118](../../src/worker/agent.rs)) | endpoint IP = host of workerAddress; resolved at insert, stored in `AllocNetwork.address` |
| Wire to worker | `TaskAllocation` proto +`network` field; `build_task_allocation` ([src/manager/dispatcher.rs:112](../../src/manager/dispatcher.rs)) | covers both PostWork and HeartbeatReply paths (shared builder) |
| Env injection | `exec.rs` `start()` (`cmd.envs`), `container.rs` `build_launch` (`--env`) | shared `allocation_env()` helper; user env wins on conflict |
| Catalog queries | new fns in `store.rs` | one JOIN over `task_config`(service not null) × `allocation` × `allocation_health` × `node` |
| RPCs | `proto/legion.proto` `service Manager`; handlers in `src/manager/server.rs` | `ListServices`, `ResolveService` (documented per AGENTS.md proto rule) |
| CLI | new `src/cmd/service.rs`, wired in `src/main.rs` + `src/cmd/mod.rs` | `list`, `resolve`, `doctor` |
| Health feed | heartbeat → `upsert_allocation_health_snapshots` ([src/manager/server.rs:2203](../../src/manager/server.rs)) | already exists — the projection reads it; tasks without checks report healthy |

Liveness-as-lease note: node heartbeats already gate node status; v1 `resolve` excludes
endpoints on nodes whose status is not ready/alive — endpoint life is bounded by the
node lease with no new machinery.

---

## 4. `legion service doctor` — the debugging contract

The research's clearest gap: nothing walks the whole path and names the failing layer.
Legion owns every layer, so it can. v1 checks, in order, stopping at the first failure
with **verdict + cause + fix hint** (the `linkerd check` contract):

```
$ legion service doctor redis
✓ service 'redis' is declared            (job 'redis-job', task 'redis', port label 'redis')
✓ 1 allocation placed                    (alloc 42 on node mac-1)
✓ node mac-1 alive                       (heartbeat 2s ago)
✓ port assigned                          (host port 6379, static)
✗ health checks failing                  (alloc 42: "tcp connect refused", 3 consecutive)
  → the task process may not be listening on 6379 yet; check task logs:
    legion job logs redis-job
  → or loosen the check: checks[0].every / unhealthy_after_count
```

Layers: declared? → placed? → node alive? → port assigned? → checks passing? →
(when reachable from the manager) TCP dial. Each maps to one projection query — cheap,
no probes deployed, no YAML spelunking.

---

## 5. Implementation steps (concrete, verifiable, delegable)

Rules: every step compiles green (`cargo build`), passes `cargo fmt --all --check`,
`cargo clippy --all -- -D warnings`, `cargo test --all --locked` before the next
begins. Steps S1–S3 are strictly sequential (shared files: proto, job.rs, store.rs).
S4a/S4b/S4c are independent once S3 lands and are the natural **subagent fan-out
points**; the verification matrix in §7 is another (parallel manual-verify agents on
Linux and macOS). A workflow can pipeline: implement-step → review-step per item.

### S1 — Types, sugar, validation (pure, no I/O)
- `src/structs/network.rs`: `NetworkConfig { mode, ports }`, `PortConfig { name,
  static, to }`, `ServiceConfig { name?, port: int|label, tags, meta }`,
  `AllocNetwork { address?, ports: [{name, host_port, to}] }`;
  `assign_ports(network, used: &HashSet<u16>) -> R<Vec<AllocPort>>`;
  `allocation_env(alloc, task) -> Vec<(String,String)>` with label sanitization.
- `TaskConfig` gains `network`/`service`; `JobConfig::normalize()` applies the port
  shorthand; validation: unique labels, static∈1..=65535, dynamic range excluded for
  statics, `service.port` label must resolve, sugar/explicit conflicts rejected.
- **Automated:** unit tests — sugar desugars exactly; label collision/dangling-ref
  errors name the field; assignment skips used ports, bails on static conflict with the
  port number and node in the message; env labels sanitize (`http-2` → `HTTP_2`).
- **Manual:** `cargo run -- job run configs/jobs/service-demo.jsonnet` against a spec
  with a dangling `service.port` label → submit is rejected with the field name.

### S2 — Storage: migrations, columns, port assignment at placement
- Migrations: `task_config` +`network` +`service` (nullable JSONB); `allocation`
  +`network`; `CREATE INDEX idx_allocation_node_id ON allocation(node_id)`.
- `queries.rs` consts extended; `task_config_from_row`/insert params;
  `allocation_from_row`; `insert_allocation` assigns ports in-tx (used-ports query by
  node), resolves node address from `workerAddress`, persists `AllocNetwork`.
  In-place config migration (`migrate_allocations_to_job_config`) **preserves** the old
  allocation's network verbatim.
- **Automated:** store tests — two allocs of a 2-count dynamic-port task on one node get
  distinct ports; static conflict on same node fails with clear error; in-place migrate
  keeps ports; migration runs on an existing DB file.
- **Manual:** run manager, submit a `count: 2` job, `legion state` /
  `sqlite3 … 'SELECT id, network FROM allocation'` shows two distinct host ports.

### S3 — Wire + env: proto, dispatcher, worker, task plugins
- `TaskAllocation` +`string network = 11` (documented); `build_task_allocation`
  serializes it (covers PostWork + heartbeat reconcile); `task_from_allocation` parses
  it back onto `Allocation`.
- `exec`/`container` inject `LEGION_ALLOC_ID`, `LEGION_JOB_ID`, `LEGION_TASK_NAME`,
  and per-port `LEGION_PORT_<L>` / `LEGION_ADDR_<L>` / `LEGION_IP_<L>` /
  `LEGION_PORT_TO_<L>`; user-specified env wins on key conflict.
- **Automated:** env-construction unit tests; round-trip test
  (alloc.network → TaskAllocation → alloc.network); container `build_launch` includes
  `--env LEGION_PORT_…`.
- **Manual (the money demo):** job whose command is
  `python3 -m http.server $LEGION_PORT_http` with a dynamic port + tcp check → task
  starts, binds the assigned port, `curl <node>:<port>` serves; on restart it gets a
  fresh port and keeps working.

### S4a — Catalog projection + RPCs
- `store.list_services()` / `store.resolve_service(name, include_unhealthy)`; healthy =
  `allocation_health.healthy` AND node alive; missing health row = registering (not
  healthy). Proto RPCs + reply messages, fully documented; server handlers.
- **Automated:** projection tests — endpoint appears only with healthy row; flipping
  healthy→false removes it from default resolve; removing the alloc removes it; node
  marked lost removes it (lease).
- **Manual:** kill the demo task's process on the worker → within one check interval
  `legion service resolve web` returns no endpoints; restart → it returns.

### S4b — CLI (`legion service list | resolve | doctor`)
- `list`: NAME | JOB | HEALTHY/TOTAL | TAGS. `resolve`: table or `-j` JSON of
  `address:port` endpoints (the scriptable output other tasks can consume today).
  `doctor`: §4 ladder.
- **Automated:** handler tests against a seeded store (existing cmd test pattern).
- **Manual:** full walkthrough in §7.

### S4c — Spec surface + docs
- `configs/jobs/maximal.jsonnet` gains `network` + `service` (per AGENTS.md);
  new `configs/jobs/service-demo.jsonnet` (server + client);
  `docs/service-discovery.md` (user-facing: blocks, env vars, CLI, doctor, macOS
  notes); README doc list; TODO checkboxes.

### S5 — Ship gate
- fmt/clippy/tests green; §7 matrix executed on Linux (this environment); macOS steps
  documented for a Mac checkout; push to PR #123.

---

## 6. Performance at scale (v1 budget + design headroom)

v1 hot paths and their costs, with the scale story for each:

| Path | Frequency | v1 cost | At-scale plan |
| --- | --- | --- | --- |
| Port assignment | per placement | one indexed query (`allocation WHERE node_id=?`, new index) + O(range) worst-case scan | fine to ~10k allocs/node-range; if needed, per-node used-port bitmap cached in the existing alloc cache |
| Catalog resolve | per CLI/API call | one 4-table JOIN, indexed on `service IS NOT NULL` tasks (small) + `allocation(task_config_id)` (existing) | v2 replaces polling with **push deltas over the worker stream**, sharded per service (EndpointSlice lesson) — resolve becomes a node-local read |
| Health ingest | per heartbeat | already existing upsert; projection adds zero writes | unchanged — the projection *reads*; no new write amplification |
| Env injection | per task start | O(ports) string builds | trivially constant; **never** O(services) (the K8s E2BIG postmortem is a hard non-goal) |
| DNS (v2) | per lookup | node-local, last-known-good, no UDP-over-NAT | scales with node count, not cluster size; central manager is off the hot path |

Two structural guarantees carried from the critique: discovery reads never scale
O(services × consumers) in any per-consumer payload, and no per-connection O(n)
rule chains exist anywhere (nothing iptables-shaped in the design).

Benchmark hooks (follow-up, `benches/`): port assignment at 1k/10k existing allocs per
node; `resolve_service` latency with 10k services × 10 endpoints seeded.

---

## 7. Manual verification playbook (run after implementation)

Linux (this repo's dev environment) — copy-paste sequence:

```bash
# terminal 1: manager
cargo run -- start manager -c configs/manager.jsonnet
# terminal 2: worker (token from manager output)
cargo run -- join <TOKEN>
# terminal 3:
cargo run -- job run configs/jobs/service-demo.jsonnet

# V1 env vars: task got its port and bound it
cargo run -- service resolve web -j        # → [{"address":"127.0.0.1","port":2XXXX,...}]
curl "$(cargo run -q -- service resolve web -j | jq -r '.[0] | "\(.address):\(.port)"')"
# → HTTP 200 directory listing from python http.server

# V2 only-healthy: kill the server process (not via legion)
pkill -f "http.server" ; sleep 12
cargo run -- service resolve web           # → no healthy endpoints
cargo run -- service doctor web            # → ✗ health checks failing + hint
# task restarts per on_unhealthy → endpoint returns with a NEW port
cargo run -- service resolve web -j

# V3 static conflict: submit two static-6379 tasks forced to one node
cargo run -- job run configs/jobs/service-conflict-demo.jsonnet
# → second allocation fails with "port 6379 already in use on node <id>"

# V4 catalog list + doctor happy path
cargo run -- service list                  # web 1/1 healthy
cargo run -- service doctor web            # all ✓
```

macOS: identical commands from a Mac checkout (worker joins over LAN or same-host);
expected differences: none for exec tasks — this is the "works on Mac" claim, verified
by the same script. Failure of any step is a release blocker for the claim it verifies.

---

## 8. Mesh follow-on (kept as plan)

Adoption path stays exactly as designed post-critique, now DX-annotated:

1. **DNS + templates** (v2 first): the no-SDK consumption path; no-NAT resolver, TCP
   fallback, `ndots:1`, coalesce/splay/fail-static. *DX: name-is-config, docker-compose
   feel.*
2. **Datapath plugin + bridge/CNI shim**: isolation on Linux, `mode: 'auto'`. *DX:
   nothing to learn until you want isolation.*
3. **L4 forwarder + mTLS + intentions** (needs WI): `connect.upstreams` localhost
   ports; per-node, never per-task — no sidecar lifecycle class of bugs; SLO'd,
   zero-drop reloads. *DX: adoption = one block; `doctor` learns to name intention
   denials (the Hubble "which policy denied it" lesson).*
4. **L7 waypoint** then **WASM middleware**: opt-in, bounded, fail-policied.
5. **Observability**: RED metrics + service graph from the data path Legion already
   owns; `doctor` gains live-traffic evidence.

The mesh ships only after v1 discovery has proven the catalog/UX core — the reverse of
the industry's mistake (mesh-first, discovery ergonomics never).

---

## 9. Acceptance criteria (v1)

- One-liner spec (`service: { port: N }`) yields: assigned port, `LEGION_*` env, catalog
  entry, `resolve` output — on Linux and macOS, no extra components.
- Only-healthy is observable end-to-end: kill → disappears within one check interval;
  recover → reappears.
- Static port conflicts fail loudly at placement with node + port in the error.
- `legion service doctor` names the failing layer with a fix hint for: undeclared,
  unplaced, dead-node, failing-checks cases.
- Dangling/conflicting spec refs are rejected at submit with the field name.
- `cargo fmt`/`clippy -D warnings`/`test --all --locked` green; jobs with no
  network/service block untouched (regression suite).
