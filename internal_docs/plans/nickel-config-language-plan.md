# Jsonnet → Nickel Configuration Language Migration

> **Judgement calls:** [`../calls/nickel-config-language-plan_calls.md`](../calls/nickel-config-language-plan_calls.md)

**Status: SHIPPED.** Every configuration surface — job specs, manager config,
worker configs, ACL policy files, job templates — is Nickel (`.ncl`). The
Jsonnet evaluator is gone from the tree. Deliberately deferred: no compatibility
shim for `.jsonnet` files, no shipped Nickel contract library for the jobspec
schema, no `legion fmt`. See "Deliberately deferred" at the end.

**Follow-up, shipped in the same branch.** A `.json` file is accepted anywhere
a `.ncl` one is, parsed rather than evaluated, for machine-emitted config. This
was not in the original plan: it became necessary once the migration made JSON
stop being a subset of the configuration language (it was one under Jsonnet, so
generators were served for free). Dispatch is by extension only; `--var`
against a `.json` file is an error. See calls #2. The same branch fixed local
jobspec resolution, which had been staging a lone file into a temp directory
and stranding every relative import — see calls #5.

## Problem statement

Legion's configuration language was Jsonnet, embedded through `jsonnet-rs`,
which is a Rust wrapper around Google's C++ implementation pinned to a personal
fork (`git = "https://github.com/mikenomitch/rust-jsonnet"`). Three concrete
costs:

1. **A C++ dependency and a forked crate on the critical build path.** Every
   `cargo build` compiled the Jsonnet C++ VM through a submodule, and the crate
   we depended on was a fork nobody upstream maintains.
2. **Errors that do not say where.** A Jsonnet failure surfaced as a single
   opaque string. `legion job run web.jsonnet` on a typo produced a message
   with a synthetic file name and no source context, and that string was what
   the manager echoed back over gRPC.
3. **No way to state what a field must be.** A jobspec that set `count: "3"`
   evaluated happily and failed later at serde deserialization, or worse, at
   admission on the manager, several hundred milliseconds and one network hop
   from the person who typed it.

The parameterization story compounded (3): `std.extVar` returns strings, so
`configs/jobs/vars.jsonnet` read `std.parseInt(std.extVar("task_count"))`, and
the CLI carried **eight** flags (`--var`, `--var-code`, `--arg`, `--arg-code`,
and a `--kv-` prefixed variant of each) to cover Jsonnet's two orthogonal
injection mechanisms — external variables and top-level arguments.

## Prior art

- **Nickel** (Tweag) — a Jsonnet-lineage configuration language with gradual
  types, contracts, and merging. `nickel-lang-core` is a pure-Rust crate with
  no C++ dependency, and `nickel export --format json` is exactly the
  evaluate-to-JSON operation Legion needs. Its `| not_exported` field
  annotation is a direct analogue of Jsonnet's `::` hidden fields, which the
  job-template manifest/render split depends on.
- **Dhall** — stronger types, but total (no general recursion) and with a
  reputation for slow evaluation on large configs; the Rust implementation
  (`dhall-rust`) lags the Haskell one.
- **CUE** — the closest competitor conceptually (unification, constraints), but
  the only usable implementation is Go. Embedding it means either cgo or
  shelling out.
- **HCL** (Nomad) — familiar to the Nomad users Legion competes for, but the
  Rust ecosystem support is parser-only and HCL's function set is deliberately
  small; it would have been a step down from Jsonnet in expressiveness.
- **Kubernetes' answer** — plain YAML plus an external templating layer (Helm,
  Kustomize). Explicitly rejected: the whole point of a real config language is
  not needing the second tool.

Nickel wins on the axis that motivated the migration: pure Rust, positioned
error reporting, and contracts. It loses on ecosystem maturity, which is the
accepted cost.

## Jobs to be done

**Job author / app developer**
- When I get a jobspec wrong, I want the error to point at the line I typed,
  so I can fix it without bisecting the file.
- When I run the same service in three environments, I want to write it once
  and pass in what differs, so the three copies cannot drift.
- When a value I pass in has to be a number, I want that stated in the spec,
  so nobody discovers it by deploying.

**Cluster operator**
- When I set up a new cluster, I want the example manager config to be
  something I can uncomment and run, so I am not translating syntax while
  reading about quorum.

**Security / platform admin**
- When someone publishes a job template, I want to know it cannot read files
  off the manager, so a template author is not implicitly a manager operator.

**Autonomous agent**
- When I author a spec and it fails, I want the failure to name the field, so
  I can correct it without a human.

*Not in scope:* the on-call responder persona — nothing on the incident path
evaluates configuration.

## Goals and non-goals

**Goals**
- One evaluator for every configuration surface, with identical error
  reporting, sandbox rules, and variable injection.
- No C++ on the build path; no forked crates.
- Errors carry the source line.
- A single, obvious parameterization mechanism.
- Every shipped example, doc snippet, and UI starter converted, and covered by
  a test that actually evaluates them.

**Non-goals**
- Accepting any *other* format. JSON earns its place by being what machines
  already emit and what `legion job schema` already describes; YAML and TOML
  have neither claim, and each additional reader is a permanent surface.
- Backwards compatibility with `.jsonnet` files. The user explicitly waived it;
  a dual-evaluator period would double the surface and the docs for a
  transitional benefit.
- A Nickel contract library expressing the whole jobspec schema. `legion job
  schema` (JSON Schema) and the manager's validator already cover it; a third
  spelling would be a third thing to keep in sync.
- Formatting (`legion fmt`). `nickel-lang-core`'s `format` feature pulls in
  topiary and tree-sitter; users have the `nickel` CLI if they want it.

## Background — what existed

Five evaluation sites, each constructing its own `JsonnetVm`:

| Site | Purpose |
| --- | --- |
| `src/cmd/job.rs:1038` | jobspec evaluation, with an import callback recording every imported file as a `Submission` |
| `src/cmd/start.rs:181` | manager config |
| `src/manager/daemon.rs:1823` | worker configs |
| `src/cmd/acl.rs:310` | ACL policy files ending in `.jsonnet` |
| `src/manager/job_templates.rs:85` | job templates, with imports rejected via the VM import callback |

Plus `tests/integration/config_jobs.rs`, which built a sixth VM to evaluate every shipped
example.

`source_jsonnet` was a proto field (`JobTemplate`, `UpsertJobTemplateRequest`),
a SQLite column (`job_template_version`), and a UI field (`sourceJsonnet` in
`Templates.tsx` / `TemplateDetail.tsx`).

## Proposed design

### 1. One module: `src/config_lang.rs`

Three entry points, so every surface gets the same behavior:

- `eval_file(path)` — a `.ncl` file to JSON. Imports resolve relative to the
  file.
- `eval_file_tracked(path, vars)` — the same, plus every source file the
  evaluation read, for the jobspec submission record.
- `eval_sandboxed(name, source, vars)` — an in-memory snippet with imports
  rejected before evaluation starts.

Errors go through `nickel_lang_core::error::report::report_as_str` with color
off, so the same snippet-bearing message appears in the CLI, in a gRPC status,
and in a log line.

### 2. `vars` replaces ext-vars and TLAs

Nickel has neither `std.extVar` nor top-level arguments, and does not need
them: `ProgramBuilder::extend_initial_env` binds identifiers next to `std`.
Legion binds exactly one, `vars`, always present and defaulting to `{}`.

```bash
legion job run web.ncl --var image=nginx --var-code replicas=3
```

```nickel
{ name = "web", task = { config = { image = vars.image }, count = vars.replicas } }
```

Four flags replace eight: `--var`, `--var-code`, `--kv-var`, `--kv-var-code`.
Defaults come from Nickel's merge rather than a flag:

```nickel
let params = { replicas | Number | default = 1 } & vars in …
```

### 3. Job templates keep their shape via `| not_exported`

Jsonnet's `job(vars)::` hidden field becomes
`job | not_exported = fun vars => …`. Manifest parsing exports the document
(dropping `job`); rendering applies `template.job` to the `vars` record. The
manifest wrapper reads `job` through record access, which `not_exported` does
not block.

Imports are rejected by **lexing the source** for the `import` keyword before
evaluation, not by an import hook. Nickel resolves imports eagerly during
program preparation, so a hook-free rejection has to happen first; lexing
(rather than string matching) means `"import"` in a string and `# import` in a
comment are not false positives.

### 4. Rename `source_jsonnet` → `source_nickel`

Proto field, SQLite column, and the UI's `sourceNickel`. The create-table
migration is edited in place — no data migration, per the compatibility waiver.

## Proto, struct, config, and persistence changes

- `proto/legion.proto`: `JobTemplate.source_jsonnet` → `source_nickel` (field 5),
  `UpsertJobTemplateRequest.source_jsonnet` → `source_nickel` (field 2), plus
  the surrounding comments describing the document shape.
- `src/storage/migrations.rs`: the `job_template_version` create-table
  statement names `source_nickel`.
- `src/storage/store.rs`: `JobTemplateVersionRecord.source_nickel` and the
  three statements that read/write it.
- `Cargo.toml`: `jsonnet-rs` (git fork) out, `nickel-lang-core` (default
  features off) in.
- Every `configs/**/*.jsonnet` → `.ncl`; `task-a`/`task-b` fragments move to
  `configs/jobs/lib/` so the example-validating test does not treat a library
  record as a jobspec.
- No new job-config fields, so `configs/jobs/maximal.ncl` changes only in
  syntax.

## Implementation phases

1. **Evaluator.** `src/config_lang.rs` with unit tests; dependency swap.
2. **Call sites.** manager config, worker configs, ACL policies, jobspecs
   (including submission tracking), job templates.
3. **CLI surface.** The `--var` family; `ConfigVars`; KV-backed variables
   rendered as Nickel expressions.
4. **Rename.** `source_jsonnet` → `source_nickel` across proto, store, MCP,
   CLI, and UI.
5. **Examples.** Every file under `configs/`, plus `benchmarks/aws/configs`
   and the shell heredocs that generate manager configs.
6. **Docs and UI.** 99 code blocks across `docs/`, the new
   `docs/configuration-language.md`, and the UI's starter template.

Phases 1–4 are one shippable unit (the tree does not build in between); 5 and 6
each stand alone.

## Testing plan

- `src/config_lang.rs` unit tests: record → JSON, `vars` binding and its empty
  default, `not_exported` dropped on export, import rejection (and the
  string/comment non-false-positive), contract errors carrying source context,
  `--var-code` expression evaluation, and import tracking.
- `tests/integration/config_jobs.rs` evaluates and fully validates every shipped jobspec —
  extended with three new tests covering the **manager** configs, the **worker**
  configs, and the **job template** (parse manifest → resolve defaults →
  render → validate), none of which had coverage before.
- `src/cmd/job.rs` tests: KV-backed vars keep their JSON type through
  `std.deserialize`, `--var-code` evaluates, and the Nickel string escaper
  escapes `%{`.
- `tests/integration/acl_endpoints.rs` template fixtures rewritten in Nickel, so the ACL
  suite exercises the real render path.
- A throwaway harness evaluated all 99 documentation code blocks; the four that
  do not evaluate standalone are illustrative alternative-value fragments and
  one block that legitimately uses `import`.

## Rollout and backward compatibility

There is none, by instruction. An existing cluster's stored template sources
are Jsonnet text in a column that no longer exists under that name; the
migration creates the column fresh. Operators upgrading a live cluster
re-publish their templates and rewrite their `.jsonnet` files.

## Deliberately deferred

- **No `.jsonnet` compatibility mode.** Named in the non-goals.
- **No jobspec contract library.** A `legion/jobspec.ncl` users could `import`
  and apply as a contract is the obvious next step; it needs a generator from
  the same source as `legion job schema` or it will drift.
- **No `legion fmt`.** The `format` feature is a large dependency for a
  convenience the `nickel` CLI already provides.
- **No environment access.** Nickel has none by design and Legion adds none;
  configs needing per-host secrets use KV or an imported, uncommitted file.
  Documented in `docs/configuration-language.md`.

## Acceptance criteria

- `cargo build` produces no C++ compilation and no git dependency.
- `cargo test --all --locked` passes, including the extended `config_jobs`
  suite.
- `grep -ri jsonnet` over the tree returns only historical plan documents.
- `legion job run configs/jobs/vars.ncl --var job_name=x --var-code cpu=250`
  places a job whose CPU is the number 250.
