# Typed Jsonnet: compile-time errors and editor completion for jobspecs

**Status: PLANNED.** Nothing here is built. Phase 0 (one derived schema) is the
prerequisite for every other phase and is worth landing on its own.

> **Judgement calls:** [`../calls/typed-jsonnet-plan_calls.md`](../calls/typed-jsonnet-plan_calls.md)

---

## 1. Problem statement

A Legion jobspec is a jsonnet document that renders to JSON, which is then
deserialized into `JobConfig` and deep-validated (`src/cmd/job.rs:486`,
`src/task_plugins/mod.rs:807`). Everything about that pipeline is untyped until
the very last step, and the last step happens after the author has already
finished writing.

Concretely, today:

- **Typos in top-level and task-level fields are silently dropped.**
  `JobConfigInput` (`src/structs/job.rs:1992`) and `TaskConfigInput`
  (`src/structs/job.rs:2020`) do not carry `deny_unknown_fields` — only six
  nested structs do. `prioritee: "high"` parses fine and the job runs at
  `normal`. There is no error, no warning, and no way for the author to find
  out except by noticing the behavior later, in production.
- **`config` is a `Value` until the worker sees it.** `TaskConfigInput.config`
  (`src/structs/job.rs:2045`) is an opaque blob whose shape depends on
  `runtime`. `validate_job_config` does route it to the plugin's schema, but
  nothing in the editor knows that `runtime: "exec"` means the `config` object
  below it should offer `command`, `args`, `cwd`, `env`.
- **There is no editor experience at all.** No completion, no hover, no
  inline errors. The author's loop is: write jsonnet, run
  `legion job validate`, read a one-line serde error with no file or line,
  guess which of 300 lines it refers to, repeat. `configs/jobs/maximal.jsonnet`
  is 700+ lines of that.
- **Errors have no position.** The validate path formats serde's message
  (`src/cmd/job.rs:505`) and hands back `"jobspec is not a valid job
  configuration: unknown variant ..."`. The rendered JSON's line number is
  useless — the author wrote jsonnet, not JSON, and the two have no relation
  once imports, `local`s, and comprehensions are involved.
- **The schema that agents and the UI consume is hand-written and will
  drift.** `build_jobspec_schema` (`src/cmd/job.rs:607`) is a `json!` literal
  maintained by hand next to the structs it describes. It is what
  `legion job schema`, the `legion_job_schema` MCP tool
  (`src/cmd/mcp.rs:781`), and any future UI editor read. Nothing enforces that
  it matches `JobConfigInput`. The per-runtime halves *are* derived
  (`src/task_plugins/mod.rs:326`, via `schemars`), which is exactly the
  asymmetry to fix.

The request behind this plan is "typed jsonnet". Jsonnet has no type system and
is not going to grow one. What is actually achievable, and what this plan
delivers, is:

> **Every mistake that a type system would have caught becomes an error with a
> file, a line, a column, and a suggested fix — raised while the author is
> still typing.**

That is a *contracts + tooling* story, not a *language* story. Section 2
explains why every serious jsonnet shop landed in the same place.

---

## 2. Prior art

### 2.1 Languages that actually have types

| Language | What it gives you | Why we are not adopting it |
| --- | --- | --- |
| **[Nickel](https://nickel-lang.org/)** (Tweag) | Literally "jsonnet with a gradual type system and contracts". `let f : Number -> Number`, plus runtime *contracts* that blame the right source span. | The contract-with-blame idea is the single best thing here and we take it. Adopting the language means every existing jobspec, every `.libsonnet`, and every stored template (`template.source_jsonnet`, `src/cmd/template.rs:242`) is a migration. |
| **[CUE](https://cuelang.org/)** | Types *are* values; `cue vet` unifies data against constraints; `cue import jsonschema` generates constraints from a JSON Schema — which we would have. | Best-in-class validation, genuinely tempting for a v2. Lattice semantics are hard to explain at 3am, which is the bar Legion sets for itself. |
| **[Pkl](https://pkl-lang.org/)** (Apple) | Typed config language shipped *with* an LSP and IntelliJ plugin, which is the exact product we want. | Same migration cost, plus a JVM-adjacent toolchain in a single-static-binary project. |
| **[Dhall](https://dhall-lang.org/)** | Total, strongly typed, provably terminating. | Ergonomics; the community is small and shrinking. |
| **[KCL](https://kcl-lang.io/)** | Typed, K8s-oriented, schema-first with an LSP. | Newer, and the same migration argument. |

The lesson taken: **contracts with blame beat static types for config**, because
config errors are overwhelmingly "wrong field name / wrong enum member / wrong
unit", which a contract catches perfectly and a Hindley-Milner type system
catches no better.

### 2.2 What the jsonnet ecosystem actually does

- **[grafana/jsonnet-language-server](https://github.com/grafana/jsonnet-language-server)** — a real LSP for jsonnet
  doing pure static AST analysis: go-to-definition, references, hover on
  `local`s, completion of object fields it can see lexically. It has no notion
  of a *target schema*, so it cannot tell you `memorMb` is wrong. It proves the
  LSP half is tractable; it also shows that without a schema you get navigation,
  not typing.
- **[docsonnet](https://github.com/jsonnet-libs/docsonnet)** — a doc-comment convention
  (`#field:: d.fn(help=..., args=[...])`) that turns library metadata into
  hover text and generated docs. This is how jsonnet libraries get IDE-visible
  documentation without a type system.
- **[k8s-libsonnet](https://github.com/jsonnet-libs/k8s-libsonnet)** / **k8s-gen** — the Kubernetes OpenAPI
  spec mechanically generated into a jsonnet library of constructors and
  `withX()` mixins. Completion works because the fields are *literally in the
  library source*, so any LSP can see them. This is the highest-leverage trick
  in the whole ecosystem, and it costs us a code generator, not a language.
- **[crdsonnet](https://github.com/crdsonnet/crdsonnet)** — generates the same shape from JSON Schema
  and CRDs, and demonstrates schema → jsonnet-library codegen end to end.
- **[Tanka](https://tanka.dev/)** — the workflow shell: `tk eval`, `tk diff`,
  vendored libraries via `jsonnet-bundler`.

### 2.3 What the wider config world does

- **Kubernetes**: authors get typing from `yaml-language-server` + a JSON Schema
  bound by a `# yaml-language-server: $schema=` modeline, CI gets
  `kubeconform`, and the cluster gets structural CRD schemas plus server-side
  dry-run. Three different consumers, **one schema**. The `x-kubernetes-*`
  extension keys are the precedent for our `x-legion-*` (§5.1).
- **Nomad**: HCL2 with `nomad job validate` and a server-side plan. The editor
  story is thin, which is a gap Legion can win on outright.
- **TypeScript**: the closest analogy to this plan. A dynamic language gets
  typing from a *separate generated artifact* (`.d.ts`) plus a language server,
  without changing the runtime language. Our generated `legion.libsonnet` is
  the `.d.ts`; `legion lsp` is `tsserver`.

### 2.4 What we take

1. One schema, derived, feeding every consumer (K8s).
2. Schema → jsonnet library codegen, so completion works in *any* editor
   before we ship an LSP at all (k8s-libsonnet, crdsonnet).
3. Contracts with blame — assertions that name the offending span (Nickel).
4. Doc comments as the hover surface (docsonnet).
5. An in-tree LSP for the last mile, including things no generic tool can do:
   discriminated `config` completion and live cluster values.

---

## 3. Jobs to be done, by persona

**Job author / app developer**

- When I am writing a jobspec in my editor, I want to see the fields that are
  allowed right here, with what they mean and what they default to, so I do not
  have to keep a doc page open on a second monitor.
- When I misspell a field or an enum value, I want to be told immediately and
  told what I probably meant, so I do not discover it from a job that ran with
  the wrong settings.
- When I set a value that is legal on its own but illegal in combination —
  `createFirst` replacement on a stateful group — I want that flagged where I
  typed it, not at submit time.
- When I am filling in a task's runtime options, I want the suggestions to match
  the runtime I chose, so `exec` does not offer me `image`.

**Autonomous agent**

- When I generate a jobspec, I want a single call that tells me every problem
  with it, in a structured form with positions, so I can fix them all in one
  pass instead of one per round trip.
- When I need to know what is legal, I want one authoritative machine-readable
  description rather than prose I have to infer from.

**Cluster operator**

- When I write a manager or worker config, I want the same guarantees I get on
  jobspecs, so a typo in `configs/manager.jsonnet` fails at edit time and not
  at boot.
- When my team upgrades Legion, I want to know which fields in my checked-in
  specs are new, deprecated, or gone, without reading a changelog line by line.

**Platform admin**

- When I roll out the shared jobspec library to my team, I want its version tied
  to the cluster's version, so nobody writes a spec against fields their cluster
  does not have.

*Not in scope:* the on-call responder — this is an authoring-time surface, and
an incident is not authoring time.

### User scenarios

1. **The typo.** An author types `memorMb: 512` inside `resources`. Before the
   file is saved, a red squiggle appears under `memorMb` reading *unknown field
   `memorMb` in `resources`; did you mean `memoryMb`?*. A code action fixes it.
2. **The unknown vocabulary.** An author types `priority: "` and gets the six
   bands, each with the one-line description that
   `configs/jobs/maximal.jsonnet` currently carries as a comment block —
   including "choosing a band below `normal` is the consent to be displaced".
3. **The agent's round trip.** An agent writes a spec, calls
   `legion job check spec.jsonnet --json`, gets four positioned diagnostics with
   fix hints, applies all four, calls again, gets `{"ok": true}`, and submits.

---

## 4. Goals and non-goals

**Goals**

- One derived, authoritative jobspec schema; the hand-written one deleted.
- Unknown fields are errors everywhere in the jobspec, with "did you mean".
- Every diagnostic carries `file:line:col` in the **jsonnet** source.
- Completion, hover, and inline diagnostics in VS Code, Neovim, Zed, and the
  Legion UI's editor.
- Completion that is aware of the `runtime` discriminator and, optionally, of
  the live cluster (node pools, plugins, volumes, artifacts, templates).
- The whole thing works offline; the manager is an enhancement, never a
  requirement.

**Non-goals**

- **Replacing jsonnet.** Raw, library-free jsonnet must keep working forever.
  The typed library is opt-in; §9 is explicit that no existing spec breaks.
- **A static type checker for jsonnet the language.** We check *the value a
  document evaluates to*, plus contracts inside the library. We are not going
  to type-check arbitrary jsonnet control flow.
- **Type-checking jsonnet that we cannot evaluate.** A spec that reads a KV var
  at render time is checked with the vars it is given, not symbolically.
- **A formatter.** `jsonnetfmt` exists; `legion fmt` is backlog (§8, Phase 5).
- **Typed plugin authoring.** Plugin config schemas are already derived; this
  plan consumes them and does not redesign them.

---

## 5. Background — what exists today

| Piece | Where | State |
| --- | --- | --- |
| Jsonnet evaluation | `render_jobspec`, `src/cmd/job.rs:1037` | C++ jsonnet via the `jsonnet-rs` fork (`Cargo.toml:12`). Custom `import_callback` records every imported file into `Submission`s — **already the file set an LSP needs**. |
| Ext/TLA vars, KV-backed | `src/cmd/job.rs:843`, `:890` | `--var`, `--var-code`, `--arg`, `--kv-var` … resolved against the manager. |
| Deserialization | `JobConfigInput`, `src/structs/job.rs:1992` | Two-stage: lenient input struct → validated `JobConfig`. **No `deny_unknown_fields`** at job or task level. |
| Task config | `src/structs/job.rs:2045` | `Value`, discriminated by `runtime`. |
| Deep validation | `validate_job_config`, `src/task_plugins/mod.rs:807` | Runs at submit *and* in `legion job validate`. |
| Runtime schemas | `derived_schema`, `src/task_plugins/mod.rs:326` | `schemars` derive on each runtime's config struct. **Already the model to follow.** |
| Jobspec schema | `build_jobspec_schema`, `src/cmd/job.rs:607` | Hand-written `json!`. Envelope by hand, runtimes derived. |
| Agent surface | `src/cmd/mcp.rs:127`, `:781` | `legion_job_schema`, `legion_job_validate`, `legion_job_plan`. |
| Precedent for good errors | `src/structs/job.rs:2126` | The `kind` → `runtime` rename hint, hand-written. Proof the project already believes in this; §6.1 generalizes it. |
| Typed variables | `src/cmd/template.rs:3` | Job templates already declare typed variables — a vocabulary to share, not reinvent. |
| Cluster config | `src/structs/config.rs` | Manager/worker configs are jsonnet + serde too, with the same gaps. |

Two facts make this plan cheap:

1. **`schemars` is already a dependency and already derives half the schema.**
   Phase 0 is mostly adding a derive and deleting a literal.
2. **Evaluating a jobspec is sub-millisecond and hermetic.** There is no
   network, no clock, no filesystem beyond imports. "Evaluate on every
   keystroke" is a real strategy, which is why an eval-time contract behaves
   like a compile-time error.

---

## 6. Proposed design

Four layers. Each is independently useful; each makes the next cheaper.

```
  Rust structs  ──derive──▶  JSON Schema  ──codegen──▶  legion.libsonnet
  (source of truth)          (+ x-legion-*)             (constructors,
                                   │                     asserts, docs)
                                   ├────────────▶  legion job check   (CLI/CI/MCP)
                                   ├────────────▶  legion lsp         (editors)
                                   └────────────▶  UI Monaco editor
```

### 6.1 Layer 0 — one schema, derived (Phase 0)

Derive `JsonSchema` on `JobConfigInput`, `TaskConfigInput`, and the config
structs they reach, and replace `build_jobspec_schema` with an assembly of
derived parts + the already-derived runtime schemas.

Three additions on top of stock `schemars`:

- **`deny_unknown_fields` at every level**, with a `did you mean` computed by
  edit distance against the known field set. Generalizes the one-off `kind`
  hint at `src/structs/job.rs:2126`.
- **`x-legion-*` extension keys** (the `x-kubernetes-*` precedent), because a
  schema alone cannot express what the editor needs:
  - `x-legion-format: "duration" | "memory" | "cpu" | "signal" | "cron"` —
    what `"15s"` / `"512Mi"` mean and how to validate them.
  - `x-legion-ref: "nodePool" | "plugin" | "volume" | "artifact" | "job" |
    "kvPath" | "policy"` — this string names a *cluster object*, which is what
    makes live completion (§6.4) possible.
  - `x-legion-requires: [{ when: {...}, then: {...} }]` — cross-field rules
    that are today only in `validate_job_config` prose, e.g. `rollout.slots`
    requires a slot-mode stateful task in the group, `replacement:
    "createFirst"` is illegal on a stateful group.
  - `x-legion-since` / `x-legion-deprecated` — version skew, for the operator
    job in §3.
- **Docs from doc comments.** `schemars` lifts `///` into `description`. The
  rule "every new job-config field goes into `configs/jobs/maximal.jsonnet`"
  gains a sibling: every field carries a doc comment, and a test enforces it.

Also in Phase 0: a **round-trip test** asserting every file in `configs/jobs/`
validates against the derived schema, and a **coverage test** asserting every
schema property appears somewhere in `maximal.jsonnet`. Those two tests are
what keep the schema honest after we start generating things from it.

### 6.2 Layer 1 — `legion.libsonnet`, generated (Phase 1)

A jsonnet library generated from the schema, embedded in the binary, and
written out on demand:

```console
$ legion jsonnet vendor ./lib          # writes lib/legion.libsonnet (+ version)
```

Why a generated library is the highest-leverage piece: **the fields are literal
jsonnet source, so completion and hover work in any editor with any jsonnet LSP,
today, with zero Legion-specific tooling.** It is exactly how k8s-libsonnet
works.

What the author writes:

```jsonnet
local legion = import 'legion.libsonnet';

legion.job('api') {
  priority: 'high',
  tasks: [
    legion.task.exec('web')
    + legion.task.withCount(3)
    + legion.task.withGroup('frontend')
    + legion.exec.withCommand('/usr/bin/api')
    + legion.exec.withArgs(['--port', '8080'])
    + legion.task.withResources(cpu=500, memoryMb=512),

    legion.task.container('sidecar')
    + legion.container.withImage('ghcr.io/acme/proxy:1.4')
    + legion.task.withLifecycle('sidecar'),
  ],
  rollout: legion.rollout.default
           + legion.rollout.withPercentSteps([10, 50, 100])
           + legion.rollout.withMinHealthyTime('10s'),
}
```

Every generated member carries four things:

1. **A constructor** with required fields as named parameters — a missing
   required field becomes a jsonnet arity error with a real file and line,
   which is as close to a compile error as this language gets.
2. **An assertion** for anything the schema constrains:

   ```jsonnet
   withPriority(band):: {
     assert std.member(['critical','high','elevated','normal','low','background'], band) :
       "priority: %s is not a band. One of: critical, high, elevated, normal, low, background."
       % band,
     priority: band,
   },
   ```

3. **A docsonnet doc comment**, so hover shows the same text as the schema
   `description` and `legion job schema`.
4. **The default**, so what the author reads is what the cluster does.

Plus a closed-object contract for authors who prefer plain object literals over
mixins — the escape hatch that keeps the library from being all-or-nothing:

```jsonnet
legion.check.job({
  name: 'api',
  prioritee: 'high',      // ERROR: unknown field `prioritee`; did you mean `priority`?
  tasks: [ ... ],
})
```

**Laziness.** Jsonnet only evaluates what is forced, so an assertion on an
unread field never fires. This is not a problem in practice, because both
consumers *manifest the whole document*: `legion job check` renders it to JSON,
and the LSP evaluates it to produce diagnostics. Manifestation forces every
field. The one real gap is a field guarded behind a conditional that is false
for the vars given — checked when those vars change, not before. Stated as a
known limitation in the public doc.

**Distribution and versioning.** The library is embedded in the binary and
stamped with the Legion version. `legion job check` warns when the vendored
copy's version does not match the CLI's, and the LSP surfaces it as a hint. No
package manager, no registry, no `jsonnet-bundler` requirement.

### 6.3 Layer 2 — `legion job check`, with positions (Phase 2)

```console
$ legion job check configs/jobs/api.jsonnet
error[L0203]: unknown field `memorMb` in `resources`
  --> configs/jobs/api.jsonnet:14:20
   |
14 |       resources: { cpu: 500, memorMb: 512 },
   |                              ^^^^^^^ did you mean `memoryMb`?
   |

error[L0107]: `priority` must be one of critical, high, elevated, normal, low, background
  --> configs/jobs/api.jsonnet:4:13
   |
 4 |   priority: 'hgh',
   |             ^^^^^ did you mean `high`?
   |

warning[L0402]: `replacement: "createFirst"` is downgraded on group `frontend`
  --> configs/jobs/api.jsonnet:31:18
   |
   = note: the group has a slot-mode stateful task; two live allocations would
           share one instance id (docs/stateful-workloads.md)

2 errors, 1 warning
```

`--json` emits LSP-shaped diagnostics, so the CLI, CI, the MCP tool, and the
language server all consume one representation:

```json
{ "ok": false, "diagnostics": [
  { "file": "configs/jobs/api.jsonnet",
    "range": { "start": {"line": 13, "character": 29}, "end": {"line": 13, "character": 36} },
    "severity": "error", "code": "L0203",
    "message": "unknown field `memorMb` in `resources`",
    "path": "$.tasks[0].resources.memorMb",
    "hint": "did you mean `memoryMb`?",
    "fix": { "replace": "memoryMb" } } ] }
```

**Mapping a JSON path back to a jsonnet span** is the genuinely hard part. Three
strategies, in descending fidelity, and the checker uses the best one available:

1. **Library assertions already carry a stack trace.** A failed `assert` inside
   `legion.libsonnet` reports the jsonnet call stack with file and line —
   exact, free, and the strongest reason Layer 1 comes before Layer 2.
2. **Static AST descent** for library-free specs. Parse the entrypoint and its
   imports, walk the object-field path (`tasks` → index → `resources` →
   `memorMb`) as far as it stays a literal field, and report the span of the
   deepest node matched. Covers the overwhelming majority of real specs, which
   are mostly literals with `local`s at the edges. **When the path leaves
   literal ground — a comprehension, a computed field name, a `std.foldl` — we
   report the file-level span and say the location is approximate. A confidently
   wrong caret is worse than an honest absence.**
3. **Instrumented evaluation** (Phase 5 / open question): evaluate with a
   manifester that records the originating AST node for every leaf, giving exact
   provenance through arbitrary computation. Requires a jsonnet implementation
   we can modify — see §11.

`legion job check` supersedes `legion job validate` as the recommended verb;
`validate` stays as an alias forever.

### 6.4 Layer 3 — `legion lsp` (Phase 3–4)

An in-tree language server (`tower-lsp`), so an editor gets:

- **Diagnostics on change** — debounced ~150 ms: evaluate, validate, map,
  publish. Sub-millisecond eval makes this comfortable.
- **Completion**, which is where the discriminator pays off. Compute the JSON
  path at the cursor from the AST, look up the schema at that path, and offer
  its properties with descriptions, defaults, and enum members. Inside a task's
  `config`, read the sibling `runtime` field and offer *that runtime's* schema —
  `exec` gets `command`/`args`/`cwd`, `container` gets `image`/`ports`. No
  generic tool can do this.
- **Hover** — the schema `description`, the default, `x-legion-since`, and a
  deep link to the `docs/` page.
- **Go-to-definition / references / rename** for `local`s and imports (the
  standard jsonnet-LSP feature set).
- **Code actions** — apply a `did you mean`; insert all required fields for the
  block under the cursor; convert a raw object into the equivalent
  `legion.libsonnet` constructor call (the migration on-ramp for Layer 1).
- **Live cluster completion, opt-in** (`legion lsp --manager <addr>`): every
  field marked `x-legion-ref` completes from the cluster — real node pool names,
  installed plugin names, existing volumes, artifacts, job names, KV paths, ACL
  policies. Read-only RPCs, cached, and **off by default**: an editor that
  silently talks to production is a surprise, and a language server must work
  on a plane.

Clients: a thin VS Code extension (bundled `.vsix`, no logic of its own), and
copy-paste config snippets for Neovim, Zed, Helix, and Emacs `lsp-mode` —
generic LSP clients need no bespoke work.

### 6.5 Layer 4 — the UI and the agent surface

- The UI's jobspec editor mounts Monaco with the schema fetched over the
  existing `job schema` path (`src/cmd/mcp.rs:781` shows it is already wired for
  machine consumers), giving completion and hover in the browser. Full LSP in
  the browser is possible later via WASM; the schema alone covers most of the
  value.
- MCP gains `legion_jobspec_check` returning the structured diagnostics of
  §6.3 — per `docs/agent-readiness.md`, read-only, always listed, never gated.
  This is the agent job from §3: one call, every problem, positions and fixes.

### 6.6 Cluster configs

Manager and worker configs (`src/structs/config.rs`) get the same treatment in
Phase 5: derive their schema, generate `legion-config.libsonnet`, and let the
LSP recognize `configs/manager.jsonnet` by path convention or a
`// legion:schema manager` modeline (the `yaml-language-server` precedent). The
machinery is identical; only the schema root differs.

---

## 7. Struct, config, and CLI surface changes

**Structs.** `JsonSchema` derives across the `*Input` structs and their reach;
`deny_unknown_fields` added to `JobConfigInput` and `TaskConfigInput`; doc
comments completed on every field. `#[schemars(extend(...))]` carries the
`x-legion-*` keys.

**No proto changes** in Phases 0–3. `GetJobSchema` as an RPC (so the UI and
remote agents can fetch the schema without a local binary) is Phase 4 and gets
full proto comments and an ACL classification — read-only, `AclResource::Job` /
`AclAction::Read` with `AclScope::any()`, plus its row in
`tests/acl_endpoints.rs`.

**CLI.**

| Command | Purpose |
| --- | --- |
| `legion job check <path> [--json]` | Positioned diagnostics. Supersedes `validate`. |
| `legion job schema [--json] [--runtime <name>]` | Existing, now derived; new `--runtime` filter. |
| `legion jsonnet vendor <dir>` | Write `legion.libsonnet` for the running version. |
| `legion lsp [--manager <addr>]` | Language server on stdio. |

Every one takes `--json`/`-j` and routes through `crate::cmd::output`; every one
gets its `catalog()` entry in `src/cmd/capabilities.rs` with `mutates: false`,
`read_only_safe: true`.

**Configs.** `configs/jobs/maximal.jsonnet` stays as the raw-jsonnet reference.
A new `configs/jobs/maximal-typed.jsonnet` expresses the same job through
`legion.libsonnet`, and a test asserts the two render to identical JSON — which
is simultaneously the library's best documentation and its regression suite.

---

## 8. Implementation phases

**Phase 0 — One derived schema.** Derives, `deny_unknown_fields` + `did you
mean`, `x-legion-*` extensions, `build_jobspec_schema` deleted in favor of
assembly, round-trip and coverage tests.
*Ships alone:* typos become errors and `legion job schema` stops drifting —
worth landing even if nothing else here is ever built.

**Phase 1 — `legion.libsonnet` + `legion jsonnet vendor`.** Generator,
embedded library, drift test (regenerate and diff in CI),
`maximal-typed.jsonnet`, public doc page.
*Ships alone:* completion, hover, and assertion errors in any editor that
already has a jsonnet LSP.

**Phase 2 — `legion job check`.** Rendered-JSON validation, strategies 1 and 2
of the position mapper, rustc-style human output, `--json` diagnostics, the
`legion_jobspec_check` MCP tool, a CI recipe.
*Ships alone:* CI gate and the agent's one-call loop.

**Phase 3 — `legion lsp`, core.** `tower-lsp` server; diagnostics, completion,
hover, go-to-definition; VS Code extension; editor snippets.
*Ships alone:* the headline feature.

**Phase 4 — Cluster-aware and UI.** `x-legion-ref` completion against a live
manager, code actions, `GetJobSchema` RPC, Monaco in the UI.
*Ships alone:* completion that knows your cluster — the thing neither Nomad nor
K8s tooling does.

**Phase 5 — Backlog.** Cluster-config schemas (§6.6); exact provenance via an
instrumented evaluator (§11); `legion fmt`; deprecation lints from
`x-legion-since`.

---

## 9. Testing plan

| Phase | Tests |
| --- | --- |
| 0 | Every `configs/jobs/*.jsonnet` validates against the derived schema. Every schema property appears in `maximal.jsonnet`. Every property has a non-empty `description`. Unknown-field fixtures produce the expected suggestion. Snapshot of the full schema, so any change is visible in review. |
| 1 | Generator is deterministic. CI regenerates and diffs against the checked-in library. `maximal-typed.jsonnet` and `maximal.jsonnet` render byte-identical JSON. One negative fixture per assertion, asserting the message text. |
| 2 | Golden diagnostics: a corpus of deliberately broken jobspecs, each with expected code, message, and span. Mapper degradation tests — a comprehension-built spec must produce an *approximate* location, never a wrong one. `--json` shape test. |
| 3 | LSP protocol tests over an in-process transport: `didOpen` → diagnostics, `completion` at recorded cursor positions (including inside `config` for each runtime), `hover`. Latency budget: p99 under 50 ms for a `maximal.jsonnet`-sized document. |
| 4 | Cluster completion against a fake manager. Assert the LSP fully functions with the manager unreachable. |

No scheduler code is touched, so the benchmark rule in `AGENTS.md` does not
apply.

---

## 10. Rollout and backward compatibility

- **Phases 1–4 are purely additive.** No existing jobspec changes; the library
  is opt-in.
- **Phase 0 is the one compatibility event.** Turning on `deny_unknown_fields`
  turns silently-ignored fields into hard errors — which is the entire point,
  and also means a spec that "worked" yesterday can fail today. Mitigation: land
  it as a **warning** in the first release (`legion job check` reports it,
  submit still accepts it, and the manager logs it once per submit), and promote
  it to an error in the next minor. `legion job check --strict` opts in early.
  The version skew rules in `docs/upgrades.md` get a note.
- **Version skew.** A vendored `legion.libsonnet` older than the cluster is
  fine (it generates a subset). Newer is not — the check warns, naming both
  versions.
- **Nothing is gated by a feature flag.** The LSP is a separate subcommand;
  not running it costs nothing.

---

## 11. Open questions

1. **Which jsonnet implementation does the LSP use?** The CLI's C++
   `jsonnet-rs` fork (`Cargo.toml:12`) has no incremental parse, no AST access,
   and no way to instrument manifestation.
   [jrsonnet](https://github.com/CertainLach/jrsonnet) is pure Rust, gives us
   the AST for position mapping, and could support strategy 3.
   *Current lean:* jrsonnet **in the LSP only**, with the C++ evaluator
   remaining authoritative for submit until a differential-evaluation test over
   `configs/jobs/` proves parity. Two evaluators is a real cost — a spec that
   checks clean and submits dirty would be the worst possible failure — so the
   parity test is a gate, not a nicety. If parity holds, converge on jrsonnet
   everywhere and delete the fork.
2. **Are cross-field rules (`x-legion-requires`) declarative or code?** JSON
   Schema can express some of it via `if/then`, but not "the group contains a
   slot-mode stateful task". *Lean:* declarative for the expressible subset
   (the editor can act on it), a named Rust rule with a stable code for the
   rest, and the schema records the code so the LSP can at least link the docs.
3. **Does `legion.libsonnet` become the documented default?** Mixin style is
   powerful and unfamiliar; plain objects with `legion.check.job(...)` are
   friendlier. *Lean:* document the plain-object + `check` form first, keep
   mixins for library authors, and let `maximal-typed.jsonnet` show both.
4. **Do we publish the schema to SchemaStore** so `$schema`-aware tools pick it
   up for rendered JSON? Cheap, and free coverage for non-jsonnet users.
   *Lean:* yes, after Phase 0 stabilizes.

---

## 12. Acceptance criteria

**Phase 0** — `build_jobspec_schema` is gone; the schema is derived.
`prioritee: "high"` produces `unknown field 'prioritee'; did you mean
'priority'?`. Every `configs/jobs/*.jsonnet` validates against the derived
schema in CI. Every property has a description.

**Phase 1** — `legion jsonnet vendor ./lib` writes a library that renders
`maximal-typed.jsonnet` to JSON identical to `maximal.jsonnet`. A misspelled
band raises a jsonnet assertion naming the file and line. With
grafana/jsonnet-language-server installed, typing `legion.task.` offers the
constructors with hover docs.

**Phase 2** — `legion job check` on the broken-spec corpus reports every
diagnostic with the correct code and span; `--json` matches the documented
shape; the MCP tool returns the same payload; a comprehension-built spec yields
an approximate location, never a wrong one.

**Phase 3** — In VS Code, opening a jobspec shows inline errors within 200 ms
of a keystroke; completion inside a `runtime: "exec"` task's `config` offers
`command`/`args`/`cwd` and not `image`; hover on `priority` shows the band
vocabulary. The server works with no manager reachable.

**Phase 4** — Completion on `nodePool` lists the connected cluster's real
pools; disconnecting the manager degrades to schema-only completion with no
error dialog and no hang.

---

## 13. Documentation

- `docs/typed-jobspecs.md` — the public page: what the library is, how to vendor
  it, the check command, editor setup per editor, the diagnostic code index,
  and the known limitations (lazy fields behind false conditionals; approximate
  positions in computed specs; the offline/online completion split).
- `docs/agent-readiness.md` — the `legion_jobspec_check` tool and the
  check-then-submit loop.
- `docs/README.md` and `README.md` — index entries.
- `docs/upgrades.md` — the `deny_unknown_fields` warn-then-error transition.
- `configs/jobs/maximal-typed.jsonnet` — the worked example.
