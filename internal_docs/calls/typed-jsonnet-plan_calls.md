# Typed Jsonnet — Judgement Calls

Records the decisions taken while writing
[`plans/typed-jsonnet-plan.md`](../plans/typed-jsonnet-plan.md). **Ordered by
project impact, highest first**; an entry's number is its rank.

These are design-time calls: the plan is `PLANNED` and no code exists yet.
Entries added during execution get inserted at their rank, not appended.

## 1. "Typed jsonnet" is contracts plus tooling, not a new language

The ask — a config language that throws compile errors and autocompletes —
describes three languages that already exist and do it properly: Nickel (gradual
types + contracts with blame), CUE (types are values, `cue vet`), and Pkl
(typed, ships its own LSP). The call is to keep jsonnet and get the *effects* of
typing from a derived schema, a generated contract library, and a language
server.

**The alternatives.** (a) Adopt Nickel — the closest thing to literal "typed
jsonnet", and its contract-with-blame model is better than anything we can
retrofit. (b) Adopt CUE and generate constraints from the JSON Schema we would
have anyway; `cue vet` would be strictly stronger validation than a schema walk.
(c) Fork jsonnet and add annotations — the "typed jsonnet" reading taken most
literally.

**The trade-off.** We will never get real static typing. Nothing catches a type
error in a `local` that is never forced, in a helper function's parameter, or in
a user's own `.libsonnet` — we check the *value the document evaluates to*, not
the program that produced it. A Nickel or CUE adoption would have caught all
three. We are also accepting that our errors are eval-time errors dressed as
compile errors, which is honest but is not the same thing.

**Why.** The migration is the whole argument. Jsonnet is already load-bearing in
five places: `render_jobspec` (`src/cmd/job.rs:1037`), 30+ checked-in specs
under `configs/`, manager and worker configs, job templates that persist
`source_jsonnet` in the store (`src/cmd/template.rs:242`), and every jobspec a
user has already written. A language swap invalidates all of it and buys typing
for the one persona — the job author — who is already served by completion and
positioned errors. And the errors we are chasing are not the errors static
typing catches: they are `memorMb`, `"hgh"`, `"15"` instead of `"15s"` — all
contract violations, all caught perfectly by a schema-driven check. Simplicity:
one language in the project, not two. Revisit if jobspecs grow real computation
(a library ecosystem, generics-in-anger); at that point CUE for validation and
jsonnet for generation is a coherent v2.

## 2. One derived schema, and the hand-written one is deleted

`build_jobspec_schema` (`src/cmd/job.rs:607`) is a `json!` literal, while the
per-runtime halves it embeds are `schemars`-derived (`src/task_plugins/mod.rs:326`).
Phase 0 derives the envelope too and deletes the literal, making the Rust structs
the single source of truth for `job schema`, the MCP tool, the CLI checker, the
generated library, the LSP, and the UI.

**The alternatives.** (a) Keep the hand-written schema and add a test that
diffs it against a derived one — keeps hand-authored prose and enum ordering
under human control. (b) Go the other way and make the *schema* the source,
generating the Rust structs from it, as k8s-gen does from OpenAPI.

**The trade-off.** Derived schemas are uglier. `schemars` will emit
`$ref`-heavy output with `anyOf` wrappers for `Option<T>` and untagged-enum
shapes for things like `TaskConfigList` (`src/structs/job.rs:2013`) that a
human would have written as a clean `oneOf`. Some of the curated prose in the
current literal — the `priority` description that explains preemption in four
sentences — has to move into `///` comments, and doc comments now carry
user-facing weight, which is a new obligation on every future field.

**Why.** Two sources of truth for a schema is a drift bug with a delay fuse,
and this one already feeds agents. Everything downstream in this plan
multiplies the cost of that drift: a wrong schema becomes a wrong generated
library, wrong completions, and confidently wrong editor errors. Option (b) is
strictly better in a schema-first project and strictly worse here, where the
structs carry serde behavior (`#[serde(alias = "options")]`, the `kind` trap at
`src/structs/job.rs:2126`) that no schema expresses.

## 3. Unknown fields become errors, shipped as a warning first

`JobConfigInput` and `TaskConfigInput` (`src/structs/job.rs:1992`, `:2020`) do
not set `deny_unknown_fields`, so `prioritee: "high"` is silently dropped today.
Phase 0 closes them — but ships as a warning in the first release and an error
in the next minor.

**The alternatives.** (a) Error immediately: the failure mode being fixed is
"the cluster ignored what you wrote", and a warning is exactly that failure mode
with extra steps. (b) Warn forever, error only under `--strict`.

**The trade-off.** For one release, the headline bug is only half-fixed, and
somebody's typo still reaches production having been reported only in a log
line. We are also taking on the promotion itself as work that must actually
happen — a "warn now, error later" that never graduates is worse than either
endpoint.

**Why.** This is the plan's only behavior change to *existing, working* specs,
and it can break a submit at the moment a user upgrades — with no relation to
anything they changed. `docs/upgrades.md` sets the expectation that a minor
upgrade does not invalidate configs. One release of warning gives operators a
window to run `legion job check` across their repos, which is precisely the
tool this plan is shipping. Alternative (b) fails the goal: the whole point is
that the default is safe, and a strictness flag is a knob that the careful
people set and the people who need it do not.

## 4. Generate a jsonnet library, do not stop at an LSP

Phase 1 generates `legion.libsonnet` from the schema before Phase 3 writes a
language server, and the library — not the LSP — is where required fields,
enum vocabularies, and defaults are enforced.

**The alternatives.** (a) Skip the library; put everything in the LSP, where a
schema-driven completer needs no code generation and no vendoring step. (b)
Ship the library as documentation only, with no assertions, so it is a
convenience rather than a contract.

**The trade-off.** A generated library is a build artifact to keep in sync
(hence the CI regenerate-and-diff test), a vendoring step for the user, and a
version-skew surface that did not exist before. It also invites two dialects of
jobspec — raw and library — which is why the plan carries
`maximal-typed.jsonnet` alongside `maximal.jsonnet` and a test that they render
identically. That is real duplicated maintenance.

**Why.** Two reasons, both decisive. First, the library makes fields *literal
jsonnet source*, so completion and hover work in any editor with any existing
jsonnet LSP — value ships in Phase 1, months before Phase 3, and users on Emacs
or an editor we never package for get it too. This is the trick k8s-libsonnet
and crdsonnet are built on. Second, a failed `assert` inside the library
produces a jsonnet stack trace with a real file and line *for free* — which is
the highest-fidelity strategy in the position mapper (plan §6.3) and the thing
that makes an eval-time error read like a compile error. Building the LSP first
would mean solving position mapping the hard way to deliver what the library
gives away.

## 5. The language server lives in this repo, in Rust

`legion lsp` is a subcommand of the existing binary using `tower-lsp`, rather
than a fork of or contribution to grafana/jsonnet-language-server.

**The alternatives.** (a) Fork the Grafana server (Go) and add schema
awareness — it already has go-to-definition, references, and hover on `local`s
working, which is weeks of work we would be redoing. (b) Contribute schema
support upstream and configure it for Legion. (c) Ship no LSP and rely on
Grafana's plus the generated library.

**The trade-off.** We reimplement the generic jsonnet-LSP feature set that
already exists and works, and we own it forever. Go-to-definition across
imports and `local` scoping is fiddly, and our version will be worse than
Grafana's for a while. A second binary to install is also, honestly, easier for
some users than "the scheduler is also your language server".

**Why.** The features that make this worth doing at all are the ones no generic
server can have: completing a task's `config` against the schema selected by
its sibling `runtime` field, and completing `x-legion-ref` fields from a live
cluster over gRPC. Both need the Legion schema, the Legion type registry, and a
Legion client — all of which are in this binary. Shipping it as a subcommand
means the LSP version and the cluster version are the same number by
construction, which kills a whole class of skew bug. Option (c) is the fallback
if Phase 3 slips: Phases 0–2 leave it genuinely usable.

## 6. jrsonnet in the LSP only, gated on a differential parity test

The plan proposes evaluating with jrsonnet (pure Rust, AST accessible) inside
the language server while the C++ `jsonnet-rs` fork (`Cargo.toml:12`) stays
authoritative for submit — with a differential test over `configs/jobs/` as the
gate before converging on one.

**The alternatives.** (a) Switch everything to jrsonnet at once — one
evaluator, one behavior, no divergence risk. (b) Stay on the C++ binding
everywhere and do position mapping without an AST, from the rendered JSON path
and a text search.

**The trade-off.** Two evaluators is the worst thing in this plan. The failure
mode is precise and nasty: a spec that checks clean in the editor and fails at
submit, or worse, renders *differently* — a stdlib function with slightly
different edge-case behavior, a different manifest ordering. Users would rightly
stop trusting the checker. The mitigation is the parity test, which is a gate,
not a nicety.

**Why.** Position mapping without an AST is guesswork, and option (b) caps the
whole feature at "approximate location" forever. But switching the submit path
wholesale on day one bets the production evaluator on an untested swap to serve
an editor feature — wrong order of risk. Introducing jrsonnet where a mistake
costs a wrong squiggle, proving parity on the real corpus, then converging, puts
the risk where it belongs and ends in one evaluator either way.

## 7. A position mapper that admits when it does not know

When the JSON path leaves literal ground — a comprehension, a computed field
name, a `std.foldl` — the checker reports a file-level location and marks it
approximate, rather than pointing at the most plausible-looking span.

**The alternatives.** (a) Best-effort caret: find the nearest textual match for
the field name and point there. Every diagnostic gets a precise-looking
location, which is what users say they want. (b) Refuse to check specs the
mapper cannot map.

**The trade-off.** Some of the most error-prone specs are exactly the computed
ones — a task list built from a comprehension over environments — and those are
the specs where the author gets the least help. A best-effort caret would look
better in a screenshot.

**Why.** A wrong caret is worse than no caret: it sends the author to edit a
line that is correct, and once a tool has lied about a location it does not get
believed about the next one either. The honest degradation also keeps the
incentive pointed at the real fix — instrumented evaluation with true
provenance (plan §11, Phase 5) — instead of letting a heuristic paper over the
gap indefinitely.

## 8. Live cluster completion is opt-in and off by default

Completing `x-legion-ref` fields (node pools, plugins, volumes, artifacts, KV
paths) from a real cluster requires `legion lsp --manager <addr>`.

**The alternatives.** (a) On by default using the same manager address
resolution the rest of the CLI uses, since that is where the value is and users
will not find a flag. (b) Prompt on first use.

**The trade-off.** The best feature in the plan — the one neither Nomad nor
Kubernetes tooling has — is behind a flag most people will never set, so it
will be under-adopted and under-reported.

**Why.** An editor that opens a file and silently issues RPCs to production is a
surprise, and surprises about network calls are the kind users are least
forgiving about. It also breaks the offline invariant: a language server must
work on a plane, and "diagnostics hang for 5s because the manager is
unreachable" is a bad first impression that costs more than the feature earns.
Making the flag discoverable — an LSP hint the first time a `x-legion-ref` field
is completed schema-only, saying live values are available — recovers most of
the adoption without taking the default.

## 9. `legion job check` is a new verb, and `validate` stays

Positioned diagnostics land under `legion job check`; `legion job validate`
(`src/cmd/job.rs:486`) keeps working unchanged, as an alias, indefinitely.

**The alternatives.** (a) Upgrade `validate` in place — one verb, no
vocabulary growth, and everyone gets the better tool without knowing to ask.
(b) Deprecate `validate` with a warning and remove it in a later major.

**The trade-off.** Two verbs for one job is the exact kind of vocabulary
sprawl the simplicity goal exists to prevent, and "what is the difference
between check and validate" is a support question we are choosing to own
forever.

**Why.** `validate` has a contract that scripts, CI, and agents depend on: exit
code, the `{valid, job, tasks, groups, warnings}` JSON envelope, and the fact
that it consults the manager for KV vars. `check` has a different shape — a
diagnostics list, multiple errors per run, positions, no manager. Overloading
one verb with two output shapes breaks agent parsers silently, which
`docs/agent-readiness.md` treats as a cardinal sin. Keeping the old verb costs a
doc line; breaking a machine consumer costs trust. Worth revisiting at a major
version, when `check` can absorb `validate`'s envelope behind a flag.
