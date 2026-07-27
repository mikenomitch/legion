# Judgement Calls — Task Kind → Task Runtime Rename

Plan: [`../plans/task-runtime-rename-plan.md`](../plans/task-runtime-rename-plan.md)

Entries are ranked by project impact, highest first. An entry's number is its
rank, not the order it was decided in.

## 1. `kind` is a hard break, not a deprecated alias

**Call.** The old spelling stops parsing. `TaskConfigInput` has no
`#[serde(alias = "kind")]`, and a jobspec using `kind` fails at submit with an
error that names the replacement:

```
$ legion job run configs/jobs/demo.jsonnet
error: task `web`: unknown field `kind`, expected `runtime`
  hint: `kind` was renamed to `runtime`; replace `kind: "container"` with `runtime: "container"`
```

**Alternatives.** (a) Accept both, warn on `kind` — the standard, polite
deprecation, and what most projects would do for a jobspec field. (b) Accept
both silently and forever, treating `kind` as a permanent hidden alias.

**Trade-off.** Every existing jobspec in every user's repo breaks on upgrade,
including ones the user did not write. There is no window in which a team can
upgrade the cluster and migrate their specs independently — both have to happen
together. That is a real cost and the sharpest edge in this change.

**Why (simplicity).** The entire justification for the rename is that one word
should mean one thing. An alias keeps `kind` alive in the parser, in the error
messages, in every jobspec found on the internet, and in the docs that have to
explain which one is current — which is precisely the ambiguity being paid for
in the first place. A rename with a permanent alias buys the migration cost and
none of the benefit. Legion is also pre-1.0 with no compatibility promise, which
is exactly when a break like this is cheapest; the same call at 1.0 would go the
other way. The mitigation is diagnostic quality, not a shim: the error names the
field, the fix, and the literal substitution.

## 2. The axis is `runtime`, not Nomad's `driver`

**Call.** The field is `runtime`:

```jsonnet
tasks: { web: { runtime: 'container', config: { image: 'nginx:1.27' } } }
```

**Alternatives.** (a) `driver`, Nomad's word for exactly this concept, and the
one with the cleanest technical case — it collides with nothing in the codebase,
so it would have been a single mechanical rename with no `TaskRuntime` problem
to solve (see §3). (b) `executor` (Airflow, Celery). (c) `type` — rejected early
as contentless and as importing a fresh collision, since Nomad already uses
`type` for the scheduler axis.

**Trade-off.** `runtime` cost an entire second rename that `driver` would not
have — `TaskRuntime` → `TaskInstance`, a breaking change to the public plugin
API that is otherwise unrelated to this work. Two API breaks in one release
instead of one, and out-of-tree plugins pay for both. `driver` was the cheaper
call and was recommended on those grounds.

**Why (simplicity, overridden by the user).** This was the user's call against a
recommendation of `driver`, and the reasoning for `runtime` stands on its own:
"container runtime" is the most widely understood industry name for the
component that runs a workload, it generalizes past containers without strain
(`exec` is a process runtime, `noop` a null runtime), and it does not read as a
Nomad clone. The cost was quantified before the decision — the collision was
found and surfaced during scoping, not discovered mid-rename — and accepted
knowingly. Recorded here because a future reader will find the `TaskInstance`
rename in the history and wonder why a naming change touched the plugin trait.

## 3. `TaskRuntime` becomes `TaskInstance`; `ContainerRuntime` keeps its name

**Call.** The per-task live-state struct is renamed, freeing the word for the
axis. The OCI runtime type is not:

```rust
// plugin API — the live state of one running task instance
fn start(&self, alloc: &Allocation, task: &TaskConfig,
         instance: &mut TaskInstance) -> R<()>;

// container.rs — unchanged; this really is a container runtime, one level down
let oci_runtime = ContainerRuntime::detect()?;
```

**Alternatives.** (a) Rename only the user-facing surface and leave
`TaskRuntime` alone — smaller diff, one fewer API break, but it produces
`runtime: &mut TaskRuntime` for a task whose `runtime` is `"container"`, which
is a worse ambiguity than the one the rename set out to fix. (b) Rename
`ContainerRuntime` too, for total purity — rejected because it is correctly
named already; CRI calls runc a container runtime, and the nesting (a container
*task runtime* selects an OCI *container runtime*) is real rather than
accidental.

**Trade-off.** `TaskInstance` now sits near `instance` in the stateful-workloads
sense (`stateful.mode: 'slot'` binds allocations to instance identities), so
"instance" is doing two jobs in the codebase. Judged the lesser collision: those
two live in different layers and are never in scope together, whereas
`runtime`/`TaskRuntime` would have collided inside single function signatures.
Revisit if `TaskInstance` and instance identity ever meet in one module.

**Why (extensibility).** The plugin trait is the surface out-of-tree runtime
authors read first. A parameter named `runtime` that is not the runtime is the
kind of thing that costs every plugin author ten minutes exactly once, forever.
Paying a one-time rename to make the trait signature self-explanatory is the
right trade on a seam meant to be extended.

## 4. The volume system's `RuntimeVolume` is renamed too, but its `driver` is left alone

**Call.** Merging `main` brought in the volume plugin system, which landed two
names in the space this rename touches. `RuntimeVolume` — the typed mount handed
to `prepare`/`start`, hanging off `TaskInstance.volumes` — becomes
`MountedVolume`. The volume plugin axis (`driver: 'host'`) is left exactly as it
is.

```rust
pub struct TaskInstance {
    pub volumes: Vec<MountedVolume>,   // renamed: it is what the worker mounted
    ...
}
```

```jsonnet
volumes: [{ name: 'data', driver: 'host', path: '/data' }],  // untouched
```

**Alternatives.** (a) Leave `RuntimeVolume` — it is main's name, freshly landed,
and touching it widens a rename PR into someone else's subsystem. (b) Also
rename volume `driver` → something else for one-word-one-concept purity, or
conversely adopt `driver` for task runtimes to match.

**Trade-off.** Renaming `RuntimeVolume` puts this PR's hands on code it did not
otherwise need to touch, and anyone with in-flight work against the volume
branch gets a conflict on the type name. Leaving `driver` means Legion now has
two words for "pluggable component": `runtime` for what runs a task, `driver`
for what provisions a volume.

**Why (simplicity).** `RuntimeVolume` is the same collision this PR exists to
remove — it reads as "task-runtime volume" when it means "a volume as mounted for
the running instance", and it sits on the struct that was just renamed away from
`Runtime` for that exact reason. Leaving it would have reintroduced the
ambiguity one field below where it was fixed. Seven call sites, all mechanical.
`driver` stays because the two words are genuinely doing different jobs at
different layers, and "volume driver" is the industry term (Docker, CSI) the way
"container runtime" is; collapsing them onto one word would lose more than it
gains. Worth revisiting only if a third pluggable axis makes the split look
arbitrary.

## 5. Only the task-execution axis is renamed; the other five `kind`s stay

**Call.** `TaskCheckKind`, `SchedulerEvent.kind`, `Eval.kind`,
`AuthMethod.kind`, `PluginManifest.kind`, and the state-inspector object kind
are untouched. A jobspec after the rename reads:

```jsonnet
web: {
  runtime: 'container',                    // renamed
  checks: [{ kind: 'http', path: '/healthz' }],  // not renamed
}
```

**Alternative.** Rename every `kind` in the codebase for consistency — a literal
reading of "switch it to runtime throughout the whole codebase", and defensible
if the goal were to retire the word entirely.

**Trade-off.** Two spellings coexist in one jobspec block, which looks
inconsistent at a glance and will draw the question in review at least once.

**Why (simplicity).** `kind` is the correct word for a closed enum of variants,
which is exactly what a check kind and an event kind are — `http`/`tcp`/`script`
is a taxonomy, not a responsibility. Only the task axis was misnamed, because
only it names a pluggable component with behavior. Renaming the others would
churn the scheduler, event stream, and auth surfaces for no gain, and would make
`runtime` the new overloaded word — repeating the original mistake with a
different token. The request was read as "rename the thing we were discussing,
everywhere it appears", not "purge the token `kind`".

## 6. One phase, not a staged migration

**Call.** The rename lands as a single change across proto, structs, storage,
plugin API, configs, tests, UI, and docs.

**Alternative.** Phase it the way `plans/README.md` normally asks — proto and
structs first, call sites second, docs third — so each step is independently
reviewable.

**Trade-off.** One large diff to review, with the mechanical bulk (test fixtures,
docs) mixed in with the load-bearing parts (migration, plugin API). Reviewer
attention is the cost.

**Why (simplicity).** The plans README asks that each phase be independently
shippable *and* valuable. No intermediate state of this rename is either: a tree
where `TaskConfig.runtime` exists but the docs, configs, and plugin API still
say `kind` is not releasable and is more confusing than either endpoint. The
phasing rule exists to avoid unshippable half-states, so following it literally
here would violate its purpose. Mitigated by ordering the work so the compiler
verifies each step (plan §8).
