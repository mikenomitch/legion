# Job Templates — Judgement Calls

Records the decisions taken while executing
[`plans/job-templates-plan.md`](../plans/job-templates-plan.md). **Ordered by
project impact, highest first**; an entry's number is its rank.

## 1. Delegation is a `templates` selector on `job` rules, not a `template:run` action

"Only submit via template X" is expressed by scoping an ordinary `job:write`
rule with a new glob selector:

```jsonnet
[
  { resources: ['template'], actions: ['read'], templates: ['render'] },
  { resources: ['job'], actions: ['read', 'write'], templates: ['render'] },
]
```

**The alternatives.** (a) A third action verb, `template:run`, so submission
is `template:run` on the template rather than `job:write` on the job. (b) A
dedicated `dispatch` resource, mirroring Nomad's `dispatch-job` capability.

**The trade-off.** The selector approach makes the ACL vocabulary carry an
attribute (`template`) that only some endpoints populate, so a
`templates`-scoped rule silently grants nothing on endpoints that do not set
it (instance management, logs, exec — see entry 4). A `template:run` verb
would have been self-contained and impossible to under-populate. It also
means the policy takes two rules instead of one, which we tried and failed
to talk ourselves out of (see entry 7).

**Why.** `AclAction` is deliberately a two-value closed set (`read`,
`write`), and every endpoint maps onto exactly one `(resource, action)`
pair. Adding a third verb would have rippled through every table, doc, and
match in the ACL engine to serve one feature — the opposite of the
simplicity goal. The selector reuses machinery that already exists and,
critically, gets the default right for free: `selector_matches` returns
false for `(Some(selector), None)`, so a job rule carrying `templates` can
never match a raw `RunJob`. The restrictive behavior is the *engine's*
existing semantics, not a special case someone has to remember.

## 2. The manager renders Jsonnet server-side, in a sandbox

Template submission renders on the manager (imports rejected, 512 KiB
source cap, 64 KiB variables cap, 5s wall-clock budget on a dedicated
thread), where before today all Jsonnet evaluation was client-side
(`src/cmd/job.rs`).

**The alternative.** Keep rendering client-side: the CLI fetches the
template source, renders locally, and submits the result through the
existing `RunJob`. That preserves the property that the manager never
executes user-supplied code.

**The trade-off.** We accepted executing author-controlled Jsonnet in the
manager process. Jsonnet is sandboxed (no I/O, and we block `import` /
`importstr` so it cannot even read the manager's filesystem), but it is
Turing-complete and the C VM cannot be interrupted: on timeout we *abandon*
the render thread rather than kill it, so a malicious template can burn one
core until the process restarts. That is a real availability cost, gated
behind `template:write`.

**Why.** The whole point of the feature is a guided form in the UI and
thin-client submission for agents. A browser cannot run Jsonnet, so
client-side rendering would have meant either shipping a Jsonnet
implementation to the UI or giving the UI a second, weaker template format —
two divergent renderers, and the UI's preview would no longer prove what the
CLI would submit. Server-side rendering also makes the ACL story honest: the
manager sees the template a submission came through, rather than trusting a
client's claim about it.

## 3. Variables arrive as one typed object, not as `std.extVar` strings

A template declares typed variables and receives them as a single native
JSON object passed to a hidden `job(vars)::` function:

```jsonnet
{
  variables: [{ key: 'frames', type: 'number', default: 4 }],
  job(vars):: { tasks: [{ count: vars.frames }] },   // a real number
}
```

**The alternative.** PR #108's shape: a `/* legion-template {...} */` JSON
manifest in a comment, values delivered as `std.extVar` strings, so every
template does `std.parseInt(std.extVar('frames'))` and
`std.extVar('debug') == 'true'`.

**The trade-off.** The manifest is no longer greppable as literal JSON, and
reading it requires evaluating the file (the manager runs Jsonnet once at
upsert to extract it). We also lose the ability to parse metadata from a
template that does not evaluate — a broken template reports a Jsonnet error
rather than a nice "your manifest says…".

**Why.** A comment-embedded manifest scanned by regex is invisible to
Jsonnet: it cannot be computed, cannot be shared between templates, and
drifts silently from the code below it. Making the manifest *be* the
evaluated object means the declaration and the code are one artifact, and
declaring `job` as a hidden field (`::`) is what lets a plain evaluation
yield the metadata without any variable values. Typed delivery then follows
for free and removes the stringly-typed tax from every template body.

## 4. Provenance scopes reads and lifecycle writes, never spec-changing writes

The `template` scope attribute is populated from a job's stored provenance
on `ListJobs`, `GetJobInfo`, `GetJobSpec`, `GetJobSpecByConfig`,
`ListJobSpecHistory`, `StopJob`, and `DestroyJob` — but on the submit path
it is only ever the template actually being run.

**The alternative.** Populate it from provenance everywhere, including
`RunJob`. That would be uniform and would let a template-scoped account
update "its own" jobs by any means.

**The trade-off.** Endpoints we did not touch (instance management, logs,
exec, rollout advance/stop) do not populate the attribute, so a
`templates`-scoped rule grants nothing there. A delegated account that needs
those has to be given `names`-scoped rules too. The inconsistency is a wart;
it fails closed, and it is documented as a known limitation.

**Why.** If `RunJob` inherited the attribute from provenance, then the
moment a template created job `web`, any identity holding
`templates: ['that-template']` could push a *hand-written arbitrary spec* to
`web` — the exact escalation the feature exists to prevent. The asymmetry is
the security boundary: provenance answers "may you touch this job", never
"may you replace its spec".

## 5. A collision guard rejects template runs onto foreign jobs

A template run that would update an existing job whose latest config did not
come from the same template fails:

```
FAILED_PRECONDITION: job 'web' already exists and was not created from
template 'render'; updating it through this template requires job:write on
the job
```

unless the caller also holds plain `job:write` on that name.

**The alternative.** Trust the rendered name: whatever the template
produces is what gets submitted, and rely on operators to also add a `names`
selector when they care.

**The trade-off.** An extra store lookup on every template run, an error
operators can hit legitimately (adopting an existing job into a template
requires plain `job:write` once), and a rule that is invisible until you
trip it.

**Why.** Rendered job names are usually derived from user input
(`'render-' + vars.scene`). Without the guard, a template-only account could
pick `scene` such that the rendered name collides with an unrelated
production job and overwrite it — turning the delegation boundary into a
hijack primitive. Relying on operators to remember a second selector makes
the safe configuration the one you have to know about; the guard makes it
the default.

## 6. `WhoAmI` reports the caller's effective rules so the UI can gate itself

`WhoAmIReply` gained `rules_json`: the identity's flattened rules, mirrored
client-side by `ui/src/utils/acl.ts` to hide nav entries and disable
mutating controls.

**The alternatives.** (a) A `CheckAccess(resource, action, scope)` RPC the
UI calls per control. (b) Leave the UI as it was — render everything and let
denied calls fail.

**The trade-off.** The evaluation logic now exists twice, in Rust and
TypeScript, and they can drift; the TS copy is explicitly cosmetic, and the
server remains the only enforcement point. We also accepted that a
management token reports a synthetic wildcard rule rather than its real
(empty) rule set.

**Why.** A `CheckAccess` RPC would mean a network round trip per rendered
control — dozens per page — and would still need batching to be usable.
Returning rules is strictly self-introspection: it reveals nothing the
caller's own requests would not reveal. Doing nothing was the status quo the
user explicitly asked us to fix: a locked-down account was getting a UI full
of buttons that could only fail.

## 7. Browsing a template and running it are separate grants

The delegation recipe needs two rules: `template:read` to see it,
`job:write` (templates-scoped) to run it.

**The alternative.** Have a `templates`-scoped job rule imply read access to
the templates it names, collapsing the recipe to one rule.

**The trade-off.** Every delegation policy is twice as long as it feels like
it should be, and the two-rule shape is easy to get half-right (a policy
with only the job rule can run a template it cannot display, which reads as
a UI bug).

**Why.** Implicit grants in an ACL system are how people end up with access
they did not intend; the engine has no other implication rules today (`write`
does not even imply `read`), and adding the first one for ergonomics would
make the model harder to reason about at 3am. Documented as a recipe
instead. If it grates in practice, the implication is easy to add later —
the reverse is not.

## 8. Templates are named, versioned entities; versions are immutable

`legion template create render -f …` appends version N+1 and moves a latest
pointer. Versions are never overwritten, and there is no per-version delete.

**The alternative.** Mutable templates (upsert replaces the source), with
history left to whatever git the author happens to use.

**The trade-off.** Storage grows without bound for templates that are
edited often, and "delete this bad version" is not expressible — only
deleting the whole template.

**Why.** A template's version is stamped into every job it produces, and the
spec history stores the source that ran. If versions were mutable, that
provenance would be a lie: a config would claim it came from `render v1`
while `render v1` now says something else. Immutability is what makes the
audit trail worth having. Retention caps are the obvious follow-up if size
becomes real.

## 9. Trial rendering at upsert, but only when it is free

`UpsertJobTemplate` renders the template and validates the resulting jobspec
— but only when every declared variable has a default (so no values need
inventing).

**The alternative(s).** Never trial-render (validate the manifest shape
only), or always trial-render by synthesizing placeholder values for
required variables.

**The trade-off.** Templates with required variables get shape validation
only, so a body-level mistake in one surfaces at the first *submit*, in
front of whoever is running it rather than whoever wrote it.

**Why.** Synthesizing placeholders means inventing values a template may
reject for good reasons (a name that must match a pattern, an image that
must exist), producing confusing failures at publish time for templates that
are perfectly fine. Catching the fully-defaulted case is cheap and honest;
guessing is neither.

## 10. `RenderJobTemplate` requires the same access as running

Preview is gated on `job:write` for the rendered name, not on
`template:read`.

**The alternative.** Treat preview as a read: anyone who can see the
template can render it with their own values.

**The trade-off.** A user who may browse templates but not submit jobs
cannot use the preview to understand what a template does — they see the
source, not a rendered example.

**Why.** Rendering executes author-controlled code with caller-supplied
input, and the rendered spec can expose things the raw source does not (KV
references resolved, artifact pins). Gating it as a write keeps "what may I
cause to be executed" and "what may I read" from diverging, and means the
preview button in the UI is visible exactly when the submit button is.

## 11. Both submission paths share one `submit_rendered_job` helper

`RunJob` and `RunJobTemplate` now call a single private helper that does KV
resolution, ACL enforcement, validation, artifact admission, persistence,
status clears, instance seeding, and eval enqueueing.

**The alternative.** PR #108's approach: give the template path its own copy
of the submit sequence.

**The trade-off.** The helper takes six parameters and its ACL behavior is
now conditional on `template: Option<&_>`, which is more indirection than
either caller alone would want.

**Why.** The copy in PR #108 had already drifted from `RunJob` — it missed
the stopped/failed status clears, the artifact-pin event, and keyed-instance
seeding, so template-submitted jobs would behave subtly differently from
identical hand-submitted ones. One path means one place to get scheduling
semantics right, and it paid off immediately during the rebase onto main:
the new `validate_job_volumes` admission step landed in one place and
applied to both.
