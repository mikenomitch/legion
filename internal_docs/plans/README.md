# Writing a Plan

Non-trivial features get a plan in this directory *before* they get code. The
plan is where the design argument happens while it is still cheap to lose it.

The index of current plans and their statuses is in
[`internal_docs/README.md`](../README.md).

## When a plan is required

Write one when the work does any of these:

- introduces a **user-facing surface** — a jobspec block, a gRPC method, a CLI
  verb, an env var contract, a UI page;
- adds or reshapes a **subsystem**, or crosses manager / worker / storage / UI
  boundaries;
- changes a **default**, a **security boundary**, or an **on-disk or wire
  format**;
- needs a **migration**, a feature gate, or a rollout story for existing
  clusters;
- is big enough that you would want to hand a phase to someone else.

Skip it for bug fixes, refactors with no behavior change, doc-only work, and
anything that lands as one reviewable diff inside one module. When you are
genuinely unsure, write the plan — it is cheaper than relitigating the design in
review.

## Naming, status, and linking

- File name: `internal_docs/plans/<feature>-plan.md`. Something explicitly
  speculative can use `<feature>-proposal.md`.
- Add the judgement-calls pointer directly under the title — a blockquote line
  reading `**Judgement calls:**` followed by a link to
  `../calls/<feature>-plan_calls.md`.
- Register the plan in the table in [`internal_docs/README.md`](../README.md)
  with its status.
- **Keep the status honest, at the top.** `PLANNED`, `IN EXECUTION`, `SHIPPED`,
  or `SUPERSEDED`, with the deliberately-deferred items named. A plan whose
  header claims more than the code does is worse than no plan.
- **Never silently rewrite a superseded plan.** Leave it, and head it with which
  document wins now — the service-discovery set is the worked example: the
  original plan, the cookbook, and the self-critique all still exist and all
  point at the final plan.

## What a plan contains

The house shape, in order. Not every plan needs all of it — a small one can be
three sections — but skip a section because it does not apply, never because it
is hard to write.

1. **Problem statement.** What is impossible or painful *today*, concretely. If
   you cannot name what a user currently has to do instead, there may be no
   feature here.
2. **Prior art.** How Nomad, Kubernetes, Consul, or whoever solved this, with
   links, and what we take versus reject. This is the section that most often
   changes the design — and the source of the strongest arguments in the
   judgement calls that follow.
3. **Jobs to be done, by persona.** Who needs this, and what they are trying to
   get done, in plain language (see below). Follow it with the two or three
   **user scenarios** the design has to serve, written the way the user
   experiences them, not the way the code will.
4. **Goals and non-goals.** Non-goals are load-bearing: they are what stops a
   plan sprawling into seven phases nobody ships.
5. **Background — what exists today.** Real code recon, citing `src/…:line`. A
   plan that does not ground itself in the current code will propose something
   that does not fit it.
6. **Proposed design.** Numbered sections. Show the jobspec, CLI, and API
   surface as the user will actually type and read it (see below).
7. **Proto, struct, config, and persistence changes.** The concrete diff
   surface, including migrations and every new job-config field (which also goes
   into `configs/jobs/maximal.ncl`).
8. **Implementation phases.** Each phase independently shippable *and*
   independently valuable — if phase 2 is worthless without phase 5, the
   phasing is wrong.
9. **Testing plan.** Unit, integration, and regression, mapped to the phases.
10. **Rollout and backward compatibility.** What happens to existing jobs and
    clusters, what is gated, what is default-on, what an operator has to do.
11. **Open questions.** Only genuinely open ones, each with your current lean.
12. **Acceptance criteria.** Per phase, observable — something a reviewer can
    actually check.

## Break the work down into jobs to be done, per persona

Before the design, say who this is for and what they are trying to accomplish.
Legion is used by several kinds of people (and by agents), and a feature that
reads as obvious for one of them is often useless or dangerous for another.

Write each job in **simple language, from the persona's point of view**, in the
form *when \<situation>, I want to \<motivation>, so I can \<outcome>*. No
Legion vocabulary that the persona would not use themselves, no implementation
nouns, no gRPC method names. If a job cannot be stated without naming a struct,
it is a task in the design, not a job to be done.

The persona list is not fixed — pick the ones the feature actually touches, and
say when one is explicitly out of scope. The usual cast:

- **Cluster operator** — runs the managers and workers, cares about upgrades,
  quorum, capacity, and blast radius.
- **Job author / app developer** — writes the jobspec and ships the service,
  cares about getting a deploy right the first time and debugging it when it
  is not.
- **On-call responder** — arrives mid-incident with no context, cares about
  seeing what broke and stopping the bleeding.
- **Security / platform admin** — owns tokens, ACLs, namespaces, and what a
  tenant is allowed to do.
- **Autonomous agent** — drives the CLI, gRPC, or MCP surface without a human
  reading the output; cares about structured results, dry-runs, and
  idempotency (see [`docs/agent-readiness.md`](../../docs/agent-readiness.md)).

For example, on the exec/actions work:

```markdown
### Jobs to be done

**On-call responder**
- When a task is failing and the logs do not say why, I want to get a shell
  inside the exact instance that is broken, so I can look around before I
  restart it and lose the evidence.
- When I am handed a runbook step like "flush the cache", I want to run it
  without being trusted with a root shell, so a 3am mistake cannot take the
  service down.

**Job author**
- When I ship a maintenance procedure with my service, I want to hand it to
  the responder as a named button with typed inputs, so nobody has to
  remember the command.

**Cluster operator**
- When someone opens a shell in production, I want it in the audit trail with
  who, where, and when, so I can answer for it afterwards.

*Not in scope:* the autonomous agent persona — interactive exec is a human
surface, and agents get the non-interactive one-shot form instead.
```

Each job should be traceable to something later in the plan: a user scenario, a
piece of surface, a phase, or an explicit non-goal. A job nothing in the design
serves is either a missing feature or a persona you should say no to, out loud.

## Show the surface

Same rule as the judgement calls: when a decision is visible to a user, show it
rather than describe it — the jobspec block, the CLI invocation, the error they
will hit at submit. A design section with no surface in it is usually still
describing the implementation, not the feature.

## Open questions become judgement calls

When you resolve an open question during implementation, it does not just quietly
disappear from the plan: it becomes an entry in the plan's
[`_calls.md`](../calls/README.md) doc, with the alternative that lost and the
reason. Same when you deviate from the plan or knowingly accept a limitation.
The plan records the intent; the calls doc records what actually happened and
why.

## The plan is not the public doc

When the feature ships, write or refresh its page in [`docs/`](../../docs/) —
that page is the user's reference and must stand alone. The plan stays here as
history. Never leave the only description of shipped behavior inside a plan.
