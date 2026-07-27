# Writing Judgement Calls

Every non-trivial feature leaves a record here of the decisions it made. One
file per plan, named after it: `plans/<plan-name>.md` →
`calls/<plan-name>_calls.md`. If the work has no plan, name the file after the
feature (`<feature>_calls.md`). Append to the existing file when a plan already
has one — never start a second file for the same plan.

The index of current calls docs is in [`internal_docs/README.md`](../README.md);
how to write the plan itself is in [`plans/README.md`](../plans/README.md).

## What counts as one

A judgement call is any point where the work could reasonably have gone another
way: you interpreted an ambiguous request, chose between real alternatives,
deviated from the plan, or knowingly accepted a limitation. The test is whether
you can name an alternative a competent engineer would have picked. If you
can't, it isn't a judgement call — it's just the work, and it belongs in the
code or the public doc, not here.

Write them down as you make them, not at the end. The reasoning is the
perishable part; by the time the branch is green you will only remember the
conclusion.

## Rank entries by project impact

**Each doc is ordered most-impactful first, and an entry's number is its rank.**
Impact means how much of the system the decision constrains, how expensive it
would be to reverse, and how visible it is to users — *not* how long the decision
took to make. A call that fixes a public API shape, a security boundary, or a
subsystem's default behavior outranks a naming choice, even if the name was
argued about for an hour.

When you add an entry to an existing doc, insert it at its rank and renumber
rather than appending to the end; if another doc cites an entry by number, fix
the citation. State the ordering in the doc's header so readers know the order
carries meaning.

## Anatomy of an entry

Every entry answers four things, in this order:

1. **The call.** What was decided, in the heading and the opening sentence.
   Phrase it as a decision ("Fencing is two cooperating timers"), not a
   description of the area ("Fencing").
2. **The alternative(s).** What else was genuinely on the table. Name the option
   a reasonable engineer would have chosen — the version of the argument that
   could have won. "We could have done nothing" is not an alternative, and a
   strawman here makes the whole doc untrustworthy.
3. **The trade-off.** What the call costs. Every real call costs something; an
   entry with no cost reads as marketing and will be ignored by the next person.
   Say what you accepted, and — when you know it — what evidence would make you
   revisit.
4. **Why.** The reasoning that picked the winner. Tie it to one of Legion's core
   goals wherever you can:
   - **Simplicity / ease** — the headline claim is being the simplest scheduler
     to run. Calls that keep one concept where K8s or Nomad have three, avoid a
     knob, or keep something explainable at 3am land here.
   - **Extensibility** — the plugin surfaces (task kinds, schedulers, datapath,
     volumes, artifacts, rollout strategies, auth methods). So do calls that
     deliberately keep a seam *closed* for now.
   - **Performance / scale** — the critical path: scheduler and eval loop,
     status writes, staging, anything on a per-allocation or per-heartbeat path.

   When a call trades one goal against another, say which one won. That sentence
   is usually the most useful line in the doc.

## Show the UX

When a call is visible to a user, show the surface instead of describing it: the
jobspec block, the CLI invocation, the error message they will hit, the env var
name, the field that is now rejected at submit. Show before/after when the call
changed an existing surface. A reader should be able to see what the rejected
alternative would have felt like to use.

## Worked example

````markdown
## 3. Fencing is two cooperating timers, not a new lifecycle state

**Call.** `stateful.onLost` is two numbers whose relationship is enforced at
submit: the worker self-terminates stateful tasks `selfTerminateAfter` (default
90s) after losing manager contact, and the manager holds a lost node's stateful
allocations until `replaceAfter` (default 2×). Validation rejects
`replaceAfter <= selfTerminateAfter`.

```jsonnet
stateful: {
  mode: 'slot',
  onLost: { selfTerminateAfter: '90s', replaceAfter: '180s' },  // or 'never'
}
```

A gated allocation keeps its row and its instance binding; the operator sees the
computed detail `pending-loss-confirmation`, and `legion instance replace <id>`
is the force path (the K8s force-delete analog).

**Alternatives.** (a) A real `Fenced` allocation state with its own transitions
— the correct modelling answer, and what a state-machine purist would build.
(b) K8s's default: never replace without an operator force-delete.

**Trade-off.** Because there is no new state, the fencing window is a computed
diagnostic rather than something you can filter allocations by. Accepted: a new
lifecycle state touches the scheduler, the store, the UI, and every consumer of
allocation state, permanently, to describe one transient window. Known bounded
hole, documented rather than hidden: a config update can still replace
allocations on a partitioned node, bounded by `selfTerminateAfter`.

**Why (simplicity).** At-most-one is a guarantee an operator has to reason about
during an incident. Two numbers and one validation rule are explainable in a
sentence; a state machine is not. It also ships stronger than both comparators
*by default* — K8s needs a manual force-delete, Nomad's
`prevent_reschedule_on_lost` is opt-in and has no worker-side kill — which is
the simplicity claim paying off rather than costing.
````

## Keep it honest

- Record the calls that went badly and were reverted, with what the symptom was.
  A reversal is the highest-value entry in the file.
- If you accepted a security or correctness gap, say so plainly, say why the
  alternative was worse, and make sure the gap is also visible where a *user*
  would look (public doc, proto comment, or code comment at the site).
- Never quietly upgrade a limitation into a feature between the calls doc and
  the public doc. They must agree.
- Judgement calls are internal by definition — they never go in
  [`docs/`](../../docs/). Anything in them a user needs to know must also be
  stated on the public page.
