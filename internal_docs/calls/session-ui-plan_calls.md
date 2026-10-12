# Judgement calls: sessions in the UI

Calls made for [session-ui-plan.md](../plans/session-ui-plan.md). Ordered by
project impact, highest first; the number is the rank.

## 1. Past ~50,000 possible sessions, the job page does not fetch the listing until asked

**The call.** The job page estimates how many sessions the job could be
running (placed envelopes × sessions one envelope admits, from the spec) and,
above `AUTO_LIST_MAX_SESSIONS` (50,000) or when it cannot estimate, shows a
card instead of calling `ListInstances`:

```
This job's 1,200 envelopes can hold up to 72,000 sessions. The manager lists
instances and sessions in a single reply with no paging, so the list is not
loaded automatically. …   [Load instances and sessions]   legion instance list agents -j
```

**Alternatives.** (a) Always fetch, cap only the rendering. (b) Fetch, and
abort the request if the reply is large.

**Trade-off.** A large session job, or any session job viewed without
`node:read`, costs one click before its instances show, including the keyed
rows that share the reply. The estimate is an upper bound, so a job that
could hold 60,000 but runs 30 also asks.

**Why.** Performance: decoding is the cost, not rendering. Building one
million `InstanceInfo` messages took ~6 s in Node; a browser decoding a
million-session reply would hang the tab before any cap applied, and (b)
cannot abort a grpc-web body partway in a useful way. The bound is from data
the page already has (ledger and spec), so it costs no request.

## 2. A listing of 5,000 or more is not re-polled

**The call.** The 2-second instance poll skips the RPC once the last reply
held `LARGE_LISTING` (5,000) rows; the card says so and has Refresh, and any
action refreshes once.

**Alternatives.** Back off the interval with size (10 s, 30 s, …).

**Trade-off.** A big listing goes stale until someone refreshes; new
sessions and ended ones do not appear on their own.

**Why.** Performance and simplicity: a slower interval still re-reads the
whole index forever from every open tab, on the manager and every peer it
fans out to. "Not live past 5,000" is one rule an operator can predict.

## 3. Render 100 rows at a time, at most 1,000; counts come from one pass

**The call.** The Instances card scans the listing once for counts and once
per filter change for the first `limit` matches, and builds table rows only
for those. "Show 100 more" stops at 1,000, then points at a filter or the
CLI.

**Alternatives.** Virtualize the table (render the visible window of all N).

**Trade-off.** You cannot scroll to row 400,000 in the UI.

**Why.** Simplicity: virtualization needs fixed row heights, and instance
rows wrap (last stop, checkpoints, actions). Nobody reads 400,000 rows; the
summary (counts, busiest nodes) plus a filter answers the actual questions.

## 4. Node, envelope, task, and mode filters live in the URL

**The call.** `?mode=session&node=w-17#instances` on the job page; the node
page, the envelope card, the task page, and the instance page link there.

**Alternatives.** A node-scoped session list on the node page, built from one
`ListInstances` per job running there.

**Trade-off.** Seeing a node's sessions takes you to the job page (and for a
big job, a Load click).

**Why.** Performance: there is no node-scoped RPC, so a node page list would
be N full-listing fetches, one per session job on the node. One page owns the
listing.

## 5. An envelope is recognized by name, confirmed by the spec when there is one

**The call.** `<task>-envelope` is an envelope when the spec has a session
task `<task>`; on the node page, which has no spec, the suffix alone decides.

**Alternatives.** Look up each allocation's task config for runtime
`envelope` (one state-store read per row).

**Trade-off.** On the node page, a non-session job's task named `x-envelope`
gets a wrong "envelope" badge. Validation refuses that name only beside a
session task `x`.

**Why.** Performance: no per-row requests. The ledger carries the task name,
not the runtime, and the rendered spec never lists the generated task.

## 6. Promote and End are on the job page's rows, unlike keyed Stop and Start

**The call.** Session rows get Promote and End inline; keyed rows keep Stop
and Start on the instance page only.

**Alternatives.** Keep the job page's rows for checkpoint actions only.

**Trade-off.** Two row layouts.

**Why.** Ease: those two are a session's whole lifecycle, and finding the
one session to keep among thousands happens on the job page.

## 7. The text filter is case-sensitive

**The call.** It matches id, node, task, and promoted-from as typed.

**Alternatives.** Lowercase both sides.

**Trade-off.** `W-17` does not find `w-17`.

**Why.** Performance: lowercasing four strings per row per keystroke cost
~40% more over a million rows, and ids are case-sensitive anyway.

## 8. Fork lineage is not looked up in a job with session tasks

**The call.** The instance page skips its `ListInstances` for a session and
for any instance of a job with a session task, and says "not looked up".

**Alternatives.** Look up with the same deferral gate as the job page.

**Trade-off.** A forked keyed instance in a session job does not show its
forks on its own page (the job page's lineage strip does).

**Why.** Simplicity: forks in session jobs are rare, and the page would need
the ledger to estimate.
