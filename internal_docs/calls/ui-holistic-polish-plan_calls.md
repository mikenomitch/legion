# UI Holistic Polish — Judgement Calls

Plan: [`../plans/ui-holistic-polish-plan.md`](../plans/ui-holistic-polish-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Mutations move into the dashboard: stop a job, drain a worker, scale a task

**Call.** The job page gets **Stop job** beside Delete, the worker page gets
**Cordon / Drain / Uncordon**, and the job overview gets a **Scale** card —
each calling the RPC the CLI verb calls, each showing the RPC's reply, each
disabled with a tooltip when the identity lacks the matching write
permission (`job:write`, `node:write` on the pool, `scale:write`). Drain and
Stop use the two-step confirm; Cordon, Uncordon and Scale are one click.

**Alternatives.** (a) Keep the UI read-mostly, as it is for volume deletion
and keyed instances, and point at the CLI. (b) Add a modal per action with
options (Nomad's drain deadline dialog).

**Trade-off.** More ways to change the cluster from a browser, with only
cosmetic gating on the client (the manager enforces). Scale-in and drain
are reversible; Stop is not, hence the confirm. No per-action options: a
drain here is always "evict", matching `legion node drain`, and a cordon is
`legion node cordon`.

**Why (simplicity).** The page already showed the button whose
precondition it could not satisfy — "Delete job" fails on a running job
and there was no Stop. The three actions are the ones an operator opens a
dashboard for during an incident; the rest (instances, volume deletion)
destroy data and keep their CLI-only gate. Mirroring the CLI one-for-one
keeps one mental model: what the button does is what the verb does.

## 2. `ListJobs` grows a server-side `status` filter and per-status counts

**Call.** `ListJobsRequest.status` (exact, case-insensitive, after `query`,
before paging) and `ListJobsReply.status_counts` (over the ACL- and
query-filtered set, *before* the status filter). The Jobs index renders
chips from the counts; `legion job list --status` and the MCP tool expose
the same filter.

```
$ legion job list --status failed
```

**Alternatives.** (a) Filter in the browser — impossible with server
paging without fetching every page. (b) A separate counts RPC. (c) Make
`query` match status words (it already substring-matches status, but
`run` would match `running`, and there is no way to ask for counts).

**Trade-off.** Two more fields on a public RPC to keep documented, and an
O(n) count pass over the in-memory job list on every call. Counts are
computed before the status filter on purpose, so a selected chip does not
zero the others.

**Why (simplicity / ease).** "Show me the failed jobs" is the first filter
anyone wants on a job list, and the counts make the filter self-describing.
The handler already held the whole list in memory for ACL filtering, so
the cost is one loop; a second RPC would have doubled the requests for
one screen.

## 3. Polling is two small helpers, not a fetch library

**Call.** `usePoll(interval)` (a tick signal that pauses while the tab is
hidden) and `retainOnError(fetcher, key)` (keep the last good value when a
later fetch for the same key fails), fed into ordinary Solid resources.
Pages guard on "this is the entity in the URL", not on `loading`.

**Alternatives.** (a) TanStack Query / a Solid query library: cache,
dedup, stale-while-revalidate, retries. (b) Leave each page with its own
`setInterval` and `refetch()`.

**Trade-off.** No request dedup across components (the job page's status
poll and the scale panel each ask separately), no shared cache across
routes, and each detail page has to write its own entity guard. A
transient failure is silent on the page — the connection banner is what
reports it — which is right for a restart and wrong if a single RPC starts
failing permanently while the manager is otherwise up (the page then
shows old data with no local hint). Accepted, and documented in the public
page.

**Why (simplicity).** Solid resources already retain the previous value on
refetch; the missing pieces were forty lines. A query library would be a
second reactive system to explain in `docs/ui-architecture.md`, and its
main win — dedup — matters at a scale this dashboard does not have.

## 4. Losing the manager is a banner, never a gate

**Call.** After the first connect, a watchdog probes `WhoAmI` (10s; 3s
while lost; paused when hidden). Loss shows a banner with the address, the
time since, and a Retry button; pages keep their data. The initial
`ConnectionGate` still gates, but its card now retries on its own every 5s
(found while testing the banner: a manager restarted under a fresh tab left
the card waiting for a click).

**Alternatives.** (a) Flip the gate back to "unreachable" on loss, hiding
the page. (b) Do nothing and let each card error.

**Trade-off.** Stale numbers stay on screen while the banner is up; the
banner says so, and the sidebar's manager dot pulses red. Recovery is
detected within ~3s, and re-reads the identity in case ACLs changed across
the restart.

**Why (ease).** An operator mid-incident loses nothing by keeping the
last-known state visible and loses a lot by having it replaced with a
card. The first probe still gates, because with no data at all a blank
page with no address and no retry is the worse failure.

## 5. Route splitting through a resource, not `lazy` + `Suspense`

**Call.** `lazyPage()` / `lazyComponent()` load a module through a resource
and render the component once it arrives; no `Suspense` anywhere.

**Alternatives.** Solid's `lazy()` with a `<Suspense>` around the outlet or
around each lazy page.

**Trade-off.** No transitions, and a page-level skeleton instead of a
finer boundary. The module promise is memoized so back-and-forth does not
re-import.

**Why (correctness first, then simplicity).** A `Suspense` boundary catches
every resource *beneath* it, and a resource that is refetching triggers
the boundary — so a polled page under Suspense flashes its fallback every
tick. The resource-backed loader sidesteps the whole class of bug. The
same reasoning already keeps `AccessGate` out of the layout.

## 6. Workers index: liveness is the status; health is a column

**Call.** The Workers list's Status column now shows `node.status`
(ready / lost / down), with the health-check summary in its own Health
column and a badge when availability is not `available`; the last column
is labelled **Heartbeat**.

**Alternatives.** Keep the health summary as the status (the previous
behavior), which is what the overview's stat cards derive from.

**Trade-off.** One more column, and the two "status-ish" badges sit side by
side. A lost node with stale-but-passing checks used to read "healthy"; now
it reads `lost` — the answer that matters.

**Why (ease).** The overview already laid the two out as separate columns;
the index disagreed with it, and put a heartbeat under a header that said
"Updated" (the deferred item from the job-updated-at plan). Now both pages
say the same thing.

## 7. Recents and the shortcut sheet are per browser, in localStorage

**Call.** The last eight detail pages are recorded from the URL in the
layout and listed first in ⌘K; `?` opens a static sheet whose rows are
hand-written from the real bindings.

**Alternatives.** (a) Recents per identity on the manager (a KV key or a
new RPC). (b) Derive the sheet from a keymap registry so it cannot drift.

**Trade-off.** Recents do not follow you between browsers, and the sheet
must be edited by hand when a binding changes (its header comment says
so). A registry would have been a refactor of three key handlers for a
list of nine rows.

**Why (simplicity).** Zero server surface, zero permissions to reason
about, and the feature is a convenience, not a record.

## 8. Relative times keep aging on one shared clock

**Call.** `RelativeTime` renders "3 minutes ago" with the exact time as the
tooltip and re-renders on a single 30s app-wide clock; `SharedIndex` takes
the raw timestamp instead of a preformatted string.

**Alternatives.** Absolute timestamps in tables; or relative text computed
once at render (the previous behavior, which read "2 seconds ago" for an
hour on a page that never refetched).

**Trade-off.** One more thing to know about (`now()` in `utils/polling`),
and audit-style surfaces (raft history) that arguably want absolute times
now show relative text with the absolute in the tooltip.

**Why (ease).** A dashboard's question is "how long ago", the tooltip
answers "when exactly", and one clock is cheaper than a timer per page.

## 9. CI regenerates the client with the runner's `protoc` and fails on drift

**Call.** The `ui-build` job installs `protobuf-compiler` from apt, runs
`npm run gen` with the pinned `protoc-gen-es` / `protoc-gen-connect-es`,
and `git diff --exit-code`s `ui/src/gen`.

**Alternatives.** (a) Trust contributors to run the README command. (b)
Pin `protoc` through `buf` or a downloaded release in CI.

**Trade-off.** The check assumes the apt `protoc` (3.21 on ubuntu-latest,
the same as the dev container this shipped from) produces the same
descriptors the pinned plugin turns into identical TypeScript. A future
runner image could bump `protoc` and produce a false drift; the fix then is
to pin, not to drop the check.

**Why (extensibility of the workflow).** The TODO entry that motivated this
turned out stale — the client had been regenerated — but nothing stopped it
from drifting again. A proto change now cannot merge without its client.

## 10. The UI stays served from disk, not embedded in the binary

**Call.** No change to how the UI is served. The bundle is split so the
first load is a third of what it was, which helps either way.

**Alternatives.** Embed `ui/dist` with `rust-embed` and serve it from the
manager listener.

**Trade-off.** Operators still run Vite or a static server for the UI.

**Why.** TODO.md frames embedding as a decision to make ("it costs binary
size on workers that never serve a UI"), not a task to do; a UI polish
change is the wrong place to make it.
