# UI Holistic Polish

> **Judgement calls:** [`../calls/ui-holistic-polish-plan_calls.md`](../calls/ui-holistic-polish-plan_calls.md)

**Status: SHIPPED** (2026-09-02, one phase). Deliberately deferred: embedding
the built UI in the manager binary (still the open decision in TODO.md),
jump hints past the tenth row, a toast/notification primitive, and
server-side recents per identity. See "Deferred" at the end.

## Problem statement

The UI had grown one feature at a time and it showed. Concretely, today an
operator:

- opens the Jobs list and **never sees it change**: the index pages
  (`Jobs`, `Workers`, `Rollouts`, `Managers`, `Templates`) fetched once and
  sat there, while the overview, Services and Volumes polled every 2s. A job
  that failed while the list was open stayed "running" until a reload;
- cannot **stop a job** from its page. The header offered only "Delete job",
  which the manager rejects for a running job — the CLI was the only way to
  do the one thing the page's button needed done first;
- cannot **drain a worker** or **scale a task** from the UI, although both
  RPCs (`SetNodeAvailability`, `ScaleTask`) have shipped and the CLI uses
  them;
- gets **no signal when the manager goes away** mid-session: the first
  connect is gated, but after that every card fails on its own, with no
  explanation and no retry;
- lands on the **overview for any wrong URL**, reads "Join a worker with
  `legion agent --join`" (a command that does not exist) and "Submit a job
  with `legion job submit`" (neither does that), and sees a breadcrumb that
  title-cases service names (`my-api` → `My Api`);
- has keyboard shortcuts with **no way to discover them** beyond the one
  line under an index table, and no `Ctrl+Enter` in the State Explorer off
  macOS;
- downloads a **1.6 MB script on every visit**, most of it the API
  Explorer's embedded proto sources and the terminal emulator, whether or
  not either page is ever opened;
- gets **no CI coverage** for any of this: the workflow builds Rust only, so
  a broken `npm run build` or a proto change that outruns the generated
  client is invisible until someone runs the dev server.

## Prior art

- **Nomad UI**: every list page auto-refreshes ("watch" via blocking
  queries); job pages carry Stop / Start / Scale; node pages carry Drain /
  Eligibility. We take the *set of actions* — those are the things an
  operator reaches for a dashboard to do — and the always-live lists. We
  reject Nomad's modal drain dialog with deadline options: Legion's drain
  has one policy (`stateful.onLost` per task), so a two-step confirm button
  says everything there is to say.
- **Kubernetes Dashboard / Lens**: a connection-lost banner that keeps the
  last-known state visible rather than blanking the page. Copied: contact
  loss is a banner with a retry, never a gate.
- **Linear / GitHub / Gmail**: `?` opens a shortcut sheet; the command
  palette lists recent destinations first. Both copied verbatim as
  conventions; users arrive already knowing them.
- **Vercel / Grafana**: per-page `document.title` and a favicon so tabs are
  tellable apart. Copied.
- **TanStack Query / SWR** for the fetch layer: stale-while-revalidate with
  visibility-aware refetch. We take the *behavior* (previous data stays on
  screen, hidden tabs stop polling, transient errors do not blank a page)
  and reject the library: Solid resources already retain values across
  refetches, so two small helpers (`usePoll`, `retainOnError`) cover it
  without a cache layer to explain.

## Jobs to be done

**Operator on call.** "Something is wrong; show me what changed, let me act
on it here, and tell me if I lose the control plane while I am looking."
Live lists, the Stop / Drain / Scale actions, the lost-contact banner, the
overview's recent-activity feed.

**Developer shipping a service.** "Is my job up, where is it, and what is
it logging?" The job page's created/updated times, the Scale panel, a
`Find in logs` box, endpoints that stay current, and a tab title that says
which job this tab is.

**Keyboard-first user.** "Do not make me reach for the mouse, and do not
make me memorize anything." `?` for the cheat sheet, recents in ⌘K, and
`Ctrl+Enter` that works off macOS.

**Contributor.** "Tell me in CI when I broke the UI or forgot to regenerate
the client." The `ui-build` job with the generated-client drift check,
`npm run typecheck`, `npm run gen`.

### User scenarios

1. A rollout misbehaves. The operator opens the job, presses **Stop job**,
   confirms, watches the allocations table empty out (it polls), then
   presses **Delete job**.
2. A host needs patching. The operator opens the worker, presses **Drain**,
   confirms "Evict allocations?", and sees the availability badge flip to
   `drained` with a note explaining what will and will not move.
3. Traffic spikes. The developer opens the job, types `6` into the Scale
   panel's Desired field for `web`, presses Enter, and sees "Effective 6 ·
   by operator" a few seconds later — no rollout, no new version.
4. The manager is restarted under an operator's feet. A red banner appears
   ("Lost contact with the manager … Retrying"), the tables keep their last
   rows, and the banner clears on its own when the manager returns.
5. A new teammate holds `⌘`, sees the hints, presses `?`, and has the whole
   keyboard story in one dialog.

## Goals and non-goals

**Goals**

- Every list and detail page reflects the cluster without a reload, and
  the polling story is one helper rather than five copies of `setInterval`.
- The three mutations an operator most needs from a dashboard — stop a job,
  drain a worker, scale a task — exist, gated the same way the CLI is.
- Losing the manager is reported once, in one place, without destroying
  what is on screen.
- The keyboard story is discoverable (`?`) and consistent across platforms.
- The first page load ships only what the first page needs.
- CI builds the UI and checks the generated client.

**Non-goals**

- Embedding `ui/dist` in the manager binary. That is a product decision
  with a binary-size cost on every worker, tracked separately in TODO.md.
- A notification/toast system. Inline `Note`s next to the control that
  produced them are enough for every message this work adds.
- Instance mutations (keyed create/destroy) and volume deletion in the UI —
  both stay CLI-only for the reasons their pages already state.
- A general data-fetching layer or client cache.

## Background — what exists today

- `ui/src/utils/polling.ts` was one constant; pages that polled each owned
  a `setInterval` (`Homepage.tsx`, `Services.tsx`, `Volumes.tsx`,
  `VolumeDetail.tsx`, `ServiceDetail.tsx`, `NodeDeepView.tsx`, and three
  separate tickers in `JobDetail.tsx`).
- `ui/src/components/SharedIndex.tsx` rendered the five index pages with a
  preformatted `updatedAt` string, a hard-coded "Updated" column header
  (Workers put a heartbeat under it — a deferred item from the
  job-updated-at plan), and no filter row beyond search + sort.
- `ui/src/utils/connection.ts` probed `WhoAmI` once, gating the app; nothing
  re-checked afterwards.
- `ui/src/App.tsx` owned the palette, the jump hints, and `u`; the `*` route
  rendered `Homepage`.
- `src/manager/server.rs` `list_jobs` filtered in memory (ACL, then free
  text) before paging, with no status filter and no counts.
- `.github/workflows/ci.yml` had no UI job. `ui/src/gen` had been
  regenerated recently (the drift TODO was stale) but nothing enforced it.

## Proposed design

### 1. Live data, one way

`usePoll(intervalMs)` returns a tick signal that pauses while
`document.hidden` and fires immediately on return. Pages feed it into a
resource source; Solid keeps the previous value on screen during the
refetch. Two cadences: `POLL_INTERVAL_MS` (2s) for things being watched
(allocations, health, a rollout in progress) and `INDEX_POLL_INTERVAL_MS`
(5s) for index pages and the detail pages that fan out into many RPCs.

`retainOnError(fetcher, key)` wraps a polled fetcher so a failure *after* a
success for the same key returns the last value instead of rejecting. The
first fetch (and the first after navigating to a different entity) still
rejects, so a missing job is still reported as missing. Detail pages guard
on "this is the entity in the URL" (`data()?.name === jobName()`) rather
than on `loading`, so a poll never flashes the spinner and a job→job
navigation never shows the previous job.

### 2. Actions where the question is asked

- Job page header: **Stop job** (two-step confirm; hidden once the job is
  stopped/failed/done) next to **Delete job**; both gated by `job:write`.
- Job page overview: a **Scale** card (per task: spec, override with source
  and age, effective, pending scale-in; a Desired input with Scale/Reset),
  gated by `scale:write`; keyed stateful rows read-only with the reason in
  the tooltip. Hidden entirely when `GetJobScaleStatus` is denied.
- Worker page header: **Cordon** / **Drain** (confirm) when available,
  **Uncordon** when drained; gated by `node:write` on the pool. A warn note
  explains a drained node.

All three mirror the CLI verbs one-for-one and show the RPC's reply message.

### 3. The manager going away

After the first successful connect, a watchdog probes `WhoAmI` every 10s
(3s once contact is lost; never while the tab is hidden). Loss shows a red
banner under the ACL banner with the address, how long ago, and a Retry
button; the sidebar's manager dot pulses red. Pages keep their last data.
Recovery clears the banner and re-reads the identity, in case ACLs changed
across the restart.

### 4. Finding your way

- `?` toggles a shortcut sheet listing every binding (also in ⌘K and a
  topbar button). `Esc` closes it; index keys are inert while it is open.
- ⌘K lists **Recent** detail pages first (last eight, per browser, recorded
  from the URL in the layout), then navigation, then the current index's
  rows, then **Actions** (toggle theme, shortcuts, clear recents).
- `document.title` reads innermost-first: `web · api · Jobs · Legion`.
- A favicon (the brand mark) and `theme-color`.
- An unknown URL renders a not-found card naming the path.
- The sidebar footer shows who you are (principal, source, ACL state) with
  a link to Access, above the manager address.

### 5. Jobs index: status chips

`ListJobs` gains `status` (exact, case-insensitive, applied after `query`
and before paging) and the reply gains `status_counts` (over the
ACL-filtered, query-matched set, before the status filter). The index
renders one chip per status with its count; selecting one filters the page
and the other chips keep their numbers. `legion job list --status` and the
MCP `legion_job_list` tool take the same filter.

```
$ legion job list --status failed
```

### 6. Smaller corrections

Copy: the real commands in empty states (`legion join <token>`,
`legion job run <spec.ncl>`). Breadcrumbs stop title-casing service and
volume names. Workers index shows liveness as the status and health as its
own column, plus a badge for a non-available availability, with the last
column named **Heartbeat**. Relative times everywhere carry the exact time
as a tooltip and keep aging on a shared 30s clock. `Ctrl+Enter` runs a
State Explorer query off macOS. Event chips link to the job and node.
The log viewer gets **Find in logs** with match highlighting.

### 7. Shipping less

The API Explorer, State Explorer and Observability pages and the xterm
terminal load on first use through `lazyPage`/`lazyComponent` — a
resource-backed loader rather than Solid's `lazy`, because a `Suspense`
boundary around a polled page would flash its fallback on every refetch.
The unused `d3` dependency is dropped.

### 8. DX

`npm run typecheck`, `npm run gen` (the README's protoc command), and a CI
job (`ui-build`, path-filtered on `ui/**` and `proto/legion.proto`) that
regenerates the client and fails on drift, then typechecks and builds.

## Proto, struct, config, and persistence changes

- `ListJobsRequest.status` (optional string, field 6).
- `ListJobsReply.status_counts` (map<string, uint32>, field 5).
- No storage, config, or job-spec changes. Nothing in
  `configs/jobs/maximal.ncl` changes.

## Implementation phases

One phase. Every item is independently small, but the polling helper, the
error retention, and the guards on the detail pages only make sense
together — half of them shipped would leave some pages flashing spinners
and others not.

## Testing plan

- Rust: `list_jobs_filters_by_status_and_reports_counts` (server unit test)
  covers the filter, its case-insensitivity, exactness, and that counts
  ignore the status filter but follow the query. `cargo test --all --locked`
  and clippy stay green; the ACL classification test is unaffected (no new
  RPC).
- UI: `npm run typecheck` and `npm run build`; a manual pass over every
  page against a manager started with `LEGION_SEED_UI_DATA=1 --run-worker`
  with a job submitted, in both themes, including the palette, the
  shortcut sheet, the not-found page, a job stop, a worker cordon /
  uncordon, a scale-out, and a manager restart under the banner.
- CI: the new `ui-build` job.

## Rollout and backward compatibility

Front-end only apart from two additive proto fields. An older manager
ignores `status` (the UI sends it only when a chip other than All is
selected, and then the older manager returns every status — visible, not
wrong) and returns no `status_counts` (the chip row shows only All). No
config, no migration, no persisted state beyond two localStorage keys
(`legion-recent-pages`, plus the existing theme and sidebar keys).

## Open questions

- Should the UI ship inside the binary? Unchanged: still the TODO.md item,
  and this work makes the built assets smaller without deciding it.

## Acceptance criteria

- Every index and detail page updates without a reload; no page shows a
  spinner on a refetch; a transient RPC failure on a polled page keeps the
  last data.
- Stop job, Cordon / Drain / Uncordon, and Scale work end to end against a
  running manager and are disabled (with a reason) for identities lacking
  the matching write permission.
- Killing the manager shows the banner within ~15s and restarting it clears
  the banner without a reload.
- `?` opens the sheet; ⌘K shows recents after visiting a job; a wrong URL
  shows the not-found card; the tab title names the page.
- `legion job list --status stopped` and the Jobs chips agree.
- The initial JS bundle no longer contains xterm or the proto sources.
- CI runs `ui-build` on a UI or proto change.

## Deferred

- Embedding the UI in the manager binary (product decision, TODO.md).
- Jump hints past the tenth row and on detail-page tables (unchanged from
  the jump-hints plan).
- A toast primitive, should a future feature need feedback away from the
  control that caused it.
- Recents per identity (server-side) rather than per browser.
- Restart worker from the UI (`RestartWorker` exists; it restarts the
  worker *process*, which is a bigger hammer than the page's other
  controls and wants its own confirmation copy).
