# UI: Task Page Tabs, Guided Template Launch, Actions Panel

> **Judgement calls:** [`../calls/ui-task-and-template-redesign-plan_calls.md`](../calls/ui-task-and-template-redesign-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (task page tabs) and phase 2 (guided
template launch) are built on branch `claude/peaceful-mendel-4rirac`. Phase 3
(the actions panel) waits on a layout choice between five variations of one
design (§5). Deliberately deferred: per-check, per-instance health results;
server-side run history for actions; a server-side plan/diff RPC for template
runs; the template catalog view. See "Deferred" at the end.

## Problem statement

Three UI surfaces had grown one section at a time, and the order on screen no
longer matched the order people need things in.

- **The task page is one long scroll that leads with static config.** It opens
  on Definition, Volumes, Artifacts and Service & ports; health sits in the
  middle; Exec and Logs are last. Someone who arrives from an alert — "api is
  unhealthy" — scrolls past the spec to find out which instance is failing and
  then scrolls again to read its logs.
- **The task page lists the same allocations three times.** "Reported health"
  lists them with health and ports, "Runtime allocations" lists them with
  worker, role and artifact progress, and Exec & actions lists them again, each
  with its own command box and a copy of every action form. On a three-instance
  task with four actions that is twelve action forms, and which allocation an
  action will run in is implied by which card the button sits in.
- **The template page launches blind.** It shows the variables form beside the
  full Nickel source; "Preview rendered spec" is a separate button that prints
  the whole rendered JSON; "Submit job" submits. When the rendered job already
  exists — the common case for templates like a preview environment that is
  re-launched per push — nothing says that this submit *replaces* a running
  job's config, which values changed since the last launch, or what the
  replacement does to the spec.

## Prior art

- **Nomad UI.** The allocation page splits into tabs (overview, file browser,
  logs), and exec opens in its own window. Nomad 1.7 actions appear as an
  "Actions" dropdown on the job and allocation pages; running one opens a
  flyout with output, per allocation or across all of them. We take the tabs
  and the "run across allocations" affordance; we reject the separate exec
  window — the terminal belongs next to the instance it is attached to.
- **Kubernetes Dashboard / Lens.** The pod page is metadata, conditions and
  events, with logs and exec as top-bar buttons; Lens docks logs and shells in
  a bottom panel. Events on the overview — "what changed recently" — is the
  part we take.
- **Rundeck and AWS SSM Run Command.** Both are the closest prior art for
  actions: a typed parameter form generated from a declared schema, a target
  selector (one node, a set, or a filter), and a result *per target* with its
  own exit status and output. Phase 3's shared core is this shape.
- **Backstage Software Templates.** Fill parameters → review → run, with a
  live step log. We take the three-step structure.
- **`terraform plan`, `nomad job plan`, `helm diff`.** Show what changes
  before applying it. The template Review step is a client-side version of this
  for the rendered spec; a scheduler-aware plan is deferred (§Deferred).

## Jobs to be done

**On-call responder**
- When I get paged that a service is unhealthy, I want to see which instance is
  failing and why before anything else, so I can decide whether to restart it,
  run a fix, or escalate.
- When I've found the bad instance, I want its logs and a way into it one click
  away, without scrolling past configuration I don't need right now.
- When the runbook says "flush the cache", I want to run that named step on the
  instance I pick — or on all of them — and see per instance whether it worked.

**Job author / app developer**
- When I've just deployed, I want to check the instances came up healthy and
  read what they're printing, so I know the deploy is good.
- When I need to check how a task is configured — ports, storage, artifacts,
  network policy — I want it all in one place, not interleaved with live state.

**Template consumer** (often not the person who wrote the template)
- When I launch a template, I want to know whether I'm creating something new
  or replacing a job that's already running, so I don't clobber someone's
  environment by accident.
- When I'm re-launching with slightly different values, I want to see which
  values changed and what that does to the job, so the launch holds no
  surprises.
- After launching, I want to watch it roll out and get to the job, so I know it
  worked.

**Template author** — served by the Source tab (versions, source, publish); a
richer authoring view (diff between versions, which jobs run which version) is
deferred.

*Not in scope:* the autonomous agent persona. Agents already have
`legion template render` / `run`, `legion action run`, and the MCP tools; this
plan changes only the UI.

## Goals and non-goals

**Goals**

1. The task page answers "is it healthy, and if not, which instance and why"
   first, with logs and exec one click away.
2. One list of instances on the task page, with health, placement, address and
   artifact progress together.
3. Every task-page view is addressable by URL, so a link in an alert or a chat
   can land on the logs of one allocation.
4. A template launch always says whether it creates or updates a job, and for
   an update shows changed values and the spec diff before submitting.
5. No new RPCs for phases 1 and 2.

**Non-goals**

- Redesigning the job page, the logs viewer, or the terminal.
- A template catalog or a template authoring workbench.
- Server-side action history or per-check health results (both need new state;
  see Deferred).

## Background — what exists today

- `ui/src/pages/details/TaskDetail.tsx` — one resource fetches job info,
  rollouts, the job spec, the ledger, allocation health
  (`fetchAllocationRuntime`, a state-store query over `allocation_health`),
  the live task status, volumes, service endpoints and the effective network
  policy, then renders nine `SectionCard`s top to bottom.
- `ui/src/components/AllocationExecPanel.tsx` — per-allocation exec launcher
  plus action forms from `ListJobActions`, collapsed by default.
- `ui/src/components/LogViewer.tsx` — polled log tail with stream, allocation
  and text filters.
- `ui/src/pages/details/TemplateDetail.tsx` — typed variables form,
  `RenderJobTemplate` preview, `RunJobTemplate` submit, version select,
  new-version editor, delete.
- A template run stores its source as the primary submission plus the
  resolved variables (`template.variables.json`) and the rendered JSON, and
  stamps provenance on the job (`GetJobInfoReply.template`) —
  [`docs/job-templates.md`](../../docs/job-templates.md#what-a-run-records).
- `SchedulerEvent` carries `allocation_id`, so `ListEvents { job_id }` can be
  narrowed to a task's allocations client-side.
- `ui/src/components/ui.tsx` already has an accessible `Tabs` primitive.

## Proposed design

### 1. Task page: four tabs

```
api  ● RUNNING  ● 2/3 HEALTHY                         [Logs] [Open shell]
In job web · group web · runtime container
─────────────────────────────────────────────────────────────────────────
 Overview │ Logs │ Exec & actions (4) │ Spec
```

- **Overview** — stat tiles (Instances, Health, Endpoints, Failures · 24h);
  one **Instances** table (allocation, node, status, health with the reported
  reason, ports, artifacts, and per-row **Logs** / **Shell** buttons), unhealthy
  rows first; **Health checks** (declared) beside **Recent events** (this
  task's allocations plus job- and rollout-level events).
- **Logs** — the existing `LogViewer`, full width.
- **Exec & actions** — one target picker for the task's running allocations
  and one exec/actions panel for the chosen one (replacing one panel per
  allocation). Its layout is replaced in phase 3.
- **Spec** — Definition, Service & ports (with live endpoints), Volumes,
  Artifacts, Network: the old sections, unchanged.

URL state: `/jobs/web/tasks/api?tab=logs&alloc=4219` opens the Logs tab
filtered to allocation 4219. `tab` is one of `overview` (default), `logs`,
`exec`, `spec`; an unknown value falls back to Overview, so old links keep
working.

### 2. Template page: guided launch

```
preview-env  v12                                        [New version] [Delete]
─────────────────────────────────────────────────────────────────────────
 Launch │ Source

 ① Variables ✓   ② Review ●   ③ Launch
```

- **Variables** — the typed form (unchanged input types), the version picker,
  and an "about this template" panel. **Next: review** renders the template;
  a render error stays on this step.
- **Review** — "Creates a new job `preview-feat-login`" or "Updates the running
  job `preview-feat-login` (launched from preview-env v11)". The values table
  marks values that changed since that job's last launch (`was 1`) and keys
  that are new. Below it, a unified diff of the job's current rendered spec
  against the new one, keys sorted so ordering is not noise; with no
  comparable current spec, the full rendered spec. **Launch job** / **Update
  job**.
- **Launch** — the run result, rollout progress per group (polled until it
  settles), and **Open job**, **Launch another**, **Copy as CLI**.
- **Source** tab — source of the selected version, the version list, and the
  publish-new-version editor for `template:write`.

### 3. Actions panel (phase 3 — layout pending)

The chosen direction keeps one shape: pick an action, fill its typed form,
see the exact argv it will run, choose the target (one instance, all running,
or only the unhealthy ones; tty actions run in one), and read an exit code and
output per instance, with the equivalent `legion action run …` line. Five
layouts of that shape are under review: list + detail, expand in place, cards +
side panel, command builder, and an action × instance grid. The parameter
controls are common to all of them:

| Param | Control |
| --- | --- |
| `enum`, ≤ 4 values | segmented control |
| `enum`, 5+ values | chip group |
| `int` with `min`/`max` | text input, bounds checked as you type |
| `string` with `pattern` | text input, pattern checked as you type |
| `sensitive` string | masked input; masked in the argv preview and CLI line |
| `bool` | switch |

The tab's name is open too (Actions / Operations / Controls / Commands /
Toolbox); the lean is **Actions**, the word the jobspec (`actions = [...]`),
the CLI (`legion action run`) and the ACL resource (`action`) already use.

## Proto, struct, config, and persistence changes

None for phases 1 and 2. Phase 3 as designed needs none either: the argv
preview comes from the action's `command` in the job spec (readable with
`job:read`), and fanning out to several instances is one exec session per
allocation from the browser. The deferred items below each need one.

## Implementation phases

1. **Task page tabs** — `TaskDetail.tsx` restructure; `LogViewer`
   `initialAllocation` prop; `AllocationExecPanel` `defaultOpen` prop.
2. **Guided template launch** — `TemplateDetail.tsx` stepper; a small
   dependency-free line diff in `ui/src/utils/diff.ts`.
3. **Actions panel** — replace the exec tab's body with the chosen layout;
   rename the tab.

Each phase ships alone: phase 1 keeps today's exec panel inside its tab, and
phase 2 touches only the template page.

## Testing plan

The UI has no component test runner. Each phase: `npm run typecheck` and
`npm run build`; then a manual pass against a seeded manager
(`LEGION_SEED_UI_DATA=1`, see `ui/README.md`) covering: a task with running
allocations, a task with none, a token without `state:read` (exec target list
falls back to the job's allocations), deep links with `?tab=` and `?alloc=`,
a template launch that creates a job, a re-launch that updates it, and a render
error. The diff helper is small enough to reason through by hand: identical
inputs, pure insertions, pure deletions, a change in the middle, empty sides,
and the size cap.

## Rollout and backward compatibility

UI-only. Existing task links open the Overview tab. Nothing changes for the
CLI, MCP, or the API.

## Open questions

- Phase 3 layout and tab name — waiting on the review (§3).
- Should the template page also offer the Review step's diff for a *raw*
  `legion job run` job (no template provenance)? Lean: yes when the current
  spec has a JSON submission; otherwise show the full spec, as built.

## Acceptance criteria

- Phase 1: a task page opens on Overview with health tiles and a single
  instances table; `?tab=logs&alloc=<id>` lands on that allocation's logs; the
  Exec tab shows one panel for one chosen allocation; the Spec tab shows every
  section the old page had.
- Phase 2: launching a template whose job exists says "Updates the running job"
  and shows changed values and a diff before anything is submitted; a new job
  says "Creates"; after launching, rollout progress shows and settles.
- Phase 3: an action runs on one, all, or unhealthy instances with a result per
  instance; invalid parameters block the run with the reason shown.

## Deferred

- **Per-check, per-instance health results.** `allocation_health` stores one
  healthy flag and a status string per allocation, so the Overview shows
  declared checks and the per-instance verdict, not each check's last result
  per instance.
- **Restart counts.** There is no restart counter; the Overview counts
  `allocation.failed` events in the last 24h instead.
- **Action run history** ("last run 3h ago by dana", re-run). Exec audit is a
  tracing target, not queryable state.
- **Scheduler-aware template plan.** The Review diff is of rendered specs; it
  cannot say how many allocations a change replaces. A `PlanJobTemplate` RPC
  (the template analogue of `job plan`) would.
- **Template catalog and authoring views** (usage counts, jobs behind the
  latest version, version-to-version diff).
