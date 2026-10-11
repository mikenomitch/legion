import { A, useParams, useSearchParams } from '@solidjs/router'
import type { JobAction, LedgerAllocation, RolloutAllocationStatus, SchedulerEvent } from '../../gen/legion_pb'
import { AccessGate } from '../../components/AccessGate'
import { For, Index, Match, Show, Switch, createMemo, createResource } from 'solid-js'
import {
  Badge,
  Button,
  DefList,
  Dot,
  EmptyState,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  Spinner,
  StatCard,
  StatusBadge,
  TableShell,
  Tabs,
  cx,
  linkClass,
  selectClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
  type TabItem,
} from '../../components/ui'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../../utils/polling'
import { EffectivePolicyView, parseEffectivePolicy } from '../../components/NetPolicyView'
import AllocationExecPanel from '../../components/AllocationExecPanel'
import { AllocationPorts, ServiceEndpointsTable, ServiceTags } from '../../components/ServiceEndpoints'
import {
  allocationHealthLabel,
  fetchAllocationRuntime,
  fetchTaskConfigIds,
  fetchTaskStatusByName,
  parseAllocNetwork,
  type AllocNetwork,
  type AllocationRuntime,
} from '../../utils/allocations'
import {
  artifactProgressPercent,
  buildTaskInstancesByName,
  extractJobSpecData,
  parseArtifactStates,
  shortenArtifactDigest,
  type ArtifactStateRow,
  type JobTaskArtifact,
  type JobTaskCheck,
} from '../../utils/jobspec'
import { allows } from '../../utils/acl'
import { pluralize, toDate } from '../../utils/format'
import type { Tone } from '../../utils/status'
import { shortVolumeId, volumePinsPlacement, volumeStateTone } from '../../utils/volumes'

// Checksum column value: pinned sources carry their digest in the source
// (`…@sha256:…`); otherwise fall back to the declared checksum.
const declaredArtifactDigest = (artifact: JobTaskArtifact): string | undefined => {
  if (artifact.pinnedFrom) {
    const separator = artifact.source.lastIndexOf('@')
    if (separator >= 0) return artifact.source.slice(separator + 1)
  }
  return artifact.checksum
}

const formatMb = (bytes: number): string => `${(bytes / (1024 * 1024)).toFixed(1)} MB`

const artifactProgressLabel = (row: ArtifactStateRow): string | undefined => {
  if (row.bytesFetched === undefined || row.bytesTotal === undefined) return undefined
  const percent = artifactProgressPercent(row)
  const prefix = percent === undefined ? '' : `${percent}% · `
  return `${prefix}${formatMb(row.bytesFetched)} / ${formatMb(row.bytesTotal)}`
}

const formatDurationMs = (ms: number): string => (ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`)

const TASK_TABS = ['overview', 'logs', 'exec', 'spec'] as const
type TaskTab = (typeof TASK_TABS)[number]
const isTaskTab = (value: unknown): value is TaskTab =>
  typeof value === 'string' && (TASK_TABS as readonly string[]).includes(value)

// The events card and the failure count read the job's most recent events;
// a busy job can push a day's worth past this window.
const EVENT_FETCH_LIMIT = 200
const RECENT_EVENT_ROWS = 8
const DAY_MS = 24 * 60 * 60 * 1000

const runnableStatuses = new Set(['placed', 'active', 'running'])

// One allocation of this task, joined from the ledger (placement, ports,
// liveness), the state store (health) and the latest rollout (role,
// artifact staging). `key` is the id as a string — ids are bigint on the
// wire, while the health map is keyed by Number(id).
type InstanceRow = {
  key: string
  id: bigint
  nodeId: string
  status: string
  // Running per the ledger. Rows only the rollout knows (a replaced
  // replacee, a finished run) are kept for the record; they count as running
  // only when the ledger itself could not be read.
  running: boolean
  health?: AllocationRuntime
  network?: AllocNetwork
  artifacts: ArtifactStateRow[]
  rollout?: RolloutAllocationStatus
  instanceId?: string
}

// A rollout row carries its own health snapshot; used when the state-store
// read that normally supplies health was denied.
const rolloutHealth = (allocation: RolloutAllocationStatus | undefined): AllocationRuntime | undefined =>
  allocation
    ? { taskName: allocation.taskName, healthy: allocation.allocationHealthy, status: allocation.allocationHealth ?? '' }
    : undefined

// Unhealthy first (that is what someone opening this page is looking for),
// then not-yet-reported, then healthy, then rows that are no longer running.
const instanceRank = (row: InstanceRow): number => {
  if (!row.running) return 3
  if (row.health?.healthy === false) return 0
  if (row.health?.healthy === undefined) return 1
  return 2
}

const compareInstances = (left: InstanceRow, right: InstanceRow): number =>
  instanceRank(left) - instanceRank(right) || (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)

const instanceTooltip = (row: InstanceRow): string =>
  [
    row.nodeId ? `worker ${row.nodeId}` : undefined,
    row.rollout ? `group ${row.rollout.taskGroup}` : undefined,
    row.rollout ? `lifecycle ${row.rollout.lifecycle}` : undefined,
    row.rollout?.role ? `role ${row.rollout.role}` : undefined,
    row.instanceId ? `instance ${row.instanceId}` : undefined,
  ]
    .filter(Boolean)
    .join(' · ')

const instanceSecondary = (row: InstanceRow): string =>
  [row.rollout?.role, row.instanceId ? `instance ${row.instanceId}` : undefined].filter(Boolean).join(' · ')

type HealthSummary = { healthy: number; unhealthy: number; unknown: number; total: number; tone: Tone }

// "Not reported yet" is not a failure (see allocationHealthLabel), so a task
// whose instances have simply not reported is a warning, not an alarm.
const summarizeHealth = (rows: readonly InstanceRow[]): HealthSummary | undefined => {
  if (rows.length === 0) return undefined
  const healthy = rows.filter((row) => row.health?.healthy === true).length
  const unhealthy = rows.filter((row) => row.health?.healthy === false).length
  const total = rows.length
  const tone: Tone = healthy === total ? 'ok' : healthy === 0 && unhealthy > 0 ? 'danger' : 'warn'
  return { healthy, unhealthy, unknown: total - healthy - unhealthy, total, tone }
}

const checkTarget = (check: JobTaskCheck): string =>
  [check.port ? `port ${check.port}` : undefined, check.path].filter(Boolean).join(' ')

const checkTiming = (check: JobTaskCheck): string =>
  [
    check.every ? `every ${check.every}` : undefined,
    check.timeout ? `timeout ${check.timeout}` : undefined,
    check.unhealthyAfterCount === undefined ? undefined : `unhealthy after ${pluralize(check.unhealthyAfterCount, 'failure')}`,
  ]
    .filter(Boolean)
    .join(' · ')

const eventTone = (level: string): Tone => (level === 'error' ? 'danger' : level === 'warn' ? 'warn' : 'info')

// Allocation and task-config ids an event names. Single-allocation events
// carry `allocation_id` plus a `taskConfigId` in their metadata; batch events
// (placed, stopping, preempted) list both per allocation.
const eventSubjects = (event: SchedulerEvent): { allocations: string[]; taskConfigs: string[] } => {
  const allocations = event.allocationId === undefined ? [] : [String(event.allocationId)]
  const taskConfigs: string[] = []
  const note = (item: unknown) => {
    if (!item || typeof item !== 'object') return
    const entry = item as Record<string, unknown>
    if (typeof entry.allocationId === 'number') allocations.push(String(entry.allocationId))
    if (typeof entry.taskConfigId === 'number') taskConfigs.push(String(entry.taskConfigId))
  }
  try {
    const meta = JSON.parse(event.metadataJson || '{}') as Record<string, unknown>
    note(meta)
    for (const list of [meta.placements, meta.allocations]) {
      if (Array.isArray(list)) list.forEach(note)
    }
  } catch {
    // Metadata only widens the match; the allocation id still applies.
  }
  return { allocations, taskConfigs }
}

const artifactRowDetail = (row: ArtifactStateRow): string =>
  [
    `${row.name ?? row.source}: ${row.state}`,
    row.digest ? shortenArtifactDigest(row.digest) : undefined,
    row.durationMs === undefined ? undefined : formatDurationMs(row.durationMs),
    row.state === 'failed' ? row.error : undefined,
  ]
    .filter(Boolean)
    .join(' · ')

// One table cell's worth of artifact staging: a failure first (the error on
// hover), then progress while fetching, then how many are ready. The
// per-artifact detail is in the tooltip.
function ArtifactSummary(props: { rows: readonly ArtifactStateRow[] }) {
  const failed = () => props.rows.filter((row) => row.state === 'failed')
  const staged = () => props.rows.filter((row) => row.state === 'staged').length
  const inFlight = () => props.rows.find((row) => row.state !== 'staged' && row.state !== 'failed')
  const detail = () => props.rows.map(artifactRowDetail).join('\n')
  return (
    <Show when={props.rows.length > 0} fallback={<span class="text-faint">—</span>}>
      <Switch
        fallback={
          <span class="text-xs text-muted" title={detail()}>
            {staged()}/{props.rows.length} ready
          </span>
        }
      >
        <Match when={failed().length > 0}>
          <Badge tone="danger" dot title={failed().map((row) => `${row.name ?? row.source}: ${row.error ?? 'failed'}`).join('\n')}>
            {failed().length} failed
          </Badge>
        </Match>
        <Match when={inFlight()}>
          {(row) => (
            <span class="text-xs text-muted" title={detail()}>
              {row().state} · {artifactProgressLabel(row()) ?? `${staged()}/${props.rows.length} ready`}
            </span>
          )}
        </Match>
      </Switch>
    </Show>
  )
}

function TaskDetailContent() {
  const params = useParams()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const taskName = createMemo(() => decodeURIComponent(params.taskName))

  // Tabs live in the URL so a link can land on one (`?tab=logs`), and
  // `?alloc=` carries the instance a row's Logs/Shell button picked. Replace,
  // not push: flipping tabs should not fill the back button.
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = createMemo<TaskTab>(() => (isTaskTab(searchParams.tab) ? searchParams.tab : 'overview'))
  const selectTab = (tab: string) => setSearchParams({ tab: tab === 'overview' ? undefined : tab }, { replace: true })
  const openTab = (tab: TaskTab, allocation?: string) =>
    setSearchParams({ tab: tab === 'overview' ? undefined : tab, alloc: allocation }, { replace: true })

  // Keyed on the route params so task→task navigation refetches, and polled
  // at the slow cadence so health, endpoints and allocations stay current.
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [data] = createResource(
    () => [jobName(), taskName(), tick()] as const,
    retainOnError(async ([job, task]: readonly [string, string, number]) => {
    const [jobInfo, rollouts, spec, ledger, allocationHealth, liveStatus, volumes, actions, events, taskConfigIds, scale] = await Promise.all([
      managerClient.getJobInfo({ name: job }),
      managerClient.listRollouts({ jobId: job, activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: job }),
      // node:read garnish (allocation placement); job-scoped tokens
      // still get the rest of the page.
      managerClient.getLedger({}).catch(() => undefined),
      fetchAllocationRuntime(job),
      // The same live status the job page's task cards show, so the two
      // pages never disagree about whether this task is running.
      fetchTaskStatusByName(job),
      // Actual volumes rather than the declaration: this page should answer
      // "where is my data and is it healthy", which the spec cannot.
      managerClient.listVolumes({ jobId: job }).catch(() => ({ volumes: [] })),
      // Only for the Exec tab's count; the exec panel loads its own copy.
      managerClient
        .listJobActions({ job })
        .then((reply) => reply.actions.filter((action) => action.taskName === task || action.taskName === ''))
        .catch(() => [] as JobAction[]),
      // operator:read garnish; undefined (not empty) when the token cannot
      // read events, so the page can say "unavailable" rather than "none".
      allows('operator', 'read')
        ? managerClient
            .listEvents({ jobId: job, limit: EVENT_FETCH_LIMIT })
            .then((reply): SchedulerEvent[] | undefined => reply.events)
            .catch(() => undefined)
        : Promise.resolve(undefined),
      fetchTaskConfigIds(job, task),
      // scale:read garnish: an operator's `job scale` override is the count
      // the scheduler actually converges to, not the spec's.
      managerClient
        .getJobScaleStatus({ name: job })
        .then((reply) => reply.tasks.find((entry) => entry.taskName === task))
        .catch(() => undefined),
    ])

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined

    const taskSpec = extractJobSpecData(spec.submissions)?.tasks.find((entry) => entry.name === task)
    const instancesByTask = buildTaskInstancesByName(rolloutStatus?.groups ?? [])
    const taskInstances = instancesByTask.get(task)

    // All running allocations for this job (from the ledger), regardless of
    // whether a rollout produced them. The exec panel targets these; the
    // manager rejects exec into an allocation whose task != this page's task.
    const jobAllocations = (ledger?.nodes ?? [])
      .flatMap((node) => node.allocations)
      .filter((alloc) => alloc.jobId === job && runnableStatuses.has(alloc.status))

    // Endpoints for the service this task declares (if any), unhealthy
    // included so a failing endpoint is visible rather than absent.
    const serviceName = taskSpec?.service ? (taskSpec.service.name ?? task) : undefined
    const endpoints = serviceName
      ? await managerClient
          .resolveService({ service: serviceName, includeUnhealthy: true })
          .then((reply) => reply.endpoints.filter((endpoint) => endpoint.taskName === task))
          .catch(() => [])
      : []

    // The task's effective network policy (docs/network-handlers.md): stored
    // policies merged with its own block, each rule naming its source. A
    // job-scoped token without net:read simply gets no section.
    const netPolicy = await managerClient
      .showNetPolicy({ jobId: job, taskName: task, nodePool: '' })
      .catch(() => undefined)

    return {
      key: `${job}/${task}`,
      job: jobInfo.job,
      netPolicy,
      taskSpec,
      taskInstances,
      jobAllocations,
      ledgerAvailable: ledger !== undefined,
      latestRollout,
      groups: rolloutStatus?.groups ?? [],
      allocationHealth,
      liveStatus: liveStatus.get(task),
      serviceName,
      endpoints,
      // Tombstones belong on the Volumes page (which has a filter for them);
      // here they are just noise between the reader and their live storage.
      volumes: volumes.volumes.filter(
        (volume) => volume.taskName === task && volume.state !== 'destroyed',
      ),
      actions,
      events,
      taskConfigIds,
      scale,
    }
  }, ([job, task]) => `${job}/${task}`),
  )

  // The ledger identifies allocations by task_config_id only, so the task name
  // comes from the same state-store lookup that carries health — or, for a
  // token whose state-store read was denied, from the latest rollout.
  const taskAllocations = createMemo((): LedgerAllocation[] => {
    const current = data()
    if (!current) return []
    const inRollout = new Set((current.taskInstances?.allocations ?? []).map((allocation) => String(allocation.allocationId)))
    return current.jobAllocations.filter(
      (alloc) => current.allocationHealth.get(Number(alloc.id))?.taskName === taskName() || inRollout.has(String(alloc.id)),
    )
  })

  // Allocations the exec panel may target. The manager rejects exec into an
  // allocation whose task is not this page's task, so a sibling task's
  // allocation is a dead panel — and on a multi-task job it renders this
  // task's whole action form set once per sibling allocation. Filter to this
  // task whenever the task↔allocation mapping is available; a job-scoped token
  // without `state:read` gets an empty map, and there we still show the job's
  // allocations rather than an empty section (the manager is the real gate).
  const execAllocations = createMemo(() => {
    const runtime = data()?.allocationHealth
    const allocations = data()?.jobAllocations ?? []
    if (!runtime || runtime.size === 0) return allocations
    return allocations.filter((alloc) => {
      const known = runtime.get(Number(alloc.id))
      return known === undefined || known.taskName === taskName()
    })
  })
  // Strings, not ledger rows: rows are new objects every poll, and the target
  // picker's options must survive a poll for the selection to hold.
  const execTargetIds = createMemo(() => execAllocations().map((alloc) => String(alloc.id)))

  // The exec target: the URL's pick while it is still running, else whatever
  // was already showing (a poll must never swap the terminal out from under
  // someone), else the first unhealthy allocation — the likeliest reason to
  // open a shell — else the first.
  const execTarget = createMemo<string | undefined>((previous) => {
    const ids = execTargetIds()
    const wanted = searchParams.alloc
    if (wanted && ids.includes(wanted)) return wanted
    if (previous && ids.includes(previous)) return previous
    const unhealthy = execAllocations().find((alloc) => data()?.allocationHealth.get(Number(alloc.id))?.healthy === false)
    return unhealthy ? String(unhealthy.id) : ids[0]
  })

  const execOptionLabel = (id: string): string => {
    const alloc = execAllocations().find((candidate) => String(candidate.id) === id)
    const health = allocationHealthLabel(data()?.allocationHealth.get(Number(id)))
    return [`#${id}`, alloc?.nodeId, health].filter(Boolean).join(' · ')
  }

  // One row per allocation: the old "reported health", "runtime allocations"
  // and per-allocation exec lists were the same set three times.
  const instanceRows = createMemo((): InstanceRow[] => {
    const current = data()
    if (!current) return []
    const rolloutById = new Map(
      (current.taskInstances?.allocations ?? []).map((allocation) => [String(allocation.allocationId), allocation] as const),
    )
    const rows: InstanceRow[] = taskAllocations().map((alloc) => {
      const key = String(alloc.id)
      const rollout = rolloutById.get(key)
      rolloutById.delete(key)
      return {
        key,
        id: alloc.id,
        nodeId: alloc.nodeId,
        status: alloc.status,
        running: true,
        health: current.allocationHealth.get(Number(alloc.id)) ?? rolloutHealth(rollout),
        network: parseAllocNetwork(alloc.network),
        // The rollout's copy is the one the old runtime card showed; the
        // ledger carries the same heartbeat payload for rollout-less deploys.
        artifacts: parseArtifactStates(rollout?.artifactStates ?? alloc.artifactStates),
        rollout,
        instanceId: alloc.instanceId,
      }
    })
    for (const [key, rollout] of rolloutById) {
      rows.push({
        key,
        id: rollout.allocationId,
        nodeId: rollout.nodeId,
        status: rollout.status,
        running: !current.ledgerAvailable && runnableStatuses.has(rollout.status),
        health: current.allocationHealth.get(Number(rollout.allocationId)) ?? rolloutHealth(rollout),
        artifacts: parseArtifactStates(rollout.artifactStates),
        rollout,
      })
    }
    return rows.sort(compareInstances)
  })

  const liveInstances = createMemo(() => instanceRows().filter((row) => row.running))
  const healthSummary = createMemo(() => summarizeHealth(liveInstances()))
  const execIds = createMemo(() => new Set(execTargetIds()))

  const logAllocationIds = createMemo(() => instanceRows().map((row) => row.id))
  const logAllocation = () => {
    const wanted = searchParams.alloc
    return wanted && instanceRows().some((row) => row.key === wanted) ? wanted : undefined
  }

  // The count the scheduler converges to: a scale override when there is
  // one, else the spec's. Plain counts only — `per` multiplies by the
  // cluster's attribute values, a keyed stateful task's instances are created
  // on demand, and a batch task is not meant to stay running, so none of
  // those has a target to compare against.
  const desiredCount = createMemo(() => {
    const current = data()
    const spec = current?.taskSpec
    if (!spec || spec.count?.per || spec.lifecycle === 'batch' || spec.stateful?.mode === 'keyed') return undefined
    return current?.scale?.effectiveCount ?? spec.count?.num
  })

  // Events about this task, newest first: anything naming one of its
  // allocations or task configs, plus job- and rollout-level events. Matching
  // on task config matters because a failed allocation is deleted as soon as
  // its failure is reported, so its id is not in any list this page has.
  const taskEvents = createMemo(() => {
    const current = data()
    if (!current?.events) return undefined
    const allocationIds = new Set(instanceRows().map((row) => row.key))
    const taskConfigIds = new Set(current.taskConfigIds)
    for (const alloc of taskAllocations()) taskConfigIds.add(String(alloc.taskConfigId))
    for (const allocation of current.taskInstances?.allocations ?? []) taskConfigIds.add(String(allocation.taskConfigId))
    return current.events
      .filter((event) => {
        if (event.kind.startsWith('job.') || event.kind.startsWith('rollout.')) return true
        const subjects = eventSubjects(event)
        return (
          subjects.allocations.some((id) => allocationIds.has(id)) ||
          subjects.taskConfigs.some((id) => taskConfigIds.has(id))
        )
      })
      .reverse()
  })

  // `partial` when the fetched window is full and does not reach back a whole
  // day: the count is then a floor, and the card says so.
  const recentFailures = createMemo(() => {
    const fetched = data()?.events
    const events = taskEvents()
    if (!fetched || !events) return undefined
    const since = Date.now() - DAY_MS
    const oldest = toDate(fetched[0]?.createdAt)?.getTime() ?? 0
    return {
      events: events.filter(
        (event) => event.kind === 'allocation.failed' && (toDate(event.createdAt)?.getTime() ?? 0) >= since,
      ),
      partial: fetched.length >= EVENT_FETCH_LIMIT && oldest > since,
    }
  })

  // Sticky: once the exec tab has been shown it stays mounted (see below).
  const execOpened = createMemo((opened: boolean) => opened || activeTab() === 'exec', false)

  // A memo on the count alone, so a poll does not rebuild the tab bar (and
  // drop keyboard focus) unless the count actually changed.
  const actionCount = createMemo(() => data()?.actions.length ?? 0)
  const tabs = createMemo((): TabItem[] => [
    { id: 'overview', label: 'Overview' },
    { id: 'logs', label: 'Logs' },
    { id: 'exec', label: 'Exec & actions', count: actionCount() > 0 ? actionCount() : undefined },
    { id: 'spec', label: 'Spec' },
  ])

  return (
    <Show when={!data.error} fallback={<Note tone="danger">Failed to load task details: {String(data.error?.message ?? data.error)}</Note>}>
      <Show when={data()?.key === `${jobName()}/${taskName()}`} fallback={<div class="py-6"><Spinner label="Loading task…" /></div>}>
        <div>
          <PageHeader
            eyebrow="Task"
            title={taskName()}
            subtitle={
              <>
                In job{' '}
                <A class={linkClass} href={`/jobs/${encodeURIComponent(jobName())}`}>
                  {data()!.job?.name ?? jobName()}
                </A>
                <Show when={data()!.taskSpec}>
                  {(spec) => (
                    <>
                      {' · '}group {spec().group} · runtime {spec().runtime}
                    </>
                  )}
                </Show>
              </>
            }
            badges={
              <>
                <StatusBadge status={data()!.liveStatus || data()!.taskInstances?.statuses[0] || 'unknown'} />
                <Show when={healthSummary()}>
                  {(summary) => (
                    <Badge
                      tone={summary().tone}
                      dot
                      class="uppercase tracking-wide"
                      title={`${summary().healthy} healthy · ${summary().unhealthy} unhealthy · ${summary().unknown} not reported yet`}
                    >
                      {summary().healthy}/{summary().total} healthy
                    </Badge>
                  )}
                </Show>
              </>
            }
            actions={
              <>
                <Button onClick={() => openTab('logs')}>Logs</Button>
                <Button variant="primary" icon="terminal" onClick={() => openTab('exec')}>
                  Open shell
                </Button>
              </>
            }
          />

          <Tabs class="mb-5" tabs={tabs()} active={activeTab()} onChange={selectTab} />

          <Switch>
            <Match when={activeTab() === 'overview'}>
              <div class="space-y-4" role="tabpanel" aria-label="Overview">
                <div class={cx('grid grid-cols-2 gap-3', data()!.taskSpec?.service ? 'lg:grid-cols-4' : 'lg:grid-cols-3')}>
                  <StatCard
                    label="Instances"
                    icon="boxes"
                    value={
                      desiredCount() === undefined
                        ? String(liveInstances().length)
                        : `${liveInstances().length} / ${desiredCount()}`
                    }
                    tone={desiredCount() !== undefined && liveInstances().length < desiredCount()! ? 'warn' : undefined}
                    hint={(() => {
                      const count = data()!.taskSpec?.count
                      const scale = data()!.scale
                      if (count?.per) return `running · ${count.num} per ${count.per}`
                      if (desiredCount() === undefined) return 'running'
                      if (scale?.overrideCount !== undefined) return `running · count ${desiredCount()} (spec ${scale.specCount}, scaled)`
                      return `running · count ${desiredCount()}`
                    })()}
                  />
                  <StatCard
                    label="Health"
                    icon="stethoscope"
                    value={healthSummary() ? `${healthSummary()!.healthy} / ${healthSummary()!.total}` : '—'}
                    tone={healthSummary()?.tone}
                    hint={(() => {
                      const summary = healthSummary()
                      if (!summary) return 'nothing running'
                      const failing = liveInstances().find((row) => row.health?.healthy === false)
                      if (failing) return `#${failing.key} ${failing.health?.status || 'unhealthy'}`
                      if (summary.unknown > 0) return `${summary.unknown} not reported yet`
                      return (data()!.taskSpec?.checks.length ?? 0) > 0 ? 'all checks passing' : 'no checks declared'
                    })()}
                  />
                  <Show when={data()!.taskSpec?.service}>
                    <StatCard
                      label="Endpoints"
                      icon="radio"
                      href={data()!.serviceName ? `/services/${encodeURIComponent(data()!.serviceName!)}` : undefined}
                      value={`${data()!.endpoints.filter((endpoint) => endpoint.healthy).length} / ${data()!.endpoints.length}`}
                      tone={(() => {
                        const total = data()!.endpoints.length
                        const live = data()!.endpoints.filter((endpoint) => endpoint.healthy).length
                        if (total === 0) return undefined
                        return live === total ? 'ok' : live > 0 ? 'warn' : 'danger'
                      })()}
                      hint={`service ${data()!.serviceName} · port ${data()!.taskSpec!.service!.port}`}
                    />
                  </Show>
                  <StatCard
                    label="Failures · 24h"
                    icon="alertTriangle"
                    value={(() => {
                      const failures = recentFailures()
                      if (!failures) return '—'
                      return `${failures.events.length}${failures.partial && failures.events.length > 0 ? '+' : ''}`
                    })()}
                    tone={(recentFailures()?.events.length ?? 0) > 0 ? 'danger' : undefined}
                    hint={(() => {
                      const failures = recentFailures()
                      if (!failures) return 'event history unavailable'
                      if (failures.events.length > 0) return failures.events[0].message
                      return failures.partial
                        ? `none in the last ${EVENT_FETCH_LIMIT} job events`
                        : 'no allocation failures'
                    })()}
                  />
                </div>

                <SectionCard
                  title="Instances"
                  description="One row per allocation: placement, health, where it is reachable, and its artifacts."
                  flush
                  actions={
                    <Show when={data()!.latestRollout}>
                      <A class={linkClass + ' text-sm'} href={`/rollouts/${data()!.latestRollout!.id}`}>Open rollout</A>
                    </Show>
                  }
                >
                  <Show
                    when={instanceRows().length > 0}
                    fallback={
                      <EmptyState
                        icon="boxes"
                        title="No allocations running"
                        body="Instances appear here once the scheduler places this task."
                      />
                    }
                  >
                    <TableShell>
                      <table class={tableClass}>
                        <thead class={theadClass}>
                          <tr>
                            <th class={thClass}>Allocation</th>
                            <th class={thClass}>Node</th>
                            <th class={thClass}>Status</th>
                            <th class={thClass}>Health</th>
                            <th class={thClass}>Ports</th>
                            <th class={thClass}>Artifacts</th>
                            <th class={thClass}>
                              <span class="sr-only">Open</span>
                            </th>
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-line">
                          {/* Index, not For: rows are rebuilt every poll, and
                              re-creating them would drop focus from a row's
                              buttons every few seconds. */}
                          <Index each={instanceRows()}>
                            {(row) => (
                              <tr class={cx(trHoverClass, row().running && row().health?.healthy === false && 'bg-red-500/5')}>
                                <td class={tdClass} title={instanceTooltip(row())}>
                                  <span class="font-mono text-xs font-semibold">#{row().key}</span>
                                  <Show when={instanceSecondary(row())}>
                                    <div class="text-2xs text-faint">{instanceSecondary(row())}</div>
                                  </Show>
                                </td>
                                <td class={tdClass}>
                                  <Show when={row().nodeId} fallback={<span class="text-faint">—</span>}>
                                    <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(row().nodeId)}`}>
                                      {row().nodeId}
                                    </A>
                                  </Show>
                                </td>
                                <td class={tdClass}>
                                  <StatusBadge status={row().status} />
                                </td>
                                <td class={tdClass}>
                                  <StatusBadge status={allocationHealthLabel(row().health)} title={row().health?.status || undefined} />
                                  <Show when={row().health?.healthy === false && row().health?.status}>
                                    <div class="mt-0.5 max-w-[18rem] truncate text-2xs text-red-600 dark:text-red-400" title={row().health!.status}>
                                      {row().health!.status}
                                    </div>
                                  </Show>
                                </td>
                                <td class={tdClass}>
                                  <AllocationPorts network={row().network} />
                                </td>
                                <td class={tdClass}>
                                  <ArtifactSummary rows={row().artifacts} />
                                </td>
                                <td class={tdClass + ' whitespace-nowrap text-right'}>
                                  <Button
                                    variant="ghost"
                                    size="xs"
                                    onClick={() => openTab('logs', row().key)}
                                    aria-label={`Logs for allocation #${row().key}`}
                                  >
                                    Logs
                                  </Button>
                                  <Show when={execIds().has(row().key)}>
                                    <Button
                                      variant="ghost"
                                      size="xs"
                                      icon="terminal"
                                      onClick={() => openTab('exec', row().key)}
                                      aria-label={`Open a shell in allocation #${row().key}`}
                                    >
                                      Shell
                                    </Button>
                                  </Show>
                                </td>
                              </tr>
                            )}
                          </Index>
                        </tbody>
                      </table>
                    </TableShell>
                  </Show>
                </SectionCard>

                <div class="flex flex-wrap gap-4">
                  <SectionCard
                    class="min-w-0 flex-[1_1_26rem]"
                    title="Health checks"
                    description="What the worker probes on every instance; each one's latest result is in the table above."
                    flush
                  >
                    <Show
                      when={data()!.taskSpec}
                      fallback={<p class="px-4 py-3 text-sm text-muted">The current job spec does not declare this task.</p>}
                    >
                      <Show
                        when={data()!.taskSpec!.checks.length > 0}
                        fallback={
                          <p class="px-4 py-3 text-sm text-muted">
                            No checks declared — the task counts as healthy as soon as it is running.
                          </p>
                        }
                      >
                        <ul class="divide-y divide-line">
                          <For each={data()!.taskSpec!.checks}>
                            {(check) => (
                              <li class="px-4 py-2.5">
                                <p class="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
                                  <span class="font-medium text-ink">{check.name ?? check.type}</span>
                                  <Badge tone="neutral">{check.kind}</Badge>
                                  <Show when={checkTarget(check)}>
                                    <span class="font-mono text-xs text-muted">{checkTarget(check)}</span>
                                  </Show>
                                </p>
                                <Show when={checkTiming(check)}>
                                  <p class="mt-0.5 text-xs text-faint">{checkTiming(check)}</p>
                                </Show>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </Show>
                  </SectionCard>

                  <SectionCard
                    class="min-w-0 flex-[1_1_26rem]"
                    title="Recent events"
                    description="Placements, failures and rollouts touching this task."
                    flush
                    actions={
                      <Show when={taskEvents()}>
                        <A class={linkClass + ' text-sm'} href="/events">All events →</A>
                      </Show>
                    }
                  >
                    <Show
                      when={taskEvents()}
                      fallback={<p class="px-4 py-3 text-sm text-muted">Events are not available to this token.</p>}
                    >
                      {(events) => (
                        <Show
                          when={events().length > 0}
                          fallback={<p class="px-4 py-3 text-sm text-muted">No recent events for this task.</p>}
                        >
                          <ol class="divide-y divide-line">
                            <For each={events().slice(0, RECENT_EVENT_ROWS)}>
                              {(event) => (
                                <li class="flex items-start gap-2.5 px-4 py-2 text-sm leading-5">
                                  <span class="flex h-5 shrink-0 items-center">
                                    <Dot tone={eventTone(event.level)} />
                                  </span>
                                  <span class="min-w-0 flex-1 break-words text-ink" title={event.kind}>
                                    {event.message}
                                  </span>
                                  <RelativeTime value={event.createdAt} class="shrink-0 whitespace-nowrap text-xs leading-5 text-faint" />
                                </li>
                              )}
                            </For>
                          </ol>
                        </Show>
                      )}
                    </Show>
                  </SectionCard>
                </div>
              </div>
            </Match>

            <Match when={activeTab() === 'logs'}>
              <div role="tabpanel" aria-label="Logs">
                <SectionCard title="Logs" description="Live tail of this task's allocations.">
                  <Show
                    when={logAllocationIds().length > 0}
                    fallback={
                      <EmptyState
                        icon="file"
                        title="No allocations yet"
                        body="Logs stream here once an allocation of this task is placed."
                      />
                    }
                  >
                    <LogViewer job={jobName()} allocationIds={logAllocationIds()} initialAllocation={logAllocation()} />
                  </Show>
                </SectionCard>
              </div>
            </Match>

            <Match when={activeTab() === 'spec'}>
              <div class="space-y-4" role="tabpanel" aria-label="Spec">
                <Show when={data()!.taskSpec} fallback={<Note tone="warn">Task not present in the current job spec.</Note>}>
                  <SectionCard title="Definition">
                    <DefList
                      cols={3}
                      items={[
                        ['Group', data()!.taskSpec!.group],
                        ['Runtime', data()!.taskSpec!.runtime],
                        [
                          'Lifecycle',
                          <span class="inline-flex items-center gap-2">
                            {data()!.taskSpec!.lifecycle}
                            <Show when={data()!.taskSpec!.lifecycle === 'sidecar'}>
                              <Badge tone="info">runs alongside its group</Badge>
                            </Show>
                          </span>,
                        ],
                        ...(data()!.taskSpec!.stateful
                          ? ([[
                              'Stateful',
                              <span class="inline-flex flex-wrap items-center gap-2">
                                <Badge tone="accent">stateful · {data()!.taskSpec!.stateful!.mode}</Badge>
                                <Show when={data()!.taskSpec!.stateful!.storagePath}>
                                  <span class="font-mono text-xs text-muted" title="Where the task sees its instance storage">{data()!.taskSpec!.stateful!.storagePath}</span>
                                </Show>
                                <Show when={data()!.volumes.find((volume) => volume.instanceId)}>
                                  {(volume) => <Badge tone={volume().nodeLocal ? 'neutral' : 'ok'}>{volume().driver}</Badge>}
                                </Show>
                              </span>,
                            ]] as Array<[string, any]>)
                          : []),
                        [
                          'Depends on',
                          data()!.taskSpec!.dependsOn.length > 0 ? (
                            <span class="inline-flex flex-wrap gap-x-2 gap-y-1">
                              <For each={data()!.taskSpec!.dependsOn}>
                                {(dependency) => (
                                  <A class={linkClass} href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(dependency.task)}`}>
                                    {dependency.task}
                                    <Show when={dependency.condition}>
                                      <span class="text-faint"> ({dependency.condition})</span>
                                    </Show>
                                  </A>
                                )}
                              </For>
                            </span>
                          ) : (
                            <span class="text-muted">none</span>
                          ),
                        ],
                      ]}
                    />
                  </SectionCard>

                  {/* Discovery: what this task advertises, on which port, and
                      where it is reachable right now. */}
                  <Show when={data()!.taskSpec!.service || data()!.taskSpec!.ports.length > 0}>
                    <SectionCard
                      title="Service & ports"
                      description="What this task advertises to discovery, and the ports it declares."
                      actions={
                        <Show when={data()!.serviceName}>
                          <A class={linkClass + ' text-sm'} href={`/services/${encodeURIComponent(data()!.serviceName!)}`}>
                            Open service
                          </A>
                        </Show>
                      }
                    >
                      <div class="space-y-4">
                        <Show when={data()!.taskSpec!.service}>
                          <DefList
                            cols={3}
                            items={[
                              ['Service', <span class="font-medium text-ink">{data()!.serviceName}</span>],
                              ['Advertised port', <span class="font-mono text-xs">{data()!.taskSpec!.service!.port}</span>],
                              [
                                'Tags & meta',
                                <ServiceTags tags={data()!.taskSpec!.service!.tags} meta={data()!.taskSpec!.service!.meta} />,
                              ],
                            ]}
                          />
                        </Show>

                        <Show when={data()!.taskSpec!.ports.length > 0}>
                          <div>
                            <p class="eyebrow mb-1.5">
                              Declared ports
                              <Show when={data()!.taskSpec!.networkMode}>
                                <span class="ml-2 normal-case tracking-normal text-faint">mode {data()!.taskSpec!.networkMode}</span>
                              </Show>
                            </p>
                            <div class="flex flex-wrap gap-1.5">
                              <For each={data()!.taskSpec!.ports}>
                                {(port) => (
                                  <Badge tone="neutral" mono>
                                    {port.name}
                                    {port.staticPort === undefined ? ' · dynamic' : ` · ${port.staticPort}`}
                                    {port.to === undefined ? '' : ` → ${port.to}`}
                                  </Badge>
                                )}
                              </For>
                            </div>
                          </div>
                        </Show>

                        <Show when={data()!.serviceName}>
                          <div>
                            <p class="eyebrow mb-1.5">Live endpoints</p>
                            <ServiceEndpointsTable
                              endpoints={data()!.endpoints}
                              emptyTitle="No endpoints yet"
                              emptyBody="An endpoint appears once an allocation is placed, gets its port, and passes its checks."
                            />
                          </div>
                        </Show>
                      </div>
                    </SectionCard>
                  </Show>

                  <Show when={(data()!.volumes ?? []).length > 0}>
                    <SectionCard
                      title="Volumes"
                      description="Persistent storage for this task. A node-local volume pins its instance to the node holding the data."
                      flush
                    >
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>Volume</th>
                              <th class={thClass}>Driver</th>
                              <th class={thClass}>State</th>
                              <th class={thClass}>Instance</th>
                              <th class={thClass}>Node</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-line">
                            <For each={data()!.volumes}>
                              {(volume) => (
                                <tr class={trHoverClass}>
                                  <td class={tdClass}>
                                    <A
                                      href={`/volumes/${encodeURIComponent(volume.volumeId)}`}
                                      class="font-mono text-xs text-ink hover:text-brass-strong"
                                      title={volume.volumeId}
                                    >
                                      {shortVolumeId(volume.volumeId)}
                                    </A>
                                    <div class="text-2xs text-faint">{volume.name}</div>
                                  </td>
                                  <td class={tdClass}>
                                    <Badge tone={volume.nodeLocal ? 'neutral' : 'ok'}>{volume.driver}</Badge>
                                  </td>
                                  <td class={tdClass}>
                                    <Badge tone={volumeStateTone(volume)} dot class="uppercase tracking-wide">
                                      {volume.state}
                                    </Badge>
                                    <Show when={volume.lastError}>
                                      <div class="mt-0.5 max-w-[16rem] truncate text-2xs text-red-500" title={volume.lastError}>
                                        {volume.lastError}
                                      </div>
                                    </Show>
                                  </td>
                                  <td class={tdClass + ' font-mono text-xs text-muted'}>{volume.instanceId ?? '—'}</td>
                                  <td class={tdClass}>
                                    <Show when={volume.nodeId} fallback={<span class="text-faint">—</span>}>
                                      <A href={`/nodes/${encodeURIComponent(volume.nodeId!)}`} class="text-muted hover:text-brass-strong">
                                        {volume.nodeId}
                                      </A>
                                      <Show when={volumePinsPlacement(volume)}>
                                        <span class="ml-1 text-brass-strong" title="Node-local: this instance can only be replaced here">*</span>
                                      </Show>
                                    </Show>
                                  </td>
                                </tr>
                              )}
                            </For>
                          </tbody>
                        </table>
                      </TableShell>
                    </SectionCard>
                  </Show>

                  <Show when={data()!.taskSpec!.artifacts.length > 0}>
                    <SectionCard
                      title="Artifacts"
                      description="As declared. Per-instance staging is in the Instances table on the Overview tab."
                      flush
                    >
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>Name</th>
                              <th class={thClass}>Source</th>
                              <th class={thClass}>Checksum / digest</th>
                              <th class={thClass}>Resolve</th>
                              <th class={thClass}>Auth</th>
                              <th class={thClass}>Dest</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-line">
                            <For each={data()!.taskSpec!.artifacts}>
                              {(artifact) => {
                                const digest = declaredArtifactDigest(artifact)
                                return (
                                  <tr class={trHoverClass}>
                                    <td class={tdClass}>{artifact.name ?? '—'}</td>
                                    <td class={tdClass}>
                                      <span class="block max-w-[320px] truncate font-mono text-xs text-muted" title={artifact.source}>{artifact.source}</span>
                                    </td>
                                    <td class={tdClass}>
                                      <Show when={digest} fallback={<span class="text-faint">—</span>}>
                                        <span class="font-mono text-xs" title={digest}>{shortenArtifactDigest(digest!)}</span>
                                      </Show>
                                      <Show when={artifact.pinnedFrom}>
                                        <p class="text-2xs text-faint">pinned from <span class="font-mono">{artifact.pinnedFrom}</span></p>
                                      </Show>
                                    </td>
                                    <td class={tdClass + ' text-muted'}>{artifact.resolve ?? '—'}</td>
                                    <td class={tdClass + ' text-muted'}>{artifact.authLabel}</td>
                                    <td class={tdClass + ' text-muted'}>{artifact.dest ?? '—'}</td>
                                  </tr>
                                )
                              }}
                            </For>
                          </tbody>
                        </table>
                      </TableShell>
                    </SectionCard>
                  </Show>
                </Show>

                <Show when={data()!.netPolicy}>
                  <SectionCard
                    title="Network"
                    description="What this task's egress and ingress are held to: stored policies merged with its own block, each rule naming its source."
                    actions={<A class={linkClass} href="/network">Policies</A>}
                  >
                    <EffectivePolicyView
                      policy={parseEffectivePolicy(data()!.netPolicy!.effectiveJson)}
                      declaredMode={data()!.netPolicy!.declaredMode}
                      effectiveMode={data()!.netPolicy!.effectiveMode}
                      matched={data()!.netPolicy!.matchedPolicies}
                      nodeStatus={data()!.netPolicy!.nodeStatusJson}
                    />
                  </SectionCard>
                </Show>
              </div>
            </Match>
          </Switch>

          {/* Mounted on first visit and only hidden after that, unlike the
              other tabs: a shell session should survive a look at the logs. */}
          <Show when={execOpened()}>
            <div role="tabpanel" aria-label="Exec & actions" hidden={activeTab() !== 'exec'}>
              <SectionCard title="Exec & actions" description="Run commands or declared actions inside one running allocation of this task.">
                <Show
                  when={execTargetIds().length > 0}
                  fallback={<EmptyState icon="terminal" title="No running allocations" body="Once an allocation is running you can open a shell or trigger actions here." />}
                >
                  <label class="flex flex-wrap items-center gap-2 text-xs font-medium text-muted">
                    Target allocation
                    <select
                      class={selectClass}
                      value={execTarget() ?? ''}
                      onChange={(event) => openTab('exec', event.currentTarget.value)}
                    >
                      <For each={execTargetIds()}>{(id) => <option value={id}>{execOptionLabel(id)}</option>}</For>
                    </select>
                  </label>
                  {/* Keyed on the id string: switching target remounts the
                      panel (a fresh terminal for the new allocation), while
                      a poll, which rebuilds every ledger row, does not. */}
                  <Show when={execTarget()} keyed>
                    {(id) => (
                      <AllocationExecPanel allocationId={BigInt(id)} jobName={jobName()} taskName={taskName()} defaultOpen />
                    )}
                  </Show>
                </Show>
              </SectionCard>
            </div>
          </Show>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function TaskDetail() {
  return (
    <AccessGate resource="job">
      <TaskDetailContent />
    </AccessGate>
  )
}
