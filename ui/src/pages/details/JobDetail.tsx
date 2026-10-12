import { A, useLocation, useNavigate, useParams, useSearchParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { For, Show, createEffect, createMemo, createResource, createSignal, on } from 'solid-js'
import { Badge, Button, CodeBlock, ConfirmButton, EmptyState, MonoBadge, Note, PageHeader, RelativeTime, SectionCard, Spinner, StatusBadge, TableShell, Tabs, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../../components/ui'
import DagVisualization from '../../components/DagVisualization'
import ScalePanel from '../../components/ScalePanel'
import InstancesPanel, { DeferredInstancesCard } from '../../components/InstancesPanel'
import type { InstanceRow } from '../../components/InstancesTable'
import { PressureHistoryCard } from '../../components/PressureHistory'
import SnapshotsPanel from '../../components/SnapshotsPanel'
import { AllocationPorts, ServiceEndpointsTable, ServiceTags } from '../../components/ServiceEndpoints'
import { InstanceInfo } from '../../gen/legion_pb'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import { nodePublicAddress } from '../../utils/services'
import { allows, jobScope } from '../../utils/acl'
import { allocationHealthLabel, fetchAllocationRuntime, fetchTaskStatusByName, parseAllocNetwork } from '../../utils/allocations'
import { formatDate, pluralize } from '../../utils/format'
import { buildTaskInstancesByName, buildTaskInstancesFromStatuses, extractJobSpecFromReply, topoSortTaskNames, type TaskInstanceSummary } from '../../utils/jobspec'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../../utils/polling'
import { stableList } from '../../utils/instances'
import {
  AUTO_LIST_MAX_SESSIONS,
  ENVELOPE_COPY,
  LARGE_LISTING,
  SESSION_COPY,
  countInstances,
  envelopeServes,
  envelopeTaskName,
  formatCount,
  isSession,
  isSessionTask,
  jobInstancesHref,
  sessionsPerEnvelope,
} from '../../utils/sessions'
import { bytesToMiB, fetchAllocationUsageByNode, formatIdle, formatMiB, reservationLabel } from '../../utils/usage'

// Allocation rows the job page renders per "Show more".
const ALLOCATION_PAGE = 200

// What each derived job status means on this page (the badge's tooltip).
const JOB_STATUS_HINTS: Record<string, string> = {
  running: 'At least one allocation of this job is live.',
  pending: 'Nothing is live yet: a task is waiting to be placed or for a dependency. The task cards show which.',
  complete: 'Every task finished successfully and nothing is live.',
  scheduled: 'A cron task finished its last run and is waiting for the next one.',
  failed: 'A task failed, or a required KV key is missing, and nothing is live. The task cards and events show why.',
  suspended: 'Every wanted instance is suspended. Start an instance to resume it.',
  idle: 'No task wants an allocation: scaled to zero, or no keyed instances exist.',
  stopped: 'Stopped. Run the job again to bring it back.',
}

function JobDetailContent() {
  const params = useParams()
  const navigate = useNavigate()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const [activeTab, setActiveTab] = createSignal<'overview' | 'spec' | 'logs'>('overview')
  const [actionMessage, setActionMessage] = createSignal('')
  const [actionFailed, setActionFailed] = createSignal(false)

  // Keyed on the route param so navigating job→job (e.g. via the command
  // palette) refetches instead of showing the previous job's data, and polled
  // at the slow cadence (this is ~8 RPCs) so allocations, rollouts and the
  // spec list follow the cluster without a reload.
  const pageTick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [jobData, { refetch: refetchJob }] = createResource(
    () => [jobName(), pageTick()] as const,
    retainOnError(async ([name]: readonly [string, number]) => {
    const [jobInfo, ledger, rollouts, spec, specHistory, services, preemptions, pressureEvents] = await Promise.all([
      managerClient.getJobInfo({ name: name }),
      // node:read garnish (allocation placement); job-scoped tokens
      // still get the rest of the page.
      managerClient.getLedger({}).catch(() => undefined),
      managerClient.listRollouts({ jobId: name, activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: name }),
      managerClient.listJobSpecHistory({ name: name }),
      managerClient.listServices({}),
      // Preemption history: what displaced this job's allocations, and when.
      // Best-effort — the event journal is bounded (~10k rows), so this is an
      // observability surface rather than an audit log, and a job whose
      // preemptions have aged out simply shows none.
      managerClient
        .listEvents({ kind: 'allocation.preempted', jobId: name, limit: 20 })
        .then((reply) => reply.events)
        .catch(() => []),
      // Memory pressure history: what a node's pressure response did to this
      // job's allocations (docs/resource-enforcement.md, "Memory pressure").
      // Same best-effort journal, same operator:read requirement.
      managerClient
        .listEvents({ kind: 'node.pressure', jobId: name, limit: 20 })
        .then((reply) => reply.events)
        .catch(() => []),
    ])

    const jobId = jobInfo.job?.id ?? name
    // Without node:read the ledger is unavailable, which is not the same as
    // the job having no allocations: the card says which it is.
    const ledgerDenied = ledger === undefined
    // Port links use a node's public address when its worker declares one.
    const publicAddresses = new Map(
      (ledger?.nodes ?? []).flatMap((entry) => {
        const address = nodePublicAddress(entry.node?.configAttrs)
        return entry.node && address ? [[entry.node.id, address] as const] : []
      }),
    )
    const allocations = (ledger?.nodes ?? []).flatMap((node) =>
      node.allocations.filter((allocation) => allocation.jobId === jobId),
    )

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined
    const specData = extractJobSpecFromReply(spec)

    // Endpoints for this job's services, unhealthy included: an endpoint that
    // exists but is failing is exactly what someone is looking for here.
    const jobServices = services.services.filter((service) => service.jobId === jobId)
    const endpoints = (
      await Promise.all(
        jobServices.map((service) =>
          managerClient
            .resolveService({ service: service.service, includeUnhealthy: true })
            .then((reply) => reply.endpoints)
            .catch(() => []),
        ),
      )
    ).flat()

    return {
      name,
      job: jobInfo.job,
      template: jobInfo.template,
      allocations,
      ledgerDenied,
      publicAddresses,
      rollouts: rollouts.rollouts,
      activeRollout: rollouts.rollouts.find((rollout) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(rollout.status.toLowerCase())),
      spec: spec.submissions,
      specHistory: specHistory.items,
      taskSpec: specData,
      taskInstances: buildTaskInstancesByName(rolloutStatus?.groups ?? []),
      services: jobServices,
      endpoints,
      preemptions,
      pressureEvents,
    }
  }, ([name]) => name),
  )

  // Live per-task status, polled from the manager state store. The ledger only
  // exposes task_config_id (not names) and initial deploys have no rollout, so
  // we join task_config -> allocation -> task_status to get authoritative
  // running/success/failed status per task. Falls back gracefully on error.
  const statusTick = usePoll(3000)

  const [liveRuntime] = createResource(
    () => [jobName(), statusTick()] as const,
    async ([name]) => buildTaskInstancesFromStatuses(await fetchTaskStatusByName(name)),
  )

  // Latest health snapshot per allocation, refreshed with the status poll.
  const [allocationHealth] = createResource(
    () => [jobName(), statusTick()] as const,
    async ([name]) => fetchAllocationRuntime(name),
  )

  // Prefer live state-store status; fall back to rollout-derived runtime.
  const taskInstances = createMemo(() => {
    const live = liveRuntime()
    if (live && live.size > 0) return live
    return jobData()?.taskInstances ?? new Map<string, TaskInstanceSummary>()
  })

  // True until the first status fetch resolves and there is no fallback data,
  // so tasks show "loading…" instead of a misleading "not allocated" on first paint.
  const statusLoading = createMemo(
    () => liveRuntime() === undefined && (jobData()?.taskInstances?.size ?? 0) === 0,
  )

  const statefulTasks = createMemo(() => (jobData()?.taskSpec?.tasks ?? []).filter((task) => task.stateful))
  // Session tasks (docs/session-mode.md): their sessions are not allocations,
  // and their envelopes (`<task>-envelope`, one per eligible node) are.
  const sessionTasks = createMemo(() => statefulTasks().filter((task) => isSessionTask(task)))
  // Keyed and session tasks have no count, so the scale panel shows them read-only.
  const keyedTaskNames = createMemo(
    () =>
      new Set(
        statefulTasks()
          .filter((task) => task.stateful?.mode === 'keyed' || isSessionTask(task))
          .flatMap((task) => (isSessionTask(task) ? [task.name, envelopeTaskName(task.name)] : [task.name])),
      ),
  )

  // How many sessions this job's envelopes could hold: placed envelopes
  // times the sessions one admits. ListInstances returns every session in
  // one reply with no paging, so past AUTO_LIST_MAX_SESSIONS (or when the
  // envelopes cannot be counted without node:read) the page waits to be
  // asked before fetching it.
  const specLoaded = () => jobData.state === 'ready' || jobData.state === 'refreshing'
  const sessionCapacity = createMemo(() => {
    const data = specLoaded() ? jobData() : undefined
    if (!data || sessionTasks().length === 0) return undefined
    let capacity = 0
    let envelopes = 0
    let known = !data.ledgerDenied
    for (const task of sessionTasks()) {
      const placed = data.allocations.filter((allocation) => allocation.taskName === envelopeTaskName(task.name)).length
      const per = sessionsPerEnvelope(task)
      envelopes += placed
      if (per === undefined) known = false
      else capacity += per * placed
    }
    return { capacity, envelopes, known }
  })
  const [search] = useSearchParams()
  const [listRequested, setListRequested] = createSignal(search.load === '1')
  const listDeferred = createMemo(() => {
    if (listRequested()) return false
    const capacity = sessionCapacity()
    return capacity !== undefined && (!capacity.known || capacity.capacity > AUTO_LIST_MAX_SESSIONS)
  })

  // Stateful instances and running sessions for this job, polled at the
  // standard interval while the page is mounted (scoped to this route; the
  // interval is torn down on navigation). Unchanged instances keep their
  // objects across polls, so a row with an armed Suspend or Resume is not
  // re-rendered by a poll that changed some other row.
  //
  // A listing of LARGE_LISTING or more is not re-polled: ListInstances has
  // no paging, so re-reading a few hundred thousand sessions every two
  // seconds would pin the manager and the tab. It refreshes on request (the
  // card's Refresh button, or after an action).
  const instanceTick = usePoll()
  const [instanceForce, setInstanceForce] = createSignal(0)
  const refetchInstances = () => setInstanceForce((value) => value + 1)
  // Instance ids are unique cluster-wide, so one merge serves every job.
  const mergeInstances = stableList<InstanceInfo>((instance) => instance.instanceId, (a, b) => InstanceInfo.equals(a, b))
  type Listing = { job: string; instances: InstanceInfo[]; complete: boolean; asOf: string; force: number }
  let lastListing: Listing | undefined
  const [instanceListing] = createResource(
    () => (specLoaded() && !listDeferred() ? ([jobName(), instanceTick(), instanceForce()] as const) : undefined),
    async ([name, , force]): Promise<Listing> => {
      const previous = lastListing?.job === name ? lastListing : undefined
      if (previous && previous.instances.length >= LARGE_LISTING && previous.force === force) return previous
      try {
        const response = await managerClient.listInstances({ jobId: name })
        const next = mergeInstances(response.instances)
        const unchanged =
          previous !== undefined && next.length === previous.instances.length && next.every((instance, index) => instance === previous.instances[index])
        lastListing = {
          job: name,
          instances: unchanged ? previous!.instances : next,
          complete: response.complete,
          asOf: response.asOf,
          force,
        }
        // Once shown, the card stays: envelopes placed later do not swap a
        // loaded listing for the "not loaded" card.
        if (!listRequested()) setListRequested(true)
        return lastListing
      } catch {
        // Stateless jobs (or older managers) simply have no instances to show.
        return previous ?? { job: name, instances: [], complete: true, asOf: '', force }
      }
    },
  )
  const instances = createMemo(() => {
    const listing = instanceListing()
    return listing?.job === jobName() ? listing.instances : undefined
  })
  // Snapshots are of keyed instances only; a session has nothing to snapshot.
  const durableInstances = createMemo(() => {
    const all = instances() ?? []
    return all.some(isSession) ? all.filter((instance) => !isSession(instance)) : all
  })
  // One pass over the listing for every count the page shows.
  const instanceCounts = createMemo(() => countInstances(instances() ?? []))
  const sessionCounts = () => instanceCounts().sessionsByTask
  const sessionTotal = () => instanceCounts().sessions
  const sessionsByEnvelope = () => instanceCounts().sessionsByEnvelope
  // The Allocations card renders a page at a time: a session job places one
  // envelope per eligible node, so a large fleet means thousands of rows.
  const [allocationLimit, setAllocationLimit] = createSignal(ALLOCATION_PAGE)
  // A new job starts from the first page and its own listing choice.
  createEffect(
    on(jobName, () => {
      setAllocationLimit(ALLOCATION_PAGE)
      setListRequested(search.load === '1')
    }, { defer: true }),
  )

  // Land on the Instances card when a link asks for it (`#instances`), once
  // it has rendered.
  const location = useLocation()
  let scrolledFor = ''
  createEffect(() => {
    const key = `${location.pathname}${location.search}${location.hash}`
    if (scrolledFor === key || location.hash !== '#instances') return
    if (instances() === undefined && !listDeferred()) return
    scrolledFor = key
    queueMicrotask(() => document.getElementById('instances')?.scrollIntoView({ block: 'start' }))
  })
  // Measured usage per allocation, from GetNodeUsage on each node this job
  // runs on. Needs node:read; without it (or on an older manager) the map is
  // empty and the usage columns fall back to the instances' own `usage`.
  const [allocationUsage] = createResource(
    () => [jobName(), (jobData()?.allocations ?? []).map((allocation) => allocation.nodeId).join(','), pageTick()] as const,
    ([, nodeIds]) => fetchAllocationUsageByNode(nodeIds ? nodeIds.split(',') : []),
  )

  // Rows are built only for the instances the card shows, never for the
  // whole listing.
  const tasksByName = createMemo(() => new Map((jobData()?.taskSpec?.tasks ?? []).map((task) => [task.name, task])))
  const instanceRow = (instance: InstanceInfo): InstanceRow => ({
    instance,
    task: tasksByName().get(instance.task),
    allocation: instance.currentAllocId !== undefined ? allocationUsage()?.get(Number(instance.currentAllocId)) : undefined,
  })

  // The manager derives the status from the job's workload (see
  // docs/jobs-tasks-and-scheduling.md#job-status). The hint says what each
  // word means here, so `pending` and `idle` are not mistaken for faults.
  const displayStatus = createMemo(() => jobData()?.job?.status || 'unknown')
  const statusHint = createMemo(() => JOB_STATUS_HINTS[displayStatus().toLowerCase()])

  // The instances panel appears for jobs that declare stateful tasks, or that
  // have instance rows anyway (e.g. the spec failed to parse but the manager
  // knows about instances).
  const showInstancesPanel = createMemo(() => statefulTasks().length > 0 || (instances()?.length ?? 0) > 0)
  // Keyed instances and sessions have per-row controls; slot instances follow count.num.
  const hasInstanceActions = createMemo(
    () =>
      statefulTasks().some((task) => task.stateful?.mode === 'keyed' || isSessionTask(task)) ||
      (instances() ?? []).some((instance) => instance.mode === 'keyed' || isSession(instance)),
  )

  // The DAG is only meaningful when tasks actually depend on each other.
  // Single-task and independent-task jobs render as the "Tasks" cards
  // instead of a big empty canvas with disconnected nodes.
  const dagHasEdges = createMemo(() => {
    const tasks = jobData()?.taskSpec?.tasks ?? []
    if (tasks.length < 2) return false
    const names = new Set(tasks.map((task) => task.name))
    return tasks.some((task) => task.dependsOn.some((dependency) => names.has(dependency.task)))
  })

  const mayWrite = () => allows('job', 'write', jobScope(jobName(), jobData()?.template?.name))
  // Stop is offered until the job is stopped; destroy needs a stopped job
  // first, which is why both live here. `failed` and `complete` stay
  // stoppable, because stopping is the step before destroying either.
  const isStoppable = () => !['stopped', 'destroyed'].includes((jobData()?.job?.status ?? '').toLowerCase())

  const stopJob = async () => {
    try {
      const response = await managerClient.stopJob({ name: jobName() })
      setActionFailed(false)
      setActionMessage(response.message || 'Stop requested — allocations are being torn down.')
      void refetchJob()
    } catch (error) {
      setActionFailed(true)
      setActionMessage(`Stop failed: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  const destroyJob = async () => {
    try {
      const response = await managerClient.destroyJob({ name: jobName() })
      setActionFailed(false)
      setActionMessage(response.message || 'Job destroyed')
      setTimeout(() => navigate('/jobs'), 900)
    } catch (error) {
      console.error(error)
      setActionFailed(true)
      setActionMessage('Destroy failed: stop the job first, then retry.')
    }
  }

  return (
    <Show when={!jobData.error} fallback={<Note tone="danger">Failed to load job {jobName()}: {String(jobData.error?.message ?? jobData.error)}</Note>}>
      <Show when={jobData()?.name === jobName()} fallback={<div class="py-6"><Spinner label="Loading job…" /></div>}>
        <div>
          <PageHeader
            eyebrow="Job"
            title={jobData()!.job?.name || jobData()!.job?.id}
            subtitle={
              <>
                Created <RelativeTime value={jobData()!.job?.createdAt} />
                <Show when={jobData()!.job && jobData()!.job!.updatedAt !== jobData()!.job!.createdAt}>
                  {' · '}updated <RelativeTime value={jobData()!.job?.updatedAt} />
                </Show>
              </>
            }
            badges={
              <>
                <StatusBadge
                  status={displayStatus()}
                  title={statusHint()}
                />
                {/* The priority band decides what this job can displace and
                    what can displace it, so it belongs beside the status
                    rather than buried in the spec. */}
                <Show when={jobData()!.job?.priority}>
                  {(priority) => (
                    <MonoBadge
                      tone={['critical', 'high'].includes(priority()) ? 'accent' : 'neutral'}
                      title="Priority band: orders the eval queue and governs preemption"
                    >
                      <>priority: {priority()}</>
                    </MonoBadge>
                  )}
                </Show>
                <Badge tone="neutral">{pluralize(jobData()!.allocations.length, 'allocation')}</Badge>
                <Show when={sessionTotal() > 0}>
                  <Badge tone="info" title={SESSION_COPY}>
                    {formatCount(sessionTotal())} {sessionTotal() === 1 ? 'session' : 'sessions'}
                  </Badge>
                </Show>
                <Show when={jobData()!.template}>
                  {(template) => (
                    <A href={`/templates/${encodeURIComponent(template().name)}`}>
                      <MonoBadge tone="accent" title="Submitted from a job template">
                        <>template: {template().name} v{template().version}</>
                      </MonoBadge>
                    </A>
                  )}
                </Show>
              </>
            }
            actions={
              <>
                <Show when={isStoppable()}>
                  <ConfirmButton
                    label="Stop job"
                    confirmLabel="Stop every allocation?"
                    icon="square"
                    onConfirm={() => void stopJob()}
                    disabled={!mayWrite()}
                  />
                </Show>
                <ConfirmButton
                  label="Delete job"
                  onConfirm={() => void destroyJob()}
                  disabled={!mayWrite()}
                />
              </>
            }
          />
          <Show when={actionMessage()}>
            <div aria-live="polite">
              <Note tone={actionFailed() ? 'danger' : 'info'} class="mb-4">{actionMessage()}</Note>
            </div>
          </Show>

          <Tabs
            class="mb-5"
            tabs={[
              { id: 'overview', label: 'Overview' },
              { id: 'spec', label: 'Job spec', count: jobData()!.specHistory.length },
              { id: 'logs', label: 'Logs' },
            ]}
            active={activeTab()}
            onChange={(id) => setActiveTab(id as 'overview' | 'spec' | 'logs')}
          />

          <Show when={activeTab() === 'overview'}>
            <div class="space-y-4">
              {/* A placement found no room: the job waits under a key and
                  places on its own when capacity that could host it appears
                  (docs/scheduler-and-eval-loop.md, "Blocked evals"). */}
              <Show when={jobData()!.job?.blocked}>
                {(blocked) => (
                  <Note tone="warn">
                    <div class="space-y-1">
                      <div>
                        <span class="font-semibold">Waiting for capacity</span> · blocked on{' '}
                        <code class="font-mono">{blocked().key}</code> since <RelativeTime value={blocked().since} />
                      </div>
                      <div>
                        Task <code class="font-mono">{blocked().task}</code> needs{' '}
                        {Object.entries(blocked().shortfall)
                          .sort(([a], [b]) => a.localeCompare(b))
                          .map(([resource, amount]) => `${resource} ${amount}`)
                          .join(', ') || 'room'}{' '}
                        per placement. It places on its own when a node it can run on joins or frees room.
                      </div>
                      <div class="text-xs opacity-80">{blocked().message}</div>
                    </div>
                  </Note>
                )}
              </Show>
              <Show when={jobData()!.activeRollout}>
                <Note tone="info">
                  <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
                    <span class="font-semibold">Rollout #{String(jobData()!.activeRollout!.id)} in progress</span>
                    <StatusBadge status={jobData()!.activeRollout!.status} />
                    <span class="text-xs opacity-80">
                      {jobData()!.allocations.filter((allocation) => allocation.jobConfigId === jobData()!.activeRollout!.toJobConfigId).length} allocation(s) on target config
                    </span>
                    <A href={`/rollouts/${jobData()!.activeRollout!.id}`} class={linkClass}>Open rollout</A>
                  </div>
                </Note>
              </Show>

              <SectionCard title="Tasks">
                <Show
                  when={jobData()!.taskSpec?.tasks.length}
                  fallback={<p class="text-sm text-muted">No task information available in the current job spec.</p>}
                >
                  <div class="space-y-4">
                    <For each={[...new Set(jobData()!.taskSpec!.tasks.map((task) => task.group))]}>
                      {(groupName) => (
                        <div>
                          <p class="eyebrow mb-2">Group · {groupName}</p>
                          <div class="grid gap-2.5 md:grid-cols-2 xl:grid-cols-3">
                            <For each={jobData()!.taskSpec!.tasks.filter((task) => task.group === groupName)}>
                              {(task) => {
                                // A session task has no allocations of its own: its
                                // envelopes are what the scheduler places.
                                const session = isSessionTask(task)
                                const runtime = () => taskInstances().get(task.name) ?? (session ? taskInstances().get(envelopeTaskName(task.name)) : undefined)
                                const primaryStatus = () =>
                                  runtime()?.statuses[0] ?? (statusLoading() ? 'loading' : session ? 'no envelopes' : 'not allocated')
                                return (
                                  <A
                                    href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(task.name)}`}
                                    class="group rounded-lg border border-line bg-surface p-3 transition-colors hover:border-brass/50 hover:bg-surface2/50"
                                  >
                                    <div class="flex items-start justify-between gap-2">
                                      <div class="min-w-0">
                                        <p class="truncate text-sm font-semibold text-ink group-hover:text-brass-strong">{task.name}</p>
                                        <p class="mt-0.5 text-xs text-faint">{task.runtime} · {task.lifecycle}</p>
                                      </div>
                                      <StatusBadge status={primaryStatus()} loading={statusLoading() && !runtime()} />
                                    </div>
                                    <Show when={task.stateful || task.dependsOn.length > 0 || task.lifecycle === 'sidecar'}>
                                      <div class="mt-2 flex flex-wrap items-center gap-1.5">
                                        <Show when={task.stateful}>
                                          <Badge tone="accent" title={session ? SESSION_COPY : undefined}>
                                            stateful · {task.stateful!.mode}
                                          </Badge>
                                        </Show>
                                        <Show when={session && instances() !== undefined}>
                                          <Badge tone="info" title={`Running sessions, from the session index. Each node's envelope (${envelopeTaskName(task.name)}) reserves their room.`}>
                                            {formatCount(sessionCounts().get(task.name) ?? 0)} running
                                          </Badge>
                                        </Show>
                                        <Show when={task.lifecycle === 'sidecar'}>
                                          <Badge tone="neutral">sidecar</Badge>
                                        </Show>
                                        <Show when={task.dependsOn.length > 0}>
                                          <span class="text-2xs text-faint">
                                            after {task.dependsOn.map((dependency) => dependency.task).join(', ')}
                                          </span>
                                        </Show>
                                      </div>
                                    </Show>
                                    <Show when={task.stateful?.storagePath}>
                                      <p class="mt-1.5 truncate font-mono text-2xs text-faint" title={`Persistent storage path: ${task.stateful!.storagePath}`}>
                                        {task.stateful!.storagePath}
                                      </p>
                                    </Show>
                                  </A>
                                )
                              }}
                            </For>
                          </div>
                        </div>
                      )}
                    </For>
                  </div>
                </Show>
              </SectionCard>

              <ScalePanel jobName={jobName()} tick={statusTick} keyedTasks={keyedTaskNames()} />

              {/* Where this job is reachable. Endpoints are derived from the
                  allocations plus health, so this is the honest answer to
                  "is my service up and what is its address". */}
              <Show when={jobData()!.services.length > 0}>
                <SectionCard
                  title="Services"
                  description="Endpoints this job advertises to the rest of the cluster."
                  flush
                >
                  <div class="divide-y divide-line">
                    <For each={jobData()!.services}>
                      {(service) => {
                        const endpoints = () => jobData()!.endpoints.filter((endpoint) => endpoint.service === service.service)
                        return (
                          <div class="p-4">
                            <div class="mb-2.5 flex flex-wrap items-center gap-2">
                              <A href={`/services/${encodeURIComponent(service.service)}`} class="font-medium text-ink hover:text-brass-strong">
                                {service.service}
                              </A>
                              <StatusBadge
                                status={
                                  service.totalCount === 0
                                    ? 'no endpoints'
                                    : service.healthyCount === 0
                                      ? 'unavailable'
                                      : service.healthyCount === service.totalCount
                                        ? 'healthy'
                                        : 'degraded'
                                }
                              />
                              <span class="text-xs text-faint">
                                {service.healthyCount}/{service.totalCount} healthy · task {service.taskName}
                              </span>
                              <span class="flex-1" />
                              <ServiceTags tags={service.tags} meta={service.meta} />
                            </div>
                            <ServiceEndpointsTable endpoints={endpoints()} />
                          </div>
                        )
                      }}
                    </For>
                  </div>
                </SectionCard>
              </Show>

              <Show when={dagHasEdges()}>
                <SectionCard title="Task graph" description="Execution order derived from task dependencies.">
                  <DagVisualization
                    tasks={jobData()!.taskSpec!.tasks}
                    layers={topoSortTaskNames(jobData()!.taskSpec!.tasks)}
                    taskInstances={taskInstances()}
                    statusLoading={statusLoading()}
                    jobName={jobName()}
                  />
                </SectionCard>
              </Show>

              <Show when={showInstancesPanel()}>
                <div id="instances" class="scroll-mt-20">
                  <Show
                    when={!listDeferred()}
                    fallback={
                      <DeferredInstancesCard
                        jobName={jobName()}
                        capacity={sessionCapacity()?.known ? sessionCapacity()!.capacity : undefined}
                        envelopes={sessionCapacity()?.envelopes ?? 0}
                        onLoad={() => setListRequested(true)}
                        loading={instanceListing.loading}
                      />
                    }
                  >
                    <Show when={instances() !== undefined} fallback={<SectionCard title="Instances"><p class="text-sm text-muted">Loading instances…</p></SectionCard>}>
                      <InstancesPanel
                        jobName={jobName()}
                        instances={instances()!}
                        toRow={instanceRow}
                        actions={hasInstanceActions()}
                        onChanged={refetchInstances}
                        listing={{
                          complete: instanceListing()?.complete ?? true,
                          asOf: instanceListing()?.asOf || undefined,
                          paused: (instances()?.length ?? 0) >= LARGE_LISTING,
                          refreshing: instanceListing.loading,
                          onRefresh: refetchInstances,
                        }}
                      />
                    </Show>
                  </Show>
                </div>
                <SnapshotsPanel
                  jobName={jobName()}
                  instances={durableInstances()}
                  tasks={jobData()!.taskSpec?.tasks ?? []}
                  onChanged={refetchInstances}
                />
              </Show>

              <SectionCard title="Allocations" flush>
                <Show
                  when={jobData()!.allocations.length > 0}
                  fallback={
                    <Show
                      when={!jobData()!.ledgerDenied}
                      fallback={<EmptyState icon="ban" title="Allocations hidden" body="Listing allocations needs node:read, which your token does not have." />}
                    >
                      <EmptyState
                        icon="boxes"
                        title="No allocations"
                        body={
                          jobData()!.activeRollout?.message
                            ? `Rollout #${String(jobData()!.activeRollout!.id)}: ${jobData()!.activeRollout!.message}`
                            : displayStatus() === 'stopped'
                              ? 'The job is stopped.'
                              : 'The scheduler has not placed this job on any worker. The Events page (filtered to this job) shows why.'
                        }
                      />
                    </Show>
                  }
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>ID</th>
                          <th class={thClass}>Task</th>
                          <th class={thClass}>Instance</th>
                          <th class={thClass}>Status</th>
                          <th class={thClass}>Health</th>
                          <th class={thClass}>Ports</th>
                          <th class={thClass} title="Measured use, with the reservation and burst ceiling (MiB)">Memory</th>
                          <th class={thClass} title="Time since the allocation last used 1% of a core or moved traffic">Idle</th>
                          <th class={thClass}>Worker</th>
                          <th class={thClass}>Job config</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={jobData()!.allocations.slice(0, allocationLimit())}>
                          {(allocation) => {
                            const health = () => allocationHealth()?.get(Number(allocation.id))
                            const usage = () => allocationUsage()?.get(Number(allocation.id))
                            return (
                            <tr class={trHoverClass}>
                              <td class={tdClass + ' font-mono text-xs'}>
                                <A class="font-semibold text-ink hover:text-brass-strong" href={`/allocations/${String(allocation.id)}`}>
                                  #{String(allocation.id)}
                                </A>
                              </td>
                              <td class={tdClass}>
                                <Show when={allocation.taskName} fallback={<span class="text-faint">—</span>}>
                                  <A
                                    class="text-muted hover:text-brass-strong"
                                    href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(allocation.taskName)}`}
                                  >
                                    {allocation.taskName}
                                  </A>
                                  <Show when={envelopeServes(allocation.taskName, jobData()!.taskSpec?.tasks)}>
                                    {(served) => (
                                      <A
                                        href={jobInstancesHref(jobName(), { mode: 'session', envelope: String(allocation.id) })}
                                        class="ml-1.5"
                                        title={`${ENVELOPE_COPY} It holds ${served()}'s sessions on this node. Open them.`}
                                      >
                                        <Badge tone="info">
                                          {instances() === undefined
                                            ? 'envelope'
                                            : `envelope · ${formatCount(sessionsByEnvelope().get(String(allocation.id)) ?? 0)} sessions`}
                                        </Badge>
                                      </A>
                                    )}
                                  </Show>
                                </Show>
                              </td>
                              <td class={tdClass}>
                                <Show when={allocation.instanceId} fallback={<span class="text-faint">—</span>}>
                                  <A
                                    href={`/jobs/${encodeURIComponent(jobName())}/instances/${encodeURIComponent(allocation.instanceId!)}`}
                                    title={`Stateful instance ${allocation.instanceId}`}
                                  >
                                    <MonoBadge class="hover:border-brass/50">{allocation.instanceId}</MonoBadge>
                                  </A>
                                </Show>
                              </td>
                              <td class={tdClass}><StatusBadge status={allocation.status} /></td>
                              <td class={tdClass}>
                                <StatusBadge status={allocationHealthLabel(health())} title={health()?.status || undefined} />
                              </td>
                              <td class={tdClass}>
                                <AllocationPorts
                                  network={parseAllocNetwork(allocation.network)}
                                  publicAddress={jobData()!.publicAddresses.get(allocation.nodeId)}
                                />
                              </td>
                              <td class={tdClass + ' font-mono text-xs tabular-nums'}>
                                <Show when={usage()} fallback={<span class="text-faint">-</span>}>
                                  {(entry) => (
                                    <>
                                      <span class={entry().usage ? 'text-ink' : 'text-faint'}>
                                        {entry().usage ? `${formatMiB(bytesToMiB(entry().usage!.memoryCurrentBytes))} MiB` : 'not measured'}
                                      </span>
                                      <div class="text-2xs text-faint">res/max {reservationLabel(entry().reservedMemoryMib, entry().memoryMaxMib)}</div>
                                    </>
                                  )}
                                </Show>
                              </td>
                              <td class={tdClass + ' font-mono text-xs tabular-nums text-muted'}>
                                {usage()?.usage ? formatIdle(usage()!.idleSeconds) : '-'}
                              </td>
                              <td class={tdClass}>
                                <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A>
                              </td>
                              <td class={tdClass}>
                                <A class="font-mono text-xs text-muted hover:text-brass-strong" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(allocation.jobConfigId)}`}>
                                  {allocation.jobConfigId}
                                </A>
                              </td>
                            </tr>
                            )
                          }}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                  <Show when={jobData()!.allocations.length > allocationLimit()}>
                    <div class="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-t border-line px-4 py-2.5 text-xs text-muted">
                      <span>
                        Showing the first {formatCount(allocationLimit())} of {formatCount(jobData()!.allocations.length)} allocations.
                      </span>
                      <Button size="xs" onClick={() => setAllocationLimit((value) => value + ALLOCATION_PAGE)}>
                        Show {formatCount(Math.min(ALLOCATION_PAGE, jobData()!.allocations.length - allocationLimit()))} more
                      </Button>
                      <span class="text-faint">
                        or list them from the CLI with <code class="rounded bg-surface2 px-1 font-mono">legion ledger -j</code>
                      </span>
                    </div>
                  </Show>
                </Show>
              </SectionCard>

              <SectionCard title="Rollout history" flush>
                <Show
                  when={jobData()!.rollouts.length > 0}
                  fallback={<EmptyState icon="rocket" title="No rollouts" body="Rollouts appear when you submit a new version of this job." />}
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>Rollout</th>
                          <th class={thClass}>Status</th>
                          <th class={thClass}>Config change</th>
                          <th class={thClass + ' text-right'}>Updated</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={jobData()!.rollouts}>
                          {(rollout) => (
                            <tr class={trHoverClass}>
                              <td class={tdClass}>
                                <A href={`/rollouts/${rollout.id}`} class="font-medium hover:text-brass-strong">#{String(rollout.id)}</A>
                              </td>
                              <td class={tdClass}><StatusBadge status={rollout.status} /></td>
                              <td class={tdClass}>
                                <span class="inline-flex flex-wrap items-center gap-1.5 font-mono text-xs text-muted">
                                  <A class="hover:text-brass-strong" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.fromJobConfigId || '')}`}>{rollout.fromJobConfigId || '—'}</A>
                                  <span class="text-faint">→</span>
                                  <A class="hover:text-brass-strong" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.toJobConfigId)}`}>{rollout.toJobConfigId}</A>
                                </span>
                              </td>
                              <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}><RelativeTime value={rollout.updatedAt} /></td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </Show>
              </SectionCard>

              {/* Memory pressure: why an allocation of this job was reclaimed
                  or evicted by its node, with the hint for the job author. */}
              <Show when={jobData()!.pressureEvents.length}>
                <PressureHistoryCard events={jobData()!.pressureEvents} scope="job" />
              </Show>

              {/* Preemption history. Only rendered when there is any: a job
                  that has never been displaced should not carry an empty
                  section implying it might be. */}
              <Show when={jobData()!.preemptions.length}>
                <SectionCard
                  title="Preemption history"
                  description="When this job's allocations were displaced to make room for higher-priority work. Best-effort: the event journal is bounded, so older preemptions age out."
                  flush
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>When</th>
                          <th class={thClass}>Displaced by</th>
                          <th class={thClass}>Band</th>
                          <th class={thClass + ' text-right'}>Allocations</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={jobData()!.preemptions}>
                          {(event) => {
                            const meta = (() => {
                              try {
                                return JSON.parse(event.metadataJson || '{}') as {
                                  count?: number
                                  band?: string
                                  preemptorJob?: string
                                  preemptorBand?: string
                                }
                              } catch {
                                return {}
                              }
                            })()
                            return (
                              <tr class={trHoverClass}>
                                <td class={tdClass + ' whitespace-nowrap text-xs text-faint'}>
                                  <RelativeTime value={event.createdAt} />
                                </td>
                                <td class={tdClass}>
                                  <Show when={meta.preemptorJob} fallback={<span class="text-faint">—</span>}>
                                    {(job) => (
                                      <A href={`/jobs/${encodeURIComponent(job())}`} class="font-medium hover:text-brass-strong">
                                        {job()}
                                      </A>
                                    )}
                                  </Show>
                                  <Show when={meta.preemptorBand}>
                                    <span class="ml-2 font-mono text-2xs text-muted">({meta.preemptorBand})</span>
                                  </Show>
                                </td>
                                <td class={tdClass}>
                                  <span class="font-mono text-xs text-muted">{meta.band ?? '—'}</span>
                                </td>
                                <td class={tdClass + ' text-right text-xs text-muted'}>{meta.count ?? 1}</td>
                              </tr>
                            )
                          }}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </SectionCard>
              </Show>
            </div>
          </Show>

          <Show when={activeTab() === 'spec'}>
            <div class="space-y-4">
              <SectionCard title="Current spec">
                <div class="space-y-3">
                  <For each={jobData()!.spec}>
                    {(submission) => <CodeBlock code={submission.content} label={submission.path} maxHClass="max-h-96" />}
                  </For>
                </div>
              </SectionCard>

              <SectionCard title="Spec history" flush>
                <Show
                  when={jobData()!.specHistory.length > 0}
                  fallback={<EmptyState icon="file" title="No spec history" />}
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>Config</th>
                          <th class={thClass}>Version</th>
                          <th class={thClass + ' text-right'}>Created</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={jobData()!.specHistory}>
                          {(item) => (
                            <tr class={trHoverClass}>
                              <td class={tdClass}>
                                <A href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(item.configId)}`} class="font-mono text-xs font-medium hover:text-brass-strong">
                                  {item.configId}
                                </A>
                              </td>
                              <td class={tdClass + ' tabular-nums text-muted'}>{item.version !== undefined ? String(item.version) : '—'}</td>
                              <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>{formatDate(item.createdAt)}</td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </Show>
              </SectionCard>
            </div>
          </Show>

          <Show when={activeTab() === 'logs'}>
            <SectionCard title="Task logs" description="Live tail across this job's allocations.">
              <LogViewer job={jobName()} />
            </SectionCard>
          </Show>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function JobDetail() {
  return (
    <AccessGate resource="job">
      <JobDetailContent />
    </AccessGate>
  )
}
