import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { Badge, CodeBlock, ConfirmButton, EmptyState, MonoBadge, Note, PageHeader, SectionCard, Spinner, StatusBadge, TableShell, Tabs, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../../components/ui'
import DagVisualization from '../../components/DagVisualization'
import { AllocationPorts, ServiceEndpointsTable, ServiceTags } from '../../components/ServiceEndpoints'
import { InstanceInfo } from '../../gen/legion_pb'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import { allocationHealthLabel, fetchAllocationRuntime, parseAllocNetwork } from '../../utils/allocations'
import { formatDate, formatRelativeTime } from '../../utils/format'
import { buildTaskRuntimeByName, buildTaskRuntimeFromStatuses, extractJobSpecData, topoSortTaskNames, type TaskRuntimeSummary } from '../../utils/jobspec'
import { POLL_INTERVAL_MS } from '../../utils/polling'

// A storage handle is `{plugin}/{volume_id}`; plugin names never contain a
// slash, so the split is unambiguous.
const volumeIdFromHandle = (handle: string) => handle.slice(handle.indexOf('/') + 1)

export default function JobDetail() {
  const params = useParams()
  const navigate = useNavigate()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const [activeTab, setActiveTab] = createSignal<'overview' | 'spec' | 'logs'>('overview')
  const [actionMessage, setActionMessage] = createSignal('')
  const [actionFailed, setActionFailed] = createSignal(false)

  // Keyed on the route param so navigating job→job (e.g. via the command
  // palette) refetches instead of showing the previous job's data.
  const [jobData] = createResource(jobName, async () => {
    const [jobInfo, ledger, rollouts, spec, specHistory, services] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.getLedger({}),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
      managerClient.listJobSpecHistory({ name: jobName() }),
      managerClient.listServices({}),
    ])

    const jobId = jobInfo.job?.id ?? jobName()
    const allocations = ledger.nodes.flatMap((node) =>
      node.allocations.filter((allocation) => allocation.jobId === jobId),
    )

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined
    const specData = extractJobSpecData(spec.submissions)

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
      job: jobInfo.job,
      allocations,
      rollouts: rollouts.rollouts,
      activeRollout: rollouts.rollouts.find((rollout) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(rollout.status.toLowerCase())),
      spec: spec.submissions,
      specHistory: specHistory.items,
      taskSpec: specData,
      taskRuntime: buildTaskRuntimeByName(rolloutStatus?.groups ?? []),
      services: jobServices,
      endpoints,
    }
  })

  // Live per-task status, polled from the manager state store. The ledger only
  // exposes task_config_id (not names) and initial deploys have no rollout, so
  // we join task_config -> allocation -> task_status to get authoritative
  // running/success/failed status per task. Falls back gracefully on error.
  const [statusTick, setStatusTick] = createSignal(0)
  onMount(() => {
    const timer = setInterval(() => setStatusTick((value) => value + 1), 3000)
    onCleanup(() => clearInterval(timer))
  })

  const [liveRuntime] = createResource(
    () => [jobName(), statusTick()] as const,
    async ([name]) => {
      try {
        const escaped = name.replace(/'/g, "''")
        const sql =
          'SELECT tc.name AS task_name, COALESCE(ts.status, a.status) AS status ' +
          'FROM allocation a JOIN task_config tc ON tc.id = a.task_config_id ' +
          'LEFT JOIN task_status ts ON ts.task_config_id = a.task_config_id ' +
          `WHERE tc.job_id = '${escaped}'`
        const response = await managerClient.queryStateStore({ sql })
        const statusByName = new Map<string, string>()
        for (const row of response.rows) {
          try {
            const parsed = JSON.parse(row.json) as { task_name?: string; status?: string }
            if (parsed.task_name) statusByName.set(parsed.task_name, parsed.status ?? '')
          } catch {
            // ignore malformed rows
          }
        }
        return buildTaskRuntimeFromStatuses(statusByName)
      } catch {
        return new Map<string, TaskRuntimeSummary>()
      }
    },
  )

  // Latest health snapshot per allocation, refreshed with the status poll.
  const [allocationHealth] = createResource(
    () => [jobName(), statusTick()] as const,
    async ([name]) => fetchAllocationRuntime(name),
  )

  // Prefer live state-store status; fall back to rollout-derived runtime.
  const taskRuntime = createMemo(() => {
    const live = liveRuntime()
    if (live && live.size > 0) return live
    return jobData()?.taskRuntime ?? new Map<string, TaskRuntimeSummary>()
  })

  // True until the first status fetch resolves and there is no fallback data,
  // so tasks show "loading…" instead of a misleading "not allocated" on first paint.
  const statusLoading = createMemo(
    () => liveRuntime() === undefined && (jobData()?.taskRuntime?.size ?? 0) === 0,
  )

  const statefulTasks = createMemo(() => (jobData()?.taskSpec?.tasks ?? []).filter((task) => task.stateful))

  // Stateful instances for this job, polled at the standard interval while the
  // page is mounted (scoped to this route; the interval is torn down on
  // navigation). The fetcher returns the previous array when nothing changed
  // so polling does not cause needless table rerenders.
  const [instanceTick, setInstanceTick] = createSignal(0)
  onMount(() => {
    const timer = setInterval(() => setInstanceTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  let lastInstances: InstanceInfo[] = []
  const [instances] = createResource(
    () => [jobName(), instanceTick()] as const,
    async ([name]) => {
      try {
        const response = await managerClient.listInstances({ jobId: name })
        const next = response.instances
        const unchanged =
          next.length === lastInstances.length &&
          next.every((instance, index) => InstanceInfo.equals(instance, lastInstances[index]))
        if (!unchanged) lastInstances = next
        return lastInstances
      } catch {
        // Stateless jobs (or older managers) simply have no instances to show.
        return lastInstances
      }
    },
  )

  // The instances panel appears for jobs that declare stateful tasks, or that
  // have instance rows anyway (e.g. the spec failed to parse but the manager
  // knows about instances).
  const showInstancesPanel = createMemo(() => statefulTasks().length > 0 || (instances()?.length ?? 0) > 0)
  const hasKeyedTasks = createMemo(
    () =>
      statefulTasks().some((task) => task.stateful?.mode === 'keyed') ||
      (instances() ?? []).some((instance) => instance.mode === 'keyed'),
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
    <Show when={!jobData.loading} fallback={<div class="py-6"><Spinner label="Loading job…" /></div>}>
      <Show when={!jobData.error && jobData()} fallback={<Note tone="danger">Failed to load job.</Note>}>
        <div>
          <PageHeader
            eyebrow="Job"
            title={jobData()!.job?.name || jobData()!.job?.id}
            badges={
              <>
                <StatusBadge status={jobData()!.job?.status || 'unknown'} />
                <Badge tone="neutral">{jobData()!.allocations.length} allocation{jobData()!.allocations.length === 1 ? '' : 's'}</Badge>
              </>
            }
            actions={<ConfirmButton label="Delete job" onConfirm={() => void destroyJob()} />}
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
                                const runtime = () => taskRuntime().get(task.name)
                                const primaryStatus = () =>
                                  runtime()?.statuses[0] ?? (statusLoading() ? 'loading' : 'not allocated')
                                return (
                                  <A
                                    href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(task.name)}`}
                                    class="group rounded-lg border border-line bg-surface p-3 transition-colors hover:border-brass/50 hover:bg-surface2/50"
                                  >
                                    <div class="flex items-start justify-between gap-2">
                                      <div class="min-w-0">
                                        <p class="truncate text-sm font-semibold text-ink group-hover:text-brass-strong">{task.name}</p>
                                        <p class="mt-0.5 text-xs text-faint">{task.kind} · {task.lifecycle}</p>
                                      </div>
                                      <StatusBadge status={primaryStatus()} loading={statusLoading() && !runtime()} />
                                    </div>
                                    <Show when={task.stateful || task.dependsOn.length > 0 || task.lifecycle === 'sidecar'}>
                                      <div class="mt-2 flex flex-wrap items-center gap-1.5">
                                        <Show when={task.stateful}>
                                          <Badge tone="accent">stateful · {task.stateful!.mode}</Badge>
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
                    taskRuntime={taskRuntime()}
                    statusLoading={statusLoading()}
                    jobName={jobName()}
                  />
                </SectionCard>
              </Show>

              <Show when={showInstancesPanel()}>
                <SectionCard
                  title="Instances"
                  description="Stable identities for stateful tasks — instance ids and storage survive reschedules."
                  flush
                >
                  <Show
                    when={(instances() ?? []).length > 0}
                    fallback={
                      <p class="px-4 py-4 text-sm text-muted">
                        {instances() === undefined ? 'Loading instances…' : 'No instances yet.'}
                      </p>
                    }
                  >
                    <TableShell>
                      <table class={tableClass}>
                        <thead class={theadClass}>
                          <tr>
                            <th class={thClass}>Instance</th>
                            <th class={thClass}>Mode</th>
                            <th class={thClass}>State</th>
                            <th class={thClass}>Slot</th>
                            <th class={thClass}>Allocation</th>
                            <th class={thClass}>Storage</th>
                            <th class={thClass + ' text-right'}>Created</th>
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-line">
                          <For each={instances() ?? []}>
                            {(instance) => (
                              <tr class={trHoverClass}>
                                <td class={tdClass + ' font-mono text-xs'}>{instance.instanceId}</td>
                                <td class={tdClass + ' text-muted'}>{instance.mode}</td>
                                <td class={tdClass}><StatusBadge status={instance.state} /></td>
                                <td class={tdClass + ' tabular-nums text-muted'}>{instance.slotIndex !== undefined ? String(instance.slotIndex) : '—'}</td>
                                <td class={tdClass + ' font-mono text-xs text-muted'}>{instance.currentAllocId !== undefined ? `#${String(instance.currentAllocId)}` : '—'}</td>
                                <td class={tdClass}>
                                  <Show when={instance.storageHandle} fallback={<span class="text-faint">—</span>}>
                                    {/* The handle is `{plugin}/{volume_id}` — link the volume id
                                        straight to the volume that holds this instance's data. */}
                                    <A
                                      href={`/volumes/${encodeURIComponent(volumeIdFromHandle(instance.storageHandle!))}`}
                                      class="inline-block max-w-[12rem] truncate align-bottom font-mono text-xs text-muted hover:text-brass-strong"
                                      title={instance.storageHandle}
                                    >
                                      {instance.storageHandle}
                                    </A>
                                  </Show>
                                </td>
                                <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>{formatDate(instance.createdAt)}</td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </TableShell>
                    <Show when={hasKeyedTasks()}>
                      <p class="border-t border-line px-4 py-2.5 text-xs text-faint">
                        Keyed instances are managed from the CLI via <code class="rounded bg-surface2 px-1 font-mono">legion instance create/destroy</code> — read-only here.
                      </p>
                    </Show>
                  </Show>
                </SectionCard>
              </Show>

              <SectionCard title="Allocations" flush>
                <Show
                  when={jobData()!.allocations.length > 0}
                  fallback={<EmptyState icon="boxes" title="No allocations" body="The scheduler has not placed this job on any worker yet." />}
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>ID</th>
                          <th class={thClass}>Instance</th>
                          <th class={thClass}>Status</th>
                          <th class={thClass}>Health</th>
                          <th class={thClass}>Ports</th>
                          <th class={thClass}>Worker</th>
                          <th class={thClass}>Job config</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={jobData()!.allocations}>
                          {(allocation) => {
                            const health = () => allocationHealth()?.get(Number(allocation.id))
                            return (
                            <tr class={trHoverClass}>
                              <td class={tdClass + ' font-mono text-xs'}>#{String(allocation.id)}</td>
                              <td class={tdClass}>
                                <Show when={allocation.instanceId} fallback={<span class="text-faint">—</span>}>
                                  <MonoBadge title={`Stateful instance ${allocation.instanceId}`}>{allocation.instanceId}</MonoBadge>
                                </Show>
                              </td>
                              <td class={tdClass}><StatusBadge status={allocation.status} /></td>
                              <td class={tdClass}>
                                <StatusBadge status={allocationHealthLabel(health())} title={health()?.status || undefined} />
                              </td>
                              <td class={tdClass}>
                                <AllocationPorts network={parseAllocNetwork(allocation.network)} />
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
                              <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>{formatRelativeTime(rollout.updatedAt)}</td>
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
