import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { DetailBadge, DetailSection, detailDangerButtonClass, detailTabButtonClass } from '../../components/DetailPrimitives'
import DagVisualization from '../../components/DagVisualization'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'
import { buildTaskRuntimeByName, buildTaskRuntimeFromStatuses, extractJobSpecData, taskStatusTone, topoSortTaskNames, type TaskRuntimeSummary } from '../../utils/jobspec'

export default function JobDetail() {
  const params = useParams()
  const navigate = useNavigate()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const [activeTab, setActiveTab] = createSignal<'overview' | 'spec' | 'logs'>('overview')
  const [actionMessage, setActionMessage] = createSignal('')

  const [jobData] = createResource(async () => {
    const [jobInfo, ledger, rollouts, spec, specHistory] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.getLedger({}),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
      managerClient.listJobSpecHistory({ name: jobName() }),
    ])

    const allocations = ledger.nodes.flatMap((node) =>
      node.allocations.filter((allocation) => allocation.jobId === (jobInfo.job?.id ?? jobName())),
    )

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined
    const specData = extractJobSpecData(spec.submissions)

    return {
      job: jobInfo.job,
      allocations,
      rollouts: rollouts.rollouts,
      activeRollout: rollouts.rollouts.find((rollout) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(rollout.status.toLowerCase())),
      spec: spec.submissions,
      specHistory: specHistory.items,
      taskSpec: specData,
      taskRuntime: buildTaskRuntimeByName(rolloutStatus?.groups ?? []),
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

  // The DAG is only meaningful when tasks actually depend on each other.
  // Single-task and independent-task jobs render as the "Tasks by group" cards
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
      setActionMessage(response.message || 'Job destroyed')
      setTimeout(() => navigate('/jobs'), 900)
    } catch (error) {
      console.error(error)
      setActionMessage('Destroy failed: stop the job first, then retry.')
    }
  }

  return (
    <Show when={!jobData.loading} fallback={<p>Loading job…</p>}>
      <Show when={!jobData.error && jobData()} fallback={<p class="text-rose-500">Failed to load job.</p>}>
        <div class="space-y-4">
          <DetailSection>
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 class="text-2xl font-semibold">{jobData()!.job?.name || jobData()!.job?.id}</h2>
                <p class="text-sm text-neutral-500">Status: {jobData()!.job?.status || 'unknown'} · Allocations: {jobData()!.allocations.length}</p>
              </div>
              <button type="button" onClick={destroyJob} class={detailDangerButtonClass}>
                Delete Job
              </button>
            </div>
            <Show when={actionMessage()}><p class="mt-2 text-sm text-neutral-600 dark:text-neutral-300">{actionMessage()}</p></Show>

            <div class="mt-4 flex gap-2">
              <button type="button" onClick={() => setActiveTab('overview')} class={detailTabButtonClass(activeTab() === 'overview')}>Overview</button>
              <button type="button" onClick={() => setActiveTab('spec')} class={detailTabButtonClass(activeTab() === 'spec')}>Job Spec</button>
              <button type="button" onClick={() => setActiveTab('logs')} class={detailTabButtonClass(activeTab() === 'logs')}>Logs</button>
            </div>
          </DetailSection>

          <Show when={activeTab() === 'overview'}>
            <Show when={jobData()!.activeRollout}>
              <DetailSection class="border-amber-300 bg-amber-50/70 dark:border-amber-700 dark:bg-amber-950/30">
                <h3 class="font-semibold">Active rollout</h3>
                <p class="text-sm">Rollout #{String(jobData()!.activeRollout!.id)} · {jobData()!.activeRollout!.status}</p>
                <A href={`/rollouts/${jobData()!.activeRollout!.id}`} class="text-sm underline">Open rollout</A>
              </DetailSection>
            </Show>


            <Show when={jobData()!.activeRollout}>
              <DetailSection>
                <h3 class="font-semibold">Allocations in active rollout target</h3>
                <div class="mt-2 space-y-1 text-sm">
                  <For each={jobData()!.allocations.filter((allocation) => allocation.jobConfigId === jobData()!.activeRollout!.toJobConfigId)}>
                    {(allocation) => <p>#{String(allocation.id)} on <A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A> · {allocation.status}</p>}
                  </For>
                </div>
              </DetailSection>
            </Show>

            <DetailSection>
              <h3 class="mb-2 font-semibold">Tasks by group</h3>
              <Show
                when={jobData()!.taskSpec?.tasks.length}
                fallback={<p class="text-sm text-neutral-500">No task information available in current job spec.</p>}
              >
                <div class="space-y-4">
                  <For each={[...new Set(jobData()!.taskSpec!.tasks.map((task) => task.group))]}>
                    {(groupName) => (
                      <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                        <h4 class="text-sm font-semibold uppercase tracking-wide text-neutral-500">Group {groupName}</h4>
                        <div class="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                          <For each={jobData()!.taskSpec!.tasks.filter((task) => task.group === groupName)}>
                            {(task) => {
                              const runtime = () => taskRuntime().get(task.name)
                              const primaryStatus = () =>
                                runtime()?.statuses[0] ?? (statusLoading() ? 'loading…' : 'not allocated')
                              return (
                                <A
                                  href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(task.name)}`}
                                  class="rounded border border-neutral-200 bg-neutral-50 p-3 transition hover:border-neutral-400 hover:bg-white dark:border-neutral-700 dark:bg-neutral-900"
                                >
                                  <div class="flex items-start justify-between gap-2">
                                    <div>
                                      <p class="text-sm font-semibold">{task.name}</p>
                                      <p class="text-xs text-neutral-500">{task.kind} · lifecycle {task.lifecycle}</p>
                                    </div>
                                    <DetailBadge tone={taskStatusTone(primaryStatus())}>
                                      <span class={statusLoading() && !runtime() ? 'animate-pulse' : ''}>{primaryStatus()}</span>
                                    </DetailBadge>
                                  </div>
                                  <Show when={task.lifecycle === 'sidecar'}>
                                    <p class="mt-2 text-xs text-indigo-600 dark:text-indigo-300">
                                      Sidecar task for group <span class="font-semibold">{task.group}</span>
                                    </p>
                                  </Show>
                                  <Show when={task.dependsOn.length > 0}>
                                    <p class="mt-2 text-xs text-neutral-500">
                                      Depends on: {task.dependsOn.map((dependency) => dependency.task).join(', ')}
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
            </DetailSection>

            <Show when={dagHasEdges()}>
              <DetailSection>
                <h3 class="mb-2 font-semibold">Task DAG</h3>
                <DagVisualization
                  tasks={jobData()!.taskSpec!.tasks}
                  layers={topoSortTaskNames(jobData()!.taskSpec!.tasks)}
                  taskRuntime={taskRuntime()}
                  statusLoading={statusLoading()}
                  jobName={jobName()}
                />
              </DetailSection>
            </Show>

            <DetailSection>
              <h3 class="mb-2 font-semibold">Allocations</h3>
              <div class="overflow-x-auto">
                <table class="min-w-full text-sm">
                  <thead><tr class="text-left"><th class="py-2 pr-4">ID</th><th class="py-2 pr-4">Status</th><th class="py-2 pr-4">Worker</th><th class="py-2 pr-4">Job Config</th></tr></thead>
                  <tbody>
                    <For each={jobData()!.allocations}>{(allocation) => (
                      <tr class="border-t border-neutral-200 dark:border-neutral-800"><td class="py-2 pr-4">{String(allocation.id)}</td><td class="py-2 pr-4">{allocation.status}</td><td class="py-2 pr-4"><A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></td><td class="py-2 pr-4"><A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(allocation.jobConfigId)}`}>{allocation.jobConfigId}</A></td></tr>
                    )}</For>
                  </tbody>
                </table>
              </div>
            </DetailSection>

            <DetailSection>
              <h3 class="mb-2 font-semibold">Associated rollouts</h3>
              <div class="space-y-2">
                <For each={jobData()!.rollouts}>{(rollout) => (
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <div class="flex items-center justify-between gap-2"><A href={`/rollouts/${rollout.id}`} class="font-semibold underline">Rollout #{String(rollout.id)}</A><span>{rollout.status}</span></div>
                    <p class="mt-1 text-xs text-neutral-500">Updated {formatDate(rollout.updatedAt)}</p>
                    <p class="mt-1 text-xs">from <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.fromJobConfigId || '')}`}>{rollout.fromJobConfigId || '—'}</A> → to <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.toJobConfigId)}`}>{rollout.toJobConfigId}</A></p>
                  </div>
                )}</For>
              </div>
            </DetailSection>
          </Show>

          <Show when={activeTab() === 'spec'}>
            <DetailSection>
              <h3 class="font-semibold">Current spec submissions</h3>
              <For each={jobData()!.spec}>{(submission) => (
                <div class="mt-3 rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <div class="text-sm font-semibold">{submission.path}</div>
                  <pre class="mt-2 max-h-80 overflow-auto rounded bg-neutral-100 p-2 text-xs dark:bg-neutral-950">{submission.content}</pre>
                </div>
              )}</For>
            </DetailSection>

            <DetailSection>
              <h3 class="font-semibold">Spec history</h3>
              <div class="mt-2 space-y-2">
                <For each={jobData()!.specHistory}>{(item) => (
                  <A href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(item.configId)}`} class="block rounded border border-neutral-200 p-3 text-sm hover:border-neutral-400 dark:border-neutral-700">
                    {item.configId} · version {item.version !== undefined ? String(item.version) : '—'} · {formatDate(item.createdAt)}
                  </A>
                )}</For>
              </div>
            </DetailSection>
          </Show>

          <Show when={activeTab() === 'logs'}>
            <DetailSection title="Task logs" actions={<span class="text-xs text-neutral-500">live tail across this job's allocations</span>}>
              <LogViewer job={jobName()} />
            </DetailSection>
          </Show>
        </div>
      </Show>
    </Show>
  )
}
