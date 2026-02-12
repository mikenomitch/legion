import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { DetailBadge, DetailSection, detailDangerButtonClass, detailTabButtonClass } from '../../components/DetailPrimitives'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'
import { parseJobTasksFromSubmissions } from '../../utils/tasks'

const statusTone = (status: string) => {
  const normalized = status.toLowerCase()
  if (['running', 'healthy', 'completed', 'done', 'success'].some((value) => normalized.includes(value))) return 'success'
  if (['failed', 'error', 'unhealthy'].some((value) => normalized.includes(value))) return 'danger'
  if (['pending', 'starting', 'queued', 'unknown'].some((value) => normalized.includes(value))) return 'warning'
  return 'neutral'
}

type RuntimeTaskAllocation = {
  allocationId: bigint
  nodeId: string
  taskConfigId: bigint
  taskName: string
  taskGroup: string
  lifecycle: string
  status: string
  allocationHealthy?: boolean
  allocationHealth?: string
  rolloutId: bigint
}

export default function JobDetail() {
  const params = useParams()
  const navigate = useNavigate()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const [activeTab, setActiveTab] = createSignal<'overview' | 'tasks' | 'spec'>('overview')
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

    const rolloutStatusReplies = await Promise.all(
      rollouts.rollouts.map(async (rollout) => {
        try {
          return await managerClient.getRolloutStatus({ id: rollout.id })
        } catch {
          return undefined
        }
      }),
    )

    const runtimeTaskAllocations: RuntimeTaskAllocation[] = rolloutStatusReplies.flatMap((reply) =>
      reply?.groups.flatMap((group) =>
        group.allocations.map((allocation) => ({
          allocationId: allocation.allocationId,
          nodeId: allocation.nodeId,
          taskConfigId: allocation.taskConfigId,
          taskName: allocation.taskName,
          taskGroup: allocation.taskGroup || group.group,
          lifecycle: allocation.lifecycle,
          status: allocation.status,
          allocationHealthy: allocation.allocationHealthy,
          allocationHealth: allocation.allocationHealth,
          rolloutId: reply.rollout?.id ?? 0n,
        })),
      ) ?? [],
    )

    const tasks = parseJobTasksFromSubmissions(spec.submissions)
    const taskStatuses = new Map<string, { statuses: string[]; health: string[]; healthyCount: number; unhealthyCount: number }>()

    for (const allocation of runtimeTaskAllocations) {
      const current = taskStatuses.get(allocation.taskName) ?? { statuses: [], health: [], healthyCount: 0, unhealthyCount: 0 }
      current.statuses.push(allocation.status)
      if (allocation.allocationHealth) current.health.push(allocation.allocationHealth)
      if (allocation.allocationHealthy === true) current.healthyCount += 1
      if (allocation.allocationHealthy === false) current.unhealthyCount += 1
      taskStatuses.set(allocation.taskName, current)
    }

    const knownTaskNames = new Set(tasks.map((task) => task.name))
    for (const allocation of runtimeTaskAllocations) {
      if (!knownTaskNames.has(allocation.taskName)) {
        tasks.push({
          name: allocation.taskName,
          group: allocation.taskGroup || 'default',
          lifecycle: allocation.lifecycle || 'service',
          kind: 'runtime-only',
          dependsOn: [],
        })
      }
    }

    const tasksByGroup = tasks.reduce<Record<string, typeof tasks>>((acc, task) => {
      const group = task.group || 'default'
      if (!acc[group]) acc[group] = []
      acc[group].push(task)
      return acc
    }, {})

    const serviceTasksByGroup = new Map<string, string[]>()
    for (const task of tasks) {
      if (task.lifecycle.toLowerCase() !== 'sidecar') {
        const group = task.group || 'default'
        const groupTasks = serviceTasksByGroup.get(group) ?? []
        groupTasks.push(task.name)
        serviceTasksByGroup.set(group, groupTasks)
      }
    }

    return {
      job: jobInfo.job,
      allocations,
      runtimeTaskAllocations,
      rollouts: rollouts.rollouts,
      activeRollout: rollouts.rollouts.find((rollout) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(rollout.status.toLowerCase())),
      spec: spec.submissions,
      specHistory: specHistory.items,
      tasks,
      tasksByGroup,
      taskStatuses,
      serviceTasksByGroup,
    }
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
              <button type="button" onClick={() => setActiveTab('tasks')} class={detailTabButtonClass(activeTab() === 'tasks')}>Tasks</button>
              <button type="button" onClick={() => setActiveTab('spec')} class={detailTabButtonClass(activeTab() === 'spec')}>Job Spec</button>
            </div>
          </DetailSection>

          <Show when={activeTab() === 'overview'}>
            <DetailSection>
              <h3 class="mb-2 font-semibold">Task runtime status</h3>
              <div class="overflow-x-auto">
                <table class="min-w-full text-sm">
                  <thead><tr class="text-left"><th class="py-2 pr-4">Allocation</th><th class="py-2 pr-4">Task</th><th class="py-2 pr-4">Group</th><th class="py-2 pr-4">Lifecycle</th><th class="py-2 pr-4">Status</th><th class="py-2 pr-4">Health</th><th class="py-2 pr-4">Node</th></tr></thead>
                  <tbody>
                    <For each={jobData()!.runtimeTaskAllocations}>{(allocation) => (
                      <tr class="border-t border-neutral-200 dark:border-neutral-800"><td class="py-2 pr-4">{String(allocation.allocationId)}</td><td class="py-2 pr-4"><A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(allocation.taskName)}`}>{allocation.taskName}</A></td><td class="py-2 pr-4">{allocation.taskGroup || 'default'}</td><td class="py-2 pr-4">{allocation.lifecycle || 'service'}</td><td class="py-2 pr-4">{allocation.status}</td><td class="py-2 pr-4">{allocation.allocationHealth || (allocation.allocationHealthy === true ? 'healthy' : allocation.allocationHealthy === false ? 'unhealthy' : 'unknown')}</td><td class="py-2 pr-4"><A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></td></tr>
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

          <Show when={activeTab() === 'tasks'}>
            <DetailSection title="Task groups and DAG">
              <p class="text-sm text-neutral-600 dark:text-neutral-300">All tasks are shown below, grouped and connected by dependency edges when declared in the job spec.</p>
              <div class="mt-4 space-y-5">
                <For each={Object.entries(jobData()!.tasksByGroup)}>{([groupName, tasks]) => (
                  <div class="rounded-xl border border-neutral-200 bg-neutral-50/60 p-4 dark:border-neutral-700 dark:bg-neutral-900/40">
                    <div class="flex flex-wrap items-center gap-2">
                      <h4 class="text-lg font-semibold">Group: {groupName}</h4>
                      <DetailBadge>{tasks.length} task(s)</DetailBadge>
                    </div>
                    <div class="mt-4 grid gap-3 md:grid-cols-2">
                      <For each={tasks}>{(task) => {
                        const runtime = jobData()!.taskStatuses.get(task.name)
                        const status = runtime?.statuses[0] || 'pending'
                        const sidecarHosts = task.lifecycle.toLowerCase() === 'sidecar' && !task.sidecarOf
                          ? (jobData()!.serviceTasksByGroup.get(groupName) || []).join(', ')
                          : task.sidecarOf
                        return (
                          <div class="rounded-lg border border-neutral-200 bg-white p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-950/80">
                            <div class="flex items-center justify-between gap-2">
                              <A href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(task.name)}`} class="font-semibold underline">{task.name}</A>
                              <DetailBadge tone={statusTone(status)}>{status}</DetailBadge>
                            </div>
                            <div class="mt-2 flex flex-wrap gap-2 text-xs">
                              <DetailBadge>{task.lifecycle}</DetailBadge>
                              <DetailBadge>{task.kind}</DetailBadge>
                              <Show when={runtime}><DetailBadge tone={runtime!.unhealthyCount > 0 ? 'danger' : runtime!.healthyCount > 0 ? 'success' : 'warning'}>{runtime!.health[0] || (runtime!.healthyCount > 0 ? 'healthy' : runtime!.unhealthyCount > 0 ? 'unhealthy' : 'unknown health')}</DetailBadge></Show>
                            </div>
                            <Show when={task.lifecycle.toLowerCase() === 'sidecar'}>
                              <p class="mt-2 text-xs text-indigo-600 dark:text-indigo-300">Sidecar for: {sidecarHosts || 'group peers'}</p>
                            </Show>
                            <Show when={task.dependsOn.length > 0} fallback={<p class="mt-2 text-xs text-neutral-500">No dependencies</p>}>
                              <div class="mt-2 text-xs">
                                <p class="font-medium text-neutral-500">Depends on</p>
                                <ul class="mt-1 list-disc pl-4">
                                  <For each={task.dependsOn}>{(dep) => <li>{dep.task}{dep.condition ? ` (${dep.condition})` : ''}</li>}</For>
                                </ul>
                              </div>
                            </Show>
                          </div>
                        )
                      }}</For>
                    </div>
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
        </div>
      </Show>
    </Show>
  )
}
