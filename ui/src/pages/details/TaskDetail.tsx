import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { DetailBadge, DetailSection } from '../../components/DetailPrimitives'
import { managerClient } from '../../grpcClient'
import { parseJobTasksFromSubmissions } from '../../utils/tasks'

const healthTone = (healthy?: boolean) => {
  if (healthy === true) return 'success'
  if (healthy === false) return 'danger'
  return 'warning'
}

export default function TaskDetail() {
  const params = useParams()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const taskName = createMemo(() => decodeURIComponent(params.taskName))

  const [data] = createResource(async () => {
    const [rollouts, spec] = await Promise.all([
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
    ])

    const rolloutStatuses = await Promise.all(
      rollouts.rollouts.map(async (rollout) => {
        try {
          return await managerClient.getRolloutStatus({ id: rollout.id })
        } catch {
          return undefined
        }
      }),
    )

    const taskAllocations = rolloutStatuses.flatMap((status) =>
      status?.groups.flatMap((group) =>
        group.allocations
          .filter((allocation) => allocation.taskName === taskName())
          .map((allocation) => ({ ...allocation, groupName: group.group })),
      ) ?? [],
    )

    const task = parseJobTasksFromSubmissions(spec.submissions).find((item) => item.name === taskName())

    return { allocations: taskAllocations, task }
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading task details…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load task details.</p>}>
        <div class="space-y-4">
          <DetailSection class="bg-gradient-to-br from-white to-sky-50/60 dark:from-neutral-900/80 dark:to-sky-950/30">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 class="text-2xl font-semibold">{taskName()}</h2>
                <p class="text-sm text-neutral-500">Job: <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}`}>{jobName()}</A></p>
              </div>
              <div class="flex flex-wrap gap-2">
                <Show when={data()!.task}><DetailBadge>{data()!.task!.group}</DetailBadge></Show>
                <Show when={data()!.task}><DetailBadge>{data()!.task!.lifecycle}</DetailBadge></Show>
              </div>
            </div>
            <Show when={data()!.task?.dependsOn.length}>
              <div class="mt-3 rounded-lg border border-neutral-200 bg-white/80 p-3 text-sm dark:border-neutral-700 dark:bg-neutral-900/70">
                <p class="font-semibold">Dependencies</p>
                <ul class="mt-1 list-disc pl-4">
                  <For each={data()!.task?.dependsOn ?? []}>{(dep) => <li>{dep.task}{dep.condition ? ` (${dep.condition})` : ''}</li>}</For>
                </ul>
              </div>
            </Show>
          </DetailSection>

          <DetailSection title="Task health and allocations">
            <div class="space-y-3">
              <For each={data()!.allocations}>{(allocation) => (
                <article class="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm dark:border-neutral-800 dark:bg-neutral-950/80">
                  <div class="flex flex-wrap items-center justify-between gap-2">
                    <p class="font-semibold">Allocation #{String(allocation.allocationId)}</p>
                    <div class="flex gap-2">
                      <DetailBadge>{allocation.status || 'unknown'}</DetailBadge>
                      <DetailBadge tone={healthTone(allocation.allocationHealthy)}>{allocation.allocationHealth || (allocation.allocationHealthy === true ? 'healthy' : allocation.allocationHealthy === false ? 'unhealthy' : 'unknown health')}</DetailBadge>
                    </div>
                  </div>
                  <dl class="mt-3 grid gap-2 text-sm sm:grid-cols-2">
                    <div><dt class="text-neutral-500">Node</dt><dd><A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></dd></div>
                    <div><dt class="text-neutral-500">Task group</dt><dd>{allocation.taskGroup || allocation.groupName || data()!.task?.group || 'default'}</dd></div>
                    <div><dt class="text-neutral-500">Task config ID</dt><dd>{String(allocation.taskConfigId)}</dd></div>
                    <div><dt class="text-neutral-500">Lifecycle</dt><dd>{allocation.lifecycle || data()!.task?.lifecycle || 'service'}</dd></div>
                  </dl>
                </article>
              )}</For>
              <Show when={data()!.allocations.length === 0}><p class="text-sm text-neutral-500">No allocations yet for this task.</p></Show>
            </div>
          </DetailSection>

          <Show when={data()!.task?.config !== undefined}>
            <DetailSection title="Task config">
              <pre class="max-h-[420px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{JSON.stringify(data()!.task?.config, null, 2)}</pre>
            </DetailSection>
          </Show>
        </div>
      </Show>
    </Show>
  )
}
