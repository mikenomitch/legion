import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { DetailBadge, DetailSection } from '../../components/DetailPrimitives'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'
import {
  artifactProgressPercent,
  artifactStateTone,
  buildTaskRuntimeByName,
  extractJobSpecData,
  parseArtifactStates,
  shortenArtifactDigest,
  taskStatusTone,
  type ArtifactStateRow,
  type JobTaskArtifact,
} from '../../utils/jobspec'

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

export default function TaskDetail() {
  const params = useParams()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const taskName = createMemo(() => decodeURIComponent(params.taskName))

  const [data] = createResource(async () => {
    const [jobInfo, rollouts, spec] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
    ])

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined

    const taskSpec = extractJobSpecData(spec.submissions)?.tasks.find((task) => task.name === taskName())
    const runtimeByTask = buildTaskRuntimeByName(rolloutStatus?.groups ?? [])
    const taskRuntime = runtimeByTask.get(taskName())

    return {
      job: jobInfo.job,
      taskSpec,
      taskRuntime,
      latestRollout,
      groups: rolloutStatus?.groups ?? [],
    }
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading task…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load task details.</p>}>
        <div class="space-y-4">
          <DetailSection class="bg-gradient-to-br from-neutral-50 to-white dark:from-neutral-900 dark:to-neutral-950">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p class="text-xs uppercase tracking-wide text-neutral-500">Task details</p>
                <h2 class="text-2xl font-semibold">{taskName()}</h2>
                <p class="mt-1 text-sm text-neutral-500">
                  Job <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}`}>{data()!.job?.name ?? jobName()}</A>
                </p>
              </div>
              <DetailBadge tone={taskStatusTone(data()!.taskRuntime?.statuses[0] ?? 'unknown')}>
                {data()!.taskRuntime?.statuses[0] ?? 'unknown'}
              </DetailBadge>
            </div>
          </DetailSection>

          <Show when={data()!.taskSpec} fallback={<DetailSection><p class="text-sm text-neutral-500">Task not present in current job spec.</p></DetailSection>}>
            <DetailSection title="Task summary">
              <div class="grid gap-3 md:grid-cols-2">
                <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <p class="text-xs uppercase tracking-wide text-neutral-500">Group</p>
                  <p class="mt-1 font-semibold">{data()!.taskSpec!.group}</p>
                </div>
                <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <p class="text-xs uppercase tracking-wide text-neutral-500">Lifecycle</p>
                  <p class="mt-1 font-semibold">{data()!.taskSpec!.lifecycle}</p>
                  <Show when={data()!.taskSpec!.lifecycle === 'sidecar'}>
                    <p class="mt-1 text-xs text-indigo-600 dark:text-indigo-300">This task runs as a sidecar in its task group.</p>
                  </Show>
                </div>
                <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <p class="text-xs uppercase tracking-wide text-neutral-500">Task kind</p>
                  <p class="mt-1 font-semibold">{data()!.taskSpec!.kind}</p>
                </div>
                <Show when={data()!.taskSpec!.stateful}>
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <p class="text-xs uppercase tracking-wide text-neutral-500">Stateful</p>
                    <p class="mt-1"><DetailBadge tone="neutral">stateful · {data()!.taskSpec!.stateful!.mode}</DetailBadge></p>
                    <Show when={data()!.taskSpec!.stateful!.storagePath}>
                      <p class="mt-1 font-mono text-xs text-neutral-500" title="Persistent storage path">{data()!.taskSpec!.stateful!.storagePath}</p>
                    </Show>
                  </div>
                </Show>
                <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <p class="text-xs uppercase tracking-wide text-neutral-500">Dependencies</p>
                  <Show when={data()!.taskSpec!.dependsOn.length > 0} fallback={<p class="mt-1 text-sm text-neutral-500">No dependencies.</p>}>
                    <ul class="mt-1 space-y-1 text-sm">
                      <For each={data()!.taskSpec!.dependsOn}>{(dependency) => (
                        <li>
                          <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(dependency.task)}`}>
                            {dependency.task}
                          </A>
                          <Show when={dependency.condition}> <span class="text-neutral-500">({dependency.condition})</span></Show>
                        </li>
                      )}</For>
                    </ul>
                  </Show>
                </div>
              </div>
            </DetailSection>

            <Show when={data()!.taskSpec!.artifacts.length > 0}>
              <DetailSection title="Artifacts">
                <div class="overflow-x-auto">
                  <table class="min-w-full text-sm">
                    <thead>
                      <tr class="text-left">
                        <th class="py-2 pr-4">Name</th>
                        <th class="py-2 pr-4">Source</th>
                        <th class="py-2 pr-4">Checksum / digest</th>
                        <th class="py-2 pr-4">Resolve</th>
                        <th class="py-2 pr-4">Auth</th>
                        <th class="py-2 pr-4">Dest</th>
                      </tr>
                    </thead>
                    <tbody>
                      <For each={data()!.taskSpec!.artifacts}>{(artifact) => {
                        const digest = declaredArtifactDigest(artifact)
                        return (
                          <tr class="border-t border-neutral-200 dark:border-neutral-800">
                            <td class="py-2 pr-4">{artifact.name ?? '—'}</td>
                            <td class="py-2 pr-4">
                              <span class="block max-w-[320px] truncate font-mono text-xs" title={artifact.source}>{artifact.source}</span>
                            </td>
                            <td class="py-2 pr-4">
                              <Show when={digest} fallback={<span class="text-neutral-500">—</span>}>
                                <span class="font-mono text-xs" title={digest}>{shortenArtifactDigest(digest!)}</span>
                              </Show>
                              <Show when={artifact.pinnedFrom}>
                                <p class="text-xs text-neutral-500">pinned from <span class="font-mono">{artifact.pinnedFrom}</span></p>
                              </Show>
                            </td>
                            <td class="py-2 pr-4">{artifact.resolve ?? '—'}</td>
                            <td class="py-2 pr-4">{artifact.authLabel}</td>
                            <td class="py-2 pr-4">{artifact.dest ?? '—'}</td>
                          </tr>
                        )
                      }}</For>
                    </tbody>
                  </table>
                </div>
              </DetailSection>
            </Show>

            <DetailSection title="Task health">
              <Show
                when={data()!.taskRuntime}
                fallback={<p class="text-sm text-neutral-500">No runtime health found for this task yet.</p>}
              >
                <div class="grid gap-3 md:grid-cols-2">
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <p class="text-xs uppercase tracking-wide text-neutral-500">Allocation health</p>
                    <p class="mt-1 text-lg font-semibold">
                      {data()!.taskRuntime!.healthy === undefined ? 'Unknown' : data()!.taskRuntime!.healthy ? 'Healthy' : 'Unhealthy'}
                    </p>
                  </div>
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <p class="text-xs uppercase tracking-wide text-neutral-500">Allocations</p>
                    <p class="mt-1 text-lg font-semibold">{data()!.taskRuntime!.allocations.length}</p>
                  </div>
                </div>
                <Show when={data()!.taskRuntime!.health.length > 0}>
                  <div class="mt-3 rounded border border-neutral-200 p-3 text-sm dark:border-neutral-700">
                    <p class="font-semibold">Health details</p>
                    <ul class="mt-2 list-disc space-y-1 pl-5">
                      <For each={data()!.taskRuntime!.health}>{(health) => <li>{health}</li>}</For>
                    </ul>
                  </div>
                </Show>
              </Show>
            </DetailSection>

            <DetailSection title="Runtime allocations" actions={<Show when={data()!.latestRollout}><A class="text-sm underline" href={`/rollouts/${data()!.latestRollout!.id}`}>Open rollout</A></Show>}>
              <Show when={data()!.taskRuntime?.allocations.length} fallback={<p class="text-sm text-neutral-500">No runtime allocations available.</p>}>
                <div class="space-y-2">
                  <For each={data()!.taskRuntime!.allocations}>{(allocation) => {
                    const artifactRows = parseArtifactStates(allocation.artifactStates)
                    return (
                      <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                        <div class="flex flex-wrap items-center justify-between gap-2">
                          <p class="text-sm font-semibold">Allocation #{String(allocation.allocationId)}</p>
                          <DetailBadge tone={taskStatusTone(allocation.status)}>{allocation.status}</DetailBadge>
                        </div>
                        <p class="mt-1 text-xs text-neutral-500">Node {allocation.nodeId} · Group {allocation.taskGroup} · {allocation.lifecycle}</p>
                        <p class="mt-1 text-xs text-neutral-500">Role {allocation.role} · Updated in latest rollout {data()!.latestRollout ? formatDate(data()!.latestRollout!.updatedAt) : '—'}</p>
                        <Show when={artifactRows.length > 0}>
                          <div class="mt-2 space-y-1 border-t border-neutral-200 pt-2 dark:border-neutral-700">
                            <p class="text-xs uppercase tracking-wide text-neutral-500">Artifacts</p>
                            <For each={artifactRows}>{(artifact) => (
                              <div class="text-xs">
                                <div class="flex flex-wrap items-center gap-2">
                                  <DetailBadge tone={artifactStateTone(artifact.state)}>{artifact.state}</DetailBadge>
                                  <span class="max-w-[280px] truncate font-mono text-neutral-700 dark:text-neutral-200" title={artifact.source}>
                                    {artifact.name ?? artifact.source}
                                  </span>
                                  <Show when={artifactProgressLabel(artifact)}>
                                    <span class="text-neutral-500">{artifactProgressLabel(artifact)}</span>
                                  </Show>
                                  <Show when={artifact.digest}>
                                    <span class="font-mono text-neutral-500" title={artifact.digest}>{shortenArtifactDigest(artifact.digest!)}</span>
                                  </Show>
                                  <Show when={artifact.durationMs !== undefined}>
                                    <span class="text-neutral-500">{formatDurationMs(artifact.durationMs!)}</span>
                                  </Show>
                                </div>
                                <Show when={artifact.state === 'failed' && artifact.error}>
                                  <p class="mt-0.5 text-rose-500">{artifact.error}</p>
                                </Show>
                              </div>
                            )}</For>
                          </div>
                        </Show>
                      </div>
                    )
                  }}</For>
                </div>
              </Show>
            </DetailSection>
          </Show>

          <DetailSection title="Logs">
            <Show
              when={(data()!.taskRuntime?.allocations.length ?? 0) > 0}
              fallback={<p class="text-sm text-neutral-500">No allocations are running for this task yet.</p>}
            >
              <LogViewer
                job={jobName()}
                allocationIds={data()!.taskRuntime!.allocations.map((allocation) => allocation.allocationId)}
              />
            </Show>
          </DetailSection>
        </div>
      </Show>
    </Show>
  )
}
