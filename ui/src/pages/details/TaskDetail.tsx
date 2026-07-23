import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { Badge, DefList, EmptyState, Note, PageHeader, SectionCard, Spinner, StatusBadge, TableShell, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../../components/ui'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import AllocationExecPanel from '../../components/AllocationExecPanel'
import {
  artifactProgressPercent,
  buildTaskRuntimeByName,
  extractJobSpecData,
  parseArtifactStates,
  shortenArtifactDigest,
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

  // Keyed on the route params so task→task navigation refetches.
  const [data] = createResource(() => `${jobName()}/${taskName()}`, async () => {
    const [jobInfo, rollouts, spec, ledger] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
      managerClient.getLedger({}),
    ])

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined

    const taskSpec = extractJobSpecData(spec.submissions)?.tasks.find((task) => task.name === taskName())
    const runtimeByTask = buildTaskRuntimeByName(rolloutStatus?.groups ?? [])
    const taskRuntime = runtimeByTask.get(taskName())

    // All running allocations for this job (from the ledger), regardless of
    // whether a rollout produced them. The exec panel targets these; the
    // manager rejects exec into an allocation whose task != this page's task.
    const runnable = new Set(['placed', 'active', 'running'])
    const jobAllocations = ledger.nodes
      .flatMap((node) => node.allocations)
      .filter((alloc) => alloc.jobId === jobName() && runnable.has(alloc.status))

    return {
      job: jobInfo.job,
      taskSpec,
      taskRuntime,
      jobAllocations,
      latestRollout,
      groups: rolloutStatus?.groups ?? [],
    }
  })

  return (
    <Show when={!data.loading} fallback={<div class="py-6"><Spinner label="Loading task…" /></div>}>
      <Show when={!data.error && data()} fallback={<Note tone="danger">Failed to load task details.</Note>}>
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
              </>
            }
            badges={<StatusBadge status={data()!.taskRuntime?.statuses[0] ?? 'unknown'} />}
          />

          <div class="space-y-4">
            <Show when={data()!.taskSpec} fallback={<Note tone="warn">Task not present in the current job spec.</Note>}>
              <SectionCard title="Definition">
                <DefList
                  cols={3}
                  items={[
                    ['Group', data()!.taskSpec!.group],
                    ['Kind', data()!.taskSpec!.kind],
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
                              <span class="font-mono text-xs text-muted" title="Persistent storage path">{data()!.taskSpec!.stateful!.storagePath}</span>
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

              <Show when={data()!.taskSpec!.artifacts.length > 0}>
                <SectionCard title="Artifacts" flush>
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

              <SectionCard title="Health">
                <Show
                  when={data()!.taskRuntime}
                  fallback={<p class="text-sm text-muted">No runtime health found for this task yet.</p>}
                >
                  <div class="flex flex-wrap items-center gap-2">
                    <StatusBadge
                      status={data()!.taskRuntime!.healthy === undefined ? 'unknown' : data()!.taskRuntime!.healthy ? 'healthy' : 'unhealthy'}
                    />
                    <Badge tone="neutral">{data()!.taskRuntime!.allocations.length} allocation{data()!.taskRuntime!.allocations.length === 1 ? '' : 's'}</Badge>
                  </div>
                  <Show when={data()!.taskRuntime!.health.length > 0}>
                    <ul class="mt-3 space-y-1 text-sm text-muted">
                      <For each={data()!.taskRuntime!.health}>{(health) => <li>· {health}</li>}</For>
                    </ul>
                  </Show>
                </Show>
              </SectionCard>

              <SectionCard
                title="Runtime allocations"
                actions={
                  <Show when={data()!.latestRollout}>
                    <A class={linkClass + ' text-sm'} href={`/rollouts/${data()!.latestRollout!.id}`}>Open rollout</A>
                  </Show>
                }
              >
                <Show when={data()!.taskRuntime?.allocations.length} fallback={<p class="text-sm text-muted">No runtime allocations available.</p>}>
                  <div class="space-y-2.5">
                    <For each={data()!.taskRuntime!.allocations}>
                      {(allocation) => {
                        const artifactRows = parseArtifactStates(allocation.artifactStates)
                        return (
                          <div class="rounded-lg border border-line bg-surface2/40 p-3">
                            <div class="flex flex-wrap items-center justify-between gap-2">
                              <p class="font-mono text-sm font-semibold">#{String(allocation.allocationId)}</p>
                              <StatusBadge status={allocation.status} />
                            </div>
                            <p class="mt-1 text-xs text-faint">
                              Worker <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A>
                              {' · '}group {allocation.taskGroup} · {allocation.lifecycle} · role {allocation.role}
                            </p>
                            <Show when={artifactRows.length > 0}>
                              <div class="mt-2 space-y-1.5 border-t border-line pt-2">
                                <p class="eyebrow">Artifacts</p>
                                <For each={artifactRows}>
                                  {(artifact) => (
                                    <div class="text-xs">
                                      <div class="flex flex-wrap items-center gap-2">
                                        <StatusBadge status={artifact.state} />
                                        <span class="max-w-[280px] truncate font-mono text-muted" title={artifact.source}>
                                          {artifact.name ?? artifact.source}
                                        </span>
                                        <Show when={artifactProgressLabel(artifact)}>
                                          <span class="text-faint">{artifactProgressLabel(artifact)}</span>
                                        </Show>
                                        <Show when={artifact.digest}>
                                          <span class="font-mono text-faint" title={artifact.digest}>{shortenArtifactDigest(artifact.digest!)}</span>
                                        </Show>
                                        <Show when={artifact.durationMs !== undefined}>
                                          <span class="text-faint">{formatDurationMs(artifact.durationMs!)}</span>
                                        </Show>
                                      </div>
                                      <Show when={artifact.state === 'failed' && artifact.error}>
                                        <p class="mt-0.5 text-red-600 dark:text-red-400">{artifact.error}</p>
                                      </Show>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </Show>
                          </div>
                        )
                      }}
                    </For>
                  </div>
                </Show>
              </SectionCard>
            </Show>

            <SectionCard title="Exec & actions" description="Run commands or declared actions inside this task's running allocations.">
              <Show
                when={data()!.jobAllocations.length > 0}
                fallback={<EmptyState icon="terminal" title="No running allocations" body="Once an allocation is running you can open a shell or trigger actions here." />}
              >
                <div class="space-y-2.5">
                  <For each={data()!.jobAllocations}>
                    {(alloc) => (
                      <div class="rounded-lg border border-line bg-surface2/40 p-3">
                        <div class="flex flex-wrap items-center justify-between gap-2">
                          <p class="font-mono text-sm font-semibold">#{String(alloc.id)}</p>
                          <div class="flex items-center gap-2">
                            <span class="text-xs text-faint">on {alloc.nodeId}</span>
                            <StatusBadge status={alloc.status} />
                          </div>
                        </div>
                        <AllocationExecPanel
                          allocationId={alloc.id}
                          jobName={jobName()}
                          taskName={taskName()}
                        />
                      </div>
                    )}
                  </For>
                </div>
              </Show>
            </SectionCard>

            <SectionCard title="Logs">
              <Show
                when={(data()!.taskRuntime?.allocations.length ?? 0) > 0}
                fallback={<p class="text-sm text-muted">No allocations are running for this task yet.</p>}
              >
                <LogViewer
                  job={jobName()}
                  allocationIds={data()!.taskRuntime!.allocations.map((allocation) => allocation.allocationId)}
                />
              </Show>
            </SectionCard>
          </div>
        </div>
      </Show>
    </Show>
  )
}
