import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { Badge, Button, ConfirmButton, Icon, Note, PageHeader, SectionCard, StatusBadge, cx, inputClass } from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'
import { artifactProgressPercent, parseArtifactStates } from '../../utils/jobspec'

const isRolloutDone = (status: string) => {
  const normalizedStatus = status.toLowerCase()
  return (
    normalizedStatus === 'completed'
    || normalizedStatus === 'failed'
    || normalizedStatus === 'done'
    || normalizedStatus === 'cancelled'
    || normalizedStatus === 'completed_incomplete'
  )
}

const logLevelTone = (level: string) => {
  const normalized = level.toLowerCase()
  if (normalized.includes('error') || normalized.includes('fail')) return 'text-red-400'
  if (normalized.includes('warn')) return 'text-amber-300'
  if (normalized.includes('debug') || normalized.includes('trace')) return 'text-sky-300'
  return 'text-emerald-300'
}

export default function RolloutDetail() {
  const params = useParams()
  const rolloutId = createMemo(() => BigInt(params.id))
  const [logFilter, setLogFilter] = createSignal('')
  const [message, setMessage] = createSignal('')

  const [data, { refetch }] = createResource(async () => {
    const [status, events, ledger] = await Promise.all([
      managerClient.getRolloutStatus({ id: rolloutId() }),
      managerClient.listRolloutEvents({ rolloutId: rolloutId(), limit: 1000 }),
      managerClient.getLedger({}),
    ])
    const rollout = status.rollout
    const placed = rollout
      ? ledger.nodes.flatMap((node) => node.allocations).filter((allocation) => allocation.jobConfigId === rollout.toJobConfigId)
      : []
    return { status, rollout, events: events.events, placed }
  })

  const filteredEvents = createMemo(() => {
    const q = logFilter().trim().toLowerCase()
    const events = data()?.events ?? []
    if (!q) return events
    return events.filter((event) => `${event.level} ${event.message} ${event.groupName ?? ''} ${event.metadataJson}`.toLowerCase().includes(q))
  })

  const advance = async () => {
    await managerClient.advanceRollout({ id: rolloutId(), groups: [] })
    setMessage('Rollout advance queued')
    await refetch()
  }

  const stop = async () => {
    const jobId = data()?.rollout?.jobId
    if (!jobId) return
    try {
      await managerClient.stopJob({ name: jobId })
      setMessage('Stop requested for associated job')
    } catch {
      setMessage('Stop request failed')
    }
  }

  return (
    <Show when={!data.loading} fallback={<div class="py-6 text-sm text-muted">Loading rollout…</div>}>
      <Show when={!data.error && data()?.rollout} fallback={<Note tone="danger">Failed to load rollout.</Note>}>
        {(() => {
          const rollout = data()!.rollout!
          const isManualProgression = rollout.progression?.toLowerCase() === 'manual'
          const done = isRolloutDone(rollout.status)

          return (
            <div>
              <PageHeader
                eyebrow="Rollout"
                title={`#${params.id}`}
                badges={
                  <>
                    <StatusBadge status={rollout.status} />
                    <Badge tone="neutral">{data()!.placed.length} placed</Badge>
                  </>
                }
                subtitle={
                  <>
                    Job{' '}
                    <A href={`/jobs/${encodeURIComponent(rollout.jobId)}`} class="font-medium text-brass-strong underline-offset-2 hover:underline">
                      {rollout.jobId}
                    </A>
                    <span class="mx-2 text-faint">·</span>
                    <span class="font-mono text-xs">
                      <A class="hover:text-brass-strong" href={`/jobs/${encodeURIComponent(rollout.jobId)}/specs/${encodeURIComponent(rollout.fromJobConfigId || '')}`}>
                        {rollout.fromJobConfigId || '—'}
                      </A>
                      {' → '}
                      <A class="hover:text-brass-strong" href={`/jobs/${encodeURIComponent(rollout.jobId)}/specs/${encodeURIComponent(rollout.toJobConfigId)}`}>
                        {rollout.toJobConfigId}
                      </A>
                    </span>
                  </>
                }
                actions={
                  <>
                    <Show when={isManualProgression}>
                      <Button variant="primary" icon="play" onClick={() => void advance()}>Advance</Button>
                    </Show>
                    <Show when={!done}>
                      <ConfirmButton label="Stop" icon="pause" onConfirm={() => void stop()} />
                    </Show>
                  </>
                }
              />
              <Show when={message()}>
                <Note tone="info" class="mb-4">{message()}</Note>
              </Show>

              <div class="space-y-4">
                <SectionCard title="Allocation health">
                  <Show
                    when={data()!.status.groups.some((group) => group.allocations.length > 0)}
                    fallback={<p class="text-sm text-muted">No allocations reported for this rollout yet.</p>}
                  >
                    <div class="grid gap-2.5 md:grid-cols-2 xl:grid-cols-3">
                      <For each={data()!.status.groups.flatMap((group) => group.allocations)}>
                        {(allocation) => {
                          const artifactRows = parseArtifactStates(allocation.artifactStates)
                          const stagedCount = artifactRows.filter((row) => row.state === 'staged').length
                          const staging = artifactRows.length > 0 && stagedCount < artifactRows.length
                          return (
                            <div class="rounded-lg border border-line bg-surface2/40 p-3">
                              <div class="flex flex-wrap items-center justify-between gap-2">
                                <p class="font-mono text-sm font-semibold">#{String(allocation.allocationId)}</p>
                                <StatusBadge status={allocation.status} />
                              </div>
                              <p class="mt-1 text-xs text-faint">
                                {allocation.role} · worker{' '}
                                <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A>
                              </p>
                              <div class="mt-2 flex flex-wrap gap-1.5">
                                <Badge tone={allocation.nodeHealthy ? 'ok' : 'danger'} dot>worker {allocation.nodeHealthy ? 'healthy' : 'unhealthy'}</Badge>
                                <Badge tone={allocation.allocationHealthy ? 'ok' : 'danger'} dot>alloc {allocation.allocationHealthy ? 'healthy' : 'unhealthy'}</Badge>
                              </div>
                              <Show when={staging}>
                                <div class="mt-2 border-t border-line pt-2 text-xs">
                                  <p class="font-medium text-amber-700 dark:text-amber-300">staging artifacts · {stagedCount}/{artifactRows.length} staged</p>
                                  <ul class="mt-1.5 space-y-1">
                                    <For each={artifactRows}>
                                      {(artifact) => (
                                        <li class="flex flex-wrap items-center gap-2">
                                          <StatusBadge status={artifact.state} />
                                          <span class="max-w-[200px] truncate font-mono text-muted" title={artifact.source}>{artifact.name ?? artifact.source}</span>
                                          <Show when={artifactProgressPercent(artifact) !== undefined}>
                                            <span class="text-faint">{artifactProgressPercent(artifact)}%</span>
                                          </Show>
                                          <Show when={artifact.state === 'failed' && artifact.error}>
                                            <span class="text-red-600 dark:text-red-400">{artifact.error}</span>
                                          </Show>
                                        </li>
                                      )}
                                    </For>
                                  </ul>
                                </div>
                              </Show>
                            </div>
                          )
                        }}
                      </For>
                    </div>
                  </Show>
                </SectionCard>

                <SectionCard
                  title="Event log"
                  actions={
                    <div class="relative">
                      <Icon name="search" size={13} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
                      <input
                        value={logFilter()}
                        onInput={(e) => setLogFilter(e.currentTarget.value)}
                        placeholder="Filter events…"
                        class={cx(inputClass, 'w-56 pl-8')}
                      />
                    </div>
                  }
                >
                  <Show when={filteredEvents().length > 0} fallback={<p class="text-sm text-muted">No log events for this filter.</p>}>
                    <div class="max-h-[360px] overflow-auto rounded-md border border-line bg-[#0d0c0a] p-2 font-mono text-xs text-neutral-100">
                      <For each={filteredEvents()}>
                        {(event) => (
                          <div class="grid grid-cols-[64px_150px_minmax(0,1fr)] items-start gap-2 border-b border-white/5 px-1 py-1.5 last:border-b-0">
                            <span class={`font-semibold uppercase tracking-wide ${logLevelTone(event.level)}`}>{event.level}</span>
                            <span class="text-neutral-400">{formatDate(event.createdAt)}</span>
                            <div class="min-w-0">
                              <span class="break-words text-neutral-100">{event.message}</span>
                              <Show when={event.groupName}>
                                <span class="ml-2 text-[11px] text-neutral-400">group={event.groupName}</span>
                              </Show>
                              <Show when={event.metadataJson && event.metadataJson !== '{}'}>
                                <span class="ml-2 break-words text-[11px] text-neutral-500">{event.metadataJson}</span>
                              </Show>
                            </div>
                          </div>
                        )}
                      </For>
                    </div>
                  </Show>
                </SectionCard>
              </div>
            </div>
          )
        })()}
      </Show>
    </Show>
  )
}
