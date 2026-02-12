import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { DetailBadge, DetailSection, detailDangerButtonClass, detailPrimaryButtonClass } from '../../components/DetailPrimitives'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'

const logLevelTone = (level: string) => {
  const normalized = level.toLowerCase()
  if (normalized.includes('error') || normalized.includes('fail')) return 'text-rose-400'
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
    <Show when={!data.loading} fallback={<p>Loading rollout…</p>}>
      <Show when={!data.error && data()?.rollout} fallback={<p class="text-rose-500">Failed to load rollout.</p>}>
        <div class="space-y-4">
          <DetailSection>
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 class="text-2xl font-semibold">Rollout #{params.id}</h2>
                <p class="text-sm text-neutral-500">Job <A href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}`} class="underline">{data()!.rollout!.jobId}</A> · Status {data()!.rollout!.status} · Placed allocations {data()!.placed.length}</p>
              </div>
              <div class="flex gap-2">
                <button type="button" onClick={advance} class={detailPrimaryButtonClass}>Advance</button>
                <button type="button" onClick={stop} class={detailDangerButtonClass}>Stop</button>
              </div>
            </div>
            <p class="mt-2 text-xs">From spec: <A class="underline" href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}/specs/${encodeURIComponent(data()!.rollout!.fromJobConfigId || '')}`}>{data()!.rollout!.fromJobConfigId || '—'}</A> → To spec: <A class="underline" href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}/specs/${encodeURIComponent(data()!.rollout!.toJobConfigId)}`}>{data()!.rollout!.toJobConfigId}</A></p>
            <Show when={message()}><p class="mt-2 text-sm text-neutral-600 dark:text-neutral-300">{message()}</p></Show>
          </DetailSection>

          <DetailSection title="Allocation health">
            <div class="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
              <For each={data()!.status.groups.flatMap((group) => group.allocations)}>
                {(allocation) => (
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <div class="font-semibold">Allocation #{String(allocation.allocationId)}</div>
                    <p class="text-xs text-neutral-500">{allocation.role} · {allocation.status}</p>
                    <p class="text-xs text-neutral-500">Worker <A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></p>
                    <div class="mt-2 flex gap-2 text-[11px] uppercase">
                      <DetailBadge tone={allocation.nodeHealthy ? 'success' : 'danger'}>Worker {allocation.nodeHealthy ? 'healthy' : 'unhealthy'}</DetailBadge>
                      <DetailBadge tone={allocation.allocationHealthy ? 'success' : 'danger'}>Alloc {allocation.allocationHealthy ? 'healthy' : 'unhealthy'}</DetailBadge>
                    </div>
                  </div>
                )}
              </For>
            </div>
          </DetailSection>

          <DetailSection title="Rollout event log" actions={<input value={logFilter()} onInput={(e) => setLogFilter(e.currentTarget.value)} placeholder="Filter log events" class="rounded border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"/>}>
            <Show when={filteredEvents().length > 0} fallback={<p class="text-sm text-neutral-500">No log events for this filter.</p>}>
              <div class="max-h-[360px] overflow-auto rounded border border-neutral-200 bg-neutral-950/95 p-2 font-mono text-xs text-neutral-100 dark:border-neutral-700">
                <For each={filteredEvents()}>
                  {(event) => (
                    <div class="grid grid-cols-[70px_170px_minmax(0,1fr)] items-start gap-2 border-b border-white/10 px-1 py-1.5 last:border-b-0">
                      <span class={`font-semibold uppercase tracking-wide ${logLevelTone(event.level)}`}>{event.level}</span>
                      <span class="text-neutral-400">{formatDate(event.createdAt)}</span>
                      <div class="min-w-0">
                        <span class="break-words text-neutral-100">{event.message}</span>
                        <Show when={event.groupName}>
                          <span class="ml-2 text-[11px] text-neutral-400">group={event.groupName}</span>
                        </Show>
                        <Show when={event.metadataJson && event.metadataJson !== '{}' }>
                          <span class="ml-2 break-words text-[11px] text-neutral-500">{event.metadataJson}</span>
                        </Show>
                      </div>
                    </div>
                  )}
                </For>
              </div>
            </Show>
          </DetailSection>
        </div>
      </Show>
    </Show>
  )
}
