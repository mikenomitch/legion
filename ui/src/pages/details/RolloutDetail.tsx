import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'

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
          <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 class="text-2xl font-semibold">Rollout #{params.id}</h2>
                <p class="text-sm text-neutral-500">Job <A href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}`} class="underline">{data()!.rollout!.jobId}</A> · Status {data()!.rollout!.status} · Placed allocations {data()!.placed.length}</p>
              </div>
              <div class="flex gap-2">
                <button type="button" onClick={advance} class="rounded border px-3 py-2 text-sm font-semibold">Advance</button>
                <button type="button" onClick={stop} class="rounded border border-rose-300 px-3 py-2 text-sm font-semibold text-rose-700 dark:border-rose-700 dark:text-rose-300">Stop</button>
              </div>
            </div>
            <p class="mt-2 text-xs">From spec: <A class="underline" href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}/specs/${encodeURIComponent(data()!.rollout!.fromJobConfigId || '')}`}>{data()!.rollout!.fromJobConfigId || '—'}</A> → To spec: <A class="underline" href={`/jobs/${encodeURIComponent(data()!.rollout!.jobId)}/specs/${encodeURIComponent(data()!.rollout!.toJobConfigId)}`}>{data()!.rollout!.toJobConfigId}</A></p>
            <Show when={message()}><p class="mt-2 text-sm text-neutral-600 dark:text-neutral-300">{message()}</p></Show>
          </section>

          <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <h3 class="mb-2 font-semibold">Allocation health</h3>
            <div class="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
              <For each={data()!.status.groups.flatMap((group) => group.allocations)}>
                {(allocation) => (
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <div class="font-semibold">Allocation #{String(allocation.allocationId)}</div>
                    <p class="text-xs text-neutral-500">{allocation.role} · {allocation.status}</p>
                    <p class="text-xs text-neutral-500">Node <A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></p>
                    <div class="mt-2 flex gap-2 text-[11px] uppercase">
                      <span class={`rounded border px-2 py-0.5 ${allocation.nodeHealthy ? 'border-emerald-400 text-emerald-700' : 'border-rose-400 text-rose-700'}`}>Node {allocation.nodeHealthy ? 'healthy' : 'unhealthy'}</span>
                      <span class={`rounded border px-2 py-0.5 ${allocation.allocationHealthy ? 'border-emerald-400 text-emerald-700' : 'border-rose-400 text-rose-700'}`}>Alloc {allocation.allocationHealthy ? 'healthy' : 'unhealthy'}</span>
                    </div>
                  </div>
                )}
              </For>
            </div>
          </section>

          <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <div class="mb-2 flex items-center justify-between gap-2"><h3 class="font-semibold">Rollout event log</h3><input value={logFilter()} onInput={(e) => setLogFilter(e.currentTarget.value)} placeholder="Filter log events" class="rounded border border-neutral-300 px-3 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-900"/></div>
            <div class="space-y-2">
              <For each={filteredEvents()}>
                {(event) => (
                  <div class="rounded border border-neutral-200 p-3 text-sm dark:border-neutral-700">
                    <div class="flex items-center justify-between gap-2"><span class="font-semibold">{event.level}</span><span class="text-xs text-neutral-500">{formatDate(event.createdAt)}</span></div>
                    <p class="mt-1">{event.message}</p>
                    <Show when={event.groupName}><p class="mt-1 text-xs text-neutral-500">Group: {event.groupName}</p></Show>
                  </div>
                )}
              </For>
            </div>
          </section>
        </div>
      </Show>
    </Show>
  )
}
