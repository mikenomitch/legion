import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js'
import type {
  Rollout,
  RolloutAllocationStatus,
  RolloutGroupStatus,
  GetRolloutStatusReply,
  RolloutEvent,
} from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { asDisplay, formatDate, formatRelativeTime } from '../utils/format'
import { POLL_INTERVAL_MS } from '../utils/polling'

const statusTone = (status: string) => {
  const normalized = status.toLowerCase()
  if (normalized.includes('failed')) {
    return 'border-red-300 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-200'
  }
  if (normalized.includes('completed')) {
    return 'border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-200'
  }
  if (normalized.includes('paused') || normalized.includes('waiting')) {
    return 'border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200'
  }
  return 'border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-900 dark:bg-sky-950 dark:text-sky-200'
}

const allocationTone = (allocation: RolloutAllocationStatus) => {
  const status = allocation.status.toLowerCase()
  if (allocation.allocationHealthy === false) {
    return 'border-red-500 bg-red-500'
  }
  if (allocation.allocationHealthy === true && allocation.role === 'replacement') {
    return 'border-emerald-500 bg-emerald-500'
  }
  if (allocation.nodeHealthy === false) {
    return 'border-red-500 bg-red-500'
  }
  if (status.includes('failed')) {
    return 'border-red-500 bg-red-500'
  }
  if (status.includes('running') || status.includes('placed')) {
    return allocation.role === 'replacement'
      ? 'border-emerald-500 bg-emerald-500'
      : 'border-amber-500 bg-amber-500'
  }
  if (status.includes('stopped') || status.includes('complete')) {
    return 'border-neutral-400 bg-neutral-400'
  }
  return 'border-neutral-300 bg-neutral-200 dark:border-neutral-600 dark:bg-neutral-700'
}

export default function Rollouts() {
  const [activeOnly, setActiveOnly] = createSignal(false)
  const [jobFilter, setJobFilter] = createSignal('')
  const [statusFilter, setStatusFilter] = createSignal('all')
  const [selectedId, setSelectedId] = createSignal<bigint | undefined>(undefined)
  const [selectedGroup, setSelectedGroup] = createSignal('all')
  const [logFilter, setLogFilter] = createSignal('')
  const [advanceTarget, setAdvanceTarget] = createSignal('all')
  const [advanceBusy, setAdvanceBusy] = createSignal(false)
  const [advanceMessage, setAdvanceMessage] = createSignal('')

  const fetchRollouts = async (): Promise<readonly Rollout[]> => {
    const response = await managerClient.listRollouts({ activeOnly: activeOnly() })
    return response.rollouts
  }

  const [rollouts, { refetch: refetchRollouts }] = createResource(fetchRollouts)

  const filteredRollouts = createMemo(() => {
    const normalizedJob = jobFilter().trim().toLowerCase()
    const normalizedStatus = statusFilter().trim().toLowerCase()
    return (rollouts() ?? [])
      .filter((rollout) => {
        if (normalizedJob && !rollout.jobId.toLowerCase().includes(normalizedJob)) {
          return false
        }
        if (normalizedStatus !== 'all' && rollout.status.toLowerCase() !== normalizedStatus) {
          return false
        }
        return true
      })
      .sort((a, b) => Number(b.id - a.id))
  })

  createEffect(() => {
    activeOnly()
    void refetchRollouts()
  })

  createEffect(() => {
    const current = selectedId()
    const available = filteredRollouts()
    if (!available.length) {
      setSelectedId(undefined)
      return
    }
    if (!current || !available.some((rollout) => rollout.id === current)) {
      setSelectedId(available[0].id)
    }
  })

  const fetchDetails = async (id?: bigint): Promise<GetRolloutStatusReply | undefined> => {
    if (!id) return undefined
    return managerClient.getRolloutStatus({ id })
  }
  const [details, { refetch: refetchDetails }] = createResource(selectedId, fetchDetails)

  const fetchEvents = async (id?: bigint): Promise<readonly RolloutEvent[]> => {
    if (!id) return []
    const response = await managerClient.listRolloutEvents({ rolloutId: id, limit: 1000 })
    return response.events
  }
  const [events, { refetch: refetchEvents }] = createResource(selectedId, fetchEvents)

  createEffect(() => {
    selectedId()
    setSelectedGroup('all')
    setAdvanceTarget('all')
  })

  createEffect(() => {
    const interval = window.setInterval(() => {
      void refetchRollouts()
      if (selectedId()) {
        void refetchDetails()
        void refetchEvents()
      }
    }, POLL_INTERVAL_MS)
    onCleanup(() => window.clearInterval(interval))
  })

  const displayedGroups = createMemo(() => {
    const all = details()?.groups ?? []
    const target = selectedGroup()
    if (target === 'all') return all
    return all.filter((group) => group.group === target)
  })

  const eventLog = createMemo(() => {
    const q = logFilter().trim().toLowerCase()
    const selected = selectedGroup()
    const entries = (events() ?? [])
      .filter((event) => selected === 'all' || event.groupName === selected)
      .sort((a, b) => Number(b.id - a.id))

    if (!q) {
      return entries
    }
    return entries.filter((entry) => {
      const haystack = [
        entry.groupName ?? 'all',
        entry.level,
        entry.message,
        entry.metadataJson,
      ]
        .join(' ')
        .toLowerCase()
      return haystack.includes(q)
    })
  })

  const statuses = createMemo(() => {
    const values = new Set<string>()
    for (const rollout of rollouts() ?? []) {
      values.add(rollout.status)
    }
    return ['all', ...Array.from(values).sort()]
  })

  const advanceRollout = async () => {
    const id = selectedId()
    if (!id || advanceBusy()) return
    setAdvanceBusy(true)
    setAdvanceMessage('')
    try {
      const groups = advanceTarget() === 'all' ? [] : [advanceTarget()]
      const response = await managerClient.advanceRollout({ id, groups })
      setAdvanceMessage(response.message || 'Rollout advance queued')
      await Promise.all([refetchRollouts(), refetchDetails()])
    } catch (error) {
      console.error('Failed to advance rollout', error)
      setAdvanceMessage('Failed to queue rollout advance')
    } finally {
      setAdvanceBusy(false)
    }
  }

  return (
    <div class="grid gap-4 xl:grid-cols-[380px_minmax(0,1fr)]">
      <section class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
        <header class="space-y-2">
          <h2 class="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Rollouts</h2>
          <p class="text-sm text-neutral-500 dark:text-neutral-400">Active and historical rollout executions.</p>
        </header>
        <div class="mt-3 space-y-2">
          <label class="block text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
            Job Filter
          </label>
          <input
            value={jobFilter()}
            onInput={(event) => setJobFilter(event.currentTarget.value)}
            placeholder="Filter by job id"
            class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
          />
          <div class="grid grid-cols-2 gap-2">
            <label class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
              Status
              <select
                value={statusFilter()}
                onChange={(event) => setStatusFilter(event.currentTarget.value)}
                class="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
              >
                <For each={statuses()}>{(status) => <option value={status}>{status}</option>}</For>
              </select>
            </label>
            <label class="flex items-end gap-2 pb-1 text-sm text-neutral-700 dark:text-neutral-300">
              <input
                type="checkbox"
                checked={activeOnly()}
                onChange={(event) => setActiveOnly(event.currentTarget.checked)}
              />
              Active only
            </label>
          </div>
        </div>
        <div class="mt-4 space-y-2">
          <Show when={!rollouts.loading} fallback={<p class="text-sm text-neutral-500">Loading rollouts…</p>}>
            <Show when={!rollouts.error} fallback={<p class="text-sm text-red-500">Failed to load rollouts.</p>}>
              <Show when={filteredRollouts().length > 0} fallback={<p class="text-sm text-neutral-500">No matching rollouts.</p>}>
                <For each={filteredRollouts()}>
                  {(rollout) => (
                    <button
                      type="button"
                      onClick={() => setSelectedId(rollout.id)}
                      class={`w-full rounded border p-3 text-left transition ${
                        selectedId() === rollout.id
                          ? 'border-neutral-900 bg-neutral-100 dark:border-neutral-100 dark:bg-neutral-800'
                          : 'border-neutral-200 bg-white hover:border-neutral-400 dark:border-neutral-700 dark:bg-neutral-900'
                      }`}
                    >
                      <div class="flex items-center justify-between gap-2">
                        <div class="font-semibold text-neutral-900 dark:text-neutral-100">
                          #{asDisplay(rollout.id)} {rollout.jobId}
                        </div>
                        <span class={`rounded border px-2 py-0.5 text-[11px] uppercase tracking-wide ${statusTone(rollout.status)}`}>
                          {rollout.status}
                        </span>
                      </div>
                      <div class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">
                        Updated {formatRelativeTime(rollout.updatedAt)}
                      </div>
                    </button>
                  )}
                </For>
              </Show>
            </Show>
          </Show>
        </div>
      </section>

      <section class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
        <Show when={selectedId()} fallback={<p class="text-neutral-500">Select a rollout to inspect details.</p>}>
          <Show when={!details.loading} fallback={<p class="text-neutral-500">Loading rollout details…</p>}>
            <Show when={!details.error && details()} fallback={<p class="text-red-500">Failed to load rollout details.</p>}>
              {(detail) => (
                <div class="space-y-4">
                  <header class="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <h2 class="text-xl font-semibold text-neutral-900 dark:text-neutral-100">
                        Rollout #{asDisplay(detail().rollout?.id)} for {asDisplay(detail().rollout?.jobId)}
                      </h2>
                      <p class="text-sm text-neutral-500 dark:text-neutral-400">
                        Created {formatDate(detail().rollout?.createdAt)} • Updated {formatRelativeTime(detail().rollout?.updatedAt)}
                      </p>
                    </div>
                    <span class={`rounded border px-3 py-1 text-xs uppercase tracking-wide ${statusTone(detail().rollout?.status ?? '')}`}>
                      {asDisplay(detail().rollout?.status)}
                    </span>
                  </header>

                  <div class="grid gap-3 md:grid-cols-[1fr_auto]">
                    <div class="space-y-2">
                      <label class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
                        Group Scope
                      </label>
                      <select
                        value={selectedGroup()}
                        onChange={(event) => setSelectedGroup(event.currentTarget.value)}
                        class="w-full rounded border border-neutral-300 bg-white px-2 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
                      >
                        <option value="all">all groups</option>
                        <For each={detail().groups}>{(group) => <option value={group.group}>{group.group}</option>}</For>
                      </select>
                    </div>
                    <div class="space-y-2">
                      <label class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
                        Advance Target
                      </label>
                      <div class="flex gap-2">
                        <select
                          value={advanceTarget()}
                          onChange={(event) => setAdvanceTarget(event.currentTarget.value)}
                          class="rounded border border-neutral-300 bg-white px-2 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
                        >
                          <option value="all">all groups</option>
                          <For each={detail().groups}>{(group) => <option value={group.group}>{group.group}</option>}</For>
                        </select>
                        <button
                          type="button"
                          onClick={advanceRollout}
                          disabled={advanceBusy()}
                          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm font-semibold uppercase tracking-[0.08em] text-neutral-800 shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 disabled:opacity-60 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100 dark:shadow-[3px_3px_0_#0b0d12]"
                        >
                          {advanceBusy() ? 'Advancing…' : 'Advance'}
                        </button>
                      </div>
                    </div>
                  </div>
                  <Show when={advanceMessage()}>
                    <p class="text-sm text-neutral-600 dark:text-neutral-300">{advanceMessage()}</p>
                  </Show>

                  <div class="space-y-2">
                    <h3 class="text-sm font-semibold uppercase tracking-[0.1em] text-neutral-700 dark:text-neutral-300">Groups</h3>
                    <For each={displayedGroups()}>
                      {(group: RolloutGroupStatus) => (
                        <article class="rounded border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-800 dark:bg-neutral-950/50">
                          <div class="flex flex-wrap items-center justify-between gap-2">
                            <div class="font-semibold text-neutral-900 dark:text-neutral-100">{group.group}</div>
                            <div class="text-xs text-neutral-600 dark:text-neutral-400">
                              Step {group.stepIndex}/{group.totalSteps || 1} • Manual credits {group.pendingManualAdvances}
                            </div>
                          </div>
                          <Show
                            when={group.allocations.length <= 200}
                            fallback={
                              <div class="mt-2 text-xs text-neutral-600 dark:text-neutral-400">
                                Large scale view ({group.allocations.length} allocations): use the log panel below to filter.
                              </div>
                            }
                          >
                            <div class="mt-2 grid grid-cols-12 gap-1 sm:grid-cols-16 lg:grid-cols-20">
                              <For each={group.allocations}>
                                {(allocation) => (
                                  <div
                                    class={`h-3 w-3 rounded-sm border ${allocationTone(allocation)}`}
                                    title={`alloc=${allocation.allocationId} task=${allocation.taskName} role=${allocation.role} node=${allocation.nodeId} status=${allocation.status} health=${allocation.allocationHealth || 'unknown'}`}
                                  />
                                )}
                              </For>
                            </div>
                          </Show>
                        </article>
                      )}
                    </For>
                  </div>

                  <div class="space-y-2">
                    <div class="flex flex-wrap items-center justify-between gap-2">
                      <h3 class="text-sm font-semibold uppercase tracking-[0.1em] text-neutral-700 dark:text-neutral-300">Rollout Log</h3>
                      <input
                        value={logFilter()}
                        onInput={(event) => setLogFilter(event.currentTarget.value)}
                        placeholder="Filter event log"
                        class="rounded border border-neutral-300 bg-white px-3 py-1.5 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
                      />
                    </div>
                    <div class="max-h-[360px] space-y-1 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-2 text-xs dark:border-neutral-800 dark:bg-neutral-950/50">
                      <Show when={eventLog().length > 0} fallback={<p class="text-neutral-500">No rollout events for this filter.</p>}>
                        <For each={eventLog()}>
                          {(entry) => (
                            <div class="rounded border border-neutral-200 bg-white px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                              <span class="font-semibold text-neutral-800 dark:text-neutral-100">
                                {entry.groupName ?? 'all'}
                              </span>
                              <span class="mx-1 rounded border border-neutral-300 px-1 py-0.5 uppercase tracking-wide text-neutral-600 dark:border-neutral-700 dark:text-neutral-300">
                                {entry.level}
                              </span>
                              <span class="mx-1 text-neutral-500">{formatRelativeTime(entry.createdAt)}</span>
                              <span class="text-neutral-800 dark:text-neutral-100">{entry.message}</span>
                              <Show when={entry.metadataJson && entry.metadataJson !== '{}' }>
                                <span class="mx-1 text-neutral-500">{entry.metadataJson}</span>
                              </Show>
                            </div>
                          )}
                        </For>
                      </Show>
                    </div>
                  </div>
                </div>
              )}
            </Show>
          </Show>
        </Show>
      </section>
    </div>
  )
}
