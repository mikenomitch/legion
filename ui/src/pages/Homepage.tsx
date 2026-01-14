import { For, Show, type Resource } from 'solid-js'
import type { LedgerNode } from '../gen/legion_pb'
import { asDisplay, formatRelativeTime } from '../utils/format'
import { healthToneClasses, summarizeHealthChecks } from '../utils/health'

interface HomepageProps {
  ledger: Resource<readonly LedgerNode[] | undefined>
}

export default function Homepage(props: HomepageProps) {
  return (
    <Show when={!props.ledger.loading} fallback={<p class="text-neutral-500">Loading ledger…</p>}>
      <Show when={!props.ledger.error} fallback={<p class="text-red-500">Failed to load ledger.</p>}>
        {props.ledger() && props.ledger()!.length > 0 ? (
          <div class="space-y-4">
            <For each={props.ledger()!}>
              {(item) => (
                <article class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[6px_6px_0_#d4d4d8] transition dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[6px_6px_0_#0b0d12]">
                  <header class="flex items-center justify-between gap-4">
                    <div>
                      <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Node</p>
                      <div class="font-semibold text-neutral-900 dark:text-neutral-100">{item.node?.id ?? 'Unknown node'}</div>
                    </div>
                    <span class="rounded border border-neutral-300 px-3 py-1 text-xs uppercase tracking-wide text-neutral-800 dark:border-neutral-700 dark:text-neutral-200">
                      {item.node?.status ?? 'N/A'}
                    </span>
                  </header>
                  <div class="mt-3 grid grid-cols-2 gap-2 text-sm text-neutral-700 dark:text-neutral-300 md:grid-cols-3">
                    <span class="rounded border border-neutral-200 bg-neutral-50 px-3 py-2 dark:border-neutral-800 dark:bg-neutral-900">
                      Pool: {item.node?.pool ?? '—'}
                    </span>
                    <span class="rounded border border-neutral-200 bg-neutral-50 px-3 py-2 dark:border-neutral-800 dark:bg-neutral-900">
                      Availability: {item.node?.availability ?? '—'}
                    </span>
                      <span class="rounded border border-neutral-200 bg-neutral-50 px-3 py-2 dark:border-neutral-800 dark:bg-neutral-900">
                        Last heartbeat: {formatRelativeTime(item.node?.lastHeartbeat)}
                      </span>
                  </div>
                  <div class="mt-4">
                    <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Allocations</p>
                    <div class="mt-2 space-y-2">
                      <Show
                        when={item.allocations.length}
                        fallback={
                            <p class="rounded border border-dashed border-neutral-200 px-3 py-2 text-sm text-neutral-500 dark:border-neutral-700 dark:text-neutral-500">
                              No current allocations
                            </p>
                        }
                      >
                        <For each={item.allocations}>
                          {(allocation) => (
                            <div class="rounded border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-800 dark:bg-neutral-950/60">
                              {(() => {
                                const healthSummary = summarizeHealthChecks(item.node?.healthChecks)
                                return (
                                  <div class="mb-2 flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-wide">
                                    <span class={`rounded border px-2 py-1 ${healthToneClasses(healthSummary.tone)}`}>
                                      {healthSummary.label}
                                    </span>
                                  </div>
                                )
                              })()}
                              <div class="flex flex-wrap items-center justify-between gap-2 text-sm text-neutral-900 dark:text-neutral-100">
                                <span class="font-semibold">Allocation #{asDisplay(allocation.id)}</span>
                                <span class="rounded border border-neutral-300 px-2 py-1 text-[11px] uppercase tracking-wide dark:border-neutral-700">
                                  {allocation.status}
                                </span>
                              </div>
                              <div class="mt-2 grid gap-2 text-xs text-neutral-600 dark:text-neutral-400 md:grid-cols-3">
                                <span class="rounded border border-neutral-200 bg-white px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                  Resources: {asDisplay(allocation.resources)}
                                </span>
                                <span class="rounded border border-neutral-200 bg-white px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                  Job Config: {asDisplay(allocation.jobConfigId)}
                                </span>
                                <span class="rounded border border-neutral-200 bg-white px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                  Task Config: {asDisplay(allocation.taskConfigId)}
                                </span>
                              </div>
                            </div>
                          )}
                        </For>
                      </Show>
                    </div>
                  </div>
                </article>
              )}
            </For>
          </div>
        ) : (
          <p class="text-neutral-500">No ledger entries available.</p>
        )}
      </Show>
    </Show>
  )
}
