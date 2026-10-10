import { A } from '@solidjs/router'
import { For, Show } from 'solid-js'
import type { SchedulerEvent } from '../gen/legion_pb'
import { newestFirst, pressureMeta, snapshotMeta } from '../utils/events'
import { formatBytes } from '../utils/format'
import { PressureSteps } from './NodeUsagePanel'
import { RelativeTime, SectionCard } from './ui'

/*
 * What a node's memory pressure response did, from `node.pressure` events.
 * The job page shows it for the job (the app developer's "why did my task
 * restart?"), the worker page for the node. Same shape as the job page's
 * preemption history, and hidden by the caller when there is nothing to say.
 */

/** Detail and hint as labeled lines: the hint is what the reader can change. */
export function PressureExplanation(props: { event: SchedulerEvent }) {
  const meta = () => pressureMeta(props.event)
  return (
    <div class="space-y-0.5 text-xs">
      <p class="text-ink">
        <span class="mr-1.5 font-semibold text-faint">Detail</span>
        {meta().detail ?? props.event.message}
      </p>
      <Show when={meta().hint}>
        <p class="text-amber-700 dark:text-amber-300">
          <span class="mr-1.5 font-semibold">Hint</span>
          {meta().hint}
        </p>
      </Show>
    </div>
  )
}

export function PressureHistoryCard(props: { events: readonly SchedulerEvent[]; scope: 'job' | 'node' }) {
  return (
    <SectionCard
      title="Memory pressure"
      description={
        props.scope === 'job'
          ? "When a node ran short of memory and its pressure response acted on this job's allocations. Evicted allocations reschedule with cause node_pressure. Best-effort: the event journal is bounded."
          : 'What this node did when it ran short of memory, most recent first. Best-effort: the event journal is bounded.'
      }
      flush
    >
      <ul class="divide-y divide-line">
        <For each={newestFirst(props.events)}>
          {(event) => {
            const meta = () => pressureMeta(event)
            return (
              <li class="space-y-1.5 px-4 py-3">
                <div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
                  <RelativeTime value={event.createdAt} class="whitespace-nowrap text-xs text-faint" />
                  <Show when={meta().step}>{(step) => <PressureSteps steps={[step()]} />}</Show>
                  <Show when={event.allocationId !== undefined}>
                    <span class="font-mono text-xs">#{String(event.allocationId)}</span>
                  </Show>
                  <Show when={props.scope === 'node' && event.jobId}>
                    <A href={`/jobs/${encodeURIComponent(event.jobId!)}`} class="font-medium hover:text-brass-strong">
                      {event.jobId}
                    </A>
                  </Show>
                  <Show when={props.scope === 'job' && event.nodeId}>
                    <span class="text-xs text-faint">
                      on{' '}
                      <A href={`/nodes/${encodeURIComponent(event.nodeId!)}`} class="text-muted hover:text-brass-strong">
                        {event.nodeId}
                      </A>
                    </span>
                  </Show>
                  <span class="flex-1" />
                  <span class="font-mono text-2xs text-muted">
                    {meta().step === 'reclaim'
                      ? `reclaimed ${formatBytes(meta().freedBytes ?? 0)}, nothing stopped`
                      : meta().freedBytes !== undefined
                        ? `freed ${formatBytes(meta().freedBytes)}`
                        : ''}
                  </span>
                </div>
                <PressureExplanation event={event} />
              </li>
            )
          }}
        </For>
      </ul>
    </SectionCard>
  )
}

/** The fields of a `snapshot.*` event worth reading without opening the JSON. */
export function SnapshotEventSummary(props: { event: SchedulerEvent }) {
  const meta = () => snapshotMeta(props.event)
  return (
    <p class="flex flex-wrap gap-x-4 gap-y-1 text-muted">
      <Show when={meta().snapshotId}>
        <span><span class="font-semibold text-faint">snapshot</span> {meta().name ? `${meta().name} (${meta().snapshotId})` : meta().snapshotId}</span>
      </Show>
      <Show when={meta().instanceId}>
        <span><span class="font-semibold text-faint">instance</span> {meta().instanceId}</span>
      </Show>
      <Show when={meta().kind}>
        <span><span class="font-semibold text-faint">kind</span> {meta().kind}</span>
      </Show>
      <Show when={meta().totalBytes}>
        <span><span class="font-semibold text-faint">size</span> {formatBytes(meta().totalBytes)}</span>
      </Show>
      <Show when={meta().durationMs}>
        <span><span class="font-semibold text-faint">took</span> {meta().durationMs} ms</span>
      </Show>
      <Show when={meta().error}>
        <span class="text-red-600 dark:text-red-400"><span class="font-semibold">error</span> {meta().error}</span>
      </Show>
    </p>
  )
}
