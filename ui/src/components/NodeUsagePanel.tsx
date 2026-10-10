import { A } from '@solidjs/router'
import { For, Show } from 'solid-js'
import type { GetNodeUsageReply, NodeAllocationUsage, NodeResourceUsage } from '../gen/legion_pb'
import {
  UNREPORTED_REASONS,
  bytesToMiB,
  cpuCapacityLabel,
  fillTone,
  formatAmount,
  formatCores,
  formatIdle,
  formatMiB,
  isBursting,
  percentOf,
  physicalCapacity,
  pressureOrder,
  pressureSummary,
  ratioLabel,
  reservationLabel,
  stepDescription,
  stepTone,
  usedCoresLabel,
} from '../utils/usage'
import { Badge, Dot, Meter, MonoBadge, RelativeTime, SectionCard, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * The worker page's Usage card, built from one GetNodeUsage reply: capacity
 * (with the oversubscription ratio), reserved and used per resource, the
 * memory pressure response's status, every allocation's reservation and
 * measured use, and the order the response would act in. It mirrors
 * `legion node usage` so the CLI and the UI never disagree.
 *
 * Presentational only: the reply comes from the caller, which is what lets
 * /testing render it from fixtures.
 */

/** The step names a pressure order row will take, as chips joined by arrows. */
export function PressureSteps(props: { steps: readonly string[] }) {
  return (
    <span class="inline-flex flex-wrap items-center gap-1">
      <For each={props.steps}>
        {(step, index) => (
          <>
            <Show when={index() > 0}>
              <span class="text-2xs text-faint" aria-hidden="true">→</span>
            </Show>
            <Badge tone={stepTone(step)} mono title={stepDescription(step)}>
              {step}
            </Badge>
          </>
        )}
      </For>
    </span>
  )
}

function ResourceRow(props: { name: 'memory' | 'cpu'; resource: NodeResourceUsage; reported: boolean }) {
  // A node can fail to fingerprint a resource (CPU MHz on some macOS hosts).
  // Percentages of zero would read as "empty", so say so instead.
  const known = () => props.resource.capacity > 0
  // Apple Silicon reports cores but no usable clock speed: say how many
  // cores, and measure use in cores, rather than "capacity unknown".
  const coresOnly = () => (props.name === 'cpu' ? cpuCapacityLabel(props.resource) : undefined)
  const ratio = () => ratioLabel(props.resource.oversubscribeRatio)
  const physical = () => physicalCapacity(props.resource)
  const reservedPercent = () => percentOf(props.resource.reserved, props.resource.capacity)
  // Used is compared to what the node physically has: an oversubscribed
  // node cannot use its ratio.
  const usedPercent = () =>
    coresOnly() ? percentOf(Number(props.resource.usedMillicores) / 1000, props.resource.cores) : percentOf(props.resource.used, physical())
  return (
    <div>
      <div class="flex flex-wrap items-baseline justify-between gap-x-3 text-sm">
        <p class="font-medium text-ink">
          {props.name === 'memory' ? 'Memory' : 'CPU'}
          <span class="ml-2 font-mono text-xs font-normal text-muted">
            {known() ? formatAmount(props.name, physical()) : (coresOnly() ?? 'capacity unknown')}
            <Show when={ratio()}>
              {(label) => (
                <span
                  class="ml-1 text-brass-strong"
                  title={`Oversubscribed ${label()}: the scheduler packs against ${formatAmount(props.name, props.resource.capacity)}`}
                >
                  {label()}
                </span>
              )}
            </Show>
          </span>
        </p>
      </div>
      <div class="mt-1.5 space-y-1.5">
        <div>
          <div class="mb-0.5 flex justify-between font-mono text-2xs tabular-nums text-muted">
            <span>reserved</span>
            <span>
              {formatAmount(props.name, props.resource.reserved)}
              <Show when={known()}> · {reservedPercent()}%</Show>
            </span>
          </div>
          <Meter percent={reservedPercent()} tone={fillTone(reservedPercent())} label={`${props.name} reserved`} />
        </div>
        <div>
          <div class="mb-0.5 flex justify-between font-mono text-2xs tabular-nums text-muted">
            <span>used</span>
            <span>
              <Show when={props.reported} fallback="not measured">
                <Show when={coresOnly()} fallback={formatAmount(props.name, props.resource.used)}>
                  {usedCoresLabel(props.resource)}
                </Show>
                <Show when={known() || coresOnly()}> · {usedPercent()}%</Show>
              </Show>
            </span>
          </div>
          <Meter percent={props.reported ? usedPercent() : 0} tone={fillTone(usedPercent())} label={`${props.name} used`} />
        </div>
      </div>
    </div>
  )
}

function AllocationLink(props: { allocation: NodeAllocationUsage }) {
  return (
    <span class="inline-flex flex-wrap items-center gap-1.5">
      <span class="font-mono text-xs">#{String(props.allocation.allocationId)}</span>
      <A class="font-medium hover:text-brass-strong" href={`/jobs/${encodeURIComponent(props.allocation.jobId)}`}>
        {props.allocation.jobId}
      </A>
    </span>
  )
}

export function NodeUsageCard(props: { usage: GetNodeUsageReply; class?: string }) {
  const reported = () => props.usage.usageReported
  const pressure = () => pressureSummary(props.usage.memory, reported())
  const order = () => pressureOrder(props.usage.allocations)
  const steps = () => props.usage.memory?.pressureResponse?.steps ?? []

  return (
    <SectionCard
      title="Usage"
      class={props.class}
      description={
        <Show when={reported()} fallback="Capacity and reservations. This node has not reported measured usage.">
          Measured use against reservations.{' '}
          <span title="Usage reports arrive every 10 seconds">
            Sampled <RelativeTime value={String(props.usage.sampledAtUnixMs)} />
          </span>
          <Show when={Number(props.usage.reportAgeSeconds) > 30}>
            <span class="text-amber-600 dark:text-amber-400"> · the node stopped reporting</span>
          </Show>
        </Show>
      }
      actions={
        <Badge tone={pressure().tone} dot class="uppercase tracking-wide" title={pressure().title}>
          {pressure().label}
          <Show when={pressure().detail}>
            <span class="normal-case tracking-normal opacity-80">· {pressure().detail}</span>
          </Show>
        </Badge>
      }
      flush
    >
      <div class="grid gap-x-8 gap-y-4 p-4 md:grid-cols-2">
        <Show when={props.usage.memory}>{(memory) => <ResourceRow name="memory" resource={memory()} reported={reported()} />}</Show>
        <Show when={props.usage.cpu}>{(cpu) => <ResourceRow name="cpu" resource={cpu()} reported={reported()} />}</Show>
      </div>

      <Show when={!reported()}>
        <p class="border-t border-line px-4 py-2.5 text-xs text-muted">
          No usage reported. Likely causes: {UNREPORTED_REASONS.join('; ')}.
        </p>
      </Show>

      <Show
        when={props.usage.allocations.length > 0}
        fallback={<p class="border-t border-line px-4 py-3 text-sm text-muted">Nothing is placed on this node.</p>}
      >
        <TableShell class="border-t border-line">
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <th class={thClass}>Allocation</th>
                <th class={thClass}>Instance</th>
                <th class={thClass + ' text-right'} title="Reservation / burst ceiling (resources.memoryMax)">Res/Max MiB</th>
                <th class={thClass + ' text-right'}>Used MiB</th>
                <th class={thClass + ' text-right'}>Peak MiB</th>
                <th class={thClass + ' text-right'} title="Average cores over the last report interval">CPU</th>
                <th class={thClass + ' text-right'} title="Time since the allocation last used 1% of a core or moved traffic">Idle</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              <For each={props.usage.allocations}>
                {(allocation) => (
                  <tr class={trHoverClass}>
                    <td class={tdClass}>
                      <AllocationLink allocation={allocation} />
                    </td>
                    <td class={tdClass}>
                      <Show when={allocation.instanceId} fallback={<span class="text-faint">-</span>}>
                        <MonoBadge>{allocation.instanceId}</MonoBadge>
                      </Show>
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                      {reservationLabel(allocation.reservedMemoryMib, allocation.memoryMaxMib)}
                    </td>
                    <td
                      class={cx(tdClass, 'text-right font-mono text-xs tabular-nums', isBursting(allocation) && 'text-amber-600 dark:text-amber-400')}
                      title={isBursting(allocation) ? 'Using more than its reservation: the first to give memory back under pressure' : undefined}
                    >
                      <Show when={allocation.usage} fallback={<span class="text-faint">-</span>}>
                        {(usage) => formatMiB(bytesToMiB(usage().memoryCurrentBytes))}
                      </Show>
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                      <Show when={allocation.usage} fallback={<span class="text-faint">-</span>}>
                        {(usage) => formatMiB(bytesToMiB(usage().memoryPeakBytes))}
                      </Show>
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                      {formatCores(allocation.usage?.cpuMillicores)}
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                      {allocation.usage ? formatIdle(allocation.idleSeconds) : '-'}
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </TableShell>
      </Show>

      <Show when={reported()}>
        <div class="border-t border-line px-4 py-3">
          <p class="eyebrow mb-2">Under pressure, in order</p>
          <Show
            when={props.usage.memory?.pressureResponse}
            fallback={<p class="text-sm text-muted">This worker does not report a pressure order (it predates the memory pressure response).</p>}
          >
            <Show
              when={props.usage.memory?.pressureResponse?.enabled}
              fallback={<p class="text-sm text-muted">The memory pressure response is off on this node. The kernel OOM killer is the only backstop.</p>}
            >
              <Show
                when={order().length > 0}
                fallback={
                  <p class="text-sm text-muted">
                    Nothing would be stopped or reclaimed for memory on this node right now. Steps enabled:{' '}
                    <PressureSteps steps={steps()} />
                  </p>
                }
              >
                <ol class="space-y-1.5">
                  <For each={order()}>
                    {(allocation) => (
                      <li class="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
                        <span class="w-5 text-right font-mono text-xs tabular-nums text-faint">{allocation.pressureRank}</span>
                        <AllocationLink allocation={allocation} />
                        <Show when={allocation.band}>
                          <Badge tone="neutral" mono title="Priority band: lower bands go first">{allocation.band}</Badge>
                        </Show>
                        <span class="font-mono text-2xs tabular-nums text-muted" title="Reserved / used MiB">
                          {formatMiB(allocation.reservedMemoryMib)}/
                          {allocation.usage ? formatMiB(bytesToMiB(allocation.usage.memoryCurrentBytes)) : '-'} MiB
                        </span>
                        <span class="flex-1" />
                        <PressureSteps steps={allocation.pressureSteps} />
                      </li>
                    )}
                  </For>
                </ol>
              </Show>
            </Show>
          </Show>
        </div>
      </Show>
    </SectionCard>
  )
}

/**
 * The workers index's one-cell summary: memory used and reserved, the ratio,
 * and a red dot while the response is acting. Blank when the node has not
 * reported, so an unmeasured node never reads as empty.
 */
export function CompactNodeUsage(props: { usage: GetNodeUsageReply | null | undefined }) {
  const memory = () => props.usage?.memory
  return (
    <Show when={memory()} fallback={<span class="text-faint">-</span>}>
      {(resource) => {
        const reported = () => props.usage?.usageReported ?? false
        const reservedPercent = () => percentOf(resource().reserved, resource().capacity)
        const usedPercent = () => percentOf(resource().used, physicalCapacity(resource()))
        const pressure = () => pressureSummary(resource(), reported())
        const title = () =>
          [
            `Reserved ${formatAmount('memory', resource().reserved)} of ${formatAmount('memory', resource().capacity)} schedulable (${reservedPercent()}%)`,
            reported() ? `Used ${formatAmount('memory', resource().used)} of ${formatAmount('memory', physicalCapacity(resource()))} physical (${usedPercent()}%)` : 'Usage not reported',
            `Pressure: ${pressure().label}`,
          ].join('\n')
        return (
          <div class="flex min-w-[8rem] items-center gap-2" title={title()}>
            <div class="w-16 space-y-0.5">
              <Meter size="xs" percent={reservedPercent()} tone="neutral" label="memory reserved" />
              <Meter size="xs" percent={reported() ? usedPercent() : 0} tone={fillTone(usedPercent())} label="memory used" />
            </div>
            <span class="whitespace-nowrap font-mono text-2xs tabular-nums text-muted">
              {reported() ? `${usedPercent()}%` : '-'} / {reservedPercent()}%
              <Show when={ratioLabel(resource().oversubscribeRatio)}>{(label) => <span class="ml-1 text-brass-strong">{label()}</span>}</Show>
            </span>
            <Show when={pressure().label === 'under pressure'}>
              <Dot tone="danger" pulse />
            </Show>
          </div>
        )
      }}
    </Show>
  )
}
