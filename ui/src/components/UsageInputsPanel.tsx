import { For, Show, createResource, type Accessor } from 'solid-js'
import type { ScalingInputs } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { fillTone, formatMiB } from '../utils/usage'
import type { Tone } from '../utils/status'
import { Badge, Icon, Meter, SectionCard, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * Request vs usage for every task of a job, cluster-wide — the same
 * GetScalingInputs reply `legion job info` renders (docs/autoscaling.md,
 * "Scaling inputs"). The status-lane leader merges the usage every manager
 * hears, so this card reads the same whichever manager the UI talks to.
 * Polled with the page; hidden when the token lacks scale:read or the
 * manager predates the RPC.
 */

const percent = (ratio: number): number => (Number.isFinite(ratio) ? Math.round(ratio * 100) : 0)

const freshnessTone = (state: string): Tone => {
  if (state === 'current') return 'ok'
  if (state === 'resyncing') return 'warn'
  return 'neutral'
}

const freshnessTitle: Record<string, string> = {
  current: 'Every manager holding this task\'s usage confirmed it recently.',
  missing: 'Nothing reports usage for this task: no replicas, a runtime that reports none (noop), enforcement off, or its manager cannot reach the leader.',
  resyncing: 'The manager answering took over the status lane in the last 10 seconds; numbers may be partial.',
}

function UsageRow(props: { inputs: ScalingInputs }) {
  const cpu = () => props.inputs.cpu
  const memory = () => props.inputs.memory
  const requests = () => props.inputs.requests
  const freshness = () => props.inputs.freshness
  const measured = () => (cpu()?.measuredReplicas ?? 0) > 0
  const cpuMeasurable = () => measured() && (cpu()?.unmeasurableReplicas ?? 0) < (cpu()?.measuredReplicas ?? 0)
  const cpuPercent = () => percent(cpu()?.utilization ?? 0)
  const memoryPercent = () => percent(memory()?.utilization ?? 0)

  return (
    <tr class={trHoverClass}>
      <td class={tdClass}>
        <span class="font-medium text-ink">{props.inputs.taskName}</span>
        <span class="ml-1.5 text-2xs text-faint">group {props.inputs.taskGroup}</span>
      </td>
      <Show
        when={!props.inputs.error}
        fallback={
          <td class={cx(tdClass, 'text-muted')} colspan={5}>
            {props.inputs.error}
          </td>
        }
      >
        <td class={cx(tdClass, 'tabular-nums')}>
          <span class="text-ink">{props.inputs.readyReplicas}</span>
          <span class="text-faint">/{props.inputs.placedReplicas} ready</span>
          <span class="ml-1 text-2xs text-faint">desired {props.inputs.desiredReplicas}</span>
          <Show when={props.inputs.unplacedReplicas > 0}>
            <Badge tone="warn" class="ml-1.5" title="Replicas the scheduler could not place, usually for lack of room">
              {props.inputs.unplacedReplicas} unplaced
            </Badge>
          </Show>
          <Show when={props.inputs.rolloutActive}>
            <Badge tone="info" class="ml-1.5">rollout</Badge>
          </Show>
        </td>
        <td class={cx(tdClass, 'min-w-[11rem]')}>
          <Show when={measured()} fallback={<span class="text-faint">—</span>}>
            <div class="mb-0.5 flex justify-between gap-2 font-mono text-2xs tabular-nums text-muted">
              <span>
                <Show
                  when={(cpu()?.usedMhz ?? 0) > 0 || (cpu()?.reservedMhz ?? 0) > 0}
                  fallback={<>{String(cpu()?.usedMillicores ?? 0n)}m</>}
                >
                  {formatMiB(cpu()?.usedMhz ?? 0)} / {formatMiB(cpu()?.reservedMhz ?? 0)} MHz
                </Show>
              </span>
              <Show when={cpuMeasurable()}>
                <span title={`Busiest replica: ${percent(cpu()?.maxUtilization ?? 0)}% of its reservation`}>
                  {cpuPercent()}%
                </span>
              </Show>
            </div>
            <Show when={cpuMeasurable()}>
              <Meter size="xs" percent={cpuPercent()} tone={fillTone(cpuPercent())} label="cpu used of reserved" />
            </Show>
          </Show>
        </td>
        <td class={cx(tdClass, 'min-w-[10rem]')}>
          <Show when={measured()} fallback={<span class="text-faint">—</span>}>
            <div class="mb-0.5 flex justify-between gap-2 font-mono text-2xs tabular-nums text-muted">
              <span>
                {formatMiB(memory()?.usedMib ?? 0)} / {formatMiB(memory()?.reservedMib ?? 0)} MiB
              </span>
              <Show when={(memory()?.reservedMib ?? 0) > 0}>
                <span>{memoryPercent()}%</span>
              </Show>
            </div>
            <Show when={(memory()?.reservedMib ?? 0) > 0}>
              <Meter size="xs" percent={memoryPercent()} tone={fillTone(memoryPercent())} label="memory used of reserved" />
            </Show>
          </Show>
        </td>
        <td class={cx(tdClass, 'font-mono text-2xs tabular-nums text-muted')}>
          <Show when={requests()} fallback={<span class="text-faint">—</span>}>
            {(current) => (
              <span title={`Service ${current().service}; internal callers only`}>
                {current().requestsPerSecond.toFixed(1)} rps · {(current().errorRatio * 100).toFixed(1)}% err
                <Show when={current().latencyMsMean > 0}> · {Math.round(current().latencyMsMean)}ms</Show>
              </span>
            )}
          </Show>
        </td>
        <td class={tdClass}>
          <Show when={freshness()}>
            {(current) => (
              <Badge tone={freshnessTone(current().state)} dot title={freshnessTitle[current().state]}>
                {current().state}
                <Show when={current().state === 'current'}> · {current().ageSeconds}s</Show>
              </Badge>
            )}
          </Show>
        </td>
      </Show>
    </tr>
  )
}

export default function UsageInputsPanel(props: { jobName: string; tick: Accessor<number> }) {
  const [inputs] = createResource(
    () => [props.jobName, props.tick()] as const,
    async ([job]) => {
      try {
        return (await managerClient.getScalingInputs({ targets: [{ job }] })).inputs
      } catch {
        // Denied (`scale:read` missing) or an older manager: no card.
        return null
      }
    },
  )

  return (
    <Show when={(inputs() ?? []).length > 0}>
      <SectionCard
        title="Request vs usage"
        description="Measured CPU and memory against each task's reservation, cluster-wide, and requests into the service it backs."
        flush
      >
        <TableShell>
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <th class={thClass}>Task</th>
                <th class={thClass}>Replicas</th>
                <th class={thClass}>CPU used / reserved</th>
                <th class={thClass}>Memory used / reserved</th>
                <th class={thClass}>Requests</th>
                <th class={thClass}>Data</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              <For each={inputs() ?? []}>{(row) => <UsageRow inputs={row} />}</For>
            </tbody>
          </table>
        </TableShell>
        <p class="flex items-center gap-1.5 border-t border-line px-4 py-2 text-2xs text-faint">
          <Icon name="info" size={12} />
          Rolled up every 10 seconds on every manager and merged by the status-lane leader. Requests count internal callers only. Same as <code class="font-mono">legion job info</code>.
        </p>
      </SectionCard>
    </Show>
  )
}
