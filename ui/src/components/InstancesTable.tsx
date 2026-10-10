import { A } from '@solidjs/router'
import { For, Show, type JSX } from 'solid-js'
import type { InstanceInfo, NodeAllocationUsage } from '../gen/legion_pb'
import { formatDate } from '../utils/format'
import type { JobTask } from '../utils/jobspec'
import { bytesToMiB, formatCores, formatIdle, formatMiB, idleSecondsFromUsage, reservationLabel } from '../utils/usage'
import { now } from '../utils/polling'
import { Badge, StatusBadge, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * The job page's Instances table: stable identities for stateful tasks, with
 * what each one's current allocation reserves and uses.
 *
 * Columns are a list, not inline markup, so a later pass adds the
 * checkpoint, idle countdown, and suspend/resume/fork actions as entries
 * here (internal_docs/plans/suspend-resume-ui-plan.md §6.4) without touching
 * the job page. Each cell reads only fields the manager sends today, and an
 * absent field renders as `-`.
 */

export type InstanceRow = {
  instance: InstanceInfo
  // The task's spec, for reservation and ceiling when GetNodeUsage is not
  // available (no node:read, or an older manager).
  task?: JobTask
  // The node's view of the current allocation, when the caller can read it.
  allocation?: NodeAllocationUsage
}

type Column = {
  key: string
  header: JSX.Element
  title?: string
  class?: string
  cell: (row: InstanceRow) => JSX.Element
}

// A storage handle is `{plugin}/{volume_id}`; plugin names never contain a
// slash, so the split is unambiguous.
const volumeIdFromHandle = (handle: string) => handle.slice(handle.indexOf('/') + 1)

const reservedMiB = (row: InstanceRow): number | undefined => row.allocation?.reservedMemoryMib ?? row.task?.resources?.memory
const maxMiB = (row: InstanceRow): number | undefined =>
  row.allocation ? row.allocation.memoryMaxMib || undefined : row.task?.resources?.memoryMax

const usedMiB = (row: InstanceRow): number | undefined => {
  const usage = row.instance.usage ?? row.allocation?.usage
  return usage ? bytesToMiB(usage.memoryCurrentBytes) : undefined
}

function MemoryCell(props: { row: InstanceRow }) {
  const used = () => usedMiB(props.row)
  const reserved = () => reservedMiB(props.row)
  const bursting = () => used() !== undefined && reserved() !== undefined && used()! > reserved()!
  return (
    <div class="font-mono text-xs tabular-nums">
      <span
        class={cx(used() === undefined ? 'text-faint' : bursting() ? 'text-amber-600 dark:text-amber-400' : 'text-ink')}
        title={bursting() ? 'Using more than its reservation: the first to give memory back under node pressure' : undefined}
      >
        {used() === undefined ? '-' : `${formatMiB(used()!)} MiB`}
      </span>
      <Show when={props.row.instance.currentAllocId !== undefined && reserved()}>
        <div class="text-2xs text-faint" title="Reservation / burst ceiling (resources.memoryMax), MiB">
          res/max {reservationLabel(reserved(), maxMiB(props.row))}
        </div>
      </Show>
    </div>
  )
}

export const INSTANCE_COLUMNS: Column[] = [
  {
    key: 'instance',
    header: 'Instance',
    class: 'font-mono text-xs',
    cell: (row) => row.instance.instanceId,
  },
  { key: 'mode', header: 'Mode', class: 'text-muted', cell: (row) => row.instance.mode },
  {
    key: 'state',
    header: 'State',
    cell: (row) => (
      <span class="inline-flex flex-wrap items-center gap-1.5">
        <StatusBadge status={row.instance.state} title={row.instance.detail} />
        <Show when={row.instance.restoreSnapshotId}>
          {(snapshot) => (
            <Badge tone="info" title="Rolling back to this snapshot (RestoreInstance) until its node reports the outcome">
              restoring from <span class="font-mono">{snapshot()}</span>
            </Badge>
          )}
        </Show>
      </span>
    ),
  },
  {
    key: 'slot',
    header: 'Slot',
    class: 'tabular-nums text-muted',
    cell: (row) => (row.instance.slotIndex !== undefined ? String(row.instance.slotIndex) : '-'),
  },
  {
    key: 'allocation',
    header: 'Allocation',
    class: 'font-mono text-xs text-muted',
    cell: (row) => (row.instance.currentAllocId !== undefined ? `#${String(row.instance.currentAllocId)}` : '-'),
  },
  {
    key: 'memory',
    header: 'Memory',
    title: 'Measured use of the current allocation, with its reservation and burst ceiling',
    cell: (row) => <MemoryCell row={row} />,
  },
  {
    key: 'cpu',
    header: 'CPU',
    title: 'Average cores over the last report interval',
    class: 'font-mono text-xs tabular-nums text-muted',
    cell: (row) => formatCores((row.instance.usage ?? row.allocation?.usage)?.cpuMillicores),
  },
  {
    key: 'idle',
    header: 'Idle',
    title: 'Time since the allocation last used 1% of a core or moved traffic',
    class: 'font-mono text-xs tabular-nums text-muted',
    cell: (row) => {
      const usage = row.instance.usage ?? row.allocation?.usage
      if (!usage) return '-'
      // Ages on the shared clock between polls.
      return formatIdle(idleSecondsFromUsage(usage, now()))
    },
  },
  {
    key: 'storage',
    header: 'Storage',
    cell: (row) => (
      <Show when={row.instance.storageHandle} fallback={<span class="text-faint">-</span>}>
        {/* Link the volume id straight to the volume that holds this
            instance's data. */}
        <A
          href={`/volumes/${encodeURIComponent(volumeIdFromHandle(row.instance.storageHandle!))}`}
          class="inline-block max-w-[12rem] truncate align-bottom font-mono text-xs text-muted hover:text-brass-strong"
          title={row.instance.storageHandle}
        >
          {row.instance.storageHandle}
        </A>
      </Show>
    ),
  },
  {
    key: 'created',
    header: 'Created',
    class: 'whitespace-nowrap text-right text-xs text-faint',
    cell: (row) => formatDate(row.instance.createdAt),
  },
]

// Rows are keyed by object identity. Today no cell holds state, so a poll
// that rebuilds the rows costs only a re-render. Before a later pass adds
// two-step actions here, keep unchanged rows' objects stable (as
// SnapshotsPanel does), or a poll will disarm a half-confirmed button.
export default function InstancesTable(props: { rows: readonly InstanceRow[]; columns?: readonly Column[] }) {
  const columns = () => props.columns ?? INSTANCE_COLUMNS
  return (
    <TableShell>
      <table class={tableClass}>
        <thead class={theadClass}>
          <tr>
            <For each={columns()}>
              {(column) => (
                <th class={cx(thClass, column.key === 'created' && 'text-right')} title={column.title}>
                  {column.header}
                </th>
              )}
            </For>
          </tr>
        </thead>
        <tbody class="divide-y divide-line">
          <For each={props.rows}>
            {(row) => (
              <tr class={trHoverClass}>
                <For each={columns()}>{(column) => <td class={cx(tdClass, column.class)}>{column.cell(row)}</td>}</For>
              </tr>
            )}
          </For>
        </tbody>
      </table>
    </TableShell>
  )
}
