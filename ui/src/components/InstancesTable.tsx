import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, onCleanup, onMount, type Accessor, type JSX } from 'solid-js'
import type { InstanceInfo, NodeAllocationUsage } from '../gen/legion_pb'
import { formatBytes, formatDate } from '../utils/format'
import {
  formatMs,
  idleSecondsLeft,
  idleSecondsSoFar,
  isPlatformStop,
  pendingOpLabel,
  stopCauseLabel,
  stopCauseTone,
  suspendReasonLabel,
} from '../utils/instances'
import type { JobTask } from '../utils/jobspec'
import { bytesToMiB, formatCores, formatIdle, formatMiB, heldLabel, idleSecondsFromUsage, reservationLabel } from '../utils/usage'
import { now } from '../utils/polling'
import { Badge, MonoBadge, RelativeTime, StatusBadge, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * The job page's Instances table: stable identities for stateful tasks, with
 * what each one's current allocation reserves and uses, its checkpoint when
 * suspended, its idle countdown, why it last stopped, and its fork lineage.
 *
 * Columns are a list, so the job page adds the actions column without
 * touching the rest. Each cell takes the row as an accessor and reads it
 * inside its own JSX, so a poll that changes one number updates that cell
 * only. Rows are keyed by instance id, not object identity: an armed
 * Suspend or Resume confirm survives every poll as long as the instance
 * exists (internal_docs/calls/suspend-resume-ui-plan_calls.md).
 */

export type InstanceRow = {
  instance: InstanceInfo
  // The task's spec, for reservation and ceiling when GetNodeUsage is not
  // available (no node:read, or an older manager).
  task?: JobTask
  // The node's view of the current allocation, when the caller can read it.
  allocation?: NodeAllocationUsage
}

export type InstanceColumn = {
  key: string
  header: JSX.Element
  title?: string
  class?: string
  cell: (row: Accessor<InstanceRow>) => JSX.Element
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

/**
 * The state badge, plus what the manager is doing, why it last stopped, and
 * whether its last resume booted cold (InstanceInfo.cold_boot): a restore
 * that failed under onRestoreFailure = "cold" keeps the volume but loses the
 * running processes, which a platform must not mistake for a clean resume.
 */
export function InstanceStateCell(props: { instance: InstanceInfo }) {
  const stop = () => (isPlatformStop(props.instance.lastStop) ? props.instance.lastStop : undefined)
  const title = () =>
    [props.instance.detail, props.instance.state === 'suspended' ? suspendReasonLabel(props.instance.suspendReason) : undefined].filter(Boolean).join('\n') ||
    undefined
  return (
    <div class="space-y-1">
      <span class="inline-flex flex-wrap items-center gap-1.5">
        <StatusBadge status={props.instance.state} tone={props.instance.state === 'suspended' ? 'accent' : undefined} title={title()} />
        <Show when={props.instance.state === 'suspended' && props.instance.suspendReason}>
          <span class="text-2xs text-muted" title={suspendReasonLabel(props.instance.suspendReason)}>
            {props.instance.suspendReason}
          </span>
        </Show>
        <Show when={props.instance.pendingOp}>
          {(op) => (
            <Badge tone="info" title={pendingOpLabel(op())}>
              {op()}
            </Badge>
          )}
        </Show>
        <Show when={props.instance.restoreSnapshotId && props.instance.pendingOp !== 'forking'}>
          <Badge tone="info" title="Restoring from this snapshot until its node reports the outcome">
            restoring from <span class="font-mono">{props.instance.restoreSnapshotId}</span>
          </Badge>
        </Show>
      </span>
      {/* Why the platform last stopped it (InstanceInfo.last_stop), so the
          row explains a restart without events or operator:read. */}
      <Show when={stop()}>
        {(lastStop) => (
          <div class="max-w-xs text-2xs" title={[lastStop().detail, lastStop().hint && `Hint: ${lastStop().hint}`].filter(Boolean).join('\n')}>
            <Badge tone={stopCauseTone(lastStop().cause)} class="mr-1">
              {stopCauseLabel(lastStop().cause)}
            </Badge>
            <RelativeTime value={lastStop().at} class="text-faint" />
            <Show when={lastStop().hint}>
              <p class="mt-0.5 text-amber-700 dark:text-amber-300">{lastStop().hint}</p>
            </Show>
          </div>
        )}
      </Show>
      <Show when={props.instance.coldBoot}>
        {(cold) => (
          <div
            class="max-w-xs text-2xs"
            title={`Restore from ${cold().snapshotId} failed, so the instance booted fresh on its volume (onRestoreFailure = "cold"):\n${cold().reason}`}
          >
            <Badge tone="warn" class="mr-1">
              resumed cold
            </Badge>
            <RelativeTime value={cold().at} class="text-faint" />
            <p class="mt-0.5 text-amber-700 dark:text-amber-300">Running processes were lost: {cold().reason}</p>
          </div>
        )}
      </Show>
    </div>
  )
}

/** Kind, where it lives, size, and age of a suspended instance's checkpoint. */
export function CheckpointCell(props: { instance: InstanceInfo }) {
  const instance = () => props.instance
  const durability = () =>
    instance().checkpointState === 'published' ? 'published' : instance().checkpointDurability === 'store' ? 'local, publishing' : 'local'
  const timings = () =>
    [
      instance().lastSuspendMs !== undefined ? `suspend ${formatMs(instance().lastSuspendMs)}` : undefined,
      instance().lastRestoreMs !== undefined ? `restore ${formatMs(instance().lastRestoreMs)}` : undefined,
    ]
      .filter(Boolean)
      .join(' · ')
  return (
    <Show
      when={instance().checkpointId}
      fallback={
        <Show when={timings() || instance().restoredFromSnapshotId} fallback={<span class="text-faint">-</span>}>
          <div class="text-2xs text-faint" title={instance().restoredFromSnapshotId ? `Last restored from ${instance().restoredFromSnapshotId}` : undefined}>
            {timings() || 'restored'}
          </div>
        </Show>
      }
    >
      {(checkpoint) => (
        <div class="space-y-0.5 text-xs">
          <div class="flex flex-wrap items-center gap-1.5">
            <MonoBadge title={checkpoint()}>{checkpoint().length > 13 ? `${checkpoint().slice(0, 13)}…` : checkpoint()}</MonoBadge>
            <Show when={instance().checkpointKind}>{(kind) => <Badge tone={kind() === 'memory' ? 'accent' : 'neutral'}>{kind()}</Badge>}</Show>
            <Badge
              tone={instance().checkpointState === 'published' ? 'ok' : 'neutral'}
              title={
                instance().checkpointState === 'published'
                  ? 'In the content store: resumes on any compatible node and survives this node'
                  : `On ${instance().checkpointNodeId ?? 'its node'} only: a resume is pinned there`
              }
            >
              {durability()}
            </Badge>
          </div>
          <div class="text-2xs text-faint">
            <Show when={instance().checkpointKind === 'disk'} fallback={instance().checkpointBytes ? formatBytes(Number(instance().checkpointBytes)) : '-'}>
              <span title="A disk suspend keeps the volume in place, so it copies nothing">volume in place</span>
            </Show>
            <Show when={instance().checkpointNodeId}>
              {(node) => (
                <>
                  {' · '}
                  <A href={`/nodes/${encodeURIComponent(node())}`} class="hover:text-brass-strong">
                    {node()}
                  </A>
                </>
              )}
            </Show>
            <Show when={instance().suspendedAt}>
              {' · '}
              <RelativeTime value={instance().suspendedAt} />
            </Show>
          </div>
          <Show when={heldLabel(instance().heldMemoryMib, instance().heldCpuMhz)}>
            {(held) => (
              <div class="text-2xs text-faint" title={`Kept charged to ${instance().checkpointNodeId ?? 'its node'} so the resume finds room`}>
                {held()}
              </div>
            )}
          </Show>
          <Show when={timings()}>
            <div class="text-2xs text-faint">{timings()}</div>
          </Show>
        </div>
      )}
    </Show>
  )
}

/**
 * Idle time, and for a task with an idle policy, how long until the idle
 * sweep suspends it. Ticks every second while a countdown is showing.
 */
export function IdleCell(props: { row: InstanceRow }) {
  const [tickMs, setTickMs] = createSignal(Date.now())
  onMount(() => {
    const timer = window.setInterval(() => setTickMs(Date.now()), 1000)
    onCleanup(() => window.clearInterval(timer))
  })
  const left = () => idleSecondsLeft(props.row.instance, tickMs())
  const usage = () => props.row.instance.usage ?? props.row.allocation?.usage
  return (
    <Show when={left() !== undefined} fallback={<span>{usage() ? formatIdle(idleSecondsFromUsage(usage()!, now())) : '-'}</span>}>
      <div title={`Suspends after ${String(props.row.instance.idleAfterSeconds)}s idle (suspend.idle.after)`}>
        <span>{formatIdle(idleSecondsSoFar(props.row.instance, tickMs()) ?? 0)}</span>
        <div class={cx('text-2xs', left()! <= 30 ? 'text-amber-600 dark:text-amber-400' : 'text-faint')}>
          {left()! > 0 ? `parks in ${formatIdle(left()!)}` : 'parking now'}
        </div>
      </div>
    </Show>
  )
}

export const INSTANCE_COLUMNS: InstanceColumn[] = [
  {
    key: 'instance',
    header: 'Instance',
    class: 'font-mono text-xs',
    cell: (row) => (
      <div>
        <span>{row().instance.instanceId}</span>
        <Show when={row().instance.forkedFrom}>
          {(origin) => (
            <div class="text-2xs text-faint" title="Created by forking this instance (ForkInstance)">
              fork of{' '}
              <a href={`#instance-${encodeURIComponent(origin())}`} class="hover:text-brass-strong">
                {origin()}
              </a>
            </div>
          )}
        </Show>
      </div>
    ),
  },
  { key: 'mode', header: 'Mode', class: 'text-muted', cell: (row) => row().instance.mode },
  { key: 'state', header: 'State', cell: (row) => <InstanceStateCell instance={row().instance} /> },
  {
    key: 'slot',
    header: 'Slot',
    class: 'tabular-nums text-muted',
    cell: (row) => (row().instance.slotIndex !== undefined ? String(row().instance.slotIndex) : '-'),
  },
  {
    key: 'allocation',
    header: 'Allocation',
    class: 'font-mono text-xs text-muted',
    cell: (row) => (row().instance.currentAllocId !== undefined ? `#${String(row().instance.currentAllocId)}` : '-'),
  },
  {
    key: 'memory',
    header: 'Memory',
    title: 'Measured use of the current allocation, with its reservation and burst ceiling',
    cell: (row) => <MemoryCell row={row()} />,
  },
  {
    key: 'cpu',
    header: 'CPU',
    title: 'Average cores over the last report interval',
    class: 'font-mono text-xs tabular-nums text-muted',
    cell: (row) => formatCores((row().instance.usage ?? row().allocation?.usage)?.cpuMillicores),
  },
  {
    key: 'idle',
    header: 'Idle',
    title: 'Time since the allocation last used 1% of a core or moved traffic, and for tasks with suspend.idle, how long until it suspends',
    class: 'font-mono text-xs tabular-nums text-muted',
    cell: (row) => <IdleCell row={row()} />,
  },
  {
    key: 'checkpoint',
    header: 'Checkpoint',
    title: "A suspended instance's checkpoint: kind, where it lives, size, node, and when. Also the last suspend and restore durations.",
    cell: (row) => <CheckpointCell instance={row().instance} />,
  },
  {
    key: 'storage',
    header: 'Storage',
    cell: (row) => (
      <Show when={row().instance.storageHandle} fallback={<span class="text-faint">-</span>}>
        {(handle) => (
          // Link the volume id straight to the volume that holds this
          // instance's data.
          <A
            href={`/volumes/${encodeURIComponent(volumeIdFromHandle(handle()))}`}
            class="inline-block max-w-[12rem] truncate align-bottom font-mono text-xs text-muted hover:text-brass-strong"
            title={handle()}
          >
            {handle()}
          </A>
        )}
      </Show>
    ),
  },
  {
    key: 'created',
    header: 'Created',
    class: 'whitespace-nowrap text-right text-xs text-faint',
    cell: (row) => formatDate(row().instance.createdAt),
  },
]

export default function InstancesTable(props: { rows: readonly InstanceRow[]; columns?: readonly InstanceColumn[]; highlight?: string }) {
  const columns = () => props.columns ?? INSTANCE_COLUMNS
  const byId = createMemo(() => new Map(props.rows.map((row) => [row.instance.instanceId, row])))
  // Strings compare by value, so a poll never remounts a row that still
  // exists. Each row then reads its latest data through a memo that only
  // notifies when that row's object changes.
  const ids = createMemo(() => props.rows.map((row) => row.instance.instanceId))
  return (
    <TableShell>
      <table class={tableClass}>
        <thead class={theadClass}>
          <tr>
            <For each={columns()}>
              {(column) => (
                <th class={cx(thClass, (column.key === 'created' || column.key === 'actions') && 'text-right')} title={column.title}>
                  {column.header}
                </th>
              )}
            </For>
          </tr>
        </thead>
        <tbody class="divide-y divide-line">
          <For each={ids()}>
            {(id) => {
              // Keep the last row while the list drops this id, so cells
              // never read a missing row during teardown.
              const initial = byId().get(id)!
              const row = createMemo<InstanceRow>((previous) => byId().get(id) ?? previous, initial)
              return (
                <tr id={`instance-${encodeURIComponent(id)}`} class={cx(trHoverClass, props.highlight === id && 'bg-brass/5')}>
                  <For each={columns()}>{(column) => <td class={cx(tdClass, 'align-top', column.class)}>{column.cell(row)}</td>}</For>
                </tr>
              )
            }}
          </For>
        </tbody>
      </table>
    </TableShell>
  )
}
