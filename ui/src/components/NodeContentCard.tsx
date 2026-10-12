import { A } from '@solidjs/router'
import { For, Show } from 'solid-js'
import type { GetNodeContentReply } from '../gen/legion_pb'
import { formatBytes } from '../utils/format'
import { percentOf, fillTone, type ContentBudget } from '../utils/usage'
import { snapshotLabel, snapshotStateTone } from '../utils/snapshots'
import { Badge, Meter, MonoBadge, SectionCard, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * The worker page's "Content and locality" card, from GetNodeContent: what
 * the node advertises to the manager's location index, which snapshot each
 * entry belongs to, and the snapshots captured here or fetched here with how
 * many of their parts the node holds (suspend/resume UI plan §6.7).
 *
 * The index is soft state on the manager that receives the node's
 * heartbeats, so an empty card says why rather than reading as "empty".
 * Presentational, so /testing renders it from fixtures.
 */

/**
 * The content store's per-class byte budget, when the manager reports it.
 * GetNodeContent does not carry budgets (the index has digests, not
 * classes), so the caller maps the `snapshots` budget from GetNodeUsage
 * (`snapshotBudgets`), with a note on the room left for a new checkpoint
 * and whether the budget or the disk under it is what limits it. Nothing
 * shows for a node that has not reported one.
 */
export type { ContentBudget }

export function NodeContentCard(props: { content: GetNodeContentReply; budgets?: readonly ContentBudget[] }) {
  const capability = (on: boolean, label: string, title: string) => (
    <Badge tone={on ? 'ok' : 'neutral'} title={title}>
      {label}: {on ? 'yes' : 'no'}
    </Badge>
  )
  return (
    <SectionCard
      title="Content and locality"
      description={
        <Show
          when={props.content.indexReported}
          fallback="This manager holds no content report from this node: it has no content store, predates content reports, has not reported since the manager started, or sends its heartbeats to another manager."
        >
          What this node advertises to the location index (entries of 1 MiB and up). Placement prefers nodes that already hold a workload's bytes.
        </Show>
      }
      actions={
        <span class="flex flex-wrap gap-1.5">
          {capability(props.content.canCheckpoint, 'checkpoint', 'The worker can capture snapshots and suspend checkpoints')}
          {capability(props.content.canPublish, 'publish', 'The worker has a contentStore { url } to publish snapshots to, so store-durability suspends and spread forks can use it')}
        </span>
      }
      flush
    >
      <Show when={(props.budgets ?? []).length > 0}>
        <div class="grid gap-x-8 gap-y-3 border-b border-line p-4 md:grid-cols-2">
          <For each={props.budgets}>
            {(budget) => {
              const percent = () => percentOf(budget.usedBytes, budget.budgetBytes)
              return (
                <div>
                  <div class="mb-0.5 flex justify-between font-mono text-2xs tabular-nums text-muted">
                    <span>{budget.name}</span>
                    <span>
                      {formatBytes(budget.usedBytes)} of {formatBytes(budget.budgetBytes)} · {percent()}%
                    </span>
                  </div>
                  <Meter percent={percent()} tone={fillTone(percent())} label={`${budget.name} budget`} />
                  <Show when={budget.note}>
                    <p class={cx('mt-1 text-2xs', budget.tight ? 'text-amber-700 dark:text-amber-300' : 'text-faint')}>{budget.note}</p>
                  </Show>
                </div>
              )
            }}
          </For>
        </div>
      </Show>

      <Show when={props.content.indexReported}>
        <p class="border-b border-line px-4 py-2.5 text-xs text-muted">
          {props.content.entries.length} {props.content.entries.length === 1 ? 'entry' : 'entries'},{' '}
          {formatBytes(Number(props.content.advertisedBytes))} advertised <span class="text-faint">· epoch {String(props.content.epoch)}</span>
        </p>
      </Show>

      <Show when={props.content.snapshots.length > 0}>
        <TableShell>
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <th class={thClass}>Snapshot</th>
                <th class={thClass}>Job</th>
                <th class={thClass}>Kind</th>
                <th class={thClass}>State</th>
                <th class={thClass} title="Captured on this node, or fetched or warmed here from the content store">
                  Here because
                </th>
                <th class={thClass + ' text-right'} title="Memory parts this node advertises, of the snapshot's total">
                  Parts here
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              <For each={props.content.snapshots}>
                {(row) => (
                  <Show when={row.snapshot}>
                    {(snapshot) => (
                      <tr class={trHoverClass}>
                        <td class={tdClass}>
                          <span class={cx('text-xs', snapshot().name ? 'font-medium' : 'font-mono')} title={snapshot().snapshotId}>
                            {snapshotLabel(snapshot())}
                          </span>
                          <Show when={snapshot().instanceId}>
                            <span class="ml-1.5 font-mono text-2xs text-faint">{snapshot().instanceId}</span>
                          </Show>
                        </td>
                        <td class={tdClass}>
                          <A href={`/jobs/${encodeURIComponent(snapshot().jobId)}`} class="font-medium hover:text-brass-strong">
                            {snapshot().jobId}
                          </A>
                        </td>
                        <td class={tdClass}>
                          <Badge tone={snapshot().kind === 'memory' ? 'accent' : 'neutral'}>{snapshot().kind}</Badge>
                          <span class="ml-1.5 text-2xs text-muted">{snapshot().purpose}</span>
                        </td>
                        <td class={tdClass}>
                          <Badge tone={snapshotStateTone(snapshot().state)} dot class="uppercase tracking-wide" title={snapshot().error || undefined}>
                            {snapshot().state}
                          </Badge>
                        </td>
                        <td class={tdClass + ' text-xs text-muted'}>{row.capturedHere ? 'captured here' : 'fetched or warmed'}</td>
                        <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                          {row.partsTotal > 0 ? `${row.partsHere}/${row.partsTotal}` : <span title="A disk snapshot lives in its volume driver, not the content store">volume</span>}
                        </td>
                      </tr>
                    )}
                  </Show>
                )}
              </For>
            </tbody>
          </table>
        </TableShell>
      </Show>

      <Show when={props.content.entries.length > 0}>
        <TableShell class="border-t border-line">
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <th class={thClass}>Digest</th>
                <th class={thClass}>Belongs to</th>
                <th class={thClass + ' text-right'}>Size</th>
                <th class={thClass + ' text-right'} title="Nodes advertising this digest, this one included">
                  Holders
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              <For each={props.content.entries}>
                {(entry) => (
                  <tr class={trHoverClass}>
                    <td class={tdClass}>
                      <MonoBadge title={entry.digest}>{entry.digest.length > 23 ? `${entry.digest.slice(0, 23)}…` : entry.digest}</MonoBadge>
                    </td>
                    <td class={tdClass + ' text-xs text-muted'}>
                      <Show when={entry.snapshotId} fallback={<span class="text-faint">artifact or unknown</span>}>
                        <span class="font-mono">{entry.snapshotId}</span> <span class="text-faint">{entry.role}</span>
                      </Show>
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>{formatBytes(Number(entry.bytes))}</td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>{entry.holders}</td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </TableShell>
      </Show>

      <Show when={props.content.snapshots.length === 0 && props.content.entries.length === 0}>
        <p class="px-4 py-3 text-sm text-muted">No snapshots or advertised content on this node.</p>
      </Show>
    </SectionCard>
  )
}
