import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal } from 'solid-js'
import type { ListSessionNodesReply, SessionNode } from '../gen/legion_pb'
import { INSTANCE_PAGE, SESSION_NODE_REASONS, formatCount, formatRoom, jobInstancesHref } from '../utils/sessions'
import { Badge, Button, Note, RelativeTime, SectionCard, TableShell, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * The job page's Edge nodes card for one session task (docs/session-mode.md):
 * what `Manager.ListSessionNodes` tells an edge picker. Every node holding an
 * envelope for the task, its tag (the 8 characters a session id minted there
 * carries before its final `.`), whether it takes new edge starts and why
 * not, its weight (how many more sessions of this size fit), its sessions,
 * and its envelope's room left. The owner fetches the reply; this card only
 * renders it, a page of rows at a time, since a session task places one
 * envelope per eligible node.
 */

export default function SessionNodesCard(props: {
  jobId: string
  taskName: string
  /** Sessions one envelope admits, from the spec, when it says. */
  perEnvelope?: number
  /** The ListSessionNodes reply, the error it failed with, or undefined while loading. */
  reply?: ListSessionNodesReply | Error
}) {
  const [limit, setLimit] = createSignal(INSTANCE_PAGE)
  const nodes = createMemo<readonly SessionNode[]>(() => (props.reply && !(props.reply instanceof Error) ? props.reply.nodes : []))
  const accepting = createMemo(() => nodes().reduce((total, node) => total + (node.accepting ? 1 : 0), 0))
  const sessions = createMemo(() => nodes().reduce((total, node) => total + node.sessions, 0))
  const shown = createMemo(() => nodes().slice(0, limit()))

  return (
    <SectionCard
      title={`Edge nodes · ${props.taskName}`}
      description="Where an edge picker may start this task's sessions (ListSessionNodes): every node holding an envelope for it, whether it takes new starts, and its weight. Advisory: each node admits or refuses a start on its own."
      actions={
        <Show when={props.reply && !(props.reply instanceof Error)}>
          <Badge tone="neutral">
            {formatCount(nodes().length)} {nodes().length === 1 ? 'node' : 'nodes'}
          </Badge>
          <Badge tone={accepting() > 0 ? 'ok' : 'warn'}>{formatCount(accepting())} accepting</Badge>
          <Badge tone="info">{formatCount(sessions())} sessions</Badge>
        </Show>
      }
      flush
    >
      <Show when={props.reply instanceof Error}>
        <div class="px-4 py-3">
          <Note tone="warn">Could not list the nodes: {(props.reply as Error).message}</Note>
        </div>
      </Show>
      <Show when={props.reply === undefined}>
        <p class="px-4 py-3 text-sm text-muted">Loading nodes…</p>
      </Show>
      <Show when={props.reply && !(props.reply instanceof Error)}>
        <Show when={nodes().length > 0} fallback={<p class="px-4 py-3 text-sm text-muted">No node holds an envelope for this task yet.</p>}>
          <TableShell>
            <table class={tableClass}>
              <thead class={theadClass}>
                <tr>
                  <th class={thClass}>Node</th>
                  <th class={thClass} title="The 8 characters a session id minted on this node carries before its final dot">
                    Tag
                  </th>
                  <th class={thClass}>Edge starts</th>
                  <th class={thClass} title="How many more sessions of this task's size fit in the envelope's room">
                    Weight
                  </th>
                  <th class={thClass}>Sessions</th>
                  <th class={thClass}>Room left</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-line">
                <For each={shown()}>
                  {(node) => (
                    <tr class={trHoverClass}>
                      <td class={tdClass}>
                        <A href={`/nodes/${encodeURIComponent(node.nodeId)}`} class="font-mono text-xs text-muted hover:text-brass-strong" title={node.address || 'No worker-plane address advertised'}>
                          {node.nodeId}
                        </A>
                      </td>
                      <td class={tdClass}>
                        <span class="font-mono text-xs">{node.tag || '—'}</span>
                      </td>
                      <td class={tdClass}>
                        <Show when={node.accepting} fallback={<Badge tone="warn" title={SESSION_NODE_REASONS[node.reason] ?? node.reason}>{node.reason || 'not accepting'}</Badge>}>
                          <Badge tone="ok">accepting</Badge>
                        </Show>
                      </td>
                      <td class={`${tdClass} tabular-nums`}>{formatCount(node.weight)}</td>
                      <td class={`${tdClass} tabular-nums`}>
                        <A href={jobInstancesHref(props.jobId, { mode: 'session', task: props.taskName, node: node.nodeId })} class="hover:text-brass-strong" title="Open this node's sessions on the Instances card">
                          {formatCount(node.sessions)}
                        </A>
                        <Show when={props.perEnvelope !== undefined}>
                          <span class="text-faint"> of {formatCount(props.perEnvelope!)}</span>
                        </Show>
                      </td>
                      <td class={tdClass}>
                        <span class="font-mono text-2xs text-muted">{formatRoom(node.room) || '—'}</span>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </TableShell>
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-t border-line px-4 py-2.5 text-xs text-muted">
            <Show when={nodes().length > shown().length}>
              <span>
                Showing {formatCount(shown().length)} of {formatCount(nodes().length)} nodes.
              </span>
              <Button size="xs" onClick={() => setLimit((value) => value + INSTANCE_PAGE)}>
                Show {formatCount(Math.min(INSTANCE_PAGE, nodes().length - shown().length))} more
              </Button>
            </Show>
            <Show when={(props.reply as ListSessionNodesReply).asOf}>
              <span class="text-faint">
                read <RelativeTime value={(props.reply as ListSessionNodesReply).asOf} />
              </span>
            </Show>
            <span class="text-faint">
              or <code class="rounded bg-surface2 px-1 font-mono text-2xs">legion session nodes {props.jobId} --task {props.taskName}</code>
            </span>
          </div>
        </Show>
      </Show>
    </SectionCard>
  )
}
