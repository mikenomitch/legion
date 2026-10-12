import { A } from '@solidjs/router'
import { Show, createEffect, createSignal, on } from 'solid-js'
import type { SessionNode } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import type { ResourceEntry } from '../utils/allocationDetail'
import { ENVELOPE_COPY, SESSION_NODE_REASONS, countMap, formatCount, formatRoom, jobInstancesHref } from '../utils/sessions'
import { Badge, Button, Note, RelativeTime, SectionCard, linkClass } from './ui'

/*
 * What an envelope allocation is (docs/session-mode.md): a placeholder that
 * reserves one node's room for a session task's sessions. It runs nothing,
 * so health, ports, logs, and exec do not apply; what matters is how much
 * room it holds and how many sessions sit in it.
 *
 * A node holds one envelope per session task, so the envelope's sessions are
 * its node's sessions of that task: ListSessionNodes says how many, whether
 * the node takes edge starts, and the room left, without listing a single
 * session. A manager that refuses it falls back to ListInstances with
 * `session_limit: 0`, whose per-node totals cover every session task of the
 * job. Either costs the same at any session count, so the card reads it on
 * open. The sessions themselves are on the job page's Instances card,
 * filtered to this node and task.
 */

/** What the card shows about the envelope's node. */
export type EnvelopeCount = {
  /** Sessions in the envelope. */
  sessions: number
  /** The node's ListSessionNodes entry; absent on the ListInstances fallback. */
  node?: SessionNode
  complete: boolean
  asOf: string
}

export default function EnvelopeCard(props: {
  jobId?: string
  allocationId: bigint
  nodeId?: string
  /** The session task this envelope serves. */
  servedTask: string
  /** Sessions one envelope admits, from the spec, when it says. */
  perEnvelope?: number
  /** What the allocation reserves (its `resources`). */
  reserved: readonly ResourceEntry[]
  /** A count to show without asking the manager (the /testing fixtures). */
  initial?: EnvelopeCount
}) {
  const [count, setCount] = createSignal<EnvelopeCount | undefined>(props.initial)
  const [loading, setLoading] = createSignal(false)
  const [error, setError] = createSignal<string>()

  const load = async () => {
    const jobId = props.jobId
    const nodeId = props.nodeId
    if (!jobId || !nodeId) return
    setLoading(true)
    setError(undefined)
    try {
      try {
        const reply = await managerClient.listSessionNodes({ jobId, taskName: props.servedTask })
        const node = reply.nodes.find((entry) => entry.nodeId === nodeId)
        setCount({ sessions: node?.sessions ?? 0, node, complete: true, asOf: reply.asOf })
      } catch {
        const reply = await managerClient.listInstances({ jobId, sessionLimit: 0 })
        setCount({ sessions: countMap(reply.sessionsByNode).get(nodeId) ?? 0, complete: reply.complete, asOf: reply.asOf })
      }
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure))
    } finally {
      setLoading(false)
    }
  }
  // Read on open, and again if the allocation's job or node arrives late.
  createEffect(
    on([() => props.jobId, () => props.nodeId, () => props.servedTask], () => {
      if (!props.initial) void load()
    }),
  )

  return (
    <SectionCard
      title="Session envelope"
      description={ENVELOPE_COPY}
      actions={<Badge tone="info">envelope for {props.servedTask}</Badge>}
    >
      <div class="space-y-3 text-sm">
        <p class="text-muted">
          This allocation holds room on{' '}
          <Show when={props.nodeId} fallback="its node">
            <A href={`/nodes/${encodeURIComponent(props.nodeId!)}`} class={linkClass}>
              {props.nodeId}
            </A>
          </Show>{' '}
          for task{' '}
          <Show when={props.jobId} fallback={<span class="font-mono text-xs">{props.servedTask}</span>}>
            <A href={`/jobs/${encodeURIComponent(props.jobId!)}/tasks/${encodeURIComponent(props.servedTask)}`} class={linkClass}>
              {props.servedTask}
            </A>
          </Show>
          's sessions. The node admits each session against this room, without the scheduler, and the scheduler counts the room as used
          whether sessions fill it or not. Its status says whether the room is held, not whether anything is running.
        </p>
        <div class="flex flex-wrap gap-x-6 gap-y-2">
          <Show when={props.reserved.length > 0}>
            <div>
              <p class="eyebrow">Reserves</p>
              <p class="mt-0.5 font-mono text-xs text-ink">{props.reserved.map((entry) => `${entry.label} ${entry.value}`).join(' · ')}</p>
            </div>
          </Show>
          <Show when={props.perEnvelope !== undefined}>
            <div title="The fewest whole sessions any resource the session declares allows">
              <p class="eyebrow">Room for</p>
              <p class="mt-0.5 text-ink">{formatCount(props.perEnvelope!)} sessions</p>
            </div>
          </Show>
          <Show when={count()}>
            {(value) => (
              <>
                <div>
                  <p class="eyebrow">Running in it</p>
                  <p class="mt-0.5 text-ink">
                    {formatCount(value().sessions)}
                    <Show when={props.perEnvelope}>
                      <span class="text-faint"> of {formatCount(props.perEnvelope!)}</span>
                    </Show>
                    <Show when={value().asOf}>
                      <span class="ml-2 text-xs text-faint">
                        read <RelativeTime value={value().asOf} />
                      </span>
                    </Show>
                  </p>
                </div>
                <Show when={value().node}>
                  {(node) => (
                    <>
                      <div>
                        <p class="eyebrow">Edge starts</p>
                        <p class="mt-0.5">
                          <Show when={node().accepting} fallback={<Badge tone="warn" title={SESSION_NODE_REASONS[node().reason] ?? node().reason}>{node().reason || 'not accepting'}</Badge>}>
                            <Badge tone="ok" title={`Room for ${formatCount(node().weight)} more`}>accepting</Badge>
                          </Show>
                        </p>
                      </div>
                      <Show when={node().room}>
                        <div title="The envelope's room left, from the node's last report plus the managers' own recent starts">
                          <p class="eyebrow">Room left</p>
                          <p class="mt-0.5 font-mono text-xs text-ink">{formatRoom(node().room)}</p>
                        </div>
                      </Show>
                      <Show when={node().tag}>
                        <div title="Session ids minted on this node carry this tag before their final dot">
                          <p class="eyebrow">Node tag</p>
                          <p class="mt-0.5 font-mono text-xs text-ink">{node().tag}</p>
                        </div>
                      </Show>
                    </>
                  )}
                </Show>
              </>
            )}
          </Show>
        </div>

        <Show when={count()?.complete === false}>
          <Note tone="warn">Some nodes have not reported their sessions to any manager yet, so this count may be low.</Note>
        </Show>
        <Show when={count() && !count()!.node}>
          <Note tone="info">Counted from the job's per-node session totals, which include every session task the job has on this node.</Note>
        </Show>
        <Show when={error()}>{(message) => <Note tone="danger">Could not count sessions: {message()}</Note>}</Show>

        <Show when={props.jobId}>
          <div class="flex flex-wrap items-center gap-2">
            <Button size="xs" icon="refresh" disabled={loading() || !props.nodeId} onClick={() => void load()}>
              {loading() ? 'Counting…' : 'Count again'}
            </Button>
            <A href={jobInstancesHref(props.jobId!, { mode: 'session', task: props.servedTask, node: props.nodeId })} class={linkClass + ' text-xs'}>
              Open its sessions on the job page
            </A>
          </div>
        </Show>
      </div>
    </SectionCard>
  )
}
