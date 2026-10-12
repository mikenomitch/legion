import { A } from '@solidjs/router'
import { For, Show, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import type { ResourceEntry } from '../utils/allocationDetail'
import { instanceHref } from '../utils/instances'
import { ENVELOPE_COPY, formatCount, isSession, jobInstancesHref, takeMatching } from '../utils/sessions'
import { Badge, Button, Note, RelativeTime, SectionCard, linkClass } from './ui'

/*
 * What an envelope allocation is (docs/session-mode.md): a placeholder that
 * reserves one node's room for a session task's sessions. It runs nothing,
 * so health, ports, logs, and exec do not apply; what matters is how much
 * room it holds and which sessions sit in it.
 *
 * Its sessions come from ListInstances, which returns every session of the
 * job in one reply with no node or envelope filter. So they are counted
 * only when asked, once, and only the first few are listed here; the job
 * page's Instances card, filtered to this envelope, pages through the rest.
 */

// Session ids listed on the card after a count.
const SHOWN = 10

type Count = { matching: number; ids: string[]; total: number; complete: boolean; asOf: string }

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
}) {
  const [count, setCount] = createSignal<Count>()
  const [loading, setLoading] = createSignal(false)
  const [error, setError] = createSignal<string>()

  const load = async () => {
    if (!props.jobId) return
    setLoading(true)
    setError(undefined)
    try {
      const reply = await managerClient.listInstances({ jobId: props.jobId })
      const id = props.allocationId
      const { shown, matching } = takeMatching(reply.instances, (instance) => isSession(instance) && instance.envelopeAllocId === id, SHOWN)
      setCount({ matching, ids: shown.map((instance) => instance.instanceId), total: reply.instances.length, complete: reply.complete, asOf: reply.asOf })
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure))
    } finally {
      setLoading(false)
    }
  }

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
              <div>
                <p class="eyebrow">Running in it</p>
                <p class="mt-0.5 text-ink">
                  {formatCount(value().matching)}
                  <Show when={props.perEnvelope}>
                    <span class="text-faint"> of {formatCount(props.perEnvelope!)}</span>
                  </Show>
                  <span class="ml-2 text-xs text-faint">
                    read <RelativeTime value={value().asOf || undefined} />
                  </span>
                </p>
              </div>
            )}
          </Show>
        </div>

        <Show when={count()?.complete === false}>
          <Note tone="warn">Some nodes have not reported their sessions to any manager yet, so this count may be low.</Note>
        </Show>
        <Show when={error()}>{(message) => <Note tone="danger">Could not list sessions: {message()}</Note>}</Show>

        <Show when={count() && count()!.ids.length > 0}>
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
            <For each={count()!.ids}>
              {(id) => (
                <A href={instanceHref(props.jobId!, id)} class="font-mono text-muted hover:text-brass-strong">
                  {id}
                </A>
              )}
            </For>
            <Show when={count()!.matching > count()!.ids.length}>
              <span class="text-faint">and {formatCount(count()!.matching - count()!.ids.length)} more</span>
            </Show>
          </div>
        </Show>

        <Show when={props.jobId}>
          <div class="flex flex-wrap items-center gap-2">
            <Button size="xs" icon="refresh" disabled={loading()} onClick={() => void load()}>
              {loading() ? 'Counting…' : count() ? 'Count again' : 'Count its sessions'}
            </Button>
            <A href={jobInstancesHref(props.jobId!, { mode: 'session', envelope: String(props.allocationId) })} class={linkClass + ' text-xs'}>
              Open its sessions on the job page
            </A>
            <span class="text-2xs text-faint">
              Counting reads the job's whole session listing once{count() ? ` (${formatCount(count()!.total)} instances last time)` : ''}.
            </span>
          </div>
        </Show>
      </div>
    </SectionCard>
  )
}
