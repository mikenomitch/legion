import { Show, createSignal } from 'solid-js'
import type { NodeMsg } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { allows } from '../utils/acl'
import { Button, ConfirmButton } from './ui'

/*
 * Cordon / drain / uncordon from the worker page — the SetNodeAvailability
 * RPC behind `legion node cordon|drain|uncordon`. Drain evicts, so it is the
 * two-step confirm; cordon and uncordon are reversible and one click.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

export type AvailabilityOutcome = { message: string; failed: boolean }

export default function NodeAvailabilityControls(props: { node: NodeMsg; onOutcome: (outcome: AvailabilityOutcome) => void }) {
  const [busy, setBusy] = createSignal(false)
  const mayWrite = () => allows('node', 'write', { pool: props.node.pool || 'default' })
  const title = () => (mayWrite() ? undefined : 'Your token cannot change this node (needs node:write on its pool)')

  const apply = async (availability: 'available' | 'drained', evict: boolean) => {
    setBusy(true)
    try {
      const reply = await managerClient.setNodeAvailability({ nodeId: props.node.id, availability, evict })
      props.onOutcome({ message: reply.message || 'Availability updated.', failed: false })
    } catch (error) {
      props.onOutcome({ message: errorText(error), failed: true })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div class="flex flex-wrap items-center gap-2" title={title()}>
      <Show
        when={props.node.availability === 'drained'}
        fallback={
          <Show when={props.node.availability === 'available'}>
            <Button
              icon="ban"
              disabled={busy() || !mayWrite()}
              title="Stop scheduling new work here; running allocations stay"
              onClick={() => void apply('drained', false)}
            >
              Cordon
            </Button>
            <ConfirmButton
              label="Drain"
              confirmLabel="Evict allocations?"
              icon="logOut"
              disabled={busy() || !mayWrite()}
              onConfirm={() => void apply('drained', true)}
            />
          </Show>
        }
      >
        <Button
          variant="accent"
          icon="play"
          disabled={busy() || !mayWrite()}
          title="Return this node to the scheduling pool"
          onClick={() => void apply('available', false)}
        >
          Uncordon
        </Button>
      </Show>
    </div>
  )
}
