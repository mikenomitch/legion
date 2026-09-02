import { useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { createMemo, createResource } from 'solid-js'
import NodeDeepView from '../../components/NodeDeepView'
import { managerClient } from '../../grpcClient'
import { retainOnError, usePoll } from '../../utils/polling'

function NodeDetailContent() {
  const params = useParams()
  const nodeId = createMemo(() => decodeURIComponent(params.id))
  const tick = usePoll()

  // Polled: a drain in progress, a heartbeat going stale, or an allocation
  // landing should all show up without a reload.
  const [nodes, { refetch: refetchNode }] = createResource(
    () => [nodeId(), tick()] as const,
    retainOnError(
      async ([id]: readonly [string, number]) => {
        const response = await managerClient.getNode({ id })
        return response.node ? [response.node] : []
      },
      ([id]) => id,
    ),
  )

  const [ledger, { refetch: refetchLedger }] = createResource(
    () => [nodeId(), tick()] as const,
    retainOnError(
      async ([id]: readonly [string, number]) => {
        const response = await managerClient.getLedger({})
        return response.nodes.filter((entry) => entry.node?.id === id)
      },
      ([id]) => id,
    ),
  )

  return (
    <NodeDeepView
      nodes={nodes}
      ledger={ledger}
      onChanged={() => {
        void refetchNode()
        void refetchLedger()
      }}
    />
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function NodeDetail() {
  return (
    <AccessGate resource="node">
      <NodeDetailContent />
    </AccessGate>
  )
}
