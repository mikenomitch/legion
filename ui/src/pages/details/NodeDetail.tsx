import { useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { createMemo, createResource } from 'solid-js'
import NodeDeepView from '../../components/NodeDeepView'
import { managerClient } from '../../grpcClient'

function NodeDetailContent() {
  const params = useParams()
  const nodeId = createMemo(() => decodeURIComponent(params.id))

  const [nodes] = createResource(nodeId, async (id) => {
    const response = await managerClient.getNode({ id })
    return response.node ? [response.node] : []
  })

  const [ledger] = createResource(nodeId, async (id) => {
    const response = await managerClient.getLedger({})
    return response.nodes.filter((entry) => entry.node?.id === id)
  })

  return <NodeDeepView nodes={nodes} ledger={ledger} />
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
