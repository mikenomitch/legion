import { useParams } from '@solidjs/router'
import { createMemo, createResource } from 'solid-js'
import NodeDeepView from '../../components/NodeDeepView'
import { managerClient } from '../../grpcClient'

export default function NodeDetail() {
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
