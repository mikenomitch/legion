import { createPromiseClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { Manager } from './gen/legion_connect'

export const managerEndpoint =
  import.meta.env.VITE_MANAGER_ENDPOINT ?? 'http://127.0.0.1:4747'

const transport = createConnectTransport({
  baseUrl: managerEndpoint,
})

export const managerClient = createPromiseClient(Manager, transport)
