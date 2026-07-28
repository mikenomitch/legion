import type { ServiceEndpoint } from '../gen/legion_pb'

/*
 * Service discovery helpers shared by the services pages, the job/task views,
 * and the allocation tables.
 */

// Schemes we can hand to the browser as a link. Anything else (grpc, redis,
// postgres…) is still shown as host:port, just not linkified.
const LINKABLE_PROTOCOLS = new Set(['http', 'https'])

// `service.meta.protocol` is the convention for "how do I talk to this".
export const endpointProtocol = (meta: Record<string, string>): string | undefined => {
  const protocol = (meta.protocol ?? meta.scheme ?? '').trim().toLowerCase()
  return protocol || undefined
}

export const endpointAddress = (endpoint: { address: string; port: number }): string | undefined => {
  if (!endpoint.address || !endpoint.port) return undefined
  return `${endpoint.address}:${endpoint.port}`
}

// A browser-clickable URL, when the service says it speaks HTTP. Without a
// declared protocol we assume http: an advertised TCP port that a browser can
// reach is overwhelmingly a web endpoint, and a wrong guess costs one failed
// tab rather than a hidden link.
export const endpointUrl = (endpoint: {
  address: string
  port: number
  meta: Record<string, string>
}): string | undefined => {
  const hostPort = endpointAddress(endpoint)
  if (!hostPort) return undefined
  const protocol = endpointProtocol(endpoint.meta) ?? 'http'
  if (!LINKABLE_PROTOCOLS.has(protocol)) return undefined
  return `${protocol}://${hostPort}`
}

export type DiagnosisLevel = 'ok' | 'fail' | 'warn'

export type DiagnosisStep = {
  level: DiagnosisLevel
  title: string
  detail?: string
  hint?: string
}

/*
 * Mirrors `legion service doctor`: walk the discovery layers in order and stop
 * at the first one that fails, so the answer to "why can't I reach this?" is
 * the failing layer rather than a wall of green checks.
 *
 * Kept in the UI (like the CLI keeps it client-side) because every input is
 * already on the page: the summary from ListServices and every endpoint from
 * ResolveService with include_unhealthy.
 */
export const diagnoseService = (
  service: string,
  summary: { jobId: string; taskName: string } | undefined,
  endpoints: readonly ServiceEndpoint[],
): DiagnosisStep[] => {
  if (!summary) {
    return [
      {
        level: 'fail',
        title: `Service "${service}" is not declared by any job`,
        hint: 'Declare one by adding `service: { port: <port> }` to a task.',
      },
    ]
  }

  const steps: DiagnosisStep[] = [
    {
      level: 'ok',
      title: 'Service is declared',
      detail: `job ${summary.jobId} · task ${summary.taskName}`,
    },
  ]

  if (endpoints.length === 0) {
    steps.push({
      level: 'fail',
      title: 'No allocations placed for this service',
      hint: 'The job may still be scheduling, or placement failed — check the job and the scheduler events.',
    })
    return steps
  }
  steps.push({ level: 'ok', title: `${endpoints.length} allocation(s) placed` })

  // The projection stuffs node liveness into health_status ("node X is lost").
  const deadNodes = endpoints.filter((endpoint) => endpoint.healthStatus.startsWith('node '))
  if (deadNodes.length === endpoints.length) {
    steps.push({
      level: 'fail',
      title: 'All endpoints are on dead or lost nodes',
      detail: deadNodes.map((endpoint) => `alloc ${endpoint.allocationId}: ${endpoint.healthStatus}`).join(' · '),
      hint: 'The scheduler will reschedule when capacity allows.',
    })
    return steps
  }
  steps.push({ level: 'ok', title: 'Nodes are alive' })

  const missingPort = endpoints.filter((endpoint) => endpoint.port === 0)
  if (missingPort.length === endpoints.length) {
    steps.push({
      level: 'fail',
      title: 'No port assigned to the advertised port label',
      hint: `Check that service.port matches a network.ports entry on task "${summary.taskName}".`,
    })
    return steps
  }
  steps.push({ level: 'ok', title: 'Port assigned' })

  const unhealthy = endpoints.filter((endpoint) => !endpoint.healthy)
  if (unhealthy.length > 0) {
    steps.push({
      level: unhealthy.length === endpoints.length ? 'fail' : 'warn',
      title:
        unhealthy.length === endpoints.length
          ? 'All endpoints are failing health checks'
          : `${unhealthy.length}/${endpoints.length} endpoints are failing health checks`,
      detail: unhealthy
        .map(
          (endpoint) =>
            `alloc ${endpoint.allocationId} on ${endpoint.nodeId}: ${
              endpoint.healthStatus || 'no health report yet (still starting?)'
            }`,
        )
        .join(' · '),
      hint: 'The task may not be listening yet — check its logs and its checks config.',
    })
    if (unhealthy.length === endpoints.length) return steps
  } else {
    steps.push({ level: 'ok', title: 'Health checks passing' })
  }

  const healthy = endpoints.filter((endpoint) => endpoint.healthy)
  steps.push({
    level: 'ok',
    title: `Resolves to ${healthy.length} healthy endpoint(s)`,
    detail: healthy.map((endpoint) => endpointAddress(endpoint) ?? endpoint.nodeId).join(' · '),
  })
  return steps
}
