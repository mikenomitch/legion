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

// RFC 1918, carrier-grade NAT, loopback, link-local, and IPv6 ULA/loopback.
// A browser outside the cluster's network usually cannot open these, so the
// UI says so instead of offering what looks like a working link.
export const isPrivateHost = (host: string): boolean => {
  const value = host.trim().toLowerCase().replace(/^\[|\]$/g, '')
  if (value === 'localhost' || value === '::1' || value.startsWith('fc') || value.startsWith('fd') || value.startsWith('fe80:')) return true
  const octets = value.split('.').map((part) => Number(part))
  if (octets.length !== 4 || octets.some((octet) => !Number.isInteger(octet) || octet < 0 || octet > 255)) return false
  const [a, b] = octets
  return (
    a === 10 ||
    a === 127 ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 169 && b === 254) ||
    (a === 100 && b >= 64 && b <= 127)
  )
}

// A node's public address, from the `publicAddress` config attribute its
// worker registers (LEGION_WORKER_PUBLIC_ADDRESS). `configAttrs` is the
// node's JSON-encoded attribute map.
export const nodePublicAddress = (configAttrs: string | undefined): string | undefined => {
  if (!configAttrs) return undefined
  try {
    const parsed = JSON.parse(configAttrs) as Record<string, unknown>
    const value = parsed.publicAddress
    return typeof value === 'string' && value.trim() ? value.trim() : undefined
  } catch {
    return undefined
  }
}

export const PUBLIC_ADDRESS_HINT =
  "This is the node's private address, which a browser outside the cluster's network usually can't reach. Start the worker with LEGION_WORKER_PUBLIC_ADDRESS set to its public host or IP (and open the port) to get a working link here."

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
