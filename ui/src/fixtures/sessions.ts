import { InstanceInfo } from '../gen/legion_pb'

/*
 * Fixture listings for session mode (docs/session-mode.md), rendered at
 * /testing. A dev cluster rarely runs more than a handful of sessions, and
 * never the hundreds of thousands the job page has to survive, so these
 * cover both: a small mixed listing (keyed, promoted, starting and running
 * sessions on two nodes), and a generator for a listing of any size.
 */

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567'
const isoAgo = (seconds: number) => new Date(Date.now() - seconds * 1000).toISOString()

/** A minted-shape session id (`<task>-<26 base32>`), deterministic in `n`. */
export const sessionId = (task: string, n: number): string => {
  let value = (n + 1) * 2654435761
  let id = ''
  for (let index = 0; index < 26; index += 1) {
    id += BASE32[value % 32]
    value = Math.floor(value / 32) + index * 7919 + n
  }
  return `${task}-${id}`
}

const session = (n: number, node: string, envelope: bigint, state: 'active' | 'pending', ageSeconds: number) =>
  new InstanceInfo({
    instanceId: sessionId('workspace', n),
    jobId: 'agents',
    group: 'default',
    task: 'workspace',
    mode: 'session',
    state,
    createdAt: isoAgo(ageSeconds),
    updatedAt: isoAgo(ageSeconds),
    durable: false,
    named: false,
    nodeId: node,
    leaseIssuedAt: isoAgo(ageSeconds),
    envelopeAllocId: envelope,
  })

/** A session job's listing: a keyed instance, two promoted sessions, and running and starting sessions on two nodes. */
export const sessionListing: InstanceInfo[] = [
  new InstanceInfo({
    instanceId: 'user-42',
    jobId: 'agents',
    group: 'default',
    task: 'workspace',
    mode: 'keyed',
    state: 'active',
    currentAllocId: 88214n,
    createdAt: isoAgo(3600),
    updatedAt: isoAgo(600),
    durable: true,
    named: true,
    promotedFrom: sessionId('workspace', 900),
  }),
  new InstanceInfo({
    instanceId: sessionId('workspace', 901),
    jobId: 'agents',
    group: 'default',
    task: 'workspace',
    mode: 'keyed',
    state: 'active',
    currentAllocId: 88215n,
    createdAt: isoAgo(1800),
    updatedAt: isoAgo(300),
    durable: true,
    named: false,
    promotedFrom: sessionId('workspace', 901),
  }),
  session(1, 'w-17', 4101n, 'active', 420),
  session(2, 'w-17', 4101n, 'active', 95),
  session(3, 'w-17', 4101n, 'pending', 2),
  session(4, 'w-22', 4102n, 'active', 1300),
  session(5, 'w-22', 4102n, 'active', 61),
]

/**
 * `count` running sessions spread over `nodes` nodes (one envelope each,
 * `#5000 + node`), uneven so the busiest-nodes summary has a shape.
 */
export const largeSessionListing = (count: number, nodes: number): InstanceInfo[] => {
  const listing: InstanceInfo[] = new Array(count)
  const now = Date.now()
  for (let n = 0; n < count; n += 1) {
    // Skew: low-numbered nodes take more sessions.
    const node = Math.floor(nodes * Math.pow((n * 7919) % count / count, 1.6))
    const issued = new Date(now - (n % 3600) * 1000).toISOString()
    listing[n] = new InstanceInfo({
      instanceId: sessionId('workspace', n),
      jobId: 'agents',
      group: 'default',
      task: 'workspace',
      mode: 'session',
      state: n % 97 === 0 ? 'pending' : 'active',
      createdAt: issued,
      updatedAt: issued,
      durable: false,
      nodeId: `w-${String(node).padStart(4, '0')}`,
      leaseIssuedAt: issued,
      envelopeAllocId: BigInt(5000 + node),
    })
  }
  return listing
}
