import { InstanceInfo, ListInstancesReply, ListSessionNodesReply, SessionNode } from '../gen/legion_pb'

/*
 * Fixture listings for session mode (docs/session-mode.md), rendered at
 * /testing. A dev cluster rarely runs more than a handful of sessions, and
 * never the hundreds of thousands the job page has to survive, so these
 * cover both: a small mixed listing (keyed, promoted, starting and running
 * sessions on two nodes), and a generator for a listing of any size.
 * `listInstancesFrom` answers a ListInstances request from a listing the way
 * the manager does (rows in full, one page of sessions in id order, totals
 * by node), so the paged card can be looked at with no manager.
 */

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567'
const isoAgo = (seconds: number) => new Date(Date.now() - seconds * 1000).toISOString()

/** A node's 8-character tag, deterministic in its id (the manager derives it from the node). */
export const nodeTag = (node: string): string => {
  let value = 2166136261
  for (let index = 0; index < node.length; index += 1) value = Math.imul(value ^ node.charCodeAt(index), 16777619) >>> 0
  let tag = ''
  for (let index = 0; index < 8; index += 1) {
    tag += BASE32[value % 32]
    value = (Math.floor(value / 32) + index * 7919) >>> 0
  }
  return tag
}

/** A session id minted on `node` (`<task>-<node tag>.<26 base32>`), deterministic in `n`. */
export const sessionId = (task: string, n: number, node = 'w-17'): string => {
  let value = (n + 1) * 2654435761
  let id = ''
  for (let index = 0; index < 26; index += 1) {
    id += BASE32[value % 32]
    value = Math.floor(value / 32) + index * 7919 + n
  }
  return `${task}-${nodeTag(node)}.${id}`
}

const session = (n: number, node: string, envelope: bigint, state: 'active' | 'pending', ageSeconds: number) =>
  new InstanceInfo({
    instanceId: sessionId('workspace', n, node),
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
    const nodeId = `w-${String(node).padStart(4, '0')}`
    listing[n] = new InstanceInfo({
      instanceId: sessionId('workspace', n, nodeId),
      jobId: 'agents',
      group: 'default',
      task: 'workspace',
      mode: 'session',
      state: n % 97 === 0 ? 'pending' : 'active',
      createdAt: issued,
      updatedAt: issued,
      durable: false,
      nodeId,
      leaseIssuedAt: issued,
      envelopeAllocId: BigInt(5000 + node),
    })
  }
  return listing
}

/** A listing split the way the manager holds it: rows, and sessions in id order with totals by node. */
export type SessionIndex = {
  rows: InstanceInfo[]
  sessions: InstanceInfo[]
  byNode: { [node: string]: bigint }
}

export const sessionIndex = (listing: readonly InstanceInfo[]): SessionIndex => {
  const rows: InstanceInfo[] = []
  const sessions: InstanceInfo[] = []
  const byNode: { [node: string]: bigint } = {}
  for (const instance of listing) {
    if (instance.mode !== 'session') {
      rows.push(instance)
      continue
    }
    sessions.push(instance)
    const node = instance.nodeId ?? ''
    byNode[node] = (byNode[node] ?? 0n) + 1n
  }
  sessions.sort((a, b) => (a.instanceId < b.instanceId ? -1 : a.instanceId > b.instanceId ? 1 : 0))
  return { rows, sessions, byNode }
}

/**
 * A ListInstances reply from `index`, as the manager pages it:
 * `sessionLimit` unset is 5000, at most 50000, and 0 returns no sessions;
 * `sessionPageToken` resumes after that id. Rows are always in full.
 */
export const listInstancesFrom = (
  index: SessionIndex,
  request: { sessionLimit?: number; sessionPageToken?: string },
  complete = true,
): ListInstancesReply => {
  const limit = Math.min(request.sessionLimit ?? 5000, 50_000)
  const after = request.sessionPageToken ?? ''
  // First session whose id sorts after the token.
  let low = 0
  let high = index.sessions.length
  while (after && low < high) {
    const middle = (low + high) >>> 1
    if (index.sessions[middle].instanceId <= after) low = middle + 1
    else high = middle
  }
  const page = limit > 0 ? index.sessions.slice(low, low + limit) : []
  const more = limit > 0 && low + limit < index.sessions.length
  return new ListInstancesReply({
    instances: [...index.rows, ...page],
    complete,
    asOf: new Date().toISOString(),
    nextSessionPageToken: more ? page[page.length - 1].instanceId : '',
    sessionCount: BigInt(index.sessions.length),
    sessionsByNode: index.byNode,
  })
}

/** A ListSessionNodes reply for the large listing's nodes: some full, one draining, one without edge starts. */
export const sessionNodesFrom = (index: SessionIndex, perEnvelope: number, task = 'workspace'): ListSessionNodesReply => {
  const nodes = Object.keys(index.byNode)
    .sort()
    .map((nodeId, position) => {
      const sessions = Number(index.byNode[nodeId])
      const weight = Math.max(0, perEnvelope - sessions)
      const reason = position === 1 ? 'draining' : position === 2 ? 'no-edge-sessions' : weight === 0 ? 'full' : ''
      return new SessionNode({
        nodeId,
        address: `https://${nodeId}.internal:4848`,
        tag: nodeTag(nodeId),
        accepting: reason === '',
        reason,
        room: JSON.stringify({ cpu: weight * 500, memory: weight * 1024 }),
        weight,
        sessions,
      })
    })
  return new ListSessionNodesReply({ jobId: 'agents', taskName: task, nodes, asOf: new Date().toISOString() })
}
