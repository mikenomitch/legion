import { managerClient } from '../grpcClient'

/*
 * Allocation-level runtime facts that the job/ledger RPCs do not carry:
 * assigned ports (on the allocation record) and the latest health snapshot
 * (its own table). Both are read through the state store, the same escape
 * hatch the job pages already use for live task status.
 */

export type AllocPort = {
  name: string
  hostPort: number
  to?: number
}

export type AllocNetwork = {
  address?: string
  ports: AllocPort[]
}

// `LedgerAllocation.network` is the JSON-encoded AllocNetwork the scheduler
// assigned at placement (camelCase, empty string when the task has no ports).
export const parseAllocNetwork = (raw: string | undefined): AllocNetwork | undefined => {
  if (!raw) return undefined
  try {
    const parsed = JSON.parse(raw) as { address?: unknown; ports?: unknown }
    const ports = Array.isArray(parsed.ports)
      ? parsed.ports
          .map((port): AllocPort | undefined => {
            if (!port || typeof port !== 'object') return undefined
            const item = port as Record<string, unknown>
            const name = typeof item.name === 'string' ? item.name : ''
            const hostPort = typeof item.hostPort === 'number' ? item.hostPort : undefined
            if (!name || hostPort === undefined) return undefined
            return { name, hostPort, to: typeof item.to === 'number' ? item.to : undefined }
          })
          .filter((port): port is AllocPort => port !== undefined)
      : []
    const address = typeof parsed.address === 'string' ? parsed.address : undefined
    if (!address && ports.length === 0) return undefined
    return { address, ports }
  } catch {
    return undefined
  }
}

export type AllocationRuntime = {
  // Task this allocation runs — the ledger only carries task_config_id.
  taskName: string
  healthy?: boolean
  status: string
}

// Latest health snapshot (and owning task) per allocation for one job. Workers
// report health on every heartbeat; an allocation with checks but no snapshot
// yet is "unknown", which is not the same as unhealthy and is shown as such.
export const fetchAllocationRuntime = async (
  jobId: string,
): Promise<Map<number, AllocationRuntime>> => {
  const escaped = jobId.replace(/'/g, "''")
  const sql =
    'SELECT a.id AS allocation_id, tc.name AS task_name, ah.healthy AS healthy, ah.status AS status ' +
    'FROM allocation a ' +
    'JOIN task_config tc ON tc.id = a.task_config_id ' +
    'LEFT JOIN allocation_health ah ON ah.allocation_id = a.id ' +
    `WHERE tc.job_id = '${escaped}'`

  const runtime = new Map<number, AllocationRuntime>()
  try {
    const response = await managerClient.queryStateStore({ sql })
    for (const row of response.rows) {
      try {
        const parsed = JSON.parse(row.json) as {
          allocation_id?: number
          task_name?: string
          healthy?: number | boolean | null
          status?: string | null
        }
        if (parsed.allocation_id === undefined) continue
        runtime.set(parsed.allocation_id, {
          taskName: parsed.task_name ?? '',
          healthy: parsed.healthy === null || parsed.healthy === undefined ? undefined : Boolean(parsed.healthy),
          status: parsed.status ?? '',
        })
      } catch {
        // ignore malformed rows
      }
    }
  } catch {
    // Older managers or a denied state-store read: the pages fall back to
    // showing no health rather than failing.
  }
  return runtime
}

// Live per-task status for one job, straight from the state store: the
// ledger only carries task_config_id (not names) and an initial deploy has
// no rollout, so this join is the authoritative running/success/failed per
// task. Empty on denial or an older manager; callers fall back to the
// rollout-derived view.
export const fetchTaskStatusByName = async (jobId: string): Promise<Map<string, string>> => {
  const escaped = jobId.replace(/'/g, "''")
  const sql =
    'SELECT tc.name AS task_name, COALESCE(ts.status, a.status) AS status ' +
    'FROM allocation a JOIN task_config tc ON tc.id = a.task_config_id ' +
    'LEFT JOIN task_status ts ON ts.task_config_id = a.task_config_id ' +
    `WHERE tc.job_id = '${escaped}'`
  const statusByName = new Map<string, string>()
  try {
    const response = await managerClient.queryStateStore({ sql })
    for (const row of response.rows) {
      try {
        const parsed = JSON.parse(row.json) as { task_name?: string; status?: string }
        if (parsed.task_name) statusByName.set(parsed.task_name, parsed.status ?? '')
      } catch {
        // ignore malformed rows
      }
    }
  } catch {
    // Denied state-store read or an older manager: no live status.
  }
  return statusByName
}

// Every task_config id this task has had in the job, across config versions.
// A failed allocation is deleted the moment its failure is reported, so its
// events can only be tied back to a task through the `taskConfigId` they
// carry. Empty on a denied state-store read or an older manager.
export const fetchTaskConfigIds = async (jobId: string, taskName: string): Promise<Set<string>> => {
  const job = jobId.replace(/'/g, "''")
  const task = taskName.replace(/'/g, "''")
  const sql = `SELECT id FROM task_config WHERE job_id = '${job}' AND name = '${task}'`
  const ids = new Set<string>()
  try {
    const response = await managerClient.queryStateStore({ sql })
    for (const row of response.rows) {
      try {
        const parsed = JSON.parse(row.json) as { id?: number }
        if (parsed.id !== undefined) ids.add(String(parsed.id))
      } catch {
        // ignore malformed rows
      }
    }
  } catch {
    // Denied state-store read or an older manager: callers fall back to the
    // task config ids of the allocations they can see.
  }
  return ids
}

// "unknown" is deliberate: no snapshot yet reads very differently from a
// failing check, and the status vocabulary already tones it as a warning.
export const allocationHealthLabel = (runtime: AllocationRuntime | undefined): string => {
  if (!runtime || runtime.healthy === undefined) return 'unknown'
  return runtime.healthy ? 'healthy' : 'unhealthy'
}
