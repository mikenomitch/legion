import type { KvJobReference } from '../gen/legion_pb'

/**
 * Where a job's config reads a KV key, made readable. The manager records
 * each read as a JSON pointer into the job config (GetKvReply.references):
 *
 *   /tasks/web/kvInjections/env/GREETING   → task web · env GREETING
 *   /tasks/web/kvInjections/files/etc/app  → task web · file etc/app
 *   /tasks/0/config/env/DB_PASSWORD        → task #0 · config.env.DB_PASSWORD
 *
 * Injection pointers are built from the task name and the env name / file
 * path verbatim (not RFC 6901-escaped), so a file path keeps its slashes.
 * Everything else is a `$path/key` reference: an escaped pointer into the
 * jobspec, where `tasks` is an array and so names the task by index.
 */
export type KvReadKind = 'env' | 'file' | 'config'

export type KvReadLocation = {
  /** `task web` / `task #0`, or `null` when the read is outside any task. */
  scope: string | null
  /** `env GREETING`, `file /etc/app.conf`, `config.env.X`. */
  detail: string
  kind: KvReadKind
  /** The raw pointer, for a tooltip. */
  pointer: string
}

const INJECTION_POINTER = /^\/tasks\/([^/]*)\/kvInjections\/(env|files)\/(.*)$/s

const unescapeSegment = (segment: string) => segment.replace(/~1/g, '/').replace(/~0/g, '~')

// `config.env.X`, `config.args[2]`, `meta["a.b"]`: dotted where that reads
// unambiguously, bracketed where a dot would mislead.
const joinPath = (segments: string[]): string =>
  segments
    .map((segment, index) => {
      if (/^\d+$/.test(segment)) return `[${segment}]`
      if (segment === '' || /[.\s[\]"]/.test(segment)) return `[${JSON.stringify(segment)}]`
      return index === 0 ? segment : `.${segment}`
    })
    .join('')

const taskLabel = (segment: string) => (/^\d+$/.test(segment) ? `task #${segment}` : `task ${segment}`)

export function describeKvPointer(pointer: string): KvReadLocation {
  const injection = INJECTION_POINTER.exec(pointer)
  if (injection) {
    const [, task, kind, name] = injection
    return {
      scope: taskLabel(task),
      detail: kind === 'env' ? `env ${name}` : `file ${name}`,
      kind: kind === 'env' ? 'env' : 'file',
      pointer,
    }
  }

  const segments = pointer === '' ? [] : pointer.replace(/^\//, '').split('/').map(unescapeSegment)
  if (segments[0] === 'tasks' && segments.length >= 2) {
    const rest = segments.slice(2)
    return {
      scope: taskLabel(segments[1]),
      detail: rest.length > 0 ? joinPath(rest) : '(whole task)',
      kind: 'config',
      pointer,
    }
  }
  return {
    scope: null,
    detail: segments.length > 0 ? joinPath(segments) : '(whole jobspec)',
    kind: 'config',
    pointer,
  }
}

export const kvReadLabel = (location: KvReadLocation) =>
  location.scope ? `${location.scope} · ${location.detail}` : location.detail

export type KvJobReads = {
  jobId: string
  /** Distinct job configs the reads were recorded from (normally one: the latest). */
  jobConfigIds: string[]
  locations: KvReadLocation[]
}

/** One row per job, jobs and their reads in a stable, readable order. */
export function groupKvReferences(references: readonly KvJobReference[]): KvJobReads[] {
  const byJob = new Map<string, KvJobReads>()
  for (const reference of references) {
    let group = byJob.get(reference.jobId)
    if (!group) {
      group = { jobId: reference.jobId, jobConfigIds: [], locations: [] }
      byJob.set(reference.jobId, group)
    }
    if (reference.jobConfigId && !group.jobConfigIds.includes(reference.jobConfigId)) {
      group.jobConfigIds.push(reference.jobConfigId)
    }
    if (!group.locations.some((location) => location.pointer === reference.jsonPointer)) {
      group.locations.push(describeKvPointer(reference.jsonPointer))
    }
  }
  const groups = [...byJob.values()].sort((a, b) => a.jobId.localeCompare(b.jobId))
  for (const group of groups) {
    group.locations.sort((a, b) => kvReadLabel(a).localeCompare(kvReadLabel(b)))
  }
  return groups
}

/** Distinct jobs among the references: what a delete warning counts. */
export const kvReferencingJobCount = (references: readonly KvJobReference[]) =>
  new Set(references.map((reference) => reference.jobId)).size
