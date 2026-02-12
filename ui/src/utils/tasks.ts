export type TaskDependency = {
  task: string
  condition?: string
}

export type ParsedJobTask = {
  name: string
  group: string
  lifecycle: string
  kind: string
  dependsOn: TaskDependency[]
  config?: unknown
  sidecarOf?: string
}

const stripComments = (source: string) => source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|\s+)\/\/.*$/gm, '$1')

const toJsonCandidate = (source: string) =>
  stripComments(source)
    .replace(/([\{\[,\s])(\w+)\s*:/g, '$1"$2":')
    .replace(/,\s*([}\]])/g, '$1')

const parseTaskListFromContent = (content: string): ParsedJobTask[] => {
  const jsonCandidate = toJsonCandidate(content)
  const parsed = JSON.parse(jsonCandidate) as { tasks?: unknown[] }
  if (!parsed || !Array.isArray(parsed.tasks)) return []

  return parsed.tasks.flatMap((item) => {
    if (!item || typeof item !== 'object') return []
    const record = item as Record<string, unknown>
    const name = typeof record.name === 'string' ? record.name : ''
    if (!name) return []

    const dependsOn = Array.isArray(record.dependsOn)
      ? record.dependsOn.flatMap((dep) => {
          if (!dep || typeof dep !== 'object') return []
          const depRecord = dep as Record<string, unknown>
          const depTask = typeof depRecord.task === 'string' ? depRecord.task : ''
          if (!depTask) return []
          return [{ task: depTask, condition: typeof depRecord.condition === 'string' ? depRecord.condition : undefined }]
        })
      : []

    return [{
      name,
      group: typeof record.group === 'string' ? record.group : 'default',
      lifecycle: typeof record.lifecycle === 'string' ? record.lifecycle : 'service',
      kind: typeof record.kind === 'string' ? record.kind : 'unknown',
      dependsOn,
      config: record.config,
      sidecarOf: typeof record.sidecarOf === 'string' ? record.sidecarOf : undefined,
    }]
  })
}

export const parseJobTasksFromSubmissions = (submissions: readonly { content: string }[]): ParsedJobTask[] => {
  for (const submission of submissions) {
    try {
      const parsed = parseTaskListFromContent(submission.content)
      if (parsed.length > 0) return parsed
    } catch {
      continue
    }
  }
  return []
}
