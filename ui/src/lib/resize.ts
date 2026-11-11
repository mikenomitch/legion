// A resize override (docs/resize.md) as one line: what was set in place of
// the spec's amounts, by whom, and why. Undefined when the unit runs at spec.
export function resizeOverrideNote(raw: string | undefined): string | undefined {
  if (!raw) return undefined
  try {
    const record = JSON.parse(raw) as {
      memory?: number
      memoryMax?: number
      cpu?: number
      spec?: { memory?: number; memoryMax?: number; cpu?: number }
      reason?: string
      by?: string
      at?: string
    }
    const parts: string[] = []
    const part = (label: string, unit: string, now?: number, spec?: number) => {
      if (now === undefined) return
      parts.push(`${label} ${now} ${unit}${spec !== undefined ? ` (spec ${spec})` : ''}`)
    }
    part('memory', 'MiB', record.memory, record.spec?.memory)
    part('memoryMax', 'MiB', record.memoryMax, record.spec?.memoryMax)
    part('cpu', 'MHz', record.cpu, record.spec?.cpu)
    if (parts.length === 0) return undefined
    const who = [record.by, record.at].filter(Boolean).join(', ')
    return `Resized in place: ${parts.join(', ')}${record.reason ? `. Reason: ${record.reason}` : ''}${who ? ` (${who})` : ''}`
  } catch {
    return undefined
  }
}
