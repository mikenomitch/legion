type HealthTone = 'healthy' | 'unhealthy' | 'unknown'

type HealthSummary = {
  label: string
  tone: HealthTone
}

const FAIL_KEYWORDS = ['fail', 'failed', 'error', 'unhealthy', 'critical', 'down']
const PASS_KEYWORDS = ['pass', 'passed', 'ok', 'healthy', 'up']

const collectStrings = (value: unknown, bucket: string[]): void => {
  if (value === null || value === undefined) return

  if (typeof value === 'string') {
    bucket.push(value)
    return
  }

  if (Array.isArray(value)) {
    value.forEach((entry) => collectStrings(entry, bucket))
    return
  }

  if (typeof value === 'object') {
    Object.values(value as Record<string, unknown>).forEach((entry) => collectStrings(entry, bucket))
  }
}

const extractHealthStrings = (value: unknown): string[] => {
  const bucket: string[] = []
  collectStrings(value, bucket)
  return bucket.map((entry) => entry.trim()).filter((entry) => entry.length > 0)
}

const inferHealthTone = (values: string[]): HealthTone => {
  const normalized = values.map((entry) => entry.toLowerCase())
  if (normalized.some((entry) => FAIL_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'unhealthy'
  }
  if (normalized.some((entry) => PASS_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'healthy'
  }
  return 'unknown'
}

export const summarizeHealthChecks = (healthChecks?: string): HealthSummary => {
  if (!healthChecks || healthChecks.trim() === '' || healthChecks.trim() === '{}') {
    return { label: 'No checks', tone: 'unknown' }
  }

  let parsed: unknown = healthChecks
  try {
    parsed = JSON.parse(healthChecks)
  } catch {
    parsed = healthChecks
  }

  const values = extractHealthStrings(parsed)
  if (values.length === 0) {
    return { label: 'No checks', tone: 'unknown' }
  }

  const tone = inferHealthTone(values)
  if (tone === 'healthy') return { label: 'Healthy', tone }
  if (tone === 'unhealthy') return { label: 'Unhealthy', tone }
  return { label: 'Unknown', tone }
}

export const healthToneClasses = (tone: HealthTone): string => {
  if (tone === 'healthy') {
    return 'border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-600 dark:bg-emerald-950 dark:text-emerald-100'
  }
  if (tone === 'unhealthy') {
    return 'border-rose-300 bg-rose-50 text-rose-700 dark:border-rose-600 dark:bg-rose-950 dark:text-rose-100'
  }
  return 'border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-600 dark:bg-amber-950 dark:text-amber-100'
}
