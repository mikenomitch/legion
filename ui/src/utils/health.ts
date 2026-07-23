import type { Tone } from './status'

type HealthSummary = {
  label: string
  tone: Tone
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

const inferHealthTone = (values: string[]): Tone => {
  const normalized = values.map((entry) => entry.toLowerCase())
  if (normalized.some((entry) => FAIL_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'danger'
  }
  if (normalized.some((entry) => PASS_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'ok'
  }
  return 'neutral'
}

// Summarize a node's raw health-check JSON blob into a label + tone for badges.
export const summarizeHealthChecks = (healthChecks?: string): HealthSummary => {
  if (!healthChecks || healthChecks.trim() === '' || healthChecks.trim() === '{}') {
    return { label: 'no checks', tone: 'neutral' }
  }

  let parsed: unknown = healthChecks
  try {
    parsed = JSON.parse(healthChecks)
  } catch {
    parsed = healthChecks
  }

  const values = extractHealthStrings(parsed)
  if (values.length === 0) {
    return { label: 'no checks', tone: 'neutral' }
  }

  const tone = inferHealthTone(values)
  if (tone === 'ok') return { label: 'healthy', tone }
  if (tone === 'danger') return { label: 'unhealthy', tone }
  return { label: 'unknown', tone: 'warn' }
}
