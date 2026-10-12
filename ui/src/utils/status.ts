/*
 * Central status vocabulary for the whole UI.
 *
 * Every status string the manager emits (job/allocation/rollout/node/instance
 * states, health summaries, log levels) funnels through `statusTone` so the
 * same word always gets the same color everywhere.
 */

export type Tone = 'ok' | 'warn' | 'danger' | 'info' | 'neutral' | 'accent'

const DANGER = ['fail', 'error', 'unhealthy', 'critical', 'down', 'dead', 'lost', 'cancel', 'reject', 'denied', 'fatal', 'destroy']
const WARN = ['pend', 'warn', 'degrad', 'drain', 'pause', 'stop', 'incomplete', 'expir', 'stale', 'reconnect', 'queue', 'reschedul', 'unknown', 'staging']
const INFO = ['deploy', 'roll', 'updat', 'schedul', 'assign', 'start', 'creat', 'connect', 'follower', 'learner', 'candidate', 'submit', 'loading', 'fetch']
const OK = ['running', 'complete', 'done', 'healthy', 'ready', 'active', 'pass', 'success', 'live', 'leader', 'placed', 'bound', 'staged']
// Whole words only — substrings would swallow "updating", "broken", "disabled", …
const OK_WORDS = /\b(up|ok|enabled)\b/

// Whole words the keyword lists would misread: "suspended" contains
// "pend", but a suspended instance is parked on purpose, not waiting.
const EXACT = new Map<string, Tone>([
  ['suspended', 'accent'],
  ['suspending', 'info'],
  ['resuming', 'info'],
  ['forking', 'info'],
  // A job waiting for capacity (docs/scheduler-and-eval-loop.md, "Blocked
  // evals"): it places on its own when room appears.
  ['blocked', 'warn'],
])

export const statusTone = (status?: string | null): Tone => {
  const normalized = (status ?? '').toLowerCase().trim()
  if (!normalized) return 'neutral'
  const exact = EXACT.get(normalized)
  if (exact) return exact
  const match = (keywords: string[]) => keywords.some((keyword) => normalized.includes(keyword))
  // Order matters: danger > warn > info > ok, so "update failed" is red and
  // "updating" is blue rather than accidentally green.
  if (match(DANGER)) return 'danger'
  if (match(WARN)) return 'warn'
  if (match(INFO)) return 'info'
  if (match(OK) || OK_WORDS.test(normalized)) return 'ok'
  return 'neutral'
}

// Subtle tinted badge: colored text on a low-alpha wash, no hard borders.
export const badgeToneClasses: Record<Tone, string> = {
  ok: 'bg-emerald-500/10 text-emerald-700 dark:bg-emerald-400/10 dark:text-emerald-300',
  warn: 'bg-amber-500/10 text-amber-700 dark:bg-amber-400/10 dark:text-amber-300',
  danger: 'bg-red-500/10 text-red-700 dark:bg-red-400/10 dark:text-red-300',
  info: 'bg-sky-500/10 text-sky-700 dark:bg-sky-400/10 dark:text-sky-300',
  neutral: 'bg-surface2 text-muted',
  accent: 'bg-brass/10 text-brass-strong',
}

export const dotToneClasses: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  danger: 'bg-red-500',
  info: 'bg-sky-500',
  neutral: 'bg-faint',
  accent: 'bg-brass',
}

export const textToneClasses: Record<Tone, string> = {
  ok: 'text-emerald-600 dark:text-emerald-400',
  warn: 'text-amber-600 dark:text-amber-400',
  danger: 'text-red-600 dark:text-red-400',
  info: 'text-sky-600 dark:text-sky-400',
  neutral: 'text-muted',
  accent: 'text-brass',
}

export const noteToneClasses: Record<Tone, string> = {
  ok: 'border-emerald-500/30 bg-emerald-500/5 text-emerald-800 dark:text-emerald-200',
  warn: 'border-amber-500/30 bg-amber-500/5 text-amber-800 dark:text-amber-200',
  danger: 'border-red-500/30 bg-red-500/5 text-red-800 dark:text-red-200',
  info: 'border-sky-500/30 bg-sky-500/5 text-sky-800 dark:text-sky-200',
  neutral: 'border-line bg-surface2 text-muted',
  accent: 'border-brass/30 bg-brass/5 text-brass-strong',
}

// Rollout states the engine never leaves. Pages that follow a rollout live
// stop polling once it reaches one of these.
const TERMINAL_ROLLOUT_STATUSES = new Set(['completed', 'failed', 'done', 'cancelled', 'completed_incomplete'])

export const isRolloutDone = (status: string): boolean => TERMINAL_ROLLOUT_STATUSES.has(status.toLowerCase())
