import { formatDistanceToNow } from 'date-fns'

const toDate = (timestamp?: string | bigint): Date | undefined => {
  if (timestamp === undefined || timestamp === null) return undefined
  if (typeof timestamp === 'bigint') {
    const millis = Number(timestamp) * 1000
    const date = new Date(millis)
    return Number.isNaN(date.getTime()) ? undefined : date
  }

  const value = timestamp.trim()
  if (!value) return undefined

  if (/^\d+$/.test(value)) {
    const numeric = Number(value)
    if (!Number.isNaN(numeric)) {
      const millis = value.length <= 10 ? numeric * 1000 : numeric
      const date = new Date(millis)
      if (!Number.isNaN(date.getTime())) return date
    }
  }

  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? undefined : parsed
}

export const formatDate = (timestamp?: string | bigint): string => {
  const parsed = toDate(timestamp)
  if (!parsed) return '—'
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(parsed)
}

export const formatRelativeTime = (timestamp?: string | bigint): string => {
  const parsed = toDate(timestamp)
  if (!parsed) return '—'
  return formatDistanceToNow(parsed, { addSuffix: true })
}

export const asDisplay = (value?: string | bigint): string => {
  if (value === undefined || value === null) return '—'
  if (typeof value === 'bigint') return value.toString()
  return value === '' ? '—' : value
}
