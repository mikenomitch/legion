export const formatDate = (timestamp?: bigint): string => {
  if (!timestamp) return '—'
  const millis = Number(timestamp) * 1000
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(millis))
}

export const asDisplay = (value?: string | bigint): string => {
  if (value === undefined || value === null) return '—'
  if (typeof value === 'bigint') return value.toString()
  return value === '' ? '—' : value
}
