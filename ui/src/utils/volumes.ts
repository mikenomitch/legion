import type { VolumeDriverInfo, VolumeInfo } from '../gen/legion_pb'
import type { Tone } from './status'

// Volume lifecycle: pending -> created -> destroying -> destroyed.
// `pending` is neutral, not a warning: provisioning is lazy, so a volume that
// has never mounted is normal for a job that has not placed yet.
export const volumeStateTone = (volume: VolumeInfo): Tone => {
  if (volume.lastError) return 'danger'
  switch (volume.state) {
    case 'created':
      return 'ok'
    case 'destroying':
      return 'warn'
    case 'destroyed':
      return 'neutral'
    default:
      return 'neutral'
  }
}

// One phrase for the property that decides whether a workload can move.
export const volumeScope = (volume: Pick<VolumeInfo, 'nodeLocal'>) =>
  volume.nodeLocal ? 'node-local' : 'network'

export const volumeScopeHint = (volume: Pick<VolumeInfo, 'nodeLocal'>) =>
  volume.nodeLocal
    ? 'The data lives on one machine, so this instance can only be replaced on that node.'
    : 'Any node can reach this storage, so the instance reschedules freely.'

// True when this volume constrains where its instance may be placed — the
// thing to check first when a stateful replica will not come back.
export const volumePinsPlacement = (volume: VolumeInfo) =>
  volume.nodeLocal && !!volume.nodeId && (volume.state === 'created' || volume.state === 'pending')

export const driverScope = (driver: VolumeDriverInfo) => {
  if (driver.ephemeral) return 'ephemeral'
  return driver.nodeLocal ? 'node-local' : 'network'
}

export const driverScopeTone = (driver: VolumeDriverInfo): Tone => {
  if (driver.availableNodes.length === 0) return 'danger'
  if (driver.ephemeral) return 'warn'
  return driver.nodeLocal ? 'neutral' : 'ok'
}

// Params/attrs arrive as JSON strings on the wire. Render them as sorted
// key/value pairs; anything unparseable falls back to the raw text rather than
// being dropped.
export const parseJsonMap = (raw?: string): Array<[string, string]> => {
  if (!raw || raw === '{}') return []
  try {
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return [['value', raw]]
    return Object.entries(parsed)
      .map(([key, value]) => [key, typeof value === 'string' ? value : JSON.stringify(value)] as [string, string])
      .sort(([a], [b]) => a.localeCompare(b))
  } catch {
    return [['value', raw]]
  }
}

// Volume ids are 32 random characters; the full value is only useful for
// copy/paste, so tables show a prefix.
export const shortVolumeId = (id: string) => (id.length > 14 ? `${id.slice(0, 12)}…` : id)
