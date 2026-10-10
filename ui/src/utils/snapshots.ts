import type { InstanceInfo, SnapshotInfo } from '../gen/legion_pb'
import { allows, jobScope } from './acl'
import { statusTone, type Tone } from './status'

/*
 * Snapshot vocabulary and the rules for which actions a row offers
 * (docs/suspend-and-resume.md). The manager enforces every one of these, so
 * the functions here only decide what the UI enables and what the disabled
 * button's tooltip says.
 */

/**
 * State tones. `local` is the healthy resting state, so it reads green even
 * though the word is not in the shared status vocabulary. Unknown states (a
 * newer manager's `published`) fall through to the shared vocabulary.
 */
export const snapshotStateTone = (state: string): Tone => {
  switch (state) {
    case 'capturing':
      return 'info'
    case 'local':
    case 'published':
      return 'ok'
    case 'deleting':
      return 'warn'
    case 'failed':
    case 'lost':
      return 'danger'
    default:
      return statusTone(state)
  }
}

/** A snapshot's display name: its operator name, else a shortened id. */
export const snapshotLabel = (snapshot: SnapshotInfo): string =>
  snapshot.name || (snapshot.snapshotId.length > 17 ? `${snapshot.snapshotId.slice(0, 13)}…` : snapshot.snapshotId)

// The same rule `legion snapshot create --name` applies.
const SNAPSHOT_NAME = /^[A-Za-z0-9._-]{1,64}$/

export const snapshotNameError = (name: string): string | undefined => {
  if (!name) return undefined
  if (!SNAPSHOT_NAME.test(name)) return 'Use 1-64 of A-Z a-z 0-9 . _ -'
  if (name.startsWith('snap-')) return 'Names cannot start with snap-'
  return undefined
}

// Instance states a restore can start from: the manager rejects stopped and
// terminated instances.
const RESTORABLE_INSTANCE_STATES = new Set(['active', 'pending', 'rescheduling'])

/**
 * Why a row cannot be restored right now, or undefined when it can. A
 * `local` snapshot is the only kind the manager restores today (Phase 7b
 * adds `published`, which will need no change here beyond this check).
 */
export const restoreBlocker = (snapshot: SnapshotInfo, instance: InstanceInfo | undefined, jobName: string): string | undefined => {
  if (snapshot.state !== 'local') return `Only a captured (local) snapshot can be restored; this one is ${snapshot.state}`
  if (!instance) return 'The instance this snapshot was taken from no longer exists'
  if (instance.mode !== 'keyed') return 'Only keyed instances can be rolled back to a snapshot'
  if (!RESTORABLE_INSTANCE_STATES.has(instance.state)) return `The instance is ${instance.state}; start it before restoring`
  if (instance.restoreSnapshotId) return `The instance is already restoring from ${instance.restoreSnapshotId}`
  if (!allows('snapshot', 'read', jobScope(jobName)) || !allows('job', 'write', jobScope(jobName))) {
    return 'Restoring needs snapshot:read and job:write on this job'
  }
  return undefined
}

export const deleteBlocker = (snapshot: SnapshotInfo, jobName: string): string | undefined => {
  if (snapshot.state === 'capturing') return 'Still capturing; delete it once it finishes'
  if (snapshot.state === 'deleting') return 'Already deleting'
  if (!allows('snapshot', 'write', jobScope(jobName))) return 'Deleting needs snapshot:write on this job'
  return undefined
}

export const mayCreateSnapshot = (jobName: string): boolean => allows('snapshot', 'write', jobScope(jobName))
export const mayReadSnapshots = (jobName: string): boolean => allows('snapshot', 'read', jobScope(jobName))
