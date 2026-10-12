import { Show, createResource } from 'solid-js'
import type { GetKeyringStatusReply } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { canRead } from '../utils/acl'
import { INDEX_POLL_INTERVAL_MS, retainOnError, swallowDenied, usePoll } from '../utils/polling'
import type { Tone } from '../utils/status'
import { Icon, Note, StatusBadge } from './ui'

/** Data keys only: the keyring also lists the checkpoint key, which seals published checkpoints. */
function dataKeyCount(keys: { state: string }[]): number {
  return keys.filter((key) => key.state !== 'checkpoint').length
}

/** Tone for a keyring state (docs/kv-encryption.md): locked is the one that pages someone. */
function keyringTone(state?: string): Tone {
  switch (state) {
    case 'unlocked':
      return 'ok'
    case 'locked':
      return 'danger'
    case 'disabled':
      return 'neutral'
    case 'uninitialized':
    case 'pending_upgrade':
      return 'warn'
    default:
      return 'neutral'
  }
}

/** humantime prints "30days" / "1month 2days"; put a space before each unit. */
const spaceUnits = (age: string) => age.replace(/(\d)([a-zA-Z])/g, '$1 $2')

/**
 * KV encryption at rest, as the manager answering the request sees it
 * (docs/kv-encryption.md). The root key is per-manager state, so this is about
 * *that* manager; the `hint` is the server's own one-line next step, shown
 * verbatim. GetKeyringStatus needs `operator:read`, so the strip renders
 * nothing for an identity without it — the KV page itself only needs `kv:read`.
 */
export function KeyringStrip() {
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [status] = createResource(
    () => (canRead('operator') ? tick() : false),
    retainOnError(
      swallowDenied<number, GetKeyringStatusReply | undefined>(async () => managerClient.getKeyringStatus({}), undefined),
      () => 'keyring',
    ),
  )
  const state = () => status()?.state
  const label = () => (state() ? state()!.replace(/_/g, ' ') : 'unavailable')

  return (
    <Show when={canRead('operator') && (status() || status.error)}>
      <section class="rounded-lg border border-line bg-surface px-4 py-2.5 shadow-card">
        <div class="flex flex-wrap items-center gap-x-4 gap-y-1.5">
          <div class="flex items-center gap-2">
            <Icon name="shieldCheck" size={15} class="text-faint" />
            <h2 class="eyebrow !text-muted">Encryption at rest</h2>
            <StatusBadge status={label()} tone={keyringTone(state())} title="Keyring state on the answering manager" />
          </div>
          <Show when={status()} fallback={<p class="text-xs text-muted">Keyring status is unavailable.</p>}>
            {(s) => (
              <p class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted">
                <span class="min-w-0 break-all">
                  Root key <span class="font-mono text-ink">{s().rootKeyProvider}</span>{' '}
                  <span class="font-mono">{s().rootKeyId || '(no key material)'}</span>
                  <Show when={s().rootKeyLocation}>
                    {' '}at <span class="font-mono">{s().rootKeyLocation}</span>
                  </Show>
                </span>
                <span class="tabular-nums">
                  {Number(s().encryptedRows)} encrypted · {Number(s().plaintextRows)} plaintext
                </span>
                <span>
                  {dataKeyCount(s().keys)} data key{dataKeyCount(s().keys) === 1 ? '' : 's'}
                  <Show when={s().dataKeyRotationAge}> · rotates every {spaceUnits(s().dataKeyRotationAge)}</Show>
                  {/* The checkpoint key seals published checkpoints, not KV values. */}
                  <Show when={s().keys.some((key) => key.state === 'checkpoint')}> · checkpoint key</Show>
                  <Show when={s().rewrap?.running}> · rewrap running ({Number(s().rewrap!.pending)} pending)</Show>
                </span>
              </p>
            )}
          </Show>
        </div>
        <Show when={status()?.hint}>
          <Note tone={keyringTone(state()) === 'ok' ? 'info' : keyringTone(state())} class="mt-2">
            {status()!.hint}
          </Note>
        </Show>
      </section>
    </Show>
  )
}
