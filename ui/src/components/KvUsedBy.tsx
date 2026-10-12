import { A } from '@solidjs/router'
import { For, Show, createMemo } from 'solid-js'
import type { KvJobReference } from '../gen/legion_pb'
import { groupKvReferences, type KvReadKind } from '../utils/kvRefs'
import { Badge, Icon, Spinner, cx, linkClass, type IconName } from './ui'

const KIND_ICON: Record<KvReadKind, IconName> = {
  env: 'terminal',
  file: 'file',
  config: 'sliders',
}

const KIND_TITLE: Record<KvReadKind, string> = {
  env: 'Injected env var (kvInjections)',
  file: 'Rendered file (kvInjections)',
  config: '$path/key reference in the jobspec',
}

/**
 * The jobs whose current config reads a KV key (GetKvReply.references), one
 * row per job with each place it reads the key. `references` is `null` while
 * they load. The manager leaves out jobs the caller cannot read, so the list
 * can be shorter than the real set; the footnote says so.
 */
export function KvUsedBy(props: { references: readonly KvJobReference[] | null; error?: string | null; class?: string }) {
  const groups = createMemo(() => groupKvReferences(props.references ?? []))

  return (
    <section class={cx('space-y-2', props.class)} aria-labelledby="kv-used-by">
      <div class="flex items-center gap-2">
        <h4 id="kv-used-by" class="eyebrow">Used by</h4>
        <Show when={props.references && groups().length > 0}>
          <Badge tone="neutral">
            {groups().length} job{groups().length === 1 ? '' : 's'}
          </Badge>
        </Show>
      </div>

      <Show
        when={props.references}
        fallback={
          <Show when={props.error} fallback={<Spinner label="Loading references…" />}>
            <p class="text-sm text-red-600 dark:text-red-400">Couldn't load the jobs that read this key: {props.error}</p>
          </Show>
        }
      >
        <Show when={groups().length > 0} fallback={<p class="text-sm text-muted">No job reads this key.</p>}>
          <ul class="divide-y divide-line overflow-hidden rounded-md border border-line">
            <For each={groups()}>
              {(group) => (
                <li class="space-y-1.5 px-3 py-2">
                  <div class="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-0.5">
                    <Icon name="layers" size={13} class="self-center text-faint" />
                    <A href={`/jobs/${encodeURIComponent(group.jobId)}`} class={cx(linkClass, 'min-w-0 break-all text-sm')}>
                      {group.jobId}
                    </A>
                    {/* Normally one: the job version the reads were recorded from. */}
                    <For each={group.jobConfigIds}>
                      {(configId) => (
                        <A
                          href={`/jobs/${encodeURIComponent(group.jobId)}/specs/${encodeURIComponent(configId)}`}
                          class="min-w-0 max-w-[14rem] truncate font-mono text-2xs text-faint hover:text-brass-strong"
                          title={`Recorded from job config ${configId}`}
                        >
                          {configId}
                        </A>
                      )}
                    </For>
                  </div>
                  <ul class="flex min-w-0 flex-wrap gap-1.5">
                    <For each={group.locations}>
                      {(location) => (
                        <li
                          class="inline-flex min-w-0 max-w-full items-center gap-1 rounded border border-line bg-surface2/60 px-1.5 py-0.5 font-mono text-2xs text-muted"
                          title={`${KIND_TITLE[location.kind]} · ${location.pointer || '/'}`}
                        >
                          <Icon name={KIND_ICON[location.kind]} size={11} class="text-faint" />
                          <span class="min-w-0 break-all">
                            <Show when={location.scope}>
                              <span class="text-ink">{location.scope}</span>
                              <span class="text-faint"> · </span>
                            </Show>
                            {location.detail}
                          </span>
                        </li>
                      )}
                    </For>
                  </ul>
                </li>
              )}
            </For>
          </ul>
        </Show>
      </Show>

      <p class="text-2xs leading-relaxed text-faint">
        Recorded when a job version is submitted, from that version's config. Jobs you can't read aren't listed.
      </p>
    </section>
  )
}
