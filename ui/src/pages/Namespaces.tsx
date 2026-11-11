/*
 * The Namespaces page (docs/namespaces.md, "The UI"): every namespace the
 * identity can see, with what each holds. A namespace is a name scope and a
 * permission scope, not an isolation boundary — the page says so, because
 * that is the misunderstanding the feature invites.
 */
import { A } from '@solidjs/router'
import { For, Show, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'
import {
  EmptyState,
  LoadingRows,
  Note,
  RelativeTime,
  SectionCard,
  TableShell,
  linkClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
} from '../components/ui'

export default function Namespaces() {
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [data] = createResource(
    () => tick() + 1,
    retainOnError(async () => (await managerClient.listNamespaces({})).namespaces, () => 'namespaces'),
  )

  return (
    <div class="space-y-4">
      <Note>
        A namespace separates names and scopes ACL rules: two teams can each run a job called <code>web</code>, and a
        token can be limited to <code>payments-*</code>. It does not isolate workloads — node pools, gated networking
        and the sandbox runtime do that.
      </Note>
      <SectionCard title="Namespaces" description="The namespaces this identity can see." flush>
        <Show when={!data.loading || data()} fallback={<LoadingRows rows={4} class="p-4" />}>
          <Show
            when={(data() ?? []).length > 0}
            fallback={
              <EmptyState
                icon="boxes"
                title="No namespaces visible"
                body="Create one with `legion namespace create <name>`."
              />
            }
          >
            <TableShell>
              <table class={tableClass}>
                <thead class={theadClass}>
                  <tr>
                    <th class={thClass}>Name</th>
                    <th class={`${thClass} text-right`}>Jobs</th>
                    <th class={`${thClass} text-right`}>Volumes</th>
                    <th class={`${thClass} text-right`}>Snapshots</th>
                    <th class={thClass}>Description</th>
                    <th class={thClass}>Created</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-line">
                  <For each={data() ?? []}>
                    {(namespace) => (
                      <tr class={trHoverClass}>
                        <td class={tdClass}>
                          <A class={`${linkClass} font-mono`} href={`/namespaces/${encodeURIComponent(namespace.name)}`}>
                            {namespace.name}
                          </A>
                        </td>
                        <td class={`${tdClass} text-right tabular-nums`}>
                          <A class="hover:text-brass-strong" href={`/jobs?ns=${encodeURIComponent(namespace.name)}`}>
                            {namespace.counts?.jobs ?? 0}
                          </A>
                        </td>
                        <td class={`${tdClass} text-right tabular-nums`}>{namespace.counts?.volumes ?? 0}</td>
                        <td class={`${tdClass} text-right tabular-nums`}>{namespace.counts?.snapshots ?? 0}</td>
                        <td class={`${tdClass} text-muted`}>{namespace.description || '—'}</td>
                        <td class={`${tdClass} text-muted`}>
                          <RelativeTime value={namespace.createdAt} />
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </TableShell>
          </Show>
        </Show>
      </SectionCard>
    </div>
  )
}
