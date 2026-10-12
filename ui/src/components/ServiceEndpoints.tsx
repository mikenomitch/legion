import { A } from '@solidjs/router'
import { For, Show } from 'solid-js'
import type { ServiceEndpoint } from '../gen/legion_pb'
import type { AllocNetwork } from '../utils/allocations'
import { PUBLIC_ADDRESS_HINT, endpointAddress, endpointUrl, isPrivateHost } from '../utils/services'
import { Badge, CopyButton, EmptyState, Icon, StatusBadge, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/**
 * The address of one endpoint: a link when the browser can open it, the bare
 * host:port otherwise, always with a copy button — reaching the thing is the
 * whole point of looking a service up.
 *
 * The node's public address (LEGION_WORKER_PUBLIC_ADDRESS) is the link when
 * the worker declares one, with the in-cluster address beside it. Without
 * one, a private in-cluster address is still offered (a VPN or tunnel may
 * reach it), but marked as private, with how to get a public link.
 */
export function EndpointAddress(props: { endpoint: ServiceEndpoint }) {
  const publicEndpoint = () => (props.endpoint.publicAddress ? { ...props.endpoint, address: props.endpoint.publicAddress } : undefined)
  const address = () => endpointAddress(publicEndpoint() ?? props.endpoint)
  const url = () => endpointUrl(publicEndpoint() ?? props.endpoint)
  const privateAddress = () => (publicEndpoint() ? endpointAddress(props.endpoint) : undefined)
  const privateOnly = () => !publicEndpoint() && isPrivateHost(props.endpoint.address)

  return (
    <Show when={address()} fallback={<span class="text-faint">no port assigned</span>}>
      <span class="inline-flex flex-wrap items-center gap-1.5">
        <Show
          when={url()}
          fallback={<span class="font-mono text-xs text-ink">{address()}</span>}
        >
          <a
            href={url()}
            target="_blank"
            rel="noreferrer"
            class={cx(
              'inline-flex items-center gap-1 font-mono text-xs font-medium underline-offset-2 hover:underline',
              privateOnly() ? 'text-muted' : 'text-brass-strong',
            )}
            title={privateOnly() ? PUBLIC_ADDRESS_HINT : `Open ${url()}`}
          >
            {address()}
            <Icon name="externalLink" size={12} />
          </a>
        </Show>
        <CopyButton text={address()!} label="" />
        <Show when={publicEndpoint()}>
          <Badge tone="ok" title={`Public address of node ${props.endpoint.nodeId}`}>public</Badge>
        </Show>
        <Show when={privateOnly()}>
          <Badge tone="warn" title={PUBLIC_ADDRESS_HINT}>private</Badge>
        </Show>
        <Show when={privateAddress()}>
          <span class="font-mono text-2xs text-faint" title="In-cluster address (what other tasks dial)">
            {privateAddress()}
          </span>
        </Show>
      </span>
    </Show>
  )
}

/**
 * Endpoint table shared by the service detail page and the job/task views.
 */
export function ServiceEndpointsTable(props: {
  endpoints: readonly ServiceEndpoint[]
  // Job and task are constant on the task view, so let callers drop them.
  showTask?: boolean
  emptyTitle?: string
  emptyBody?: string
}) {
  return (
    <Show
      when={props.endpoints.length > 0}
      fallback={
        <EmptyState
          icon="radio"
          title={props.emptyTitle ?? 'No endpoints'}
          body={props.emptyBody ?? 'Endpoints appear once an allocation is placed and its port assigned.'}
        />
      }
    >
      <TableShell>
        <table class={tableClass}>
          <thead class={theadClass}>
            <tr>
              <th class={thClass}>Address</th>
              <th class={thClass}>Health</th>
              <Show when={props.showTask}>
                <th class={thClass}>Task</th>
              </Show>
              <th class={thClass}>Allocation</th>
              <th class={thClass}>Worker</th>
              <th class={thClass}>Detail</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-line">
            <For each={props.endpoints}>
              {(endpoint) => (
                <tr class={trHoverClass}>
                  <td class={tdClass}>
                    <EndpointAddress endpoint={endpoint} />
                    <Show when={endpoint.portLabel}>
                      <span class="ml-2 text-2xs text-faint">port {endpoint.portLabel}</span>
                    </Show>
                  </td>
                  <td class={tdClass}>
                    <StatusBadge status={endpoint.healthy ? 'healthy' : 'unhealthy'} />
                  </td>
                  <Show when={props.showTask}>
                    <td class={tdClass}>
                      <A
                        class="text-muted hover:text-brass-strong"
                        href={`/jobs/${encodeURIComponent(endpoint.jobId)}/tasks/${encodeURIComponent(endpoint.taskName)}`}
                      >
                        {endpoint.taskName}
                      </A>
                    </td>
                  </Show>
                  <td class={cx(tdClass, 'font-mono text-xs text-muted')}>#{String(endpoint.allocationId)}</td>
                  <td class={tdClass}>
                    <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(endpoint.nodeId)}`}>
                      {endpoint.nodeId}
                    </A>
                  </td>
                  <td class={cx(tdClass, 'text-xs text-muted')}>
                    {endpoint.healthStatus || <span class="text-faint">no report yet</span>}
                  </td>
                </tr>
              )}
            </For>
          </tbody>
        </table>
      </TableShell>
    </Show>
  )
}

/**
 * Ports assigned to one allocation at placement time. Dynamic ports change on
 * every reschedule, so "which port did this one get" is a real question — and
 * when the node address is known, each port is directly openable.
 */
export function AllocationPorts(props: { network?: AllocNetwork; publicAddress?: string }) {
  return (
    <Show when={(props.network?.ports.length ?? 0) > 0} fallback={<span class="text-faint">—</span>}>
      <span class="inline-flex flex-wrap items-center gap-1.5">
        <For each={props.network!.ports}>
          {(port) => {
            // The node's public address when its worker declares one; the
            // in-cluster address otherwise (see EndpointAddress).
            const host = () => props.publicAddress ?? props.network!.address
            const address = () => (host() ? `${host()}:${port.hostPort}` : undefined)
            const privateOnly = () => !props.publicAddress && Boolean(props.network!.address) && isPrivateHost(props.network!.address!)
            return (
              <span class="inline-flex items-center gap-1 rounded border border-line bg-surface2/60 px-1.5 py-0.5 font-mono text-2xs">
                <span class="text-faint">{port.name}</span>
                <Show when={address()} fallback={<span class="text-ink">{port.hostPort}</span>}>
                  <a
                    href={`http://${address()}`}
                    target="_blank"
                    rel="noreferrer"
                    class={cx('underline-offset-2 hover:underline', privateOnly() ? 'text-muted' : 'text-brass-strong')}
                    title={privateOnly() ? `http://${address()}\n\n${PUBLIC_ADDRESS_HINT}` : `Open http://${address()}`}
                  >
                    {port.hostPort}
                  </a>
                </Show>
                <Show when={port.to !== undefined}>
                  <span class="text-faint">→{port.to}</span>
                </Show>
              </span>
            )
          }}
        </For>
      </span>
    </Show>
  )
}

export function ServiceTags(props: { tags: readonly string[]; meta?: Record<string, string> }) {
  return (
    <span class="inline-flex flex-wrap items-center gap-1.5">
      <For each={props.tags}>{(tag) => <Badge tone="accent">{tag}</Badge>}</For>
      <For each={Object.entries(props.meta ?? {})}>
        {([key, value]) => (
          <Badge tone="neutral" mono title={`service.meta.${key}`}>
            {key}={value}
          </Badge>
        )}
      </For>
    </span>
  )
}
