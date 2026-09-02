import { For, Show } from 'solid-js'
import { Badge, DefList, MonoBadge, cx } from './ui'

// The merged policy the manager returns from ShowNetPolicy (`effective_json`),
// mirroring `EffectivePolicy` in src/structs/netpolicy.rs. Every rule and
// handler carries `source` — `policy/<name>#<index>` or `task#<index>` — so
// the view can say where each line came from.
export type RuleMatch = {
  host?: string
  port?: number
  cidr?: string
  path?: string
  method?: string[]
  header?: Record<string, string>
  identity?: string
}

export type EffectiveRule = {
  when?: RuleMatch
  match?: RuleMatch
  action: string
  reason?: string
  source: string
}

export type EffectiveHandler = {
  handler: string
  url?: string
  service?: string
  port?: string
  node?: string
  timeout?: string
  onError?: string
  cache?: string
  source: string
}

export type EffectiveChain = {
  protocol: string
  policy: string
  onError: string
  accessLog: boolean
  tls?: { mode: string }
  rules?: EffectiveRule[]
  handlers?: EffectiveHandler[]
}

export type EffectivePolicy = {
  mode?: string
  egress?: EffectiveChain
  ingress?: EffectiveChain
  sources?: string[]
  hash?: string
}

export const parseEffectivePolicy = (json: string): EffectivePolicy | undefined => {
  if (!json.trim()) return undefined
  try {
    return JSON.parse(json) as EffectivePolicy
  } catch {
    return undefined
  }
}

export const isPolicyEmpty = (policy?: EffectivePolicy) =>
  !policy || (!policy.mode && !policy.egress && !policy.ingress)

const describeMatch = (match?: RuleMatch): string => {
  if (!match) return 'anything'
  const parts: string[] = []
  if (match.host) parts.push(`host ${match.host}`)
  if (match.port) parts.push(`port ${match.port}`)
  if (match.cidr) parts.push(`cidr ${match.cidr}`)
  if (match.path) parts.push(`path ${match.path}`)
  if (match.method?.length) parts.push(`method ${match.method.join('|')}`)
  for (const [name, glob] of Object.entries(match.header ?? {})) parts.push(`header ${name}=${glob}`)
  if (match.identity) parts.push(`identity ${match.identity}`)
  return parts.length ? parts.join(' · ') : 'anything'
}

// `policy/sandbox-egress#2` → { label: 'sandbox-egress', index: 2, kind: 'policy' }
export const describeSource = (source: string) => {
  const [where, index] = source.split('#')
  if (where === 'task') return { kind: 'task' as const, label: 'this task', index: index ?? '' }
  if (where === 'fallthrough') return { kind: 'fallthrough' as const, label: 'fallthrough', index: '' }
  return { kind: 'policy' as const, label: where.replace(/^policy\//, ''), index: index ?? '' }
}

function SourceBadge(props: { source: string }) {
  const info = () => describeSource(props.source)
  return (
    <MonoBadge tone={info().kind === 'task' ? 'neutral' : 'info'} title={props.source}>
      {info().label}
      {info().index !== '' ? `#${info().index}` : ''}
    </MonoBadge>
  )
}

function ChainView(props: { direction: 'egress' | 'ingress'; chain: EffectiveChain }) {
  return (
    <div class="rounded-md border border-line bg-surface2/40 p-3">
      <div class="mb-2 flex flex-wrap items-center gap-2">
        <span class="text-xs font-semibold uppercase tracking-wider text-muted">{props.direction}</span>
        <Badge tone="neutral">{props.chain.protocol}</Badge>
        <Badge tone={props.chain.policy === 'deny' ? 'warn' : 'ok'} dot>
          fallthrough {props.chain.policy}
        </Badge>
        <Badge tone="neutral">onError {props.chain.onError}</Badge>
        <Show when={props.chain.tls}>
          <Badge tone="info">tls {props.chain.tls!.mode}</Badge>
        </Show>
        <Show when={props.chain.accessLog}>
          <Badge tone="neutral">access log</Badge>
        </Show>
      </div>
      <Show
        when={(props.chain.rules?.length ?? 0) > 0}
        fallback={<p class="text-xs text-faint">No rules; every connection takes the fallthrough verdict.</p>}
      >
        <ol class="space-y-1 text-sm">
          <For each={props.chain.rules ?? []}>
            {(rule, index) => (
              <li class="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                <span class="w-5 shrink-0 text-right text-2xs tabular-nums text-faint">{index() + 1}.</span>
                <span
                  class={cx(
                    'w-12 shrink-0 text-xs font-semibold uppercase',
                    rule.action === 'deny' ? 'text-danger' : 'text-ok',
                  )}
                >
                  {rule.action}
                </span>
                <span class="font-mono text-xs text-ink">{describeMatch(rule.when ?? rule.match)}</span>
                <SourceBadge source={rule.source} />
                <Show when={rule.reason}>
                  <span class="text-xs text-muted">— {rule.reason}</span>
                </Show>
              </li>
            )}
          </For>
        </ol>
      </Show>
      <Show when={(props.chain.handlers?.length ?? 0) > 0}>
        <ul class="mt-2 space-y-1 border-t border-line pt-2 text-sm">
          <For each={props.chain.handlers ?? []}>
            {(handler) => (
              <li class="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                <span class="text-xs font-semibold uppercase text-muted">handler</span>
                <span class="font-mono text-xs text-ink">
                  {handler.handler}{' '}
                  {handler.url ?? (handler.service ? `service ${handler.service}` : handler.port ? `port ${handler.port}` : handler.node ? `node ${handler.node}` : '')}
                </span>
                <Badge tone="neutral">timeout {handler.timeout ?? '2s'}</Badge>
                <Show when={handler.onError}>
                  <Badge tone="neutral">onError {handler.onError}</Badge>
                </Show>
                <SourceBadge source={handler.source} />
              </li>
            )}
          </For>
        </ul>
      </Show>
    </div>
  )
}

// What the node running each allocation says its datapath is doing
// (`ShowNetPolicyReply.node_status_json`, one line per allocation) — the
// difference between "a policy exists" and "the policy is enforced".
export type NodeHandlerStatus = {
  source: string
  handler: string
  target?: string
  healthy?: boolean
  lastError?: string
  calls?: number
  errors?: number
}

export type NodeStatus = {
  nodeId?: string
  allocationId?: number
  mode?: string
  captureReady?: boolean
  policyHash?: string
  upstreamsBound?: string[]
  ingressBound?: string[]
  resolverReady?: boolean
  proxyReady?: boolean
  gateShut?: boolean
  problems?: string[]
  handlers?: NodeHandlerStatus[]
  tlsReady?: boolean
  tlsError?: string
  meshIdentity?: boolean
}

export const parseNodeStatuses = (lines?: string[]): NodeStatus[] =>
  (lines ?? []).flatMap((line) => {
    try {
      return [JSON.parse(line) as NodeStatus]
    } catch {
      return []
    }
  })

export function DatapathStatus(props: { statuses: NodeStatus[]; policyHash?: string }) {
  return (
    <Show
      when={props.statuses.length > 0}
      fallback={
        <p class="text-xs text-faint">
          No running allocation has reported datapath status yet; nodes report it on their next heartbeat.
        </p>
      }
    >
      <ul class="space-y-2">
        <For each={props.statuses}>
          {(status) => {
            const gated = () => status.mode === 'gated'
            const pending = () => !!props.policyHash && !status.policyHash
            const live = () => !!status.policyHash && (!props.policyHash || status.policyHash === props.policyHash)
            return (
              <li class="rounded-md border border-line bg-surface2/40 p-3">
                <div class="flex flex-wrap items-center gap-2 text-sm">
                  <span class="font-mono text-xs text-ink">alloc {status.allocationId ?? '?'}</span>
                  <Show when={status.nodeId}>
                    <span class="text-xs text-muted">
                      on <span class="font-mono">{status.nodeId}</span>
                    </span>
                  </Show>
                  <Badge tone={gated() ? 'info' : 'neutral'}>{status.mode || 'host'}</Badge>
                  <Badge tone={gated() ? (status.captureReady ? 'ok' : 'danger') : 'neutral'} dot>
                    {gated() ? (status.captureReady ? 'enforced' : 'no network') : 'cooperative'}
                  </Badge>
                  <Show when={props.policyHash || status.policyHash}>
                    <Badge tone={pending() ? (gated() ? 'danger' : 'warn') : live() ? 'ok' : 'neutral'}>
                      {pending() ? 'policy pending' : live() ? 'policy live' : 'older policy'}
                    </Badge>
                  </Show>
                  <Show when={status.gateShut}>
                    <Badge tone="warn">boot gate shut</Badge>
                  </Show>
                  <Show when={status.tlsReady === false}>
                    <Badge tone="danger">tls not ready</Badge>
                  </Show>
                  <Show when={status.meshIdentity}>
                    <Badge tone="info">mesh cert</Badge>
                  </Show>
                </div>
                <div class="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                  <span>{status.upstreamsBound?.length ?? 0} upstream listener(s)</span>
                  <span>{status.ingressBound?.length ?? 0} ingress listener(s)</span>
                  <Show when={status.resolverReady}>
                    <span>resolver up</span>
                  </Show>
                  <Show when={status.proxyReady}>
                    <span>egress proxy up</span>
                  </Show>
                </div>
                <Show when={(status.problems?.length ?? 0) > 0}>
                  <ul class="mt-1 space-y-0.5 text-xs text-danger">
                    <For each={status.problems ?? []}>{(problem) => <li>{problem}</li>}</For>
                  </ul>
                </Show>
                <Show when={(status.handlers?.length ?? 0) > 0}>
                  <ul class="mt-2 space-y-1 border-t border-line pt-2 text-xs">
                    <For each={status.handlers ?? []}>
                      {(hop) => (
                        <li class="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                          <Badge tone={hop.healthy ? 'ok' : 'danger'} dot>
                            {hop.healthy ? 'healthy' : 'failing'}
                          </Badge>
                          <span class="font-mono text-ink">
                            {hop.handler} → {hop.target || '?'}
                          </span>
                          <SourceBadge source={hop.source} />
                          <span class="text-faint">
                            {hop.calls ?? 0} calls · {hop.errors ?? 0} errors
                          </span>
                          <Show when={hop.lastError}>
                            <span class="text-danger">— {hop.lastError}</span>
                          </Show>
                        </li>
                      )}
                    </For>
                  </ul>
                </Show>
              </li>
            )
          }}
        </For>
      </ul>
    </Show>
  )
}

// The effective policy for one task, as `legion net policy show` prints it,
// plus what each node running it says the datapath is doing.
export function EffectivePolicyView(props: {
  policy?: EffectivePolicy
  declaredMode?: string
  effectiveMode?: string
  matched?: string[]
  nodeStatus?: string[]
}) {
  return (
    <div class="space-y-3">
      <DefList
        cols={3}
        items={[
          [
            'Network mode',
            <span class="font-mono text-sm">
              {props.effectiveMode ?? props.policy?.mode ?? 'auto'}
              <Show when={props.declaredMode && props.effectiveMode && props.declaredMode !== props.effectiveMode}>
                <span class="text-faint"> (task declared {props.declaredMode})</span>
              </Show>
            </span>,
          ],
          [
            'Stored policies',
            <Show when={(props.matched?.length ?? 0) > 0} fallback={<span class="text-faint">none matched</span>}>
              <span class="flex flex-wrap gap-1">
                <For each={props.matched ?? []}>{(name) => <MonoBadge tone="info">{name}</MonoBadge>}</For>
              </span>
            </Show>,
          ],
          ['Hash', <span class="font-mono text-xs text-faint">{props.policy?.hash?.slice(0, 16) ?? '—'}</span>],
        ]}
      />
      <Show
        when={!isPolicyEmpty(props.policy)}
        fallback={<p class="text-sm text-muted">No policy applies; everything is allowed.</p>}
      >
        <Show when={props.policy?.egress}>
          <ChainView direction="egress" chain={props.policy!.egress!} />
        </Show>
        <Show when={props.policy?.ingress}>
          <ChainView direction="ingress" chain={props.policy!.ingress!} />
        </Show>
      </Show>
      <Show when={props.nodeStatus !== undefined}>
        <div>
          <div class="mb-1 text-xs font-semibold uppercase tracking-wider text-muted">Datapath</div>
          <DatapathStatus statuses={parseNodeStatuses(props.nodeStatus)} policyHash={props.policy?.hash} />
        </div>
      </Show>
    </div>
  )
}
