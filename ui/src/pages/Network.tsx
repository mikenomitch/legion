import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { EffectivePolicyView, parseEffectivePolicy } from '../components/NetPolicyView'
import {
  Badge,
  Button,
  CodeBlock,
  ConfirmButton,
  EmptyState,
  Field,
  LoadingRows,
  MonoBadge,
  Note,
  PageHeader,
  SectionCard,
  StatCard,
  TableShell,
  cx,
  inputClass,
  selectClass,
  tableClass,
  tdClass,
  textareaClass,
  thClass,
  theadClass,
  trHoverClass,
} from '../components/ui'
import { managerClient } from '../grpcClient'
import { canWriteAny } from '../utils/acl'
import { POLL_INTERVAL_MS } from '../utils/polling'

// A stored policy object as `object_json` carries it (NetPolicyObject).
type PolicyObject = {
  name: string
  priority?: number
  enforcement?: string
  mode?: string
  description?: string
  selector?: {
    nodePools?: string[]
    namespaces?: string[]
    runtimes?: string[]
    jobs?: string[]
    labels?: Record<string, string>
  }
  egress?: { protocol?: string; policy?: string; rules?: unknown[]; handlers?: unknown[] }
  ingress?: { protocol?: string; policy?: string; rules?: unknown[]; handlers?: unknown[] }
}

const parseObject = (json: string): PolicyObject | undefined => {
  try {
    return JSON.parse(json) as PolicyObject
  } catch {
    return undefined
  }
}

const selectorSummary = (object?: PolicyObject): string => {
  const selector = object?.selector ?? {}
  const parts: string[] = []
  if (selector.nodePools?.length) parts.push(`pools ${selector.nodePools.join(', ')}`)
  if (selector.namespaces?.length) parts.push(`ns ${selector.namespaces.join(', ')}`)
  if (selector.runtimes?.length) parts.push(`runtimes ${selector.runtimes.join(', ')}`)
  if (selector.jobs?.length) parts.push(`jobs ${selector.jobs.join(', ')}`)
  const labels = Object.entries(selector.labels ?? {})
  if (labels.length) parts.push(`labels ${labels.map(([k, v]) => `${k}=${v}`).join(', ')}`)
  return parts.length ? parts.join(' · ') : 'every task'
}

const chainSummary = (chain?: PolicyObject['egress']): string => {
  if (!chain) return '—'
  return `${chain.protocol ?? 'tcp'} · ${chain.policy ?? 'allow'} · ${chain.rules?.length ?? 0} rules · ${chain.handlers?.length ?? 0} handlers`
}

const EXAMPLE_POLICY = `{
  "name": "sandbox-egress",
  "priority": 100,
  "enforcement": "require",
  "selector": { "nodePools": ["untrusted"], "runtimes": ["sandbox"] },
  "mode": "gated",
  "egress": {
    "protocol": "http",
    "policy": "deny",
    "allow": ["*.pypi.org", "files.pythonhosted.org", "api.github.com/repos/**"],
    "rules": [
      { "match": { "header": { "authorization": "*" } }, "action": "deny",
        "reason": "credentials must not leave the sandbox" }
    ]
  }
}`

function NetworkContent() {
  const [tick, setTick] = createSignal(0)
  onMount(() => {
    const timer = setInterval(() => setTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  const [policies, { refetch }] = createResource(
    () => tick() + 1,
    async () => (await managerClient.listNetPolicies({})),
  )
  const rows = createMemo(() =>
    (policies()?.policies ?? []).map((policy) => ({ policy, object: parseObject(policy.objectJson) })),
  )
  const required = createMemo(() => rows().filter((row) => row.policy.enforcement === 'require').length)

  // --- apply / dry-run ---------------------------------------------------
  const [draft, setDraft] = createSignal('')
  const [applying, setApplying] = createSignal(false)
  const [applyResult, setApplyResult] = createSignal<
    { tone: 'ok' | 'warn' | 'danger'; message: string; affected?: string[] } | undefined
  >()
  const submit = async (dryRun: boolean) => {
    setApplying(true)
    setApplyResult(undefined)
    try {
      const objectJson = JSON.stringify(JSON.parse(draft()))
      const reply = await managerClient.upsertNetPolicy({ objectJson, dryRun })
      const name = reply.policy?.name ?? '?'
      setApplyResult({
        tone: dryRun ? 'warn' : 'ok',
        message: dryRun
          ? `Valid. Applying ${name} would affect ${reply.affectedAllocations.length} running allocation(s).`
          : `Applied ${name}; ${reply.affectedAllocations.length} running allocation(s) pick it up on their next heartbeat.`,
        affected: reply.affectedAllocations,
      })
      if (!dryRun) void refetch()
    } catch (error) {
      setApplyResult({ tone: 'danger', message: error instanceof Error ? error.message : String(error) })
    } finally {
      setApplying(false)
    }
  }
  const remove = async (name: string) => {
    try {
      await managerClient.deleteNetPolicy({ name })
      void refetch()
    } catch (error) {
      setApplyResult({ tone: 'danger', message: error instanceof Error ? error.message : String(error) })
    }
  }

  // --- show / test ------------------------------------------------------
  const [job, setJob] = createSignal('')
  const [task, setTask] = createSignal('')
  const [pool, setPool] = createSignal('')
  const [direction, setDirection] = createSignal<'to' | 'from'>('to')
  const [target, setTarget] = createSignal('')
  const [method, setMethod] = createSignal('')
  const [path, setPath] = createSignal('')
  const [showKey, setShowKey] = createSignal(0)
  const [shown] = createResource(
    () => (showKey() > 0 && job().trim() ? `${showKey()}:${job()}:${task()}:${pool()}` : undefined),
    async () => managerClient.showNetPolicy({ jobId: job().trim(), taskName: task().trim(), nodePool: pool().trim() }),
  )
  const [testKey, setTestKey] = createSignal(0)
  const [tested] = createResource(
    () => (testKey() > 0 && job().trim() && target().trim() ? `${testKey()}` : undefined),
    async () =>
      managerClient.testNetPolicy({
        jobId: job().trim(),
        taskName: task().trim(),
        nodePool: pool().trim(),
        to: direction() === 'to' ? target().trim() : '',
        from: direction() === 'from' ? target().trim() : '',
        identity: '',
        method: method().trim(),
        path: path().trim(),
      }),
  )
  const verdictTone = (verdict: string) => (verdict === 'allow' ? 'ok' : verdict === 'deny' ? 'danger' : 'warn')

  return (
    <div>
      <PageHeader
        eyebrow="Network"
        title="Network policy"
        subtitle="Selector-scoped egress and ingress rules, external upstreams, and handlers on the node datapath. Policies apply to matching tasks within one heartbeat; nothing restarts."
      />

      <div class="mb-4 grid gap-3 sm:grid-cols-3">
        <StatCard label="Stored policies" value={<span class="tabular-nums">{rows().length}</span>} icon="shieldCheck" />
        <StatCard label="Required" value={<span class="tabular-nums">{required()}</span>} hint="Applied ahead of every task's own block; never weakened." />
        <StatCard label="Generation" value={<span class="font-mono text-base">{String(policies()?.generation ?? 0)}</span>} hint="Bumps on every write." />
      </div>

      <div class="space-y-4">
        <SectionCard
          title="Policies"
          description="In merge order: every required policy by priority, then the one default that wins for a task."
          flush
          actions={
            <A href="/api" class="text-xs text-muted hover:text-brass-strong">
              API
            </A>
          }
        >
          <Show when={!policies.loading || policies()} fallback={<LoadingRows rows={3} />}>
            <Show
              when={rows().length > 0}
              fallback={
                <EmptyState
                  icon="shieldCheck"
                  title="No network policies"
                  body="Apply one below, or with `legion net policy apply -f configs/policies/sandbox-egress.ncl`. Tasks can also declare `network.egress` inline."
                />
              }
            >
              <TableShell>
                <table class={tableClass}>
                  <thead class={theadClass}>
                    <tr>
                      <th class={thClass}>Policy</th>
                      <th class={thClass}>Enforcement</th>
                      <th class={thClass}>Priority</th>
                      <th class={thClass}>Selector</th>
                      <th class={thClass}>Egress</th>
                      <th class={thClass}>Ingress</th>
                      <th class={thClass}>Mode</th>
                      <th class={thClass} />
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-line">
                    <For each={rows()}>
                      {({ policy, object }) => (
                        <tr class={trHoverClass}>
                          <td class={tdClass}>
                            <span class="font-medium text-ink">{policy.name}</span>
                            <Show when={policy.description}>
                              <p class="text-xs text-faint">{policy.description}</p>
                            </Show>
                          </td>
                          <td class={tdClass}>
                            <Badge tone={policy.enforcement === 'require' ? 'warn' : 'neutral'} dot class="uppercase tracking-wide">
                              {policy.enforcement}
                            </Badge>
                          </td>
                          <td class={cx(tdClass, 'tabular-nums')}>{String(policy.priority)}</td>
                          <td class={cx(tdClass, 'text-xs text-muted')}>{selectorSummary(object)}</td>
                          <td class={cx(tdClass, 'font-mono text-xs text-muted')}>{chainSummary(object?.egress)}</td>
                          <td class={cx(tdClass, 'font-mono text-xs text-muted')}>{chainSummary(object?.ingress)}</td>
                          <td class={tdClass}>
                            <Show when={object?.mode} fallback={<span class="text-faint">—</span>}>
                              <MonoBadge tone="info">{object!.mode}</MonoBadge>
                            </Show>
                          </td>
                          <td class={cx(tdClass, 'text-right')}>
                            <div class="flex justify-end gap-2">
                              <Button size="sm" icon="file" onClick={() => setDraft(JSON.stringify(object ?? {}, null, 2))}>
                                Edit
                              </Button>
                              <Show when={canWriteAny('net')}>
                                <ConfirmButton label="Delete" confirmLabel="Delete policy" icon="trash" onConfirm={() => void remove(policy.name)} />
                              </Show>
                            </div>
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

        <div class="grid gap-4 lg:grid-cols-2">
          <SectionCard
            title="Apply a policy"
            description="A policy object as JSON (Nickel files are evaluated by the CLI). Dry-run validates it and lists the running allocations it would change."
          >
            <Show when={!canWriteAny('net')}>
              <Note tone="warn" class="mb-3">
                Your token lacks <code>net:write</code>; you can dry-run but not apply.
              </Note>
            </Show>
            <textarea
              class={cx(textareaClass, 'h-64 w-full font-mono text-xs')}
              placeholder={EXAMPLE_POLICY}
              value={draft()}
              onInput={(event) => setDraft(event.currentTarget.value)}
              spellcheck={false}
            />
            <div class="mt-3 flex flex-wrap items-center gap-2">
              <Button size="md" icon="search" disabled={applying() || !draft().trim()} onClick={() => void submit(true)}>
                Dry run
              </Button>
              <Button size="md" variant="primary" icon="check" disabled={applying() || !draft().trim() || !canWriteAny('net')} onClick={() => void submit(false)}>
                Apply
              </Button>
              <Button size="md" icon="file" onClick={() => setDraft(EXAMPLE_POLICY)}>
                Load example
              </Button>
            </div>
            <Show when={applyResult()}>
              <Note tone={applyResult()!.tone} class="mt-3">
                <p>{applyResult()!.message}</p>
                <Show when={(applyResult()!.affected?.length ?? 0) > 0}>
                  <ul class="mt-1 font-mono text-xs">
                    <For each={applyResult()!.affected!.slice(0, 20)}>{(entry) => <li>{entry}</li>}</For>
                    <Show when={applyResult()!.affected!.length > 20}>
                      <li class="text-faint">… and {applyResult()!.affected!.length - 20} more</li>
                    </Show>
                  </ul>
                </Show>
              </Note>
            </Show>
          </SectionCard>

          <SectionCard
            title="What does a task get?"
            description="The merged policy for a task, every rule naming its source — and a dry-run verdict for a destination, without sending anything."
          >
            <div class="grid gap-3 sm:grid-cols-3">
              <Field label="Job">
                <input class={inputClass} placeholder="job id" value={job()} onInput={(e) => setJob(e.currentTarget.value)} />
              </Field>
              <Field label="Task" hint="optional for single-task jobs">
                <input class={inputClass} placeholder="task" value={task()} onInput={(e) => setTask(e.currentTarget.value)} />
              </Field>
              <Field label="Node pool" hint="optional">
                <input class={inputClass} placeholder="any" value={pool()} onInput={(e) => setPool(e.currentTarget.value)} />
              </Field>
            </div>
            <div class="mt-3 flex flex-wrap items-center gap-2">
              <Button size="md" icon="eye" disabled={!job().trim()} onClick={() => setShowKey((k) => k + 1)}>
                Show effective policy
              </Button>
            </div>
            <Show when={shown.error}>
              <Note tone="danger" class="mt-3">
                {String((shown.error as Error)?.message ?? shown.error)}
              </Note>
            </Show>
            <Show when={shown()}>
              <div class="mt-3">
                <EffectivePolicyView
                  policy={parseEffectivePolicy(shown()!.effectiveJson)}
                  declaredMode={shown()!.declaredMode}
                  effectiveMode={shown()!.effectiveMode}
                  matched={shown()!.matchedPolicies}
                />
              </div>
            </Show>

            <div class="mt-5 border-t border-line pt-4">
              <div class="grid gap-3 sm:grid-cols-[auto_1fr]">
                <Field label="Direction">
                  <select class={selectClass} value={direction()} onChange={(e) => setDirection(e.currentTarget.value as 'to' | 'from')}>
                    <option value="to">egress → to</option>
                    <option value="from">ingress ← from</option>
                  </select>
                </Field>
                <Field label={direction() === 'to' ? 'Destination' : 'Peer'} hint="host[:port], IP, or URL">
                  <input
                    class={inputClass}
                    placeholder={direction() === 'to' ? 'https://api.github.com/user' : '10.0.0.5:443'}
                    value={target()}
                    onInput={(e) => setTarget(e.currentTarget.value)}
                  />
                </Field>
              </div>
              <div class="mt-2 grid gap-3 sm:grid-cols-2">
                <Field label="Method" hint="HTTP chains only">
                  <input class={inputClass} placeholder="GET" value={method()} onInput={(e) => setMethod(e.currentTarget.value)} />
                </Field>
                <Field label="Path" hint="overrides the URL's path">
                  <input class={inputClass} placeholder="/repos/x/y" value={path()} onInput={(e) => setPath(e.currentTarget.value)} />
                </Field>
              </div>
              <div class="mt-3">
                <Button size="md" variant="primary" icon="play" disabled={!job().trim() || !target().trim()} onClick={() => setTestKey((k) => k + 1)}>
                  Test
                </Button>
              </div>
              <Show when={tested.error}>
                <Note tone="danger" class="mt-3">
                  {String((tested.error as Error)?.message ?? tested.error)}
                </Note>
              </Show>
              <Show when={tested()}>
                <div class="mt-3 flex flex-wrap items-center gap-2">
                  <Badge tone={verdictTone(tested()!.verdict)} dot class="uppercase tracking-wide">
                    {tested()!.verdict}
                  </Badge>
                  <span class="text-sm text-ink">{tested()!.explanation}</span>
                  <Show when={tested()!.source}>
                    <MonoBadge tone="info" title="deciding rule">{tested()!.source}</MonoBadge>
                  </Show>
                </div>
              </Show>
            </div>
          </SectionCard>
        </div>

        <SectionCard title="From the CLI" description="The same operations, for scripts and agents.">
          <CodeBlock
            code={`legion net policy apply -f configs/policies/sandbox-egress.ncl [--dry-run]
legion net policy list
legion net policy show <job> [--task <task>] [--pool <pool>]
legion net policy test <job> --to https://api.github.com/user`}
          />
        </SectionCard>
      </div>
    </div>
  )
}

export default function Network() {
  return (
    <AccessGate resource="net">
      <NetworkContent />
    </AccessGate>
  )
}
