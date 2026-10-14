import { useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { For, Show, createMemo, createResource, createSignal, type JSX } from 'solid-js'
import {
  Badge,
  CodeBlock,
  CopyButton,
  DefList,
  Icon,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  Spinner,
  cx,
  type IconName,
} from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { normalizeManagerTimestamp } from '../../utils/managerTime'
import type { Tone } from '../../utils/status'

/*
 * One manager's effective configuration (defaults applied), as it published
 * it to the store at startup and on every config reload. The snapshot is an
 * opaque JSON document on the wire, so this page reads it defensively: the
 * well-known blocks get curated summaries, every leaf of every block is still
 * listed, and any top-level key the page does not know lands in "Other
 * settings" — a new config field shows up here without a UI change.
 */

type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
type JsonObject = { [key: string]: Json }

const isObject = (value: Json | undefined): value is JsonObject =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

function at(root: Json | undefined, path: string): Json | undefined {
  let current: Json | undefined = root
  for (const segment of path.split('.')) {
    if (!isObject(current)) return undefined
    current = current[segment]
  }
  return current
}

const str = (value: Json | undefined): string | undefined =>
  typeof value === 'string' ? (value.trim() ? value : undefined) : typeof value === 'number' ? String(value) : undefined

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

// Credential-shaped keys (raft clusterSecret/joinToken, auth-method
// hmacSecret/webhookBearer, …). Names that merely describe a credential —
// its TTL, file path, URL or id — are configuration, not secrets.
const SECRET_KEY = /(secret|password|passwd|passphrase|token|bearer|credential|api_?key|private_?key|access_?key|session_?key)/i
const NOT_SECRET_SUFFIX = /(ttl|ttl_?seconds|seconds|_?ms|interval|age|path|file|url|id|ids|count|enabled)$/i

const isSecretKey = (key: string) => SECRET_KEY.test(key) && !NOT_SECRET_SUFFIX.test(key)

const hasValue = (value: Json | undefined) =>
  value !== null && value !== undefined && value !== '' && !(Array.isArray(value) && value.length === 0)

const REDACTED = '‹redacted›'

function redact(value: Json, key = ''): Json {
  if (key && isSecretKey(key) && hasValue(value)) return REDACTED
  if (Array.isArray(value)) return value.map((item) => redact(item))
  if (isObject(value)) {
    const out: JsonObject = {}
    for (const [childKey, child] of Object.entries(value)) out[childKey] = redact(child, childKey)
    return out
  }
  return value
}

// When the snapshot is not valid JSON there is no tree to walk; scrub
// `"secretKey": "value"` pairs textually before showing it verbatim.
const redactText = (raw: string) =>
  raw.replace(/"([^"\\]+)"(\s*:\s*)"((?:[^"\\]|\\.)*)"/g, (match, key: string, sep: string, value: string) =>
    isSecretKey(key) && value ? `"${key}"${sep}"${REDACTED}"` : match,
  )

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

const WORD_OVERRIDES: Record<string, string> = {
  acl: 'ACL',
  acls: 'ACLs',
  api: 'API',
  cpu: 'CPU',
  gpu: 'GPU',
  id: 'ID',
  jwks: 'JWKS',
  jwt: 'JWT',
  kv: 'KV',
  ms: '(ms)',
  oci: 'OCI',
  oidc: 'OIDC',
  tls: 'TLS',
  ttl: 'TTL',
  url: 'URL',
}

function humanizeSegment(segment: string): string {
  const words = segment
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .map((word) => WORD_OVERRIDES[word] ?? word)
  const label = words.join(' ')
  return label.charAt(0).toUpperCase() + label.slice(1)
}

const humanizePath = (path: string) => path.split('.').map(humanizeSegment).join(' › ')

// ---------------------------------------------------------------------------
// Flattening
// ---------------------------------------------------------------------------

type Entry = { path: string; key: string; value: Json }

/** Nested objects become dotted paths; arrays and scalars are leaves. */
function flatten(value: Json | undefined, prefix = '', skip: ReadonlySet<string> = new Set()): Entry[] {
  if (!isObject(value)) return prefix ? [{ path: prefix, key: prefix.split('.').pop()!, value: value ?? null }] : []
  const entries: Entry[] = []
  for (const [key, child] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key
    if (skip.has(path)) continue
    if (isObject(child) && Object.keys(child).length > 0) entries.push(...flatten(child, path, skip))
    else entries.push({ path, key, value: child })
  }
  return entries
}

// ---------------------------------------------------------------------------
// Value rendering
// ---------------------------------------------------------------------------

const faint = (text: string) => <span class="text-xs font-normal text-faint">{text}</span>

function OnOff(props: { on: boolean; onLabel?: string; offLabel?: string; offTone?: Tone; onTone?: Tone; title?: string }) {
  return (
    <Badge dot tone={props.on ? (props.onTone ?? 'ok') : (props.offTone ?? 'neutral')} title={props.title} class="uppercase tracking-wide">
      {props.on ? (props.onLabel ?? 'on') : (props.offLabel ?? 'off')}
    </Badge>
  )
}

function Redacted() {
  return (
    <Badge tone="neutral" title="Hidden in the UI: this key looks like a credential.">
      <Icon name="shieldCheck" size={11} />
      redacted
    </Badge>
  )
}

function ConfigValue(props: { value: Json | undefined; name: string }): JSX.Element {
  const value = props.value
  if (isSecretKey(props.name) && hasValue(value ?? null)) return <Redacted />
  if (value === null || value === undefined || value === '') return faint('not set')
  if (typeof value === 'boolean') return <OnOff on={value} onLabel="true" offLabel="false" />
  if (typeof value === 'number') return <span class="font-mono text-xs tabular-nums text-ink">{value}</span>
  if (typeof value === 'string') return <span class="break-all font-mono text-xs text-ink">{value}</span>
  if (Array.isArray(value)) {
    if (value.length === 0) return faint('none')
    if (value.every((item) => !isObject(item) && !Array.isArray(item))) {
      return (
        <span class="inline-flex flex-wrap gap-1 sm:justify-end">
          <For each={value}>{(item) => <Badge mono class="rounded">{String(item)}</Badge>}</For>
        </span>
      )
    }
    return (
      <div class="w-full space-y-1.5 text-left">
        <For each={value}>
          {(item) => (
            <Show when={isObject(item)} fallback={<ConfigValue value={item} name="" />}>
              <dl class="grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-3 gap-y-1 rounded-md border border-line bg-surface2/50 px-2.5 py-2">
                <For each={flatten(item)}>
                  {(entry) => (
                    <>
                      <dt class="text-2xs text-muted" title={entry.path}>{humanizePath(entry.path)}</dt>
                      <dd class="min-w-0"><ConfigValue value={entry.value} name={entry.key} /></dd>
                    </>
                  )}
                </For>
              </dl>
            </Show>
          )}
        </For>
      </div>
    )
  }
  return Object.keys(value).length === 0 ? faint('empty') : <span class="font-mono text-xs">{JSON.stringify(redact(value))}</span>
}

/** Label-left / value-right rows: how a config file reads. Stacks on phones. */
function ConfigRows(props: { entries: Entry[]; labelPrefix?: string }) {
  const label = (path: string) => {
    const prefix = props.labelPrefix
    return humanizePath(prefix && path.startsWith(`${prefix}.`) ? path.slice(prefix.length + 1) : path)
  }
  return (
    <dl class="divide-y divide-line">
      <For each={props.entries}>
        {(entry) => (
          <div class="flex flex-col gap-1 py-2 sm:flex-row sm:items-start sm:justify-between sm:gap-6">
            <dt class="shrink-0 text-xs text-muted sm:max-w-[45%] sm:pt-px" title={entry.path}>
              {label(entry.path)}
            </dt>
            <dd class="min-w-0 sm:text-right">
              <ConfigValue value={entry.value} name={entry.key} />
            </dd>
          </div>
        )}
      </For>
    </dl>
  )
}

type Group = { heading?: string; entries: Entry[]; labelPrefix?: string; empty?: string }

function ConfigCard(props: { title: string; description?: JSX.Element; icon?: IconName; groups: Group[]; class?: string }) {
  return (
    <SectionCard
      class={props.class}
      title={
        <span class="inline-flex items-center gap-1.5">
          <Show when={props.icon}>
            <Icon name={props.icon!} size={13} class="text-faint" />
          </Show>
          {props.title}
        </span>
      }
      description={props.description}
      contentClass="px-4 py-1.5"
    >
      <For each={props.groups}>
        {(group, index) => (
          <div class={cx(index() > 0 && 'mt-1 border-t border-line pt-2')}>
            <Show when={group.heading}>
              <p class="eyebrow pb-0.5 pt-1.5">{group.heading}</p>
            </Show>
            <Show when={group.entries.length > 0} fallback={<p class="py-2 text-xs text-faint">{group.empty ?? 'Nothing configured.'}</p>}>
              <ConfigRows entries={group.entries} labelPrefix={group.labelPrefix} />
            </Show>
          </div>
        )}
      </For>
    </SectionCard>
  )
}

// ---------------------------------------------------------------------------
// Feature tiles
// ---------------------------------------------------------------------------

type Feature = {
  name: string
  icon: IconName
  on: boolean
  onLabel?: string
  offLabel?: string
  onTone?: Tone
  offTone?: Tone
  title?: string
  detail?: JSX.Element
  /** Fields of the block the summary does not cover, listed under it. */
  extra?: Entry[]
}

function FeatureTile(props: { feature: Feature }) {
  const f = () => props.feature
  return (
    <div class="flex min-w-0 flex-col gap-1.5 rounded-md border border-line bg-surface2/40 p-3">
      <div class="flex items-center justify-between gap-2">
        <span class="inline-flex min-w-0 items-center gap-2 text-sm font-medium text-ink">
          <Icon name={f().icon} size={15} class={f().on ? 'text-brass-strong' : 'text-faint'} />
          <span class="truncate">{f().name}</span>
        </span>
        <OnOff on={f().on} onLabel={f().onLabel} offLabel={f().offLabel} onTone={f().onTone} offTone={f().offTone} title={f().title} />
      </div>
      <Show when={f().detail}>
        <p class="text-xs leading-relaxed text-muted">{f().detail}</p>
      </Show>
      <Show when={(f().extra ?? []).length > 0}>
        <dl class="grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-3 gap-y-0.5 text-2xs">
          <For each={f().extra}>
            {(entry) => (
              <>
                <dt class="text-faint" title={entry.path}>{humanizeSegment(entry.key)}</dt>
                <dd class="min-w-0 text-right"><ConfigValue value={entry.value} name={entry.key} /></dd>
              </>
            )}
          </For>
        </dl>
      </Show>
    </div>
  )
}

const mono = (text: string | undefined) => (text ? <span class="font-mono text-ink">{text}</span> : undefined)

/** The leaves of `block` that the tile's summary did not already state. */
function leftovers(config: JsonObject, block: string, known: string[]): Entry[] {
  const skip = new Set(known.map((key) => `${block}.${key}`))
  return flatten(at(config, block), block, skip)
}

function features(config: JsonObject): Feature[] {
  const bool = (path: string) => at(config, path) === true
  const serverPort = Number(at(config, 'server.port'))
  const telemetryPort = str(at(config, 'telemetry.port')) ?? (Number.isFinite(serverPort) ? String(serverPort + 2) : undefined)
  const aclOn = bool('acl.enabled')
  const auditOnly = bool('acl.auditOnly')
  const authMethods = at(config, 'acl.authMethods')
  const idleSweep = str(at(config, 'suspend.idleSweep'))
  const sweepOff = !idleSweep || /^0+\s*[a-z]*$/i.test(idleSweep)
  const encryptionProvider = str(at(config, 'encryption.rootKey.provider'))

  return [
    {
      name: 'Exec & actions',
      icon: 'terminal',
      on: bool('exec.enabled'),
      detail: bool('exec.enabled') ? 'ExecAllocation and the exec WebSocket bridge are served.' : 'Exec into allocations is turned off.',
      extra: leftovers(config, 'exec', ['enabled']),
    },
    {
      name: 'Telemetry',
      icon: 'activity',
      on: bool('telemetry.enabled'),
      detail: bool('telemetry.enabled') ? (
        <>
          /metrics, /healthz and /readyz on port {mono(telemetryPort)}
          <Show when={!str(at(config, 'telemetry.port'))}> (server port + 2)</Show>
        </>
      ) : (
        'No metrics or health-probe listener.'
      ),
      extra: leftovers(config, 'telemetry', ['enabled', 'port']),
    },
    {
      name: 'ACLs',
      icon: 'shieldCheck',
      on: aclOn,
      onLabel: auditOnly ? 'audit only' : 'enforcing',
      onTone: auditOnly ? 'warn' : 'ok',
      offTone: 'warn',
      title: aclOn ? undefined : 'Every caller has full access.',
      detail: aclOn
        ? `${Array.isArray(authMethods) ? authMethods.length : 0} auth method${Array.isArray(authMethods) && authMethods.length === 1 ? '' : 's'} · logins last ${str(at(config, 'acl.loginTtl')) ?? '—'}`
        : 'Not enforced: every caller has full access.',
    },
    {
      name: 'Workload identity',
      icon: 'keySquare',
      on: bool('workloadIdentity.enabled'),
      detail: bool('workloadIdentity.enabled') ? <>Issuer {mono(str(at(config, 'workloadIdentity.issuer')) ?? '(derived)')}</> : 'No per-task JWTs or OIDC discovery.',
    },
    {
      name: 'KV encryption',
      icon: 'database',
      on: bool('encryption.enabled'),
      detail: bool('encryption.enabled') ? (
        <>
          Root key via {mono(encryptionProvider ?? 'file')} · data key rotates every {mono(str(at(config, 'encryption.dataKeyRotationAge')))}
        </>
      ) : (
        'KV values are stored as plaintext.'
      ),
    },
    {
      name: 'Edge boot',
      icon: 'zap',
      on: bool('edgeBoot.enabled'),
      onTone: 'warn',
      title: bool('edgeBoot.enabled') ? 'Nodes may start work named by a live boot ticket.' : undefined,
      detail: bool('edgeBoot.enabled') ? (
        <>
          Tickets {mono(str(at(config, 'edgeBoot.defaultTicketTtl')))} by default, capped at {mono(str(at(config, 'edgeBoot.maxTicketTtl')))}
        </>
      ) : (
        'Boot tickets and edge-initiated starts are off.'
      ),
      extra: leftovers(config, 'edgeBoot', ['enabled', 'defaultTicketTtl', 'maxTicketTtl']),
    },
    {
      name: 'Auto-suspend',
      icon: 'pause',
      on: !sweepOff,
      detail: sweepOff ? (
        'The idle sweep is off.'
      ) : (
        <>
          Idle sweep every {mono(idleSweep)} · make room for resumes {bool('suspend.makeRoom') ? 'on' : 'off'}
        </>
      ),
      extra: leftovers(config, 'suspend', ['idleSweep', 'makeRoom']),
    },
    {
      name: 'Adaptive heartbeats',
      icon: 'radio',
      on: bool('heartbeat.adaptive'),
      detail: bool('heartbeat.adaptive') ? (
        <>
          Cadence {mono(str(at(config, 'heartbeat.minInterval')))}–{mono(str(at(config, 'heartbeat.maxInterval')))}, sized for{' '}
          {mono(str(at(config, 'heartbeat.targetRatePerSecond')))} beats/s
        </>
      ) : (
        'Workers keep their worker-type heartbeat interval.'
      ),
    },
  ]
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

const PERSISTENCE_LABEL: Record<string, string> = {
  local_disk: 'local disk',
  streaming: 'streaming replica',
  raft: 'raft',
}

// Top-level keys a curated section below renders. Anything else is listed
// under "Other settings".
const KNOWN_TOP_LEVEL = new Set([
  'server',
  'storePath',
  'workerConfigPath',
  'workerCheckSeconds',
  'evalThreads',
  'schedulingPolicy',
  'defaultPriority',
  'rolloutRetentionDays',
  'kvDebounceMs',
  'schedulerSettings',
  'taskResourceDefaults',
  'customResources',
  'persistence',
  'workloadIdentity',
  'acl',
  'artifacts',
  'exec',
  'telemetry',
  'edgeBoot',
  'heartbeat',
  'liveness',
  'encryption',
  'suspend',
])

const pick = (config: JsonObject, keys: string[]): Entry[] =>
  keys.filter((key) => key in config).map((key) => ({ path: key, key, value: config[key] }))

function ConfigView(props: { config: JsonObject }) {
  const config = () => props.config
  const listen = () => {
    const host = str(at(config(), 'server.host'))
    const port = str(at(config(), 'server.port'))
    return host || port ? `${host ?? ''}${port ? `:${port}` : ''}` : undefined
  }
  const tls = () => at(config(), 'server.tls.enabled') === true
  const advertise = () => {
    const explicit = str(at(config(), 'server.advertiseAddr'))
    if (explicit) return { value: explicit, derived: false }
    const host = str(at(config(), 'server.host'))
    const port = str(at(config(), 'server.port'))
    if (!host || !port) return undefined
    const shown = host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host
    return { value: `${tls() ? 'https' : 'http'}://${shown}:${port}`, derived: true }
  }
  const persistenceType = () => str(at(config(), 'persistence.type')) ?? 'local_disk'
  const raftPeers = () => {
    const peers = at(config(), 'persistence.raft.peers')
    return Array.isArray(peers) ? peers.map(String) : []
  }

  const glance = (): Array<[JSX.Element, JSX.Element]> => {
    const items: Array<[JSX.Element, JSX.Element]> = [
      ['Listen address', mono(listen()) ?? faint('not set')],
      [
        'Advertise address',
        <Show when={advertise()} fallback={faint('not set')}>
          {(addr) => (
            <span class="inline-flex flex-wrap items-baseline gap-x-1.5">
              {mono(addr().value)}
              <Show when={addr().derived}>{faint('(derived)')}</Show>
            </span>
          )}
        </Show>,
      ],
      ['TLS', <OnOff on={tls()} onLabel="enabled" offLabel="plaintext" offTone="warn" />],
      ['Store path', mono(str(config().storePath)) ?? faint('default ($LEGION_DATA_DIR or ./.legion)')],
      ['Persistence', <Badge tone="accent" mono class="rounded">{PERSISTENCE_LABEL[persistenceType()] ?? persistenceType()}</Badge>],
      ['Scheduling policy', mono(str(config().schedulingPolicy)) ?? faint('not set')],
      ['Eval threads', mono(str(config().evalThreads)) ?? faint('not set')],
      ['Default priority', mono(str(config().defaultPriority)) ?? faint('not set')],
    ]
    if (persistenceType() === 'raft') {
      items.push([
        'Raft peers',
        <span class="inline-flex flex-wrap gap-1">
          <For each={raftPeers()} fallback={faint('none')}>
            {(peer) => (
              <Badge mono class="rounded" tone={peer === str(at(config(), 'persistence.raft.nodeId')) ? 'accent' : 'neutral'}>
                {peer}
              </Badge>
            )}
          </For>
        </span>,
      ])
    }
    return items
  }

  const persistenceGroups = (): Group[] => {
    const type = persistenceType()
    const head = flatten(at(config(), 'persistence'), 'persistence', new Set(['persistence.type', 'persistence.raft', 'persistence.streaming']))
    if (type === 'local_disk') {
      return [
        {
          entries: head,
          labelPrefix: 'persistence',
          empty: 'Single-node SQLite store in the store path. Nothing is replicated.',
        },
      ]
    }
    const active = flatten(at(config(), `persistence.${type}`), `persistence.${type}`)
    return [{ entries: [...head, ...active], labelPrefix: `persistence.${type}` }]
  }

  const otherEntries = () =>
    Object.entries(config())
      .filter(([key]) => !KNOWN_TOP_LEVEL.has(key))
      .flatMap(([key, value]) => (isObject(value) && Object.keys(value).length > 0 ? flatten(value, key) : [{ path: key, key, value }]))

  const optionalBlock = (key: string) => {
    const value = config()[key]
    return hasValue(value ?? null) && !(isObject(value ?? null) && Object.keys(value as JsonObject).length === 0)
  }

  return (
    <div class="space-y-4">
      <SectionCard title="At a glance">
        <DefList cols={3} items={glance()} />
      </SectionCard>

      <SectionCard title="Features" description="Optional manager subsystems and whether this manager runs them.">
        <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <For each={features(config())}>{(feature) => <FeatureTile feature={feature} />}</For>
        </div>
      </SectionCard>

      <div class="grid items-start gap-4 lg:grid-cols-2">
        <ConfigCard
          title="Scheduler & workers"
          icon="sliders"
          description="Eval workers, placement tuning and worker bookkeeping."
          groups={[
            { entries: pick(config(), ['schedulingPolicy', 'evalThreads', 'defaultPriority', 'workerConfigPath', 'workerCheckSeconds', 'rolloutRetentionDays', 'kvDebounceMs']) },
            {
              heading: 'Scheduler settings',
              entries: flatten(at(config(), 'schedulerSettings'), 'schedulerSettings'),
              labelPrefix: 'schedulerSettings',
              empty: 'Built-in defaults.',
            },
          ]}
        />

        <div class="grid gap-4">
          <ConfigCard
            title="API server"
            icon="server"
            description="The gRPC / gRPC-Web listener."
            groups={[
              {
                entries: flatten(at(config(), 'server'), 'server', new Set(['server.host', 'server.port', 'server.advertiseAddr', 'server.tls.enabled'])),
                labelPrefix: 'server',
              },
            ]}
          />
          <ConfigCard
            title="Storage & replication"
            icon="hardDrive"
            description={
              <>
                Persistence mode <span class="font-mono">{PERSISTENCE_LABEL[persistenceType()] ?? persistenceType()}</span>. Settings for inactive modes are in the raw config.
              </>
            }
            groups={persistenceGroups()}
          />
        </div>

        <ConfigCard
          title="Heartbeat & liveness"
          icon="radio"
          description="Worker heartbeat cadence and how lost nodes are detected."
          groups={[
            { heading: 'Heartbeat', entries: flatten(at(config(), 'heartbeat'), 'heartbeat'), labelPrefix: 'heartbeat' },
            { heading: 'Liveness', entries: flatten(at(config(), 'liveness'), 'liveness'), labelPrefix: 'liveness' },
          ]}
        />

        <ConfigCard
          title="Access control"
          icon="shieldCheck"
          description="ACL enforcement and how callers log in."
          groups={[{ entries: flatten(at(config(), 'acl'), 'acl'), labelPrefix: 'acl' }]}
        />

        <ConfigCard
          title="Workload identity"
          icon="keySquare"
          description="The OIDC issuer for per-task and per-node tokens."
          groups={[{ entries: flatten(at(config(), 'workloadIdentity'), 'workloadIdentity'), labelPrefix: 'workloadIdentity' }]}
        />

        <ConfigCard
          title="KV encryption"
          icon="database"
          description="Encryption at rest for KV values."
          groups={[{ entries: flatten(at(config(), 'encryption'), 'encryption'), labelPrefix: 'encryption' }]}
        />

        <ConfigCard
          title="Artifacts"
          icon="boxes"
          description="Cluster-wide artifact fetch policy."
          groups={[{ entries: flatten(at(config(), 'artifacts'), 'artifacts'), labelPrefix: 'artifacts' }]}
        />

        <Show when={optionalBlock('taskResourceDefaults')}>
          <ConfigCard
            title="Task resource defaults"
            icon="layers"
            groups={[{ entries: flatten(at(config(), 'taskResourceDefaults'), 'taskResourceDefaults'), labelPrefix: 'taskResourceDefaults' }]}
          />
        </Show>

        <Show when={optionalBlock('customResources')}>
          <ConfigCard
            title="Custom resources"
            icon="layers"
            groups={[{ entries: flatten(at(config(), 'customResources'), 'customResources'), labelPrefix: 'customResources' }]}
          />
        </Show>

        <Show when={otherEntries().length > 0}>
          <ConfigCard
            title="Other settings"
            icon="file"
            description="Fields this page has no dedicated section for yet."
            groups={[{ entries: otherEntries() }]}
          />
        </Show>
      </div>
    </div>
  )
}

function RawConfig(props: { json: string }) {
  const [open, setOpen] = createSignal(false)
  return (
    <section class="overflow-hidden rounded-lg border border-line bg-surface shadow-card">
      <button
        type="button"
        class="flex w-full items-center justify-between gap-3 px-4 py-2.5 text-left transition-colors hover:bg-surface2/60"
        onClick={() => setOpen(!open())}
        aria-expanded={open()}
      >
        <span class="flex items-center gap-2">
          <Icon name="chevronRight" size={14} class={cx('text-faint transition-transform', open() && 'rotate-90')} />
          <span class="eyebrow !text-muted">Raw config</span>
          <span class="hidden text-xs text-faint sm:inline">The full snapshot as JSON, credentials redacted.</span>
        </span>
        <span class="text-2xs uppercase tracking-wide text-faint">{open() ? 'Hide' : 'Show'}</span>
      </button>
      <Show when={open()}>
        <div class="border-t border-line p-4">
          <CodeBlock code={props.json} label="manager config (JSON)" maxHClass="max-h-[560px]" />
        </div>
      </Show>
    </section>
  )
}

function prettyJsonLine(line: string): string {
  try {
    return JSON.stringify(JSON.parse(line), null, 2)
  } catch {
    return line
  }
}

function parseConfig(raw: string | undefined): { config?: JsonObject; error?: string } {
  if (!raw || !raw.trim()) return { error: 'The snapshot is empty.' }
  try {
    let parsed = JSON.parse(raw) as Json
    // Tolerate a double-encoded document (a JSON string holding JSON).
    if (typeof parsed === 'string') parsed = JSON.parse(parsed) as Json
    if (!isObject(parsed)) return { error: 'The snapshot is not a JSON object.' }
    return { config: parsed }
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) }
  }
}

function ManagerNodeDetailContent() {
  const params = useParams()
  const [data] = createResource(() => params.id, async (id) => {
    const response = await managerClient.getManagerConfigs({ query: decodeURIComponent(id), page: 1, pageSize: 1 })
    return response.configs[0]
  })
  const [o11y] = createResource(async () => managerClient.getObservability({ maxLines: 200 }))

  const parsed = createMemo(() => parseConfig(data()?.configJson))
  const redactedJson = createMemo(() => {
    const config = parsed().config
    return config ? JSON.stringify(redact(config), null, 2) : redactText(data()?.configJson ?? '')
  })
  const updatedAt = () => normalizeManagerTimestamp(data()?.updatedAt)

  const headerBadges = () => {
    const config = parsed().config
    if (!config) return undefined
    const type = str(at(config, 'persistence.type')) ?? 'local_disk'
    return (
      <span class="flex flex-wrap items-center gap-1.5">
        <Badge tone="accent" mono class="rounded">{PERSISTENCE_LABEL[type] ?? type}</Badge>
        <Show when={at(config, 'persistence.type') === 'raft' && str(at(config, 'persistence.raft.topology'))}>
          {(topology) => <Badge mono class="rounded">{topology()}</Badge>}
        </Show>
      </span>
    )
  }

  return (
    <Show when={!data.loading} fallback={<div class="py-6"><Spinner label="Loading manager…" /></div>}>
      <Show when={!data.error && data()} fallback={<Note tone="danger">Failed to load manager.</Note>}>
        <div>
          <PageHeader
            eyebrow="Manager"
            title={data()!.nodeId}
            badges={headerBadges()}
            subtitle={
              <>
                Effective configuration this manager published, defaults applied · updated{' '}
                <RelativeTime value={updatedAt()} class="text-ink" />
              </>
            }
            actions={<CopyButton text={redactedJson()} label="Copy JSON" size="sm" />}
          />
          <div class="space-y-4">
            <Show
              when={parsed().config}
              fallback={
                <Note tone="warn">
                  This snapshot could not be read as JSON ({parsed().error}). Open <strong>Raw config</strong> below to see it as stored.
                </Note>
              }
            >
              {(config) => <ConfigView config={config()} />}
            </Show>

            <RawConfig json={redactedJson()} />

            <Show when={o11y()?.o11yEnabled}>
              <SectionCard title="Recent logs" description="Recent OTEL log lines captured by the manager.">
                <div class="max-h-[280px] overflow-auto rounded-md border border-line bg-surface2/70 p-3 font-mono text-xs leading-relaxed">
                  {(o11y()?.logs ?? []).map((line) => (
                    <pre class="whitespace-pre-wrap">{prettyJsonLine(line)}</pre>
                  ))}
                </div>
              </SectionCard>
            </Show>
          </div>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function ManagerNodeDetail() {
  return (
    <AccessGate resource="operator">
      <ManagerNodeDetailContent />
    </AccessGate>
  )
}
