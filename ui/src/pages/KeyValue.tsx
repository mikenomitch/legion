import { Code, ConnectError } from '@connectrpc/connect'
import { For, Match, Show, Switch, createEffect, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { KeyringStrip } from '../components/KeyringStrip'
import { KvUsedBy } from '../components/KvUsedBy'
import { EyeOffIcon, FetchCopyButton, HiddenValue, RevealedValue } from '../components/KvValueView'
import { managerClient } from '../grpcClient'
import { allows } from '../utils/acl'
import { kvReferencingJobCount } from '../utils/kvRefs'
import { KvEntry, KvJobReference, KvValueType } from '../gen/legion_pb'
import {
  Badge,
  Button,
  ConfirmButton,
  DefList,
  EmptyState,
  Field,
  Icon,
  Note,
  RelativeTime,
  SectionCard,
  Spinner,
  StatusBadge,
  cx,
  inputClass,
  selectClass,
  textareaClass,
} from '../components/ui'

type TreeNode = {
  name: string
  path: string
  children: TreeNode[]
  entry?: KvEntry
}

// Groups entries into a folder tree by splitting keys on "/". Segments are
// kept verbatim (empty ones included) so a node's path is always an exact
// prefix of the raw key: `/foo`, `foo` and `foo/bar/` stay separate nodes
// instead of collapsing onto each other, and a leaf's path is its real key.
const buildTree = (entries: KvEntry[]): TreeNode => {
  const root: TreeNode = { name: '', path: '', children: [] }
  const childrenByNode = new Map<TreeNode, Map<string, TreeNode>>()

  for (const entry of entries) {
    const parts = entry.key.split('/')
    let currentNode = root

    parts.forEach((part, index) => {
      const path = index === 0 ? part : `${currentNode.path}/${part}`
      let siblings = childrenByNode.get(currentNode)
      if (!siblings) {
        siblings = new Map()
        childrenByNode.set(currentNode, siblings)
      }
      let node = siblings.get(part)
      if (!node) {
        node = { name: part, path, children: [] }
        siblings.set(part, node)
        currentNode.children.push(node)
      }
      if (index === parts.length - 1) {
        node.entry = entry
      }
      currentNode = node
    })
  }

  const sortTree = (node: TreeNode) => {
    node.children.sort((a, b) => a.name.localeCompare(b.name))
    node.children.forEach(sortTree)
  }

  sortTree(root)
  return root
}

const valueTypeLabel = (valueType: number) => {
  switch (valueType) {
    case KvValueType.STRING:
      return 'string'
    case KvValueType.NUMBER:
      return 'number'
    case KvValueType.JSON:
      return 'json'
    default:
      return 'unspecified'
  }
}

/** How long a revealed value stays on screen before it is masked again. */
const REVEAL_SECONDS = 60

/**
 * What the panel knows about the selected key. It never holds a value: the
 * entry comes from the value-less listing or a metadata-only GetKv, and is
 * blanked again when it comes from a full read. `references` is `null` until
 * the manager answers.
 */
type Selected = {
  key: string
  entry?: KvEntry
  references: KvJobReference[] | null
  referencesError?: string
  /** The manager answered and the key does not exist (deleted since the list loaded). */
  missing?: boolean
}

/**
 * hidden: masked, the page holds no plaintext.
 * revealed: the plaintext on screen, read-only, until Hide or the countdown.
 * edit: the revealed value in an editable textarea (version-checked save).
 * replace: an empty textarea for a new value; the old one is never fetched.
 */
type ValueMode = 'hidden' | 'revealed' | 'edit' | 'replace'

/** A revealed value and the entry version it was read at. */
type Revealed = { value: string; version: bigint }

const withoutValue = (entry: KvEntry): KvEntry => {
  const copy = entry.clone()
  copy.value = ''
  return copy
}

const describeError = (err: unknown) => {
  if (err instanceof ConnectError) return err.rawMessage || err.message
  return err instanceof Error ? err.message : String(err)
}

// A write rejected by the version check, rather than by a lock or anything else.
const isVersionConflict = (err: unknown) =>
  err instanceof ConnectError && err.code === Code.AlreadyExists && /expected version/i.test(err.rawMessage)

const typeOrString = (valueType: KvValueType) => (valueType === KvValueType.UNSPECIFIED ? KvValueType.STRING : valueType)

const TypeSelect = (props: { value: KvValueType; onChange: (value: KvValueType) => void; class?: string }) => (
  <select
    class={cx(selectClass, 'w-full', props.class)}
    value={props.value}
    onChange={(event) => props.onChange(Number(event.currentTarget.value))}
  >
    <option value={KvValueType.STRING}>string</option>
    <option value={KvValueType.NUMBER}>number</option>
    <option value={KvValueType.JSON}>json</option>
  </select>
)

function KeyValueContent() {
  const [entries, setEntries] = createSignal<KvEntry[]>([])
  const [loading, setLoading] = createSignal(false)
  const [error, setError] = createSignal<string | null>(null)
  const [status, setStatus] = createSignal<string | null>(null)

  const [selected, setSelected] = createSignal<Selected | null>(null)
  const [mode, setMode] = createSignal<ValueMode>('hidden')
  // The plaintext, held only while `mode` is `revealed`.
  const [revealed, setRevealed] = createSignal<Revealed | null>(null)
  const [revealing, setRevealing] = createSignal(false)
  const [revealDeadline, setRevealDeadline] = createSignal(0)
  const [clock, setClock] = createSignal(Date.now())

  // The form: a new entry's key, value and type, or the selected entry's
  // edit/replace draft along with the version that draft is checked against.
  const [draftKey, setDraftKey] = createSignal('')
  const [draftValue, setDraftValue] = createSignal('')
  const [draftType, setDraftType] = createSignal<KvValueType>(KvValueType.STRING)
  const [draftVersion, setDraftVersion] = createSignal<bigint | undefined>(undefined)
  const [patchValue, setPatchValue] = createSignal('')
  const [busy, setBusy] = createSignal(false)

  // Bumped on every hide and every selection change, so a read that resolves
  // late never lands on another key, and never re-reveals a hidden value.
  let revealToken = 0
  let detailToken = 0

  const selectedKey = () => selected()?.key ?? null
  const entry = () => selected()?.entry
  const tree = createMemo(() => buildTree(entries()))
  const canReadKey = () => {
    const key = selectedKey()
    return key !== null && allows('kv', 'read', { path: key })
  }
  const canWriteKey = () => {
    const key = selectedKey()
    return key !== null && allows('kv', 'write', { path: key })
  }
  const referencingJobs = () => kvReferencingJobCount(selected()?.references ?? [])

  // Masks the value and drops every copy of the plaintext the page holds: the
  // revealed text and an edit/replace draft. A new entry's draft is the user's
  // own typing and is left alone.
  const hide = () => {
    revealToken++
    setRevealing(false)
    setRevealed(null)
    if (mode() === 'edit' || mode() === 'replace') {
      setDraftValue('')
      setDraftVersion(undefined)
    }
    setMode('hidden')
  }

  // Type, version, timestamps, encryption and references; never the value.
  const loadDetail = async (key: string) => {
    const token = ++detailToken
    try {
      const reply = await managerClient.getKv({ key, metadataOnly: true })
      if (token !== detailToken || selectedKey() !== key) return
      setSelected({
        key,
        entry: reply.entry ? withoutValue(reply.entry) : undefined,
        references: reply.references,
        missing: !reply.entry,
      })
    } catch (err) {
      if (token !== detailToken || selectedKey() !== key) return
      console.error('Failed to load kv entry', err)
      setSelected((current) => (current && current.key === key ? { ...current, referencesError: describeError(err) } : current))
      setError(`Unable to load ${key}: ${describeError(err)}`)
    }
  }

  const select = (key: string | null) => {
    hide()
    setPatchValue('')
    if (key === null) {
      detailToken++
      // Already on the new-entry form: keep what the user has typed.
      if (selected() === null) return
      setSelected(null)
      setDraftKey('')
      setDraftValue('')
      setDraftType(KvValueType.STRING)
      return
    }
    const listed = entries().find((candidate) => candidate.key === key)
    setSelected({ key, entry: listed ? withoutValue(listed) : undefined, references: null })
    void loadDetail(key)
  }

  const refresh = async () => {
    hide()
    setLoading(true)
    setError(null)
    try {
      // Key names and metadata only: ListKv leaves values out (and never
      // decrypts) unless `includeValues` is set, which this page never does.
      const response = await managerClient.listKv({ prefix: '' })
      setEntries(response.entries)
    } catch (err) {
      console.error('Failed to load kv entries', err)
      setError('Unable to load key/value entries')
    } finally {
      setLoading(false)
    }
    const key = selectedKey()
    if (key !== null) void loadDetail(key)
  }

  onMount(() => {
    void refresh()
    // A revealed value does not outlive the tab being in the background.
    const onVisibility = () => {
      if (document.hidden && mode() === 'revealed') hide()
    }
    document.addEventListener('visibilitychange', onVisibility)
    onCleanup(() => {
      document.removeEventListener('visibilitychange', onVisibility)
      revealToken++
      detailToken++
    })
  })

  // The reveal countdown; masks the value when it runs out.
  createEffect(() => {
    if (mode() !== 'revealed') return
    const timer = setInterval(() => {
      setClock(Date.now())
      if (Date.now() >= revealDeadline()) hide()
    }, 1000)
    onCleanup(() => clearInterval(timer))
  })
  const secondsLeft = () => Math.max(0, Math.ceil((revealDeadline() - clock()) / 1000))

  const fetchFull = async (key: string) => {
    const reply = await managerClient.getKv({ key })
    if (!reply.entry) throw new Error(`${key} no longer exists`)
    return { entry: reply.entry, references: reply.references }
  }

  const reveal = async (then: 'revealed' | 'edit') => {
    const key = selectedKey()
    if (key === null) return
    const token = ++revealToken
    setRevealing(true)
    setError(null)
    try {
      const full = await fetchFull(key)
      if (token !== revealToken || selectedKey() !== key) return
      // The metadata the plaintext was read with; an edit is checked against this version.
      setSelected({ key, entry: withoutValue(full.entry), references: full.references })
      if (then === 'edit') {
        setDraftValue(full.entry.value)
        setDraftType(typeOrString(full.entry.valueType))
        setDraftVersion(full.entry.version)
        setMode('edit')
      } else {
        setRevealed({ value: full.entry.value, version: full.entry.version })
        setClock(Date.now())
        setRevealDeadline(Date.now() + REVEAL_SECONDS * 1000)
        setMode('revealed')
      }
    } catch (err) {
      if (token !== revealToken) return
      console.error('Failed to reveal kv entry', err)
      setError(`Unable to reveal ${key}: ${describeError(err)}`)
    } finally {
      if (token === revealToken) setRevealing(false)
    }
  }

  // Revealed → editable, against the version the shown value was read at.
  const editRevealed = () => {
    const shown = revealed()
    const current = entry()
    if (!shown || !current) return
    setDraftValue(shown.value)
    setDraftType(typeOrString(current.valueType))
    setDraftVersion(shown.version)
    setRevealed(null)
    setMode('edit')
  }

  // A new value without reading the old one, checked against the metadata version.
  const startReplace = () => {
    const current = entry()
    if (!current) return
    hide()
    setDraftValue('')
    setDraftType(typeOrString(current.valueType))
    setDraftVersion(current.version)
    setMode('replace')
  }

  const saveDraft = async () => {
    const key = selectedKey()
    if (key === null) return
    setBusy(true)
    setStatus(null)
    setError(null)
    try {
      const response = await managerClient.putKv({
        key,
        value: draftValue(),
        valueType: draftType(),
        expectedVersion: draftVersion(),
      })
      const version = response.entry?.version
      hide()
      setStatus(`Saved ${key}${version !== undefined ? ` (now v${version})` : ''}`)
      await refresh()
    } catch (err) {
      console.error('Failed to save kv entry', err)
      setError(
        isVersionConflict(err)
          ? `${key} changed since you opened it (${describeError(err)}). Your draft is kept; cancel and start again from the current version to overwrite it.`
          : `Unable to save ${key}: ${describeError(err)}`,
      )
    } finally {
      setBusy(false)
    }
  }

  const create = async () => {
    const key = draftKey().trim()
    setStatus(null)
    setError(null)
    if (!key) {
      setError('Key is required')
      return
    }
    setBusy(true)
    try {
      // `expectedVersion: 0` creates only. Values are hidden now, so silently
      // writing over a key whose value you never saw is the mistake to stop.
      await managerClient.putKv({ key, value: draftValue(), valueType: draftType(), expectedVersion: 0n })
      setStatus(`Created ${key}`)
      setDraftKey('')
      setDraftValue('')
      await refresh()
      select(key)
    } catch (err) {
      console.error('Failed to create kv entry', err)
      setError(
        isVersionConflict(err)
          ? `${key} already exists. Select it in the list to reveal, edit or replace its value.`
          : `Unable to create ${key}: ${describeError(err)}`,
      )
    } finally {
      setBusy(false)
    }
  }

  const handleDelete = async () => {
    const current = entry()
    const key = selectedKey()
    if (!current || key === null) return
    setStatus(null)
    setError(null)
    try {
      await managerClient.deleteKv({ key, expectedVersion: current.version })
      setStatus(`Deleted ${key}`)
      select(null)
      await refresh()
    } catch (err) {
      console.error('Failed to delete kv entry', err)
      setError(`Unable to delete ${key}: ${describeError(err)}`)
    }
  }

  const handlePatch = async () => {
    const current = entry()
    const key = selectedKey()
    if (!current || key === null) return
    setStatus(null)
    setError(null)
    try {
      const response = await managerClient.patchKv({
        key,
        patchJson: patchValue(),
        expectedVersion: current.version,
      })
      const version = response.entry?.version
      setStatus(`Patched ${key}${version !== undefined ? ` (now v${version})` : ''}`)
      setPatchValue('')
      await refresh()
    } catch (err) {
      console.error('Failed to patch kv entry', err)
      setError(`Unable to patch ${key}: ${describeError(err)}`)
    }
  }

  // A segment renders its own name; an empty one (from a leading, trailing or
  // doubled slash in the key) has nothing to show, so label it explicitly.
  const segmentLabel = (name: string) =>
    name ? (
      <span class="truncate font-mono text-xs">{name}</span>
    ) : (
      <span class="truncate font-mono text-xs italic text-faint">(empty)</span>
    )

  const renderTree = (node: TreeNode, depth: number) => (
    <ul class={cx(depth > 0 && 'ml-3 border-l border-line pl-2')}>
      <For each={node.children}>
        {(child) => {
          const isSelected = () => Boolean(child.entry) && selectedKey() === child.entry!.key
          const rowClass = 'flex w-full items-center gap-1.5 rounded-md px-2 py-1 text-left text-sm'
          return (
            <li>
              {/* Only nodes that hold a value are clickable. A node can hold a
                  value *and* have children (`config/db` alongside
                  `config/db/host`), so the value-type label — not the icon —
                  is what marks a row as openable. Rows carry the key and
                  type only: the listing has no values. */}
              <Show
                when={child.entry}
                fallback={
                  <div class={cx(rowClass, 'text-muted')} title={child.path}>
                    <Icon name="folder" size={13} class="text-faint" />
                    {segmentLabel(child.name)}
                  </div>
                }
              >
                <button
                  type="button"
                  title={child.entry!.key}
                  aria-current={isSelected() ? 'true' : undefined}
                  class={cx(
                    rowClass,
                    'transition-colors',
                    isSelected() ? 'bg-brass/10 font-medium text-ink' : 'text-muted hover:bg-surface2 hover:text-ink',
                  )}
                  onClick={() => select(child.entry!.key)}
                >
                  <Icon
                    name={child.children.length > 0 ? 'folder' : 'file'}
                    size={13}
                    class={cx(isSelected() ? 'text-brass' : 'text-faint')}
                  />
                  {segmentLabel(child.name)}
                  <span class="ml-auto shrink-0 text-2xs text-faint">{valueTypeLabel(child.entry!.valueType)}</span>
                </button>
              </Show>
              <Show when={child.children.length > 0}>{renderTree(child, depth + 1)}</Show>
            </li>
          )
        }}
      </For>
    </ul>
  )

  const writeTitle = () => (canWriteKey() ? undefined : 'Needs kv:write on this key')

  // The masked value and the ways to see, copy or change it.
  const valueSection = () => (
    <section class="space-y-2" aria-labelledby="kv-value">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <h4 id="kv-value" class="eyebrow">
          {mode() === 'replace' ? 'New value' : 'Value'}
        </h4>
        <Show when={mode() === 'revealed'}>
          <span class="inline-flex items-center gap-1 text-2xs tabular-nums text-faint">
            <Icon name="clock" size={11} />
            Hides in {secondsLeft()}s
          </span>
        </Show>
      </div>

      <Switch>
        <Match when={mode() === 'revealed' && revealed()}>
          {(shown) => (
            <>
              <RevealedValue value={shown().value} />
              <div class="flex flex-wrap items-center gap-2">
                <Button size="sm" onClick={hide}>
                  <EyeOffIcon />
                  Hide
                </Button>
                <FetchCopyButton
                  label="Copy"
                  fetchText={async () => shown().value}
                  onError={(err) => setError(`Unable to copy ${selectedKey()}: ${describeError(err)}`)}
                />
                <Button size="sm" onClick={editRevealed} disabled={!canWriteKey()} title={writeTitle()}>
                  Edit
                </Button>
              </div>
            </>
          )}
        </Match>

        <Match when={mode() === 'edit' || mode() === 'replace'}>
          <div class="space-y-3">
            <Show when={mode() === 'replace'}>
              <p class="text-xs text-muted">
                The current value stays hidden and is overwritten when you save.
              </p>
            </Show>
            <Field label="Type" class="max-w-[10rem]">
              <TypeSelect value={draftType()} onChange={setDraftType} />
            </Field>
            <textarea
              class={cx(textareaClass, 'min-h-[140px]')}
              aria-label={mode() === 'replace' ? 'New value' : 'Value'}
              placeholder={mode() === 'replace' ? 'New value' : undefined}
              spellcheck={false}
              autocomplete="off"
              value={draftValue()}
              onInput={(event) => setDraftValue(event.currentTarget.value)}
            />
            <div class="flex flex-wrap items-center gap-2">
              <Button variant="primary" size="md" onClick={() => void saveDraft()} disabled={busy() || !canWriteKey()} title={writeTitle()}>
                {mode() === 'replace' ? 'Replace value' : 'Save changes'}
              </Button>
              <Button variant="ghost" size="md" onClick={hide} disabled={busy()}>
                Cancel
              </Button>
              <Badge tone="neutral" mono title="The version this save is checked against">
                v{String(draftVersion() ?? '—')}
              </Badge>
              <span class="text-xs text-faint">Saves are version-checked to prevent lost updates.</span>
            </div>
          </div>
        </Match>

        <Match when={mode() === 'hidden'}>
          <HiddenValue />
          <div class="flex flex-wrap items-center gap-2">
            <Button
              size="sm"
              variant="accent"
              icon="eye"
              onClick={() => void reveal('revealed')}
              disabled={revealing() || !canReadKey()}
              title={`Show the value for ${REVEAL_SECONDS}s`}
            >
              {revealing() ? 'Revealing…' : 'Reveal'}
            </Button>
            <FetchCopyButton
              fetchText={async () => (await fetchFull(selectedKey()!)).entry.value}
              onError={(err) => setError(`Unable to copy ${selectedKey()}: ${describeError(err)}`)}
              disabled={!canReadKey()}
              title="Copy the value without showing it"
            />
            <span class="mx-0.5 hidden h-5 w-px bg-line sm:block" aria-hidden="true" />
            <Button size="sm" onClick={() => void reveal('edit')} disabled={revealing() || !canWriteKey()} title={writeTitle()}>
              Reveal to edit
            </Button>
            <Button
              size="sm"
              onClick={startReplace}
              disabled={!canWriteKey()}
              title={writeTitle() ?? 'Write a new value without showing the current one'}
            >
              Replace value
            </Button>
          </div>
        </Match>
      </Switch>
    </section>
  )

  const entryPanel = () => (
    <div class="space-y-5">
      <Show
        when={entry()}
        fallback={
          <Show when={selected()?.missing} fallback={<Spinner />}>
            <EmptyState
              icon="alertTriangle"
              title="This key no longer exists"
              body="It was deleted after the list loaded. Jobs that still read it are listed below."
              class="py-6"
            />
          </Show>
        }
      >
        {(meta) => (
          <>
            <DefList
              class="grid-cols-2 lg:grid-cols-4"
              items={[
                ['Type', <Badge tone="neutral" mono>{valueTypeLabel(meta().valueType)}</Badge>],
                ['Version', <span class="font-mono">v{String(meta().version)}</span>],
                ['Updated', <RelativeTime value={meta().updatedAt} />],
                ['Created', <RelativeTime value={meta().createdAt} />],
              ]}
            />

            {valueSection()}

            {/* A patch is applied by the manager; it needs neither the value nor a reveal. */}
            <Show when={meta().valueType === KvValueType.JSON && mode() === 'hidden'}>
              <div class="space-y-3 rounded-md border border-line bg-surface2/40 p-3">
                <p class="eyebrow">JSON Patch (RFC 6902)</p>
                <textarea
                  class={cx(textareaClass, 'min-h-[90px]')}
                  aria-label="JSON Patch"
                  spellcheck={false}
                  value={patchValue()}
                  onInput={(event) => setPatchValue(event.currentTarget.value)}
                  placeholder='[{"op":"replace","path":"/name","value":"new"}]'
                />
                <Button onClick={() => void handlePatch()} disabled={!canWriteKey()} title={writeTitle()}>
                  Apply patch
                </Button>
              </div>
            </Show>
          </>
        )}
      </Show>

      <KvUsedBy
        references={selected()?.references ?? null}
        error={selected()?.referencesError}
        class={cx(entry() && 'border-t border-line pt-4')}
      />
    </div>
  )

  const newEntryForm = () => (
    <div class="space-y-4">
      <div class="grid gap-4 md:grid-cols-[1fr_160px]">
        <Field label="Key">
          <input
            class={cx(inputClass, 'font-mono')}
            value={draftKey()}
            onInput={(event) => setDraftKey(event.currentTarget.value)}
            placeholder="config/service/name"
            autocomplete="off"
          />
        </Field>
        <Field label="Type">
          <TypeSelect value={draftType()} onChange={setDraftType} />
        </Field>
      </div>

      <Field label="Value">
        <textarea
          class={cx(textareaClass, 'min-h-[140px]')}
          spellcheck={false}
          autocomplete="off"
          value={draftValue()}
          onInput={(event) => setDraftValue(event.currentTarget.value)}
        />
      </Field>

      <div class="flex flex-wrap items-center gap-3">
        <Button
          variant="primary"
          size="md"
          onClick={() => void create()}
          disabled={busy() || !allows('kv', 'write', draftKey().trim() ? { path: draftKey().trim() } : { any: true })}
        >
          Create entry
        </Button>
        <span class="text-xs text-faint">Fails if the key already exists; select it in the list to change it.</span>
      </div>
    </div>
  )

  return (
    <div class="space-y-3">
      {/* How these values are held at rest. Hidden without operator:read. */}
      <KeyringStrip />

      {/* Feedback sits above both panes: loading errors belong to the key list,
          save/delete results to the editor, and burying either inside the
          editor card pushed the form around as they appeared. */}
      <div aria-live="polite" class="empty:hidden space-y-2">
        <Show when={status()}>
          <Note tone="ok" onDismiss={() => setStatus(null)}>{status()}</Note>
        </Show>
        <Show when={error()}>
          <Note tone="danger" onDismiss={() => setError(null)}>{error()}</Note>
        </Show>
      </div>

      <div class="grid items-stretch gap-4 lg:grid-cols-[300px_1fr]">
        <SectionCard
          title="Keys"
          actions={
            <>
              <Button size="xs" icon="plus" onClick={() => select(null)}>New</Button>
              <Button size="xs" icon="refresh" onClick={() => void refresh()} aria-label="Refresh keys" />
            </>
          }
          flush
          class="flex flex-col"
          contentClass="min-h-0 flex-1"
        >
          <Show when={!loading()} fallback={<div class="p-4"><Spinner /></div>}>
            <div class="h-full min-h-[16rem] max-h-[640px] overflow-auto p-2">
              {entries().length > 0 ? (
                renderTree(tree(), 0)
              ) : (
                <EmptyState icon="database" title="No keys stored yet" body="Create your first entry with the editor." class="py-6" />
              )}
            </div>
          </Show>
        </SectionCard>

        <SectionCard
          title={selected() ? 'Entry' : 'New entry'}
          description={
            selected() ? (
              <span class="inline-flex min-w-0 flex-wrap items-center gap-2">
                <span class="break-all font-mono">{selectedKey()}</span>
                {/* How the row is held at rest (docs/kv-encryption.md). The value itself is only read on Reveal. */}
                <Show when={entry()}>
                  <StatusBadge
                    status={entry()!.encrypted ? 'encrypted at rest' : 'plaintext at rest'}
                    tone={entry()!.encrypted ? 'ok' : 'warn'}
                    title={
                      entry()!.encrypted
                        ? 'Stored encrypted under the KV keyring'
                        : 'Stored as a plaintext row (written before encryption was on, or with encryption disabled); `legion keyring rewrap` converts it'
                    }
                  />
                </Show>
              </span>
            ) : (
              'Create a key/value entry.'
            )
          }
          actions={
            <Show when={entry()}>
              <ConfirmButton
                label="Delete"
                confirmLabel={
                  referencingJobs() > 0
                    ? `${referencingJobs()} job${referencingJobs() === 1 ? ' reads' : 's read'} this key — delete?`
                    : undefined
                }
                title={
                  referencingJobs() > 0
                    ? 'Jobs that read a deleted key act on their onDelete policy; by default they are marked failed'
                    : writeTitle()
                }
                size="xs"
                onConfirm={() => void handleDelete()}
                disabled={!canWriteKey()}
              />
            </Show>
          }
        >
          <Show when={selected()} fallback={newEntryForm()}>
            {entryPanel()}
          </Show>
        </SectionCard>
      </div>
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function KeyValue() {
  return (
    <AccessGate resource="kv">
      <KeyValueContent />
    </AccessGate>
  )
}
