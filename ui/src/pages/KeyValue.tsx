import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import { KvEntry, KvValueType } from '../gen/legion_pb'
import { Badge, Button, ConfirmButton, EmptyState, Field, Icon, Note, SectionCard, Spinner, cx, inputClass, selectClass, textareaClass } from '../components/ui'

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

export default function KeyValue() {
  const [entries, setEntries] = createSignal<KvEntry[]>([])
  const [loading, setLoading] = createSignal(false)
  const [error, setError] = createSignal<string | null>(null)
  const [selectedKey, setSelectedKey] = createSignal<string | null>(null)
  const [editKey, setEditKey] = createSignal('')
  const [editValue, setEditValue] = createSignal('')
  const [editType, setEditType] = createSignal<KvValueType>(KvValueType.STRING)
  const [patchValue, setPatchValue] = createSignal('')
  const [status, setStatus] = createSignal<string | null>(null)

  const selectedEntry = createMemo(() => {
    const key = selectedKey()
    if (!key) return undefined
    return entries().find((entry) => entry.key === key)
  })

  const tree = createMemo(() => buildTree(entries()))

  const refresh = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await managerClient.listKv({ prefix: '' })
      setEntries(response.entries)
    } catch (err) {
      console.error('Failed to load kv entries', err)
      setError('Unable to load key/value entries')
    } finally {
      setLoading(false)
    }
  }

  createEffect(() => {
    void refresh()
  })

  createEffect(() => {
    const entry = selectedEntry()
    if (entry) {
      setEditKey(entry.key)
      setEditValue(entry.value)
      setEditType(entry.valueType)
    } else {
      setEditKey('')
      setEditValue('')
      setEditType(KvValueType.STRING)
    }
  })

  const handleSave = async () => {
    setStatus(null)
    setError(null)
    if (!editKey().trim()) {
      setError('Key is required')
      return
    }
    try {
      const entry = selectedEntry()
      const expectedVersion =
        entry && entry.key === editKey() ? entry.version : undefined
      const response = await managerClient.putKv({
        key: editKey().trim(),
        value: editValue(),
        valueType: editType(),
        expectedVersion,
      })
      setStatus(`Saved ${response.entry?.key ?? editKey()}`)
      await refresh()
      setSelectedKey(response.entry?.key ?? editKey())
    } catch (err) {
      console.error('Failed to save kv entry', err)
      setError('Unable to save key/value entry')
    }
  }

  const handleDelete = async () => {
    const entry = selectedEntry()
    if (!entry) return
    setStatus(null)
    setError(null)
    try {
      await managerClient.deleteKv({ key: entry.key, expectedVersion: entry.version })
      setStatus(`Deleted ${entry.key}`)
      setSelectedKey(null)
      await refresh()
    } catch (err) {
      console.error('Failed to delete kv entry', err)
      setError('Unable to delete key/value entry')
    }
  }

  const handlePatch = async () => {
    const entry = selectedEntry()
    if (!entry) return
    setStatus(null)
    setError(null)
    try {
      const response = await managerClient.patchKv({
        key: entry.key,
        patchJson: patchValue(),
        expectedVersion: entry.version,
      })
      setStatus(`Patched ${response.entry?.key ?? entry.key}`)
      setPatchValue('')
      await refresh()
    } catch (err) {
      console.error('Failed to patch kv entry', err)
      setError('Unable to patch key/value entry')
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
                  is what marks a row as openable. */}
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
                  class={cx(
                    rowClass,
                    'transition-colors',
                    isSelected() ? 'bg-brass/10 font-medium text-ink' : 'text-muted hover:bg-surface2 hover:text-ink',
                  )}
                  onClick={() => setSelectedKey(child.entry!.key)}
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

  return (
    <div class="space-y-3">
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
              <Button size="xs" icon="plus" onClick={() => setSelectedKey(null)}>New</Button>
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
          title={selectedEntry() ? 'Edit entry' : 'New entry'}
          description={selectedEntry() ? <span class="font-mono">{selectedEntry()!.key}</span> : 'Create a key/value entry.'}
          actions={
            <Show when={selectedEntry()}>
              <ConfirmButton label="Delete" size="xs" onConfirm={() => void handleDelete()} />
            </Show>
          }
        >
          <div class="space-y-4">
            <div class="grid gap-4 md:grid-cols-[1fr_160px]">
              <Field label="Key">
                <input
                  class={cx(inputClass, 'font-mono')}
                  value={editKey()}
                  onInput={(event) => setEditKey(event.currentTarget.value)}
                  placeholder="config/service/name"
                />
              </Field>
              <Field label="Type">
                <select
                  class={cx(selectClass, "w-full")}
                  value={editType()}
                  onChange={(event) => setEditType(Number(event.currentTarget.value))}
                >
                  <option value={KvValueType.STRING}>string</option>
                  <option value={KvValueType.NUMBER}>number</option>
                  <option value={KvValueType.JSON}>json</option>
                </select>
              </Field>
            </div>

            <Field label="Value">
              <textarea
                class={cx(textareaClass, 'min-h-[140px]')}
                value={editValue()}
                onInput={(event) => setEditValue(event.currentTarget.value)}
              />
            </Field>

            <div class="flex flex-wrap items-center gap-3">
              <Button variant="primary" size="md" onClick={() => void handleSave()}>
                {selectedEntry() ? 'Save changes' : 'Create entry'}
              </Button>
              <Show when={selectedEntry()}>
                <Badge tone="neutral" mono>v{String(selectedEntry()?.version ?? '—')}</Badge>
                <span class="text-xs text-faint">Saves are version-checked to prevent lost updates.</span>
              </Show>
            </div>

            <Show when={selectedEntry()?.valueType === KvValueType.JSON}>
              <div class="space-y-3 rounded-md border border-line bg-surface2/40 p-3">
                <p class="eyebrow">JSON Patch (RFC 6902)</p>
                <textarea
                  class={cx(textareaClass, 'min-h-[90px]')}
                  value={patchValue()}
                  onInput={(event) => setPatchValue(event.currentTarget.value)}
                  placeholder='[{"op":"replace","path":"/name","value":"new"}]'
                />
                <Button onClick={() => void handlePatch()}>Apply patch</Button>
              </div>
            </Show>
          </div>
        </SectionCard>
      </div>
    </div>
  )
}
