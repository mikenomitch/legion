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

const buildTree = (entries: KvEntry[]): TreeNode => {
  const root: TreeNode = { name: '', path: '', children: [] }
  const nodeMap = new Map<string, TreeNode>()
  nodeMap.set('', root)

  for (const entry of entries) {
    const parts = entry.key.split('/').filter(Boolean)
    let currentPath = ''
    let currentNode = root

    parts.forEach((part, index) => {
      currentPath = currentPath ? `${currentPath}/${part}` : part
      let node = nodeMap.get(currentPath)
      if (!node) {
        node = { name: part, path: currentPath, children: [] }
        nodeMap.set(currentPath, node)
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

  const renderTree = (node: TreeNode, depth: number) => (
    <ul class={cx(depth > 0 && 'ml-3 border-l border-line pl-2')}>
      <For each={node.children}>
        {(child) => (
          <li>
            <button
              type="button"
              class={cx(
                'flex w-full items-center gap-1.5 rounded-md px-2 py-1 text-left text-sm transition-colors',
                child.entry && selectedKey() === child.path
                  ? 'bg-brass/10 font-medium text-ink'
                  : 'text-muted hover:bg-surface2 hover:text-ink',
              )}
              onClick={() => {
                if (child.entry) {
                  setSelectedKey(child.path)
                }
              }}
            >
              <Icon
                name={child.children.length > 0 ? 'folder' : 'file'}
                size={13}
                class={cx(child.entry && selectedKey() === child.path ? 'text-brass' : 'text-faint')}
              />
              <span class="truncate font-mono text-xs">{child.name}</span>
              <Show when={child.entry && child.children.length === 0}>
                <span class="ml-auto text-2xs text-faint">{valueTypeLabel(child.entry!.valueType)}</span>
              </Show>
            </button>
            <Show when={child.children.length > 0}>{renderTree(child, depth + 1)}</Show>
          </li>
        )}
      </For>
    </ul>
  )

  return (
    <div class="grid items-start gap-4 lg:grid-cols-[300px_1fr]">
      <SectionCard
        title="Keys"
        actions={
          <>
            <Button size="xs" icon="plus" onClick={() => setSelectedKey(null)}>New</Button>
            <Button size="xs" icon="refresh" onClick={() => void refresh()} aria-label="Refresh keys" />
          </>
        }
        flush
      >
        <Show when={!loading()} fallback={<div class="p-4"><Spinner /></div>}>
          <div class="max-h-[540px] overflow-auto p-2">
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
          <Show when={status()}>
            <Note tone="ok" onDismiss={() => setStatus(null)}>{status()}</Note>
          </Show>
          <Show when={error()}>
            <Note tone="danger" onDismiss={() => setError(null)}>{error()}</Note>
          </Show>

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
  )
}
