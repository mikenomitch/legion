import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import { KvEntry, KvValueType } from '../gen/legion_pb'

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

  const renderTree = (node: TreeNode) => (
    <ul class="space-y-1 pl-4">
      <For each={node.children}>
        {(child) => (
          <li>
            <button
              type="button"
              class={`flex w-full items-center gap-2 rounded px-2 py-1 text-left text-sm transition ${
                child.entry && selectedKey() === child.path
                  ? 'bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900'
                  : 'text-neutral-700 hover:bg-neutral-100 dark:text-neutral-300 dark:hover:bg-neutral-900'
              }`}
              onClick={() => {
                if (child.entry) {
                  setSelectedKey(child.path)
                }
              }}
            >
              <span class="text-xs">{child.children.length > 0 ? '📁' : '📄'}</span>
              <span>{child.name}</span>
            </button>
            <Show when={child.children.length > 0}>{renderTree(child)}</Show>
          </li>
        )}
      </For>
    </ul>
  )

  return (
    <div class="space-y-6">
      <header class="space-y-2">
        <p class="text-sm uppercase tracking-[0.2em] text-neutral-500 dark:text-neutral-400">
          Key/Value
        </p>
        <h1 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-100">
          Key/Value Store
        </h1>
        <p class="max-w-2xl text-sm text-neutral-600 dark:text-neutral-300">
          Browse nested keys, inspect values, and update entries with version-aware saves.
        </p>
      </header>

      <div class="grid gap-6 lg:grid-cols-[280px_1fr]">
        <section class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
          <div class="flex items-center justify-between">
            <h2 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
              Keys
            </h2>
            <button
              type="button"
              class="rounded border border-neutral-300 px-2 py-1 text-xs font-semibold uppercase tracking-[0.08em] text-neutral-700 shadow-sm transition hover:-translate-y-0.5 dark:border-neutral-700 dark:text-neutral-200"
              onClick={() => void refresh()}
            >
              Refresh
            </button>
          </div>
          <Show when={!loading()} fallback={<p class="mt-3 text-sm text-neutral-500">Loading…</p>}>
            <Show when={!error()} fallback={<p class="mt-3 text-sm text-red-500">{error()}</p>}>
              <div class="mt-3 max-h-[480px] overflow-auto pr-2">
                {entries().length > 0 ? (
                  renderTree(tree())
                ) : (
                  <p class="text-sm text-neutral-500">No keys stored yet.</p>
                )}
              </div>
            </Show>
          </Show>
        </section>

        <section class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
                Entry
              </h2>
              <p class="text-sm text-neutral-500 dark:text-neutral-400">
                {selectedEntry() ? 'Editing existing entry' : 'Create a new entry'}
              </p>
            </div>
            <Show when={selectedEntry()}>
              <button
                type="button"
                class="rounded border border-red-500 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-red-600 shadow-sm transition hover:-translate-y-0.5 dark:border-red-400 dark:text-red-300"
                onClick={() => void handleDelete()}
              >
                Delete
              </button>
            </Show>
          </div>

          <Show when={status()}>
            <p class="rounded border border-green-500 bg-green-50 px-3 py-2 text-sm text-green-700 dark:border-green-700 dark:bg-green-900/40 dark:text-green-200">
              {status()}
            </p>
          </Show>
          <Show when={error()}>
            <p class="rounded border border-red-500 bg-red-50 px-3 py-2 text-sm text-red-700 dark:border-red-700 dark:bg-red-900/40 dark:text-red-200">
              {error()}
            </p>
          </Show>

          <div class="grid gap-4 md:grid-cols-[1fr_180px]">
            <label class="flex flex-col gap-2 text-sm font-semibold text-neutral-700 dark:text-neutral-300">
              Key
              <input
                class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-800 shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-200"
                value={editKey()}
                onInput={(event) => setEditKey(event.currentTarget.value)}
                placeholder="config/service/name"
              />
            </label>
            <label class="flex flex-col gap-2 text-sm font-semibold text-neutral-700 dark:text-neutral-300">
              Type
              <select
                class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-800 shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-200"
                value={editType()}
                onChange={(event) => setEditType(Number(event.currentTarget.value))}
              >
                <option value={KvValueType.STRING}>string</option>
                <option value={KvValueType.NUMBER}>number</option>
                <option value={KvValueType.JSON}>json</option>
              </select>
            </label>
          </div>

          <label class="flex flex-col gap-2 text-sm font-semibold text-neutral-700 dark:text-neutral-300">
            Value
            <textarea
              class="min-h-[140px] rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-800 shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-200"
              value={editValue()}
              onInput={(event) => setEditValue(event.currentTarget.value)}
            />
          </label>

          <div class="flex flex-wrap items-center gap-3">
            <button
              type="button"
              class="rounded border border-neutral-300 bg-neutral-900 px-4 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-white shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 dark:border-neutral-700 dark:bg-neutral-100 dark:text-neutral-900 dark:shadow-[3px_3px_0_#0b0d12]"
              onClick={() => void handleSave()}
            >
              Save
            </button>
            <Show when={selectedEntry()}>
              <span class="text-xs text-neutral-500 dark:text-neutral-400">
                Version {selectedEntry()?.version} • {valueTypeLabel(selectedEntry()?.valueType ?? 0)}
              </span>
            </Show>
          </div>

          <Show when={selectedEntry()?.valueType === KvValueType.JSON}>
            <div class="space-y-3 rounded border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-800 dark:bg-neutral-950/60">
              <h3 class="text-xs font-semibold uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
                JSON Patch
              </h3>
              <textarea
                class="min-h-[100px] rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-800 shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-200"
                value={patchValue()}
                onInput={(event) => setPatchValue(event.currentTarget.value)}
                placeholder='[{"op":"replace","path":"/name","value":"new"}]'
              />
              <button
                type="button"
                class="rounded border border-neutral-300 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-neutral-700 shadow-sm transition hover:-translate-y-0.5 dark:border-neutral-700 dark:text-neutral-200"
                onClick={() => void handlePatch()}
              >
                Apply Patch
              </button>
            </div>
          </Show>
        </section>
      </div>
    </div>
  )
}
