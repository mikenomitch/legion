import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import type { SavedStateQuery, StateStoreSchemaObject } from '../gen/legion_pb'

type QueryResult = {
  statementType: string
  columns: readonly string[]
  rows: readonly string[]
  affectedRows: bigint
}

const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'ORDER BY', 'GROUP BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'INNER JOIN',
  'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'PRAGMA', 'COUNT', 'DISTINCT',
] as const

export default function StateExplorer() {
  const [query, setQuery] = createSignal('SELECT name, type FROM sqlite_master ORDER BY name LIMIT 25;')
  const [allowWrite, setAllowWrite] = createSignal(false)
  const [isRunning, setIsRunning] = createSignal(false)
  const [isLoadingSchema, setIsLoadingSchema] = createSignal(false)
  const [result, setResult] = createSignal<QueryResult | null>(null)
  const [schema, setSchema] = createSignal<StateStoreSchemaObject[]>([])
  const [savedQueries, setSavedQueries] = createSignal<SavedStateQuery[]>([])
  const [saveName, setSaveName] = createSignal('')
  const [status, setStatus] = createSignal<string | null>(null)
  const [error, setError] = createSignal<string | null>(null)
  const [showSchema, setShowSchema] = createSignal(true)

  const tokens = createMemo(() => {
    const names = schema().flatMap((object) => [
      object.name,
      ...object.columns.map((column) => `${object.name}.${column.name}`),
      ...object.columns.map((column) => column.name),
    ])
    return [...new Set([...SQL_KEYWORDS, ...names])].sort((a, b) => a.localeCompare(b))
  })

  const loadSchema = async () => {
    setIsLoadingSchema(true)
    try {
      const response = await managerClient.getStateStoreSchema({})
      setSchema(response.objects)
    } catch (err) {
      console.error(err)
      setError('Unable to load the SQLite schema')
    } finally {
      setIsLoadingSchema(false)
    }
  }

  const loadSavedQueries = async () => {
    try {
      const response = await managerClient.listSavedStateQueries({})
      setSavedQueries(response.queries)
    } catch (err) {
      console.error(err)
      setError('Unable to load saved queries')
    }
  }

  createEffect(() => {
    void loadSchema()
    void loadSavedQueries()
  })

  const runQuery = async () => {
    setError(null)
    setStatus(null)
    setIsRunning(true)
    try {
      const response = await managerClient.queryStateStore({
        sql: query(),
        allowWrite: allowWrite(),
      })
      setResult({
        statementType: response.statementType,
        columns: response.columns,
        rows: response.rows.map((row) => row.json),
        affectedRows: response.affectedRows,
      })
      setStatus(`Executed ${response.statementType} successfully`)
      await loadSchema()
    } catch (err) {
      console.error(err)
      setError('Query failed. Check SQL and write permissions.')
    } finally {
      setIsRunning(false)
    }
  }

  const saveQuery = async () => {
    setError(null)
    setStatus(null)
    if (!saveName().trim()) {
      setError('Enter a query name before saving')
      return
    }
    try {
      await managerClient.upsertSavedStateQuery({ name: saveName().trim(), sql: query() })
      setStatus(`Saved query: ${saveName().trim()}`)
      await loadSavedQueries()
    } catch (err) {
      console.error(err)
      setError('Unable to save query')
    }
  }

  const deleteQuery = async (name: string) => {
    setError(null)
    setStatus(null)
    try {
      await managerClient.deleteSavedStateQuery({ name })
      setStatus(`Deleted query: ${name}`)
      await loadSavedQueries()
    } catch (err) {
      console.error(err)
      setError('Unable to delete query')
    }
  }

  return (
    <div class="space-y-6">
      <section class="rounded border border-neutral-200 bg-white/90 p-5 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p class="text-xs uppercase tracking-[0.18em] text-neutral-500 dark:text-neutral-400">Stare Explorer</p>
            <h2 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-100">SQLite State Explorer</h2>
            <p class="text-sm text-neutral-600 dark:text-neutral-300">Run read queries safely by default, and opt into writes only when needed.</p>
          </div>
          <button type="button" class="rounded border border-neutral-300 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] dark:border-neutral-700" onClick={() => { void loadSchema(); void loadSavedQueries() }}>
            Refresh
          </button>
        </div>

        <div class="mt-4 grid gap-3">
          <textarea
            class="min-h-[180px] w-full rounded border border-neutral-300 bg-white px-3 py-2 font-mono text-sm shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950"
            value={query()}
            onInput={(event) => setQuery(event.currentTarget.value)}
            placeholder="SELECT * FROM job LIMIT 25;"
          />
          <div class="flex flex-wrap gap-2">
            <For each={tokens().slice(0, 30)}>
              {(token) => (
                <button type="button" class="rounded-full border border-neutral-300 px-2 py-0.5 text-xs dark:border-neutral-700" onClick={() => setQuery((value) => `${value} ${token}`.trimStart())}>
                  {token}
                </button>
              )}
            </For>
          </div>

          <div class="flex flex-wrap items-center gap-3">
            <label class="inline-flex items-center gap-2 text-sm font-medium text-amber-600 dark:text-amber-300">
              <input type="checkbox" checked={allowWrite()} onChange={(event) => setAllowWrite(event.currentTarget.checked)} />
              Allow write queries
            </label>
            <button
              type="button"
              class="rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900"
              onClick={() => void runQuery()}
              disabled={isRunning()}
            >
              {isRunning() ? 'Running…' : 'Run Query'}
            </button>
          </div>
        </div>
      </section>

      <section class="grid gap-6 xl:grid-cols-[340px_1fr]">
        <div class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
          <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">Saved Queries</h3>
          <div class="space-y-2">
            <input class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-950" value={saveName()} onInput={(event) => setSaveName(event.currentTarget.value)} placeholder="weekly rollout report" />
            <button type="button" class="w-full rounded border border-neutral-300 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] dark:border-neutral-700" onClick={() => void saveQuery()}>
              Save / Overwrite
            </button>
          </div>
          <div class="max-h-[360px] space-y-2 overflow-auto pr-1">
            <For each={savedQueries()}>
              {(saved) => (
                <article class="rounded border border-neutral-200 p-2 text-sm dark:border-neutral-800">
                  <div class="flex items-center justify-between gap-2">
                    <button type="button" class="text-left font-semibold text-neutral-800 dark:text-neutral-100" onClick={() => { setSaveName(saved.name); setQuery(saved.sql) }}>
                      {saved.name}
                    </button>
                    <button type="button" class="text-xs text-red-600 dark:text-red-300" onClick={() => void deleteQuery(saved.name)}>Delete</button>
                  </div>
                  <p class="mt-1 line-clamp-2 font-mono text-xs text-neutral-500 dark:text-neutral-400">{saved.sql}</p>
                </article>
              )}
            </For>
            <Show when={savedQueries().length === 0}>
              <p class="text-sm text-neutral-500">No saved queries yet.</p>
            </Show>
          </div>
        </div>

        <div class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">Results & Schema</h3>
            <button type="button" class="text-xs underline" onClick={() => setShowSchema((value) => !value)}>{showSchema() ? 'Hide schema' : 'Show schema'}</button>
          </div>

          <Show when={status()}><p class="rounded border border-green-500/60 bg-green-50 px-3 py-2 text-sm text-green-700 dark:bg-green-900/30 dark:text-green-200">{status()}</p></Show>
          <Show when={error()}><p class="rounded border border-red-500/60 bg-red-50 px-3 py-2 text-sm text-red-700 dark:bg-red-900/30 dark:text-red-200">{error()}</p></Show>

          <Show when={result()}>
            {(queryResult) => (
              <div class="space-y-2">
                <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">{queryResult().statementType} · affected {queryResult().affectedRows.toString()}</p>
                <div class="max-h-[260px] overflow-auto rounded border border-neutral-200 dark:border-neutral-800">
                  <table class="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
                    <thead class="bg-neutral-100/80 dark:bg-neutral-900/80">
                      <tr>
                        <For each={queryResult().columns}>{(column) => <th class="px-3 py-2 text-left font-semibold">{column}</th>}</For>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-neutral-200 dark:divide-neutral-800">
                      <For each={queryResult().rows}>
                        {(row) => (
                          <tr>
                            <td colspan={Math.max(1, queryResult().columns.length)} class="px-3 py-2 font-mono text-xs text-neutral-600 dark:text-neutral-300">{row}</td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </Show>

          <Show when={showSchema()}>
            <div class="max-h-[360px] space-y-3 overflow-auto rounded border border-neutral-200 p-3 dark:border-neutral-800">
              <Show when={!isLoadingSchema()} fallback={<p class="text-sm text-neutral-500">Loading schema…</p>}>
                <For each={schema()}>
                  {(object) => (
                    <article class="rounded border border-neutral-200 p-3 dark:border-neutral-800">
                      <h4 class="text-sm font-semibold">{object.kind}: {object.name}</h4>
                      <p class="mt-1 text-xs text-neutral-500">{object.createSql}</p>
                      <ul class="mt-2 space-y-1 text-xs text-neutral-600 dark:text-neutral-300">
                        <For each={object.columns}>
                          {(column) => (
                            <li>
                              {column.name} <span class="text-neutral-400">{column.dataType}</span>
                              <Show when={column.primaryKey}><span class="ml-1 rounded bg-neutral-200 px-1 dark:bg-neutral-700">PK</span></Show>
                              <Show when={column.notNull}><span class="ml-1 rounded bg-neutral-200 px-1 dark:bg-neutral-700">NOT NULL</span></Show>
                            </li>
                          )}
                        </For>
                      </ul>
                    </article>
                  )}
                </For>
              </Show>
            </div>
          </Show>
        </div>
      </section>
    </div>
  )
}
