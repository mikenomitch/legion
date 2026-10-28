import { For, Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import type { AppViewContext, GetAppViewReply, UiExtension } from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import { errorText, isDenied, parseView, uiExtensions, viewContext, type Slot } from '../../utils/appViews'
import { usePoll } from '../../utils/polling'
import { Button, Note, SectionCard, Spinner } from '../ui'
import { ViewBadge, ViewBlocks } from './ViewBlocks'

/*
 * Slot outlets: where installed apps' views appear (docs/ui-architecture.md,
 * "App extensions"). Each outlet asks the manager which entries the viewer
 * may see here (ListUiExtensions with the page's context, so a panel shows
 * only to viewers holding its declared permission on this job or node), then
 * fetches each view through GetAppView. The browser never talks to an app.
 */

/** How often a view refreshes: the app's `ui.refresh`, at least 5 seconds. */
const refreshMs = (seconds: number) => (seconds > 0 ? Math.max(5, seconds) * 1000 : 0)

/**
 * One app view, fetched and drawn. `card` hides a view the app marks hidden
 * and a view the viewer may not see; `page` says why instead.
 */
export function AppViewPanel(props: {
  app: string
  view: string
  context: Partial<AppViewContext>
  refreshSeconds: number
  variant?: 'card' | 'page'
  onTitle?: (title: string) => void
}) {
  const tick = props.refreshSeconds > 0 ? usePoll(refreshMs(props.refreshSeconds)) : () => 0
  // The fetcher never rejects: an errored Solid resource throws wherever it
  // is read. A failed refresh keeps the last view on screen with the error.
  let last: GetAppViewReply | undefined
  const [state, { refetch }] = createResource(
    () => [props.app, props.view, JSON.stringify(props.context), tick()] as const,
    async ([app, view]): Promise<{ reply?: GetAppViewReply; error?: unknown }> => {
      try {
        last = await managerClient.getAppView({ app, view, context: props.context })
        return { reply: last }
      } catch (error) {
        // A viewer who lost access stops seeing the last answer too.
        if (isDenied(error)) last = undefined
        return { reply: last, error }
      }
    },
  )
  const reply = () => state.latest?.reply
  const failure = () => state.latest?.error
  const document = createMemo(() => {
    const current = reply()
    return current ? parseView(current.viewJson) : undefined
  })
  createEffect(() => {
    const current = reply()
    if (current && props.onTitle) props.onTitle(document()?.title ?? current.title)
  })
  return (
    <Show
      when={reply()}
      fallback={
        <Show when={failure()} fallback={<Spinner label={`Loading ${props.app}…`} />}>
          <Show when={!isDenied(failure()) || props.variant === 'page'}>
            <Note tone={isDenied(failure()) ? 'neutral' : 'warn'}>
              {isDenied(failure()) ? 'You may not see this view.' : `The ${props.app} app could not render this view: ${errorText(failure())}`}
              <Button variant="ghost" size="xs" icon="refresh" class="ml-2" onClick={() => void refetch()}>
                Retry
              </Button>
            </Note>
          </Show>
        </Show>
      }
    >
      <Show when={failure() && !isDenied(failure())}>
        <Note tone="warn" class="mb-3">
          Showing the last answer: {errorText(failure())}
        </Note>
      </Show>
      <Show when={document()}>
        {(doc) => (
          <Show
            when={!doc().hidden}
            fallback={
              <Show when={props.variant === 'page'}>
                <p class="text-sm text-muted">The {props.app} app has nothing to show here.</p>
              </Show>
            }
          >
            <ViewBlocks
              app={props.app}
              document={doc()}
              actions={reply()?.actions ?? []}
              context={props.context}
              onAction={() => void refetch()}
            />
          </Show>
        )}
      </Show>
    </Show>
  )
}

/** The entries of `slot` the viewer may see on `context`. */
function useSlotEntries(slot: Slot, context: () => Partial<AppViewContext> | undefined) {
  const [entries] = createResource(
    () => [slot, JSON.stringify(context() ?? {})] as const,
    async (): Promise<UiExtension[]> => {
      try {
        const ctx = context()
        return (await managerClient.listUiExtensions({ slot, context: ctx })).extensions
      } catch {
        return []
      }
    },
  )
  return () => entries() ?? []
}

/** One app's panel: a card titled by the entry, the view inside. */
function AppCard(props: { entry: UiExtension; context: Partial<AppViewContext> }) {
  const [title, setTitle] = createSignal(props.entry.title)
  return (
    <SectionCard title={title()} description={`From the ${props.entry.app} app`}>
      <AppViewPanel
        app={props.entry.app}
        view={props.entry.view}
        context={props.context}
        refreshSeconds={props.entry.refreshSeconds}
        onTitle={setTitle}
      />
    </SectionCard>
  )
}

/**
 * The panels apps register on a job, task, or node page, or the cards on the
 * overview. Renders nothing when no app registers anything the viewer may
 * see here.
 */
export function AppPanels(props: {
  slot: Exclude<Slot, 'nav'>
  namespace?: string
  job?: string
  group?: string
  task?: string
  node?: string
}) {
  const context = createMemo(() =>
    props.slot === 'overview'
      ? viewContext('overview')
      : viewContext(props.slot, {
          namespace: props.namespace ?? '',
          job: props.job ?? '',
          group: props.group ?? '',
          task: props.task ?? '',
          node: props.node ?? '',
        }),
  )
  // The overview has no context to filter on: the registry fetched at login
  // already holds what the viewer may see somewhere.
  const listed = useSlotEntries(props.slot, () => (props.slot === 'overview' ? undefined : context()))
  const entries = () =>
    (props.slot === 'overview' ? uiExtensions().filter((entry) => entry.slot === 'overview') : listed()).filter((entry) => entry.view)
  return (
    <For each={entries()}>{(entry) => <AppCard entry={entry} context={context()} />}</For>
  )
}

/** The badges apps put on one task's row. */
export function AppTaskBadges(props: { job: string; task: string; group?: string }) {
  const context = createMemo(() => viewContext('task', { job: props.job, task: props.task, group: props.group ?? '' }))
  const entries = useSlotEntries('task', context)
  return (
    <For each={entries().filter((entry) => entry.badge)}>
      {(entry) => {
        const [reply] = createResource(
          () => [entry.app, entry.badge, JSON.stringify(context())] as const,
          async ([app, view]) => {
            try {
              return await managerClient.getAppView({ app, view, context: context() })
            } catch {
              return undefined
            }
          },
        )
        return (
          <Show when={reply()}>
            {(current) => <ViewBadge app={entry.app} document={parseView(current().viewJson)} />}
          </Show>
        )
      }}
    </For>
  )
}
