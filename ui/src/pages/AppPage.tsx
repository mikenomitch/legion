import { useParams } from '@solidjs/router'
import { Show, createMemo, createSignal } from 'solid-js'
import { AppViewPanel } from '../components/apps/AppSlot'
import { PageHeader } from '../components/ui'
import { uiExtensions, viewContext } from '../utils/appViews'

/*
 * An app's page in the sidebar's Apps section (the `nav` slot; docs/apps.md,
 * "Views and actions"). The page is the app's view, drawn by the SPA; it has
 * no context, so it holds views only, never actions.
 */
export default function AppPage() {
  const params = useParams()
  const app = createMemo(() => decodeURIComponent(params.app))
  const view = createMemo(() => decodeURIComponent(params.view))
  const entry = createMemo(() => uiExtensions().find((candidate) => candidate.slot === 'nav' && candidate.app === app() && candidate.view === view()))
  const [title, setTitle] = createSignal<string>()
  return (
    <div>
      <PageHeader
        eyebrow={<>Apps / {app()}</>}
        title={title() ?? entry()?.title ?? view()}
        subtitle={entry()?.appDescription || undefined}
      />
      <Show when={app() && view()} keyed>
        <AppViewPanel app={app()} view={view()} context={viewContext('nav')} refreshSeconds={entry()?.refreshSeconds ?? 0} variant="page" onTitle={setTitle} />
      </Show>
    </div>
  )
}
