import { useParams } from '@solidjs/router'
import { Show, createMemo, createSignal } from 'solid-js'
import { SandboxedPage } from '../components/apps/SandboxedPage'
import { PageHeader } from '../components/ui'
import { uiExtensions } from '../utils/appViews'

/*
 * An app's sandboxed page in the sidebar's Apps section (`ui.pages`; tier 2;
 * docs/apps.md, "Sandboxed pages"): the app's own HTML, in an opaque-origin
 * iframe, reaching Legion only through the bridge.
 */
export default function AppSandboxPage() {
  const params = useParams()
  const app = createMemo(() => decodeURIComponent(params.app))
  const page = createMemo(() => decodeURIComponent(params.page))
  const entry = createMemo(() => uiExtensions().find((candidate) => candidate.slot === 'nav' && candidate.app === app() && candidate.page === page()))
  const [title, setTitle] = createSignal<string>()
  return (
    <div>
      <PageHeader eyebrow={<>Apps / {app()}</>} title={title() ?? entry()?.title ?? page()} subtitle={entry()?.appDescription || undefined} />
      <Show when={app() && page()} keyed>
        <SandboxedPage app={app()} page={page()} onTitle={setTitle} />
      </Show>
    </div>
  )
}
