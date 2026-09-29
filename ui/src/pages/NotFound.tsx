import { A, useLocation } from '@solidjs/router'
import { JUMP_MODIFIER_LABEL } from '../utils/jumpHints'
import { Icon, Kbd, linkClass } from '../components/ui'

// A wrong URL used to land silently on the overview, which reads as "the
// thing I asked for is the dashboard". Say what happened instead.
export default function NotFound() {
  const location = useLocation()
  return (
    <div class="flex justify-center py-12">
      <div class="w-full max-w-md rounded-lg border border-line bg-surface p-6 text-center shadow-card">
        <span class="mx-auto flex h-10 w-10 items-center justify-center rounded-full border border-line bg-surface2 text-faint">
          <Icon name="compass" size={19} />
        </span>
        <h2 class="mt-3 font-display text-lg font-semibold text-ink">There is no page here</h2>
        <p class="mx-auto mt-1.5 max-w-sm break-all font-mono text-xs text-muted">{location.pathname}</p>
        <p class="mx-auto mt-3 max-w-sm text-sm text-muted">
          Check the link, or press <Kbd>{JUMP_MODIFIER_LABEL}K</Kbd> and search for what you were after.
        </p>
        <div class="mt-5 flex items-center justify-center gap-4">
          <A
            href="/"
            class="inline-flex items-center gap-2 rounded-md border border-transparent bg-brass px-3.5 py-2 text-sm font-semibold text-canvas transition-colors hover:bg-brass-strong"
          >
            <Icon name="gauge" size={15} />
            Back to overview
          </A>
          <A href="/jobs" class={linkClass}>
            Jobs
          </A>
        </div>
      </div>
    </div>
  )
}
