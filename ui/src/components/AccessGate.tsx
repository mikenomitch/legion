import { A } from '@solidjs/router'
import { Show, type JSX } from 'solid-js'
import { Icon, linkClass } from './ui'
import type { AclResource } from '../utils/acl'
import { canRead, isAnonymous } from '../utils/acl'
import { managerIdentity } from '../utils/connection'

/*
 * Page-level permission gate: wraps a page's own content and swaps in a
 * clear denial card when the identity lacks `<resource>:read`, with the one
 * action that fixes it (log in / switch identity).
 *
 * IMPORTANT: this must only ever wrap content a page owns. Wrapping the
 * router's outlet (Layout's `props.children`) in any conditional captures
 * the outlet once and freezes navigation — the URL changes while the page
 * does not. Pages conditionally render their own trees all the time; that
 * is the safe place, so that is where this lives.
 */
export function AccessGate(props: { resource: AclResource; children: JSX.Element }) {
  return (
    <Show when={canRead(props.resource)} fallback={<DeniedCard resource={props.resource} />}>
      {props.children}
    </Show>
  )
}

/*
 * For pages that span many resources (the overview), where no single
 * `<resource>:read` decides access: a logged-out visitor to a cluster that
 * enforces ACLs gets the same log-in card as every gated page, while a
 * logged-in identity sees whatever its token can read, card by card.
 */
export function LoginGate(props: { what: string; children: JSX.Element }) {
  return (
    <Show when={!isAnonymous()} fallback={<DeniedCard what={props.what} />}>
      {props.children}
    </Show>
  )
}

// `resource` for a page gated on one permission; `what` (e.g. "the overview")
// for a LoginGate page, which is only ever shown to an anonymous visitor.
const DeniedCard = (props: { resource?: AclResource; what?: string }) => {
  const identity = managerIdentity

  return (
    <div class="flex justify-center py-12">
      <div class="w-full max-w-md rounded-lg border border-line bg-surface p-6 text-center shadow-card">
        <span class="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-brass/10 text-brass-strong">
          <Icon name="shieldCheck" size={19} />
        </span>
        <h2 class="mt-3 font-display text-lg font-semibold text-ink">
          <Show when={isAnonymous()} fallback="You don't have access to this page">
            Log in to view this page
          </Show>
        </h2>
        <p class="mx-auto mt-1.5 max-w-sm text-sm text-muted">
          <Show
            when={isAnonymous()}
            fallback={
              <>
                Your token doesn't grant{' '}
                <code class="font-mono text-ink">{props.resource}:read</code>. Switch to one that
                does, or ask an operator to extend your policy.
              </>
            }
          >
            <Show
              when={props.resource}
              fallback={<>This cluster enforces ACLs, so {props.what} needs a logged-in identity.</>}
            >
              <>
                This cluster enforces ACLs, so{' '}
                <code class="font-mono text-ink">{props.resource}:read</code> needs a logged-in
                identity.
              </>
            </Show>
          </Show>
        </p>
        <div class="mt-5 flex items-center justify-center gap-4">
          {/* A link styled as the primary button: nesting <Button> inside
              <A> is invalid HTML and swallows the click. */}
          <A
            href="/access"
            class="inline-flex items-center gap-2 rounded-md border border-transparent bg-brass px-3.5 py-2 text-sm font-semibold text-canvas transition-colors hover:bg-brass-strong"
          >
            <Icon name="shieldCheck" size={15} />
            <Show when={isAnonymous()} fallback="Switch identity">
              Log in
            </Show>
          </A>
          <Show when={props.resource}>
            <A href="/" class={linkClass}>
              Back to overview
            </A>
          </Show>
        </div>
        <Show when={!isAnonymous() && (identity()?.policies.length ?? 0) > 0}>
          <p class="mt-4 text-2xs text-faint">
            Current policies: {identity()!.policies.join(', ')}
          </p>
        </Show>
      </div>
    </div>
  )
}
