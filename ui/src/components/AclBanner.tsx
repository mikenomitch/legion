import { A } from '@solidjs/router'
import { createSignal, Show } from 'solid-js'
import { managerIdentity } from '../utils/connection'
import { Icon } from './ui'

const DISMISS_KEY = 'legion-acl-warning-dismissed'
const LOGIN_DISMISS_KEY = 'legion-acl-login-prompt-dismissed'

const bannerClass = (tone: 'warn' | 'info') =>
  tone === 'warn'
    ? 'flex items-center justify-between gap-3 border-b border-amber-500/25 bg-amber-500/10 px-4 py-2 text-sm text-amber-800 dark:text-amber-200 md:px-6'
    : 'flex items-center justify-between gap-3 border-b border-sky-500/25 bg-sky-500/10 px-4 py-2 text-sm text-sky-800 dark:text-sky-200 md:px-6'

const dismissClass = 'shrink-0 rounded-md p-1 opacity-60 transition-opacity hover:opacity-100'

// Light, dismissable warning shown while the manager runs without ACLs (the
// default). Dismissal is remembered in localStorage; the banner also clears
// itself when ACLs are turned on. With ACLs enabled and no credential, a
// session-dismissable prompt links to the Access page instead.
//
// The identity comes from the connection probe (utils/connection) rather than
// its own WhoAmI call — same question, one request.
export function AclBanner() {
  const [dismissed, setDismissed] = createSignal(
    localStorage.getItem(DISMISS_KEY) === 'true',
  )
  const [loginDismissed, setLoginDismissed] = createSignal(
    sessionStorage.getItem(LOGIN_DISMISS_KEY) === 'true',
  )
  const whoami = managerIdentity

  const dismiss = () => {
    localStorage.setItem(DISMISS_KEY, 'true')
    setDismissed(true)
  }

  const dismissLogin = () => {
    sessionStorage.setItem(LOGIN_DISMISS_KEY, 'true')
    setLoginDismissed(true)
  }

  const showWarning = () => !dismissed() && whoami() != null && !whoami()!.aclEnabled
  const showAudit = () => !dismissed() && whoami() != null && whoami()!.aclEnabled && whoami()!.auditOnly
  const showLoginPrompt = () =>
    !loginDismissed() &&
    whoami() != null &&
    whoami()!.aclEnabled &&
    whoami()!.source === 'anonymous'

  return (
    <>
      <Show when={showWarning()}>
        <div class={bannerClass('warn')}>
          <p class="flex min-w-0 items-center gap-2">
            <Icon name="alertTriangle" size={15} class="shrink-0" />
            <span class="min-w-0">
              <span class="font-semibold">ACLs are disabled.</span> Anyone who can reach this manager
              has full access. Set <code class="rounded bg-amber-500/15 px-1 font-mono text-xs">acl.enabled: true</code> in
              the manager config to enforce access control.
            </span>
          </p>
          <button type="button" onClick={dismiss} aria-label="Dismiss ACL warning" class={dismissClass}>
            <Icon name="x" size={14} />
          </button>
        </div>
      </Show>
      <Show when={showAudit()}>
        <div class={bannerClass('info')}>
          <p class="flex min-w-0 items-center gap-2">
            <Icon name="info" size={15} class="shrink-0" />
            <span>
              <span class="font-semibold">ACLs are in audit-only mode.</span> Denials are logged but
              not enforced.
            </span>
          </p>
          <button type="button" onClick={dismiss} aria-label="Dismiss ACL audit notice" class={dismissClass}>
            <Icon name="x" size={14} />
          </button>
        </div>
      </Show>
      <Show when={showLoginPrompt()}>
        <div class={bannerClass('info')}>
          <p class="flex min-w-0 items-center gap-2">
            <Icon name="info" size={15} class="shrink-0" />
            <span>
              <span class="font-semibold">You are browsing anonymously.</span> ACLs are enabled, so
              most pages will be empty or denied.{' '}
              <A href="/access" class="font-semibold underline underline-offset-2">
                Log in
              </A>
            </span>
          </p>
          <button type="button" onClick={dismissLogin} aria-label="Dismiss login prompt" class={dismissClass}>
            <Icon name="x" size={14} />
          </button>
        </div>
      </Show>
    </>
  )
}
