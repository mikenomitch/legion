import { A } from '@solidjs/router'
import { createResource, createSignal, Show } from 'solid-js'
import { managerClient } from '../grpcClient'

const DISMISS_KEY = 'legion-acl-warning-dismissed'
const LOGIN_DISMISS_KEY = 'legion-acl-login-prompt-dismissed'

// Light, dismissable warning shown while the manager runs without ACLs (the
// default). Dismissal is remembered in localStorage; the banner also clears
// itself when ACLs are turned on. With ACLs enabled and no credential, a
// session-dismissable prompt links to the Access page instead.
export function AclBanner() {
  const [dismissed, setDismissed] = createSignal(
    localStorage.getItem(DISMISS_KEY) === 'true',
  )
  const [loginDismissed, setLoginDismissed] = createSignal(
    sessionStorage.getItem(LOGIN_DISMISS_KEY) === 'true',
  )
  const [whoami] = createResource(async () => {
    try {
      return await managerClient.whoAmI({})
    } catch {
      return null
    }
  })

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
        <div class="flex items-center justify-between gap-3 border-b border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800 dark:border-amber-800/60 dark:bg-amber-900/20 dark:text-amber-200">
          <p>
            <span class="font-semibold">ACLs are disabled.</span> Anyone who can reach this manager
            has full access. Set <code class="rounded bg-amber-100 px-1 dark:bg-amber-900/40">acl.enabled: true</code> in
            the manager config to enforce access control.
          </p>
          <button
            type="button"
            onClick={dismiss}
            aria-label="Dismiss ACL warning"
            class="shrink-0 rounded px-2 py-0.5 text-amber-700 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900/40"
          >
            ✕
          </button>
        </div>
      </Show>
      <Show when={showAudit()}>
        <div class="flex items-center justify-between gap-3 border-b border-sky-200 bg-sky-50 px-4 py-2 text-sm text-sky-800 dark:border-sky-800/60 dark:bg-sky-900/20 dark:text-sky-200">
          <p>
            <span class="font-semibold">ACLs are in audit-only mode.</span> Denials are logged but
            not enforced.
          </p>
          <button
            type="button"
            onClick={dismiss}
            aria-label="Dismiss ACL audit notice"
            class="shrink-0 rounded px-2 py-0.5 text-sky-700 hover:bg-sky-100 dark:text-sky-300 dark:hover:bg-sky-900/40"
          >
            ✕
          </button>
        </div>
      </Show>
      <Show when={showLoginPrompt()}>
        <div class="flex items-center justify-between gap-3 border-b border-sky-200 bg-sky-50 px-4 py-2 text-sm text-sky-800 dark:border-sky-800/60 dark:bg-sky-900/20 dark:text-sky-200">
          <p>
            <span class="font-semibold">You are browsing anonymously.</span> ACLs are enabled, so
            most pages will be empty or denied.{' '}
            <A href="/access" class="font-semibold underline">
              Log in
            </A>
          </p>
          <button
            type="button"
            onClick={dismissLogin}
            aria-label="Dismiss login prompt"
            class="shrink-0 rounded px-2 py-0.5 text-sky-700 hover:bg-sky-100 dark:text-sky-300 dark:hover:bg-sky-900/40"
          >
            ✕
          </button>
        </div>
      </Show>
    </>
  )
}
