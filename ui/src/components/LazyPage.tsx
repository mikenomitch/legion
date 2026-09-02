import { Show, createResource, type Component, type JSX } from 'solid-js'
import { Dynamic } from 'solid-js/web'
import { PageSkeleton } from './ui'

/*
 * Route-level code splitting without Suspense.
 *
 * Solid's `lazy()` needs a `<Suspense>` above it, and a Suspense boundary
 * around a page also catches every resource *inside* that page — so a polled
 * page would flash its fallback on each refetch. This helper loads the module
 * through an ordinary resource and renders the page only once it is here; the
 * page's own resources never see a boundary.
 *
 * The chunk is cached by the browser, so the second visit is instant; the
 * module promise is memoized too so a fast back-and-forth does not re-import.
 */
type PageModule = { default: Component }

export function lazyPage(load: () => Promise<PageModule>): Component {
  let cached: Promise<PageModule> | undefined
  const loadOnce = () => (cached ??= load())
  return () => {
    const [mod] = createResource(loadOnce)
    return (
      <Show when={mod()} fallback={<PageSkeleton />}>
        {(page) => <Dynamic component={page().default} />}
      </Show>
    )
  }
}

/** The same idea for a heavy component inside a page (the xterm terminal). */
export function lazyComponent<P extends object>(
  load: () => Promise<{ default: Component<P> }>,
  fallback: JSX.Element,
): Component<P> {
  let cached: Promise<{ default: Component<P> }> | undefined
  const loadOnce = () => (cached ??= load())
  return (props: P) => {
    const [mod] = createResource(loadOnce)
    return (
      <Show when={mod()} fallback={fallback}>
        {(component) => <Dynamic component={component().default} {...props} />}
      </Show>
    )
  }
}
