import { createEffect, createSignal, onCleanup, type Accessor } from 'solid-js'

// Returns a signal that trails `source` by `delayMs`. Used to debounce search
// inputs so list pages don't refetch on every keystroke.
export function debounced<T>(source: Accessor<T>, delayMs = 250): Accessor<T> {
  const [value, setValue] = createSignal<T>(source())
  createEffect(() => {
    const next = source()
    const timer = setTimeout(() => setValue(() => next), delayMs)
    onCleanup(() => clearTimeout(timer))
  })
  return value
}
