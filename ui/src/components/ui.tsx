import { A } from '@solidjs/router'
import { Show, createSignal, onCleanup, splitProps, type JSX } from 'solid-js'
import { jumpHintsVisible } from '../utils/jumpHints'
import { badgeToneClasses, dotToneClasses, noteToneClasses, statusTone, textToneClasses, type Tone } from '../utils/status'

/*
 * Legion UI primitives. Every page composes these so spacing, color, type and
 * interaction stay consistent. Add here first; reach for bespoke markup last.
 */

export const cx = (...parts: Array<string | false | null | undefined>) => parts.filter(Boolean).join(' ')

// ---------------------------------------------------------------------------
// Icons (Lucide-derived paths, stroke-based, inherit currentColor)
// ---------------------------------------------------------------------------

const ICON_PATHS: Record<string, string> = {
  gauge: 'M15.6 2.7a10 10 0 1 0 5.7 5.7 M12 12l4-6',
  layers: 'M12 2 2 7l10 5 10-5-10-5z M2 17l10 5 10-5 M2 12l10 5 10-5',
  rocket: 'M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0 M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5',
  server: 'M5 2h14a2 2 0 0 1 2 2v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2z M5 14h14a2 2 0 0 1 2 2v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4a2 2 0 0 1 2-2z M6 6h.01 M6 18h.01',
  shieldCheck: 'M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z m-11 -1 2 2 4-4',
  radio: 'M4.9 19.1C1 15.2 1 8.8 4.9 4.9 M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5 M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5 M19.1 4.9C23 8.8 23 15.2 19.1 19.1 M12 12h.01',
  keySquare: 'M12.4 2.7a2.5 2.5 0 0 1 3.4 0l5.5 5.5a2.5 2.5 0 0 1 0 3.4l-3.7 3.7a2.5 2.5 0 0 1-3.4 0L8.7 9.8a2.5 2.5 0 0 1 0-3.4z M14 7l3 3 M9.4 10.6 2 18v4h4l7.4-7.4',
  terminal: 'm4 17 6-6-6-6 M12 19h8',
  database: 'M12 2C8 2 4 3 4 5v14c0 2 4 3 8 3s8-1 8-3V5c0-2-4-3-8-3z M4 5c0 2 4 3 8 3s8-1 8-3 M4 12c0 2 4 3 8 3s8-1 8-3',
  search: 'M11 19a8 8 0 1 0 0-16 8 8 0 0 0 0 16z m10 2 -4.3-4.3',
  sun: 'M12 17a5 5 0 1 0 0-10 5 5 0 0 0 0 10z M12 1v2 M12 21v2 M4.22 4.22l1.42 1.42 M18.36 18.36l1.42 1.42 M1 12h2 M21 12h2 M4.22 19.78l1.42-1.42 M18.36 5.64l1.42-1.42',
  moon: 'M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z',
  menu: 'M4 6h16 M4 12h16 M4 18h16',
  x: 'M18 6 6 18 M6 6l12 12',
  chevronRight: 'm9 18 6-6-6-6',
  chevronLeft: 'm15 18-6-6 6-6',
  copy: 'M8 8h12v12H8z M16 8V6a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h2',
  check: 'M20 6 9 17l-5-5',
  play: 'm6 3 14 9-14 9V3z',
  pause: 'M6 4h4v16H6z M14 4h4v16h-4z',
  trash: 'M3 6h18 M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6 M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2 M10 11v6 M14 11v6',
  refresh: 'M3 12a9 9 0 0 1 15-6.7L21 8 M21 3v5h-5 M21 12a9 9 0 0 1-15 6.7L3 16 M3 21v-5h5',
  plus: 'M12 5v14 M5 12h14',
  folder: 'M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2z',
  file: 'M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7z M14 2v4a2 2 0 0 0 2 2h4',
  alertTriangle: 'm21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z M12 9v4 M12 17h.01',
  info: 'M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z M12 16v-4 M12 8h.01',
  logOut: 'M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4 M16 17l5-5-5-5 M21 12H9',
  boxes: 'M2.97 12.92A2 2 0 0 0 2 14.63v3.24a2 2 0 0 0 .97 1.71l3 1.8a2 2 0 0 0 2.06 0L12 19v-5.5l-5-3-4.03 2.42z M7 16.5l-4.74-2.85 M7 16.5l5-3 M7 16.5v5.17 M12 13.5V19l3.97 2.38a2 2 0 0 0 2.06 0l3-1.8a2 2 0 0 0 .97-1.71v-3.24a2 2 0 0 0-.97-1.71L17 10.5l-5 3z M17 16.5l-5-3 M17 16.5l4.74-2.85 M17 16.5v5.17 M7.97 4.42A2 2 0 0 0 7 6.13v4.37l5 3 5-3V6.13a2 2 0 0 0-.97-1.71l-3-1.8a2 2 0 0 0-2.06 0l-3 1.8z M12 8 7.26 5.15 M12 8l4.74-2.85 M12 13.5V8',
  network: 'M9 2h6v6H9z M16 16h6v6h-6z M2 16h6v6H2z M5 16v-3a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v3 M12 12V8',
  eye: 'M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z',
  zap: 'M13 2 3 14h9l-1 8 10-12h-9l1-8z',
  users: 'M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2 M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z M22 21v-2a4 4 0 0 0-3-3.87 M16 3.13a4 4 0 0 1 0 7.75',
  gitBranch: 'M6 3v12 M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6z M6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6z M15 6a9 9 0 0 0-9 9',
  externalLink: 'M15 3h6v6 M10 14 21 3 M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6',
  hardDrive: 'M22 12H2 M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z M6 16h.01 M10 16h.01',
  stethoscope: 'M11 2v2a4 4 0 0 1-8 0V2 M7 8v3a6 6 0 0 0 12 0V9 M19 9a2 2 0 1 0 0-4 2 2 0 0 0 0 4z M13 20a3 3 0 1 0 6 0v-3',
  layoutTemplate: 'M4 3h16a1 1 0 0 1 1 1v5a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1z M4 14h7a1 1 0 0 1 1 1v5a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1v-5a1 1 0 0 1 1-1z M17 14h3a1 1 0 0 1 1 1v5a1 1 0 0 1-1 1h-3a1 1 0 0 1-1-1v-5a1 1 0 0 1 1-1z',
}

export type IconName = keyof typeof ICON_PATHS

export function Icon(props: { name: IconName; size?: number; class?: string; strokeWidth?: number }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={props.size ?? 16}
      height={props.size ?? 16}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width={props.strokeWidth ?? 1.75}
      stroke-linecap="round"
      stroke-linejoin="round"
      class={cx('shrink-0', props.class)}
      aria-hidden="true"
    >
      <path d={ICON_PATHS[props.name]} />
    </svg>
  )
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

export function Dot(props: { tone?: Tone; pulse?: boolean; class?: string }) {
  return (
    <span class={cx('inline-block h-1.5 w-1.5 rounded-full', dotToneClasses[props.tone ?? 'neutral'], props.pulse && 'animate-pulse', props.class)} />
  )
}

type BadgeProps = {
  tone?: Tone
  dot?: boolean
  mono?: boolean
  title?: string
  class?: string
  children: JSX.Element
}

export function Badge(props: BadgeProps) {
  return (
    <span
      title={props.title}
      class={cx(
        'inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-2xs font-medium',
        props.mono && 'font-mono',
        badgeToneClasses[props.tone ?? 'neutral'],
        props.class,
      )}
    >
      <Show when={props.dot}>
        <Dot tone={props.tone ?? 'neutral'} />
      </Show>
      {props.children}
    </span>
  )
}

// Status text in a badge with an auto-derived tone (overridable) and a dot.
export function StatusBadge(props: { status?: string | null; tone?: Tone; loading?: boolean; class?: string; title?: string }) {
  const label = () => (props.loading ? 'loading' : (props.status ?? '').trim() || 'unknown')
  const tone = () => (props.loading ? 'neutral' : props.tone ?? statusTone(props.status))
  return (
    <Badge tone={tone()} dot title={props.title} class={cx('uppercase tracking-wide', props.loading && 'animate-pulse', props.class)}>
      {label()}
    </Badge>
  )
}

// Compact mono identifier pill (instance ids, config ids, tokens).
export function MonoBadge(props: { children: JSX.Element; title?: string; tone?: Tone; class?: string }) {
  return (
    <Badge mono tone={props.tone ?? 'accent'} title={props.title} class={cx('rounded', props.class)}>
      {props.children}
    </Badge>
  )
}

/**
 * Opens a left gutter in an index's first column for the hint to sit in, and
 * closes it again on release. Pair it with `position: relative` on the cell and
 * an absolutely positioned `<JumpHint>`; goes on the `<th>` too so the header
 * tracks the column.
 */
export const jumpHintGutterClass = () => cx('transition-[padding] duration-100', jumpHintsVisible() && 'pl-9')

/**
 * The key that jumps to a row or nav item, shown only while the reveal modifier
 * is held. Renders nothing without a `keys` string, so callers can pass the
 * hint for a position that has none (rows past the tenth) without a guard.
 */
export function JumpHint(props: { keys?: string; class?: string }) {
  return (
    <Show when={jumpHintsVisible() && props.keys}>
      {(keys) => (
        <kbd
          aria-hidden="true"
          class={cx(
            'inline-flex select-none items-center justify-center rounded border border-brass/40 bg-brass/15 px-1 py-px font-mono text-2xs font-medium leading-none text-brass-strong',
            props.class,
          )}
        >
          {keys()}
        </kbd>
      )}
    </Show>
  )
}

// ---------------------------------------------------------------------------
// Surfaces
// ---------------------------------------------------------------------------

export function Card(props: { children: JSX.Element; class?: string; pad?: boolean }) {
  return (
    <section class={cx('rounded-lg border border-line bg-surface shadow-card', props.pad !== false && 'p-4', props.class)}>
      {props.children}
    </section>
  )
}

type SectionCardProps = {
  title?: JSX.Element
  description?: JSX.Element
  actions?: JSX.Element
  children: JSX.Element
  class?: string
  contentClass?: string
  flush?: boolean // content touches card edges (tables, lists)
}

export function SectionCard(props: SectionCardProps) {
  return (
    <section class={cx('overflow-hidden rounded-lg border border-line bg-surface shadow-card', props.class)}>
      <Show when={props.title || props.actions || props.description}>
        <header class="flex min-h-[44px] flex-wrap items-center justify-between gap-x-4 gap-y-1 border-b border-line px-4 py-2">
          <div class="min-w-0">
            <Show when={props.title}>
              <h3 class="eyebrow !text-muted">{props.title}</h3>
            </Show>
            <Show when={props.description}>
              <p class="mt-0.5 text-xs text-faint">{props.description}</p>
            </Show>
          </div>
          <Show when={props.actions}>
            <div class="flex flex-wrap items-center gap-2">{props.actions}</div>
          </Show>
        </header>
      </Show>
      <div class={cx(props.flush ? '' : 'p-4', props.contentClass)}>{props.children}</div>
    </section>
  )
}

type PageHeaderProps = {
  eyebrow?: JSX.Element
  title: JSX.Element
  subtitle?: JSX.Element
  badges?: JSX.Element
  actions?: JSX.Element
  class?: string
}

export function PageHeader(props: PageHeaderProps) {
  return (
    <header class={cx('mb-5 flex flex-wrap items-end justify-between gap-x-6 gap-y-3', props.class)}>
      <div class="min-w-0">
        <Show when={props.eyebrow}>
          <p class="eyebrow mb-1">{props.eyebrow}</p>
        </Show>
        <div class="flex flex-wrap items-center gap-3">
          <h1 class="truncate font-display text-xl font-semibold tracking-tight text-ink md:text-2xl">{props.title}</h1>
          {props.badges}
        </div>
        <Show when={props.subtitle}>
          <p class="mt-1 max-w-2xl text-sm text-muted">{props.subtitle}</p>
        </Show>
      </div>
      <Show when={props.actions}>
        <div class="flex flex-wrap items-center gap-2">{props.actions}</div>
      </Show>
    </header>
  )
}

export function StatCard(props: { label: string; value: JSX.Element; hint?: JSX.Element; tone?: Tone; href?: string; icon?: IconName }) {
  const body = (
    <>
      <div class="flex items-center justify-between gap-2">
        <p class="eyebrow">{props.label}</p>
        <Show when={props.icon}>
          <Icon name={props.icon!} size={15} class="text-faint" />
        </Show>
      </div>
      <p class={cx('mt-1.5 font-display text-2xl font-semibold tabular-nums tracking-tight', props.tone && props.tone !== 'neutral' ? textToneClasses[props.tone] : 'text-ink')}>
        {props.value}
      </p>
      <Show when={props.hint}>
        <p class="mt-0.5 truncate text-xs text-faint">{props.hint}</p>
      </Show>
    </>
  )
  return (
    <Show
      when={props.href}
      fallback={<div class="rounded-lg border border-line bg-surface p-3.5 shadow-card">{body}</div>}
    >
      <A href={props.href!} class="block rounded-lg border border-line bg-surface p-3.5 shadow-card transition-colors hover:border-line-strong hover:bg-surface2/60">
        {body}
      </A>
    </Show>
  )
}

// ---------------------------------------------------------------------------
// Feedback
// ---------------------------------------------------------------------------

export function Note(props: { tone?: Tone; children: JSX.Element; class?: string; onDismiss?: () => void }) {
  return (
    <div class={cx('flex items-start justify-between gap-3 rounded-md border px-3 py-2 text-sm', noteToneClasses[props.tone ?? 'neutral'], props.class)}>
      <div class="min-w-0">{props.children}</div>
      <Show when={props.onDismiss}>
        <button type="button" class="-m-1 rounded p-1 opacity-60 hover:opacity-100" onClick={props.onDismiss} aria-label="Dismiss">
          <Icon name="x" size={14} />
        </button>
      </Show>
    </div>
  )
}

export function EmptyState(props: { icon?: IconName; title: JSX.Element; body?: JSX.Element; children?: JSX.Element; class?: string }) {
  return (
    <div class={cx('flex flex-col items-center justify-center gap-1.5 px-6 py-10 text-center', props.class)}>
      <Show when={props.icon}>
        <div class="mb-1 flex h-9 w-9 items-center justify-center rounded-full border border-line bg-surface2 text-faint">
          <Icon name={props.icon!} size={17} />
        </div>
      </Show>
      <p class="text-sm font-medium text-ink">{props.title}</p>
      <Show when={props.body}>
        <p class="max-w-sm text-xs leading-relaxed text-muted">{props.body}</p>
      </Show>
      {props.children}
    </div>
  )
}

export function Spinner(props: { label?: string; class?: string }) {
  return (
    <span class={cx('inline-flex items-center gap-2 text-sm text-muted', props.class)}>
      <svg class="h-3.5 w-3.5 animate-spin" viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" class="opacity-20" />
        <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" stroke-width="3" stroke-linecap="round" />
      </svg>
      {props.label ?? 'Loading…'}
    </span>
  )
}

export function LoadingRows(props: { rows?: number; class?: string }) {
  return (
    <div class={cx('space-y-2 p-4', props.class)} role="status" aria-label="Loading">
      {Array.from({ length: props.rows ?? 3 }).map(() => (
        <div class="skeleton h-8" />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------

export type TabItem = { id: string; label: string; count?: number }

export function Tabs(props: { tabs: readonly TabItem[]; active: string; onChange: (id: string) => void; class?: string }) {
  let listRef: HTMLDivElement | undefined
  // Roving tabindex + arrow keys per the WAI-ARIA tabs pattern.
  const onKeyDown = (event: KeyboardEvent, index: number) => {
    const total = props.tabs.length
    let next: number | undefined
    if (event.key === 'ArrowRight') next = (index + 1) % total
    else if (event.key === 'ArrowLeft') next = (index - 1 + total) % total
    else if (event.key === 'Home') next = 0
    else if (event.key === 'End') next = total - 1
    if (next === undefined) return
    event.preventDefault()
    const tab = props.tabs[next]
    props.onChange(tab.id)
    const buttons = listRef?.querySelectorAll<HTMLButtonElement>('[role="tab"]')
    buttons?.[next]?.focus()
  }
  return (
    <div ref={listRef} class={cx('flex items-center gap-1 overflow-x-auto border-b border-line', props.class)} role="tablist">
      {props.tabs.map((tab, index) => (
        <button
          type="button"
          role="tab"
          aria-selected={props.active === tab.id}
          tabindex={props.active === tab.id ? 0 : -1}
          onClick={() => props.onChange(tab.id)}
          onKeyDown={(event) => onKeyDown(event, index)}
          class={cx(
            '-mb-px flex items-center gap-1.5 whitespace-nowrap border-b-2 px-3 py-2 text-sm font-medium transition-colors',
            props.active === tab.id
              ? 'border-brass text-ink'
              : 'border-transparent text-muted hover:border-line-strong hover:text-ink',
          )}
        >
          {tab.label}
          <Show when={tab.count !== undefined}>
            <span class="rounded-full bg-surface2 px-1.5 text-2xs tabular-nums text-muted">{tab.count}</span>
          </Show>
        </button>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Buttons & forms
// ---------------------------------------------------------------------------

type ButtonVariant = 'primary' | 'subtle' | 'ghost' | 'danger' | 'accent'
type ButtonSize = 'xs' | 'sm' | 'md'

const buttonVariantClasses: Record<ButtonVariant, string> = {
  // Brass fill with inverse text works in both themes: dark gold on light
  // canvas text in light mode, light gold with near-black text in dark mode.
  primary: 'bg-brass font-semibold text-canvas hover:bg-brass-strong border border-transparent',
  accent: 'bg-brass/15 text-brass-strong hover:bg-brass/25 border border-brass/30',
  subtle: 'border border-line bg-surface text-ink hover:border-line-strong hover:bg-surface2',
  ghost: 'border border-transparent text-muted hover:bg-surface2 hover:text-ink',
  danger: 'border border-red-500/40 bg-transparent text-red-600 hover:bg-red-500/10 dark:text-red-400',
}

const buttonSizeClasses: Record<ButtonSize, string> = {
  xs: 'px-2 py-1 text-2xs gap-1',
  sm: 'px-2.5 py-1.5 text-xs gap-1.5',
  md: 'px-3.5 py-2 text-sm gap-2',
}

type ButtonProps = JSX.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant
  size?: ButtonSize
  icon?: IconName
  children?: JSX.Element
}

export function Button(props: ButtonProps) {
  const [local, rest] = splitProps(props, ['variant', 'size', 'icon', 'children', 'class'])
  return (
    <button
      type="button"
      {...rest}
      class={cx(
        'inline-flex items-center justify-center rounded-md font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-45',
        buttonVariantClasses[local.variant ?? 'subtle'],
        buttonSizeClasses[local.size ?? 'sm'],
        local.class,
      )}
    >
      <Show when={local.icon}>
        <Icon name={local.icon!} size={local.size === 'md' ? 15 : 13} />
      </Show>
      {local.children}
    </button>
  )
}

// Two-step destructive button: first click arms it, second click confirms.
// A short grace period after arming means an accidental double-click cannot
// confirm; blur and Escape disarm; the state change is announced politely.
export function ConfirmButton(props: { label: string; confirmLabel?: string; onConfirm: () => void; size?: ButtonSize; icon?: IconName; class?: string; disabled?: boolean }) {
  const [armed, setArmed] = createSignal(false)
  let timer: ReturnType<typeof setTimeout> | undefined
  let armedAt = 0
  onCleanup(() => {
    if (timer) clearTimeout(timer)
  })
  const disarm = () => {
    setArmed(false)
    if (timer) clearTimeout(timer)
  }
  const click = () => {
    if (armed()) {
      if (Date.now() - armedAt < 450) return
      disarm()
      props.onConfirm()
      return
    }
    armedAt = Date.now()
    setArmed(true)
    timer = setTimeout(() => setArmed(false), 4000)
  }
  return (
    <Button
      variant="danger"
      size={props.size}
      icon={props.icon ?? 'trash'}
      disabled={props.disabled}
      class={cx(armed() && 'border-red-500 bg-red-500/15 font-semibold', props.class)}
      onClick={click}
      onBlur={disarm}
      onKeyDown={(event: KeyboardEvent) => {
        if (event.key === 'Escape' && armed()) {
          event.stopPropagation()
          disarm()
        }
      }}
    >
      <span aria-live="polite">
        {armed() ? (props.confirmLabel ?? `Confirm ${props.label.toLowerCase()}?`) : props.label}
      </span>
    </Button>
  )
}

export function CopyButton(props: { text: string; label?: string; size?: ButtonSize; class?: string }) {
  const [copied, setCopied] = createSignal(false)
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(props.text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard unavailable (insecure context); ignore.
    }
  }
  return (
    <Button variant="ghost" size={props.size ?? 'xs'} icon={copied() ? 'check' : 'copy'} onClick={() => void copy()} class={props.class} aria-label="Copy to clipboard">
      {copied() ? 'Copied' : (props.label ?? 'Copy')}
    </Button>
  )
}

export const inputClass =
  'form-input w-full rounded-md border-line bg-surface px-3 py-1.5 text-sm text-ink placeholder:text-faint focus:border-brass focus:ring-1 focus:ring-brass/50'
// Intrinsic width by default; add `w-full` at the call site for form layouts.
export const selectClass =
  'form-select rounded-md border-line bg-surface py-1.5 pl-3 pr-8 text-sm text-ink focus:border-brass focus:ring-1 focus:ring-brass/50'
export const textareaClass =
  'form-textarea w-full rounded-md border-line bg-surface px-3 py-2 font-mono text-xs leading-relaxed text-ink placeholder:text-faint focus:border-brass focus:ring-1 focus:ring-brass/50'
export const checkboxClass =
  'form-checkbox h-3.5 w-3.5 rounded border-line-strong bg-surface text-brass focus:ring-brass/50'
export const fieldLabelClass = 'mb-1 block text-xs font-medium text-muted'

export function Field(props: { label: JSX.Element; children: JSX.Element; hint?: JSX.Element; class?: string }) {
  return (
    <label class={cx('block', props.class)}>
      <span class={fieldLabelClass}>{props.label}</span>
      {props.children}
      <Show when={props.hint}>
        <span class="mt-1 block text-2xs text-faint">{props.hint}</span>
      </Show>
    </label>
  )
}

// ---------------------------------------------------------------------------
// Data display
// ---------------------------------------------------------------------------

export const linkClass = 'font-medium text-brass-strong underline-offset-2 hover:underline'

export const tableClass = 'min-w-full divide-y divide-line text-sm'
export const theadClass = 'bg-surface2/60'
export const thClass = 'px-4 py-2 text-left text-2xs font-semibold uppercase tracking-wider text-muted'
export const tdClass = 'px-4 py-2 align-middle text-ink'
export const trHoverClass = 'transition-colors hover:bg-surface2/50'

export function TableShell(props: { children: JSX.Element; class?: string }) {
  return <div class={cx('overflow-x-auto', props.class)}>{props.children}</div>
}

export function DefList(props: { items: Array<[JSX.Element, JSX.Element]>; class?: string; cols?: 1 | 2 | 3 }) {
  return (
    <dl class={cx('grid gap-x-8 gap-y-2.5 text-sm', (props.cols ?? 2) === 2 && 'sm:grid-cols-2', props.cols === 3 && 'sm:grid-cols-2 lg:grid-cols-3', props.class)}>
      {props.items.map(([label, value]) => (
        <div class="min-w-0">
          <dt class="text-xs text-faint">{label}</dt>
          <dd class="mt-0.5 break-words font-medium text-ink">{value}</dd>
        </div>
      ))}
    </dl>
  )
}

export function CodeBlock(props: { code: string; class?: string; maxHClass?: string; label?: string }) {
  return (
    <div class={cx('group relative overflow-hidden rounded-md border border-line bg-surface2/70', props.class)}>
      <Show when={props.label}>
        <div class="flex items-center justify-between border-b border-line px-3 py-1.5">
          <span class="font-mono text-2xs text-faint">{props.label}</span>
          <CopyButton text={props.code} />
        </div>
      </Show>
      <Show when={!props.label}>
        <div class="absolute right-1.5 top-1.5 opacity-0 transition-opacity focus-within:opacity-100 group-hover:opacity-100">
          <CopyButton text={props.code} label="" class="bg-surface/90" />
        </div>
      </Show>
      <pre class={cx('overflow-auto p-3 font-mono text-xs leading-relaxed text-ink', props.maxHClass ?? 'max-h-80')}>{props.code}</pre>
    </div>
  )
}
