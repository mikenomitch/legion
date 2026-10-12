// An xterm.js terminal bound to an exec/action session over the WebSocket
// bridge. Handles interactive PTY input/resize and one-shot output.
//
// The <Terminal> contract kept here (open/write/onData/resize) is the
// xterm.js surface that libghostty-vt wrappers (coder/ghostty-web) also
// mimic, so the renderer stays swappable later (see the plan, Q5).
//
// One mounted terminal is one session: it connects on mount and closes the
// socket on cleanup. To run again, remount it (key it on a run counter).

import { onCleanup, onMount } from 'solid-js'
import { Terminal } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'
import { ExecSession, type ExecStartOptions } from '../lib/execSocket'

/**
 * Where a session is in its life. `exited` carries the remote exit code;
 * `error` covers a refused or failed session (the bridge reports those as an
 * exit with code -1 and a message, or the socket fails before starting);
 * `stopped` means the user closed it from this side.
 */
export type ExecStatus =
  | { phase: 'connecting' }
  | { phase: 'running' }
  | { phase: 'exited'; code: number; message?: string }
  | { phase: 'error'; message: string }
  | { phase: 'stopped' }

/** Imperative handle handed to `onReady`, for toolbar buttons outside the terminal. */
export interface ExecTerminalControls {
  /** Clear the scrollback and screen (the session keeps running). */
  clear: () => void
  /** Focus the terminal so keystrokes reach an interactive session. */
  focus: () => void
  /** Close the socket, ending the session. Reports `{ phase: 'stopped' }` if it was still live. */
  stop: () => void
}

interface ExecTerminalProps {
  start: ExecStartOptions
  interactive: boolean
  onExit?: (code: number) => void
  /** Every status transition, starting with `connecting`. */
  onStatus?: (status: ExecStatus) => void
  /** Called once the terminal is mounted, with controls for an external toolbar. */
  onReady?: (controls: ExecTerminalControls) => void
  /**
   * Classes for the terminal frame. Defaults to a bordered 18rem box; pass
   * e.g. `h-full w-full` to fill a sized (or user-resizable) parent. The
   * terminal refits whenever this element changes size.
   */
  class?: string
}

const DEFAULT_FRAME_CLASS = 'h-72 w-full overflow-hidden rounded-md border border-line bg-[#0b0b0b] p-2'

export default function ExecTerminal(props: ExecTerminalProps) {
  let container: HTMLDivElement | undefined
  let term: Terminal | undefined
  let fit: FitAddon | undefined
  let session: ExecSession | undefined
  let live = true
  let started = false
  // Separate streaming decoders so a multi-byte character split across
  // frames on one stream is not corrupted by interleaved output on the other.
  const stdoutDecoder = new TextDecoder()
  const stderrDecoder = new TextDecoder()

  const report = (status: ExecStatus) => {
    if (status.phase !== 'connecting' && status.phase !== 'running') live = false
    props.onStatus?.(status)
  }

  onMount(() => {
    if (!container) return
    term = new Terminal({
      convertEol: !props.interactive,
      cursorBlink: props.interactive,
      fontSize: 13,
      // The canvas renderer cannot read CSS variables, so resolve the
      // appearance's monospace (it follows the Typeface tweak) once here.
      fontFamily:
        getComputedStyle(document.documentElement).getPropertyValue('--font-mono').trim() ||
        'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
      theme: { background: '#0b0b0b', cursor: '#f5f5f5' },
    })
    fit = new FitAddon()
    term.loadAddon(fit)
    term.open(container)
    safeFit()

    const cols = term.cols
    const rows = term.rows

    report({ phase: 'connecting' })
    session = new ExecSession(
      { ...props.start, cols, rows, tty: props.interactive },
      {
        onStdout: (data) => term?.write(stdoutDecoder.decode(data, { stream: true })),
        onStderr: (data) => term?.write(stderrDecoder.decode(data, { stream: true })),
        onStarted: () => {
          if (!live) return
          started = true
          report({ phase: 'running' })
          if (props.interactive) term?.focus()
        },
        onExit: (code, message) => {
          if (!live) return
          if (message) term?.write(`\r\n\x1b[90m[exited ${code}: ${message}]\x1b[0m\r\n`)
          else term?.write(`\r\n\x1b[90m[exited ${code}]\x1b[0m\r\n`)
          // The bridge reports refusals (ACL, unknown allocation, exec
          // disabled) and a dropped socket as exit -1 with a message.
          if (code === -1 || !started) report({ phase: 'error', message: message || `exited ${code}` })
          else report({ phase: 'exited', code, message: message || undefined })
          props.onExit?.(code)
        },
        onError: (message) => {
          if (!live) return
          term?.write(`\r\n\x1b[31m[error: ${message}]\x1b[0m\r\n`)
          report({ phase: 'error', message })
        },
      },
    )

    if (props.interactive) {
      term.onData((data) => session?.sendInput(data))
    } else {
      // One-shot: no input, signal EOF so the process's stdin closes.
      session.closeStdin()
    }

    // Refit on any size change of the frame — window resizes, a user
    // dragging the resize handle, a sidebar collapsing — not just the window.
    let frame = 0
    let lastCols = term.cols
    let lastRows = term.rows
    const onResize = () => {
      cancelAnimationFrame(frame)
      frame = requestAnimationFrame(() => {
        safeFit()
        if (!term || (term.cols === lastCols && term.rows === lastRows)) return
        lastCols = term.cols
        lastRows = term.rows
        if (props.interactive) session?.resize(term.cols, term.rows)
      })
    }
    if (typeof ResizeObserver !== 'undefined') {
      const observer = new ResizeObserver(onResize)
      observer.observe(container)
      onCleanup(() => observer.disconnect())
    } else {
      window.addEventListener('resize', onResize)
      onCleanup(() => window.removeEventListener('resize', onResize))
    }
    onCleanup(() => cancelAnimationFrame(frame))

    props.onReady?.({
      clear: () => term?.clear(),
      focus: () => term?.focus(),
      stop: () => {
        const wasLive = live
        session?.close()
        if (wasLive) {
          term?.write('\r\n\x1b[90m[session closed]\x1b[0m\r\n')
          report({ phase: 'stopped' })
        }
      },
    })
  })

  // FitAddon throws or computes nonsense for a detached / zero-size frame.
  function safeFit() {
    if (!container || container.clientWidth === 0 || container.clientHeight === 0) return
    try {
      fit?.fit()
    } catch {
      /* not laid out yet */
    }
  }

  onCleanup(() => {
    live = false
    session?.close()
    term?.dispose()
  })

  return <div ref={container} class={props.class ?? DEFAULT_FRAME_CLASS} />
}
