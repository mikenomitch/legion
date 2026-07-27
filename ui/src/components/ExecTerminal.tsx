// An xterm.js terminal bound to an exec/action session over the WebSocket
// bridge. Handles interactive PTY input/resize and one-shot output.
//
// The <Terminal> contract kept here (open/write/onData/resize) is the
// xterm.js surface that libghostty-vt wrappers (coder/ghostty-web) also
// mimic, so the renderer stays swappable later (see the plan, Q5).

import { onCleanup, onMount } from 'solid-js'
import { Terminal } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'
import { ExecSession, type ExecStartOptions } from '../lib/execSocket'

interface ExecTerminalProps {
  start: ExecStartOptions
  interactive: boolean
  onExit?: (code: number) => void
}

export default function ExecTerminal(props: ExecTerminalProps) {
  let container: HTMLDivElement | undefined
  let term: Terminal | undefined
  let fit: FitAddon | undefined
  let session: ExecSession | undefined
  const decoder = new TextDecoder()

  onMount(() => {
    if (!container) return
    term = new Terminal({
      convertEol: !props.interactive,
      cursorBlink: props.interactive,
      fontSize: 13,
      fontFamily: '"Fira Code Variable", "Fira Code", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
      theme: { background: '#0d0c0a', cursor: '#d6b45a' },
    })
    fit = new FitAddon()
    term.loadAddon(fit)
    term.open(container)
    fit.fit()

    const cols = term.cols
    const rows = term.rows

    session = new ExecSession(
      { ...props.start, cols, rows, tty: props.interactive },
      {
        onStdout: (data) => term?.write(decoder.decode(data)),
        onStderr: (data) => term?.write(decoder.decode(data)),
        onStarted: () => props.interactive && term?.focus(),
        onExit: (code, message) => {
          if (message) term?.write(`\r\n\x1b[90m[exited ${code}: ${message}]\x1b[0m\r\n`)
          else term?.write(`\r\n\x1b[90m[exited ${code}]\x1b[0m\r\n`)
          props.onExit?.(code)
        },
        onError: (message) => term?.write(`\r\n\x1b[31m[error: ${message}]\x1b[0m\r\n`),
      },
    )

    if (props.interactive) {
      term.onData((data) => session?.sendInput(data))
    } else {
      // One-shot: no input, signal EOF so the process's stdin closes.
      session.closeStdin()
    }

    const onResize = () => {
      fit?.fit()
      if (props.interactive && term) session?.resize(term.cols, term.rows)
    }
    window.addEventListener('resize', onResize)
    onCleanup(() => window.removeEventListener('resize', onResize))
  })

  onCleanup(() => {
    session?.close()
    term?.dispose()
  })

  return <div ref={container} class="h-72 w-full overflow-hidden rounded-md border border-line bg-[#0d0c0a] p-2" />
}
