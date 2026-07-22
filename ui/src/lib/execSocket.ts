// Browser client for the manager's exec WebSocket bridge.
//
// The bridge speaks the same protobuf frames as the native gRPC path, one
// protobuf message per binary WebSocket frame. It listens on the manager
// port + 1 (see `src/manager/exec_ws.rs`).

import {
  ExecClientFrame,
  ExecServerFrame,
  ExecStart,
  ExecInput,
  ExecResize,
  ExecWinsize,
} from '../gen/legion_pb'
import { managerEndpoint, getStoredToken } from '../grpcClient'

/** Derive the exec WebSocket URL (ws(s)://host:port+1/v1/exec/ws). */
export function execWsUrl(): string {
  const url = new URL(managerEndpoint)
  const wsProto = url.protocol === 'https:' ? 'wss:' : 'ws:'
  const port = url.port ? Number(url.port) : url.protocol === 'https:' ? 443 : 80
  return `${wsProto}//${url.hostname}:${port + 1}/v1/exec/ws`
}

export interface ExecStartOptions {
  allocationId: bigint
  taskName?: string
  command?: string[]
  action?: string
  actionParams?: Record<string, string>
  tty?: boolean
  cols?: number
  rows?: number
}

export interface ExecHandlers {
  onStarted?: (sessionId: string) => void
  onStdout?: (data: Uint8Array) => void
  onStderr?: (data: Uint8Array) => void
  onExit?: (code: number, message: string) => void
  onError?: (message: string) => void
}

/** A live exec/action session over the WebSocket bridge. */
export class ExecSession {
  private socket: WebSocket
  private closed = false

  constructor(opts: ExecStartOptions, handlers: ExecHandlers) {
    this.socket = new WebSocket(execWsUrl())
    this.socket.binaryType = 'arraybuffer'

    this.socket.onopen = () => {
      const start = new ExecStart({
        allocationId: opts.allocationId,
        taskName: opts.taskName ?? '',
        command: opts.command ?? [],
        action: opts.action ?? '',
        actionParams: opts.actionParams ?? {},
        tty: opts.tty ?? false,
        bearerToken: getStoredToken() ?? '',
        winsize:
          opts.tty && opts.cols && opts.rows
            ? new ExecWinsize({ cols: opts.cols, rows: opts.rows })
            : undefined,
      })
      this.sendFrame(new ExecClientFrame({ frame: { case: 'start', value: start } }))
    }

    this.socket.onmessage = (event) => {
      const bytes = new Uint8Array(event.data as ArrayBuffer)
      const frame = ExecServerFrame.fromBinary(bytes)
      switch (frame.frame.case) {
        case 'started':
          handlers.onStarted?.(frame.frame.value.sessionId)
          break
        case 'stdout':
          handlers.onStdout?.(frame.frame.value.data)
          break
        case 'stderr':
          handlers.onStderr?.(frame.frame.value.data)
          break
        case 'exit':
          this.closed = true
          handlers.onExit?.(frame.frame.value.code, frame.frame.value.message)
          break
      }
    }

    this.socket.onerror = () => {
      if (!this.closed) handlers.onError?.('WebSocket error')
    }
    this.socket.onclose = () => {
      if (!this.closed) {
        this.closed = true
        handlers.onExit?.(-1, 'connection closed')
      }
    }
  }

  private sendFrame(frame: ExecClientFrame) {
    if (this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(frame.toBinary())
    }
  }

  /** Forward keystrokes / piped input. */
  sendInput(data: string) {
    this.sendFrame(
      new ExecClientFrame({
        frame: {
          case: 'stdin',
          value: new ExecInput({ data: new TextEncoder().encode(data), eof: false }),
        },
      }),
    )
  }

  /** Half-close stdin. */
  closeStdin() {
    this.sendFrame(
      new ExecClientFrame({
        frame: { case: 'stdin', value: new ExecInput({ data: new Uint8Array(), eof: true }) },
      }),
    )
  }

  /** Notify the remote pty of a new terminal size. */
  resize(cols: number, rows: number) {
    this.sendFrame(
      new ExecClientFrame({
        frame: {
          case: 'resize',
          value: new ExecResize({ winsize: new ExecWinsize({ cols, rows }) }),
        },
      }),
    )
  }

  close() {
    this.closed = true
    try {
      this.socket.close()
    } catch {
      /* ignore */
    }
  }
}
