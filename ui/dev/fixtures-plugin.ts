/*
 * Data sources for UI work without (or with) a cluster: record the manager's
 * gRPC-web responses once, then replay them with no manager running.
 *
 *   npm run lab          dev server + theme studio; each request goes to the
 *                        live manager or the recording, per the studio's
 *                        Auto / Live / Fake switch (a `legion-data` cookie).
 *                        Auto = live when the manager answers, else recorded.
 *   npm run lab:record   proxy to a live manager AND save every response
 *   npm run lab:crawl    visit every page so the recording covers them all
 *
 * Fixtures live in dev/fixtures/<Method>.json as proto3 JSON, one file per
 * RPC, keyed by the request message — readable, diffable and hand-editable
 * (rename a worker, flip a status, add an allocation). Replay shifts every
 * timestamp by "now − recordedAt", so "3 minutes ago" stays "3 minutes ago"
 * whenever you replay.
 *
 * Dev server only: vite.config.ts installs this in the lab/record modes and
 * nothing in it reaches a production build.
 */
import type { Plugin } from 'vite'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { connect } from 'node:net'
import path from 'node:path'
import { MethodKind } from '@bufbuild/protobuf'
import { Manager } from '../src/gen/legion_connect'

type Mode = 'record' | 'lab'
type Preference = 'auto' | 'live' | 'fake'
type Entry = { request: unknown; response?: unknown; error?: { code: number; message: string } }
type Fixture = { method: string; recordedAt: string; entries: Entry[] }

const PREFIX = '/legion.Manager/'
const MAX_ENTRIES_PER_METHOD = 40
// Mutations are never replayed from a recording that happened to include one
// unless the exact request was recorded; anything else gets this answer.
const READ_ONLY_MESSAGE = 'Fake data is read-only: this call was not recorded. Run `npm run lab:record` against a live manager to capture it.'

const methods = new Map(Object.values(Manager.methods).map((method) => [method.name, method]))

// --- gRPC-web framing -------------------------------------------------------

const frame = (flag: number, payload: Uint8Array) => {
  const out = new Uint8Array(5 + payload.length)
  out[0] = flag
  new DataView(out.buffer).setUint32(1, payload.length)
  out.set(payload, 5)
  return out
}

const trailer = (code: number, message = '') =>
  frame(0x80, new TextEncoder().encode(`grpc-status:${code}\r\ngrpc-message:${encodeURIComponent(message)}\r\n`))

const parseFrames = (bytes: Uint8Array) => {
  const data: Uint8Array[] = []
  let trailers: Record<string, string> = {}
  let offset = 0
  while (offset + 5 <= bytes.length) {
    const flag = bytes[offset]
    const length = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 4).getUint32(0)
    const payload = bytes.subarray(offset + 5, offset + 5 + length)
    if (flag & 0x80) {
      trailers = Object.fromEntries(
        new TextDecoder()
          .decode(payload)
          .split('\r\n')
          .filter(Boolean)
          .map((line) => {
            const index = line.indexOf(':')
            return [line.slice(0, index).trim().toLowerCase(), line.slice(index + 1).trim()]
          }),
      )
    } else {
      data.push(payload)
    }
    offset += 5 + length
  }
  return { data, trailers }
}

const readBody = (req: IncomingMessage) =>
  new Promise<Uint8Array>((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(chunk))
    req.on('end', () => resolve(new Uint8Array(Buffer.concat(chunks))))
    req.on('error', reject)
  })

// --- timestamps ---------------------------------------------------------------

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/
const TIME_KEY = /(at|time|timestamp|heartbeat|since|until|expires|seen)$/i

const shiftNumber = (value: number, offsetMs: number) => {
  if (value >= 1e9 && value < 1e10) return Math.round(value + offsetMs / 1000)
  if (value >= 1e12 && value < 1e13) return Math.round(value + offsetMs)
  if (value >= 1e18 && value < 1e19) return value + offsetMs * 1e6
  return value
}

const shiftTimes = (value: unknown, offsetMs: number, key = ''): unknown => {
  if (Array.isArray(value)) return value.map((item) => shiftTimes(item, offsetMs, key))
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, shiftTimes(v, offsetMs, k)]))
  }
  if (typeof value === 'string') {
    if (ISO.test(value)) {
      const parsed = Date.parse(value)
      return Number.isNaN(parsed) ? value : new Date(parsed + offsetMs).toISOString()
    }
    if (TIME_KEY.test(key) && /^\d{10,19}$/.test(value)) {
      if (value.length >= 18) return (BigInt(value) + BigInt(Math.round(offsetMs)) * 1_000_000n).toString()
      return String(shiftNumber(Number(value), offsetMs))
    }
  }
  if (typeof value === 'number' && TIME_KEY.test(key)) return shiftNumber(value, offsetMs)
  return value
}

// --- fixtures on disk ---------------------------------------------------------

const fixturePath = (dir: string, method: string) => path.join(dir, `${method}.json`)

const loadFixture = (dir: string, method: string): Fixture | undefined => {
  const file = fixturePath(dir, method)
  if (!existsSync(file)) return undefined
  try {
    return JSON.parse(readFileSync(file, 'utf8')) as Fixture
  } catch (error) {
    console.warn(`[fixtures] ${file} is not valid JSON: ${(error as Error).message}`)
    return undefined
  }
}

const saveEntry = (dir: string, method: string, entry: Entry) => {
  mkdirSync(dir, { recursive: true })
  const fixture = loadFixture(dir, method) ?? { method, recordedAt: '', entries: [] }
  const key = JSON.stringify(entry.request)
  fixture.entries = [entry, ...fixture.entries.filter((existing) => JSON.stringify(existing.request) !== key)].slice(
    0,
    MAX_ENTRIES_PER_METHOD,
  )
  fixture.recordedAt = new Date().toISOString()
  writeFileSync(fixturePath(dir, method), `${JSON.stringify(fixture, null, 2)}\n`)
}

// --- handlers -----------------------------------------------------------------

const sendFrames = (res: ServerResponse, body: Uint8Array) => {
  res.statusCode = 200
  res.setHeader('content-type', 'application/grpc-web+proto')
  res.end(Buffer.from(body))
}

const sendError = (res: ServerResponse, code: number, message: string) => {
  res.statusCode = 200
  res.setHeader('content-type', 'application/grpc-web+proto')
  res.setHeader('grpc-status', String(code))
  res.setHeader('grpc-message', encodeURIComponent(message))
  res.end()
}

const UNIMPLEMENTED = 12

async function replay(dir: string, methodName: string, body: Uint8Array, req: IncomingMessage, res: ServerResponse) {
  const method = methods.get(methodName)
  if (!method) return sendError(res, UNIMPLEMENTED, `Unknown method ${methodName}`)
  if (method.kind === MethodKind.ServerStreaming) {
    // A live stream has nothing to replay; hold it open and quiet so the page
    // shows its recorded snapshot instead of reconnecting in a loop.
    res.statusCode = 200
    res.setHeader('content-type', 'application/grpc-web+proto')
    res.flushHeaders()
    req.on('close', () => res.end())
    return
  }
  const fixture = loadFixture(dir, methodName)
  if (!fixture || fixture.entries.length === 0) return sendError(res, UNIMPLEMENTED, READ_ONLY_MESSAGE)
  const { data } = parseFrames(body)
  const request = data[0] ? method.I.fromBinary(data[0]).toJson() : {}
  const key = JSON.stringify(request)
  const exact = fixture.entries.find((entry) => JSON.stringify(entry.request) === key)
  const isRead = /^(Get|List|WhoAmI|Show|Fetch|Query|Resolve|HealthCheck|Plan|Test|Render)/.test(methodName)
  const entry = exact ?? (isRead ? fixture.entries[0] : undefined)
  if (!entry) return sendError(res, UNIMPLEMENTED, READ_ONLY_MESSAGE)
  if (entry.error) return sendError(res, entry.error.code, entry.error.message)
  const offset = Date.now() - Date.parse(fixture.recordedAt)
  const message = method.O.fromJson(shiftTimes(entry.response, offset) as never, { ignoreUnknownFields: true })
  sendFrames(res, Buffer.concat([Buffer.from(frame(0x00, message.toBinary())), Buffer.from(trailer(0))]))
}

type ForwardOptions = { save: boolean; dir: string; onUnreachable?: () => Promise<void> }

// Proxy one call to the manager; optionally record the answer.
async function forward(target: string, methodName: string, body: Uint8Array, req: IncomingMessage, res: ServerResponse, options: ForwardOptions) {
  const headers = new Headers()
  for (const [name, value] of Object.entries(req.headers)) {
    if (typeof value === 'string' && !['host', 'connection', 'content-length', 'origin', 'referer', 'cookie'].includes(name)) headers.set(name, value)
  }
  const method = methods.get(methodName)
  let upstream: Response
  try {
    upstream = await fetch(`${target}${req.url}`, { method: 'POST', headers, body, duplex: 'half' } as RequestInit)
    lastFailure = 0
  } catch (error) {
    lastFailure = Date.now()
    if (options.onUnreachable) return options.onUnreachable()
    return sendError(res, 14, `Could not reach the manager at ${target} (${(error as Error).message}). Start it, or switch the theme studio's data to Fake.`)
  }
  res.statusCode = upstream.status
  upstream.headers.forEach((value, name) => {
    if (!['content-length', 'transfer-encoding', 'connection', 'content-encoding'].includes(name)) res.setHeader(name, value)
  })
  if (!upstream.body) return res.end()
  if (!method || method.kind !== MethodKind.Unary) {
    // Streams are passed through live but never recorded.
    const reader = upstream.body.getReader()
    req.on('close', () => void reader.cancel())
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      res.write(Buffer.from(value))
    }
    return res.end()
  }
  const bytes = new Uint8Array(await upstream.arrayBuffer())
  res.end(Buffer.from(bytes))
  if (!options.save) return
  try {
    const request = parseFrames(body).data[0]
    const requestJson = request ? method.I.fromBinary(request).toJson() : {}
    const headerStatus = upstream.headers.get('grpc-status')
    if (headerStatus && headerStatus !== '0') {
      saveEntry(options.dir, methodName, {
        request: requestJson,
        error: { code: Number(headerStatus), message: decodeURIComponent(upstream.headers.get('grpc-message') ?? '') },
      })
      return
    }
    const { data, trailers } = parseFrames(bytes)
    if (trailers['grpc-status'] && trailers['grpc-status'] !== '0') {
      saveEntry(options.dir, methodName, {
        request: requestJson,
        error: { code: Number(trailers['grpc-status']), message: decodeURIComponent(trailers['grpc-message'] ?? '') },
      })
      return
    }
    if (data[0]) saveEntry(options.dir, methodName, { request: requestJson, response: method.O.fromBinary(data[0]).toJson() })
  } catch (error) {
    console.warn(`[fixtures] could not record ${methodName}: ${(error as Error).message}`)
  }
}

// After a failed connect, Auto skips straight to the recording for a few
// seconds instead of paying a refused connection on every poll.
let lastFailure = 0
const RECHECK_MS = 5000

const preferenceOf = (req: IncomingMessage, fallback: Preference): Preference => {
  const match = /(?:^|;\s*)legion-data=(auto|live|fake)/.exec(req.headers.cookie ?? '')
  return (match?.[1] as Preference | undefined) ?? fallback
}

const probe = (target: string) =>
  new Promise<boolean>((resolve) => {
    const url = new URL(target)
    const socket = connect({ host: url.hostname, port: Number(url.port || (url.protocol === 'https:' ? 443 : 80)) })
    const done = (ok: boolean) => {
      socket.destroy()
      resolve(ok)
    }
    socket.setTimeout(800, () => done(false))
    socket.once('connect', () => done(true))
    socket.once('error', () => done(false))
  })

const fixtureSummary = (dir: string) => {
  if (!existsSync(dir)) return { fixtures: 0, recordedAt: undefined as string | undefined }
  const files = readdirSync(dir).filter((file) => file.endsWith('.json'))
  const times = files.map((file) => loadFixture(dir, file.replace(/\.json$/, ''))?.recordedAt).filter(Boolean) as string[]
  return { fixtures: files.length, recordedAt: times.sort().at(-1) }
}

export function legionFixtures(options: { mode: Mode; dir: string; target: string; defaultPreference?: Preference }): Plugin {
  const fallback = options.defaultPreference ?? 'auto'
  return {
    name: 'legion-fixtures',
    apply: 'serve',
    configureServer(server) {
      const where = path.relative(process.cwd(), options.dir)
      server.config.logger.info(
        options.mode === 'record'
          ? `  [lab] recording ${options.target} → ${where}`
          : `  [lab] data: live ${options.target} when it answers, else the recording in ${where} (switch in the theme studio)`,
      )
      server.middlewares.use(async (req, res, next) => {
        if (req.method === 'GET' && req.url === '/__legion-lab/status') {
          const reachable = await probe(options.target)
          res.setHeader('content-type', 'application/json')
          res.end(JSON.stringify({ mode: options.mode, preference: preferenceOf(req, fallback), target: options.target, reachable, ...fixtureSummary(options.dir) }))
          return
        }
        if (req.method !== 'POST' || !req.url?.startsWith(PREFIX)) return next()
        const methodName = req.url.slice(PREFIX.length).split('?')[0]
        try {
          const body = await readBody(req)
          const fromRecording = () => replay(options.dir, methodName, body, req, res)
          if (options.mode === 'record') {
            await forward(options.target, methodName, body, req, res, { save: true, dir: options.dir })
            return
          }
          const preference = preferenceOf(req, fallback)
          if (preference === 'fake' || (preference === 'auto' && Date.now() - lastFailure < RECHECK_MS)) {
            await fromRecording()
            return
          }
          await forward(options.target, methodName, body, req, res, {
            save: false,
            dir: options.dir,
            onUnreachable: preference === 'auto' ? fromRecording : undefined,
          })
        } catch (error) {
          sendError(res, 13, `Lab middleware failed: ${(error as Error).message}`)
        }
      })
    },
  }
}
