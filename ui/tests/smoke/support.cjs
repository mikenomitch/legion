// Shared helpers for the UI smoke tests (docs/testing-and-benchmarks.md,
// "UI smoke tests"): a tiny protobuf encoder for the replies the tests fake,
// gRPC-Web framing, and a fake manager that answers the SPA's calls in the
// browser, so no Legion process is needed.

// --- a tiny protobuf encoder (strings, varints, bools, nested messages) ---
const varint = (value) => {
  const out = []
  let v = BigInt(value)
  while (v > 127n) {
    out.push(Number((v & 127n) | 128n))
    v >>= 7n
  }
  out.push(Number(v))
  return Buffer.from(out)
}
const key = (field, wire) => varint((field << 3) | wire)
const str = (field, value) => {
  if (value === undefined || value === '') return Buffer.alloc(0)
  const bytes = Buffer.from(value, 'utf8')
  return Buffer.concat([key(field, 2), varint(bytes.length), bytes])
}
const msg = (field, bytes) => Buffer.concat([key(field, 2), varint(bytes.length), bytes])
const num = (field, value) => (value ? Buffer.concat([key(field, 0), varint(value)]) : Buffer.alloc(0))
const bool = (field, value) => (value ? Buffer.concat([key(field, 0), varint(1)]) : Buffer.alloc(0))

// --- the Legion messages the tests answer with ---
const permission = (resource, action) => Buffer.concat([str(1, resource), str(2, action)])
/** A UiExtension (ListUiExtensions). */
const extension = (e) =>
  Buffer.concat([
    str(1, e.app),
    str(2, e.slot),
    str(3, e.title),
    str(4, e.icon),
    str(5, e.view),
    str(6, e.badge),
    num(8, e.refresh),
    str(9, e.description),
    str(10, e.page),
  ])
/** An AppActionInfo (GetAppView). */
const actionInfo = (a) =>
  Buffer.concat([
    str(1, a.name),
    str(2, a.title),
    str(3, a.confirm),
    str(4, a.inputsJson),
    msg(5, permission('scale', 'write')),
    bool(6, a.allowed),
    bool(7, a.mutating),
  ])
/** A GetAppPageReply. */
const appPage = (p) =>
  Buffer.concat([
    str(1, p.app),
    str(2, p.page),
    str(3, p.title),
    str(4, p.html),
    ...p.rpcs.map((rpc) => msg(5, Buffer.concat([str(1, rpc.method), bool(2, rpc.mutating)]))),
    num(6, 1),
    str(7, 'mgr-1'),
  ])

/** One gRPC-Web response: the message frame, then an OK trailer frame. */
const grpcWeb = (message) => {
  const data = Buffer.concat([Buffer.from([0]), Buffer.alloc(4), message])
  data.writeUInt32BE(message.length, 1)
  const trailer = Buffer.from('grpc-status:0\r\ngrpc-message:\r\n')
  const frame = Buffer.concat([Buffer.from([0x80]), Buffer.alloc(4), trailer])
  frame.writeUInt32BE(trailer.length, 1)
  return Buffer.concat([data, frame])
}

const MANAGER = 'http://127.0.0.1:4747'
const cors = {
  'access-control-allow-origin': '*',
  'access-control-allow-headers': '*',
  'access-control-expose-headers': 'grpc-status,grpc-message',
}

/**
 * Answer the SPA's gRPC-Web calls on `page` with `answers[method]`
 * (`(request) => Buffer`, the reply message). Every call is recorded in the
 * returned list as `{ method, headers, body }`; a method without an answer
 * is UNIMPLEMENTED, which the SPA treats as a reachable manager refusing it.
 */
async function fakeManager(page, answers) {
  const calls = []
  await page.route(`${MANAGER}/**`, async (route) => {
    const request = route.request()
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers: cors })
    const method = new URL(request.url()).pathname.split('/').pop()
    calls.push({ method, headers: request.headers(), body: request.postDataBuffer() })
    const answer = answers[method]
    if (!answer) {
      return route.fulfill({
        status: 200,
        headers: { ...cors, 'content-type': 'application/grpc-web+proto', 'grpc-status': '12', 'grpc-message': 'not in this smoke test' },
        body: '',
      })
    }
    return route.fulfill({ status: 200, headers: { ...cors, 'content-type': 'application/grpc-web+proto' }, body: grpcWeb(answer(request)) })
  })
  return calls
}

module.exports = { str, msg, num, bool, extension, actionInfo, appPage, grpcWeb, fakeManager, MANAGER }
