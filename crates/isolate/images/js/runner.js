// The `js` image's runner: `legion:isolate/runner.run` for StarlingMonkey.
//
// componentize-js evaluates this module while it pre-initializes the image
// (Wizer), so the engine, this runner and the `legion` global are in the
// image's memory snapshot. Per run: read `request.entry` from the run
// directory (`/legion/run`) and evaluate it, reporting the exit status and
// the result.
//
// - A script entry (`.js` without `import`/`export` statements) is the body
//   of an async function: top-level `await` and `return value` work, and
//   its top-level declarations belong to the run. `globalThis` belongs to
//   the instance: a `tenant` or `session` isolate keeps what a run puts
//   there (a `request` isolate is a fresh instance every run).
// - The result is the entry's return value, else `globalThis.result`, as
//   JSON. When neither is set the host falls back to `result.json`.
// - console.log goes to stdout, console.error and uncaught errors to stderr.

import * as control from 'legion:isolate/control@0.1.0';
import * as stateApi from 'legion:isolate/state@0.1.0';
import * as vmApi from 'legion:isolate/vm@0.1.0';
import { getEnvironment } from 'wasi:cli/environment@0.2.12';
import { getDirectories } from 'wasi:filesystem/preopens@0.2.12';

const RUN_DIR = '/legion/run';
const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

class LegionStateError extends Error {
  constructor(kind, message, quota) {
    super(message);
    this.name = 'LegionStateError';
    this.kind = kind;
    if (quota !== undefined) this.quota = quota;
  }
}

class LegionVmError extends Error {
  constructor(kind, message) {
    super(message);
    this.name = 'LegionVmError';
    this.kind = kind;
  }
}

class LegionFsError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'LegionFsError';
    this.code = code;
  }
}

/** The payload of a WIT `result` error thrown by a binding. */
function payload(e) {
  return e && typeof e === 'object' && 'payload' in e ? e.payload : e;
}

function unsupported(what) {
  control.unsupported(what);
  // Not reached: the host stops the run.
  throw new Error(what);
}

// ---------------------------------------------------------------------------
// Files (the run directory, through wasi:filesystem)
// ---------------------------------------------------------------------------

let rootDescriptor = null;

function root() {
  if (rootDescriptor === null) {
    const dirs = getDirectories();
    const found = dirs.find(([, p]) => p === '/') || dirs[0];
    if (!found) throw new LegionFsError('no-entry', 'legion.fs: the isolate has no filesystem');
    rootDescriptor = found[0];
  }
  return rootDescriptor;
}

/** A path relative to the `/` preopen; relative paths are in the run directory. */
function resolve(path) {
  const abs = String(path).startsWith('/') ? String(path) : RUN_DIR + '/' + path;
  const parts = [];
  for (const p of abs.split('/')) {
    if (p === '' || p === '.') continue;
    if (p === '..') parts.pop();
    else parts.push(p);
  }
  return parts.length ? parts.join('/') : '.';
}

function fsCall(op, path, fn) {
  try {
    return fn();
  } catch (e) {
    const code = payload(e);
    if (typeof code === 'string') {
      throw new LegionFsError(code, `legion.fs.${op} '${path}': ${code}`);
    }
    throw e;
  }
}

const CHUNK = 65536n;

const fs = {
  /** The file's bytes. */
  readFile(path) {
    return fsCall('readFile', path, () => {
      const f = root().openAt({}, resolve(path), {}, { read: true });
      try {
        const parts = [];
        let offset = 0n;
        let total = 0;
        for (;;) {
          const [bytes, eof] = f.read(CHUNK, offset);
          if (bytes.length) {
            parts.push(bytes);
            total += bytes.length;
            offset += BigInt(bytes.length);
          }
          if (eof || bytes.length === 0) break;
        }
        if (parts.length === 1) return parts[0];
        const out = new Uint8Array(total);
        let at = 0;
        for (const p of parts) {
          out.set(p, at);
          at += p.length;
        }
        return out;
      } finally {
        f[Symbol.dispose]();
      }
    });
  },
  /** The file as UTF-8 text. */
  readText(path) {
    return decoder.decode(fs.readFile(path));
  },
  /** Create or replace a file with `data` (a Uint8Array, or a string written as UTF-8). */
  writeFile(path, data) {
    const bytes = typeof data === 'string' ? encoder.encode(data) : new Uint8Array(data);
    fsCall('writeFile', path, () => {
      const f = root().openAt({}, resolve(path), { create: true, truncate: true }, { write: true });
      try {
        let offset = 0;
        while (offset < bytes.length) {
          const n = f.write(bytes.subarray(offset, offset + 65536), BigInt(offset));
          offset += Number(n);
        }
      } finally {
        f[Symbol.dispose]();
      }
    });
  },
  /** The names in a directory (default: the run directory), sorted. */
  list(path = '.') {
    return fsCall('list', path, () => {
      const d = root().openAt({}, resolve(path), { directory: true }, { read: true });
      try {
        const stream = d.readDirectory();
        const names = [];
        for (let e = stream.readDirectoryEntry(); e; e = stream.readDirectoryEntry()) names.push(e.name);
        stream[Symbol.dispose]();
        return names.sort();
      } finally {
        d[Symbol.dispose]();
      }
    });
  },
  /** Create a directory; `{ recursive: true }` creates its parents too. */
  mkdir(path, opts = {}) {
    const target = resolve(path);
    if (opts.recursive) {
      const parts = target.split('/');
      for (let i = 1; i <= parts.length; i++) {
        const p = parts.slice(0, i).join('/');
        if (!fs.exists('/' + p)) fsCall('mkdir', path, () => root().createDirectoryAt(p));
      }
      return;
    }
    fsCall('mkdir', path, () => root().createDirectoryAt(target));
  },
  /** Remove a file or an empty directory. */
  remove(path) {
    const target = resolve(path);
    fsCall('remove', path, () => {
      const st = root().statAt({}, target);
      if (st.type === 'directory') root().removeDirectoryAt(target);
      else root().unlinkFileAt(target);
    });
  },
  /** Whether the path exists. */
  exists(path) {
    try {
      root().statAt({}, resolve(path));
      return true;
    } catch (e) {
      if (typeof payload(e) === 'string') return false;
      throw e;
    }
  },
};

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

function stateCall(fn) {
  try {
    return fn();
  } catch (e) {
    const p = payload(e);
    if (p && p.tag === 'quota') {
      const quota = Number(p.val);
      throw new LegionStateError('quota', `legion.state: the write would exceed the state quota of ${quota} bytes`, quota);
    }
    if (p && p.tag === 'not-available') {
      throw new LegionStateError(
        'not-available',
        'legion.state: a request isolate keeps no state (use lifetime = "tenant" or "session")',
      );
    }
    throw e;
  }
}

const state = {
  /** The bytes stored under `key` (a Uint8Array), or null. */
  get(key) {
    const v = stateCall(() => stateApi.get(String(key)));
    return v === undefined ? null : v;
  },
  /** Store a Uint8Array (or a string, as UTF-8) under `key`. */
  set(key, value) {
    let bytes;
    if (typeof value === 'string') bytes = encoder.encode(value);
    else if (value instanceof Uint8Array) bytes = value;
    else if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) bytes = new Uint8Array(value.buffer ?? value);
    else throw new TypeError('legion.state.set: value must be a Uint8Array or a string');
    stateCall(() => stateApi.set(String(key), bytes));
  },
  /** Remove `key` (no error when absent). */
  delete(key) {
    stateCall(() => stateApi.delete(String(key)));
  },
  /** Every key. */
  keys() {
    return stateCall(() => stateApi.keys());
  },
};

// ---------------------------------------------------------------------------
// vm
// ---------------------------------------------------------------------------

function vmCall(what, fn) {
  try {
    return fn();
  } catch (e) {
    const p = payload(e);
    if (p && typeof p.tag === 'string') {
      if (p.tag === 'not-configured') {
        throw new LegionVmError(p.tag, `legion.vm.${what}: this task has no VM task (set config.vm.task)`);
      }
      throw new LegionVmError(p.tag, `legion.vm.${what}: ${p.tag}${p.val ? ': ' + p.val : ''}`);
    }
    throw e;
  }
}

function bytesOf(data, what) {
  if (data === undefined || data === null) return new Uint8Array(0);
  if (typeof data === 'string') return encoder.encode(data);
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) return new Uint8Array(data.buffer ?? data);
  throw new TypeError(`${what}: data must be a Uint8Array or a string`);
}

function headerPairs(headers) {
  if (!headers) return [];
  const entries =
    typeof headers.entries === 'function' && !Array.isArray(headers) ? [...headers.entries()] : Array.isArray(headers) ? headers : Object.entries(headers);
  return entries.map(([k, v]) => [String(k).toLowerCase(), bytesOf(String(v), 'headers')]);
}

class Machine {
  #h;
  constructor(handle) {
    this.#h = handle;
    /** The child's session id: keep it in legion.state to re-attach. */
    this.id = handle.id();
  }
  /** Run argv in the VM: { exit, stdout, stderr (Uint8Array), timedOut }. */
  exec(argv, { stdin, timeoutMs = 60000 } = {}) {
    if (!Array.isArray(argv)) throw new TypeError("legion.vm exec: argv is an array of strings, e.g. ['sh', '-c', 'ls']");
    const r = vmCall('exec', () => this.#h.exec(argv.map(String), bytesOf(stdin, 'exec stdin'), timeoutMs >>> 0));
    return { exit: r.exit, stdout: r.stdout, stderr: r.stderr, timedOut: r.timedOut };
  }
  /** Write a file in the VM. */
  put(path, data) {
    vmCall('put', () => this.#h.put(String(path), bytesOf(data, 'legion.vm put')));
  }
  /** Read a file from the VM (a Uint8Array). */
  get(path) {
    return vmCall('get', () => this.#h.get(String(path)));
  }
  /** An HTTP request to a port the VM listens on; returns a Response. */
  fetch(port, { method = 'GET', path = '/', headers, body } = {}) {
    const r = vmCall('fetch', () =>
      this.#h.fetch(port, { method: method.toUpperCase(), path, headers: headerPairs(headers), body: bytesOf(body, 'legion.vm fetch') }),
    );
    const h = new Headers();
    for (const [k, v] of r.headers) h.append(k, decoder.decode(v));
    const nullBody = r.status === 204 || r.status === 205 || r.status === 304;
    return new Response(nullBody ? null : r.body, { status: r.status, headers: h });
  }
  /** Grow or shrink the VM: { memoryMib, memoryMaxMib, cpuMhz }. */
  resize({ memoryMib, memoryMaxMib, cpuMhz }) {
    vmCall('resize', () => this.#h.resize({ memoryMib, memoryMaxMib, cpuMhz }));
  }
  /** End the VM now. */
  release() {
    this.#h.release();
  }
}

const vm = {
  /** Start a child VM of the task's VM task. */
  acquire() {
    return new Machine(vmCall('acquire', () => vmApi.acquire()));
  },
  /** Re-attach to a child VM this isolate (or its session) owns. */
  attach(id) {
    return new Machine(vmCall('attach', () => vmApi.attach(String(id))));
  },
};

// ---------------------------------------------------------------------------
// The `legion` global, `process`, `require`, `Worker`
// ---------------------------------------------------------------------------

const legion = {
  /** The run's input, parsed from JSON; null when the run has none. */
  input: null,
  /** Who this isolate is. */
  info() {
    const i = control.info();
    return {
      job: i.job,
      task: i.task,
      lifetime: i.lifetime,
      tenant: i.tenant ?? null,
      session: i.session ?? null,
      resumed: i.resumed,
    };
  },
  /** A session isolate may be hibernated when this run returns. */
  park() {
    control.park();
  },
  /** Stop the run as `unsupported`, with `what` as the reason. */
  unsupported,
  state,
  vm,
  fs,
  LegionStateError,
  LegionVmError,
  LegionFsError,
};

class ProcessExit {
  constructor(code) {
    this.code = code;
  }
}

const process = {
  argv: [],
  env: {},
  /** End the run with this exit status. */
  exit(code = 0) {
    throw new ProcessExit(Number(code) | 0);
  },
};

// Node's built-in modules (Node 22): code that needs them needs a machine.
const NODE_BUILTINS = new Set(
  (
    'assert assert/strict async_hooks buffer child_process cluster console constants crypto dgram ' +
    'diagnostics_channel dns dns/promises domain events fs fs/promises http http2 https inspector ' +
    'inspector/promises module net os path path/posix path/win32 perf_hooks process punycode ' +
    'querystring readline readline/promises repl stream stream/consumers stream/promises stream/web ' +
    'string_decoder sys timers timers/promises tls trace_events tty url util util/types v8 vm wasi ' +
    'worker_threads zlib test sqlite sea'
  ).split(' '),
);

function require(name) {
  const id = String(name);
  const bare = id.startsWith('node:') ? id.slice(5) : id;
  if (id.startsWith('node:') || NODE_BUILTINS.has(bare)) {
    unsupported(`require('${bare}'): Node builtins are not available in an isolate`);
  }
  throw new Error(`Cannot find module '${id}' (packages are not installed in an isolate; bundle them or run with --tier vm)`);
}

class Worker {
  constructor() {
    unsupported('new Worker: workers are not available in an isolate');
  }
}

globalThis.legion = legion;
globalThis.process = process;
globalThis.require = require;
globalThis.Worker = Worker;
globalThis.LegionStateError = LegionStateError;
globalThis.LegionVmError = LegionVmError;

// ---------------------------------------------------------------------------
// Running an entry
// ---------------------------------------------------------------------------

// This file's name as the engine reports it in stacks.
const RUNNER_FILE = (() => {
  const m = /@(.*?):\d+:\d+/.exec(new Error().stack || '');
  return m ? m[1] : '';
})();

/** Frames of the caller's code: the entry (a relative name) and files in
 * the run directory, not the runner's or the generated bindings'. */
function callerFrame(file) {
  if (file === RUNNER_FILE) return false;
  return !file.startsWith('/') || file.startsWith(RUN_DIR + '/');
}

const MODULE_SYNTAX = /^\s*(?:import\s*(?:[\w$*{]|["'])|export\s+(?:default\b|const\b|let\b|var\b|function\b|class\b|async\b|\{|\*))/m;

function formatError(e) {
  if (!(e instanceof Error)) {
    let text;
    try {
      text = typeof e === 'string' ? e : JSON.stringify(e);
    } catch {
      text = String(e);
    }
    return `Uncaught ${text}`;
  }
  const lines = [`Uncaught ${e.name}: ${e.message}`];
  if (e instanceof SyntaxError && e.fileName && callerFrame(e.fileName)) {
    lines.push(`    at ${e.fileName}:${e.lineNumber}:${e.columnNumber}`);
  }
  for (const frame of String(e.stack || '').split('\n')) {
    const m = /^(.*?)@(.*):(\d+):(\d+)$/.exec(frame);
    if (!m || !callerFrame(m[2])) continue;
    const fn = m[1] || '<anonymous>';
    lines.push(`    at ${fn} (${m[2]}:${m[3]}:${m[4]})`);
  }
  return lines.join('\n');
}

let fresh = true;

function firstRun() {
  // The environment the runner saw while the image was built is the build
  // machine's; the isolate's comes from wasi:cli/environment.
  const env = {};
  for (const [k, v] of getEnvironment()) env[k] = v;
  process.env = env;
}

function jsonResult(value) {
  if (value === undefined) return undefined;
  try {
    const s = JSON.stringify(value);
    return s === undefined ? undefined : s;
  } catch (e) {
    console.error(`legion: the result is not JSON-serializable (${e && e.message}); it is not reported`);
    return undefined;
  }
}

async function run(req) {
  if (fresh) {
    fresh = false;
    firstRun();
  }
  const entry = req.entry;
  process.argv = ['js', entry ? RUN_DIR + '/' + entry : '', ...req.args];
  if (req.input !== undefined && req.input !== null && req.input !== '') {
    try {
      legion.input = JSON.parse(req.input);
    } catch {
      legion.input = req.input;
    }
  } else {
    legion.input = null;
  }
  if (!entry) {
    console.error('legion: no entry file was given: send the source as a file (main.js or main.mjs)');
    return { exit: 2 };
  }
  let source;
  try {
    source = fs.readText(entry);
  } catch (e) {
    console.error(`js: can't open file '${RUN_DIR}/${entry}': ${e.code || e}`);
    return { exit: 2 };
  }
  delete globalThis.result;
  let exit = 0;
  let value;
  try {
    // The entry is the body of an async function, on the wrapper's first
    // line so line numbers match the file. A top-level `result` binding
    // (let/const/var, or the global) is the result unless the entry
    // returned a value first.
    let fn;
    try {
      fn = (0, eval)(
        `(async function () {${source}\n;return typeof result === 'undefined' ? undefined : result;\n})\n//# sourceURL=${entry}`,
      );
    } catch (e) {
      // StarlingMonkey cannot link a module from source at run time
      // (import() of a file fails too), so module syntax is reported by name.
      if (e instanceof SyntaxError && MODULE_SYNTAX.test(source)) {
        unsupported(
          'ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm',
        );
      }
      throw e;
    }
    value = await fn();
  } catch (e) {
    if (e instanceof ProcessExit) {
      exit = e.code;
    } else {
      console.error(formatError(e));
      exit = 1;
    }
  }
  const result = jsonResult(value);
  return result === undefined ? { exit } : { exit, result };
}

export const runner = { run };
