# Isolate language images (`js`, `python`)

The two language images of the `isolate` task runtime: pre-initialized
WebAssembly components of the `image` world in
[`../wit/legion-isolate.wit`](../wit/legion-isolate.wit) that run a caller's
JavaScript or Python source. Each exports `legion:isolate/runner` and
imports `legion:isolate/{control,state,vm}` plus the WASI 0.2 interfaces its
toolchain needs. This page is for maintainers of the images; the public
surface is documented in `docs/task-runtime-isolate.md`.

| Path | What |
| --- | --- |
| `build.sh` | builds `js.wasm`, `python.wasm` and `manifest.json` |
| `wit/images.wit` | the `python` and `js` worlds (`legion:isolate/image` plus `wasi:http/outgoing-handler`, `wasi:cli/environment`, `wasi:filesystem`) |
| `wit/deps/` | WASI 0.2.12 WIT, copied from `wasmtime-wasi-http` 50.0.1 |
| `python/_legion_runner.py` | the Python runner (componentize-py app) |
| `python/legion.py` | the `legion` module scripts import |
| `js/runner.js` | the JS runner (componentize-js source) |
| `js/build.mjs` | calls componentize-js |
| `requirements.txt`, `js/package.json`, `js/package-lock.json` | the pinned toolchains |
| `../tests/images.rs` | tests, measurements, and the manifest writer |

## Building

```sh
crates/isolate/images/build.sh                 # both images and manifest.json into target/isolate-images/
crates/isolate/images/build.sh --out /tmp/img python
crates/isolate/images/build.sh --no-manifest js
```

Needs `python3` (with `venv` and `pip`), `node` 20+ and `npm`, and `cargo`
for the manifest step. Each missing tool stops the script with a message
naming it. The images are not checked in (13 MB and 30 MB) and `target/` is
git-ignored.

Toolchains are installed on first use into `target/isolate-images-tools/`
(`LEGION_IMAGES_TOOLS` moves it). To reuse installs that already exist:

| Variable | Points at |
| --- | --- |
| `LEGION_COMPONENTIZE_PY` | a `componentize-py` binary (must report the pinned version) |
| `LEGION_JS_NODE_MODULES` | a `node_modules` directory holding the pinned `@bytecodealliance/componentize-js` |
| `LEGION_JS_AOT=1` | (for `js/build.mjs` only) builds the weval AOT variant, for measuring it |

`build.sh` stages one WIT directory (`<out>/.wit`) from `wit/` and the host
crate's `legion-isolate.wit`, which stays the single source of the
`legion:isolate` package. componentize-py bundles every file under its `-p`
path, so the Python image is built from a copy holding only the two runner
files.

### Pinned versions and how to bump them

| Tool | Version | Pinned in |
| --- | --- | --- |
| componentize-py | 0.25.1 (CPython 3.14.0) | `requirements.txt` (sha256 of every wheel and the sdist), `COMPONENTIZE_PY_VERSION` in `build.sh`, `runtime` in `tests/images.rs` |
| componentize-js | 0.23.0 (StarlingMonkey) | `js/package.json` (exact), `js/package-lock.json`, `COMPONENTIZE_JS_VERSION` in `build.sh`, `runtime` in `tests/images.rs` |

To bump componentize-py: change the version in `requirements.txt` and take
every file's sha256 from `https://pypi.org/pypi/componentize-py/<version>/json`;
update `COMPONENTIZE_PY_VERSION` and the `runtime` string. To bump
componentize-js: set the exact version in `js/package.json`, run
`npm install --package-lock-only` beside it to regenerate the lockfile, and
update `COMPONENTIZE_JS_VERSION` and the `runtime` string. Then rebuild, run
the test suite below, and update the measurements on this page.

Builds are not bit-reproducible: two builds from the same inputs differ
(each snapshot captures the build's random state, and CPython's string-hash
seed). The manifest records the sha256 of the artifact that was built; a
release pins that artifact.

## `manifest.json`

The worker reads it. `build.sh` writes it by running the built images under
the host library (`cargo test -p legion-isolate --test images -- --ignored
--exact manifest_write_test`, with `LEGION_ISOLATE_MANIFEST_WRITE=1`):

```json
{ "images": {
    "python": { "file": "python.wasm", "sha256": "<hex>", "extensions": [".py"], "vmEntry": "main.py",
                "modules": ["__future__", "_abc", "...", "zoneinfo"],
                "initialMemoryBytes": 24313856, "idlePrivateBytes": 1397063,
                "runtime": "CPython 3.14.0 (componentize-py 0.25.1)" },
    "js":     { "file": "js.wasm", "sha256": "<hex>", "extensions": [".js", ".mjs"], "vmEntry": "main.mjs",
                "modules": [],
                "initialMemoryBytes": 9502720, "idlePrivateBytes": 734494,
                "runtime": "StarlingMonkey (componentize-js 0.23.0)" } } }
```

- `modules` is what the node's pre-flight import check compares a source's
  top-level imports against. For Python it is generated from the image: the
  candidates are `sys.stdlib_module_names`, `sys.builtin_module_names` and
  the top-level names `pkgutil.iter_modules()` finds in the standard
  library, read inside the image; each is kept when `import <name>`
  succeeds in a fresh instance of the built image; `legion` is added;
  `ctypes` (a stub, below) and componentize-py's own runtime module are
  left out. The current image lists 246 names. For JS it is empty: the
  image resolves no module specifier by itself (no packages, no Node
  builtins, no ES modules from source).
- `initialMemoryBytes` is the component's largest initial linear memory.
- `idlePrivateBytes` is measured when the manifest is written: 200
  instances, each called once with a hello-world and kept alive, the
  `Private_Dirty` delta of `/proc/self/smaps_rollup` divided by 200. It is a
  measurement of the build machine.

## What each runner does

Both runners `chdir` (Python) or resolve relative paths (JS) to the run
directory `/legion/run`, read `request.entry` from it, and run it.

| | Python | JavaScript |
| --- | --- | --- |
| Entry | executed as `__main__` with `sys.argv = [entry, *args]`; `/legion/run` is `sys.path[0]` | the body of an async function: top-level `await` and `return` work |
| Result | a top-level `result` that is JSON-serializable | the entry's return value, else a top-level `result` binding, else `globalThis.result` |
| No result | unset: the host falls back to `result.json` | same |
| Exit status | 0; `sys.exit(n)` / `SystemExit(n)` → `n` (None → 0, a string → printed to stderr, 1); uncaught exception → traceback on stderr, 1 | 0; `process.exit(n)` → `n`; uncaught error or awaited rejection → `Uncaught <Name>: <message>` and `at fn (file:line:col)` frames on stderr, 1 |
| No entry / missing file | exit 2 with a message on stderr | exit 2 with a message on stderr |
| stdout / stderr | `print` / `sys.stderr` (both line-buffered) | `console.log` → stdout, `console.error` → stderr |
| Environment | `os.environ` (re-read from the host at the instance's first run) | `process.env` (same) |
| Tracebacks | name the entry file and line with source; standard-library frames have no source line | name the entry file with line and column; runner frames are removed |

A result that is not JSON-serializable is not reported, with a one-line note
on stderr.

### The `legion` API

Python: `import legion`. JavaScript: the global `legion`.

| Python | JavaScript | |
| --- | --- | --- |
| `legion.input` | `legion.input` | the run's JSON input, parsed; `None` / `null` without input |
| `legion.info()` | `legion.info()` | `job`, `task`, `lifetime` (`request`/`tenant`/`session`), `tenant`, `session`, `resumed` |
| `legion.park()` | `legion.park()` | a session isolate may hibernate when the run returns |
| `legion.state.get(k)` → `bytes`/`None` | `legion.state.get(k)` → `Uint8Array`/`null` | state of the tenant or session |
| `.set(k, bytes \| str)`, `.delete(k)`, `.keys()` | same (`Uint8Array \| string`) | `str` is stored as UTF-8 |
| `legion.StateError` (`.kind`: `not-available`, `quota`; `.quota`) | `LegionStateError` (same) | a request isolate has no state |
| `legion.fetch(url, method="GET", headers=None, body=None, timeout=None)` → `Response` (`status`, `headers`, `body`, `.text()`, `.json()`, `.header(name)`, `.ok`) | `fetch` (StarlingMonkey's own) | over `wasi:http/outgoing-handler`, through the node's egress policy |
| `legion.FetchError` (`.kind` = the `wasi:http` error code, e.g. `http-request-denied`) | `TypeError` from `fetch` | no response arrived |
| `legion.vm.acquire()`, `legion.vm.attach(id)` → machine | same | a child VM of the task's VM task |
| `m.id`, `m.exec(argv, stdin=b"", timeout_ms=60000)` → `exit`, `stdout`, `stderr`, `timed_out` | `m.id`, `m.exec(argv, { stdin, timeoutMs })` → `{ exit, stdout, stderr, timedOut }` | |
| `m.put(path, data)`, `m.get(path)`, `m.fetch(port, method, path, headers, body)` → `Response`, `m.resize(memory_mib, memory_max_mib=None, cpu_mhz=None)`, `m.release()` | `m.put`, `m.get`, `m.fetch(port, { method, path, headers, body })` → `Response`, `m.resize({ memoryMib, memoryMaxMib, cpuMhz })`, `m.release()` | |
| `legion.VmError` (`.kind`: `not-configured`, `resource-exhausted`, `child-gone`, `needs-restart`, `failed`) | `LegionVmError` (same) | |
| `legion.unsupported(what)` | `legion.unsupported(what)` | stops the run as `unsupported` with `what` |
| | `legion.fs.readFile`, `readText`, `writeFile`, `list`, `mkdir(path, { recursive })`, `remove`, `exists` | the run directory through `wasi:filesystem`; errors are `LegionFsError` with `.code` (`no-entry`, ...) |

In Python, `http.client.HTTPConnection.request`/`getresponse` and
`urllib.request.urlopen` (http and https) send through `legion.fetch`.
CPython for WASI has no `ssl`, so the image adds an `HTTPSConnection` that
does the same; TLS is the node's. Headers `wasi:http` refuses (`host`,
`connection`, `keep-alive`, `proxy-connection`, `te`, `transfer-encoding`,
`upgrade`, `http2-settings`) are dropped from requests. Responses are
buffered whole.

`asyncio` runs on an image-provided event loop (`asyncio.IsolateEventLoop`):
coroutines, tasks, `gather`, `sleep` and timers work; a loop with nothing
scheduled raises `RuntimeError` rather than wait forever.

JavaScript provides a minimal `process`: `exit(n)`, `argv`
(`['js', '/legion/run/<entry>', ...args]`) and `env`.

### What is stubbed as unsupported

Using these (importing them is fine) calls `control.unsupported` with the
detail below, which stops the run with `stopped.reason = "unsupported"`.
Output printed before the call is kept.

Python, `P` = `processes are not available in an isolate`,
`S` = `sockets are not available in an isolate; use legion.fetch or run with --tier vm`:

| Call | Detail |
| --- | --- |
| `subprocess.run`, `call`, `check_call`, `check_output`, `getoutput`, `getstatusoutput` | `subprocess.<name>: P` |
| `subprocess.Popen(...)` | `subprocess.Popen: P` |
| `os.system`, `popen`, `fork`, `forkpty`, `execl`, `execle`, `execlp`, `execlpe`, `execv`, `execve`, `execvp`, `execvpe`, `spawnl`, `spawnle`, `spawnlp`, `spawnlpe`, `spawnv`, `spawnve`, `spawnvp`, `spawnvpe`, `posix_spawn`, `posix_spawnp` | `os.<name>: P` |
| `multiprocessing.Process(...).start()` | `multiprocessing.Process.start: P` |
| `multiprocessing.Pool`, `multiprocessing.Manager` (also on contexts) | `multiprocessing.<name>: P` |
| `concurrent.futures.ProcessPoolExecutor(...)` | `concurrent.futures.ProcessPoolExecutor: P` |
| `threading.Thread(...).start()`, `_thread.start_new_thread` | `threading.Thread.start: threads are not available in an isolate` (`_thread.start_new_thread: ...`) |
| `ctypes.<anything>` | `ctypes.<name>: native libraries are not available in an isolate` |
| `socket.socket(...)`, `create_connection`, `create_server`, `socketpair`, `fromfd`, `getaddrinfo`, `gethostbyname`, `gethostbyname_ex`, `gethostbyaddr` | `socket.<name>: S` |
| `http.client.HTTPConnection.connect` (the `putrequest`/`endheaders` path) | `http.client.HTTPConnection.connect: S` |
| asyncio loop `create_connection`, `create_server`, `create_datagram_endpoint`, `create_unix_connection`, `create_unix_server`, `getaddrinfo`, `getnameinfo`, `sock_connect`, `sock_accept`, `sock_recv`, `sock_sendall`, `connect_accepted_socket` | `asyncio.loop.<name>: S` |
| asyncio loop `subprocess_exec`, `subprocess_shell` (`asyncio.create_subprocess_*`) | `asyncio.loop.<name>: P` |

CPython for WASI leaves most of the `os` process functions out. Those that
exist are replaced; those that do not are served (by a module `__getattr__`
on `os`) only to the caller's own code — the entry, files in the run
directory, `exec`'d text — so the standard library's `hasattr(os, "fork")`
feature tests still see them absent. Threads cannot start in CPython for
WASI (`RuntimeError: can't start new thread`), hence the threading stub.
`ctypes` is not built for WASI; the stub package makes its use stop the run
by name, and it is left out of `modules` so the pre-flight check sends code
that imports it to a VM.

JavaScript:

| Call | Detail |
| --- | --- |
| `require('<Node builtin>')` (with or without `node:`; any `node:` specifier) | `require('<name>'): Node builtins are not available in an isolate` |
| `new Worker(...)` | `new Worker: workers are not available in an isolate` |
| an entry with `import`/`export` statements | `ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm` |

`require` of anything else throws an ordinary, catchable
`Error("Cannot find module 'x' (packages are not installed in an isolate; bundle them or run with --tier vm)")`.
The Node builtin list is Node 22's `module.builtinModules` (without its
`_`-prefixed internals) plus the `node:`-only `test`, `sqlite` and `sea`.

## Persistence

With `lifetime = "tenant"` or `"session"` the same instance runs many
times. Python keeps one `__main__` namespace per instance: globals assigned
by one run are there in the next (except `result`, cleared before each run).
JavaScript keeps `globalThis`; the entry's own top-level `let`/`const`/`var`
are local to its run. A `request` isolate is a fresh instance every run.
The run directory is emptied between runs, so Python forgets modules it
imported from it at the start of the next run. After a wake from
hibernation memory is fresh: `legion.info()["resumed"]` is true and only
`legion.state` survived.

## Randomness and identity after pre-initialization

Everything seeded while the image was built is in every instance's memory.
On each instance's first run the Python runner reseeds `random`'s global
generator from `os.urandom` (`wasi:random`, fresh per instance), resets
`uuid`'s clock-sequence state and `tempfile`'s name generator, and re-reads
the environment. `os.urandom`, `secrets` and `uuid.uuid4` read `wasi:random`
directly. In JavaScript, `Math.random`, `crypto.getRandomValues` and
`crypto.randomUUID` were measured to differ between instances with no
runner change; the runner only re-reads the environment. The tests hold
both images to this (two instances never produce the same values).

CPython's string-hash seed is fixed when the image is built and shared by
every instance of that image (it is part of the snapshot and cannot be
changed after interpreter start).

## The standard library (Python)

componentize-py snapshots the interpreter after the runner module ran but
does not ship the library's files: at run time `/python` does not exist, and
a module that was not imported while the image was built cannot be imported
at all. The runner therefore compiles the whole standard library during the
build (520 modules, `marshal` + zlib, one 4.3 MB bytes object in the
snapshot) and installs a `sys.meta_path` finder that loads a module from it
on first import. Left out: `test`, `idlelib`, `tkinter`, `turtle`,
`turtledemo`, `ensurepip`, `venv`, `pydoc_data`, `antigravity`, `this`,
`__hello__`, `__phello__`, `lib2to3`, and `ctypes`. Modules with C parts not
built for WASI (`ssl`, `bz2`, `lzma`, `mmap`, `curses`, `readline`, ...)
fail to import, and are not in `modules`.

### Preloaded modules

`base64 collections csv dataclasses datetime decimal functools hashlib io
itertools json math random re statistics string textwrap time typing uuid`
are imported into the snapshot. Measured with and without them (release
host, 50 runs, fresh instance per run): a run importing all twenty took
2.2 ms p50 with the preload and 10.7 ms without; hello world 2.6 ms and
3.5 ms (the first run imports `random`, `uuid` and `tempfile` to reseed);
idle private memory 1.5 MiB and 3.0 MiB. The preload costs about 0.2 MB of
image size.

## Known limits

- **Python packages**: only the standard library. No `pip`; packages are a
  VM's job (or a custom image).
- **JS packages and modules**: no npm packages, no Node builtins, no ES
  modules. StarlingMonkey 0.23 cannot link a module from source at run
  time: `import()` of a run-directory file fails with
  `InternalError: module record has unexpected status: New`.
- **JS throughput**: StarlingMonkey is an interpreter; a 12 KB snippet takes
  about 2 to 3 ms. weval AOT does not help dynamic source (below).
- **Python start**: a fresh Python instance costs about 2.5 ms to a
  hello-world (release host), against about 1 ms for JS.
- **Unhandled rejections**: a promise rejection that the entry never awaits
  does not fail the run.
- `http.client`'s low-level request API and `set_tunnel` are not shimmed.

## Measurements

Measured on a 4-vCPU Firecracker guest (Intel Xeon @ 2.30 GHz, 15 GiB,
shared with other builds), wasmtime 50.0.1 with the host's pooling engine,
by `cargo test --release -p legion-isolate --test images -- --ignored
--test-threads=1 measurements --nocapture`. "Fresh" is a new instance per
run (store, instantiate, run), "warm" is a run on an instance that already
ran; p50 over 50 runs. Idle is private memory per instance, N = 200.

| | js | js + weval AOT | python | python without preload |
| --- | --- | --- | --- | --- |
| Image size | 13.1 MB | 19.1 MB | 29.5 MB | 29.3 MB |
| `initialMemoryBytes` | 9.5 MB | | 24.3 MB | |
| Hello world, fresh p50 / p99 | 1.14 / 2.00 ms | 0.94 / 1.81 ms | 2.55 / 5.99 ms | 3.51 / 4.74 ms |
| Hello world, warm p50 | 107 µs | 155 µs | 171 µs | 88 µs |
| ~12 KB snippet, fresh p50 / p99 | 2.86 / 3.95 ms | 2.70 / 3.25 ms | 12.0 / 19.3 ms | 12.7 / 15.9 ms |
| ~12 KB snippet, warm p50 | 1.86 ms | 1.96 ms | 8.0 ms | 7.9 ms |
| 20 common imports, fresh p50 | | | 2.2 ms | 10.7 ms |
| Idle private memory | 767 KiB | 883 KiB | 1,519 KiB | 3,098 KiB |

AOT stays off: no consistent gain on dynamic source in these single runs,
6 MB more image and more idle memory. The debug-build host (the test
command without `--release`) shows the same memory and slower times
(python hello world fresh about 13 ms, js about 3.5 ms): its wasmtime is
unoptimized.

## Tests

```sh
crates/isolate/images/build.sh
cargo test -p legion-isolate -j 3 --test images -- --ignored --nocapture
```

Every test in `../tests/images.rs` is ignored (CI does not build images).
`LEGION_ISOLATE_IMAGES` names the images directory (default
`target/isolate-images`); a missing image fails with a message saying how to
build it. Compiled artifacts are cached in `<images>/.cache/`. The tests run
each image under the host's own linker and guest context with a fake
worker: hello world, input and result, `result.json`, files in the run
directory, exit codes and tracebacks, persistence across runs of one
instance and not across instances, every stub's detail string,
`legion.state`, `legion.fetch`/`fetch` and the `http.client`/`urllib` shims
through a fake egress, `legion.vm` through a fake VM backend, the standard
library (asyncio, `strptime`, xml, email, sqlite3), restore hygiene, the
measurements above, and the manifest writer.
