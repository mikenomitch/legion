# Task Runtime: isolate

`isolate` is a built-in task runtime that runs code in WebAssembly isolates
inside a node's `legion-isolate` host process, many tenants to a process,
and sends the code that needs a real machine to a microVM instead. The pieces
are the `legion-isolate` binary (`crates/isolate/`, the only part of Legion
that links the WebAssembly engine), the worker's isolate supervisor
(`crates/worker/src/worker/isolates/`), and the jobspec rules in
`crates/common/src/structs/isolate.rs`.

## Purpose

A platform that runs model-written or user-written snippets, webhooks, or
small per-tenant handlers pays for a whole VM per call with the
[`sandbox`](task-runtime-sandbox.md) runtime, even when the code is ten lines
of Python that parses JSON. An isolate is a WebAssembly instance: it starts
in microseconds to a few milliseconds, takes about a megabyte when idle, and
needs no kernel of its own. Thousands share one host process.

Use an isolate for:

- short snippets of Python or JavaScript that use the standard library and
  HTTP;
- per-tenant handlers and webhooks that answer HTTP;
- agent tool calls and notebooks whose state fits in a key-value map;
- your own code compiled to a WebAssembly component (Rust, or anything that
  targets WASI 0.2).

Do not use one for:

- **CPU-bound JavaScript.** The `js` image is an interpreter (StarlingMonkey,
  no JIT). A 12 KB snippet takes about 3 ms where Node takes well under one.
- **Native packages.** `numpy`, `pandas`, npm packages, anything with a C
  extension or a `node_modules`: the images carry the Python standard library
  and a JavaScript engine, nothing else.
- **POSIX.** No subprocesses, no threads, no sockets, no real filesystem, no
  `ctypes`.

Code that needs any of those runs in the task's **VM tier**: a fresh
single-use microVM of a sibling `sandbox` task. A run happens in exactly one
place. Legion picks the tier before anything runs, never moves a run between
tiers, and never runs your code twice on its own. When an isolate run stops
because the code needed a VM, the reply says so (`nextTier = "vm"`), and
running it again is the caller's choice.

The two tiers, side by side:

| | Isolate tier | VM tier |
| --- | --- | --- |
| What runs | a WebAssembly instance in a shared host process | a fresh single-use `sandbox` session |
| Start | microseconds (a Rust component) to a few ms (the language images) | a microVM boot ([sandbox](task-runtime-sandbox.md#how-fast-a-sandbox-boots)) |
| Memory | about 0.75 to 1.5 MiB idle per language instance | the VM task's `resources` |
| Languages | `js`, `python` (standard library only), any WASI 0.2 component | whatever the VM image has |
| Boundary | WebAssembly software isolation inside a confined process, shared by its [cordon](#cordons-who-shares-a-process) | a hypervisor |

## Requirements

- **x86-64 Linux workers.** A worker on any other platform does not run
  isolates (`isolates run on x86-64 Linux only for now`).
- **The `legion-isolate` binary** next to the worker. A worker that finds one
  that speaks its protocol advertises the `isolates` capability; an isolate
  task's envelope carries an implicit `capabilities contains "isolates"`
  constraint, so it is placed only there. See
  [Installing the binary and images](#installing-the-binary-and-images).
- **The built images** (`js.wasm`, `python.wasm`, `manifest.json`) for
  `image` tasks. A `module` task needs none.
- **A sandbox-capable node** (KVM and a VMM,
  [installation](installation.md#linux-the-sandbox-runtime-microvms)) only
  for the VM tier and child VMs.

## A complete example

`configs/jobs/isolate.ncl` runs untrusted Python snippets as isolates and
gives them a microVM to fall back to:

```nickel
{
  name = "code-runner",
  tasks = [
    {
      name = "run",
      runtime = "isolate",
      config = {
        image = "python",
        lifetime = "request",
        limits = { cpu = "2s", wall = "30s", output = "256KiB" },
        vm = { task = "machine" },
      },
      resources = { cpu = 10, memory = 16, memoryMax = 256 },
      stateful = {
        mode = "session",
        envelope = { resources = { cpu = 16000, memory = 32768 } },
      },
      children = { tasks = ["machine"] },
    },
    {
      name = "machine",
      runtime = "sandbox",
      config = {
        image = "python:3.13-slim",
        command = ["sleep", "infinity"],
        vcpus = 1,
        network = { mode = "none" },
      },
      resources = { cpu = 1000, memory = 1024 },
      stateful = {
        mode = "session",
        envelope = {
          resources = { cpu = 8000, memory = 16384 },
          max = { cpu = 32000, memory = 65536 },
        },
      },
      run = { command = ["python3", "/legion/run/main.py"], wall = "5m" },
    },
  ],
}
```

An isolate task is a [session task](session-mode.md): submitting it places
one envelope per eligible node (`run-envelope`), and nothing per isolate is
ever written to the store. With 16 MiB charged per resident isolate, each
32 GiB envelope holds about 2,000 resident isolates.

```console
$ legion job run configs/jobs/isolate.ncl
$ cat main.py
import json, legion
print(sum(range(legion.input["n"])))
result = {"n": legion.input["n"]}
$ legion session run code-runner --task run --file main.py --input '{"n": 3}' -j
{"ok":true,"data":{"task":"run","node":"w-17","sessionId":"run-q4n7c2rd.01jc0...",
 "runId":"run-q4n7c2rd.01jc0...","exit":0,"stdout":"3\n","stderr":"","truncated":false,
 "result":{"n":3},"files":[],"wallMs":2.914,"cpuMs":2.47,"stopped":null,
 "deduplicated":false,"tier":"isolate","tierReason":"auto: no imports outside the image",
 "nextTier":null,"isolateStart":"fresh"}}
```

`--task run` may be left out: a job's VM task is never the default.
Without `-j` the program's stdout goes to stdout, and Legion's notes go to
stderr:

```console
$ legion session run code-runner --file main.py --input '{"n": 3}'
3
legion: result: {"n": 3}
legion: ran in the isolate tier (auto: no imports outside the image)
```

A run that calls something an isolate does not have **stops**. That is a
successful reply, not an error, and the exit status of the CLI is 125:

```console
$ cat shell.py
import subprocess
print(subprocess.run(["uname", "-a"], capture_output=True).stdout)
$ legion session run code-runner --file shell.py -j
{"ok":true,"data":{"task":"run", ... "exit":null, ...
 "stopped":{"reason":"unsupported","limit":"",
   "detail":"subprocess.run: processes are not available in an isolate",
   "hint":"rerun with --tier vm (this code's next auto run on this node goes to a VM on its own), or pass --fallback vm to rerun it once automatically"},
 "tier":"isolate","tierReason":"auto: no imports outside the image","nextTier":"vm",
 "isolateStart":"fresh"}}
```

Run it in the VM tier yourself:

```console
$ legion session run code-runner --file shell.py --tier vm -j
{"ok":true,"data":{"task":"machine","node":"w-17","sessionId":"machine-q4n7c2rd.01jc1...",
 ... "exit":0,"stdout":"b'Linux ...'\n", ... "wallMs":61,"cpuMs":0, ...
 "tier":"vm","tierReason":"requested","nextTier":"vm","vmMs":388}}
```

Or let the CLI send the second request when the first stops (`--fallback
vm`). This runs the code twice, the second time in a VM, and says so:

```console
$ legion session run code-runner --file shell.py --fallback vm -j
{"ok":true,"data":{"task":"machine", ... "exit":0, ...
 "tier":"vm","tierReason":"fallback: stopped in the isolate, unsupported (subprocess.run)",
 "nextTier":"vm","vmMs":402,"ranTwice":true}}
```

Ask where a run would go without running it:

```console
$ cat stats.py
import numpy, json
$ legion session run code-runner --file stats.py --dry-run -j
{"ok":true,"data":{"dryRun":true,"task":"run","node":"w-17","hasRoom":true,"reason":null,
 "command":[],"wall":"30s","outputBytes":262144,"tier":"vm",
 "tierReason":"auto: import numpy is not in image python",
 "imports":["numpy","json"],"missingImports":["numpy"]}}
```

The `vm` dry run's `hasRoom` speaks for the VM task's envelope.

## What runs

A task runs exactly one of a built-in **image** or your own **module**.

### Images

An image is an interpreter compiled to a WebAssembly component and
pre-initialized, so a new instance starts with the interpreter already up.
Each run brings its source as a file, or the task names one script that
every invocation runs (`config.entry`).

| | `js` | `python` |
| --- | --- | --- |
| Engine | StarlingMonkey (SpiderMonkey compiled to WebAssembly, no JIT), built with componentize-js 0.23.0 | CPython 3.14.0, built with componentize-py 0.25.1 |
| Entry | a script, run as the body of an async function: top-level `await` and `return` work. No ES modules: an entry with `import` or `export` statements stops the run as `unsupported` | `__main__`, with `sys.argv = [entry, *args]` and `/legion/run` as `sys.path[0]` |
| Libraries | the engine's built-ins and web APIs (`fetch`, `crypto`, `TextEncoder`, ...). No npm packages, no Node builtins | the whole standard library, loaded from a compressed copy in the image on first import. No packages beyond it, and no modules whose C parts are not built for WASI (`ssl`, `bz2`, `lzma`, `mmap`, `curses`, `readline`, ...) |
| Result | the entry's return value, else a top-level `result` binding, else `globalThis.result` | a top-level `result` that is JSON-serializable |
| Exit status | 0; `process.exit(n)` is `n`; an uncaught error or an awaited rejection prints `Uncaught <Name>: <message>` with frames on stderr and is 1 | 0; `sys.exit(n)` is `n`; an uncaught exception prints its traceback on stderr and is 1 |
| No entry file | exit 2 with a message on stderr | exit 2 with a message on stderr |
| Output | `console.log` to stdout, `console.error` to stderr | `print`, `sys.stderr`; both line-buffered |
| Environment | `process.env`; `process.argv` is `['js', '/legion/run/<entry>', ...args]` | `os.environ` |

A result that is not JSON-serializable is left out, with a one-line note on
stderr. Without a result, the run's `result` is `result.json` from the run
directory when the code wrote one that parses as JSON.

Python's `asyncio` runs on an image-provided event loop: coroutines, tasks,
`gather`, `sleep`, and timers work, and a loop with nothing scheduled raises
`RuntimeError` instead of waiting forever. `urllib.request.urlopen` and
`http.client.HTTPConnection.request`/`getresponse` (http and https) send
through [outbound HTTP](#outbound-http). Twenty common modules (`json`, `re`,
`datetime`, `decimal`, `random`, `uuid`, ...) are imported into the image
when it is built, so importing them costs nothing.

#### The `legion` API

Python: `import legion`. JavaScript: the global `legion`.

| Python | JavaScript | What it does |
| --- | --- | --- |
| `legion.input` | `legion.input` | The run's JSON input, parsed. `None` / `null` without one. |
| `legion.info()` | `legion.info()` | `job`, `task`, `lifetime` (`request`, `tenant`, `session`), `tenant`, `session`, `resumed`. |
| `legion.state.get(k)`, `.set(k, v)`, `.delete(k)`, `.keys()` | the same | The tenant's or session's [state](#state). Values are bytes; a `str` is stored as UTF-8. Python returns `bytes` or `None`, JavaScript `Uint8Array` or `null`. |
| `legion.StateError` (`.kind` `not-available` or `quota`, `.quota`) | `LegionStateError` | A request isolate has no state; a write past the quota fails. |
| `legion.fetch(url, method="GET", headers=None, body=None, timeout=None)` returning a `Response` (`status`, `headers`, `body`, `.text()`, `.json()`, `.header(name)`, `.ok`) | `fetch` (the engine's own) | [Outbound HTTP](#outbound-http). |
| `legion.FetchError` (`.kind` is the `wasi:http` error code, e.g. `http-request-denied`) | a `TypeError` from `fetch` | No response arrived. |
| `legion.vm.acquire()`, `legion.vm.attach(id)` | the same | A [child VM](#child-vms). |
| `legion.VmError` (`.kind` `not-configured`, `resource-exhausted`, `child-gone`, `needs-restart`, `failed`) | `LegionVmError` | A child VM call failed. |
| `legion.park()` | `legion.park()` | A session isolate may [hibernate](#hibernation-and-memory) as soon as this call returns. |
| `legion.unsupported(what)` | `legion.unsupported(what)` | Stop the run as `unsupported`, with `what` as the detail. |
| | `legion.fs.readFile`, `readText`, `writeFile`, `list`, `mkdir(path, { recursive })`, `remove`, `exists` | The run directory. Errors are `LegionFsError` with a `.code` (`no-entry`, ...). Python uses `open()` and `os`. |

A machine from `legion.vm`:

| Python | JavaScript |
| --- | --- |
| `m.id` | `m.id` |
| `m.exec(argv, stdin=b"", timeout_ms=60000)` returning `exit`, `stdout`, `stderr`, `timed_out` | `m.exec(argv, { stdin, timeoutMs })` returning `{ exit, stdout, stderr, timedOut }` |
| `m.put(path, data)`, `m.get(path)` | `m.put(path, data)`, `m.get(path)` |
| `m.fetch(port, method, path, headers, body)` returning a `Response` | `m.fetch(port, { method, path, headers, body })` |
| `m.resize(memory_mib, memory_max_mib=None, cpu_mhz=None)` | `m.resize({ memoryMib, memoryMaxMib, cpuMhz })` |
| `m.release()` | `m.release()` |

#### What stops a run as `unsupported`

Importing these is fine; using them stops the run with `stopped.reason =
"unsupported"` and the detail below. Output printed before the call is kept.

Python, where `P` is `processes are not available in an isolate` and `S` is
`sockets are not available in an isolate; use legion.fetch or run with --tier vm`:

| Call | Detail |
| --- | --- |
| `subprocess.run`, `call`, `check_call`, `check_output`, `getoutput`, `getstatusoutput` | `subprocess.<name>: P` |
| `subprocess.Popen(...)` | `subprocess.Popen: P` |
| `os.system`, `popen`, `fork`, `forkpty`, the `exec*` and `spawn*` families, `posix_spawn`, `posix_spawnp` | `os.<name>: P` |
| `multiprocessing.Process(...).start()`, `multiprocessing.Pool`, `multiprocessing.Manager`, `concurrent.futures.ProcessPoolExecutor(...)` | `multiprocessing.Process.start: P`, `multiprocessing.<name>: P`, `concurrent.futures.ProcessPoolExecutor: P` |
| `threading.Thread(...).start()`, `_thread.start_new_thread` | `threading.Thread.start: threads are not available in an isolate` |
| `ctypes.<anything>` | `ctypes.<name>: native libraries are not available in an isolate` |
| `socket.socket(...)`, `create_connection`, `create_server`, `socketpair`, `fromfd`, `getaddrinfo`, `gethostbyname`, `gethostbyname_ex`, `gethostbyaddr` | `socket.<name>: S` |
| `http.client.HTTPConnection.connect` (the `putrequest`/`endheaders` path) | `http.client.HTTPConnection.connect: S` |
| asyncio loop socket methods (`create_connection`, `create_server`, `sock_*`, `getaddrinfo`, ...) | `asyncio.loop.<name>: S` |
| asyncio `subprocess_exec`, `subprocess_shell` | `asyncio.loop.<name>: P` |

JavaScript:

| Call | Detail |
| --- | --- |
| `require('<Node builtin>')`, with or without `node:` | `require('<name>'): Node builtins are not available in an isolate` |
| `new Worker(...)` | `new Worker: workers are not available in an isolate` |
| an entry with `import`/`export` statements | `ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm` |

`require` of anything else throws an ordinary, catchable `Error("Cannot find
module 'x' (packages are not installed in an isolate; bundle them or run
with --tier vm)")`. It does not stop the run.

#### `entry`

Without `config.entry`, every run brings its code: the file named
`main.<ext>` (`main.mjs` then `main.js` for `js`, `main.py` for `python`),
else the first file the run sends. With `config.entry`, the task delivers
one script through its `artifacts`, and that script is the entry of every
run:

```nickel
config = { image = "python", entry = "main.py", limits = { cpu = "100ms", wall = "5s" } },
artifacts = [{ source = "https://example.com/handlers/main.py", dest = "main.py" }],
```

`entry` is a path relative to the task directory where the artifacts are
staged, or `${artifact.<name>}`. Artifacts on an isolate task exist only to
deliver the entry: an isolate task with artifacts and no `entry` is refused.

- **The files a run sends are data.** They are staged beside the entry
  under `/legion/run` and never picked as the entry. A run file with the
  entry's own name is refused:

  ```text
  INVALID_ARGUMENT file main.py has the name of task run's config.entry, which is the entry of every run; the run's files are data
  Hint: rename the file
  ```

- **VM-tier runs get the entry too,** staged as the image's VM entry
  (`/legion/run/main.py` or `/legion/run/main.mjs`).
- **The node reads the entry without following links.** It opens each
  path component beneath the staging directory with no symlink followed
  (`..` and absolute paths refused), and takes only a regular file of at
  most 3 MiB. Anything else leaves the task unable to run on that node,
  with a `problem` that runs answer as `FAILED_PRECONDITION`:

  ```text
  config.entry "src/main.py" cannot be used: main.py is a symlink (or reached through one), which an entry may not be
  ```

### Modules

A module is your own WebAssembly component:

```nickel
config = {
  module = {
    source = "https://example.com/handler.wasm",
    sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
  },
  limits = { cpu = "50ms", wall = "10s" },
},
```

`source` is an `https://`, `http://`, `s3://`, `oci://`, or `file://` URL,
fetched once per node through the worker's [artifact](artifacts.md)
fetchers. A `file://` source makes the task host-level, as a `file://`
artifact does: the root worker reads the node's filesystem, so submitting
the job needs a `job:write` rule with `host = true`
([Host-level jobs](auth-and-acls.md#host-level-jobs)). Otherwise an
isolate task is host-level only for the reasons any task is (a `file://`
artifact, an artifact that presents the node's identity); the envelope
generated for it never is. `sha256` is required (64 hex digits, or `sha256:<hex>`): a
task always runs exactly the module it was submitted with. What a module may
import and must export is in [The guest's world](#the-guests-world).

## Config Schema

Every config struct refuses unknown fields: `lifetme = "tenant"` fails at
`legion job validate`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `image` | string | | `"js"` or `"python"`, optionally pinned to a build as `"python@sha256:<64 hex>"`. A pinned image runs only on nodes whose build has that digest. Exactly one of `image` and `module`. |
| `module` | `{ source, sha256 }` | | Your component ([Modules](#modules)). |
| `entry` | string | none | Images only: the script every invocation runs ([`entry`](#entry)). |
| `lifetime` | `"request"`, `"tenant"`, `"session"` | `"request"` | How long an instance lives ([Lifetimes](#lifetimes)). |
| `limits.cpu` | duration | **required** | CPU per invocation, its turns summed, e.g. `"100ms"`. At most `limits.wall`. |
| `limits.wall` | duration | **required** | Wall-clock time per invocation, at most `24h`. |
| `limits.output` | size | `"1MiB"` | The most stdout plus stderr one invocation returns; also the maximum. Output past it is dropped and the reply says `truncated`. The run is not stopped. |
| `cordon` | `"namespace"`, `"job"`, `"shared"` | `"namespace"` | Which host process the task's isolates share ([Cordons](#cordons-who-shares-a-process)). `"shared"` only where a namespace policy allows it. |
| `vm.task` | string | the only entry in `children.tasks` | The sibling sandbox session task a `tier = "vm"` run executes in. |
| `vm.memoFor` | duration | `"1h"` | How long a node remembers that a piece of code needed a VM, and sends its `auto` runs straight to one. `"0s"` remembers nothing. |
| `env` | map | `{}` | Environment variables every isolate of the task sees. |

The VM tier exists only with a `vm` block. `vm = {}` takes the task's only
`children.tasks` entry as its VM task.

### Resources

The same two memory numbers as every other runtime, with meanings for one
isolate:

| Field | Default | Meaning for an isolate |
| --- | --- | --- |
| `memory` | the image's idle size rounded up: `js` 1 MiB, `python` 2 MiB, a module 1 MiB | The **reservation**: what one resident isolate is charged against the task's envelope. It decides how many isolates are resident at once (envelope memory over `memory`). The default is capped at `memoryMax`. |
| `memoryMax` | 128 (MiB) | The **ceiling** on one isolate's linear memory plus the files it writes in its run directory. At most 4096 and the worker's `isolate.maxMemory`; at least the image's initial memory (`js` 10 MiB, `python` 24 MiB). |
| `cpu` | 10 (MHz) | A **weight** in the host's scheduler, not a reserved share of a core. It also decides how many invocations run at once per node: envelope CPU over `cpu`. |

The envelope's `resources` are the job's share of each node; a task's
isolates never use more than its envelope holds. `resources.cores` and
`resources.devices` are refused.

### What an isolate task may and may not declare

| Block | |
| --- | --- |
| `stateful` | Required: `mode = "session"` with an `envelope`. `promote` is refused: isolate sessions are never promoted to keyed instances. |
| `network.ports`, an egress policy (`network.egress`), `connect` | Allowed: they are bound for the envelope allocation, gated ([HTTP handlers](#http-handlers), [Outbound HTTP](#outbound-http)). `network.mode` may only be unset, `auto`, or `gated`. `connect.templates` is refused. |
| `service` | Refused: isolates take requests on `network.ports` through their envelope, which registers no service. |
| `run` | Refused: an isolate task takes [runs](#runs) without one. Put `run.command` on the VM task. |
| `suspend` | Only with `lifetime = "session"`. `idle`, `keepFor`, `durability = "local"`, `onPressure`, `onRestoreFailure`, and `resumeTimeout` are accepted; `checkpoint`, `checkpointEvery`, `archive`, `park`, `onResume`, `wake`, and `onWorkerStop` are refused (a stopping worker always hibernates its session isolates), and so is `durability = "store"` ([Current scope](#current-scope-and-limitations)). |
| `children` | Allowed: lists the VM task (required for `vm.task`) and caps [child VMs](#child-vms) with `max`. |
| `artifacts` | Only to deliver `config.entry`. |
| `resources.cores`, `resources.devices` | Refused. |

### Submit-time errors

Every rule above is checked by `legion job validate` and at submit, with
messages that start `[isolate] <job>/<task>: `. The ones a job author meets
most:

```text
[isolate] code-runner/run: runtime "isolate" needs stateful.mode = "session" (isolates are node-owned and never written to the store; see docs/task-runtime-isolate.md)
[isolate] code-runner/run: config.limits.cpu and config.limits.wall are required (budgets differ by orders of magnitude between a webhook and an agent loop; there is no default)
[isolate] code-runner/run: config sets both image and module; set exactly one: image runs a built-in interpreter, module runs your own component
[isolate] code-runner/run: config.image "ruby" is not a built-in image; the images are "js", "python"
Hint: run your own component with module = { source = "...", sha256 = "..." }
[isolate] code-runner/run: config.limits.cpu 1m is above config.limits.wall 30s, so it can never be reached: an isolate runs on one thread
Hint: lower limits.cpu to at most limits.wall
[isolate] code-runner/run: resources.memoryMax 16 MiB is below image "python"'s initial linear memory (24 MiB)
[isolate] code-runner/run: an isolate task takes runs without a run block (its budgets are config.limits, its entry the run's main file)
Hint: remove run; put run.command on the VM task (config.vm.task) instead
[isolate] code-runner/run: config.vm.task "machine" is not in children.tasks; add it there, which is what lets this task's isolates start it
[isolate] code-runner/machine: config.vm.task of code-runner/run names machine, which has no run block; add run.command so tier = "vm" runs know what to execute
[isolate] code-runner/run: suspend applies to session isolates only, and config.lifetime is "request": only a session isolate has an id to suspend under and wake by
Hint: set config.lifetime = "session", or remove suspend
[isolate] code-runner/notebook: suspend.onWorkerStop does not apply to an isolate: a worker that stops always hibernates its session isolates (their state is checkpointed and they wake on their next call on that node)
Hint: remove suspend.onWorkerStop
[isolate] code-runner/notebook: suspend.durability = "store" is not available for isolates yet: a session isolate's checkpoint stays on the node that took it, so it wakes only there and is lost with the node
Hint: remove suspend.durability (the default is "local"), and keep what must outlive the node in an upstream store
task run is an isolate task and declares a service. Isolates take requests on network.ports through their envelope, which registers no service
Hint: remove service and declare network.ports = [{ name = "http" }]
```

## Lifetimes

`config.lifetime` decides how long one instance lives:

| Lifetime | One instance per | Reused | Keeps state |
| --- | --- | --- | --- |
| `request` (default) | invocation | never: nothing one caller does is visible to the next | no |
| `tenant` | tenant key, per node | while it stays warm (dropped after 60 s idle, or sooner when the task needs its room) | best effort, while warm |
| `session` | session id | until the session is stopped | yes, across [hibernation](#hibernation-and-memory) |

A reused instance keeps its memory between invocations: Python keeps one
`__main__` namespace per instance (globals assigned by one run are there in
the next, except `result`), and JavaScript keeps `globalThis` (the entry's
own top-level `let`/`const`/`var` are local to its run). The run directory is
emptied before every run. `tenant` and `session` need a component that
exports `legion:isolate/runner` or `wasi:http/incoming-handler`: a
`wasi:cli/run` command is single-shot, so a node refuses to prepare such a
task with a longer lifetime.

### Tenants

A tenant key says who an invocation is for: `--tenant KEY` (`tenant` in the
API) for a run, the `Legion-Tenant` header for an HTTP request. It is 1 to
128 bytes of printable ASCII without spaces; an invocation without one
belongs to the task's anonymous tenant. The key picks the warm instance of a
`tenant` task, and every lifetime uses it for fairness: a node schedules
turns per tenant, so one tenant's burst does not starve another's.

**A tenant key is a routing key, not an identity.** The caller chooses
it, and Legion does not check it: anyone who may run code in the task,
and any HTTP client of a port the task advertises, can name any tenant
and reach that tenant's warm instance and its `state`. When the tenant
must follow an authenticated user, put an ingress handler in front that
removes the client's `Legion-Tenant` and sets its own. A request that
carries `Legion-Tenant` or `Legion-Session` more than once is refused with
`400` (`Legion-Tenant may appear at most once in a request`).

A call in a session isolate always uses the session's tenant. A run or
request that names a session and a different tenant is refused
(`INVALID_ARGUMENT`, `400` over HTTP); one that names no tenant is fine:

```text
INVALID_ARGUMENT session isolate notebook-q4n7c2rd.01jc3... belongs to tenant "acme", not tenant "other"; a call in a session uses the session's tenant
Hint: drop the tenant (Legion-Tenant), or start a session for that tenant
```

### Session isolates

A task with `lifetime = "session"` has addressable isolates, started like any
session:

```console
$ legion instance create code-runner --task notebook -j
{"ok":true,"data":{"instanceIds":["notebook-q4n7c2rd.01jc3..."],"restoringFrom":null,
 "mode":"session","durable":false,"node":"w-17","state":"running","deduplicated":false,"owner":null}}
$ legion session run code-runner --task notebook --session notebook-q4n7c2rd.01jc3... --file cell.py -j
$ legion isolate get notebook-q4n7c2rd.01jc3... -j
$ legion instance stop notebook-q4n7c2rd.01jc3...
```

- **Never listed.** A session isolate is in no manager's session index:
  `legion instance list` does not show it, and its start writes nothing. The
  node's reply to the start is the confirmation.
- **The id carries the node.** A minted id has the node's tag before its
  last `.`, so `instance get`, `stop`, `destroy`, `suspend`, `start`
  (resume), `resize`, `session run --session`, and `isolate get` all reach
  the right node. An id derived from an idempotency key carries no tag:
  `session run` and `isolate get` take `--on-node NODE` for it, and the
  `instance` verbs cannot find it.
- **Starting one on a task of another lifetime** is refused with
  `FAILED_PRECONDITION` and a hint to use `legion session run`, and so is
  a run with `--session` on such a task.
- **No promotion.** `instance promote` answers `FAILED_PRECONDITION`:

  ```text
  isolate sessions cannot be promoted (a keyed instance is an allocation each); give the session a stable id with an idempotency key, and keep what must outlive its node in an upstream store
  ```

  `--id` on `instance create` is refused. Hibernation checkpoints stay on
  the node that took them, so `legion.state` does not outlive the node
  either.
- **A caller who cannot read the session's job** gets the same
  `NOT_FOUND` from `instance get`, `stop`, `suspend`, `start`, `resize`,
  `promote`, and `isolate get` as for an id no node holds; a caller who
  can read the job but lacks the verb's permission gets
  `PERMISSION_DENIED`. A few answers still come before the job is known
  and can tell a caller that an id exists or existed: a session evicted
  from its node (`NOT_FOUND` with the node's account of the eviction), a
  node that does not answer (`UNAVAILABLE` naming it), and a stop of a
  session that ended in the last ten minutes (`already ended`).
- **A session id is a bearer capability for HTTP.** Anyone who can reach
  the task's ports and knows the id can send requests into the session
  with `Legion-Session`. The id also appears in the prefixes of the job's
  log lines ([Logs](#logs)).
- **A keyed start retried through another manager** can start a second
  isolate on another node (each manager remembers the node of a keyed start
  for 10 minutes).
- **Edge starts** work as for any session task
  ([Starting at the edge](session-mode.md#starting-at-the-edge)).

A run in a session isolate goes to the node its id names and nowhere else,
wakes the isolate if it is hibernated, and leaves it running.

### State

`legion.state` (the `legion:isolate/state` interface) is bytes keyed by
string that outlive the instance:

- **Session isolates:** scoped to the session, kept across hibernation and
  after a stopped invocation (a trap or a limit drops the instance, not its
  state; the next call gets a fresh instance with the state and `resumed =
  true`).
- **Tenant isolates:** scoped to the tenant, kept while the tenant's
  instance is warm. Best effort: an idle or evicted instance loses it.
- **Request isolates:** none. Every call returns `not-available`.

Each tenant or session may keep at most 1 MiB; a write past it fails with
`quota`.

## Runs

`legion session run` (`Manager.RunSession`, `Worker.RunSession`, the
workload API socket, and the `legion_session_run` MCP tool) runs code in an
isolate task with no `run` block. The general rules are in
[Session runs](session-runs.md); what differs:

1. **Stage.** The run's files go under `/legion/run` in the isolate's
   in-memory filesystem, with `--input` as `input.json`. The 3 MiB limit on
   files plus input applies. The environment has `LEGION_RUN_DIR`,
   `LEGION_RUN_INPUT` (with input), and `LEGION_RUN_ENTRY` (with an entry).
2. **Entry.** The task's `config.entry` when it has one (the run's files
   are then data); otherwise `main.<ext>` if the run sends one, else the
   first file.
3. **Execute.** Under `config.limits`. `--arg ARG` (repeatable, `args` in the
   API, at most 64 and 64 KiB together) is passed to the code: `sys.argv[1:]`,
   `process.argv.slice(2)`, or the program arguments after the task name for
   a command component. In the VM tier the rule is narrower
   ([The VM task](#the-vm-task)). `--command` is refused: an isolate runs
   the code the run sends.
4. **Collect.** stdout and stderr share `limits.output`, then the `--get`
   files take what is left, in request order, each cut and marked
   `truncated` when it does not fit. `result` is the runner's result (an
   image's `result`), else `result.json`.

### When a run stops

| `stopped.reason` | `limit` | What happened | CLI exit |
| --- | --- | --- | --- |
| `limits` | `cpu` | `exceeded its 2s CPU limit (2001.4ms)`. CPU is checked every epoch tick (1 ms by default), so a stop overshoots by about a tick. | 125 |
| `limits` | `wall` | `exceeded its 30s wall-clock limit`, including time spent waiting on HTTP or a child VM. | 124 |
| `limits` | `memory` | The instance needed more than `resources.memoryMax`, or the run's files did not fit in it. A failed growth is refused to the guest first; the stop happens when the guest fails because of it. | 125 |
| `unsupported` | | The code called something isolates do not have; `detail` names it. | 125 |
| `trap` | | The guest crashed: `the guest trapped: it executed \`unreachable\` (a panic or abort)`, a stack overflow, or another trap. | 125 |
| `ended` | | Its session isolate was stopped, its task was removed, or the host drained under it. | 125 |

A stopped run has `exit = null`, keeps the output printed before the stop,
and carries a `hint`. Output past `limits.output` is truncated, never a
stop. With a VM task, a `limits` or `unsupported` stop sets `nextTier =
"vm"`; a `trap` or `ended` stop does not.

**Exit status of command components.** WASI 0.2's `exit` carries only ok or
error, so a `wasi:cli/run` component reports 0 or 1: Rust's `exit(3)` arrives
as 1. A component that exports `legion:isolate/runner` (the images do)
reports its exact exit code.

**Times.** An isolate measures CPU and wall time in microseconds, so in JSON
an isolate reply's `cpuMs` and `wallMs` have fractions; `cpuMicros` and
`wallMicros` are in the API reply. A VM run keeps whole milliseconds.
`isolateStart` says how the isolate was obtained: `fresh`, `warm`, `paged`
(resident, its pages brought back), or `hibernated` (made from a
checkpoint).

**Idempotency** is as for every run: per node, the reply kept 10 minutes
([Idempotency](session-runs.md#idempotency)). The key's fingerprint includes
`tier`, `tenant`, `session_id`, and `args`.

## Tiers and burst

Each run of a task with a VM task goes to one of two tiers. `--tier` picks:

| `tier` | Where |
| --- | --- |
| `auto` (default) | Decided by the node before anything runs, below. |
| `isolate` | An isolate. Refused while the isolate runtime is disabled. |
| `vm` | A fresh single-use session of the VM task. `FAILED_PRECONDITION` without a `config.vm`. |

`auto` goes to the VM, with a VM task, when the first of these holds, and to
an isolate otherwise:

| Check | `tierReason` |
| --- | --- |
| The isolate runtime is [disabled](runtime-disable.md) for the job | `auto: the isolate runtime is disabled` |
| The **pre-flight scan** finds an import the image does not have | `auto: import torch is not in image python` |
| The node's **memo** says this code needed a VM within `vm.memoFor` | `auto: this code needed a VM 12m ago` |
| (none) | `auto: no imports outside the image` |

- **The pre-flight scan is a heuristic.** For an image task it reads the
  entry's top-level imports (at most the first 256 KiB, comments and strings
  skipped, relative imports and the run's own files counted as present) and
  compares them with the image's module list. It cannot see an import built
  at run time; the isolate finds those itself and stops the run as
  `unsupported`. A module task has no scan.
- **The memo** is per node, keyed by the job, the task, and the entry's
  bytes, at most 4,096 entries. A `limits` or `unsupported` stop in an
  isolate records it.
- **Without a VM task**, `auto` always runs in an isolate, and a stop has no
  `nextTier`.

**`nextTier`** is the tier to ask for the next time this same code runs:
`vm` after an isolate run stopped for `limits` or `unsupported` on a task
with a VM task, and after every run that went to a VM; empty when the isolate
was the right place. A caller that keeps it per code hash and passes it as
`--tier` pays for a wrong guess once, on any node.

**`--fallback vm`** is the CLI's (and the MCP tool's) second request, never
the server's: when the first reply was stopped in the isolate for `limits` or
`unsupported` and says `nextTier = "vm"`, the CLI sends the same run with
`--tier vm` and an idempotency key of `<key>:fallback`. The reply then says
`ranTwice: true`, and in text mode `your code ran twice, the second time in
a VM`. Use it only for code that is safe to run twice. It is refused with
`--session` and `--dry-run`.

### The VM task

The VM task is an ordinary sibling `sandbox` session task with a `run` block,
listed in the isolate task's `children.tasks`. A `vm` run is a
[session run](session-runs.md) of it on the same node the router picked for
the isolate task:

- the entry (the run's, or the task's `config.entry`) is always staged
  under a fixed name: `/legion/run/main.py` (`python`),
  `/legion/run/main.mjs` (`js`), or `/legion/run/main` (a module task); the
  other files keep their own names, and `--input` is `input.json`. A run
  file that has the fixed name while an entry is staged there is refused
  (`file main has the name the VM tier stages the run's entry under;
  rename it`);
- `--arg` values are appended to the VM task's own `run.command` **only
  when its last element is exactly that fixed entry path**
  (`["python3", "/legion/run/main.py"]`), after that path. What the
  command does with them is the job author's design: an interpreter passes
  them to the script, but a tool that takes the entry as an operand (a
  test runner, a grader) may parse them as its own options, so do not end
  such a command with the entry path if callers with `job:write` must not
  choose its options. A run with arguments that would go to a VM task whose
  command ends otherwise is refused before anything runs, also when
  `auto` picked the VM:

  ```text
  FAILED_PRECONDITION task machine's run.command does not end with /legion/run/main.py, so arguments cannot be passed to it safely; the run carries 2 args
  Hint: end the VM task's run.command with /legion/run/main.py (e.g. ["python3", "/legion/run/main.py"]), read LEGION_RUN_ARGS in a wrapper, or send the arguments in --input
  ```

- every VM-tier run gets `LEGION_RUN_ARGS`, the arguments as a JSON array
  (`[]` without any), for a wrapper command to read;
- the VM task's `run.wall` and `run.output` apply, not `config.limits`;
- the reply's `task` and `sessionId` are the VM task's, and `vmMs` is the
  time to the reply minus the command's own run time (the VM's start and the
  staging).

### What crosses tiers, and what does not

The files, input, `args`, and `--get` cross. A tenant's or session's `state`
does not: the VM is a fresh machine. The `legion` module and global exist
only in isolates, so code meant for both tiers reads `input.json` (or
`LEGION_RUN_INPUT`) rather than `legion.input`.

**Two Pythons.** The isolate's Python is CPython 3.14 for WASI with the
standard library. The VM's is whatever its image ships (`python:3.13-slim`
in the example), with whatever packages it has. Code that works in one can
behave differently in the other.

### Child VMs

An isolate can drive machines of its own: `legion.vm.acquire()` (the
`legion:isolate/vm` interface) starts a [child session](child-sessions.md) of
the VM task on the same node, owned by the isolate:

```python
import legion
m = legion.vm.acquire()
m.put("data.csv", legion.input["csv"].encode())
r = m.exec(["python3", "-c", "import pandas; print(pandas.read_csv('data.csv').shape)"])
print(r.stdout.decode())
m.release()
```

- **Ownership.** A session isolate owns its children as `{kind: "session",
  id: <session id>}`: they outlive its invocations and its hibernation, and
  a later invocation re-attaches with `legion.vm.attach(id)` (keep the id in
  `state`). A request or tenant isolate owns them as `{kind: "isolate", id:
  <invocation id>}`: they end when the invocation does. An owner is bound to
  its job and task, and the node acts on a child only for a session it
  holds, or an invocation it has in flight, for that job and task in the
  asking host's cordon. A host, even a compromised one, cannot reach
  another cordon's children by learning their ids.
- **Bounds.** The isolate task's `children.max` caps children per owner,
  and the VM task's envelope admits them. A full envelope is
  `resource-exhausted` (retryable).
- **`exec`**: timeout 0 means 60 s, at most 10 minutes; stdout plus stderr
  capped at the worker's `isolate.maxBodyBytes`.
- **`put` / `get`**: relative paths are under the child's run directory;
  `..` is refused; sizes capped at `isolate.maxBodyBytes`.
- **`fetch(port, ...)`** reaches a port the child listens on: any port of a
  child with its own network, only a declared port of a child that shares
  the node's network.
- **`resize`** maps a child resize ([resize](resize.md)): `needs-restart`
  past what the running VM can do, `resource-exhausted` when refused.
- **`acquire`** waits up to 60 s for the child to run. A child that does not
  start in time is stopped and the call fails.
- A child suspended by its own idle policy answers `child-gone`; resuming it
  from the isolate is not supported.

## The guest's world

What a component may import and must export is the host's linker, checked
when the module is compiled on a node: a component that imports something
the host does not provide is refused before its task takes any work, naming
the import.

### Imports

| Package | Interfaces | Legion's behavior |
| --- | --- | --- |
| `wasi:io` 0.2 (up to 0.2.12) | `error`, `poll`, `streams` | standard |
| `wasi:clocks` | `wall-clock`, `monotonic-clock` | 1 ms resolution, and frozen for the length of one turn: every read within a turn returns the turn's start |
| `wasi:random` | `random`, `insecure`, `insecure-seed` | seeded from the kernel per instance; two instances never share output |
| `wasi:cli` | `exit`, `environment`, `stdin`, `stdout`, `stderr`, `terminal-*` | stdin is empty; stdout and stderr are captured; the environment is `config.env` plus the `LEGION_RUN_*` variables |
| `wasi:filesystem` | `types`, `preopens` | an in-memory tree per instance (below) |
| `wasi:sockets` | `network`, `instance-network`, `tcp`, `tcp-create-socket`, `udp`, `udp-create-socket`, `ip-name-lookup` | linked so toolchains that import them load, and every operation is denied |
| `wasi:http` | `types`, `outgoing-handler` | [outbound HTTP](#outbound-http) through the worker |
| `legion:isolate@0.1.0` | `control`, `state`, `vm` | below |

A guest may import an older patch release of the same minor version
(`@0.2.3` against the host's 0.2.12). A newer patch, WASI 0.3, and anything
else is refused.

**The filesystem.** Two preopens: `.` (the run directory, `/legion/run`) and
`/`. The run directory is emptied before every run; files a run writes
there can be returned with `--get`. Its limits:

- File bytes count toward `resources.memoryMax`. Past it, `ftruncate` and
  positional writes fail with `ENOSPC`, but wasi-libc reports a plain
  `write()` failure as `EIO`, so Rust's `fs::write` and Python's `f.write`
  see an I/O error.
- At most 16,384 files and directories; no hard links or symbolic links.
- wasi-libc programs start with the working directory `/`, not the run
  directory: use absolute `/legion/run/...` paths, `$LEGION_RUN_DIR`, or
  change directory first. The language images change directory (Python) or
  resolve paths against `/legion/run` (JavaScript) before your code runs.

**No threads.** The engine and the compiler refuse WebAssembly threads and
shared memory.

### `legion:isolate`

The package is `crates/isolate/wit/legion-isolate.wit`. Its interfaces:

```wit
interface control {
  enum lifetime { request, tenant, session }
  record isolate-info {
    job: string, task: string, lifetime: lifetime,
    tenant: option<string>, session: option<string>,
    resumed: bool,
  }
  info: func() -> isolate-info;
  unsupported: func(what: string);   // stops the run as `unsupported`
  park: func();                      // a session isolate may hibernate when this call returns
}

interface state {
  variant state-error { not-available, quota(u64) }
  get: func(key: string) -> result<option<list<u8>>, state-error>;
  set: func(key: string, value: list<u8>) -> result<_, state-error>;
  delete: func(key: string) -> result<_, state-error>;
  keys: func() -> result<list<string>, state-error>;
}

interface vm {
  variant vm-error {
    not-configured, resource-exhausted(string), child-gone(string),
    needs-restart(string), failed(string),
  }
  resource machine {
    id: func() -> string;
    exec: func(argv: list<string>, stdin: list<u8>, timeout-ms: u32) -> result<exec-result, vm-error>;
    put: func(path: string, bytes: list<u8>) -> result<_, vm-error>;
    get: func(path: string) -> result<list<u8>, vm-error>;
    fetch: func(port: u16, request: http-request) -> result<http-response, vm-error>;
    resize: func(to: size) -> result<_, vm-error>;
    release: func();
  }
  acquire: func() -> result<machine, vm-error>;
  attach: func(id: string) -> result<machine, vm-error>;
}

interface runner {
  record request { entry: option<string>, input: option<string>, args: list<string> }
  record outcome { exit: s32, %result: option<string> }
  run: func(req: request) -> outcome;
}

world image { import control; import state; import vm; export runner; }
world imports { import control; import state; import vm; }
```

(`exec-result`, `http-request`, `http-response`, and `size` are records in
the file.) The `state` interface's comment mentions `suspend.durability =
"store"`; isolate sessions do not support it.

### Exports

A component must export at least one of:

| Export | Takes | Lifetimes |
| --- | --- | --- |
| `wasi:cli/run` | runs; arguments are `[task name, ...args]` | `request` only (single-shot); exit status 0 or 1 |
| `legion:isolate/runner` | runs; `request.entry`, `input`, `args` | all; exact exit codes, and `outcome.result` is the run's `result` |
| `wasi:http/incoming-handler` | [HTTP requests](#http-handlers) | all |

### Building a Rust component

The test guests in `crates/isolate/tests/guests/` are built with Rust 1.97
and `wit-bindgen` 0.63 for the `wasm32-wasip2` target, whose linker emits a
component directly:

```console
$ rustup target add wasm32-wasip2
$ cargo build --release --target wasm32-wasip2
$ sha256sum target/wasm32-wasip2/release/handler.wasm
```

- A plain Rust binary (`fn main`) is a `wasi:cli/run` command.
- A reusable runner or an HTTP handler is a `cdylib` that uses `wit-bindgen`
  to export `legion:isolate/runner` or `wasi:http/incoming-handler`, with
  `legion-isolate.wit` and the WASI 0.2 WIT it needs in its `wit/`
  directory.
- `std::fs` works through wasi-libc against the in-memory tree; call
  `std::env::set_current_dir(std::env::var("LEGION_RUN_DIR")?)` first if you
  use relative paths.

### Compilation

A node compiles each module once, in a `legion-isolate compile` child
process, before the task takes work:

1. The bytes are validated with `wasmparser` before the compiler sees them.
   Refused: a core WebAssembly module (`this is a core WebAssembly module,
   not a component; build for wasm32-wasip2 or wrap it with \`wasm-tools
   component new\``), threads and shared memory, Component Model async, and
   stack switching.
2. Cranelift compiles it under the node's engine settings. It is the only
   compiler; there is no interpreter or baseline tier.
3. The imports are checked against the host's linker (`the component imports
   \`wasi:nope/thing@0.2.0\`, which the isolate host does not provide ...`),
   and a component that exports none of the three entry points is refused.
4. The artifact is stored in the node's content store as derived content of
   kind `cwasm` ([artifacts](artifacts.md#derived-content)), keyed by the
   component's digest and the engine's compatibility key. A burst of tasks
   that need the same module compiles it once.

A compile that fails leaves the task unable to run on that node with a
`problem` (`the module failed to compile (unsupported-import): ...`) that
`legion isolate stats` shows and that runs answer as `FAILED_PRECONDITION`.
The node tries again after 30 seconds. A host never compiles and never
loads an artifact from outside the directories the worker's compiler
writes; a user-supplied `.cwasm` is never loaded.

The compiler parses untrusted bytes, so it confines itself like a host
([Confinement](#confinement)): it reads its input (at most 512 MiB)
first, then takes the same namespaces and PID-namespace fork, a mount root
holding only its output directory (writable, not executable), Landlock on
that directory, `no_new_privs`, and its own seccomp allowlist, and only
then parses the bytes. Confined, it keeps the caller's kernel uid but none
of its capabilities, which is why the input is read first: a file the
worker reaches only as root would otherwise be out of reach. It honours the worker's
`isolate.confine` (`legion-isolate compile --confine
enforce|best-effort|off`, `enforce` by default); under `enforce` a layer
that cannot apply fails the compile, and the task's `problem` reads `the
module failed to compile (confinement): ...`. Each compile reads a
private copy of its input, and at most two compiles run at once on a
node. A compile is limited to 300 seconds (the worker kills it then, and
passes `--deadline-secs 310`, which the compiler enforces on itself, so it
never outlives a worker that died) and to a 16 GiB address space (virtual
memory). With resource enforcement on and cgroup v2 it also runs in a
short-lived cgroup of its own with `memory.max` 4 GiB and `pids.max` 256;
elsewhere the address-space limit is its only memory bound.

## HTTP handlers

A component that exports `wasi:http/incoming-handler` answers HTTP on the
task's ports:

```nickel
{
  name = "hooks",
  runtime = "isolate",
  config = {
    module = { source = "https://example.com/hooks.wasm", sha256 = "<64 hex>" },
    lifetime = "tenant",
    limits = { cpu = "50ms", wall = "10s" },
  },
  network = { ports = [{ name = "http" }] },
  stateful = { mode = "session", envelope = { resources = { cpu = 4000, memory = 4096 } } },
}
```

- Each port in `network.ports` is bound for the envelope allocation like a
  gated allocation's port: the node's [ingress listener](network-handlers.md)
  for it terminates TLS when the port has it and applies the ingress chain,
  and delivers each request to an isolate of the task. The guest sees the
  scheme as `http`.
- `Legion-Tenant` names the tenant and `Legion-Session` the session isolate
  (for `lifetime = "session"`); both are removed before the guest sees the
  request. A `Legion-Tenant` that is not a valid key, either header given
  more than once, or a `Legion-Tenant` that differs from the named
  session's tenant is `400`. The tenant key is the client's claim, not an
  identity ([Tenants](#tenants)).
- Bodies are buffered whole in both directions, up to the worker's
  `isolate.maxBodyBytes` (16 MiB by default); a larger request is `413`.
- `config.limits` applies to each request.
- Refusals become statuses: no room within the queue timeout `503` with
  `Retry-After: 1`; no such session `404`; the task cannot run here (its
  module failed, or it does not export a handler) `502`; the host or the
  runtime unavailable `503`; a stopped invocation `503` for `limits` and
  `500` otherwise; a guest status outside 100 to 999 `502`.
- The guest's stdout and stderr for HTTP invocations go to the
  [task log](#logs).

## Outbound HTTP

Isolates have no sockets and no DNS. Their only way out is
`wasi:http/outgoing-handler` (`legion.fetch`, JavaScript's `fetch`,
`urllib`), which the host hands to the worker, which sends it through the
envelope allocation's datapath:

- **A task with no `network` and no `connect` block has no egress at all:**
  every request fails with `http-request-denied`.
- **Upstreams.** `http://<upstream>/path` or `http://<upstream>.svc.legion/path`
  reaches the task's `connect` upstream of that name, whatever port the URL
  gives, with the upstream's handlers and **credential injection**: the
  worker adds the credential on the way out, and the isolate never sees it
  ([Credential injection](network-handlers.md#credential-injection)).
  `https://<upstream>/` also goes to the upstream; the upstream's own `to`
  decides how the last hop is encrypted. A public host whose name equals an
  upstream key cannot be reached directly.
- **Everything else** goes through the envelope's egress proxy under the
  task's egress policy ([Policy](network-handlers.md#policy)), and the host
  guard. For `https://` URLs the worker makes the TLS connection, with the
  public web roots. A denial reaches the guest as `http-request-denied`.
- **Limits.** Bodies are buffered whole, both ways, up to
  `isolate.maxBodyBytes` (`http-request-body-size` or
  `http-response-body-size` past it). At most 8 requests per invocation
  are in flight; more wait their turn (so `Promise.all` over 100 URLs runs
  8 at a time within the wall limit). A request's timeout is the
  invocation's remaining wall time, or shorter when the guest sets one, and
  a request still pending when its invocation ends fails with "the
  invocation ended before the response arrived", even in a tenant or
  session instance that stays warm. Hop-by-hop headers, `Host`,
  `Content-Length`, `Legion-Tenant`, and `Legion-Session` are not
  forwarded. HTTP/1.1 to origins; no connection pooling.
- **What injection does not hide.** The credential never enters the
  isolate's process, but an upstream that echoes request headers back (a
  debug endpoint, some error pages) returns it in a response body the
  guest can read. And a host serves every isolate task of its cordon, so
  an escape from one isolate could send requests as any of those tasks,
  through their upstreams and with their credentials, without ever
  reading one: the cordon is the trust unit
  ([Cordons](#cordons-who-shares-a-process)).
- **Attribution.** `legion net denials` names the isolate task (the
  envelope's identity is reported as the task the job declares), not the
  isolate or tenant.

## Hibernation and memory

A resident isolate is in one of these states, which `legion isolate get`
reports for a session isolate:

| State | Memory | Next call |
| --- | --- | --- |
| `hot` | in memory | runs at once (`isolateStart: warm`) |
| `paged_out` | idle long enough that its private pages were pushed to swap | runs after its pages fault back (`paged`) |
| `hibernated` | a checkpoint only; no instance, no envelope room held | a fresh instance from the checkpoint (`hibernated`) |
| `empty` | a session with no instance and no checkpoint | a fresh instance |

**Hibernation keeps `state`, not memory.** A checkpoint is the session's
`state` map plus its metadata; the instance is dropped. The next call into it
gets a **fresh** instance with `legion.info().resumed = true` (in WIT,
`control.info().resumed`) and the state. Globals, open files, and anything
else in linear memory are gone. A guest that wants to resume where it was
keeps that in `state`. A checkpoint from an older engine or an older version
of the module is still restored, at the state level: version your keys.

A session isolate hibernates when:

- it has been idle for `suspend.idle.after`;
- it called `park()` (the hibernation happens when that call returns);
- the node is short of memory ([pressure](resource-enforcement.md#memory-pressure));
- `legion instance suspend <id>` asks; only that session hibernates, and
  the host's other sessions stay resident (a session in the middle of a
  call answers `FAILED_PRECONDITION`; suspend it again once it is idle);
- `legion operator runtime disable isolate --drain` covers its job; only
  the covered jobs' sessions hibernate;
- the worker stops (every session isolate is hibernated after a 5-second
  grace for running calls, which is why `suspend.onWorkerStop` is refused).

A session with a call running or waiting for it does not hibernate; the
idle sweep and pressure skip it, and `instance suspend` answers
`FAILED_PRECONDITION`. A call that arrives while a hibernation is already
under way waits for its checkpoint to be kept, then wakes the session from
it (`start` is `hibernated`); if the call's queue timeout passes first it is
refused with `RESOURCE_EXHAUSTED` (`session … was still hibernating past
the queue timeout`).

Checkpoints are kept on the node, in its content store, pinned to the
session, and deleted when the session stops or after `suspend.keepFor`,
which also ends the session. A session isolate that was resident and never
hibernated when its host process exited is lost; its next call is
`NOT_FOUND`. Tenant instances do not hibernate: an idle one is dropped
after 60 seconds, or under pressure.

**Paging out** a resident isolate that has been idle for the worker's
`isolate.pageOutAfter` (2 s by default) is done by the kernel: the worker
asks the host's cgroup to reclaim (`memory.reclaim`), and the kernel picks
the pages. It works only on nodes with cgroup v2, [resource
enforcement](resource-enforcement.md) on (which puts each host in its own
cgroup), and swap, zswap, or zram. Elsewhere `pagedOut` stays 0, and the
worker logs why once at start.

**A waiting isolate holds a stack.** On this engine version every live
instance, idle or waiting on HTTP or a child VM, holds a 128 KiB fiber stack
from its host's pool while it is resident. Hibernation releases it; waiting
does not.

## The host process

### Cordons: who shares a process

A **cordon** is the set of isolates that share one host process on a node:

| `config.cordon` | Cordon name | Who shares the process |
| --- | --- | --- |
| `namespace` (default) | `ns:<namespace>` | every isolate task of every job in that namespace |
| `job` | `job:<job id>` | this job's isolate tasks only |
| `shared` | `shared` | every isolate task, from every namespace, that chose `shared` |

`shared` is refused unless a namespace policy governing the job's namespace
lists it in `isolateCordons` (see [Namespace policy](#namespace-policy)).
The prefixes keep the kinds apart: a namespace named `shared` has the cordon
`ns:shared`, not the shared cordon.

A node runs one host process per cordon, and adds **replicas** when one
process's slots are not enough for the cordon's tasks. Replicas are added,
never removed, while the cordon has tasks: a request-lifetime call goes to
the next replica in turn, a tenant to a replica chosen by a hash of its key,
and a session stays on the replica that opened it.

### Confinement

Each host confines itself before it starts a thread or loads a module.
First it closes every descriptor it inherited except stdio and the three
its worker passed, so it trusts no spawner to have marked the rest
close-on-exec. Then, in order:

| Layer | What it does | Shown as |
| --- | --- | --- |
| User namespace | uid and gid map to 65534 inside it, and every capability is dropped. The kernel still sees the worker's uid (normally root) for file permissions; the host does not switch to an unprivileged user. New mount, IPC, and UTS namespaces too | `userns` |
| Network namespace | no interface but an unconfigured loopback that is down | `netns` |
| Mount root | `/` is an empty, read-only tmpfs holding only the compiled-artifact directories, bound read-only, `nosuid`, `nodev`, at their own paths, with any mount nested inside them read-only too (`enforce` refuses a host where that cannot be done). No `/proc`, `/sys`, or `/dev`: every other path on the node does not exist for the host (`ENOENT`, for `open`, `stat`, and `readlink` alike) | `mount-root` |
| PID namespace | the host is PID 1 of its own namespace and can name no process outside it (below) | `pid-ns` |
| `no_new_privs` | | `no-new-privs` |
| Landlock | read-only access beneath the artifact directories, nothing else on the filesystem; no TCP bind or connect | `landlock` |
| Landlock scopes | signals and abstract Unix sockets confined to the host's own domain. Needs Landlock ABI 6 (Linux 6.12); applied and required where the kernel has it, and only a note in the host's log where it does not | `landlock-scopes` |
| seccomp | an allowlist with argument filters; anything else kills the process (SIGSYS). `clone` for threads only (`clone3` fails with `ENOSYS`), `socketpair` for Unix pairs only, `fcntl` only `F_GETFD`, `F_SETFD`, `F_GETFL`, `F_DUPFD_CLOEXEC`, and `F_SETFL` without `O_ASYNC`, `ioctl` only `FIONBIO` and `FIOCLEX`, `prctl` only `PR_SET_NAME`; no `execve`, `fork`, `socket`, `connect`, `bind`, `kill`, or `ptrace` | `seccomp` |

The full allowlists are in `crates/isolate/src/confine.rs`.

**The PID namespace is a fork.** The process the worker starts unshares a
PID namespace and forks; the child is the host. The parent stays behind as
a supervisor: it closes every descriptor, drops its capabilities, sets
`no_new_privs`, takes a Landlock ruleset with no filesystem access and a
seccomp filter that leaves it only waiting, signal handling, writing,
exiting, and `kill` of itself, and then waits for the host and exits with
its status (or re-raises the signal that killed it, so a seccomp kill
still reads as SIGSYS). The host dies with its parent, so killing the pid
the worker knows kills the host. That pid is the supervisor's: it is the
`pid` in `legion isolate stats`, and `/proc/<pid>` on the node shows the
supervisor, not the host.

The worker's `isolate.confine` decides what happens when a layer cannot
apply: `enforce` (the default) makes the host exit before it loads a module,
`best-effort` runs with what applied, and `off` applies nothing (development
only). Under `enforce` every layer above is required except
`landlock-scopes` on a kernel without them. `legion isolate stats` lists
the layers in force for every host process (`confinement`); a layer
missing from the list did not apply.

Without a user namespace (only possible under `best-effort`) the host
still drops every capability, but runs as the worker's uid.

With resource enforcement on and cgroup v2, each host process also runs in
its own cgroup under `<cgroupParent>/isolate-hosts/`. The leaf's name is a
readable prefix of the cordon name (letters, digits, `.` and `-` kept,
anything else written as `_`), the first 12 hex digits of the SHA-256 of
the exact cordon name, and the replica, so two cordons never share a
leaf. `memory.max` is the sum of the cordon's envelopes on the node and
`pids.max` 1024. The worker
creates the cgroup and puts the process in it before `exec`, so the
forked host starts inside it; if that fails, the worker logs a warning and
the host runs in the worker's cgroup. `memory.max` follows the cordon:
it is rewritten when an envelope's memory grows or shrinks and when a task
joins or leaves. No `cpu.weight` is set on it.

The host holds no credentials, no TLS keys, no tokens, and no KV access. It
reaches nothing but the two sockets its worker gave it, and every request it
makes of the worker names a task, which the worker checks against the
cordon it started that process for. Child-VM calls must also name an
isolate the worker issued to that cordon: a session it holds, or an
invocation it has in flight, for that job and task; log lines are kept
only for those isolates too, and every request's job, task, isolate id,
and tenant are checked for length and form before the worker acts on it.
What a compromised host can still do: act as any session or in-flight
invocation of its own cordon (on any of the cordon's replicas), write log
lines under an invocation's id for 30 seconds after it ends, and send
requests through its tasks' upstreams with their injected credentials. It
cannot reach another cordon's isolates or children, and it never holds a
credential (one an upstream echoes back excepted, as above).

### Bounds

What a guest can make its host hold, per invocation or instance:

| Bound | Value |
| --- | --- |
| Outbound requests in flight | 8 per invocation; more wait; none outlives the invocation |
| Request and response bodies | `isolate.maxBodyBytes` each (16 MiB by default) |
| Resource table (handles, pending requests, header sets) | 4,096 entries per instance |
| One `wasi:http` header set | 64 KiB |
| `state` | 1 MiB per tenant or session |
| Output | `limits.output` per invocation |

What a host can make its worker hold:

| Bound | Value |
| --- | --- |
| A hibernation checkpoint | checked before any byte is kept: a session the worker holds for that job and task, at most 1 MiB of state plus 256 KiB |
| One call's message | `max(isolate.maxBodyBytes, 16 MiB) + 1 MiB` (17 MiB by default); a child VM `put` or `exec` stdin larger than that fails |
| Calls at once | 8 per shard of the host, at least 16 and at most 64 |
| Bodies held across an await (egress, child VM) | `max(64 MiB, 4 × the message limit)` per host; past it a call answers `RESOURCE_EXHAUSTED` |
| A line of the host's own stderr | 4 KiB; the rest of the line is skipped |
| Guest log lines | 4,096 per batch and 1 MiB of text, 16 KiB per line, 512 KiB/s per job and task (4 MiB burst) under 2 MiB/s per host (16 MiB burst) ([Logs](#logs)) |

Buffered request and response bodies live in the host's memory, outside
the isolates' accounts: they are **not** counted against `memoryMax`. A
guest can hold up to 8 × `isolate.maxBodyBytes` of them in each direction
per invocation.

### The security model

The walls, inside out: WebAssembly software isolation compiled by Cranelift
with Spectre mitigations on heap and table accesses; language engines that
run inside that sandbox, so an engine bug corrupts only its own isolate; a
fresh instance per call by default; coarse, frozen clocks and no threads;
no secrets in the process; cordons; and the confined process above.

**What this design does not claim:** that tenants in one cordon are safe
from a determined Spectre attacker. It claims that such an attacker reaches
only data in that cordon, never a credential, and that an operator decides
who shares a cordon.

More limits a security reviewer should know:

- **The kernel uid is the worker's.** The host and the compiler are 65534
  only inside their user namespace; the kernel checks file permissions
  against the worker's uid, normally root. Under `enforce` the mount root
  and Landlock leave that uid nothing but the artifact directories,
  read-only. A `best-effort` host that lacks both keeps root's access to
  the node's files.
- **Per-job memory inside a shared host is the host's accounting, not the
  kernel's.** The kernel limits the host process as a whole (its cgroup);
  how that is split between the jobs and tenants of a cordon is enforced by
  the host itself.
- **Host-side buffers are not charged to `memoryMax`.** HTTP bodies,
  `wasi:http` handles and header sets, and what the worker buffers for a
  host count against the [bounds](#bounds), not against any isolate's
  `memoryMax`.
- **Anonymous executable memory is not denied.** seccomp cannot tell an
  anonymous mapping from a file mapping in `mprotect`; denying it outright
  needs an LSM, which Legion does not configure. The host never compiles,
  and loads only artifacts from directories it can only read. Those
  directories are mounted executable, because the engine maps compiled
  code from them.
- **Tenant keys and session ids authenticate nothing.** Both are routing
  keys the caller supplies ([Tenants](#tenants)).
- **Child-VM calls are checked per cordon, not per host process.** One
  replica of a cordon can name an invocation in flight on another replica
  of the same cordon.

### Engine pinning and the CVE response

The engine is pinned to an exact version (wasmtime 50.0.1). A compiled
module is loadable only by the engine settings that compiled it, so
artifacts are keyed by a compatibility key that `legion-isolate engine-info`
prints, and a new engine recompiles every module on first use. An engine
upgrade is a new `legion-isolate` binary; it must speak the worker's host
protocol version, or the worker does not use it. Hibernation checkpoints
survive an engine upgrade (they hold `state`, not memory).

When an engine advisory is open, `legion operator runtime disable isolate`
([runtime disable](runtime-disable.md)) turns the tier off in seconds:

- `auto` runs of tasks with a VM task run in the VM instead
  (`tierReason: auto: the isolate runtime is disabled`);
- `--tier isolate` runs, runs of tasks without a VM task, session-isolate
  starts, and HTTP requests are refused (`UNAVAILABLE`, retryable; HTTP
  `503`);
- `--drain` also hibernates the covered jobs' session isolates.

## Operating it

### Installing the binary and images

Build the binary with `cargo build -p legion-isolate --release` (a bare
`cargo build` does not build it) and put `target/release/legion-isolate`
beside the `legion` binary on every isolate worker. The worker looks for it
at:

1. `isolate.binary` in the worker type's config (an absolute path);
2. `legion-isolate` beside the worker's executable;
3. `LEGION_ISOLATE_BINARY`.

At start it runs `legion-isolate engine-info` (10 s timeout) and checks the
protocol version. A worker that finds no binary, or one that fails the
check, does not advertise `isolates` and takes no isolate envelope.

Build the images with `crates/isolate/images/build.sh`, which needs
`python3` (with `venv` and `pip`), `node` 20 or later with `npm`, and
`cargo`, and writes `js.wasm`, `python.wasm`, and `manifest.json` into
`target/isolate-images/`. They are not checked in, and builds are not
bit-reproducible: the manifest records the sha256 of the artifact that was
built, so pin and ship that artifact rather than rebuilding it per node. The
worker looks for the directory at:

1. `isolate.images` (an absolute path);
2. `isolate-images` beside the `legion-isolate` binary;
3. `LEGION_ISOLATE_IMAGES`.

### The worker `isolate` block

In a worker type's config (`configs/workers/<type>.ncl`). Every field is
optional, and a bad value stops the manager at startup:

| Field | Default | Meaning |
| --- | --- | --- |
| `binary` | beside the worker, then `LEGION_ISOLATE_BINARY` | The `legion-isolate` binary. Absolute. |
| `maxMemory` | `"4GiB"` | The largest `resources.memoryMax` an isolate may have on this worker type, and the size of one memory slot. 1 MiB to 4 GiB. |
| `slotsPerHost` | `10000` | Memory slots per host process: the most isolates one process holds resident. At most 16,000 with `guard-pages`, 200,000 with `mpk`, 1,000,000 with `small`. |
| `shards` | `0` | Scheduler threads per host process; 0 is one per CPU. |
| `epoch` | `"1ms"` | How often the epoch ticker advances: how late a CPU or wall limit can be noticed. |
| `slice` | `"5ms"` | How long a turn runs before it yields its thread to the next isolate. At least `epoch`. |
| `pageOutAfter` | `"2s"` | Idle time before a resident isolate's pages are paged out; `"0s"` never. |
| `cordonBoundary` | `"process"` | What separates cordons: a confined process each. `"vm"` is refused (not built). |
| `layout` | `"guard-pages"` | How slots are laid out: `guard-pages` (a 4 GiB reservation plus guard pages each), `mpk` (memory protection keys stripe up to 15 slots over one reservation; x86-64 with PKU; falls back to guard pages where the CPU has none), or `small` (small reservations with explicit bounds checks: the densest and slowest). |
| `confine` | `"enforce"` | `enforce`, `best-effort`, or `off` ([Confinement](#confinement)). |
| `images` | `isolate-images` beside the binary, then `LEGION_ISOLATE_IMAGES` | The images directory. Absolute. |
| `maxBodyBytes` | `"16MiB"` | The largest HTTP body (either direction), child VM `exec` output, and `put`/`get` a host handles. |

### Capacity

- **Per task, per node:** resident isolates = envelope memory /
  `resources.memory`; invocations at once = envelope CPU / `resources.cpu`
  (at least 1). A call that finds no room waits up to the worker's
  `edgeBoot.queueTimeout` (1 s by default), evicting the task's coldest idle
  tenant instance if that makes room, and is then refused with
  `RESOURCE_EXHAUSTED`; a manager-routed run moves on to the next node.
- **Per host process:** `slotsPerHost`, clamped to what the kernel allows.
  Each live instance costs about 8 memory mappings, so the default
  `vm.max_map_count` of 65,530 allows about 7,900 live instances per
  process. Raise it (`sysctl vm.max_map_count=...`) for denser hosts; the
  worker logs the clamp.
- **Per cordon:** replicas = the cordon's resident isolates / slots, at most
  64 processes.
- **MPK hosts** need hundreds of slots or more: with few slots spread over
  15 key stripes, an instantiation can fail before `slots` is reached.

The managers reserve nothing for isolates and list none: they route a run to
a node that holds the task's envelope (a random spread), and the node admits
it.

### Namespace policy

A [namespace policy](namespaces.md) can limit which cordon kinds its
namespaces' jobs use:

```nickel
{ name = "tenants", namespaces = ["tenants-*"], isolateCordons = ["namespace", "job"] }
```

Entries are exact kinds (`namespace`, `job`, `shared`), no globs. Without the
field, `namespace` and `job` are allowed and `shared` is refused;
`shared` needs at least one governing policy that lists it. Every governing
policy that sets the list must include the job's kind.

```text
[isolate] tenants/code-runner/run: config.cordon = "shared" is refused by namespace policy 'tenants' (isolateCordons = ["namespace", "job"])
```

### Observing

`legion isolate stats <JOB> [--task T]` shows each node's aggregate per
isolate task, from the nodes' heartbeats (`Manager.GetIsolateStats`). A node
whose heartbeats go to another manager is asked directly. Isolates are never
listed one by one.

```console
$ legion isolate stats code-runner
NODE  TASK  CORDON      RESIDENT  ACTIVE  PAGED-OUT  HIBERNATED  QUEUED  INV/S  CPU-S/S  VM/MIN  CHILD-VMS  STOPPED  REFUSED  HOST-RSS  AGE
w-17  run   ns:default  412       3       0          0           0       88.0   0.31     1.5     0          7        0        612MiB    1.2s
```

| Column (JSON key) | Meaning |
| --- | --- |
| `resident` | isolates with an instance in memory (running, idle, or paged out) |
| `active` | invocations running now |
| `pagedOut` | resident isolates whose pages were paged out |
| `hibernated` | session isolates held only as a checkpoint |
| `queued` | invocations waiting for room |
| `invocations`, `invocationsPerSec` | since the host started; per second over the node's last interval |
| `cpuMicros`, `cpuSecondsPerSec` | CPU used, likewise |
| `vmRuns`, `vmRunsPerMin` | runs sent to the VM tier |
| `childVms` | child VMs owned now |
| `stopped`, `limits` | stops by reason, and `limits` stops by limit |
| `refusals` | invocations refused for lack of room |
| `memoryBytes`, `reservationBytes` | linear memory mapped by resident isolates (an upper bound), and one isolate's reservation |
| `hibernations` | session isolates hibernated since the host started |
| `problem` | why the node cannot run the task now (the module is compiling or failed, the host is restarting), or `null` |
| `hosts[]` | each host process of the cordon: `replica`, `pid` (the process the worker started, which supervises the host in its PID namespace), `rssBytes`, `slots`, `slotsUsed`, `shards`, `engine`, `layout`, `confinement` (the [layers](#confinement) in force), `restarts`, `startedUnixMs` |
| `ageMs` | how old the node's report is |

With `-j` the data is `{"task", "nodes"}` for one task, or `{"tasks":
[{"task", "nodes"}]}` for several.

`legion isolate get <SESSION_ID> [--on-node NODE]` describes one session
isolate by asking its node: `state`, `tenant`, invocations, memory and
state bytes, checkpoint generation and size, and child VMs. A node that
cannot be reached is `UNAVAILABLE`, not not-found. A caller who cannot read
the isolate's job gets the same `NOT_FOUND` as for an id no node holds. Both commands are in the
capability catalog (`isolate.stats`, `isolate.get`) and the MCP server
(`legion_isolate_stats`, `legion_isolate_get`).

**Metrics** on the worker's endpoint ([metrics](metrics.md#worker)), with no
per-job or per-task labels: `legion_isolate_instantiate_seconds{image}`,
`legion_isolate_turns_total`, `legion_isolate_cpu_microseconds_total`,
`legion_isolate_resident{tier}`, `legion_isolate_wakes_total{from}`,
`legion_isolate_limit_total{limit}`, `legion_isolate_stopped_total{reason}`,
`legion_isolate_vm_runs_total{reason}`, `legion_isolate_child_vms`,
`legion_isolate_compile_seconds`, `legion_isolate_host_rss_bytes{cordon}`,
and `legion_isolate_slots{cordon,state}`.

#### Logs

A run's output is in its reply. The stdout and stderr of HTTP and
session-isolate invocations are appended to the **envelope allocation's**
task log, once per invocation when it ends, each line prefixed with its
isolate (and tenant):

```text
[isolate notebook-q4n7c2rd.01jc3...] loaded 1204 rows
[isolate 7c1e... tenant=acme] handled GET /
```

Read them with `legion job logs <JOB> --allocation <envelope allocation
id>`. Runs in request and tenant isolates are not logged there. A
long-running invocation's output reaches the log only when it ends. The
host's own logs go to the worker's.

- **Who wrote a line.** The prefix comes from the worker's own record of
  the isolate and its tenant, never from the host's word, and lines are
  kept only for an isolate the worker gave that host: a session it holds,
  or an invocation in flight or ended within the last 30 seconds.
- **Control characters are escaped.** Carriage returns, escape sequences,
  and other control characters except tab are written as visible `\xHH`,
  so no line can overwrite or imitate another on a terminal; coloured
  output shows as `\x1b[31m`.
- **Limits.** A guest line is cut at 16 KiB, and one invocation's batch at
  4,096 lines and 1 MiB of text. Each job and task may log 512 KiB per
  second (bursts up to 4 MiB) within a per-host ceiling of 2 MiB per
  second (16 MiB bursts); tenants of one task share its budget. Dropped
  lines are replaced by one line saying how many:

  ```text
  [isolate 7c1e... tenant=acme] 1200 lines dropped: over the task's log rate or the batch limit
  ```

- The log files rotate as other task logs do, checked after every batch.

### Failure modes

| Symptom | Cause | What to do |
| --- | --- | --- |
| The envelope does not place; `legion job status` shows no eligible node | No worker advertises `isolates` | Install `legion-isolate` (and check the worker's log for the probe's error), on x86-64 Linux |
| `problem`: `this node has no isolate images (set isolate.images, or install isolate-images beside legion-isolate)` | An `image` task on a node without the images | Install the images directory |
| `problem`: `config.image pins python@sha256:..., but this node has python@sha256:...` | The pinned build is not the node's | Pin the build the nodes run, or write `"python"` |
| `problem`: `the module failed to compile (<kind>): ...`; runs `FAILED_PRECONDITION` | The component is invalid or imports something the host does not provide | Fix the component ([Compilation](#compilation)) |
| `UNAVAILABLE` `the module is still compiling on node ...; retry` | First use of a module on a node | Retry; a compile takes seconds for a language image |
| `UNAVAILABLE` `the isolate host for cordon X exited during the call; the worker is restarting it; retry` | The host process exited (a crash, a seccomp kill shows as SIGSYS) | Retry. The worker restarts it with backoff (200 ms doubling to 30 s), reloads its tasks, and counts `restarts`; calls that arrive while it reloads answer `UNAVAILABLE` too. Hibernated sessions survive; resident never-hibernated sessions do not |
| The host exits at once, `confinement` empty or short | A layer cannot apply under `confine = "enforce"` (no unprivileged user namespaces, no Landlock, no `pivot_root` into a tmpfs, ...); the host's stderr in the worker log says `confinement is enforced but these layers did not apply: ...` | Fix the kernel setting, or set `confine = "best-effort"` knowing which layer is missing |
| `problem`: `the module failed to compile (confinement): ...` | The same, for the compiler | As above |
| `problem`: `config.entry "..." cannot be used: ...` | The entry is a symlink, is reached through one, is not a regular file, or is over 3 MiB | Deliver a regular file in the artifact |
| Runs with `--arg` answer `FAILED_PRECONDITION` `... does not end with /legion/run/main.py ...` | The run went to the VM tier, and the VM task's `run.command` does not end with the entry's path | End the command with the entry path, read `LEGION_RUN_ARGS`, or pass the values in `--input` |
| `RESOURCE_EXHAUSTED` from runs, `refusals` rising | The envelope is full | Raise `stateful.envelope.resources`, lower `resources.memory`, or add nodes |
| `slotsPerHost ... clamped` in the worker log | `vm.max_map_count` too low | Raise it |
| `stopped.reason = "limits"`, `limit = "memory"` | The code needs more than `memoryMax` | Raise `resources.memoryMax`, or use the VM tier |

## Measured numbers

Measured on a development container: a 4-vCPU Firecracker guest (Intel Xeon
@ 2.30 GHz, 15 GiB, no swap), shared with other builds, with wasmtime 50.0.1
and the host's pooling engine. They are single-machine measurements, not a
benchmark of a production fleet, and end-to-end times through a manager add
the network and the routing on top.

| What | Measured |
| --- | --- |
| Instantiate a small Rust component (p50 / p99) | 5.0 / 9.8 µs; a whole create-and-drop cycle 8.1 / 22.7 µs |
| A small Rust component, private memory per idle instance | 33.8 KiB |
| `js` image: size; hello world on a fresh instance (p50 / p99); on a warm one | 13.1 MB; 1.14 / 2.00 ms; 107 µs |
| `js`: a ~12 KB snippet, fresh (p50 / p99); idle private memory per instance | 2.86 / 3.95 ms; 767 KiB |
| `python` image: size; hello world fresh (p50 / p99); warm | 29.5 MB; 2.55 / 5.99 ms; 171 µs |
| `python`: a ~12 KB snippet, fresh (p50 / p99); idle private memory per instance | 12.0 / 19.3 ms; 1,519 KiB |
| Memory slots one process could lay out: guard pages; MPK with `vm.max_map_count` raised; small | 21,870; 328,842; 702,500 |
| Epoch interruption overhead: call-heavy code; a tight loop | +14% to +25%; +1% to +3% |
| One await that suspends and resumes a guest | about 480 ns |
| Compiling the language components (4 threads) | 3.3 s (StarlingMonkey, 12 MB); 2.5 s (CPython, 18 MB) |
| One invocation through a host shard, in process (`isolate_bench`, optimized build): a fresh command instance; a call on a warm runner; a call whose guest awaits the worker once | 107 µs; 38 µs; 47.5 µs |

**End to end.** `legion session run` through a manager, a worker, and a
real confined host on the same container, with **debug builds** of the
worker and the host, p50 of 30 runs after a warm-up:

| What | p50 (min) |
| --- | --- |
| A Rust `wasi:cli/run` component | 4.8 ms (4.4 ms) |
| `python` hello world | 17.6 ms (16.6 ms) |
| The first `python` run on a node, including the one-time compile by a debug-build compiler | 60.5 s |

The design target for a component run is under 2 ms end to end. This
measurement does not meet it. No release-build
end-to-end measurement has been taken, so whether a release build meets
it is not known.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ❌ (`config.env` only; `connect.templates` is refused) |
| KV file injection | ❌ (an isolate has no task directory) |
| Identity env / file delivery | ❌ / ❌ (session tasks declare no `identities`) |
| Signal delivery | ❌ |
| Memory / CPU limits, exclusive cores | Memory: `memoryMax` per isolate, enforced by the host; the host process's cgroup with resource enforcement on. CPU: `config.limits.cpu` per invocation and `resources.cpu` as a scheduling weight. Exclusive cores: ❌ (`resources.cores` is refused) |
| Resize in place (`memory`, `memoryMax`, `cpu`) | Session isolates: `memory` and `memoryMax` (never below what the isolate uses now, never above `isolate.maxMemory`); `cpu` is `not_supported` ([resize](resize.md#what-each-runtime-can-change)) |
| Measured usage | Per task and node in `legion isolate stats`; not in `legion node usage` |
| Artifacts | Only to deliver `config.entry`; `config.module` is fetched by the same fetchers |
| Exec / interactive PTY | ❌ / ❌ (use runs; a child VM has `exec`) |
| Volumes | ❌ |
| Devices | ❌ (refused) |

## Current Scope and Limitations

- **x86-64 Linux only.** No aarch64, no macOS.
- **WASI 0.2 only.** No WASI 0.3 and no stackless (async callback) world;
  every guest runs on a fiber, and a waiting or idle resident instance holds
  a stack.
- **Hibernation keeps `state` only.** Linear memory never survives a
  hibernation, and a session isolate hibernates only between calls.
- **No `suspend.durability = "store"` for isolate sessions.** It is refused
  at submit. Checkpoints never leave their node, so a session isolate does
  not survive the loss of its node; keep what must outlive it in an
  upstream store. The comment on the `state` interface in
  `legion-isolate.wit` mentions `durability = "store"`; it is not
  available.
- **No gateway wake** for session isolates, and no connection handoff.
- **`cordonBoundary = "vm"`** (a microVM per cordon) is not built; a worker
  type that sets it stops the manager at startup (`isolate.cordonBoundary =
  "vm" is not available: cordon hosts run as confined processes on this
  release`).
- **The VM task cannot share the isolate task's `connect`.** Session tasks
  other than isolates refuse `connect`, and a gated sandbox session is not
  available, so the example VM task has no network.
- **`js`:** no ES modules (the entry is a script), no npm packages, no Node
  builtins; an unhandled rejection that the entry never awaits does not
  fail the run; `process.exit` inside a timer after the entry returned is
  not observed.
- **`python`:** the standard library only; no threads, no subprocesses, no
  sockets, no `ctypes`, no `ssl` (the node does TLS); CPython's string-hash
  seed is fixed per image build and shared by its instances.
- **HTTP is buffered.** Request and response bodies, inbound and outbound,
  are held whole up to `isolate.maxBodyBytes`; no streaming, HTTP/1.1 only,
  no connection pooling.
- **Exit codes of command components** are 0 or 1.
- **Replicas only grow** while a cordon has tasks; an uneven load can refuse
  on one replica while another has room.
- **Host cgroups have no `cpu.weight`**, and their placement, `memory.max`
  updates, page-out, and the compiler's cgroup have not run on a cgroup v2
  node.
- **No per-isolate attribution** in `legion net denials`.
- **Engine and memory layout**: MPK is not an operator-validated default;
  anonymous executable memory is not denied (no LSM).
- **Language images are not pre-initialized with a task's `entry`**: the
  entry is staged and run per call.
- **Not measured:** density and start-time numbers at fleet scale, and on
  production hardware; end-to-end times with release builds.
- **The VM tier has not run on a real VM.** The development machine has no
  `/dev/kvm`, and the sandbox runtime's Linux development backend takes no
  `exec`, which session runs need. The VM tier and child VMs were tested
  end to end with an `exec` session task standing in for the sandbox VM
  task (the tier decision, `nextTier`, `vmMs`, single-use sessions, the
  `args` rule, child ownership, `children.max`, a child surviving its
  owner's hibernation, children ending with their owner). Not tested end
  to end: a sandbox guest running the VM task's command, re-attaching a
  child with `legion.vm.attach` after hibernation, HTTP through the
  node's own TCP ingress listener (the test entered at the isolate
  supervisor's socket), and an upstream credential installed by the
  heartbeat rather than directly.
- **The kernel uid is not dropped** ([The security model](#the-security-model)).
