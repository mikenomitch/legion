# Configuration Language (Nickel)

Every configuration file Legion accepts is a [Nickel][nickel] program:

| File | Read by | Example |
| --- | --- | --- |
| Job specs | `legion job run/plan/validate` | [`configs/jobs/maximal.ncl`](../configs/jobs/maximal.ncl) |
| Manager config | `legion start manager -c` | [`configs/manager.ncl`](../configs/manager.ncl) |
| Worker configs | manager startup, from `workerConfigPath` | [`configs/workers/default.ncl`](../configs/workers/default.ncl) |
| ACL policies | `legion acl policy create -f` | see [Auth & ACLs](auth-and-acls.md) |
| Job templates | `legion template create -f` | [`configs/templates/render.ncl`](../configs/templates/render.ncl) |

Legion evaluates a file the way `nickel export --format json` does, then
deserializes the resulting JSON. Anything Nickel can produce, Legion accepts;
nothing about the schemas below is Nickel-specific.

Nickel is the default and the language everything here is written in. A
`.json` file is accepted for the first four, for config a program emits — see
[JSON](#json-for-machine-emitted-config). Job templates are Nickel-only,
because a template declares `job` as a *function* of its variables.

## Why Nickel

Job specs are data, but they are data people copy, parameterize, and get
wrong. Nickel gives Legion three things plain JSON does not:

- **Functions, `let`, and imports**, so a fleet of similar jobs is written
  once. There is no templating layer stapled on top of the format.
- **Contracts** (`| String`, `| Number`, custom predicates) that fail at
  evaluation with the offending line, before the manager ever sees the spec.
- **Merging with defaults** (`| default`, `&`), which is exactly the shape
  "here are the defaults, here is what the caller overrode" wants.

Error messages carry source snippets, so a mistake points at the line that
made it rather than at the JSON that came out.

## Syntax essentials

If you have written JSON, three differences cover almost everything:

```nickel
{
  name = "web",            # `=`, not `:`
  count = 2,               # `#` comments, not `//`
  tags = ["a", "b"],       # strings are double-quoted, always
}
```

Two Nickel-specific rules bite in Legion configs:

**Keyword field names must be quoted.** Nickel reserves `default`, `priority`,
`doc`, `optional`, `force`, `merge`, and `not_exported` (plus the obvious
`let`, `in`, `fun`, `if`, `import`, …). A jobspec's priority band and a job
template's variable defaults both collide:

```nickel
{
  name = "api",
  "priority" = "high",     # `priority` is a Nickel keyword
}
```

**Records are recursive.** A field refers to its own record before it refers
to an enclosing binding, so `fun name => { name = name }` is a self-reference,
not the argument. Name parameters distinctly:

```nickel
let task = fun task_name => { name = task_name, runtime = "exec" } in
{ name = "demo", tasks = [task "web"] }
```

String concatenation is `++` (`+` is numeric only), and interpolation is
`%{...}`:

```nickel
{ name = "render-%{vars.scene}" }
```

## Variables

Every configuration file is evaluated with one free identifier besides
Nickel's `std`: **`vars`**, a record built from the command line.

```bash
legion job run web.ncl \
  --var image=nginx \
  --var-code replicas=3 \
  --var-code limits='{ memory = 512 }'
```

```nickel
{
  name = "web",
  task = {
    name = "server",
    runtime = "container",
    count = vars.replicas,                    # a number, not "3"
    config = { image = vars.image },
    resources = vars.limits,
  },
}
```

| Flag | Value |
| --- | --- |
| `--var KEY=VALUE` | The literal string `VALUE`. |
| `--var-code KEY=EXPR` | The result of evaluating the Nickel expression `EXPR`. |
| `--kv-var KEY=PATH` | The [KV](kv-store.md) entry at `PATH`, as a string. |
| `--kv-var-code KEY=PATH` | The KV entry at `PATH`, keeping its declared type (`number` and `json` entries arrive as numbers/records). |

They are accepted by `legion job run`, `legion job plan`, and `legion job
validate`. The KV forms need a reachable manager; the plain forms do not, so
`legion job validate` stays usable offline.

`vars` is always bound, defaulting to `{}` — a spec that never mentions it is
unaffected, and there is no flag to "declare" a variable. Give a variable a
default by merging over `vars`, which is the idiomatic Nickel form and gives
a real error when a required one is missing:

```nickel
let params =
  { replicas | Number | default = 1, image | String }
  & vars
in
{ name = "web", task = { runtime = "container", count = params.replicas } }
```

Running that with no `--var image=…` fails at evaluation naming `image`,
rather than submitting a jobspec with a missing field.

A worked example ships at [`configs/jobs/vars.ncl`](../configs/jobs/vars.ncl).

## Imports

`import "path.ncl"` resolves relative to the importing file. Shared task
definitions are ordinary records:

```nickel
# lib/task-a.ncl
{ name = "task-a", runtime = "exec", config = { command = ["echo", "a"] } }
```

```nickel
let taskA = import "lib/task-a.ncl" in
{ name = "imports-example", tasks = [taskA] }
```

Imports are **hermetic**: the imported file does not see the importer's
scope, including `vars`. To parameterize a shared fragment, export a function
and apply it.

`legion job run` records every file the evaluation read — the spec plus its
imports, transitively — and stores them with the job, so `legion job spec`
shows the exact source a running job was submitted from.

[`configs/jobs/demo-imports-2.ncl`](../configs/jobs/demo-imports-2.ncl) is a
runnable example.

Imports resolve against the spec's own directory, including when the spec is
given by a relative path from somewhere else — `legion job run` reads a local
spec where it sits rather than copying it aside. A spec fetched from a remote
source (`https://`, `git::`, `s3://`) is staged as a whole tree, so its
imports resolve too; use the `#<relative path>` fragment to name the entry
point inside a fetched directory.

### Where imports are not allowed

[Job templates](job-templates.md) are rendered by the manager on a caller's
behalf, so a template that could `import "/etc/shadow"` would be a
file-disclosure primitive. Any use of the `import` keyword in a template is
rejected before evaluation starts, at publish time as well as at render time:

```
imports are not allowed here; the source must be self-contained
```

The check is lexical (the source is tokenized), so the word "import" inside a
string or a comment is not a false positive.

## JSON, for machine-emitted config

A file named `.json` is parsed as JSON instead of evaluated, for job specs,
the manager config, worker configs, and ACL policies:

```console
$ legion job run configs/jobs/machine-generated.json
```

This exists for config that a *program* writes: Terraform, a CI job, a control
plane, an agent. Those emitters already have a JSON serializer, and requiring
them to grow a Nickel one buys nothing — the abstractions Nickel offers are
for humans, and a generator has its own.

Write anything a person maintains in Nickel. JSON has no variables, no
imports, no comments, and no way to state a shared value once.

**The dispatch is by file extension and nothing else.** No content sniffing:
the name always tells you how the file will be read, and an error is always
reported in the language you named. Note that a `.json` file is *not* valid
Nickel — see [Known limitations](#known-limitations) — so the extension is
what keeps you out of a confusing parse error.

`--var` and friends are rejected against a `.json` file rather than ignored:

```
configs/jobs/machine-generated.json is JSON, which has no variables — drop the
variable flags, or use a .ncl file that reads `vars` and imports the JSON
```

### Mixing the two

Nickel imports JSON natively, dispatching on the same extension. That is the
supported way to combine generated data with hand-written structure — the
generator owns a data file, the spec stays readable:

```nickel
let fleet = import "generated/fleet.json" in
{
  name = "web",
  tasks = [{
    name = "server",
    runtime = "container",
    count = fleet.count,
    config = { image = fleet.image },
  }],
}
```

The imported JSON is data only; it cannot contain Nickel expressions.

### Job templates are Nickel-only

A [job template](job-templates.md) declares `job` as a function of its
variables (`job | not_exported = fun vars => …`), and JSON has no functions, so
there is no JSON form of a template. `legion template create` says so rather
than letting the parser report a syntax error:

```
$ legion template create render -f render.json
error: render.json is JSON, but a job template must be Nickel (a template
       declares `job` as a function of its variables …)
```

A generator that wants a template-shaped surface should publish the rendered
`.json` jobspec instead, or emit the small Nickel wrapper by hand once and
keep the varying data in an imported `.json`.

## No environment access

Nickel has no way to read the process environment, and Legion adds none.
Secrets and per-host values come from one of:

- **[KV](kv-store.md)** — `--kv-var` at submit time, or a `{{ kv "path" }}`
  reference resolved by the manager on every rollout.
- **A separate, uncommitted file** the config imports:

```nickel
let secrets = import "secrets.ncl" in
{ persistence = { streaming = { lease = { objectStorage = {
  accessKeyId = secrets.accessKeyId,
} } } } }
```

For a job's *runtime* environment, `config.env` is passed to the task and is
unrelated to evaluation.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `unexpected token` pointing at a `:` | JSON habit — record fields use `=`. |
| `unexpected token` pointing at `priority`/`default` | A Nickel keyword used as a bare field name. Quote it: `"priority" = …`. |
| `infinite recursion … recursive reference` | A field named the same as the binding it meant to read (`{ name = name }`). Rename the parameter. |
| `(+) expects its 1st argument to be a Number` | String concatenation written as `+`. Use `++`, or interpolate with `%{…}`. |
| `contract broken by a value` | A `\| String` / `\| Number` annotation rejecting the value — usually the actual bug, one step earlier than it would otherwise surface. |
| `imports are not allowed here` | An `import` inside a job template. Inline it. |
| `jobspec must define at least one task` | The file evaluated fine but is a fragment, not a whole jobspec. |
| `invalid contract expression … can't be used as a contract` | A JSON object being read as Nickel. Rename the file to `.json`, or convert `:` to `=`. |
| `is not valid JSON` | A `.json` file that does not parse — the message carries the line and column. |
| `is JSON, which has no variables` | `--var` against a `.json` spec. |

Evaluation errors are reported with the source line and a caret, in the CLI
and in the manager's gRPC status alike.

## Known limitations

- **No environment-variable access**, by design (see above). Configs that
  previously interpolated `$HOME`-style values need KV or an imported file.
- **No incremental evaluation cache.** Every submit re-evaluates from source;
  fine at jobspec sizes, and the reason template rendering has a 5s budget.
- **Nickel's type annotations are not Legion's schema.** Contracts catch what
  the spec author declares; `legion job validate` and the manager still run
  the full jobspec validation on the resulting JSON. Use both.
- **`legion job schema`** emits JSON Schema for the *rendered* JSON, not a
  Nickel contract. Generating an importable Legion contract library from the
  same source is not implemented.
- **JSON is not a subset of Nickel**, unlike Jsonnet, where renaming `.json`
  to `.jsonnet` just worked. Records are `key = value`, and `:` is Nickel's
  type-annotation operator — so a JSON object handed to the Nickel parser does
  not fail cleanly, it fails as `invalid contract expression` (`{"a": 1}`
  reads as a field annotated with the contract `1`). Give the file a `.json`
  extension and it is read as JSON instead. Everything else in JSON — scalars,
  arrays, `true`/`false`/`null` — *is* valid Nickel.
- **A `.json` config is data only.** No variables, no imports, no comments.
  Import it from a `.ncl` file to get those back.

[nickel]: https://nickel-lang.org
