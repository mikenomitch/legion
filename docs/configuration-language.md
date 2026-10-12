# Configuration Language (Nickel)

Legion reads every configuration file as a [Nickel][nickel] program:

| File | Read by | Example |
| --- | --- | --- |
| Job specs | `legion job run/plan/validate` | [`configs/jobs/maximal.ncl`](../configs/jobs/maximal.ncl) |
| Manager config | `legion start manager -c` | [`configs/manager.ncl`](../configs/manager.ncl) |
| Worker configs | manager startup, from `workerConfigPath` | [`configs/workers/default.ncl`](../configs/workers/default.ncl) |

Both server configs are optional. `legion start manager` with no `-c` runs
on built-in defaults and reads nothing from the working directory, so it
starts the same way from any directory. With no `workerConfigPath` (the
default) the manager registers one built-in worker type, `default`, in pool
`default` with every setting at its default, which is the type
`legion join-token create` introduces when `--type` is omitted. Set
`workerConfigPath` to a directory of worker configs to declare your own types
instead; a configured directory that does not exist stops the manager at
startup with an error naming it. The repo's `configs/manager.ncl` sets
`workerConfigPath = "./configs/workers/"`, so it is meant to be run from a
checkout.
| ACL policies | `legion acl policy create -f` | see [Auth & ACLs](auth-and-acls.md) |
| Job templates | `legion template create -f` | [`configs/templates/render.ncl`](../configs/templates/render.ncl) |

Legion evaluates a file the way `nickel export --format json` does, then
deserializes the resulting JSON. Legion accepts anything Nickel can produce.
None of the schemas below are Nickel-specific.

Nickel is the default, and every example here is written in it. The first four
file types also accept `.json`, for config that a program emits (see
[JSON](#json-for-machine-emitted-config)). Job templates are Nickel-only,
because a template declares `job` as a function of its variables.

## Why Nickel

Job specs are data, but people copy them, parameterize them, and get them
wrong. Nickel gives Legion three things plain JSON doesn't:

- **Functions, `let`, and imports**, so you write a fleet of similar jobs
  once. You don't need a separate templating layer on top of the format.
- **Contracts** (`| String`, `| Number`, custom predicates) that fail at
  evaluation with the offending line, before the manager sees the spec.
- **Merging with defaults** (`| default`, `&`), which fits the common case of
  "here are the defaults, here is what the caller overrode."

Error messages include source snippets, so a mistake points at the line that
caused it instead of at the JSON output.

[`configs/jobs/nickel-showcase.ncl`](../configs/jobs/nickel-showcase.ncl)
puts these together in one spec: a custom contract, `--var` variables with
defaults, an imported task-builder function, `std` helpers, and record
merging.

## Syntax essentials

If you know JSON, three differences cover almost everything:

```nickel
{
  name = "web",            # `=`, not `:`
  count = 2,               # `#` comments, not `//`
  tags = ["a", "b"],       # strings are double-quoted, always
}
```

Two Nickel-specific rules tend to trip people up in Legion configs.

**Quote keyword field names.** Nickel reserves `default`, `priority`,
`doc`, `optional`, `force`, `merge`, and `not_exported` (plus the usual
`let`, `in`, `fun`, `if`, `import`, and so on). Both a jobspec's priority band
and a job template's variable defaults use these names:

```nickel
{
  name = "api",
  "priority" = "high",     # `priority` is a Nickel keyword
}
```

**Records are recursive.** A field refers to its own record before it refers
to an enclosing binding. So in `fun name => { name = name }`, the inner `name`
refers to the field itself and not the argument. Give parameters distinct
names:

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
Nickel's `std`. That identifier is `vars`, a record built from the command
line.

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

`legion job run`, `legion job plan`, and `legion job validate` all accept
these flags. The KV forms need a reachable manager. The plain forms don't, so
`legion job validate` still works offline.

`vars` is always bound and defaults to `{}`. A spec that never mentions it is
unaffected, and there is no flag to "declare" a variable. To give a variable a
default, merge over `vars`. This is the idiomatic Nickel form, and it gives a
clear error when a required variable is missing:

```nickel
let params =
  { replicas | Number | default = 1, image | String }
  & vars
in
{ name = "web", task = { runtime = "container", count = params.replicas } }
```

Running that without `--var image=…` fails at evaluation with an error naming
`image`, instead of submitting a jobspec with a missing field.

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

Imports are hermetic. The imported file doesn't see the importer's scope,
including `vars`. To parameterize a shared fragment, export a function and
apply it.

`legion job run` records every file the evaluation read (the spec plus all of
its imports, transitively) and stores them with the job. `legion job spec`
then shows the source a running job was submitted from.

[`configs/jobs/demo-imports-2.ncl`](../configs/jobs/demo-imports-2.ncl) is a
runnable example.

Imports resolve against the spec's own directory, even when you pass the spec
as a relative path from somewhere else. `legion job run` reads a local spec in
place instead of copying it. A spec fetched from a remote source (`https://`,
`git::`, `s3://`) is staged as a whole tree, so its imports resolve too. Use
the `#<relative path>` fragment to name the entry point inside a fetched
directory.

### Where imports are not allowed

The manager renders [job templates](job-templates.md) on a caller's behalf.
A template that could `import "/etc/shadow"` would let callers read files off
the manager. So Legion rejects any use of the `import` keyword in a template
before evaluation starts, both at publish time and at render time:

```
imports are not allowed here; the source must be self-contained
```

The check is lexical (the source is tokenized), so the word "import" inside a
string or a comment doesn't trigger it.

## JSON, for machine-emitted config

For job specs, the manager config, worker configs, and ACL policies, a file
named `.json` is parsed as JSON instead of evaluated:

```console
$ legion job run configs/jobs/machine-generated.json
```

JSON support is for config that a program writes, such as Terraform, a CI
job, a control plane, or an agent. Those tools already have a JSON serializer.
Making them emit Nickel would gain nothing, since Nickel's abstractions are
for humans and a generator has its own.

Write anything a person maintains in Nickel. JSON has no variables, no
imports, no comments, and no way to define a shared value once.

**Legion picks the parser by file extension only.** It doesn't sniff content.
The name always tells you how the file will be read, and errors are always
reported in the language you named. A `.json` file is usually not valid Nickel
(see [Known limitations](#known-limitations)), so the extension is what saves
you from a confusing parse error.

Legion rejects `--var` and related flags against a `.json` file instead of
ignoring them:

```
configs/jobs/machine-generated.json is JSON, which has no variables — drop the
variable flags, or use a .ncl file that reads `vars` and imports the JSON
```

### Mixing the two

Nickel imports JSON natively, also based on the file extension. This is the
supported way to combine generated data with hand-written structure. The
generator owns a data file, and the spec stays readable:

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

The imported JSON is data only. It can't contain Nickel expressions.

### Job templates are Nickel-only

A [job template](job-templates.md) declares `job` as a function of its
variables (`job | not_exported = fun vars => …`). JSON has no functions, so
there is no JSON form of a template. `legion template create` tells you this
directly instead of letting the parser report a syntax error:

```
$ legion template create render -f render.json
error: render.json is JSON, but a job template must be Nickel (a template
       declares `job` as a function of its variables …)
```

If a generator needs something like a template, have it publish the rendered
`.json` jobspec instead. Or write the small Nickel wrapper by hand once and
keep the varying data in an imported `.json`.

## No environment access

Nickel can't read the process environment, and Legion doesn't add a way to.
Get secrets and per-host values from one of these instead:

- **[KV](kv-store.md)**: `--kv-var` at submit time, or a `{{ kv "path" }}`
  reference that the manager resolves on every rollout.
- **A separate, uncommitted file** the config imports:

```nickel
let secrets = import "secrets.ncl" in
{ persistence = { streaming = { credentials = {
  accessKeyId = secrets.accessKeyId,
  secretAccessKey = secrets.secretAccessKey,
} } } }
```

A job's runtime environment is a separate thing. `config.env` is passed to the
task and has nothing to do with evaluation.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `unexpected token` pointing at a `:` | JSON habit. Record fields use `=`. |
| `unexpected token` pointing at `priority`/`default` | A Nickel keyword used as a bare field name. Quote it: `"priority" = …`. |
| `infinite recursion … recursive reference` | A field named the same as the binding it meant to read (`{ name = name }`). Rename the parameter. |
| `(+) expects its 1st argument to be a Number` | String concatenation written as `+`. Use `++`, or interpolate with `%{…}`. |
| `contract broken by a value` | A `\| String` / `\| Number` annotation rejecting the value. This is usually the actual bug, caught one step earlier than it would otherwise surface. |
| `imports are not allowed here` | An `import` inside a job template. Inline it. |
| `jobspec must define at least one task` | The file evaluated fine but is a fragment, not a whole jobspec. |
| `invalid contract expression … can't be used as a contract` | A JSON object being read as Nickel. Rename the file to `.json`, or convert `:` to `=`. |
| `is not valid JSON` | A `.json` file that doesn't parse. The message includes the line and column. |
| `is JSON, which has no variables` | `--var` against a `.json` spec. |

Evaluation errors include the source line and a caret, both in the CLI and in
the manager's gRPC status.

## Known limitations

- **No environment-variable access**, by design (see above). Configs that
  interpolated `$HOME`-style values need KV or an imported file.
- **No incremental evaluation cache.** Every submit re-evaluates from source.
  That's fine at jobspec sizes, and it's why template rendering has a 5s
  budget.
- **Nickel's type annotations are not Legion's schema.** Contracts catch what
  the spec author declares. `legion job validate` and the manager still run
  full jobspec validation on the resulting JSON. Use both.
- **`legion job schema`** emits JSON Schema for the *rendered* JSON, not a
  Nickel contract. Generating an importable Legion contract library from the
  same source is not implemented.
- **JSON is not a subset of Nickel.** Records are `key = value`, and `:` is
  Nickel's type-annotation operator. A JSON object passed to the Nickel parser
  fails with a confusing `invalid contract expression` error (`{"a": 1}`
  reads as a field annotated with the contract `1`). Give the file a `.json`
  extension and Legion reads it as JSON instead. Everything else in JSON
  (scalars, arrays, `true`/`false`/`null`) is valid Nickel.
- **A `.json` config is data only.** No variables, no imports, no comments.
  Import it from a `.ncl` file to get those back.

[nickel]: https://nickel-lang.org
