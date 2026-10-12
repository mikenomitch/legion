# Job Templates

A job template is a versioned Nickel document, stored in the cluster, with
declared and typed variables. A platform team publishes a template once.
Anyone with access can then submit jobs from it by supplying only the variable
values. They can do this from the CLI, the UI's guided form, or over gRPC/MCP.
The manager renders and validates the template, so every client gets the same
result. Each submission records which template and version produced it.

Templates also work with ACLs to delegate access. A policy can let an account
submit jobs only through specific templates, without granting general job
submission. See [Templates and ACLs](#templates-and-acls).

## Why templates exist

Without templates, every submitter needs the full jobspec and a local
Nickel evaluator (`legion job run` evaluates client-side). This causes three
problems:

- Reuse means copying `.ncl` files around.
- Thin clients like the UI can't offer guided submission.
- Delegation is all-or-nothing, since `job:write` lets a token submit any spec.

Templates move the spec to the cluster and the rendering to the manager. You
can then grant access to "this template, with these knobs" instead of to all
jobs.

## Writing a template

A template is one self-contained Nickel file. It evaluates to a record with
optional `description` and `variables` fields and a
`job | not_exported = fun vars => …` function that returns the jobspec.

Templates are the only config surface with no JSON form. `job` is a function
of the variables, and JSON has no functions. `legion template create` rejects a
`.json` file with an error that says so. (Every other surface accepts either.
See [Configuration Language](configuration-language.md#json-for-machine-emitted-config).)

```nickel
{
  description = "Render a demo scene on the cluster.",
  variables = [
    { key = "scene", type = "string", label = "Scene name", description = "Short slug." },
    { key = "frames", type = "number", "default" = 4 },
    { key = "quality", type = "select", options = ["draft", "final"], "default" = "draft" },
    { key = "debug", type = "boolean", "default" = false },
  ],
  job | not_exported = fun vars => {
    name = "render-%{vars.scene}",
    tasks = [{
      name = "render",
      runtime = "exec",
      count = vars.frames,
      config = { command = ["echo", "render %{vars.scene}"] },
    }],
  },
}
```

`default` is a Nickel keyword, so it's quoted as a field name in the variable
definitions. Here it is an ordinary data field, unrelated to Nickel's
`| default` merge annotation.

Exporting the file yields the manifest. `| not_exported` drops `job` from
the export, so the manager can read the metadata without variable values.
Rendering applies `template.job` to one native Nickel record: `vars.frames` is
a number, `vars.debug` a boolean, and `vars.regions` an array. Nothing is
passed as a string to be re-parsed.

A worked example lives at
[`configs/templates/render.ncl`](../configs/templates/render.ncl).

### Variable definitions

Each entry in `variables`:

| Field | Meaning |
| --- | --- |
| `key` | Required. `[a-zA-Z_][a-zA-Z0-9_]*`. Becomes a field of `vars`. |
| `type` | Required. `string`, `text` (multiline), `number`, `boolean`, `select`, `multi_select`, `json` (any JSON value, passed through). |
| `label`, `description` | Optional. Used by the UI form. Label defaults to the key. |
| `default` | Optional typed JSON value (`4`, `false`, `["us"]`). |
| `options` | Required for `select`/`multi_select`, forbidden otherwise. |
| `required` | Defaults to true when there is no `default`, false otherwise. `required = false` with no default renders as `null`. |

At submit time the manager merges provided values with defaults, rejects
unknown keys, and type-checks everything. Strings are coerced where it makes
sense (`"4"` satisfies a `number`, `"true"` a `boolean`, `"us,eu"` a
`multi_select`), so `--var key=value` flags stay easy to use.

### Validation at upsert

`UpsertJobTemplate` rejects sources that don't evaluate, don't declare a
`job` function, or declare malformed variables. When every variable has a
default, the manager also trial-renders the template and validates the
resulting jobspec. This catches most authoring mistakes at publish time
instead of when someone else submits.

### Rendering is sandboxed

Rendering runs on the manager, with limits:

- **No imports.** Legion rejects any use of the `import` keyword before
  evaluation starts, both at publish time and at render time, with
  `imports are not allowed here; the source must be self-contained`. A
  template is one file. The check is lexical (the source is tokenized), so
  the word "import" inside a string or a comment doesn't trigger it.
- Source is capped at 512 KiB, and the variables payload at 64 KiB.
- Every evaluation, both the manifest parse at publish time and each render,
  gets a 5-second wall-clock budget. Nickel can't be interrupted
  mid-evaluation, so a runaway evaluation is abandoned and the request fails
  with `template rendering exceeded the 5s limit`. The abandoned thread keeps
  a core busy until it finishes.
- Evaluations run on dedicated threads, never on the manager's async
  workers, and at most half the manager's cores (between 1 and 4) evaluate
  templates at once. An abandoned evaluation keeps its slot until it really
  finishes, so runaway templates can't take more than that share of the
  manager away from raft and the API. When every slot stays busy for the
  whole 5-second budget, the request fails with a retryable
  `RESOURCE_EXHAUSTED: the manager is already evaluating N templates; retry
  shortly`.

## Using templates

```bash
# Publish (creates version 1; publishing again appends version 2, 3, …)
legion template create render -f configs/templates/render.ncl

# Browse
legion template list
legion template get render            # variables, history; --source for the Nickel
legion template get render --version 1

# Dry run: validate variables and print the rendered jobspec
legion template render render --var scene=demo

# Submit (renders server-side, then follows the exact RunJob path)
legion template run render --var scene=demo --var frames=8 --wait

# Typed values: --var sends strings (coerced), --var-json sends JSON,
# --vars-file sends a whole object
legion template run render --var-json regions='["us","eu"]' --vars-file vars.json

# Remove the template (all versions; running jobs are unaffected)
legion template delete render
```

Every verb takes `--json`. `run` supports `--idempotency-key`,
`--force-reschedule`, `--priority`/`--asap`, and `--wait`/`--timeout`, with
the same behavior as `legion job run`.

Versions are immutable and start at 1. `create` on an existing name appends
the next version and moves the latest pointer. Runs and renders use the
latest version unless you pin one with `--version`. You can't delete
individual versions, only whole templates.

### In the UI

The **Templates** page (under Workloads) lists templates. A template's page
launches it in three steps:

1. **Variables** — a typed form built from the variable definitions (text
   inputs, number fields, toggles, selects, option chips, a JSON editor) and a
   version picker.
2. **Review** — whether the launch creates a new job or updates a running one.
   For an update it shows which values changed since the job's last launch
   from this template, and a diff of the job's current rendered spec against
   the new one, so re-launching over someone's environment is never a
   surprise.
3. **Launch** — submits exactly what was reviewed and follows the rollout. It
   also offers the equivalent `legion template run` command.

The **Source** tab shows each version's Nickel source; identities with
`template:write` also get the editor for publishing a new version. A job
created from a template shows a `template: <name> v<N>` badge that links
back.

The Review diff compares rendered specs; it does not predict placements (use
`legion job plan` for that). A token that may only run templates cannot read a
job that does not exist yet, so its Review cannot tell a new job from an
update. See [UI architecture](ui-architecture.md#job-templates) for the
details.

### What a run records

`RunJobTemplate` renders, validates, and then follows the same path as
`RunJob`: the same KV-reference resolution, artifact admission, rollout
creation, and eval enqueueing. It also:

- Stores the template source as the config's primary submission
  (`template://<name>/<version>`), plus the resolved variable values
  (`template.variables.json`) and the rendered JSON. You can see the full
  audit trail in the job's spec history.
- Stamps provenance on the job config, shown in `GetJobInfo` and
  `ListJobSpecHistory` (and the UI badge).

If someone later runs a plain `legion job run` against the same job, it
replaces the latest config and clears the job's current provenance. History
keeps the old rows.

## Templates and ACLs

Templates add two things to the [ACL model](auth-and-acls.md):

- The **`template` resource**. `read` lists and fetches templates, and `write`
  publishes and deletes them. Scope it with the `templates` selector (a glob
  list of template names).
- The **`templates` rule selector on `job` rules**. A job rule with
  `templates` only matches requests that involve a matching template. That
  means submissions made through the template, and reads or lifecycle writes
  of jobs whose latest config came from it. A rule with a selector never
  matches a request without that attribute, so such a rule grants nothing to
  plain `RunJob`.

Here is a policy for an account that can only submit through the `render`
template, and can see and stop only the jobs it produces:

```nickel
[
  { resources = ["template"], actions = ["read"], templates = ["render"] },
  { resources = ["job"], actions = ["read", "write"], templates = ["render"] },
]
```

With that policy:

- `legion template run render …` succeeds.
- `legion job run anything.ncl` returns `PermissionDenied`.
- `job list` shows only jobs created from `render`.
- The UI shows only the pages and templates the token can use.

Other things to know:

- **Provenance-aware endpoints.** The template attribute is filled in from
  job provenance on `ListJobs`, `GetJobInfo`, `GetJobSpec`,
  `GetJobSpecByConfig`, `ListJobSpecHistory`, `StopJob`, and `DestroyJob`.
  Other job endpoints (instances, logs, exec, rollout operations) don't fill
  it in yet, so a `templates`-scoped rule doesn't grant them. Add
  `names`-scoped rules if a delegated account needs those. This is a known
  limitation. It fails closed, by denying access.
- **Spec-changing writes ignore provenance.** On that path, the attribute
  comes only from the template being run. A template-only account can't push
  arbitrary specs to a job just because its template once created it.
- **The collision guard.** A template run fails with `FAILED_PRECONDITION`
  if it would update an existing job whose latest config didn't come from the
  same template. The exception is a caller who also holds plain `job:write`
  on that job name. Without this guard, a template whose rendered job name
  depends on user input would let a template-only account overwrite unrelated
  jobs. As extra protection, authors should also constrain rendered names
  (prefix them), and operators can add `names` selectors alongside
  `templates`.
- **Delegating template access means trusting the template author.** Whoever
  holds `template:write` on a name controls what every delegated runner of
  that name submits.
- `RenderJobTemplate` (preview) requires the same access as running.
  Read-only browsing never runs template code with your values.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `template must declare a job function field` at create | The source has no `job` field, or `job` is not a function. |
| `trial render with defaults failed: …` at create | Every variable has a default, so the manager rendered the template and the result was invalid. Fix the template, not the submit. |
| `variable 'x' is required` / `variable 'x': expected a number …` at run | Missing or mistyped value. `unknown variable 'x'` means a typo'd key. |
| `imports are not allowed here` | The template used the `import` keyword. Inline everything. |
| `template rendering exceeded the 5s limit` | Runaway or extremely heavy Nickel. Simplify the template. |
| `RESOURCE_EXHAUSTED: the manager is already evaluating N templates` | Every evaluation slot is busy, possibly with runaway templates that haven't finished. Retry. If it persists, find the template that times out. Restarting the manager clears abandoned evaluations. |
| `FAILED_PRECONDITION: job 'x' already exists and was not created from template …` | The collision guard (see above). |
| `PermissionDenied: job:write requires a policy granting it` on `template run` | The identity has neither plain `job:write` for the rendered name nor a `templates`-scoped job rule matching this template. |

## Known limitations

- No imports in templates. Flatten multi-file templates first.
- No per-version deletion or retention caps.
- No secret-typed variables. Values are stored with the job's submissions,
  so don't pass credentials as variables. Use [KV references](kv-store.md) in
  the template body instead.
- `template render`/`run` evaluate author-controlled code on the manager.
  The 5s timeout abandons the evaluation thread but doesn't kill it. The
  concurrency cap bounds the damage, but a stream of runaway templates can
  keep every slot busy, which blocks all template publishes and renders
  until the threads finish or the manager restarts.
- Templates are per-cluster. Remote or federated template registries are not
  planned for now.
