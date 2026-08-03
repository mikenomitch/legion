# Job Templates

Job templates are cluster-resident, versioned Nickel documents with
declared, typed variables. A platform team publishes a template once;
anyone with access can then submit jobs from it — from the CLI, the UI's
guided form, or over gRPC/MCP — by supplying just the variable values. The
manager renders and validates server-side, so every client gets the same
result, and each submission records which template and version produced it.

Templates also plug into the ACL system as a delegation boundary: a policy
can grant an account the ability to submit jobs *only through* specific
templates, without granting general job submission. See
[Templates and ACLs](#templates-and-acls).

## Why templates exist

Without templates, every submitter needs the full jobspec and a local
Nickel evaluator (`legion job run` evaluates client-side). That makes reuse
an out-of-band problem (copying `.ncl` files around), makes guided
submission impossible for thin clients like the UI, and makes delegation
all-or-nothing: `job:write` lets a token submit *any* spec. Templates move
the spec to the cluster, the rendering to the manager, and the authorization
boundary to "this template, with these knobs".

## Writing a template

A template is **one self-contained Nickel file** that evaluates to a record
with optional `description` and `variables` fields and a
`job | not_exported = fun vars => …` function returning the jobspec:

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

`default` is a Nickel keyword, which is why it appears as a quoted field
name in the variable definitions. It is an ordinary data field here — not
Nickel's `| default` merge annotation.

Exporting the file yields the manifest: `| not_exported` drops `job` from
the export, which is how the manager reads the metadata without needing
variable values. Rendering applies `template.job` to **one native Nickel
record**: `vars.frames` is a number, `vars.debug` a boolean, `vars.regions`
an array. Nothing is passed as a string to be re-parsed.

A worked example lives at
[`configs/templates/render.ncl`](../configs/templates/render.ncl).

### Variable definitions

Each entry in `variables`:

| Field | Meaning |
| --- | --- |
| `key` | Required. `[a-zA-Z_][a-zA-Z0-9_]*`; becomes a field of `vars`. |
| `type` | Required. `string`, `text` (multiline), `number`, `boolean`, `select`, `multi_select`, `json` (any JSON value, passed through). |
| `label`, `description` | Optional; used by the UI form. Label defaults to the key. |
| `default` | Optional typed JSON value (`4`, `false`, `["us"]`). |
| `options` | Required for `select`/`multi_select`, forbidden otherwise. |
| `required` | Defaults to **true when there is no `default`**, false otherwise. `required = false` with no default renders as `null`. |

At submit time the manager merges provided values with defaults, rejects
unknown keys, and type-checks everything. Strings are leniently coerced
(`"4"` satisfies a `number`, `"true"` a `boolean`, `"us,eu"` a
`multi_select`) so `--var key=value` flags stay ergonomic.

### Validation at upsert

`UpsertJobTemplate` rejects sources that do not evaluate, do not declare a
`job` function, or declare malformed variables. When **every** variable has
a default, the manager also trial-renders the template and validates the
resulting jobspec, so most authoring mistakes surface at publish time
instead of at someone else's submit.

### Rendering is sandboxed

Rendering runs on the manager, with limits:

- **No imports.** Any use of the `import` keyword is rejected before
  evaluation starts, at publish time as well as at render time, with
  `imports are not allowed here; the source must be self-contained`. A
  template is one file. Rejection is lexical (the source is tokenized), so
  the word "import" inside a string or a comment is not a false positive.
- Source is capped at 512 KiB, the variables payload at 64 KiB.
- One render gets a 5-second wall-clock budget. Nickel cannot be
  interrupted mid-evaluation, so a runaway render is abandoned (the thread
  keeps a core busy until it finishes) and the request fails with
  `template rendering exceeded the 5s limit`.

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

Every verb takes `--json`; `run` supports `--idempotency-key`,
`--force-reschedule`, `--priority`/`--asap`, and `--wait`/`--timeout`, with
the same semantics as `legion job run`.

Versions are immutable and 1-indexed; `create` on an existing name appends
the next version and moves the latest pointer. Runs and renders use the
latest version unless `--version` pins one. Individual versions cannot be
deleted — only whole templates.

### In the UI

The **Templates** page (under Workloads) lists templates and, for identities
with `template:write`, offers a source editor. A template's page renders a
typed form from the variable definitions (text inputs, number fields,
toggles, selects, option chips, a JSON editor), a **Preview rendered spec**
button (dry run), and **Submit job**. Submissions link to the created job,
and a job created from a template shows a `template: <name> v<N>` badge
linking back.

### What a run records

`RunJobTemplate` renders, validates, and then follows the same path as
`RunJob`: same KV-reference resolution, artifact admission, rollout
creation, and eval enqueueing. Additionally it:

- stores the template source as the config's primary submission
  (`template://<name>/<version>`), plus the resolved variable values
  (`template.variables.json`) and the rendered JSON — the full audit trail
  is visible in the job's spec history;
- stamps provenance on the job config, surfaced in `GetJobInfo` and
  `ListJobSpecHistory` (and the UI badge).

A raw `legion job run` to the same job later replaces the latest config and
clears the job's *current* provenance (history keeps the old rows).

## Templates and ACLs

Two additions to the [ACL model](auth-and-acls.md):

- The **`template` resource**: `read` lists and fetches templates, `write`
  publishes and deletes them. Scoped by the `templates` selector (glob list
  of template names).
- The **`templates` rule selector on `job` rules**. A job rule carrying
  `templates` only matches requests that involve a matching template:
  submissions made *through* the template, and reads or lifecycle writes of
  jobs whose latest config *came from* it. Because a rule with a selector
  never matches a request without the attribute, such a rule grants nothing
  to plain `RunJob`.

The delegation recipe — an account that can only submit via the `render`
template, and can see and stop only the jobs it produces:

```nickel
[
  { resources = ["template"], actions = ["read"], templates = ["render"] },
  { resources = ["job"], actions = ["read", "write"], templates = ["render"] },
]
```

With that policy, `legion template run render …` succeeds while
`legion job run anything.ncl` returns `PermissionDenied`; `job list`
shows only jobs created from `render`; the UI shows only the pages and
templates the token can use.

Details worth knowing:

- **Provenance-aware endpoints.** The template attribute is populated from
  job provenance on `ListJobs`, `GetJobInfo`, `GetJobSpec`,
  `GetJobSpecByConfig`, `ListJobSpecHistory`, `StopJob`, and `DestroyJob`.
  Other job endpoints (instances, logs, exec, rollout operations) do not
  populate it yet, so a `templates`-scoped rule does not grant them — add
  `names`-scoped rules if a delegated account needs those. This is a known
  limitation, not a security hole (the failure mode is denial).
- **On the spec-changing write path the attribute is never taken from
  provenance** — only from the template actually being run. A template-only
  account cannot push arbitrary specs to a job just because its template
  once created it.
- **The collision guard.** A template run that would update an existing job
  whose latest config did not come from the same template fails with
  `FAILED_PRECONDITION`, unless the caller also holds plain `job:write` on
  that job name. Without this, a template whose rendered job name is
  user-influenced would let a template-only account overwrite unrelated
  jobs. Defense in depth: authors should also constrain rendered names
  (prefix them), and operators can add `names` selectors alongside
  `templates`.
- **Granting `template:run`-style access requires trusting the template
  author**: whoever holds `template:write` on a name controls what every
  delegated runner of that name submits.
- `RenderJobTemplate` (preview) requires the same access as running —
  read-only browsing never executes template code with your values.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `template must declare a job function field` at create | The source has no `job` field, or `job` is not a function. |
| `trial render with defaults failed: …` at create | Every variable has a default, so the manager rendered the template and the result was invalid. Fix the template, not the submit. |
| `variable 'x' is required` / `variable 'x': expected a number …` at run | Missing or mistyped value; `unknown variable 'x'` means a typo'd key. |
| `imports are not allowed here` | The template used the `import` keyword. Inline everything. |
| `template rendering exceeded the 5s limit` | Runaway or extremely heavy Nickel. Simplify the template. |
| `FAILED_PRECONDITION: job 'x' already exists and was not created from template …` | The collision guard (see above). |
| `PermissionDenied: job:write requires a policy granting it` on `template run` | The identity has neither plain `job:write` for the rendered name nor a `templates`-scoped job rule matching this template. |

## Known limitations

- No imports in templates; multi-file templates must be flattened first.
- No per-version deletion or retention caps.
- No secret-typed variables (values are stored with the job's submissions —
  do not pass credentials as variables; use [KV references](kv-store.md) in
  the template body instead).
- `template render`/`run` evaluate author-controlled code on the manager;
  the 5s timeout abandons (not kills) the render thread.
- Templates are per-cluster. Remote/federated template registries are an
  explicit non-goal for now.
