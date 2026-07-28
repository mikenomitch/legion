# Job Templates Plan

> **Judgement calls:** [../calls/job-templates-plan_calls.md](../calls/job-templates-plan_calls.md)

**Status: IN EXECUTION** — all phases in one PR. Deliberately deferred:
per-version deletion, secret-typed variables, template import/`importstr`
support in server-side rendering, remote template registries (explicitly
out of scope for now, expected later), and provenance-aware ACL scoping on
instance-level write endpoints.

## 1. Problem statement

Every job submission today requires the caller to hold the full jobspec and
render it client-side (`legion job run <path>` renders Jsonnet locally and
ships JSON — `src/cmd/job.rs:1448`). That means:

- **No reuse.** A platform team that wants "the blessed way to run a batch
  render" has to distribute a `.jsonnet` file out of band (git repo, copy
  paste). There is no cluster-resident, versioned artifact for it.
- **No guided submission.** The UI has no job-submit form at all (`RunJob` is
  never called from `ui/`). A user who just wants to tweak two parameters has
  to understand the whole jobspec.
- **No way to delegate narrowly.** ACLs can scope `job:write` by job name
  glob, but the grant covers *arbitrary specs*. There is no way to say "this
  account may only submit jobs shaped by template X" — the account can submit
  any spec whose name matches the glob.
- **The UI ignores ACLs.** Every nav item and every mutating button renders
  for every identity; denied calls fail at request time with a generic error.
  A locked-down token gets a UI full of things it cannot do.

PR #108 prototyped templates (versioned Jsonnet + manifest comment + UI form)
but predates both the ACL system and the current UI, has no CLI surface, no
ACL story, and extracts its manifest with a regex over comments.

## 2. Prior art

- **Nomad parameterized jobs** ([docs](https://developer.hashicorp.com/nomad/docs/job-specification/parameterized)):
  a job declares `meta_required`/`meta_optional` keys plus an optional payload;
  `nomad job dispatch` instantiates it. ACL-wise, `dispatch-job` is a separate
  capability from `submit-job`, which is exactly the delegation story we want.
  We take: dispatch as a distinct permission; typed-ish declared parameters.
  We reject: dispatch creates child job instances (`name/dispatch-<id>`);
  Legion templates render a full first-class job, and the template is a
  separate entity rather than a mode of a job.
- **Kubernetes** has no native template object (Helm/Kustomize live outside
  the cluster; the in-cluster attempt, OpenShift Templates, stores objects
  with `${PARAM}` string substitution). We take: the lesson that client-side
  templating cannot power a server-rendered UI form. We reject: string
  substitution — Legion already speaks Jsonnet, a real language with types.
- **Consul templated policies** name a parameterized policy that expands per
  token. Confirms the "small closed vocabulary + selectors" approach over an
  expression language.
- **PR #108** (this repo): versioned template rows + latest pointer, server
  side Jsonnet render with variables, provenance stamped into
  `job_config.meta`, UI form generated from variable definitions. We take all
  of those ideas. We reject: the `/* legion-template {...} */` regex manifest
  (fragile, invisible to Jsonnet), stringly-typed variables passed as
  `ext_var` strings (`std.parseInt(std.extVar(...))` in every template), no
  ACL integration, no CLI.

## 3. User scenarios

1. **Platform engineer publishes a template.** They write
   `render.jsonnet` with a `variables` list and a hidden `job(vars)`
   function, run `legion template create render -f render.jsonnet`, and the
   template appears (versioned) in the UI library for everyone with
   `template:read`.
2. **Operator delegates narrowly.** They attach a policy so the `render-bot`
   token can *only* submit via that template:

   ```jsonnet
   [
     { resources: ['template'], actions: ['read'], templates: ['render'] },
     { resources: ['job'], actions: ['read', 'write'], templates: ['render'] },
   ]
   ```

   `render-bot` opens the UI and sees only the Templates page with the one
   template (plus the jobs it created); `legion job run whatever.jsonnet`
   returns `PermissionDenied`.
3. **User submits with parameters.** In the UI they pick the template, get a
   typed form (numbers, toggles, selects) seeded with defaults, preview the
   rendered spec, and submit. From the CLI:
   `legion template run render --var title=demo --var frames=48 --wait`.
   The resulting job shows "from template render v3" in its spec history.

## 4. Goals and non-goals

**Goals**

- Cluster-resident, immutably versioned job templates with typed, declared
  variables; render + submit server-side.
- ACL integration: `template` resource (read/write) and a `templates` rule
  selector that makes "may only submit via template X" a one-policy
  statement — and makes jobs *created from* a template readable/stoppable by
  template-scoped identities.
- Guided UI: template library, typed parameter form, rendered preview,
  submit; plus UI-wide ACL awareness (nav and mutating controls follow the
  identity's effective rules via a `WhoAmI` extension).
- Full agent surface: CLI verbs with `--json`, MCP tools, capability catalog
  entries, idempotent run.

**Non-goals**

- Remote/federated template registries (pulling templates from other
  clusters or registries). Expected later; nothing here should preclude it —
  templates are self-contained rows keyed by name, so a future sync can
  replicate them.
- Jsonnet imports inside templates (server-side rendering forbids `import` /
  `importstr` — a template is one self-contained file).
- Secret-typed variables, per-variable regex validation, min/max bounds.
- Template-level RBAC beyond the ACL vocabulary (no per-template owner
  lists).
- Replacing `legion job run`. Raw spec submission is unchanged.

## 5. Background — what exists today

- Jsonnet renders client-side only; the manager receives JSON
  (`src/cmd/job.rs:898` `render_jobspec`; nothing under `src/manager/`
  constructs a `JsonnetVm`). The `jsonnet-rs` crate is already a dependency
  (`Cargo.toml:11`).
- `RunJob` handler (`src/manager/server.rs:1657`): coarse
  `check_acl(job, write, any())`, idempotency replay, KV-ref resolution,
  precise enforce on `AclScope::job(name)`, `validate_job_config`,
  `handle_job_submit`, rollout/eval enqueue. `RunJobRequest` carries
  `parsed_jobspec`, `submissions`, `priority`, `force_reschedule`,
  `idempotency_key` (`proto/legion.proto:734`).
- ACL engine (`src/structs/acl.rs`): resources
  `job|node|kv|state|operator|acl|exec|action`, actions `read|write`,
  glob selectors `namespaces|names|pools|paths|action_names` + `host` flag.
  `selector_matches` returns false for `(Some(_), None)` — a rule with a
  selector never matches a request lacking that attribute (`acl.rs:255-261`).
  `AclScope` carries the request attributes (`acl.rs:134`). Enforcement via
  `check_acl` / `authenticate_acl` + `acl::enforce`
  (`src/manager/server.rs:877-893`, `src/manager/acl.rs:616`).
- `WhoAmI` (`proto/legion.proto:521`, handler `src/manager/server.rs:4650`)
  returns identity metadata and policy *names* only. A scoped token cannot
  read its own rules (`GetAclPolicy` needs `acl:read`), so the UI cannot
  derive capabilities today.
- UI: SolidJS + Tailwind + ConnectRPC (`ui/src/grpcClient.ts`), token in
  localStorage, global identity signal `managerIdentity`
  (`ui/src/utils/connection.ts:33`). No permission-conditional rendering
  except the o11y nav item. Schema-driven form prior art:
  `ActionForm` (`ui/src/components/AllocationExecPanel.tsx:141`).
- Store: SQLite + append-only migrations; named-entity CRUD pattern =
  `saved_state_query` (`src/storage/store.rs:6334-6460`); idempotency via
  `idempotency_lookup`/`idempotency_record`.
- `tests/acl_endpoints.rs` parses the proto service block and fails unless
  every RPC is classified.

## 6. Proposed design

### 6.1 Template format: one Jsonnet file, no magic comments

A template is a Jsonnet file evaluating to an object with plain metadata
fields and a **hidden** `job(vars)` function:

```jsonnet
{
  description: 'Render a video scene on the batch pool',
  variables: [
    { key: 'scene', type: 'string', label: 'Scene URL' },
    { key: 'frames', type: 'number', default: 24 },
    { key: 'quality', type: 'select', options: ['draft', 'final'], default: 'draft' },
    { key: 'debug', type: 'boolean', default: false },
  ],
  job(vars):: {
    name: 'render-' + std.md5(vars.scene)[0:8],
    tasks: [{
      name: 'render',
      runtime: 'container',
      count: vars.frames,
      config: { image: 'ghcr.io/acme/render:' + vars.quality },
      env: { DEBUG: std.toString(vars.debug), SCENE: vars.scene },
    }],
  },
}
```

Evaluating the file yields the manifest (hidden `job::` is not emitted);
rendering evaluates `template.job(vars)` with `vars` as one native JSON
object — no `std.extVar` string parsing. The manager validates at upsert:
manifest shape, variable definitions, `job` present and a function; when
every variable has a default it also trial-renders and runs
`validate_job_config`.

Variable definitions: `key` (required, `[a-zA-Z_][a-zA-Z0-9_]*`), `type`
(`string | text | number | boolean | select | multi_select | json`),
optional `label`, `description`, `default` (typed JSON), `options`
(required for `select`/`multi_select`), `required` (defaults to true when
no `default`, false otherwise). Values are type-checked server-side;
strings are leniently coerced (`"2"` satisfies a `number`) so `--var`
key=value flags stay ergonomic.

Server-side rendering is sandboxed: imports are rejected via the VM import
callback, source is capped at 512 KiB, variables JSON at 64 KiB, and the
evaluation runs on a blocking thread under a 5s timeout.

### 6.2 gRPC surface

```proto
rpc UpsertJobTemplate (UpsertJobTemplateRequest) returns (UpsertJobTemplateReply);   // template:write (name)
rpc ListJobTemplates  (ListJobTemplatesRequest)  returns (ListJobTemplatesReply);    // template:read  (filtered)
rpc GetJobTemplate    (GetJobTemplateRequest)    returns (GetJobTemplateReply);      // template:read  (name)
rpc DeleteJobTemplate (DeleteJobTemplateRequest) returns (DeleteJobTemplateReply);   // template:write (name)
rpc RenderJobTemplate (RenderJobTemplateRequest) returns (RenderJobTemplateReply);   // job:write via template scope, no mutation
rpc RunJobTemplate    (RunJobTemplateRequest)    returns (RunJobTemplateReply);      // job:write via template scope
```

- `UpsertJobTemplate { name, source_jsonnet }` → stores the next immutable
  version, returns it. Names are slugs (job-name rules).
- `GetJobTemplate { name, version? }` → the requested (default latest)
  version plus a version-history list (metadata only).
- `RunJobTemplate { name, version?, variables_json, priority?,
  force_reschedule, idempotency_key? }` → validates variables, renders,
  submits through the same path as `RunJob`, stamps provenance. Reply carries
  `message`, `deduplicated`, `job_name`, `job_config_id`.
- `RenderJobTemplate` is the dry-run twin: same inputs, returns the rendered
  spec + parsed job name, mutates nothing.
- Submissions stored with the job config: the template source as primary
  (`template://<name>/<version>`), rendered JSON, and the variable values —
  full audit trail in existing spec-history UI.

### 6.3 ACL model

- **New resource `template`** — `read` = list/get, `write` = upsert/delete.
  Scoped by the new `templates` selector (one selector, one meaning:
  template names; `names` stays exclusively job names).
- **New rule selector `templates`** (glob list) + `template` attribute on
  `AclScope`:
  - On the **write path**, the attribute is set only when the submission
    goes through `RunJobTemplate`/`RenderJobTemplate` — never from
    provenance. `{ resources: ['job'], actions: ['write'],
    templates: ['render'] }` therefore grants submission *only via* the
    template; raw `RunJob` carries no template attribute and is denied by
    that rule.
  - On the **read path and lifecycle writes** (`ListJobs`, `GetJobInfo`,
    `GetJobSpec`, `GetJobSpecByConfig`, `ListJobSpecHistory`, `StopJob`,
    `DestroyJob`), the attribute is populated from the job's stored
    provenance, so template-scoped identities see and can stop the jobs
    their templates created. Endpoints not listed simply do not match
    `templates` rules (fail closed).
- **Collision guard.** A template run that would *update* an existing job
  whose latest config did not come from the same template is rejected
  (`FailedPrecondition`) unless the caller holds plain `job:write` on the
  job name. Without this, any template whose rendered name is
  user-influenced would let a template-only account overwrite unrelated
  jobs.
- Provenance is stamped into `job_config.meta.jobTemplate = { name,
  version }` and surfaced in `JobSpecHistoryItem` and `GetJobInfoReply`.

### 6.4 WhoAmI: effective rules for clients

`WhoAmIReply` gains `optional string rules_json` — the identity's flattened
effective rules (same schema as policy rules), present whenever a concrete
identity resolves; management tokens and disabled ACLs report
`[{"resources":["*"],"actions":["*"]}]`. This is self-introspection only —
it reveals nothing the caller's own requests would not reveal.

### 6.5 UI

- **Templates pages.** `/templates` list (name, latest version, variable
  count, updated) and `/templates/:name` detail: description, version
  picker, typed parameter form (text input, textarea, number, toggle,
  select, multi-select chips, JSON editor), "Preview rendered spec"
  (RenderJobTemplate) and "Submit" (RunJobTemplate) with the resulting job
  linked. "New template" / "New version" editors gated on `template:write`.
- **ACL awareness everywhere.** A small `ui/src/utils/acl.ts` mirrors the
  server's evaluate/glob logic over `rules_json` from `WhoAmI`, exposed as
  `allows(resource, action, scope?)`. Nav items hide when the identity lacks
  the backing `read` (Jobs→job, Nodes→node, KV→kv, State→state,
  Raft/Events/Observability/Manager→operator, Templates→template, admin
  panels→acl). Mutating controls (destroy/stop job, rollout advance/stop,
  KV save/delete, raft command, admin CRUD, exec) disable with a tooltip
  when not permitted. Fail open only when ACLs are disabled.

### 6.6 CLI

```
legion template create <NAME> -f render.jsonnet        # new version (upsert)
legion template list [-q render] [-j]
legion template get render [--version 2] [--source]
legion template run render --var scene=s3://... --var frames=48 \
    [--vars-file vars.json] [--version N] [--wait] [--idempotency-key K]
legion template render render --var scene=...          # dry run, prints spec
legion template delete render
```

`--var` values are strings coerced server-side per the declared type;
`--vars-file`/`--var-json` provide native JSON. All verbs take `--json` and
route through `output::resolve`/`print_success`.

MCP tools: `legion_template_list`, `legion_template_get`,
`legion_template_render` (read-only), `legion_template_run`,
`legion_template_upsert`, `legion_template_delete` (mutating, behind
`--allow-write`). Capability catalog entries for each verb.

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto`: six RPCs + messages (`JobTemplate`,
  `JobTemplateVersionInfo`, `JobTemplateVariable`, requests/replies),
  `JobTemplateProvenance` referenced from `JobSpecHistoryItem` and
  `GetJobInfoReply`, `WhoAmIReply.rules_json`, updated `AclPolicy` rule
  schema comment (`templates` selector, `template` resource).
- `src/structs/acl.rs`: `AclResource::Template` (+ `as_str`, `all()`),
  `AclRule.templates`, `AclScope.template` + `with_template(..)`,
  `has_scope_selectors`, `rule_matches`, parse validation, tests.
- Migrations (append-only): `job_template` (name PK, latest_version,
  timestamps) and `job_template_version` (name+version PK, description,
  source, variables JSONB, created_by, created_at).
- `src/storage/store.rs`: records + CRUD following `saved_state_query`;
  provenance plumbed through job submit (private inner fn; public
  `handle_job_submit` signature unchanged); `job_template_provenance(name)`
  and a batch map for list filtering.
- No new job-config fields → no `configs/jobs/maximal.jsonnet` change. A
  worked example template lands at `configs/templates/render.jsonnet`.

## 8. Implementation phases

1. **Template entity + authoring surface.** ACL resource `template`; store
   tables/CRUD; Upsert/List/Get/Delete RPCs + handlers + `acl_endpoints`
   rows; `legion template create|list|get|delete`. Valuable alone as a
   versioned spec library.
2. **Run/render path + delegation.** `templates` selector + scope attribute;
   Render/Run RPCs with sandboxed rendering, provenance, collision guard,
   idempotency; provenance-aware read/lifecycle scoping; CLI `run`/`render`;
   MCP + capabilities. The core feature.
3. **Templates UI.** Regenerated TS clients, `/templates` +
   `/templates/:name`, typed form, preview, submit, editors.
4. **UI ACL awareness.** `WhoAmIReply.rules_json`, `ui/src/utils/acl.ts`,
   nav + control gating. Valuable independent of templates.
5. **Docs.** `docs/job-templates.md`, auth-and-acls/agent-readiness/ui
   updates, indexes, example template, calls doc.

## 9. Testing plan

- **Unit (acl.rs):** `templates` selector matching, `(Some, None)` denial of
  raw submits, deny-rule interaction, parse validation of the new resource
  and selector.
- **Unit (store):** version increment, latest pointer, get by version,
  delete cascade, provenance stamp/readback, list summaries.
- **Server tests (manager/server.rs mod tests):** manifest validation
  errors, typed variable validation and coercion, import rejection, render
  timeout config, run happy path, collision guard (reject + plain-job:write
  override), provenance in spec history, idempotent re-run.
- **`tests/acl_endpoints.rs`:** six new classified rows + builders + seeded
  fixture template; scenario test: template-scoped token can run its
  template but not raw `RunJob`, sees only its jobs.
- **UI:** `npm run build` (tsc) green; manual smoke via seeded data.
- Full `cargo fmt` / `clippy -D warnings` / `cargo test --all --locked`.

## 10. Rollout and backward compatibility

- Pure addition: no change to existing jobs, policies, or `RunJob`.
- Migrations are append-only new tables.
- Policies using the `templates` selector are rejected by *older* managers
  (`deny_unknown_fields`) — upgrade managers before shipping such policies.
- ACL-disabled clusters get templates with no restrictions (same as every
  other resource); `WhoAmI` reports wildcard rules so the UI hides nothing.
- The UI gates purely cosmetically — the server remains the enforcement
  point.

## 11. Open questions

- Should `template:read` be implied by a `templates:`-selector job rule so
  the two-rule policy collapses to one? Lean: no — explicit beats implied in
  ACLs; ship the two-rule recipe in docs and revisit if it grates.
- Per-version deletion / retention caps. Lean: defer; whole-template delete
  only.

## 12. Acceptance criteria

- P1: `legion template create/list/get/delete` round-trips; unclassified-RPC
  test passes; template CRUD respects `template:read|write` scoping.
- P2: scenario in §3.2 works end to end exactly as written (delegated token
  can template-run but not raw-run; collision guard demonstrably blocks
  hijack); `legion template run --wait` lands a job whose spec history shows
  the template provenance and stored variables.
- P3: a template with every variable type renders a usable form in the UI;
  preview matches what `template render` prints; submit links to the job.
- P4: with the §3.2 policy, the UI shows only Templates (+ Jobs scoped to
  the template's jobs); admin UI unchanged for management tokens; ACLs
  disabled shows everything.
- P5: `docs/job-templates.md` stands alone; every new RPC/field has proto
  comments; capability catalog and MCP list include the new tools.
