# Apps

An **app** is one of Legion's add-ons: the autoscaler, and whatever you
install from a manifest. It is three things:

1. a **job** in the reserved `legion-system` namespace;
2. a **grant**: what that job's `legion` identity may do in Legion
   ([Workload Identity: Permissions](workload-identity.md#permissions));
3. **registrations**: the UI slots and jobspec keys it adds (stored now,
   rendered by later releases; see [Limitations](#limitations)).

A **manifest** declares all three. An **app record**, stored on the
managers, holds the installed manifest, the operator's settings and config,
and the grant. The manager's **app reconciler** keeps the job matching the
record: it submits the job, re-renders it when the record changes, reverts
direct edits to it, and restarts it when someone stops it.

An app is an ordinary job. It is scheduled, restarted, rolled out, logged,
metered, and exec'd into (by those allowed) like any other. Everything that
makes it an app is around the job.

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI   DETAIL
autoscaler  0.1.0    bundled  running  managers   1/1       -    on legion-system-x8f2k1qz

$ legion app status autoscaler
$ legion app logs autoscaler -f
$ legion app config get autoscaler
$ legion app set autoscaler --placement any
$ legion app disable autoscaler
$ legion app install ./gitops.ncl
```

## Bundled and installed apps

**Bundled apps** are compiled into the `legion` binary. Today there is one,
`autoscaler`. The first manager to lead a new cluster's control plane
**seeds** a record for each bundled app, enabled, with its manifest's
permissions granted by `legion:bootstrap`, and the app starts with nothing
configured. A bundled app runs the node's own binary: its job's command is
`legion:self app run <name>` (see [Inside an app](#inside-an-app)), so it is
always the same build as the node that runs it, with nothing to download.

Bundled apps can be disabled, configured, and moved, but not uninstalled.
They upgrade with the binary ([Upgrades](#upgrades-of-bundled-apps)).

**Installed apps** come from a manifest file or URL (`legion app install`).
They carry their own `version`, run their own code (an artifact or an image,
like any job), and can be upgraded and uninstalled.

### Seeding and the manager config

A manager config may set seed-time settings for bundled apps:

```nickel
# configs/manager.ncl
apps = {
  autoscaler = { enabled = false },   # also: placement, replicas, config
},
```

The block is read **only when a bundled app's record is first seeded**.
Managers can disagree about their config, and the record is the single
source of truth afterwards, so later edits do nothing; a manager that finds
the block for an app that already exists logs that it was not applied. Use
`legion app` from then on. A `config` that does not match the app's schema,
or a bad `placement` or `replicas`, is logged and replaced with the
manifest's default.

## The manifest

A manifest is Nickel or JSON. Unknown fields are errors.

```nickel
{
  name = "gitops",                       # the app, and its job: legion-system/gitops
  description = "Deploys jobs from git repositories.",
  version = "0.2.0",                     # required for installed apps
  placement = "managers",                # managers (default) | workers | any
  replicas = 1,                          # default 1, at most 32
  job = {                                # a jobspec, minus name and namespace
    task = {
      name = "sync",
      runtime = "exec",
      artifacts = ["https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz"],
      config = { command = ["./legion-gitops"] },
      resources = { cpu = 200, memory = 128 },
    },
  },
  permissions = [                        # the grant the app asks for
    { resources = ["job"], actions = ["read", "write"], namespaces = ["team-*"] },
    { resources = ["template"], actions = ["read"] },
    { resources = ["kv"], actions = ["read"], paths = ["legion/apps/gitops/*"] },
  ],
  ui = { ... },                          # UI registrations (stored; see Limitations)
  extensions = [ ... ],                  # jobspec keys (stored; see Limitations)
  config = {
    schema = { type = "object", properties = { repos = { type = "array" } } },
    default = { repos = [] },
  },
  codeFields = [                         # config that changes the code the app runs
    "/plugins/*/url",
    { pointer = "/plugins/*/source", artifact = true },
  ],
}
```

[`configs/apps/example-app.ncl`](../configs/apps/example-app.ncl) is a
complete example with comments.

| Field | Meaning |
| --- | --- |
| `name` | Lowercase letters, digits and `-`, starting with a letter, at most 40. The job is `legion-system/<name>`. |
| `description` | One line. |
| `version` | Required for an installed app, any non-blank string without spaces; a bundled manifest has none. |
| `placement` | Where the app runs by default ([Placement](#placement)). |
| `replicas` | Each task's `count` by default. |
| `job` | A jobspec without `name` or `namespace`, which Legion sets. Must define `task` or `tasks`, and parse as a jobspec. |
| `permissions` | ACL rules in the shape `legion acl policy apply` takes: what the app's `legion` identity asks to be granted. |
| `ui` | A record of UI registrations. Validated as a record and stored. |
| `extensions` | Jobspec key declarations. Validated as a list and stored. |
| `config.schema` | A JSON Schema (draft 7) the config must match. It must describe an object, be at most 64 KiB, and use only local `$ref`s (`"#/…"`); a remote ref is refused before anything is fetched. Default: any object. |
| `config.default` | The config a fresh install gets. Must match the schema. Default `{}`. |
| `codeFields` | JSON pointers into the config whose values change the code the app runs. A `*` segment matches every key of an object or element of an array. An entry `{ pointer, artifact = true }` is also fetched as an artifact of the app's job (below). |
| `namespace` | Reserved for apps owned by a tenant namespace, which Legion does not support yet: setting it is an error. |

### What Legion renders

The reconciler renders the app's job from the record, as a pure function of
it (and, for a bundled app, the build marker):

- the manifest's `job`, named `legion-system/<app>`;
- every task's `count` is the app's `replicas`;
- the placement becomes each task's `permit`, constraints, and affinities
  (below), replacing the manifest's permit and adding to its constraints;
- `resources` from `legion app set --cpu/--memory` override each task's;
- an `exec` task that names no `user` runs as `systemWorker.appUser`
  (default `nobody`). Tasks in `legion-system` may not run as root
  ([Namespaces](namespaces.md#reserved-namespaces));
- every task's `legion` identity carries the app's recorded grant, as its
  `permissions` (a `policies` list in the manifest is dropped);
- the config is written as a file into each `exec` and `container` task's
  directory, named by `$LEGION_APP_CONFIG`;
- each string value at an artifact code field becomes an artifact fetched into
  `code/<path>/` in the task directory, with the sibling `checksum` string,
  if there is one, as its checksum (`/plugins/aws/source` lands in
  `code/plugins/aws/`);
- a bundled app gets the build-marker constraint, and `LEGION_APP_BUILD` in
  its environment.

### Placement

Placement is a setting, not part of the build: `legion app set autoscaler
--placement workers` changes the record, and the reconciler resubmits the
job (a normal rollout).

| `placement` | Permit | Constraint / affinity |
| --- | --- | --- |
| `managers` (default) | pools `legion-system` and `default` | constraint `legion.role = "manager"` |
| `workers` | pool `default` (or `--pool`) | constraint `legion.role is_not_set` |
| `any` | both (`legion-system` and `default`, or `--pool`) | affinity toward `legion.role = "manager"` |

`legion.role = "manager"` marks a manager's own worker: its
[system worker](architecture-managers-workers.md#the-system-worker), or a
`--run-worker` manager's worker. Keying `managers` on the attribute rather
than the pool is what makes it work on both. With `systemWorker.enabled =
false` on every manager, `managers` placement has nowhere to run, and
`legion app status` says so; set `--placement any` or `workers`.

## Lifecycle

### Installing

```console
$ legion app install https://example.com/gitops/0.2.0/app.ncl
gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,
in legion-system, and asks for:
  job       read, write   namespaces: team-*
  template  read
  kv        read          paths: legion/apps/gitops/*
UI: a GitOps page, a Sync panel on job pages, one action (Sync now, needs job:write)
Your token holds all of these. Grant them and install? [y/N] y
installed gitops 0.2.0; job legion-system/gitops starting
```

`install` takes a file (`.ncl` or `.json`) or an `http(s)` URL. A URL is
fetched by the CLI (at most 1 MiB); a Nickel manifest from a URL must be
self-contained, because imports are refused. The CLI asks the manager for a
dry run first (`InstallApp` with `dry_run`), prints the prompt, and asks
before installing; `--yes` skips the question (and is required with
`--json`), `--dry-run` stops after the prompt, and `--config <file>` sets
the initial config.

**You can grant only what you hold.** Installing needs `app:write` on the
app's name, and every permission the manifest asks for, checked exactly as a
job submit checks a job's `permissions`
([Job grants](auth-and-acls.md#job-grants)). The grant is recorded on the app
record in the installer's name:

```text
Error: app gitops 0.2.0 asks for job:write in team-*, which your token does not hold
Hint: have someone who holds it do this, or ask for it to be granted to you
```

A bundled app's name cannot be installed, and an installed name cannot be
installed again (`legion app upgrade` replaces a manifest).

### Upgrading

```console
$ legion app upgrade gitops ./gitops-0.3.0.ncl
gitops 0.3.0 runs its own code (1 artifact) on your managers, as user nobody, ...
Permission changes:
  + net       read
  - template  read
Approve the new permissions and upgrade? [y/N] y
upgraded gitops 0.2.0 → 0.3.0
```

A new manifest is new code running with the grant, so upgrading needs
`app:write` and every permission of the **new** manifest, as installing
does. When the new manifest asks for anything the installed one did not, the
upgrade needs approval (`--yes`, or the question); one that only keeps or
drops permissions upgrades without asking. The stored config must match the
new schema; `--config <file>` replaces it in the same step. `--dry-run`
shows the diff and changes nothing. Bundled apps cannot be upgraded this
way.

### Config

```console
$ legion app config schema autoscaler     # JSON Schema
$ legion app config get autoscaler        # JSON, valid input for `config set`
$ legion app config set autoscaler -f autoscaler.ncl
autoscaler: config changed; its job rolls with the new config
```

The config lives on the app record. `config set` replaces it whole,
validated against the manifest's schema, and the reconciler resubmits the
job with the config rendered as a file. There is no live reload: a config
change is a rollout, so the job's version history is the config's history.

**Code fields need the grant.** A change that adds, removes, or changes a
value at one of the manifest's `codeFields` (for the autoscaler, a plugin's
`source` or `url`) changes what code runs with the app's grant. It needs the
caller to hold every permission of the app's recorded grant, as installing
does; other config changes need only `app:write`. Without this, `app:write`
on the autoscaler would be a path to `node:write`.

```text
Error: changing app autoscaler's code (/plugins/aws/source) runs code with its grant, which asks for
node:write, which your token does not hold
```

### Settings, enable, disable

```console
$ legion app set autoscaler --replicas 2 --placement any
autoscaler: settings changed (replicas 1 → 2, placement managers → any)
$ legion app set autoscaler --cpu 500 --memory 512    # per task; 0 clears an override
$ legion app disable autoscaler     # stops the job; keeps the record, config, and grant
$ legion app enable autoscaler
```

`--pool` picks the worker pool for `workers` and `any` placement. Enabling
an app that is already enabled is not a no-op: it is a record write, which
retries a reconcile a refusal parked ([Failures](#failures)).

### Uninstalling

```console
$ legion app uninstall gitops
uninstalled gitops; job legion-system/gitops stopped and 3 grant(s) revoked (`legion job destroy legion-system/gitops` removes the job)
```

Uninstall disables the app, stops its job, revokes the job's grants (its
workload tokens lose them at their next request), and deletes the record. The
stopped job stays until someone destroys it. Bundled apps cannot be
uninstalled; disable them.

## How an app stays running

The reconciler runs on the manager that leads the control plane's Control
lane. It handles one app at a time, with at most one submit in flight per
app. Each pass, for each app:

1. **Render** the job from the record (above), and digest it.
2. **Compare the digest**, not the job. Every version the reconciler submits
   is stamped `managedBy = { app, digest }`. When the job's latest version
   carries the fresh render's digest, there is nothing to do. The reconciler
   never diffs the rendered job against the stored one: the stored config has
   defaults filled in and fields normalized, so a structural diff would find
   differences that are not there.
3. **Check the job's state.** An enabled app whose job was stopped is
   resubmitted; a disabled app's running job is stopped.
4. **Submit** through the normal submit path, as `legion:app-reconciler`.
   The submit is checked against the grant recorded on the app, not against
   any principal's own rights, and the version's job grant is recorded as
   `<granter> (app <name>)`.

**Which writes keep the stamp.** The reconciler's own submits set it. A KV
re-render of the job ([KV Store](kv-store.md)) keeps it, because it
re-renders the same spec. Any other write that stores a version (`RunJob`,
`RunJobTemplate`) clears it.

**Direct edits are reverted.** A direct submit of an app's job clears the
stamp, and its job event wakes the reconciler, so the edit is reverted within
seconds, and the event says by whom:

```console
$ legion job info legion-system/autoscaler
...
managed by app autoscaler (digest 3f9a12c4…); direct edits are reverted. Use `legion app`.

$ legion events --kind app.reconciled
app.reconciled  App autoscaler: reverted a direct edit by userpass:alice (from legion-system/autoscaler-7)
```

Submitting an app's job directly still needs everything the app's grant
holds (a submit always checks the job's `permissions` against the
submitter), so a token with `job:write` on `legion-system` alone cannot
launder the app's grant by editing its job.

**Exactly once per transition.** Each submit carries the idempotency key
`app:<name>:<digest>:<from>`, where `<from>` is the state it starts from (the
job's latest version, `none`, or `stopped-<version>`). If the leader dies
after its submit lands but before it records that, the next leader's pass
finds the stamp and does nothing, and a retry of the same key is
deduplicated. A new transition (reverting the same edit a second time,
enabling within a day of disabling) starts from another state and gets a new
key.

**Triggers.** A pass runs for an app when its record changes (any `legion
app` write), when its job changes (a new version, a stop), when a node joins
or re-registers (bundled apps' build markers), when a manager takes over the
Control lane (it also seeds bundled apps then), and every 60 seconds for
every app, in case an event was missed. Bursts coalesce: changes that arrive
during a pass queue one more pass, not one each.

### Failures

- A failed submit retries with backoff, from 5 seconds doubling to 5
  minutes. The app's status is `degraded` with `reconcile failing: <error>`,
  and `app.reconcile_failed` is emitted on the first failure and then at
  most every 10 minutes. A write to the app retries at once.
- A submit the manager **refuses** (the job fails validation, a grant check,
  or a quota) does not retry on its own, since retrying cannot fix it. The
  status reads `reconcile refused: <error>`, and the reconciler waits for the
  next change to the app's record or to its build marker. `legion app enable
  <app>` is a write that retries it after you fix the cause elsewhere (a
  namespace quota, say).

### Status

`legion app list`, `legion app status`, and `GetApp` derive an app's status
when they are read:

| Status | When |
| --- | --- |
| `stopped` | the app is disabled |
| `degraded` | the last reconcile failed, or fewer allocations run healthy than it asks for (`2/3 running`) |
| `pending` | nothing runs yet: no build marker, no job yet, the job was stopped and is being restarted, or the scheduler's reason it is blocked (`no room in pool legion-system`) |
| `running` | every allocation it asks for runs and reports healthy (its health checks pass) |

`legion app status` also shows the record, the grant and who approved it,
the reconciler's last action, error, and digest, the build marker and what
the app waits for, and notes (no system worker for `managers` placement,
workload identity off).

## Upgrades of bundled apps

A bundled app's code is the binary of the node it runs on. The reconciler
restarts it only when there is one new build to move to:

- Workers report `buildVersion` (the package version) and `buildId` (a hash
  of their own executable, computed at startup) when they register. A
  rebuild without a version bump changes `buildId`.
- The app's **build marker** is the (`buildVersion`, `buildId`) pair, defined
  when every ready node its placement allows reports the same one. Nodes that
  report `unknown`, an unparsable version, or no build id are left out.
- While those nodes disagree (during an upgrade), the marker keeps its last
  value: a straggler holds it, and an older node joining later does not move
  it backwards. When they agree again, the marker moves and the app rolls
  once. That covers a deliberate downgrade of the whole set too.
- With no ready eligible node there is no marker yet, and the app waits
  (`pending: waiting for an eligible node to report its build`).
- The job carries `{ key = "buildVersion", operator = "version_matches", value
  = ">=<marker version>" }`, so a reschedule never lands it on an older node.

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI  DETAIL
autoscaler  0.14.0   bundled  running  managers   1/1       -   upgrade waiting: 2 of 3 eligible nodes on 0.15.0 (waiting for mgr-3)
```

**New bundled manifests.** A new build can change a bundled app's job,
permissions, or config schema. The reconciler adopts the new manifest only
when the leading manager itself runs the marker's build, since it can read
only manifests compiled into its own binary; until then it renders from the
manifest stored on the record. New permissions are recorded as granted by
`legion:bootstrap` and reported in the `app.upgraded` event. A stored config
that no longer matches the new schema is kept, and reported in the app's
status.

Limits:

- **"Once" counts the reconciler's own restarts.** A manager restarting for
  its upgrade still kills the allocations on its system worker, and the
  scheduler places them again, possibly on a manager that has not upgraded.
- **Pre-releases.** `version_matches` uses semver requirements, under which a
  pre-release of a newer version (`0.16.0-rc.1`) does not satisfy
  `>=0.15.0`. During a mixed window, such nodes cannot host the app; once
  every eligible node runs the pre-release, the marker moves to it.

## Trust model

- **An app's power is its grant**, and the grant is never more than a person
  held: the installer (checked at install), an upgrader (checked at
  upgrade), or whoever changes a code field (checked at `config set`).
  Bundled apps are the exception: their grants are recorded by
  `legion:bootstrap` at seed time, without a check, because the manifest is
  part of the binary you chose to run, which is already the manager.
- **`app:write` is not the grant.** It changes settings, config outside code
  fields, and enabled state; it cannot add code to an app.
- **Apps run unprivileged.** Tasks in `legion-system` may not run as root,
  and privileged containers are refused there
  ([Namespaces](namespaces.md#reserved-namespaces)). Root on a manager could
  read the raft state and the data directory, so an app's blast radius stays
  its grant.
- **Tenants cannot reach apps.** ACL globs do not match the reserved
  `legion-*` namespaces or the `legion/` KV prefix unless the pattern names
  them, so a tenant admin with `namespaces = ["*"]` cannot edit, exec into,
  or borrow the identity of an app's job.
- **Grant changes are visible.** `legion acl grants list --job
  legion-system/<app>` shows every version's grant and its granter, and the
  app events name who installed, upgraded, changed, enabled, or disabled an
  app.

## ACLs

`app` is a cluster-scoped [ACL resource](auth-and-acls.md#resources-and-actions)
with a `names` selector, matched against app names:

```nickel
{ resources = ["app"], actions = ["read"] },                                   # see every app
{ resources = ["app"], actions = ["read", "write"], names = ["autoscaler"] },  # manage the autoscaler
```

| RPC | Needs |
| --- | --- |
| `ListApps` | `app:read` (the list is filtered by `names`) |
| `GetApp` | `app:read` on the app |
| `InstallApp` | `app:write` on the app, and every permission the manifest asks for |
| `UpgradeApp` | `app:write`, and every permission the new manifest asks for |
| `UninstallApp`, `SetAppEnabled`, `UpdateAppSettings` | `app:write` |
| `SetAppConfig` | `app:write`, and the app's whole grant when a code field changes |

Every mutating RPC takes `idempotency_key`, and a repeat returns the first
reply with `deduplicated` set (`--idempotency-key` on the CLI).

## Inside an app

The app's tasks get, in their environment:

| Variable | Value |
| --- | --- |
| `LEGION_APP_NAME` | the app's name |
| `LEGION_APP_CONFIG` | the path of the app's config file (JSON), in the task's own view (`exec` and `container` tasks) |
| `LEGION_APP_BUILD` | the build marker the job was rendered for, `<buildVersion>+<buildId>` (bundled apps) |
| `LEGION_ADDR`, `LEGION_SECRETS_DIR`, `LEGION_IDENTITY_TOKEN_FILE` | the allocation's API socket and token, authenticated as the app's `legion` identity, when workload identity is on ([The API socket](workload-identity.md#control-plane-identity)) |
| `LEGION_ADDR_API`, `LEGION_PORT_API` | the app's `api` port, when its task declares one |

So `legion kv get legion/apps/gitops/state` inside the task just works,
with the app's grant and no token handling.

**`legion app run <name>`** is the entrypoint bundled apps run. It reads
`$LEGION_APP_CONFIG` (validated against the app's schema), serves the app's
HTTP port (`$LEGION_PORT_API` on every interface; `--listen` overrides) with
`/healthz`, which
answers `200` or `503` with the health the app reports, starts the app, and
stops it on SIGTERM or SIGINT, giving it 10 seconds. The bundled autoscaler
declares an `api` port and a health check on `/healthz`.

`exec` tasks may run the node's own binary with `command =
["legion:self", ...]`: the exec runtime resolves `legion:self` to the
worker's executable.

## Events and metrics

| Event | When |
| --- | --- |
| `app.installed` | an app was installed, or a bundled app seeded (`by legion:bootstrap`) |
| `app.upgraded` | a manifest was replaced: by `legion app upgrade`, or a bundled app's new build (permissions added and removed) |
| `app.uninstalled` | an app was uninstalled (grants revoked) |
| `app.enabled`, `app.disabled` | an app was enabled or disabled |
| `app.config_changed` | an app's config was replaced (the code fields it touched) |
| `app.reconciled` | the reconciler changed an app's job: `created the job`, `config changed`, `settings changed (…)`, `reverted a direct edit by <who>`, `started the job, which was stopped`, `stopped (app disabled)` |
| `app.reconcile_failed` | a reconcile failed (`refused` when it waits for a change) |

App events are scoped to the app's job, so `legion events --job
legion-system/<app>` shows them, and they reach whoever may read that job.
`legion_apps{status}` on `/metrics` counts apps by status, from the leading
manager ([Metrics](metrics.md)).

## API and CLI reference

| CLI | RPC |
| --- | --- |
| `legion app list` | `ListApps` |
| `legion app status <app>` | `GetApp` |
| `legion app logs <app> [-f] [--stream S] [--tail]` | `legion job logs legion-system/<app>` |
| `legion app config get <app>`, `config schema <app>` | `GetApp` |
| `legion app config set <app> -f <file>` | `SetAppConfig` |
| `legion app set <app> [--replicas N] [--placement P] [--pool P] [--cpu MHZ] [--memory MIB]` | `UpdateAppSettings` |
| `legion app enable <app>`, `disable <app>` | `SetAppEnabled` |
| `legion app install <file|url> [--config F] [--yes] [--dry-run]` | `InstallApp` |
| `legion app upgrade <app> <file|url> [--config F] [--yes] [--dry-run]` | `UpgradeApp` |
| `legion app uninstall <app>` | `UninstallApp` |
| `legion app run <app> [--listen ADDR] [--config F]` | (the in-task entrypoint) |

Every command but `run` takes `--json`. The MCP server exposes
`legion_app_list` and `legion_app_status` ([Agent Readiness](agent-readiness.md)).
`proto/legion.proto` documents every RPC and field.

## Limitations

- **UI registrations and jobspec extensions are stored, not used.** The
  manifest's `ui` and `extensions` are validated for shape and kept on the
  record; `legion app list` shows the UI slots. Rendering panels and pages,
  and resolving jobspec keys, come in later releases.
- **Without workload identity, an app cannot reach Legion.** Workload
  identity is off by default (`workloadIdentity.enabled = false`). An app
  then runs, but its `legion` identity gets no token and no API socket, so
  it holds no credentials; `legion app status` notes it.
- **The autoscaler does not scale yet.** The bundled `autoscaler` runs and
  reports healthy, and logs that its engine is not wired in this build.
- **The config file reaches `exec` and `container` tasks only.** A sandbox
  task gets `LEGION_APP_NAME` but no config file.
- **No registry.** `install` takes a file or a URL.
- **Apps live in `legion-system`.** Apps owned by a tenant namespace are not
  supported (`namespace` in a manifest is reserved).
- **A manifest's job reads KV only within the app's grant.** The
  reconciler's submit is checked against the recorded grant, so a manifest
  whose job names a KV key (a template, a reference) needs `kv:read` on it in
  its own `permissions`; otherwise the submit is refused and the app shows
  `reconcile refused`.
- **Managers should agree on `systemWorker.appUser`.** The leading manager's
  value is rendered into exec tasks, so a manager with another value renders
  a different job, and resubmits the app when it takes over.
- **Backoff is the leader's memory.** A new leader retries a failing or
  refused app once, then backs off from scratch.
- **`config get` prints JSON**, not Nickel. JSON is valid input for `config
  set`.
- **Uninstall leaves the stopped job.** `legion job destroy
  legion-system/<app>` removes it.
- **A worker hashes its executable at startup** for its build id: under a
  second for a release binary, longer for a debug build.
