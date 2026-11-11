# Plan: GitOps — a Built-In App That Deploys Jobs From Git

> **Judgement calls:** [`calls/gitops-plan_calls.md`](../calls/gitops-plan_calls.md)

**Status: SHIPPED** (planned and built 2026-11-07, Phases G1-G3 in one
change, on branch `claude/dreamy-thompson-fuodyz`). Public page:
[`docs/gitops.md`](../../docs/gitops.md). Building it changed three things
this plan says, each a judgement call: a rollout in progress reports
`Ready=unknown` (the object reads `Pending`), not `Ready=false` (calls entry
7); gitops checks what no Repo authority covers (host-level tasks, bands)
before its `PlanJob` preflight, which cannot see them (entry 2); and a
`file://` remote reads a global git config for `safe.directory` (entry 10).
It also fixed a core bug, a band on recorded reads (entry 3).
Deliberately deferred and named: SSH URLs and deploy keys, push webhooks,
image automation (writing a new image tag back to git), more than one
replica of the app, Repos that deploy at a priority band other than
`normal`, jobs that ask for workload-identity `permissions`, `vars`
injected into the rendered jobspecs, a `legion gitops diff`, and
notifications (§4, §11).

---

## 1. Problem statement

Legion has every primitive a deploy flow needs: jobspecs as files, rollouts,
`legion job run --wait`, and an apps substrate built so that "GitOps" could
be an add-on ([legion-apps-plan.md](legion-apps-plan.md) §1;
[app-object-kinds-plan.md](app-object-kinds-plan.md) §1 names `Repo` as its
first motivating kind). What it does not have is the loop that makes git
the source of truth. Today a team that wants "merge to main and it
deploys" has to:

1. write a CI job that checks out the repository, installs `legion`, holds a
   management-grade token as a CI secret, and runs `legion job run` on
   every file, in order, with retries;
2. notice drift itself: a hand-run `legion job run` or `job destroy` stays
   until the next pipeline run happens to touch that job;
3. remove jobs by hand when their file is deleted, because nothing knows
   which jobs a repository produced;
4. explain to the security team why CI holds a token that can deploy
   anything, anywhere.

The `configs/apps/example-app.ncl` manifest and the object-kind docs
describe a `gitops` app and its `gitops/Repo` kind in detail, but there is
no such app: `internal_docs/TODO.md` lists "GitOps helper — deploy to git,
job updates automatically" as open.

## 2. Prior art

- **Flux** (`GitRepository` + `Kustomization`). A source controller polls a
  repository (`interval`, default chosen per object; `ref.branch`,
  `ref.tag`, `ref.commit`), a kustomize controller applies the path with
  server-side apply, keeps an **inventory** of what it applied in
  `status.inventory`, prunes (`prune: true`) what left git, and reports
  `Ready` with `lastAppliedRevision`. `flux reconcile kustomization X`
  sets the `reconcile.fluxcd.io/requestedAt` annotation and waits for
  `status.lastHandledReconcileAt` to echo it. Multi-tenancy needed
  `serviceAccountName` impersonation and a lockdown flag because the
  controller otherwise applies with its own cluster-admin rights.
  *Take:* polling with an interval, one `ref` (branch, tag, or commit),
  the inventory in status, prune by inventory, the requestedAt/handled
  echo for "sync now", `Ready` as the one thing to wait on. *Reject:* two
  objects (source + apply) for the common case; kustomize overlays.
- **Argo CD** (`Application`). One object names `repoURL`,
  `targetRevision`, `path`, `destination`; a UI shows sync and health;
  auto-sync, self-heal, and prune are opt-in flags; health is computed per
  resource. The controller deploys with its own rights; AppProjects bound
  them after the fact. *Take:* one object per repository and path, health
  that follows the rollouts, a UI panel with "Sync". *Reject:* sync waves,
  hooks, the separate repo server, and the default of auto-sync off (a
  GitOps tool that does not deploy on push surprises people).
- **Nomad.** No built-in GitOps; teams use Nomad Pack plus CI, or
  Terraform's `nomad_job`. Waypoint had a git poller and was retired.
  *Take:* nothing; this is the gap.
- **Kubernetes escalation through GitOps objects.** An Argo `Application`
  or a Flux `Kustomization` deploys whatever its author names with the
  controller's rights (the reason app objects have `requires`; see
  [app-object-kinds-plan.md](app-object-kinds-plan.md) §1). *Take:* the
  Repo's writer is checked for every job the Repo can deploy, at write,
  and the app acts for the Repo with exactly that authority.
- **Legion itself.** The object system already gives a kind, a schema,
  CEL rules, `requires`, recorded authority, `legion-for-object`, a
  reconcile loop with `requeue_after`, status with conditions, cleanup
  windows, and `legion object apply|get|list|delete --wait`. The app
  framework gives views and actions. The artifact `git` fetcher shows how
  Legion shells out to `git` with env-injected credentials
  (`crates/worker/src/artifacts/fetchers/git.rs`). This plan composes them.

## 3. Jobs to be done

**App developer / job author**
- When I merge a change to my service's jobspec, I want it deployed without
  running anything myself, so the main branch is what runs.
- When I start, I want to point Legion at a repository with one command and
  see my jobs come up, so I don't read a manual first.
- When a deploy goes wrong, I want to see which commit is running, which job
  is unhealthy, and why, in one place.
- When I delete a job's file, I want the job to go away, so the repository
  stays the whole truth.

**Platform engineer running CI**
- When CI pushes a deploy commit, I want to trigger the deploy now and have
  the pipeline wait for it and fail if the rollout fails, so the pipeline
  reports the truth instead of "pushed".
- When I set this up, I want CI to hold a token that can only nudge
  deploys, so a leaked CI secret is not a cluster-admin token.

**Security / platform admin**
- When a team points Legion at a repository, I want what it can deploy to
  be bounded by what that person may deploy, so the GitOps tool is never a
  way around ACLs.
- When the repository is private, I want its token kept in KV, readable by
  whoever the ACLs say, not pasted into a manifest.

**On-call responder**
- When I need to stop the bleeding, I want to pause syncing or stop a job
  without the GitOps tool undoing it a minute later, so my fix holds until
  someone reverts in git.
- When I arrive, I want the job page to tell me it is deployed from git,
  from which commit, so I know a hand edit will be written back.

**Autonomous agent**
- When I am asked to deploy from a repository, I want to create the Repo,
  trigger a sync, and wait on a structured result, so I can tell whether
  it worked without screen-scraping.

*Not in scope:* the cluster operator as a separate persona beyond enabling
or disabling the app: GitOps is a bundled app and needs no setup.

### User scenarios

1. **First deploy.** On a fresh dev cluster, Ana runs `legion gitops add web
   --url https://github.com/ana/deploy --path jobs --wait`. The Repo is
   created in `default`; gitops resolves `main`, fetches it, renders every
   `.ncl` and `.json` file in `jobs/`, deploys each job, and reports
   `Ready (Synced: at 3f9a1c2: 2 jobs deployed)`. `--wait` returns when the
   rollouts finish and prints the jobs.
2. **CI deploy.** The pipeline for `acme/storefront` builds an image, commits
   the new tag to `deploy/storefront.ncl`, pushes, and runs `legion gitops
   sync storefront -n team-a --wait`. gitops picks up the commit at once,
   deploys the one job whose file changed, and the command exits 0 when its
   rollout completes, or non-zero naming the failed rollout.
3. **Drift and incident.** Someone runs `legion job run` over `team-a/web`
   by hand. Within one interval gitops writes the git version back, and
   the job page's GitOps panel says so. During an incident the responder
   runs `legion job stop team-a/web`; gitops leaves the stopped job alone
   (the Repo reads `Ready`, with `1 stopped by hand`) until its file
   changes.

## 4. Goals and non-goals

**Goals**

- A **bundled** app, `gitops`, seeded and enabled on every cluster, with
  one kind, `gitops/Repo`: a URL, a ref, a path, an interval.
- **Bounded by the Repo's writer.** The app's own credential can do
  nothing; every call it makes for a Repo acts for that Repo
  (`legion-for-object`), holding only what the Repo's writer was checked
  for: `job:read`/`job:write` in the Repo's namespace (on the jobs it lists,
  or namespace-wide), host-level writes only when the Repo says so, and the
  exact KV keys it names.
- **Never half-deploy a bad commit.** Render every file, then preflight
  every changed job with `PlanJob`; if anything fails, deploy nothing and
  keep the previous commit running.
- **Inventory, prune, drift.** Status carries the jobs the Repo deployed and
  a digest of each; a job whose file left git is stopped; a job someone
  replaced or destroyed is written back.
- **Ready means healthy.** `Ready` follows each job's rollout, so `--wait`
  and CI mean "deployed and healthy".
- **One-command start and CI**: `legion gitops add`, `legion gitops sync
  --wait`, `legion gitops status`; MCP tools for agents.
- **A UI** from app views: a GitOps page, a panel on deployed jobs, and a
  Sync now button.

**Non-goals**

- SSH URLs and deploy keys (HTTPS with a token covers GitHub, GitLab, and
  Bitbucket; deferred).
- Push webhooks: the app's port is not reachable from the internet; CI calls
  `legion gitops sync` instead, and the interval covers the rest.
- Image automation (watching a registry and committing new tags).
- Templating beyond Nickel itself: no Helm, no kustomize, no `vars`.
- Deploying into a namespace other than the Repo's: write a Repo in each
  namespace.
- Sync waves, hooks, and ordering between jobs.
- Running more than one replica of the app.

## 5. Background — what exists today

- **Bundled apps** are one entry in `BUNDLED` (`src/apps/mod.rs:73`): a
  Nickel manifest under `crates/common/src/bundled_apps/`, a body `fn(AppContext) ->
  AppFuture`, and an optional manifest filler. The first Control leader
  seeds a record for each (`crates/manager/src/manager/apps.rs:434`), with its permissions
  granted by `legion:bootstrap`, and the job runs `legion:self app run
  <name>`. Installing an app whose name is bundled is refused
  (`crates/manager/src/manager/server/app_rpcs.rs:801`).
- **Object kinds** (`docs/app-objects.md`): schema, CEL `rules`,
  `requires` resolved from spec pointers (`resolve_requirements`,
  `crates/common/src/structs/app_object.rs:1632`: one permission per value of a list
  pointer, at most 64 per object, a single `band`), recorded as authority;
  `authority_rule` (`crates/manager/src/manager/delegation.rs:196`) turns each into an
  exact rule (no globs in a name or KV path), intersected with the app's
  grant including `via = "objects"` rules.
- **The reconcile loop** (`src/apps/objects.rs:853`): level-triggered,
  serialized per object, `requeue_after`, status written only when it
  differs, `on_cleanup` for deleting objects, a client that acts for the
  object (`ObjectRequest::client`). An object's upsert is any change to
  `spec_version`, `generation`, `state`, or `deleting`
  (`object_moved`, `src/apps/objects.rs:112`), so a label write wakes it.
  Nothing outside the loop can wake one object.
- **RunJob** (`crates/manager/src/manager/server.rs:2770`): an identical spec is
  `Unchanged` and adds no version, but restarts a job that is stopped,
  failed, or idle. So resubmitting every interval would rerun finished
  batch jobs and undo a responder's `job stop`: the app must submit only
  when something changed.
- **Stamps**: a job submitted while acting for an object carries
  `GetJobInfoReply.managed_by.object = <app>/<Kind>/<ns>/<name>#<uid>`;
  a person's submit replaces it.
- **Health**: `ListRollouts(job_id)` newest first, with `status` and
  `to_job_config_id`; `Job.status` (`running`, `complete`, `failed`,
  `stopped`, `pending`, `blocked`, ...).
- **Host-level jobs** need `job:write` with `host = true`; every `exec` task
  is host-level (`docs/auth-and-acls.md`, "Host-level jobs").
- **Nickel**: `config_lang::eval_file_tracked` resolves imports relative to
  the file with no confinement; `eval_sandboxed` refuses imports outright.
  The lexer (`nickel_lang_parser::lexer`) yields `import` followed by a
  static string (or a package identifier), so imports can be checked before
  evaluation.
- **git**: `crates/worker/src/artifacts/fetchers/git.rs` runs the `git` binary with
  `GIT_ALLOW_PROTOCOL` and credentials in `GIT_CONFIG_*` env, never argv.

## 6. Proposed design

### 6.1 The Repo

```nickel
# repo.ncl — `legion object apply -f repo.ncl`
{
  kind = "gitops/Repo",
  name = "storefront",
  namespace = "team-a",                   # the namespace its jobs deploy into
  spec = {
    url = "https://github.com/acme/deploy",
    ref = "main",                         # a branch, a tag, or a full commit SHA (default "main")
    path = "storefront",                  # the directory of jobspecs (default ".", the root)
    interval = "1m",                      # how often to look for a new commit (default "1m", at least "10s")
    jobs = ["storefront", "storefront-worker"],   # optional: only these jobs; writing needs only them
    prune = true,                         # stop jobs whose file left git, and on delete (default true)
    suspend = false,                      # stop syncing; jobs keep running (default false)
    hostLevel = false,                    # its jobs include host-level tasks, such as exec (default false)
    credentials = { kv = "secret/gitops/github-token", username = "x-access-token" },
    kvKeys = ["secret/storefront/db-password"],   # KV keys its jobs read
  },
}
```

Only `url` is required. **What a Repo reads.** Every `.ncl` and `.json`
file directly in `path`, in name order; a file whose name starts with `.`
or `_` is skipped, and subdirectories are not read, so shared Nickel lives
in one and is imported (`import "lib/common.ncl"`). A file evaluates to one
jobspec or a list of them. Imports may reach any file in the checkout and
nothing outside it.

**Where jobs go.** Into the Repo's namespace. A jobspec that names another
namespace (`namespace = "prod"`, or `name = "prod/web"`) fails the render.

### 6.2 What writing a Repo requires

```nickel
requires = [
  { resource = "job", actions = ["read", "write"], name = "/spec/jobs" },
  { resource = "job", actions = ["write"], name = "/spec/jobs", host = true, when = "/spec/hostLevel" },
  { resource = "kv",  actions = ["read"], path = "/spec/credentials/kv" },
  { resource = "kv",  actions = ["read"], path = "/spec/kvKeys" },
]
```

Without `jobs`, the Repo needs `job:read` and `job:write` in its namespace,
namespace-wide. The app's grant is only `via = "objects"`:

```nickel
permissions = [
  { resources = ["job"], actions = ["read", "write"], namespaces = ["*"], via = "objects" },
  { resources = ["job"], actions = ["write"], namespaces = ["*"], host = true, via = "objects" },
  { resources = ["kv"], actions = ["read"], paths = ["*"], via = "objects" },
]
```

so gitops's own token can do nothing at all, and acting for a Repo it
holds what that Repo's writer was checked for. `*` reaches neither
`legion-system` nor the reserved `legion/` KV prefix.

### 6.3 One sync

On every reconcile (start, any change to the Repo, `requeue_after`
= the interval, a Sync request):

1. **Resolve** `ref` with `git ls-remote` (a full SHA skips it). A failure
   is `Ready=false FetchFailed`; jobs keep running.
2. **Fetch and render**, only when the commit differs from the last one
   rendered (kept in memory): a shallow fetch of that commit into a
   per-Repo directory, then each file is rendered **in a child process**
   (`legion:self app gitops-render`) with a 10-second limit, its imports
   confined to the checkout. Every job gets a digest of its rendered spec.
   Any failure, a duplicate job name, a job outside `spec.jobs`, a
   foreign namespace, a missing `path`, or zero jobs where the Repo had
   deployed some, is `Ready=false RenderFailed`, and nothing is deployed.
3. **Decide** per job, with `GetJobInfo` as the Repo: submit when the job
   does not exist, its digest differs from the inventory's, or its latest
   version is not stamped with this Repo (someone replaced it). A job
   stamped with **another** Repo is a conflict. A job that exists unstamped
   and is not in the inventory is adopted. A stopped job whose digest is
   unchanged is left stopped.
4. **Preflight** every job to submit with `PlanJob`; any refusal is
   `Ready=false ApplyFailed` naming the job, and nothing is deployed.
   (Built: before it, each job is also checked for what no Repo authority
   covers, a host-level task without `hostLevel` or a band other than
   `normal`, as `RenderFailed`: `PlanJob` is a read and does not see them.)
5. **Apply** with `RunJob` in file order, with the source files as
   `submissions`. A refusal midway is `ApplyFailed`; the inventory records
   what did apply.
6. **Prune** (when `prune`): each job in the inventory and not in the
   render, still stamped with this Repo and not already stopped, is
   stopped (`StopJob`, never destroyed).
7. **Health**: for each job, its latest rollout when that rollout is for
   the version gitops submitted, else its status. `Ready=true Synced` when
   every job is deployed (stopped-by-hand jobs are listed, not counted
   against it); `Ready=unknown Progressing` while any rolls out (the object
   reads `Pending`, which `--wait` waits through; requeue in 5 seconds);
   `Ready=false RolloutFailed` naming the jobs.

**Status** carries the inventory:

```json
{
  "commit": "3f9a1c2…", "latest": "3f9a1c2…", "subject": "Bump web to 1.4.2",
  "syncedAt": "2026-11-07T10:02:11Z", "syncRequest": "1762509731000",
  "jobs": [ { "name": "web", "file": "storefront/web.ncl", "digest": "9c1e…", "version": "a1b2…", "state": "deployed" } ]
}
```

### 6.4 Deleting a Repo

The kind has `cleanup = "2m"`. With `prune = true`, the cleanup stops
every job in the inventory still stamped with the Repo, acting for it, and
releases it; with `prune = false` it releases at once and the jobs keep
running. `legion object delete --now` skips cleanup: the jobs keep running.

### 6.5 The CLI

```console
$ legion gitops add web --url https://github.com/ana/deploy --path jobs --wait
gitops/Repo default/web: created
web: Ready (Synced: at 3f9a1c2: 2 jobs deployed)
JOB     FILE           STATE     DETAIL
api     jobs/api.ncl   deployed
worker  jobs/worker.ncl deployed

$ legion gitops status -A
NAMESPACE  NAME        STATUS                       REF   COMMIT   JOBS  SYNCED
default    web         Ready                        main  3f9a1c2  2     4m ago
team-a     storefront  NotReady: RolloutFailed      main  8d0e44b  3     1m ago

$ legion gitops sync storefront -n team-a --wait --timeout 300
storefront: sync requested
storefront: at 8d0e44b (Fix health check); 1 job updated
storefront: Ready (Synced: at 8d0e44b: 3 jobs deployed)
```

`sync` sets the label `gitops/sync=<token>` on the Repo (an apply, so the
caller needs what writing the Repo needs), waits for the app to echo the
token in `status.fields.syncRequest`, and with `--wait` waits for `Ready`
or a failure. Every command takes `--json`.

### 6.6 The UI

App views, no SPA code: a **GitOps** page in the sidebar (Repos in the
namespaces where the viewer has `job:read`), a **GitOps** panel on every
job a Repo deployed (repository, ref, commit and subject, file, state,
last sync, errors), and a **Sync now** action on that panel (`job:write`
on the job), which wakes the Repo's reconcile at once.

### 6.7 Config

```nickel
# legion app config set gitops -f gitops.ncl
{ allowFileUrls = false }   # file:// repositories on the managers' disk: for dev clusters and tests
```

## 7. Proto, struct, config, and persistence changes

- **No proto changes, no new RPCs.** Everything rides `ApplyObject`,
  `GetObject`, `ListObjects`, `WatchObjects`, `UpdateObjectStatus`,
  `ReleaseObject`, `GetJobInfo`, `PlanJob`, `RunJob`, `StopJob`,
  `ListRollouts`, and `GetKv`.
- **No new job-config fields**, so `configs/jobs/maximal.ncl` is
  unchanged.
- New: `crates/common/src/bundled_apps/gitops.ncl`, `src/apps/gitops/` (engine, git,
  render, UI), `src/cmd/gitops.rs`; `config_lang::eval_file_confined`;
  `Reconciler::wake` in the object framework.
- **Persistence**: none new. The inventory lives in the Repo's status
  fields; the checkout is a cache in the task directory.
- **Breaking**: the name `gitops` is now bundled, so `legion app install`
  of an app named `gitops` is refused; the example third-party manifest
  (`configs/apps/example-app.ncl`) and the test fixtures that used the name
  move to `deployer`.

## 8. Implementation phases

- **G1: the app.** Manifest and kind, git, confined rendering, the sync
  engine, inventory, prune, health, cleanup. Independently valuable:
  `legion object apply -f repo.ncl --wait` deploys from git.
- **G2: the CLI and MCP.** `legion gitops add|status|sync`, the
  capabilities catalog, `legion_gitops_status` and `legion_gitops_sync`.
- **G3: the UI.** The page, the job panel, Sync now, and the framework's
  `Reconciler::wake`.

## 9. Testing plan

- **Unit**: Repo spec parsing and defaults; the manifest's examples parse
  into the spec type; import confinement (relative, `..` escapes, absolute,
  symlinks, package imports); file discovery; render errors (duplicate
  names, foreign namespace, list files, `jobs` allow-list); per-job
  decisions (missing, digest change, foreign stamp, stopped); health from
  rollouts; ls-remote output parsing; the label token; the wake hook.
- **Integration** (`tests/integration/gitops.rs`, local `file://`
  repositories made with the `git` binary):
  - the reconcile loop over gRPC deploys a Repo's jobs and reports `Ready`
    with the inventory; a commit that changes one file resubmits one job;
  - drift: a job resubmitted by hand is written back; a destroyed job is
    recreated; a stopped job is left;
  - prune on a deleted file; cleanup on a deleted Repo; `prune = false`;
  - a bad commit (render error, then a PlanJob refusal) deploys nothing;
  - the sync label is echoed;
  - with ACLs on, in process: a Repo's authority deploys its jobs and is
    refused a job outside `spec.jobs`, a host-level job without
    `hostLevel`, and KV it does not name.
- **Regression**: the bundled-manifest validity test covers the new
  manifest; the apps and object tests keep passing with the `deployer`
  fixture.

## 10. Rollout and backward compatibility

The app is seeded enabled on new clusters and on existing ones at their
next Control leader pass (bundled apps are seeded when missing). With no
Repos it watches and does nothing. Operators who do not want it run
`legion app disable gitops`, or seed it off with `apps = { gitops = {
enabled = false } }`. `git` must be on the `PATH` of the nodes that run it
(the managers, by default). An installed app named `gitops` on an existing
cluster keeps its record; the bundled one is not seeded over it. No
migration.

## 11. Open questions

None open; each was resolved as a judgement call. Deferred, with leans:
SSH (lean: `credentials = { kv, ssh = true }` with a known-hosts field),
webhooks (lean: through the gateway once it routes to app ports), HA
(lean: the autoscaler's lease-based shards), bands (lean: a `band` field,
one per Repo, as `requires` resolves one band).

## 12. Acceptance criteria

- **G1**: `legion object apply -f repo.ncl --wait` on a fresh cluster
  deploys the repository's jobs and returns `Ready`; `legion object get
  repo X` shows the commit and inventory; a bad commit leaves the old one
  running with `RenderFailed` or `ApplyFailed`; a job outside the Repo's
  authority is refused by the manager.
- **G2**: `legion gitops add|status|sync` work with and without `--json`;
  `sync --wait` exits non-zero on a failed rollout; the MCP tools are
  listed (sync only with `--allow-write`).
- **G3**: `legion app view gitops nav` lists Repos; `legion app view gitops
  job <ns>/<job>` shows the panel for a deployed job and hides for others;
  Sync now triggers a sync.
