# GitOps

Legion deploys jobs from git. Point it at a repository, and every jobspec in
the directory you name is deployed into a namespace and kept deployed:
merge to the branch and the change rolls out; delete a file and its job
stops; edit a job by hand and the repository's version is written back.

It is built in. **gitops** is an [app](apps.md) bundled with Legion and
running on every cluster, and a repository is a **Repo**, an
[object](app-objects.md) of its kind `gitops/Repo`. There is nothing to
install.

```console
$ legion gitops add web --url https://github.com/acme/deploy --path jobs --wait
gitops/Repo default/web: created (generation 1, spec version 41)
web: Pending (Progressing: at 3f9a1c2: rolling out api, worker (0 of 2 jobs deployed))
web: Ready (Synced: at 3f9a1c2: 2 jobs deployed)
 JOB      FILE              STATE      DETAIL
 api      jobs/api.ncl      deployed
 worker   jobs/worker.ncl   deployed
```

## Quick start

1. **Put your jobspecs in a repository**, one file per job, the same `.ncl`
   or `.json` files `legion job run` takes
   ([`configs/gitops/deploy`](../configs/gitops/deploy) is one to copy):

   ```text
   deploy/
     jobs/
       api.ncl
       worker.ncl
     lib/
       common.ncl       # shared Nickel, imported by the jobs
   ```

2. **Add a Repo.** It deploys into the namespace you add it in (`-n`, else
   `$LEGION_NAMESPACE`, else `default`):

   ```console
   $ legion gitops add web --url https://github.com/acme/deploy --path jobs --wait
   ```

3. **Push.** gitops looks for a new commit every minute (`--interval`), and
   at once when asked:

   ```console
   $ git commit -am "Ship api 1.4.2" && git push
   $ legion gitops sync web --wait      # optional: deploy now, wait for the rollout
   ```

`legion gitops status` shows every Repo; `legion gitops status web` shows one,
with the deployed commit and every job's state. Jobs gitops deployed are
ordinary jobs: `legion job status`, `logs`, `exec`, and the UI work as
always, and `legion job info api` says `managed by gitops for Repo
default/web`.

A Repo is an object, so it can live in git too: write it as a file and
`legion object apply -f repo.ncl --wait`
([`configs/gitops/repo.ncl`](../configs/gitops/repo.ncl) has every field,
with comments).

```nickel
# repo.ncl
{
  kind = "gitops/Repo",
  name = "web",
  namespace = "team-a",
  spec = {
    url = "https://github.com/acme/deploy",
    path = "jobs",
  },
}
```

## The Repo

```nickel
spec = {
  url = "https://github.com/acme/deploy",       # required
  ref = "main",                                 # a branch, a tag, or a full commit SHA
  path = "storefront",                          # the directory of jobspecs
  interval = "1m",                              # how often to look for a new commit
  jobs = ["storefront", "storefront-worker"],   # only these jobs (and writing needs only them)
  prune = true,                                 # stop jobs that leave git, and on delete
  suspend = false,                              # stop syncing; the jobs keep running
  hostLevel = false,                            # its jobs have host-level tasks (exec, ...)
  credentials = { kv = "secret/gitops/github-token", username = "x-access-token" },
  kvKeys = ["secret/storefront/db-password"],   # KV keys its jobs read
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `url` | required | The repository: `https://` (or `http://` without credentials). `file://` only where the operator allows it ([Configuration](#configuration)). SSH URLs are not supported. A URL with credentials in it is refused: use `credentials`. |
| `ref` | `main` | A branch, a tag, or a full 40-character commit SHA. A branch or tag is followed; a SHA pins the Repo. When a branch and a tag share a name, the branch wins. |
| `path` | `.` | The directory, relative to the repository root, whose files are the jobspecs ([What a Repo reads](#what-a-repo-reads)). |
| `interval` | `1m` | How often gitops checks `ref` for a new commit, as a duration. At least `10s`. A check is one `git ls-remote`; the repository is fetched only when the commit changes. |
| `jobs` | every job | The jobs the Repo may deploy, 1 to 20 names. A file that defines another job fails the sync. Writing the Repo then needs `job:read` and `job:write` on these jobs alone, instead of namespace-wide. |
| `prune` | `true` | Stop a job whose file leaves the repository, and stop every job the Repo deployed when the Repo is deleted. Jobs are stopped, never destroyed. |
| `suspend` | `false` | Stop syncing: nothing is deployed, written back, or stopped, and the Repo reads `NotReady: Suspended`. |
| `hostLevel` | `false` | The repository's jobs include [host-level](auth-and-acls.md#host-level-jobs) tasks: an `exec` task, a container on the host network, a `dev` sandbox. Writing the Repo then also needs `job:write` with `host = true`. |
| `credentials` | none | For a private repository: `kv`, the KV key holding an HTTPS token, and `username` (default `x-access-token`), sent with it as basic auth. Only to an `https://` URL. |
| `kvKeys` | none | The exact KV keys the repository's jobs read (`$path/key` values, `kvInjections`, ...): a submit needs `kv:read` on each ([Who can do what](#who-can-do-what)). |

`legion object explain gitops/Repo` prints every field, and the rules the
manager checks on every apply (`url` is `https://`, `http://`, or `file://`;
credentials only over `https://`; `path` stays inside the repository; `jobs`
and `kvKeys` hold exact, distinct names).

## What a Repo reads

- **Every `.ncl` and `.json` file directly in `path`**, in name order, is a
  jobspec. A file may also hold a list of jobspecs.
- **Subdirectories are not read**, and files whose name starts with `.` or
  `_` are skipped. Put shared Nickel in a subdirectory and import it:
  `let common = import "../lib/common.ncl" in { ... }`.
- **Imports stay inside the repository.** An import must be a plain relative
  path (`import "lib/web.ncl"`, `import "data.json"`, `import "motd.txt" as
  'Text`). An absolute path, a path or symlink that leads out of the
  checkout, and a package import are refused before anything is evaluated.
- **Jobs deploy into the Repo's namespace.** A jobspec may leave `namespace`
  out or name the Repo's; one that names another namespace fails the sync
  (write a Repo in that namespace). Jobspecs get no `vars`.
- **Each file has 10 seconds to evaluate**, in a process of its own, and a
  Repo deploys at most 100 jobs from at most 200 files.
- The files are submitted with each job as its source, so `legion job spec
  api` shows the Nickel that rendered it.

## How a sync works

A sync runs every `interval`, whenever the Repo changes, when someone asks
(`legion gitops sync`, the UI's Sync now), and every 5 seconds while a
rollout it started is under way:

1. **Resolve** `ref` to a commit (`git ls-remote`).
2. **Fetch and render** that commit, once per commit: a shallow fetch, then
   every file. If any file fails to render, names another namespace, defines
   a job `jobs` does not list or a job twice, or defines a job the Repo's
   authority cannot cover (a host-level task without `hostLevel`, a priority
   band other than `normal`), **nothing is deployed** and the Repo reads
   `NotReady: RenderFailed` with every problem listed.
3. **Decide**, job by job, from the Repo's inventory (the jobs it deployed,
   in its status) and the job as it is:

   | The job | gitops |
   | --- | --- |
   | does not exist (new, or destroyed by someone) | submits it |
   | its file changed | submits it |
   | someone submitted another version over it | submits the repository's version back |
   | someone stopped it (`legion job stop`) | leaves it stopped, until its file changes |
   | exists, was not deployed by any Repo | adopts it: submits the repository's version |
   | was deployed by another Repo | refuses: a job belongs to one Repo (`ApplyFailed`) |
   | is as the inventory says | nothing |

4. **Preflight** every submit with `PlanJob`. If any is refused (an invalid
   spec, a KV key the Repo does not name), nothing is deployed: `NotReady:
   ApplyFailed`. A refusal only the submit itself can make still stops the
   sync there, and the Repo names the jobs that were deployed before it.
5. **Apply**: `RunJob` for each, in file order.
6. **Prune**: with `prune`, every job in the inventory that is no longer in
   the repository, and is still the Repo's, is stopped.
7. **Health**: each job's latest rollout (for the version gitops
   submitted), else its status.

A commit that fails steps 2 or 4 leaves the last good commit running: the
Repo's status shows both (`commit` deployed, `latest` seen). A commit that
removes every job, where the Repo had deployed some, is taken for a
mistake (a moved directory, a wrong `path`) and not applied: delete the
Repo to stop its jobs.

## Status

```console
$ legion gitops status storefront -n team-a
gitops/Repo team-a/storefront: Ready (Synced: at 3f9a1c2: 2 jobs deployed)
  repository  https://github.com/acme/deploy (ref main, path storefront, every 1m)
  commit      3f9a1c2 Bump web to 1.4.2, synced 4m ago
 JOB                 FILE                        STATE      DETAIL
 storefront          storefront/web.ncl          deployed
 storefront-worker   storefront/worker.ncl       deployed
```

| Summary | Means |
| --- | --- |
| `Ready` (`Synced`) | Every job deployed: its rollout completed, or it runs (a batch job ran to completion). Jobs stopped by hand are listed, not counted against it. |
| `Pending` (`Progressing`) | Applied, and a rollout is under way (or a job waits to be placed); `--wait` waits through it. Also before the first sync. |
| `NotReady: FetchFailed` | `git ls-remote` or the fetch failed (the URL, the token, the network). Jobs stay as they are. |
| `NotReady: RenderFailed` | A file did not render, or the commit asks for what the Repo cannot deploy ([How a sync works](#how-a-sync-works)). Nothing deployed. |
| `NotReady: ApplyFailed` | The manager refused a job (the preflight, or a submit), or another Repo owns one. |
| `NotReady: RolloutFailed` | A job's rollout failed, or the job did. |
| `NotReady: PruneFailed` | A job that left the repository could not be stopped. |
| `NotReady: CredentialsMissing`, `CredentialsUnavailable` | The token's KV key is missing, or the Repo's authority cannot read it. |
| `NotReady: Suspended` | `suspend = true`. |
| `Rejected: <reason>` | The spec cannot work: `FileUrlsDisabled`, `CredentialsInUrl`, `UnsupportedUrl`, `InvalidInterval`, `InsecureCredentials`. |
| `Stale` | gitops is not running (`legion app status gitops`). |

The status fields (`legion object get repo storefront -j`) are the
inventory: `commit` (deployed), `latest` (last seen), `subject`, `syncedAt`,
`syncRequest`, `deployed`, and `jobs`, each with its `name`, `file`,
`digest`, `version`, `state` (`deployed`, `progressing`, `pending`,
`failed`, `stopped`), and `detail`.

## Deploying from CI

The pipeline pushes a deploy commit and runs:

```console
$ legion gitops sync storefront -n team-a --wait --timeout 10m
storefront: sync requested
storefront: Pending (Progressing: at 8d0e44b: rolling out storefront (1 of 2 jobs deployed))
storefront: Ready (Synced: at 8d0e44b: 2 jobs deployed)
```

`sync` returns once gitops has looked at the repository; with `--wait`, once
the commit is deployed and every rollout finished. It exits non-zero when the
Repo reads `NotReady` or `Rejected` (a render error, a refused job, a failed
rollout), so the pipeline fails with the reason. `--json` prints the Repo's
report.

How it works: `sync` sets the Repo's label `gitops/sync` to a fresh token
(an apply, compare-and-set on the spec version), which wakes gitops, and
waits until gitops echoes the token in `status.fields.syncRequest`. So the
CI token needs what writing the Repo needs: `object:write` on `gitops/Repo`
in the namespace, and the Repo's `requires` (`job:read` and `job:write` on
its jobs). Re-applying records the CI token as the Repo's authorizer.

```nickel
# The CI token's policy: sync team-a's Repos, deploy team-a's jobs.
[
  { resources = ["object"], actions = ["read", "write"], namespaces = ["team-a"], kinds = ["gitops/Repo"] },
  { resources = ["job"], actions = ["read", "write"], namespaces = ["team-a"] },
]
```

To deploy a new image, the pipeline commits the new tag into the jobspec
(or into a JSON file the jobspec imports, which a script can rewrite
safely) before it syncs.

## Private repositories

Put an HTTPS token in KV and name the key:

```console
$ legion kv put secret/gitops/github-token ghp_xxxxxxxxxxxx
$ legion gitops add api --url https://github.com/acme/private-deploy --credentials-kv secret/gitops/github-token
```

gitops reads the key acting for the Repo, so whoever writes the Repo needs
`kv:read` on it. The token is sent to git through its environment, never
its arguments or a URL, and only to an `https://` remote. GitHub
fine-grained tokens and GitLab personal or project access tokens work with
the default username; Bitbucket wants `username = "x-token-auth"`. SSH URLs
and deploy keys are not supported yet.

## Who can do what

A Repo can make gitops deploy jobs, so writing one needs what deploying them
would. The kind's `requires`, checked against the writer on every apply and
delete, fail-closed:

| Writing a Repo needs | When |
| --- | --- |
| `object:write` on `gitops/Repo` in its namespace | always |
| `job:read` and `job:write` in its namespace, on every job (namespace-wide) | without `jobs` |
| `job:read` and `job:write` on each job `jobs` names | with `jobs` |
| `job:write` with `host = true` (on the same jobs) | `hostLevel = true` |
| `kv:read` on the key | `credentials` |
| `kv:read` on each key | `kvKeys` |

**gitops can do no more than the Repo's writer could.** Its own credential
can do nothing: every rule in its grant is `via = "objects"`. Each call it
makes for a Repo acts for that Repo ([Acting for an
object](app-objects.md#acting-for-an-object)), and the manager holds it to
the Repo's recorded authority: the permissions above, as its writer held
them when they applied it. A job outside them is refused by the manager,
whatever the repository says. Jobs gitops submits are stamped `managed by
gitops for Repo <namespace>/<name>`.

The authority is recorded, not re-checked: if the Repo's writer later loses
a right, gitops keeps deploying within what was recorded until someone
re-applies the Repo. `legion object list repos -A --authority-lapsed` finds
such Repos ([Authority is recorded, not
revoked](app-objects.md#authority-is-recorded-not-revoked)).

## Deleting a Repo

```console
$ legion object delete repo storefront -n team-a --wait   # stops the jobs it deployed
$ legion object delete repo storefront -n team-a --now    # leaves them running
```

With `prune` (the default), deleting a Repo stops every job in its
inventory that is still its own, within a 2-minute cleanup window, then the
Repo goes. With `prune = false`, or `--now`, the jobs keep running, as
ordinary jobs. A Repo created again under the same name adopts them.

## Incidents

- **Stop a job**: `legion job stop`. gitops leaves it stopped until its
  file changes (the Repo reads `Ready`, with `1 stopped by hand`).
- **Pause syncing**: apply the Repo with `suspend = true`. Nothing is
  deployed, written back, or stopped until it is `false` again.
- **Hot-fix by hand**: a `legion job run` over a gitops job works, and is
  written back within one interval. Fix it in git, or suspend the Repo
  first.
- **Roll back**: revert the commit in git and `legion gitops sync`.

## In the UI

gitops adds, through [app views](apps.md#views-and-actions):

- a **GitOps** page in the sidebar: every Repo in a namespace where you can
  read jobs, its state, ref, commit, and every job it deployed;
- a **GitOps** panel on every job a Repo deployed: the repository, ref,
  commit, file, the job's state, and what the Repo reports when it is not
  ready;
- a **Sync now** button on that panel (needs `job:write` on the job), which
  makes gitops look at the repository at once.

`legion app view gitops nav` and `legion app view gitops job <job>` print
the same views in a terminal.

## Configuration

gitops is seeded enabled on every cluster, on the managers. With no Repos it
watches and does nothing. Its config:

```nickel
# legion app config set gitops -f gitops.ncl
{
  allowFileUrls = false,   # file:// Repos: repositories on the disk of the nodes gitops runs on
}
```

`allowFileUrls` is for dev clusters and tests: anyone who may write a Repo
can then deploy from any git repository gitops's user can read on those
nodes. To seed it on a new cluster, set it in the manager config:
`apps = { gitops = { config = { allowFileUrls = true } } }`.

`legion app disable gitops` stops it (Repos read `Stale` and keep their
jobs); `legion app set gitops --placement workers` moves it ([Apps](apps.md)).
The nodes it runs on need the `git` binary on their `PATH`.

## CLI, API, and MCP reference

| CLI | What |
| --- | --- |
| `legion gitops add <NAME> --url URL [--ref] [--path] [--interval] [--jobs a,b] [--host-level] [--no-prune] [--credentials-kv KEY [--credentials-username U]] [--kv-key KEY]... [-n NS] [--dry-run] [--wait [--timeout D]] [--idempotency-key K]` | Create a Repo (create only: change one with `legion object apply`). |
| `legion gitops status [REPO] [-n NS \| -A]` | Every Repo, or one with its jobs. |
| `legion gitops sync <REPO> [-n NS] [--wait] [--timeout D]` | Sync now; `--wait` until deployed and healthy. |
| `legion object apply\|get\|list\|delete\|explain` | Repos are `gitops/Repo` objects ([App Objects](app-objects.md)). |

Every command takes `--json`. Over gRPC, Repos are `ApplyObject`,
`GetObject`, `ListObjects`, and `DeleteObject` with kind `gitops/Repo`; a
sync request is an `ApplyObject` that sets the `gitops/sync` label. The MCP
server lists `legion_gitops_status` and, with `--allow-write`,
`legion_gitops_sync`; agents create Repos with `legion_object_apply`
([Agent Readiness](agent-readiness.md)).

## Failure modes

| You see | Why | What to do |
| --- | --- | --- |
| `RenderFailed: ... has a host-level task (... uses runtime "exec"): set hostLevel = true` | `exec` tasks run on the host | `hostLevel = true` (`--host-level`), by someone with a host-level `job:write` grant |
| `RenderFailed: ... deploys web into namespace prod` | a jobspec names another namespace | drop `namespace`, or write a Repo in `prod` |
| `RenderFailed: ... defines job x, which spec.jobs does not list` | `jobs` narrows the Repo | add `x` to `jobs`, or remove the file |
| `RenderFailed: ... imports "../secret", which is outside the repository` | an import leaves the checkout | keep imports inside the repository |
| `RenderFailed: ... took longer than 10s to evaluate` | a Nickel program that does not finish | fix the program |
| `RenderFailed: ... has no jobs in "jobs"` | the commit removes every job | fix `path`, or delete the Repo to stop its jobs |
| `ApplyFailed: ... nothing deployed: web: ... (the jobspec reads kv secret/db; ...)` | a job reads a KV key the Repo does not name | add it to `kvKeys`, as someone who may read it |
| `ApplyFailed: job web is deployed by gitops for Repo team-a/other` | two Repos define one job | remove it from one repository |
| `ApplyFailed: ... is outside what gitops for Repo ... holds` | the Repo's authority does not cover the call | re-apply the Repo as someone who holds what it needs |
| `FetchFailed: git ls-remote of ... failed: ...` | the URL, the token, or the network | check the URL and `credentials` |
| `FetchFailed: git is not installed on this node` | gitops runs the `git` binary | install git on the nodes gitops runs on |
| `Rejected: FileUrlsDisabled` | `file://` URLs are off | an operator sets `allowFileUrls`, or use `https://` |
| `writing gitops/Repo ... needs job:write in team-a ..., which your token does not hold` | the kind's `requires` | have someone who holds it write the Repo |
| `Stale` | gitops is not running | `legion app status gitops` |
| `gitops has not picked up the sync request ... yet` | gitops is down or busy | `legion app status gitops`, `legion app logs gitops` |

## Limitations

- **HTTPS only.** No SSH URLs or deploy keys yet; use an HTTPS token.
- **No push webhooks.** gitops polls (`interval`, `ls-remote` only) and
  syncs on request; CI calls `legion gitops sync`.
- **One namespace per Repo**, at the `normal` priority band: a job with
  another `priority` band is refused, since a Repo's authority records
  `normal` only.
- **No jobs that ask for grants.** A job with workload-identity
  `permissions` needs its submitter to hold them, and a Repo's authority
  records only job and KV permissions, so such a job is refused at submit.
- **No templating beyond Nickel**: no `vars`, no Helm or kustomize, and no
  job templates (`RunJobTemplate`).
- **Whole commits.** The fetch is shallow but takes the whole tree: point a
  Repo at a deploy repository, not a large monorepo.
- **No ordering** between jobs in a commit beyond file order.
- **One replica.** gitops runs one replica; more would sync each Repo more
  than once.
- **Stopped, not destroyed.** Pruned jobs stay in `legion job list` as
  stopped until someone destroys them.
- **With ACLs off**, nothing is stamped, so gitops tells its jobs apart by
  the version it submitted: a job two Repos define flips between them
  instead of being refused.
- **Image automation** (watching a registry and committing new tags) is not
  built in.
