# Judgement Calls: gitops-plan

Decisions made while designing and building
[`plans/gitops-plan.md`](../plans/gitops-plan.md). **Entries are ordered by
project impact, highest first, and an entry's number is its rank.** Entries
3, 7, 9, 13, and 14 were made while building, after the plan was written;
entries 2 and 10 changed then (each says how).

## 1. GitOps is a bundled app whose own credential can do nothing: every call acts for a Repo

**Call.** `gitops` ships in the binary, seeded and enabled like the
autoscaler, and its manifest's permissions are all `via = "objects"`:

```nickel
permissions = [
  { resources = ["job"], actions = ["read", "write"], namespaces = ["*"], via = "objects" },
  { resources = ["job"], actions = ["write"], namespaces = ["*"], host = true, via = "objects" },
  { resources = ["kv"], actions = ["read"], paths = ["*"], via = "objects" },
]
```

Every Legion call the app makes for a Repo carries `legion-for-object`, so
it holds only what the Repo's writer was checked for. Reads it needs for
health (`GetJobInfo`, `ListJobSpecHistory`, `ListRollouts`, `PlanJob`) come
from the Repo's `requires` (`job:read`), not from a plain `job:read *` on
the app. Its views draw from the app's memory and make no Legion call.

**Alternatives.** (a) An installable app (the `configs/apps/example-app.ncl`
shape), so clusters opt in. (b) A plain `job:read` in every namespace for
the app's own credential, as the autoscaler holds, which halves the Repo's
resolved permissions (the 64-permission budget) and lets views read jobs
directly.

**Trade-off.** Bundling takes the name `gitops` from installed apps
(breaking the example manifest and the test fixtures, which move to
`deployer`), and every cluster runs one more small process on its managers.
Putting `job:read` in `requires` means a Repo's writer needs read as well
as write (they almost always have both), and a Repo with a `jobs` list
spends two of its 64 resolved permissions per job (three with
`hostLevel`), so a list tops out at 20 jobs; past that, leave `jobs` out
for namespace-wide authority.

**Why (simplicity, security).** "Deploys from git" is the deploy flow, not
an add-on: a first-time user should not have to find, install, and grant an
app first (`legion gitops add ... --wait` is Ready in about two seconds on a
fresh single-node cluster). And the security story is one sentence: gitops
can deploy a job only where the person who wrote the Repo could have run
`legion job run` themselves. That is the guarantee Flux needed
service-account impersonation and Argo needed AppProjects to approximate.

## 2. A bad commit deploys nothing: render everything, check what no authority covers, preflight with PlanJob, then apply

**Call.** A sync renders every file of the commit first; then checks each
job for what a Repo's authority can never cover (a host-level task without
`hostLevel`, a priority band other than `normal`); then runs `PlanJob` for
every job it would submit. Any failure deploys nothing (`Ready=false`,
`RenderFailed` or `ApplyFailed`), and the previous commit keeps running.

```text
RenderFailed: e38ea7f: jobs/web.ncl defines job web, which has a host-level task ("server" uses runtime "exec"): set hostLevel = true on the Repo (writing it then needs job:write with host = true); jobs stay at 3f9a1c2
```

*Changed while building.* The plan had only the `PlanJob` preflight. The
ACL integration test showed it does not see host-level tasks or bands:
`PlanJob` is gated as a read and never checks the grants a submit claims,
so a host-level job passed the preflight and was refused by `RunJob`
halfway through the commit. gitops now parses each jobspec offline (as `job
run` does) and classifies its tasks with the manager's own
`task_host_level_reason`, before anything is submitted.

**Alternatives.** (a) Apply each job independently and report per job
(Argo's per-resource sync). (b) Apply without a preflight and stop at the
first refusal. (c) Make `PlanJob` check the write grants.

**Trade-off.** One extra `PlanJob` per changed job (only on a commit change
or drift, not every interval), and a commit that fixes job A while breaking
job B ships neither until B is fixed. The client-side check duplicates two
of the manager's rules; a `RunJob`-only refusal neither catches still stops
the sync midway, and the Repo names the jobs deployed before it. (c) is the
complete fix and is open in TODO.md: it changes what a read-gated RPC
checks, which is not this change's to make.

**Why (simplicity).** "The repository is at commit X, so the cluster is at
X or at the last good commit" is explainable at 3am; "some of X" is not.

## 3. A recorded read carries no band (a core fix)

*Found while building.* **Call.** `resolve_requirements` now gives a band
only to a `job:write` permission. Before, every `job` permission a kind
`requires` got band `normal`, including `job:read`; its authority rule then
named `bands = ["normal"]`, and since a read scope never carries a band, the
rule matched no read at all: an app acting for its object was refused
`job:read` its object's writer had been checked for.

```text
permission denied: job:read is outside what gitops for Repo team-a/site holds
```

**Alternatives.** Drop the band in `authority_rule` for non-write actions,
leaving `resolve_requirements` alone.

**Trade-off.** Recorded authority on existing objects keeps the band its
reads were recorded with until they are re-applied. No shipped kind
required `job:read` (the autoscaler's kinds map onto `scale` and `node`),
so nothing in the field held one.

**Why.** The band is what a submit claims; a read never does. Fixing it at
the resolution also fixes the writer check at apply, which would otherwise
refuse a writer whose `job:read` rule names bands.

## 4. Submit only when something changed, and leave a stopped job stopped

**Call.** gitops submits a job only when it is missing, its rendered spec's
digest differs from the one in the Repo's inventory, or its latest version
is not the one gitops submitted (someone replaced it). It does not restart
a job someone stopped; the job's next file change does. "Someone replaced
it" compares the job's latest config version (`ListJobSpecHistory`) with
the version the inventory recorded, which works with ACLs off too; the
`managedBy` stamp is used to refuse a job another Repo deployed.

**Alternatives.** (a) Resubmit every job every interval and let `RunJob`'s
`Unchanged` make it a no-op. (b) Restart stopped jobs too (full self-heal,
Flux's and Argo's default). (c) Detect drift by the stamp alone.

**Trade-off.** Drift detection costs a `GetJobInfo` and a
`ListJobSpecHistory` per job per interval, and a stopped job is a place the
cluster differs from git until someone reverts or edits the file. The Repo
lists it (`1 stopped by hand`).

**Why.** (a) is wrong, not just slow: `RunJob` restarts an unchanged job
that is stopped, failed, or idle, so it would rerun finished batch jobs
every minute. (b) fights the on-call responder: `legion job stop` is the
bleeding-stopper, and a tool that undoes it a minute later makes incidents
worse. A destroyed job is different (destroying says "it should not
exist", which git contradicts), so it is recreated. (c) fails with ACLs
off, where nothing is stamped.

## 5. The inventory lives in the Repo's status; prune stops, never destroys

**Call.** Status fields carry every job the Repo deployed (name, file,
digest, version, state). Prune stops jobs in the inventory that left git,
only while they are still the Repo's. Deleting a Repo with `prune = true`
stops its jobs within a 2-minute cleanup window; `prune = false`, or
`delete --now`, leaves them running, and a Repo created again under the same
name adopts them.

**Alternatives.** (a) List the namespace's jobs and prune by stamp. (b)
`DestroyJob` (Flux deletes). (c) Leave jobs running when a Repo is deleted.

**Trade-off.** The inventory is bounded by the 16 KiB status limit, so a
Repo deploys at most 100 jobs. Stopped jobs stay in `legion job list` until
someone destroys them.

**Why.** The inventory is Flux's model and needs no list RPC (which has no
stamp filter). Stopping is reversible: reverting the deletion in git
resubmits the job, which revives it. Deleting a Repo and having its jobs
stop is the symmetric, expected behavior, and `--now` is the escape hatch.

## 6. "Sync now" is a label the app echoes, not a new RPC

**Call.** `legion gitops sync` writes the label `gitops/sync=<token>` on the
Repo (compare-and-set on its spec version); the label write wakes the
reconcile loop, and the app echoes the token as `status.fields.syncRequest`,
which the CLI waits on. The UI's Sync now button wakes the loop in-process
(`Reconciler::wake`, a new hook in the object framework).

```console
$ legion gitops sync storefront -n team-a --wait
```

**Alternatives.** (a) A new `SyncRepo` RPC routed to the app. (b) An app
action on a job context, invoked from the CLI.

**Trade-off.** Triggering a sync is an apply, so the caller needs what
writing the Repo needs, and re-applying records them as the Repo's
authorizer; a CI token that syncs therefore needs `job:write` in the
namespace, not just "may nudge". The label shows up in `legion object get`.

**Why (simplicity).** It is Flux's `requestedAt` annotation pattern, needs
no proto change, works with the app down (the request waits), and is
retry-safe.

## 7. A rollout in progress is `Ready=unknown` (the object reads `Pending`)

*Changed while building* (the plan said `Ready=false, Progressing`).
**Call.** While a job gitops submitted is rolling out, the Repo reports
`Ready=unknown` with reason `Progressing`, through a new framework
constructor, `ObjectOutcome::progressing`. The manager summarizes it as
`Pending`, which `--wait` waits through.

```text
web: Pending (Progressing: at efc0ba5: rolling out web (1 of 2 jobs deployed))
web: Ready (Synced: at efc0ba5: 2 jobs deployed)
```

**Alternatives.** `Ready=false, Progressing`, the plan's shape; or a change
to the generic `--wait` so `NotReady: Progressing` is not settled.

**Trade-off.** The summary reads `Pending`, not `Progressing`; the reason
is in the detail. A suspended Repo still reads `NotReady: Suspended`.

**Why.** `legion object apply -f repo.ncl --wait` is a documented way to
create a Repo, and the generic wait treats every `NotReady` as a failure,
so it failed the moment the first rollout started. `Ready=Unknown` while
progressing is exactly Flux's convention, and needs no special case in
core.

## 8. Repo files are rendered in a child process with imports confined to the checkout

**Call.** The app renders each file with `legion:self app gitops-render`, a
hidden command, killed after 10 seconds, with an empty environment. Before
evaluating, it lexes every Nickel file it would load and refuses an import
that is not a plain string, is absolute, or resolves (symlinks followed)
outside the checkout (`config_lang::eval_file_confined`).

**Alternatives.** Evaluate in the app's process with `eval_file_tracked`.

**Trade-off.** A process spawn per file per new commit, and the hidden
command. Tests render in process.

**Why.** Repo content is written by the Repo's authors, not by gitops's
operator. In process, `let rec f = fun x => f x in f 1` hangs a thread
forever, and `import "/etc/passwd" as 'Text` (or a symlink) reads the
managers' disk into a jobspec, or into a parse error shown in status.
Checking before evaluating, not after, is what keeps an outside file out of
the error message too.

## 9. Host-level jobs need `hostLevel`, on every cluster

*Made while building.* **Call.** A Repo whose jobs have host-level tasks
(every `exec` task) must say `hostLevel = true`, even on a cluster with ACLs
off, where the manager would not check it.

**Alternatives.** Skip the check where ACLs are off (the app cannot easily
tell), or default `hostLevel` to true.

**Trade-off.** The simplest demo (an `exec` job on a laptop) needs
`--host-level`. The refusal says exactly that.

**Why (security).** A default of true would make every Repo writer need a
host-level grant, which is what turns "can deploy" into "is root on every
node"; and behaving differently with ACLs off would make a Repo that works
on a dev cluster fail on a real one.

## 10. One `ref` field, polled with `git ls-remote`; HTTPS tokens only; `file://` behind a switch

**Call.** `ref` is a branch, a tag, or a full commit SHA (default `main`).
Each interval runs `git ls-remote`; a fetch happens only on a new commit.
Private repositories use an HTTPS token from KV (`credentials = { kv,
username }`); SSH is deferred. `file://` URLs need the app's
`allowFileUrls`. *Changed while building:* running it on a real cluster
showed git refuses a local repository owned by another user, and honors
`safe.directory` only from a protected config scope (not `GIT_CONFIG_*`, not
`-c`), so a `file://` remote reads a one-line global config the app writes
into its cache directory.

**Alternatives.** Flux's `ref.branch` / `ref.tag` / `ref.commit`; SSH deploy
keys now.

**Trade-off.** A branch and a tag with the same name resolve to the branch.
GitHub deploy keys (SSH-only) do not work; fine-grained tokens do.

**Why (simplicity).** One field covers every case Argo's `targetRevision`
does. SSH needs host-key policy to be safe, which is its own design.

## 11. Every `.ncl` and `.json` file directly in `path`; `path` defaults to the root

**Call.** A Repo reads the files directly in `path` (default `.`), skipping
names that start with `.` or `_`; subdirectories hold shared Nickel to
import.

**Alternatives.** Recursive discovery (Flux, Argo); an index file listing
the jobspecs (kustomization.yaml); a required `path`.

**Trade-off.** A Repo pointed at an application repository's root reads its
`package.json` as a jobspec and fails the render, with the file named; set
`path`.

**Why (simplicity).** No index file to keep in step, and a library file can
never be mistaken for a job: whether a file is a job is where it sits.

## 12. A commit with no jobs, where the Repo had some, is refused

**Call.** A render that finds zero jobs when the inventory has some is
`RenderFailed: no jobs at <path>`, and nothing is pruned. A missing `path`
is the same.

**Alternatives.** Treat it as "remove everything" (Flux).

**Trade-off.** A deliberate "turn everything off" commit does not work;
delete the Repo instead (which stops its jobs).

**Why.** A typo in `path`, a moved directory, or a bad merge would
otherwise stop every job in the namespace.

## 13. The app remembers what it reported; the stored status is read only after a restart

*Made while building.* **Call.** A Repo's next sync starts from the status
the app last reported, kept in memory (`Book`), and falls back to the status
the watch delivered only for a Repo it has not synced since it started.

**Alternatives.** Read the object's status from the watch every time.

**Trade-off.** One more map in the app, swept of deleted Repos every
minute.

**Why.** Status reports never wake the app's watch, by design, so the
watch's copy of a Repo is as old as its last spec change: the inventory
would have been forgotten between syncs. The integration test caught it.

## 14. `legion gitops add` creates only

*Made while building.* **Call.** `add` applies with `expected_version = 0`:
it creates a Repo, and refuses one that exists. Changing a Repo is `legion
object apply`.

**Alternatives.** `add` as create-or-update.

**Trade-off.** Two commands where one could do.

**Why.** `add` builds the whole spec from flags: as an update it would
silently drop every field the flags leave out (`kvKeys`, `credentials`).
