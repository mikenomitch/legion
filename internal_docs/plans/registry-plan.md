# Package Registry Plan

> **Judgement calls:** [../calls/registry-plan_calls.md](../calls/registry-plan_calls.md)

**Status: PLANNED** (written 2026-11-12, for review; nothing is built).
Deliberately deferred or refused: hosting container images, transitive
dependency resolution, a stable native plugin ABI (still its own TODO item), a
Nickel-library package kind, client-side Sigstore verification, a
transparency log, private or paid packages on the public registry, ratings and
comments, and Windows builds.

**In one paragraph.** Legion gets **packages**: named, semver-versioned,
immutable bundles of one of four kinds — **job**, **template**, **app**, or
**plugin** — each version pinned by one sha256 digest. Packages live in
**registries**. The simplest registry is a git repository with a fixed layout
and version tags. The default registry is a hosted service (Rust, Postgres,
Cloudflare R2, running on Legion) that adds search and filters, a web UI,
GitHub sign-in, publishing from the CLI or from a git tag, and an admin panel
with analytics. Registries are cluster configuration, so every teammate's
`legion/postgres` means the same thing. Every install verb that exists today
(`job run`, `template`, `app install`) takes a package reference, and the new
`legion plugin install` has the managers hand a plugin to every matching node.
Worker and manager configs can also declare plugins by package or URL with a
pinned hash, which the daemon fetches and verifies before loading.

## Contents

1. [Problem statement](#1-problem-statement)
2. [Prior art](#2-prior-art) — [Helm, right and wrong](#21-helm-what-it-gets-right-and-what-it-gets-wrong) · [the rest](#22-other-registries) · [what users want from a registry](#23-what-users-want-from-a-registry)
3. [Jobs to be done, by persona](#3-jobs-to-be-done-by-persona)
4. [Goals and non-goals](#4-goals-and-non-goals)
5. [Background — what exists today](#5-background--what-exists-today)
6. [Proposed design](#6-proposed-design)
7. [Proto, struct, config, and persistence changes](#7-proto-struct-config-and-persistence-changes)
8. [Implementation phases](#8-implementation-phases)
9. [Testing plan](#9-testing-plan)
10. [Rollout and backward compatibility](#10-rollout-and-backward-compatibility)
11. [Open questions](#11-open-questions)
12. [Acceptance criteria](#12-acceptance-criteria)

## 1. Problem statement

Everything a Legion user might want to share already exists as a file, but
there is no way to name it, version it, find it, trust it, or update it.

- **Jobs are shared by copying files.** A team that wants "the good Postgres
  job" copies a `.ncl` from a repo, a blog post, or `configs/jobs/postgres.ncl`.
  `legion job run` can fetch a URL or git source
  (`src/cmd/job.rs:1253-1340`), but with no checksum, no version, and no way
  to learn that a better one exists.
- **Templates stop at the cluster boundary.** The public doc says so: "Templates
  are per-cluster. Remote or federated template registries are not planned for
  now" (`docs/job-templates.md`, Known limitations). Sharing one across three
  clusters means running `template create` from a file in each of them.
- **Apps install from a file or a URL with no integrity check.**
  `AppSource` is `Bundled | File | Url` (`crates/common/src/structs/app.rs:895-902`).
  The CLI fetches a URL with a plain GET (`src/cmd/app.rs:918-932`), and the
  bytes are never checked against anything. `version` is a free-form string
  (`app.rs:328`, `:583-609`) that is never compared, so an "upgrade" can be a
  downgrade. There is no rollback verb (`proto/legion.proto`, `UpgradeApp`
  comment). The apps plan deferred all of this to "the registry plan"
  (`legion-apps-plan.md:1633-1635`), which did not exist until now.
- **Plugins are files an operator copies onto every machine.** The loader reads
  `<root>/<kind>/<name>/` from `LEGION_PLUGIN_DIR` or next to the binary
  (`crates/common/src/util.rs:365-395`). Nothing fetches, verifies, versions, or
  reports a plugin across the fleet ("a `legion plugin status`" is still open,
  `internal_docs/TODO.md:1162`). Native plugins must match the exact Legion
  build (`crates/worker/src/plugin_runtime.rs:25-71`), so every release means
  rebuilding and re-copying them to every node.
- **Three surfaces already fetch code by URL plus sha256**, each with its own
  convention and none with a name or version: net-plugin modules
  (`configs/netplugins/read-only-free-tier.ncl`), autoscaler strategies
  (`configs/scaling/autoscaler-app-config.ncl:38-43`), and isolate modules
  (`crates/common/src/structs/isolate.rs:223-235`).
- **Nothing is discoverable.** The TODO has asked for "a job registry: default +
  added registries, `@<registry>/<job>`, inspect metadata, a UI to fill in
  variables; plugins by URL or registry" (`TODO.md:1071-1072`) since templates
  shipped.

Users work around this today with GitHub search, copy and paste, commit SHAs
pinned by hand in READMEs, and Ansible roles that drop plugin binaries on
hosts.

## 2. Prior art

### 2.1 Helm: what it gets right, and what it gets wrong

Helm is the registry most Legion users will compare this to, and most of its
lessons are expensive ones.

**What Helm gets right (we take these):**

- **A package is the unit of reuse, versioned on its own.** A chart has a
  version, a name, metadata, and a README, independent of the software inside it.
- **Install, upgrade, and rollback as verbs**, with release history. People
  think in "upgrade postgres", not "submit a new spec".
- **Local rendering.** `helm template` shows exactly what will be applied, with
  no cluster. Legion's equivalents are `template render` and `job plan`, and
  the registry should expose them before install too.
- **Lint and test as part of authoring** (`helm lint`, chart-testing). The
  validator that runs at publish should be the one authors run locally.
- **Defaults plus overrides.** Most users set three values and accept the rest.
- **Discovery through a hub.** Artifact Hub made charts findable, with
  security reports, a values-schema viewer, and changelogs. People shop for
  infrastructure the way they shop for libraries.
- **It moved to OCI** to reuse registry infrastructure people already run (we
  take the content-addressing, not the protocol; see calls #4).

**What Helm gets wrong (we design against these):**

- **Text templating of a whitespace-sensitive format.** Charts are Go templates
  that emit YAML (`{{- toYaml .Values.resources | nindent 12 }}`). Errors
  surface as malformed YAML at apply time, and indentation bugs are a genre.
  *Legion:* Nickel evaluates to structured data, so there is no text layer to
  get wrong.
- **`values.yaml` is an untyped API, and the schema is optional.** Most charts
  ship no `values.schema.json`, so a typo'd key is silently ignored.
  *Legion:* variables are declared and typed (`docs/job-templates.md`, Variable
  definitions). Unknown keys are refused, and the registry shows the schema.
- **Charts re-expose the whole Kubernetes API through values.** Every chart
  grows `podAnnotations`, `extraVolumes`, `extraEnv`, `tolerations`, … because
  values are the only way in. The real escape hatch (post-renderers, Kustomize)
  is a second tool. *Legion:* packages expose a few curated knobs. The escape
  hatch is first class: `legion registry pull` copies the package for you to
  own, and a job package is plain Nickel you can `import` and merge over.
- **Install instructions don't work on a fresh machine.** `helm repo add
  bitnami …` must run first, and the alias it creates is local to that laptop,
  so READMEs, teammates, and CI disagree about what `bitnami/redis` means.
  *Legion:* registries are cluster configuration, one list for everyone
  (calls #1). The default registry needs no setup, and a reference can name a
  registry by location with no alias at all.
- **A monolithic `index.yaml`.** A chart repository is one file listing every
  version of every chart, downloaded whole on every `helm repo update`. It
  reaches megabytes for big repositories and is stale between updates.
  *Legion:* a sparse per-package index (Cargo's lesson), fetched on demand.
- **Mutable versions and optional integrity.** Nothing stops a repository
  serving different bytes for `1.2.3` tomorrow. Provenance files (`.prov`, PGP)
  exist and almost nobody uses them. *Legion:* versions are immutable, every
  version is pinned by a digest, and the cluster refuses an install it cannot
  pin.
- **Tiller.** Helm 2 ran an in-cluster installer with cluster-admin, so anyone
  who could talk to Tiller was root. Helm 3 removed it. *Legion:* no privileged
  installer. Every install is an ordinary ACL-checked RPC made as the caller,
  and an app's extra power is a grant the installer must already hold
  (`app_grant_refusal`, `crates/manager/src/manager/server/app_rpcs.rs:850-851`).
- **A second copy of state.** Release state is a Secret per revision in the
  namespace, with a size limit, separate from what is actually running, and
  drift goes undetected. *Legion:* the job, template, or app *is* the release.
  Provenance is a field on it, not a parallel object.
- **Upgrades lose or leak values.** `--reuse-values` versus `--reset-values`
  (and later `--reset-then-reuse-values`) is a well-known footgun: new defaults
  are ignored, or old overrides vanish. *Legion:* template runs already record
  their resolved variables (`template.variables.json`). An upgrade re-applies
  them to the new version, refuses when the new version needs a value nobody
  gave, and shows the rendered diff before anything changes (§6.10).
- **Diff is a plugin.** `helm diff` is the most-installed Helm plugin because
  upgrades are blind without it. *Legion:* plan diffs and the template launch
  Review step already exist and are part of every upgrade.
- **CRDs are installed once and never upgraded.** A chart's `crds/` directory is
  installed but never updated or removed, so schema changes ship out of band.
  *Legion:* app object kinds upgrade with the app, and the registry flags a
  narrowed schema as a breaking change (§6.3).
- **Hooks are an imperative side channel.** Pre/post-install hook resources
  have weight-based ordering and aren't part of the release. *Legion:* ordering
  lives in the jobspec (`dependsOn`, lifecycles), and packages get no hooks.
- **Two versions, and people bump the wrong one.** `version` against
  `appVersion`. *Legion:* one version resolves. `upstream` is display-only
  text ("redis 7.4.1").
- **Compatibility is rarely declared.** `kubeVersion` is optional and seldom
  set, so charts break silently on new clusters. *Legion:* `legion` is a
  required version requirement, and default resolution picks the newest
  version compatible with *your* cluster.
- **Dependencies (subcharts) are confusing.** Value scoping across parents and
  children is hard to explain. *Legion:* no transitive dependencies; `requires`
  is checked, never auto-installed (non-goal).

### 2.2 Other registries

- **Artifact Hub** ([artifacthub.io](https://artifacthub.io)) aggregates many
  repositories (Helm, OLM, Falco, …) and adds verified publishers (a metadata
  file in the repo), official badges, image security reports, a values-schema
  browser, template viewers, and per-version changelogs. *Take:* all of those
  page features, especially "see the templates before installing". *Reject:*
  splitting discovery from distribution. Artifact Hub can't promise that what
  you browse is what you install. Our hosted registry does both, and can also
  ingest a git registry (§6.11.2).
- **Nomad Pack** ([github.com/hashicorp/nomad-pack](https://github.com/hashicorp/nomad-pack))
  is the closest sibling: registries are git repositories added with `nomad-pack
  registry add`, packs carry `metadata.hcl` and `variables.hcl`, and
  `render`/`plan`/`run` mirror Helm. *Take:* a git repository as a complete
  registry, and render and plan before run. *Reject:* Go text templates over
  HCL (Helm's mistake again), whole-registry versioning by git ref instead of
  per-pack versions, and per-machine registry aliases.
- **Terraform Registry**
  ([publishing modules](https://developer.hashicorp.com/terraform/registry/modules/publish),
  [provider protocol](https://developer.hashicorp.com/terraform/internals/provider-registry-protocol),
  [dependency lock](https://developer.hashicorp.com/terraform/language/files/dependency-lock)).
  Modules publish by connecting a GitHub repository, and every semver tag
  becomes a version. Providers ship per-platform binaries with GPG-signed
  `SHA256SUMS`, and `.terraform.lock.hcl` records hashes. Service discovery
  uses `/.well-known/terraform.json`, and a network-mirror protocol covers
  air-gapped installs. *Take:* publish-on-tag, a `.well-known` discovery
  document, per-platform binaries, and mirrors. *Reject:* per-platform hashes
  in the lock file. Teams hit "the lock has no hash for linux_arm64" and have
  to run `terraform providers lock -platform=…`. Our version digest pins every
  platform at once (calls #3).
- **crates.io / Cargo**
  ([sparse index](https://doc.rust-lang.org/cargo/reference/registry-index.html),
  [trusted publishing](https://crates.io/docs/trusted-publishing)). Versions are
  immutable, yank is not delete, the index is sparse per crate, and CI can
  publish with no stored token. *Take:* all of it, including Cargo's semver
  requirement syntax and prerelease rules.
- **Go module proxy and checksum database**
  ([go.dev/ref/mod](https://go.dev/ref/mod#checksum-database)). Once seen, a
  version's bytes are fixed forever and checked against a transparency log.
  *Take:* "a version is its digest" and "a moved tag is an error". *Defer:* the
  transparency log, which is more machinery than a first registry needs
  (calls #19).
- **Krew** ([plugin manifest](https://krew.sigs.k8s.io/docs/developer-guide/plugin-manifest/))
  and **Homebrew taps** ([docs.brew.sh/Taps](https://docs.brew.sh/Taps)).
  Both use a git repository of manifests, each with per-platform `uri` plus
  `sha256`, and both support custom indexes. *Take:* the per-platform
  build-list shape and "a tap is just a repo". *Reject:* metadata-only
  indexes pointing at third-party release URLs, which rot. The hosted registry
  copies the bytes it serves.
- **npm, PyPI, crates.io trusted publishing**
  ([PyPI](https://docs.pypi.org/trusted-publishers/),
  [npm provenance](https://docs.npmjs.com/generating-provenance-statements)).
  A CI job's OIDC token is exchanged for a short-lived publish token tied to
  one repository and workflow, and the registry records which commit built
  which version. *Take:* exactly this, as the recommended way to publish.
- **Packer `required_plugins`**
  ([docs](https://developer.hashicorp.com/packer/docs/templates/hcl_templates/blocks/packer#specifying-plugin-requirements)).
  The config declares plugin versions, and `packer init` fetches them and
  verifies checksums. *Take:* the shape of our declarative node-config block.
- **Kubernetes node plugins** (CNI binaries, CSI drivers, device plugins) are
  distributed by privileged DaemonSets whose init containers copy files onto
  the host. It works, but it's a workaround: a root container per node and no
  record of which build is on which machine. **Nomad** has no distribution at
  all; task drivers go into `plugin_dir` by config management
  ([docs](https://developer.hashicorp.com/nomad/docs/configuration/plugin)).
  *Legion makes node-plugin distribution first class* (§6.9).
- **Grafana plugin signing**
  ([docs](https://grafana.com/docs/grafana/latest/administration/plugin-management/plugin-sign/)).
  Unsigned plugins are refused unless allowlisted. The apps plan cited it as
  the reason it rejected in-origin JS plugins (`legion-apps-plan.md:250`).
  *Take:* the cluster policy that can require provenance for native plugins
  (Phase 7).
- **Docker Hub's 2020 pull limits** are a reminder that registry economics
  shape the user experience. Serving immutable blobs from R2 (no egress
  fees) behind a CDN keeps rate limits a matter of abuse, not cost.

### 2.3 What users want from a registry

Distilled from the above and from the personas in §3. Each item points at
the part of the design that answers it.

| # | Users want… | Answered by |
| --- | --- | --- |
| W1 | To find the thing fast: search that tolerates typos, filters that match how they think (kind, category, runtime, "works on my cluster") | §6.16 |
| W2 | To trust it before running it: who published it, whether it's official, what it does to the cluster (host access, privileges, permissions), and the source itself | §6.12, §6.17 (capabilities panel, source browser) |
| W3 | Copy-paste install that works on a fresh machine | §6.5, §6.7 |
| W4 | A few clear settings with good defaults, and an exit to full control | §6.4 (variables), §6.8.1 (`pull`) |
| W5 | To know it works with their Legion version | §6.3 (`legion` requirement, default resolution) |
| W6 | Stable, immutable versions they can pin and reproduce | §6.3, §6.6.3 |
| W7 | To see what changed between versions, especially permission and capability changes | §6.10, §6.17 (version diff) |
| W8 | Upgrades that keep their settings, and rollback when it goes wrong | §6.10 |
| W9 | Private packages and air-gapped use with the same tools | §6.6.1, §6.7, §6.9.3 |
| W10 | Publishing that is no harder than pushing a tag | §6.11 |
| W11 | Signals of quality and life: downloads, last release, maintained or deprecated | §6.16, §6.17 |
| W12 | Fast action on bad packages: yank, takedown, advisories | §6.3, §6.18 |
| W13 | A machine-readable surface for scripts and agents | §6.15, §6.19 |

## 3. Jobs to be done, by persona

**Package user (job author, app developer)**
- When I need a common piece of infrastructure like a database or a cache, I
  want to find a ready-made, well-regarded version and run it with two or
  three settings, so I don't spend a day writing config someone else already
  got right.
- When I find something that looks right, I want to see exactly what it will
  run and what it needs from my cluster before I install it, so a privileged
  container or a host mount doesn't surprise me.
- When the ready-made version is 90% of what I need, I want my own copy to
  change, so I'm not stuck waiting on its author.
- When a new version comes out, I want to hear about it, see what changed, and
  upgrade without re-entering my settings, so staying current isn't a project.

**Cluster operator**
- When some of my machines need a plugin (a GPU driver, a storage driver, a
  runtime), I want to tell the cluster once and have every matching machine
  fetch it, so I don't log into fifty hosts or keep a separate recipe for it.
- When I build machine images or bootstrap nodes from config files, I want to
  name the exact plugin build in the node's config and have the node refuse
  anything else, so a tampered download can never run on my fleet.
- When a plugin rollout goes wrong on some machines, I want to see which
  machines have which version and why the others failed, so I can fix it or
  back it out.
- When my cluster can't reach the internet, I want installs to come from a
  copy I control, so an isolated cluster gets the same experience.

**Security / platform admin**
- When my teams install shared packages, I want to limit them to sources I've
  approved, so nobody pulls code from a random repository into production.
- When something is installed, I want a record of exactly which bytes came from
  where and who installed them, so I can answer for it in an incident or audit.
- When a package asks for more power in a new version, I want that change shown
  and approved on purpose, so access doesn't grow silently through upgrades.

**Package author / contributor**
- When I've built something useful, I want to share it with one command or by
  pushing a tag, so publishing is no harder than committing.
- When I publish, I want problems caught before anyone installs it, so I don't
  ship a broken release to strangers.
- When people use what I made, I want to see how much, and on which versions,
  so I know where to spend my time.

**Registry admin (runs the public registry)**
- When something malicious or infringing is published, I want to pull it in a
  minute and know how widely it spread, so the damage stops.
- When I'm deciding what to build or promote next, I want to see what people
  search for and don't find, what's growing, and which Legion versions are in
  use, so I put effort where it matters.
- When the registry misbehaves, I want its health and its traffic in one place,
  so running it doesn't become a second job.

**Autonomous agent**
- When I'm asked to "set up Postgres on the cluster", I want to search packages
  and read their settings as structured data, so I can choose one and fill it
  in without scraping a web page.
- When I install something, I want to preview the exact version and change, and
  then install exactly that, so a retry, or a release landing between my two
  steps, can't change what I install.

**Private registry host (a company running its own)**
- When my company has internal packages, I want to share them the same way
  public ones are shared, by running the same registry privately or just using
  a git repository, so there is one way to do it.

*Not in scope:* the on-call responder. A registry isn't an incident tool. They
benefit indirectly, because every installed object now says which package
version it came from and whether that version was yanked.

### User scenarios

1. **Postgres in five minutes.** Dana searches "postgres" on the registry
   site, filters to *Templates* and *Official*, and opens `legion/postgres`.
   The page shows the variables, a "what this does to your cluster" panel
   (one container, a 20 GiB volume, no host access), and the template source.
   She copies `legion template install legion/postgres`, runs it, opens the
   Templates page in her cluster's UI, fills in three fields, and launches.
   Two months later `legion registry outdated` shows `16.4.0 → 16.6.0`. `legion
   template upgrade postgres` shows the source diff, and her next launch reuses
   her recorded values.
2. **A GPU plugin across a pool.** Sam runs `legion plugin install
   legion/nvidia-device --pool gpu`. The managers fetch the package once, and
   each of the 40 GPU nodes pulls its own platform's build from a manager,
   checks the digest, and loads it. `legion plugin status nvidia-device` shows
   38 `active` and 2 `failed: driver library libnvidia-ml.so.1 not found`. For
   the next machine image, Sam runs `legion registry pin
   legion/nvidia-device@1.4.2` and pastes the printed `{ package, digest }` line
   into the worker config, so new nodes come up with the plugin before they
   take work.
3. **Publish on tag.** Priya keeps her team's `redis-sentinel` job in
   GitHub. She runs `legion package init` (it writes `legion-package.ncl` from
   her jobspec's `vars` usage), signs in to the registry with GitHub, and links
   the repository. From then on, `git tag redis-sentinel/v1.1.0 && git push
   --tags` publishes the version, with "built from acme/infra@4f1c2e9" on its
   page.
4. **The locked-down cluster.** Lee's clusters can't reach the internet. Lee
   runs the registry container internally, mirrors the dozen public packages
   the company approved, and on each cluster runs `legion registry remove
   legion` and `legion registry add corp https://registry.corp.internal
   --default`. Unqualified `legion/postgres` now resolves against the mirror,
   and an install from any other source fails with
   `registry 'github.com/foo/bar' is not configured on this cluster`.
5. **The registry admin's Monday.** The admin opens `/admin`: downloads up 12%
   week over week, `legion/postgres` trending, three open abuse reports, and a
   zero-result search list led by "kafka" (41 searches, no results). They take
   down a typosquat (`legoin/postgres`), which shows who downloaded it, and pin
   "Kafka" to the wanted list on the home page.

## 4. Goals and non-goals

**Goals**

- One **package** model for jobs, templates, apps, and plugins: semver
  versions, immutable, one digest per version across all platforms.
- **Two registry types** behind one resolver: a git repository with a fixed
  layout (the simplest form), and an HTTP registry speaking a small,
  static-servable read protocol.
- A **hosted registry service**: one container image, Postgres for metadata and
  search, R2 for files, running on Legion. It has a fast, beautiful web UI with
  search and filters, GitHub sign-in, publishing from the CLI, a linked repo,
  or GitHub Actions, and an **admin panel with analytics**.
- **Install from a reference** with every existing verb (`job run`, `template
  install`, `app install`), plus a new **`legion plugin install`** that the
  managers distribute to matching nodes, with per-node status.
- **Declarative, hash-pinned plugins** in worker and manager configs, fetched
  and verified by the daemon before load.
- **Provenance on every installed object**, `outdated`, upgrades that keep
  settings, and rollback by version.
- **Registries are cluster configuration**, and the configured list is the
  allowlist.
- A full **agent surface**: `--json` everywhere, MCP search and info tools,
  dry-run installs that return the digest, and `--digest` to install exactly
  that.

**Non-goals**

- **Hosting container images.** Packages reference images by digest in the
  registries people already use (GHCR, Docker Hub, ECR). The
  "push images straight into a worker" item stays separate (`TODO.md:823`).
- **Transitive dependency resolution.** `requires` is checked at install and
  reported, never auto-installed. Helm's subcharts are the cautionary tale.
- **A stable native plugin ABI.** Native plugins keep their per-build
  fingerprint (`plugin_runtime.rs:28-71`). The registry carries one build per
  Legion release and platform, and says plainly when a build is missing. The
  stable ABI stays its own TODO item (`TODO.md:1159`).
- **A Nickel-library kind** (`import "pkg:legion/std"`). It's the natural fifth
  kind, but it needs import resolution changes and has its own plan.
- **Building from source on the registry.** Publishers' CI builds. The registry
  validates, stores, and serves.
- **Private or paid packages on the public registry.** Private means a git
  registry or a self-hosted registry.
- **Ratings, reviews, comments, stars.** Downloads, recency, and verified or
  official status are the quality signals.
- **Client telemetry.** The CLI reports nothing beyond the requests it makes.
- **Windows builds**, and replacing GitOps. GitOps can reference packages
  (§6.9.4), but it stays the "apply this repo" tool.

## 5. Background — what exists today

**Templates** (`docs/job-templates.md`)
- Cluster objects with immutable integer versions starting at 1
  (`proto/legion.proto:4500-4566`).
- Rendered on the manager, sandboxed: no imports, a 5 s budget, and a capped
  thread pool (`crates/common/src/config_lang.rs:684-702`, `:807-817`).
- Runs record `template://<name>/<version>` plus their resolved variables, and
  stamp provenance on the job config.
- **Fits:** a template package installs as a new cluster revision with a
  registry provenance. Nothing about rendering changes.

**Jobs**
- Evaluated client-side (`render_jobspec`, `src/cmd/job.rs:1346-1361`). Imports
  resolve relative to the file, and every loaded source is stored with the job
  as a `Submission` (`config_lang.rs:458-535`).
- Remote sources (`https`, `git::`, `s3://`) are fetched to a temporary
  directory (`job.rs:1283-1340`), with no checksum, and imports are not
  confined to the fetched tree.
- `eval_file_confined` exists for GitOps: it refuses absolute paths, escapes,
  and package imports, and caps evaluation at 512 files of 1 MiB each
  (`config_lang.rs:538-682`).
- **Fits:** job packages are evaluated with `eval_file_confined` rooted at the
  package.

**Job variables**
- Every jobspec sees a free `vars` record from `--var` and `--var-code`
  (`configs/jobs/vars.ncl`; `src/cmd/job.rs:1055-1110`). These are undeclared
  and untyped.
- **Fits:** job packages may declare variables with the template schema, and
  the CLI then validates `--var` against them.

**Apps**
- The `AppManifest` is evaluated from Nickel or JSON (`app.rs:316-379`).
- The source is `Bundled | File | Url` (`app.rs:895-902`), resolved
  client-side by `load_manifest` (`src/cmd/app.rs:879-915`).
- The `AppRecord` keeps `source`, `source_ref`, and `manifest_digest`
  (`app.rs:1090-1110`). The digest is the first 16 bytes of SHA-256 over
  canonical JSON, used for change detection, not integrity (`app.rs:1388-1394`).
- `InstallApp` / `UpgradeApp` take a dry run that returns the consent prompt
  and the permission diff (`proto/legion.proto:3076`, `:3131`;
  `src/cmd/app.rs:957-1175`).
- Installed apps run "an artifact or an image, like any job"
  (`docs/apps.md:142-144`), and their `codeFields` can pin artifact checksums.
- **Fits:** a registry source kind, semver versions, and rollback by ref.

**Plugins** — three mechanisms (`docs/plugin-system.md:5-36`):
- **Native Rust dylibs** (`tasks`, `scheduling`, `rollouts`, `node_checks`,
  `artifacts`):
  - Loaded by `plugin_runtime.rs` from `<root>/<kind>/<name>/plugin.json` plus
    `lib<name>.<ext>` (`:276-315`).
  - Guarded by `legionApiVersion == 3` (`:6`) and a build fingerprint: rustc
    version, crate version, target, panic strategy, and the sizes of the
    boundary types (`:28-71`). A mismatch refuses the load. The code calls this
    "a guard, not a stable ABI" (`:25`).
  - Upgrades require rebuilding every dylib (`docs/upgrades.md:459-476`).
- **Exec plugins**, a process per call with JSON on stdio:
  - `volumes`, auto-discovered under `<plugin_dir>/volumes`, with
    `legionVolumeApiVersion` (`crates/common/src/volumes/mod.rs:734-760`).
  - `devices` (`legion-device/1`, configured by `command` in the worker config,
    `configs/workers/default.ncl:313-315`).
  - `keyring` and `cluster_ca` on the manager (`configs/manager.ncl:236`,
    `:255`).
  - `scaling` strategies, inside the autoscaler app, fetched by `source` +
    `checksum`.
- **WASM:** net plugins (proxy-wasm on wasmi in `netd`) are manager-stored
  objects whose module is fetched by `source` + `checksum` through the
  artifact stager into the node content store
  (`crates/worker/src/wasm/module.rs:5-45`; `docs/network-plugins.md:297-315`).
- **No distribution exists for native or exec plugins**: no fetch, no
  signature, no `legion plugin` command.

**Artifacts and the content store**
- Built-in fetchers: `https`, `file`, `git`, `s3`, and `oci`.
- `checksum = "sha256:<hex>"` is verified while streaming
  (`crates/common/src/structs/artifact.rs:155-158`;
  `crates/worker/src/artifacts/stage.rs:1231-1250`).
- `resolve = "pin"` pins tags and refs at submit (`docs/artifacts.md:450-481`).
- The node content store is content-addressed with per-class budgets, pins,
  LRU, and a crash-safe index (`crates/common/src/content_store.rs:1-60`).
- The object store client is S3-compatible with a custom endpoint, and the docs
  say R2 works (`crates/worker/src/objects/aws.rs:288-430`;
  `docs/artifacts.md:229`).
- **Fits:** plugins fetched onto a node land in a new `plugins` content class,
  pinned while installed.

**Cluster-wide records pushed to workers**
- Runtime-disable records are sent "on every heartbeat reply, and push[ed] on
  the work stream (`StreamWork`) the moment the set changes", so every node
  learns "within one heartbeat interval" (`docs/runtime-disable.md:219-226`).
- **Fits:** plugin installs reach workers the same way.

**CLI state**
- No config file.
- Tokens live in `~/.legion/tokens/<host_port>` (`crates/common/src/cli_paths.rs:20-74`).
- Everything else comes from environment variables (`src/cmd/helpers.rs:124-256`).
- **Fits:** registry *sign-in* tokens follow the same pattern
  (`~/.legion/registry-tokens/<host>`). Registry *lists* do not live on the
  laptop at all (§6.7).

**UI**
- SolidJS, Vite, and Tailwind, with tokens as CSS variables
  (`ui/src/index.css:16-40`) and primitives in `ui/src/components/ui.tsx`.
- Served separately from the binary (`docs/ui-architecture.md`).

**Versioning helpers**
- `semver 1.0` is already a dependency (`Cargo.toml:206`), used for
  `version_matches` constraints (`crates/common/src/constraints.rs:503`).
- `ring` (Ed25519), `sha2`, `tar`, `flate2`, `object_store`, and `reqwest` are
  all workspace dependencies.
- No Postgres client is in the tree.

**Agent surfaces**
- `catalog()` in `src/cmd/capabilities.rs:40`, hand-written MCP tools in
  `src/cmd/mcp.rs`, and one-to-one RPC tools in `src/cmd/mcp_tools.rs:122-160`.
- A test requires every user-facing RPC to have a tool or an exclusion
  (`mcp_tools.rs:451`).

## 6. Proposed design

### 6.1 The concepts on one screen

```text
registry ──has──▶ package ──has──▶ version ──pinned by──▶ digest (sha256 of the version document)
 (git | http)     (one kind,        (semver,                 └─ lists every file: source archive,
                   owner/name)       immutable)                  per-platform plugin builds

reference:   [registry:]package[@version-or-range]       e.g.  legion/postgres@^16
install:     the kind's own verb resolves a reference to one version + digest, fetches it,
             verifies it, and records provenance { registry, package, version, digest }
```

- A **package** has one kind for life. On a hosted registry its name is
  `owner/name`. On a git registry the repository is the owner, so the name is
  just `name`.
- A **version** is immutable. Its **version document** is a canonical JSON
  file listing the manifest and every file's sha256. The document's own
  sha256 is the version's **digest**, so one 64-hex string pins every byte for
  every platform.
- The **cluster's registry list** is where cluster installs resolve, and it is
  also the allowlist (§6.7).

### 6.2 The four kinds

| Kind | What's in the package | Evaluated / runs where | Installed with | Becomes |
| --- | --- | --- | --- | --- |
| `job` | A jobspec directory (`job.ncl` + local imports), optional declared variables | Client-side, like `job run <file>` | `legion job run <ref>`, or `legion registry pull` to own a copy | A job whose config carries package provenance |
| `template` | One self-contained template `.ncl` (no imports, as today) | On the manager, sandboxed, as today | `legion template install <ref>` | A cluster template revision with provenance |
| `app` | An app manifest (`app.ncl`) + README; code ships as the artifacts or images it names | The manager's app reconciler, as today | `legion app install <ref>` | An app record with `source = registry` |
| `plugin` | Per-platform builds of one plugin of one type, plus its `plugin.json` | Inside a worker or manager process (native), as a child process (exec), or in a WASM VM | `legion plugin install <ref>`, or `plugins.install` in node config | Files in the node's managed plugin directory, loaded by the existing loaders |

**Why job and template are separate kinds** (calls #5):
- A **job** package is code you run from your machine and may want to own.
  It can span files, it's evaluated client-side, and `pull` copies it to edit.
- A **template** is a cluster-resident object behind a guided UI form, and ACL
  delegation can say "this account may only run template X". It must be one
  file, because the manager renders it.
- The same Postgres could be published as either, and the site links the two
  when a publisher ships both.

### 6.3 Versioning scheme

- **SemVer 2.0.0 for every package.** Build metadata (`+…`) is refused at
  publish, so no two versions share a precedence.
- **Immutable.** `(package, version)` names one digest forever, on every
  registry type.
  - The hosted registry has no update path for a version row.
  - A git registry is checked the other way round. Clusters record the digest
    they installed, and if a tag later yields different bytes, re-fetching
    fails:

  ```text
  error: acme:redis@1.2.0 changed: this cluster installed sha256:4be1…, the tag now gives sha256:91c0…
  hint:  versions are immutable; ask the publisher to release 1.2.1 instead of moving the tag
  ```
- **Yank, deprecate, takedown** (calls #17):

  | Action | Who | Effect |
  | --- | --- | --- |
  | **Yank** a version | Owner | Gone from default resolution, from ranges, and from "latest". Still fetchable by exact version, so running clusters and pinned configs keep working. Has a reason, shown everywhere. Reversible. |
  | **Deprecate** a package | Owner | Still resolves. The CLI warns and names the successor (`use legion/postgres-ha`). |
  | **Take down** a version | Registry admin | Bytes deleted. Fetches return `410` with the reason, and the version number can never be reused. For malware and legal requests only. |

- **Ranges.** References use npm-style shorthand, and manifests use Cargo
  requirement syntax:

  | You write | Means |
  | --- | --- |
  | `legion/postgres` | The newest stable, non-yanked version compatible with this cluster |
  | `legion/postgres@16.4.0` | Exactly 16.4.0 (even if yanked, with a warning) |
  | `legion/postgres@16` / `@16.4` | Newest `16.x.y` / `16.4.y` |
  | `legion/postgres@^16.2` / `@~16.4.1` / `@">=16, <18"` | Cargo semantics |
  | `legion/postgres@latest` | Newest stable, ignoring the cluster-compatibility filter |

  A range never selects a prerelease (`17.0.0-rc.1`) unless the range itself
  names one (Cargo's rule).
- **Compatibility is required.**
  - Every manifest declares `legion = ">=0.9"`, filled in by `legion package
    init` from the CLI's own version.
  - Default resolution skips versions whose requirement rejects the cluster's
    version, which the manager reports. Client-only commands use the CLI's
    version instead.
  - A skip is never silent: `info` shows `17.0.0 needs legion >=0.11 (this
    cluster: 0.10.2)`.
  - Native plugin builds compute their compatibility from their build
    fingerprint instead of declaring it (§6.9.1).
- **What a major version means, per kind.** The registry checks these at
  publish against the previous version on the same major line, and blocks a
  breaking change published as a minor or patch unless the author passes
  `--accept-breaking "<reason>"`, which is shown on the version page (calls #12):

  | Kind | Breaking (needs a major) |
  | --- | --- |
  | job, template | A variable removed or renamed, a new required variable, a variable type narrowed, `select` options removed, the rendered job name changed |
  | app | A config key removed, the config schema narrowed, an object kind or jobspec key removed, `placement` widened (workers → managers) |
  | plugin | `type` changed, the plugin protocol or API version changed, config schema narrowed, a platform dropped |

  New **permissions** in an app are not "breaking", but they always require
  fresh consent on upgrade (as today), and they are highlighted in the version
  diff.
- **`upstream`** is optional, display-only text (`"redis 7.4.1"`). It's shown
  on the site and never used for resolution. This is Helm's `appVersion`
  without making it a second version people bump.
- **Inside the cluster, existing counters stay.**
  - Template revisions stay integers and job configs keep their versions.
    Provenance links each one to the package version that produced it
    (calls #8).
  - **App `version` becomes required semver** for every non-bundled app,
    whatever the source (breaking; §10).
  - **`plugin.json` gains `version`**, checked against the package version.

### 6.4 The package manifest: `legion-package.ncl`

One file at the package root, in Nickel or JSON like every other config
surface. Common fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | `[a-z0-9][a-z0-9-]{0,62}`. On a hosted registry, the owner prefix comes from the publishing account. |
| `version` | yes | SemVer 2.0.0, no build metadata |
| `kind` | yes | `job`, `template`, `app`, or `plugin` |
| `description` | yes | One sentence, ≤ 200 characters. It's the search snippet. |
| `legion` | yes | Requirement on Legion versions, e.g. `">=0.9"` |
| `license` | yes on hosted | SPDX expression |
| `readme`, `changelog` | no | Default `README.md`, `CHANGELOG.md`. The changelog section headed with this version is shown on the version page. |
| `repository`, `homepage`, `documentation` | no | URLs |
| `keywords` | no | ≤ 10 free tags |
| `categories` | no | From the registry's fixed list (`databases`, `observability`, `networking`, `security`, `storage`, `messaging`, `ci-cd`, `ai-ml`, `gpu`, `web`, `dev-tools`) |
| `upstream` | no | Display-only text |
| `icon`, `screenshots` | no | Paths in the package (square PNG/SVG ≤ 256 KiB; PNG/JPEG ≤ 1 MiB each) |
| `include`, `exclude` | no | Globs. Default: everything except `.git/` and `.legionignore` entries. |
| `requires` | no | `{ plugins = ["legion/firecracker-lite@^0.3"], apps = ["legion/ingress"], runtimes = ["sandbox"] }`. Checked at install and reported, never auto-installed. |
| `entry` | per kind | `job.ncl`, `template.ncl`, `app.ncl` by default |
| `variables` | `job` only | Same schema as template variables. Templates declare theirs in the template, as today, and the packer reads them from its export. |
| `plugin` | `plugin` only | `{ type, builds, config }`, see §6.9.1 |

A job package:

```nickel
# packages/redis/legion-package.ncl
{
  name = "redis",
  version = "7.4.1",
  kind = "job",
  description = "Redis 7: a single cache node, or a primary with replicas behind a service.",
  legion = ">=0.9",
  license = "Apache-2.0",
  upstream = "redis 7.4.1",
  repository = "https://github.com/acme/legion-packages",
  keywords = ["cache", "key-value"],
  categories = ["databases"],
  variables = [
    { key = "replicas", type = "number", "default" = 0, label = "Read replicas" },
    { key = "memory", type = "number", "default" = 512, label = "Memory (MiB)" },
    { key = "persistence", type = "select", options = ["none", "aof", "rdb"], "default" = "rdb" },
  ],
}
```

A template package is the same with `kind = "template"` and no `variables`
(they come from `template.ncl`). An app package is the same with `kind =
"app"`; the packer refuses a package whose `app.ncl` declares a different
`name` or `version` (`legion package init --from app.ncl` writes a matching
one). Plugins are in §6.9.1.

**Archive format.** `legion package pack` writes a deterministic
`<name>-<version>.tar.gz`:
- entries sorted, mtime 0, uid and gid 0, modes normalized to 0644 or 0755;
- no symlinks, devices, or paths that escape the root;
- source archive ≤ 10 MiB, each plugin build file ≤ 256 MiB.

Packing the same tree twice gives the same bytes, which is what lets a git
registry and the hosted registry agree on a digest (§6.6.1). The existing
`tar` and `flate2` crates do the work.

### 6.5 References and resolution

```text
reference  := [ registry ":" ] package [ "@" version-or-range ]
registry   := alias                     # a name in the cluster's registry list:  acme
            | location                  # contains "." or "/":  github.com/acme/legion-packages
package    := owner "/" name            # hosted registries:  legion/postgres
            | name                      # git registries:     redis
```

| Reference | Resolves against |
| --- | --- |
| `legion/postgres` | The cluster's default registry (`legion` unless changed) |
| `acme:redis@1.2` | The registry the cluster calls `acme` |
| `corp:platform/api-gateway@^3` | A self-hosted registry with owners |
| `github.com/acme/legion-packages:redis@1.2` | That git repository directly. Works with no alias for client-side use (`job run`, `pull`, `info`); cluster installs accept it only if the location is in the cluster's list (§6.7). |
| `pkg:legion/read-only-free-tier@1.2` | The `pkg:` form, used where a field also accepts URLs (§6.9.4) |

**Resolution:**
1. Fetch the package's index (§6.6.2).
2. Drop yanked versions, unless the reference names an exact version.
3. Drop versions whose `legion` requirement rejects the cluster.
4. Pick the highest version that satisfies the range.
5. Fetch the version document, check that its sha256 equals the digest the
   index advertised, and fetch the files it lists, verifying each.

A file that fails verification is never written outside the content store's
temporary directory.

**Exact installs for scripts and agents.** Every install verb takes
`--dry-run`, which resolves and returns `{package, version, digest}` along with
the usual preview. It also takes `--digest sha256:…`, which refuses anything
else:

```console
$ legion template install legion/postgres --dry-run --json | jq -r .data.digest
sha256:3f9a0c…
$ legion template install legion/postgres --digest sha256:3f9a0c…
```

**No lock file for cluster installs.** The cluster's provenance records *are*
the lock. For configs and GitOps repos, `legion registry pin` prints the
digest-pinned forms:

```console
$ legion registry pin legion/nvidia-device@1.4
{ package = "legion/nvidia-device@1.4.2", digest = "sha256:5b1e0d…" }   # node config (§6.9.3)
pkg:legion/nvidia-device@1.4.2  checksum = "sha256:5b1e0d…"                # artifact fields (§6.9.4)
```

### 6.6 Registries: a git repository, or the HTTP protocol

#### 6.6.1 Git registries (the simplest form)

Any git repository with this layout is a registry:

```text
legion-packages/                    # any git host Legion's git fetcher can reach
  legion-registry.ncl               # optional: { description, homepage }
  packages/
    redis/
      legion-package.ncl
      job.ncl
      lib/sentinel.ncl
      README.md
    postgres/
      legion-package.ncl
      template.ncl
      README.md
```

- **Versions are tags**, named `<package>/v<version>` (`redis/v7.4.1`). This is
  Go's convention for many modules in one repository (calls #15).
  - A repository holding one package keeps `legion-package.ncl` at the root
    and uses plain `v7.4.1` tags.
  - The tag's version must equal the manifest's `version`.
  - Listing versions is one `git ls-remote --tags`. Fetching one is a shallow
    fetch of that tag.
- **An untagged branch is installable but marked.** A branch or commit
  reference (`acme:redis@main`, `@4f1c2e9`) is resolved to a commit and
  recorded, and the CLI warns `unversioned: pinned to commit 4f1c2e9`. It's
  for development, not for configs.
- **The digest** is that of a version document built by the same
  deterministic packer from the tag's tree. The hosted registry ingesting the
  same repository computes the same digest.
- **Plugins in a git registry** can't sensibly keep binaries in git. Their
  `builds` entries name a URL and a sha256 instead of a file (Krew's shape,
  §6.9.1). The hosted registry copies those bytes in at ingest, so its own
  copies never rot.
- **Search** over a git registry is a shallow clone of the default branch,
  reading every manifest. It is fine for the tens to hundreds of packages a
  team or company keeps, and cached for 5 minutes.
- **Auth for private repositories** works as for GitOps: an HTTPS token
  from KV, sent as an `http.extraHeader`, never in argv
  (`docs/gitops.md:241-255`; `src/apps/gitops/git.rs:211-240`).

#### 6.6.2 The HTTP read protocol (v1)

The hosted registry, and anything else that wants to be one, serves these
documents. Every read is a GET of a document, so the protocol can be served
from a bucket or static host. The hosted registry generates them
dynamically.

| Path | What | Cache |
| --- | --- | --- |
| `/.well-known/legion-registry.json` | `{ "protocol": 1, "name": "Legion Registry", "api": "https://registry.legion.dev/v1/", "owners": true, "search": true }` | 1 h |
| `/v1/index/<owner>/<name>` | Every version: `version`, `digest`, `legion`, `yanked`, `platforms`, `published` + package `kind`, `deprecated` | 60 s, ETag |
| `/v1/versions/<owner>/<name>/<version>` | The version document. Its sha256 is the digest. | Immutable |
| `/v1/blobs/sha256/<hex>` | A file. Counts a download, then `302` to the CDN copy. | Immutable |
| `/v1/search?q=…` | Optional; advertised by `"search": true`. See §6.16. | 30 s |

A version document (canonical JSON: sorted keys, no insignificant
whitespace):

```json
{
  "schema": 1,
  "package": "legion/nvidia-device",
  "version": "1.4.2",
  "kind": "plugin",
  "manifest": { "description": "…", "legion": ">=0.9", "license": "Apache-2.0", "plugin": { "type": "devices" } },
  "files": [
    { "role": "source", "digest": "sha256:aa01…", "size": 18234, "mediaType": "application/vnd.legion.package.v1.tar+gzip" },
    { "role": "build", "platform": "linux/amd64", "digest": "sha256:bb02…", "size": 4120345 },
    { "role": "build", "platform": "linux/arm64", "digest": "sha256:cc03…", "size": 3987112 }
  ],
  "capabilities": { "runsOn": "workers", "mechanism": "exec", "hostAccess": true },
  "provenance": { "repository": "https://github.com/legion-packages/official", "commit": "4f1c2e9…", "workflow": "publish.yml" }
}
```

`capabilities` is the registry's computed summary of what the version does to
a cluster (§6.12). It's in the signed, digested document, so a cluster can
show it at install time without trusting the website.

### 6.7 Registries are cluster configuration

**The decision.** The list of registries lives in the cluster, in raft, as a
small set of records, not on each laptop (calls #1). Consequences:

- Every teammate, CI job, the UI, and an agent resolve `legion/postgres`
  identically. There is no `repo add` before an install command works, and no
  stale local cache.
- **The list is the allowlist.** Cluster installs resolve only against
  configured registries. Removing the default `legion` registry locks a
  cluster to its own sources.
- The managers do the resolving and fetching for cluster installs (calls #2),
  so the policy is enforced where it can't be bypassed, and nodes never need
  internet access.

```console
$ legion registry list
ALIAS   TYPE   LOCATION                                  DEFAULT  AUTH
legion  http   https://registry.legion.dev               yes      -
acme    git    https://github.com/acme/legion-packages   no       kv:registries/acme

$ legion registry add acme https://github.com/acme/legion-packages --token-kv registries/acme
added registry acme (git, 4 packages)

$ legion registry add corp https://registry.corp.internal --default
added registry corp (http, protocol 1); unqualified references now resolve against corp

$ legion registry remove legion
removed registry legion; packages already installed from it keep running and keep their provenance
```

- **New clusters are seeded** with `legion` → the hosted registry as the
  default. The manager config can seed a different list at bootstrap, like
  `apps.<name>` seeds bundled apps (`crates/common/src/structs/config.rs:172-190`),
  so an air-gapped cluster never contacts the public registry:

  ```nickel
  # manager.ncl
  registries = {
    default = "corp",
    list = {
      corp = { location = "https://registry.corp.internal" },
      infra = { location = "https://git.corp.internal/platform/legion-packages", tokenKv = "registries/infra" },
    },
  },
  ```

  Once bootstrapped, the CLI and RPCs own the list. The config only seeds it.
- **Client-side commands** (`job run`, `pull`, `search`, `info`) read the list
  from the manager (`ListRegistries`) and talk to the registries directly.
  With no reachable cluster, the CLI uses the built-in default plus any
  location reference.

### 6.8 Installing each kind

#### 6.8.1 Jobs

```console
$ legion job run legion/redis --var replicas=2 --wait
resolved legion/redis 7.4.1 (sha256:1d7e…) from legion
job redis submitted (config redis-1), rollout complete: 3/3 running

$ legion job info redis
…
package:  legion/redis 7.4.1 (sha256:1d7e…) from legion
```

- **How an argument is read.** An argument that exists on disk is a file.
  Anything containing `://` or starting `git::` is a URL. Otherwise it's a
  reference. When nothing matches, the error names both lookups: `no file
  'legion/redis' and no package 'legion/redis' in registry legion`.
- **Evaluation.** The package is evaluated with `eval_file_confined` rooted
  at the package (`config_lang.rs:543-642`), so imports can't leave it. Its
  declared `variables` validate `--var` the way template variables do: unknown
  keys are refused, and strings are coerced. The sources are stored with the
  job as `Submission`s, as for any file.
- **Owning a copy** is first class:

  ```console
  $ legion registry pull legion/redis ./redis
  wrote 4 files to ./redis (legion/redis 7.4.1); .legion-package.json records where they came from
  ```

  `legion registry diff ./redis` later shows how your copy differs from the
  version you pulled, and from the newest version.

#### 6.8.2 Templates

```console
$ legion template install legion/postgres
resolved legion/postgres 16.4.0 (sha256:3f9a…) from legion
template postgres v1 created (variables: name, size_gb, version, replicas)
run it:  legion template run postgres --var name=orders   ·   or from Templates in the UI

$ legion template install legion/postgres --as pg-small --version '^16'
```

- A template is installed under the package's name unless `--as` names
  another.
- Installing over an existing template that came from a different package (or
  from a file) is refused unless `--replace` is given.
- Installing again appends a revision (`v2`), exactly like `template create`,
  with `source = { registry, package, version, digest }`.
- `template get` shows the provenance, and the UI badge reads `from
  legion/postgres 16.4.0`.

#### 6.8.3 Apps

```console
$ legion app install legion/deployer
resolved legion/deployer 0.3.1 (sha256:77ab…) from legion
deployer 0.3.1 runs its own code (1 image: ghcr.io/acme/deployer@sha256:…) on your workers,
as its own unprivileged user, in legion-system, and asks for:
  job       read, write   namespaces: team-*
  kv        read          paths: deployer/*
Your token holds all of these. Grant them and install? [y/N]
```

This is the existing consent flow (`install_prompt`, `app.rs:1882-1949`) with
the package line added.
- `AppSource` gains `Registry`, and the record's `source_ref` is the resolved
  reference plus digest.
- `legion app upgrade deployer` with no file picks the newest compatible
  version **within the installed major**. `--to 2.0.0` crosses a major or goes
  back.
- **`legion app rollback deployer`** upgrades to the previous recorded version.
  The record keeps `previous_source_ref`, a reference rather than a copy of the
  previous manifest (the apps calls doc rejected the copy, entry 115). The
  bytes are still in the registry, because versions are immutable. This closes
  the `app rollback` CLI gap (`TODO.md:1115`).

#### 6.8.4 Plugins

See §6.9; plugins are the kind with the most new machinery.

### 6.9 Plugins on every node

#### 6.9.1 Plugin packages

```nickel
{
  name = "nvidia-device",
  version = "1.4.2",
  kind = "plugin",
  description = "Discovers NVIDIA GPUs and their health; allocates them to tasks.",
  legion = ">=0.9",
  license = "Apache-2.0",
  plugin = {
    type = "devices",        # tasks | scheduling | rollouts | node_checks | artifacts | volumes | devices
                             # | keyring | cluster_ca | net | scaling
    builds = [
      { platform = "linux/amd64", file = "dist/linux-amd64/legion-device-nvidia" },
      { platform = "linux/arm64", file = "dist/linux-arm64/legion-device-nvidia" },
      # In a git registry, a build is a URL plus a hash instead of a file:
      # { platform = "linux/amd64", url = "https://github.com/acme/x/releases/download/v1.4.2/x-linux-amd64", sha256 = "…" },
    ],
    config = {               # optional: JSON Schema for the plugin's config, like an app's
      schema = { type = "object", properties = { pollInterval = { type = "string" } } },
      default = { pollInterval = "30s" },
    },
  },
}
```

The `type` fixes the **mechanism** and **where it runs**. The site shows all
three, because "native code in every worker" and "a WASM filter" deserve
different levels of trust:

| Mechanism | Types | Runs in | Build |
| --- | --- | --- | --- |
| native (dylib) | `tasks`, `scheduling`, `rollouts`, `node_checks`, `artifacts` | The worker or manager process | Per platform **and per Legion build** |
| exec | `volumes`, `devices`, `keyring`, `cluster_ca`, `scaling` | A child process per call or session | Per platform |
| wasm | `net` | A fuel-bounded VM in `netd` | One build for every platform |

**Native builds are bound to a Legion build** (calls #7):
- The packer reads each library's `legion_plugin_abi_build_info` and
  fingerprint (`plugin_runtime.rs:87-130`). It records them as
  `{ platform, legionBuild = "0.10.2", fingerprint = "…" }`, so a native build
  is selected by the node's exact fingerprint.
- When a node has no match, the error is specific:

  ```text
  error: legion/firecracker-lite 0.3.0 has no native build for this node (legion 0.10.2, linux/arm64)
  hint:  builds exist for legion 0.10.1 (linux/amd64, linux/arm64) and 0.10.2 (linux/amd64);
         native plugins are rebuilt for every Legion release, so ask the publisher for one
  ```

- The site shows a build matrix, and the default filters prefer exec and wasm
  plugins.
- Official native plugins are rebuilt by Legion's own release CI for every
  release.

#### 6.9.2 Cluster install: the managers hand a plugin to every matching node

The surface:

```console
$ legion plugin install legion/nvidia-device --pool gpu --config pollInterval=15s
resolved legion/nvidia-device 1.4.2 (sha256:5b1e…) from legion
type devices (exec, runs on workers), builds: linux/amd64, linux/arm64
target: pool gpu (40 nodes: 36 linux/amd64, 4 linux/arm64)
install legion/nvidia-device on 40 nodes? [y/N] y
installed; nodes are fetching it from the managers. Follow it: legion plugin status nvidia-device

$ legion plugin status nvidia-device
nvidia-device 1.4.2 (legion/nvidia-device, sha256:5b1e…)  pool gpu  38/40 active
NODE        PLATFORM      STATE     DETAIL
gpu-01      linux/amd64   active    since 14:02:11
…
gpu-17      linux/amd64   failed    plugin check: libnvidia-ml.so.1 not found
gpu-33      linux/arm64   failed    plugin check: libnvidia-ml.so.1 not found

$ legion plugin list
NAME            TYPE      VERSION   SOURCE                     TARGET     NODES
nvidia-device   devices   1.4.2     legion/nvidia-device       pool gpu   38/40 active
zfs             volumes   0.3.0     node config (12 nodes)     -          12/12 active
```

Targets: `--pool <name>` (repeatable), `--node <id>` (repeatable), `--all`, or
`--managers` for the types that run there. Installing to one node first and
then widening the target is the manual canary.

**How it works:**

1. **Record.** `InstallPlugin` checks the caller's ACL (`operator:write`) and
   resolves the reference against the cluster's registries. It checks
   `requires`, checks for builds for every platform and native fingerprint in
   the target, and checks for name collisions: two packages of the same type
   with the same name are refused, since the name is what jobs reference
   (`runtime = "firecracker-lite"`). It then writes one `PluginInstall` record
   on the Control lane: package, version, digest, type, target, config,
   installer.
2. **Fetch once per manager.** Each manager fetches the version document and
   the builds its nodes need into a manager-side content store (the same
   `ContentStore` the worker uses, `crates/common/src/content_store.rs`, class
   `plugins`), verifying every digest. Concurrent requests for a digest are
   single-flighted.
3. **Tell the nodes.** Managers send the set of plugin installs on every
   heartbeat reply and push it on `StreamWork` the moment it changes. This is
   the runtime-disable mechanism (`docs/runtime-disable.md:219-226`), so every
   node learns within one heartbeat interval.
4. **Each node fetches from a manager.** A matching worker asks any manager
   for its build by digest over a new worker-plane RPC, `FetchPluginFile`,
   which streams bytes. On a cache miss the manager fetches through. The
   worker verifies the sha256 itself, so a manager can't hand it anything
   else. Bytes land in the node content store's new `plugins` class, pinned
   while installed.
5. **Materialize and pre-flight.** Each version is unpacked into its own
   immutable directory under the worker's managed plugin root,
   `<data dir>/plugins/<type>/<name>/<version>/`, in the layout the loaders
   already read (`plugin_runtime.rs:276-315`). The node then runs the cheap
   checks the loaders already have before activating anything:
   - `plugin.json` and the build fingerprint for native builds;
   - `legionVolumeApiVersion` for volumes;
   - the `legion-device/1` handshake for devices.
6. **Activate**, by mechanism:

   | Mechanism | New plugin | Upgrade |
   | --- | --- | --- |
   | exec | On the next call (devices: the next 30 s node-check pass) | Same: each call execs the new directory |
   | native | On next use where the kind loads lazily (`artifacts`); otherwise `restart-required` | `restart-required`: a Rust dylib can't be unloaded safely, so the old one keeps serving until the worker restarts |
   | wasm (`net`) | Not through this path; net plugins stay manager objects, with `pkg:` sources (§6.9.4) | — |

7. **Report.** Workers report `{name, version, digest, state, detail}` per
   plugin in their heartbeat. The states are `pending`, `fetching`,
   `verifying`, `active`, `restart-required`, `failed`, and `conflict`.
   `legion plugin status` and the node page show them. A node only
   advertises a plugin once it is `active`, so the scheduler never places a
   task on a runtime that isn't there yet.

**Edges:**
- **Conflict with a file on disk.** If a plugin of the same type and name is
  already on disk under the operator's plugin root (`LEGION_PLUGIN_DIR`), the
  node reports `conflict` and leaves both alone. Hand-placed files are never
  overwritten.
- **Uninstall.** `legion plugin uninstall nvidia-device` deactivates the
  plugin: immediately for exec, on restart for native. Pins are released and
  the content store's LRU reclaims the bytes. Uninstalling a task-runtime
  plugin while allocations use it is refused without `--force`, and the
  refusal names the jobs.
- **Upgrade.** `legion plugin upgrade nvidia-device [--to 1.5.0]` replaces the
  record. Nodes fetch the new build beside the old one and switch per the
  activation table, and a rollback is an upgrade `--to` the old version.
- **Plugins that run on managers** (`scheduling`, `rollouts`, `--managers`)
  follow the same record. Each manager fetches for itself.
  - `keyring` and `cluster_ca` are needed before raft starts, so they can
    only be declared in node config (§6.9.3). A cluster install of those types
    is refused with that hint.
- **Air-gapped clusters** point the managers at a mirror registry (§6.7).
  Uploading a package file straight to the managers (`legion plugin install
  ./nvidia-device-1.4.2.tar.gz`), with manager-to-manager copy, is a later
  phase (8b).

#### 6.9.3 Declarative plugins in node config, hash-pinned

For machine images, bootstrap, and anything that must be on a node before it
takes work. The same block exists in the worker and manager configs:

```nickel
# worker.ncl
plugins = {
  install = [
    # From a registry: one digest pins the version document, which pins every platform's build.
    { package = "legion/nvidia-device@1.4.2", digest = "sha256:5b1e0d…", config = { pollInterval = "15s" } },

    # From a URL: one file, one checksum. `type` and `name` are needed because nothing else says them.
    { type = "volumes", name = "zfs",
      source = "https://github.com/acme/legion-zfs/releases/download/v0.3.0/legion-zfs-linux-amd64.tar.gz",
      checksum = "sha256:9c0d4e…" },
  ],
},
```

The rules:
- **A hash is mandatory.** A `package` entry needs `digest` and an exact
  version; a `source` entry needs `checksum`. The config is refused at load
  without one: `plugins.install[1]: a checksum is required; run legion
  registry pin to get one`. Nodes never trust on first use (calls #6).
- **The daemon fetches at start, before any plugin loads**, verifies, and
  materializes as in §6.9.2 steps 4–5.
  - A `source` is fetched directly with the artifact fetchers (`https`, `s3`,
    `oci`, `git` are all available).
  - A `package` entry fetches the version document by digest from wherever it
    can: the managers' cache if the node is joined, else the registry the
    reference names.

  Because the digest pins the bytes, it doesn't matter which source answered.
  That is also how air-gapped nodes work: a manager, or a mirror, answers.
- **A failed fetch doesn't stop the daemon.** The node starts without that
  plugin, doesn't advertise it, reports a failing `plugins` node check, and
  retries with backoff. A mismatched hash is a hard failure for that entry and
  is never retried with the same bytes:

  ```text
  plugin zfs: checksum mismatch: config pins sha256:9c0d4e…, https://github.com/…/legion-zfs-linux-amd64.tar.gz served sha256:13af77…
  ```
- **Node config wins over a cluster install** of the same plugin on that node.
  The status shows `pinned by node config (0.3.0); cluster install wants
  0.4.0`, and `legion plugin list` counts those nodes under "node config"
  (calls #6).
- **The manager config** uses the same block for `scheduling`, `rollouts`,
  `keyring`, and `cluster_ca`. For the last two it's the only way, and the
  fetch happens before raft starts.

#### 6.9.4 Package references in artifact fields

The URL-plus-checksum surfaces from §1 (net-plugin modules, autoscaler
strategies, isolate modules, job `artifacts`) accept `pkg:<reference>` as a
`source`. At submit it is resolved and pinned to the build's blob URL and
sha256, exactly as `resolve = "pin"` already pins OCI tags and git refs
(`docs/artifacts.md:450-481`):

```nickel
# configs/netplugins/read-only-free-tier.ncl
module = { source = "pkg:legion/read-only-free-tier@1.2" },   # stored as https://…/blobs/sha256/…, checksum sha256:…
```

The package must have exactly one file for the platform being asked for, else
the submit fails and lists the platforms that exist. GitOps repos can use the
same `pkg:` sources, and `legion registry pin` prints the pinned form for
repos that want the pin written down.

### 6.10 Updates, provenance, and rollback

**Provenance** is one shape stored on every installed object:
- job configs (beside the existing template provenance),
- template revisions,
- app records,
- plugin installs.

```text
PackageProvenance { registry: "legion", location: "https://registry.legion.dev",
                    package: "legion/postgres", version: "16.4.0", digest: "sha256:3f9a…",
                    installed_by: "accessor:4c1d…", installed_at: … }
```

`legion registry outdated` finds what has a newer version, across kinds. It
reads provenance from the cluster and asks each registry for its index:

```console
$ legion registry outdated
KIND      NAME            INSTALLED              LATEST COMPATIBLE   LATEST     NOTE
template  postgres        legion/postgres 16.4.0  16.6.0             17.1.0     17.x needs legion >=0.11
app       deployer        legion/deployer 0.3.1   0.3.4              1.0.0      1.0.0 asks for 1 new permission
plugin    nvidia-device   legion/nvidia-device 1.4.2  1.4.2          1.4.2      -
job       redis           legion/redis 7.4.1      7.4.1              7.4.1      7.4.1 was yanked: "AOF corruption on restart"
```

**Upgrades keep settings and show the change first:**
- **Template upgrade** appends a revision. The next launch from the UI or
  `template run` starts from the job's recorded values; Review already shows
  "values changed since the last launch" and the rendered diff
  (`docs/job-templates.md`, In the UI).
  - A new required variable with no recorded value is refused, naming the
    variable.
  - `legion template upgrade postgres --relaunch` re-runs every job whose
    latest config came from the template, with recorded values, through
    `PlanJob` first. Only after confirmation.
- **App upgrade:** the existing permission diff and consent (`src/cmd/app.rs:1111-1175`).
- **Job package:** `legion job run legion/redis` again, or `legion job upgrade
  redis`, which resolves the newest compatible version and re-runs the job
  with the variables recorded on its current config. That is Helm's
  `--reuse-values` without the reset-or-reuse question.
- **Plugin:** `legion plugin upgrade`.

**Rollback is "upgrade to the previous version"**, available for every kind
because versions are immutable and the provenance remembers the previous
reference.

### 6.11 Publishing and contributing

Three ways in, from simplest:

#### 6.11.1 From GitHub, no CLI: link a repository

On the site, sign in with GitHub and choose **Publish → Link a repository**.
The registry installs its GitHub App on the chosen repository, reads its
layout (§6.6.1), and shows each detected package with its validation result.
Then:

- **Each pushed version tag publishes that version.** The tag webhook queues
  an ingest job; the job validates the package, computes the digest, and
  copies the files into R2.
- **The version page records the source:** "built from acme/infra@4f1c2e9 by
  tag `redis-sentinel/v1.1.0`".
- **Failed validation is reported in two places:** a commit status on the tag
  and an email, with the same report `legion package validate` prints.

#### 6.11.2 From CI: trusted publishing

```yaml
# .github/workflows/publish.yml
permissions: { id-token: write, contents: read }
steps:
  - uses: actions/checkout@v4
  - run: legion package publish packages/redis   # no token: exchanges the job's OIDC token
```

- On the package's settings page, the owner names the repository, workflow,
  and (optionally) environment allowed to publish.
- The CLI sees `ACTIONS_ID_TOKEN_REQUEST_URL` and exchanges the OIDC token at
  `POST /v1/auth/oidc/github` for a 15-minute token scoped to that package.
  This is the PyPI, npm, and crates.io model.

#### 6.11.3 From a laptop: the CLI

```console
$ legion package init
detected a jobspec (job.ncl) reading vars: replicas, memory, persistence
name [redis-sentinel]:
kind [job]:
description: Redis with Sentinel failover across three nodes.
license [Apache-2.0]:
wrote legion-package.ncl (legion ">=0.10" from this CLI). Next: legion package validate

$ legion package validate
✓ manifest        redis-sentinel 1.0.0 (job)
✓ evaluates       with defaults; 2 tasks, 3 instances
✓ variables       3 declared, all used, 0 undeclared vars read
✓ capabilities    container runtime, 1 volume, no host access
! readme          README.md has no usage example (shown first on the package page)
✓ size            14 KiB

$ legion registry login          # GitHub device flow; token saved to ~/.legion/registry-tokens/registry.legion.dev
$ legion package publish
published acme/redis-sentinel 1.0.0 (sha256:0be3…)  https://registry.legion.dev/acme/redis-sentinel
```

Authoring verbs: `legion package init|validate|pack|publish|yank|unyank|deprecate|owner`.
`validate` is the exact check the registry runs: the registry calls `legion
package inspect` in a sandboxed subprocess (calls #11), so the two can't drift.

**Owners and names:**
- Owners are GitHub users or organizations, verified at sign-in. Organization
  membership is checked through the GitHub API (calls #13).
- `legion/` is reserved for official packages.
- Names are first come, first served. Admins can reserve names, and a
  typosquat check at first publish flags names within edit distance 1 of a
  popular package for review before they appear in search.
- Packages can have several owners (`legion package owner add acme/redis
  github:alice`) and be transferred.

### 6.12 Trust and integrity

**Layers, from always-on to opt-in:**

| Layer | Protects against | Phase |
| --- | --- | --- |
| TLS to the registry, git, and managers | Network tampering | 1 |
| A digest on every version, verified on every fetch, at every hop (manager *and* worker) | A tampered bucket or CDN, a manager serving the wrong bytes, a moved git tag | 1 |
| Immutable versions; yank ≠ delete | The bytes under a pinned version changing | 1–2 |
| **Capabilities summary** computed at publish and shown at install | Installing something more powerful than you thought | 2 |
| Trusted publishing provenance (repo, commit, workflow) on the version page | Not knowing which source built the bytes | 3 |
| Registry-signed version documents (Ed25519 via `ring`; the default registry's key pinned in the binary, others trust on first use per cluster) | A compromised CDN or bucket serving a forged index along with matching blobs | 7 |
| Cluster policy: `requireProvenance` for native plugins, kinds allowed per registry | Unreviewed native code in production | 7 |
| Advisories: a version flagged by the registry shows in `outdated`, `info`, and on the installed object | Running a known-bad version | 7 |

**The capabilities summary** is the registry's answer to "what does this do
to my cluster". It's computed at publish by evaluating the package with its
defaults, stored in the version document, and shown on the page, in `info`,
and in install prompts:

- **job, template:**
  - runtimes used and the container images, by digest or by tag (a tag is
    flagged);
  - host access: `hostLevel`, host network, host-path volumes, `exec` without
    isolation;
  - privileged containers;
  - devices; ports and ingress hosts; KV paths read; workload identity
    claims.
- **app:** the permission grant in plain words (`describe_rule`,
  `app.rs:1846-1878`), placement (managers or workers), code artifacts and
  images, object kinds, jobspec keys.
- **plugin:** type, mechanism, where it runs, platforms, and for native
  builds, "runs inside every worker process it's installed on, with the
  worker's privileges".

**What the design does not protect against**, said plainly on the public
page: an author publishing malicious code under their own name. The
mitigations are the capabilities summary, version diffs that highlight
capability changes, verified and official badges, install-time warnings for
native plugins from unverified publishers, admin takedown with a record of
who downloaded the version, and advisories.

### 6.13 The hosted registry service

#### 6.13.1 Shape

```text
             Cloudflare (TLS, CDN)
     ┌────────────────┴───────────────────────┐
     │ registry.legion.dev                    │ blobs.registry.legion.dev   (R2 custom domain,
     ▼                                        ▼                              public read, immutable)
 Legion ingress ──▶ legion-registry serve ×2 ──────▶ R2 bucket legion-registry-blobs
                    (API + SSR pages + admin)
                          │    ▲
                          ▼    │ jobs (SKIP LOCKED)
                       Postgres ◀── legion-registry worker ×1
                          │        (ingest, validate via `legion package inspect`,
                          │         README render, rollups, reindex, GC, GitHub webhooks)
                          └──▶ WAL-G ──▶ R2 bucket legion-registry-backups (private)
```

- **One image, one binary, `legion-registry`**, with subcommands: `serve` (API,
  web pages, admin), `worker` (background jobs), `migrate`, `admin` (bootstrap
  and break-glass). One image is what a company self-hosting it wants: one
  image, a Postgres, a bucket (calls #9).
- **Stack:**
  - `axum`, plus `sqlx` with committed offline query metadata, so `cargo build`
    needs no database;
  - `object_store` for R2, already in the workspace;
  - `minijinja` templates for server-rendered pages;
  - `comrak` and `ammonia` for README markdown, sanitized;
  - tree-sitter highlighting for Nickel and JSON;
  - `rust-embed` for static assets.
- **Interactive parts** are small SolidJS islands built with Vite and embedded
  in the binary: search-as-you-type, the variables configurator, the version
  diff viewer, and the admin charts.
  - Tailwind uses the Legion UI's token set (`ui/src/index.css`,
    `ui/tailwind.config.js`), so the registry and the cluster UI look like one
    product.
- **No Redis, no search engine, no queue service.** Postgres does search
  (§6.16) and jobs (`SELECT … FOR UPDATE SKIP LOCKED`); calls #10.
- **Validation runs the real CLI.** The image contains the `legion` binary.
  The worker runs `legion package inspect --json <dir>` per publish in a
  subprocess with a 30 s timeout, rlimits, no network, and a scratch
  directory. Nickel evaluation can't hang the service, and the registry's
  rules are by construction the CLI's (calls #11).
- **Where the code lives:**
  - `crates/registry-protocol/` (`legion-registry-protocol`): reference
    grammar, manifest and version-document types, canonical JSON, digests,
    range selection. It sits below `legion-common` and depends on no Legion
    crate, so both the CLI and the server use it.
  - `registry/`: the service. `server/` (the `legion-registry` crate),
    `web/` (templates, islands), `migrations/`, `Dockerfile`,
    `deploy/registry.ncl`, `README.md`. It is a workspace member, so `cargo
    test --all` covers it.

#### 6.13.2 Running it on Legion

`registry/deploy/registry.ncl`, abridged:

```nickel
let image = "ghcr.io/legion-sh/legion-registry@sha256:…" in
let secret = fun name key => { name = name, value = { template = "{{ kv \"registry/%{key}\" }}" } } in
let env = [
  secret "DATABASE_URL" "database-url",
  secret "R2_ACCESS_KEY_ID" "r2-access-key-id",
  secret "R2_SECRET_ACCESS_KEY" "r2-secret-access-key",
  secret "GITHUB_CLIENT_SECRET" "github-client-secret",
  secret "SESSION_KEY" "session-key",
] in
{
  name = "legion-registry",
  tasks = [
    { name = "postgres", runtime = "container", lifecycle = "service", group = "db",
      config = { image = "postgres:16-alpine" },
      stateful = { mode = "slot", storage = { path = "/var/lib/postgresql/data", driver = "local-dir", params = { size = "50G" } } },
      # WAL-G sidecar archives WAL continuously to the private R2 bucket.
    },
    { name = "migrate", runtime = "container", lifecycle = "batch", group = "app",
      dependsOn = [{ task = "postgres", condition = "start" }],
      config = { image = image, command = ["legion-registry", "migrate"] }, env = env },
    { name = "web", runtime = "container", lifecycle = "service", count = 2, group = "app",
      dependsOn = [{ task = "migrate", condition = "success" }],
      config = { image = image, command = ["legion-registry", "serve"] },   # listens on $LEGION_PORT_HTTP
      env = env,
      network = { ports = [{ name = "http" }] },
      service = { port = "http", ingress = { host = ["registry.legion.dev"] } },
      checks = [{ type = "health", kind = "http", path = "/healthz", port = "http", every = "5s", timeout = "2s" }] },
    { name = "worker", runtime = "container", lifecycle = "service", count = 1, group = "app",
      dependsOn = [{ task = "migrate", condition = "success" }],
      config = { image = image, command = ["legion-registry", "worker"] }, env = env },
    { name = "basebackup", runtime = "container", lifecycle = "batch", schedule = "0 3 * * *",
      config = { image = image, command = ["legion-registry", "admin", "backup"] }, env = env },
  ],
}
```

- **TLS:** the ingress app and the cert manager (`docs/ingress.md`).
- **Secrets:** KV references, encrypted at rest.
- **Health:** `/healthz` (process up) and `/readyz` (Postgres and R2 reachable).
- **Metrics:** `/metrics` (Prometheus) for request rates, latencies, the job
  queue depth, ingest failures, and R2 errors. They feed the admin panel's
  health tab and the cluster's observability stack.
- **Backups and restore:** continuous WAL archiving plus nightly base backups
  to the private bucket, and a documented restore drill run in staging before
  launch. Blobs need no backup beyond R2's own durability, because Postgres
  holds every digest and a lost blob is detectable (`legion-registry admin
  verify-blobs`).
- **Where Postgres runs** is an open question (§11): this sketch dogfoods a
  stateful Legion task, and a managed Postgres is the alternative.

**Configuration** is environment variables only, so the container runs
anywhere:
- `DATABASE_URL`
- `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_PUBLIC_URL`, plus the access keys
- `GITHUB_CLIENT_ID`/`SECRET`, `GITHUB_APP_ID`/`KEY`
- `SESSION_KEY`, `SIGNING_KEY` (Phase 7)
- `REGISTRY_PUBLIC_URL`, `REGISTRY_BOOTSTRAP_ADMINS` (GitHub logins granted
  `admin` at first start)
- `REGISTRY_MODE=public|private` (private requires sign-in to read)

### 6.14 Data model (Postgres) and storage (R2)

The main tables:

| Table | Key columns |
| --- | --- |
| `accounts` | `id`, `github_id` (unique), `login`, `kind` (user/org), `display_name`, `avatar_url`, `role` (`user`/`moderator`/`admin`), `suspended_at` |
| `account_members` | `org_id`, `user_id`, `role` (org owners/members, refreshed from GitHub) |
| `packages` | `id`, `owner_id`, `name`, `kind`, `description`, `keywords text[]`, `categories text[]`, `license`, `repository`, `latest_version_id`, `deprecated` (message, successor), `official`, `downloads_total`, `downloads_30d`, `search tsvector` (generated, weighted), `created_at`, unique (`owner_id`, `name`) |
| `package_owners` | `package_id`, `account_id` |
| `versions` | `id`, `package_id`, `version`, `major`/`minor`/`patch`/`pre` (for ordering), `digest` (unique), `document jsonb`, `legion_req`, `platforms text[]`, `capabilities jsonb`, `readme_html`, `changelog_html`, `yanked_at`/`yanked_reason`, `taken_down_at`/`reason`, `breaking_override`, `provenance jsonb`, `published_by`, `published_at`; unique (`package_id`, `version`); no `UPDATE` path except yank/takedown columns |
| `files` | `digest` (pk), `size`, `media_type`, `r2_key`, `refcount` |
| `version_files` | `version_id`, `digest`, `role`, `platform`, `legion_build`, `fingerprint` |
| `linked_repos` | `package_id` or `owner_id`, `github_repo`, `installation_id`, `path`, `last_ingest` |
| `trusted_publishers` | `package_id`, `repository`, `workflow`, `environment` |
| `api_tokens` | `id`, `account_id`, `sha256(secret)`, `scopes`, `expires_at`, `last_used_at` |
| `jobs` | `id`, `kind`, `payload jsonb`, `run_after`, `attempts`, `locked_until`, `last_error` |
| `events` (partitioned by month) | `ts`, `type`, `package_id`, `version_id`, `platform`, `client` (cli/manager/worker/web), `legion_version`, `ci`, `country`, `client_hash`, `query`, `results`, `referrer_host` |
| `daily_package_stats`, `daily_search_stats`, `daily_registry_stats` | Nightly rollups (§6.18) |
| `reports` | `id`, `package_id`, `version_id`, `reporter_id`, `reason`, `body`, `status`, `resolved_by` |
| `advisories` | `id`, `package_id`, `affected` (range), `severity`, `summary`, `published_at` |
| `collections`, `collection_items` | Admin-curated lists for the home page |
| `reserved_names` | `name`, `reason` |
| `audit_log` | Every publish, yank, takedown, ownership, role, and token change: who, what, when, from where |

**R2 layout:**
- `legion-registry-blobs` (public read through a custom domain):
  - `sha256/<aa>/<hex>`: immutable, `Cache-Control: public, max-age=31536000, immutable`.
  - `uploads/<session>/<hex>`: in-flight, cleared by a lifecycle rule after
    24 h.
- `legion-registry-backups` (private): WAL and base backups.

**Upload flow:**
1. The CLI calls `POST /v1/publish` with the version document draft.
2. The API answers with presigned R2 `PUT` URLs for the files it doesn't
   already have, so large plugin builds never pass through the app server.
3. The CLI uploads, then calls `POST /v1/publish/<session>/finalize`.
4. The worker re-hashes every upload from R2, validates, copies the files to
   `sha256/…`, and commits the version in one transaction. Until then nothing
   is visible.

### 6.15 HTTP API (v1)

The read protocol is §6.6.2. The rest:

| Method and path | Auth | Purpose |
| --- | --- | --- |
| `GET /v1/search` | none | Search with filters and facets (§6.16) |
| `GET /v1/packages/<owner>/<name>` | none | Package page data: metadata, versions, owners, stats summary |
| `GET /v1/packages/<owner>/<name>/versions/<v>/files/<path>` | none | One file from the source archive (the source browser) |
| `GET /v1/packages/<owner>/<name>/diff?from=&to=` | none | Source, variables, and capabilities diff between versions |
| `GET /v1/packages/<owner>/<name>/stats` | none | Downloads per day by version, platform, Legion version (public, like crates.io) |
| `POST /v1/publish`, `POST /v1/publish/<id>/finalize` | token | Publish (§6.14) |
| `POST /v1/packages/<owner>/<name>/versions/<v>/yank` (`unyank`) | token, owner | Yank with reason |
| `PUT /v1/packages/<owner>/<name>/deprecation` | token, owner | Deprecate |
| `PUT /v1/packages/<owner>/<name>/owners` | token, owner | Manage owners |
| `PUT /v1/packages/<owner>/<name>/trusted-publishers` | session, owner | Trusted publishing config |
| `POST /v1/auth/device`, `POST /v1/auth/device/token` | none | CLI device flow (GitHub underneath) |
| `POST /v1/auth/oidc/github` | GitHub OIDC JWT | Trusted-publishing token exchange |
| `POST /v1/reports` | session | Report a package or version |
| `POST /v1/webhooks/github` | GitHub signature | Tag pushes, App installs |
| `/v1/admin/*` | session, role | Admin API behind the admin panel (§6.18) |

Errors use one envelope, the same shape as the CLI's
(`{ ok: false, error: { code, message, hint, retryable } }`), so the CLI passes
them through untouched. Rate limits:
- anonymous reads per IP: generous, because Cloudflare caches the documents;
- searches per IP;
- publishes per account.

### 6.16 Search and filters

**Query language.** Plain words plus optional inline qualifiers, like GitHub
search. Every qualifier is also a UI filter chip and an API parameter:

```text
postgres kind:template official:true
gpu kind:plugin type:devices platform:linux/arm64
cache runtime:container host:none sort:downloads
```

| Filter | Values | Notes |
| --- | --- | --- |
| `kind` | job, template, app, plugin | Tabs at the top of results |
| `category` | the fixed list (§6.4) | Admin-editable taxonomy |
| `keyword` | free tags | |
| `runtime` | exec, container, sandbox, isolate, any plugin runtime | From the capabilities summary |
| `type` / `mechanism` | plugin type; native, exec, wasm | Plugins only |
| `platform` | linux/amd64, linux/arm64, darwin/arm64 | Plugins only |
| `compatible` | a Legion version | The CLI and the cluster UI pass the cluster's version automatically, with the filter on by default and visible ("works with 0.10.2 ✓") |
| `host` | none, any | "Doesn't need host access": the security-minded filter |
| `publisher` | official, verified, any | |
| `license` | SPDX id | |
| `updated` | 30d, 90d, 1y | |
| `owner` | an owner | |
| `sort` | relevance (default), downloads, updated, new, name | |

**Ranking**, all in one SQL query:
- the weighted full-text rank: name A, keywords B, description C, README D;
  `websearch_to_tsquery`, plus a prefix query for search-as-you-type;
- plus `pg_trgm` similarity on the name, so `postgress` finds `postgres`;
- times a popularity factor, `log(1 + downloads_30d)`;
- plus boosts for official and verified;
- times a gentle recency decay;
- with deprecated packages sunk and yanked-only packages hidden.

A small synonym table in the service (`pg → postgres`, `k8s → kubernetes`,
`tls → certificates`) expands queries before they reach Postgres.

**Facets.** Facet counts come from one grouped query over the filtered set,
fine at the tens of thousands of packages a registry like this can expect.
Revisit with a dedicated engine only past a measured p95 > 100 ms (calls #10).

**Search analytics.** Each search logs `{normalized query, filters, result
count}`. A click from a result page carries `?from=search&pos=N` into the page
view, so the admin can see click position and zero-result queries (§6.18).

### 6.17 The web UI

**Direction.** The UI should be calm, fast, and dense where it matters:
- Legion's token set (canvas, surface, ink, brass accent), Inter for text,
  Space Grotesk for display, Fira Code for code;
- light and dark modes;
- one accent color per kind (job, template, app, plugin) as a small marker,
  not a rainbow.

Pages are server-rendered HTML, with a target of < 100 ms to first byte from
the edge and no layout shift. `/` focuses search from anywhere, and ⌘K opens a
command palette. Every page has stable, shareable URLs and Open Graph previews
(the package card rendered as an image), since links get pasted in Slack and
READMEs.

**Pages:**

1. **Home.** One search box, kind tabs, the curated collections ("Start here",
   "Observability stack", "GPU nodes", "Official"), trending this week,
   recently updated, and a two-line "publish yours" pitch.
2. **Search.** Results on the left, filter chips at the top (with the
   cluster-compatibility chip pre-set when the visitor arrived from a CLI or
   cluster link), facet counts, and a "nothing found? request it" link that
   records the query. Each result card shows the kind marker,
   `owner/name`, description, latest version, downloads (30 d), updated,
   badges (official, verified, deprecated), and capability icons (host
   access, privileged, runs on managers).
3. **Package page:**

   ```text
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │ ▣ TEMPLATE   legion/postgres   16.4.0 ▾          ✓ official   Apache-2.0       │
   │ PostgreSQL 16 with a volume, backups, and a psql action.                     │
   │ ┌──────────────────────────────────────────────┐   12.4k downloads · 30 d     │
   │ │ $ legion template install legion/postgres  ⧉ │   updated 6 days ago        │
   │ └──────────────────────────────────────────────┘   works with legion ≥0.9     │
   ├─ README ─ Configure ─ Source ─ Versions ─ Changes ─ Capabilities ─ Stats ─────┤
   │                                                    │ WHAT THIS DOES           │
   │  (rendered README)                                 │ ● 1 container task       │
   │                                                    │   postgres:16 (digest)   │
   │                                                    │ ● 20 GiB volume          │
   │                                                    │ ○ no host access         │
   │                                                    │ ○ no extra permissions   │
   │                                                    │ VARIABLES (4)            │
   │                                                    │ name · size_gb · …       │
   └──────────────────────────────────────────────────────────────────────────────┘
   ```

   - **README.** Rendered and sanitized.
   - **Configure.** A form built from the variables, the same component
     semantics as the cluster UI's launch form. It writes the exact command
     (`legion template run postgres --var name=orders --var size_gb=50`) and
     an equivalent `--vars-file` JSON. Server-side preview rendering of the
     resulting jobspec is a later addition (§11).
   - **Source.** A file browser over the version's archive, with Nickel
     highlighting: "see exactly what runs" before installing.
   - **Versions.** A list with dates, yanked badges and reasons, `legion`
     requirement, and native build matrix for plugins.
   - **Changes.** The diff between any two versions: source, variables (added,
     removed, changed default), capabilities (with additions in red), app
     permissions, and the changelog section.
   - **Capabilities.** The full summary (§6.12).
   - **Stats.** Downloads over time, split by version, platform, and Legion
     version.
4. **Owner page.** Packages, members, and a verified badge.
5. **Publish.** Link a repository (with the detected-package checklist), the
   trusted-publishing setup, and the CLI quickstart.
6. **Account.** API tokens (scoped, expiring), linked repositories, the
   owner's own analytics for every package they own, and notification
   settings (publish failures, reports, advisories).
7. **Docs.** "Publishing your first package", the manifest reference, the
   versioning rules, the protocol, and the policies (names, takedowns).
   Rendered from `docs/registry*.md` in this repository so the public docs
   have one source.

**Accessibility and performance budgets:**
- WCAG 2.1 AA: contrast checked in both themes, full keyboard navigation,
  visible focus;
- under 50 KiB of JS on content pages; islands load only where they're used;
- Lighthouse ≥ 95 on the home, search, and package pages, checked in CI.

**In the cluster UI** (Phase 8): a **Catalog** page under Workloads searches
the cluster's registries through the manager, and installs open the existing
flows (template launch, app consent, plugin target picker). Installed
objects show "update available" badges from `outdated`.

### 6.18 Admin panel and analytics

`/admin` requires a session with role `moderator` or `admin`. Moderators
handle content; admins also handle people, roles, and settings. Every admin
action lands in `audit_log`, shown on its own tab.

**Overview (the default tab):**

```text
┌ Downloads 7d ─┐┌ Searches 7d ─┐┌ Zero-result ─┐┌ New versions ┐┌ Publishers ──┐┌ Open reports ┐
│ 48.2k  ▲12%   ││ 9.1k  ▲4%    ││ 6.3%  ▼0.8   ││ 212  ▲31     ││ 57 active    ││ 3            │
└───────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘
Downloads per day, stacked by kind (90 d)            Legion versions in use (from downloads, 30 d)
Top packages · Trending (week-over-week growth)      Platforms (amd64 / arm64 / darwin)
Top searches · Zero-result searches ("kafka" 41)     Clients (CLI / manager / worker / web) · CI share
```

**Tabs:**

- **Analytics.** Every chart can be filtered by kind, package, owner, and date
  range, and exported to CSV:
  - downloads over time, by kind, package, version, platform, Legion version,
    client, and human vs CI;
  - unique clients per day;
  - countries (coarse, from Cloudflare's country header);
  - referrers;
  - the funnel: search → package view → install-command copy → download;
  - publish activity: new packages, versions, publishers, failed validations
    by rule;
  - search quality: top queries, zero-result queries, low-click queries, and
    the average click position.
- **Packages.** Search every package, including hidden ones:
  - yank or unyank any version, take down (with a required public reason and
    a private note), deprecate;
  - transfer ownership; mark official or verified;
  - **who downloaded this**, aggregated by client hash, Legion version, and
    country, for incident response;
  - publish an advisory against a version range.
- **Reports.** The abuse queue: triage, resolve, take down, contact owner.
- **Review.** Packages held by the typosquat check, and first-time
  publishers of native plugins, if that policy is on (§11).
- **Accounts.** Search, role changes, suspension (hides the account's
  packages from search and blocks publishing; existing versions stay
  installable unless taken down), and the account's API tokens and recent
  activity.
- **Curation.** Collections, categories, synonyms, reserved names, and the
  home page "wanted" list.
- **Health.** Request rate and latency per route, error rate, job queue depth
  and failures, ingest backlog, Postgres size and slow queries, R2 storage
  and object counts, and certificate expiry. It reads the service's own
  metrics and links to the cluster's observability for depth.

**How the analytics are collected:**
- **Events are written on the request path** to the monthly-partitioned
  `events` table, by a batched in-process writer that flushes every second or
  every 500 events. Analytics never slow a download.
- **Nightly rollups** go into the `daily_*` tables. Raw events are kept 90
  days and rollups forever (calls #14).
- **Privacy:**
  - No IP address is stored. `client_hash = sha256(ip ‖ user-agent ‖
    daily_salt)`, and the salt is rotated and destroyed daily, so unique
    counts work within a day and nobody can be followed across days. This is
    Plausible's approach.
  - The CLI's `User-Agent` carries its version, client kind, and `ci=1` when
    `CI` is set, nothing else.
  - The privacy page says exactly this.
- **Bots.** Known crawlers and the registry's own health checks are dropped at
  write time.
- **Owner analytics** are the same rollups filtered to their packages, on
  their account page.

### 6.19 Agent surfaces

- **Every new CLI verb takes `--json`** and uses the standard envelope. The
  verbs are `registry list|add|remove|search|info|versions|pull|diff|pin|outdated|login`,
  `package init|validate|pack|publish|yank|unyank|deprecate|owner`, and
  `plugin install|list|status|upgrade|uninstall`, plus the new
  `template|app|job` forms.
- **Capability catalog** (`src/cmd/capabilities.rs`) entries for all of them.
  Read-only ones (`registry.search`, `registry.info`, `registry.outdated`,
  `plugin.status`, …) are `read_only_safe: true`, and every install, publish,
  and yank is `mutates: true, read_only_safe: false`.
- **MCP tools:**
  - read-only, always listed: `legion_registry_search` (returns variables and
    capabilities per result, so an agent can choose without another call),
    `legion_registry_info`, `legion_registry_outdated`, `legion_plugin_status`;
  - mutating, behind `--allow-write`: `legion_template_install`,
    `legion_app_install` (dry-run first, returning the consent prompt as data),
    `legion_plugin_install`.
- **Idempotency.** New mutating RPCs take `idempotency_key` and report
  `deduplicated`.
- **Exact installs.** `--dry-run` returns the digest, and `--digest` installs
  exactly that (§6.5).
- `docs/agent-readiness.md` gets a "Packages" section.

### 6.20 ACLs

| RPC | Check |
| --- | --- |
| `ListRegistries` | Authenticated identities only, no resource. The list holds no secrets (credentials stay in KV), and every client needs it to resolve references. Marked `// ACL: authenticated-open` and classified in `acl_endpoints.rs` with that justification. |
| `AddRegistry`, `RemoveRegistry`, `SetDefaultRegistry` | `operator:write` |
| `ResolvePackage` (resolve a reference against the cluster's registries; used by the CLI and the UI) | Authenticated; returns only what the registries publicly serve |
| `InstallJobTemplate` / `UpgradeJobTemplate` | `template:write` on the target name, the same as `template create` |
| `InstallApp` / `UpgradeApp` with a registry source | Unchanged: the installer's token must cover the grant |
| `InstallPlugin`, `UpgradePlugin`, `UninstallPlugin` | `operator:write` |
| `ListPlugins`, `GetPluginStatus` | `node:read` |
| `FetchPluginFile` | Worker plane (`acl::enforce_worker_plane`); a worker may only fetch digests of installs that target it |

Node plugins are root-level code on every machine they touch, so they sit
with the operator, not in a new resource (calls #18).

## 7. Proto, struct, config, and persistence changes

**`proto/legion.proto`.** Every new RPC and field gets full public comments.

- **New messages:** `PackageRef`, `PackageProvenance`, `Registry`,
  `PluginInstall`, `PluginTarget`, `NodePluginStatus`.
- **New RPCs:**
  - `ListRegistries`, `AddRegistry`, `RemoveRegistry`, `SetDefaultRegistry`;
  - `ResolvePackage`;
  - `InstallJobTemplate`, `UpgradeJobTemplate`. These are new, and leave
    `UpsertJobTemplate` for file sources;
  - `InstallPlugin`, `UpgradePlugin`, `UninstallPlugin`, `ListPlugins`,
    `GetPluginStatus`;
  - `FetchPluginFile` (worker plane, server-streaming).
- **Changed:**
  - `InstallAppRequest` / `UpgradeAppRequest` gain `PackageRef package` and
    `string expected_digest`, and `App` gains `PackageProvenance provenance`
    and `previous_source_ref`;
  - `JobConfig` meta and `JobTemplateVersion` gain `PackageProvenance`;
  - `HeartbeatReply` / `StreamWorkReply` gain `repeated PluginInstall
    plugin_installs`, and `HeartbeatRequest` gains `repeated
    NodePluginStatus plugins`.

**Structs**
- `legion-registry-protocol` (new bottom crate): `Reference`, `Manifest`,
  `VersionDocument`, `IndexDocument`, `Capabilities`, canonical JSON, digest,
  range selection.
- `legion-common`: `AppSource::Registry`, and the app `version` validated as
  semver. `PluginInstallSpec` for the config block, and the managed plugin
  root in `util.rs` searched after `plugin_root_dir()` (`util.rs:382`).
  `ContentClass::Plugins`.
- `legion-worker`: the plugin fetcher and reconciler (fetch, verify,
  materialize, pre-flight, activate, report), and loaders taking a resolved
  directory.
- `legion-manager`: registry records, the resolver (git and HTTP clients), the
  manager content store, and the plugin-install fan-out on heartbeat and
  `StreamWork`.

**Config**
- **Manager:** `registries = { default, list }` (seed only), and `plugins.install`.
- **Worker:** `plugins.install`, and `content.budgets.plugins` (default 2 GiB).
- **Job config:** none. No new jobspec fields; `pkg:` is a new value in
  existing `source` fields. `configs/jobs/maximal.ncl` still gets a
  commented `pkg:` artifact example, so the form is discoverable.
- **Example configs:** `configs/workers/default.ncl` and `configs/manager.ncl`
  get commented `plugins.install` examples.

**Persistence (manager store)**
- New Control-lane tables `registry` and `plugin_install`.
- Provenance columns on `job_template_version`, `app`, and job-config meta.
- One migration, applied like every other.

**Persistence (registry service):** the Postgres schema in §6.14, managed by
`legion-registry migrate`, forward-only.

## 8. Implementation phases

Each phase ships on its own and is useful on its own.

**Phase 1 — Packages, versions, and git registries.** *No server yet.*
- `legion-registry-protocol`: references, manifests, the deterministic packer,
  version documents, digests, and range selection.
- `legion package init|validate|pack|inspect`.
- Git registries: a reader built on the existing git fetcher.
- Cluster registry records with `legion registry list|add|remove`, seeded with
  an **official git registry** as the default.
- `legion registry search|info|versions|pull|diff|pin`.
- `legion job run <ref>` with declared variables.
- `legion template install|upgrade <ref>`.
- Provenance on job configs and templates, and `legion registry outdated` for
  both.

*Value:* a team can share versioned, digest-pinned jobs and templates from a
git repository today, and the official repository gets its first packages:
`postgres`, `redis`, `nginx`, `minio`, `otel-collector`, `vector`.

**Phase 2 — The hosted registry.**
- `legion-registry` serve, worker, and migrate.
- Postgres schema, R2 storage, and the HTTP protocol.
- GitHub sign-in (web and device flow), API tokens, and `legion package
  publish|yank|unyank|deprecate|owner`.
- Search and filters with facets.
- The web UI: home, search, package page with README, Configure, Source,
  Versions, Capabilities, and Stats; owner page; docs.
- Capabilities summaries.
- Minimum moderation: takedown, suspension, the reports queue, reserved names,
  and the audit log.
- Event capture from day one.
- Deployed on Legion as `registry/deploy/registry.ncl`, with backups and a
  restore drill.
- The official git registry is ingested and **the default flips to the hosted
  registry** for new clusters.

*Value:* the public registry exists and is searchable, and anyone can publish
from a laptop.

**Phase 3 — Contribute from GitHub.**
- Linked repositories through a GitHub App: publish on tag, commit statuses,
  and ingest of git-registry plugin builds from URLs.
- Trusted publishing (OIDC exchange) and the documented Action recipe.
- Provenance on version pages.
- The Changes tab: version diffs.
- Breaking-change detection at publish, with `--accept-breaking`.

*Value:* publishing becomes `git push --tags`.

**Phase 4 — Apps from registries.**
- `AppSource::Registry`, `legion app install|upgrade <ref>`, `app rollback`.
- Semver app versions (breaking, §10).
- App permission diffs on the site.
- `outdated` covers apps.

*Value:* closes the apps plan's deferred "registry installs".

**Phase 5 — Plugins.**
- **5a: declarative and hash-pinned.**
  - `plugins.install` in worker and manager configs, from a `source` +
    `checksum`, or from a `package` + `digest` fetched directly.
  - Plugin packages, the `plugins` content class, the managed plugin root,
    and pre-flight.
  - Per-node plugin status in heartbeats, plus `legion plugin list|status`.
  - *Value:* operators stop copying plugin files by hand. 5a needs only
    Phase 1's package format, so it can be pulled ahead of Phases 2–4.
- **5b: cluster install distributed by the managers.**
  - `legion plugin install|upgrade|uninstall`, `PluginInstall` records, and
    the heartbeat/`StreamWork` fan-out.
  - The manager content store, `FetchPluginFile`, the activation table, and
    conflicts.
  - *Value:* "tell the cluster once".
- **5c: `pkg:` sources.** `pkg:` in artifact `source` fields (net-plugin
  modules, autoscaler strategies, isolate modules, job artifacts), pinned at
  submit.

**Phase 6 — Admin panel and analytics.**
- The full `/admin`: overview, analytics tabs, package tools including "who
  downloaded this", review, accounts, curation, and health.
- Nightly rollups and CSV export.
- Owner analytics on account pages.
- Collections on the home page.

*Value:* the registry admin persona's jobs. Event capture since Phase 2
means the charts open with history.

**Phase 7 — Trust and policy.**
- Registry-signed version documents, with the default key pinned in the
  binary.
- Cluster registry policy: per-registry allowed kinds, `requireProvenance` for
  native plugins.
- Verified publishers (domain or org verification).
- Advisories in `outdated`, `info`, and on installed objects.

**Phase 8 — The cluster UI and air gap.**
- **8a:** the Catalog page in the cluster UI, through `ResolvePackage` and a
  manager-proxied search, with "update available" badges.
- **8b:** offline plugin install by upload (`legion plugin install
  ./file.tar.gz`), with manager-to-manager copy by digest.
- **8c:** `legion registry mirror` to copy a set of packages into a
  self-hosted registry.

## 9. Testing plan

**Unit** (in `legion-registry-protocol`, `legion-common`, `legion-worker`)
- **Reference grammar:** every row of the §6.5 table, plus the ambiguity
  rules (file vs reference).
- **Range selection:** prereleases, yanked handling, the compatibility filter,
  and "exact names a yanked version".
- **Canonical JSON:** golden digests, so a change in serialization breaks a
  test.
- **The deterministic packer:** the same tree gives the same bytes across runs
  and platforms; symlinks and escapes are refused.
- **The breaking-change detector:** one case per row of the §6.3 table.
- **Manifest validation:** every field's error message.
- **The plugin reconciler:**
  - state transitions;
  - a mismatched hash is never retried with the same bytes;
  - node config wins over a cluster install;
  - a file on disk produces `conflict`;
  - the activation table per mechanism.
- **`plugins.install` config validation:** the missing-hash refusal.

**Integration** (`tests/integration/registry*.rs`, modules of the one test
binary)
- **Git registries:** a fixture repository (a `file://` git repo built in the
  test) with tags. Resolve, install a template, `outdated`, a moved tag
  refused, and a branch reference warned.
- **End-to-end plugin distribution:** a manager and two workers. Install the
  `noop` task plugin as a package to one node, assert `active` there and
  absent on the other, then widen. A worker given a tampered file by a test
  manager refuses it. Uninstall while a job uses the runtime is refused.
- **Declarative plugins:** a worker config with a `source` + `checksum`
  served by a local HTTP server; a bad checksum leaves the node up without
  the plugin and reports it.
- **Apps:** install from a package, upgrade with new permissions (needs
  `approve`), rollback.
- **ACLs:** every new RPC in `acl_endpoints.rs`, including the
  authenticated-open classification for `ListRegistries` and the worker-plane
  check on `FetchPluginFile` (a worker can't fetch a digest it isn't
  targeted for).
- **MCP:** the `mcp_tools.rs` coverage test, plus the capabilities test that
  mutating entries aren't read-only-safe.

**Registry service** (`registry/server`)
- **API tests against a real Postgres** (a CI service container) and MinIO
  standing in for R2 (S3-compatible). Locally: `docker compose up` in
  `registry/`.
- **Publish flow:**
  - presigned upload;
  - a re-hash mismatch rejected;
  - concurrent publishes of the same version (one wins, one gets `409`);
  - no `UPDATE` path on versions (a test that tries it through every handler).
- **Search ranking golden tests:** a seeded corpus with expected top-3 for
  ~40 queries, including typos and synonyms.
- **Analytics:** the salt rotation means yesterday's hashes can't be
  recomputed; rollups are correct across partitions.
- **UI:**
  - Playwright smoke tests for every page in both themes;
  - axe accessibility checks;
  - Lighthouse budgets in CI;
  - a visual-diff snapshot set for the package page.
- **Load:** index and version-document endpoints at 2,000 req/s from one
  `serve` replica with Cloudflare caching off, as a floor.

**Regression**
- The existing template, app, and plugin suites run unchanged.
- File and URL installs still work.

## 10. Rollout and backward compatibility

No installed base to protect (AGENTS.md), so these break on purpose, and each
PR says so:

- **App `version` must be semver** for every non-bundled app (it was a
  free-form string). `legion app install` of a manifest with `version =
  "beta-3"` is refused: `app version 'beta-3' is not semver (e.g. 0.3.0)`.
- **`plugin.json` gains a required `version`**, and the in-tree plugins are
  updated.
- **`legion job run <arg>`** reads a non-file, non-URL argument as a package
  reference, so a mistyped local path now reports a registry miss as well as
  a file miss (§6.8.1).
- **New clusters are seeded with the `legion` registry as the default.** Phase
  1 seeds the official git registry; Phase 2 seeds the hosted one. Existing
  dev clusters get it on their next bootstrap.

**Default-on:** registries, references, provenance, and the plugin
reconciler (inert with no installs).

**What an operator has to do:** nothing to keep working; `legion registry
remove legion` to lock a cluster to its own sources.

**The hosted registry's launch** is staged:
1. A private beta on a staging cluster, with the official packages ingested
   and a restore drill run.
2. Public read.
3. Public publish, once Phase 2's moderation minimum is live.

## 11. Open questions

1. **Hostname and org.** `registry.legion.dev` and `ghcr.io/legion-sh/…` above
   are placeholders. *Lean:* a short, product-owned domain; blobs on a
   subdomain of it.
2. **Where Postgres runs.** A stateful Legion task with WAL-G to R2
   (dogfooding, one bill), or a managed Postgres (less to operate).
   *Lean:* dogfood it. It exercises stateful workloads in public, and the
   registry's data is small. Keep the restore drill in CI.
3. **SSR with islands, or a SolidStart app.** *Lean:* SSR in Rust with Solid
   islands, for one image and fast content pages (calls #9). The cost is two
   component systems, kept small. Worth your eye, since "beautiful" is the
   bar.
4. **GitHub-only sign-in.** *Lean:* yes for launch; GitLab and email later if
   asked.
5. **Native plugins from unverified publishers on the public registry.**
   Allowed with warnings, held for review, or refused. *Lean:* allowed, listed
   with a clear "native code, unverified publisher" badge, excluded from
   default search results until verified, and with an install prompt that
   says so.
6. **Should the breaking-change check block or warn?** *Lean:* block with
   `--accept-breaking "<reason>"` (calls #12). Elm proved enforced semver is
   livable; the override keeps false positives cheap.
7. **Server-side render preview on the package page** (fill variables, see
   the jobspec) evaluates untrusted Nickel on the registry. *Lean:* yes in a
   later phase, through the same sandboxed `legion package inspect`
   subprocess, rate-limited.
8. **Restart orchestration for native plugin upgrades.** Should `legion plugin
   upgrade` offer a rolling worker restart, or leave restarts to the operator?
   *Lean:* leave it to the operator in Phase 5. Make it a follow-up once
   worker restarts keep running tasks everywhere.
9. **Should a job package and a template package of the same software be one
   package with two kinds?** *Lean:* no, one kind per package (calls #5); the
   site links siblings.

## 12. Acceptance criteria

**Phase 1**
- `legion template install acme:postgres@1.0.0` against a git registry
  fixture creates the template with provenance, and `outdated` reports
  `1.1.0` after the fixture tags it.
- Moving the `postgres/v1.0.0` tag makes a reinstall fail with the
  immutability error.
- `legion job run acme:redis --var replica=2` fails with `unknown variable
  'replica'`.
- `legion package pack` twice on the same tree gives byte-identical archives.

**Phase 2**
- From a fresh machine with only the `legion` binary, `legion template
  install legion/postgres` works with no setup.
- Searching `postgress` on the site lists `legion/postgres` first.
- The package page's Source tab shows `template.ncl`.
- `legion package publish` of an existing version returns `409` with
  `versions are immutable`.
- Killing the Postgres task and restoring from the R2 backups loses no
  committed version.

**Phase 3**
- Pushing tag `redis-sentinel/v1.1.0` on a linked repository publishes it
  within one minute, with the commit on its page.
- A GitHub Actions workflow with no secrets publishes through trusted
  publishing.
- Removing a variable in a minor version is refused without
  `--accept-breaking`.

**Phase 4**
- `legion app install legion/deployer` shows the consent prompt with the
  package line.
- `app upgrade` to a version that adds a rule requires approval.
- `app rollback` returns to the previous version and its grant.

**Phase 5**
- A worker config with a `source` plugin and a wrong checksum starts, reports
  the mismatch in `legion plugin status`, and never loads it.
- `legion plugin install legion/noop-runtime --node w1` makes `w1` (and only
  `w1`) advertise the runtime within one heartbeat interval plus the fetch
  time, with no network access from `w1` except to the managers.
- `uninstall` with a running job on the runtime is refused.

**Phase 6**
- The admin overview shows downloads, searches, zero-result queries, and open
  reports for the last 7 days.
- A takedown shows the downloaders, aggregated, and returns `410` to the CLI
  with the reason.
- No IP address is stored anywhere in the database (a test scans the schema
  and a sample of rows).

**Phase 7**
- A version document with a bad registry signature is refused by the CLI and
  the managers.
- With `requireProvenance = ["plugin"]`, installing a native plugin with no
  provenance fails with the policy's name.

**Phase 8**
- An operator installs a template from the cluster UI's Catalog page without
  the CLI.
- An air-gapped cluster installs a plugin from an uploaded file to every
  node, with no egress.
