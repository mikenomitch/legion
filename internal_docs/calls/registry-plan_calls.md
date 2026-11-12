# Package Registry — Judgement Calls

Plan: [../plans/registry-plan.md](../plans/registry-plan.md). Status: the plan
is PLANNED, so every entry below is a design-time call made while writing it.
Entries will be amended, and reversals recorded, as phases ship.

**Entries are ranked by project impact, highest first; the number is the
rank.** The plan cites them as "calls #N".

---

## 1. Registries are cluster configuration, and the list is the allowlist

**Call.** The registry list lives in the cluster (Control-lane records), not
on each laptop. Every client resolves against the same list:
- the CLI, through `ListRegistries`;
- the UI and agents;
- the managers, which do the cluster installs.

Cluster installs resolve only against configured registries. A new cluster is
seeded with one default, `legion`.

```console
$ legion registry add acme https://github.com/acme/legion-packages     # operator:write, for everyone
$ legion template install acme:redis                                    # works for every teammate and CI job
$ legion app install github.com/foo/bar:thing
error: registry 'github.com/foo/bar' is not configured on this cluster
hint:  legion registry add foo https://github.com/foo/bar   (needs operator:write)
```

**Alternatives.**
- (a) Helm's and Nomad Pack's model: each user runs `registry add` locally,
  and aliases live in a file on the laptop.
- (b) Both: a local list plus a cluster list, merged.

**Trade-off.**
- Adding a registry needs `operator:write`. A developer who wants to try a
  random repository can still `job run`, `info`, or `pull` it by location,
  but can't install it into the cluster without an operator.
- A laptop with no cluster connection sees only the default registry and
  location references.

**Why (simplicity, and security by default).**
- Helm's local aliases are the reason install instructions start with `helm
  repo add` and still break, because the alias in the README isn't the one
  on your machine. One list means one meaning.
- Making that list the allowlist gives the security admin their main control
  without a second policy object. `legion registry remove legion` locks a
  cluster to its own sources.
- (b) was rejected because two lists, merged, raise "which one did it use?"
  at exactly the wrong moment.

## 2. The managers resolve and fetch for every cluster install

**Call.** For template, app, and plugin installs, the CLI sends a reference,
and the managers resolve it, fetch the bytes, verify them, and install.
- For plugins, the managers then serve the bytes to the nodes.
- The CLI resolves and fetches itself only for client-side work: `job run`,
  `pull`, `info`, and `search`.

**Alternatives.**
- (a) The CLI resolves and fetches everything, and sends content plus a
  claimed provenance, the way `app install <url>` works today
  (`src/cmd/app.rs:879-932`).
- (b) Each worker fetches plugins from the registry itself.

**Trade-off.**
- Managers need egress to the registries, or a mirror.
- A mirror becomes the air-gap answer instead of "the operator's laptop has
  internet".
- Template and app installs gain a manager-side registry client, where the
  CLI's client would have been enough.

**Why (security, then simplicity).**
- With (a), the allowlist is advisory. The caller asserts where content came
  from, and the manager can't check it.
- With (b), every worker needs internet access and registry credentials, and
  a fleet of 1,000 nodes downloads 1,000 times.
- Manager-side fetching makes the policy real. It lets the cluster UI install
  without a CLI (Phase 8). It gives "tell all my nodes to get the plugin from
  the manager", which was asked for directly. Workers reach only the
  managers.
- Digests are verified at every hop, so the managers being in the path adds
  no trust.

## 3. One digest per version pins every platform

**Call.** A version is a canonical JSON **version document** listing the
manifest and the sha256 of every file, every platform's build included. The
document's sha256 is *the* digest:
- A node config pins one string: `{ package = "legion/nvidia-device@1.4.2",
  digest = "sha256:5b1e…" }`.
- Each node picks its platform's file from the document and verifies it.

**Alternatives.**
- (a) Per-file checksums in configs, one per platform, which is Terraform's
  lock file.
- (b) Hash the source archive only, and trust platform builds by URL.

**Trade-off.**
- Fetching a version is always two steps, document then files.
- The canonical-JSON rules become a format we can never change without a new
  `schema` number. Golden-digest tests guard them.

**Why (simplicity).**
- Terraform users regularly hit "the lock file has no hash for linux_arm64"
  and run `terraform providers lock -platform=…` by hand. One digest for a
  mixed amd64/arm64 fleet removes that whole class of chore.
- It makes the source of the bytes irrelevant (registry, manager cache,
  mirror), which is the whole air-gap story.
- It's the same idea as an OCI image index.

## 4. Our own small, static-servable protocol and a git layout — not OCI artifacts

**Call.** The HTTP registry protocol is four GET documents:
- `.well-known`,
- a per-package index,
- an immutable version document,
- content-addressed blobs.

Git repositories with a fixed layout are the other registry type. Packages
are not stored as OCI artifacts.

**Alternatives.**
- (a) OCI artifacts (ORAS), as Helm 3.8+, Flux, and WASM tooling do. Any OCI
  registry (GHCR, ECR, Docker Hub) would be a Legion registry, with cosign
  signatures for free.
- (b) A monolithic index file, as Helm's `index.yaml` does.

**Trade-off.**
- People can't reuse their existing OCI registry as a Legion registry.
- We write and maintain a protocol, and don't inherit cosign's ecosystem.
- `oci://` stays available for *payloads* (app images, isolate modules)
  through the existing fetchers.

**Why (simplicity and the requested surface).**
- The user asked that the simplest registry be a GitHub repository. OCI
  can't express that, and search, READMEs, variables, and capabilities aren't
  part of OCI distribution. We would end up running a metadata service beside
  it anyway (Artifact Hub is exactly that).
- Our protocol can be served from any bucket, and the read path is ~200
  lines.
- (b) is Helm's scaling mistake.
- Revisit if users ask to keep packages in GHCR. An `oci` registry type could
  be added behind the same resolver.

## 5. One package model, four kinds, one kind per package, and each kind keeps its own install verb

**Call.** `job`, `template`, `app`, and `plugin` share a manifest, versioning,
references, and protocol. A package has exactly one kind for life. Installs
go through each kind's verb:
- `legion job run <ref>`
- `legion template install <ref>`
- `legion app install <ref>`
- `legion plugin install <ref>`

There is no generic `legion install`. Job and template stay separate kinds.

**Alternatives.**
- (a) A generic `legion install <ref>` that dispatches on kind.
- (b) Merge job and template into one kind, either "template with zero
  variables" or "job with declared variables".
- (c) Multi-kind packages (a template plus the plugin it needs).

**Trade-off.**
- Users must know the kind to know the verb. The site and `legion registry
  info` print the right command, which softens this.
- An author who wants both a job and a template publishes two packages, and
  the site links them as siblings.

**Why (simplicity).**
- "Install" means four different things: run a job, create a cluster object,
  grant an app power, or put code on machines. One verb would hide the very
  difference the user needs to see, especially the consent step for apps
  and the target selection for plugins.
- Job and template differ in where they evaluate and what they're for. A
  job is multi-file, client-side, and ejectable; a template is single-file,
  manager-rendered, and the unit of ACL delegation. Merging them would make
  one of those properties conditional.
- One kind per package keeps references, filters, and the version semantics
  in §6.3 unambiguous.

## 6. Declarative plugins require a hash, and node config wins over a cluster install

**Call.** Every `plugins.install` entry in a worker or manager config must
carry a `digest` (package) or `checksum` (URL), and the config is refused at
load without one. When node config and a cluster install name the same
plugin on a node, the node config wins, and status says so.

```text
plugins.install[1]: a checksum is required; run `legion registry pin` to get one
nvidia-device on gpu-07: pinned by node config (1.4.2); cluster install wants 1.5.0
```

**Alternatives.**
- (a) Trust on first use: record the hash of the first download.
- (b) Allow a version range in config and resolve at boot.
- (c) Let the cluster install win, so the cluster is the single source of
  truth.

**Trade-off.**
- Writing a node config needs one extra command, `legion registry pin`.
- A fleet can carry two versions of a plugin when configs and cluster installs
  disagree. It's visible, but it can happen.

**Why (security, then simplicity).**
- This block exists for machine images and bootstrap. The user asked that
  the daemon "go grab and hash match". A node that takes whatever a URL
  serves on first boot is the attack the block must prevent.
- A range in config means two boots of the same image can run different code.
- Node config wins because it's the operator's local, reviewed declaration,
  and often a prerequisite for the node working at all. A cluster-wide
  command silently overriding a machine image is the more surprising failure.

## 7. Native plugins ship one build per Legion release, rather than waiting for a stable ABI or excluding them

**Call.** Native (dylib) plugin packages carry builds keyed by platform
*and* Legion build fingerprint, read from the library at pack time. Nodes
select by exact fingerprint. A missing build is a specific, named error. The
site shows a build matrix and prefers exec and WASM plugins in default
sorting, and Legion's release CI rebuilds official native plugins.

**Alternatives.**
- (a) Exclude native plugins from the registry until the stable ABI
  (`TODO.md:1159`) exists.
- (b) Make the stable ABI (`abi_stable`, a C ABI, or out-of-process gRPC) a
  prerequisite phase of this plan.

**Trade-off.**
- Every Legion release orphans third-party native plugins until their
  publishers rebuild, and the registry makes that visible rather than fixing
  it.
- A package page for a native plugin can look broken right after a release.

**Why (extensibility now; keep the ABI decision separate).**
- Task runtimes, scheduling, and rollouts are native-only today. Excluding
  them would leave out the plugins operators most want distributed.
- Folding the ABI redesign into a registry plan would make this plan's
  plugin phase wait on the hardest problem in the plugin system.
- The fingerprint guard already exists and fails closed, so the registry
  only needs to name the right build.
- Revisit when the stable-ABI work lands. The `legionBuild` key then becomes
  an ABI version.

## 8. SemVer for packages; in-cluster counters stay, linked by provenance; app versions become semver

**Call.**
- Every package version is SemVer 2.0.0 and immutable.
- Template revisions stay integers, and job config versions stay as they
  are. Each records `PackageProvenance { registry, package, version, digest }`.
- App manifest `version` becomes required semver for every non-bundled app,
  whatever its source.

**Alternatives.**
- (a) Make template revisions semver too.
- (b) Leave app versions free-form for file and URL installs, and require
  semver only for registry installs.

**Trade-off.**
- A template shows two numbers (`v3 from legion/postgres 16.4.0`).
- Existing app manifests with versions like `beta-3` are refused. That's a
  breaking change, called out in §10 of the plan.

**Why (simplicity, and an honest history).**
- A cluster revision is the cluster's own history, and includes local edits
  that never came from a package. A semver there would be invented.
- App versions have to compare for `upgrade` to mean "newer" and for
  `rollback` to mean anything. A version field that is semver only sometimes
  is a dual code path, which AGENTS.md says not to keep.

## 9. The hosted registry is one Rust image: server-rendered pages with small SolidJS islands

**Call.** `legion-registry` is one binary in one container image. It serves
the API and server-rendered HTML pages (`minijinja`), and the admin panel.
The interactive pieces are SolidJS islands built with Vite and embedded, on
the Legion UI's Tailwind token set:
- search-as-you-type,
- the configurator,
- the diff viewer,
- the charts.

**Alternatives.**
- (a) A Rust API plus a SolidStart (Node) SSR front end: two images, and one
  component system end to end.
- (b) A Rust API plus a client-side SPA like the cluster UI: weaker SEO and a
  slower first paint for a public catalogue.
- (c) Leptos: Rust SSR plus hydration in one language, at the cost of a WASM
  bundle and a new framework.

**Trade-off.**
- Two component systems, templates and islands, must be kept visually
  identical. The mitigation is shared tokens and as few islands as possible.
- Front-end contributors write Jinja templates for most pages.

**Why (simplicity for whoever runs it).**
- Companies are expected to self-host this as their private registry. "One
  image, one Postgres, one bucket" is the setup they will actually run.
- Content pages (READMEs, versions, metadata) are mostly static, and
  server-rendered HTML is the fastest and most crawlable way to serve them.
- This is the call most worth the user's review, since "beautiful" is the
  bar. It's open question 3 in the plan.

## 10. Postgres does search, facets, and background jobs: no Elasticsearch, Meilisearch, or Redis

**Call.**
- Search is Postgres full text with weighted `tsvector`, plus `pg_trgm` for
  typos, a synonym table in the service, and facet counts from one grouped
  query.
- Background jobs are a `jobs` table drained with `FOR UPDATE SKIP LOCKED`.

**Alternatives.**
- (a) Meilisearch or Typesense: better typo tolerance and relevance tuning
  out of the box, and instant facets.
- (b) Redis plus a job library for the queue.

**Trade-off.**
- Relevance tuning is our SQL to maintain, and typo tolerance is weaker than
  a dedicated engine's.
- The trigger to revisit: search p95 above 100 ms, measured, or relevance
  golden tests we can't make pass.

**Why (simplicity).** The user asked for Postgres, a registry's corpus is
tens of thousands of documents at most, and every extra stateful service is
something a self-hoster must run, back up, and upgrade.

## 11. Publish-time validation runs the real `legion` CLI in a sandboxed subprocess

**Call.** The registry image includes the `legion` binary. The worker runs
`legion package inspect --json` per publish, in a subprocess with a timeout,
rlimits, no network, and a scratch directory. The output gives the
validation, the variables, and the capabilities summary.

**Alternatives.**
- (a) Link `legion-common` (Nickel, structs) and jobspec validation (which
  lives in `legion-worker`) into the server.
- (b) Reimplement manifest validation in the server.

**Trade-off.**
- One process per publish costs about 100 ms.
- The registry validates with *one* Legion version, the one in its image, so
  it can't check a package against every version in its `legion` range.

**Why (simplicity, and a safety property we need anyway).**
- "`legion package validate` passes but publish fails" can't happen, because
  it's the same code.
- Nickel evaluation can't be interrupted in-process, which is why the
  manager's template renderer abandons threads (`docs/job-templates.md`). A
  subprocess can simply be killed.
- The server stays a small crate: axum, sqlx, and the protocol crate.

## 12. Breaking changes on a minor or patch version are blocked at publish, with an override

**Call.** At publish, the registry compares the new version with the previous
one on the same major line, using the per-kind rules in plan §6.3: removed
variables, narrowed schemas, dropped platforms. It refuses a breaking change
that isn't a major bump unless the publisher gives a reason:

```console
$ legion package publish
error: 1.3.0 looks breaking: variable 'size' was removed (it exists in 1.2.4)
hint:  publish as 2.0.0, or pass --accept-breaking "<why this is safe>" (shown on the version page)
```

**Alternatives.**
- (a) Warn only.
- (b) Block with no override, as Elm does.

**Trade-off.**
- False positives cost authors a flag and a sentence.
- The detector is code we maintain per kind.

**Why (simplicity for the consumer).**
- "Upgrades within a major are safe" is only worth something if it's mostly
  true, and it's the promise that makes `app upgrade` defaulting to the
  current major reasonable.
- A warning goes unread.
- An override with a public reason keeps the rule honest without making it a
  wall.

## 13. GitHub is the only sign-in, and owners are GitHub users and organizations

**Call.** Web and CLI sign-in go through GitHub: OAuth on the web, and the
device flow for `legion registry login`. An owner namespace is a GitHub login
or org, with org membership refreshed from GitHub. `legion/` is reserved.

**Alternatives.**
- (a) Email and password accounts.
- (b) Several identity providers at launch.
- (c) Registry-native organizations decoupled from GitHub.

**Trade-off.**
- Contributors without GitHub can't publish.
- Renaming a GitHub account needs an admin-assisted transfer.

**Why (simplicity and trust).**
- Contributors already live on GitHub, and their packages' source is there.
- Trusted publishing and linked repositories are GitHub features, and a
  GitHub-verified owner gives a cheap first trust signal.
- Not storing passwords removes a whole class of incident.

## 14. Analytics live in Postgres, with daily-salted client hashes and no client telemetry

**Call.**
- Download, search, and page-view events go to a month-partitioned
  Postgres table and are rolled up nightly.
- Raw events are kept 90 days and rollups forever.
- No IP addresses are stored. Unique clients are
  `sha256(ip ‖ ua ‖ daily_salt)`, with the salt destroyed daily.
- The CLI sends only its version, client kind, and a `ci=1` hint in its
  `User-Agent`.

**Alternatives.**
- (a) ClickHouse or a hosted product-analytics tool.
- (b) Opt-in CLI telemetry for richer data, such as install success and
  failure.
- (c) Store IPs for abuse investigation.

**Trade-off.**
- The admin can't follow a client across days, and can't see install
  failures, only downloads.
- Heavy analytical queries share the database with the API. The rollups keep
  the dashboards off raw events.

**Why (simplicity and trust).**
- A registry that clusters fetch from must not become a tracking point, and
  "we don't keep IPs" is a sentence we want to be able to say.
- Postgres keeps the stack at one database.
- "Who downloaded this" for an incident works at the granularity of the
  daily hash, Legion version, and country, which is what a takedown notice
  needs.

## 15. Git registry versions are tags named `<package>/v<version>`

**Call.**
- In a git registry, a version exists when a tag `<package>/v<version>`
  points at a commit whose manifest declares that version.
- A single-package repository uses `v<version>`.
- Branch and commit references install, recorded as `unversioned: pinned to
  commit …`.

**Alternatives.**
- (a) A directory per version (`packages/redis/7.4.1/`).
- (b) The manifest's `version` at the default branch's head is the only
  version.
- (c) Whole-repository versioning by ref, as Nomad Pack does.

**Trade-off.**
- Releasing needs a tag, not just a merge.
- Tags can be moved, which is caught only on re-fetch by a cluster that
  recorded the digest.

**Why (simplicity for contributors).**
- Tagging is the release act git users already know, and Go uses this exact
  convention for multi-module repositories.
- (a) means copying files for every release.
- (b) has no history, so nothing could ever be pinned.
- (c) forces one release cadence on every package in the repo.
- The hosted registry's "publish on tag" (Phase 3) reads the same tags, so a
  git registry upgrades to the hosted one without changing habits.

## 16. `requires` is checked and reported, never resolved

**Call.** A package may declare `requires = { plugins, apps, runtimes }`.
Install checks them and refuses with the missing list. Nothing is installed
transitively.

**Alternatives.**
- (a) Full dependency resolution with automatic installs, like Helm subcharts
  or package managers.

**Trade-off.** Installing a template that needs a plugin and an app takes
three commands the first time. The error lists all three.

**Why (simplicity).**
- Each of those installs has its own consent and targeting: app permissions,
  plugin node selection. Doing them implicitly would hide exactly the
  decisions the operator must make.
- Helm's subchart value scoping shows what transitive packages cost in
  explanation.

## 17. Yank, deprecate, and take down are three different actions

**Call.**
- **Yank** (owner): out of resolution, still fetchable by exact version.
- **Deprecate** (owner, whole package): resolves, but the CLI warns.
- **Take down** (admin): bytes deleted, `410`, and the version number can
  never be reused.

**Alternatives.**
- (a) Let owners delete versions, as npm did before left-pad.
- (b) One "remove" action with flags.

**Trade-off.** Owners can't make a version disappear, even one published by
mistake; they yank it and publish a fix. Takedowns break running clusters'
re-fetches, on purpose.

**Why (simplicity and stability).** Clusters and node configs pin exact
versions and digests. Deletion by owners would break them without warning,
which is the left-pad lesson. A takedown exists only for malware and legal
cases, where breaking re-fetches is the point.

## 18. Plugin installs are gated by `operator:write`, not a new ACL resource

**Call.** `InstallPlugin`, `UpgradePlugin`, and `UninstallPlugin` check
`operator:write`; status reads check `node:read`. Registry list changes check
`operator:write`. `ListRegistries` is authenticated-open.

**Alternatives.**
- (a) A new `plugin` resource with per-type scopes, so someone could install
  volume drivers but not task runtimes.
- (b) A `registry` resource.

**Trade-off.** No delegation finer than "operator". Someone who should only
manage volume drivers needs full operator power.

**Why (simplicity and honesty about the boundary).** A node plugin is code
running as root on every machine it touches, whatever its type. Per-type
scopes would suggest a boundary that doesn't exist. Revisit if WASM-only
plugin types grow, since those genuinely are sandboxed.

## 19. No transparency log in v1

**Call.** Integrity comes from:
- digests everywhere;
- immutability enforced by the hosted registry;
- recorded digests in clusters, which catch a moved tag or changed bytes on
  re-fetch;
- registry-signed version documents in Phase 7.

There is no append-only public log of `(package, version, digest)`.

**Alternatives.**
- (a) A Go-style checksum database (`sum.golang.org`) or Sigstore Rekor
  entries for every publish, so a registry that serves different bytes to
  different clients can be caught.

**Trade-off.** A compromised registry *server* could serve a targeted client
a different, correctly signed version document on its first install, and
nothing would notice.

**Why (simplicity, for now).** A log needs witnesses, monitoring, and a
client that checks inclusion proofs. That's a project on its own, and it
protects against a threat that is further down the list than unreviewed code
from honest-but-careless publishers. The version-document format leaves room
for it: `schema` is versioned.
