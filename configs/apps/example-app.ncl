# A complete third-party app manifest (docs/apps.md, "The manifest").
#
# An app is a job in the reserved `legion-system` namespace, a grant (what
# that job's `legion` identity may do in Legion), and registrations. Install
# it with:
#
#   legion app install configs/apps/example-app.ncl --dry-run   # the prompt only
#   legion app install configs/apps/example-app.ncl
#
# Installing needs `app:write` on the app's name and every permission listed
# under `permissions`: you can grant only what you hold. The app reconciler
# then keeps `legion-system/gitops` running from this manifest, your settings
# (`legion app set`), and your config (`legion app config set`).
{
  # The app's name, and its job's: legion-system/gitops. Lowercase letters,
  # digits and '-', starting with a letter, at most 40.
  name = "gitops",
  description = "Deploys jobs from git repositories.",
  # Required for an installed app. Bundled apps carry none.
  version = "0.2.0",

  # Where it runs by default: managers (their system workers; the default),
  # workers, or any. The operator can move it with `legion app set
  # gitops --placement workers`.
  placement = "managers",
  # Each task's count by default (1-32).
  replicas = 1,

  # A jobspec without `name` and `namespace`, which Legion sets. Legion also
  # renders: count (replicas), the permit and placement constraints, the
  # `legion` identity carrying the grant, the app's own uid as every exec
  # task's `user` (so leave `user` out; no two apps share one), and the
  # config as a file named by $LEGION_APP_CONFIG. An app's job may not hold
  # anything host-level besides that: no exec health check, no `file://`
  # artifact, no `auth.nodeIdentity`.
  job = {
    task = {
      name = "sync",
      runtime = "exec",
      # The app's own code, fetched like any job's artifact. Pin a checksum.
      artifacts = [
        {
          source = "https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz",
          checksum = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        },
      ],
      config = { command = ["./legion-gitops"] },
      resources = { cpu = 200, memory = 128 },
      # The app's HTTP port: health, and the views and actions below
      # (`ui.port`). Not a catalog service: the worker delivers the
      # manager's requests to it locally.
      network = { ports = [{ name = "api" }] },
      checks = [
        {
          type = "health",
          kind = "http",
          path = "/healthz",
          port = "api",
          every = "10s",
          timeout = "2s",
        },
      ],
    },
  },

  # What the app's `legion` identity may do, as ACL rules (the shape `legion
  # acl policy apply` takes). The installer must hold every one. Inside the
  # task, `legion ...` reaches the cluster through the allocation's API
  # socket with this grant, when workloadIdentity.enabled is on.
  permissions = [
    { resources = ["job"], actions = ["read"], namespaces = ["team-*"] },
    # `via = "objects"`: gitops writes jobs only while it acts for one of its
    # Repos (`legion-for-object`), and the manager holds each such call to
    # what that Repo's author was checked for when they applied it (the
    # Repo's recorded authority; see `requires` below). Its own credential
    # writes no job anywhere, and the install prompt says so: "only for its
    # objects". The installer must still hold it, as any permission here.
    # The jobs it writes this way are stamped `managedBy` the Repo
    # (docs/app-objects.md, "Acting for an object").
    { resources = ["job"], actions = ["write"], namespaces = ["*"], via = "objects" },
    { resources = ["template"], actions = ["read"] },
    # Apps keep their state under the reserved `legion/` KV prefix, which a
    # plain `*` path never reaches.
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/gitops/*"] },
  ],

  # UI registrations (docs/apps.md, "Views and actions"). The SPA draws the
  # app's declarative views (legion-view/1 JSON) in these slots, and
  # `legion app view gitops job web` prints them. The manager checks each
  # entry's `permission` as the viewer, on the job, task, or node in context,
  # before it asks the app anything; a viewer without it sees nothing.
  ui = {
    # The port label the app serves views and actions on (default "api").
    port = "api",
    # A page in the sidebar's Apps section. No context: views only.
    nav = [{ title = "GitOps", icon = "gitBranch", view = "repos" }],
    # A card on the cluster overview.
    overview = [{ title = "GitOps sync", view = "summary" }],
    # A panel on job pages, for viewers holding job:read on that job.
    job = [
      { title = "Sync", view = "job-sync", permission = { resource = "job", action = "read" } },
    ],
    # A badge on task rows (the `badge` view answers `{ badge = { text } }`).
    task = [{ badge = "task-source", permission = { resource = "job", action = "read" } }],
    # Buttons a view's `actions` block can show. Pressed on a job, task, or
    # node (never in nav or overview), checked as the viewer, audited as
    # `app.action`, and confirmed first when the permission is a write.
    # gitops answers `sync-now` by acting for the Repo that deploys the job:
    # acting as the viewer is bounded by its own grant, which (job:write
    # being `via = "objects"`) writes no job.
    actions = [
      {
        name = "sync-now",
        title = "Sync now",
        confirm = "Sync {job} from its repository now?",
        # Typed inputs: string, number, duration, enum (with `values`),
        # boolean. They can never name another target.
        inputs = { mode = { type = "enum", values = ["apply", "dry-run"], default = "apply" } },
        permission = { resource = "job", action = "write" },
      },
    ],
    # How often the SPA re-fetches the views (at least 5s).
    refresh = "10s",
    # Sandboxed pages, for UIs the view blocks cannot draw: single-file HTML
    # the app serves (at `bundle`, default /legion/v1/pages/<name>), mounted
    # by the SPA in an opaque-origin iframe with a strict CSP, in the
    # sidebar. `rpcs` is what the page may call as its viewer through the
    # bridge; writes (RunJob here) are confirmed by the viewer every time.
    # A page can read whatever its viewer can through these RPCs, so the
    # install prompt lists them (docs/apps.md, "Sandboxed pages").
    pages = [
      {
        name = "console",
        title = "GitOps console",
        icon = "gitBranch",
        rpcs = ["ListJobs", "GetJobInfo", "RunJob"],
        permission = { resource = "job", action = "read" },
      },
    ],
  },

  # Jobspec keys the app adds (docs/apps.md, "Jobspec extensions"): one key
  # per extension point (job, group, task, volume, port, service; `task`
  # and `group` count as one). Job authors write them where they belong
  # (configs/apps/example-app-job.ncl), or as `apps.gitops` at the same
  # point. Each value is checked against its schema at submit, stored as
  # written, and read back with the schema's defaults filled; the app reads
  # its values with WatchAppExtensions. Install refuses a key that is a core
  # field at its point or another installed app's key there (rename it with
  # `legion app install --key job.gitops=<key>`), a schema with a remote
  # $ref, and an example that fails its own schema.
  extensions = [
    {
      point = "job",
      key = "gitops",
      description = "Where this job came from, and whether gitops keeps it in sync.",
      schema = {
        type = "object",
        additionalProperties = false,
        required = ["repo", "path"],
        properties = {
          repo = { type = "string", description = "The repository URL." },
          path = { type = "string", description = "The jobspec's path in the repository." },
          commit = { type = "string", description = "The commit last synced." },
          autoSync = { type = "boolean", default = true, description = "Sync on every push." },
        },
      },
      examples = [{ repo = "https://github.com/acme/deploy", path = "jobs/storefront.ncl" }],
      # Also ask the app at submit (`POST /legion/v1/extensions/validate`):
      # is the path in the repository? Fails open: an app that cannot answer
      # within 2 seconds lets the submit through with a warning.
      validate = true,
    },
    {
      # Written on one task of a group; applies to the whole group.
      point = "group",
      key = "syncWave",
      description = "The wave this group syncs in; lower waves sync first.",
      schema = {
        type = "object",
        additionalProperties = false,
        properties = {
          wave = { type = "integer", minimum = 0, default = 0 },
        },
      },
      examples = [{ wave = 1 }],
      # Setting or changing the value needs more than job:write: the
      # submitter must also hold this permission on the job, checked when
      # the value is new or changed.
      requires = { resource = "template", action = "write" },
    },
  ],

  # Object kinds the app owns (docs/app-objects.md): Legion's custom
  # resources. People apply objects of them with `legion object apply`
  # (configs/apps/example-repo.ncl); the manager checks each against its
  # schema and what writing it requires of the writer, and stores it; the
  # app watches them (WatchObjects), does the work, and reports status. The
  # kind's name is `gitops/Repo`; `legion object get repo storefront` works
  # while no other app declares a Repo.
  kinds = [
    {
      # PascalCase, singular. Reserved: Legion's own nouns (job, node, ...).
      name = "Repo",
      description = "A git repository gitops deploys jobs from into this namespace.",
      # `namespaced` (the default) or `cluster`.
      scope = "namespaced",
      # The spec's JSON Schema. Closed by default: a field it does not name
      # is an error (with a did-you-mean). `default`s are stored with the
      # object at write.
      schema = {
        type = "object",
        required = ["url"],
        properties = {
          url = { type = "string", description = "The repository URL." },
          branch = { type = "string", default = "main", description = "The branch to deploy." },
          path = { type = "string", default = "jobs/", description = "The directory of jobspecs." },
          jobs = {
            type = "array",
            items = { type = "string" },
            description = "The jobs it may write; default every job in the namespace.",
          },
          # Shown as <redacted> to everyone but the app. A credential is
          # better kept in KV and named here by path.
          deployKey = {
            type = "string",
            writeOnly = true,
            description = "A read-only deploy key.",
          },
        },
      },
      # The schema of the status `fields` the app reports (facts, at most
      # 16 KiB), beside its Accepted and Ready conditions.
      status = {
        schema = {
          type = "object",
          properties = {
            commit = { type = "string" },
            jobs = { type = "integer" },
          },
        },
      },
      examples = [{ url = "https://github.com/acme/deploy", path = "jobs/storefront" }],
      # Also ask the app on each apply (`POST /legion/v1/objects/validate`).
      # Advice: it fails open when the app does not answer.
      validate = true,
      # What governs a Repo itself: `object` (the default) by namespace and
      # kind; or `job` / `scale` (namespace-wide) or `node` (cluster kinds).
      access = { resource = "object" },
      # What writing (or deleting) a Repo also needs of its writer, scoped
      # from the object: write on the jobs it names (`/spec/jobs`; every job
      # when it names none) in its namespace. What was checked is recorded
      # on the Repo as its authority, and bounds what gitops can do for it:
      # a Repo can never make gitops deploy more than its author could.
      requires = [
        { resource = "job", actions = ["write"], name = "/spec/jobs" },
      ],
      # Pointers an update may not change.
      immutable = ["/spec/url"],
      # CEL rules every apply must pass, checked by the manager whether or
      # not the app is up (docs/app-objects.md, "Validation rules"): what a
      # schema cannot say across fields. `self` is the spec with defaults;
      # a rule that uses `oldSelf` (the stored spec) checks updates only.
      rules = [
        {
          rule = "!has(self.deployKey) || self.url.startsWith('ssh://') || self.url.startsWith('git@')",
          message = "a deploy key is an SSH key: use an ssh:// or git@ URL with it",
          path = "/spec/deployKey",
        },
      ],
      # A delete waits up to this long for gitops to clean up (and release
      # the object); then the object is removed anyway.
      cleanup = "10m",
      # At most this many Repos (default 10,000).
      maxObjects = 500,
      # The columns `legion object list repos` shows.
      columns = ["/spec/branch", { name = "COMMIT", pointer = "/status/fields/commit" }],
    },
  ],

  # The app's config: a JSON Schema (draft 7, only local $refs) and the
  # config a fresh install starts with. `legion app config set` validates
  # against it; the app reads it from $LEGION_APP_CONFIG.
  config = {
    schema = {
      type = "object",
      additionalProperties = false,
      properties = {
        repos = {
          type = "array",
          items = {
            type = "object",
            required = ["url", "namespace"],
            properties = {
              url = { type = "string" },
              path = { type = "string" },
              namespace = { type = "string" },
            },
          },
        },
        hooks = {
          type = "object",
          additionalProperties = {
            type = "object",
            properties = {
              source = { type = "string" },
              checksum = { type = "string" },
              url = { type = "string" },
            },
          },
        },
      },
    },
    default = { repos = [], hooks = {} },
  },

  # Config that changes the code the app runs. Changing a value here needs
  # the caller to hold the app's whole grant, as installing does; other
  # config changes need only app:write. `artifact = true` also fetches the
  # value into the task directory (code/hooks/<name>/), with the sibling
  # `checksum` string as its checksum.
  codeFields = [
    { pointer = "/hooks/*/source", artifact = true },
    "/hooks/*/url",
  ],
}
