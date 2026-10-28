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
  # `legion` identity carrying the grant, `user = systemWorker.appUser` for
  # an exec task that names none (tasks in legion-system may not run as
  # root), and the config as a file named by $LEGION_APP_CONFIG.
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
      # The app's HTTP port: health now, UI views later. Not a catalog
      # service.
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
    { resources = ["job"], actions = ["read", "write"], namespaces = ["team-*"] },
    { resources = ["template"], actions = ["read"] },
    # Apps keep their state under the reserved `legion/` KV prefix, which a
    # plain `*` path never reaches.
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/gitops/*"] },
  ],

  # UI registrations (apps plan §6.5). Stored and listed by `legion app list`
  # (UI column); rendered by a later release.
  ui = {
    port = "api",
    nav = [{ title = "GitOps", view = "repos" }],
    job = [
      { title = "Sync", view = "job-sync", permission = { resource = "job", action = "read" } },
    ],
    actions = [
      {
        name = "sync-now",
        title = "Sync now",
        permission = { resource = "job", action = "write" },
      },
    ],
    refresh = "10s",
  },

  # Jobspec keys the app adds (apps plan §6.6). Stored; resolved by a later
  # release.
  extensions = [
    {
      point = "job",
      key = "gitops",
      description = "Where this job came from.",
      schema = {
        type = "object",
        properties = {
          repo = { type = "string" },
          path = { type = "string" },
          commit = { type = "string" },
        },
      },
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
