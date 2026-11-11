# The bundled GitOps app's manifest (docs/gitops.md; gitops plan §6). It is
# compiled into the binary and seeded by the first Control leader of a
# cluster, with its permissions granted by `legion:bootstrap`. It has no
# `version`: the app is the node's own binary (`legion:self`).
{
  name = "gitops",
  description = "Deploys jobs from git repositories and keeps them deployed.",
  placement = "managers",
  replicas = 1,
  job = {
    priority = "high",
    task = {
      name = "sync",
      runtime = "exec",
      config = { command = ["legion:self", "app", "run", "gitops"] },
      # git, and one renderer process per changed file of a new commit.
      resources = { cpu = 200, memory = 256 },
      network = { ports = [{ name = "api" }] },
      checks = [
        {
          type = "health",
          name = "gitops /healthz",
          kind = "http",
          path = "/healthz",
          port = "api",
          every = "10s",
          timeout = "2s",
        },
      ],
    },
  },
  # Every rule is `via = "objects"`: gitops's own credential can do nothing
  # at all. Each call it makes for a Repo names the Repo
  # (`legion-for-object`), and the manager holds the call to what the
  # Repo's writer was checked for when they applied it (its `requires`
  # below, recorded as its authority), within these rules: reading and
  # writing the jobs the Repo may deploy (GetJobInfo, ListJobSpecHistory,
  # ListRollouts, PlanJob, RunJob, StopJob), host-level writes only for a
  # Repo with `hostLevel`, and the exact KV keys it names (its token, and
  # the keys its jobs read). `*` reaches neither `legion-system` nor the
  # reserved `legion/` KV prefix. WatchObjects, UpdateObjectStatus, and
  # ReleaseObject are the app plane: owning the kind is enough.
  permissions = [
    { resources = ["job"], actions = ["read", "write"], namespaces = ["*"], via = "objects" },
    { resources = ["job"], actions = ["write"], namespaces = ["*"], host = true, via = "objects" },
    { resources = ["kv"], actions = ["read"], paths = ["*"], via = "objects" },
  ],
  kinds = [
    {
      name = "Repo",
      description = "A git repository whose jobs gitops deploys into this namespace, and keeps deployed.",
      scope = "namespaced",
      access = { resource = "object" },
      validate = true,
      # A Repo can make gitops deploy and read every job it may deploy, so
      # its writer must be able to: namespace-wide, or only the jobs
      # `spec.jobs` lists. Host-level jobs (an `exec` task, for one) also
      # need a host-level grant, asked for with `hostLevel`. Its token and
      # the KV keys its jobs read need kv:read on each.
      requires = [
        { resource = "job", actions = ["read", "write"], name = "/spec/jobs" },
        { resource = "job", actions = ["write"], name = "/spec/jobs", host = true, when = "/spec/hostLevel" },
        { resource = "kv", actions = ["read"], path = "/spec/credentials/kv" },
        { resource = "kv", actions = ["read"], path = "/spec/kvKeys" },
      ],
      schema = {
        type = "object",
        required = ["url"],
        properties = {
          url = {
            type = "string",
            description = "The repository to deploy from: an https:// URL (http:// without credentials; file:// when the app's config sets allowFileUrls). SSH URLs are not supported.",
          },
          ref = {
            type = "string",
            default = "main",
            description = "What to deploy: a branch, a tag, or a full 40-character commit SHA. A branch or tag is followed: each new commit is deployed.",
          },
          path = {
            type = "string",
            default = ".",
            description = "The directory, relative to the repository root, whose .ncl and .json files are the jobspecs. Subdirectories are not read (put shared Nickel there and import it), and files starting with . or _ are skipped.",
          },
          interval = {
            type = "string",
            default = "1m",
            description = "How often gitops looks for a new commit, as a duration (\"1m\"). At least 10s. `legion gitops sync` checks at once.",
          },
          jobs = {
            type = "array",
            items = { type = "string" },
            description = "Only these jobs may be deployed from the repository. Writing the Repo then needs job:read and job:write on them alone, instead of namespace-wide. At most 20.",
          },
          prune = {
            type = "boolean",
            default = true,
            description = "Stop a job whose file leaves the repository, and stop every job the Repo deployed when it is deleted. Jobs are stopped, never destroyed.",
          },
          suspend = {
            type = "boolean",
            default = false,
            description = "Stop syncing: nothing is deployed, written back, or stopped. The jobs keep running.",
          },
          hostLevel = {
            type = "boolean",
            default = false,
            description = "The repository's jobs include host-level tasks (an exec task, a container on the host network, ...). Writing the Repo then also needs job:write with host = true.",
          },
          credentials = {
            type = "object",
            required = ["kv"],
            description = "For a private repository: an HTTPS token read from KV, sent as basic auth. Writing the Repo needs kv:read on the key.",
            properties = {
              kv = { type = "string", description = "The KV key holding the token (a GitHub or GitLab personal access token, say)." },
              username = {
                type = "string",
                default = "x-access-token",
                description = "The basic-auth username sent with the token. GitHub and GitLab accept any; Bitbucket wants x-token-auth.",
              },
            },
          },
          kvKeys = {
            type = "array",
            items = { type = "string" },
            description = "The KV keys the repository's jobs read ({ kv = ... } values). A submit needs kv:read on every key a job reads, so writing the Repo needs it on each.",
          },
        },
      },
      status = {
        schema = {
          type = "object",
          properties = {
            commit = { type = "string", description = "The commit whose jobs are deployed." },
            short = { type = "string", description = "The deployed commit, abbreviated." },
            latest = { type = "string", description = "The commit `ref` pointed at when last checked; differs from commit while it cannot be deployed." },
            subject = { type = "string", description = "The deployed commit's subject line." },
            syncedAt = { type = "string", description = "When the deployed commit was applied (RFC 3339)." },
            syncRequest = { type = "string", description = "The last `gitops/sync` label value gitops handled." },
            deployed = { type = "integer", description = "How many jobs the Repo deployed." },
            jobs = {
              type = "array",
              description = "The jobs the Repo deployed: its inventory, which prune and drift use.",
              items = {
                type = "object",
                properties = {
                  name = { type = "string" },
                  file = { type = "string" },
                  digest = { type = "string" },
                  version = { type = "string" },
                  state = { type = "string", enum = ["deployed", "progressing", "failed", "stopped", "pending"] },
                  detail = { type = "string" },
                },
              },
            },
          },
        },
      },
      # What a schema cannot say, checked by the manager on every apply,
      # whether or not the app is up.
      rules = [
        {
          rule = "self.url.startsWith('https://') || self.url.startsWith('http://') || self.url.startsWith('file://')",
          message = "url must be an https:// repository URL (or http://, or file:// on a cluster that allows it); SSH URLs are not supported yet",
          path = "/spec/url",
        },
        {
          rule = "!has(self.credentials) || self.url.startsWith('https://')",
          message = "credentials are sent only to an https:// URL",
          path = "/spec/credentials",
        },
        { rule = "size(self.ref) > 0", message = "ref names a branch, a tag, or a commit", path = "/spec/ref" },
        {
          rule = "!self.path.startsWith('/') && !self.path.contains('..')",
          message = "path is a directory inside the repository, relative to its root",
          path = "/spec/path",
        },
        {
          rule = "!has(self.jobs) || (size(self.jobs) > 0 && size(self.jobs) <= 20)",
          message = "jobs lists 1 to 20 jobs; leave it out to deploy any job in the namespace",
          path = "/spec/jobs",
        },
        {
          rule = "!has(self.jobs) || self.jobs.all(j, size(j) > 0 && !j.contains('*') && !j.contains('/') && self.jobs.exists_one(k, k == j))",
          message = "jobs are distinct job names in the Repo's namespace, without globs or a namespace",
          path = "/spec/jobs",
        },
        {
          rule = "!has(self.kvKeys) || self.kvKeys.all(k, size(k) > 0 && !k.contains('*'))",
          message = "kvKeys are exact KV keys, without globs",
          path = "/spec/kvKeys",
        },
        {
          rule = "!has(self.credentials) || (size(self.credentials.kv) > 0 && !self.credentials.kv.contains('*'))",
          message = "credentials.kv is an exact KV key",
          path = "/spec/credentials/kv",
        },
      ],
      # Deleting a Repo with prune stops the jobs it deployed, then releases
      # it; `legion object delete --now` skips that and leaves them running.
      cleanup = "2m",
      maxObjects = 1000,
      examples = [
        { url = "https://github.com/acme/deploy", path = "storefront" },
        {
          url = "https://github.com/acme/private-deploy",
          ref = "release",
          path = "jobs",
          interval = "5m",
          jobs = ["api", "worker"],
          credentials = { kv = "secret/gitops/github-token" },
          kvKeys = ["secret/api/database-url"],
        },
      ],
      columns = [
        { name = "REF", pointer = "/spec/ref" },
        { name = "COMMIT", pointer = "/status/fields/short" },
        { name = "JOBS", pointer = "/status/fields/deployed" },
      ],
    },
  ],
  # The app's UI, as declarative views (docs/gitops.md, "In the UI").
  ui = {
    port = "api",
    nav = [
      { title = "GitOps", icon = "gitBranch", view = "repos",
        permission = { resource = "job", action = "read" } },
    ],
    job = [
      { title = "GitOps", view = "job", permission = { resource = "job", action = "read" } },
    ],
    actions = [
      { name = "sync", title = "Sync now",
        confirm = "Check {job}'s repository for a new commit and deploy it now?",
        permission = { resource = "job", action = "write" } },
    ],
    refresh = "10s",
  },
  config = {
    schema = {
      type = "object",
      additionalProperties = false,
      properties = {
        allowFileUrls = {
          type = "boolean",
          description = "Allow Repos with file:// URLs: git repositories on the disk of the nodes gitops runs on. For dev clusters and tests; anyone who may write a Repo can then deploy from any repository the app's user can read there.",
        },
      },
    },
    default = {},
  },
}
