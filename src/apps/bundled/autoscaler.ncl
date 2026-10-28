# The bundled autoscaler's manifest (docs/apps.md; apps plan §6.1). It is
# compiled into the binary and seeded by the first Control leader of a
# cluster, with its permissions granted by `legion:bootstrap`. It has no
# `version`: the app is the node's own binary (`legion:self`), so its version
# is the build its job was rendered for.
{
  name = "autoscaler",
  description = "Scales tasks and node groups from scaling policies.",
  placement = "managers",
  replicas = 1,
  job = {
    priority = "critical",
    task = {
      name = "agent",
      runtime = "exec",
      config = { command = ["legion:self", "app", "run", "autoscaler"] },
      resources = { cpu = 200, memory = 256 },
      # The app's HTTP port: `/healthz` now, the UI views later. Not a
      # catalog service.
      network = { ports = [{ name = "api" }] },
      checks = [
        {
          type = "health",
          name = "autoscaler /healthz",
          kind = "http",
          path = "/healthz",
          port = "api",
          every = "10s",
          timeout = "2s",
        },
      ],
    },
  },
  # Exactly what the app calls: WatchScalingPolicies and GetScalingInputs
  # (scale:read) and ScaleTask (scale:write). ReportScalingStatus and
  # RecordScalingAction are app-plane: the manager accepts them from this
  # app's identity alone, with no rule needed. Node groups and sharding add
  # node and kv rules when the app calls those APIs.
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
  ],
  ui = { port = "api" },
  config = {
    schema = {
      type = "object",
      additionalProperties = false,
      properties = {
        interval = {
          type = "string",
          description = "How often every policy is evaluated, as a duration (\"15s\", the default). The app counts as not running after three intervals without a report.",
        },
        mode = {
          type = "string",
          enum = ["enforce", "dry-run"],
          description = "`dry-run` forces every policy into dry-run: decisions are recorded as \"would\" and nothing is scaled. Default `enforce`: each policy's own mode applies.",
        },
        plugins = {
          type = "object",
          description = "Scaling plugins (sources, strategies, targets), by name (docs/autoscaling.md, \"Plugins\"). Policies name a plugin; they never carry a command or a URL. Set exactly one of builtin, source/command, or url.",
          additionalProperties = {
            type = "object",
            additionalProperties = false,
            properties = {
              builtin = {
                type = "string",
                enum = ["prometheus", "http", "exec", "noop"],
                description = "A plugin built into the autoscaler: the prometheus and http sources, the exec and noop targets.",
              },
              source = { type = "string", description = "An artifact the app's job fetches into its task directory, at code/plugins/<name>/ (archives are unpacked)." },
              checksum = { type = "string", description = "The artifact's checksum, `sha256:<hex>`." },
              command = {
                type = "array",
                items = { type = "string" },
                minItems = 1,
                description = "The program and its arguments, speaking JSON-RPC on stdin and stdout. With source, relative paths resolve in the plugin's directory; the default is ./<name> there.",
              },
              url = { type = "string", description = "A plugin served over HTTP: each JSON-RPC request is a POST." },
              config = { type = "object", description = "The plugin's own settings, sent once when it starts (a builtin's address, url, or command)." },
              namespaces = {
                type = "array",
                items = { type = "string" },
                description = "Only policies in these namespaces (names or *-globs) may use it. Default: every namespace.",
              },
            },
          },
        },
      },
    },
    default = { plugins = {} },
  },
  # What the app runs: a plugin's source is fetched as an artifact of the
  # app's job, a command is a program it starts, a URL plugin is code it
  # calls with its grant, and a builtin's config names a command (exec) or
  # an address it reads with that grant. Changing any of them needs the
  # app's whole grant.
  codeFields = [
    { pointer = "/plugins/*/source", artifact = true },
    "/plugins/*/command",
    "/plugins/*/url",
    # An `exec` target's command: a program the app runs.
    "/plugins/*/config/command",
  ],
}
