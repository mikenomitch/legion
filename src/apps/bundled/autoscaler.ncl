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
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
    { resources = ["job"], actions = ["read"], namespaces = ["*"] },
    { resources = ["node"], actions = ["read", "write"] },
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/autoscaler/*"] },
  ],
  ui = { port = "api" },
  config = {
    schema = {
      type = "object",
      additionalProperties = false,
      properties = {
        plugins = {
          type = "object",
          description = "External scaling plugins, by name. Policies name a plugin; they never carry a command or a URL.",
          additionalProperties = {
            type = "object",
            additionalProperties = false,
            properties = {
              source = { type = "string", description = "Where to fetch the plugin from (an artifact source)." },
              checksum = { type = "string", description = "The artifact's checksum, `sha256:<hex>`." },
              url = { type = "string", description = "A plugin served over HTTP JSON-RPC instead of run by the app." },
              builtin = { type = "string", description = "A plugin built into the autoscaler." },
              config = { type = "object", description = "Passed to the plugin when it starts." },
              namespaces = {
                type = "array",
                items = { type = "string" },
                description = "Only policies in these namespaces may use it. Default: every namespace.",
              },
            },
          },
        },
      },
    },
    default = { plugins = {} },
  },
  # What the app runs: a plugin's source is fetched as an artifact of the
  # app's job, and a URL plugin is code the app calls with its grant. Changing
  # either needs the app's whole grant.
  codeFields = [
    { pointer = "/plugins/*/source", artifact = true },
    "/plugins/*/url",
  ],
}
