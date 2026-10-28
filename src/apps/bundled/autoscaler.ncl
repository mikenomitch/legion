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
      # The app's HTTP port: `/healthz`, and the UI views below. Not a
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
  # Exactly what the app calls: WatchScalingPolicies, GetScalingInputs,
  # GetScalingStatus, and GetScaleHistory (scale:read); ScaleTask and
  # PauseScaling (scale:write); and, for the node-group panel, GetNode,
  # GetNodes, and GetCapacityDemand (node:read). ReportScalingStatus and
  # RecordScalingAction are app-plane: the manager accepts them from this
  # app's identity alone, with no rule needed. Node-group scaling and
  # sharding add node:write and kv rules when the app calls those APIs.
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
    { resources = ["node"], actions = ["read"] },
  ],
  # The autoscaler's UI, as declarative views the SPA, `legion app view`,
  # and `legion_app_view` render (docs/autoscaling.md, "In the UI"). Each
  # entry is shown only to viewers holding its permission: on the job for
  # the panel and badges, on the node's pool for the node-group panel, and
  # somewhere for the sidebar page, which then lists only the namespaces
  # the viewer may read.
  ui = {
    port = "api",
    nav = [
      { title = "Autoscaling", icon = "gauge", view = "policies",
        permission = { resource = "scale", action = "read" } },
    ],
    job = [
      { title = "Autoscaling", view = "job",
        permission = { resource = "scale", action = "read" } },
    ],
    task = [
      { badge = "task-badge", permission = { resource = "scale", action = "read" } },
    ],
    node = [
      { title = "Node group", view = "node",
        permission = { resource = "node", action = "read" } },
    ],
    actions = [
      { name = "pause", title = "Pause autoscaling",
        confirm = "Pause autoscaling for every autoscaled task of {job}? People can still scale it by hand.",
        inputs = { for = { type = "duration", default = "1h", label = "For" } },
        permission = { resource = "scale", action = "write" } },
      { name = "resume", title = "Resume autoscaling",
        confirm = "Resume autoscaling for {job}?",
        permission = { resource = "scale", action = "write" } },
      { name = "release", title = "Release the pin",
        confirm = "Release the pin on {job}? The autoscaler takes the count over again, at its normal rate.",
        permission = { resource = "scale", action = "write" } },
    ],
    refresh = "10s",
  },
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
