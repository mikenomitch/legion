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
  # PauseScaling (scale:write); for node groups, GetCapacityDemand, GetNode
  # and GetNodes (node:read), CreateJoinToken, SetNodeAvailability and
  # ForgetNode (node:write), and the job names on a member (job:read; it
  # can read jobs, not change them); for its replicas' shard leases,
  # AcquireKvLock, ReleaseKvLock and ListKv under legion/apps/autoscaler/
  # (kv, reserved: a tenant's `paths = ["*"]` does not reach it).
  # ReportScalingStatus and RecordScalingAction are app-plane: the manager
  # accepts them from this app's identity alone, with no rule needed.
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
    { resources = ["job"], actions = ["read"], namespaces = ["*"] },
    { resources = ["node"], actions = ["read", "write"] },
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/autoscaler/*"] },
  ],
  # The autoscaler's own objects (docs/app-objects.md; docs/autoscaling.md,
  # "Standalone policies" and "Node groups"), written with `legion object
  # apply`. Each kind's `schema` and `status.schema` are generated from the
  # parser's own types when the manifest loads
  # (src/apps/autoscaler/objects.rs, `fill_manifest`), so the manager's
  # schema check and the app's parser cannot drift apart. What a schema
  # cannot say across fields (min <= max, unique check names) is each
  # kind's `rules`, CEL the manager evaluates on every apply, fail-closed,
  # whether or not the app is up. What needs the cluster (a plugin closed to
  # the namespace, a strategy's settings, durations) stays with the app: its
  # validate call reports it at apply, as advice, and the engine holds such
  # an object `Accepted=false`. The app reads both kinds over WatchObjects
  # and reports their durable verdicts with UpdateObjectStatus, which owning
  # the kinds allows with no rule.
  kinds = [
    {
      name = "ScalingPolicy",
      description = "A scaling policy for a target outside Legion, run through a target plugin.",
      scope = "namespaced",
      # Governed by `scale`, namespace-wide: whoever may scale in a namespace
      # writes its policies, as `legion job scale` needs.
      access = { resource = "scale" },
      validate = true,
      # A policy that moves to another target is a new policy.
      immutable = ["/spec/target/plugin"],
      # The parser's cross-field checks, enforced by the manager at apply
      # (the parser repeats them; the validate call reports the rest).
      rules = [
        { rule = "self.min <= self.max", message = "min must not exceed max", path = "/spec/min" },
        { rule = "self.max >= 1", message = "max must be at least 1", path = "/spec/max" },
        {
          rule = "self.checks.all(c, self.checks.exists_one(d, d.name == c.name))",
          message = "every check needs its own name",
          path = "/spec/checks",
        },
        {
          rule = "self.checks.all(c, c.source != 'legion' || !has(c.query) || c.query == '')",
          message = "a check on the legion source reads a Legion task, and a policy has none: leave its query out",
          path = "/spec/checks",
        },
        {
          rule = "self.checks.all(c, !has(c.activation) || c.activation >= 0)",
          message = "a check's activation is 0 or more",
          path = "/spec/checks",
        },
        {
          rule = "!has(self.tolerance) || (self.tolerance >= 0 && self.tolerance < 1)",
          message = "tolerance is at least 0 and below 1 (a fraction of the target: 0.1 is ±10%)",
          path = "/spec/tolerance",
        },
        {
          rule = "!has(self.schedule) || self.schedule.all(s, has(s.min) || has(s.max))",
          message = "a schedule window sets min, max, or both",
          path = "/spec/schedule",
        },
        {
          rule = "!has(self.schedule) || self.schedule.all(s, !has(s.min) || !has(s.max) || type(s.min) != int || type(s.max) != int || s.min <= s.max)",
          message = "a schedule window's min must not exceed its max",
          path = "/spec/schedule",
        },
        {
          rule = "!has(self.up) || !has(self.up.step) || has(self.up.step.percent) || has(self.up.step.replicas)",
          message = "up.step names percent, replicas, or both",
          path = "/spec/up/step",
        },
        {
          rule = "!has(self.down) || !has(self.down.step) || has(self.down.step.percent) || has(self.down.step.replicas)",
          message = "down.step names percent, replicas, or both",
          path = "/spec/down/step",
        },
        {
          rule = "!has(self.down) || !has(self.down.step) || !has(self.down.step.percent) || self.down.step.percent <= 100",
          message = "down.step.percent is at most 100: scale-in removes at most every replica",
          path = "/spec/down/step/percent",
        },
      ],
      examples = [
        {
          target = { plugin = "noop" },
          min = 0,
          max = 10,
          checks = [
            {
              name = "queue",
              source = "team-prom",
              query = "sum(queue_depth)",
              strategy = { name = "per-replica", value = 10 },
            },
          ],
        },
      ],
      columns = [
        { name = "TARGET", pointer = "/spec/target/plugin" },
        "/spec/min",
        "/spec/max",
        { name = "MODE", pointer = "/spec/mode" },
      ],
    },
    {
      name = "NodeGroup",
      description = "A pool of workers a target plugin starts and stops: grown for waiting work, drained and terminated when empty.",
      scope = "cluster",
      # Writing (or deleting) one adds or removes machines: node:write, and
      # operator:write for the cloud spend.
      access = { resource = "node" },
      requires = [{ resource = "operator", actions = ["write"] }],
      validate = true,
      # Its members belong to the provider that started them.
      immutable = ["/spec/target/nodes/plugin"],
      rules = [
        { rule = "!has(self.min) || self.min <= self.max", message = "min must not exceed max", path = "/spec/min" },
        {
          rule = "self.max >= 1",
          message = "max must be at least 1: a group that should never run a node does not need a policy",
          path = "/spec/max",
        },
        {
          rule = "!has(self.target.nodes.shape) || !has(self.target.nodes.shape.attributes) || !('legion.nodegroup' in self.target.nodes.shape.attributes)",
          message = "shape.attributes may not set legion.nodegroup: every member gets it from the group's name",
          path = "/spec/target/nodes/shape/attributes",
        },
        {
          rule = "!has(self.up) || !has(self.up.step) || has(self.up.step.percent) || has(self.up.step.replicas)",
          message = "up.step names percent, replicas, or both",
          path = "/spec/up/step",
        },
        {
          rule = "!has(self.down) || !has(self.down.step) || has(self.down.step.percent) || has(self.down.step.replicas)",
          message = "down.step names percent, replicas, or both",
          path = "/spec/down/step",
        },
      ],
      # Deleting a group drains, terminates, and forgets its members through
      # the scale-in path, then releases the object; at the deadline it goes
      # anyway, leaving any member the path could not drain.
      cleanup = "15m",
      examples = [
        {
          target = {
            nodes = {
              pool = "default",
              shape = { cpu = 8000, memory = 16384 },
              plugin = "aws-asg",
              config = { asg = "legion-workers" },
            },
          },
          min = 0,
          max = 20,
        },
      ],
      columns = [
        { name = "PLUGIN", pointer = "/spec/target/nodes/plugin" },
        { name = "POOL", pointer = "/spec/target/nodes/pool" },
        "/spec/min",
        "/spec/max",
      ],
    },
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
        identities = {
          type = "array",
          description = "Workload identities the app's job gets beside its own `legion` one, as a task's `identities` (docs/workload-identity.md): a token whose audience a cloud trusts, for a plugin to exchange for credentials (`aws-asg`'s webIdentityTokenFile), so no access keys appear anywhere.",
          items = { type = "object" },
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
              concurrency = {
                type = "integer",
                minimum = 1,
                maximum = 64,
                description = "The most calls each replica has in flight to it at once. Default 8, and 1 for a program plugin (source or command), which may not set more: it answers one request at a time.",
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
  # an address it reads with that grant; and the identities its job holds.
  # Changing any of them needs the app's whole grant.
  codeFields = [
    { pointer = "/plugins/*/source", artifact = true },
    "/plugins/*/command",
    "/plugins/*/url",
    # An `exec` target's command: a program the app runs.
    "/plugins/*/config/command",
    # A credential the app's job holds: who it can act as elsewhere.
    "/identities",
  ],
}
