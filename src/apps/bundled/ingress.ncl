# The bundled ingress's manifest (docs/ingress.md; ingress plan §6). It is
# compiled into the binary and seeded by the first Control leader of a
# cluster, disabled: it takes ports 80 and 443 on the nodes it runs on, so
# it waits for `legion app enable ingress` (or `apps.ingress.enabled = true`
# in the manager config). It has no `version`: the app is the node's own
# binary (`legion:self`).
let host_pattern = "^(\\*\\.)?([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.)*[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$" in
# Named apart from the `host` field below: Nickel records are recursive, so
# a field would see itself under the same name.
let host_name = {
  type = "string",
  pattern = host_pattern,
  maxLength = 253,
} in
{
  name = "ingress",
  description = "Routes public hostnames to services, with automatic HTTPS.",
  placement = "managers",
  replicas = 1,
  job = {
    priority = "critical",
    task = {
      name = "proxy",
      runtime = "exec",
      config = {
        command = ["legion:self", "app", "run", "ingress"],
        # Ports 80 and 443 without root: the one capability an exec task may
        # keep (docs/task-runtime-exec.md). The app runs as its own uid.
        hardening = { capabilities = { add = ["net_bind_service"] } },
      },
      resources = { cpu = 500, memory = 256 },
      network = {
        ports = [
          # Static: the scheduler places at most one replica per node, so
          # `legion app set ingress --replicas 3` spreads three nodes.
          { name = "http", static = 80 },
          { name = "https", static = 443 },
          # `/healthz` and the UI views. Not public.
          { name = "api" },
        ],
      },
      checks = [
        {
          type = "health",
          name = "ingress /healthz",
          kind = "http",
          path = "/healthz",
          port = "api",
          every = "10s",
          timeout = "2s",
        },
      ],
    },
  },
  # Exactly what the app calls: ResolveService for every routed service, in
  # any tenant namespace (job:read; it reads endpoints, never changes a
  # job), and its own KV under the reserved legion/apps/ingress/: operator
  # certificate bundles, the certificates and ACME account it keeps, HTTP-01
  # answers, its replicas' addresses, and the issuer lock. WatchAppExtensions
  # is app-plane: the manager answers it for this app's identity alone.
  permissions = [
    { resources = ["job"], actions = ["read"], namespaces = ["*"] },
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/ingress/*"] },
  ],
  # The route: `service = { port = "http", ingress = { host = "…" } }`.
  extensions = [
    {
      point = "service",
      key = "ingress",
      description = "Route a public hostname (and optionally a path prefix) to this service through the ingress app, with an automatic HTTPS certificate (docs/ingress.md).",
      schema = {
        type = "object",
        additionalProperties = false,
        required = ["host"],
        properties = {
          host = {
            description = "The hostname to route, or a list of up to 16. `*.example.com` matches any one label under example.com. Case-insensitive.",
            oneOf = [
              host_name,
              { type = "array", items = host_name, minItems = 1, maxItems = 16, uniqueItems = true },
            ],
          },
          path = {
            type = "string",
            description = "Route only requests under this path prefix, matched on segment boundaries: `/api` matches `/api` and `/api/users`, not `/apix`. Default `/`, everything.",
            pattern = "^/[^?#\\s]*$",
            maxLength = 256,
            default = "/",
          },
          stripPath = {
            type = "boolean",
            description = "Remove `path` from the request before it reaches the service: with `path = \"/api\"`, `/api/users` arrives as `/users`. Default false.",
            default = false,
          },
        },
      },
      examples = [
        { host = "shop.example.com" },
        { host = ["example.com", "www.example.com"] },
        { host = "example.com", path = "/api", stripPath = true },
      ],
      # The app refuses a host and path another service already holds.
      validate = true,
    },
  ],
  ui = {
    port = "api",
    nav = [
      { title = "Ingress", icon = "network", view = "routes",
        permission = { resource = "job", action = "read" } },
    ],
    job = [
      { title = "Ingress", view = "job",
        permission = { resource = "job", action = "read" } },
    ],
    refresh = "10s",
  },
  config = {
    schema = {
      type = "object",
      additionalProperties = false,
      properties = {
        acme = {
          type = "object",
          additionalProperties = false,
          description = "Automatic certificates for routed hosts, from an ACME CA over HTTP-01 on port 80.",
          properties = {
            enabled = {
              type = "boolean",
              description = "Order certificates for routed hosts. Default true: enabling the app agrees to the CA's terms of service. Set false behind a load balancer that terminates TLS.",
            },
            directory = {
              type = "string",
              description = "`letsencrypt` (the default), `letsencrypt-staging`, or an ACME directory URL (https://).",
            },
            email = {
              type = "string",
              description = "The account's contact address. Optional.",
            },
            trustRoots = {
              type = "string",
              description = "Extra PEM root certificates to trust when talking to the directory: a private ACME CA such as step-ca or Pebble.",
            },
          },
        },
        redirectHttps = {
          type = "boolean",
          description = "Answer plain HTTP with a 308 redirect to HTTPS for every host that has a certificate. Default true.",
        },
      },
    },
    default = {},
  },
  # Where the app sends its account key's signatures and which CA it trusts:
  # changing either changes who can obtain certificates for the cluster's
  # hosts, so it needs the app's whole grant.
  codeFields = [
    "/acme/directory",
    "/acme/trustRoots",
  ],
}
