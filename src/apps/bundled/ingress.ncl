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
  # One on every node it may run on (per-node plan, calls entry 1): every
  # manager, or every node of its pool after `legion app set ingress
  # --placement workers --pool edge`. A node that joins gets one. Every
  # replica is identical (certificates live in KV, HTTP-01 is stateless),
  # and each node it runs on is a front door DNS can point at.
  replicas = "per-node",
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
          # Static: at most one replica fits on a node, and per-node
          # replicas put one on each.
          { name = "http", static = 80 },
          { name = "https", static = 443 },
          # `/healthz` and the UI views. Not public.
          { name = "api" },
        ],
      },
      # The replicas, in the catalog: `legion service resolve
      # legion-system/ingress` lists every node the ingress answers on, with
      # its public address (LEGION_WORKER_PUBLIC_ADDRESS) when it has one.
      # The app reads the same list for its "Point DNS here" block.
      service = { name = "ingress", port = "http", meta = { protocol = "http" } },
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
  # any tenant namespace, and for its own (job:read; it reads endpoints,
  # never changes a job); its own KV under the reserved legion/apps/ingress/ (operator
  # certificate bundles, the bundles the cert manager issues for it, its
  # replicas' addresses, and its lock); and the cert manager's objects: the
  # certs/Certificate it keeps for each routed host, in legion-system only,
  # every certs/Issuer to read (its thumbprint answers HTTP-01), and the one
  # Issuer it keeps itself, named `ingress`. A Certificate's `requires` asks
  # its writer for kv read and write on the key it names, which this grant
  # holds for legion/apps/ingress/ alone. WatchAppExtensions is app-plane:
  # the manager answers it for this app's identity alone.
  permissions = [
    { resources = ["job"], actions = ["read"], namespaces = ["*"] },
    { resources = ["job"], actions = ["read"], namespaces = ["legion-system"], names = ["ingress"] },
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/ingress/*"] },
    { resources = ["object"], actions = ["read", "write"], namespaces = ["legion-system"], kinds = ["certs/Certificate"] },
    { resources = ["object"], actions = ["read"], namespaces = ["*"], kinds = ["certs/Issuer"] },
    { resources = ["object"], actions = ["write"], namespaces = ["*"], kinds = ["certs/Issuer"], names = ["ingress"] },
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
        certificates = {
          type = "boolean",
          description = "Ask the cert manager (the bundled certs app) for a certificate for every routed host its Issuer can prove. Default true. Set false behind a load balancer that terminates TLS.",
        },
        issuer = {
          type = "string",
          description = "The certs/Issuer that issues them. Default `ingress`, an ACME Issuer this app keeps from `acme` (HTTP-01, answered on port 80). Name another (a DNS-01 Issuer for wildcard hosts, a ca Issuer for internal names) to use it instead; it must list legion-system in its namespaces.",
        },
        acme = {
          type = "object",
          additionalProperties = false,
          description = "The `ingress` Issuer's ACME settings; ignored when `issuer` names another. Creating that Issuer agrees to the CA's terms of service.",
          properties = {
            server = {
              type = "string",
              description = "`letsencrypt` (the default), `letsencrypt-staging`, or an ACME directory URL (https://).",
            },
            email = {
              type = "string",
              description = "The account's contact address. Optional.",
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
  # Which CA issues the certificates for the cluster's public hosts:
  # changing it changes who can obtain them, so it needs the app's whole
  # grant.
  codeFields = [
    "/issuer",
    "/acme/server",
  ],
}
