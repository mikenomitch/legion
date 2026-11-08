# The bundled cert manager's manifest (docs/cert-manager.md;
# internal_docs/plans/cert-manager-plan.md). Compiled into the binary and
# seeded by the first Control leader of a cluster, enabled, with its
# permissions granted by `legion:bootstrap`. It does nothing until someone
# applies a certs/Issuer and a certs/Certificate.
{
  name = "certs",
  description = "Issues and renews TLS certificates: Let's Encrypt and other ACME CAs, private CAs, and self-signed.",
  placement = "managers",
  # One replica: two would issue every certificate twice.
  replicas = 1,
  job = {
    priority = "critical",
    task = {
      name = "manager",
      runtime = "exec",
      config = { command = ["legion:self", "app", "run", "certs"] },
      resources = { cpu = 100, memory = 128 },
      # `/healthz` and the UI views. Not a catalog service.
      network = { ports = [{ name = "api" }] },
      checks = [
        {
          type = "health",
          name = "certs /healthz",
          kind = "http",
          path = "/healthz",
          port = "api",
          every = "10s",
          timeout = "2s",
        },
      ],
    },
  },
  # Exactly what the app calls. Its own KV (GetKv, PutKv) for one ACME
  # account key per Issuer, under the reserved legion/apps/certs/ (a
  # tenant's `paths = ["*"]` does not reach it). Every other key it reads
  # or writes, it does acting for one of its objects (`legion-for-object`):
  # a Certificate's kv and parts, an Issuer's CA, trust key, DNS token, or
  # EAB key. `via = "objects"` means its own credential can touch none of
  # them, and acting for an object it holds only what that object's writer
  # was checked for (the kinds' `requires`, below), so a Certificate can
  # never make it write a key its writer could not. `*` does not reach the
  # reserved legion/ keys, so `legion/apps/*` is named too: an app that
  # writes a Certificate into its own private prefix (the ingress keeps
  # its certificates under legion/apps/ingress/issued/) gets it there, and
  # no one else can, since only that app holds the key. WatchObjects,
  # UpdateObjectStatus, and ReleaseObject are app-plane for its own kinds
  # and need no rule.
  permissions = [
    { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/certs/*"] },
    { resources = ["kv"], actions = ["read", "write"], paths = ["*", "legion/apps/*"], via = "objects" },
  ],
  kinds = [
    {
      name = "Issuer",
      description = "Where certificates come from: an ACME CA such as Let's Encrypt, a private CA kept in KV, or self-signed.",
      scope = "cluster",
      # Whoever writes an Issuer must be able to read (and, for a CA, write)
      # every key it names: the app reads them acting for the Issuer.
      requires = [
        { resource = "kv", actions = ["read", "write"], path = "/spec/ca/kv" },
        { resource = "kv", actions = ["read", "write"], path = "/spec/ca/trust" },
        { resource = "kv", actions = ["read"], path = "/spec/acme/dns01/cloudflare/apiToken/kv" },
        { resource = "kv", actions = ["read"], path = "/spec/acme/dns01/webhook/token/kv" },
        { resource = "kv", actions = ["read"], path = "/spec/acme/eab/hmacKey/kv" },
      ],
      schema = {
        type = "object",
        properties = {
          namespaces = {
            type = "array",
            items = { type = "string" },
            default = ["*"],
            description = "Namespaces (names or *-globs) whose Certificates may use this Issuer. Default every namespace (`*` does not reach the reserved legion-* namespaces).",
          },
          selfSigned = {
            type = "object",
            properties = {},
            description = "Each certificate signs itself: for development, or for a client that pins the certificate.",
          },
          ca = {
            type = "object",
            required = ["kv"],
            description = "A private CA whose certificate and key live in KV. Generated (ECDSA P-256, 10 years) the first time, when the key does not exist.",
            properties = {
              kv = {
                type = "string",
                description = "The KV key holding the CA: its certificate (then any intermediates toward a root), then its PKCS#8 private key.",
              },
              trust = {
                type = "string",
                description = "A KV key to publish the CA's certificate to, without its key, for clients to trust (`ports[].tls.clients.ca = { kv }`, `connect.upstreams[].ca = { kv }`).",
              },
            },
          },
          acme = {
            type = "object",
            description = "An ACME (RFC 8555) CA such as Let's Encrypt. Needs dns01, http01, or both: how it proves a name is yours.",
            properties = {
              server = {
                type = "string",
                default = "letsencrypt",
                description = "`letsencrypt`, `letsencrypt-staging` (untrusted certificates, generous rate limits: try this first), or a directory URL.",
              },
              email = { type = "string", description = "A contact address for the account. Optional." },
              eab = {
                type = "object",
                required = ["kid", "hmacKey"],
                description = "External account binding, for CAs that require one (ZeroSSL, Google Trust Services).",
                properties = {
                  kid = { type = "string", description = "The key id the CA gave you." },
                  hmacKey = {
                    type = "object",
                    required = ["kv"],
                    properties = { kv = { type = "string", description = "The KV key holding the MAC key (base64url, as the CA gave it)." } },
                  },
                },
              },
              dns01 = {
                type = "object",
                description = "Prove names with a TXT record at _acme-challenge.<name>. Works for every name, wildcards included, and needs nothing reachable from the internet.",
                properties = {
                  cloudflare = {
                    type = "object",
                    required = ["apiToken"],
                    properties = {
                      apiToken = {
                        type = "object",
                        required = ["kv"],
                        properties = { kv = { type = "string", description = "The KV key holding a Cloudflare API token with Zone:Read and DNS:Edit on the zone." } },
                      },
                    },
                  },
                  webhook = {
                    type = "object",
                    required = ["url"],
                    description = "Any DNS API behind lego's httpreq shape: POST <url>/present and <url>/cleanup with { fqdn, value }.",
                    properties = {
                      url = { type = "string" },
                      token = {
                        type = "object",
                        required = ["kv"],
                        properties = { kv = { type = "string", description = "The KV key holding a token, sent as Authorization: Bearer." } },
                      },
                    },
                  },
                  propagationDelay = {
                    type = "string",
                    description = "How long to wait after publishing the record before the CA checks it. Default 30s, at most 10m.",
                  },
                },
              },
              http01 = {
                type = "object",
                properties = {},
                description = "Prove names over HTTP on port 80: something there must answer /.well-known/acme-challenge/ with the Issuer's thumbprint (`legion cert http01`). Not for wildcards.",
              },
            },
          },
        },
      },
      # What a schema cannot say: one type, and a usable acme block.
      rules = [
        {
          rule = "(has(self.selfSigned) ? 1 : 0) + (has(self.ca) ? 1 : 0) + (has(self.acme) ? 1 : 0) == 1",
          message = "an Issuer is exactly one of selfSigned, ca, and acme",
        },
        {
          rule = "!has(self.acme) || has(self.acme.dns01) || has(self.acme.http01)",
          message = "acme needs dns01, http01, or both: how the CA checks a name is yours",
          path = "/spec/acme",
        },
        {
          rule = "!has(self.acme) || !has(self.acme.dns01) || (has(self.acme.dns01.cloudflare) ? 1 : 0) + (has(self.acme.dns01.webhook) ? 1 : 0) == 1",
          message = "acme.dns01 names exactly one provider: cloudflare or webhook",
          path = "/spec/acme/dns01",
        },
        {
          rule = "!has(self.ca) || !has(self.ca.trust) || self.ca.trust != self.ca.kv",
          message = "ca.trust must differ from ca.kv: trust is published without the key",
          path = "/spec/ca/trust",
        },
      ],
      status = {
        schema = {
          type = "object",
          properties = {
            type = { type = "string" },
            server = { type = "string" },
            accountUri = { type = "string" },
            thumbprint = { type = "string" },
            email = { type = "string" },
            caSubject = { type = "string" },
            caNotAfter = { type = "string" },
            caFingerprint = { type = "string" },
            generated = { type = "boolean" },
          },
        },
      },
      examples = [
        {
          acme = {
            server = "letsencrypt-staging",
            email = "ops@example.com",
            dns01 = { cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } } },
          },
        },
        { acme = { http01 = {} } },
        { ca = { kv = "certs/ca/internal", trust = "certs/ca/internal.pem" } },
        { namespaces = ["dev-*"], selfSigned = {} },
      ],
      maxObjects = 100,
      columns = [
        { name = "TYPE", pointer = "/status/fields/type" },
        { name = "SERVER", pointer = "/status/fields/server" },
      ],
    },
    {
      name = "Certificate",
      description = "A TLS certificate the certs app issues and renews, written to a KV key as the chain then the key.",
      scope = "namespaced",
      # The app writes the certificate acting for the Certificate, so its
      # writer must be able to read and write every key it names.
      requires = [
        { resource = "kv", actions = ["read", "write"], path = "/spec/kv" },
        { resource = "kv", actions = ["read", "write"], path = "/spec/parts/cert" },
        { resource = "kv", actions = ["read", "write"], path = "/spec/parts/key" },
        { resource = "kv", actions = ["read", "write"], path = "/spec/parts/ca" },
      ],
      schema = {
        type = "object",
        required = ["issuer", "kv"],
        properties = {
          issuer = { type = "string", description = "The certs/Issuer that issues it." },
          kv = {
            type = "string",
            description = "The KV key the certificate goes to: the chain, leaf first, then the private key. What `ports[].tls.cert = { kv }` serves.",
          },
          dnsNames = {
            type = "array",
            items = { type = "string" },
            maxItems = 100,
            default = [],
            description = "DNS names. `*.example.com` is a wildcard (DNS-01 only, for an ACME Issuer).",
          },
          ipAddresses = {
            type = "array",
            items = { type = "string" },
            maxItems = 100,
            default = [],
            description = "IP addresses (HTTP-01 only, for an ACME Issuer).",
          },
          commonName = { type = "string", maxLength = 64, description = "Default: the first name." },
          duration = {
            type = "string",
            default = "2160h",
            description = "How long it lasts (2160h, 90d). An ACME CA picks its own lifetime and ignores this.",
          },
          renewBefore = {
            type = "string",
            description = "Renew this long before it expires. Default: two thirds of the way through its lifetime.",
          },
          keyAlgorithm = {
            type = "string",
            enum = ["ecdsa-p256", "ecdsa-p384"],
            default = "ecdsa-p256",
            description = "The private key's algorithm. A new key is made at every issuance.",
          },
          usages = {
            type = "array",
            items = { type = "string", enum = ["server", "client"] },
            default = ["server", "client"],
            description = "What it may authenticate: a TLS server, a TLS client, or both. An ACME CA picks its own.",
          },
          parts = {
            type = "object",
            description = "Also write the parts to keys of their own, for software that reads the certificate and key from separate files.",
            properties = {
              cert = { type = "string", description = "The chain, leaf first." },
              key = { type = "string", description = "The private key (PKCS#8 PEM)." },
              ca = { type = "string", description = "The issuing CA's certificate(s)." },
            },
          },
        },
      },
      rules = [
        {
          rule = "size(self.dnsNames) + size(self.ipAddresses) > 0",
          message = "a certificate names at least one of dnsNames and ipAddresses",
          path = "/spec/dnsNames",
        },
        {
          rule = "!has(self.parts) || ((!has(self.parts.cert) || self.parts.cert != self.kv) && (!has(self.parts.key) || self.parts.key != self.kv) && (!has(self.parts.ca) || self.parts.ca != self.kv))",
          message = "parts keys must differ from kv, which holds the whole bundle",
          path = "/spec/parts",
        },
      ],
      status = {
        schema = {
          type = "object",
          properties = {
            issuer = { type = "string" },
            kv = { type = "string" },
            names = { type = "array", items = { type = "string" } },
            notBefore = { type = "string" },
            notAfter = { type = "string" },
            renewalTime = { type = "string" },
            serial = { type = "string" },
            revision = { type = "integer" },
            specHash = { type = "string" },
            issuerIdentity = { type = "string" },
            renewRequest = { type = "string" },
            failedAttempts = { type = "integer" },
            lastFailure = { type = "string" },
            lastFailureTime = { type = "string" },
            nextAttemptTime = { type = "string" },
          },
        },
      },
      examples = [
        { issuer = "letsencrypt", kv = "certs/shop", dnsNames = ["shop.example.com", "www.shop.example.com"] },
        {
          issuer = "internal",
          kv = "certs/db",
          dnsNames = ["db.internal"],
          duration = "720h",
          parts = { cert = "certs/db.crt", key = "certs/db.key", ca = "certs/db-ca.crt" },
        },
      ],
      columns = [
        { name = "ISSUER", pointer = "/spec/issuer" },
        { name = "KV", pointer = "/spec/kv" },
        { name = "EXPIRES", pointer = "/status/fields/notAfter" },
      ],
    },
  ],
  ui = {
    port = "api",
    nav = [
      { title = "Certificates", icon = "shieldCheck", view = "certificates",
        permission = { resource = "object", action = "read" } },
    ],
    overview = [
      { title = "Certificates", view = "summary",
        permission = { resource = "object", action = "read" } },
    ],
    refresh = "30s",
  },
  # Everything a user sets lives on the objects.
  config = {
    schema = { type = "object", additionalProperties = false, properties = {} },
    default = {},
  },
}
