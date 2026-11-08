# The HTTP-01 responder for an ACME Issuer with `http01 = {}`
# (docs/cert-manager.md, "HTTP-01").
#
# Let's Encrypt checks a name over HTTP-01 by fetching
# http://<name>/.well-known/acme-challenge/<token> on port 80. The answer is
# <token>.<thumbprint>, where the thumbprint is the Issuer's ACME account's,
# and public: so one stateless responder answers every challenge, for every
# name, with no secret. Everything else it redirects to HTTPS.
#
# Run one on every node your names point at (here: every node in the
# default pool), once per cluster:
#
#   legion job run configs/certs/http01-responder.ncl
#   curl -i http://shop.example.com/.well-known/legion-certs-http01   # 200 ok
#
# It reads the Issuer's thumbprint through the allocation's API socket, so
# its `legion` identity asks for object:read on certs/Issuer (submitting it
# needs that too). Port 80 must be free on those nodes.
{
  name = "acme-http01",
  task = {
    name = "responder",
    runtime = "exec",
    lifecycle = "service",
    # One per node, including nodes that join later.
    count = { num = 1, per = "id" },
    config = {
      command = ["legion:self", "cert", "http01", "--issuer", "letsencrypt"],
    },
    network = { ports = [{ name = "http", static = 80 }] },
    identities = [
      {
        name = "legion",
        permissions = [
          { resources = ["object"], actions = ["read"], namespaces = ["*"], kinds = ["certs/Issuer"] },
        ],
      },
    ],
    checks = [
      {
        type = "health",
        name = "responder",
        kind = "http",
        path = "/.well-known/legion-certs-http01",
        port = "http",
        every = "10s",
        timeout = "2s",
      },
    ],
    resources = { cpus = 0.05, memory = 32 },
  },
}
