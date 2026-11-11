# An internal certificate from the private `internal` Issuer
# (configs/certs/internal-ca-issuer.ncl), for a database that reads its
# certificate and key from separate files, as Postgres does.
#
#   legion object apply -f configs/certs/internal-certificate.ncl --wait
#
# Besides the bundle in `kv`, `parts` writes the chain, the key, and the CA
# to keys of their own. Render them as files that are rewritten in place on
# renewal (docs/kv-store.md, "Live file updates"):
#
#   files = [
#     { path = "tls/server.crt", file = { template = "{{ kv \"certs/db.crt\" }}" },
#       kv = { watch = true, onChange = "update" }, changeMode = "signal", changeSignal = "SIGHUP" },
#     { path = "tls/server.key", mode = "0600", file = { template = "{{ kv \"certs/db.key\" }}" },
#       kv = { watch = true, onChange = "update" }, changeMode = "signal", changeSignal = "SIGHUP" },
#   ],
#
# Clients trust the CA through the Issuer's `trust` key:
#   connect = { upstreams = [{ name = "db", to = "tls://db.internal:5432",
#                              ca = { kv = "certs/ca/internal.pem" } }] }
{
  kind = "certs/Certificate",
  name = "db",
  namespace = "prod-data",
  spec = {
    issuer = "internal",
    dnsNames = ["db.internal"],
    kv = "certs/db",
    # Private certificates last as long as you say; renewed at two thirds.
    duration = "720h",
    parts = { cert = "certs/db.crt", key = "certs/db.key", ca = "certs/db-ca.crt" },
  },
}
