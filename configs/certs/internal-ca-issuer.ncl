# A private CA for internal TLS and mTLS (docs/cert-manager.md, "A private
# CA").
#
# The first time the cert manager sees this Issuer, it generates a CA
# (ECDSA P-256, valid 10 years) into `ca.kv`, and publishes the CA's
# certificate alone to `ca.trust`, for clients to trust:
#
#   legion object apply -f configs/certs/internal-ca-issuer.ncl --wait
#   legion kv get certs/ca/internal.pem
#
# To bring your own CA (an intermediate under your root), put its
# certificate, any intermediates toward the root, and its PKCS#8 key into
# `ca.kv` before applying:
#
#   legion kv put certs/ca/internal "$(cat ca.pem root.pem ca-pkcs8.key)"
#
# Applying it needs object:write on certs/Issuer everywhere, and kv:read and
# kv:write on both keys.
{
  kind = "certs/Issuer",
  name = "internal",
  spec = {
    # Only Certificates in these namespaces may use it: a CA signs whatever
    # names it is asked to, so share one only between namespaces that trust
    # each other.
    namespaces = ["prod-*"],
    ca = {
      kv = "certs/ca/internal",
      trust = "certs/ca/internal.pem",
    },
  },
}
