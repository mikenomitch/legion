# A Let's Encrypt Issuer for the bundled cert manager (docs/cert-manager.md).
#
# DNS-01 through Cloudflare: works for every name, wildcards included, and
# needs nothing reachable from the internet. Put a Cloudflare API token
# (Zone:Read and DNS:Edit on your zone) in KV first:
#
#   legion kv put secrets/cloudflare-dns "<token>"
#   legion object apply -f configs/certs/letsencrypt-issuer.ncl --wait
#   legion object get issuer letsencrypt
#
# Applying it needs object:write on certs/Issuer in every namespace (an
# Issuer is cluster-wide) and kv:read on secrets/cloudflare-dns: the cert
# manager reads the token acting for this Issuer, within what you hold.
#
# Try `server = "letsencrypt-staging"` first: its certificates are not
# trusted by browsers, but its rate limits are generous while you get the
# DNS right. Switching `server` later re-issues every certificate.
{
  kind = "certs/Issuer",
  name = "letsencrypt",
  spec = {
    acme = {
      server = "letsencrypt",
      email = "ops@example.com",
      dns01 = { cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } } },
      # And HTTP-01, for names whose port 80 reaches a
      # `legion cert http01` responder (configs/certs/http01-responder.ncl).
      # DNS-01 is used whenever it is set; drop dns01 to use HTTP-01 alone.
      # http01 = {},
    },
  },
}
