# A public certificate from the `letsencrypt` Issuer
# (configs/certs/letsencrypt-issuer.ncl), renewed for you.
#
#   legion object apply -f configs/certs/certificate.ncl --wait
#   legion object list certificates
#
# The certificate lands in KV as one PEM bundle (the chain, then the key):
# exactly what a port's `tls.cert = { kv }` serves. Use it from any job
# (configs/certs/web-https.ncl):
#
#   network = { ports = [
#     { name = "https", static = 443, tls = { cert = { kv = "certs/shop" } } },
#   ] },
#
# When it renews (two thirds of the way through its life), the node swaps it
# in without a restart. `legion cert renew shop` renews it now.
#
# Applying it needs object:write on certs/Certificate in its namespace, and
# kv:read and kv:write on certs/shop: the cert manager writes there acting
# for this Certificate, never further than you could.
{
  kind = "certs/Certificate",
  name = "shop",
  namespace = "default",
  spec = {
    issuer = "letsencrypt",
    dnsNames = ["shop.example.com", "www.shop.example.com"],
    kv = "certs/shop",
  },
}
