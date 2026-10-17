# Cluster CA providers (`exec`)

Reference `clusterCa.provider = "exec"` helpers for
[the cluster CA](../../docs/cluster-ca.md). Each is a small script the manager
runs with one JSON request on stdin and reads one JSON reply from stdout,
the same conventions as the KV keyring's root key helpers
([`plugins/keyring/`](../keyring/README.md)). The whole contract is two
operations:

```json
{"op":"describe"}
→ {"bundle_pem":"<every CA holders trust>","chain_pem":"<the signing CA, then intermediates toward the bundle>"}

{"op":"sign","role":"node","csr_pem":"…","common_name":"node-1.node.legion",
 "dns_sans":["node-1.node.legion"],"uri_sans":["legion://node/node-1"],
 "ttl_seconds":86400,"usages":["server_auth","client_auth"]}
→ {"cert_pem":"<the leaf, optionally followed by its intermediates>"}
```

`role` is one of `node`, `fleet`, `manager_server`, `manager_client`. Sign
exactly the names in the request and ignore any the CSR asks for. Legion
checks every certificate before handing it out (the names, the CSR's key, the
usages, the lifetime, the chain to `bundle_pem`) and refuses one that does not
match, naming the mismatch. A non-zero exit, a timeout
(`clusterCa.timeout`, default `10s`), or anything that is not a JSON object
with the required field on stdout is a provider error.

Before pointing a manager at a helper, run the conformance harness:

```sh
legion net ca provider test -- plugins/cluster_ca/vault-pki/legion-ca-vault --mount pki_legion --role legion
```

It reads the bundle, has a node, a manager server, and a manager client
certificate signed, verifies each one as a manager would, and checks that the
requested names win over a CSR's own. It prints the config block to paste
when everything passes.

| Helper | Backend | Needs |
| --- | --- | --- |
| [`vault-pki/legion-ca-vault`](vault-pki/legion-ca-vault) | HashiCorp Vault PKI (`pki/sign/<role>`, the mount's issuers) | the `vault` CLI, `jq`, `VAULT_ADDR`, and a token with the policy below |

## Vault PKI

One mount and one role for Legion. The mount holds an intermediate your root
signed (or, to try it out, a root of its own):

```sh
vault secrets enable -path=pki_legion pki
vault secrets tune -max-lease-ttl=87600h pki_legion

# An intermediate under your root: generate a CSR, sign it with your root,
# and import the result.
vault write -format=json pki_legion/intermediate/generate/internal \
    common_name="Acme Legion Intermediate" key_type=ec key_bits=256 \
  | jq -r .data.csr > legion-intermediate.csr
#   ... sign legion-intermediate.csr with your root (pathlen:0, keyCertSign) ...
vault write pki_legion/intermediate/set-signed certificate=@legion-intermediate.pem

# The role: Legion's names only, never the CSR's, 24-hour certificates.
vault write pki_legion/roles/legion \
    allowed_domains=legion allow_subdomains=true \
    allowed_uri_sans="legion://*" \
    use_csr_sans=false use_csr_common_name=false \
    server_flag=true client_flag=true \
    key_type=any ttl=24h max_ttl=25h no_store=true
```

The manager's token needs:

```hcl
path "pki_legion/sign/legion" { capabilities = ["create", "update"] }
path "pki_legion/issuer/*"    { capabilities = ["read"] }
path "pki_legion/issuers"     { capabilities = ["list"] }
```

- **`use_csr_sans=false` and `use_csr_common_name=false` are required.**
  Vault's defaults copy names from the CSR; Legion refuses such a
  certificate, and the harness's "requested names win" check fails.
- **The trust bundle is every issuer in the mount.** Rotation is: generate or
  import a new issuer, make it the default
  (`vault write pki_legion/config/issuers default=<id>`), and delete the old
  issuer a day later. Every step reaches every worker within a heartbeat
  plus `clusterCa.refreshInterval`. Pass `--bundle-file /path/to/root.pem`
  to make holders trust your root instead (rotating an intermediate under
  that root is then a single step).
- **Keep the mount for Legion alone.** Anything signed under it with a
  `legion://` URI SAN is a node or a manager to every peer.
- `no_store=true` keeps Vault from storing a copy of every 24-hour
  certificate; drop it if you want them in Vault's inventory.

The helper is a starting point and is not exercised against a live Vault in
Legion's CI. Copy it for any other CA: decode the request, call your CA, print
the reply.
