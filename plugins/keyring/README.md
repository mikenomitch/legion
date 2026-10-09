# Root key providers (`exec`)

Reference `encryption.rootKey.provider = "exec"` helpers for
[KV encryption at rest](../../docs/kv-encryption.md). Each is a small script
the manager runs with one JSON request on stdin and reads one JSON reply
from stdout — the whole contract is three operations:

```json
{"op":"describe"}                                                    → {"key_id":"<root key id>"}
{"op":"wrap","key_id":"dk_…","plaintext":"<base64>"}                 → {"ciphertext":"<base64>","key_id":"<root key id>"}
{"op":"unwrap","key_id":"dk_…","ciphertext":"<base64>","root_key_id":"…"} → {"plaintext":"<base64>"}
```

A non-zero exit, a timeout (`encryption.rootKey.timeout`, default `10s`), or
anything that is not a JSON object on stdout is a provider error. The
manager calls the provider only at start, on keyring changes, and on
rotation — never on a KV read or write — so a slow KMS never slows KV.

Before pointing a manager at a helper, run the conformance harness:

```sh
legion keyring provider test -- plugins/keyring/aws-kms/legion-kms-aws --key-id arn:aws:kms:…:key/…
```

It checks `describe`, a wrap/unwrap round trip, that a tampered ciphertext
is refused, and that wrapping is randomized, and prints the config block to
paste when everything passes.

| Helper | Backend | Needs |
| --- | --- | --- |
| [`aws-kms/legion-kms-aws`](aws-kms/legion-kms-aws) | AWS KMS (`aws kms encrypt` / `decrypt`) | the `aws` CLI with credentials that may use the key |
| [`vault-transit/legion-kms-vault`](vault-transit/legion-kms-vault) | HashiCorp Vault Transit (`transit/encrypt`, `transit/decrypt`) | the `vault` CLI, `VAULT_ADDR`, and a token with `update` on the two paths |

Both bind the data key id into the KMS call (AWS encryption context, Vault
`context`), so a wrapped blob cannot be replayed for a different data key.
Copy one as the starting point for any other KMS or HSM: the shape is
"decode the request, call your tool, print the reply".
