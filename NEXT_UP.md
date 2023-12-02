# Next Up
* Goal: Save node data to a store

* Test of Goal:
- Run server with `cr start``
- Get a join-token
- Join with the token
- The register command passes along a name and fingerprint data
- A new list workers command shows the node in there
- You can do this with two different workers at same time

* Steps:
- Extend Register RPC to have fingerprint data
- Add fingerprint data to call
- Add temporary sqlite store using rusqlite (or sled?)
- Boot the store when the manager starts
- Write to it
- Read from it
- Make a general store interface
- Make a store plugin system? (look for store-plugin in manager config, otherwise use sqlite?)
- Do backups of the store?

# Deferred Work

Start with:
* Goal: Make join process establish TLS

* Test:
- Run server with `cr start``
- Try `cr job status`
- Note that it fails
- Get a join-token
- Join with this token. Note that the client connection works.
- You should also be able to get a join-token for CLI certs

* Steps:
- Run worker start
