# Next Up
* Goal: Add expiry on join-tokens

* Success:
- I can pass an argument into the join-token generation for unlimited or a time
- It defaults to 1 hour use
- The JWT has this auto encoded in it and this is validated on join

# Next Up
* Goal: Add node type on join-token

* Success: I can make a new node type and save it
- I can pass an argument into the join-token generation for node type
- It defaults to something?
- It adds a node_pool and initial dynamic attributes to the node info and that's it for now

# After that
* Goal: Add more node stuff
- Other status checks along with heartbeats
- "Official" resource stuff + ledger? (start w/ allocs?)
- Then make this all pluggable

# After that
* Goal: Scheduling a Job on a node for real?!?!?

# Deferred Work

### TLS joining
* Goal: Make join process establish TLS

* Success:
- Run server with `cr start``
- Try `cr job status`
- Note that it fails
- Get a join-token
- Join with this token. Note that the client connection works.
- You should also be able to get a join-token for CLI certs

* Steps:
- Run worker start
