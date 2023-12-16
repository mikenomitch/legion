# Next Up
* Goal: Clean up

# After that
* Goal: Add more node stuff
- Other status checks along with heartbeats
- "Official" resource stuff + ledger? (start w/ allocs?)
- Then make this all pluggable

# After that
* Goal: Clean up

* Success:
- Nicely printing out node info (in a way that can be copied to other fns)
- Has a jsony printing option for commands
- Figure out nicer way of doing Node to Node (or skipped altogether)
- SQL queries generally cleaned up
- Heartbeating happens nicely and roughly performantly?
-

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
