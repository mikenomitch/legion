# Next Up
* Goal: Heartbeats actually update status

* Success:
- I can start a node
- I can get its status with a "node info" command
- If I kill the node, the status updates in N seconds
- I can restart the node
- The status updates again

* To Do:
- In server.rs start a loop getting nodes
- Make the updates to status on lost
- Update the ones that are found again

# After that
* Goal: Clean up
- Rip out any "follower" stuff

# After that
* Goal: Add more node stuff
- Other status checks along with heartbeats
- "Official" resource stuff + ledger? (start w/ allocs?)
- Then make this all pluggable

# After that
* Goal: Clean up

* Success:
- Nicely printing out node info (in a way that can be copied)
- Figure out nicer way of doing Node to Node (or skipped altogether)
- SQL queries generally cleaned up
- Heartbeating happens nicely and roughly performantly?

# Deferred Work

Start with:
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
