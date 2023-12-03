# Next Up
* Goal: Save node data to a store

* Test of Goal:
- You can do this with two different workers at same time (diff names)

* Steps:

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
