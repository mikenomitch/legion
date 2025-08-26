# TODOs:

Not exhaustive, but noting things so I remember:
* Figure out if plugin return type should change so its more language agnostic

## Deferred Work

### TLS joining

- Goal: Make join process establish TLS and/or WG

- Success:

* Run server with `cr start``
* Try `cr job status`
* Note that it fails
* Get a join-token
* Join with this token. Note that the client connection works.
* You should also be able to get a join-token for CLI certs

- Steps:

* Run worker start
