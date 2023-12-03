# Next Up
* Goal: Get dynamic node metadata call working

* Success:
- The following commands all work:
    - cr node update worker -a foo baz
    - cr node update worker -a foo.bar baz
    - cr node update worker -a foo.bar baz -a wiz.woz wat
    - cr bode update worker -f ./path/to/data.jsonette

# After that
* Goal: Heartbeats actually monitor status

* Success:
- I can start a node
- I can get its status with a "node info" command
- If I kill the node, the status updates in N seconds
- I can restart the node
- The status updates again

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
