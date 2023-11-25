# Next Up

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
- In join_server.rs the get_join_token function should add a cert to the JWT
- The receiver of this JWT should grab the cert, use it, and also stick it in some data dir for future reference
- You should verify an encrypted client connection at this point
- You should also create a function for the CLI to grab a cert and stick it in a well-known place, and then that is just used from then on.
