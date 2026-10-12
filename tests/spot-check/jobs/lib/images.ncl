# Image references for the spot-check jobs.
#
# Imported files cannot see `vars`, so this is a function the job calls with
# its own `vars`. The default is the rig's private ECR registry (build and
# push with images/build-push.sh). Point elsewhere with
# `--var registry=<host>/<prefix>`.
fun vars =>
  let params = { registry | String | default = "594906412132.dkr.ecr.us-east-1.amazonaws.com/spot-check" } & vars in
  {
    web = "%{params.registry}/web:latest",
    api = "%{params.registry}/api:latest",
    frontend = "%{params.registry}/frontend:latest",
    agent = "%{params.registry}/agent:latest",
    db = "%{params.registry}/db:latest",
  }
