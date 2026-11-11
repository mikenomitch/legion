# A gitops Repo (docs/gitops.md): gitops deploys every jobspec in `path` of
# the repository into this namespace, and keeps it deployed.
#
#   legion object apply -f configs/gitops/repo.ncl --wait
#
# or, without a file:
#
#   legion gitops add storefront -n team-a --url https://github.com/acme/deploy --path storefront --wait
#
# Writing it needs object:write on gitops/Repo in team-a, and job:read and
# job:write there (on `jobs` alone, when it lists them): gitops can deploy
# only what you could deploy yourself.
{
  kind = "gitops/Repo",
  name = "storefront",
  namespace = "team-a",
  labels = { team = "storefront" },
  spec = {
    # Required. https:// (http:// without credentials); file:// only where
    # an operator set the app's allowFileUrls. No SSH yet.
    url = "https://github.com/acme/deploy",
    # A branch, a tag, or a full commit SHA. Default "main".
    ref = "main",
    # The directory whose .ncl and .json files are the jobspecs (not
    # recursive; names starting with . or _ are skipped). Default ".".
    path = "storefront",
    # How often to look for a new commit (`legion gitops sync` looks now).
    # Default "1m", at least "10s".
    interval = "1m",
    # Optional: only these jobs may be deployed, and writing the Repo then
    # needs job:read and job:write on them alone. 1 to 20 names.
    jobs = ["storefront", "storefront-worker"],
    # Stop a job whose file leaves the repository, and every job when the
    # Repo is deleted (stopped, never destroyed). Default true.
    prune = true,
    # Pause syncing; the jobs keep running. Default false.
    suspend = false,
    # Set when the jobs have host-level tasks (any `exec` task): writing the
    # Repo then needs job:write with host = true. Default false.
    hostLevel = false,
    # A private repository: an HTTPS token in KV (`legion kv put
    # secret/gitops/github-token <token>`); writing the Repo needs kv:read on
    # it. Bitbucket wants username = "x-token-auth".
    credentials = { kv = "secret/gitops/github-token", username = "x-access-token" },
    # The KV keys the jobs read ($path/key values, kvInjections): a submit
    # needs kv:read on each, so writing the Repo does too.
    kvKeys = ["secret/storefront/database-url"],
  },
}
