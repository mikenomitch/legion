# An object of the example app's `Repo` kind (configs/apps/example-app.ncl;
# docs/app-objects.md). With gitops installed:
#
#   legion object explain gitops/Repo          # the kind's fields
#   legion object apply -f configs/apps/example-repo.ncl --dry-run
#   legion object apply -f configs/apps/example-repo.ncl --wait
#   legion object get repo storefront -n team-a
#
# A file holds one object or a list of them. `kind` is always qualified,
# `<app>/<Kind>`, so a file means the same thing on every cluster. `status`
# is the app's to write, never the file's.
{
  kind = "gitops/Repo",
  name = "storefront",
  # Default: -n, then $LEGION_NAMESPACE, then `default`.
  namespace = "team-a",
  labels = { team = "storefront" },
  spec = {
    url = "https://github.com/acme/deploy",
    path = "jobs/storefront",
    # Writing this Repo needs job:write on these jobs in team-a.
    jobs = ["storefront", "storefront-worker"],
  },
}
