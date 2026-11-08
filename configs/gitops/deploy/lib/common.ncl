# Shared settings for the jobs in ../jobs. gitops reads only the files
# directly in a Repo's `path`, so this file is imported, never deployed.
{
  resources = { cpu = 250, memory = 128 },
}
