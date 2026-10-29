# A worker type for dedicated CI hosts (docs/namespaces.md, "Nodes that
# accept only some namespaces"). Its nodes run only jobs from `ci-*`
# namespaces, whatever a job's `permit` says: the list is the node's
# restriction, not a toleration a job can write. The `ci` pool keeps every
# job that does not permit it off these nodes too.
#
# Globs follow the reserved-prefix rule: `["*"]` would accept every tenant
# namespace but not `legion-system`; `["*", "legion-*"]` accepts all.
{
  workerType = "ci",
  pool = "ci",
  namespaces = ["ci-*"],
}
