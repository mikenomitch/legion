# A reusable task builder. Imports are hermetic (this file can't see the
# importer's `vars`), so a shared fragment that needs parameters exports a
# function instead of a record.
#
# Parameter names differ from field names on purpose: Nickel records are
# recursive, so `{ name = name }` would refer to the field, not the argument.
fun svc_name svc_memory svc_command =>
  {
    name = svc_name,
    runtime = "exec",
    lifecycle = "service",
    config = { command = svc_command },
    resources = { memory = svc_memory },
  }
