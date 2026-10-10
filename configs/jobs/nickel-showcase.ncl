# A tour of the Nickel features that make jobspecs easier to maintain:
# `let` bindings, variables with defaults, contracts, an imported function,
# `std` helpers, conditionals, string interpolation, and record merging.
# See docs/configuration-language.md.
#
#   legion job validate configs/jobs/nickel-showcase.ncl
#   legion job run configs/jobs/nickel-showcase.ncl --var env=prod --var-code workers=4

# A custom contract: `env` must be one of these strings.
let Env = std.contract.from_predicate (fun e => std.array.elem e ["dev", "prod"]) in

# Variables with defaults. Anything passed via --var / --var-code wins over a
# `default`, and the contracts check the caller's values too.
let params =
  {
    env | Env | default = "dev",
    workers | Number | default = 2,
    version | String | default = "1.4.2",
  }
  & vars
in

# Derived values and conditionals: prod gets more memory than dev.
let is_prod = params.env == "prod" in
let memory = if is_prod then 512 else 128 in

# An imported function, applied like any other.
let service = import "lib/service.ncl" in

# A local helper that builds one queue worker per index.
let worker = fun i =>
  service "worker-%{std.string.from_number i}" memory ["echo", "worker", "%{std.string.from_number i}"]
in

{
  name = "showcase-%{params.env}",
  tasks =
    [
      # Merge (`&`) layers extra fields onto what the function returned.
      (service "api" memory ["echo", "api v%{params.version}"])
      & { count = if is_prod then 3 else 1 },
    ]
    # `std.array.generate` stamps out N workers from one definition.
    @ std.array.generate worker params.workers,
}
