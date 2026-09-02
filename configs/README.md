A set of configuration files to help with development and testing

Everything here is [Nickel](https://nickel-lang.org) (`.ncl`) — Legion's
configuration language. `docs/configuration-language.md` covers the language,
the `vars` parameterization hook, and imports.

The one exception is `jobs/machine-generated.json`. A `.json` file is accepted
anywhere a `.ncl` one is, parsed as plain JSON rather than evaluated, for
config that a *program* emits — Terraform, CI, a control plane — where the
emitter already has a JSON serializer and no reason to grow a Nickel one.
Prefer Nickel for anything a person writes and maintains: JSON has no
variables, no imports, and no comments.
