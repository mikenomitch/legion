# Example job template (docs/job-templates.md). Upload with:
#   legion template create render -f configs/templates/render.ncl
#
# A template is one self-contained Nickel file that evaluates to a record with
# optional `description` and `variables` fields plus a
# `job | not_exported = fun vars => ...` function. Exporting the file yields
# the manifest — `not_exported` keeps `job` out of it — and the manager
# applies `job` to the resolved variable values at submit time, as one native
# Nickel record. Imports are not allowed: templates must be self-contained.
{
  description = "Render a demo scene on the cluster.",
  variables = [
    # `type` is one of: string, text, number, boolean, select,
    # multi_select, json. A variable with no `default` is required unless
    # it says `required = false` (then it renders as null).
    #
    # `default` is a Nickel keyword, so it is written as a quoted field name.
    { key = "scene", type = "string", label = "Scene name", description = "Short slug for the scene to render." },
    { key = "frames", type = "number", label = "Frames", "default" = 4 },
    { key = "quality", type = "select", label = "Quality", options = ["draft", "final"], "default" = "draft" },
    { key = "regions", type = "multi_select", label = "Regions", options = ["us", "eu", "ap"], "default" = ["us"] },
    { key = "debug", type = "boolean", label = "Debug logging", "default" = false },
    { key = "notes", type = "text", label = "Notes", required = false },
    { key = "overrides", type = "json", label = "Extra config", required = false },
  ],
  job | not_exported = fun vars => {
    # Values arrive with their real types: vars.frames is a number,
    # vars.debug a boolean, vars.regions an array.
    name = "render-%{vars.scene}",
    tasks = [{
      name = "render",
      runtime = "exec",
      count = vars.frames,
      config = {
        command = [
          "echo",
          "render %{vars.scene} quality=%{vars.quality} debug=%{std.to_string vars.debug}",
        ],
      },
    }],
  },
}
