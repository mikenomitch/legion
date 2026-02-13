/*
LEGION_TEMPLATE_START
{
  "name": "Template demo: command shaping",
  "description": "Shows code and structured variables driving command-style fields.",
  "variables": [
    {
      "key": "binary",
      "inputType": "string",
      "label": "Binary",
      "description": "Command to run as the first argument.",
      "required": true,
      "defaultValueJson": "\"echo\""
    },
    {
      "key": "working_dir",
      "inputType": "string",
      "label": "Working directory",
      "description": "String passed through to the task config.",
      "required": false,
      "defaultValueJson": "\"/tmp\""
    },
    {
      "key": "extra_args",
      "inputType": "code",
      "label": "Extra args",
      "description": "JSON array appended to args.",
      "required": false,
      "defaultValueJson": "[\"--template\", \"--verbose\"]"
    },
    {
      "key": "run_count",
      "inputType": "number",
      "label": "Run count",
      "description": "Task allocation count.",
      "required": false,
      "defaultValueJson": "1"
    },
    {
      "key": "safe_mode",
      "inputType": "toggle",
      "label": "Safe mode",
      "description": "Enable safer rendering behavior flag.",
      "required": false,
      "defaultValueJson": "true"
    },
    {
      "key": "modes",
      "inputType": "multi_select",
      "label": "Modes",
      "description": "Mode tags used by config output.",
      "required": false,
      "optionsJson": "[\"diagnostic\", \"json\", \"parallel\"]",
      "defaultValueJson": "[\"diagnostic\", \"json\"]"
    },
    {
      "key": "env_vars",
      "inputType": "code",
      "label": "Environment vars",
      "description": "Arbitrary JSON object merged into config metadata.",
      "required": false,
      "defaultValueJson": "{\"LEGION_SOURCE\":\"template\", \"TRACE\":false}"
    }
  ]
}
LEGION_TEMPLATE_END
*/
{
  name: 'template-code-objects',
  tasks: [
    {
      kind: 'exec',
      count: std.parseJson(std.extVar('run_count')),
      config: {
        command: [std.extVar('binary')],
        args: std.parseJson(std.extVar('extra_args')) +
          (if std.parseJson(std.extVar('safe_mode')) then ["--safe"] else []),
        cwd: std.extVar('working_dir'),
        modeTags: std.parseJson(std.extVar('modes')),
        metadata: std.parseJson(std.extVar('env_vars')),
      },
    },
  ],
}
