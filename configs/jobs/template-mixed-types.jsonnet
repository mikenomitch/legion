/*
LEGION_TEMPLATE_START
{
  "name": "Template demo: mixed input types",
  "description": "Showcases string, text, number, toggle, and multi-select inputs in one template.",
  "variables": [
    {
      "key": "title",
      "inputType": "string",
      "label": "Title",
      "description": "A short title for the rendered task metadata.",
      "required": true,
      "defaultValueJson": "\"Template task\""
    },
    {
      "key": "details",
      "inputType": "text",
      "label": "Details",
      "description": "Long-form description rendered into the job config.",
      "required": false,
      "defaultValueJson": "\"Use this template as an example for richer inputs.\""
    },
    {
      "key": "retry_count",
      "inputType": "number",
      "label": "Retry count",
      "description": "Number written directly into task config.",
      "required": false,
      "defaultValueJson": "3"
    },
    {
      "key": "dry_run",
      "inputType": "toggle",
      "label": "Dry run",
      "description": "Adds a dry-run flag to rendered config.",
      "required": false,
      "defaultValueJson": "false"
    },
    {
      "key": "labels",
      "inputType": "multi_select",
      "label": "Labels",
      "description": "Select one or more labels.",
      "required": false,
      "optionsJson": "[\"alpha\", \"beta\", \"canary\"]",
      "defaultValueJson": "[\"alpha\", \"beta\"]"
    },
    {
      "key": "metadata",
      "inputType": "code",
      "label": "Metadata payload",
      "description": "Arbitrary JSON object to include in config.",
      "required": false,
      "defaultValueJson": "{\"owner\":\"platform\",\"environment\":\"dev\"}"
    }
  ]
}
LEGION_TEMPLATE_END
*/
{
  name: 'template-mixed-types',
  tasks: [
    {
      kind: 'noop',
      config: {
        title: std.extVar('title'),
        details: std.extVar('details'),
        retryCount: std.parseJson(std.extVar('retry_count')),
        dryRun: std.parseJson(std.extVar('dry_run')),
        labels: std.parseJson(std.extVar('labels')),
        metadata: std.parseJson(std.extVar('metadata')),
      },
    },
  ],
}
