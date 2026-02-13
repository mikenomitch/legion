# Templates

This document describes Legion templates: versioned, reusable Jsonnet job specs that less-advanced users can submit through a form.

## Goals
- Store unrendered Jsonnet sources so Jsonnet logic runs at submit time.
- Define template metadata and variable schema inside a single template file.
- Version templates and record template provenance on job submissions.

## Schema-in-spec options evaluated
1. **Out-of-band schema file** (`template.yaml` next to jobspec).
   - Pros: easy parsing.
   - Cons: two files to keep in sync.
2. **Top-level JSON field in rendered jobspec** (`template` object).
   - Pros: in-band.
   - Cons: requires successful render before we can read schema.
3. **Embedded source annotation in Jsonnet comments (chosen)**.
   - Pros: one file upload, no pre-render required, simple to parse.
   - Cons: requires a small convention.

## Chosen convention
Templates may include:

```jsonnet
/*
LEGION_TEMPLATE_START
{
  "name": "Deploy API",
  "description": "Deploy API service with env + image settings",
  "variables": [
    {"key":"image","inputType":"string","label":"Image","required":true},
    {"key":"replicas","inputType":"number","label":"Replicas","required":true},
    {"key":"notes","inputType":"text","label":"Release notes"}
  ]
}
LEGION_TEMPLATE_END
*/
```

Supported input types in UI:
- `string`
- `text`
- `toggle`
- `multi_select`
- `number`
- `code` (fallback)

## Submission provenance
`RunJobRequest` supports an optional `template` reference (`template_id`, `version`).
Job config rows store these fields for traceability.
