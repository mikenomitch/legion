/* 
LEGION_TEMPLATE_START
{
  "name": "Parameterized macOS exec",
  "description": "Reusable macOS exec task where you can set the command and args.",
  "variables": [
    {
      "key": "command",
      "inputType": "string",
      "label": "Command",
      "description": "Binary to execute (for example: \"echo\").",
      "required": true,
      "defaultValueJson": "\"echo\""
    },
    {
      "key": "args",
      "inputType": "code",
      "label": "Args",
      "description": "JSON array passed as the command args.",
      "required": false,
      "defaultValueJson": "[\"hi\"]"
    }
  ]
}
LEGION_TEMPLATE_END
*/
{
  name: 'exec-template-macos',
  tasks: [
    {
      kind: 'exec',
      config: {
        command: [std.extVar('command')],
        args: std.parseJson(std.extVar('args')),
        cwd: '/Users/mike/Desktop',
      },
      constraints: {
        arch: 'aarch64',
        os: 'macos',
      },
      count: 10,
    },
  ],
}
