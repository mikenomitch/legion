// The UI's ACL mirror against the manager's engine, on the cases both are
// checked with (tests/fixtures/acl_parity_cases.json; the Rust side is
// `parity_fixture` in src/structs/acl.rs).
//
//   npm run test:unit
//
// Plain Node, no dependencies: Node 22.18+ loads the TypeScript engine
// directly (type stripping), which is why ui/src/utils/aclEngine.ts imports
// nothing.
import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { evaluateRules } from '../../src/utils/aclEngine.ts'

const here = dirname(fileURLToPath(import.meta.url))
const fixture = JSON.parse(
  readFileSync(resolve(here, '../../../tests/fixtures/acl_parity_cases.json'), 'utf8'),
)

let failed = 0
for (const testCase of fixture.cases) {
  const got = evaluateRules(testCase.rules, testCase.resource, testCase.action, testCase.scope)
  if (got !== testCase.allowed) {
    failed += 1
    console.error(`FAIL ${testCase.name}: expected allowed = ${testCase.allowed}, got ${got}`)
  }
}
const total = fixture.cases.length
if (failed > 0) {
  console.error(`${failed} of ${total} ACL parity cases failed`)
  process.exit(1)
}
console.log(`ok: ${total} ACL parity cases`)
