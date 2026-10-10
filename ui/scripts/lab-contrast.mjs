// WCAG contrast audit for every design-lab preset (src/themes/catalog.ts),
// using the same checks the theme studio shows live (src/themes/contrast.ts).
// Run: npm run lab:contrast   (Node >= 22.18 strips the TS types)
import { ALL_PRESETS, BASELINE_THEMES } from '../src/themes/catalog.ts'
import { auditTheme } from '../src/themes/contrast.ts'

let failures = 0
for (const theme of ALL_PRESETS) {
  const checks = auditTheme(theme)
  const bad = checks.filter((check) => check.ratio < check.min)
  // The shipped look is reported for reference; only candidates gate the exit
  // code (its faint-text gap is tracked in internal_docs/TODO.md).
  if (!BASELINE_THEMES.includes(theme)) failures += bad.length
  const worst = checks.reduce((a, b) => (a.ratio / a.min < b.ratio / b.min ? a : b))
  console.log(
    `${bad.length ? 'FAIL' : ' ok '} ${theme.name.padEnd(30)} worst: ${worst.label} ${worst.ratio.toFixed(2)} (min ${worst.min})` +
      (bad.length ? '\n      ' + bad.map((check) => `${check.label} ${check.ratio.toFixed(2)} < ${check.min}`).join('\n      ') : ''),
  )
}
process.exitCode = failures ? 1 : 0
