// Builds the `js` image (crates/isolate/images/build.sh runs it):
//
//   node build.mjs <node_modules dir> <runner.js> <wit dir> <out.wasm>
//
// The node_modules directory is the pinned toolchain's (package.json and
// package-lock.json beside this file).
import { writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [modules, sourcePath, witPath, out] = process.argv.slice(2);
if (!out) {
  console.error('usage: node build.mjs <node_modules dir> <runner.js> <wit dir> <out.wasm>');
  process.exit(2);
}
const pkgDir = join(modules, '@bytecodealliance', 'componentize-js');
const { version } = JSON.parse(await readFile(join(pkgDir, 'package.json'), 'utf8'));
const { componentize } = await import(pathToFileURL(join(pkgDir, 'src', 'componentize.js')).href);
// componentize-js reads the WIT through a WASI shim that, in this version,
// starts with no preopened directories; give it the WIT directory (the same
// module instance the splicer resolved: node_modules/@bytecodealliance/preview2-shim).
const shim = join(modules, '@bytecodealliance', 'preview2-shim', 'dist', 'nodejs', 'filesystem.js');
const { _addPreopen } = await import(pathToFileURL(shim).href);
_addPreopen(witPath, witPath);
const started = Date.now();
const { component } = await componentize({
  sourcePath,
  witPath,
  worldName: 'js',
  // weval AOT measured no gain for dynamic source (README, "Measurements").
  enableAot: false,
});
await writeFile(out, component);
console.log(`js: componentize-js ${version}, ${component.length} bytes in ${Date.now() - started} ms`);
