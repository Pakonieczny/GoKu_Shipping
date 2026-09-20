// Build the public site and stage only callable Netlify endpoints. Helpers stay
// in their original location for esbuild to follow through each import graph.
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const source = path.join(root, 'netlify/functions');
const output = path.join(root, 'netlify/production-functions');
const { endpoints: entries, modules } = require('./netlify-function-entries.json');
const classified = [...entries, ...modules];
const expected = new Set(classified);
if (classified.length !== expected.size) throw new Error('Duplicate function manifest entry');
for (const name of fs.readdirSync(source)) {
  if (!name.endsWith('.js')) continue;
  const file = path.join(source, name);
  if (!fs.lstatSync(file).isFile()) throw new Error('Expected function source file: ' + name);
  if (!expected.has(name)) {
    throw new Error('Classify the new source in scripts/netlify-function-entries.json: ' + name);
  }
}
for (const name of classified) {
  if (!/^[\w-]+\.js$/.test(name) || !fs.existsSync(path.join(source, name))) {
    throw new Error('Invalid or missing function entrypoint: ' + name);
  }
}
// Both directories contain reproducible build output only. Do not remove the
// rest of .netlify: it can contain site linkage and other build configuration.
fs.rmSync(path.join(root, '.netlify/functions'), { recursive: true, force: true });
fs.rmSync(output, { recursive: true, force: true });
fs.mkdirSync(output, { recursive: true });
for (const name of entries) {
  // A forwarding entry preserves original paths for both esbuild and zisi.
  // zisi does not resolve a symlink's relative imports from its real location.
  const text = fs.readFileSync(path.join(source, name), 'utf8');
  if (/^export default\b/m.test(text)) {
    const modernConfig = text.match(/^export const config\s*=\s*(\{[\s\S]*?\});/m);
    if (/^export const config\b/m.test(text) && !modernConfig) throw new Error('Expected literal function config: ' + name);
    fs.writeFileSync(path.join(output, name), `import handler from '../functions/${name}';\nexport default handler;\n` + (modernConfig ? modernConfig[0] + '\n' : ''));
    continue;
  }
  const config = text.match(/^exports\.config\s*=\s*(\{[\s\S]*?\});/m);
  if (/^exports\.config\s*=/m.test(text) && !config) {
    throw new Error('Expected literal inline function config: ' + name);
  }
  // Netlify reads inline config statically from the entrypoint, so retain its
  // literal declaration as well as forwarding the original runtime exports.
  fs.writeFileSync(path.join(output, name),
    `module.exports = require('../functions/${name}');\n` + (config ? config[0] + '\n' : ''));
}
for (const name of ['fonts', 'prompts']) {
  fs.cpSync(path.join(source, name), path.join(output, name), { recursive: true });
}
require('./build-public.cjs');
console.log(`Staged ${entries.length} callable functions; helpers, self-test modules and archives are not standalone endpoints.`);
