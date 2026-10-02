'use strict';
const path = require('node:path'), fs = require('node:fs');
async function main() {
  const root = path.resolve(__dirname, '..'), output = path.join(root, 'assets', 'brites-concierge-avatar-scene.mjs');
  let esbuild; try {esbuild = require('esbuild');} catch {throw Error('Install the existing project dependencies (Three.js and Vite/esbuild) before building the avatar.');}
  fs.mkdirSync(path.dirname(output), {recursive: true});
  await esbuild.build({entryPoints: [path.join(root, 'brites-concierge-avatar-scene.mjs')], outfile: output, bundle: true, format: 'esm', platform: 'browser', target: ['es2020'], minify: true, sourcemap: false, legalComments: 'eof'});
  const bytes = fs.statSync(output).size; console.log(JSON.stringify({asset: path.relative(root, output), bytes, source: 'Local Three.js dependency and official addons; no external runtime downloads.'}));
}
main().catch(error => {console.error(error.message); process.exitCode = 1;});
