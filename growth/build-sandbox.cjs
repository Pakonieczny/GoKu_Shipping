'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');
const out = path.join(root, 'growth-sandbox-source');
const branch = 'codex/brites-growth-2026-10';

function assertSandboxBuild(env) {
  if (env.NETLIFY !== 'true') return;
  if (env.BRANCH !== branch) throw Error('The sandbox build requires the authorized growth branch.');
  if (env.BRITES_GROWTH_SANDBOX !== '1' || env.BRITES_GROWTH_NAMESPACE !== 'Brites_Growth_Sandbox') {
    throw Error('The sandbox build requires its isolated build environment.');
  }
}

function build() {
  assertSandboxBuild(process.env);
  // Remove only this reproducible stage. Reused public output must never retain
  // unrelated applications, and failed guards must not delete existing output.
  fs.rmSync(out, { recursive: true, force: true });
  const avatar = spawnSync(process.execPath, [path.join(root, 'scripts/build-concierge-avatar.cjs')], {
    cwd: root, stdio: 'inherit'
  });
  if (avatar.error || avatar.status !== 0) throw Error('The concierge avatar build failed.');
  const result = require('../scripts/build-growth-ads-sandbox.cjs').build(out);
  console.log(JSON.stringify(result));
  return result;
}

if (require.main === module) {
  try { build(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
module.exports = { assertSandboxBuild, build };
