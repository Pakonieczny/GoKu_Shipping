const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

// Preloaded into each suite: a suite whose awaited step never settles fails instead of exiting 0 with checks skipped.
const guard = 'suite-guard.cjs';
const files = fs.readdirSync(__dirname).filter(name => name.endsWith('.cjs') && name !== 'run.cjs' && name !== guard).sort();
let failed = 0;
for (const file of files) {
  const result = spawnSync(process.execPath, ['-r', path.join(__dirname, guard), path.join(__dirname, file)], {
    cwd: path.resolve(__dirname, '../..'), encoding: 'utf8', timeout: 300000
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  if (result.status !== 0) {
    failed++;
    process.stderr.write(`${file}: ${result.error ? result.error.message : 'failed'}\n`);
  }
}
console.log(`${files.length - failed}/${files.length} ad regression suites passed.`);
process.exitCode = failed ? 1 : 0;
