const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const files = fs.readdirSync(__dirname).filter(name => name.endsWith('.cjs') && name !== 'run.cjs').sort();
let failed = 0;
for (const file of files) {
  const result = spawnSync(process.execPath, [path.join(__dirname, file)], {
    cwd: path.resolve(__dirname, '../..'), encoding: 'utf8', timeout: 60000
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
