// Preloaded into every suite by run.cjs: node -r ./tests/adwords/suite-guard.cjs tests/adwords/<suite>.cjs
//
// An async suite ends each of its top-level async functions with this line:
//   require('./suite-guard.cjs').done();
// If a step awaits something that never settles (a fake that never resolves), Node runs out of work and exits with
// status 0, although the checks after that step never ran. This fails such a suite instead: when the event loop
// drains before every such line in the suite was reached. A suite without the line, an explicit process.exit() (a
// deliberate skip) and a suite that already failed are left as they are.
'use strict';
const fs = require('node:fs'), path = require('node:path');
const MARK = /require\((['"])\.\/suite-guard\.cjs\1\)\.done\(\)/g;
let reached = 0;
exports.done = () => { reached++; };

// Run directly (not as a preload or from a suite), it is not a suite: nothing to check.
if (require.main !== module) process.on('beforeExit', () => {
  const suite = process.argv[1];
  if (process.exitCode || !suite) return;
  let marks;
  try { marks = (fs.readFileSync(suite, 'utf8').match(MARK) || []).length; } catch { return; }
  if (reached >= marks) return;
  console.error(`FAIL ${path.basename(suite)}: it exited before its last check. A step awaited something that never settled, `
    + `so Node ran out of work (${marks - reached} of ${marks} async part${marks > 1 ? 's' : ''} unfinished).`);
  process.exitCode = 1;
});
