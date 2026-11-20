// The nest's pure helpers live in charm-nest-nest-plan.js (cloud architecture v2, build step 6): buildJob, handLocks, feedTurn, orderRank, keepOrdersWhole, CHARM_SCALE,
// CAREFUL_ANGLES, the side-by-side searches and the KEEPSEARCH pick. charm-nest-1.html loads it with a <script> before its own code and keeps a thin function of each name.
// A test that runs a slice of the page in a vm loads the module into that vm first, as the page does:
//   const NP = require('./_nestPlan.cjs'); vm.createContext(ctx); NP.into(ctx);
// It is run IN the vm (not required here and handed over), so what it makes is made in the vm, as the page's own code was. `root`: another tree's files (an older tree
// without the module needs none: into() then does nothing and returns null).
'use strict';
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const REPO = path.join(__dirname, '../..');
const FILE = 'charm-nest-nest-plan.js';
const source = root => fs.readFileSync(path.join(root || REPO, FILE), 'utf8');
/** Loads the module into the vm context `ctx` as the global CharmNestNestPlan (whatever `module` or `self` the context has) → the module, or null when `root` has none. */
function into(ctx, root) {
  if (!fs.existsSync(path.join(root || REPO, FILE))) return null;
  vm.runInContext('(function (module, self) {' + source(root) + '\n}).call(this);', ctx);
  return ctx.CharmNestNestPlan;
}
module.exports = { into, source, FILE, REPO };
