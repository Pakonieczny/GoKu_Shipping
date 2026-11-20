// The piece maker (charm-nest-pool-make.js, PMAKE: cloud plan v2, step 3) for tests that run the bridge's own Pool or Orders source in a vm context.
// The page loads it as a classic script before charm-nest-bridge.js (window.CharmNestPoolMake); here it is run IN the test's context, so every object it
// makes belongs to that context, as the bridge's own objects did when this code lived in the bridge.
//   require('./_poolMake.cjs')(ctx)   → ctx.window.CharmNestPoolMake (the context is made one first when it is not)
//   require('./_poolMake.cjs').source → its text, for the tests that read the code itself
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const file = path.join(__dirname, '../../charm-nest-pool-make.js');
const source = fs.readFileSync(file, 'utf8');
function loadPoolMake(ctx) {
  if (!vm.isContext(ctx)) vm.createContext(ctx);
  const api = vm.runInContext(source + '\n;(typeof self !== "undefined" ? self : this).CharmNestPoolMake', ctx, { filename: file });
  if (!ctx.window) ctx.window = {};
  ctx.window.CharmNestPoolMake = api;
  return api;
}
module.exports = loadPoolMake;
module.exports.source = source;
module.exports.file = file;
