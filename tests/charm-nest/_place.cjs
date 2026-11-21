// The sheet choice (charm-nest-place.js, SHEETCHOICE: cloud plan v2, step 5) for tests that run the bridge's own Pool, LiveNest or the page's topup code in a vm context.
// The page loads it as a classic script before charm-nest-bridge.js (window.CharmNestPlace); here it is run IN the test's context, so every object it makes belongs to that
// context, as the bridge's own objects did when this code lived in the bridge. The context should hold CharmNestNoSpread, CharmNestNestPlan and CharmNestPair when the
// rules that read them are run (the page loads them first too).
//   require('./_place.cjs')(ctx)   → ctx.window.CharmNestPlace (the context is made one first when it is not)
//   require('./_place.cjs').source → its text, for the tests that read the code itself
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const file = path.join(__dirname, '../../charm-nest-place.js');
const source = fs.readFileSync(file, 'utf8');
function loadPlace(ctx) {
  if (!vm.isContext(ctx)) vm.createContext(ctx);
  const api = vm.runInContext(source + '\n;(typeof self !== "undefined" ? self : this).CharmNestPlace', ctx, { filename: file });
  if (!ctx.window) ctx.window = {};
  ctx.window.CharmNestPlace = api;
  if (!ctx.CharmNestPlace) ctx.CharmNestPlace = api;
  return api;
}
module.exports = loadPlace;
module.exports.source = source;
module.exports.file = file;
