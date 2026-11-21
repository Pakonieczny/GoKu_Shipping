// GATECORE (Paul, 20 Nov 2026: the page and the cloud decide sets with ONE copy): the page's Gate (charm-nest-bridge.js) asks charm-nest-assembly-core.js for every decision of the set
// assembly, and the core reads charm-nest-place.js (SPLITWAIT). The page loads both as classic scripts before charm-nest-bridge.js; a test that runs the bridge's Gate in a vm context
// loads them IN that context the same way, so every object they make belongs to it, as the Gate's own objects do.
//   require('./_gateCore.cjs')(ctx)   → after the test made its `window` in the context: window.CharmNestPlace and window.CharmNestAssemblyCore (each kept when the test gave its own)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const files = { place: path.join(__dirname, '../../charm-nest-place.js'), core: path.join(__dirname, '../../charm-nest-assembly-core.js') };
const sources = { place: fs.readFileSync(files.place, 'utf8'), core: fs.readFileSync(files.core, 'utf8') };
function gateCore(ctx) {
  if (!vm.isContext(ctx)) vm.createContext(ctx);
  vm.runInContext(sources.place, ctx, { filename: files.place });
  vm.runInContext(sources.core, ctx, { filename: files.core });
  vm.runInContext(`(function () { const w = typeof window !== "undefined" ? window : (this.window = this.window || {});
    if (!w.CharmNestPlace) w.CharmNestPlace = CharmNestPlace; if (!w.CharmNestAssemblyCore) w.CharmNestAssemblyCore = CharmNestAssemblyCore; }).call(this)`, ctx);
  return ctx;
}
module.exports = gateCore;
module.exports.files = files;
