// SKUSHOW (Paul, 25 Nov 2026, order 4248582494): a SKU changed, the cloud made the piece again as the new charm (same pool id, another drawing) and named the sheet on the feed; the page kept drawing the old charm.
// charm-nest-cloud-sheets.js in a bare context, fed the record as the cloud has it:
//   1  a sheet shown only as its saved record whose piece is another charm now: drawn again from the new record (Recall.redraw), the order window and thumbnails painted
//   2  the same record read again (nothing of its pieces moved): not drawn again, nothing painted
//   3  a sheet built on this page, a newer revision saved elsewhere: rebuilt AND painted (it painted nothing before)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-cloud-sheets.js'), 'utf8');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c ? '' : ' :: ' + JSON.stringify(d))); if (!c) failed++; };
const charm = (sku, hash) => ({ poolId: '4248582494_5311392136_1', sku, hash, lineKey: '4248582494_5311392136' });
const calls = { redraw: 0, rebuild: 0, remade: [], thumbs: 0 };
let rec = null;
const pages = [];
const ctx = { console: { log() {}, info() {}, warn() {} }, setTimeout: () => 0, clearTimeout() {}, Promise, Date, Math, Number, String, Array, Object, Set, Map, JSON };
ctx.allSheets = () => pages;
ctx.CN = { api: async (name, b) => (b.op === 'getSheet' ? { sheet: JSON.parse(JSON.stringify(rec)), pools: [], rev: String(rec.layoutRev) + ':' + rec.charms[0].hash } : {}), renderCard() {}, agent() {} };
ctx.Recall = { redraw: async p => { calls.redraw++; p.charms = JSON.parse(JSON.stringify(p.answer.charms)); return true; }, rebuild: async (p, o) => { calls.rebuild++; p.charms = JSON.parse(JSON.stringify(o.answer.charms)); }, piecesOf: async () => [] };
ctx.OrderWin = { remade: ids => calls.remade.push(ids.join()) };
ctx.CharmNestMadeThumb = { refreshSoon: () => { calls.thumbs++; } };
ctx.window = ctx; ctx.self = ctx;
vm.createContext(ctx);
vm.runInContext(src.replace(/\}\)\(typeof window[^)]*\)[^;]*;?\s*$/, '})(this);'), ctx);
const CS = ctx.CharmNestCloudSheets;
(async () => {
  ok('the module loads and exposes the feed handler', !!(CS && CS._changed));
  // 1
  const p1 = { sheetId: 'd-29f20ca4af6d742e', status: 'complete', recalled: { id: 'd-29f20ca4af6d742e' }, charms: [charm('PENGUIN 2', 'dc072efd')], placements: [], dirty: false };
  pages.push(p1);
  rec = { id: p1.sheetId, layoutRev: 57, charms: [charm('PENGUIN 2 SHAPE', '2d8228ed')], placements: [] };
  await CS._changed(['s:' + p1.sheetId]);
  ok('1 a record-only sheet whose piece is another charm now is drawn again from the new record', calls.redraw === 1 && p1.charms[0].sku === 'PENGUIN 2 SHAPE' && p1.answer === p1.recalled && p1.answer.charms[0].hash === '2d8228ed', { calls, c: p1.charms });
  ok('1 ... the order window and the thumbnails are painted', calls.remade.includes(p1.sheetId) && calls.thumbs >= 1, calls);
  // 2
  const r0 = calls.redraw, m0 = calls.remade.length;
  rec = Object.assign({}, rec, { label: 'GF Draft 26' });
  await CS._changed(['s:' + p1.sheetId]);
  ok('2 the same pieces read again: not drawn again, nothing painted', calls.redraw === r0 && calls.remade.length === m0, calls);
  // 3
  pages.length = 0;
  const p3 = { sheetId: 'd-live', status: 'complete', layoutRev: 5, charms: [charm('PENGUIN 2', 'dc072efd')], placements: [], dirty: false };
  pages.push(p3);
  rec = { id: 'd-live', layoutRev: 6, charms: [charm('PENGUIN 2 SHAPE', '2d8228ed')], placements: [] };
  const b0 = calls.rebuild, m1 = calls.remade.length;
  await CS._changed(['s:d-live']);
  ok('3 a sheet built here, a newer revision saved elsewhere: rebuilt and painted', calls.rebuild === b0 + 1 && p3.charms[0].sku === 'PENGUIN 2 SHAPE' && calls.remade.length === m1 + 1 && calls.remade.includes('d-live'), { calls, c: p3.charms });
  console.log(failed ? `FAIL sku-show-follower (${failed})` : 'PASS sku-show-follower');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL sku-show-follower: ' + (e && e.stack || e)); process.exit(1); });
