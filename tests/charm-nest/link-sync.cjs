// LINKDONE and SYNCNOW (Paul, 22 Nov 2026, order 4244297416: "the one necklace charm should have changed which it didn't" · "Now it updated, but it took several minutes. This is
// unacceptable ... Once all the green checkmarks are visible to the user everything by then should have been updated. Nothing should be waiting to update afterwards"). The bridge's real
// noticeSet and freshNow run in a vm with the page's other parts stood in. No network, no Etsy.
//   1  LINKDONE: a saved link read while the old piece is still on its sheet offers "Changed … (a saved link)"; once the pieces ARE the linked charm (the cloud made them again) the card
//      is taken back and the line is no longer "changed" (no hold); a card for an order Etsy changed is never taken back by it
//   2  SYNCNOW: freshNow reads the cloud's reading of the orders, the run's lines, the pieces of the lines (poolGet) and the sheets (forced) BEFORE drawing again, and the pieces it reads
//      replace what the page held
//   3  the windows say "Updated everywhere" (Change SKU) / done (Just this order, Update repository) only AFTER freshNow
//   node tests/charm-nest/link-sync.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const cut = (from, to) => { const i = src.indexOf(from), j = src.indexOf(to, i); if (i < 0 || j < 0) throw new Error('not found: ' + from); return src.slice(i, j); };

/* ═══ 1 · LINKDONE ═══ */
{
  const code = cut('  const setToldFixed = new Set();', '  /** REVHOLD: the line\'s pieces are on a sheet and the order is in Review');
  const items = [];
  const w = { console, Math, String, Set, Map, Object, JSON, Array };
  w.B = { review: { items } };
  w.Review = { add: it => { const i = items.findIndex(x => x.key === it.key); if (i >= 0) items.splice(i, 1); items.push(it); }, remove: k => { const i = items.findIndex(x => x.key === k); if (i >= 0) items.splice(i, 1); } };
  w.O = { setDesignsOf: () => null, sameDesign: (a, b) => String(a).toUpperCase() === String(b).toUpperCase() };
  w.Master = { entryFor: s => ({ sku: s }) };
  w.allSheets = () => []; w.replaceLaserWhy = () => ''; w.keepsPlace = () => false; w.agent = () => {}; w.TL = { line: () => {}, hash: () => 'h' };
  vm.runInContext(code + '\nthis.noticeSet = noticeSet;', vm.createContext(w));
  const row = { key: '4244297416_5306026548', state: 'pooled', order: { receiptId: '4244297416' }, spec: { skuSource: 'alias', designSku: 'SPORTS 12 - BULLSEYE', problems: [], quantity: 1, pieceCount: 1, material: 'gold' }, poolIds: ['p1'] };
  w.noticeSet(row, [{ sku: 'SPORTS 12- BULLSEYE', copy: 1 }]);
  ok('1 · the link read while the old piece is still on its sheet offers the card (the line changed)', items.some(x => x.key === 'chg:' + row.key && /\(a saved link\)/.test(x.why)) && row.changePending && row.repoolChanged, { items, row });
  w.noticeSet(row, [{ sku: 'SPORTS 12 - BULLSEYE', copy: 1 }]);
  ok('1 · LINKDONE: once the piece IS the linked charm (the cloud made it again), the card is taken back and the line is no longer "changed"', !items.some(x => x.key === 'chg:' + row.key) && !row.changePending && !row.repoolChanged, { items, row });
  const etsy = { key: 'chg:' + row.key, kind: 'orderChanged', why: 'Etsy changed this order: 2 pieces now' }; items.push(etsy); row.changePending = true; row.repoolChanged = true;
  w.noticeSet(row, [{ sku: 'SPORTS 12 - BULLSEYE', copy: 1 }]);
  ok('1 · a card for an order Etsy changed is never taken back by it', items.includes(etsy) && row.changePending, { items });
}
/* ═══ 2 · SYNCNOW ═══ */
(async () => {
  const code = cut('  async function freshNow(o) {', '  window.CNFreshNow = freshNow;');
  const order = [];
  const pool = new Map([['p1', { sku: 'SPORTS 12- BULLSEYE', updatedAt: 1 }]]);
  const rows = [{ key: 'k1', spec: { designSku: 'SPORTS 12 - BULLSEYE' }, poolIds: ['p1'] }, { key: 'k2', spec: { designSku: 'OTHER' }, poolIds: ['p2'] }];
  const w = { console, Promise, Object, Set, Map, String, Array, JSON };
  w.window = w;
  w.CloudWorld = { refresh: async why => { order.push('world:' + why); } };
  w.CharmNestRunFollow = { now: async why => { order.push('lines:' + why); }, state: () => ({ runId: 'R1' }) };
  w.Recall = { follow: async (rid, o) => { order.push('sheets:' + rid + ':' + !!(o && o.force)); } };
  w.B = { pool: { rows: pool }, orders: { byKey: new Map(rows.map(r => [r.key, r])) }, run: null };
  w.Orders = { rows: () => rows, interpretAll: () => order.push('read'), render: () => order.push('draw') };
  w.syncOrderItems = () => order.push('items');
  w.api = async (fn, b) => { order.push('pieces:' + b.poolIds.join(',')); return { pools: { p1: { sku: 'SPORTS 12 - BULLSEYE', updatedAt: 5, sheetId: 'd-346' } } }; };
  vm.runInContext(code + '\nthis.freshNow = freshNow;', vm.createContext(w));
  const r = await w.freshNow({ keys: ['k1'] });
  ok('2 · SYNCNOW: the cloud\'s reading, the run\'s lines, the pieces and the sheets (forced) are read BEFORE the page is drawn again', JSON.stringify(order) === JSON.stringify(['world:sku', 'lines:sku', 'pieces:p1', 'sheets:R1:true', 'read', 'items', 'draw']), order);
  ok('2 · the piece the cloud made replaces what the page held', pool.get('p1').sku === 'SPORTS 12 - BULLSEYE' && pool.get('p1').sheetId === 'd-346' && r.pieces === 1 && r.sheets === true, { p1: pool.get('p1'), r });
  order.length = 0; await w.freshNow({ sku: 'SPORTS 12 - BULLSEYE' });
  ok('2 · asked for a charm, it reads every line of that charm (and no other)', order.includes('pieces:p1') && !order.some(x => /p2/.test(x)), order);
  /* ═══ 3 · the windows ═══ */
  const end = cut('    const end = async (flow, all, linked) => {', '    const later = (flow, all, linked)');
  ok('3 · Change SKU says "Updated everywhere" only after freshNow', end.indexOf('await freshNow(') > 0 && end.indexOf('await freshNow(') < end.indexOf('"Updated everywhere'));
  const once = cut('  async function editSkuOnce(row, sku, opts) {', '    const rid = String(row.order.receiptId), was = off.cur');
  ok('3 · Just this order answers done only after freshNow', /if \(ans && ans\.ok\) \{[\s\S]{0,300}?await freshNow\(\{ keys: \[row\.key\] \}\)[\s\S]{0,80}?return \{ ok: true, sku, via: "once"/.test(once));
  const repo = fs.readFileSync(path.join(ROOT, 'charm-nest-ce-repo.js'), 'utf8');
  ok('3 · Update repository says it is done only after freshNow (every line of the charm)', /await root\.CNFreshNow\(\{ sku: st\.id\.sku \}\)/.test(repo) && repo.indexOf('await root.CNFreshNow(') < repo.indexOf("log('repoUpdate'"));
  ok('3 · the page loads the changed bridge under a new token', /charm-nest-bridge\.js\?v=[^"]*-ld1(?:-\w+)*"/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')));
  console.log(failed ? `FAIL link-sync (${failed})` : 'PASS link-sync'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL link-sync: ' + (e && e.stack || e)); process.exit(1); });
