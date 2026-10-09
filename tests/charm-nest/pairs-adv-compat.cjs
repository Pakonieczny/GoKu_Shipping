// ADVCOMPAT (adversarial review, pairs 9 Oct 2026, flow: regression). Paul's rule: a line that is not a pair (a single charm, letters,
// discs without a count) gives the results it gave before the pairs work. Findings are in plans/pairs-1009/ADVCOMPAT-findings.md.
// Findings 1 and 2 (a plain quantity-N line was treated as a group) are settled by one shared definition (charm-nest-pair.js isGroupLine / inGroup):
// only earring pair lines (a Left and a Right piece), counted-option necklace lines (n discs / letters / charms) and lines the intake marks multi-piece are groups.
//   node tests/charm-nest/pairs-adv-compat.cjs                 checks that must hold now; an OPEN finding (none today) is listed and does not fail
//   ADV_STRICT=1 node tests/charm-nest/pairs-adv-compat.cjs    every open finding is a hard failure (green when they are all settled)
// Offline: the real modules read as source, no network, nothing written. The before/after harness that found these (old tree against new
// tree, 411 Sep 17 lines, 6823 per-SKU masters, fake Firestore read/write counts, headless Chromium) is not in the repo; this file keeps
// the cases that can be pinned without it.
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert');
const root = path.join(__dirname, '../..');
const STRICT = !!process.env.ADV_STRICT;
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
let passed = 0; const open = [];
const later = [];
const ok = (name, fn) => { const good = () => { passed++; console.log('ok    ' + name); }, bad = e => { console.error('FAIL  ' + name + '\n' + (e && e.stack || e)); process.exitCode = 1; }; try { const r = fn(); if (r && typeof r.then === 'function') later.push(r.then(good, bad)); else good(); } catch (e) { bad(e); } };
/** An open finding: strict mode fails when `fn` throws; otherwise the failure is listed as OPEN (and a pass is celebrated). */
const finding = (id, name, fn) => {
  const good = () => { passed++; console.log('ok    [finding ' + id + ' settled] ' + name); };
  const bad = e => { if (STRICT) { console.error('FAIL  [finding ' + id + '] ' + name + '\n' + (e && e.message || e)); process.exitCode = 1; } else { open.push(id); console.log('OPEN  [finding ' + id + '] ' + name + '\n        ' + String(e && e.message || e).split('\n')[0]); } };
  try { const r = fn(); if (r && typeof r.then === 'function') later.push(r.then(good, bad)); else good(); } catch (e) { bad(e); }
};
const slice = (src, a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i + a.length); if (i < 0 || j < 0) throw new Error('cannot slice ' + a); return src.slice(i, j); };

/* ── pins that must hold: a plain line is what it was ─────────────────────────────────────────────────────────────────────────────── */
const Orders = require(path.join(root, 'charm-nest-orders.js'));
const plainLine = (qty, extra) => Object.assign({ receipt_id: 4300000301, transaction_id: 5300000301, sku: 'PLAIN_CHARM_1', title: 'Gold Heart Charm Necklace Pendant', quantity: qty, variations: [{ formatted_name: 'Metal', formatted_value: 'Gold' }] }, extra || {});
ok('a plain charm line of quantity 1 and of quantity 3 makes exactly its quantity of pieces, none sided', () => {
  for (const q of [1, 3]) { const n = Orders.pieceCountOf(plainLine(q)); assert.equal(n, q); const ps = Orders.piecesOf ? Orders.piecesOf(plainLine(q)) : []; assert.ok(ps.every(p => !p.side), 'no side on a plain piece'); }
});
ok('a letters / initials line with no count option is still one piece per unit', () => {
  const l = plainLine(1, { sku: 'INITIAL_NECKLACE_1', title: 'Custom Initial Necklace Gold', variations: [{ formatted_name: 'Personalization', formatted_value: 'Initial: K' }] });
  assert.equal(Orders.pieceCountOf(l), 1);
});
ok('the station page counts a plain line by its quantity (StationLiveOrder.pieces)', () => {
  const g = { CharmNestOrders: Orders }; g.window = g;
  const src = read('station-live-order.js');
  const mod = { exports: {} };
  new Function('window', 'self', 'module', 'exports', 'document', 'localStorage', src + '\n;return typeof StationLiveOrder!=="undefined"?StationLiveOrder:(window.StationLiveOrder||module.exports);')(g, g, mod, mod.exports, undefined, undefined);
  const S = g.StationLiveOrder || mod.exports; assert.ok(S && S.pieces, 'StationLiveOrder.pieces');
  const out = S.pieces([{ transaction_id: 1, quantity: 2, sku: 'PLAIN_CHARM_1', title: 'Gold Heart Charm Necklace', variations: [] }, { transaction_id: 2, quantity: 1, sku: 'CAT(+FISH) - Cat Only', title: 'Cat Add On Charm Dainty Cat Charm', variations: [] }]);
  assert.equal(out.pieceCount, 3); assert.ok(out.pieces.every(p => !p.side && !p.both), 'no side and no glued flag on a plain piece (a SKU with a plus is no pair)');
});

/* ── findings 1 and 2 (fixed, Paul's ruling 9 Oct): a plain quantity-N line is NOT a group; a pair, counted discs and an intake-marked multi-piece line still are ───────── */
const Placement = require(path.join(root, 'netlify/functions/_charmNestPlacement.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const ids = [1, 2, 3, 4].map(n => `4175364753_5219708940_${n}`), otherId = '4175370999_5219709999_1';
ok('Placement.groupMates leaves the other copies of a plain quantity-4 charm line alone (no group info: nothing is a group)', () => {
  const sheet = { poolIds: ids.concat(otherId) };
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]])), []);
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]]), new Set()), []);
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]]), () => false), []);
});
ok('Placement.groupMates still lifts the rest of a GROUP line (a pair, counted discs) and only that line', () => {
  const sheet = { poolIds: ids.concat(otherId) }, key = '4175364753:5219708940';
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]]), new Set([key])), [ids[0], ids[2], ids[3]]);
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]]), k => k === key), [ids[0], ids[2], ids[3]]);
  assert.deepEqual(Placement.groupMates(sheet, new Set([ids[1]]), true), [ids[0], ids[2], ids[3]], 'true = every candidate (the take-off asks the rows before it keeps them)');
  assert.deepEqual(Placement.groupMates(sheet, new Set([otherId]), new Set([key])), [], 'a group the take-off does not name brings nothing');
  assert.deepEqual(Placement.groupMates(sheet, new Set(ids), new Set([key])), [], 'all named: nothing left over');
});
ok('a take-off of a single-piece line has no mates (nothing extra comes off)', () => {
  assert.deepEqual(Placement.groupMates({ poolIds: ['4175370999_5219709999_1', '4175364753_5219708940_1'] }, new Set(['4175370999_5219709999_1']), true), []);
});
ok('splitsOf: the copies of a plain quantity-4 line spread over two sheets are not a split group; a pair or a counted group with the same spread is', () => {
  const placement = {}, pools = [];
  ids.forEach((id, i) => { placement[id] = { state: 'sheet', sheetId: i < 2 ? 's1' : 's2', sheetLabel: 'S' + (i < 2 ? 1 : 2) }; pools.push({ poolId: id }); });
  assert.deepEqual(Placement.splitsOf(placement, pools), [], 'plain rows carry no group size');
  const sized = pools.map(r => Object.assign({}, r, { groupSize: 4 }));
  assert.equal(Placement.splitsOf(placement, sized).length, 1, 'rows of a group do');
});

/* the one shared definition (charm-nest-pair.js) */
const ln = (qty, extra) => Object.assign({ key: '4175364753_5219708940', sku: 'PLAIN_CHARM_1', title: 'Gold Heart Charm Necklace Pendant', quantity: qty, spec: { quantity: qty, pieceCount: qty } }, extra || {});
ok('isGroupLine: a plain line of quantity 1 or 4 is no group; a counted-option line (n discs, quantity 1) is; an earring pair line is; a glued legacy line is not', () => {
  assert.equal(Pair.isGroupLine(ln(1)), false); assert.equal(Pair.isGroupLine(ln(4)), false);
  assert.equal(Pair.isGroupLine(ln(1, { spec: { quantity: 1, pieceCount: 3 } })), true);
  assert.equal(Pair.isGroupLine(ln(2, { spec: { quantity: 2, pieceCount: 4 } })), true, 'two sets of two discs per unit');
  assert.equal(Pair.isGroupLine(ln(1, { spec: { quantity: 1, pieceCount: 2, pair: { kind: 'pair' } } })), true);
  assert.equal(Pair.isGroupLine(ln(1, { spec: { quantity: 1, pieceCount: 3, pair: { glued: true } } })), false);
});
ok('inGroup / sharedKey: groupSize 2 or more, or an ear with no size, is a group; no size and no ear, a size of 1 or 0, is not', () => {
  const k = '4175364753:5219708940';
  assert.equal(Pair.inGroup({ groupKey: k, groupSize: 4 }), true); assert.equal(Pair.inGroup({ groupKey: k, side: 'L' }), true);
  assert.equal(Pair.inGroup({ groupKey: k }), false); assert.equal(Pair.inGroup({ groupKey: k, groupSize: 1 }), false); assert.equal(Pair.inGroup({ groupKey: k, groupSize: 0 }), false);
  assert.equal(Pair.inGroup({ groupKey: k, groupSize: 1, side: 'L' }), false, 'a single earring that names its ear (size 1) is alone');
  assert.equal(Pair.sharedKey({ groupKey: k }), ''); assert.equal(Pair.sharedKey({ groupKey: k, groupSize: 3 }), k);
  assert.equal(Pair.mustShareSheet({ groupKey: k }, { groupKey: k }), false); assert.equal(Pair.mustShareSheet({ groupKey: k, groupSize: 2 }, { groupKey: k, groupSize: 2 }), true);
});

/* the sorter (charm-nest-1.html) */
const html = read('charm-nest-1.html');
const sorter = () => {
  const log = [], ctx = { agent: (...a) => log.push(['agent', String(a[2] || '')]), toast: (...a) => log.push(['toast', a[0]]), console };
  vm.createContext(ctx);
  let src = '';
  if (html.includes('function groupOf(')) src += slice(html, 'function groupOf(c)', '/** A multi-piece order is never split');
  src += slice(html, 'function keepOrdersWhole(sh, byId', 'function orderSummary(charms)') + slice(html, 'function orderSummary(charms)', 'function overflowToNextSheet') + ';this.keepOrdersWhole=keepOrdersWhole;this.orderSummary=orderSummary;';
  vm.runInContext(src, ctx);
  const pages = []; Object.assign(ctx, { allSheets: () => pages, manualSheetClosed: () => false, pagesOf: () => pages, S: { unassigned: [] }, flushManualIntake() {}, renderRail() {}, updateTopSub() {}, renderCard() {}, sheetDirty() {}, window: { Session: null, CharmNestPair: Pair } });
  vm.runInContext(slice(html, 'function assignCharm(charm, metal)', 'function removeSource(src)') + ';this.assignCharm=assignCharm;', ctx);
  return { ctx, log, pages };
};
const ch = (id, order, poolId, rid, tx, extra) => Object.assign({ id, order, poolId, orderDate: 1, orderInfo: { receiptId: rid, transactionId: tx } }, extra || {});
ok('orderSummary of plain lines says what it said (orders, multi-piece), no pairs', () => {
  const { ctx } = sorter();
  const r = ctx.orderSummary([ch('c1', 'o1', '4175364753_5219708940_1', 4175364753, 5219708940), ch('c2', 'o1', '4175364753_5219708940_2', 4175364753, 5219708940), ch('c3', 'o2', '4175370999_5219709999_1', 4175370999, 5219709999)]);
  assert.equal(r.text, '2 order(s), 1 multi-piece');
});
ok('a single-quantity charm moves to another metal by hand as it always did', () => {
  const { ctx, pages } = sorter(); const s1 = ch('s1', 'o9', '4175370555_5219700000_1', 4175370555, 5219700000);
  pages.push({ metal: 'gold', charms: [s1], placements: [], status: 'ready' }); ctx.assignCharm(s1, 'silver'); assert.equal(s1.pendingMetal, 'silver');
});
ok('one copy of a plain quantity-2 line can still be moved by hand to another metal (as before the pairs work)', () => {
  const { ctx, log, pages } = sorter(); const a = ch('c1', 'o1', '4175364753_5219708940_1', 4175364753, 5219708940), b = ch('c2', 'o1', '4175364753_5219708940_2', 4175364753, 5219708940);
  pages.push({ metal: 'gold', charms: [a, b], placements: [], status: 'ready' }); ctx.assignCharm(a, 'silver');
  assert.equal(a.pendingMetal, 'silver', 'refused: ' + (log.find(x => x[0] === 'toast') || [])[1]);
});
ok('a pair (groupSize 2) or counted discs (groupSize 3) still cannot be moved to another metal one piece at a time', () => {
  for (const [size, n] of [[2, 2], [3, 3]]) {
    const { ctx, log, pages } = sorter(); const cs = Array.from({ length: n }, (_, i) => ch('c' + i, 'o1', `4175364753_5219708940_${i + 1}`, 4175364753, 5219708940, { groupSize: size, groupKey: '4175364753:5219708940' }));
    pages.push({ metal: 'gold', charms: cs, placements: [], status: 'ready' }); ctx.assignCharm(cs[0], 'silver');
    assert.notEqual(cs[0].pendingMetal, 'silver', 'a group of ' + size + ' moved alone'); assert.ok(log.some(x => x[0] === 'toast'), 'and it says why');
  }
});
ok('a saved copy of a plain quantity-2 line stays on its top-up sheet when the other copy did not fit (keepOrdersWhole)', () => {
  const { ctx } = sorter(); const a = ch('c1', 'o1', '4175364753_5219708940_1', 4175364753, 5219708940), b = ch('c2', 'o1', '4175364753_5219708940_2', 4175364753, 5219708940), o = ch('c3', 'o2', '4175370999_5219709999_1', 4175370999, 5219709999);
  const byId = new Map([a, b, o].map(x => [x.id, x]));
  const sh = { metal: 'gold', placements: [{ id: 'c1' }, { id: 'c3' }], rejects: ['c2'], appendOnly: true, nestInitial: [{ id: 'c1' }, { id: 'c3' }], topup: { base: .7, tried: [] } };
  ctx.keepOrdersWhole(sh, byId);
  assert.deepEqual(sh.placements.map(p => p.id), ['c1', 'c3'], 'saved copy lifted off: ' + JSON.stringify(sh.placements.map(p => p.id)));
});

/* ── finding 3 (fixed): two stacked bars of different ink are not a Left / Right pair ───────────────────────────────────────────────────────── */
const rect = (x0, y0, x1, y1, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]]] }, extra || {});
const eng = (x0, y0, x1, y1, rgb) => Object.assign(rect(x0, y0, x1, y1), { layer: 'ENGRAVE', strokeRGB: rgb || [1, 0, 0] });
ok('a master that draws two bars one ABOVE the other (BAR_BRACELET_1646, CUSTOMIZED_6964, GOLD_9976, VERTICAL_BAR_1277) is not read as a mismatched pair by its geometry (finding 3, fixed by PAIRMASTER 338dc247)', () => {
  const top = rect(11, 39, 96, 53), bottom = rect(11, 11, 96, 25), ink = [eng(15, 42, 90, 50), eng(15, 44, 20, 48)];
  const stacked = { outline: top, members: [top, ...ink, bottom], bbox: [11, 11, 96, 53] };
  assert.equal(Pair.isMismatched(stacked), false, 'bodies stacked in one column read as a Left and a Right (isMismatched true)');
});
ok('a row of two bodies of one cut shape and different engraving is still a mismatched pair (MITTENS)', () => {
  const o1 = rect(0, 0, 20, 26), e1 = eng(4, 2, 16, 6), o2 = rect(21, 0, 41, 26), e2 = eng(25, 10, 30, 15, [0, 0, 1]);
  assert.equal(Pair.isMismatched({ outline: o2, members: [o2, e2, o1, e1], bbox: [0, 0, 41, 26] }), true);
});

/* ── finding 4 (fixed): hovering the piece count of a sheet with no pair reads nothing from the cloud ──────────────────────────────────────── */
ok('onCountHover (Library / laser cards) does not call OrderPieces.loadSheet for a sheet that holds no pair (finding 4, fixed by PAIRSHEETWIN c2c3ccc4)', async () => {
  const src = read('charm-nest-library.js'), fn = slice(src, 'function onCountHover(e) {', '\n  window.LibraryDone = ') ;
  const calls = []; const readOnce = new Set();
  const win = { PiecePlacement: { sheetWords: () => '' }, OrderPieces: { loadSheet: id => { calls.push(id); return Promise.resolve(true); } } };
  const span = { dataset: { sheetCount: 'sheet-plain-1' }, textContent: '12/12', removeAttribute() {}, set title(v) {} };
  const ev = { target: { closest: () => span } };
  new Function('window', 'readOnce', fn.replace(/^function onCountHover/, 'return function onCountHover'))(win, readOnce)(ev);
  await new Promise(r => setTimeout(r, 20));   // (loadSheet is called a moment after the hover)
  assert.deepEqual(calls, [], 'loadSheet called for: ' + calls.join(', '));
});

Promise.all(later).then(() => console.log(`\n${passed} checks passed` + (open.length ? `, ${open.length} open finding(s): ${[...new Set(open)].join(', ')} (ADV_STRICT=1 makes them failures)` : '')));
