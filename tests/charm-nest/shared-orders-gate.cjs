// The cardinal rule inside the set assembly (Gate.assemble, charm-nest-bridge.js; Paul, 5 Oct: every sheet that shares a multi-piece
// order with another is in the SAME set). The real Gate over fake pages, no cloud and no browser (as solid-per-sheet.cjs):
//   a partner that cannot release by itself is pulled into the set with its mate · both wait while one is still nesting · a set
//   already started takes what can come and says what cannot · Rose Gold is never pulled by itself · a partner in a committed set is
//   named, nothing is held for it · removing the order lets the pulled sheet go again · untick of Include moves the group together
//   or says why not · a pulled solid sheet is written without its own tick and never reloads as ticked.
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), Ops = require('../../charm-nest-operations.js'), O = require('../../charm-nest-orders.js');
const SO = require('../../charm-nest-shared-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8'), start = source.indexOf('const Gate ='), end = source.indexOf('/* ═══ 21', start);
const ops = Ops.create(), sets = [], saved = new Map();
const run = { runId: 'r', releasePolicy: 2, status: 'review', step: 'engrave', solidIncluded: {}, errors: [] };
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
let pages = [];
const sheet = (metal, n, keys, extra = {}) => ({ metal, page: n, runId: 'r', sheetId: `${metal}-${n}`, draft: true, status: 'complete', outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true },
  charms: keys.map((k, i) => ({ id: `c${metal}${n}_${i}`, poolId: k, order: k.split('_')[0] })), placements: keys.map((k, i) => ({ id: `c${metal}${n}_${i}` })), ...extra });
const ctx = { window: { CharmNestOrders: O, CharmNestOperations: ops, SharedOrders: SO }, B: { run, sets: new Map() }, S: { mode: 'nest', settings: {}, cloud: { ok: true }, library: { rows: [] } }, O, allSheets: () => pages, pagesOf: m => pages.filter(p => p.metal === m), refreshAllCards: () => {}, Session: { schedule() {} }, document: { getElementById: () => null }, toast: () => {}, Pool: { update: async () => {} }, Orders: { rows: () => [] }, Engrave: { items: () => new Map(), saveSheetBacks: async () => {} }, RunCtl: { save: async () => {}, poke() {}, onSheetDone() {}, membershipUpdated() {} },
  CN: { sheetFileBase: sh => 'Set-1-Sheet-' + sh.page, persistSheet: () => { throw Error('must not re-upload artwork'); }, renderLibrary() {} },
  Sets: { ofRun: () => sets, ensure: async () => { const set = { setId: 'set', runId: 'r', seq: 1, day: '2026-10-05', group: 'dispatch', sheetIds: [], labelFiles: [], materials: [], orders: {} }; sets.push(set); return set; }, save: async () => {}, labelsReady: () => true,
    onSheetSaved: async sh => { const set = sets.find(s => s.setId === 'set'); if (!set.sheetIds.includes(sh.sheetId)) set.sheetIds.push(sh.sheetId); } },
  api: async (name, body) => { if (body.sheet) saved.set(body.sheet.id, Object.assign(saved.get(body.sheet.id) || {}, body.sheet)); return {}; }, stockFor: () => ({ wIn: 1, hIn: 1 }), labelOf: m => CODE[m], esc: x => x };
vm.createContext(ctx); vm.runInContext(source.slice(start, end), ctx); const Gate = ctx.window.Gate;
// the page's own records of its sets (committed ones are fixed)
SO.configure({ sets: id => sets.find(s => s.setId === id) || null });
const inSet = () => pages.filter(p => !p.draft && p.setId === 'set').map(p => `${CODE[p.metal]}${p.page}`).sort();
const reset = list => { pages = list; sets.length = 0; saved.clear(); for (const k of Object.keys(run.solidIncluded)) delete run.solidIncluded[k]; };
const gold = (n, keys, extra) => sheet('gold', n, keys, extra), silver = (n, keys, extra) => sheet('silver', n, keys, extra);
(async () => {
  // ── 1. a full sheet and a partial one share order 1001: the partial one is pulled in; the other order is alone ──
  const g1 = gold(1, ['1001_1_1', '1002_1_1'], { releaseFull: true }), s1 = silver(1, ['1001_2_1', '1003_1_1']);
  reset([g1, s1]);
  assert.equal(Gate.policy(s1, 1).include, false, 'on its own a partial silver sheet is not released');
  await Gate.assemble(run);
  assert.deepEqual(inSet(), ['GF1', 'SS1'], 'both sheets of order 1001 are in the set');
  assert.equal(Gate.policy(s1, 1).include, true); assert(/Shares order 1001 with GF Sheet 1/.test(Gate.policy(s1, 1).reason), Gate.policy(s1, 1).reason);
  assert.equal(saved.get('silver-1').draft, false); assert(sets[0].sheetIds.includes('silver-1'));
  const again = saved.size; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS1'], 'assembled again: the same, nothing moves'); assert.equal(saved.size, again);
  // ── 2. the order comes off the silver sheet: nothing ties them, the silver sheet goes back out of the set ──
  s1.charms = s1.charms.filter(c => c.order !== '1001'); s1.placements = s1.placements.filter(p => s1.charms.some(c => c.id === p.id));
  await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1'], 'once the order is off, the partial sheet is not pulled any more'); assert.equal(s1.draft, true); assert(!s1.cardinalPull);
  // ── 3. both wait while the partner is still being nested: nobody starts a split ──
  const g2 = gold(1, ['2001_1_1'], { releaseFull: true }), s2 = silver(1, ['2001_2_1'], { status: 'nesting', persistedDone: false });
  reset([g2, s2]);
  await Gate.assemble(run); assert.deepEqual(inSet(), [], 'the full sheet waits for its mate'); assert.equal(sets.length, 0, 'no set was started for it');
  assert(/Waits for SS Sheet 1 \(it is still being nested or saved\).*order 2001/.test(Gate.policy(g2, 1).reason), Gate.policy(g2, 1).reason); assert.equal(Gate.policy(g2, 1).include, false);
  s2.status = 'complete'; s2.persistedDone = true; await Gate.assemble(run);
  assert.deepEqual(inSet(), ['GF1', 'SS1'], 'the mate is saved: both come in together'); assert(!g2.cardinalHold);
  // ── 4. the set is started: a new partner that cannot join yet is told about, the sheets already in stay ──
  const g3 = gold(2, ['2001_1_2', '2005_1_1'], { status: 'nesting', persistedDone: false });
  pages.push(g3); await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS1'], 'nothing is thrown out of the set for it');
  assert(/also on GF Sheet 2, which cannot join the set yet: it is still being nested or saved/.test(g2.cardinalNote), g2.cardinalNote);
  const split = Gate.cardinalSplit(sets[0]); assert.deepEqual(split.map(i => i.orderId), ['2001']); assert.deepEqual(split[0].there, ['GF Sheet 2'], 'the set cannot be released while it is outside');
  g3.status = 'complete'; g3.persistedDone = true; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'GF2', 'SS1'], 'saved: it joins'); assert.deepEqual(Gate.cardinalSplit(sets[0]), []);
  // ── 5. Rose Gold is never pulled in by itself ──
  const g5 = gold(1, ['5001_1_1'], { releaseFull: true }), r5 = sheet('rose', 1, ['5001_2_1']);
  reset([g5, r5]); run.solidIncluded.rose = false; await Gate.assemble(run);
  assert.deepEqual(inSet(), [], 'the gold sheet waits: its Rose Gold mate joins only by its own Cut Sheet press'); assert(/Waits for RG Sheet 1 \(a Rose Gold sheet joins a set only by its own Cut Sheet press\)/.test(Gate.policy(g5, 1).reason), Gate.policy(g5, 1).reason);
  run.solidIncluded.rose = true; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'RG1'], 'selected (as the press does): both come in'); run.solidIncluded.rose = false;
  // ── 6. a mate in a committed set: named with the exact reason, nothing is held back for it ──
  const g6 = gold(1, ['6001_1_1'], { releaseFull: true }), c6 = gold(2, ['6001_1_2'], { setId: 'set-c', draft: false, seq: 2, releaseFull: true, sheetId: 'gold-c' });
  reset([g6, c6]); sets.push({ setId: 'set-c', runId: 'r', seq: 2, group: 'dispatch', committedAt: 1, sheetIds: ['gold-c'], labelFiles: [], orders: {} });
  await Gate.assemble(run); assert(/order 6001 also on GF Sheet 2: Set 2 was committed to the Design Station/.test(g6.cardinalNote), g6.cardinalNote);
  assert(!g6.cardinalHold, 'the work goes on'); assert(!g6.draft && g6.setId === 'set', 'it joins the open set');
  // ── 7. 10K / 14K: ticking one sheet takes its order mates with it; one that cannot follow stops it, with the reason ──
  const a = sheet('gold14k', 1, ['7001_1_1', '7002_1_1']), b = sheet('gold14k', 2, ['7001_1_2', '7003_1_1']), c = sheet('gold14k', 3, ['7003_1_2']);
  reset([a, b, c]);
  let rule = Gate.cardinalFor(a, true); assert.deepEqual(rule.list.map(p => p.page).sort(), [1, 2, 3], 'the chain 1 – 2 – 3 changes together'); assert.equal(rule.blocked, ''); assert.deepEqual(rule.orders, ['7001', '7003']);
  await Gate.changeMembership('gold14k', true, rule.list); assert.deepEqual(inSet(), ['14K1', '14K2', '14K3']); assert.equal(saved.get('gold14k-1').solidIncluded, true);
  rule = Gate.cardinalFor(c, false); assert.deepEqual(rule.list.map(p => p.page).sort(), [1, 2, 3], 'taking one out takes the chain out'); await Gate.changeMembership('gold14k', false, rule.list); assert.deepEqual(inSet(), []);
  // a full gold sheet that shares an order is in the set on its own: the 14K sheet cannot be taken out from under it
  const gf = gold(1, ['7001_2_1'], { releaseFull: true }); pages.push(gf); await Gate.changeMembership('gold14k', true, [a, b, c]); assert.deepEqual(inSet(), ['14K1', '14K2', '14K3', 'GF1']);
  rule = Gate.cardinalFor(a, false); assert.equal(rule.blocked, '14K Sheet 1 stays with GF Sheet 1: they share an order.', rule.blocked); assert(!/reason|stay in the same set|take the order off/.test(rule.blocked), 'names only: no reasons, no instruction');
  // a pulled solid sheet is in the set without its own tick: the record says null (never "ticked"), the library row says it is in
  reset([gold(1, ['8001_1_1'], { releaseFull: true }), sheet('gold14k', 1, ['8001_1_2'])]);
  await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', 'GF1']); assert.strictEqual(saved.get('gold14k-1').solidIncluded, null, 'in by the rule, not by its own tick');
  assert.equal(Gate.solidSelected('gold14k', pages[1]), true); assert.equal(typeof pages[1].solidPick, 'undefined');
  const rows = Gate.projectLibraryRecords([{ id: 'gold14k-1', runId: 'r', metal: 'gold14k', draft: true }]); assert.equal(rows[0].solidIncluded, true); assert.equal(rows[0].draft, false);
  pages[0].charms = []; pages[0].placements = []; pages[0].releaseFull = false; await Gate.assemble(run); assert.deepEqual(inSet(), [], 'the order and the full sheet gone: the solid sheet goes back out');
  console.log('PASS: cardinal rule in the set assembly: partner pulled in, both wait while one nests, a started set takes what can come and says what cannot, Rose Gold never pulled, committed mate named, order off lets it go, 10K/14K chain moves together or says why not');
})().catch(e => { console.error(e); process.exit(1); });
