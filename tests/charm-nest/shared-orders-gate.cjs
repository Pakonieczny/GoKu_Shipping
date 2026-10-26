// The cardinal rule inside the set assembly (Gate.assemble, charm-nest-bridge.js; Paul, 5 Oct: every sheet that shares a multi-piece
// order with another is in the SAME set). The real Gate over fake pages, no cloud and no browser (as solid-per-sheet.cjs):
//   a partner that cannot release by itself is pulled into the set with its mate · a partner still nesting (or Rose Gold before its Cut Sheet press) no longer holds the
//   other sheets out (Paul, 12 Oct): the set forms, every sheet says which partner to add · a set already started takes what can come and says what cannot · Rose Gold is never pulled by itself · a partner in a committed set is
//   named, nothing is held for it · removing the order lets the pulled sheet go again · untick of Include moves the group together
//   or says why not · a pulled solid sheet is written without its own tick and never reloads as ticked.
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), Ops = require('../../charm-nest-operations.js'), O = require('../../charm-nest-orders.js');
const SO = require('../../charm-nest-shared-orders.js'), SR = require('../../charm-nest-set-rules.js');   // (SETFIX: the page's assembly needs the set rules; without them no set is formed, loudly)
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8'), start = source.indexOf('const Gate ='), end = source.indexOf('/* ═══ 21', start);
const ops = Ops.create(), sets = [], saved = new Map();
const run = { runId: 'r', releasePolicy: 2, status: 'review', step: 'engrave', solidIncluded: {}, errors: [] };
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
let pages = [];
const sheet = (metal, n, keys, extra = {}) => ({ metal, page: n, runId: 'r', sheetId: `${metal}-${n}`, draft: true, status: 'complete', outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true },
  charms: keys.map((k, i) => ({ id: `c${metal}${n}_${i}`, poolId: k, order: k.split('_')[0] })), placements: keys.map((k, i) => ({ id: `c${metal}${n}_${i}` })), ...extra });
const ctx = { window: { CharmNestOrders: O, CharmNestOperations: ops, SharedOrders: SO, CharmNestSetRules: SR }, B: { run, sets: new Map() }, S: { mode: 'nest', settings: {}, cloud: { ok: true }, library: { rows: [] } }, O, allSheets: () => pages, pagesOf: m => pages.filter(p => p.metal === m), refreshAllCards: () => {}, Session: { schedule() {} }, document: { getElementById: () => null }, toast: () => {}, Pool: { update: async () => {} }, Orders: { rows: () => [] }, Engrave: { items: () => new Map(), saveSheetBacks: async () => {} }, RunCtl: { save: async () => {}, poke() {}, onSheetDone() {}, membershipUpdated() {} },
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
// (SETFIX, Paul 17 Oct 2026) This file used to run the Gate WITHOUT the set rules, where the assembly's fallback put every sheet that wanted in into one set: a partial silver sheet was "pulled in" with its
//  full gold mate, an uncut Rose Gold sheet came in with the metal's tick, an uncut 14K sheet was pulled. Those are exactly the sets the owner refused (a partial sheet never joins a set, 13 Oct; a Rose Gold, 10K or
//  14K sheet joins only with its green line cut and its back engraving approved, or by its own press, 12 Oct). The Gate now needs the set rules, so every scenario below is the owner's rule:
//  a SET needs a completed GF and a completed SS sheet (`base()`: a completed silver sheet no order ties to anything), a partial sheet waits with the sheets that share an order with it, and a
//  Rose Gold / 14K sheet is pulled by an order only when its green line is cut.
const base = () => silver(9, ['9999_1_1'], { releaseFull: true });
(async () => {
  // ── 1. a 14K sheet whose green line is cut shares order 1001 with a full gold sheet: it is pulled into that set with its mate; the other order is alone ──
  const g1 = gold(1, ['1001_1_1', '1002_1_1'], { releaseFull: true }), k1 = sheet('gold14k', 1, ['1001_2_1', '1003_1_1'], { roseCutAt: 1760000000000 });
  reset([g1, k1, base()]);
  assert.equal(Gate.policy(k1, 1).include, false, 'on its own a 14K sheet nobody ticked is not released');
  await Gate.assemble(run);
  assert.deepEqual(inSet(), ['14K1', 'GF1', 'SS9'], 'both sheets of order 1001 are in the set');
  assert.equal(Gate.policy(k1, 1).include, true); assert.equal(Gate.policy(k1, 1).reason, 'Cut · stays in its set', 'pulled in by the order, then a recorded cut keeps it in the set it is in');
  assert.equal(saved.get('gold14k-1').draft, false); assert(sets[0].sheetIds.includes('gold14k-1'));
  const again = saved.size; await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', 'GF1', 'SS9'], 'assembled again: the same, nothing moves'); assert.equal(saved.size, again);
  // ── 2. the order comes off the 14K sheet: nothing ties them any more, but a recorded cut keeps a sheet in the set it is in (the physical rule; SetEdit sheetCut): it stays ──
  k1.charms = k1.charms.filter(c => c.order !== '1001'); k1.placements = k1.placements.filter(p => k1.charms.some(c => c.id === p.id));
  await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', 'GF1', 'SS9'], 'cut: it stays in the set it is in'); assert.equal(k1.draft, false);
  // before the cut it would go: a 14K sheet nobody ticked and nothing ties to the set is not in it (the pulled sheet "goes again" when its order is taken off)
  k1.roseCutAt = null; k1.draft = true; k1.setId = null; k1.seq = null; sets[0].sheetIds = sets[0].sheetIds.filter(i => i !== 'gold14k-1');
  await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'uncut and tied to nothing: not in the set'); assert.equal(k1.draft, true); assert(!k1.cardinalPull);
  // a 14K sheet that is NOT cut is never pulled by an order (R12: its green line is cut, or the person says yes): it is the partner to add, the set forms without it
  const g1b = gold(1, ['1101_1_1'], { releaseFull: true }), k1b = sheet('gold14k', 1, ['1101_2_1']);
  reset([g1b, k1b, base()]); await Gate.assemble(run);
  assert.deepEqual(inSet(), ['GF1', 'SS9'], 'the uncut 14K sheet is not pulled in'); assert(!k1b.cardinalPull); assert.equal(k1b.draft, true);
  assert(/^Order 1101 shared with 14K Sheet 1: add that sheet to Set 1 \(a 14K sheet joins a set only by its own Include or Cut Sheet press\)$/.test(g1b.cardinalNote), g1b.cardinalNote);
  // ── 3. the partner is still being nested (Paul, 12 Oct: it no longer holds the full sheet out): the set forms with the sheets that can join, the card names the partner to add, the partner joins that set once it is saved ──
  const g2 = gold(1, ['2001_1_1'], { releaseFull: true }), s2 = silver(1, ['2001_2_1'], { releaseFull: true, status: 'nesting', persistedDone: false });
  reset([g2, s2, base()]);
  await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'the full sheet is not held for its mate'); assert.equal(sets.length, 1, 'its set formed');
  assert.equal(Gate.policy(g2, 1).include, true); assert(!g2.cardinalHold, 'no hold');
  assert(/^Order 2001 shared with SS Sheet 1: add that sheet to Set 1 \(it is still being nested or saved\)$/.test(g2.cardinalNote), g2.cardinalNote);
  assert.equal(Gate.cardinalSplit(sets[0])[0].orderId, '2001', 'the set cannot go to laser cutting while the mate is outside it');
  s2.status = 'complete'; s2.persistedDone = true; await Gate.assemble(run);
  assert.deepEqual(inSet(), ['GF1', 'SS1', 'SS9'], 'the mate is saved: it joins the set its mate is in'); assert(!g2.cardinalNote && !g2.cardinalHold); assert.deepEqual(Gate.cardinalSplit(sets[0]), []);
  // ── 4. the set is started: a new partner that cannot join yet is told about, the sheets already in stay ──
  const g3 = gold(2, ['2001_1_2', '2005_1_1'], { releaseFull: true, status: 'nesting', persistedDone: false });
  pages.push(g3); await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS1', 'SS9'], 'nothing is thrown out of the set for it');
  assert(/^Order 2001 shared with GF Sheet 2: add that sheet to Set 1 \(it is still being nested or saved\)$/.test(g2.cardinalNote), g2.cardinalNote);
  const split = Gate.cardinalSplit(sets[0]); assert.deepEqual(split.map(i => i.orderId), ['2001']); assert.deepEqual(split[0].there, ['GF Draft 2'], 'the set cannot be released while it is outside');
  g3.status = 'complete'; g3.persistedDone = true; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'GF2', 'SS1', 'SS9'], 'saved: it joins'); assert.deepEqual(Gate.cardinalSplit(sets[0]), []);
  // ── 5. Rose Gold is never pulled in by itself while its green line is not cut, and the metal is never included as a whole (SETFIX) ──
  const g5 = gold(1, ['5001_1_1'], { releaseFull: true }), r5 = sheet('rose', 1, ['5001_2_1']), r6 = sheet('rose', 2, ['5002_1_1']);
  reset([g5, r5, r6, base()]); run.solidIncluded.rose = false; await Gate.assemble(run);
  assert.deepEqual(inSet(), ['GF1', 'SS9'], 'the gold sheet is not held for its Rose Gold mate (Paul, 12 Oct): the set forms, the mate joins only by its own Cut Sheet press'); assert(!g5.cardinalHold);
  assert(/^Order 5001 shared with RG Sheet 1: add that sheet to Set 1 \(a Rose Gold sheet joins a set only by its own Cut Sheet press\)$/.test(g5.cardinalNote), g5.cardinalNote); assert(!r5.cardinalPull, 'Rose Gold is never pulled by itself');
  // the run-wide Rose Gold tick (one press for every Rose Gold sheet of the run, and the former even-numbered rule) is not read any more: it brought every uncut sheet into a set that had formed
  run.solidIncluded.rose = true; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'the metal-level tick brings nobody in'); assert.equal(r5.draft, true); assert.equal(r6.draft, true); run.solidIncluded.rose = false;
  // the person's own yes on THIS sheet (the Include switch, the Cut Sheet press): this sheet comes in, and only this sheet
  await Gate.changeMembership('rose', true, r5); assert.deepEqual(inSet(), ['GF1', 'RG1', 'SS9'], 'its own yes: this Rose Gold sheet is in'); assert.equal(r6.draft, true, 'the other Rose Gold sheet of the run is not brought in with it');
  assert.equal(saved.get('rose-1').solidIncluded, true, 'its own tick is written (and only that: never false on a Rose Gold record)');
  await Gate.changeMembership('rose', false, r5); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'and out again by its own switch');
  // its green line is cut: the order brings it into the set of the sheet it shares the order with (R12 (a)), nothing else does
  r5.roseCutAt = 1760000000000; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'RG1', 'SS9'], 'cut: it joins the set of its order mate'); assert.equal(r6.draft, true, 'an unrelated Rose Gold sheet stays out'); assert(!g5.cardinalNote);
  assert.strictEqual(saved.get('rose-1').solidIncluded, null, 'in by the rule, not by its own tick');
  // its back engraving is not approved (the readiness report says so): it stays outside and is named, even with its line cut
  r5.draft = true; r5.setId = null; r5.seq = null; sets[0].sheetIds = sets[0].sheetIds.filter(i => i !== 'rose-1');
  ctx.window.CharmNestReadiness = { decisions: () => ({}), sheet: () => ({ stages: { approval: false }, faces: null }), orderReports: () => ({}) };
  ctx.window.SetEdit = { engravingGap: rec => (rec.laser && rec.laser.stages.approval === false ? { key: 'backsNotApproved', label: 'RG Sheet 1: back engraving not approved yet (0 of 2 approved)' } : null) };
  await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'cut, but its back engraving is not approved: it is not pulled');
  assert(/^Order 5001 shared with RG Sheet 1: add that sheet to Set 1 \(its back engraving not approved yet \(0 of 2 approved\)\)$/.test(g5.cardinalNote), g5.cardinalNote);
  delete ctx.window.CharmNestReadiness; delete ctx.window.SetEdit;
  // ── 6. a mate in a committed set: named with the exact reason, nothing is held back for it ──
  const g6 = gold(1, ['6001_1_1'], { releaseFull: true }), c6 = gold(2, ['6001_1_2'], { setId: 'set-c', draft: false, seq: 2, releaseFull: true, sheetId: 'gold-c' });
  reset([g6, c6, base()]); sets.push({ setId: 'set-c', runId: 'r', seq: 2, group: 'dispatch', committedAt: 1, sheetIds: ['gold-c'], labelFiles: [], orders: {} });
  await Gate.assemble(run); assert(/order 6001 also on GF Sheet 2: Set 2 was committed to the Design Station/.test(g6.cardinalNote), g6.cardinalNote);
  assert(!g6.cardinalHold, 'the work goes on'); assert(!g6.draft && g6.setId === 'set', 'it joins the open set');
  // ── 7. 10K / 14K: ticking one sheet takes its order mates with it; one that cannot follow stops it, with the reason (the set has formed: a 10K or 14K sheet never starts one) ──
  const a = sheet('gold14k', 1, ['7001_1_1', '7002_1_1']), b = sheet('gold14k', 2, ['7001_1_2', '7003_1_1']), c = sheet('gold14k', 3, ['7003_1_2']);
  reset([a, b, c, gold(8, ['7999_1_1'], { releaseFull: true }), base()]); await Gate.assemble(run); assert.deepEqual(inSet(), ['GF8', 'SS9'], 'the minimum formed the set; the 14K sheets are not in it');
  let rule = Gate.cardinalFor(a, true); assert.deepEqual(rule.list.map(p => p.page).sort(), [1, 2, 3], 'the chain 1 – 2 – 3 changes together'); assert.equal(rule.blocked, ''); assert.deepEqual(rule.orders, ['7001', '7003']);
  await Gate.changeMembership('gold14k', true, rule.list); assert.deepEqual(inSet(), ['14K1', '14K2', '14K3', 'GF8', 'SS9']); assert.equal(saved.get('gold14k-1').solidIncluded, true);
  rule = Gate.cardinalFor(c, false); assert.deepEqual(rule.list.map(p => p.page).sort(), [1, 2, 3], 'taking one out takes the chain out'); await Gate.changeMembership('gold14k', false, rule.list); assert.deepEqual(inSet(), ['GF8', 'SS9']);
  // a full gold sheet that shares an order is in the set on its own: the 14K sheet cannot be taken out from under it
  // (Paul, 26 Oct 2026: a 10K / 14K / Rose Gold sheet never ties a GF / SS sheet to a set: "not a reason to pull that other unit in". The full GF sheet that shares an order only with the 14K sheet
  //  waits for its own SS sheet; the order is a known split, said plainly)
  await Gate.changeMembership('gold14k', true, [a, b, c]); const gf = gold(1, ['7001_2_1'], { releaseFull: true }); pages.push(gf); await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', '14K2', '14K3', 'GF8', 'SS9'], 'the gold sheet arrives after the chain is in: the 14K sheet does not pull it into the formed set');
  assert(/ships in two parts|ship in two parts/.test(gf.cardinalNote || '') || /needs a completed SS sheet/.test(gf.setWait || ''), 'it says why: ' + gf.cardinalNote + ' / ' + gf.setWait);
  rule = Gate.cardinalFor(a, false); assert.equal(rule.blocked, '14K Sheet 1 stays with GF Sheet 1: they share an order.', rule.blocked); assert(!/reason|stay in the same set|take the order off/.test(rule.blocked), 'names only: no reasons, no instruction');
  // a pulled solid sheet is in the set without its own tick: the record says null (never "ticked"), the library row says it is in
  reset([gold(1, ['8001_1_1'], { releaseFull: true }), sheet('gold14k', 1, ['8001_1_2'], { roseCutAt: 1760000000000 }), base()]);
  await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', 'GF1', 'SS9']); assert.strictEqual(saved.get('gold14k-1').solidIncluded, null, 'in by the rule, not by its own tick');
  assert.equal(Gate.solidSelected('gold14k', pages[1]), true); assert.equal(typeof pages[1].solidPick, 'undefined');
  const rows = Gate.projectLibraryRecords([{ id: 'gold14k-1', runId: 'r', metal: 'gold14k', draft: true }]); assert.equal(rows[0].solidIncluded, true); assert.equal(rows[0].draft, false);
  pages[0].charms = []; pages[0].placements = []; pages[0].releaseFull = false; await Gate.assemble(run); assert.deepEqual(inSet(), ['14K1', 'SS9'], 'the order and the full sheet gone: the 14K sheet was cut, and a recorded cut keeps a sheet in the set it is in');
  // ── 9. a pair (a Left and a Right piece, matching or not) on two sheets: the set keeps both sheets together, and the notes say what the pieces are (Paul, 9 Oct) ──
  const sideOn = (page, idx, side) => { page.charms[idx].side = side; return page; };
  // the sheet with the right earring is still filling: a partial sheet joins no set (13 Oct), so the full sheet that holds the left earring waits for it, and the line says it is the pair
  const pl = sideOn(gold(1, ['9001_1_1', '9002_1_1'], { releaseFull: true }), 0, 'L'), pr = sideOn(gold(2, ['9001_1_2']), 0, 'R');
  reset([pl, pr, base()]);
  assert.equal(Gate.policy(pr, 1).include, false, 'on its own the partial gold sheet is not released');
  await Gate.assemble(run); assert.deepEqual(inSet(), [], 'the full sheet waits for the sheet with the other earring');
  assert(/^Waits: order 9001 \(its left and right earrings\) shared with GF Sheet 2, which is not full yet, so they stay out of a set together$/.test(pl.cardinalHold), pl.cardinalHold);
  pr.releaseFull = true; await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'GF2', 'SS9'], 'the sheet with the right earring is full: both sheets come in together');
  // a 14K sheet (its line cut) with the right earring is pulled in with the one that holds the left: the reason says it is a pair
  // (Paul, 26 Oct 2026: a green-line sheet goes into its ONE set, the set of the GF / SS sheet it shares the order with, once that set exists)
  const ql = sideOn(gold(1, ['9201_1_1', '9202_1_1'], { releaseFull: true }), 0, 'L'), qr = sideOn(sheet('gold14k', 2, ['9201_1_2']), 0, 'R');
  reset([ql, qr, base()]); await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'uncut: it waits outside the set of its order mate');
  qr.roseCutAt = 1760000000000; Gate.cardinalApply(run, pages, Gate.openSets(run.runId));
  assert(/Shares order 9201 \(its left and right earrings\) with GF Sheet 1/.test(Gate.policy(qr, 1).reason), Gate.policy(qr, 1).reason);
  await Gate.assemble(run); assert.deepEqual(inSet(), ['14K2', 'GF1', 'SS9']);
  // the right earring's sheet is still being nested: the full sheet is not held out for it, the set forms and names it
  const wl = sideOn(gold(1, ['9101_1_1'], { releaseFull: true }), 0, 'L'), wr = sideOn(gold(2, ['9101_1_2'], { releaseFull: true, status: 'nesting', persistedDone: false }), 0, 'R');
  reset([wl, wr, base()]); await Gate.assemble(run); assert.deepEqual(inSet(), ['GF1', 'SS9'], 'the full sheet is not held out for the sheet with the other earring: the set forms and names it');
  assert(/^Order 9101 \(its left and right earrings\) shared with GF Sheet 2: add that sheet to Set 1 \(it is still being nested or saved\)$/.test(wl.cardinalNote), wl.cardinalNote);
  console.log('PASS: cardinal rule in the set assembly (with the set rules): a cut 14K partner is pulled in with its mate, an uncut one and an uncut Rose Gold sheet are named as the partner to add, a Rose Gold sheet comes in only by its own tick (this sheet alone, never the metal) or once its line is cut and its backs are approved, a partner still nesting holds nobody out (the set forms and names it), a started set takes what can come and says what cannot, a partial sheet keeps its group out of every set, committed mate named, order off lets it go, 10K/14K chain moves together or says why not');
})().catch(e => { console.error(e); process.exit(1); });
