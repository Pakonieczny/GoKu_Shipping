/* Regression (round 7, Paul, 5 Oct 2026, and round 8, 5 Oct 2026): "I'm not sure why this specific order is holding up the gold fill sheet that doesn't make any sense. It looks
 * perfectly fine to me and it's on both sheets and both sheets are in the same set." / "you cannot have a green approved button on a single sheet that is part
 * of a set where the other sheets are not ready yet". Round 8: "Remove any references to the other sheet from this list. The list originated in the GF sheet but is also showing the
 * SS missing engraving. Please do not do that, only related items to that particular sheet."
 *
 * Order 4170252963 (Nathaly Soto) has one piece on GF Sheet 1 and one on SS Sheet 1, both in Set 1. SS Sheet 1 is not ready (back engravings 7 of 25). The
 * order is FINE; it was listed in GF Sheet 1's "!" Order check panel as "Waits on SS Sheet 1" (round 7 took that out). A set advances as ONE, so a mate sheet of the SAME set that
 * is not ready is the SET's wait. Round 7 said it once, quietly, at the top of GF Sheet 1's panel ("Waiting for SS Sheet 1 · Engraving", opened by a small clock on its rail);
 * round 8 takes that out of the sheet's list altogether: the set's wait is said ONCE, under the grey Approve button (its reason line "SS Sheet 1 · back engravings 7 of 25"
 * and its shortcut, which opens SS Sheet 1's own '!'), and nowhere in a sheet's list, header count or rail.
 *
 * What this holds to, with the real CharmNestReadiness (page and server twin), the real LaserReview rail and the real '!' panel, on a shop shaped like Paul's:
 *   1. both sheets in the same set, the mate not ready: NO order issue on either sheet, in every reading (the page's rows, the records, the server's own reading);
 *   2. GF Sheet 1's list holds NOTHING about SS Sheet 1: no 'waitsOnSheet', nothing quiet, no entry that names or opens the mate; SS Sheet 1's list is its own engraving only;
 *      the gate (setGate, the Approve button's own truth) says the wait once: "SS Sheet 1 · back engravings N of M";
 *   3. the mate ready again: the gate is ready and every list is empty;
 *   4. the same two pieces on sheets of DIFFERENT sets still report, worded as the split they are ("Split between Set 1 and Set 2"), with no 'waits' in the words;
 *   5. a real problem of another piece of the same order still lists the order, for that piece only (the mate sheet's piece is not named as waiting);
 *   6. on screen: GF Sheet 1's rail carries no '!' and no clock (it is fine); its grey Approve line names SS Sheet 1 once and is a link that opens SS Sheet 1's '!';
 *      Paul's image 2 (GF Sheet 1 with four real order issues next to a not-ready SS Sheet 1): its Order check panel says "4 issues", lists those four orders (one of them waits
 *      on RG Sheet 1, a sheet in no set: that order is on this sheet, so it stays) and says nothing of SS Sheet 1, no 'Waiting' row, no quiet header;
 *   7. the old rule put back, the set's wait put back in the list (readiness), the clock put back on the rail (bridge) and the panel's drop of such an entry taken out are each CAUGHT.
 *
 *   NODE_PATH=<dir with jsdom> node tests/charm-nest/same-set-no-wait-issue.cjs      (part 6 is skipped, with a note, when jsdom is not installed)
 * Offline: no network, nothing written. */
'use strict';
const fs = require('fs'), path = require('path'), assert = require('node:assert/strict');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const P = require('./issues-property.cjs');
const { paulSpec, paulShop } = require('./issues-paul.cjs');
const { withWaitsBack } = require('./issues-mutants.cjs');
const ROOT = path.join(__dirname, '../..');
const READINESS = path.join(ROOT, 'charm-nest-readiness.js');
const SRC = fs.readFileSync(READINESS, 'utf8');
const NEW = require(READINESS);
const RULE = '!ownSetWait(b,mySet)';
const BRIDGE = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'), ISSUES = fs.readFileSync(path.join(ROOT, 'charm-nest-library-issues.js'), 'utf8');

/** The real module with the OLD same-set rule put back: a not-ready mate sheet of the same set is an order's wait again. */
function oldRule() {
  assert(SRC.includes(RULE), 'the same-set rule is where the regression expects it (charm-nest-readiness.js forSheet)');
  const src = SRC.replace(RULE, 'true'), m = { exports: {} };
  new Function('module', 'exports', 'self', src)(m, m.exports, undefined);
  return { R: m.exports, src };
}

const GF = 'gf-sheet-1', SS = 'ss-sheet-1';
const wordsOf = e => [e.why, e.label, ...(e.pieces || []).flatMap(p => [p.why, p.line, p.text])].filter(Boolean).join(' | ');

/** What each sheet of Paul's shop says, three ways (the page's rows, the page's records, the server's own reading of each sheet), with its set. */
function read(R, shop) {
  const rows = S.recordRows(shop), all = P.pageSheets(shop, R), pre = P.serverLike(shop, R), set = shop.sets.find(x => x.sheetIds.includes(GF)), out = {};
  for (const mode of ['rows', 'pre']) {
    const members = (mode === 'rows' ? all : pre).filter(s => set.sheetIds.includes(s.id));
    for (const id of [GF, SS]) {
      const subject = members.find(s => s.id === id);
      out[mode + ':' + id] = R.issues(subject, mode === 'rows' ? { rows, allSheets: all, set, sheets: members } : { set, sheets: members });
    }
  }
  return out;
}

/** The set's wait, as the gate (the Approve button's own truth) says it, for each reading of the sheets: the first blocker's text ("SS Sheet 1 · back engravings 7 of 25"), or '' when the set is ready. */
function gateOf(R, shop) {
  const all = P.pageSheets(shop, R), pre = P.serverLike(shop, R), set = shop.sets.find(x => x.sheetIds.includes(GF)), out = {};
  for (const [mode, list] of [['rows', all], ['pre', pre]]) { const g = R.setGate(set, list.filter(s => set.sheetIds.includes(s.id))); out[mode] = { ready: g.ready, reason: g.reason, blockers: g.blockers.map(b => b.sheetId) }; }
  return out;
}

/** The checks of 1-3 on the shop where both sheets are in the SAME set; expectWait: SS Sheet 1 is not ready, so the GATE says the set waits for it. Returns what is wrong. */
function sameSetChecks(R, shop, expectWait) {
  const bad = [], got = read(R, shop);
  for (const [key, list] of Object.entries(got)) {
    const id = key.split(':')[1], orders = list.filter(i => i.step === 'orders');
    if (orders.length) bad.push(`${key}: ${orders.length} order issues listed (${orders.slice(0, 2).map(i => `${i.orderId} ${i.key}`).join(', ')}); the orders are on two sheets of ONE set`);
    if (list.some(i => i.key === 'otherSheetNotReady' || (i.pieces || []).some(p => p.kind === 'otherSheetNotReady'))) bad.push(`${key}: something says "waits on another sheet"`);
    if (list.some(i => /\bwaits? (on|for)\b/i.test(wordsOf(i)))) bad.push(`${key}: an entry's words say it waits`);
    // round 8: the set's wait is no entry of any sheet's list: nothing quiet, no 'waitsOnSheet', nothing that names or opens a mate
    if (list.some(i => i.quiet === true || i.key === 'waitsOnSheet')) bad.push(`${key}: the set's wait is an entry of this sheet's list: ${JSON.stringify(list.filter(i => i.quiet === true || i.key === 'waitsOnSheet').map(i => [i.key, i.label]))}`);
    const other = id === GF ? SS : GF, otherLabel = id === GF ? 'SS Sheet 1' : 'GF Sheet 1';
    if (list.some(i => (i.open && i.open.id === other) || i.label === otherLabel || /\b(SS|GF) Sheet 1\b/.test(wordsOf(i)))) bad.push(`${key}: an entry names or opens the other sheet of the set: ${JSON.stringify(list.filter(i => (i.open && i.open.id === other) || i.label === otherLabel).map(i => [i.key, i.label]))}`);
    if (id === GF && list.length) bad.push(`${key}: GF Sheet 1 is fine: its list is not empty: ${JSON.stringify(list.map(i => i.key))}`);
    if (id === SS) {
      if (expectWait && (list.length !== 1 || list[0].key !== 'approvalsNeeded' || list[0].step !== 'engraving')) bad.push(`${key}: SS Sheet 1's one real entry is its own engravings: ${JSON.stringify(list.map(i => i.key))}`);
      if (!expectWait && list.length) bad.push(`${key}: SS Sheet 1 is ready: its list is not empty`);
    }
  }
  // the wait is said once, by the gate: the sheet that holds the set, and what it lacks, in the words under the grey Approve button
  for (const [mode, g] of Object.entries(gateOf(R, shop))) {
    if (expectWait) { if (g.ready || g.blockers.join() !== SS || !/^SS Sheet 1 · back engravings \d+ of \d+$/.test(g.reason)) bad.push(`gate ${mode}: ${JSON.stringify(g)}`); }
    else if (!g.ready || g.reason) bad.push(`gate ${mode}: SS Sheet 1 is ready, the set is not: ${JSON.stringify(g)}`);
  }
  return bad;
}

/** Paul's image 2 (5 Oct, round 8), small: GF Sheet 1 and a not-ready SS Sheet 1 in Set 1, three orders with an unknown SKU (their other piece has no sheet), and Leslie Suhr's order whose
 *  other piece sits on RG Sheet 1, a sheet in NO set. GF Sheet 1's Order check lists exactly those four, and nothing about SS Sheet 1. */
function imageTwoSpec() {
  const spec = paulSpec({ gf: 6, ss: 4, shared: 2, unapproved: 3 }), base = { problems: [], sku: 'SKU', hold: null, change: false, noDesign: false, kind: 'paul' };
  spec.sheets.push({ id: 'rg-sheet-1', metal: 'rose', index: 1, own: 'draft', setId: null });
  let tx = 9100;
  for (let i = 0; i < 3; i++) spec.orders.push({ id: String(4170408845 + i * 1000), buyer: 'Unknown SKU ' + i, lines: [
    { ...base, tx: ++tx, metal: 'gold', q: 1, copies: [{ sheet: GF, pooled: true }], state: 'written', engrave: 'plain' },
    { ...base, tx: ++tx, metal: 'gold', q: 1, problems: ['unmatchedSku'], copies: [{ sheet: null, pooled: false }], state: 'unmatched', engrave: 'plain' }] });
  spec.orders.push({ id: '4170837249', buyer: 'Leslie Suhr', lines: [
    { ...base, tx: ++tx, metal: 'gold', q: 1, copies: [{ sheet: GF, pooled: true }], state: 'written', engrave: 'plain' },
    { ...base, tx: ++tx, metal: 'rose', q: 1, copies: [{ sheet: 'rg-sheet-1', pooled: true }], state: 'written', engrave: 'plain' }] });
  return spec;
}

const failures = [];
const ok = (cond, msg) => { if (!cond) failures.push(msg); };
const must = (list, what) => { if (list.length) { console.error(`FAIL: ${what}\n  ${list.slice(0, 6).join('\n  ')}`); process.exitCode = 1; return false; } return true; };

(async () => {
  // ── 0. the oracle says what Paul says: both sheets fine
  const shop = paulShop(), t = O.truth(shop);
  assert.equal(shop.sets.length, 1, 'one set holds both sheets'); assert.deepEqual(shop.sets[0].sheetIds.slice().sort(), [GF, SS]);
  assert.deepEqual(O.issueOrders(t, GF), [], 'the oracle: no order issue on GF Sheet 1'); assert.deepEqual(O.issueOrders(t, SS), [], 'nor on SS Sheet 1');
  assert.deepEqual(O.setWaits(shop, t, GF), [SS], 'the oracle: the set waits for SS Sheet 1'); assert.deepEqual(O.setWaits(shop, t, SS), []);
  assert(O.issueOrders(O.truth(shop, { sameSetIsIssue: true }), GF).length === 5, 'under the OLD rule the five orders spread over the two sheets were listed on GF Sheet 1 (the false alarm Paul saw)');

  // ── 1-3. the real code, same set
  must(sameSetChecks(NEW, shop, true), '1 + 2: both sheets in the same set, SS Sheet 1 not ready');
  const ready = S.materialize(paulSpec({ unapproved: 0 }));
  must(sameSetChecks(NEW, ready, false), '3: SS Sheet 1 ready again');
  // (the page's whole-order answer given to one sheet, as the bridge's release check and the order window read it)
  {
    const rows = S.recordRows(shop), all = P.pageSheets(shop), reps = NEW.orderReports(rows, all), gf = all.find(s => s.id === GF), ss = all.find(s => s.id === SS);
    assert(Object.values(reps).some(r => !r.ready), 'the whole-order reading does see the pieces on SS Sheet 1 as not ready');
    const asGf = { ...gf, orderReadiness: reps }, asSs = { ...ss, orderReadiness: reps };
    assert.deepEqual(NEW.orderBlockers(asGf), [], 'the release check reads no order wait for GF Sheet 1'); assert.deepEqual(NEW.orderBlockers(asSs), []);
    assert.equal(NEW.sheet(asGf).stages.orders, true, 'the sheet\'s Order check step is done'); assert.equal(NEW.sheet(asSs).stages.orders, true);
  }

  // ── 4. split across DIFFERENT sets: still a real issue, worded as a split
  {
    const spec = paulSpec(); spec.sheets[1].setId = 'set-2';
    const apart = S.materialize(spec), ta = O.truth(apart);
    assert.equal(apart.sets.length, 2);
    assert.equal(O.issueOrders(ta, GF).length, 5, 'the oracle: five orders are split between Set 1 and Set 2'); assert.deepEqual(O.issueOrders(ta, SS), []);
    assert.deepEqual(P.disagreements(apart), [], 'the real code agrees with the oracle on the split shop');
    for (const [mode, rows, all] of [['rows', S.recordRows(apart), P.pageSheets(apart)], ['pre', null, null]]) {
      const subject = mode === 'rows' ? all.find(s => s.id === GF) : P.serverLike(apart).find(s => s.id === GF);
      const list = NEW.issues(subject, mode === 'rows' ? { rows, allSheets: all } : {}), orders = list.filter(i => i.step === 'orders');
      assert.equal(orders.length, 5, `${mode}: the split orders are listed`); assert.equal(list.filter(i => i.quiet || i.key === 'waitsOnSheet').length, 0, `${mode}: no set wait of any kind`);
      for (const e of orders) {
        assert.equal(e.key, 'otherSheetNotReady'); assert.equal(e.split, true, 'flagged as a split'); assert.deepEqual(e.sets, ['Set 1', 'Set 2']);
        assert.match(e.why, /^Split between Set 1 and Set 2/); assert.doesNotMatch(wordsOf(e), /\bwaits?\b/i, 'the split is worded as where the pieces are, never as "waits"');
        assert.equal(e.pieces.length, 1); assert.equal(e.pieces[0].sheetLabel, 'SS Sheet 1'); assert.equal(e.pieces[0].split, true); assert.equal(e.pieces[0].setLabel, 'Set 2');
      }
    }
  }

  // ── 5. a real problem of another piece keeps the order listed, for that piece only
  {
    const spec = paulSpec(), base = { problems: [], sku: 'SKU', hold: null, change: false, noDesign: false, kind: 'paul' };
    spec.orders.push({ id: '4179999001', buyer: 'Mixed buyer', lines: [
      { ...base, tx: 9001, metal: 'gold', q: 1, copies: [{ sheet: GF, pooled: true }], state: 'written', engrave: 'approved' },
      { ...base, tx: 9002, metal: 'silver', q: 1, copies: [{ sheet: SS, pooled: true }], state: 'pooled', engrave: 'approved' },
      { ...base, tx: 9003, metal: 'gold', q: 1, copies: [{ pooled: true }], state: 'pooled', engrave: 'approved' }] });
    const mixed = S.materialize(spec), tm = O.truth(mixed);
    assert.deepEqual(O.issueOrders(tm, GF), ['4179999001'], 'the oracle: the order is listed for its piece that is on no sheet');
    assert.deepEqual(P.disagreements(mixed), [], 'the real code agrees with the oracle');
    const list = read(NEW, mixed)['rows:' + GF], orders = list.filter(i => i.step === 'orders');
    assert.deepEqual(orders.map(i => [i.orderId, i.key]), [['4179999001', 'pooled']]); assert.equal(orders[0].pieces.length, 1, 'only the piece that is on no sheet is listed');
    assert.equal(orders[0].pieces[0].sheetLabel, null); assert.doesNotMatch(wordsOf(orders[0]), /SS Sheet 1|\bwaits?\b/i, 'the piece on the set\'s other sheet is not named as waiting');
    assert.equal(list.filter(i => i.step !== 'orders').length, 0, 'and nothing about the set\'s wait (SS Sheet 1) is in the list: it is said once, by the gate');
  }

  // ── image 2 (data): GF Sheet 1's Order check lists its four real orders and nothing about SS Sheet 1 (the order that waits on RG Sheet 1 is on this sheet: it stays)
  const img2 = S.materialize(imageTwoSpec()), t2 = O.truth(img2);
  assert.deepEqual(P.disagreements(img2), [], 'the real code agrees with the oracle on the image 2 shop');
  assert.equal(O.issueOrders(t2, GF).length, 4, 'the oracle: four orders on GF Sheet 1'); assert.deepEqual(O.setWaits(img2, t2, GF), [SS], 'and the set still waits for SS Sheet 1 (the gate says so)');
  for (const [mode, rows, all] of [['rows', S.recordRows(img2), P.pageSheets(img2)], ['pre', null, null]]) {
    const set = img2.sets.find(x => x.sheetIds.includes(GF)), members = mode === 'rows' ? all.filter(x => set.sheetIds.includes(x.id)) : P.serverLike(img2).filter(x => set.sheetIds.includes(x.id));
    const subject = mode === 'rows' ? all.find(x => x.id === GF) : P.serverLike(img2).find(x => x.id === GF), list = NEW.issues(subject, mode === 'rows' ? { rows, allSheets: all, set, sheets: members } : { set, sheets: members });
    assert.deepEqual(list.map(i => [i.step, i.key]).sort(), [['orders', 'otherSheetNotReady'], ['orders', 'unmatched'], ['orders', 'unmatched'], ['orders', 'unmatched']], `${mode}: exactly the four orders of this sheet, nothing else`);
    assert.doesNotMatch(JSON.stringify(list), /SS Sheet 1|ss-sheet-1|waitsOnSheet|"quiet"/, `${mode}: nothing in GF Sheet 1's list names or opens SS Sheet 1`);
    const leslie = list.find(i => i.orderId === '4170837249'); assert(leslie && leslie.pieces.length === 1 && leslie.pieces[0].sheetLabel === 'RG Sheet 1', `${mode}: the order with a piece on RG Sheet 1 stays listed, naming RG Sheet 1`);
  }

  // ── 7a. the old rule put back is caught by 1 + 2 (and the server twin reads each sheet with its own set)
  const old = oldRule(), caught = sameSetChecks(old.R, paulShop(), true);
  assert(caught.length >= 4 && caught.some(c => /order issues listed/.test(c)) && caught.some(c => /waits on another sheet/.test(c)), `the regression catches the old rule: ${JSON.stringify(caught.slice(0, 3))}`);
  assert(P.mateChecks(old.R, paulShop()).some(d => d.type === 'sameSetWaitListed'), 'the property harness catches it too');
  // the set's wait put back in the list (round 7's quiet entries, from the shipped module with its source patched): caught by the data checks and by the property harness
  const waits = withWaitsBack(SRC), caughtWait = sameSetChecks(waits.R, paulShop(), true);
  assert(caughtWait.some(c => /the set's wait is an entry of this sheet's list/.test(c)) && caughtWait.some(c => /names or opens the other sheet/.test(c)), `the regression catches the set's wait put back in the list: ${JSON.stringify(caughtWait.slice(0, 3))}`);
  assert(P.mateChecks(waits.R, paulShop()).some(d => d.type === 'setWaitListed'), 'the property harness catches the wait put back too');
  assert(sameSetChecks(waits.R, S.materialize(paulSpec({ unapproved: 0 })), false).length === 0, '(and the mutant says nothing once SS Sheet 1 is ready: it is the wait that is caught, not the shop)');
  const server = fs.readFileSync(path.join(ROOT, 'netlify/functions/charmNestLibrary.js'), 'utf8');
  assert(server.includes('Readiness.forSheet(orders[id],s.id || s.sheetId,Readiness.setOf(s))'), 'the server twin (productionReadiness) reads each sheet\'s orders with that sheet\'s own set');
  assert(SRC.includes('forSheet(s.orderReadiness?.[id],sid,setOf(s))') && SRC.includes('forSheet(reps[id],sid,mine)') && SRC.includes('forSheet(s.orderReadiness[id],sid,setOf(s))'), 'the page reads every sheet with its own set in all three places (stage, readFrom, orderBlockers)');
  // nothing of the quiet wait is left in the shipped sources: no entry, no row, no clock, no mark
  assert(!/waitsOnSheet/.test(SRC.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')), 'readiness makes no waitsOnSheet entry');
  assert(!/data-issues-quiet|flowWait|CLOCK|waitOn\b/.test(BRIDGE) && !/lisWait|waitRowHtml|m\.waits|data-issues-quiet|lisCount\.quiet/.test(ISSUES), 'no quiet clock, wait row or wait header is left in the bridge or the panel');

  // ── 6. on screen (needs jsdom)
  let JSDOM = null; try { ({ JSDOM } = require('jsdom')); } catch (_) { /* skipped below */ }
  if (!JSDOM) console.log('  - jsdom is not installed (set NODE_PATH): the on-screen part 6 was not run');
  else {
    const sleep = n => new Promise(r => setTimeout(r, n)), until = async (f, ms = 3000) => { const t0 = Date.now(); for (;;) { const v = f(); if (v) return v; if (Date.now() - t0 > ms) return null; await sleep(10); } };
    /** One page of the Library with the REAL LaserReview, set card, '!' panel and Approve button. src: the sources to run (the shipped ones unless a mutant is passed). */
    const ui = async (spec, R, label, src = {}) => {
      const readinessSrc = src.readiness || SRC, bridge = src.bridge || BRIDGE, issuesSrc = src.issues || ISSUES, image2 = spec === 'image2', theShop = image2 ? img2 : shop;
      const bad = [], dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true }), w = dom.window, d = w.document, calls = [], net = [];
      try {
        w.matchMedia = () => ({ matches: true }); w.Element.prototype.getClientRects = function () { return [{}]; }; w.fetch = (...a) => { net.push(a[0]); return Promise.reject(new Error('offline')); };
        Object.assign(w, { S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: false } }, api: async () => { net.push('api'); return { sheets: [], sets: [] }; }, allSheets: () => [], Orders: { rows: () => w.__rows || [] },
          Engrave: { items: () => new Map(), backsMarkup: () => '' }, esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
          Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, pvRatio: () => '',
          sheetHead: r => `<div class="h"><span class="nm">${r.metal} Sheet ${r.sheetIndex}</span></div>`, openLibrarySheet: id => { calls.push(['sheet', id]); return true; }, openOrderFrom: (b, rid) => { calls.push(['order', rid]); return true; }, setMode: m => calls.push(['mode', m]),
          ListMedia: { peek: () => null, listing: () => Promise.resolve(null) }, LibraryFlow: { approve: async () => { calls.push(['approve']); return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; } } });
        w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders; w.eval(readinessSrc);
        for (const f of ['charm-nest-activity.js', 'charm-nest-motion.js']) w.eval(fs.readFileSync(path.join(ROOT, f), 'utf8'));
        w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
        const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
        w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {libraryCard,libraryGroups};})();'); w.eval(issuesSrc);
        const L = w.LaserReview, LI = w.LibraryIssues, body = d.getElementById('libBody'), recs = JSON.parse(JSON.stringify(P.serverLike(theShop, R)));
        w.__rows = JSON.parse(JSON.stringify(S.uiRows(theShop)));
        const sets = theShop.sets.map(x => ({ ...x, status: 'saved', name: 'Set ' + x.seq, day: '2026-10-05', seq: x.seq }));
        L.sections(body); for (const r of recs) L.record(r);
        for (const g of w.Sets.libraryGroups(sets, recs)) { const card = w.Sets.libraryCard(g, g.sheets, g.sheets); L.place(card, L.group(g, g.sheets).ready, body); }
        L.changed(); await sleep(60);
        const marks = id => [...body.querySelectorAll(`button[data-issues-open][data-issues-id="${id}"]`)], gf = marks(GF), ss = marks(SS), closePanel = async () => { d.querySelectorAll('.lisPanel').forEach(n => n.classList.remove('handed')); LI.close(); await sleep(10); d.querySelectorAll('#libIssuesPanel').forEach(n => n.remove()); };
        const approve = id => body.querySelector(`.approveBox[data-approve-for="sheet:${id}"]`), say = id => { const bx = approve(id); return bx ? bx.querySelector('[data-approve-why]').textContent.trim() : null; };
        // nothing of the quiet wait anywhere on the page: no clock, no wait mark, no wait row, no 'Waiting' header
        if (d.querySelector('[data-issues-quiet],.flowWait,.lisWait,.lisCount.quiet')) bad.push(`${label}: a quiet clock, wait mark or wait row is on the page`);
        // the sheet that holds the set: its own real '!' (Engraving), its panel has no wait
        const sreal = ss.filter(b => !b.hasAttribute('data-issues-quiet')), sq = ss.length - sreal.length;
        if (sq) bad.push(`${label}: SS Sheet 1 has a quiet clock`);
        if (sreal.length !== 1 || sreal[0].getAttribute('data-issues-step') !== 'engraving') bad.push(`${label}: SS Sheet 1's marks: ${sreal.map(b => b.getAttribute('data-issues-step'))}`);
        else { sreal[0].click(); const panel = await until(() => d.getElementById('libIssuesPanel')); if (!panel) bad.push(`${label}: SS Sheet 1's '!' opens no panel`); else { if (panel.querySelector('.lisWait')) bad.push(`${label}: SS Sheet 1's panel shows a wait`); if (panel.querySelectorAll('.lisRow').length) bad.push(`${label}: SS Sheet 1's engraving panel lists orders`); await closePanel(); } }
        // the set's wait is said ONCE, under the grey Approve button of every sheet that is not cut: the sheet that holds the set, and what it lacks; a press on it opens SS Sheet 1's '!'
        for (const id of [GF, SS]) {
          const box = approve(id), btn = box && box.querySelector('[data-approve-btn]');
          if (!box) { bad.push(`${label}: no Approve button under ${id}`); continue; }
          if (btn.getAttribute('aria-disabled') !== 'true') bad.push(`${label}: ${id}'s Approve button is not grey while SS Sheet 1 is not ready`);
          if (!/^SS Sheet 1 · back engravings \d+ of \d+$/.test(say(id) || '')) bad.push(`${label}: ${id}'s reason line says "${say(id)}"`);
        }
        const why = approve(GF) && approve(GF).querySelector('button[data-approve-reason]');
        if (!why) bad.push(`${label}: GF Sheet 1's reason line is not a link to SS Sheet 1's '!'`);
        else { why.click(); const panel = await until(() => d.getElementById('libIssuesPanel')); if (!panel) bad.push(`${label}: the reason line under GF Sheet 1's button opens no panel`); else { if (panel.getAttribute('data-issues-for') !== 'sheet:' + SS) bad.push(`${label}: the reason line opened ${panel.getAttribute('data-issues-for')}, not SS Sheet 1's panel`); await closePanel(); } }
        const gfCard = body.querySelector(`[data-laser-card]`);
        if (image2) {
          // Paul's image 2: GF Sheet 1's Order check, the four orders of this sheet; no row, header, count or word of SS Sheet 1
          const real = gf.filter(b => !b.hasAttribute('data-issues-quiet'));
          if (gf.length !== 1 || real[0].getAttribute('data-issues-step') !== 'orders' || !real[0].classList.contains('flowBang')) bad.push(`${label}: GF Sheet 1's marks: ${gf.map(b => b.getAttribute('data-issues-step') + (b.hasAttribute('data-issues-quiet') ? ':quiet' : ''))}`);
          else {
            real[0].click(); const panel = await until(() => d.getElementById('libIssuesPanel'));
            if (!panel) bad.push(`${label}: GF Sheet 1's Order check opens no panel`);
            else {
              const rows = [...panel.querySelectorAll('.lisRow')], head = panel.querySelector('.lisCount'), text = panel.textContent.replace(/\s+/g, ' ');
              if (rows.length !== 4) bad.push(`${label}: the Order check lists ${rows.length} orders, expected 4`);
              if (!head || head.textContent.trim() !== '4 issues' || head.classList.contains('quiet')) bad.push(`${label}: the header says "${head && head.textContent}", expected 4 issues`);
              if (panel.querySelector('.lisWait,[data-quiet],[data-issue-key="waitsOnSheet"],[data-issue-sheet]')) bad.push(`${label}: a wait row is in GF Sheet 1's list`);
              if (/Waiting|SS Sheet|\bEngraving ?\d/.test(text)) bad.push(`${label}: the panel mentions the other sheet or a wait: "${text.slice(0, 160)}"`);
              const chips = rows.map(r => r.querySelector('.lisChip').textContent.trim()).sort();
              if (JSON.stringify(chips) !== JSON.stringify(['Unknown SKU', 'Unknown SKU', 'Unknown SKU', 'Waits on RG Sheet 1'])) bad.push(`${label}: the four orders say ${JSON.stringify(chips)}`);
              if (panel.querySelector('.lisBody').firstElementChild && !panel.querySelector('.lisBody').firstElementChild.querySelector('.lisRow')) bad.push(`${label}: the first thing in the list is not an order of this sheet`);
              await closePanel();
            }
          }
          // the one place that says SS Sheet 1 holds the set: the Approve line under GF Sheet 1 (and not twice)
          const mentions = (gfCard ? [...body.querySelectorAll(`.librarySheet`)] : []).filter(x => x.querySelector(`[data-approve-for="sheet:${GF}"]`)).map(x => (x.textContent.match(/SS Sheet 1/g) || []).length);
          if (mentions.length !== 1 || mentions[0] !== 1) bad.push(`${label}: SS Sheet 1 is named ${JSON.stringify(mentions)} times in GF Sheet 1's own article, expected once (its Approve line)`);
        } else {
          // the shop where GF Sheet 1 is fine: its rail carries NO mark at all (no '!', no clock), Laser cutting is its plain current dot
          if (gf.length) bad.push(`${label}: GF Sheet 1 has ${gf.length} mark(s) on its rail (${gf.map(b => b.getAttribute('data-issues-step') + (b.hasAttribute('data-issues-quiet') ? ':quiet' : ':bang')).join(',')}); it is fine`);
          const gfFlow = body.querySelector(`.flowBox[data-flow-for="sheet:${GF}"]`), cur = gfFlow && gfFlow.querySelector('.flowStep.current');
          if (!cur || cur.querySelector('span').textContent !== 'Laser cutting' || cur.querySelector('button') || !cur.querySelector('i.flowDot')) bad.push(`${label}: GF Sheet 1's current step is not a plain Laser cutting dot: ${cur && cur.outerHTML.slice(0, 160)}`);
        }
        if (net.length) bad.push(`${label}: a network call was made (${net.length})`);
      } finally { w.close(); }
      return bad;
    };
    for (const spec of ['paul', 'image2']) must(await ui(spec, NEW, 'real ' + spec), `6: on screen (${spec}), both sheets in the same set, SS Sheet 1 not ready`);
    // the mutants, on screen: the set's wait put back in the list, the clock put back on the rail (bridge), the panel's drop of such an entry taken out
    const waitsUi = await ui('paul', waits.R, 'wait put back', { readiness: waits.src });
    assert(waitsUi.some(x => /mark\(s\) on its rail/.test(x)), `the on-screen check catches the set's wait put back in the list: ${JSON.stringify(waitsUi.slice(0, 3))}`);
    const clock = BRIDGE.replace('      :`<i class="flowDot">${s.state===\'done\'', '      :(s.key===\'laser\' && s.current && !e.ready && !e.done)?`<button type="button" class="flowDot flowWait" data-issues-open data-issues-quiet="1" data-issues-kind="sheet" data-issues-id="${esc(id)}" data-issues-step="laser" data-sheet-id="${esc(id)}" data-step="laser" aria-label="clock">o</button>`\n      :`<i class="flowDot">${s.state===\'done\'');
    assert(clock !== BRIDGE, 'the clock mutant found its place in the rail');
    const clockUi = await ui('paul', NEW, 'clock put back', { bridge: clock });
    assert(clockUi.some(x => /quiet clock/.test(x)) && clockUi.some(x => /mark\(s\) on its rail/.test(x)), `the on-screen check catches the clock put back on the rail: ${JSON.stringify(clockUi.slice(0, 3))}`);
    const noDrop = ISSUES.replace('.filter(it => it && !isWait(it))', '.filter(Boolean)');
    assert(noDrop !== ISSUES, 'the panel mutant found the drop of wait entries');
    const panelUi = await ui('paul', waits.R, 'panel keeps the entry', { readiness: waits.src, issues: noDrop });
    assert(panelUi.some(x => /mentions the other sheet|panel/.test(x)) || panelUi.some(x => /mark\(s\) on its rail/.test(x)), `the on-screen check catches the entry reaching the panel: ${JSON.stringify(panelUi.slice(0, 3))}`);
  }

  must(failures, 'regression');
  if (!process.exitCode) console.log(`PASS: same set, mate not ready: no order issue on either sheet (page rows, records and the server's reading); the set's wait is in NO sheet's list (GF Sheet 1's is empty, SS Sheet 1's is its own engravings) and is said once, by the gate ("SS Sheet 1 · back engravings N of M"); gone when SS is ready; Paul's image 2 lists its four orders and nothing of SS Sheet 1; a split between two sets still reports as a split without "waits"; another piece's real problem still lists the order for that piece only; the old rule, the wait put back in the list, the clock put back on the rail and the panel keeping such an entry are all caught (${caught.length} + ${caughtWait.length} findings)${JSDOM ? '; on screen: no mark on GF Sheet 1, the Approve line names SS Sheet 1 once and opens its \'!\'' : ''}`);
})().catch(e => { console.error(e); process.exit(1); });
