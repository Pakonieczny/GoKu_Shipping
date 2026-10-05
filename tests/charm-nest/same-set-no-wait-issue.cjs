/* Regression (round 7, Paul, 5 Oct 2026): "I'm not sure why this specific order is holding up the gold fill sheet that doesn't make any sense. It looks
 * perfectly fine to me and it's on both sheets and both sheets are in the same set." / "you cannot have a green approved button on a single sheet that is part
 * of a set where the other sheets are not ready yet".
 *
 * Order 4170252963 (Nathaly Soto) has one piece on GF Sheet 1 and one on SS Sheet 1, both in Set 1. SS Sheet 1 is not ready (back engravings 7 of 25). The
 * order is FINE; it was listed in GF Sheet 1's "!" Order check panel as "Waits on SS Sheet 1". A set advances as ONE, so a mate sheet of the SAME set that is not
 * ready is the SET's wait, said once and quietly ("Waiting for SS Sheet 1 · Engraving"), never an issue of an order and never a red '!'.
 *
 * What this holds to, with the real CharmNestReadiness (page and server twin), the real LaserReview rail and the real '!' panel, on a shop shaped like Paul's:
 *   1. both sheets in the same set, the mate not ready: NO order issue on either sheet, in every reading (the page's rows, the records, the server's own per-sheet reading);
 *   2. the set's wait is ONE quiet entry (not an issue: quiet:true, never counted), on the sheet that is done, naming the mate and its step; none on the mate itself;
 *   3. the mate ready again: the wait is gone too;
 *   4. the same two pieces on sheets of DIFFERENT sets still report, worded as the split they are ("Split between Set 1 and Set 2"), with no 'waits' in the words;
 *   5. a real problem of another piece of the same order still lists the order, for that piece only (the mate sheet's piece is not named as waiting);
 *   6. on screen: the done sheet's rail has no '!' but one quiet clock; the panel it opens says the wait once, lists no order and counts nothing; a press on the row
 *      opens the mate; the mate's own rail keeps its real '!' on Engraving and its panel shows no wait;
 *   7. the old rule put back (the real module patched) is CAUGHT by every one of these checks; the server twin reads each sheet with its own set.
 *
 *   NODE_PATH=<dir with jsdom> node tests/charm-nest/same-set-no-wait-issue.cjs      (part 6 is skipped, with a note, when jsdom is not installed)
 * Offline: no network, nothing written. */
'use strict';
const fs = require('fs'), path = require('path'), assert = require('node:assert/strict');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const P = require('./issues-property.cjs');
const { paulSpec, paulShop } = require('./issues-paul.cjs');
const ROOT = path.join(__dirname, '../..');
const READINESS = path.join(ROOT, 'charm-nest-readiness.js');
const SRC = fs.readFileSync(READINESS, 'utf8');
const NEW = require(READINESS);
const RULE = '!ownSetWait(b,mySet)';

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

/** The checks of 1-3 on the shop where both sheets are in the SAME set; expectWait: SS Sheet 1 is not ready, so GF Sheet 1 carries the set's wait. Returns what is wrong. */
function sameSetChecks(R, shop, expectWait) {
  const bad = [], got = read(R, shop);
  for (const [key, list] of Object.entries(got)) {
    const id = key.split(':')[1], orders = list.filter(i => i.step === 'orders'), waits = list.filter(i => i.key === 'waitsOnSheet');
    if (orders.length) bad.push(`${key}: ${orders.length} order issues listed (${orders.slice(0, 2).map(i => `${i.orderId} ${i.key}`).join(', ')}); the orders are on two sheets of ONE set`);
    if (list.some(i => i.key === 'otherSheetNotReady' || (i.pieces || []).some(p => p.kind === 'otherSheetNotReady'))) bad.push(`${key}: something says "waits on another sheet"`);
    if (list.some(i => /\bwaits? (on|for)\b/i.test(wordsOf(i)) && i.key !== 'waitsOnSheet')) bad.push(`${key}: an order's words say it waits`);
    if (id === SS && waits.length) bad.push(`${key}: the sheet that holds the set is told to wait for the sheet that is fine`);
    if (id === GF) {
      if (waits.length !== (expectWait ? 1 : 0)) bad.push(`${key}: ${waits.length} set waits, expected ${expectWait ? 1 : 0}`);
      for (const w of waits) {
        if (w.quiet !== true) bad.push(`${key}: the set's wait is not quiet`);
        if (w.label !== 'SS Sheet 1' || w.stepLabel !== 'Engraving' || !(w.open && w.open.id === SS) || !w.counter || !(w.counter.of > w.counter.done)) bad.push(`${key}: the wait does not name SS Sheet 1, Engraving and the count: ${JSON.stringify(w).slice(0, 200)}`);
        if (!/^back engravings \d+ of \d+$/.test(w.why || '')) bad.push(`${key}: the wait says "${w.why}"`);
      }
      if (list.some(i => !i.quiet && i.step !== 'orders')) bad.push(`${key}: GF Sheet 1 is fine: ${JSON.stringify(list.filter(i => !i.quiet).map(i => i.key))}`);
    }
    if (id === SS && expectWait) { const own = list.filter(i => !i.quiet); if (own.length !== 1 || own[0].key !== 'approvalsNeeded' || own[0].step !== 'engraving') bad.push(`${key}: SS Sheet 1's one real entry is its own engravings: ${JSON.stringify(own.map(i => i.key))}`); }
  }
  return bad;
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
      assert.equal(orders.length, 5, `${mode}: the split orders are listed`); assert.equal(list.filter(i => i.quiet).length, 0, `${mode}: SS Sheet 1 is in ANOTHER set: no set wait for it`);
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
    assert.equal(list.filter(i => i.quiet).length, 1, 'and the set\'s wait is still said once');
  }

  // ── 7a. the old rule put back is caught by 1 + 2 (and the server twin reads each sheet with its own set)
  const old = oldRule(), caught = sameSetChecks(old.R, paulShop(), true);
  assert(caught.length >= 4 && caught.some(c => /order issues listed/.test(c)) && caught.some(c => /waits on another sheet/.test(c)), `the regression catches the old rule: ${JSON.stringify(caught.slice(0, 3))}`);
  assert(P.mateChecks(old.R, paulShop()).some(d => d.type === 'sameSetWaitListed'), 'the property harness catches it too');
  const server = fs.readFileSync(path.join(ROOT, 'netlify/functions/charmNestLibrary.js'), 'utf8');
  assert(server.includes('Readiness.forSheet(orders[id],s.id || s.sheetId,Readiness.setOf(s))'), 'the server twin (productionReadiness) reads each sheet\'s orders with that sheet\'s own set');
  assert(SRC.includes('forSheet(s.orderReadiness?.[id],sid,setOf(s))') && SRC.includes('forSheet(reps[id],sid,mine)') && SRC.includes('forSheet(s.orderReadiness[id],sid,setOf(s))'), 'the page reads every sheet with its own set in all three places (stage, readFrom, orderBlockers)');

  // ── 6. on screen (needs jsdom)
  let JSDOM = null; try { ({ JSDOM } = require('jsdom')); } catch (_) { /* skipped below */ }
  if (!JSDOM) console.log('  - jsdom is not installed (set NODE_PATH): the on-screen part 6 was not run');
  else {
    const sleep = n => new Promise(r => setTimeout(r, n)), until = async (f, ms = 3000) => { const t0 = Date.now(); for (;;) { const v = f(); if (v) return v; if (Date.now() - t0 > ms) return null; await sleep(10); } };
    const ui = async (readinessSrc, R, label) => {
      const bad = [], dom = new JSDOM('<body><div class="topbar"></div><main id="libBody"></main></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true }), w = dom.window, d = w.document, calls = [], net = [];
      try {
        w.matchMedia = () => ({ matches: true }); w.Element.prototype.getClientRects = function () { return [{}]; }; w.fetch = (...a) => { net.push(a[0]); return Promise.reject(new Error('offline')); };
        Object.assign(w, { S: { mode: 'library', library: { rows: [], kind: 'sets', metal: 'all' }, cloud: { ok: false } }, api: async () => { net.push('api'); return { sheets: [], sets: [] }; }, allSheets: () => [], Orders: { rows: () => w.__rows || [] },
          Engrave: { items: () => new Map(), backsMarkup: () => '' }, esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
          Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS', rose: 'RG' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, cors: x => x, pvRatio: () => '',
          sheetHead: r => `<div class="h"><span class="nm">${r.metal} Sheet ${r.sheetIndex}</span></div>`, openLibrarySheet: id => { calls.push(['sheet', id]); return true; }, openOrderFrom: (b, rid) => { calls.push(['order', rid]); return true; }, setMode: m => calls.push(['mode', m]),
          ListMedia: { peek: () => null, listing: () => Promise.resolve(null) } });
        w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders; w.eval(readinessSrc);
        for (const f of ['charm-nest-activity.js', 'charm-nest-motion.js']) w.eval(fs.readFileSync(path.join(ROOT, f), 'utf8'));
        const bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
        w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
        const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
        w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {libraryCard,libraryGroups};})();'); w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-library-issues.js'), 'utf8'));
        const L = w.LaserReview, LI = w.LibraryIssues, body = d.getElementById('libBody'), recs = JSON.parse(JSON.stringify(P.serverLike(shop, R)));
        w.__rows = JSON.parse(JSON.stringify(S.uiRows(shop)));
        const sets = shop.sets.map(x => ({ ...x, status: 'saved', name: 'Set ' + x.seq, day: '2026-10-05', seq: x.seq }));
        L.sections(body); for (const r of recs) L.record(r);
        for (const g of w.Sets.libraryGroups(sets, recs)) { const card = w.Sets.libraryCard(g, g.sheets, g.sheets); L.place(card, L.group(g, g.sheets).ready, body); }
        L.changed(); await sleep(60);
        const marks = id => [...body.querySelectorAll(`button[data-issues-open][data-issues-id="${id}"]`)], quiet = b => b.hasAttribute('data-issues-quiet');
        const gf = marks(GF), ss = marks(SS);
        // the done sheet: no '!', one quiet clock on Laser cutting
        if (gf.some(b => !quiet(b))) bad.push(`${label}: GF Sheet 1 has a '!' on ${gf.filter(b => !quiet(b)).map(b => b.getAttribute('data-issues-step')).join(',')} (it is fine)`);
        const clock = gf.filter(quiet);
        if (clock.length !== 1) bad.push(`${label}: GF Sheet 1 has ${clock.length} quiet clocks, expected one`);
        else {
          const k = clock[0];
          if (k.getAttribute('data-issues-step') !== 'laser' || k.classList.contains('flowBang') || /!/.test(k.textContent) || !k.querySelector('svg')) bad.push(`${label}: the quiet mark is not a clock on Laser cutting: ${k.outerHTML.slice(0, 160)}`);
          k.click(); const panel = await until(() => d.getElementById('libIssuesPanel'));
          if (!panel) bad.push(`${label}: the clock opens no panel`);
          else {
            const waitRows = [...panel.querySelectorAll('.lisWait')], text = panel.textContent.replace(/\s+/g, ' '), head = panel.querySelector('.lisCount');
            if (waitRows.length !== 1) bad.push(`${label}: the wait is said ${waitRows.length} times, expected once`);
            else if (!/^Waiting for SS Sheet 1 · Engraving ?\d+ \/ \d+$/.test(waitRows[0].textContent.replace(/\s+/g, ' ').trim())) bad.push(`${label}: the wait row says "${waitRows[0].textContent.trim()}"`);
            if (panel.querySelectorAll('.lisRow').length) bad.push(`${label}: the panel lists ${panel.querySelectorAll('.lisRow').length} orders`);
            if (!head || head.textContent.trim() !== 'Waiting' || !head.classList.contains('quiet')) bad.push(`${label}: the header counts the wait: ${head && head.textContent}`);
            if (/\bWaits on\b|\bissues?\b|\blines?\b/i.test(text)) bad.push(`${label}: the panel says "${text.slice(0, 120)}"`);
            if (waitRows[0]) { calls.length = 0; waitRows[0].click(); await sleep(30); if (!calls.some(x => x[0] === 'sheet' && x[1] === SS)) bad.push(`${label}: a press on the wait row does not open SS Sheet 1 (${JSON.stringify(calls)})`); }
            panel.classList.remove('handed'); LI.close(); await sleep(10); d.querySelectorAll('#libIssuesPanel').forEach(n => n.remove());
          }
        }
        // the sheet that holds the set: its own real '!' (Engraving), its panel has no wait
        const sreal = ss.filter(b => !quiet(b)), sq = ss.filter(quiet);
        if (sq.length) bad.push(`${label}: SS Sheet 1 has a quiet clock (it holds the set, it does not wait)`);
        if (sreal.length !== 1 || sreal[0].getAttribute('data-issues-step') !== 'engraving') bad.push(`${label}: SS Sheet 1's marks: ${sreal.map(b => b.getAttribute('data-issues-step'))}`);
        else { sreal[0].click(); const panel = await until(() => d.getElementById('libIssuesPanel')); if (!panel) bad.push(`${label}: SS Sheet 1's '!' opens no panel`); else { if (panel.querySelector('.lisWait')) bad.push(`${label}: SS Sheet 1's panel shows a wait`); if (panel.querySelectorAll('.lisRow').length) bad.push(`${label}: SS Sheet 1's engraving panel lists orders`); LI.close(); await sleep(10); d.querySelectorAll('#libIssuesPanel').forEach(n => n.remove()); } }
        if (net.length) bad.push(`${label}: a network call was made (${net.length})`);
      } finally { w.close(); }
      return bad;
    };
    must(await ui(SRC, NEW, 'real'), '6: on screen, both sheets in the same set, SS Sheet 1 not ready');
    const oldUi = await ui(old.src, old.R, 'old rule');
    assert(oldUi.length >= 1 && oldUi.some(x => /GF Sheet 1 has a '!'/.test(x)), `the on-screen check catches the old rule: ${JSON.stringify(oldUi.slice(0, 3))}`);
  }

  must(failures, 'regression');
  if (!process.exitCode) console.log(`PASS: same set, mate not ready: no order issue on either sheet (page rows, records and the server's reading); the set's wait is one quiet entry (GF Sheet 1, "SS Sheet 1 · Engraving"), gone when SS is ready; a split between two sets still reports as a split without "waits"; another piece's real problem still lists the order for that piece only; the old rule put back is caught (${caught.length} findings)${JSDOM ? '; on screen: a quiet clock, no \'!\', one wait row, a way to SS Sheet 1' : ''}`);
})().catch(e => { console.error(e); process.exit(1); });
