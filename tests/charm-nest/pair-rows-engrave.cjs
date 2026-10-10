// ROWENGRAVE (pair-rows-1010): the ENGRAVE tab lists ONE ROW per earring order line, with both ears on it.
//
// Paul, 10 Oct 2026: "All earring sets (Stud and Huggie Hoop) should be listed as one order with 2 individual thumbnails or the 1 thumbnail we currently
// have with both left and right charm vectors displayed side by side." The Engrave list made one row per piece (a Left row and a Right row of the same order).
//
// What it proves, offline (no network, no paid call, nothing live, nothing written anywhere but memory):
//   1  charm-nest-engrave-rows.js: the Left and the Right job of one line are one row; a single, a single earring, a disc, a plain quantity-N line and an old
//      line without sides are rows of their own; quantity 2 is ONE row; the counter, Back / Next and the "Approve both ears" gate work in rows
//   2  the real Engrave module drawn in a fake page (needs jsdom, skipped with a line when it is not installed): the duck line is ONE placement row with both
//      ears told on it (their words and state) and the line's own picture asked of ListMedia.pair once (the line's row, never an ear's); a single, a necklace's
//      discs and a plain quantity-N line keep the row they had; the tab badge, the card's "N of M · K done" and Back / Next count and move row by row; the editor
//      has a Left | Right switch; the Decided tab shows the line as one row with a Reopen for each ear
//   3  approval: pressing "Approve both ears" runs the ORDINARY approval of the Left, then of the Right: the same per-piece records, seals, timeline events and back
//      saves as two separate presses; ears whose words differ, or an ear never put in front of the person, are never approved together; a single is untouched
//   node tests/charm-nest/pair-rows-engrave.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Rows = require(path.join(root, 'charm-nest-engrave-rows.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Activity = require(path.join(root, 'charm-nest-activity.js'));
const F = require('./pairs-fixtures.cjs');
let JSDOM = null; try { ({ JSDOM } = require('jsdom')); } catch (_) { /* the page checks are skipped */ }

let failed = 0, passed = 0;
const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n      ')); } };

/* ── a world: the duck huggie line, two pairs of one design, a mismatched pair, a three-disc necklace, a pendant, a single earring, a plain quantity-3 line ── */
const W = F.world({ orders: [
  { rid: F.rid(1), lines: [{ n: 10, kind: 'hoop', title: 'HUGGIE HOOPS- RUBBER DUCK(SHAPE)', on: 'sh-gf1' }] },
  { rid: F.rid(2), lines: [{ n: 20, kind: 'pair', qty: 2, on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'mismatched', on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(6), lines: [{ n: 60, kind: 'earring-single', on: 'sh-gf1' }] },
  { rid: F.rid(7), lines: [{ n: 70, kind: 'single', qty: 3, on: 'sh-gf1' }] },
  { rid: F.rid(8), lines: [{ n: 80, kind: 'pair', on: 'sh-gf1' }] },   // (its old record, below, has no side anywhere)
] });
const LEG = F.legacy(W);
const lineRow = (w, ord, n) => {
  const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n);
  return { key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku, title: l.line.title, quantity: l.qty, variations: [{ name: 'Metal choice', value: 'Gold' }] },
    spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['Mom'], engraveCandidate: true }, state: 'written', material: 'gold', problems: [] };
};
const poolOf = w => new Map(w.pool.map(p => [p.poolId, p]));
const ROW = { duck: lineRow(W, 1, 10), pair2: lineRow(W, 2, 20), mis: lineRow(W, 3, 30), disc: lineRow(W, 4, 40), pend: lineRow(W, 5, 50), ear1: lineRow(W, 6, 60), plain3: lineRow(W, 7, 70), old: lineRow(LEG, 8, 80) };
const ctxFor = w => { const rows = poolOf(w); return { poolRow: id => rows.get(id), charmOf: id => { const r = rows.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => F.entryOf(sku), pair: Pair, mergeSeals: (...r) => Seals.merge(...r) }; };
const CTX = ctxFor(W), CTXL = ctxFor(LEG);

/* a job the way the Engrave module holds it, for the pure checks */
const job = (row, slot, state, lines, extra) => { const j = Object.assign({ key: Sides.jobKey(row.key, slot), rowKey: slot ? row.key : undefined, slot: slot || undefined, state, lines: lines || [], copies: slot ? Sides.idsOfSlot(CTX, row, slot) : row.poolIds.slice() }, extra || {}); if (!slot) delete j.slot; if (!slot) delete j.rowKey; return j; };
const readyFit = () => ({ fit: { ok: true, size: 2, capMm: 1.8 }, verify: { geometry: { ok: true } } });

(async () => {
  console.log('ROWENGRAVE');

  // ═══ 1 · the pure row module ═══
  await t('1a the Left and the Right job of one line are one row; every other job is a row of its own', () => {
    const duckL = job(ROW.duck, 'L', 'review', ['Mom']), duckR = job(ROW.duck, 'R', 'review', ['Mom']);
    const d1 = job(ROW.disc, 'D1', 'review', ['A']), d2 = job(ROW.disc, 'D2', 'review', ['B']), d3 = job(ROW.disc, 'D3', 'review', ['C']);
    const pend = job(ROW.pend, null, 'review', ['Mom']), plain = job(ROW.plain3, null, 'review', ['Mom']), old = job(ROW.old, null, 'review', ['Mom']);
    const jobs = [duckL, d1, duckR, d2, pend, d3, plain, old];
    const rows = Rows.group(jobs, jobs);
    assert.equal(rows.length, 7, '8 jobs: the two ears are one row, everything else is its own');
    assert.deepEqual(rows.map(r => r.jobs.map(j => j.key)), [[duckL.key, duckR.key], [d1.key], [d2.key], [pend.key], [d3.key], [plain.key], [old.key]], 'the row sits where its first job stood; the ears are Left then Right');
    assert.equal(rows[0].pair, true); assert.equal(rows[0].lead, duckL); assert.ok(rows.slice(1).every(r => !r.pair), 'discs, a single, a plain quantity-3 line and an old line are never a pair');
    assert.equal(Rows.earOf(d1), null); assert.equal(Rows.earOf(pend), null); assert.equal(Rows.earOf(duckR), 'R');
    assert.equal(Rows.lineKeyOf(duckR), ROW.duck.key); assert.equal(Rows.lineKeyOf(pend), ROW.pend.key);
  });
  await t('1b quantity 2 is ONE row (the Left job holds both left pieces), as the Orders list shows one row "Qty 2 · Left + Right"', () => {
    const jl = job(ROW.pair2, 'L', 'review', ['Anna']), jr = job(ROW.pair2, 'R', 'review', ['Anna']);
    assert.equal(jl.copies.length, 2); assert.equal(jr.copies.length, 2);
    const rows = Rows.group([jl, jr], [jl, jr]); assert.equal(rows.length, 1); assert.equal(rows[0].pair, true);
  });
  await t('1c a search that finds one ear, or a tab holding one ear, still gives the line its other ear when it is in the same set; a line with one ear here is a one-ear row', () => {
    const jl = job(ROW.duck, 'L', 'review', ['Mom']), jr = job(ROW.duck, 'R', 'review', ['Mom']);
    assert.deepEqual(Rows.group([jl], [jl, jr])[0].jobs, [jl, jr], 'selected = the ears that matched; the row takes both from the universe');
    const only = Rows.group([jr], [jr]); assert.equal(only.length, 1); assert.equal(only[0].pair, false); assert.deepEqual(only[0].jobs, [jr], 'the Left is decided: the Right is a row of one ear (the row a single piece has today)');
  });
  await t('1d counts: "N of M · K done" in rows (a pair is one), and Back / Next move row by row', () => {
    const jl = job(ROW.duck, 'L', 'review'), jr = job(ROW.duck, 'R', 'review'), s = job(ROW.pend, null, 'review'), p = job(ROW.plain3, null, 'review');
    const q = Rows.group([jl, jr, s, p], [jl, jr, s, p]);
    assert.deepEqual(Rows.counts(q, []), { decided: 0, remaining: 3, n: 1, of: 3, text: '1 of 3 · 0 done' });
    assert.equal(Rows.counts(q, [{}, {}]).text, '3 of 5 · 2 done');
    assert.equal(Rows.neighbour(q, jl.key, 1).lead, s, 'Next from the Left: the next ROW, not the Right ear');
    assert.equal(Rows.neighbour(q, jr.key, 1).lead, s, 'Next from the Right: the same');
    assert.equal(Rows.neighbour(q, s.key, -1).lead, jl, 'Back from the single: the pair row');
    assert.equal(Rows.neighbour(q, p.key, 1).lead, jl, 'Next wraps to the first row'); assert.equal(Rows.neighbour(q, jl.key, -1).lead, p, 'Back wraps to the last row');
    assert.equal(Rows.neighbour(q, 'nowhere', 1).lead, jl); assert.equal(Rows.neighbour(q, 'nowhere', -1).lead, p); assert.equal(Rows.neighbour([], 'x', 1), null);
    assert.equal(Rows.openLead(q[0], j => j === jl), jr, 'a click on a row opens the first ear that is not being prepared'); assert.equal(Rows.openLead(q[0], () => true), jl);
    assert.equal(Rows.openLead(q[0]), jl);
  });
  await t('1e the words and the state of an ear, in plain words', () => {
    assert.equal(Rows.wordsOf({ lines: ['Anna', ' Ben '] }), 'Anna / Ben'); assert.equal(Rows.sameWords({ lines: ['Mom'] }, { lines: ['Mom'] }), true);
    assert.equal(Rows.sameWords({ lines: ['Mom'] }, { lines: ['Mum'] }), false); assert.equal(Rows.sameWords({ lines: [] }, { lines: [] }), false, 'no words is never "the same words"');
    assert.equal(Rows.stageOf({ state: 'review' }), 'Placement to check'); assert.equal(Rows.stageOf({ state: 'words' }), 'Words to confirm'); assert.equal(Rows.stageOf({ state: 'classify' }, true), 'Reading words…'); assert.equal(Rows.stageOf({ state: 'ready' }, true), 'Preparing preview…');
  });
  await t('1f "Approve both ears" is offered only for a pair ready on both ears, with the very same words, both placements shown, nothing running', () => {
    const mk = (state, words, extra) => job(ROW.duck, null, state, words, Object.assign(readyFit(), extra || {}));
    const L = Object.assign(job(ROW.duck, 'L', 'review', ['Mom']), readyFit()), R = Object.assign(job(ROW.duck, 'R', 'review', ['Mom']), readyFit());
    const row = { pair: true, jobs: [L, R] }, shown = new Set([L, R]), o = { shown: j => shown.has(j) };
    assert.equal(Rows.approveBoth(row, o).ok, true);
    assert.equal(Rows.approveBoth({ pair: false, jobs: [L] }, o).ok, false, 'one ear: nothing to approve together');
    R.lines = ['Dad']; assert.match(Rows.approveBoth(row, o).why, /words differ/); R.lines = ['Mom'];
    shown.delete(R); assert.match(Rows.approveBoth(row, o).why, /Right ear's placement first/); shown.add(R);
    R.state = 'words'; assert.equal(Rows.approveBoth(row, o).ok, false); R.state = 'review';
    R.verify = { geometry: { ok: false } }; assert.equal(Rows.approveBoth(row, o).ok, false, 'a placement that failed its check is never approved'); R.verify = { geometry: { ok: true } };
    L.approvalPreparing = true; assert.equal(Rows.approveBoth(row, o).ok, false); L.approvalPreparing = false;
    assert.equal(Rows.approveBoth(row, { shown: o.shown, busy: j => j === L }).ok, false);
    assert.equal(Rows.approveBoth(row, o).ok, true); void mk;
  });

  // ═══ 1 (continued) · DISCCYCLE (10 Oct 2026): a counted-disc order is ONE row, its discs cycled one by one; the same pure module and the shared piece switch ═══
  const Switch = require(path.join(root, 'charm-nest-piece-switch.js'));
  await t('1g GOLDEN: for pairs, singles and plain quantity-N lines the grouping is byte for byte what it was (the ROWENGRAVE group(), copied here, on 3000 random sets); opting into discs changes nothing without discs', () => {
    // the module as ROWENGRAVE shipped it (f275e21a), for the comparison
    const KEY0 = /^(.*)#(L|R|D\d{1,2})$/;
    const slot0 = j => { if (!j) return null; if (j.slot) return String(j.slot); const m = KEY0.exec(String(j.key == null ? '' : j.key)); return m ? m[2] : null; };
    const ear0 = j => { const s = slot0(j); return s === 'L' || s === 'R' ? s : null; };
    const line0 = j => { if (!j) return ''; if (j.rowKey) return String(j.rowKey); const m = KEY0.exec(String(j.key == null ? '' : j.key)); return m ? m[1] : String(j.key == null ? '' : j.key); };
    const mk0 = (lineKey, jobs) => ({ key: jobs[0].key, lineKey, jobs, lead: jobs[0], pair: jobs.length === 2 });
    function group0(selected, universe) {
      const uni = Array.isArray(universe) ? universe : selected || [], ears = new Map();
      for (const j of uni) { const s = ear0(j); if (!s) continue; const k = line0(j); if (!ears.has(k)) ears.set(k, {}); if (!ears.get(k)[s]) ears.get(k)[s] = j; }
      const out = [], done = new Set();
      for (const j of selected || []) {
        const s = ear0(j), k = line0(j);
        if (!s) { out.push(mk0(k, [j])); continue; }
        if (done.has(k)) continue; done.add(k);
        const e = Object.assign({}, ears.get(k) || {}); if (!e[s]) e[s] = j;
        out.push(mk0(k, ['L', 'R'].filter(x => e[x]).map(x => e[x])));
      }
      return out;
    }
    let seed = 20261010; const rnd = n => { seed = (Math.imul(seed, 1103515245) + 12345) >>> 0; return seed % n; };
    for (let round = 0; round < 3000; round++) {
      const all = [];
      for (let ln = 0; ln < 1 + rnd(5); ln++) {
        const key = 'ord' + rnd(4) + ':' + ln, kind = rnd(4);   // 0 pair, 1 single ear, 2 plain line, 3 a one-slot line
        const mkj = (slot, extra) => Object.assign({ key: slot ? key + '#' + slot : key, rowKey: slot ? key : undefined, slot: slot || undefined, state: 'review', lines: ['w' + rnd(3)] }, extra || {});
        if (kind === 0) { if (rnd(5)) all.push(mkj('L')); if (rnd(5)) all.push(mkj('R')); }
        else if (kind === 1) all.push(mkj(rnd(2) ? 'L' : 'R'));
        else if (kind === 2) all.push(mkj(null));
        else all.push(mkj(null, { key: key + '/' + rnd(9) }));
      }
      for (let i = all.length - 1; i > 0; i--) { const j = rnd(i + 1); [all[i], all[j]] = [all[j], all[i]]; }
      const sel = all.filter(() => rnd(4)), uni = rnd(3) ? all : sel;
      const want = group0(sel, uni);
      for (const got of [Rows.group(sel, uni), Rows.group(sel, uni, {}), Rows.group(sel, uni, { discs: true }), Rows.group(sel, uni, { discs: false })]) {
        assert.equal(got.length, want.length); got.forEach((r, i) => { assert.deepEqual(Object.keys(r), Object.keys(want[i])); assert.equal(r.key, want[i].key); assert.equal(r.lineKey, want[i].lineKey); assert.equal(r.pair, want[i].pair); assert.equal(r.lead, want[i].lead); assert.equal(r.jobs.length, want[i].jobs.length); r.jobs.forEach((j, x) => assert.equal(j, want[i].jobs[x])); });
      }
    }
    // the rest of the module, for lines with no discs
    const jl = job(ROW.duck, 'L', 'review', ['Mom']), jr = job(ROW.duck, 'R', 'review', ['Mom']), pend = job(ROW.pend, null, 'review', ['Mom']);
    const q = Rows.group([jl, jr, pend], [jl, jr, pend], { discs: true });
    assert.deepEqual(Rows.counts(q, []), { decided: 0, remaining: 2, n: 1, of: 2, text: '1 of 2 · 0 done' });
    assert.equal(Rows.neighbour(q, jr.key, 1).lead, pend); assert.equal(Rows.kindOf(q[0]), 'ears'); assert.equal(Rows.kindOf(q[1]), null); assert.equal(Rows.multi(q[0]), true); assert.equal(Rows.multi(q[1]), false);
    assert.equal(Rows.step(q, jl.key, 1).row, q[1], 'an earring line is ONE stop: from the Left, Next is the next row'); assert.equal(Rows.step(q, jr.key, 1).row, q[1]); assert.equal(Rows.step(q, pend.key, -1).row, q[0]);
    assert.equal(Rows.step(q, jl.key, 1).job, null, 'a row that is not a disc row is one stop: the caller opens its lead');
    assert.equal(Rows.approveAll(q[0], { shown: () => true }).ok, false, 'two ears are never approved by "all discs"');
  });
  await t('1h discs: the discs of one counted line are ONE row (D1, D2, D3 in disc order, wherever they stood), placed where the first one stood; ears, singles and plain lines around them are as before', () => {
    const duckL = job(ROW.duck, 'L', 'review', ['Mom']), duckR = job(ROW.duck, 'R', 'review', ['Mom']);
    const d1 = job(ROW.disc, 'D1', 'review', ['J']), d2 = job(ROW.disc, 'D2', 'review', ['Q']), d3 = job(ROW.disc, 'D3', 'review', ['K']);
    const pend = job(ROW.pend, null, 'review', ['Mom']), plain = job(ROW.plain3, null, 'review', ['Mom']);
    const jobs = [d2, duckL, pend, d1, duckR, plain, d3];
    const rows = Rows.group(jobs, jobs, { discs: true });
    assert.deepEqual(rows.map(r => r.jobs.map(j => j.key)), [[d1.key, d2.key, d3.key], [duckL.key, duckR.key], [pend.key], [plain.key]], 'one row per line; the discs are in disc order, the row is where the first of them stood');
    assert.equal(rows[0].discs, true); assert.equal(rows[0].pair, false); assert.equal(rows[0].lead, d1, 'the lead is Disc 1, not the first that happened to come'); assert.equal(rows[0].key, d1.key); assert.equal(rows[0].lineKey, ROW.disc.key);
    assert.equal(Rows.kindOf(rows[0]), 'discs'); assert.equal(Rows.multi(rows[0]), true); assert.equal(Rows.discOf(d3), 3); assert.equal(Rows.discOf(duckL), 0); assert.equal(Rows.pieceOf(d2), 'D2'); assert.equal(Rows.pieceOf(duckR), 'R'); assert.equal(Rows.pieceOf(pend), null);
    assert.equal(Rows.discOf({ key: ROW.disc.key + '#D10' }), 10, 'ten discs and more read from the key');
    // without the option the discs are rows of their own, as ROWENGRAVE shipped
    assert.equal(Rows.group(jobs, jobs).length, 7 - 1, 'seven jobs, the two ears one row');
    // a search that finds one disc, or a tab holding one disc, gives the row the others from the same set; with one disc here it is a row of one disc
    assert.deepEqual(Rows.group([d2], [d1, d2, d3], { discs: true })[0].jobs, [d1, d2, d3]);
    const one = Rows.group([d3], [d3], { discs: true }); assert.equal(one.length, 1); assert.equal(one[0].discs, false); assert.equal(Rows.multi(one[0]), false); assert.deepEqual(one[0].jobs, [d3]);
    const two = Rows.group([d1, d3], [d1, d3], { discs: true }); assert.equal(two.length, 1); assert.equal(two[0].discs, true); assert.deepEqual(two[0].jobs, [d1, d3], 'Disc 2 is decided: the row holds the discs still to settle');
    // two necklaces, three discs and two discs
    const n2a = job(ROW.disc, 'D1', 'review', ['A']), n2b = job(ROW.disc, 'D2', 'review', ['B']);
    assert.equal(Rows.group([n2a, n2b], [n2a, n2b], { discs: true })[0].discs, true);
  });
  await t('1i counts in ROWS (an order of 3 discs is one), and Back / Next walk through every disc of every order in turn', () => {
    const d1 = job(ROW.disc, 'D1', 'review', ['J']), d2 = job(ROW.disc, 'D2', 'review', ['Q']), d3 = job(ROW.disc, 'D3', 'review', ['K']);
    const pend = job(ROW.pend, null, 'review', ['Mom']), duckL = job(ROW.duck, 'L', 'review', ['Mom']), duckR = job(ROW.duck, 'R', 'review', ['Mom']);
    const rows = Rows.group([pend, d1, d2, d3, duckL, duckR], [pend, d1, d2, d3, duckL, duckR], { discs: true });
    assert.equal(rows.length, 3); assert.deepEqual(Rows.counts(rows, []), { decided: 0, remaining: 3, n: 1, of: 3, text: '1 of 3 · 0 done' }, 'three orders to settle, as ROWENGRAVE counts');
    assert.equal(Rows.counts(rows, [{}, {}]).text, '3 of 5 · 2 done');
    const order = []; let at = pend.key; for (let i = 0; i < 6; i++) { const s = Rows.step(rows, at, 1); order.push(s.job ? s.job.key : s.row.lead.key); at = s.job ? s.job.key : s.row.lead.key; }
    assert.deepEqual(order, [d1.key, d2.key, d3.key, duckL.key, pend.key, d1.key], 'Next: pendant, Disc 1, Disc 2, Disc 3, the ears (one stop), the pendant again (wraps)');
    const back = []; at = pend.key; for (let i = 0; i < 6; i++) { const s = Rows.step(rows, at, -1); const k = s.job ? s.job.key : s.row.lead.key; back.push(k); at = k; }
    assert.deepEqual(back, [duckL.key, d3.key, d2.key, d1.key, pend.key, duckL.key], 'Back: the ears, then Disc 3, Disc 2, Disc 1, the pendant');
    assert.equal(Rows.step(rows, 'nowhere', 1).row, rows[0], 'a key not in the rows: the first stop'); assert.equal(Rows.step(rows, 'nowhere', -1).row, rows[2], 'or the last'); assert.equal(Rows.step([], 'x', 1), null);
    assert.equal(Rows.step(rows, d2.key, 1).job, d3); assert.equal(Rows.step(rows, d2.key, -1).job, d1); assert.equal(Rows.step(rows, duckR.key, -1).job, d3, 'from the Right ear, Back is the last disc of the order before');
  });
  await t('1j "Approve all discs": only when every remaining disc is ready with a placement that passed, nothing running, the very same words, and every disc has been shown', () => {
    const mkd = (n, words) => Object.assign(job(ROW.disc, 'D' + n, 'review', words), readyFit());
    const d1 = mkd(1, ['Mom']), d2 = mkd(2, ['Mom']), d3 = mkd(3, ['Mom']);
    const row = { discs: true, jobs: [d1, d2, d3] }, shown = new Set([d1, d2, d3]), o = { shown: j => shown.has(j) };
    assert.equal(Rows.approveAll(row, o).ok, true);
    assert.equal(Rows.approveAll({ discs: false, jobs: [d1] }, o).ok, false, 'one disc: nothing to approve together'); assert.equal(Rows.approveAll(null, o).ok, false);
    d3.lines = ['Dad']; assert.match(Rows.approveAll(row, o).why, /words differ between the discs/); assert.equal(Rows.sameWordsAll(row.jobs), false); d3.lines = ['Mom']; assert.equal(Rows.sameWordsAll(row.jobs), true);
    shown.delete(d2); assert.match(Rows.approveAll(row, o).why, /Look at Disc 2's placement first/); shown.add(d2);
    d2.state = 'words'; assert.match(Rows.approveAll(row, o).why, /placement to check first/); d2.state = 'review';
    d1.verify = { geometry: { ok: false } }; assert.equal(Rows.approveAll(row, o).ok, false, 'a placement that failed its check is never approved'); d1.verify = { geometry: { ok: true } };
    d3.approvalPreparing = true; assert.match(Rows.approveAll(row, o).why, /still being prepared/); d3.approvalPreparing = false;
    assert.equal(Rows.approveAll(row, { shown: o.shown, busy: j => j === d1 }).ok, false);
    d1.lines = []; d2.lines = []; d3.lines = []; assert.equal(Rows.approveAll(row, o).ok, false, 'no words is never "the same words"'); d1.lines = d2.lines = d3.lines = ['Mom'];
    assert.equal(Rows.approveAll(row, {}).ok, true, 'without a shown() test the caller owns the check');
  });
  await t('1k the piece switch (shared by every place that shows a counted line): pieces, the current one, next / back, per-piece state, and the very markup of the Left | Right switch for two ears', () => {
    const d1 = Object.assign(job(ROW.disc, 'D1', 'review', ['J']), readyFit(), { font: { name: 'Typewriter', asked: 'Typewriter' } }), d2 = job(ROW.disc, 'D2', 'words', ['Q'], { font: 'Source Sans 3' }), d3 = job(ROW.disc, 'D3', 'classify', []);
    const ps = Switch.list([d1, d2, d3], { of: 3, isWorking: j => j === d3 });
    assert.deepEqual(ps.map(p => [p.slot, p.kind, p.n, p.label, p.name, p.tag, p.words, p.font.name, p.stage, p.busy]), [
      ['D1', 'disc', 1, 'Disc 1', 'Disc 1', 'DISC 1 of 3', 'J', 'Typewriter', 'Placement to check', false],
      ['D2', 'disc', 2, 'Disc 2', 'Disc 2', 'DISC 2 of 3', 'Q', 'Source Sans 3', 'Words to confirm', false],
      ['D3', 'disc', 3, 'Disc 3', 'Disc 3', 'DISC 3 of 3', '', '', 'Reading words…', true]]);
    assert.equal(Switch.kindOf(ps), 'discs');
    const c = Switch.cursor(ps, d2.key); assert.equal(c.index, 1); assert.equal(c.count, 3); assert.equal(c.current.slot, 'D2'); assert.equal(c.prev.slot, 'D1'); assert.equal(c.next.slot, 'D3'); assert.equal(c.hasPrev, true); assert.equal(c.hasNext, true);
    assert.equal(Switch.cursor(ps, d3.key).next.slot, 'D1', 'next wraps round the line'); assert.equal(Switch.cursor(ps, d1.key).prev.slot, 'D3'); assert.equal(Switch.cursor(ps, d3.key).hasNext, false);
    assert.equal(Switch.step(ps, d3.key, 1), null, 'no wrap: the end'); assert.equal(Switch.step(ps, d3.key, 1, true).slot, 'D1'); assert.equal(Switch.step(ps, d2.key, -1).slot, 'D1'); assert.equal(Switch.step(ps, 'x', 1).slot, 'D1'); assert.equal(Switch.step(ps, 'x', -1).slot, 'D3');
    assert.equal(Switch.allSameWords(ps), false);
    const html = Switch.html(ps, d2.key, { fonts: true });
    assert.equal((html.match(/class="egEarTab"/g) || []).length, 3); assert.equal((html.match(/aria-pressed="true"/g) || []).length, 1); assert.match(html, /data-ear="[^"]*#D2"[^>]*aria-pressed="true"/);
    assert.match(html, /<span class="egPiece" data-slot="D1">DISC 1 of 3<\/span><span class="egEarWords">J<\/span><span class="egEarFont">Typewriter<\/span><span class="egEarStage">Placement to check<\/span>/, 'each disc: its tag, its words, its font by name, where it stands');
    assert.match(html, /aria-label="The discs of this order, one by one"/); assert.match(html, /^<span class="egEarSwitch egMany" /, 'a switch of discs may wrap on a narrow window');
    // two ears: the Left | Right switch of the editor, character for character (the old markup is rebuilt here from its own parts)
    const L = Object.assign(job(ROW.duck, 'L', 'review', ['Mom']), readyFit()), R = Object.assign(job(ROW.duck, 'R', 'review', ['Mom & "Dad"']), readyFit(), { copies: ['a', 'b'] });
    const old = (job_, ears) => `<span class="egEarSwitch" role="group" aria-label="The left and the right ear of this line">${ears.map(j => {
      const on = j === job_, words = Rows.wordsOf(j), stage = Rows.stageOf(j, false), ear = Sides.earOf(j.slot).toLowerCase(), esc = x => String(x == null ? '' : x).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
      const chip = `<span class="egPiece" data-slot="${esc(j.slot)}">${esc(Sides.tagOf(j.slot) + ((j.copies || []).length > 1 ? ' ×' + j.copies.length : ''))}</span>`;
      return `<button type="button" class="egEarTab" data-a="ear" data-ear="${esc(j.key)}" aria-pressed="${on}" title="${on ? 'Shown now' : 'Show'}: the ${esc(ear)} · ${esc(words || 'words not settled')} · ${esc(stage)}">${chip}<span class="egEarWords">${esc(words || '…')}</span><span class="egEarStage">${esc(stage)}</span></button>`;
    }).join('')}</span>`;
    for (const cur of [L, R]) assert.equal(Switch.html(Switch.list([L, R]), cur.key), old(cur, [L, R]), 'the ears\' switch is the very markup it was');
    assert.equal(Switch.html(Switch.list([L, R]), L.key, { fonts: true }).includes('egEarFont'), false, 'ears carry no font in the switch (no font name known)');
    // the page binds one handler for every button
    const picked = []; const host = { listeners: [], contains: () => true, addEventListener(t, f) { this.listeners.push(f); }, removeEventListener(t, f) { this.listeners = this.listeners.filter(x => x !== f); } };
    const off = Switch.bind(host, (k, b) => picked.push([k, b.tag])); host.listeners[0]({ target: { closest: sel => (sel === '[data-a="ear"][data-ear]' ? { dataset: { ear: d3.key }, tag: 'b' } : null) } }); host.listeners[0]({ target: { closest: () => null } });
    assert.deepEqual(picked, [[d3.key, 'b']]); off(); assert.equal(host.listeners.length, 0);
  });

  // ═══ 2 · the real Engrave module, drawn in a fake page ═══
  const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  await t('2a the page loads the new module before the bridge, with a cache token; the public build ships it', () => {
    const iRows = html.indexOf('charm-nest-engrave-rows.js?v='), iBridge = html.indexOf('charm-nest-bridge.js?v=');
    assert.ok(iRows > 0 && iRows < iBridge, 'loaded before the bridge'); assert.ok(html.indexOf('charm-nest-engrave-sides.js') < iRows, 'after the sides module it builds on');
    assert.ok(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8').includes('"charm-nest-engrave-rows.js"'));
    assert.ok(/charm-nest-activity\.css\?v=[^"]*-re1/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-re1/.test(html), 'the changed files carry a new cache token');
  });

  if (!JSDOM) console.log('  – no jsdom installed: the page checks (2b to 2h) were not run');
  else {
    const dom = new JSDOM('<body><div id="engraveView"></div></body>', { pretendToBeVisual: false });
    const document = dom.window.document, jobs = new Map(), frames = [], timers = [], errors = [], pairCalls = [], mountCalls = [];
    dom.window.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    dom.window.HTMLCanvasElement.prototype.getContext = () => ({ fillRect() {} });
    Object.assign(dom.window, { CharmNestEngraveRows: Rows, CharmNestPieceSwitch: Switch, CharmNestEngraveSides: Sides, CharmNestPair: Pair, CNListActivity: Activity, CNEngravingSeals: Seals });
    const pool = poolOf(W);
    const ListMedia = { pair: row => { pairCalls.push(row); return '<div class="comparePair"><figure><span data-vector></span></figure><figure><span data-listing></span></figure></div>'; }, mount: (node, row) => mountCalls.push([node, row]), more() {}, pairRow: () => false };
    const c = vm.createContext({ WORKSPACE_SANDBOX: true, localStorage: { getItem() { return null; }, setItem() {} }, CNListActivity: Activity, CNEngravingSeals: Seals, window: dom.window, document, console: { error: (...x) => errors.push(x), warn() {}, log() {} },
      ResizeObserver: dom.window.ResizeObserver, B: { engrave: { items: jobs, fonts: { ok: true } }, pool: { rows: pool }, orders: { byKey: new Map() } },
      O: require(path.join(root, 'charm-nest-orders.js')), S: { settings: {}, mode: 'engrave' }, PT: 72 / 25.4, MM: 25.4 / 72, SOURCE_LABEL: {},
      Pool: { charmOf: () => null, sheetOf: () => null }, P: {}, Review: { items: () => [], count: () => 0, remove() {}, add() {} }, Master: { entryFor: sku => F.entryOf(sku), fetchEntry: async () => null },
      LiveStrip: { render() {} }, RunCtl: { poke() {} }, Orders: { render() {}, rows: () => [] }, ListMedia,
      setTimeout: fn => (timers.push(fn), timers.length), clearTimeout() {}, requestAnimationFrame: fn => (frames.push(fn), frames.length), cancelAnimationFrame() {},
      el: (tag, cls, html_ = '') => { const e = document.createElement(tag); e.className = cls; e.innerHTML = html_; return e; },
      esc: v => String(v ?? '').replace(/[&<>"']/g, x => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[x])),
      toast() {}, agent() {}, getComputedStyle: () => ({ flexGrow: '0' }), employeeName: () => 'Paul', humanAct() {}, TL: { rec() {}, line() {} } });
    const pmStart = source.indexOf('function purchaseMarkup('); vm.runInContext(source.slice(pmStart, source.indexOf('\n}\n', pmStart) + 3), c);   // (only this one function: the page's ListMedia is stood in for here)
    const BACK_CALL = 'const bc = renderBack(job, px, { grid: true, editable: true, include: framed });';
    let moduleSrc = source.slice(source.indexOf('const Engrave = window.Engrave ='), source.indexOf('/* ═══ 22 · Sets'));
    assert.ok(moduleSrc.includes(BACK_CALL), 'the editor draws its back here (the test stands a plain canvas in for the drawing machinery)');
    moduleSrc = moduleSrc.replace(BACK_CALL, 'const bc = standInBack();');
    c.standInBack = () => { const cv = document.createElement('canvas'); cv._map = { bb: [0, 0, 1, 1], k: 1 }; cv._paint = () => {}; return cv; };
    vm.runInContext(moduleSrc, c);
    const runFrames = () => { for (let n = 0; n < 20 && frames.length; n++) frames.splice(0).forEach(f => f()); };
    const E = dom.window.Engrave;
    const slotJobs = row => E.ensureJobs(row);
    // jobs in the order of the page's own pull: the duck, two pairs, the mismatched pair, the necklace's discs, a pendant, a single earring, the plain line, an old pair
    const made = {}; for (const k of ['duck', 'pair2', 'mis', 'disc', 'pend', 'ear1', 'plain3']) made[k] = slotJobs(ROW[k]);
    // (the old pair: its pool rows carry no side, so the page cuts it into no ears: one job under the line key, as before)
    c.B.pool.rows = poolOf(LEG);   // ← only for the old line below
    made.old = slotJobs(ROW.old); c.B.pool.rows = pool;
    const setup = (js, state, words) => js.forEach((j, i) => { j.state = state; j.lines = Array.isArray(words[0]) ? (words[i] || words[0]).slice() : words.slice(); j.text = j.lines.join('\n'); if (state === 'review') Object.assign(j, { fit: { ok: true, size: 2, capMm: 1.8, weight: 'Regular', angle: 0, centre: [0, 0], fittedMax: 3, glyphs: [] }, view: {}, verify: { geometry: { ok: true } } }); });
    const reset = () => {
      setup(made.duck, 'review', ['Mom']); setup(made.pair2, 'words', [['Anna'], ['Ben']]); setup(made.mis, 'review', [['Tennis ball'], ['Racket']]);
      setup(made.disc, 'review', [['A'], ['B'], ['C']]); setup(made.pend, 'review', ['Mom']); setup(made.ear1, 'review', ['Mom']); setup(made.plain3, 'review', ['Mom']); setup(made.old, 'review', ['Mom']);
      for (const j of jobs.values()) { j.row.engrave = Object.assign({ needed: true, approved: false }, { state: j.state }); delete j.approvedAt; delete j.approvedBy; }
      pairCalls.length = 0; mountCalls.length = 0;
    };
    reset();
    // the exact markup the Engrave lists drew for a single piece before this work (captured from the unchanged page code): a row that is not an earring pair is not touched
    const GOLD_PLACE = "<div class=\"doneRow placementRow hoverItem\" role=\"button\" tabindex=\"0\" data-open=\"4190000005_5000000050\" data-mkey=\"eg:4190000005_5000000050\" data-rid=\"4190000005\" aria-busy=\"false\" aria-disabled=\"false\" aria-label=\"Open engraving for order 4190000005 · ONE-PENDANT\"><div class=\"comparePair\"><figure><span data-vector=\"\"></span></figure><figure><span data-listing=\"\"></span></figure></div><div class=\"engravingIdentity\"><span class=\"queueLabel\">Engraving</span><div class=\"engravingOrder\"><b class=\"mono\" data-order=\"\">4190000005</b><span class=\"sku mono\">ONE-PENDANT</span><span class=\"mailSlot\" hidden=\"\"></span><span class=\"teamSlot\" hidden=\"\"></span></div><span class=\"purchaseLabel\">Words on the back</span><span class=\"w\">Mom</span><span class=\"dim\" data-stage=\"\"></span></div><div class=\"purchaseSummary\" data-purchase=\"\"><div class=\"purchaseType\"><span class=\"purchaseLabel\">Jewellery</span><strong>Necklace</strong></div><div class=\"purchaseChoices\"><span class=\"purchaseLabel\">Selected options</span><dl><div><dt>Metal choice</dt><dd>Gold</dd></div></dl></div></div><div class=\"egPlacementSeals\"></div></div>";
    const GOLD_DONE = "<div class=\"doneRow decidedRow hoverItem\" tabindex=\"0\" aria-expanded=\"false\" data-rid=\"4190000005\" data-key=\"4190000005_5000000050\" title=\"View engraving details\"><div class=\"comparePair\"><figure><span data-vector=\"\"></span></figure><figure><span data-listing=\"\"></span></figure></div><div class=\"engravingIdentity\"><span class=\"queueLabel\">Engraving · decided</span><div class=\"engravingOrder\"><b class=\"mono\">4190000005</b><span class=\"sku mono\">ONE-PENDANT</span></div><span class=\"purchaseLabel\">Words on the back</span><span class=\"w\">Mom</span></div><div class=\"purchaseSummary\"><div class=\"purchaseType\"><span class=\"purchaseLabel\">Jewellery</span><strong>Necklace</strong></div><div class=\"purchaseChoices\"><span class=\"purchaseLabel\">Selected options</span><dl><div><dt>Metal choice</dt><dd>Gold</dd></div></dl></div></div><div class=\"decisionActions\"><div class=\"decisionStatus\"><span class=\"ost ok\" title=\"approved — the back file is written when the sheet is\">Approved</span><span class=\"by\">Decision details not recorded</span></div><button class=\"btn ghost sm\" data-a=\"reopen\" title=\"Reopen this engraving for changes\">Reopen</button></div></div>";
    const rowsOnPage = () => [...document.querySelectorAll('#egQueue .placementRow, .egPlacementList .placementRow')];
    const showList = () => { E.restoreView({ tab: 'place', chosen: true, list: true, focus: null }); E.render(); };

    await t('2b the jobs under the rows are the per-piece jobs they were: the duck line has a Left job and a Right job, a necklace one for each disc, a single one', () => {
      assert.deepEqual(Array.from(made.duck, j => j.slot), ['L', 'R']); assert.deepEqual(Array.from(made.pair2, j => j.copies.length), [2, 2]); assert.deepEqual(Array.from(made.disc, j => j.slot), ['D1', 'D2', 'D3']);
      assert.deepEqual(Array.from(made.pend, j => j.slot || null), [null]); assert.deepEqual(Array.from(made.old, j => j.slot || null), [null], 'an old line without sides is one job');
      assert.equal(jobs.size, 2 + 2 + 2 + 3 + 1 + 1 + 1 + 1);
    });
    await t('2c Placements: ONE row for the duck line with both ears told on it, ONE row for the necklace with its three discs; a single, a plain quantity-3 line and an old pair are rows of their own', () => {
      showList();
      const rows = rowsOnPage();
      assert.equal(rows.length, 8, '13 jobs (2+2+2+3+1+1+1+1) are 8 rows: three pair lines, the necklace, four plain ones');
      const duck = rows.filter(r => r.dataset.rid === ROW.duck.order.receiptId); assert.equal(duck.length, 1, 'the duck order is ONE row, not two');
      const d = duck[0]; assert.ok(d.classList.contains('earPairRow')); assert.equal(d.querySelectorAll('.comparePair').length, 1, 'one picture block for the line');
      assert.deepEqual([...d.querySelectorAll('.egEar')].map(e => e.querySelector('.egPiece').textContent), ['LEFT EAR', 'RIGHT EAR']);
      assert.deepEqual([...d.querySelectorAll('.egEar .w')].map(n => n.textContent), ['Mom', 'Mom'], 'the words of each ear');
      assert.deepEqual([...d.querySelectorAll('.egEar [data-stage]')].map(n => n.textContent), ['Placement to check', 'Placement to check'], 'the state of each ear');
      assert.equal(d.dataset.jobs, made.duck.map(j => j.key).join(' '));
      assert.equal(d.dataset.open, made.duck[0].key, 'a click opens the Left (the first ear to settle)');
      // the line's picture is asked of the ONE ListMedia.pair, once, with the LINE's row (never an ear's side row)
      const asked = pairCalls.filter(r => r.key === ROW.duck.key); assert.equal(asked.length, 1); assert.equal(asked[0], made.duck[0].row.parentRow, 'the line row');
      const mounted = mountCalls.filter(([n]) => n === d); assert.equal(mounted.length, 1); assert.equal(mounted[0][1], made.duck[0].row.parentRow, 'mounted with the line row, so it draws Left + Right');
      // the mismatched pair and the quantity-2 pair are one row each; quantity 2 says each ear holds two pieces
      const p2 = rows.find(r => r.dataset.rid === ROW.pair2.order.receiptId); assert.deepEqual([...p2.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['LEFT EAR ×2', 'RIGHT EAR ×2']);
      assert.deepEqual([...p2.querySelectorAll('.egEar .w')].map(n => n.textContent), ['Anna', 'Ben'], 'ears with different words show both');
      assert.equal(rows.filter(r => r.dataset.rid === ROW.mis.order.receiptId).length, 1);
      // the necklace: ONE row, its three discs told on it (DISCCYCLE)
      const discs = rows.filter(r => r.dataset.rid === ROW.disc.order.receiptId); assert.equal(discs.length, 1, 'the necklace is ONE row, not three'); const nk = discs[0];
      assert.deepEqual([...nk.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3']); assert.deepEqual([...nk.querySelectorAll('.egEar .w')].map(n => n.textContent), ['A', 'B', 'C'], 'the words of each disc');
      assert.deepEqual([...nk.querySelectorAll('.egEar [data-stage]')].map(n => n.textContent), ['Placement to check', 'Placement to check', 'Placement to check']); assert.equal(nk.dataset.jobs, made.disc.map(j => j.key).join(' ')); assert.equal(nk.dataset.open, made.disc[0].key, 'a click opens Disc 1');
      assert.equal(nk.querySelectorAll('.comparePair').length, 1, 'one picture block for the line'); assert.ok(pairCalls.some(r => r === made.disc[0].row.parentRow), 'drawn from the line\'s row');
      // a single, a single earring, a plain quantity-3 line, an old pair: the row they had (one words line, one stage line, the job's own row handed to the picture)
      for (const k of ['pend', 'ear1', 'plain3', 'old']) {
        const r = rows.filter(x => x.dataset.rid === ROW[k].order.receiptId); assert.equal(r.length, 1, k); const row = r[0];
        assert.equal(row.querySelectorAll('.egEars,.egEar,.egPiece').length, 0, k + ': no ear block'); assert.equal(row.querySelectorAll('.w').length, 1); assert.equal(row.querySelectorAll('[data-stage]').length, 1);
        assert.equal(row.dataset.open, made[k][0].key); assert.equal(row.dataset.mkey, 'eg:' + made[k][0].key); assert.equal(row.querySelector('.w').textContent, 'Mom');
        assert.ok(pairCalls.includes(made[k][0].row), k + ': the picture is asked for the job\'s own row, as before');
      }
      assert.equal(document.querySelector('.egPlacementList .placementRow [data-order]').textContent.length > 0, true);
    });
    await t('2c2 a single piece\'s row is exactly what it was, byte for byte: in Placements and in Decided (the same for a necklace disc, a single earring, a plain quantity-3 line, an old pair)', () => {
      reset(); showList();
      const norm = n => n.outerHTML.replace(/\s+/g, ' ');
      const pend = rowsOnPage().find(r => r.dataset.rid === ROW.pend.order.receiptId); assert.equal(norm(pend), GOLD_PLACE);
      const was = made.pend[0].state; made.pend[0].state = 'approved'; made.pend[0].approvedBy = 'Paul'; made.pend[0].approvedAt = 1.7e12; made.pend[0].backs = [];
      E.restoreView({ tab: 'done', chosen: true, list: true, focus: null, openDone: null }); E.render();
      const dn = [...document.querySelectorAll('#egDone .decidedRow')]; assert.equal(dn.length, 1); assert.equal(norm(dn[0]), GOLD_DONE);
      made.pend[0].state = was; delete made.pend[0].approvedAt; delete made.pend[0].approvedBy; E.restoreView({ tab: 'place', chosen: true, list: true, focus: null }); E.render();
      // none of these carry the ear markup or the row's extra attributes
      for (const k of ['pend', 'ear1', 'plain3', 'old']) for (const r of rowsOnPage().filter(x => x.dataset.rid === ROW[k].order.receiptId)) { assert.equal(r.hasAttribute('data-jobs'), false, k); assert.equal(r.querySelectorAll('.egEars,.egEar').length, 0, k); }
    });
    await t('2d the tab badges count rows: Placements 8, Decided 0 (neither the ears nor the discs are counted twice), and the rail says 8 to settle', () => {
      showList();
      assert.equal(document.querySelector('.egTab[data-tab="place"] b').textContent, '8'); assert.equal(document.querySelector('.egTab[data-tab="done"] b'), null);
      assert.equal(E.pendingRows(), 8, 'rows still to settle'); assert.equal(E.pendingCount(), 13, 'jobs: what the run itself checks, unchanged');
      assert.equal(E.reviewedRows(), 0);
    });
    await t('2e a click on the duck row opens the editor on its Left ear: a Left | Right switch, the counter in rows, the line told once', () => {
      showList();
      const d = rowsOnPage().find(r => r.dataset.rid === ROW.duck.order.receiptId); d.click();
      const card = document.querySelector('#egQueue .rvItem'); assert.ok(card, 'the editor opened'); assert.equal(card.dataset.key, made.duck[0].key, 'on the Left');
      const kind = card.querySelector('.rh .kind').textContent; assert.equal(kind, '1 of 8 · 0 done', 'the counter counts rows (8), not jobs (13)');
      const sw = card.querySelector('.egEarSwitch'); assert.ok(sw, 'a Left | Right switch'); const tabs = [...sw.querySelectorAll('.egEarTab')];
      assert.deepEqual(tabs.map(b => b.querySelector('.egPiece').textContent), ['LEFT EAR', 'RIGHT EAR']); assert.deepEqual(tabs.map(b => b.getAttribute('aria-pressed')), ['true', 'false']);
      assert.deepEqual(tabs.map(b => b.querySelector('.egEarWords').textContent), ['Mom', 'Mom'], 'both ears\' words are on the switch');
      assert.equal(card.querySelectorAll('.rh .reviewIdentity > .egPiece').length, 0, 'the switch replaces the single tag');
      assert.equal(card.querySelectorAll('[data-f="words"]').length, 1, 'each ear keeps its own words box and approval: one card shown at a time');
      assert.equal(card.querySelectorAll('[data-a="approve"]').length, 1);
      const both = card.querySelector('[data-a="approveBoth"]'); assert.ok(both, 'same words: the row offers Approve both ears'); assert.equal(both.disabled, true, 'but not before both placements have been on screen');
      // the Right ear
      tabs[1].click(); const card2 = document.querySelector('#egQueue .rvItem'); assert.equal(card2.dataset.key, made.duck[1].key, 'the switch shows the Right ear\'s own card');
      assert.deepEqual([...card2.querySelectorAll('.egEarTab')].map(b => b.getAttribute('aria-pressed')), ['false', 'true']); assert.equal(card2.querySelector('.rh .kind').textContent, '1 of 8 · 0 done', 'the other ear is not another step');
    });
    await t('2e2 "Approve both ears" wakes only once both placements have been on screen: shown on the Left alone it stays asleep, and on the Right (the Left already seen) it is ready', () => {
      reset(); frames.length = 0; showList();
      const open = k => { E.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); E.render(); runFrames(); return document.querySelector('#egQueue .rvItem'); };
      let card = open(made.duck[0].key); let b = card.querySelector('[data-a="approveBoth"]');
      assert.equal(b.disabled, true, 'the Left is on screen, the Right has not been'); assert.match(b.title, /Right ear's placement first/);
      card = open(made.duck[1].key); b = card.querySelector('[data-a="approveBoth"]');
      assert.equal(b.disabled, false, 'the Left was seen, the Right is on screen now'); assert.match(b.title, /each with its own seal and back file/);
      card = open(made.duck[0].key); assert.equal(card.querySelector('[data-a="approveBoth"]').disabled, false, 'and back on the Left it is ready too');
      // words that differ: no such press at all
      made.duck[1].lines = ['Dad']; made.duck[1].text = 'Dad'; card = open(made.duck[0].key); assert.equal(card.querySelector('[data-a="approveBoth"]'), null, 'ears with different words are approved one by one');
      made.duck[1].lines = ['Mom']; made.duck[1].text = 'Mom';
      // a single never has it; a necklace with different words on its discs has no "all discs" press either, only the Disc 1 | Disc 2 | Disc 3 switch
      assert.equal(open(made.pend[0].key).querySelector('[data-a="approveBoth"]'), null); assert.equal(open(made.disc[0].key).querySelector('[data-a="approveBoth"]'), null);
      assert.equal(open(made.pend[0].key).querySelector('.egEarSwitch'), null, 'a single has no switch'); assert.equal(open(made.disc[0].key).querySelectorAll('.egEarSwitch .egEarTab').length, 3, 'the necklace has Disc 1 | Disc 2 | Disc 3');
      assert.equal(open(made.disc[0].key).querySelectorAll('.rh .reviewIdentity > .egPiece').length, 0, 'the switch replaces the single tag');
    });
    await t('2e3 the Review list\'s embedded card (no row context) counts in rows too and has no switch', () => {
      reset(); const card = E.placementCard(made.duck[0], 1);
      assert.equal(card.querySelector('.egEarSwitch'), null); assert.equal(card.querySelector('[data-a="approveBoth"]'), null); assert.match(card.querySelector('.rh .kind').textContent, /^1 of 1 · 0 done$/);
      assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'LEFT EAR', 'the single-piece tag, as before');
    });
    await t('2f Back / Next move row by row: from either ear of the duck line to the next ROW; wrap at the ends; a single is a stop of its own', () => {
      showList();
      const order = [...new Set([...jobs.values()].filter(j => ['review', 'words', 'blocked', 'classify', 'ready', 'fitting'].includes(j.state)).map(j => Rows.lineKeyOf(j) + (Rows.earOf(j) ? '' : '#' + j.key)))];   // the stops, in the order Back / Next walk
      assert.equal(order.length, 10, 'the stops: each disc is one, so the necklace is three of them');
      const open = k => { E.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); E.render(); return document.querySelector('#egQueue .rvItem'); };
      const press = (card, a) => { card.querySelector(`[data-a="${a}"]`).click(); return document.querySelector('#egQueue .rvItem').dataset.key; };
      const walk = []; let card = open(made.duck[0].key); walk.push(card.dataset.key);
      for (let i = 0; i < 9; i++) { const k = press(card, 'next'); walk.push(k); card = document.querySelector('#egQueue .rvItem'); }
      assert.equal(walk.length, 10); assert.equal(new Set(walk.map(k => Rows.lineKeyOf(jobs.get(k)) + (Rows.earOf(jobs.get(k)) ? '' : '#' + k))).size, 10, 'ten Nexts from the first row visit ten different stops');
      assert.equal(walk.filter(k => Rows.lineKeyOf(jobs.get(k)) === ROW.duck.key).length, 1, 'the duck line is ONE stop: Next never goes to its second ear');
      assert.equal(press(card, 'next'), made.duck[0].key, 'one more Next wraps to the first row (on its Left)');
      // from the Right ear: Next goes to the next row, Back to the previous
      card = open(made.duck[1].key); const nxt = press(card, 'next'); assert.notEqual(Rows.lineKeyOf(jobs.get(nxt)), ROW.duck.key);
      card = open(made.duck[1].key); const prv = press(card, 'prev'); assert.notEqual(Rows.lineKeyOf(jobs.get(prv)), ROW.duck.key);
      // a single (a pendant) is a stop of its own: Next from it moves, and Back from the next comes back to it
      card = open(made.pend[0].key); const after = press(card, 'next'); card = document.querySelector('#egQueue .rvItem'); assert.equal(press(card, 'prev'), made.pend[0].key);
      void after;
    });
    await t('2g the counter and the Decided tab follow the rows: both duck ears decided = ONE decided row; one ear decided = the line is in both tabs, each with the ear it holds', () => {
      for (const j of made.duck) { j.state = 'approved'; j.approvedBy = 'Paul'; j.approvedAt = 1.7e12; j.backs = []; }
      E.restoreView({ tab: 'done', chosen: true, list: true, focus: null }); E.render();
      const done = [...document.querySelectorAll('#egDone .decidedRow')]; assert.equal(done.length, 1, 'one Decided row for the line'); const d = done[0];
      assert.ok(d.classList.contains('earPairRow')); assert.deepEqual([...d.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['LEFT EAR', 'RIGHT EAR']);
      assert.deepEqual([...d.querySelectorAll('[data-a="reopen"]')].map(b => b.textContent), ['Reopen Left', 'Reopen Right'], 'each ear can be reopened on its own (the other keeps its approval)');
      assert.equal(document.querySelector('.egTab[data-tab="done"] b').textContent, '1'); assert.equal(document.querySelector('.egTab[data-tab="place"] b').textContent, '7');
      assert.equal(pairCalls.filter(r => r.key === ROW.duck.key).length > 0, true); assert.ok(pairCalls.some(r => r === made.duck[0].row.parentRow), 'the Decided row draws the line\'s picture too');
      E.restoreView({ tab: 'place', chosen: true, list: false, focus: made.pend[0].key }); E.render();
      assert.equal(document.querySelector('#egQueue .rvItem .kind').textContent, '2 of 8 · 1 done', '1 decided row + 7 rows still to settle');
      // one ear of the quantity-2 pair decided: it is a one-ear row in Decided, the other ear a one-ear row in Placements
      made.pair2[0].state = 'approved'; made.pair2[0].approvedBy = 'Paul'; made.pair2[0].approvedAt = 1.7e12; made.pair2[0].backs = [];
      E.restoreView({ tab: 'place', chosen: true, list: true, focus: null }); E.render();
      const rows = rowsOnPage(), p2 = rows.filter(r => r.dataset.rid === ROW.pair2.order.receiptId); assert.equal(p2.length, 1); assert.equal(p2[0].classList.contains('earPairRow'), false, 'one ear left: the row a single piece has');
      assert.equal(p2[0].dataset.open, made.pair2[1].key);
      assert.equal(document.querySelector('.egTab[data-tab="done"] b').textContent, '2'); assert.equal(document.querySelector('.egTab[data-tab="place"] b').textContent, '7');
      E.restoreView({ tab: 'done', chosen: true, list: true, focus: null }); E.render();
      assert.equal(document.querySelectorAll('#egDone .decidedRow').length, 2); assert.equal(E.reviewedRows(), 2);
      made.pair2[0].state = 'words';
    });
    await t('2h after one ear is approved the editor stays on the line: the other ear\'s card is shown, not the first of the queue', () => {
      reset();
      for (const j of made.duck) { j.state = 'review'; delete j.approvedAt; }
      E.restoreView({ tab: 'place', chosen: true, list: false, focus: made.duck[0].key }); E.render();
      assert.equal(document.querySelector('#egQueue .rvItem').dataset.key, made.duck[0].key);
      made.duck[0].state = 'approved'; made.duck[0].approvedBy = 'Paul'; made.duck[0].approvedAt = 1.7e12; made.duck[0].backs = [];
      E.render();
      const card = document.querySelector('#egQueue .rvItem'); assert.equal(card.dataset.key, made.duck[1].key, 'the Right ear of the same line follows');
      assert.equal(card.querySelector('.egEarSwitch'), null, 'one ear left to settle: the plain tag, as a single piece has');
      assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'RIGHT EAR');
      made.duck[1].state = 'approved'; E.render();
      assert.notEqual(Rows.lineKeyOf(jobs.get(document.querySelector('#egQueue .rvItem').dataset.key)), ROW.duck.key, 'both done: the first row of the queue');
    });
    await t('2i search finds a line by either ear\'s words and still shows both ears on its row', () => {
      reset(); made.duck[1].lines = ['Dad']; made.duck[1].text = 'Dad';
      E.restoreView({ tab: 'place', chosen: true, list: true, focus: null, q: 'dad' }); E.render();
      const rows = rowsOnPage(); assert.equal(rows.length, 1); assert.deepEqual([...rows[0].querySelectorAll('.egEar .w')].map(n => n.textContent), ['Mom', 'Dad']);
      E.restoreView({ tab: 'place', chosen: true, list: true, focus: null, q: '' }); made.duck[1].lines = ['Mom']; made.duck[1].text = 'Mom';
    });
    await t('2j DISCCYCLE: a necklace is ONE row; the editor has Disc 1 | Disc 2 | Disc 3, each with its own words, font and state; Back / Next walk every disc of every order; "Approve all discs" only for the same words on every disc once each was shown; Decided lists the discs with a Reopen each', () => {
      reset(); made.disc[0].font = { name: 'Typewriter', asked: 'Typewriter' }; made.disc[1].font = 'Source Sans 3'; frames.length = 0; showList();
      const nk = rowsOnPage().find(r => r.dataset.rid === ROW.disc.order.receiptId);
      assert.deepEqual([...nk.querySelectorAll('.egEar [data-font]')].map(n => n.textContent), ['Typewriter', 'Source Sans 3', ''], 'the list says each disc\'s font by name');
      nk.click(); let card = document.querySelector('#egQueue .rvItem'); assert.equal(card.dataset.key, made.disc[0].key, 'a click on the row opens Disc 1');
      assert.equal(card.querySelector('.rh .kind').textContent, '1 of 8 · 0 done', 'the counter counts the necklace as ONE order');
      let tabs = [...card.querySelectorAll('.egEarSwitch .egEarTab')]; assert.equal(tabs.length, 3);
      assert.deepEqual(tabs.map(b => b.querySelector('.egPiece').textContent), ['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3']); assert.deepEqual(tabs.map(b => b.getAttribute('aria-pressed')), ['true', 'false', 'false']);
      assert.deepEqual(tabs.map(b => b.querySelector('.egEarWords').textContent), ['A', 'B', 'C'], 'each disc\'s own words'); assert.deepEqual(tabs.map(b => (b.querySelector('.egEarFont') || {}).textContent), ['Typewriter', 'Source Sans 3', undefined], 'its own font, by name');
      assert.deepEqual(tabs.map(b => b.querySelector('.egEarStage').textContent), ['Placement to check', 'Placement to check', 'Placement to check']);
      assert.equal(card.querySelectorAll('[data-f="words"]').length, 1, 'one card at a time: each disc keeps its own words box, approval, seal and back file'); assert.equal(card.querySelector('[data-a="approveBoth"]'), null, 'the words differ: no "all discs" press');
      tabs[2].click(); card = document.querySelector('#egQueue .rvItem'); assert.equal(card.dataset.key, made.disc[2].key, 'the switch shows Disc 3\'s own card'); assert.equal(card.querySelector('[data-f="words"]').value, 'C');
      assert.deepEqual([...card.querySelectorAll('.egEarTab')].map(b => b.getAttribute('aria-pressed')), ['false', 'false', 'true']); assert.equal(card.querySelector('.rh .kind').textContent, '1 of 8 · 0 done', 'a disc is not another step');
      // Back / Next: through every disc of every order
      const press = (a) => { card.querySelector(`[data-a="${a}"]`).click(); card = document.querySelector('#egQueue .rvItem'); return card.dataset.key; };
      const open = k => { E.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); E.render(); runFrames(); card = document.querySelector('#egQueue .rvItem'); return card; };
      open(made.disc[0].key); assert.equal(press('next'), made.disc[1].key, 'Next from Disc 1 is Disc 2 of the same order'); assert.equal(press('next'), made.disc[2].key); const after = press('next'); assert.notEqual(Rows.lineKeyOf(jobs.get(after)), ROW.disc.key, 'after Disc 3: the next order');
      assert.equal(press('prev'), made.disc[2].key, 'Back from there is the last disc of the order before'); assert.equal(press('prev'), made.disc[1].key); assert.equal(press('prev'), made.disc[0].key);
      const before = press('prev'); assert.notEqual(Rows.lineKeyOf(jobs.get(before)), ROW.disc.key, 'Back from Disc 1: the order before'); assert.equal(press('next'), made.disc[0].key, 'and Next comes back to Disc 1');
      // "Approve all discs": the very same words on every disc, and every disc has been on screen
      for (const j of made.disc) { j.lines = ['Mom']; j.text = 'Mom'; } delete made.disc[0].font;
      card = open(made.disc[0].key); let b = card.querySelector('[data-a="approveBoth"]'); assert.ok(b, 'the same words: the row offers one press'); assert.equal(b.textContent, 'Approve all discs'); assert.equal(b.disabled, true, 'but Disc 2 and Disc 3 have not been on screen'); assert.match(b.title, /Look at Disc 2's placement first/);
      card = open(made.disc[1].key); b = card.querySelector('[data-a="approveBoth"]'); assert.equal(b.disabled, true); assert.match(b.title, /Look at Disc 3's placement first/);
      card = open(made.disc[2].key); b = card.querySelector('[data-a="approveBoth"]'); assert.equal(b.disabled, false, 'all three were shown'); assert.match(b.title, /each with its own seal and back file/);
      made.disc[1].lines = ['Dad']; made.disc[1].text = 'Dad'; card = open(made.disc[0].key); assert.equal(card.querySelector('[data-a="approveBoth"]'), null, 'one disc with other words: each disc is approved on its own'); made.disc[1].lines = ['Mom']; made.disc[1].text = 'Mom';
      // Decided: Disc 1 and Disc 3 approved, Disc 2 left: the order is in both tabs, each holding its discs; the Placements row of one disc reads as a single piece's
      for (const k of [0, 2]) { const j = made.disc[k]; j.state = 'approved'; j.approvedBy = 'Paul'; j.approvedAt = 1.7e12; j.backs = []; }
      E.restoreView({ tab: 'done', chosen: true, list: true, focus: null }); E.render();
      const done = [...document.querySelectorAll('#egDone .decidedRow')].filter(r => r.dataset.rid === ROW.disc.order.receiptId); assert.equal(done.length, 1, 'ONE Decided row'); const dr = done[0];
      assert.deepEqual([...dr.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['DISC 1 of 3', 'DISC 3 of 3']); assert.deepEqual([...dr.querySelectorAll('[data-a="reopen"]')].map(x => x.textContent), ['Reopen Disc 1', 'Reopen Disc 3'], 'each disc can be reopened on its own (the other discs keep their approval)');
      assert.match(dr.querySelector('[data-a="reopen"]').title, /the other discs keep theirs/);
      E.restoreView({ tab: 'place', chosen: true, list: true, focus: null }); E.render();
      const left = rowsOnPage().filter(r => r.dataset.rid === ROW.disc.order.receiptId); assert.equal(left.length, 1); assert.equal(left[0].hasAttribute('data-jobs'), false, 'one disc left: the row a single piece has'); assert.equal(left[0].dataset.open, made.disc[1].key);
      card = open(made.disc[1].key); assert.equal(card.querySelector('.egEarSwitch'), null); assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'DISC 2 of 3', 'the plain tag, as a single piece has');
      reset();
    });
    assert.equal(errors.length, 0, 'no errors were logged by the page code: ' + JSON.stringify(errors.slice(0, 2)));
    dom.window.close();
  }

  // ═══ 3 · approval: the same per-piece records as two separate presses ═══
  const approvalWorld = (opts) => {
    opts = opts || {};
    const jobsA = new Map(), saved = [], events = [], toasts = [], removed = [], moved = [];
    const pool = poolOf(W);
    const win = { CharmNestEngraveRows: Rows, CharmNestEngraveSides: Sides, CharmNestPair: Pair };
    const parent = Object.assign({}, opts.discs ? ROW.disc : ROW.duck, { engrave: undefined });
    const mkJob = (slot, lines) => { const j = Object.assign({ key: Sides.jobKey(parent.key, slot), rowKey: parent.key, slot, groupKey: '', state: 'review', lines, text: lines.join('\n'), copies: Sides.idsOfSlot(CTX, parent, slot), engraveRec: { needed: true, state: 'review', approved: false }, backs: [] }, readyFit()); j.view = { cx: 0, cy: 0, cutMembers: [] }; j.fit.glyphs = []; j.row = Sides.sideRow(CTX, parent, slot, j); return j; };
    const list = (opts.discs ? ['D1', 'D2', 'D3'].slice(0, opts.discs) : ['L', 'R']).map((sl, i) => mkJob(sl, opts.words ? opts.words[i] : ['Mom'])), [L, R] = list;
    for (const j of list) jobsA.set(j.key, j); Sides.linkParent(CTX, parent, list);
    const names = [];
    const ctx = { window: win, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, console, PT: 72 / 25.4, P: { buildBackFile: async () => ({ bytes: new Uint8Array([1]) }) }, S: { settings: {} }, B: { pool: { rows: pool } },
      charmFor: () => ({ sourceId: 'source' }), sheetFor: () => ({ sheetId: 'sheet-1' }), sourceOf: () => ({ parsed: {} }), fitOpts: () => ({ lineGap: .18 }), verifyBackFile: async () => ({ ok: true }), EG: { cardKey: null, card: null, drafts: {} },
      CNListActivity: Activity, CNEngravingSeals: Object.assign({}, Seals, { press: async () => {} }), employeeName: () => 'Paul', needEmployee: async () => { names.push('asked'); return 'Paul'; }, Review: { remove(k) { removed.push(k); } }, goes(j) { moved.push(j.key); }, EG_TAB: () => '',
      agent() {}, render() {}, saveBacks: async j => { saved.push(j.key); }, toast: m => toasts.push(m), humanAct() {}, TL: { rec: ev => events.push(ev) }, SheetEvents: { label: () => 'Sheet 1' }, SIDES: () => Sides,
      items: () => jobsA, jobsOf: () => [...jobsA.values()], fitTasks: new WeakMap(), DECIDED: ['approved', 'written', 'skipped'], isWorking: () => false, lineOf: r => r.parentRow || r,
      queuedJobs: js => js.filter(j => ['review', 'words', 'blocked', 'classify', 'ready', 'fitting'].includes(j.state)), decidedJobs: () => [...jobsA.values()].filter(j => ['approved', 'written', 'skipped'].includes(j.state)), esc: s => String(s) };
    vm.createContext(ctx);
    const rowsHelpers = source.slice(source.indexOf('  const ROWS = () =>'), source.indexOf('  // a draft outlives its card only while'));
    const approveCode = source.slice(source.indexOf('  async function prepareApproval('), source.indexOf("  /** An approval's back files are written"));
    const earCode = source.slice(source.indexOf('  const earTagText = job'), source.indexOf('  /** The row of the Engrave lists for one job'));
    const rowApprove = source.slice(source.indexOf('  /** The Left | Right switch of a row'), source.indexOf('  /** The placement review card'));
    vm.runInContext(rowsHelpers + approveCode + earCode + rowApprove + '\nthis.__api = { approve, approveRow, approveBothHtml, rowsIn, queueUniverse, markShown };', ctx);
    if (opts.shown !== false) for (const j of list) ctx.__api.markShown(j);
    return { ctx, L, R, list, parent, saved, events, toasts, moved, names, mark: j => ctx.__api.markShown(j), api: ctx.__api };
  };
  const summary = (w, list) => (list || [w.L, w.R]).map(j => ({ slot: j.slot, state: j.state, by: j.approvedBy, text: j.text, rec: { needed: j.row.engrave.needed, state: j.row.engrave.state, approved: j.row.engrave.approved, text: j.row.engrave.text, approvedBy: j.row.engrave.approvedBy }, seals: Seals.list(j).map(s => [s.how, s.by]),
    events: w.events.filter(e => e.data.poolId && j.copies.includes(e.data.poolId)).map(e => [e.type, e.id.startsWith(e.data.poolId + '-'), e.data.slot, e.data.side, e.by, e.text]) }));

  await t('3a "Approve both ears" writes exactly what two separate presses write: per ear a state, a seal, a record on its own side row, a timeline event, a back save (Left first)', async () => {
    const A = approvalWorld(); await A.api.approve(A.L, 'Paul'); await A.api.approve(A.R, 'Paul');
    const B = approvalWorld(); await B.api.approveRow(B.L, undefined);
    assert.deepEqual(summary(B), summary(A), 'the same records, seals and timeline events');
    assert.deepEqual(B.saved, A.saved); assert.deepEqual(B.saved, [B.L.key, B.R.key], 'the back files are written for the Left, then the Right, each through the ordinary save');
    assert.ok(B.L.state === 'approved' && B.R.state === 'approved'); assert.ok(summary(B)[0].events.length === 1 && summary(B)[1].events.length === 1, 'each ear has its own timeline event');
    assert.notEqual(B.L.row.engrave, B.R.row.engrave, 'two records'); assert.equal(B.parent.engrave.approved, true, 'the line reads approved only now that both ears are');
    assert.deepEqual(B.names, ['asked'].slice(0, 0), 'a name is known: nobody is asked');
  });
  await t('3b ears whose words differ are never approved together (and the editor does not even offer the press)', async () => {
    const A = approvalWorld({ words: [['Anna'], ['Ben']] });
    assert.equal(A.api.approveBothHtml([A.L, A.R]), '', 'no button for different words'); await A.api.approveRow(A.L); assert.equal(A.L.state, 'review'); assert.equal(A.R.state, 'review'); assert.match(A.toasts[0], /words differ/); assert.deepEqual(A.saved, []);
  });
  await t('3c an ear that has not been put in front of the person is never approved with the other', async () => {
    const A = approvalWorld({ shown: false }); const h = A.api.approveBothHtml([A.L, A.R]); assert.match(h, /disabled/, 'offered but greyed, with the reason'); assert.match(h, /placement first/);
    await A.api.approveRow(A.L); assert.equal(A.L.state, 'review'); assert.equal(A.R.state, 'review'); assert.match(A.toasts[0], /Left ear's placement first/);
    A.mark(A.L); await A.api.approveRow(A.L); assert.equal(A.R.state, 'review'); assert.match(A.toasts[1], /Right ear's placement first/); assert.deepEqual(A.saved, []);
    A.mark(A.R); assert.doesNotMatch(A.api.approveBothHtml([A.L, A.R]), /disabled/);
  });
  await t('3d when the Left does not settle, the Right is left as it is; a failed placement is never forced', async () => {
    const A = approvalWorld(); A.L.verify = { geometry: { ok: false } }; await A.api.approveRow(A.R); assert.equal(A.L.state, 'review'); assert.equal(A.R.state, 'review');
    const B = approvalWorld(); B.ctx.verifyBackFile = async (bytes, job) => (job === B.L ? { ok: false, why: 'engraving intersects a cut-out' } : { ok: true });
    await B.api.approveRow(B.L); assert.equal(B.L.state, 'review', 'the Left did not pass its file check'); assert.equal(B.R.state, 'review', 'so the Right was not approved either'); assert.deepEqual(B.saved, []); assert.ok(B.toasts.some(m => /Not approved/.test(m)) && B.toasts.some(m => /other ear was left as it is/.test(m)));
  });
  await t('3e a single piece, a necklace disc and a plain quantity-N line have no "both": the press does nothing and approving one is as it was', async () => {
    const A = approvalWorld(); const single = { key: 'S', state: 'review', lines: ['Mom'], copies: ['p1'], row: { key: 'S', order: { receiptId: F.rid(5) }, line: { transactionId: F.tx(50) }, spec: { designSku: 'ONE-PENDANT' }, state: 'written', engrave: { needed: true, state: 'review' } }, ...readyFit(), view: { cx: 0, cy: 0, cutMembers: [] } };
    single.fit.glyphs = []; A.ctx.items().set('S', single);
    await A.api.approveRow(single); assert.equal(single.state, 'review'); assert.match(A.toasts[0], /one ear to approve/);
    await A.api.approve(single, 'Paul'); assert.equal(single.state, 'approved', 'approving a single is the ordinary approval'); assert.deepEqual(A.saved, ['S']); assert.equal(A.L.state, 'review', 'nothing else moved');
    assert.equal(Rows.group([single], [single])[0].pair, false);
  });

  await t('3f "Approve all discs" writes exactly what three separate presses write: per disc a state, a seal, a record on its own side row, a timeline event, a back save (Disc 1 first)', async () => {
    const A = approvalWorld({ discs: 3 }); for (const j of A.list) await A.api.approve(j, 'Paul');
    const B = approvalWorld({ discs: 3 }); await B.api.approveRow(B.list[1], undefined);
    assert.deepEqual(summary(B, B.list), summary(A, A.list), 'the same records, seals and timeline events, whichever disc was on screen');
    assert.deepEqual(B.saved, A.saved); assert.deepEqual(B.saved, B.list.map(j => j.key), 'the back files are written for Disc 1, then Disc 2, then Disc 3, each through the ordinary save');
    assert.ok(B.list.every(j => j.state === 'approved')); assert.ok(summary(B, B.list).every(x => x.events.length === 1), 'each disc has its own timeline event'); assert.equal(new Set(B.list.map(j => j.row.engrave)).size, 3, 'three records');
    assert.equal(B.parent.engrave.approved, true, 'the order reads approved only now that every disc is');
    const C = approvalWorld({ discs: 2 }); await C.api.approveRow(C.L); assert.ok(C.list.every(j => j.state === 'approved'), 'two discs work the same way');
  });
  await t('3g discs whose words differ, a disc not put in front of the person, or a disc that does not settle: never approved together; the discs after a failed one are left as they are', async () => {
    const A = approvalWorld({ discs: 3, words: [['J'], ['Q'], ['J']] });
    assert.equal(A.api.approveBothHtml(A.list, A.L), '', 'no button for different words'); await A.api.approveRow(A.L); assert.ok(A.list.every(j => j.state === 'review')); assert.match(A.toasts[0], /words differ between the discs/); assert.deepEqual(A.saved, []);
    const B = approvalWorld({ discs: 3, shown: false }); const h = B.api.approveBothHtml(B.list, B.L); assert.match(h, />Approve all discs</); assert.match(h, /disabled/); assert.match(h, /placement first/);
    await B.api.approveRow(B.L); assert.ok(B.list.every(j => j.state === 'review')); assert.match(B.toasts[0], /Look at Disc 1's placement first/);
    B.list.slice(0, 2).forEach(B.mark); await B.api.approveRow(B.L); assert.match(B.toasts[1], /Look at Disc 3's placement first/); assert.deepEqual(B.saved, []); B.mark(B.list[2]); assert.doesNotMatch(B.api.approveBothHtml(B.list, B.L), /disabled/);
    const C = approvalWorld({ discs: 3 }); C.ctx.verifyBackFile = async (bytes, job) => (job === C.list[1] ? { ok: false, why: 'engraving intersects a cut-out' } : { ok: true });
    await C.api.approveRow(C.L); assert.deepEqual(C.list.map(j => j.state), ['approved', 'review', 'review'], 'Disc 1 settled, Disc 2 did not pass its file check, so Disc 3 was left as it is'); assert.deepEqual(C.saved, [C.list[0].key]); assert.ok(C.toasts.some(m => /discs after it were left as they are/.test(m)));
    // two of the three still to settle (one decided): the press says so and approves those two
    const D = approvalWorld({ discs: 3 }); D.list[0].state = 'approved'; D.mark(D.list[1]); assert.match(D.api.approveBothHtml(D.list.slice(1), D.list[1]), />Approve the 2 discs left</);
    await D.api.approveRow(D.list[1]); assert.deepEqual(D.list.map(j => j.state), ['approved', 'approved', 'approved']);
  });

  console.log(`\npair-rows-engrave: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
