/* Pocket fill (Paul, 7 Oct 2026): "The nesting process did not do a good job of filling this sheet before it moved onto the next
 * sheet. It needs to be more methodical to fit in the small charms into tight spaces."
 *
 * The cause, in the solver (charm-nest-solver.js, the end of pass(), and keepOrdersWhole in charm-nest-1.html): the oldest order
 * that misses a sheet lifts every younger order off with it ("a sheet holds no order younger than one it leaves out"), a small
 * charm that fitted a pocket included, and the sheet closes with the pocket open. The pocket pass tries every order turned away
 * or lifted once more against the layout that stands, oldest first, whole orders only, nothing seated moving, then again at the
 * half steps between the 2° angles.
 *
 * Offline and deterministic. The first blocks build the pockets by hand, one runs the page's own rules on a lifted layout, the
 * last feeds a seeded stream of library charms the way the sorter does and scans each closed sheet exhaustively (every
 * position, every angle, the one collision test). Nothing here touches the network, Firestore or a paid call.             */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const L = require('./pocket-fill-lib.cjs'), S = L.solverOf(L.REPO), K = 4;

const piece = (id, w, h, date = 0, order) => ({ id, w: w * K, h: h * K, scale: K, bits: new Uint8Array(Math.round(w * K) * Math.round(h * K)).fill(1), areaPt2: w * h, order: order || id, orderDate: date });
/** A bar drawn already turned by `deg` (a charm that lies slanted in its own drawing). */
function slanted(id, len, th, deg, date) {
  const W0 = Math.round(len * K), H0 = Math.round(th * K), r = S.rotateBitmap(new Uint8Array(W0 * H0).fill(1), W0, H0, deg); let n = 0; for (const v of r.bits) n += v;
  return { id, w: r.w, h: r.h, scale: K, bits: r.bits, areaPt2: n / (K * K), widthPt: r.w / K, heightPt: r.h / K, order: id, orderDate: date || 0 };
}
const same = (a, b) => a.id === b.id && a.angle === b.angle && a.cxPt === b.cxPt && a.cyPt === b.cyPt;

(async () => {
  const base = { sheet: { wPt: 60, hPt: 40, insetPt: 1 }, angles: [0, 90], fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 20000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true };
  // two locked blocks leave a 9 × 9 pt hole at the top left and a corridor along them
  const blocks = [['r1', 10, 1, 56, 39], ['r2', 1, 10, 10, 39]].map(([id, x0, y0, x1, y1]) => ({ p: piece(id, x1 - x0, y1 - y0), at: { id, cxPt: (x0 + x1) / 2, cyPt: (y0 + y1) / 2, angle: 0 } }));
  const locked = blocks.map(b => b.at), fixed = blocks.map(b => b.p);

  /* 1. The older order that fits nowhere no longer takes the younger small charm that fits the hole with it. */
  {
    const big = piece('BIG', 30, 30, 1), small = piece('SMALL', 7, 7, 2), job = { ...base, pieces: [...fixed, big, small], lockedPlacements: locked };
    const r = await S.solve(job, {}), ids = r.placements.map(p => p.id).sort();
    assert.deepEqual(ids, ['SMALL', 'r1', 'r2'], 'the small, younger charm fits the hole and stays: ' + ids);
    assert.deepEqual(r.rejects, ['BIG'], 'the older order that fits nowhere is the one turned away');
    assert.deepEqual(r.pocketFilled, ['SMALL'], 'the page is told which order the pocket pass seated');
    assert(S.verify(job, r.placements, 4).ok, 'no overlap');
    for (const l of locked) assert(same(r.placements.find(p => p.id === l.id), l), 'a seated charm never moves');
    const s = r.placements.find(p => p.id === 'SMALL');
    assert(s.cxPt < 11 && s.cyPt < 11, 'in the hole: ' + JSON.stringify(s));
    // the old behaviour, still there with the pass off (this is what closed the sheet with the hole open)
    const old = await S.solve({ ...job, pocketFill: false }, {});
    assert.deepEqual(old.rejects.slice().sort(), ['BIG', 'SMALL'], 'without the pass the small charm leaves with the older order');
    assert(!old.pocketFilled.length && old.careful.pocket.ms === 0, 'and the pass costs nothing when it is off');
  }

  /* 2. Whole orders only: a two-piece order that fits the hole with one piece is not split. */
  {
    const big = piece('BIG', 30, 30, 1), a = piece('PAIR.a', 7, 7, 2, 'PAIR'), b = piece('PAIR.b', 7, 7, 2, 'PAIR'), one = piece('ONE', 7, 7, 3);
    const job = { ...base, pieces: [...fixed, big, a, b, one], lockedPlacements: locked };
    const r = await S.solve(job, {}), ids = new Set(r.placements.map(p => p.id));
    assert(!ids.has('PAIR.a') && !ids.has('PAIR.b'), 'half of a two-piece order is never seated: ' + [...ids]);
    assert(ids.has('ONE'), 'the single-piece order behind it takes the hole');
    assert(!r.pocketFilled.includes('PAIR.a') && !r.pocketFilled.includes('PAIR.b'));
    assert(S.verify(job, r.placements, 4).ok);
    for (const l of locked) assert(same(r.placements.find(p => p.id === l.id), l));
  }

  /* 3. Oldest first among the orders that fit the one hole. */
  {
    const big = piece('BIG', 30, 30, 1), s1 = piece('S1', 7, 7, 2), s2 = piece('S2', 7, 7, 3);
    for (const order of [[s1, s2], [s2, s1]]) {
      const job = { ...base, pieces: [...fixed, big, ...order], lockedPlacements: locked };
      const r = await S.solve(job, {});
      assert.deepEqual(r.pocketFilled, ['S1'], 'the older of the two takes the hole whatever order they are listed in: ' + r.pocketFilled);
      assert(r.rejects.includes('S2') && r.rejects.includes('BIG'));
    }
  }

  /* 4. A tight slot that takes a bar only at an angle the 2° steps skip: the half-step stage finds it. */
  {
    const W = 100, Ht = 30, inset = 1, y0 = 9.5, H = 4.8, top = piece('top', W - 2 * inset, y0 - inset, 1), bot = piece('bot', W - 2 * inset, Ht - inset - (y0 + H), 2);
    const lock = [{ id: 'top', cxPt: W / 2, cyPt: inset + (y0 - inset) / 2, angle: 0 }, { id: 'bot', cxPt: W / 2, cyPt: y0 + H + (Ht - inset - (y0 + H)) / 2, angle: 0 }];
    const bar = slanted('bar', 70, 3, 31, 3), angles = Array.from({ length: 180 }, (_, i) => i * 2);
    const job = { sheet: { wPt: W, hPt: Ht, insetPt: inset }, angles, fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 60000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true, pieces: [top, bot, bar], lockedPlacements: lock };
    // the check does not use the solver's search: every position of the sheet with the two blocks on it
    const grid = S.makeSheetGrid({ ...job.sheet, fixedPieces: lock.map((p, i) => ({ piece: i ? bot : top, placement: p })) }, 0, 2);
    assert(!L.fitsAnywhere(S, grid, bar, 0, 2), 'no 2° angle of the bar fits the slot');
    const odd = L.fitsAnywhere(S, grid, bar, 0, 1);
    assert(odd && odd.angle % 2 === 1, 'a 1° angle does: ' + JSON.stringify(odd));
    const off = await S.solve({ ...job, pocketFill: false }, {});
    assert.deepEqual(off.rejects, ['bar'], 'without the pass the bar goes to the next sheet');
    const r = await S.solve(job, {});
    assert.deepEqual(r.rejects, [], 'with it the bar is seated: ' + JSON.stringify(r.placements.map(p => [p.id, p.angle])));
    assert(r.placements.find(p => p.id === 'bar').angle % 2 === 1, 'at a half step');
    assert(S.verify(job, r.placements, 4).ok);
    for (const l of lock) assert(same(r.placements.find(p => p.id === l.id), l));
  }

  /* 5. Cheap and bounded: it runs only when an order was turned away, adds nothing otherwise, and a spent budget ends it
        without losing what the search placed. */
  {
    const fits = piece('FITS', 7, 7, 2), job = { ...base, pieces: [...fixed, fits], lockedPlacements: locked };
    const r = await S.solve(job, {});
    assert.deepEqual(r.rejects, [], 'everything fits'); assert.deepEqual(r.pocketFilled, []); assert.equal(r.careful.pocket.ms, 0, 'no order turned away, no pocket pass');
    // a crowded sheet: 24 rectangles of 5 to 18 pt, each its own order, the youngest last
    const rnd = L.rng(5), many = Array.from({ length: 24 }, (_, i) => piece('m' + i, 5 + Math.floor(rnd() * 14), 5 + Math.floor(rnd() * 14), 1 + i));
    const j2 = { ...base, maxFill: .8, timeBudgetMs: 60000, pieces: many };
    const off = await S.solve({ ...j2, pocketFill: false }, {}), on = await S.solve(j2, {}), spent = await S.solve({ ...j2, pocketMs: 1 }, {});
    assert(off.rejects.length > 0, 'the crowded sheet turns orders away');
    assert(on.rejects.length <= off.rejects.length, 'and the pass turns none more away');
    for (const p of off.placements) assert(on.placements.some(q => same(p, q)), 'the pocket pass moves nothing the search seated: ' + p.id);
    assert(on.placements.length >= off.placements.length && on.density >= off.density - 1e-9, `never fewer charms (${off.placements.length} → ${on.placements.length})`);
    assert(S.verify(j2, on.placements, 4).ok && S.verify(j2, spent.placements, 4).ok, 'no overlap, with or without the budget');
    for (const p of off.placements) assert(spent.placements.some(q => same(p, q)), 'a spent budget keeps what the search placed');
    assert(on.careful.pocket.ms < 15000 + 5000, 'the pass stays inside its budget: ' + on.careful.pocket.ms + ' ms');
    for (const id of on.pocketFilled) assert(on.placements.some(p => p.id === id) && !off.placements.some(p => p.id === id), 'pocketFilled lists only charms the pass seated');
  }

  /* 6. The page keeps what the pocket pass seated (keepOrdersWhole runs as the page has it). */
  {
    const ctx = L.pageRules(L.REPO, { pages: [], work: [], overflow: () => null });
    const mk = rejects => {
      const charms = [['A', 1], ['B', 2], ['C', 3], ['D', 4]].map(([id, d]) => ({ id, name: id, order: id, orderDate: d }));
      return { sh: { metal: 'gold14k', rejects: rejects.slice(), placements: ['B', 'C', 'D'].map(id => ({ id })) }, byId: new Map(charms.map(c => [c.id, c])) };
    };
    let t = mk(['A']); ctx.keepOrdersWhole(t.sh, t.byId);
    assert.deepEqual(t.sh.placements, [], 'without the pocket pass the younger orders leave with the older one that missed');
    t = mk(['A']); ctx.keepOrdersWhole(t.sh, t.byId, new Set(['C']));
    assert.deepEqual(t.sh.placements.map(p => p.id), ['C'], 'an order the pocket pass seated stays: ' + JSON.stringify(t.sh.placements));
    assert.deepEqual(t.sh.rejects.slice().sort(), ['A', 'B', 'D'], 'the others still go on in order');
    t = mk(['A', 'C']); ctx.keepOrdersWhole(t.sh, t.byId, new Set(['C']));
    assert(!t.sh.placements.some(p => p.id === 'C'), 'an order that is itself turned away is never kept');
    const page = fs.readFileSync(path.join(L.REPO, 'charm-nest-1.html'), 'utf8');
    assert(/keepOrdersWhole\(sh, byId, new Set\(result\.pocketFilled \|\| \[\]\)\)/.test(page), 'finishNest hands the solver\'s list to it');
  }

  /* 7. Client-side only: no network, Firestore or function call in the pass. */
  {
    const src = fs.readFileSync(path.join(L.REPO, 'charm-nest-solver.js'), 'utf8'), a = src.indexOf('Pocket pass (Paul'), b = src.indexOf('Room for later orders (Paul');
    assert(a > 0 && b > a, 'the pocket pass block is where this test looks');
    assert(!/fetch\s*\(|XMLHttpRequest|firestore|\/\.netlify|postMessage|localStorage/i.test(src.slice(a, b)), 'the pass reads and writes nothing outside the solver');
  }

  /* 8. The sorter's own flow (the page's feedTurn, keepOrdersWhole, sheetFull and feedOn around the real solver): three orders come
        in one update, the middle one is too big for what the first leaves, the youngest is a small charm. Before, the small one
        moved on to the next sheet with the big one and the first sheet closed with its pocket open (the 7 Oct screenshot). */
  {
    const mk = (id, side, date) => ({ id, kind: 'x', pieces: [L.square(id, side, id, date)] }), stock = { wPt: 80, hPt: 50 };
    const layout = run => run.pages.map(p => `${p.page}:${p.placements.map(q => q.id).sort().join('+')}`).join(' ');
    const orders = () => [mk('o0', 34, 1001), mk('o1', 46, 1002), mk('o2', 8, 1003)];
    const off = await L.runStream({ metal: 'gold14k', sheet: stock, orders: orders(), batch: 3, seed: 1, job: { pocketFill: false } });
    assert.equal(layout(off), '1:o0 2:o1+o2', 'the old flow: the small charm leaves with the big order that missed');
    const mOff = L.measure(off, stock, { cut: false });
    assert.deepEqual(mOff.leftOut.map(x => x.id), ['o2'], 'and the exhaustive scan finds it fitting the closed sheet');
    const on = await L.runStream({ metal: 'gold14k', sheet: stock, orders: orders(), batch: 3, seed: 1 });
    assert.equal(layout(on), '1:o0+o2 2:o1', 'the small charm fills the pocket of the first sheet, the big order starts the next');
    assert(on.closed(on.pages[0]), 'the first sheet still closes at the miss, as before');
    const mOn = L.measure(on, stock, { cut: false });
    assert.deepEqual(mOn.leftOut, [], 'nothing left out of the closed sheet fits it');
    assert(on.pages.every(p => p.verification.ok), 'every sheet verified');
  }

  /* 9. A seeded mixed stream of library charms (small, middle and large, a few two-piece orders) on a 50 × 46 mm 14K leftover
        runs through the same flow: every sheet verifies, and the pass takes only a few seconds a sheet. */
  {
    const sc = { wPt: L.PT(50), hPt: L.PT(46) }, stock = { ...sc, remnant: L.leftover(sc.wPt, sc.hPt, L.SCREENSHOT_STEPS) };
    const stream = L.makeOrders(S, 4, 11, { small: .45, mid: .40, large: .15, two: .15, grow: 1 });
    const run = await L.runStream({ metal: 'gold14k', sheet: stock, orders: stream, batch: 3, seed: 1 });
    assert(run.pages.every(p => !p.placements.length || p.verification.ok), 'every sheet verified');
    assert.equal(run.pages.reduce((n, p) => n + p.placements.length, 0), stream.reduce((n, o) => n + o.pieces.length, 0), 'every charm is on a sheet');
    for (const o of stream) assert.equal(new Set(run.pages.filter(p => p.placements.some(q => o.pieces.some(c => c.id === q.id))).map(p => p.page)).size, 1, 'order ' + o.id + ' is on one sheet');
    const worst = Math.max(0, ...run.searches.map(s => s.pocket ? s.pocket.ms : 0));
    assert(worst < 15000 + 3000, 'the pocket pass stays inside its budget on every search: ' + worst + ' ms');
  }

  console.log('Pocket fill OK: a younger small charm keeps the hole an older order missed, whole orders only (a two-piece order is never split), oldest first, a slot only a half-step angle fits, seated charms never move, off and spent budgets change nothing, nothing outside the solver is called, the page keeps pocket-filled orders, and the flow of the sorter keeps a small charm on the sheet whose pocket it fits and a mixed 14K leftover stream still verifies');
})().catch(e => { console.error(e); process.exitCode = 1; });
