/* STR1 (Paul, 11 Nov 2026): a Rose Gold sheet "doesn't follow the rule of keeping things straight, meaning that as the new parts are added,
 * they should be added as straight as practical while keeping things efficient as the pieces get added" (an RG 14/20 sheet cut once on 9 Nov:
 * its green line a staircase, the new charms out along the far edge).
 *
 * Cause (charm-nest-solver.js, carefulAppend's reachOf): the block's front, past which each mm of growth costs a whole strip of stock
 * (BLOCK_WEIGHTS), was read from every occupied cell of the sheet. The corner keep-outs (CRN1, 9 Nov) reach past the inset, so the far
 * corners read as a charm at the sheet's far end: the front was the whole sheet from the first charm and growth cost nothing. And on a cut
 * sheet the stock already cut away read as charms, so every bay of a ragged old line was "behind the front" at no cost. The front is now
 * the farthest charm, never the inset, a corner keep-out or cut stock.
 *
 * nest-front.cjs and the other suites build their jobs without cornerPt; this one sends it as the page does (buildJob: 1.5 mm).
 * Offline and deterministic: the real solver and the stand-in intake of pocket-fill-lib.cjs, charms from the shop's library silhouettes.
 * Nothing here touches the network, Firestore or a paid call.   node tests/charm-nest/nest-front-corners.cjs                          */
'use strict';
const assert = require('node:assert/strict');
const L = require('./pocket-fill-lib.cjs'), { front } = require('./nest-front-lib.cjs');
const wPt = L.PT(100), hPt = L.PT(50), CORNER = L.PT(1.5), ANGLES = Array.from({ length: 15 }, (_, i) => i * 24);
const MIX = { small: .35, mid: .40, large: .25, two: .15, grow: .9 };
const mean = a => a.reduce((s, x) => s + x, 0) / a.length;

/** One stream of orders onto one Rose Gold sheet: { wPt, hPt, remnant? }, with or without the page's corner keep-out. */
async function stream(seed, n, corner, remnant) {
  const S = L.solverOf(L.REPO), orders = L.makeOrders(S, n, seed, MIX);
  const sheet = { wPt, hPt, insetPt: 1.5, ...(corner ? { cornerPt: CORNER } : {}), ...(remnant ? { remnant } : {}) };
  const run = await L.runStream({ metal: 'rose', sheet: { wPt, hPt, ...(remnant ? { remnant } : {}) }, orders, batch: remnant ? [n] : [2, 3, 5, 4], seed: 1, job: { timeBudgetMs: 36e5, stallMs: 36e5, angles: ANGLES, sheet } });
  const p = run.pages[0];
  assert(p.verification.ok, 'the sheet verified');
  return { S, p, wanted: orders.reduce((a, o) => a + o.pieces.length, 0) };
}

/** The next green line of a cut sheet, row by row: the old line or the farthest new charm, whichever is farther (mm). */
function nextLine(S, p, remnant) {
  const FH = Math.round(hPt * 2), far = new Float64Array(FH), byId = new Map(p.charms.map(c => [c.id, c]));
  for (let y = 0; y < FH; y++) far[y] = remnant.values[Math.min(remnant.values.length - 1, Math.floor(y / 2 / remnant.step))] * 2;
  for (const q of p.placements) {
    const v = S.prepareVariant(byId.get(q.id), q.angle, -.5, 2), x0 = Math.round(q.cxPt * 2 - v.solid.cx), y0 = Math.round(q.cyPt * 2 - v.solid.cy);
    for (let r = 0; r < v.fine.h; r++) for (let c = 0; c < v.fine.w; c++) if (v.fine.bits[r * v.fine.w + c]) { const y = y0 + r; if (y >= 0 && y < FH && x0 + c + 1 > far[y]) far[y] = x0 + c + 1; }
  }
  const rows = Array.from(far).slice(3, FH - 3).map(c => c * L.MM / 2), m = mean(rows);
  return { std: Math.sqrt(mean(rows.map(x => (x - m) ** 2))), max: Math.max(...rows) };
}

(async () => {
  /* 1. A fresh sheet with the page's corners keeps the straight front it has without them (on main: 13.6 and 6.1 mm against 5.6 and 3.0). */
  const plain = [], corners = [];
  for (const seed of [1, 2]) {
    plain.push(front((await stream(seed, 10, false)).p));
    corners.push(front((await stream(seed, 10, true)).p));
  }
  const std = { plain: +mean(plain.map(r => r.frontStd)).toFixed(2), corners: +mean(corners.map(r => r.frontStd)).toFixed(2) };
  const cav = { plain: +mean(plain.map(r => r.cavernMm)).toFixed(1), corners: +mean(corners.map(r => r.cavernMm)).toFixed(1) };
  console.log('fresh sheet: front std (mm)', std, '| widest empty disc behind the front (mm)', cav);
  assert(std.corners <= std.plain * 1.2 + .5, `the corners leave the front straight (${std.corners} vs ${std.plain} mm without them)`);
  assert(cav.corners <= cav.plain + 2, `no cavern opens behind it (${cav.corners} vs ${cav.plain} mm)`);
  for (const r of corners) assert(r.cavernMm < 14, `no cavern behind the front (${r.cavernMm} mm)`);

  /* 2. A sheet cut once along a ragged staircase (bays 35 mm deep beside 84 to 88 mm steps, like the 9 Nov sheet): the next orders fill the
        bays and the next green line comes out straighter (on main: 9.5 and 12.1 mm). */
  const remnant = L.leftover(wPt, hPt, [84, 53, 53, 53, 67, 76, 88, 88, 54, 54]), old = nextLine(L.solverOf(L.REPO), { charms: [], placements: [] }, remnant), lines = [];
  for (const seed of [2, 3]) {
    const { S, p, wanted } = await stream(seed, 8, true, remnant);
    assert.equal(p.placements.length, wanted, 'every charm placed');
    lines.push(nextLine(S, p, remnant));
  }
  console.log('cut sheet: old line std', +old.std.toFixed(2), 'mm | next line std', lines.map(l => +l.std.toFixed(2)), 'mm');
  assert(mean(lines.map(l => l.std)) < 8.5, `the next line is straight as practical (${lines.map(l => l.std.toFixed(2))} mm)`);
  for (const l of lines) assert(l.std < old.std * .7, `straighter than the old line (${l.std.toFixed(2)} vs ${old.std.toFixed(2)} mm)`);

  console.log('Nest front with corners OK: the corner keep-outs and the stock already cut away never count as the block\'s front; a fresh sheet stays straight, a cut sheet\'s bays fill first');
})().catch(e => { console.error(e); process.exitCode = 1; });
