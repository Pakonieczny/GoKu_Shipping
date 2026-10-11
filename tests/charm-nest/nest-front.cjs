/* Gold-filled and Sterling Silver sheets grow from the left edge as one block with a straight front, like Rose Gold (Paul, 11 Oct 2026):
 * "I don't like how the GF and SS sheets sometimes create these empty caverns [charms in the upper-left and the lower-left corner of a
 * 100 x 50 sheet with a 13 mm hole between them and the right half empty]. Please adapt both the GF and SS sheets to nest the pieces in a
 * somewhat straight vertical line front exactly like we already do with the RG 14/20 sheet."
 *
 * Cause: Gold and Silver were the one careful flow without job.block. Their grade charged 1.5 mm² for each mm the occupied part of the
 * sheet grows, so once a first charm had set the front, a second charm in the OTHER corner of the left edge (the bottom-left one) grew
 * nothing and cost the same as one under the first; the rows between the two groups were nobody's, and later charms (three orders at a time)
 * kept choosing among corners. Rose Gold's block weights (BLOCK_WEIGHTS: a mm of growth costs a whole strip of stock across the sheet, a mm²
 * of new claimed space costs 1) make every charm that fits behind the front go there first.
 *
 * Offline and deterministic: the page's own job (buildJob, sliced out of charm-nest-1.html and run in a vm: gold-block-align.cjs), the real
 * solver and the stand-in intake of pocket-fill-lib.cjs (the page's feedTurn: three orders at a time, top-up, "making room"), charms drawn
 * from the shop's library silhouettes. Nothing here touches the network, Firestore or a paid call.
 *   node tests/charm-nest/nest-front.cjs [--long]    (--long: more seeds and orders)                                                       */
'use strict';
const assert = require('node:assert/strict');
const L = require('./pocket-fill-lib.cjs'), { front } = require('./nest-front-bench.cjs');
const long = process.argv.includes('--long');
const stock = { wPt: L.PT(100), hPt: L.PT(50) }, ANGLES = Array.from({ length: 15 }, (_, i) => i * 24);   // the whole circle in 24° steps: the shape of the result, in seconds

const read = (run, S) => {
  const p = run.pages[0];
  return { ...front({ charms: p.charms, placements: p.placements }), placed: p.placements.length, fill: 1 - p.freePt2 / p.usablePt2 };
};
async function stream(metal, seed, orders, block, from) {
  const S = L.solverOf(L.REPO), list = L.makeOrders(S, orders, seed, { small: .35, mid: .40, large: .25, two: .15, grow: .9 });
  const run = await L.runStream({ metal, sheet: stock, orders: list, batch: [2, 3, 5, 4], seed: 1, block, job: { timeBudgetMs: 36e5, stallMs: 36e5, angles: ANGLES }, ...(from ? { start: from(S) } : {}) });
  assert(run.pages.every(p => !p.placements.length || p.verification.ok), 'every sheet verified');
  return run;
}

(async () => {
  /* 1. A fresh sheet fed by the day's trickle (2 to 5 orders an update, taken three at a time): at about a fifth to a third full the front is
        straight and there is no cavern behind it, where without the block weights a second corner opens one. */
  const seeds = long ? [1, 2, 3, 4, 5, 6] : [1, 2], N = long ? 20 : 10, rows = { plain: [], block: [] };
  for (const metal of ['gold', 'silver']) for (const seed of seeds.slice(0, metal === 'gold' ? seeds.length : 1)) {
    for (const block of [false, true]) {
      const r = read(await stream(metal, seed, N, block), null);
      rows[block ? 'block' : 'plain'].push({ metal, seed, ...r });
    }
  }
  const mean = (a, k) => a.reduce((s, r) => s + r[k], 0) / a.length;
  const sum = k => ({ plain: +mean(rows.plain, k).toFixed(2), block: +mean(rows.block, k).toFixed(2) });
  const std = sum('frontStd'), cav = sum('cavernMm'), bf = sum('blockFill');
  console.log('front std (mm)', std, '| widest empty disc behind the front (mm)', cav, '| fill behind the front', bf);
  assert(std.block < std.plain * .75, `the front is clearly straighter (${std.block} vs ${std.plain} mm)`);
  assert(cav.block < cav.plain * .85, `the cavern is smaller (${cav.block} vs ${cav.plain} mm)`);
  assert(bf.block > bf.plain, 'the block is denser behind its front');
  for (const r of rows.block) assert(r.cavernMm < 14, `${r.metal} seed ${r.seed}: no cavern behind the front (${r.cavernMm} mm)`);

  /* 2. A sheet already saved with a cavern (nest-front-cavern.json: 29 charms of the first batches of seed 1 as the old grade left them, a
        26 mm hole across the left half of its middle rows, the right of the sheet empty) is never rebuilt: its charms stay put, and the
        next orders fill the cavern first instead of growing the front. */
  {
    const fx = require('./nest-front-cavern.json'), S = L.solverOf(L.REPO), list = L.makeOrders(S, 37, fx.seed, { small: .35, mid: .40, large: .25, two: .15, grow: fx.grow });
    const byOrder = new Map(list.map(o => [o.id, o])), done = new Set(fx.orders), rest = list.filter(o => !done.has(o.id)), charms = fx.orders.flatMap(id => byOrder.get(id).pieces);
    const after = {};
    for (const block of [false, true]) {
      const run = await L.runStream({ metal: 'gold', sheet: stock, orders: rest, batch: [2, 3, 5, 4], seed: 1, block, job: { timeBudgetMs: 36e5, stallMs: 36e5, angles: ANGLES }, start: { charms, placements: fx.placements } });
      const p = run.pages[0], now = new Map(p.placements.map(q => [q.id, q]));
      assert(p.verification.ok, 'the sheet verified');
      for (const q of fx.placements) assert.deepEqual([now.get(q.id).cxPt, now.get(q.id).cyPt, now.get(q.id).angle], [q.cxPt, q.cyPt, q.angle], 'a saved charm never moves');
      after[block ? 'block' : 'plain'] = { ...front(p), placed: p.placements.length };
    }
    console.log('saved sheet with a cavern, after the next orders: plain', JSON.stringify(after.plain), '| block', JSON.stringify(after.block));
    assert(after.block.frontStd < after.plain.frontStd * .5, `the front straightens (${after.block.frontStd} vs ${after.plain.frontStd} mm)`);
    assert(after.block.cavernMm < after.plain.cavernMm * .75, `the cavern is filled (${after.block.cavernMm} vs ${after.plain.cavernMm} mm)`);
    assert(after.block.placed >= after.plain.placed, 'as many charms placed');
  }

  console.log('Nest front OK: Gold-filled and Sterling Silver fill the sheet as one block with a straight front (as Rose Gold does), with no cavern behind it; a saved sheet with a cavern keeps its charms and the next orders fill the cavern first');
})().catch(e => { console.error(e); process.exitCode = 1; });
