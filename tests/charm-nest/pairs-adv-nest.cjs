/* ADVNEST (pairs-1009, phase 2): the nester and the mirrored geometry attacked with real shapes. Offline: no browser, no network.
 *   node tests/charm-nest/pairs-adv-nest.cjs
 * 5 · the placement oracle on the REAL solver's output: a sheet charm of a pair carries `shapeJson`, its laid outline read from the bits the
 *     nester really placed (CharmNestSolver.laidOutline), so the oracle's reflection checks (reflected, not-mirror, shape-mismatch) see what the nester did
 *     and not what the master says; a Right that reached the solver with the Left's bits (the failure PAIRNEST M5 cannot see) is caught.
 * The real-file runs (50+ designs of the master, the Sep 17 sandbox batches) are scratch scripts, their numbers are in
 * plans/pairs-1009/ADVNEST-findings.md; this file keeps the cases that need no master file.                                                   */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Geom = require(path.join(root, 'charm-nest-geom.js'));
const S = require(path.join(root, 'charm-nest-solver.js'));
const pass = name => console.log('  ok', name);
const MM = 25.4 / 72;

/** A design's piece as the nester gets it: the (mirrored) body's own silhouette bits (what Pool.ensureBase builds), the pair fields the page spreads. */
function jobPiece(world, p, id, order, orderDate, bitsFrom) {
  const charm = F.charmOf(p.sku), geo = Pair.pieceGeometry(charm, { side: p.side, bodyIndex: p.bodyIndex, mirror: !!p.mirror });
  const own = Geom.silhouetteBits(bitsFrom || geo, 6, {});
  const c = { id, w: own.w, h: own.h, scale: own.scale, bits: own.bits, areaPt2: own.areaPt2, order, orderDate, pinned: null, hold: false, side: p.side, mirror: !!p.mirror, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.groupSize };
  return Object.assign({}, c, S.pairFields(c), { id, w: own.w, h: own.h, scale: own.scale, bits: own.bits, areaPt2: own.areaPt2, order, orderDate, pinned: null, hold: false });
}
const sheet = { wPt: 100 / MM, hPt: 50 / MM, insetPt: 1.5 };
const baseJob = { sheet, angles: Array.from({ length: 36 }, (_, i) => i * 10), fineRes: 2, coarseRes: .5, maxFill: .8, timeBudgetMs: 60000, maxTrials: 30, clearancePt: -.5, seed: 3, careful: true };

(async () => {
  /* ── 5 · the oracle on real solver output ── */
  for (const [kind, sku] of [['pair', 'PAIR-FACE-L'], ['pair', 'PAIR-FACE-R'], ['mismatched', 'MITTENS-MIS'], ['mismatched', 'TENNIS-MIS']]) {
    const world = F.world({ orders: [{ rid: F.ids.rid(1), lines: [{ n: 10, kind, sku, qty: 1, on: 'sh-gf1' }] }] });
    const sh = world.sheets[0], pieces = sh.charms.map((c, i) => ({ c, p: world.pieces.find(x => x.poolId === c.poolId) }));
    const jp = pieces.map(({ c, p }) => jobPiece(world, p, c.id, p.orderId, 1));
    const job = { ...baseJob, pieces: jp };
    const r = await S.solve(job, {});
    assert.equal(r.placements.length, 2, sku + ': both pieces placed');
    assert.equal(typeof S.laidOutline, 'function', 'the solver gives a sheet charm its laid outline (CharmNestSolver.laidOutline)');
    // every angle the nester used, plus turns of our own: the laid outline of each placed piece is written into the sheet charm
    for (const turn of [0, 90, 180, 270, 37]) {
      const w2 = F.clone(world), s2 = w2.sheets[0];
      for (const pl of r.placements) {
        const piece = jp.find(x => x.id === pl.id), place = { ...pl, angle: ((pl.angle + turn) % 360) };
        // re-solving every turn would be slow: the outline is read from the bits at the placement's angle, which is all the oracle needs
        const polys = S.laidOutline(piece, place, { sheetHPt: sheet.hPt });
        assert(Array.isArray(polys) && polys.length >= 1 && polys[0].length >= 8, 'a polygon of at least 8 points');
        const text = JSON.stringify(polys);
        assert(!/\[\[\[/.test(JSON.stringify({ shapeJson: text })) || typeof text === 'string', 'stored as a string');
        s2.charms.find(c => c.id === pl.id).shapeJson = text;
        s2.placements.find(x => x.id === pl.id).angle = place.angle;
      }
      const found = F.problems(w2, { tracked: [] });
      assert.deepEqual(found.map(x => x.code), [], `${sku} turned by ${turn}: the oracle finds nothing wrong in the nester's own outlines (${found.map(x => x.code + ' ' + x.text).join('; ')})`);
    }
    // a Right that reached the solver with the LEFT's bits (not mirrored): the nester lays it as drawn while the record says mirrored
    if (kind === 'pair') {
      const bad = pieces.map(({ c, p }) => jobPiece(world, p, c.id, p.orderId, 1, p.mirror ? Pair.pieceGeometry(F.charmOf(p.sku), { bodyIndex: p.bodyIndex, mirror: false }) : undefined));
      const w3 = F.clone(world), s3 = w3.sheets[0];
      for (const pl of r.placements) { const piece = bad.find(x => x.id === pl.id); s3.charms.find(c => c.id === pl.id).shapeJson = JSON.stringify(S.laidOutline(piece, pl, { sheetHPt: sheet.hPt })); }
      const sym = F.problems(F.clone(world)).length === 0 && ['PAIR-STUD', 'PAIR-SET-R'].includes(sku);
      const codes = F.problems(w3, { tracked: [] }).map(x => x.code);
      if (!sym) assert(codes.includes('reflected') || codes.includes('not-mirror'), sku + ': a Right nested with the Left\'s bits is found: ' + codes.join(','));
    }
  }
  pass('5 the oracle reads the nester\'s own laid outlines: nothing wrong at any turn, a Right with the Left\'s bits is found');
})().catch(e => { console.error(e); process.exit(1); });
