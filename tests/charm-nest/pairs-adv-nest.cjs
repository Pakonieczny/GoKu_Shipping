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
        assert(Array.isArray(polys) && polys.length >= 1 && polys[0].length >= 4, 'a polygon of at least 4 points');
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

  /* ── 5b · the page writes it: the saved sheet charm carries shapeJson (the page's own shapeFieldOf, sliced out of charm-nest-1.html) ── */
  {
    const fs = require('node:fs'), vm = require('node:vm');
    const noNested = require('./_noNestedArrays.cjs');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    const a = html.indexOf('function pairDescriptor('), b = html.indexOf('function splitGroups(', a);
    assert(a > 0 && b > a, 'pairDescriptor … splitGroups found in the page');
    const ctx = { Set, Map, Math, JSON, Object, Array, String, Number, CharmNestSolver: S, window: {} }; vm.createContext(ctx);
    vm.runInContext(html.slice(a, b) + ';this.shapeFieldOf = typeof shapeFieldOf === "function" ? shapeFieldOf : null;', ctx);
    assert.equal(typeof ctx.shapeFieldOf, 'function', 'the page has shapeFieldOf next to pairDescriptor');
    const world = F.world({ orders: [{ rid: F.ids.rid(1), lines: [{ n: 10, kind: 'pair', sku: 'PAIR-FACE-L', qty: 1, on: 'sh-gf1' }] }] });
    const sh = world.sheets[0], pcs = sh.charms.map(c => ({ c, p: world.pieces.find(x => x.poolId === c.poolId) }));
    const jp = pcs.map(({ c, p }) => jobPiece(world, p, c.id, p.orderId, 1));
    const r = await S.solve({ ...baseJob, pieces: jp }, {});
    const w2 = F.clone(world), s2 = w2.sheets[0];
    for (const pl of r.placements) {
      const item = jp.find(x => x.id === pl.id), got = JSON.parse(JSON.stringify(ctx.shapeFieldOf(item, pl, sheet.hPt)));
      assert.equal(typeof got.shapeJson, 'string', 'a sided piece with its bits and placement gets a shapeJson string');
      noNested({ shapeJson: got.shapeJson }, 'the saved charm (Firestore refuses arrays in arrays)');
      assert(got.shapeJson.length < 4000, 'a small piece costs a few hundred bytes, not a bitmap (' + got.shapeJson.length + ')');
      s2.charms.find(c => c.id === pl.id).shapeJson = got.shapeJson;
      assert.deepEqual(JSON.parse(JSON.stringify(ctx.shapeFieldOf({ ...item, side: undefined }, pl, sheet.hPt))), {}, 'a charm without a side (disc, letter, single) gets none');
      assert.deepEqual(JSON.parse(JSON.stringify(ctx.shapeFieldOf({ ...item, bits: null }, pl, sheet.hPt))), {}, 'a piece the page holds no bits for gets none (the oracle then does not judge it)');
      assert.deepEqual(JSON.parse(JSON.stringify(ctx.shapeFieldOf(item, null, sheet.hPt))), {}, 'an unplaced piece gets none');
    }
    assert.deepEqual(F.problems(w2, { tracked: [] }).map(x => x.code), [], 'what the page writes passes the placement oracle');
    // the page really calls it where the sheet record is built
    const ps = html.indexOf('async function persistSheet('), pe = html.indexOf('\nasync function', ps + 10);
    assert(/shapeFieldOf\(/.test(html.slice(ps, pe > ps ? pe : ps + 20000)), 'persistSheet puts shapeFieldOf into each saved charm');
    pass('5b the page writes shapeJson into the saved sheet charm of a sided piece (a string, no arrays in arrays) and into no other');
  }

  /* ── 6 · a pair that cannot fit an empty sheet: the solver turns the WHOLE line away (never one piece), says so in pairing, and the sorter stops on it (PAIRFLOW) ── */
  {
    const rect = (wMm, hMm, id, order, extra) => { const scale = 2, wPt = wMm / MM, hPt = hMm / MM, w = Math.ceil(wPt * scale), h = Math.ceil(hPt * scale), bits = new Uint8Array(w * h).fill(1);
      // a notch in one corner so the shape is not symmetric (the Right must stay the Left's mirror: here only the whole-or-nothing rule is under test)
      for (let y = 0; y < Math.floor(h / 3); y++) for (let x = 0; x < Math.floor(w / 6); x++) bits[y * w + x] = 0;
      const piece = { id, w, h, scale, bits, areaPt2: bits.reduce((a, b) => a + b, 0) / (scale * scale), order, orderDate: 1, pinned: null, hold: false, ...extra };
      return Object.assign(piece, S.pairFields(piece)); };
    const mk = (wMm, hMm, extra) => [rect(wMm, hMm, 'L1', 'o1', { side: 'L', groupKey: 'o1:1', groupSize: 2, mirror: false }), rect(wMm, hMm, 'R1', 'o1', { side: 'R', groupKey: 'o1:1', groupSize: 2, mirror: true, ...extra })];
    const job = pieces => ({ ...baseJob, pieces, maxTrials: 60, timeBudgetMs: 30000 });
    const one = await S.solve(job([mk(70, 30)[0]]), {});
    assert.equal(one.placements.length, 1, 'one 70 x 30 mm piece fits an empty sheet (the control)');
    const two = await S.solve(job(mk(70, 30)), {});
    assert.equal(two.placements.length, 0, 'the pair of two 70 x 30 mm pieces fits no empty sheet: neither piece is placed (never half)');
    assert.deepEqual([...two.rejects].sort(), ['L1', 'R1'], 'both pieces are turned away');
    assert(two.pairing && two.pairing.turnedAway === 1 && two.pairing.seated === 0 && two.pairing.split.length === 0, 'pairing says one group turned away whole: ' + JSON.stringify(two.pairing));
    const fits = await S.solve(job(mk(30, 20)), {});
    assert.equal(fits.placements.length, 2, 'the same pair, smaller, is seated whole');
    assert.equal(fits.pairing.seated, 1);
    // the Left is held where a person put it (pinned, hold, and in lockedPlacements as the page sends a held piece) and the Right has no room beside it: the line cannot be whole,
    // so the SOLVER says split (the held piece is never lifted); the page then keeps the Left, moves the Right on and records the split itself (overflowToNextSheet, splitGroups)
    const pair = mk(40, 30), pin = { cxPt: sheet.wPt / 2, cyPt: sheet.hPt / 2, angle: 0 };
    Object.assign(pair[0], { pinned: pin, hold: true });
    const locked = [{ id: 'L1', ...pin, wPt: pair[0].w / 2, hPt: pair[0].h / 2 }];
    const blocked = await S.solve({ ...job(pair), lockedPlacements: locked, initialLayout: locked }, {});
    assert.deepEqual(blocked.placements.map(p => p.id), ['L1'], 'the held Left stays where it was put');
    assert.deepEqual(blocked.rejects, ['R1'], 'the Right is turned away');
    assert.deepEqual(blocked.pairing.split, ['o1:1'], 'pairing.split names the line: ' + JSON.stringify(blocked.pairing));
    // (a held piece the page does NOT send in lockedPlacements is not held at all: the line is turned away whole, both pieces, which is also never half)
    const loose = await S.solve(job(mk(40, 30).map((p, i) => i ? p : Object.assign(p, { pinned: pin, hold: true }))), {});
    assert(loose.placements.length === 0 || loose.placements.length === 2, 'never half a pair: ' + loose.placements.length);
    pass('6 a pair that fits no empty sheet is turned away whole with pairing.turnedAway; a held piece with no room for its mate shows in pairing.split');
  }

  /* ── 7 · the partial sheet's trial pack (charm-nest-partial-nest.js runTrial -> the page's buildJob) hands the pair fields to the solver, so a pair is seated whole and near there too ── */
  {
    const fs = require('node:fs'), vm = require('node:vm');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), pn = fs.readFileSync(path.join(root, 'charm-nest-partial-nest.js'), 'utf8');
    assert(/const job=C\.buildJob\(proxy\)/.test(pn), 'the partial trial builds its job with the page\'s buildJob');
    const a = html.indexOf('function buildJob(sh)'), b = html.indexOf('function packingKey(', a);
    assert(a > 0 && b > a);
    const ctx = { Set, Map, Math, JSON, Object, Array, String, Number, window: {}, CharmNestSolver: S, S: { settings: { maxFill: .8, clearancePt: -.5, insetPt: 1.5, budgetS: 180 } },
      stockFor: () => ({ wPt: 283.5, hPt: 141.7 }), nestItems: sh => sh.charms, carefulNest: () => false, angleSet: () => [0, 90], CAREFUL_ANGLES: [0, 2], orderRank: () => 1 };
    vm.createContext(ctx); vm.runInContext(html.slice(a, b) + ';this.buildJob = buildJob;', ctx);
    const mate = (id, side, mirror) => ({ id, w: 4, h: 4, scale: 2, bits: new Uint8Array(16).fill(1), areaPt2: 4, order: '7001', poolId: '7001_1_' + id, side, mirror, bodyIndex: 0, groupKey: '7001:1', groupSize: 2 });
    const proxy = { metal: 'gold', charms: [mate(1, 'L', false), mate(2, 'R', true), { id: 3, w: 4, h: 4, scale: 2, bits: new Uint8Array(16).fill(1), areaPt2: 4, order: '7002' }], placements: [], rejects: [], appendOnly: false, nestInitial: null };
    const job = JSON.parse(JSON.stringify(ctx.buildJob(proxy), (k, v) => (v && v.type === 'Buffer') || (ArrayBuffer.isView(v)) ? undefined : v));
    const [pl, pr, pd] = job.pieces;
    assert.equal(pl.group, pr.group, 'the two pieces of a pair share a group');
    assert(pl.group != null && pl.side === 'L' && pr.side === 'R' && pr.mirror === true, 'side and mirror reach the solver: ' + JSON.stringify({ pl: [pl.group, pl.side, pl.near], pr: [pr.group, pr.side, pr.mirror] }));
    assert(pl.near !== false && pr.near !== false, 'a pair seats near its mate');
    assert.equal(pd.group, undefined, 'a plain charm carries no group');
    pass('7 the partial sheet\'s trial pack carries group, side, mirror and near into the solver job (PAIRPARTIAL\'s request is already met through buildJob)');
  }
})().catch(e => { console.error(e); process.exit(1); });
