/* Pairs in the nester (Paul, 9 Oct 2026): "the system needs to understand that it needs to place both charms together for the same order and
 * the same colour sheet", and (18:47) the Right earring is the Left mirrored: "the directionality and left and right sides must always be
 * maintained". The solver (charm-nest-solver.js) takes a pair as TWO pieces that share an `order` and carry `group`, `side`, `mirror` and
 * `near`; this suite checks, with plain shapes and no network:
 *   1  a pair of two different bodies is seated whole or turned away whole (search, pocket pass, unjam), a half is never seated;
 *   2  `group` joins pieces whatever order key the page gave them; a group with a saved piece and a turned-away one is reported (`pairing.split`);
 *   3  singles and discs nest exactly as before (nothing is read from a piece without pair fields);
 *   4  `near` seats the second body close to the first, at no cost in pieces or fill;
 *   5  the Right is never reflected: a cavity shaped like the mirrored piece takes the Right at its own angle and never the Left, and a mirror pair
 *      stays a mirror pair under every rotation (no cache is shared between a piece and its mirror);
 *   6  the job and result shapes between the page and the workers (pairFields, the server fallback, the learned solver, no nested arrays).
 * Offline and deterministic; generous time ceilings keep it independent of the machine.                                                     */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const L = require('./pocket-fill-lib.cjs'), S = L.solverOf(L.REPO), noNested = require('./_noNestedArrays.cjs'), K = 4;

const bitsOf = (w, h, f) => { const W = Math.round(w * K), H = Math.round(h * K), b = new Uint8Array(W * H); for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) b[y * W + x] = f(x / K, y / K) ? 1 : 0; return { W, H, b }; };
const area = b => { let n = 0; for (const v of b) n += v; return n / (K * K); };
const piece = (id, w, h, order, date, extra = {}) => { const { W, H, b } = bitsOf(w, h, () => true); return { id, w: W, h: H, scale: K, bits: b, areaPt2: w * h, order: order || id, orderDate: date || 0, ...extra }; };
/** A pair piece: own bits (any shape), the pair fields the page gives it (S.pairFields). */
const pairPiece = (id, w, h, order, date, group, side, extra = {}) => piece(id, w, h, order, date, { group, side, near: true, ...extra });
const asJob = (base, pieces, more = {}) => ({ ...base, ...more, pieces: pieces.map(p => { const { name, hash, widthPt, heightPt, ...q } = p; return q; }) });
const mirrorBits = (b, W, H) => { const o = new Uint8Array(W * H); for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) o[y * W + (W - 1 - x)] = b[y * W + x]; return o; };
/** An F: a stem and two arms, no symmetry of any kind (a mirror of it is not a rotation of it). */
const F = (w, h) => bitsOf(w, h, (x, y) => x < w * .3 || (y < h * .22 && x < w) || (y > h * .45 && y < h * .67 && x < w * .75));
const placedOf = (r, id) => r.placements.find(p => p.id === id);
const same = (a, b) => a.id === b.id && a.angle === b.angle && a.cxPt === b.cxPt && a.cyPt === b.cyPt;
const gapMm = (r, a, b) => { const A = placedOf(r, a), B = placedOf(r, b); if (!A || !B) return null; return Math.hypot(Math.max(0, A.xPt - (B.xPt + B.wPt), B.xPt - (A.xPt + A.wPt)), Math.max(0, A.yPt - (B.yPt + B.hPt), B.yPt - (A.yPt + A.hPt))) * L.MM; };

(async () => {
  const base = { sheet: { wPt: 60, hPt: 40, insetPt: 1 }, angles: [0, 90], fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 60000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true };
  const blk = (id, x0, y0, x1, y1) => ({ p: piece(id, x1 - x0, y1 - y0), at: { id, cxPt: (x0 + x1) / 2, cyPt: (y0 + y1) / 2, angle: 0 } });

  /* 1a. One 9 x 9 pt hole. A pair of two DIFFERENT bodies that needs more than the hole is turned away whole; the single order behind it takes the hole. */
  {
    const blocks = [blk('r1', 10, 1, 56, 39), blk('r2', 1, 10, 10, 39)], locked = blocks.map(b => b.at), fixed = blocks.map(b => b.p);
    const big = piece('BIG', 30, 30, 'BIG', 1), a = pairPiece('P.L', 7, 7, 'P', 2, 'P:1', 'L'), b = pairPiece('P.R', 5, 9, 'P', 2, 'P:1', 'R'), one = piece('ONE', 7, 7, 'ONE', 3);
    const job = asJob(base, [...fixed, big, a, b, one], { lockedPlacements: locked });
    const r = await S.solve(job, {}), ids = new Set(r.placements.map(p => p.id));
    assert(ids.has('ONE'), 'the single behind it takes the hole: ' + [...ids]);
    // (7 x 7 and 5 x 9 cannot both be in a 9 x 9 hole; either alone could, which is exactly the half-pair the sheet must never hold)
    assert(!ids.has('P.L') && !ids.has('P.R'), 'half of a pair is never seated: ' + [...ids]);
    assert(r.pairing && r.pairing.groups === 1 && r.pairing.turnedAway === 1 && r.pairing.seated === 0 && r.pairing.split.length === 0, 'the result says so: ' + JSON.stringify(r.pairing));
    assert(S.verify(job, r.placements, 4).ok);
    for (const l of locked) assert(same(placedOf(r, l.id), l), 'a seated charm never moves');
    // the control: with no link between the two bodies the same hole would take one of them and split the pair
    const split = await S.solve(asJob(base, [...fixed, big, { ...a, order: 'P.L', group: undefined, side: undefined, near: undefined }, { ...b, order: 'P.R', group: undefined, side: undefined, near: undefined }, one], { lockedPlacements: locked }), {});
    assert(split.placements.some(p => p.id === 'P.L' || p.id === 'P.R'), 'unlinked, one body would have taken the hole (what the link prevents)');
  }

  /* 1b. `group` joins pieces whatever order key the page gave them. */
  {
    const blocks = [blk('r1', 10, 1, 56, 39), blk('r2', 1, 10, 10, 39)], locked = blocks.map(b => b.at), fixed = blocks.map(b => b.p);
    const big = piece('BIG', 30, 30, 'BIG', 1), a = pairPiece('G.a', 7, 7, 'ORD-A', 2, 'G:1', 'L'), b = pairPiece('G.b', 5, 9, 'ORD-B', 2, 'G:1', 'R');
    const job = asJob(base, [...fixed, big, a, b], { lockedPlacements: locked });
    const r = await S.solve(job, {}), ids = new Set(r.placements.map(p => p.id));
    assert(!ids.has('G.a') && !ids.has('G.b'), 'one group, two order keys: neither is seated alone: ' + [...ids]);
    assert.equal(r.pairing.split.length, 0);
    const nogroup = await S.solve(asJob(base, [...fixed, big, { ...a, group: undefined, near: undefined, side: undefined }, { ...b, group: undefined, near: undefined, side: undefined }], { lockedPlacements: locked }), {});
    assert(nogroup.placements.some(p => p.id === 'G.a' || p.id === 'G.b'), 'without the group the orders are independent');
    assert.equal(S.linkGroups(job).pieces.filter(p => p.group === 'G:1').map(p => p.order).filter((o, i, x) => x.indexOf(o) === i).length, 1, 'linkGroups gives them one order');
    assert.equal(S.linkGroups({ pieces: [a, { ...b, order: 'ORD-A' }] }).pieces[1].order, 'ORD-A');
    const j2 = { pieces: [{ id: 1, order: 'X' }, { id: 2, order: 'X', group: 'X:1' }] };
    assert.equal(S.linkGroups(j2), j2, 'nothing to join: the same job back');
  }

  /* 1c. Two holes, one body each (a pair does not have to share one pocket): the pocket pass seats BOTH; one hole only: neither. */
  {
    const two = [blk('m', 10, 1, 50, 39), blk('bl', 1, 10, 10, 39), blk('br', 50, 10, 59, 39)], one = [blk('m', 10, 1, 50, 39), blk('bl', 1, 10, 10, 39), blk('br', 50, 1, 59, 39)];
    const big = piece('BIG', 30, 30, 'BIG', 1), a = pairPiece('H.L', 7, 7, 'H', 2, 'H:1', 'L'), b = pairPiece('H.R', 6, 8, 'H', 2, 'H:1', 'R');
    for (const [name, blocks, both] of [['two holes', two, true], ['one hole', one, false]]) {
      const locked = blocks.map(x => x.at), job = asJob(base, [...blocks.map(x => x.p), big, a, b], { lockedPlacements: locked });
      const r = await S.solve(job, {}), ids = new Set(r.placements.map(p => p.id));
      if (both) {
        assert(ids.has('H.L') && ids.has('H.R'), name + ': both bodies seated, one in each hole');
        assert.deepEqual(r.pocketFilled.slice().sort(), ['H.L', 'H.R'], name + ': the pocket pass reports both');
        assert.equal(r.pairing.seated, 1);
      } else {
        assert(!ids.has('H.L') && !ids.has('H.R'), name + ': neither');
        assert(!r.pocketFilled.includes('H.L') && !r.pocketFilled.includes('H.R'));
        assert.equal(r.pairing.turnedAway, 1);
      }
      assert(S.verify(job, r.placements, 4).ok);
      for (const l of locked) assert(same(placedOf(r, l.id), l));
    }
  }

  /* 2. A saved piece whose partner cannot be seated: the group is reported split (the page flags it, the solver never hides it). */
  {
    const a = pairPiece('S.L', 10, 10, 'S', 1, 'S:1', 'L'), b = pairPiece('S.R', 30, 30, 'S', 1, 'S:1', 'R'), blocks = [blk('wall', 1, 12, 59, 39)];
    const job = asJob(base, [...blocks.map(x => x.p), a, b], { lockedPlacements: [...blocks.map(x => x.at), { id: 'S.L', cxPt: 6, cyPt: 6, angle: 0 }] });
    const r = await S.solve(job, {});
    assert(placedOf(r, 'S.L') && !placedOf(r, 'S.R'), 'the saved body stays, the big one cannot be seated');
    assert.deepEqual(r.pairing.split, ['S:1'], 'reported: ' + JSON.stringify(r.pairing));
    noNested(r.pairing, 'pairing'); noNested(S.publicLayout(r).pairing, 'pairing (public)');
    const pub = S.publicLayout(r); assert(pub.pairing.split !== r.pairing.split && pub.pairing.near !== r.pairing.near, 'publicLayout copies it');
  }

  /* 3. Singles, discs and anything without pair fields: the same nest as without them. A disc set carries a group and no side: it does not ask to be near. */
  {
    const rnd = L.rng(11), mk = () => Array.from({ length: 12 }, (_, i) => piece('d' + i, 5 + Math.floor(rnd() * 12), 5 + Math.floor(rnd() * 12), i >= 6 && i < 9 ? 'disc' : 'd' + i, 1 + i));   // (pieces 6, 7, 8: one necklace of three discs)
    const plain = mk();
    const withGroup = plain.map(p => /^disc/.test(p.order) ? { ...p, group: 'R:' + p.order } : p), withNearOff = plain.map(p => ({ ...p, group: 'R:' + p.order, near: false }));
    const j = x => asJob(base, x, { maxFill: .8, sheet: { wPt: 70, hPt: 40, insetPt: 1 } });
    const r0 = await S.solve(j(plain), {}), r1 = await S.solve(j(withGroup), {}), r2 = await S.solve(j(withNearOff), {});
    const key = r => JSON.stringify(r.placements.map(p => [p.id, p.angle, p.cxPt, p.cyPt]));
    assert.equal(key(r1), key(r0), 'a disc group without sides nests as it did');
    assert.equal(key(r2), key(r0), 'near:false changes nothing');
    assert.equal(r0.pairing, undefined, 'no group, no pairing report');
    assert(r1.pairing && r1.pairing.near.n === 0, 'a report, and no group asked to be near');
    assert.deepEqual(S.pairFields({ id: 'x' }), {}, 'a charm without a group gives nothing');
    assert.deepEqual(S.pairFields({ orderInfo: { receiptId: 5, transactionId: 9 } }), { group: '5:9' });
  }

  /* 4. `near`: the second body goes by the first. Seeded streams of rectangles, a pair in the middle of each; same pieces, near on and off. */
  {
    const run = async (seed, near) => {
      const r = L.rng(seed), ps = [];
      for (let i = 0; i < 10; i++) ps.push(piece('s' + i, 5 + Math.floor(r() * 12), 5 + Math.floor(r() * 12), 's' + i, 1 + i));
      ps.splice(5, 0, pairPiece('PL', 11, 13, 'P', 6, 'P:1', 'L', near ? {} : { near: false }), pairPiece('PR', 14, 9, 'P', 6, 'P:1', 'R', near ? {} : { near: false }));
      const job = asJob(base, ps, { sheet: { wPt: 100, hPt: 40, insetPt: 1 }, maxFill: .8, timeBudgetMs: 60000 });
      const res = await S.solve(job, {});
      assert(S.verify(job, res.placements, 4).ok);
      return { res, gap: gapMm(res, 'PL', 'PR') };
    };
    let on = 0, off = 0, n = 0;
    for (const seed of [1, 3, 6, 8]) {
      const a = await run(seed, true), b = await run(seed, false);
      assert.equal(a.res.placements.length, b.res.placements.length, 'seed ' + seed + ': near costs no piece');
      assert(a.res.density >= b.res.density - .01, 'seed ' + seed + ': nor fill (' + a.res.density + ' vs ' + b.res.density + ')');
      assert(a.gap != null && b.gap != null && a.res.pairing.split.length === 0, 'both bodies seated');
      assert.equal(a.res.pairing.near.n, 1); assert(Math.abs(a.res.pairing.near.maxMm - a.gap) < .05, 'the report measures the gap: ' + JSON.stringify(a.res.pairing.near) + ' vs ' + a.gap);
      on += a.gap; off += b.gap; n++;
    }
    assert(on / n < off / n, `near seats the pair closer: mean gap ${(on / n).toFixed(2)} mm against ${(off / n).toFixed(2)} mm`);
  }

  /* 4b. In the Rose Gold block (job.block) the mate term only breaks ties inside the block: the block grows no more because of it. */
  {
    const run = async near => {
      const r = L.rng(5), ps = [];
      for (let i = 0; i < 8; i++) ps.push(piece('s' + i, 5 + Math.floor(r() * 12), 5 + Math.floor(r() * 12), 's' + i, 1 + i));
      ps.splice(4, 0, pairPiece('PL', 11, 13, 'P', 5, 'P:1', 'L', near ? {} : { near: false }), pairPiece('PR', 14, 9, 'P', 5, 'P:1', 'R', near ? {} : { near: false }));
      const job = asJob(base, ps, { sheet: { wPt: 100, hPt: 40, insetPt: 1 }, maxFill: .8, block: true });
      const res = await S.solve(job, {}); assert(S.verify(job, res.placements, 4).ok);
      return { res, far: Math.max(...res.placements.map(p => p.xPt + p.wPt)) };
    };
    const a = await run(true), b = await run(false);
    assert.equal(a.res.placements.length, b.res.placements.length);
    assert(a.far <= b.far + 1.5, `the block is no longer: ${a.far.toFixed(1)} against ${b.far.toFixed(1)} pt`);
    assert.equal(a.res.pairing.split.length, 0);
  }

  /* 5a. Unjam: a saved pair is lifted whole or not at all. Eight 10 pt charms in a lattice (the unjam suite's jam), a 30 pt charm missed. */
  {
    const jamBase = { ...base, sheet: { wPt: 102, hPt: 64, insetPt: 1 }, timeBudgetMs: 120000, unjamMs: 120000 };
    const spot = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, scale: .975 });
    const lattice = pairOf => {
      const pieces = [], saved = []; let n = 0;
      for (const y of [14, 41]) for (const x of [14, 41, 68, 95]) { const id = 's' + n, p = pairOf[id]; pieces.push(piece(id, 10, 10, p ? 'P' : id, 0, p ? { group: 'P:1', side: p, near: true } : {})); saved.push(spot(id, x, y)); n++; }
      return { pieces, saved };
    };
    const M = piece('M', 30, 30, 'M', 1);
    // the pair is the two newest saved charms
    let { pieces, saved } = lattice({ s6: 'L', s7: 'R' });
    let r = await S.solve({ ...jamBase, unjam: true, pieces: [...pieces, M], lockedPlacements: saved }, {});
    assert.deepEqual(r.rejects, [], 'the missed charm is seated: ' + JSON.stringify(r.unjam));
    const moved = new Set(r.unjam.moved);
    assert.equal(moved.has('s6'), moved.has('s7'), 'the pair is lifted whole or not at all: ' + [...moved]);
    assert(S.verify({ ...jamBase, pieces: [...pieces, M], lockedPlacements: saved.map(l => moved.has(l.id) ? { ...placedOf(r, l.id), scale: .975 } : l) }, r.placements.map(p => ({ ...p, scale: .975 })), 4).ok);
    assert.equal(r.pairing.split.length, 0);
    // the pair's other body is old (outside the window of the last two): it cannot be lifted alone, so neither is
    ({ pieces, saved } = lattice({ s7: 'R', s2: 'L' }));
    r = await S.solve({ ...jamBase, unjam: true, unjamWindow: 2, pieces: [...pieces, M], lockedPlacements: saved }, {});
    assert(!r.unjam.moved.includes('s7') && !r.unjam.moved.includes('s2'), 'a pair with one body outside the window is never lifted: ' + r.unjam.moved);
    assert.equal(r.pairing.split.length, 0, JSON.stringify(r.pairing));
    for (const l of saved) assert(same(placedOf(r, l.id), l) || r.unjam.moved.includes(l.id), 'nothing else moved');
  }

  /* 5b. The Right is the Left mirrored, and the nester never reflects. A cavity shaped like the mirrored piece takes the Right at its own angle and
         never the Left, at any of the 180 angles; a mirror pair stays a mirror pair under every rotation; no variant is shared between the two. */
  {
    const w = 12, h = 16, f = F(w, h), left = { W: f.W, H: f.H, b: f.b }, right = { W: f.W, H: f.H, b: mirrorBits(f.b, f.W, f.H) };
    const same2 = left.b.every((v, i) => v === right.b[i]); assert(!same2, 'the F is not its own mirror');
    const mk = (id, o, side, mirror) => ({ id, w: o.W, h: o.H, scale: K, bits: o.b, areaPt2: area(o.b), order: 'E', orderDate: 1, group: 'E:1', side, near: true, ...(mirror ? { mirror: true } : {}) });
    const Lp = mk('E.L', left, 'L', false), Rp = mk('E.R', right, 'R', true);
    // the cavity: a window of a locked filler with the mirrored F cut out of it, a little larger than the F (a raster of the filler grows into it by a cell)
    const grow = 2, cavity = S.dilate(right.b, right.W, right.H, grow), cw = cavity.w / K, ch = cavity.h / K;
    const fillBits = cavity.bits.map(v => v ? 0 : 1);
    const fill = { id: 'FILL', w: cavity.w, h: cavity.h, scale: K, bits: fillBits, areaPt2: area(fillBits), order: 'FILL', orderDate: 0 };
    const cav = { sheet: { wPt: cw + 2, hPt: ch + 2, insetPt: 1 }, angles: Array.from({ length: 180 }, (_, i) => i * 2), fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 120000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true, lockedPlacements: [{ id: 'FILL', cxPt: (cw + 2) / 2, cyPt: (ch + 2) / 2, angle: 0 }] };
    const onlyR = await S.solve({ ...cav, pieces: [fill, { ...Rp, group: undefined, side: undefined, near: undefined, order: 'ER' }] }, {}), onlyL = await S.solve({ ...cav, pieces: [fill, { ...Lp, group: undefined, side: undefined, near: undefined, order: 'EL' }] }, {});
    assert(placedOf(onlyR, 'E.R') && [0, 2, 358].includes(placedOf(onlyR, 'E.R').angle), 'the Right fits its own cavity at its own angle: ' + JSON.stringify(onlyR.placements.map(p => [p.id, p.angle])));
    assert(!placedOf(onlyL, 'E.L'), 'the Left does not fit it at any angle: the solver rotates, it never reflects');
    // a mirror pair under every rotation: rotating the mirrored piece by -a is the mirror of the original rotated by a
    const iou = (a, b) => { const W = Math.max(a.w, b.w), H = Math.max(a.h, b.h); let i = 0, u = 0; const at = (m, x, y) => { const ox = Math.floor((W - m.w) / 2), oy = Math.floor((H - m.h) / 2); const xx = x - ox, yy = y - oy; return xx >= 0 && yy >= 0 && xx < m.w && yy < m.h ? m.bits[yy * m.w + xx] : 0; }; for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const p = at(a, x, y), q = at(b, x, y); i += p & q; u += p | q; } return i / u; };
    let worst = 1, bestChiral = 0;
    for (let a = 0; a < 360; a += 15) {
      const rl = S.rotateBitmap(left.b, left.W, left.H, a), rr = S.rotateBitmap(right.b, right.W, right.H, -a);
      const mirrored = { bits: mirrorBits(rl.bits, rl.w, rl.h), w: rl.w, h: rl.h };
      worst = Math.min(worst, iou(rr, mirrored));
      for (let b = 0; b < 360; b += 15) bestChiral = Math.max(bestChiral, iou(S.rotateBitmap(left.b, left.W, left.H, a), S.rotateBitmap(right.b, right.W, right.H, b)));
    }
    assert(worst > .93, 'rotating a mirror pair keeps it a mirror pair (worst overlap ' + worst.toFixed(3) + ')');
    assert(bestChiral < .93, 'no rotation turns the Right into the Left (best overlap ' + bestChiral.toFixed(3) + '): a reflection would be needed');
    // a pair nested together: each is placed from its own bits, only angle and place are reported
    const job = { ...base, angles: Array.from({ length: 24 }, (_, i) => i * 15), sheet: { wPt: 40, hPt: 30, insetPt: 1 }, pieces: [Lp, Rp] };
    const r = await S.solve(job, {});
    assert.equal(r.placements.length, 2, 'the pair is seated whole'); assert(S.verify(job, r.placements, 6).ok);
    for (const pl of r.placements) for (const k of Object.keys(pl)) assert(!/mirror|flip|reflect|scaleX/i.test(k), 'a placement carries no reflection: ' + k);
    assert(r.pairing.seated === 1 && r.pairing.near.maxMm < 15, JSON.stringify(r.pairing));
    assert.notEqual(S.shapeKey({ hash: 'H', w: 48, h: 64, scale: 4, areaPt2: 100 }), S.shapeKey({ hash: 'H', w: 48, h: 64, scale: 4, areaPt2: 100, mirror: true }), 'the mirrored piece has a cache key of its own');
    assert.equal(S.pairFields({ side: 'R', mirror: true, groupKey: '1:2' }).mirror, true); assert.equal(S.pairFields({ side: 'L', groupKey: '1:2' }).mirror, undefined);
  }

  /* 6. Job and result shapes between the page and the workers. */
  {
    assert.deepEqual(S.pairFields({ orderInfo: { receiptId: 7, transactionId: 8 }, side: 'L', bodyIndex: 0 }), { group: '7:8', side: 'L', near: true, bodyIndex: 0 });
    assert.deepEqual(S.pairFields({ groupKey: ':' }), {}, '":" is no group: it would join unrelated orders');
    assert.deepEqual(S.pairFields({ groupKey: ':4', side: 'R' }), { side: 'R', near: true }, 'a key with no receipt is ignored, the side is kept');
    assert.deepEqual(S.pairFields({ groupKey: '9:', side: 'R' }), { group: '9:', side: 'R', near: true }, 'a receipt with no transaction is the whole receipt: still one unit');
    const jp = { id: 'a', group: '1:2', side: 'R', near: false, bodyIndex: 1 }; assert.deepEqual(S.pairFields(jp), { group: '1:2', side: 'R', near: false, bodyIndex: 1 }, 'a job piece gives its own fields back');
    const server = fs.readFileSync(path.join(L.REPO, 'netlify/functions/charmNestSolve-background.js'), 'utf8');
    assert(/\.\.\.Solver\.pairFields\(p\)/.test(server), 'the server fallback passes the pair fields on');
    const html = fs.readFileSync(path.join(L.REPO, 'charm-nest-1.html'), 'utf8');
    assert(/hold: !!\(c\.pinned && !c\.arrivalPin\), \.\.\.\(typeof CharmNestSolver !== "undefined" && CharmNestSolver\.pairFields \? CharmNestSolver\.pairFields\(c\) : \{\}\)/.test(html), 'the page job carries the pair fields');
    const learned = fs.readFileSync(path.join(L.REPO, 'charm-nest-learned.js'), 'utf8');
    assert(/Sv\.linkGroups\(job\)/.test(learned), 'the learned solver joins groups too');
    // the worker, the page and the rose demo name the same solver and worker files (one cache token)
    const tok = f => [...fs.readFileSync(path.join(L.REPO, f), 'utf8').matchAll(/charm-nest-(?:solver|worker)\.js\?v=([\w.-]+)/g)].map(m => m[1]);
    for (const f of ['charm-nest-worker.js', 'charm-nest-1.html', 'charm-nest-rose-ui.js']) for (const t of tok(f)) assert(/pair\d/.test(t), f + ' names a solver or worker without the pair token: ' + t);
  }
  console.log('pairs-nest: ok');
})().catch(e => { console.error(e); process.exit(1); });
