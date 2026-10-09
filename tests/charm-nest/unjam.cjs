/* Unjam (Paul, 9 Oct 2026): "The system should have been smart enough to re-arrange some of the most recent placements near the
 * edge of the sheet to better expose usable space on the sheet for 2-3 more pieces. Currently the nesting is kind-of dumb and locks
 * itself into these impossible situations where there is a lot of surface area remaining but due to poor placement decisions this
 * remaining area is not accessible due to being too thin or awkwardly shaped."
 *
 * The solver (charm-nest-solver.js, "Unjam" at the end of carefulAppend and `movable` in solveAppend) lifts the few most recent
 * charms around the bottleneck when an order is turned away from a sheet that has room by area, seats the missed order first and the
 * lifted charms after it, and commits only when all of them are seated; the page (charm-nest-1.html) asks for it, keeps what a person
 * pinned, saves the moved charms at their new spots and says "Making room" on the card.
 *
 * Offline and deterministic: hand-made jams of square charms (a 102 x 64 pt sheet, eight 10 pt charms in a lattice whose corridors are
 * 17 pt wide, so no 30 pt charm fits although 90% of the sheet is free) and a seeded stream of the shop's own silhouettes through the
 * page's own rules (pocket-fill-lib.cjs). Nothing here touches the network, Firestore or a paid call; generous time ceilings keep it
 * independent of the machine.                                                                                                   */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const L = require('./pocket-fill-lib.cjs'), S = L.solverOf(L.REPO), R = require('../../charm-nest-rose');
const sq = (id, side, date = 0, extra = {}) => ({ ...L.square(id, side, extra.order || id, date), ...extra });
const spot = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, scale: .975 });   // a saved charm, as the page keeps it (shrunk 2.5%)
const base = { sheet: { wPt: 102, hPt: 64, insetPt: 1 }, angles: [0, 90], fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 120000, maxTrials: 50, clearancePt: 0, seed: 3, careful: true, unjamMs: 120000 };
/** Eight 10 pt charms saved in two rows of four, 27 pt apart: s0..s3 along the top, s4..s7 below (s7 the newest). */
function lattice(extra = () => ({})) {
  const pieces = [], saved = []; let n = 0;
  for (const y of [14, 41]) for (const x of [14, 41, 68, 95]) { const id = 's' + n; pieces.push(sq(id, 10, 0, extra(id, n))); saved.push(spot(id, x, y)); n++; }
  return { pieces, saved };
}
const same = (a, b) => a.id === b.id && a.angle === b.angle && a.cxPt === b.cxPt && a.cyPt === b.cyPt;
/** What the page does with a result: every charm scaled again, the moved saved ones saved at their new spots (finishNest). */
function rebased(job, result) {
  const moved = new Set((result.unjam && result.unjam.moved) || []), placements = result.placements.map(p => ({ ...p, scale: .975 }));
  return { placements, job: { ...job, lockedPlacements: job.lockedPlacements.map(l => moved.has(l.id) ? { ...placements.find(p => p.id === l.id) } : l) } };
}
const solve = (job, cb) => S.solve(job, cb || {});

(async () => {
  const { pieces, saved } = lattice();
  const jam = (extra = {}, more = []) => ({ ...base, pieces: [...pieces, ...more], lockedPlacements: saved, ...extra });

  /* 1. A 30 pt charm fits nowhere between the saved ones. Without the unjam it moves on to the next sheet (nothing moves). With it, the
        two newest saved charms are lifted, the missed charm goes in and they are seated again; nothing older moves, nothing is lost. */
  {
    const M = sq('M', 30, 1), off = await solve(jam({}, [M])), job = jam({ unjam: true }, [M]), moves = [];
    assert.deepEqual(off.rejects, ['M'], 'without the unjam the charm is turned away');
    assert.equal(off.unjam, undefined, 'a job without unjam reports none');
    const r = await solve(job, { onProbe: p => { if (p.unjam) moves.push(p.stage + ':' + p.id); } });
    assert.deepEqual(r.rejects, [], 'the missed charm is seated: ' + JSON.stringify(r.unjam));
    assert.equal(r.placements.length, 9, 'every charm is on the sheet, once');
    assert.equal(new Set(r.placements.map(p => p.id)).size, 9);
    assert(r.unjam.committed && r.unjam.rounds >= 1 && r.unjam.filled.includes('M'), 'the page is told: ' + JSON.stringify(r.unjam));
    const t0 = r.unjam.tries[0];
    assert(t0.needMm > t0.widestMm && t0.widestMm > 0, 'the bottleneck is measured: the widest free gap (' + t0.widestMm + ' mm) is narrower than the charm needs (' + t0.needMm + ' mm)');
    assert(r.pocketFilled.includes('M'), 'the page keeps the order the unjam seated (it is not lifted for being younger than a miss)');
    assert(r.unjam.moved.length >= 1 && r.unjam.moved.length <= 7, 'a few charms move: ' + r.unjam.moved);
    for (const id of r.unjam.moved) assert(['s4', 's5', 's6', 's7'].includes(id), id + ' is among the newest saved charms');
    for (const l of saved) if (!r.unjam.moved.includes(l.id)) assert(same(r.placements.find(p => p.id === l.id), l), l.id + ' is where it was saved');
    const moved = r.unjam.moved.map(id => r.placements.find(p => p.id === id));
    assert(moved.some(p => !same(p, saved.find(l => l.id === p.id))), 'a moved charm lies somewhere else');
    const { placements, job: after } = rebased(job, r);
    assert(S.verify(after, placements, 4).ok, 'no overlap, nothing outside the sheet, the charms the page holds are where they were saved');
    assert(!S.verify(job, placements, 4).ok, 'saved at their old spots the moved charms would be reported moved: the page must save the new ones');
    assert(moves.some(m => m.startsWith('lift:')) && moves.some(m => m.startsWith('place:M')), 'the card can show it: ' + moves.join(' '));
    // the same job again gives the same answer
    const again = await solve(job);
    assert.deepEqual(again.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), r.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), 'deterministic');
  }

  /* 2. A charm that fits nowhere however the newest are arranged leaves the sheet exactly as it was. */
  {
    const HUGE = sq('HUGE', 80, 1), off = await solve(jam({}, [HUGE])), on = await solve(jam({ unjam: true }, [HUGE]));
    assert.deepEqual(on.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), off.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), 'nothing moved');
    assert.deepEqual(on.rejects, ['HUGE']);
    assert(!on.unjam.committed && on.unjam.moved.length === 0 && on.unjam.filled.length === 0);
  }

  /* 3. Only the newest charms may move: a 60 x 40 charm needs the last three lifted. Allowed the last two, the sheet stays as it was;
        allowed three, the three move, and s0..s4 never do. */
  {
    const W = sq('W', 60, 1, {}); W.w = 60 * 4; W.h = 40 * 4; W.bits = new Uint8Array(W.w * W.h).fill(1); W.areaPt2 = 60 * 40; W.widthPt = 60; W.heightPt = 40;
    const two = await solve(jam({ unjam: true, unjamWindow: 2 }, [W]));
    assert.deepEqual(two.rejects, ['W'], 'two lifted charms leave no room: ' + JSON.stringify(two.unjam));
    assert(!two.unjam.committed && two.unjam.moved.length === 0);
    for (const l of saved) assert(same(two.placements.find(p => p.id === l.id), l));
    const three = await solve(jam({ unjam: true, unjamWindow: 3 }, [W]));
    assert.deepEqual(three.rejects, [], 'three lifted charms leave room: ' + JSON.stringify(three.unjam));
    assert(three.unjam.moved.length <= 3);
    for (const id of three.unjam.moved) assert(['s5', 's6', 's7'].includes(id), id + ' is within the window');
    for (const l of saved.slice(0, 5)) assert(same(three.placements.find(p => p.id === l.id), l), l.id + ' is older than the window and never moves');
  }

  /* 4. A charm a person pinned (the page sends hold) never moves, nor does one the green line holds. */
  {
    const M = sq('M', 30, 1), held = lattice((id, n) => ({ hold: n >= 6 })), r = await solve({ ...base, pieces: [...held.pieces, M], lockedPlacements: held.saved, unjam: true });
    assert.deepEqual(r.rejects, [], 'the others make room: ' + JSON.stringify(r.unjam));
    for (const id of ['s6', 's7']) assert(same(r.placements.find(p => p.id === id), held.saved.find(l => l.id === id)), id + ' is pinned and stays');
    assert(r.unjam.moved.length >= 1 && r.unjam.moved.every(id => ['s0', 's1', 's2', 's3', 's4', 's5'].includes(id)));
    // the run pins every placed charm itself (arrivalPin): that is `pinned` with no `hold`, and does not stop the unjam
    const runPinned = lattice(id => ({ pinned: { cxPt: 1, cyPt: 1, angle: 0 } })), q = await solve({ ...base, pieces: [...runPinned.pieces, M], lockedPlacements: runPinned.saved, unjam: true });
    assert.deepEqual(q.rejects, [], 'the run\'s own pin on a placed charm is not a person\'s');
    // a Rose Gold green line: the charms it holds are never moved, whatever the unjam would gain
    const guarded = lattice(), plan = R.plan([{ id: 's6', paths: [[[1, 1], [2, 1], [2, 2], [1, 2]]] }], 102, 64, null, .2);
    for (const rose of [{ ...plan, placements: guarded.saved.slice(6) }, { ...plan, placements: guarded.saved }]) {
      const job = { ...base, pieces: [...guarded.pieces, M], lockedPlacements: guarded.saved, protectedRose: rose, unjam: true }, g = await solve(job);
      for (const l of rose.placements) assert(same(g.placements.find(p => p.id === l.id), l), l.id + ' is held by the green line and stays');
      for (const id of g.unjam.moved) assert(!rose.placements.some(l => l.id === id));
      assert(S.verify(rebased(job, g).job, rebased(job, g).placements, 4).ok || g.unjam.moved.length === 0, 'the saved layout stands');
    }
  }

  /* 5. An order is never split: two charms of one order lift together or not at all (whole order in the window, none of it held). */
  {
    const M = sq('M', 30, 1), pair = lattice((id, n) => (n >= 6 ? { order: 'two' } : {}));
    const one = await solve({ ...base, pieces: [...pair.pieces, M], lockedPlacements: pair.saved, unjam: true, unjamWindow: 1 });
    assert.deepEqual(one.rejects, ['M'], 'half an order in the window is not lifted');
    for (const l of pair.saved) assert(same(one.placements.find(p => p.id === l.id), l));
    const both = await solve({ ...base, pieces: [...pair.pieces, M], lockedPlacements: pair.saved, unjam: true, unjamWindow: 2 });
    assert.deepEqual(both.rejects, [], 'both pieces lift and are seated again: ' + JSON.stringify(both.unjam));
    assert(both.placements.some(p => p.id === 's6') && both.placements.some(p => p.id === 's7'), 'the order stays whole');
    assert(both.unjam.moved.includes('s6') === both.unjam.moved.includes('s7'), 'moved together');
    const held = lattice((id, n) => (n >= 6 ? { order: 'two', hold: n === 7 } : {}));
    const stuck = await solve({ ...base, pieces: [...held.pieces, M], lockedPlacements: held.saved, unjam: true, unjamWindow: 2 });
    assert(!stuck.unjam.moved.includes('s6'), 'a pinned piece keeps its whole order where it is');
    for (const id of ['s6', 's7']) assert(same(stuck.placements.find(p => p.id === id), held.saved.find(l => l.id === id)));
  }

  /* 6. Two orders waiting: the second round seats the next one too (Paul: room for 2-3 more pieces), and a charm that waits for a
        sheet that does not need the unjam is untouched. */
  {
    const M1 = sq('M1', 30, 1), M2 = sq('M2', 30, 2), r = await solve(jam({ unjam: true }, [M1, M2]));
    assert(r.placements.some(p => p.id === 'M1'), 'the oldest missed order is seated');
    assert(r.placements.length >= 10, 'room for the next one too: ' + r.placements.length + ' ' + JSON.stringify(r.unjam));
    assert(r.unjam.filled.includes('M1') && r.unjam.committed);
    const { placements, job } = rebased(jam({ unjam: true }, [M1, M2]), r);
    assert(S.verify(job, placements, 4).ok);
    const free = await solve(jam({ unjam: true }, [sq('tiny', 8, 1)]));
    assert.deepEqual(free.rejects, [], 'a charm that fits a gap needs no unjam');
    assert(!free.unjam.committed && free.unjam.attempts === 0 && free.unjam.moved.length === 0, 'and none is tried');
    for (const l of saved) assert(same(free.placements.find(p => p.id === l.id), l));
  }

  /* 7. A stopped search, or one out of time, leaves the saved layout alone. */
  {
    const M = sq('M', 30, 1), stopped = await solve(jam({ unjam: true }, [M]), { shouldStop: () => true });
    for (const l of saved) assert(same(stopped.placements.find(p => p.id === l.id), l), 'stopped: ' + l.id);
    const spent = await solve(jam({ unjam: true, unjamMs: 1 }, [M]));
    for (const l of saved) assert(same(spent.placements.find(p => p.id === l.id), l) || spent.unjam.committed, 'a spent budget changes nothing it did not finish');
    assert(S.verify(rebased(jam({ unjam: true, unjamMs: 1 }, [M]), spent).job, rebased({ ...jam({}, [M]), unjam: true }, spent).placements, 4).ok);
  }

  /* 7b. A fault inside the unjam (here: the live view's callback throwing while a charm is lifted) never fails the search: the sheet is
         left exactly as the passes made it and the fault is reported. */
  {
    const M = sq('M', 30, 1), off = await solve(jam({}, [M])), r = await solve(jam({ unjam: true }, [M]), { onProbe: p => { if (p.unjam) throw new Error('boom'); } });
    assert.deepEqual(r.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), off.placements.map(p => [p.id, p.cxPt, p.cyPt, p.angle]), 'the sheet as the passes left it');
    assert.deepEqual(r.rejects, ['M']);
    assert(/boom/.test(r.unjam.error || '') && !r.unjam.committed && r.unjam.moved.length === 0, 'the fault is reported: ' + JSON.stringify(r.unjam));
  }

  /* 8. The page: asks for it on every metal but Rose Gold, never lifts a charm a person pinned, shows "Making room", saves the moved
        charms at their new spots and says so in the log. */
  {
    const html = fs.readFileSync(path.join(L.REPO, 'charm-nest-1.html'), 'utf8');
    assert(/unjam: careful && sh\.metal !== "rose" && !sh\.roseCutAt/.test(html), 'buildJob: on for careful nests, never Rose Gold or a cut sheet');
    assert(/hold: !!\(c\.pinned && !c\.arrivalPin\)/.test(html), "buildJob: a person's pin is `hold`");
    assert(/pr\.unjam \? "Making room · " : ""/.test(html) && /m\.stage === "unjam"/.test(html), 'the card says "Making room"');
    assert(/result\.unjam\?\.moved\?\.length && sh\.nestInitial/.test(html) && /scale: p\.scale/.test(html), 'finishNest saves the moved charms at their new spots, scale kept');
    assert(/\(sh\._unjamFails \|\| 0\) < 3/.test(html) && /sh\._unjamFails = \(sh\._unjamFails \|\| 0\) \+ 1/.test(html), 'a sheet whose attempts failed tries less each time and not at all after three');
    assert(/_unjamLift/.test(html), 'a charm lifted for the unjam that is not seated again is put back on the card');
    const worker = fs.readFileSync(path.join(L.REPO, 'charm-nest-worker.js'), 'utf8');
    assert(/onProbe: \(probe\) => post\(\{ type: "probe", probe \}\)/.test(worker), 'the worker passes every probe on, the unjam flag included');
  }

  /* 9. The sorter's own flow: a seeded stream of the shop's silhouettes on a 14K 50 x 46 mm sheet through the page's rules. Every layout
        verifies (the run throws otherwise), every order stays whole on one sheet, nothing is lost, and no saved charm moves outside
        the newest window. */
  if (process.argv.includes('--stream') || process.env.UNJAM_STREAM) {
    const stock = { wPt: L.PT(50), hPt: L.PT(46) }, stream = L.makeOrders(S, +(process.env.UNJAM_ORDERS || 10), 7, { small: .35, mid: .40, large: .25, two: .2, grow: 1 });
    const run = await L.runStream({ metal: 'gold14k', sheet: stock, orders: stream, batch: 3, seed: 1, unjam: true, unjamMs: 90000 });
    const where = new Map(); for (const p of run.pages) for (const pl of p.placements) { assert(!where.has(pl.id), pl.id + ' is on two sheets'); where.set(pl.id, p.page); }
    for (const o of stream) { const sheets = new Set(o.pieces.map(c => where.get(c.id))); assert(!sheets.has(undefined), o.id + ' was lost'); assert.equal(sheets.size, 1, o.id + ' is split across sheets'); }
    for (const p of run.pages) assert(p.verification.ok, 'sheet ' + p.page + ' verifies');
  }

  console.log('Unjam OK: a bottleneck is found when an order misses a sheet that has room by area; the newest charms around it (whole orders, at most 7 of the last 10, never a pinned or green-line one) lift and are seated again with the missed order first; committed only when all are seated, else the sheet is exactly as it was; a second order is seated too; stopped or out of time changes nothing; the page asks for it (not on Rose Gold), keeps pins, saves the moved charms and says "Making room"');
})().catch(e => { console.error(e); process.exitCode = 1; });
