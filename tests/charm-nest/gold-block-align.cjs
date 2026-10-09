/* 10K and 14K sheets are arranged like the RG 14/20 sheet (Paul, 8 Oct 2026): "Ensure that the 14k Gold and 10K Gold sheets get
 * arranged in the same way as the RG 14/20 sheet ... the GF sheet as seen in image #2 is aligned fairly straight from top to
 * bottom of the sheet whereas the 14K gold sheet is a mess because the sheet was not initially forced to be aligned like the
 * RG 14/20."
 *
 * Cause (charm-nest-1.html, feedTurn): Rose Gold was the one metal exempt from "a big batch goes on a sheet three orders at a time".
 * 10K and 14K got their green line, their leftover sheets and the block weights (job.block) on 7 Oct, but feedTurn still asked
 * `sh.metal === "rose"`, so a released batch of 10K or 14K charms was seated three orders at a time: each little group saw only
 * itself, was seated against the last group, and the front of the block came out ragged. Rose Gold sees the whole batch in one
 * search and packs it as one tight block with a straight front.
 *
 * Offline and deterministic: the page's own feedTurn and buildJob (sliced out of charm-nest-1.html and run in a vm), then the
 * real solver on one batch through the stand-in intake of pocket-fill-lib.cjs. Nothing here touches the network, Firestore or
 * a paid call.                                                                                                              */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const L = require('./pocket-fill-lib.cjs'), Rose = require('../../charm-nest-rose.js');
const html = fs.readFileSync(path.join(L.REPO, 'charm-nest-1.html'), 'utf8');
const CUT = ['rose', 'gold10k', 'gold14k'], PLAIN = ['gold', 'silver'];

(async () => {
  /* 1. The page: a batch released on a sheet of a metal with a green line is placed in ONE search; Gold and Silver keep the
        three-orders-at-a-time feed. */
  {
    const ctx = L.pageRules(L.REPO, { pages: [], work: [], overflow() {} });
    const items = Array.from({ length: 12 }, (_, i) => ({ id: 'c' + i, order: 'o' + i, orderDate: 1000 + i }));
    for (const metal of CUT) {
      const sh = { metal, runId: 'run-1', placements: [], topup: null }, got = ctx.feedTurn(sh, items);
      assert.equal(got.length, 12, metal + ': the whole batch is placed in one search');
      assert.equal(sh.feedWait, null, metal + ': no order waits for a later turn');
    }
    for (const metal of PLAIN) {
      const sh = { metal, runId: 'run-1', placements: [], topup: null }, got = ctx.feedTurn(sh, items);
      assert.equal(got.length, 3, metal + ': three orders at a time, as before');
      assert.equal(sh.feedWait.length, 9, metal + ': the rest wait their turn');
    }
    // (a sheet with no run, or the learned flow, was never fed: nothing changes there)
    assert.equal(ctx.feedTurn({ metal: 'gold', placements: [], topup: null }, items).length, 12);
  }

  /* 2. The job: one tight block from the sheet's start, left to right, for all three metals; Gold and Silver are not. */
  {
    const FEED = html.slice(html.indexOf('/* A big batch goes onto a sheet'), html.indexOf('/* A stopped run starts none'));
    const jc = { S: { settings: { maxFill: .8, clearancePt: 0, insetPt: 1.5, budgetS: 180 } }, stockFor: () => ({ wPt: 283, hPt: 142 }), activeCharms: s => s.charms, angleSet: () => [0, 10], packingKey: () => '', CharmNestRose: Rose };
    vm.createContext(jc);
    vm.runInContext(FEED, jc);
    vm.runInContext(html.slice(html.indexOf('const CAREFUL_ANGLES'), html.indexOf('function packingKey(')), jc);
    for (const metal of CUT) {
      const job = jc.buildJob({ metal, runId: 'run-1', charms: [{ id: 'a', order: 'o', orderDate: 1 }], placements: [] });
      assert.equal(job.block, true, metal + ': the block weights');
      assert.equal(job.nearFullContact, false, metal + ': built from the left, not from the contacts');
      assert.equal(job.careful, true, metal + ': one charm at a time');
      assert.equal(job.angles.length, 180, metal + ': at 2° steps');
    }
    for (const metal of PLAIN) assert.equal(jc.buildJob({ metal, runId: 'run-1', charms: [{ id: 'a', order: 'o', orderDate: 1 }], placements: [] }).block, false, metal + ' is not a block');
  }

  /* 3. The solver, one released batch of nine orders (more than the three the old feed took): 10K and 14K come out exactly as
        Rose Gold does, the same charms at the same spots (the three orders at a time of the old feed lay them differently). */
  {
    const stock = { wPt: L.PT(100), hPt: L.PT(50) }, K = 4;
    const rect = (id, w, h, date) => { const W = Math.round(w * K), H = Math.round(h * K); return { id, name: id, w: W, h: H, scale: K, bits: new Uint8Array(W * H).fill(1), areaPt2: w * h, widthPt: w, heightPt: h, order: id, orderDate: date, hash: id }; };
    const sizes = [[34, 22], [20, 30], [26, 16], [14, 28], [30, 12], [18, 18], [12, 24], [22, 14], [16, 20]];
    const orders = () => sizes.map(([w, h], i) => ({ id: 'o' + i, kind: 'x', pieces: [rect('o' + i, w, h, 1000 + i)] }));
    const key = run => run.pages.map(p => p.placements.map(q => [q.id, q.angle, +q.cxPt.toFixed(2), +q.cyPt.toFixed(2)].join(':')).sort().join(',')).join(' | ');
    const job = { angles: [0, 90] }, runs = {};
    for (const metal of CUT) runs[metal] = await L.runStream({ metal, sheet: stock, orders: orders(), batch: 9, seed: 1, job });
    assert.equal(key(runs.gold10k), key(runs.rose), '10K lies exactly as Rose Gold does');
    assert.equal(key(runs.gold14k), key(runs.rose), '14K lies exactly as Rose Gold does');
    for (const metal of CUT) { assert(runs[metal].pages.every(p => !p.placements.length || p.verification.ok), metal + ': every sheet verified'); assert.equal(runs[metal].pages[0].placements.length, 9, metal + ': the batch is whole on the sheet'); }
  }

  console.log('Gold block alignment OK: feedTurn places a whole batch of 10K or 14K (as Rose Gold) and still feeds Gold and Silver three orders at a time; buildJob marks the three green-line metals as blocks; a released batch lies on 10K and 14K exactly as on Rose Gold');
})().catch(e => { console.error(e); process.exitCode = 1; });
