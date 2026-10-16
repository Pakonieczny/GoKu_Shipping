// pieces-downstream.cjs part E: the REAL sorter page (Chromium, offline fake site, real drawings, real intake / pool / classifier / fit: _engraveRealWorld.cjs).
// A made-up order shaped like Paul's: ONE line of one charm x2 ("COMET_9427" stands for CORGI 34728), the buyer's two entries "Anna" and "Ben". Nothing live, no paid reading, no Etsy.
'use strict';
module.exports = async function pageParts({ R, t, assert, RID, Readiness }) {
  const V = (n, v) => ({ name: n, value: v });
  R.ORDERS.pc2 = Object.assign({}, R.ORDERS.back, { rid: '4290000151', listing: '1890000151', buyer: 'Name Eleven', placed: { day: 0, h: 11, m: 10 }, qty: 2, personalization: ['Anna', 'Ben'], variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')] });
  R.EXPECT_JOBS.pc2 = 2;
  const w = await R.world({ viewport: { width: 1356, height: 900 }, reducedMotion: 'reduce' });
  if (!w) { console.log('  SKIP E (no playwright-core)'); return; }
  const page = w.page, cons = []; page.on('console', m => { if (m.type() === 'error') cons.push(m.text()); });
  const plain = x => JSON.parse(JSON.stringify(x));
  try {
    const made = await R.build(w, ['pc2', 'discs']);
    const lineKey = made.pc2.key, discKey = made.discs.key;
    const jobsOf = key => page.evaluate(k => Engrave.jobsOf(B.orders.byKey.get(k)).filter(j => j.face !== 'front').map(j => ({ key: j.key, slot: j.slot || null, state: j.state, text: j.text, copies: j.copies.slice() })), key);
    const view = key => page.evaluate(k => {
      const row = B.orders.byKey.get(k), dec = CharmNestReadiness.decisions(Orders.rows()), ids = row.poolIds.slice();
      const rep = CharmNestReadiness.sheet({ id: 'sh-e', poolIds: ids, engraving: dec, placedCount: ids.length });
      return { approved: !!(row.engrave && row.engrave.approved), pieces: ids.map(id => !!(dec[id] && dec[id].approved)), waiting: rep.waiting, gate: rep.stages.approval, pools: Engrave.engravePoolsOf(row), ready: rep.ready };
    }, key);
    const rail = () => page.evaluate(() => { const b = document.querySelector('#owRail .tlStop[data-stage="engraved"]'); return b ? b.className.replace('tlStop', '').trim() : null; });
    const approve = jobKey => page.evaluate(async k => { const j = Engrave.items().get(k); await Engrave.approve(j, 'Paul', document.createElement('button')); return j.state; }, jobKey);
    const settleRail = async (want, ms = 12000) => { const t0 = Date.now(); let r; while (Date.now() - t0 < ms) { r = await rail(); if (want(r)) return r; await R.settle(page, 300); } return r; };

    await t('E1', 'the qty-2 charm is TWO pieces: a job each (C1 Anna, C2 Ben), a pool id each; the discs order keeps D1..D3', async () => {
      const js = await jobsOf(lineKey);
      assert.deepEqual(plain(js.map(j => [j.slot, j.text])), [['C1', 'Anna'], ['C2', 'Ben']]);
      assert.ok(js.every(j => j.copies.length === 1) && js[0].copies[0] !== js[1].copies[0], 'one pool id each');
      assert.deepEqual(plain((await jobsOf(discKey)).map(j => [j.slot, j.text])), [['D1', 'A'], ['D2', 'B'], ['D3', 'C']]);
    });
    await t('E2', 'nothing approved: the sheet gate waits for both pieces and the rail\'s Engraved is not done', async () => {
      const v = await view(lineKey);
      assert.deepEqual(plain(v.pieces), [false, false]); assert.equal(v.waiting, 2); assert.equal(v.gate, false); assert.equal(v.approved, false);
      assert.equal(v.pools.length, 2, 'the timeline is told to wait for two pieces');
      await page.evaluate(async k => { CN.setMode('orders'); await OrderWin.open(k); }, lineKey);
      await page.waitForSelector('#owRail .tlStop[data-stage="engraved"]', { timeout: 20000 });
      const r = await settleRail(x => x && /\b[cf]\b/.test(x)); assert.ok(r && !/\bd\b/.test(r), 'Engraved is not done: ' + r);
    });
    await t('E3', 'piece 1 approved: the line is HALF approved, the gate waits for the other ONE, the rail still waits; piece 2 has no seal', async () => {
      const [j1, j2] = await jobsOf(lineKey);
      assert.equal(['approved', 'written'].includes(await approve(j1.key)), true);
      const v = await view(lineKey);
      assert.deepEqual(plain(v.pieces), [true, false]); assert.equal(v.waiting, 1); assert.equal(v.gate, false); assert.equal(v.approved, false); assert.equal(v.ready, false);
      const r = await settleRail(x => x && !/\bd\b/.test(x), 6000); assert.ok(r && !/\bd\b/.test(r), 'Engraved must wait for piece 2: ' + r);
      const seals = await page.evaluate(({ a, b }) => [a, b].map(k => CNEngravingSeals.list(Engrave.items().get(k)).filter(s => s.how === 'engraveApproved').length), { a: j1.key, b: j2.key });
      assert.deepEqual(plain(seals), [1, 0]);
      assert.equal((await jobsOf(lineKey))[1].state !== 'approved', true, 'piece 2 is untouched');
    });
    await t('E4', 'piece 2 approved: now the gate opens and the rail\'s Engraved is done; each piece has exactly its own seal and words', async () => {
      const [j1, j2] = await jobsOf(lineKey);
      assert.equal(['approved', 'written'].includes(await approve(j2.key)), true);
      const v = await view(lineKey);
      assert.deepEqual(plain(v.pieces), [true, true]); assert.equal(v.waiting, 0); assert.equal(v.gate, true); assert.equal(v.approved, true);
      const r = await settleRail(x => x && /\bd\b/.test(x), 15000); assert.ok(r && /\bd\b/.test(r), 'Engraved is done: ' + r);
      const info = await page.evaluate(({ a, b }) => [a, b].map(k => { const j = Engrave.items().get(k); return { text: j.text, seals: CNEngravingSeals.list(j).filter(s => s.how === 'engraveApproved').length, copies: j.copies.slice() }; }), { a: j1.key, b: j2.key });
      assert.deepEqual(plain(info.map(x => [x.text, x.seals])), [['Anna', 1], ['Ben', 1]]); assert.notEqual(info[0].copies[0], info[1].copies[0]);
    });
    await t('E5', 'discs are as before (a job and a seal per disc, D1..D3) and the gate waits for the discs not approved', async () => {
      const js = await jobsOf(discKey); assert.equal(js.length, 3);
      assert.equal(['approved', 'written'].includes(await approve(js[0].key)), true);
      const v = await view(discKey); assert.deepEqual(plain(v.pieces), [true, false, false]); assert.equal(v.waiting, 2); assert.equal(v.approved, false);
      const keys = await page.evaluate(k => Engrave.jobsOf(B.orders.byKey.get(k)).map(j => j.key), discKey);
      assert.deepEqual(plain(keys.map(x => x.replace(/^\d+_\d+/, ''))), ['#D1', '#D2', '#D3']);
    });
    await t('E6', 'no console error on the page', async () => {
      assert.deepEqual(cons.filter(x => !/favicon|Failed to load resource|net::ERR|ResizeObserver/i.test(x)), []);
    });
  } finally { await w.close(); }
};
