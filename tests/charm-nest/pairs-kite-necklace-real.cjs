// The kite necklace in the REAL-DRAWING world (KITEPAIR; Paul, 13 Oct 2026, order 4199929230). Headless Chromium over the repo's fake site, offline: no Etsy, no paid call, nothing live.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/pairs-kite-necklace-real.cjs        (skips with one line when there is no playwright-core)
// The order, as Paul saw it: title "Kite Necklace ... Kite Shape Earrings ...", SKU KITE, options Metal Choice "Silver + Engrave" and Necklace Length 20", one personalisation word. The app made a pair of
// earrings of it ("1 pair", "2 pieces: Left and Right", the drawing twice with the Right mirrored, a Left and a Right engraving card). pairs-kite-necklace.cjs holds the reading; this holds what the page DOES:
//   1  the kite necklace (SKU KITE, and the master's own BEADY KITE): ONE piece, one drawing, ONE engraving job and card, no Left or Right, no "1 pair"; a stud earring pair beside it is still a pair
//   2  the order already made as a pair before the fix (the old reading is put back in with a stand-in recorded from main, the page pools it as two pieces, Left and Right, as the live order was): once the
//      reading is the fixed one, the app's own Hold and then Release hold (no new code, no bulk, one order) make the line up again as ONE piece with ONE engraving card
// The drawings are the repo's real fixture (COMET_9427) standing in for the kite; every name is made up.
'use strict';
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const R = require('./_engraveRealWorld.cjs');
const V = (name, value) => ({ name, value });
const KITE_TITLE = 'Kite Necklace Kite Pendant Gift for Her Kite Jewelry Kite Charm Kite Gift Kite Shape Earrings Silver Kite Necklace Birthday Gift';
const comet = JSON.parse(fs.readFileSync(path.join(R.root, 'tests/charm-nest/fixtures/comet-master-entries.json'), 'utf8')).entries.find(e => e.sku === 'COMET_9427');
for (const sku of ['KITE', 'BEADY KITE']) R.DESIGNS[sku] = { file: 'COMET_9427.ai', entry: Object.assign({}, comet, { sku }) };
const kite = (rid, listing, sku, buyer, min) => ({ rid, sku, listing, title: KITE_TITLE, buyer, placed: { day: 0, h: 10, m: min }, ship: 5, variations: [V('Metal Choice', 'Silver + Engrave'), V('Necklace Length', '20"')], personalization: ['Name One'] });
R.ORDERS.kite = kite('4290000131', '1890000131', 'KITE', 'Name Nine', 20);
R.ORDERS.kiteMaster = kite('4290000132', '1890000132', 'BEADY KITE', 'Name Ten', 21);
R.ORDERS.kiteOld = kite('4290000133', '1890000133', 'KITE', 'Name Eleven', 22);   // (read as a pair by the stand-in below while it is made up: the live order's state)
Object.assign(R.EXPECT_JOBS, { kite: 1, kiteMaster: 1, kiteOld: 2 });
const NAMES = ['back', 'earrings', 'kite', 'kiteMaster', 'kiteOld'], OLD = R.ORDERS.kiteOld.rid;

let passed = 0; const ok = (c, m) => { assert.ok(c, m); passed++; console.log('  ✓ ' + m); };
(async () => {
  const w = await R.world({ shots: process.env.SHOTS || '/tmp/kitepair-shots' });
  if (!w) { console.log('  – no playwright-core: not run'); return; }
  try {
    // the OLD reading of the kite line, as main made it (recorded 13 Oct: earring true, soldAs pair, two pieces, sides L and R, kind pair), for the one made-up order "kiteOld"
    await w.page.evaluate(listing => {
      const O = window.CharmNestOrders, real = O.interpretLine; window.__realRead = real;
      O.interpretLine = function (order, line, ctx) { const s = real.apply(this, arguments); if (String(line && line.listingId) === listing) { Object.assign(s.pair, { earring: true, soldAs: 'pair', perUnit: 2, kind: 'pair', sides: ['L', 'R'] }); s.pieceCount = 2; } return s; };
    }, R.ORDERS.kiteOld.listing);
    const made = await R.build(w, NAMES);
    const read = (rid) => w.page.evaluate(rid => { const r = Orders.rows().find(x => x.order.receiptId === rid), s = r.spec, p = s.pair; return { state: r.state, hold: r.hold || null, poolIds: r.poolIds.slice(), pieceCount: s.pieceCount, earring: p.earring, kind: p.kind, sides: p.sides, legacy: !!p.legacy, lineForm: s.lineForm, never: !!s.engraveNever,
      jobs: [...Engrave.items().values()].filter(j => j.row === r || (j.row && j.row.parentRow === r)).map(j => j.slot || null) }; }, rid);
    const text = async name => { await R.openOrderWindow(w, name); return w.page.evaluate(() => { const ow = document.getElementById('orderWin'); const q = [...document.querySelectorAll('#owMeta .m')].find(m => m.querySelector('i') && m.querySelector('i').textContent === 'Quantity');
      return { text: ow ? ow.innerText : '', meta: (document.getElementById('owMeta') || {}).textContent || '', qty: q ? q.querySelector('span').textContent.replace(/\s+/g, ' ').trim() : null, cards: document.querySelectorAll('#owEng .engCard').length, sides: [...document.querySelectorAll('#orderWin .owPcSide')].map(e => e.textContent.trim()) }; }); };
    const close = () => w.page.evaluate(() => { if (window.OrderWin && OrderWin.close) OrderWin.close(); });

    // ── 1 · the kite necklace, under both SKUs ──
    for (const name of ['kite', 'kiteMaster']) {
      const r = await read(R.ORDERS[name].rid), sku = R.ORDERS[name].sku;
      ok(!r.earring && r.pieceCount === 1 && r.kind === 'single' && r.poolIds.length === 1 && r.sides.every(x => x === null), `${sku}: one piece, no pair, no side (pool ids ${r.poolIds.length})`);
      ok(r.lineForm === 'necklace' && !r.never, `${sku}: a necklace that keeps its engraving`);
      ok(r.jobs.length === 1 && r.jobs[0] === null, `${sku}: ONE engraving job, none for a Left or a Right (${JSON.stringify(r.jobs)})`);
      const t = await text(name);
      // (HDRHEAD, Paul 16 Oct 2026: the header lost its "buyer · N pieces · ship by" line, the count is told where it already was: the Quantity fact of Details; a pair adds a "1 pair" chip)
      ok(t.qty === '1' && !/\b2 pieces\b|\b1 pair\b/.test(t.text + ' ' + t.meta), `${sku}: the order window says Quantity 1, not "2 pieces" or "1 pair" (${JSON.stringify(t.qty)})`);
      ok(t.sides.length === 0 && !/\bLeft\b|\bRight\b/.test(t.text), `${sku}: no Left or Right anywhere in the order window`);
      ok(t.cards === 1, `${sku}: ONE engraving card (${t.cards})`);
      await close();
    }
    {   // the control: a stud earring pair beside them is still a pair, a Left and a Right, never engraved
      const r = await read(R.ORDERS.earrings.rid);
      ok(r.earring && r.pieceCount === 2 && r.sides.join('') === 'LR' && r.poolIds.length === 2 && r.jobs.length === 0, 'the stud earring pair beside them: still a pair, a Left and a Right, no engraving job');
      const t = await text('earrings'); ok(/\b1 pair\b/.test(t.text) && /1 pair · 2 pieces: Left and Right/.test(t.qty) && t.sides.length === 2, 'its order window still says 1 pair, Left and Right (' + t.qty + ')'); await close();
    }

    // ── 2 · the order made as a pair before the fix ──
    {
      const was = await read(OLD);
      ok(was.earring && was.pieceCount === 2 && was.poolIds.length === 2 && was.sides.join('') === 'LR' && was.jobs.slice().sort().join('') === 'LR', 'the stand-in reproduces the live order: pooled as two pieces, Left and Right, a Left and a Right engraving job');
      await w.page.evaluate(() => { window.CharmNestOrders.interpretLine = window.__realRead; for (const r of Orders.rows()) r.spec = null; Orders.interpretAll(); Orders.render(); });   // the fix is live: the page reads every line again
      const re = await read(OLD);
      ok(!re.earring && re.legacy && re.pieceCount === 2 && re.poolIds.length === 2, 'the fixed reading keeps the pieces the line was already made as (pinned: nothing is added to the pool or taken from it by a re-read)');
      const plan = await w.page.evaluate(async rid => { const p = await OrderHold.plan(rid, { exact: false }); return { canHold: p.canHold, why: p.blockedWhy, pieces: p.pieces.map(x => x.state) }; }, OLD);
      ok(plan.canHold && plan.pieces.join() === 'onSheet,onSheet', 'Hold can take the order\'s two pieces off their sheet');
      const held = await w.page.evaluate(async rid => { const r = await OrderHold.run(rid, { name: 'Test Operator' }); return { ok: r.ok, held: r.held, error: r.error || null }; }, OLD);
      ok(held.ok && held.held, 'Hold: the order is on hold, its pieces are off every sheet');
      const rel = await w.page.evaluate(async rid => { const r = await OrderHold.release(rid, { name: 'Test Operator' }); return { ok: r.ok, released: r.released, error: r.error || null }; }, OLD);
      ok(rel.ok && rel.released, 'Release hold: the order is back in the queue');
      await w.page.waitForTimeout(800);
      const now = await read(OLD);
      ok(!now.earring && now.pieceCount === 1 && now.kind === 'single' && now.sides.every(x => x === null) && now.poolIds.length === 0 && !now.legacy, `made up again at the fixed reading: ONE piece, no side (${JSON.stringify({ pieces: now.pieceCount, kind: now.kind, state: now.state })})`);
      const t = await text('kiteOld');
      ok(t.qty === '1' && !/\b2 pieces\b|\b1 pair\b/.test(t.text + ' ' + t.meta) && t.sides.length === 0 && !/\bLeft\b|\bRight\b/.test(t.text), 'its order window: Quantity 1 (HDRHEAD: no count in the header), no pair, no Left or Right');
      ok(t.cards === 1, `ONE engraving card for it (${t.cards})`); await close();
    }
    ok(w.errors.length === 0, 'no page error (' + w.errors.join(' | ').slice(0, 200) + ')');
    console.log(`pairs-kite-necklace-real: ${passed} checks passed`);
  } finally { await w.close(); }
})().catch(e => { console.error(e); process.exit(1); });
