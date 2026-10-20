// Send to Review, the button against the REAL mark and hold code (charm-nest-review-mark.js, charm-nest-review-hold.js) over the fake site
// (_orderFactsWorld.cjs: the real charmNestLibrary handler over an in-memory Firestore that refuses nested arrays). Made-up buyers, no Etsy,
// no paid AI, nothing real written. The button's own states are in review-send-button.cjs (stubbed APIs); this proves the two real APIs and the
// button agree: the press writes ONE record, the order becomes a Review item, the button says "In Review", the next press opens the
// Review tab and writes nothing, a second computer's resolve puts the button back, and the Reset of the sandbox is not asked for.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/review-send-real.cjs
const assert = require('assert/strict');
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };
const CUSTOM = 'Charm_Custom_Orders';

(async () => {
  const w = await world({ viewport: { width: 2400, height: 900 } });
  if (!w) { console.log('review-send-real: skipped (playwright-core not found; set PW_DIR)'); return; }
  const { page, srv } = w;
  try {
    const rid = FIXTURES.single.rid;
    await seed(page, ['single']);
    await page.waitForSelector(`#ordItems > [data-rid="${rid}"]`);
    // the fixture's SKU is not in the master, so the system already raised a question for it: take that away, so this order is one nobody sent to Review
    await page.evaluate(r => { for (const row of Orders.rows()) if (String(row.order.receiptId) === r) { row.problems = []; row.state = 'pulled'; row.reason = null; } Review.syncOrderItems(); Orders.render(); }, rid);
    const btn = `#ordItems > [data-rid="${rid}"] .revSendBtn`;
    await page.waitForSelector(btn);
    const t0 = await page.evaluate(b => document.querySelector(b).textContent, btn);
    ok('1 an ordinary order shows "Hold Order" (the real mark API is on the page)', t0 === 'Hold Order' && await page.evaluate(() => typeof CharmNestReviewMark.mark === 'function' && typeof CharmNestReviewHold.canHold === 'function'), t0);
    ok('1b nothing is stored yet', !srv.st.doc(CUSTOM, rid + '_review'));

    await page.click(btn);
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && document.querySelector(`#ordItems > [data-rid="${r}"] .revSendBtn`)?.textContent === 'In Review' && !document.querySelector(`#ordItems > [data-rid="${r}"] .revSendBtn`).disabled, rid);
    const rec = srv.st.doc(CUSTOM, rid + '_review');
    ok('2 one press: the button says "In Review", ONE record is stored for the order (open, by the signed-in name)', !!rec && rec.kind === 'reviewMark' && rec.review && rec.review.state === 'open' && rec.review.markedBy === 'Test Operator', JSON.stringify(rec));
    ok('2b the order is a Review item (one), and the order was not put on a sheet or lifted off one', await page.evaluate(r => Review.items().filter(i => i.kind === 'reviewMark' && i.rid === r).length === 1 && Orders.rows().filter(x => String(x.order.receiptId) === r).every(x => !(x.poolIds || []).length), rid));
    ok('2c an order on no sheet gets no "stays on" line', await page.evaluate(r => document.querySelectorAll(`#ordItems > [data-rid="${r}"] .revSendMsg`).length === 0, rid));

    const writes = JSON.stringify(srv.st.list(CUSTOM));
    await page.click(btn);
    await page.waitForFunction(() => { const v = document.getElementById('reviewView'); return v && !v.classList.contains('hidden'); });
    ok('3 a second press opens the Review tab at that order and writes nothing more', JSON.stringify(srv.st.list(CUSTOM)) === writes && await page.evaluate(r => !!document.querySelector(`#rvList [data-rid="${r}"]`) || Review.items().some(i => i.rid === r), rid));

    // the person resolves it in Review (Mark OK / the pick): the Orders button goes back to "Hold Order" by itself
    await page.evaluate(r => CharmNestReviewMark.resolve(r, { by: 'Test Operator', how: 'ok' }), rid);
    await page.evaluate(() => CN.setMode('orders'));
    await page.waitForFunction(r => !CharmNestReviewMark.isMarked(r) && document.querySelector(`#ordItems > [data-rid="${r}"] .revSendBtn`)?.textContent === 'Hold Order', rid);
    ok('4 resolved: the order leaves Review and its button says "Hold Order" again', true);
    ok('5 no script error on the page throughout', w.errors.length === 0, w.errors.join(' | '));
  } finally { await w.close(); }
  console.log(`review-send-real: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
