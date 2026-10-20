// SENDREVIEW (Paul, 20 Oct 2026: "The Send to Review button is disconnected from reality ... This button should be directly linked and communicate with the orders that are already in the
// Review tab, and this communication needs to be real time"). A custom "own design" order (an add-on listing: no master design, waits for its own designs or Complete Order) is a card of
// Review's Open (Custom Orders) the moment it is pulled: its Orders card says "In Review", not "Send to Review"; a press opens that Review card and marks nothing; Complete Order pressed on the
// Review card takes it out of Review and the Orders card (on screen, no reload) says "Send to Review" again, with the Review tab's count one less.
// Offline only: the real sorter page in headless Chromium over the fake site (_orderFactsWorld.cjs), made-up buyers, nothing real written.
//   node tests/charm-nest/review-send-custom.cjs [playwright-core dir]   (or PW_DIR=<dir>)
const assert = require('assert/strict');
if (process.argv[2] && !process.env.PW_DIR) process.env.PW_DIR = process.argv[2];
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };

(async () => {
  const w = await world({ viewport: { width: 1800, height: 900 } });
  if (!w) { console.log('review-send-custom: skipped (playwright-core not found; pass its dir or set PW_DIR)'); return; }
  const { page } = w;
  try {
    FIXTURES.ownDesign = { rid: '4195000091', sku: 'DAINTY_1450', title: 'Add a Custom Piece to your Jewelry!', listing: '595264213', buyer: 'Holly Fixture', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [{ name: 'Metal choice', value: 'Gold' }] };
    const rid = FIXTURES.ownDesign.rid, btn = `#ordItems > [data-rid="${rid}"] .revSendBtn`;
    await seed(page, ['single', 'ownDesign']);
    await page.waitForSelector(btn);

    const facts = await page.evaluate(rid => { const r = Orders.rows().find(x => String(x.order.receiptId) === rid), it = Review.openOrders().get(rid); return { own: !!(r && r.spec && r.spec.ownDesign), kind: it && it.kind, key: it && it.key, marked: CharmNestReviewMark.isMarked(rid), decision: Review.items().some(x => (x.rows || [x.row]).some(y => y && String(y.order.receiptId) === rid)) }; }, rid);
    // (the root cause: it is no decision, Review.items(), which is all the button used to ask)
    ok('1 the own-design order is a card of Review\'s Open (Custom Orders), no decision and no Send to Review mark', facts.own && facts.kind === 'customOrder' && !facts.marked && !facts.decision, JSON.stringify(facts));

    await page.waitForFunction(sel => document.querySelector(sel)?.textContent === 'In Review', btn);
    ok('2 its Orders card says "In Review" (the same list the Review tab counts)', true);

    // a press opens that card in Review, marks nothing
    await page.click(btn);
    await page.waitForFunction(() => !document.getElementById('reviewView').classList.contains('hidden'));
    const opened = await page.evaluate(rid => { const it = Review.openOrders().get(rid), mk = Review.mkeyOf(it); return { card: !!document.querySelector(`#rvList .reviewListRow[data-mkey="${CSS.escape(mk)}"]`), marked: CharmNestReviewMark.isMarked(rid) }; }, rid);
    ok('3 a press opens its Review card and adds nothing', opened.card && !opened.marked, JSON.stringify(opened));

    // back on the Orders tab: Complete Order pressed on the Review card (its own handler) takes it out of Review; the card follows with no reload
    await page.evaluate(() => CN.setMode('orders'));
    await page.waitForFunction(sel => document.querySelector(sel)?.textContent === 'In Review', btn);
    const before = await page.evaluate(() => { window.__marker = Math.random(); window.__node = document.querySelector('#ordItems'); return +(document.getElementById('tabReviewN').textContent || 0); });
    await page.evaluate(rid => CustomPrint.complete(Review.openOrders().get(rid)), rid);
    await page.waitForFunction(sel => document.querySelector(sel)?.textContent === 'Send to Review', btn, { timeout: 15000 });
    const after = await page.evaluate(rid => ({ same: window.__marker !== undefined && window.__node === document.querySelector('#ordItems'), open: Review.openOrders().has(rid), n: +(document.getElementById('tabReviewN').textContent || 0) }), rid);
    ok('4 resolved in Review: the Orders card says "Send to Review" at once, no reload, and the Review tab counts one less', after.same && !after.open && after.n === before - 1, JSON.stringify({ before, after }));

    ok('5 no script error on the page throughout', w.errors.length === 0, w.errors.join(' | '));
  } finally { await w.close(); }
  console.log(`review-send-custom: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
