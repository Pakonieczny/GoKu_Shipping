// REVMENU (19 Oct 2026): the Review tab's date menu stays open (the same element) across a redraw of the tab, the one a cloud read brings every few seconds (OrderPieces.notify -> Review.render).
// Before, Review.render rewrote its bar and the menu was rebuilt closed under the pointer (filterbar.cjs's "page.click: Timeout ... [data-range=\"all\"]" flake).
//   node tests/charm-nest/review-menu-redraw.cjs [playwright-core dir]
'use strict';
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
(async () => {
  const w = await world({ viewport: { width: 1500, height: 900 }, facts: false }); const { page } = w;
  await seed(page, ['single', 'pairMatching', 'mismatched', 'multi', 'qty3', 'engraved', 'late', 'message']);
  await page.evaluate(() => CN.setMode('review')); await page.waitForTimeout(5000);
  const sel = '[data-activity-tools="review-open"]';
  await page.click(sel + ' .cnDateBtn'); await page.waitForTimeout(300);
  const before = await page.evaluate(s => { const m = document.querySelector(s + ' .cnMenu'); window.__m = m; return { open: !m.hidden }; }, sel);
  // the same path the placement feed's tick takes: OrderPieces.notify() -> bump -> subscribers (hold-ui.js follow: Review.render() after 60 ms)
  await page.evaluate(() => OrderPieces.notify()); await page.waitForTimeout(400);
  const after = await page.evaluate(s => { const m = document.querySelector(s + ' .cnMenu'); return { sameNode: m === window.__m, oldConnected: window.__m.isConnected, open: !m.hidden }; }, sel);
  let clicked = true; try { await page.click(sel + ' [data-range="all"]', { timeout: 3000 }); } catch (e) { clicked = false; }
  console.log('REPRO', JSON.stringify({ before, after, clickAllWithin3s: clicked }));
  const ok = before.open && after.sameNode && after.oldConnected && after.open && clicked;
  console.log(ok ? 'review-menu-redraw: the open date menu survives a redraw and takes the click' : 'FAIL review-menu-redraw: the date menu was rebuilt or closed by a redraw');
  process.exitCode = ok ? 0 : 1;
  await w.close();
})();
