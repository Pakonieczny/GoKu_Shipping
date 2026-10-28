// (HOLDBTN, Paul 20 Oct 2026: "Remove the 'Hold' button everywhere in the app and replace it with the 'Send to Review' and ensure it has the same look, feel, and functionality as
// the original 'Send to Review' that we are already using including the stamp and seal.")
// The REAL sorter page over the fake site (_reviewSendWorld.cjs: the real charmNest* handlers over an in-memory Firestore), two orders on one saved sheet not yet cut:
//   1  no button says exactly "Hold" on the Orders tab, the Review tab, the order window or the sheet window
//   2  the order window's piece row has ReviewSend's own button where Hold stood (the same class, words and state as the Orders row's)
//   3  pressing it marks the order (it is in Review) and nothing else (HOLDSTAYS, 28 Oct 2026): its piece stays on the uncut sheet, which is held with its set until the order is released
//   4  a second order on the same sheet held the same way: the sheet is held by both
//   PW_DIR=<playwright-core's node_modules> node tests/charm-nest/hold-btn-review.cjs   (CHROMIUM=<chrome>)
'use strict';
const assert = require('assert');
const W = require('./_reviewSendWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };

(async () => {
  const w = await W.open('production');
  if (!w) { console.log('hold-btn-review: skipped (playwright-core not found; set PW_DIR)'); return; }
  const { page } = w;
  try {
    await W.addOrders(w, ['sheeted', 'mate']);
    const sh = await W.seedSheet(w, { id: 'sh-hb1', orders: ['sheeted', 'mate'] });
    const rid = W.ORDERS.sheeted.rid, key = W.keyOf('sheeted'), pid = W.poolIdOf('sheeted');
    // every visible button under `sel` whose words are exactly "Hold"
    const holds = sel => page.evaluate(s => [...document.querySelectorAll(`${s} button`)].filter(b => b.offsetParent !== null && b.textContent.trim() === 'Hold').map(b => b.outerHTML.slice(0, 120)), sel);

    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await page.waitForSelector(`#ordItems [data-rid="${rid}"] .revSendBtn`);
    const label = await page.evaluate(() => ReviewSend.LABEL);
    ok('1a Orders tab: no button says "Hold"', (await holds('#ordersView')).length === 0, JSON.stringify(await holds('#ordersView')));
    await page.evaluate(() => { CN.setMode('review'); Review.render(); }); await W.settle(page, 300);
    ok('1b Review tab: no button says "Hold"', (await holds('#reviewView')).length === 0, JSON.stringify(await holds('#reviewView')));
    await page.evaluate(id => SheetWin.open(id), sh.id);
    await page.waitForFunction(() => SheetWin.isOpen && SheetWin.isOpen());
    await W.settle(page, 500);
    ok('1c sheet window: no button says "Hold"', (await holds('dialog[open]')).length === 0, JSON.stringify(await holds('dialog[open]')));
    await page.evaluate(() => SheetWin.close && SheetWin.close()); await W.settle(page, 400);

    await page.evaluate(() => CN.setMode('orders'));
    await page.evaluate(k => OrderWin.open(k), key);
    const rb = '#owPcSum .revSendBtn[data-rev-host]';
    await page.waitForSelector(rb, { timeout: 20000 });
    ok('1d order window: no button says "Hold"', (await holds('#orderWin')).length === 0, JSON.stringify(await holds('#orderWin')));
    const b0 = await page.evaluate(s => { const b = document.querySelector(s); return { text: b.textContent.trim(), cls: b.className, rid: b.dataset.rid }; }, rb);
    ok('2 the piece row has the Send to Review button where Hold stood: the same class and words as the Orders row', b0.text === label && /\bbtn ghost sm revSendBtn\b/.test(b0.cls) && b0.rid === rid, JSON.stringify(b0));

    // 3 · the press (HOLDSTAYS, Paul 28 Oct 2026: "The 'Hold' button should NOT remove a given charm off a sheet ... It just puts the sheet and the set of sheets on hold"): the order is
    //     marked by the REAL mark API and nothing else: the old Hold's engine (OrderHold.plan / run, through HoldUI.takeOff) is never asked, the piece stays on its sheet, and the sheet is held
    await page.evaluate(() => {
      const T = window.__eng = { plans: [], runs: [], real: window.OrderHold };
      window.OrderHold = Object.assign({}, T.real, { plan: async r => { T.plans.push(r); return T.real.plan(r); }, run: async (r, o) => { T.runs.push(r); return T.real.run(r, o); } });
    });
    await page.click(rb);
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r), rid, { timeout: 30000 });
    await page.waitForFunction(() => /Stays on/.test([...document.querySelectorAll('.revSendMsg')].map(n => n.textContent).join(' ')), null, { timeout: 8000 }).catch(() => {});
    const eng = await page.evaluate(() => JSON.parse(JSON.stringify({ plans: window.__eng.plans, runs: window.__eng.runs })));
    ok('3a the press marks the order: it is in Review', await page.evaluate(r => CharmNestReviewMark.isMarked(r) && Review.openOrders().has(r), rid));
    ok('3b nothing is taken off: the old Hold\'s engine is never asked (no plan, no run)', eng.plans.length === 0 && eng.runs.length === 0, JSON.stringify(eng));
    const msg = await page.evaluate(() => [...document.querySelectorAll('.revSendMsg')].map(n => n.textContent).join(' | '));
    const held = await page.evaluate(id => CharmNestReviewHold.isHeld(id), sh.id), pool = w.srv.st.doc('Charm_Pool', pid);
    const row = await page.evaluate(r => Orders.rows().filter(x => String(x.order.receiptId) === r).map(x => ({ hold: x.hold || null, pools: (x.poolIds || []).length })), rid);
    ok('3c the piece stays on its sheet (its pool row, its line), the sheet is held by the Review, and the line under the button says so',
      held && pool && pool.sheetId === sh.id && row.every(x => !x.hold && x.pools > 0) && msg.includes('Stays on ' + sh.name), JSON.stringify({ held, sheetId: pool && pool.sheetId, row, msg }));

    // 4 · the other order on the sheet, held the same way: the sheet stays held while either is in Review
    await page.evaluate(() => { window.OrderHold = window.__eng.real; });
    const mate = W.ORDERS.mate.rid;
    await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await W.settle(page, 400);
    await page.evaluate(k => OrderWin.open(k), W.keyOf('mate'));
    await page.waitForFunction(r => document.querySelector(`#owPcSum .revSendBtn[data-rev-host][data-rid="${r}"]`), mate, { timeout: 20000 });
    await page.click(`#owPcSum .revSendBtn[data-rev-host][data-rid="${mate}"]`);
    await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r), mate, { timeout: 30000 });
    const pool2 = w.srv.st.doc('Charm_Pool', W.poolIdOf('mate')), held2 = await page.evaluate(id => CharmNestReviewHold.heldBy(id).map(h => h.rid), sh.id);
    ok('4 the second order held too: its piece stays, and the sheet is held by both orders', pool2 && pool2.sheetId === sh.id && held2.includes(rid) && held2.includes(mate), JSON.stringify({ held2, sheetId: pool2 && pool2.sheetId }));
    ok('5 no script error on the page throughout', w.errors.length === 0, (w.errors || []).join(' | '));
  } finally { await w.close(); }
  console.log(`hold-btn-review: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
