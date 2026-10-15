// ADV-REVIEW (final adversarial test, Paul, 15 Oct 2026): Send to Review and Mark OK when the connection is lost, a reply is lost, a second bench got there first, the page is
// reloaded half way, and the order is cancelled while it sits in Review. The REAL page in headless Chromium over the repo's fake site (tests/charm-nest/_reviewSendWorld.cjs): the real
// charmNestLibrary handler over an in-memory Firestore. Nothing leaves the machine; every buyer is made up. Production workspace (the sandbox runs the same code: review-send-e2e).
//   1  no connection: Send to Review says so in one line, marks nothing (page and cloud), and a press once back online marks the order ONCE
//   2  the cloud saved the mark but its reply was lost: the page says "not sent", the press again finds it already there (no second cycle, same time)
//   3  Mark OK with no connection: the order stays in Review, the sheet stays held, one toast; the press again once online resolves it
//   4  Mark OK saved but its reply lost: the page says "not saved", then agrees with the cloud (resolved, sheet released) by itself; nothing more is written
//   5  a second bench resolved it first: a stale page pressing Mark OK ends resolved, no error, the first person's name stays, no second timeline line
//   6  a reload while Mark OK is in flight: the page afterwards says what the cloud says, whichever way the request went
//   7  the order is cancelled (Etsy) while in Review and its piece stays on a committed sheet: the sheet is NOT held for a card nobody can see
//   0  nothing left the fake site and the page threw no error
'use strict';
const assert = require('assert');
const W = require('./_reviewSendWorld.cjs');
const { ORDERS } = W;

const J = x => JSON.stringify(x);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const until = async (fn, ms = 8000, every = 120) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return v; await sleep(every); } };
const results = [];
const CARD = rid => `#reviewView [data-mkey="ord:rvm:${rid}"]`;

(async () => {
  const w = await W.open('production');
  if (!w) { console.log('  - no playwright-core / chrome: not run'); return; }
  const st = w.srv.st, P = w.prefix, page = w.page, ctx = page.context();
  page.setDefaultTimeout(8000);
  const errors = []; page.on('pageerror', e => errors.push(String(e.message || e)));
  const rec = rid => st.doc(P + 'Charm_Custom_Orders', rid + '_review');
  const marked = rid => page.evaluate(rid => CharmNestReviewMark.isMarked(rid), rid);
  const held = sid => page.evaluate(sid => CharmNestReviewHold.isHeld(sid), sid);
  const toasts = () => page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent.trim()));
  const btn = rid => page.evaluate(rid => { const b = document.querySelector(`#ordItems [data-rid="${rid}"] .revSendBtn`); const m = document.querySelector(`#ordItems [data-rid="${rid}"] .revSendMsg`); return b ? { text: b.textContent.trim(), disabled: b.disabled, msg: m ? m.textContent.trim() : '' } : null; }, rid);
  const calls = op => st.calls.filter(c => c.op === op).length;
  // every request to the library for this op: let it through to the fake cloud, then (lose = true) drop the answer on the way back; stall = never answer
  let mode = null;
  await page.route(/charmNestLibrary/, async route => {
    let op = ''; try { op = JSON.parse(route.request().postData() || '{}').op; } catch (_) {}
    if (op !== 'reviewMarkPut' || !mode) return route.continue();
    if (mode === 'lose') { mode = null; await route.fetch(); return route.abort('failed'); }
    if (mode === 'stall') { mode = null; return; }   // (never answered: the page is reloaded under it)
    if (mode === 'stallAfter') { mode = null; await route.fetch(); return; }   // (the cloud has it; the answer never comes back)
    return route.continue();
  });
  const step = async (id, title, fn) => {
    const t0 = Date.now();
    try { await fn(); results.push({ id, title, ok: true, ms: Date.now() - t0 }); }
    catch (e) { results.push({ id, title, ok: false, why: String(e && e.message || e).split('\n').slice(0, 3).join(' | ') }); }
  };
  try {
    await W.addOrders(w, ['matched', 'alone']);
    const A = ORDERS.matched.rid, B = ORDERS.alone.rid;

    await step('1', 'no connection: Send to Review marks nothing, says so, and works once back online (once)', async () => {
      await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(page, 500);
      await ctx.setOffline(true);
      await page.click(`#ordItems [data-rid="${A}"] .revSendBtn`);
      const b = await until(async () => { const x = await btn(A); return x && x.msg ? x : null; }, 8000);
      assert(b && /could not reach the server/i.test(b.msg), 'one plain line about the connection: ' + J(b));
      assert(b.text === 'Send to Review' && !b.disabled, 'the button is back to Send to Review and can be pressed again: ' + J(b));
      assert(!(await marked(A)), 'the page does not hold the order as marked');
      assert(!rec(A), 'and the cloud has no record');
      assert(!(await page.$(CARD(A))), 'no Review card is left behind');
      await ctx.setOffline(false);
      await page.click(`#ordItems [data-rid="${A}"] .revSendBtn`);
      assert(await until(async () => rec(A) && (await marked(A))), 'marked once back online');
      await W.settle(page, 600);
      const n = [...st.docs.keys()].filter(k => k.startsWith(P + 'Charm_Custom_Orders/' + A)).length;
      assert.equal(n, 1, 'one record in the cloud, not two: ' + n);
      assert.equal(calls('reviewMarkPut'), 1, 'one write reached the cloud in all (the first press never left the machine)');
    });

    await step('2', 'the cloud kept the mark but the answer was lost: "not sent", and the press again finds it already there', async () => {
      const first = rec(B); assert(!first);
      mode = 'lose';
      await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await W.settle(page, 400);
      await page.click(`#ordItems [data-rid="${B}"] .revSendBtn`);
      const b = await until(async () => { const x = await btn(B); return x && x.msg ? x : null; }, 8000);
      assert(b && /not sent|could not reach/i.test(b.msg), 'the page says it did not get the answer: ' + J(b));
      const saved = rec(B); assert(saved && saved.review.state === 'open', 'the cloud did keep it: ' + J(saved));
      const at = saved.review.markedAt;
      await page.click(`#ordItems [data-rid="${B}"] .revSendBtn`);
      assert(await until(() => marked(B)), 'the second press ends marked on the page');
      await W.settle(page, 600);
      const again = rec(B);
      assert.equal(again.review.markedAt, at, 'the cloud still holds the FIRST mark (no second cycle): ' + J(again.review));
      assert(!again.review.earlier, 'and no earlier cycle was made out of it');
    });

    // ── a sheet that holds a second order (the control), for the Mark OK steps ──
    await W.reboot(w);
    const sh = await W.seedSheet(w, { id: 'sh-rs1', orders: ['sheeted', 'mate'] });
    const S1 = ORDERS.sheeted.rid, S2 = ORDERS.mate.rid;
    // (the card's Mark OK button, as a locator: the Review list redraws under a press, so a held element handle would be stale)
    const card = async rid => {
      await page.evaluate(() => { CN.setMode('review'); const v = Review.view(); v.cseg = 'open'; v.filter = null; v.q = ''; Review.render(); });
      const sel = CARD(rid) + ' [data-rv-markok], ' + CARD(rid) + ' [data-a="markok"]';
      return (await until(() => page.$(sel), 6000)) ? page.locator(sel).first() : null;
    };
    const mark = async rid => { const r = await page.evaluate(rid => CharmNestReviewMark.mark(rid, { by: 'Name One' }), rid); assert(r.ok, 'marked: ' + J(r)); await until(() => rec(rid)); };
    await mark(S1);
    await until(() => held(sh.id));
    assert(await held(sh.id), 'the sheet is held by the order in Review (control)');

    await step('3', 'Mark OK with no connection: still in Review, the sheet still held, one toast; works once back online', async () => {
      const b = await card(S1); assert(b, 'the order has its Mark OK button');
      await ctx.setOffline(true);
      await b.click();
      const t = await until(async () => { const l = (await toasts()).filter(x => /not saved/i.test(x)); return l.length ? l : null; }, 8000);
      assert(t && t.length === 1, 'one toast says it was not saved: ' + J(await toasts()));
      assert(await marked(S1), 'the page still has the order in Review');
      assert(await held(sh.id), 'and the sheet is still held');
      assert.equal(rec(S1).review.state, 'open', 'the cloud still has it open');
      const b2 = await card(S1); assert(b2 && !(await b2.isDisabled()), 'the button can be pressed again');
      await ctx.setOffline(false);
      await b2.click();
      assert(await until(async () => !(await marked(S1))), 'resolved once back online');
      await until(() => rec(S1).review.state === 'done');
      assert(await until(async () => !(await held(sh.id))), 'the sheet is released');
    });

    await step('4', 'Mark OK saved but its answer lost: the page says "not saved", then agrees with the cloud (resolved) by itself; nothing more is written', async () => {
      await page.evaluate(rid => CharmNestReviewMark.mark(rid, { by: 'Name One' }), S1);   // (a new cycle)
      await until(async () => rec(S1).review.state === 'open' && (await held(sh.id)));
      const b = await card(S1); assert(b, 'Mark OK is there');
      const n0 = calls('reviewMarkPut');
      mode = 'lose';
      await b.click();
      await until(async () => (await toasts()).some(x => /not saved/i.test(x)), 8000);
      const cloud = rec(S1).review; assert.equal(cloud.state, 'done', 'the cloud did resolve it: ' + J(cloud));
      // (the page first rolls back to "in Review", having heard nothing; the Review feed then brings the cloud's answer within seconds: no second press is needed)
      assert(await until(async () => !(await marked(S1)), 10000), 'the page agrees with the cloud: resolved');
      assert(await until(async () => !(await held(sh.id)), 5000), 'and the sheet is released on the page too');
      assert.equal(rec(S1).review.doneAt, cloud.doneAt, 'the cloud kept the one resolve (same time)');
      assert.equal(calls('reviewMarkPut') - n0, 1, 'one request in all');
    });

    await step('5', 'a second bench resolved it first: a stale page ends resolved, with no error, the first name kept, no second timeline line', async () => {
      await page.evaluate(rid => CharmNestReviewMark.mark(rid, { by: 'Name One' }), S1);
      await until(async () => rec(S1).review.state === 'open');
      await page.evaluate(() => CN.setMode('nest')); await W.settle(page, 300);   // (the Review feed rests on the Nest tab: this page cannot hear the other bench)
      const other = await page.evaluate(async rid => CN.api('charmNestLibrary', { op: 'reviewMarkPut', action: 'resolve', rid, by: 'Name Two', how: 'ok' }, { quiet: true }), S1);
      assert(other.ok && !other.already, 'the other bench resolved it: ' + J(other));
      assert(await marked(S1), 'this page still believes it is in Review (stale)');
      await page.evaluate(() => { window.__lines = []; const T = window.CNTimeline; if (T && T.line && !T.line.__spy) { const f = T.line; T.line = function (row, kind, o) { window.__lines.push(String(o && o.text || '')); return f.apply(this, arguments); }; T.line.__spy = true; } });
      const r = await page.evaluate(rid => CharmNestReviewMark.resolve(rid, { by: 'Name One', how: 'ok' }), S1);
      assert(r.ok && r.already, 'the stale press is told "already": ' + J(r));
      assert(!(await marked(S1)), 'and the page now agrees it is resolved');
      assert.equal(rec(S1).review.doneBy, 'Name Two', 'the first person stays on the record: ' + J(rec(S1).review));
      assert.deepEqual(await page.evaluate(() => window.__lines.filter(x => /marked OK|charm picked/i.test(x))), [], 'no second timeline line for a decision already made');
    });

    await step('6', 'a reload while Mark OK is in flight: the page says what the cloud says, whichever way the request went', async () => {
      // (a) the request never reached the cloud: after the reload the order is still in Review and Mark OK still works
      await page.evaluate(rid => CharmNestReviewMark.mark(rid, { by: 'Name One' }), S1);
      await until(async () => rec(S1).review.state === 'open');
      const b = await card(S1); assert(b);
      mode = 'stall'; await b.click(); await sleep(300);
      await W.reboot(w);
      await page.evaluate(() => { CN.setMode('orders'); });
      assert(await until(() => marked(S1), 8000), 'after the reload the order is in Review (the cloud never got the press)');
      assert.equal(rec(S1).review.state, 'open');
      // (b) the cloud got it, the page died before hearing: after the reload the order is resolved
      const b2 = await (async () => { await W.addOrders(w, ['sheeted', 'mate'], { loadMaps: true }); return card(S1); })();
      assert(b2, 'Mark OK is on the card again');
      mode = 'stallAfter'; await b2.click(); await until(() => rec(S1).review.state === 'done', 8000); await sleep(200);
      await W.reboot(w);
      await page.evaluate(() => { CN.setMode('orders'); });
      await W.addOrders(w, ['sheeted', 'mate']);
      assert(await until(async () => !(await marked(S1)) && (await page.evaluate(rid => CharmNestReviewMark.info(rid) && CharmNestReviewMark.info(rid).state, S1)) === 'done', 8000), 'after the reload the order is resolved, as the cloud says');
    });

    await step('7', 'cancelled while in Review, its piece left on a committed sheet: the sheet is not held for a card nobody can see', async () => {
      await W.reboot(w);
      const S3 = ORDERS.again.rid;   // (two other orders on a second sheet, so the first sheet's pieces are not listed twice)
      const sh2 = await W.seedSheet(w, { id: 'sh-rs2', orders: ['again', 'variant'], n: 2 });
      await mark(S3);
      assert(await until(() => held(sh2.id)), 'held while it is in Review');
      // Etsy cancels the order: the page finds it (AutoCancel) and the run keeps the line as gone; the piece stays on the sheet (a set already sent to the station keeps it)
      await page.evaluate(rid => { for (const r of Orders.rows()) if (String(r.order.receiptId) === rid) { r.state = 'gone'; r.reason = 'cancelled on Etsy'; } Orders.interpretAll(); try { Review.syncOrderItems(); } catch (_) {} Orders.render(); try { OrderPieces.notify(); } catch (_) {} }, S3);
      const run = JSON.parse(JSON.stringify(st.doc(P + 'Charm_Nest_Runs', 'run-rs'))); for (const l of Object.values(run.lines)) if (String(l.orderId) === S3) l.state = 'gone';
      st.put(P + 'Charm_Nest_Runs', 'run-rs', run);
      const inReview = await page.evaluate(() => Review.items().some(it => it.kind === 'reviewMark'));
      assert(!inReview, 'the Review tab has no card for it any more (nothing to press)');
      assert(await until(async () => !(await held(sh2.id)), 6000), 'so the sheet must not stay held by it: ' + J(await page.evaluate(sid => CharmNestReviewHold.heldBy(sid).map(x => x.rid), sh2.id)));
      const r = await page.evaluate(async sid => { try { return await CN.api('charmNestLibrary', { op: 'laserDone', kind: 'sheet', id: sid, done: true, by: 'Test Operator', stage: 'laser' }, { quiet: true }); } catch (e) { return { error: String(e && e.message || e) }; } }, sh2.id);
      assert(r && !r.error, 'and the server lets the sheet through (a cancelled order is never waited for): ' + J(r));
    });

    await step('0', 'nothing left the fake site and the page threw no error', async () => {
      assert.deepEqual(w.foreign, [], 'foreign requests: ' + J(w.foreign));
      assert.deepEqual(errors, [], 'page errors: ' + J(errors));
    });
  } finally { await w.close(); }
  let bad = 0;
  for (const r of results) { if (!r.ok) bad++; console.log(`  ${r.ok ? 'ok  ' : 'FAIL'} ${String(r.id).padStart(2)} ${r.title}${r.ok ? '' : '\n         ' + r.why}`); }
  console.log(bad ? `\nadv-review-resilience: ${bad} step(s) FAILED` : `\nadv-review-resilience: ${results.length} steps ok`);
  process.exit(bad ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
