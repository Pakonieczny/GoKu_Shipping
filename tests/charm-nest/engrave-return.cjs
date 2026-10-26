// The way back from the Engraving tab (ENGBACK, engrave-ui-1012 round 2b; Paul, 12 Oct 2026: "you completely removed the process flow that allowed the user to come back to
// the order view from the engraving tab if that's where they started the journey ... They're starting position every single time does it change").
// charm-nest-engrave-return.js reuses the Sheet window's return pill (class swReturn, in the top bar) for the order window and the tabs. Checked in the REAL-DRAWING world
// (tests/charm-nest/_engraveRealWorld.cjs: real charm drawings, real intake, pool, classifier and fit; the fake site underneath, nothing real is written, no Etsy, no paid AI):
//   1  order opened from the Orders list, on its Overview, scrolled; the card's thumbnail pressed -> the Engrave detail, the pill; one press back -> the same order, the same tab, the same scroll, the Orders list as it was
//   2  the same from the Timeline tab and from the Sheet tab (the window comes back on the tab it was on)
//   3  an order of two pieces, the second chosen on its pieces list -> back: the second is still chosen
//   4  from the Library (no window): back to the Library, scroll as it was
//   5  the origin follows the journey: a second order opened from the Engrave detail replaces the first; leaving the tab clears it; a direct open has none
//   6  Back / Next in the Engrave queue, a list / card switch (an old saved Decided view is the list) and a redraw keep the origin; an approval says so and goes back by itself; staged edits stop that and are never touched
//   7  the transition back: the order window grows out of the pill (clip-path animation), the pill folds away
//   node tests/charm-nest/engrave-return.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>); screenshots: round2/images/BACK-*.png (SHOTS=<dir> to change)
const path = require('path'), assert = require('assert/strict');
const Base = require('./_orderFactsWorld.cjs');
// (an extra line of an order may carry its own personalisation: a second piece with its own engraving)
{ const src = Base.orderOf.toString(); Base.orderOf = new Function('fx', 'now', 'const o = (' + src + ')(fx, now); (fx.more || []).forEach((m, i) => { if (m.personalization) o.lines[i + 1].personalization = m.personalization; }); return o;'); }
const R = require('./_engraveRealWorld.cjs');

async function main() {
  R.ORDERS.two = Object.assign({}, R.ORDERS.back, { rid: '4290000107', listing: '1890000107', more: [{ sku: 'COMET_9427', title: R.ORDERS.back.title, variations: R.ORDERS.back.variations, personalization: ['Name Two'] }] });
  const w = await R.world({ viewport: { width: 1356, height: 900 }, shots: process.env.SHOTS || '/mnt/project-files/plans/engrave-ui-1012/round2/images' });
  if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { page } = w, fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const wait = (fn, arg, ms) => page.waitForFunction(fn, arg, { timeout: ms || 15000 });
  const sleep = ms => page.waitForTimeout(ms);
  try {
    const made = await R.build(w, ['back', 'both', 'two']);
    const A = made.both, B = made.two, C = made.back;
    check(await page.evaluate(() => !!window.EngraveReturn && EngraveLink.open.__engBack === true && Engrave.openBack.__engBack === true), 'the page loads charm-nest-engrave-return.js (EngraveReturn; EngraveLink.open and Engrave.openBack carry it)');
    check(A.jobs.length === 2 && B.jobs.length === 2, 'the real world has the orders: A front+back (2 jobs), B two pieces (2 jobs)');

    const pill = () => page.evaluate(() => {
      const el = document.querySelector('.swReturn'), vis = !!el && !el.hidden && el.getClientRects().length > 0, r = el && el.getBoundingClientRect();
      return { vis, text: vis ? el.innerText.replace(/\s+/g, ' ').trim() : '', top: r ? Math.round(r.top) : null, right: r ? Math.round(r.right) : null, inBar: !!el && !!el.closest('#modeNav'), clipped: !!el && [...el.querySelectorAll('.go,.st')].some(n => n.scrollWidth > n.clientWidth + 1), cur: window.EngraveReturn.current() && { kind: EngraveReturn.current().kind, rid: EngraveReturn.current().rid, label: EngraveReturn.current().label } };
    });
    const where = () => page.evaluate(() => ({ mode: CN.S.mode, ow: OrderWin.isOpen(), rid: OrderWin.isOpen() ? OrderWin.rid() : null, key: OrderWin.isOpen() ? OrderWin.key() : null, view: OrderWin.isOpen() ? OrderWin.view() : null, piece: OrderWin.isOpen() ? OrderWin.selectedPiece() : null, ev: Engrave.view() }));
    const landed = () => wait(() => CN.S.mode === 'engrave' && !OrderWin.isOpen() && !SheetWin.isOpen() && !!document.querySelector('.swReturn:not([hidden])') && document.querySelector('#egQueue .rvItem'));
    const reset = async () => { await page.evaluate(() => { EngraveReturn.clear(); try { OrderWin.close(); } catch (_) {} Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: '', openDone: null }); CN.setMode('orders'); Orders.render(); }); await sleep(500); };
    // a tall spacer in a scroller, so there is something to keep: returns the element's selector
    const scrollIn = (sel, spacer, top) => page.evaluate(({ sel, spacer, top }) => { const e = document.querySelector(sel); if (!e) return null; if (spacer) { const d = document.createElement('div'); d.dataset.spacer = '1'; d.style.cssText = 'height:3000px;flex:none'; e.appendChild(d); } e.scrollTop = top; return e.scrollTop; }, { sel, spacer, top });
    const scrollOf = sel => page.evaluate(sel => { const e = document.querySelector(sel); return e ? Math.round(e.scrollTop) : null; }, sel);
    const shot = n => R.shot(w, 'BACK-' + n);
    const thumb = (key, face) => `#owEng .engCard[data-key="${key}"] .engFace[data-face=${face}] .engThumb`;
    const pressBack = () => page.click('.swReturn [data-a=back]');

    // ── 1 · Orders list -> order window (Overview, scrolled) -> the card's thumbnail -> Engrave detail -> back ──
    await page.evaluate(() => CN.setMode('orders')); await sleep(300);
    check(await scrollIn('#ordBody', true, 420) > 100, 'the Orders list is scrolled (420 px)');
    await page.evaluate(k => OrderWin.open(k), A.key);
    await page.waitForSelector(thumb(A.key, 'front'), { timeout: 15000 }); await sleep(900);
    const owS = await scrollIn('.owMain', true, 260);
    check(owS > 100, 'the order window is scrolled on its Overview (' + owS + ' px)');
    await R.shot(w, 'BACK-1-order-window-start');
    await page.click(thumb(A.key, 'front'));
    await landed(); await sleep(900);
    let p = await pill(), at = await where();
    check(at.mode === 'engrave' && !at.ow, '1: the Engraving tab is open, the order window is closed');
    check(p.vis && /^Back to order$/.test(p.text), '1: the return pill is there: "' + p.text + '"');
    check(p.inBar && p.top < 70 && !p.clipped, '1: the pill sits in the top bar beside the tabs and its words are whole (top ' + p.top + ', clipped ' + p.clipped + ')');
    check(p.cur && p.cur.kind === 'order' && p.cur.rid === A.rid, '1: the memory says: started in the order window of ' + A.rid);
    check(at.ev.focus === A.jobs.find(j => j.face === 'front').key && at.ev.list === false, '1: the Engrave detail is on the front of that piece (' + at.ev.focus + ')');
    await shot('1-engrave-detail-with-return');
    // the transition back: record the order window's animations from the press
    await page.evaluate(() => { window.__rec = { max: 0, clip: false, grow: false, pillAnim: 0 }; const d = document.getElementById('orderWin'), pl = document.querySelector('.swReturn'); const id = setInterval(() => { const an = d.getAnimations(); window.__rec.max = Math.max(window.__rec.max, an.length); if (an.some(a => a.effect && a.effect.getKeyframes().some(k => k.clipPath))) window.__rec.clip = true; if (d.classList.contains('owGrow')) window.__rec.grow = true; window.__rec.pillAnim = Math.max(window.__rec.pillAnim, pl.getAnimations().length); }, 16); setTimeout(() => clearInterval(id), 1800); });
    await pressBack();
    await wait(() => OrderWin.isOpen());
    await sleep(120); await shot('2-order-window-growing-out-of-the-pill');
    await sleep(1500);
    at = await where(); p = await pill();
    check(at.ow && at.rid === A.rid && at.key === A.key && at.view === 'info', '1: back in the same order (' + at.rid + ', line ' + at.key + ') on its Overview');
    check(at.mode === 'orders', '1: the tab beneath is the Orders list again (' + at.mode + ')');
    check(Math.abs((await scrollOf('.owMain')) - owS) <= 2, '1: the order window is scrolled where it was (' + await scrollOf('.owMain') + ' of ' + owS + ')');
    check(Math.abs((await scrollOf('#ordBody')) - 420) <= 2, '1: the Orders list is scrolled where it was (' + await scrollOf('#ordBody') + ' of 420)');
    check(!p.vis && p.cur === null, '1: the pill is gone and the memory is cleared once back');
    const rec = await page.evaluate(() => window.__rec);
    check(rec.clip && rec.grow && rec.max >= 1 && rec.pillAnim >= 1, '7: the transition back: the order window grew out of the pill (clip-path animation ' + rec.clip + ', owGrow ' + rec.grow + ') and the pill folded away (' + rec.pillAnim + ' animation)');
    await shot('3-back-in-the-order');
    await reset();

    // ── 2 · the same from the Timeline tab and the Sheet tab ──
    for (const view of ['timeline', 'sheet']) {
      await page.evaluate(k => OrderWin.open(k), A.key); await page.waitForSelector(thumb(A.key, 'back'), { timeout: 15000 }); await sleep(500);
      await page.evaluate(v => OrderWin.setView(v), view); await sleep(900);
      // (the Timeline has no engraving card: a shortcut into Engraving there is EngraveLink.open, which is what every button and thumbnail calls)
      await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key, piece: key + '~front', face: 'front' }), { rid: A.rid, key: A.key });
      await landed(); await sleep(500);
      p = await pill(); check(p.vis && p.cur.kind === 'order', `2 (${view}): the pill is there after landing in the Engrave detail`);
      await pressBack(); await wait(() => OrderWin.isOpen()); await sleep(1400);
      at = await where();
      check(at.ow && at.rid === A.rid && at.view === view && at.mode === 'orders', `2 (${view}): back in order ${A.rid} on its ${view} tab (${at.view}), over ${at.mode}`);
      if (view === 'timeline') await shot('4-back-on-the-timeline-tab');
      await reset();
    }

    // ── 3 · an order of two pieces: the second chosen on its pieces list ──
    const k2 = B.jobs[1].key, k1 = B.jobs[0].key;
    await page.evaluate(k => OrderWin.open(k), B.key); await page.waitForSelector('#owEng .engCard', { timeout: 15000 }); await sleep(700);
    check(await page.evaluate(k => OrderWin.selectPiece(k), k2), '3: piece 2 is chosen on the order window\'s pieces list');
    await sleep(900);
    await page.waitForSelector(`#owEng .engCard[data-key="${k2}"] .engThumb`, { timeout: 15000 });
    await page.click(`#owEng .engCard[data-key="${k2}"] .engThumb`);
    await landed(); await sleep(500);
    at = await where();
    check(at.ev.focus === k2, '3: the Engrave detail is on piece 2 (' + at.ev.focus + ')');
    await pressBack(); await wait(() => OrderWin.isOpen()); await sleep(1600);
    at = await where();
    check(at.ow && at.rid === B.rid && at.key === k2 && at.piece === k2, '3: back in order ' + B.rid + ' with piece 2 still chosen (line ' + at.key + ', piece ' + at.piece + ')');
    await reset();

    // ── 4 · from the Library: no window ──
    await page.evaluate(() => CN.setMode('library')); await sleep(500);
    const libSel = '#stage';   // (the Library scrolls in the stage that holds it)
    check(await scrollIn(libSel, true, 350) > 100, '4: the Library is scrolled (350 px)');
    await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: C.rid, key: C.key });
    await landed(); await sleep(700);
    p = await pill(); at = await where();
    check(at.mode === 'engrave' && p.vis && /^Back to Library\b/.test(p.text) && p.cur.kind === 'page', '4: the pill says "' + p.text + '"');
    await shot('5-from-the-library');
    await pressBack(); await wait(() => CN.S.mode === 'library'); await sleep(1500);
    p = await pill();
    check(await page.evaluate(() => CN.S.mode) === 'library' && !p.vis, '4: back on the Library, the pill gone');
    check(Math.abs((await scrollOf(libSel)) - 350) <= 3, '4: the Library is scrolled where it was (' + await scrollOf(libSel) + ' of 350)');
    await reset();

    // ── 5 · the origin follows the journey ──
    await page.evaluate(() => CN.setMode('orders')); await sleep(300);
    await page.evaluate(k => OrderWin.open(k), A.key); await page.waitForSelector(thumb(A.key, 'back'), { timeout: 15000 }); await sleep(600);
    await page.click(thumb(A.key, 'back')); await landed(); await sleep(500);
    p = await pill(); check(p.cur.rid === A.rid, '5: journey 1 started from order ' + A.rid);
    // (a) the Orders tab is a different place on purpose: the origin goes
    await page.click('#modeSeg button[data-mode="orders"]'); await sleep(500);
    p = await pill(); check(!p.vis && p.cur === null, '5: pressing another tab (Orders) clears the origin and the pill');
    // (b) a second order, opened from the Orders list: the pill goes to the second, not the first
    await page.evaluate(k => OrderWin.open(k), C.key); await page.waitForSelector(thumb(C.key, 'back'), { timeout: 15000 }); await sleep(600);
    await page.click(thumb(C.key, 'back')); await landed(); await sleep(500);
    p = await pill(); check(p.cur.rid === C.rid, '5: journey 2 started from order ' + C.rid + ' (' + p.text + ')');
    // (c) a journey from the Engrave detail itself: order B opened over the Engraving tab replaces order C
    await page.evaluate(k => OrderWin.open(k), B.key); await page.waitForSelector('#owEng .engCard', { timeout: 15000 }); await sleep(900);
    await page.click(`#owEng .engCard[data-key="${B.jobs[0].key}"] .engThumb`); await landed(); await sleep(500);
    p = await pill(); check(p.cur.rid === B.rid && !p.text.includes(C.rid), '5: journey 3 (order ' + B.rid + ' opened over the Engraving tab) replaced the first: ' + p.text);
    await pressBack(); await wait(() => OrderWin.isOpen()); await sleep(1400);
    at = await where();
    check(at.ow && at.rid === B.rid && at.mode === 'engrave', '5: back goes to order ' + B.rid + ' (not ' + C.rid + ' or ' + A.rid + '), over the Engraving tab it started on (' + at.mode + ')');
    await reset();
    // (d) direct open: the Engraving tab pressed on the top bar
    await page.click('#modeSeg button[data-mode="engrave"]'); await sleep(800);
    p = await pill(); check(!p.vis && p.cur === null, '5: the Engraving tab opened directly has no return control');
    await shot('6-direct-open-no-return');
    await reset();

    // ── 6 · Back / Next, tab switch, redraw, approval, staged edits ──
    // (an order of two pieces: the search box is on its number, so the queue is its two pieces)
    await page.evaluate(k => OrderWin.open(k), B.key); await page.waitForSelector(`#owEng .engCard[data-key="${k1}"] .engThumb`, { timeout: 15000 }); await sleep(700);
    await page.click(`#owEng .engCard[data-key="${k1}"] .engThumb`); await landed(); await sleep(600);
    const q0 = await page.evaluate(() => Engrave.view().focus);
    await page.click('#egQueue [data-a=next]');
    await page.waitForFunction(q => Engrave.view().focus !== q, q0, { timeout: 8000 }).catch(() => {});
    await sleep(900);
    const q1 = await page.evaluate(() => Engrave.view().focus);
    p = await pill();
    check(q1 !== q0 && q0 === k1, '6: Next moved the Engrave queue to the other piece (' + q0.slice(-4) + ' -> ' + q1.slice(-4) + ')');
    check(p.vis && p.cur.rid === B.rid && p.cur.kind === 'order', '6: Next keeps the SAME origin (' + p.text + ')');
    await page.click('#egQueue [data-a=prev]'); await sleep(900);
    p = await pill(); check(p.vis && p.cur.rid === B.rid, '6: Back keeps it too');
    await page.evaluate(() => { Engrave.restoreView({ list: true, chosen: true }); Engrave.render(); }); await sleep(500);
    await page.evaluate(k => { Engrave.restoreView({ list: false, chosen: true, focus: k }); Engrave.render(); }, q1); await sleep(500);
    await page.evaluate(() => { Engrave.restoreView({ tab: 'done', chosen: true }); Engrave.render(); }); await sleep(500);   // (an old saved "Decided" view: it is the one list's Approved chip now, APPTAB)
    const compat = await page.evaluate(() => ({ tab: Engrave.view().tab, list: Engrave.view().list, gone: document.querySelectorAll('#egDone, #egBacks, .egTab[data-tab="done"]').length }));
    check(compat.tab === 'approved' && compat.list === true && compat.gone === 0, '6: an old saved Decided view lands on the Approved view of the one list (APPTAB): ' + JSON.stringify(compat));
    p = await pill(); check(p.vis && p.cur.rid === B.rid, '6: list / card switches (and an old saved Decided view) keep the origin');
    await reset();
    // the order A, the back of its piece
    await page.evaluate(k => OrderWin.open(k), A.key); await page.waitForSelector(thumb(A.key, 'back'), { timeout: 15000 }); await sleep(600);
    await page.click(thumb(A.key, 'back')); await landed(); await sleep(600);
    // back to the opened piece, then a staged edit on it, then the approval is seen
    const open = A.jobs.find(j => j.face === 'back').key;
    await page.evaluate(k => { Engrave.restoreView({ tab: 'place', focus: k, list: false, chosen: true }); Engrave.render(); }, open); await sleep(700);
    // (the opened job is the back; make the origin's job known to the memory, as a landing on it would)
    await page.evaluate(k => { const c = EngraveReturn.current(); EngraveReturn.set(Object.assign({}, c, { key: k, was: Engrave.items().get(k).state })); }, open);
    await page.evaluate(k => { const P = CharmNestEngravePanel; P.DRAFTS.set(k, { dirty: true, posTouched: false, faces: { back: { text: 'STAGED' } } }); }, open);
    await page.evaluate(k => { Engrave.items().get(k).state = 'approved'; }, open);
    await sleep(1200); p = await pill();
    check(p.vis && /Approved/.test(p.text) && !/going back/.test(p.text), '6: approved with edits staged: the pill says Approved and does not take the person away (' + p.text + ')');
    await sleep(2000);
    check((await where()).mode === 'engrave' && (await pill()).vis, '6: and it stays: nothing returned by itself while edits are staged');
    await pressBack(); await wait(() => OrderWin.isOpen()); await sleep(1200);
    check(await page.evaluate(k => { const d = CharmNestEngravePanel.DRAFTS.get(k); return !!d && d.dirty === true && d.faces.back.text === 'STAGED'; }, open), '6: going back left the staged edit staged (never touched, no new question)');
    await page.evaluate(k => { CharmNestEngravePanel.DRAFTS.delete(k); }, open);
    await reset();
    // the approval seen with nothing staged: it goes back by itself
    await page.evaluate(k => { Engrave.items().get(k).state = 'review'; }, open);
    await page.evaluate(k => OrderWin.open(k), A.key); await page.waitForSelector(thumb(A.key, 'back'), { timeout: 15000 }); await sleep(600);
    await page.click(thumb(A.key, 'back')); await landed(); await sleep(500);
    await page.evaluate(k => { Engrave.items().get(k).state = 'approved'; }, open);
    await page.waitForFunction(() => /Approved · going back/.test((document.querySelector('.swReturn') || {}).innerText || ''), null, { timeout: 4000 }).catch(() => {});
    p = await pill(); await shot('7-approved-going-back');
    check(/Approved · going back/.test(p.text), '6: approved, nothing staged: the pill says "Approved · going back"');
    await wait(() => OrderWin.isOpen(), null, 6000); await sleep(1200);
    at = await where(); check(at.ow && at.rid === A.rid && at.mode === 'orders', '6: and it went back to the order by itself (' + at.rid + ' over ' + at.mode + ')');
    await reset();

    // ── 8 · from the Sheet window: its own journey now rides on the same pill (one control, one memory) ──
    const SH = 'sheet-back-test', pid = A.poolIds[0];
    w.srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', folder: 'GF_Oct.5.26_Set-1_Sheet-1', stock: { wPt: 250, hPt: 100 }, orders: [A.rid],
      placements: [{ id: 'b0', cxPt: 40, cyPt: 40, angle: 0, wPt: 20, hPt: 20 }], charms: [{ id: 'b0', name: A.rid + ' · SKU', poolId: pid, order: A.rid, sku: 'SCHRODINGER CAT 2' }], poolIds: [pid] });
    w.srv.st.put('Charm_Pool', pid, { poolId: pid, orderId: A.rid, transactionId: A.rid + '1', lineKey: A.key, sku: 'SCHRODINGER CAT 2', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: 'GF_Oct.5.26_Set-1_Sheet-1', updatedAt: Date.now() });
    // (an order journey first: the Sheet window's replaces it, there is never a second pill)
    await page.evaluate(() => CN.setMode('orders')); await sleep(300);
    await page.evaluate(k => OrderWin.open(k), B.key); await page.waitForSelector('#owEng .engCard', { timeout: 15000 }); await sleep(800);
    await page.click(`#owEng .engCard[data-key="${k1}"] .engThumb`); await landed(); await sleep(400);
    check((await pill()).cur.kind === 'order', '8: an order journey is in the pill');
    await page.evaluate(({ sh, pid }) => SheetWin.open(sh, { select: pid }), { sh: SH, pid });
    await wait(pid => { const W = SheetWin._W; return W.sel && W.sel.poolId === pid && W.view === 'piece' && !W.flip && !W.flying && W.el.detail.querySelector('[data-r2=eng] [data-e=engrave]'); }, pid, 30000); await sleep(600);
    await page.click('[data-r2=eng] [data-e=engrave]');
    await wait(() => CN.S.mode === 'engrave' && !SheetWin.isOpen() && !!document.querySelector('.swReturn:not([hidden])') && EngraveReturn.current() && EngraveReturn.current().kind === 'sheet'); await sleep(900);
    p = await pill();
    check(p.cur.kind === 'sheet' && /^Back to .*Sheet/i.test(p.text) && p.text.includes(A.rid), '8: the Sheet window\'s journey replaced it: "' + p.text + '"');
    check(await page.evaluate(() => document.querySelectorAll('div.swReturn:not([hidden])').length) === 1, '8: exactly one return control');
    await pressBack(); await wait(pid => SheetWin.isOpen() && SheetWin._W.sel && SheetWin._W.sel.poolId === pid, pid, 20000); await sleep(1200);
    check(await page.evaluate(() => ({ lib: CN.S.mode === 'library', pill: !!document.querySelector('div.swReturn:not([hidden])'), cur: EngraveReturn.current() })).then(x => x.lib && !x.pill && x.cur === null), '8: one press back: the sheet window is open on the same charm, the pill and the memory are gone');
    await page.evaluate(() => { try { SheetWin.close(); } catch (_) {} }); await sleep(600);
    await reset();

    // ── errors ──
    check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | ').slice(0, 300));
  } finally { await w.close(); }
  if (fails.length) { console.log('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('engrave-return: ok');
}
main().catch(e => { console.error(e); process.exit(1); });
