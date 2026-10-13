// ENGCRASH (engrave-ui-1012, Paul 12 Oct 2026): "When I click on the back engraving card of this particular listing it crashes the system and all engraving results disappear". The order was a back-only
// necklace on HOLD, not on a sheet: it had NO Engrave job (a job was made once the piece was on a sheet), yet the order window drew a Back card from the intake's reading. The press waited 7 s ("Finding the
// engraving..."), closed the order window and left the Engrave tab filtered to the order number: an empty list and a search box holding the number.
// ENGCRASH answered that with a plain refusal ("on hold and not on a sheet yet"); Paul (13 Oct): "a charm does not have to be placed on that sheet for it to be approved/updated by the user for the correct back
// engraving". ENGPRE (engrave-prenest-1013) reversed it: such a line gets its Engrave job at once (Engrave.early) and the press OPENS it. What stays is the empty-tab protection: a press that can open nothing
// (a line with nothing to engrave, a job with nothing to engrave) closes nothing and filters nothing.
//
// This test uses the REAL-DRAWING world (_engraveRealWorld.cjs): real drawings, real intake, pool, classifier and fit, offline, made-up names, nothing written anywhere real. It checks, for a back-only necklace
// order that is on hold and has no job yet (tests/charm-nest/engrave-prenest-job.cjs checks the job itself, its approval and its carry-over):
//   1  the order window card is drawn from the reading; its Back box and button carry no refusal; pressing the Back box OPENS the Engrave detail on that order's job (the order window closes as it lands);
//   2  the Engrave tab afterwards lists the real jobs (and the held one); nothing is filtered;
//   3  EngraveLink.open() itself (the door every caller uses) opens a piece that is on no sheet yet and not on hold, and still refuses, at once, a job with nothing to engrave and a line with nothing to engrave
//      (nothing closed, nothing filtered, one plain line: inside the open window, else a toast);
//   4  an empty filtered list is never blank: it says what matched nothing and offers "Clear search" (which puts the whole list back);
//   5  one row / one card that throws is a small quiet row / card: the others are still drawn (the Placements list, the order window's cards).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/engrave-no-job-click.cjs
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
let pass = 0, fail = 0;
const check = (ok, msg) => { if (ok) { pass++; console.log('  ✓ ' + msg); } else { fail++; console.log('  ✗ ' + msg); } };

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp' });
  if (!w) { console.log('skipped: no playwright-core'); return; }
  const page = w.page;
  try {
    await R.build(w, ['back', 'both', 'discs']);
    const made = w.made;
    check(made.back.jobs.length === 1 && made.both.jobs.length === 2 && made.discs.jobs.length === 3, 'the real world has jobs: back 1, both 2, discs 3 (' + [made.back, made.both, made.discs].map(m => m.jobs.length).join('/') + ')');

    // a back-only necklace order that is ON HOLD and was never pooled: no job (the same shape as the live order: row state "pulled", a person's hold on it)
    const HELD = '4290000199';
    const heldKey = await page.evaluate(async ({ fx, orderSrc, rid }) => {
      const orderOf = eval('(' + orderSrc + ')'), order = orderOf(Object.assign({}, fx, { rid, listing: '1890000199', buyer: 'Name Nine' }), Date.now());
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll();
      row.state = 'pulled'; row.hold = 'held by Test'; row.reason = null;
      return key;
    }, { fx: Object.assign({}, R.ORDERS.back, { variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Engraving Side', value: 'Back' }, { name: 'Font Choice', value: 'Typewriter' }], personalization: ['Typewriter font\n\nName One\nDate One'] }), orderSrc: Base.orderOf.toString(), rid: HELD });
    const heldState = await page.evaluate(({ key, rid }) => { const r = B.orders.byKey.get(key); return { jobs: Engrave.jobsOf(r).length, place: window.PiecePlacement && PiecePlacement.ofRow(r).state, state: r.state, cand: !!(r.spec && r.spec.engraveCandidate), mode: r.spec && r.spec.engraving && r.spec.engraving.mode, job: !!EngraveLink.resolve({ rid, key }).job }; }, { key: heldKey, rid: HELD });
    check(heldState.jobs === 0 && heldState.place === 'hold' && heldState.state === 'pulled', 'the held order has no job and reads as on hold while its row is "pulled" (' + JSON.stringify(heldState) + ')');
    check(heldState.job === false, 'EngraveLink.resolve has no job for it yet (the job is made when it is asked for)');

    const view = () => page.evaluate(() => { const v = Engrave.view(); return { tab: v.tab, list: v.list, q: v.q || '', focus: v.focus || null, mode: CN.S.mode, owOpen: OrderWin.isOpen(), box: (document.getElementById('egQ') || {}).value || '' }; });
    const listState = () => page.evaluate(() => { const l = document.querySelector('#egQueue .egPlacementList'); return { rows: l ? l.querySelectorAll(':scope > .placementRow').length : -1, quiet: l ? l.querySelectorAll(':scope > .quietRow').length : -1, none: !!document.querySelector('#egQueue .egNone'), err: !!document.querySelector('#engraveView [data-eg-retry]'), box: (document.getElementById('egQ') || {}).value || '' }; });
    const toGo = async (q) => { await page.evaluate(q => { try { OrderWin.close(); } catch (_) {} Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: q || '', openDone: null }); CN.setMode('engrave'); Engrave.render(); }, q || ''); await R.settle(page, 500); };

    // ── 1 · the press on the Back box of the held order's card ──
    await toGo('');
    const before = await view(), list0 = await listState();
    check(list0.rows >= 3 && !list0.none, 'the Engrave tab lists the real jobs before the press (' + list0.rows + ' rows)');
    await page.evaluate(() => { CN.setMode('orders'); });
    await page.evaluate(k => OrderWin.open(k), heldKey);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard .engFace[data-face=back] .engThumb'); }, null, { timeout: 20000 });
    await R.settle(page, 500);
    const card = await page.evaluate(() => { const c = document.querySelector('#owEng .engCard'), b = c.querySelector('.egOpen'), t = c.querySelector('.engFace[data-face=back] .engThumb'); return { btn: b ? { aria: b.getAttribute('aria-disabled'), title: b.title } : null, thumb: t ? { aria: t.getAttribute('aria-disabled'), title: t.title } : null, faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face) }; });
    check(card.faces.includes('back') && card.btn && !card.btn.aria && !card.btn.title && card.thumb && !card.thumb.aria && !/hold|sheet/i.test(card.thumb.title), 'the order window draws the Back card; its button and Back box carry no "on hold / not on a sheet" refusal (' + JSON.stringify(card) + ')');
    await page.click('#owEng .engCard .engFace[data-face=back] .engThumb');
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem'); return c && c.dataset.key === k && !document.getElementById('engraveView').classList.contains('hidden'); }, heldKey, { timeout: 90000 });
    await R.settle(page, 500);
    const after = await view();
    check(after.mode === 'engrave' && after.tab === 'place' && after.focus === heldKey && !after.owOpen, 'the press OPENED the Engrave detail on the held order\'s own job, the order window closed as it landed (' + JSON.stringify(after) + ')');
    const held2 = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { state: r.state, hold: r.hold || null, pooled: (r.poolIds || []).length, jobs: Engrave.jobsOf(r).length }; }, heldKey);
    check(held2.state === 'pulled' && held2.hold === 'held by Test' && held2.pooled === 0 && held2.jobs === 1, 'the order stays held, pulled and in no pool; it has its job (' + JSON.stringify(held2) + ')');
    await R.shot(w, 'ENGPRE-held-opened');

    // ── 2 · the Engrave tab afterwards ──
    check(after.q === HELD, 'the press landed with the search box on the order number, as every link into Engraving does (' + after.q + ')');
    await toGo('');   // (a person clears the search to see the whole list)
    const list1 = await listState(), v1 = await view();
    check(list1.rows === list0.rows + 1 && list1.box === '' && v1.q === '' && !list1.none && !list1.err, 'the Engrave tab afterwards lists everything and the held order too, the search box is empty (' + JSON.stringify(list1) + ' vs ' + list0.rows + ' before)');

    // ── 3 · EngraveLink.open itself ──
    // a piece on no sheet and not on hold (pulled) is opened like any other
    const pulledState = await page.evaluate(({ key }) => { const r = B.orders.byKey.get(key); r.hold = null; r.state = 'pulled'; r.poolIds = []; return PiecePlacement.ofRow(r).state; }, { key: heldKey });
    await page.evaluate(() => { CN.setMode('orders'); });
    await page.evaluate(k => OrderWin.open(k), heldKey);
    await page.waitForFunction(() => OrderWin.isOpen());
    const t1 = Date.now();
    const r3 = await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: HELD, key: heldKey });
    const v3 = await view();
    check(pulledState === 'waiting' && r3 === true && v3.mode === 'engrave' && v3.focus === heldKey && !v3.owOpen && Date.now() - t1 < 20000, 'EngraveLink.open on a pulled piece on no sheet (not held): true, on that job (' + pulledState + ', ' + JSON.stringify(v3) + ')');
    // a job with nothing to engrave: nothing moves (the Engrave tab is put back to its list first, then the order window opens over the orders tab)
    await toGo('');
    await page.evaluate(() => { CN.setMode('orders'); });
    await page.evaluate(k => OrderWin.open(k), heldKey);
    await page.waitForFunction(() => OrderWin.isOpen());
    const vBefore = await view();
    const noneKey = made.back.jobs[0].key;
    await page.evaluate(k => { const j = Engrave.items().get(k); window.__was = j.state; j.state = 'none'; }, noneKey);
    const r4 = await page.evaluate(k => EngraveLink.open({ rid: '4290000101', key: k }), noneKey);
    const v4 = await view(), note4 = await page.evaluate(() => (document.querySelector('dialog[open] .elWait[data-note]') || {}).textContent || '');
    await page.evaluate(k => { Engrave.items().get(k).state = window.__was; }, noneKey);
    check(r4 === false && v4.owOpen && v4.q === '' && /nothing to engrave/.test(note4), 'a job with nothing to engrave: false, the window stays, no filter (' + note4 + ')');
    // a line with nothing to engrave at all (no personalisation): no job is coming; the same plain answer, at once
    const PLAIN = '4290000198';
    const plainKey = await page.evaluate(async ({ fx, orderSrc, rid }) => {
      const orderOf = eval('(' + orderSrc + ')'), order = orderOf(Object.assign({}, fx, { rid, listing: '1890000198', buyer: 'Name Eight', personalization: [], variations: [{ name: 'Metal choice', value: 'Gold' }] }), Date.now());
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll(); row.state = 'pulled'; row.hold = 'held by Test';
      return key;
    }, { fx: R.ORDERS.back, orderSrc: Base.orderOf.toString(), rid: PLAIN });
    const t2 = Date.now();
    const r5 = await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: PLAIN, key: plainKey });
    const v5 = await view(), note5 = await page.evaluate(() => (document.querySelector('dialog[open] .elWait[data-note]') || {}).textContent || '');
    check(r5 === false && Date.now() - t2 < 2500 && v5.owOpen && v5.q === '' && v5.list === vBefore.list && /not loaded here/.test(note5) && !/hold|sheet/i.test(note5), 'a line with nothing to engrave: false, at once, the window stays, no filter, one plain line that is not about hold or a sheet (' + note5 + ')');

    // with NO window open the line is a toast
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} });
    await page.waitForFunction(() => !OrderWin.isOpen());
    await page.waitForTimeout(600);
    await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: PLAIN, key: plainKey });
    const toasts = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent).concat(['dialogs:' + document.querySelectorAll('dialog[open]').length, 'fixedNotes:' + document.querySelectorAll('.elWait').length]));
    check(toasts.some(t => /not loaded here/.test(t)) && !(await view()).q, 'with no window open it is a toast, and still nothing is filtered (' + toasts.join(' | ') + ')');

    // ── 4 · a list that matches nothing is never blank ──
    await toGo('zzz-no-such-order');
    const none = await page.evaluate(() => { const m = document.querySelector('#egQueue .egNone'); return { text: m ? m.textContent : '', btn: !!(m && m.querySelector('[data-eg-clear]')) }; });
    check(/Nothing here matches/.test(none.text) && none.btn, 'a search that matches nothing says so and offers Clear search (' + none.text + ')');
    await page.click('#egQueue .egNone [data-eg-clear]');
    await R.settle(page, 400);
    const cleared = await listState(), vc = await view();
    check(cleared.rows === list1.rows && cleared.box === '' && vc.q === '' && !cleared.none, 'Clear search puts the whole list back and empties the search box (' + JSON.stringify(cleared) + ')');

    // ── 5 · one row that throws does not stop the rest ──
    await page.evaluate(k => { const S = window.CNEngravingSeals; window.__list = S.list; S.list = function (j) { if (j && j.key === k) throw new Error('boom (test)'); return window.__list.apply(this, arguments); }; }, made.back.jobs[0].key);
    await toGo('');
    const list5 = await listState();
    check(list5.rows >= 3 && list5.quiet === 1 && !list5.err, 'one row that throws is a small quiet row, the other rows are drawn (rows ' + list5.rows + ', quiet ' + list5.quiet + ', error pane ' + list5.err + ')');
    const quiet = await page.evaluate(() => { const q = document.querySelector('#egQueue .quietRow'); return q ? q.textContent : ''; });
    check(/4290000101/.test(quiet) && /could not be drawn/.test(quiet), 'the quiet row says which piece and one short line (' + quiet + ')');
    await page.evaluate(() => { window.CNEngravingSeals.list = window.__list; });
    await toGo('');
    check((await listState()).quiet === 0, 'once the fault is gone every row is drawn normally');

    // the order window's cards: one card that throws is a quiet card, the others are drawn
    await page.evaluate(() => { const C = window.CharmNestEngraveCard; window.__card = C.card; C.card = function (piece, o) { if (piece.slot === 'D2') throw new Error('boom card (test)'); return window.__card.apply(this, arguments); }; });
    await R.openOrderWindow(w, 'discs');
    const cards = await page.evaluate(() => [...document.querySelectorAll('#owEng > .engCard')].map(c => ({ quiet: !!c.dataset.quiet, slot: c.dataset.slot || '', text: c.textContent })));
    check(cards.length === 3 && cards.filter(c => c.quiet).length === 1 && cards.filter(c => !c.quiet).length === 2, 'the order window: the disc whose card throws is a quiet card, the other two are drawn (' + cards.map(c => c.quiet ? 'quiet' : 'ok').join(',') + ')');
    check(cards.some(c => c.quiet && /could not be drawn/.test(c.text)), 'the quiet card says so in one short line');
    await page.evaluate(() => { CharmNestEngraveCard.card = window.__card; try { OrderWin.close(); } catch (_) {} });

    check(w.errors.length === 0, 'no page error (' + w.errors.join(' | ') + ')');
  } finally { await w.close(); }
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
