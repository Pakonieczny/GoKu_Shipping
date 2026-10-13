// ENGCRASH (engrave-ui-1012, Paul 12 Oct 2026): "When I click on the back engraving card of this particular listing it crashes the system and all
// engraving results disappear". The order was a back-only necklace on HOLD, not on a sheet: it has NO Engrave job (a job is made once the piece is on a
// sheet), yet the order window drew a Back card from the intake's reading. The press waited 7 s ("Finding the engraving..."), closed the order window and left
// the Engrave tab filtered to the order number: an empty list and a search box holding the number.
//
// This test uses the REAL-DRAWING world (_engraveRealWorld.cjs): real drawings, real intake, pool, classifier and fit, offline, made-up names, nothing written
// anywhere real. It checks, for a back-only necklace order that is on hold and has no job:
//   1  the order window card is drawn from the reading, and pressing its Back box says why in ONE plain line inside the open order window (a modal window
//      covers the toasts) at once (no 7 s wait), closes nothing, and leaves the Engrave tab exactly as it was (tab, list, an empty search box);
//   2  the Engrave tab afterwards still lists the real jobs of the other orders; nothing is filtered;
//   3  EngraveLink.open() itself (the door every caller uses) does the same for a piece that is on no sheet yet, and for a job with nothing to engrave;
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
    const heldState = await page.evaluate(({ key, rid }) => { const r = B.orders.byKey.get(key); return { jobs: Engrave.jobsOf(r).length, place: window.PiecePlacement && PiecePlacement.ofRow(r).state, state: r.state, cand: !!(r.spec && r.spec.engraveCandidate), mode: r.spec && r.spec.engraving && r.spec.engraving.mode, link: EngraveLink.resolve({ rid, key }).why }; }, { key: heldKey, rid: HELD });
    check(heldState.jobs === 0 && heldState.place === 'hold' && heldState.state === 'pulled', 'the held order has no job and reads as on hold while its row is "pulled" (' + JSON.stringify(heldState) + ')');
    check(/on hold and not on a sheet yet/.test(heldState.link), 'EngraveLink.resolve says why: ' + heldState.link);

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
    const card = await page.evaluate(() => { const c = document.querySelector('#owEng .engCard'); return { noJob: c.dataset.noJob, title: (c.querySelector('.engFace[data-face=back] .engThumb') || {}).title || '', faces: [...c.querySelectorAll('.engFace')].map(f => f.dataset.face) }; });
    check(card.noJob === '1' && /on hold/.test(card.title) && card.faces.includes('back'), 'the order window draws the Back card from the reading, marked as having no job, its tooltip says why (' + JSON.stringify(card) + ')');
    const t0 = Date.now();
    await page.click('#owEng .engCard .engFace[data-face=back] .engThumb');
    await page.waitForSelector('dialog[open] .elWait[data-note]', { timeout: 3000 });
    const took = Date.now() - t0;
    const note = await page.evaluate(() => { const n = document.querySelector('dialog[open] .elWait[data-note]'); return { text: n.textContent, role: n.getAttribute('role'), ring: !!n.querySelector('i'), notes: document.querySelectorAll('.elWait').length }; });
    check(took < 2500, 'the answer is at once, not after the 7 s wait (' + took + ' ms)');
    check(/on hold and not on a sheet yet/.test(note.text) && note.role === 'status' && !note.ring && note.notes === 1, 'one plain line says why inside the open order window, no spinner left (' + note.text + ')');
    await page.waitForTimeout(400);
    const after = await view();
    check(after.owOpen === true && after.mode === 'orders', 'the order window is still open: nothing was closed (' + after.mode + ')');
    check(after.tab === before.tab && after.list === before.list && after.q === '' && after.focus === before.focus, 'the Engrave tab view is exactly as it was: same tab, same list, no filter left (' + JSON.stringify(after) + ')');
    const busy = await page.evaluate(() => !!document.querySelector('#owEng .engThumb[aria-busy]'));
    check(!busy, 'the Back box is not left with a loading ring');
    await R.shot(w, 'ENGCRASH-held-card-note');

    // ── 2 · the Engrave tab afterwards ──
    await toGo(after.q);
    const list1 = await listState(), v1 = await view();
    check(list1.rows === list0.rows && list1.box === '' && v1.q === '' && !list1.none && !list1.err, 'the Engrave tab afterwards still lists everything, the search box is empty (' + JSON.stringify(list1) + ')');

    // ── 3 · EngraveLink.open itself ──
    await page.evaluate(() => { CN.setMode('orders'); });
    await page.evaluate(k => OrderWin.open(k), heldKey);
    await page.waitForFunction(() => OrderWin.isOpen());
    const t1 = Date.now();
    const r3 = await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: HELD, key: heldKey });
    const v3 = await view();
    check(r3 === false && Date.now() - t1 < 2500 && v3.owOpen && v3.q === '' && v3.list === before.list, 'EngraveLink.open on the held piece: false, at once, the window stays, no filter (' + JSON.stringify(v3) + ')');
    // a piece on no sheet and not on hold (pulled): the other plain line
    const pulledKey = await page.evaluate(async ({ key }) => { const r = B.orders.byKey.get(key); r.hold = null; r.state = 'pulled'; r.poolIds = []; return PiecePlacement.ofRow(r).state; }, { key: heldKey });
    const why2 = await page.evaluate(({ rid, key }) => EngraveLink.resolve({ rid, key }).why, { rid: HELD, key: heldKey });
    check(pulledKey === 'waiting' && /not on a sheet yet/.test(why2) && !/on hold/.test(why2), 'a pulled piece on no sheet (not held): "' + why2 + '"');
    await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: HELD, key: heldKey });
    const note2 = await page.evaluate(() => (document.querySelector('dialog[open] .elWait[data-note]') || {}).textContent || '');
    check(/not on a sheet yet/.test(note2) && !/on hold/.test(note2) && (await view()).owOpen, 'and open() says it in the open window (' + note2 + ')');
    // a job with nothing to engrave: stays too
    const noneKey = made.back.jobs[0].key;
    await page.evaluate(k => { const j = Engrave.items().get(k); window.__was = j.state; j.state = 'none'; }, noneKey);
    const r4 = await page.evaluate(k => EngraveLink.open({ rid: '4290000101', key: k }), noneKey);
    const v4 = await view(), note4 = await page.evaluate(() => (document.querySelector('dialog[open] .elWait[data-note]') || {}).textContent || '');
    await page.evaluate(k => { Engrave.items().get(k).state = window.__was; }, noneKey);
    check(r4 === false && v4.owOpen && v4.q === '' && /nothing to engrave/.test(note4), 'a job with nothing to engrave: false, the window stays, no filter (' + note4 + ')');

    // with NO window open the line is a toast
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} });
    await page.waitForFunction(() => !OrderWin.isOpen());
    await page.waitForTimeout(600);
    await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: HELD, key: heldKey });
    const toasts = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent).concat(['dialogs:' + document.querySelectorAll('dialog[open]').length, 'fixedNotes:' + document.querySelectorAll('.elWait').length]));
    check(toasts.some(t => /not on a sheet yet/.test(t)) && !(await view()).q, 'with no window open it is a toast, and still nothing is filtered (' + toasts.join(' | ') + ')');

    // ── 4 · a list that matches nothing is never blank ──
    await toGo('zzz-no-such-order');
    const none = await page.evaluate(() => { const m = document.querySelector('#egQueue .egNone'); return { text: m ? m.textContent : '', btn: !!(m && m.querySelector('[data-eg-clear]')) }; });
    check(/Nothing here matches/.test(none.text) && none.btn, 'a search that matches nothing says so and offers Clear search (' + none.text + ')');
    await page.click('#egQueue .egNone [data-eg-clear]');
    await R.settle(page, 400);
    const cleared = await listState(), vc = await view();
    check(cleared.rows === list0.rows && cleared.box === '' && vc.q === '' && !cleared.none, 'Clear search puts the whole list back and empties the search box (' + JSON.stringify(cleared) + ')');

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
