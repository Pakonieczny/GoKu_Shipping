// RELASK (Paul, 25 Nov 2026: "when I press the release hold button inside the detailed order modal, it does not present me with the pop-up modal to choose if the released charms should be
// nested or not"). An order sent to Review (Send to Review / Hold Order: a review mark) has its pieces made already and is not held: the NESTASK question filtered every line of it out and
// Release Hold released it straight away, in the order window and on the Review card alike. The REAL sorter page over the fake site (_engraveRealWorld.cjs: the real drawings, the real intake,
// the real pool), every Release Hold that can release such an order, each answer:
//   L  the page that carries the run (B.run held here):
//     1  Review card, marked order with its piece made (pooled, on no sheet): the pop-up names the charm; Cancel does nothing at all (still marked, still pooled, nothing written)
//     2  the same card, Skip: out of Review, the line skipped with no pieces left, "no charm is made"
//     3  the ORDER WINDOW's Release Hold on a marked order: the pop-up; Nest: out of Review, the line keeps its pieces (goes on to the sheets), not skipped
//     4  the ORDERS card's Release Hold (when the card draws one for a marked order): the same pop-up
//     5  a held order (the line on hold) still gets the pop-up (nothing it did before is lost)
//     6  a marked order whose only line was skipped before: no pop-up, the release goes through, the line stays skipped
//   V  a staff computer (a viewer: the cloud carries the run):
//     7  Review card, Skip: the cloud's run line is skipped with no pieces, the page's row too, out of Review, a "skipped" step on the timeline
//     8  order window, Nest: out of Review, the cloud's run line keeps its pieces, not skipped
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/release-ask-marked.cjs
'use strict';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let failed = 0, passed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1600))); if (cond) passed++; else failed++; };
ok('source: the bridge asks about a marked order\'s made pieces (markedRow in mayNest) and its cache token carries -ra2',
  /held = !!x\.hold \|\| Orders\.heldNow\(x\) \|\| \(x\.state !== "skipped" && markedRow\(x\)\)/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8')) && /charm-nest-bridge\.js\?v=[^"]*-ra2/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8')));

(async () => {
  const Base = require('./_orderFactsWorld.cjs'), Rw = require('./_engraveRealWorld.cjs');
  const W = await Rw.world({ viewport: { width: 1600, height: 950 }, reducedMotion: 'reduce' });
  if (!W) { console.log('release-ask-marked: no playwright-core, the page part is not run'); return done(); }
  const { page, srv } = W, st = srv.st;
  const V = (name, value) => ({ name, value });
  const fx = (n, sku) => ({ rid: '42930000' + String(n).padStart(2, '0'), sku, listing: '18930000' + String(n).padStart(2, '0'), title: 'Test Charm Necklace', buyer: 'Name ' + n, placed: { day: 0, h: 9, m: n }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] });
  const O = { cancelSkip: fx(1, 'COMET_9427'), winNest: fx(2, 'SCHRODINGER CAT 1'), card: fx(3, 'COMET_9427'), held: fx(4, 'SCHRODINGER CAT 1'), skipped: fx(5, 'COMET_9427'), vSkip: fx(6, 'COMET_9427'), vNest: fx(7, 'SCHRODINGER CAT 1') };
  const keyOf = f => `${f.rid}_${f.rid}1`;
  const RUN = 'run-2026-11-25-relask';
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  const rowOf = f => page.evaluate(k => { const r = B.orders.byKey.get(k); return r ? { state: r.state, pool: (r.poolIds || []).length, hold: !!r.hold, held: Orders.heldNow(r), marked: CharmNestReviewMark.isMarked(String(r.order.receiptId)), reason: r.reason || null } : null; }, keyOf(f));
  const dlg = () => page.evaluate(() => { const d = [...document.querySelectorAll('dialog.naDlg')].find(x => x.open); return d ? { rid: d.dataset.rid, text: d.innerText.replace(/\s+/g, ' ').slice(0, 200) } : null; });
  const answer = async na => { await page.click(`dialog.naDlg[open] [data-na="${na}"]`); await until(async () => !(await dlg()), 5000); };
  const toastsFrom = n0 => page.evaluate(n0 => window.__toasts.slice(n0), n0);
  const nToasts = () => page.evaluate(() => window.__toasts.length);
  const review = () => page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
  const cardBtn = f => `#rvList .reviewListRow[data-rid="${f.rid}"] [data-rel-hold]`;
  const openWin = async f => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForTimeout(300); await page.evaluate(k => OrderWin.open(k), keyOf(f)); await until(() => page.evaluate(() => OrderWin.isOpen() && !document.getElementById('owPcSum').hidden), 20000); await page.waitForTimeout(1500); };
  const winBtn = '#owPcSum .owPcRow [data-rel-hold]';
  const pressAndAsk = async sel => { if (!(await until(() => page.$(sel), 20000))) return { err: 'no button ' + sel }; await page.click(sel); const d = await until(dlg, 8000); return d || { none: true }; };
  const settled = (f, n0) => until(() => page.evaluate(({ rid, n0 }) => window.__toasts.length > n0 && !CharmNestReviewMark.isMarked(rid) && !document.querySelector('[data-rel-hold][aria-busy="true"]'), { rid: f.rid, n0 }), 60000);
  const writeList = () => st.calls.filter(c => c.name === 'charmNestLibrary' && c.body && ['reviewMarkPut', 'releaseAsk', 'orderHoldRelease', 'releaseJobPut', 'timelineAdd'].includes(c.body.op)).map(c => c.body.op + ':' + (c.body.action || '') + ':' + (c.body.orderId || c.body.rid || ''));
  const writes = () => writeList().length;
  // (the fake site keeps no workspace on the browser: "Workspace could not be saved on this browser" is that, never the release's)
  const bad = said => said.filter(t => t[0] === 'bad' && !/Workspace could not be saved on this browser/.test(t[1]));
  try {
    await page.evaluate(RUN => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, RUN);
    await page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const f of fixtures) { const order = orderOf(f, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: Object.values(O), orderSrc: Base.orderOf.toString() });
    await page.evaluate(async () => { await Pool.addAll(B.run); });
    const made = {}; for (const [n, f] of Object.entries(O)) made[n] = await rowOf(f);
    ok('0 every order\'s piece is made (pooled, with its pool ids) by the real pool', Object.values(made).every(r => r && r.state === 'pooled' && r.pool > 0), made);
    // nothing of it is on a sheet (the pop-up's own test of that: PiecePlacement)
    const onSheet = await page.evaluate(keys => keys.map(k => { try { const p = PiecePlacement.ofRow(B.orders.byKey.get(k)); return !!(p && p.onSheet); } catch (_) { return 'err'; } }), Object.values(O).map(keyOf));
    ok('0 ... and on no sheet', onSheet.every(x => x === false), onSheet);

    /* ── L: the run's own page ── */
    for (const f of [O.cancelSkip, O.winNest, O.card, O.skipped]) ok(`L mark ${f.rid}`, (await page.evaluate(rid => CharmNestReviewMark.mark(rid), f.rid)).ok === true);
    await page.evaluate(k => { const r = B.orders.byKey.get(k); Object.assign(r, { state: 'skipped', poolIds: [], reason: 'no charm made: skipped by hand' }); }, keyOf(O.skipped));
    await page.evaluate(k => { const r = B.orders.byKey.get(k); const t = 'Taken off GF Sheet 1 by Test Operator'; Object.assign(r, { state: 'held', hold: t, reason: t, heldAt: Date.now() }); }, keyOf(O.held));

    // 1 · Review card, Cancel
    await review();
    let w0 = writes(), wl0 = writeList(), d = await pressAndAsk(cardBtn(O.cancelSkip));
    ok('1 Review card, a marked order with its piece made: Release Hold asks Nest it / Skip it, naming the charm', d && d.rid === O.cancelSkip.rid && /Put this charm on a sheet\?/.test(d.text) && /COMET_9427/.test(d.text) && /Nest it/.test(d.text) && /Skip it/.test(d.text), d);
    await answer(''); await page.waitForTimeout(1500);
    let r = await rowOf(O.cancelSkip);
    const relOf = rid => st.calls.filter(c => c.name === 'charmNestLibrary' && c.body && ((['releaseAsk', 'orderHoldRelease', 'releaseJobPut'].includes(c.body.op) && String(c.body.orderId) === rid) || (c.body.op === 'reviewMarkPut' && ['resolve', 'release'].includes(c.body.action) && String(c.body.orderId || c.body.rid) === rid) || (c.body.op === 'timelineAdd' && (c.body.events || []).some(e => String(e.orderId) === rid && ['released', 'skipped'].includes(e.type))))).map(c => c.body.op + ':' + (c.body.action || ''));
    const wl1 = relOf(O.cancelSkip.rid);   // (the marks made just before may still be landing: only a release of THIS order counts)
    ok('1 ... Cancel does nothing at all: still marked, still pooled with its pieces, no release or Review write for it', r.marked && r.state === 'pooled' && r.pool > 0 && !wl1.length, { r, wl1, all: writeList().slice(wl0.length) });

    // 2 · the same card, Skip
    await review();
    let n0 = await nToasts(); d = await pressAndAsk(cardBtn(O.cancelSkip));
    ok('2 pressed again: asked again', !!(d && d.rid === O.cancelSkip.rid), d);
    await answer('skip'); await settled(O.cancelSkip, n0); await page.waitForTimeout(1200);
    r = await rowOf(O.cancelSkip); let said = await toastsFrom(n0);
    ok('2 Skip: out of Review, the line skipped with no pieces left', !r.marked && r.state === 'skipped' && r.pool === 0 && /skipped at Release Hold by Test Operator/.test(r.reason || ''), r);
    ok('2 ... told "no charm is made" (and never "it goes on to the sheets"), no failure', said.some(t => /no charm is made/.test(t[1])) && !said.some(t => /goes on to the sheets/.test(t[1])) && !bad(said).length, said);

    // 3 · the order window, Nest
    await openWin(O.winNest);
    const wb = await page.$$eval(winBtn, l => l.map(b => ({ t: b.textContent.trim(), on: b.getAttribute('aria-disabled') !== 'true' })));
    ok('3 the order window of a marked order: its piece row has Release Hold, enabled', wb.length === 1 && wb[0].t === 'Release Hold' && wb[0].on, wb);
    n0 = await nToasts(); d = await pressAndAsk(winBtn);
    ok('3 ... pressed in the window: the pop-up (this is what was missing)', d && d.rid === O.winNest.rid && /SCHRODINGER CAT 1/.test(d.text), d);
    await answer('nest'); await settled(O.winNest, n0); await page.waitForTimeout(1200);
    r = await rowOf(O.winNest); said = await toastsFrom(n0);
    ok('3 Nest: out of Review, the line keeps its pieces (goes on to the sheets), not skipped', !r.marked && r.state !== 'skipped' && r.pool > 0, r);
    ok('3 ... told it left Review for the sheets, no failure', said.some(t => /left Review: it goes on to the sheets/.test(t[1])) && !bad(said).length, said);
    await page.evaluate(() => OrderWin.isOpen() && OrderWin.close());

    // 4 · the Orders card
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await page.waitForTimeout(800);
    const ordSel = `#ordBody [data-rid="${O.card.rid}"] .relHold:not([data-gate])`, hasOrd = !!(await until(() => page.$(ordSel), 4000));
    if (hasOrd) {
      d = await pressAndAsk(ordSel);
      ok('4 the Orders card\'s Release Hold on a marked order: the same pop-up', d && d.rid === O.card.rid, d);
      if (d && d.rid) { await answer(''); await page.waitForTimeout(800); }
      r = await rowOf(O.card); ok('4 ... Cancel: nothing changed', r.marked && r.state === 'pooled' && r.pool > 0, r);
    } else {
      console.log('  · 4: the Orders card draws no Release Hold for a marked order whose line is not held (as before this change); its Review card and order window have it');
      await openWin(O.card); d = await pressAndAsk(winBtn);
      ok('4 (its order window instead) the pop-up', d && d.rid === O.card.rid, d);
      if (d && d.rid) { await answer(''); await page.waitForTimeout(800); }
      await page.evaluate(() => OrderWin.isOpen() && OrderWin.close());
    }

    // 5 · a held order (regression)
    await review(); d = await pressAndAsk(cardBtn(O.held));
    ok('5 a held order still gets the pop-up', d && d.rid === O.held.rid, d);
    if (d && d.rid) { await answer(''); await page.waitForTimeout(800); }
    r = await rowOf(O.held); ok('5 ... Cancel: still held', r.held, r);

    // 6 · a marked order whose only line was skipped before
    await review(); n0 = await nToasts(); d = await pressAndAsk(cardBtn(O.skipped));
    ok('6 a marked order whose only line was skipped before: no pop-up (nothing to make)', d && d.none === true, d);
    if (d && d.rid) await answer('');
    await settled(O.skipped, n0); await page.waitForTimeout(1000);
    r = await rowOf(O.skipped); said = await toastsFrom(n0); ok('6 ... released (out of Review), the line stays skipped, told no charm is made (not "goes on to the sheets")', !r.marked && r.state === 'skipped' && said.some(t => /left Review: no charm is made/.test(t[1])) && !said.some(t => /goes on to the sheets/.test(t[1])) && !bad(said).length, { r, said });

    /* ── V: a staff computer (a viewer) ── */
    const recs = await page.evaluate(() => Object.fromEntries(Orders.rows().map(Orders.lineRecord)));
    await page.evaluate(RUN => {
      B.run = null;
      const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: RUN });
      window.__viewerOn = CNViewer.on; CNViewer.on = () => true; CNViewer.asViewer();
      Session.freeze();
    }, RUN);
    st.put('Charm_Nest_Runs', RUN, { runId: RUN, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-25', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: Object.values(O).map(f => f.rid), lines: recs }, false);
    st.put('Charm_Nest_Open', 'run', { runId: RUN, at: Date.now() });
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'takeoff', 'cmd:release', 'cmd:placewaiting'], flippedAt: Date.now() - 3600e3 });
    st.put('Charm_Nest_Run_Carrier', RUN, { runId: RUN, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    const viewer = await page.evaluate(() => ({ frozen: Session.frozen(), run: !!B.run, on: CNViewer.on() }));
    ok('V the page is a viewer: workspace frozen, no run held', viewer.frozen && !viewer.run && viewer.on, viewer);
    const cloudLine = k => { const lines = Object.assign({}, (st.doc('Charm_Nest_Runs', RUN) || {}).lines || {}); for (const p of st.list('Charm_Nest_Run_Live').filter(x => x.runId === RUN)) { try { Object.assign(lines, JSON.parse(p.json || '{}')); } catch (_) {} } return lines[k] || null; };
    for (const f of [O.vSkip, O.vNest]) ok(`V mark ${f.rid}`, (await page.evaluate(rid => CharmNestReviewMark.mark(rid), f.rid)).ok === true);

    // 7 · Review card, Skip
    await review(); n0 = await nToasts(); d = await pressAndAsk(cardBtn(O.vSkip));
    ok('7 viewer, Review card: the pop-up', d && d.rid === O.vSkip.rid, d);
    await answer('skip'); await settled(O.vSkip, n0);
    const l7 = await until(() => { const l = cloudLine(keyOf(O.vSkip)); return l && l.state === 'skipped' ? l : null; }, 20000) || cloudLine(keyOf(O.vSkip));
    r = await rowOf(O.vSkip); said = await toastsFrom(n0);
    ok('7 Skip: the cloud\'s run line is skipped with no pieces, in the person\'s name', !!l7 && l7.state === 'skipped' && !(l7.poolIds || []).length && /skipped at Release Hold by Test Operator/.test(l7.reason || ''), l7);
    ok('7 ... the page\'s row says so too (no pieces), out of Review, told "no charm is made" (never "goes on to the sheets"), no failure', !r.marked && r.state === 'skipped' && r.pool === 0 && said.some(t => /no charm is made/.test(t[1])) && !said.some(t => /goes on to the sheets/.test(t[1])) && !bad(said).length, { r, said });
    const sk7 = await until(() => st.list('Order_Timeline').filter(e => String(e.orderId) === O.vSkip.rid && e.type === 'skipped').length || null, 15000);
    ok('7 ... a "skipped" step on the order\'s timeline', !!sk7, st.list('Order_Timeline').filter(e => String(e.orderId) === O.vSkip.rid).map(e => [e.type, e.text]));

    // 8 · order window, Nest
    await openWin(O.vNest); n0 = await nToasts(); d = await pressAndAsk(winBtn);
    ok('8 viewer, order window: the pop-up', d && d.rid === O.vNest.rid, d);
    await answer('nest'); await settled(O.vNest, n0); await page.waitForTimeout(1500);
    const l8 = cloudLine(keyOf(O.vNest)); r = await rowOf(O.vNest); said = await toastsFrom(n0);
    ok('8 Nest: out of Review, the cloud\'s run line keeps its pieces, not skipped; the page\'s row too', !r.marked && !!l8 && l8.state !== 'skipped' && (l8.poolIds || []).length > 0 && r.state !== 'skipped' && r.pool > 0, { r, l8: l8 && { state: l8.state, pool: (l8.poolIds || []).length } });
    ok('8 ... no failure told', !bad(said).length, said);
    await page.evaluate(() => { OrderWin.isOpen() && OrderWin.close(); CNViewer.on = window.__viewerOn; });
  } catch (e) { ok('run', false, String(e && e.stack || e).split('\n').slice(0, 5).join(' | ')); }
  const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
  ok('no page error', errs.length === 0, errs.slice(0, 3).join(' | '));
  await W.close();
  done();
})().catch(e => { console.log('FAIL release-ask-marked: ' + (e && e.stack || e)); process.exit(1); });
function done() { console.log(`\nrelease-ask-marked: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0); }
