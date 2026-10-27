// RELHOLD in the REAL page (Paul, 18 Oct 2026: "Replace the 'Completed' function & button with a new button and functionality called 'Release Hold'", "Modify and update the current Stamp/Seal used to
// represent 'Completed' into an updated design representing 'Release Hold'", pressed "the order is released back into the Nesting queue", and "In the Orders tab, that released order's card shows a large,
// clearly visible 'Release Hold' seal with the date/time and the person who released it"; and, order 4204568396 SEA_96442: "Remove this 'Release to nesting queue' button since it will clash with the new
// 'Release Hold' button"). The real sorter in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory store), motion ON. Every name is made up.
//   1  the Review rows: Release Hold stands in the card's own column where Complete Order stood (no Complete Order on them, no "Release to nesting queue" anywhere, none under a reason): enabled (RHSAME, 20 Oct 2026: the seal orange, .btn.relHoldBtn; was gold) on an
//      order sent to Review, and (RELEASE, Paul 19 Oct 2026) on an unknown SKU too: its old reason is the note "Releasing anyway: ..." on the title, not a grey; a Custom Orders card has the same Release Hold (RELORDER, Paul 20 Oct 2026; it kept Complete Order before)
//   2  the press: the RELEASE HOLD seal is stamped on the button (Seal.stampOn, how "releaseHold", who) as the card leaves Review; the order is released (its Send to Review mark resolved: out of Review),
//      who and when kept on its lines (row.releaseHold, in the run's line record and back from it), in the cloud (its Review document's release { at, by }) and on its timeline (a "released" step, data.seal)
//   3  the Orders tab: the order's card shows the seal LARGE (72 px), family "release", with "Release Hold", the person and the date / time
//   4  a reload / another computer: a second page over the same cloud, the order pulled afresh (no line record of the press): the same seal, the same person and time, read from the cloud's record
//   5  the seal itself: RELEASE HOLD, the release family, a process seal; an old Complete Order seal (how "button") is drawn as it always was (ORDER COMPLETE, fulfilment)
//   PW_DIR=<playwright node_modules> node tests/charm-nest/release-hold.cjs     (SHOTS=<dir> saves PNGs)
'use strict';
const path = require('path'), fs = require('fs');
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
const V = (name, value) => ({ name, value });
const MARKED = { rid: '4290000233', sku: 'COMET_9427', listing: '1890000233', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Twenty', placed: { day: 0, h: 9, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const UNKNOWN = { rid: '4290000234', sku: 'NO_SUCH_DESIGN_78', listing: '1890000234', title: 'Mystery Charm Necklace', buyer: 'Name Twentyone', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const keyOf = fx => `${fx.rid}_${fx.rid}1`;
const TL = 'Order_Timeline';
const OLD = () => ({ old: [...document.querySelectorAll('button')].filter(b => /Release to nesting queue/i.test(b.textContent)).length, under: document.querySelectorAll('.rvRel').length });

const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };

(async () => {
  const shots = process.env.SHOTS || '';
  const W = await Base.world({ viewport: { width: 1440, height: 950 }, reducedMotion: 'no-preference' });
  if (!W) { console.log('no playwright-core: skipped'); process.exit(0); }
  const { page, srv, context } = W, st = srv.st;
  const saw = []; page.on('console', m => saw.push(`[${m.type()}] ${m.text()}`.slice(0, 500)));
  const until = async (f, ms, what, pg) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await (pg || page).waitForTimeout(250); } };
  const shot = async (name, pg) => { if (!shots) return; try { fs.mkdirSync(shots, { recursive: true }); await (pg || page).screenshot({ path: path.join(shots, name + '.png') }); } catch (_) {} };
  const seedRows = (pg, fixtures) => pg.evaluate(({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
    Orders.interpretAll();
  }, { fixtures, orderSrc: Base.orderOf.toString() });
  const sealOnCard = (pg, rid) => pg.evaluate(rid => {
    const n = [...document.querySelectorAll('#ordBody [data-rid]')].find(x => x.dataset.rid === rid); if (!n) return { none: 'card' };
    const s = n.querySelector('.relSeal'); if (!s) return { none: 'seal' };
    const face = s.querySelector('.seal'), svg = face && face.querySelector('svg'), r = face.getBoundingClientRect();
    return { family: svg && svg.dataset.sealFamily, text: svg && svg.textContent, words: s.querySelector('.relSealWords').textContent, by: s.querySelector('.relSealBy').textContent, at: +s.dataset.relSeal, w: Math.round(r.width), aria: face.getAttribute('aria-label'), kind: [...face.classList].join(' ') };
  }, rid);
  try {
    R.seedMaster(st);
    await page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); });
    await page.evaluate(() => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      window.__stamps = []; const so = Seal.stampOn; Seal.stampOn = (host, spec) => { try { const s = spec && (spec.stamp || spec); window.__stamps.push({ how: s && s.how, by: s && s.by, at: s && s.at, btn: typeof spec.btn === 'string' ? spec.btn : '', label: spec.label || '' }); } catch (_) {} return so(host, spec); };
    });
    await seedRows(page, [MARKED, UNKNOWN]);
    const marked = await page.evaluate(async rid => { const r = await CharmNestReviewMark.mark(rid, { by: 'Test Operator' }); CN.setMode('review'); Review.syncOrderItems(); Review.render(); return r; }, MARKED.rid);
    check(marked && marked.ok, `0 · the comet order is sent to Review (${JSON.stringify(marked)})`);
    const rowSel = fx => `#rvList .reviewListRow[data-rid="${fx.rid}"]`;
    await until(() => page.$(rowSel(MARKED) + ' [data-rel-hold]'), 20000, 'Release Hold on the marked row');

    // ── 1 · the rows ──
    const look = fx => page.evaluate(sel => {
      const n = document.querySelector(sel); if (!n) return null; const bs = n.querySelectorAll('[data-rv-release]'), b = bs[0];
      return { n: bs.length, text: b && b.textContent.trim(), cls: b && b.className, how: b && b.dataset.rvRelease, disabled: b && b.getAttribute('aria-disabled'), title: b && b.title, parent: b && b.parentElement.className, complete: !!n.querySelector('[data-cu-complete]'), col: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()) };
    }, rowSel(fx));
    const m1 = await look(MARKED), u1 = await look(UNKNOWN);
    check(m1 && m1.n === 1 && m1.text === 'Release Hold' && m1.how === 'mark' && m1.disabled === null && /\brelHoldBtn\b/.test(m1.cls) && m1.parent === 'rowActions' && !m1.complete, `1 · the marked order: ONE Release Hold, enabled, in the card's column, no Complete Order (${JSON.stringify(m1)})`);
    // (RELEASE, Paul 19 Oct 2026: was "greyed with its one plain reason"; the unknown SKU's question is set aside by the press, so the button is enabled and the old reason is a note in its title)
    check(u1 && u1.n === 1 && u1.text === 'Release Hold' && u1.how === 'anyway' && u1.disabled === null && /\brelHoldBtn\b/.test(u1.cls) && /Releasing anyway: SKU NO_SUCH_DESIGN_78 has no design in any master file yet/.test(u1.title) && !u1.complete && u1.col[1] === 'Release Hold', `1 · the unknown SKU: Release Hold enabled, its old reason a note in the title, where Complete Order stood (${JSON.stringify(u1)})`);
    check(JSON.stringify(await page.evaluate(OLD)) === JSON.stringify({ old: 0, under: 0 }), '1 · no "Release to nesting queue" button anywhere, none under a reason');
    await shot('01-review');
    // (RHSAME, Paul 20 Oct 2026: "This Release Hold button [Orders tab] needs to be the exact same as the one in the Review tab", the seal's orange on both) the unknown SKU's order held for a moment: its Orders card's Release Hold
    // is the Review card's own button (ReviewRelease.orderButton on the Review card's item, the same press), the same words, class and colour as the Review row's, the seal's orange #a2591c
    const rh = await page.evaluate(async ({ sel, rid }) => {
      const rvB = document.querySelector(sel + ' [data-rv-release]'), rvBg = rvB && getComputedStyle(rvB).backgroundColor;
      const rows = B.orders.rows.filter(r => String(r.order.receiptId) === rid); for (const r of rows) r.hold = 'test hold';
      CN.setMode('orders'); Orders.showPile('', ''); Orders.renderNow ? Orders.renderNow() : Orders.render(); await new Promise(r => setTimeout(r, 300));
      const card = [...document.querySelectorAll('#ordBody [data-rid]')].find(x => x.dataset.rid === rid), b = card && card.querySelector('.relHold:not([data-gate])');
      const it = ReviewRelease.itemOf(rid), out = { text: b && b.textContent.trim(), cls: b && b.className, how: b && b.dataset.rvRelease, bg: b && getComputedStyle(b).backgroundColor, rvBg, rvCls: rvB && rvB.className, item: it.key, card: Review.items().find(i => i.kind === 'unmatchedSku' && (i.rows || [i.row]).some(r => r && String(r.order.receiptId) === rid))?.key, itHow: ReviewRelease.of(it).how };
      for (const r of rows) delete r.hold; CN.setMode('review'); Review.render(); return out;
    }, { sel: rowSel(UNKNOWN), rid: UNKNOWN.rid });
    check(rh.text === 'Release Hold' && /\brelHoldBtn\b/.test(rh.cls) && /\bsm\b/.test(rh.cls) && rh.how === rh.itHow && rh.how === 'anyway' && rh.item && rh.item === rh.card && rh.bg === 'rgb(162, 89, 28)' && rh.bg === rh.rvBg && /\brelHoldBtn\b/.test(rh.rvCls), `1 · RHSAME: the Orders card's Release Hold is the Review card's: same press (its item), words, class and seal orange (${JSON.stringify(rh)})`);

    // ── 2 · the press ──
    const t0 = Date.now();
    await page.click(rowSel(MARKED) + ' [data-rel-hold]');
    await until(() => page.evaluate(rid => !CharmNestReviewMark.isMarked(rid) && !Review.items().some(i => i.kind === 'reviewMark' && i.rid === rid), MARKED.rid), 30000, 'the order to leave Review');
    const left2 = await until(async () => !(await page.$(rowSel(MARKED))), 10000, 'the row to go').then(() => null, async () => page.evaluate(sel => { const n = document.querySelector(sel); return { q: n && n.querySelector('.queueLabel') && n.querySelector('.queueLabel').textContent, items: Review.items().filter(i => (i.rows || [i.row]).some(r => r && String(r.order.receiptId) === '4290000233')).map(i => i.kind), seg: Review.view().cseg, busy: Seal.busy() }; }, rowSel(MARKED)));
    check(!left2, `2 · the order left Review Open (its mark resolved, its row gone) ${JSON.stringify(left2)}`);
    const stamped = await until(() => page.evaluate(() => window.__stamps.find(s => s.how === 'releaseHold')), 10000, 'the stamp on the leaving card').catch(() => null);
    check(stamped && stamped.by === 'Test Operator' && stamped.btn === '[data-rel-hold]' && stamped.label === 'Release Hold' && stamped.at >= t0 - 1000, `2 · the RELEASE HOLD seal is stamped on the button as the card leaves (${JSON.stringify(stamped)})`);
    await page.waitForTimeout(800);
    const r2 = await page.evaluate(k => { const row = B.orders.byKey.get(k), rec = Orders.lineRecord(row)[1], back = Orders.rowFromRecord(k, JSON.parse(JSON.stringify(rec))); return { mine: row.releaseHold || null, rec: rec.releaseHold || null, back: back.releaseHold || null }; }, keyOf(MARKED));
    check(r2.mine && r2.mine.by === 'Test Operator' && r2.mine.at >= t0 - 1000 && r2.rec && r2.rec.at === r2.mine.at && r2.back && r2.back.by === 'Test Operator', `2 · who and when on the line, in its record and back from it (${JSON.stringify(r2)})`);
    const cloud = await until(() => { const d = st.doc('Charm_Custom_Orders', MARKED.rid + '_review'); return d && d.release && d.release.at && d; }, 20000, 'the cloud record of the release');
    check(cloud.release.by === 'Test Operator' && cloud.release.at === r2.mine.at && cloud.review && cloud.review.state === 'done', `2 · the cloud keeps it on the order's Review document, beside its resolved mark (${JSON.stringify({ release: cloud.release, review: cloud.review && cloud.review.state })})`);
    const ev = await until(() => st.list(TL).find(e => String(e._id).startsWith(MARKED.rid + '~') && e.type === 'released' && e.data && e.data.seal === 'releaseHold'), 30000, 'the released step on the timeline').catch(() => null);
    check(ev && ev.by === 'Test Operator' && +ev.data.sealAt === r2.mine.at, `2 · the timeline's "released" step carries the seal (${JSON.stringify(ev && { text: ev.text, by: ev.by, data: ev.data })})`);
    const t2 = await page.evaluate(() => window.__toasts.filter(t => t[0] === 'bad'));
    check(!t2.length, `2 · no failure toast (${JSON.stringify(t2)})`);

    // ── 3 · the Orders tab ──
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });
    const s3 = await until(async () => { const s = await sealOnCard(page, MARKED.rid); return s && !s.none && s; }, 20000, 'the seal on the Orders card');
    const when = await page.evaluate(at => new Date(at).toLocaleString('en-US', { timeZone: 'America/Toronto', month: 'short', day: 'numeric', year: 'numeric' }), r2.mine.at);
    check(s3.family === 'release' && /ORDER/.test(s3.text) && /RELEASED/.test(s3.text) && s3.w >= 46 && /seal-releaseHold/.test(s3.kind), `3 · the Orders card shows the RELEASE HOLD seal, large (${JSON.stringify(s3)})`);
    check(/Order released/.test(s3.words) && s3.by === 'by Test Operator' && s3.words.includes(when) && /\d{1,2}:\d{2}\s?[AP]M/.test(s3.words) && s3.at === r2.mine.at && /Hold released to the nesting queue by Test Operator/.test(s3.aria), `3 · with "Order released", who, the date and the time (and, RELEASED 27 Oct 2026, when it had been held) (${s3.words})`);
    const none = await sealOnCard(page, UNKNOWN.rid);
    check(none && none.none === 'seal', `3 · an order never released has none (${JSON.stringify(none)})`);
    await shot('03-orders');

    // ── 4 · a reload / another computer: the order pulled afresh, nothing kept on its lines; the seal comes from the cloud ──
    const p2 = await context.newPage(); p2.setDefaultTimeout(30000);
    await p2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await p2.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await p2.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); });
    // (the same browser restores this page's workspace: its rows go, so the order is pulled afresh and nothing of the press is on its lines; only the cloud can say it)
    await p2.waitForFunction(() => !window.Session || !Session.ready || Session.ready(), null, { timeout: 30000 }).catch(() => {});
    await p2.evaluate(() => { B.orders.rows = []; B.orders.byKey = new Map(); });
    await seedRows(p2, [MARKED]);
    check(await p2.evaluate(() => B.orders.rows.every(r => !r.releaseHold) && B.orders.rows.length === 1), '4 · the other page holds the order once, pulled afresh, with no release on its line');
    check(await p2.evaluate(k => !B.orders.byKey.get(k).releaseHold, keyOf(MARKED)), '4 · the line on the other page knows nothing of the press');
    await p2.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });
    const s4 = await until(async () => { const s = await sealOnCard(p2, MARKED.rid); return s && !s.none && s; }, 20000, 'the seal on the other page', p2).catch(() => null);
    check(s4 && s4.family === 'release' && s4.by === 'by Test Operator' && s4.at === r2.mine.at && s4.words.includes(when), `4 · the other page draws the same seal, person and time from the cloud (${JSON.stringify(s4)})`);
    await shot('04-other-page', p2);
    await p2.close();

    // ── 5 · the seal itself, and the old Complete Order seal ──
    const f5 = await page.evaluate(() => { const at = Date.UTC(2026, 9, 18, 19, 14); const m = Seal.modelOf({ how: 'releaseHold', at, by: 'X' }), o = Seal.modelOf({ how: 'button', at, by: 'X' }); return { m, o, fam: !!Seal.FAMILY.release, title: Seal.titleOf({ how: 'releaseHold', at, by: 'X' }), old: Seal.titleOf({ how: 'button', at, by: 'X' }), oldHtml: /seal-button/.test(Seal.html({ how: 'button', at, by: 'X' })) }; });
    check(f5.fam && f5.m.family === 'release' && f5.m.action === 'ORDER RELEASED' && f5.m.icon === 'release' && /^Hold released to the nesting queue by X/.test(f5.title), `5 · the RELEASE HOLD seal (${JSON.stringify(f5.m)})`);
    check(f5.o.family === 'fulfilment' && f5.o.action === 'ORDER COMPLETE' && /Complete Order button/.test(f5.old) && f5.oldHtml, `5 · an old Complete Order seal is drawn as it always was (${JSON.stringify(f5.o)})`);

    const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
    console.log(saw.slice(-25).join('\n'));
  } finally { await W.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nrelease-hold: ${passed} checks passed`);
  process.exit(fails.length ? 1 : 0);
})();
