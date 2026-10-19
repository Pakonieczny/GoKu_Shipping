// RELSEAL (Paul, 19 Oct 2026): "I believe we already designed a custom seal for releasing orders, and the seal would be visible in the list of view of each order card and both the Engraving and Orders Tabs as part of
// each relevant Order card. Please verify."   (the seal is RELHOLD's: the Release Hold press leaves a 72 px seal with "Release Hold", who, the date and the time; this test is where it must be SEEN)
// The real sorter in headless Chromium over the repo's fake site (the engraving-UI real world: real orders through the real intake, pool, classifier and fit). Orders are released the way a person does it: the Review card's Release Hold button
// (and, for the live cases, the same press from code while the tab in question is in front). Every name is made up.
//   1  source: the seal is drawn once per card, in the seals row beside the approval strip (CNOrderSeals), no longer in the actions column; the Engraving rows have it in their seals cell; nothing in it reads, waits or listens; cache token
//   ORDLIST (Paul, 19 Oct 2026): the Orders rows and cards are laid out like the Engraving rows: a one-line order's Release Hold seal stands first in its seals cell (.egPlacementSeals, as on an Engraving row), a grouped order's in its head; the approval strip is gone
//   2  Orders tab, CARDS view: an engraved order, an order of two lines (one card for the order), a monogram: the same 72 px seal with its words, whole inside the card, in the seals row beside the approval strip; an order never released: none
//   3  Orders tab, LIST view (rows): the same
//   4  Engraving tab (Placements list): the engraved row, the two-face row (front + back), the monogram row: the same seal, in the seals cell, whole inside the row; engraved orders never released: none
//   5  live: a release pressed while the Engraving tab (or the Orders tab) is in front paints its seal at once, no tab change; a seal already drawn is kept (the card / row is not drawn again) and nothing is asked of the network for it
//   6  a reload: the order's lines, the Engrave jobs and the seals come back; and with the lines' own record of the press taken away, the cloud's Review document still gives the seal (the order released on ANOTHER computer: a page that
//      pulled the order afresh shows it on its Orders card and its row too)
//   PW_DIR=<playwright node_modules> node tests/charm-nest/release-seal-cards.cjs     (SHOTS=<dir> saves PNGs)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const SHOTS = process.env.SHOTS || '';
const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };

// ═══ 1 · the source ═══
try {
  const bridge = read('charm-nest-bridge.js'), html = read('charm-nest-1.html');
  const code = s => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + 10); if (!(i > 0 && j > i)) throw new Error('markers ' + start); return src.slice(i, j); };
  const group = fn(bridge, 'function groupNode(', 'function renderBody()'), single = fn(bridge, 'const { sp, m, st, shown, due, where, fc, attn, why, gateBtn, mail, team, sealRec, seals, held, rel } = lineView(r)', 'const number=node.querySelector');
  check(/ordSealsRow" data-testid="order-seals-row">\$\{releaseSealHtml\(rel\)\}/.test(group) && /lineSealsHtml\(r, e, rel\)/.test(single) && !/rowActions[^`]*releaseSealHtml/.test(code(single)) && /relSig\(rel\)/.test(group) && /relSig\(rel\)/.test(single),
    'source: (ORDLIST) a grouped order draws the Release Hold seal once in its head, a one-line card / row first in its seals cell (lineSealsHtml), never in the actions column; the seal is in the stamp (a release repaints it)');
  const row = fn(bridge, 'const cardSeals = rows =>', '/** The pictures a node of an earlier drawing'), eg = fn(bridge, 'const releaseOfRow = row =>', '  function renderPlacementRows(');
  check(/const lineSealsHtml = \(r, e, rel\) => `<div class="egPlacementSeals">\$\{rel \? releaseSealHtml\(rel\) : ""\}\$\{e \? e\.seals : recordSealHtml\(r\)\}<\/div>`/.test(row), 'source: the seals cell holds the release seal first and the line\'s approval seals after it (no strip)');
  check(!/fetch\(|api\(|setTimeout|setInterval|requestAnimationFrame|addEventListener|onSnapshot|OrderTimeline|ensureSeals|loadSeals/.test(code(row) + code(eg)), 'source: the seals row and the Engraving rows\' lookup read what the page holds: no read, timer or listener');
  const place = fn(bridge, 'function renderPlacementRows(', 'function quietRow('), mono = fn(bridge, 'function monoPlacementRow(grp) {', 'function monoStep(');
  check(/rel\.html\(\)\+\(told\?/.test(place) && /"\|"\+rel\.sig|\+"\|"\+rel\.sig/.test(place) && /rel\.html\(\) \+ \(window\.CNMonogramSeals/.test(mono) && /"\|" \+ rel\.sig/.test(mono), 'source: the Engraving tab\'s engraved row and monogram row draw the seal first in their seals cell, and keep its signature in the cell\'s stamp');
  check(/\.ordSealsRow\{/.test(html) && !/\.relSealAt\{[^}]*nowrap/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-rc1/.test(html), 'source: the seals row has its CSS, the date and time wrap (they were cut at the card\'s edge), the bridge\'s cache token carries -rc1');
} catch (e) { fails.push('source: ' + e.message); console.log('  FAIL source: ' + e.message); }

// ═══ the real page ═══
(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('release-seal-cards: no playwright-core, the browser part is skipped'); return finish(); }
  process.env.PW_DIR = pwDir;
  const R = require('./_engraveRealWorld.cjs'), Alpha = require('./_monogramAlphabets.cjs'), V = (name, value) => ({ name, value });
  Object.assign(R.ORDERS, {
    g2: Object.assign({}, R.ORDERS.back, { rid: '4290000141', listing: '1890000141', buyer: 'Name Fifteen', placed: { day: 0, h: 11, m: 0 }, more: [{ sku: 'COMET_9427', title: R.ORDERS.back.title, variations: [V('Metal choice', 'Silver'), V('Font Choice', 'Stylish')] }] }),   // (two lines: one card for the order)
    m1: { rid: '4290000131', sku: 'MONOGRAM_9317', listing: '606439317', title: 'Monogram Necklace Dainty Monogram Necklace', buyer: 'Name Eleven', placed: { day: 0, h: 10, m: 20 }, ship: 5, variations: [V('Metal choice', 'Silver')], personalization: ['A P V'] }
  });
  R.EXPECT_JOBS.g2 = 1; R.EXPECT_JOBS.m1 = 0;
  const RID = n => R.ORDERS[n].rid;
  // pressed on the Review card: back (one engraved line), g2 (two lines), m1 (a monogram) · pressed while a tab is in front: both (front + back: the two-face row) on Engraving, earrings (never engraved) on Orders · never released: front
  const BY_CARD = ['back', 'g2', 'm1'], LIVE_ENG = 'both', LIVE_ORD = 'earrings', NEVER = 'front';
  const NAMES = [...BY_CARD, LIVE_ENG, LIVE_ORD, NEVER];
  const w = await R.world({ viewport: { width: 1600, height: 1000 }, reducedMotion: 'reduce', shots: SHOTS || undefined });
  if (!w) { console.log('release-seal-cards: no browser, the page part is skipped'); return finish(); }
  const { page, srv, context } = w, st = srv.st;
  const until = async (f, ms, what, pg) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await (pg || page).waitForTimeout(250); } };
  const shot = async (name, pg) => { if (!SHOTS) return; try { fs.mkdirSync(SHOTS, { recursive: true }); await (pg || page).screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (_) {} };
  const saw = []; page.on('console', m => saw.push(`[${m.type()}] ${m.text()}`.slice(0, 400)));
  /** what one surface draws for each order: the seal's size, family, words, and whether it is whole inside the card / row (not cut at its edge) */
  const probe = (pg, scope) => pg.evaluate(scope => {
    const out = {};
    for (const n of document.querySelectorAll(scope)) {
      const rid = n.dataset.rid, s = n.querySelector('.relSeal'), nb = n.getBoundingClientRect(), sb = s && s.getBoundingClientRect(), face = s && s.querySelector('.seal'), svg = face && face.querySelector('svg');
      const words = s ? [...s.querySelectorAll('.relSealWords > *')] : [], strip = n.querySelector('.ordSeals'), tb = strip && strip.getBoundingClientRect();
      const o = {
        seal: !!s, w: face ? Math.round(face.getBoundingClientRect().width) : 0, family: svg && svg.dataset.sealFamily, text: svg ? svg.textContent : '', all: s ? s.querySelector('.relSealWords').textContent : '', by: s ? s.querySelector('.relSealBy').textContent : '', at: s ? +s.dataset.relSeal : 0,
        whole: !!(sb && sb.width > 0 && sb.left >= nb.left - 1 && sb.right <= nb.right + 1 && words.every(c => { const rg = document.createRange(); rg.selectNodeContents(c); const b = rg.getBoundingClientRect(); return b.width > 0 && b.right <= nb.right + 1 && b.right <= sb.right + 1; })   /* (the text itself, not its box: a nowrap word stretches its box over the seal's width and runs out of it) */),
        row: !!(s && s.closest('.ordSealsRow')), inActions: !!(s && s.closest('.rowActions')), cell: !!(s && s.closest('.egPlacementSeals')), strip: !!strip, stripIn: !!(strip && s && s.closest('.ordSealsRow') && s.closest('.ordSealsRow').contains(strip)),
        beside: !!(tb && sb && tb.left >= sb.right - 1 && tb.top < sb.bottom && tb.bottom > sb.top), cls: n.className.slice(0, 60)
      };
      if (!out[rid] || (o.seal && !out[rid].seal)) out[rid] = o;
    }
    return out;
  }, scope);
  const ORD = '#ordBody [data-rid]', ENG = '#engraveView .placementRow[data-rid]';
  const orders = async (pg, view) => { await pg.evaluate(v => { CN.setMode('orders'); Orders.showPile('', ''); const b = document.querySelector(`#ordViewSeg [data-view="${v}"]`); if (b) b.click(); Orders.render(); }, view); await pg.waitForTimeout(900); return probe(pg, ORD); };
  const engrave = async pg => { await pg.evaluate(() => { CN.setMode('engrave'); try { Engrave.restoreView({ tab: 'place', list: true, chosen: true }); Engrave.render(); } catch (e) { console.warn(e); } }); await pg.waitForTimeout(1500); return probe(pg, ENG); };
  const seeSeal = (s, who) => !!s && s.seal && s.w >= 46 && s.family === 'release' && /RELEASE/.test(s.text) && /HOLD/.test(s.text) && /Release Hold/.test(s.all) && s.by === 'by ' + who && /\b20\d\d\b/.test(s.all) && /\d{1,2}:\d{2}\s?[AP]M/.test(s.all) && s.whole;
  const WHO = 'Test Operator';
  try {
    await page.evaluate(() => { try { S.settings.pollOrders = 'off'; saveSettings(); } catch (_) {} });
    Alpha.seed(st, await Alpha.make());
    await page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); });
    const made = await R.build(w, NAMES);
    check(made.back.jobs.length === 1 && made.g2.jobs.length === 1 && made.both.jobs.length === 2 && made.front.jobs.length === 1 && made.earrings.jobs.length === 0, `0 · the real intake made the jobs: back 1, two-line order 1, front + back 2, front 1, earrings 0 (${NAMES.map(n => n + ':' + made[n].jobs.length)})`);
    // the approvals of two orders (as order-card-seals.cjs stages them): the strip is on those cards and rows, the release seal must stand beside it
    await page.evaluate(({ back, g2 }) => {
      const T = Date.now(), rowOf = rid => Orders.rows().filter(r => r.order.receiptId === rid), approve = (job, by, at) => { job.state = 'approved'; job.approvedBy = by; job.approvedAt = at; CNEngravingSeals.add(job, 'engraveApproved', by, at); job.row.engrave = Object.assign(job.row.engrave || {}, { needed: true, state: 'approved', approved: true }); };
      for (const j of Engrave.jobsOf(rowOf(back)[0])) approve(j, 'Ana Test', T - 3600e3);
      approve(Engrave.jobsOf(rowOf(g2)[0])[0], 'Hal Test', T - 700e3);
    }, { back: RID('back'), g2: RID('g2') });
    // ── nothing is released yet: no seal anywhere ──
    const none0 = { cards: await orders(page, 'cards'), list: await orders(page, 'list'), eng: await engrave(page) };
    check(Object.values(none0.cards).length >= 6 && Object.values(none0).every(m => Object.values(m).every(o => !o.seal)), '0 · before any release: no Release Hold seal on any card, row or Engraving row');
    check(Object.values(none0.cards).every(o => !o.strip), '0 · ORDLIST: no approval strip on any card (each approval seal is in its own seals cell)');

    // ── the press, on the Review card's Release Hold button ──
    await page.evaluate(() => { window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); }; });
    for (const n of [...BY_CARD, LIVE_ENG, LIVE_ORD]) { const r = await page.evaluate(async rid => CharmNestReviewMark.mark(rid, { by: 'Test Operator' }), RID(n)); if (!(r && r.ok)) throw new Error('mark ' + n + ' ' + JSON.stringify(r)); }
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    for (const n of BY_CARD) {
      const sel = `#rvList .reviewListRow[data-rid="${RID(n)}"] [data-rel-hold]`;
      await until(() => page.$(sel), 30000, 'Release Hold on the Review row of ' + n);
      await page.click(sel);
      await until(() => page.evaluate(rid => !CharmNestReviewMark.isMarked(rid), RID(n)), 60000, 'the release of ' + n);
      await until(() => page.evaluate(k => Orders.rows().some(r => r.order.receiptId === k && r.releaseHold), RID(n)), 20000, 'its press kept on its lines: ' + n);
    }
    await page.waitForTimeout(1200);
    check(!(await page.evaluate(() => window.__toasts.filter(t => t[0] === 'bad'))).length, '1 · three orders pressed Release Hold on their Review cards, no failure toast');

    // ── 2 · Orders, cards ──
    const cards = await orders(page, 'cards');
    for (const n of BY_CARD) check(seeSeal(cards[RID(n)], WHO) && (cards[RID(n)].row || cards[RID(n)].cell) && !cards[RID(n)].inActions, `2 · Orders cards: ${n}: the large seal with "Release Hold", ${WHO}, the date and the time, whole inside the card, in its seals cell (or the order's head) (${JSON.stringify(cards[RID(n)])})`);
    check(/\bordGroup\b/.test(cards[RID('g2')].cls) && cards[RID('g2')].row && !cards[RID('g2')].strip, `2 · the two-line order is one card: its release seal in the order's head, no approval strip (each approval seal is on its own piece) (${JSON.stringify([cards[RID('g2')].cls, cards[RID('g2')].row, cards[RID('g2')].strip])})`);
    check(cards[RID('back')].cell && !cards[RID('back')].strip, '2 · a one-line card: the release seal stands in the seals cell with the approval seals, and no strip is drawn');
    check(!cards[RID(NEVER)].seal && !cards[RID(LIVE_ENG)].seal && !cards[RID(LIVE_ORD)].seal, '2 · orders never released (and the two marked but not pressed yet): no seal');
    await shot('cards');

    // ── 3 · Orders, list rows ──
    const list = await orders(page, 'list');
    for (const n of BY_CARD) check(seeSeal(list[RID(n)], WHO) && (list[RID(n)].row || list[RID(n)].cell) && !list[RID(n)].inActions, `3 · Orders list: ${n}: the same seal, whole inside the row (its date and time are not cut at the column's edge) (${JSON.stringify(list[RID(n)])})`);
    check(!list[RID(NEVER)].seal && !list[RID(LIVE_ENG)].seal, '3 · Orders list: an order never released has none');
    await shot('list');

    // ── 4 · Engraving tab ──
    const eng = await engrave(page);
    for (const n of BY_CARD) check(seeSeal(eng[RID(n)], WHO) && eng[RID(n)].cell && !eng[RID(n)].inActions, `4 · Engraving tab: ${n}: the same large seal in the row's seals cell, whole inside the row (${JSON.stringify(eng[RID(n)])})`);
    check(!!eng[RID('m1')] && /eg-mono-row/.test(await page.evaluate(rid => (document.querySelector(`#engraveView .placementRow[data-rid="${rid}"]`) || {}).dataset ? document.querySelector(`#engraveView .placementRow[data-rid="${rid}"]`).dataset.testid : '', RID('m1'))), '4 · the monogram order\'s row is the monogram row (monoPlacementRow)');
    check(!eng[RID(NEVER)].seal && !eng[RID(LIVE_ENG)].seal, '4 · Engraving tab: engraved orders never released (or not pressed yet) have none');
    await shot('engrave');

    // ── 5 · live: the press while the Engraving tab is in front ──
    await page.evaluate(() => { window.__net = []; const a = window.api; if (typeof a === 'function' && !a.__spy) { const s = function (...x) { try { window.__net.push(String(x[0]) + ' ' + JSON.stringify(x[1] || {}).slice(0, 120)); } catch (_) {} return a.apply(this, x); }; s.__spy = true; try { window.api = s; } catch (_) {} } });
    await page.evaluate(async rid => { Review.syncOrderItems(); const it = Review.items().find(i => i.kind === 'reviewMark' && i.rid === rid); if (!it) throw new Error('no Review item for ' + rid); window.__press = ReviewRelease.press(it); }, RID(LIVE_ENG));
    const live = await until(async () => { const m = await probe(page, ENG); return seeSeal(m[RID(LIVE_ENG)], WHO) && m; }, 30000, 'the seal on the Engraving row, live').catch(() => null);
    check(!!live, '5 · the order pressed while the Engraving tab is in front shows its seal on its two-face row at once (no tab change)');
    await shot('engrave-live');
    await page.evaluate(() => window.__press);
    const o2 = await orders(page, 'cards');
    check(!o2[RID(LIVE_ENG)] || o2[RID(LIVE_ENG)].seal, '5 · and on its Orders card when that tab is next shown');
    await shot('orders-after-live');
    // the press while the Orders tab is in front: a card of an order that is never engraved
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await page.waitForTimeout(500);
    await page.evaluate(async rid => { Review.syncOrderItems(); const it = Review.items().find(i => i.kind === 'reviewMark' && i.rid === rid); if (!it) throw new Error('no Review item for ' + rid); window.__press = ReviewRelease.press(it); }, RID(LIVE_ORD));
    const liveO = await until(async () => { const m = await probe(page, ORD); return seeSeal(m[RID(LIVE_ORD)], WHO) && m; }, 30000, 'the seal on the Orders card, live').catch(() => null);
    check(!!liveO, '5 · the order pressed while the Orders tab is in front shows its seal on its card at once');
    await page.evaluate(() => window.__press); await page.waitForTimeout(800);
    // a seal that is drawn is KEPT (the card and the row are not drawn again for another render), and drawing them asks nothing of the cloud
    const kept = await page.evaluate(async ({ a, b }) => {
      const card = document.querySelector(`#ordBody [data-rid="${a}"] .relSeal`); if (card) card.__kept = 1;
      window.__net.length = 0; Orders.render(); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 400)));
      const afterOrders = document.querySelector(`#ordBody [data-rid="${a}"] .relSeal`);
      CN.setMode('engrave'); Engrave.render(); await new Promise(r => setTimeout(r, 500));
      const rowSeal = document.querySelector(`#engraveView .placementRow[data-rid="${b}"] .relSeal`); if (rowSeal) rowSeal.__kept = 1;
      Engrave.render(); await new Promise(r => setTimeout(r, 400));
      const afterEng = document.querySelector(`#engraveView .placementRow[data-rid="${b}"] .relSeal`);
      return { cardKept: !!(afterOrders && afterOrders.__kept), rowKept: !!(afterEng && afterEng.__kept), spy: !!(window.api && window.api.__spy), net: window.__net.filter(x => /timeline|reviewMark|release|Engrave|orders/i.test(x)) };
    }, { a: RID('back'), b: RID('back') });
    check(kept.cardKept && kept.rowKept, `5 · a drawn seal is kept: the Orders card and the Engraving row are not drawn again for another render (${JSON.stringify(kept)})`);
    check(kept.spy && kept.net.length === 0, `5 · drawing the seals asks nothing of the network (the page's api() watched: ${JSON.stringify(kept)})`);

    // ── 6 · a reload ──
    await page.waitForTimeout(2500);
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.waitForFunction(() => !window.Session || !Session.ready || Session.ready(), null, { timeout: 30000 }).catch(() => {});
    await page.evaluate(async () => { try { await Master.load(true); await Orders.loadMaps(true); } catch (e) { console.warn(e); } });
    await until(() => page.evaluate(n => Orders.rows().length >= n && [...Engrave.items().values()].length >= 4, 7), 60000, 'the workspace restored after the reload');
    const released = [...BY_CARD, LIVE_ENG, LIVE_ORD];
    const o6 = { cards: await orders(page, 'cards'), list: await orders(page, 'list') }, e6 = await engrave(page);
    for (const n of released) check(seeSeal(o6.cards[RID(n)], WHO) && seeSeal(o6.list[RID(n)], WHO), `6 · after a reload: ${n}: its Orders card and list row show the seal, who and when (rebuilt from the lines' record and the cloud, no press on this page)`);
    for (const n of [...BY_CARD, LIVE_ENG]) check(seeSeal(e6[RID(n)], WHO), `6 · after a reload: ${n}: its Engraving row shows the seal`);
    check(!o6.cards[RID(NEVER)].seal && !o6.list[RID(NEVER)].seal && !e6[RID(NEVER)].seal, '6 · after a reload: the order never released still has none, on all three surfaces');
    await shot('reload-engrave');
    // the lines' own record of the press taken away (this page never saw the press: the order was released on another computer): the cloud's Review document gives the seal
    const taken = await page.evaluate(() => { let n = 0; for (const r of Orders.rows()) if (r.releaseHold) { delete r.releaseHold; n++; } Orders.render(); Engrave.render(); return n; });
    check(taken >= 5, `6 · the lines' record of the press taken away (${taken} lines)`);
    await page.waitForTimeout(600);
    const o7 = { cards: await orders(page, 'cards'), list: await orders(page, 'list') }, e7 = await engrave(page);
    for (const n of released) check(seeSeal(o7.cards[RID(n)], WHO) && seeSeal(o7.list[RID(n)], WHO), `6 · released elsewhere: ${n}: the Orders card and the list row still show the seal (the cloud's Review document)`);
    for (const n of [...BY_CARD, LIVE_ENG]) check(seeSeal(e7[RID(n)], WHO), `6 · released elsewhere: ${n}: the Engraving row still shows it`);
    // another computer: a second page over the same cloud, the orders pulled afresh (no line record at all)
    const p2 = await context.newPage(); p2.setDefaultTimeout(30000);
    await p2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await p2.waitForFunction(() => window.CN && window.Orders && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await p2.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); });
    await p2.waitForFunction(() => !window.Session || !Session.ready || Session.ready(), null, { timeout: 30000 }).catch(() => {});
    await p2.evaluate(({ fixtures, orderSrc }) => {
      B.orders.rows = []; B.orders.byKey = new Map();
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: NAMES.map(n => R.ORDERS[n]), orderSrc: require('./_orderFactsWorld.cjs').orderOf.toString() });
    check(await p2.evaluate(() => Orders.rows().length >= 7 && Orders.rows().every(r => !r.releaseHold)), '6 · the other computer holds the orders pulled afresh, no line knows of any press');
    const c8 = await orders(p2, 'cards'), l8 = await orders(p2, 'list');
    for (const n of released) check(seeSeal(c8[RID(n)], WHO) && seeSeal(l8[RID(n)], WHO), `6 · another computer: ${n}: the card and the row show the seal, from the cloud`);
    check(!c8[RID(NEVER)].seal && !l8[RID(NEVER)].seal, '6 · another computer: the order never released has none');
    await shot('other-computer', p2);
    await p2.close();
    // ── 7 · a phone: the seal wraps its words, is whole inside every card and row, and the lists do not scroll sideways ──
    await page.setViewportSize({ width: 420, height: 900 }); await page.waitForTimeout(500);
    if (await page.evaluate(() => !document.getElementById('app').classList.contains('railOff'))) { await page.click('#btnRail').catch(() => {}); await page.waitForTimeout(500); }   // (the rail folded, as a person would on a phone)
    await page.evaluate(() => document.querySelectorAll('.cnNbX, .toast .x').forEach(b => { try { b.click(); } catch (_) {} })); await page.waitForTimeout(400);
    const narrow = { cards: await orders(page, 'cards'), list: await orders(page, 'list'), eng: await engrave(page) };
    for (const [k, m] of Object.entries(narrow)) check(released.filter(n => k !== 'eng' || n !== LIVE_ORD).every(n => seeSeal(m[RID(n)], WHO)), `7 · at 420 px (${k}): every released order's seal is whole inside its ${k === 'eng' ? 'row' : 'card'}, words and all (${released.filter(n => k !== 'eng' || n !== LIVE_ORD).filter(n => !seeSeal(m[RID(n)], WHO)).join(',')} not)`);
    const sideways = await page.evaluate(() => ({ page: document.documentElement.scrollWidth - innerWidth, eng: (document.getElementById('engraveView') || {}).scrollWidth - (document.getElementById('engraveView') || {}).clientWidth }));
    check(sideways.page <= 1 && sideways.eng <= 1, `7 · at 420 px nothing scrolls sideways (${JSON.stringify(sideways)})`);
    await shot('narrow-engrave');
    await orders(page, 'cards'); await shot('narrow-cards');
    const errs = w.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
    console.log(saw.slice(-20).join('\n'));
  } finally { await w.close(); }
  finish();
})();
function finish() {
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nrelease-seal-cards: ${passed} checks passed`);
  process.exit(fails.length ? 1 : 0);
}
