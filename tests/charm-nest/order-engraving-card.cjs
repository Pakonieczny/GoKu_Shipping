// The order window's Overview carries the piece's back engraving card, under its pictures (Paul, 5 Oct 2026, round 3 point 2:
// "incorporate the full back engraving view as you can see in the attached image #3 into the detail order modal underneath the
// existing thumbnail image ... the Approved button/Seal and also the shortcut to the back engraving tab modal").
// charm-nest-order-engraving.js (window.OrderEngraving) draws the very card the Sheet tab draws (CNEngravingSeals.panel and
// wirePanel). Headless Chromium against the local fake site (bridge-server.cjs), offline; Engrave.approve is a stand-in that
// presses the real seal through CNEngravingSeals.press, as the real one does, so the page's own Engrave is not run on invented jobs.
//   SHOTS=<dir> node tests/charm-nest/order-engraving-card.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const R1 = '4170837249', R2 = '4175550002', R3 = '4175550003', SHEET = 'sheet-oe-1';
const ln = (rid, n, sku, metal, label) => ({ transactionId: rid + n, listingId: '19037' + n + '9935', sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: label }], metalKey: metal, metalLabel: label, personalization: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  order(R1, 'Leslie Suhr', [ln(R1, '1', 'CABLE_CHAIN_ONLY', 'rose', 'RG 14/20'), ln(R1, '2', 'MIDDLE_9935', 'gold', 'GF 14/20'), ln(R1, '3', 'MIDDLE_9935', 'rose', 'RG 14/20')]),
  order(R2, 'Ava Patel', [ln(R2, '1', 'ASTER_FLOWER', 'gold', 'GF 14/20'), ln(R2, '2', 'LEAF_CHARM', 'gold', 'GF 14/20')]),
  order(R3, 'Hannah Whitford', [ln(R3, '1', 'PLAIN_TAG', 'gold', 'GF 14/20')])];
const key = (rid, n) => `${rid}_${rid}${n}`, pool = (rid, n) => key(rid, n) + '_1';
const WORDS = { [key(R1, 2)]: 'I\ndissent', [key(R1, 3)]: 'KMB //\nSMH', [key(R2, 1)]: 'Aster', [key(R2, 2)]: 'Leaf' };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  // the piece of order 1 that is engraved (RG MIDDLE) sits on a saved sheet, so the Sheet tab has something to draw
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  srv.st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'rose', sheetIndex: 1, day: '2026-10-03', status: 'written', density: 0.4, stock: { wPt: 283.46, hPt: 141.73 }, orders: [R1],
    placements: [box('pa', 60, 50)], charms: [{ id: 'pa', name: `${R1} · MIDDLE_9935`, poolId: pool(R1, 3), order: R1, sku: 'MIDDLE_9935' }], backPool: [] });
  srv.st.put('Charm_Pool', pool(R1, 3), { poolId: pool(R1, 3), orderId: R1, transactionId: R1 + '3', lineKey: key(R1, 3), sku: 'MIDDLE_9935', material: 'rose', copy: 1, quantity: 1, state: 'placed', sheetId: SHEET, sheetName: 'RG_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the orders, their jobs (as Engrave holds them), and a stand-in for the approval itself ──
    await page.evaluate(async ({ orders, WORDS, k, p, R1, R2, SHEET }) => {
      const T0 = Date.now() - 6 * 3600e3;
      for (const o of orders) for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const art = text => { const c = document.createElement('canvas'); c.width = c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; text.split('\n').forEach((t, i) => x.fillText(t, 150, 130 + i * 48)); return c; };
      Engrave.renderBack = (job) => art(job.text || '');                      // (the placement picture: fitting is Engrave's own, not this test's)
      const mk = (key, state, text, extra) => { const row = B.orders.byKey.get(key), j = Engrave.ensureJob(row); Object.assign(j, { state, text, lines: text.split('\n') }, extra || {}); row.engrave = { needed: true, state, approved: false, text }; return j; };
      const fit = () => ({ size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' });
      // order 1 (three pieces): the chain has no engraving, the GF middle was approved by Giovanna, the RG middle waits for approval
      const at = Date.now() - 2 * 3600e3;
      const j2 = mk(k[1], 'approved', WORDS[k[1]], { approvedBy: 'Giovanna', approvedAt: at, backs: [{ poolId: p[1], approvedAt: at, approvedBy: 'Giovanna', png: art(WORDS[k[1]]).toDataURL() }] });
      Object.assign(B.orders.byKey.get(k[1]).engrave, { approved: true, approvedBy: 'Giovanna', approvedAt: at }); CNEngravingSeals.add(j2, 'engraveApproved', 'Giovanna', at); CNEngravingSeals.keep(j2);
      mk(k[2], 'review', WORDS[k[2]], { fit: fit(), view: {}, verify: { geometry: { ok: true } } });
      // order 2 (two pieces): both wait for approval
      mk(k[3], 'review', WORDS[k[3]], { fit: fit(), view: {}, verify: { geometry: { ok: true } } });
      mk(k[4], 'review', WORDS[k[4]], { fit: fit(), view: {}, verify: { geometry: { ok: true } } });
      // the approval, as Engrave does it: the seal is pressed on the button it was given, then the state is settled and the timeline told
      window.__approve = []; window.__poke = 0; window.__nudge = 0; window.__links = [];
      Engrave.approve = async (job, by, button) => {
        __approve.push({ key: job.key, by, button: !!button && button.isConnected });
        const t = Date.now(), seal = CNEngravingSeals.add(job, 'engraveApproved', by, t);
        job.stamping = true; try { await CNEngravingSeals.press(button, seal); } finally { job.stamping = false; }
        Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: t }); Object.assign(job.row.engrave, { state: 'approved', approved: true, approvedBy: by, approvedAt: t });
        try { Engrave.timelineApproved(job); } catch (_) {}
      };
      const poke = RunCtl.poke; RunCtl.poke = function () { __poke++; return poke.apply(this, arguments); };
      const nudge = OrderWin.nudge; OrderWin.nudge = function () { __nudge++; return nudge.apply(this, arguments); };
      Engrave.render = () => { window.__engraveRendered = (window.__engraveRendered || 0) + 1; };   // (the Engraving tab itself is not under test)
      // the order's timeline, answered here, so what another computer wrote can be handed to the feed
      window.__evs = {}; const wait = window.setTimeout.bind(window);
      OrderTimeline.get = async rid => { await new Promise(r => wait(r, 15)); return JSON.parse(JSON.stringify({ id: rid, events: window.__evs[rid] || [{ id: rid + '~arrived', orderId: rid, type: 'arrived', at: T0, by: 'Etsy', source: 'etsy' }], cancelled: null, where: null })); };
      CN.setMode('orders'); Orders.render();
    }, { orders: ORDERS, WORDS, k: { 1: key(R1, 2), 2: key(R1, 3), 3: key(R2, 1), 4: key(R2, 2) }, p: { 1: pool(R1, 2) }, R1, R2, SHEET });
    const calls = () => page.evaluate(() => ({ approve: __approve.map(a => a.key + '|' + a.by + '|' + a.button), poke: __poke, nudge: __nudge, links: __links }));
    const card = () => page.evaluate(() => {
      const h = document.getElementById('owEng'), q = s => h.querySelector(s), e = q('.swEng');
      const seals = [...h.querySelectorAll('.egButtonSeal .seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '|' + m.by; } catch (_) { return '?'; } });
      const r = h.getBoundingClientRect(), sku = document.getElementById('owSku').getBoundingClientRect(), vec = document.getElementById('owVector').getBoundingClientRect(), ph = document.getElementById('owPhoto').getBoundingClientRect();
      return { hidden: h.hidden, state: e && e.dataset.state, title: q('.egWhy b')?.textContent, chip: q('.egPill')?.textContent, words: q('.words')?.textContent, label: q('.fLabel')?.textContent, approve: !!q('[data-e=approve]'), disabled: q('.egApproveButton')?.disabled,
        approveText: q('.egApproveButton')?.textContent, open: q('[data-e=engrave]')?.textContent.trim(), preview: !!q('.pv canvas, .pv img'), seals, wait: q('.owEngWait')?.textContent, box: { top: Math.round(r.top), left: Math.round(r.left), w: Math.round(r.width), h: Math.round(r.height) },
        under: r.top >= sku.bottom - 1 && r.top >= vec.bottom - 1 && r.top >= ph.bottom - 1 && Math.abs(r.left - ph.left) < 2, settledText: /still to be settled/i.test(document.getElementById('orderWin').textContent) };
    });
    // (an order of several opens on "All pieces", which draws a compact card for each piece; these checks are about one piece's card, so the piece is picked: "All pieces" has its own check below)
    const open = async (k, extra) => { await page.evaluate(k => OrderWin.open(k), k); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !document.querySelector('#orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), k, { timeout: 15000 });
      await page.evaluate(k => { if (OrderWin.selectedPiece() !== k) OrderWin.selectPiece(k); }, k);
      await page.waitForFunction(k => OrderWin.key() === k && !document.querySelector('#owEng .egCompact'), k, { timeout: 8000 }).catch(() => {}); };
    const settled = () => page.waitForFunction(() => !document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 });
    const pick = async n => { await page.evaluate(i => { const rows = [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')]; OrderWin.selectPiece(i === 1 ? null : rows[i - 2].dataset.piece); }, n); };   // (1: all pieces, then the open order's pieces in the list's order: the Its pieces list's own switch; the order shown, not always the first)
    const k = { c: key(R1, 1), g: key(R1, 2), r: key(R1, 3) }; await page.evaluate(k => { window.__k = k; }, k);

    // 1 · the Overview of the RG middle (piece 3 of 3): the card sits under the pictures and the SKU, and is image 3
    await open(k.r);
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approve] .pv canvas'), null, { timeout: 10000 });
    await settled(); await page.mouse.move(700, 880);
    let c = await card();
    check(!c.hidden && c.state === 'approve' && c.label === 'Back engraving' && c.title === 'Waiting for your approval' && c.chip === 'To approve', 'the Overview shows the piece\'s back engraving card: ' + JSON.stringify([c.state, c.title, c.chip]));
    check(c.words === 'KMB //\nSMH' && c.preview && c.approve && c.approveText === 'Approve engraving' && c.open === 'Fix in Engraving →', 'its words, the placement picture, the Approved button and the shortcut: ' + JSON.stringify([c.words, c.preview, c.approveText, c.open]));
    check(c.under, 'it sits directly under the Etsy listing, the vector design and the SKU line, in the same column: ' + JSON.stringify(c.box));
    check(!c.settledText, 'the words "still to be settled" appear nowhere in the order window while the engraving is not settled (the red box is gone)');
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-approve.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approve.png') }); }

    // 2 · the pieces: each has its own back and approval; switching swaps the card, nothing of the other piece shows
    await page.evaluate(() => { const h = document.getElementById('owEng'); window.__log = []; new MutationObserver(() => __log.push({ key: OrderWin.key(), compact: !!h.querySelector('.egCompact'), hidden: h.hidden, state: h.querySelector('.swEng')?.dataset.state || '', words: h.querySelector('.words')?.textContent || '', wait: !!h.querySelector('.owEngWait') })).observe(h, { childList: true, subtree: true, attributes: true, attributeFilter: ['hidden'] }); });
    await pick(2); await page.waitForFunction(k => OrderWin.key() === k && document.getElementById('owEng').hidden, k.c, { timeout: 8000 });
    c = await card(); check(c.hidden && !c.state, 'the chain (piece 1) has no back engraving: no card, the host is hidden');
    await pick(3); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approved]'), k.g, { timeout: 8000 });
    c = await card();
    check(c.state === 'approved' && c.words === 'I\ndissent' && c.disabled === true && !c.approve && c.open === 'View in Engraving →' && c.chip === 'Approved' && c.title === 'The back is approved', 'the GF middle (piece 2) shows its own approved card: ' + JSON.stringify([c.state, c.words, c.chip, c.open]));
    check(c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna', 'with Giovanna\'s BACK ENGRAVING seal on the button: ' + JSON.stringify(c.seals));
    if (shots) await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approved.png') });
    await pick(4); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approve]'), k.r, { timeout: 8000 });
    c = await card(); check(c.state === 'approve' && c.words === 'KMB //\nSMH' && c.seals.length === 0, 'the RG middle (piece 3) is its own: to approve, no seal');
    await pick(1);   // All 3 pieces: a compact card for each piece that has a back engraving (the chain has none)
    await page.waitForFunction(() => document.querySelectorAll('#owEng .swEng.egCompact').length === 2, null, { timeout: 8000 });
    const all2 = await page.evaluate(() => [...document.querySelectorAll('#owEng .swEng')].map(e => e.dataset.state + ':' + (e.querySelector('.words') || {}).textContent));
    check(all2.join('|') === 'approve:KMB //\nSMH|approved:I\ndissent', '"All pieces": a compact card for each piece with a back engraving, none for the chain, the piece the window holds first: ' + JSON.stringify(all2));
    await pick(4); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approve]:not(.egCompact)'), k.r, { timeout: 8000 });
    const log = await page.evaluate(() => __log);
    const bad = log.filter(l => !l.compact && l.words && l.words !== (WORDS[l.key] || '#none#'));   // ('All pieces' draws every piece's own card, each with its own words: that is the compact list)
    check(log.length > 0 && bad.length === 0 && !log.some(l => l.key === k.c && (!l.hidden && l.state)), 'at no moment did a piece show another piece\'s words or a card on the chain (' + log.length + ' changes seen)');

    // 3 · approve: the name is asked as everywhere (me() || askEmployee()), Engrave.approve gets the very button, the seal lands on it
    await page.evaluate(() => { window.__prompts = 0; window.prompt = () => { __prompts++; return null; }; B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
    const nameless = await page.evaluate(() => (window.CNEmployee.name() || '') === '');
    if (nameless) {
      await page.click('#owEng [data-e=approve]'); await page.waitForTimeout(250);
      check((await calls()).approve.length === 0 && (await page.evaluate(() => __prompts)) === 1 && (await card()).approve, 'with no name, the name is asked for and a refusal approves nothing (the button stays)');
      await page.evaluate(() => { window.prompt = () => 'Zed Tester'; });
    } else console.log('  – the name could not be cleared here: the ask-for-a-name path was not run');
    await page.click('#owEng [data-e=approve]');
    await page.waitForFunction(() => __approve.length === 1, null, { timeout: 8000 });
    const during = await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); return { disabled: b.disabled, busy: b.getAttribute('aria-busy'), pending: !!document.querySelector('#owEng .seal.pending, #owEng .egButtonSeal .seal') }; });
    check(during.disabled && during.busy === 'true' && during.pending, 'pressed: the button waits (disabled, busy) while the seal is pressed on it: ' + JSON.stringify(during));
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]') && !document.querySelector('#owEng .seal.pending'), null, { timeout: 20000 });
    await settled();
    c = await card(); const who = nameless ? 'Zed Tester' : 'Test Operator';
    check((await calls()).approve.length === 1 && (await calls()).approve[0] === `${k.r}|${who}|true`, 'Engrave.approve was called once, for this piece\'s job, with the name and the button on screen: ' + JSON.stringify((await calls()).approve));
    check(c.state === 'approved' && c.disabled === true && !c.approve && c.seals.length === 1 && c.seals[0] === `BACK ENGRAVING|${who}`, 'the card is approved, the button settled and the BACK ENGRAVING seal on it, once: ' + JSON.stringify([c.state, c.seals]));
    check(!c.settledText, 'and still nowhere once it is settled');
    const after = await calls(); check(after.poke >= 1 && after.nudge >= 1, `the run and the order's timeline feed were told (RunCtl.poke ${after.poke}, OrderWin.nudge ${after.nudge})`);
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-approved.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approved-by-me.png') }); }
    // permanent, never duplicated: away and back, and the window closed and opened again
    await pick(3); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approved]'), k.g, { timeout: 8000 });
    await pick(4); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approved] .egButtonSeal .seal'), k.r, { timeout: 8000 });
    await page.waitForTimeout(1300);
    c = await card(); check(c.seals.length === 1 && c.seals[0] === `BACK ENGRAVING|${who}`, 'switching away and back: the seal is there, once');
    await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); b.click(); });
    await page.waitForTimeout(300);
    check((await calls()).approve.length === 1 && (await card()).seals.length === 1, 'pressing it again approves nothing and adds no seal');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen() && !document.getElementById('orderWin').open, null, { timeout: 4000 });
    check(await page.evaluate(() => !document.getElementById('owEng')._orderEngraving && !document.getElementById('owEng').firstChild), 'the window closed: the card and its timer are gone');
    await open(k.r); await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved] .egButtonSeal .seal'), null, { timeout: 10000 }); await page.waitForTimeout(1200);
    c = await card(); check(c.seals.length === 1 && c.words === 'KMB //\nSMH', 'opened again: still approved, one seal');

    // 4 · the Sheet tab shows the approval made here, with its own card (kept as it is), and the timeline got it
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] .swEng[data-state=approved]'), null, { timeout: 15000 });
    const sheetSeals = await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel [data-engraving-panel] .egButtonSeal .seal svg[data-seal-model]')].map(s => JSON.parse(s.getAttribute('data-seal-model')).action));
    check(sheetSeals.length === 1 && sheetSeals[0] === 'BACK ENGRAVING', 'the Sheet tab shows the same approval, its own card as before: ' + JSON.stringify(sheetSeals));
    await page.click('.owTabsV [data-ow-view="info"]'); await settled();
    const told = await (async () => { for (let i = 0; i < 40; i++) { if (srv.st.calls.some(c => JSON.stringify(c.body || {}).includes('engraveApproved'))) return true; await page.waitForTimeout(250); } return false; })();
    check(told, 'the order\'s timeline was told (an engraveApproved step went to the record)');

    // 5 · the shortcut: EngraveLink when it is there, with the order, line, pool id and piece; else the Sheet tab's own way
    // (the real EngraveLink, when this page has it: the order window closes and the Engraving tab is set to this order's approved piece)
    if (await page.evaluate(() => !!(window.EngraveLink && EngraveLink.open))) {
      await page.click('#owEng [data-e=engrave]');
      await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
      const rv = await page.evaluate(() => Engrave.view());
      check(rv.tab === 'done' && rv.chosen === true && rv.list === false, 'with the real EngraveLink: the window closed and the Engraving tab is on the approved piece: ' + JSON.stringify({ tab: rv.tab, q: rv.q, chosen: rv.chosen, list: rv.list }));
      await page.waitForTimeout(600); await page.evaluate(() => CN.setMode('orders')); await open(k.r);
      await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved] .egButtonSeal .seal'), null, { timeout: 10000 });
    }
    await page.evaluate(() => { window.EngraveLink = { open: async t => { __links.push(t); await new Promise(r => setTimeout(r, 1500)); } }; });
    await page.click('#owEng [data-e=engrave]');
    const mid = await page.evaluate(() => { const b = document.querySelector('#owEng [data-e=engrave]'); return { disabled: b.disabled, text: b.textContent.trim() }; });
    check(mid.disabled && /Opening Engraving/.test(mid.text), 'while it opens the button says so: ' + JSON.stringify(mid));
    await page.waitForFunction(() => __links.length === 1, null, { timeout: 4000 });
    await page.waitForFunction(() => { const b = document.querySelector('#owEng [data-e=engrave]'); return b && !b.disabled && /View in Engraving/.test(b.textContent); }, null, { timeout: 4000 });
    const links = (await calls()).links;
    check(links.length === 1 && JSON.stringify(links[0]) === JSON.stringify({ rid: R1, key: k.r, poolId: pool(R1, 3), piece: k.r }), 'it called EngraveLink.open({rid, key, poolId, piece}) for this piece: ' + JSON.stringify(links[0]));
    check(await page.evaluate(() => OrderWin.isOpen()), 'and the window was left to EngraveLink (it closes it first)');
    await pick(3); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approved]'), k.g, { timeout: 8000 });
    await page.click('#owEng [data-e=engrave]'); await page.waitForFunction(() => __links.length === 2, null, { timeout: 4000 });
    check(JSON.stringify((await calls()).links[1]) === JSON.stringify({ rid: R1, key: k.g, poolId: pool(R1, 2), piece: k.g }), 'the other piece asks for its own: ' + JSON.stringify((await calls()).links[1]));
    // (until EngraveLink exists: the Sheet tab's way, the window closes and the Engraving tab is set to that order and piece)
    await page.evaluate(() => { delete window.EngraveLink; });
    await page.click('#owEng [data-e=engrave]');
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 8000 });
    const v1 = await page.evaluate(() => Engrave.view());
    check(v1.tab === 'done' && v1.q === R1 && v1.chosen === true && v1.list === false && !v1.focus, 'without EngraveLink: the window closes and the Engraving tab opens on the approved piece\'s order: ' + JSON.stringify({ tab: v1.tab, q: v1.q, chosen: v1.chosen, list: v1.list }));
    await page.evaluate(() => CN.setMode('orders'));

    // 6 · live: an approval made in the Engraving tab / Sheet tab / sheet window (Engrave's own job), and one another computer made
    await open(key(R2, 1)); await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approve] [data-e=approve]'), null, { timeout: 10000 });
    c = await card(); check(c.state === 'approve' && c.words === 'Aster' && !c.settledText, 'order 2, piece 1: to approve, with no "still to be settled" text');
    const t0 = Date.now();
    await page.evaluate(k => { const j = Engrave.items().get(k), at = Date.now(); CNEngravingSeals.add(j, 'engraveApproved', 'Sam Tester', at); Object.assign(j, { state: 'approved', approvedBy: 'Sam Tester', approvedAt: at }); Object.assign(j.row.engrave, { state: 'approved', approved: true, approvedBy: 'Sam Tester', approvedAt: at }); }, key(R2, 1));
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 5000 });
    const took = Date.now() - t0; c = await card();
    check(took <= 3000 && !c.approve && c.disabled === true && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Sam Tester', `approved in the Engraving tab or the sheet window: the card follows within ${took} ms, with the seal`);
    // another computer: only the timeline knows (its feed is read by the open window already)
    await page.evaluate(k => OrderWin.selectPiece(k), key(R2, 2)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approve]'), key(R2, 2), { timeout: 8000 });
    const t1 = Date.now();
    await page.evaluate(({ R2, k }) => { const at = Date.now(); window.__evs[R2] = [{ id: R2 + '~arrived', orderId: R2, type: 'arrived', at: at - 3600e3, by: 'Etsy', source: 'etsy' }, { id: `${k}_1-${at}`, orderId: R2, type: 'engraveApproved', at, by: 'Giovanna', source: 'sorter', station: 'sorter', lineKey: k, transactionId: k.split('_')[1], text: '“Leaf”', data: { text: 'Leaf', poolId: k + '_1', copy: 1 } }]; OrderWin._feed().refresh({ force: true }); }, { R2, k: key(R2, 2) });
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]'), null, { timeout: 6000 });
    const took2 = Date.now() - t1; c = await card();
    check(took2 <= 3500 && !c.approve && c.words === 'Leaf' && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna', `approved on another computer: the card shows it ${took2} ms after the timeline feed read it, nobody can approve it twice here: ` + JSON.stringify([c.state, c.words, c.seals]));
    check((await page.evaluate(k => Engrave.items().get(k).state, key(R2, 2))) === 'review', 'and Engrave\'s own job was not touched by showing it');
    const n0 = (await calls()).approve.length; await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); b && b.click(); }); await page.waitForTimeout(200);
    check((await calls()).approve.length === n0, 'no second approval is possible from the card');

    // 7 · an order with one piece and no back engraving: no card; an order still being read says what it waits for
    await open(key(R3, 1)); await page.waitForTimeout(500);
    c = await card(); check(c.hidden && !c.state && !c.wait, 'an order with no back engraving shows no card');
    const early = await page.evaluate(() => { OrderWin.openOrder('4179990001', { q: '4179990001', row: null }); const h = document.getElementById('owEng'); return { hidden: h.hidden, wait: h.querySelector('.owEngWait')?.textContent || '' }; });
    check(!early.hidden && /Reading the back engraving/.test(early.wait), 'an order still being read: a small spinner with its words, not a stale card: ' + JSON.stringify(early));
    await page.waitForTimeout(800);
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-engraving-card: ok');
})().catch(e => { console.error(e); process.exit(1); });
