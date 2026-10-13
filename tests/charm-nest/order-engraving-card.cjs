// The order window carries one engraving card for each piece (Paul, 12 Oct 2026: "the cards are currently full of needless text ... front and back
// side-by-side in a larger card ... click on either one"): charm-nest-order-engraving.js (window.OrderEngraving) keeps charm-nest-engrave-card.js's
// card alive: Front and Back in ONE card, the words under each, one state dot, ONE Approve button (the seal pressed on it through
// CNEngravingSeals.press), a press on a thumbnail opens that face in Engraving (EngraveLink). The Sheet tab keeps its own card.
// Headless Chromium against the local fake site (bridge-server.cjs), offline; Engrave.approve is a stand-in that
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
    const DEL = ['TO APPROVE', 'To approve', 'Waiting for your approval', 'WORDS ON THE BACK', 'Words on the back', 'Read from', 'sure', 'Fix in Engraving', 'View in Engraving', 'ENGRAVES', 'nothing in the listing', 'The system read', 'Undo'];
    // (a piece's card is found by its line key, whatever else the section shows: the section never follows the piece switcher)
    const card = (lk) => page.evaluate(lk => {
      const h = document.getElementById('owEng'), c = h.querySelector(`.engCard[data-key="${lk || window.__k.r}"]`), q = s => c && c.querySelector(s);
      const seals = [...(c ? c.querySelectorAll('.egButtonSeal .seal svg[data-seal-model]') : [])].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '|' + m.by; } catch (_) { return '?'; } });
      const face = f => { const e = q(`.engFace[data-face=${f}]`); return e ? { empty: e.dataset.empty === '1', text: (e.querySelector('.engTxt') || {}).title || '', label: (e.querySelector('.engLbl') || {}).textContent, picture: !!e.querySelector('.engThumb canvas, .engThumb img, .engThumb .engpv'), laid: ((e.querySelector('.engpvText') || {}).textContent || '') } : null; };
      const ab = q('.egApproveButton');
      return { hidden: h.hidden, n: h.querySelectorAll('.engCard').length, direct: [...h.children].every(x => x.classList.contains('engCard')), state: c && c.dataset.state, dot: q('.engDot') && q('.engDot').getAttribute('aria-label'), dots: c ? c.querySelectorAll('.engDot').length : 0, name: q('.engName') && q('.engName').textContent,
        front: face('front'), back: face('back'), thumbs: c ? c.querySelectorAll('.engThumb').length : 0, approve: !!q('[data-e=approve]'), disabled: ab && ab.disabled, approveText: ab && ab.textContent.trim(), header: q('.engHd') ? [...q('.engHd').children].map(x => x.className.split(' ')[0]).join(',') : '', height: c && Math.round(c.getBoundingClientRect().height), buttons: c ? c.querySelectorAll('button:not(.engThumb)').length : 0, seals,
        wait: h.querySelector('.owEngWait') && h.querySelector('.owEngWait').textContent, text: c ? c.textContent.replace(/\s+/g, ' ').trim() : '', settledText: /still to be settled/i.test(document.getElementById('orderWin').textContent) };
    }, lk);
    const none = c => DEL.filter(w => c.text.includes(w));
    // (an order of several opens on "All pieces"; one piece's card is read by picking the piece)
    const open = async (k, extra) => { await page.evaluate(k => OrderWin.open(k), k); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !document.querySelector('#orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), k, { timeout: 15000 });
      await page.evaluate(k => { if (OrderWin.selectedPiece() !== k) OrderWin.selectPiece(k); }, k); };
    const settled = () => page.waitForFunction(() => !document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 });
    const pick = async n => { await page.evaluate(i => { const rows = [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')]; OrderWin.selectPiece(i === 1 ? null : rows[i - 2].dataset.piece); }, n); };
    const k = { c: key(R1, 1), g: key(R1, 2), r: key(R1, 3) }; await page.evaluate(k => { window.__k = k; }, k);
    const cardOf = lk => `.engCard[data-key="${lk}"]`;
    const thumb = (lk, face) => `${cardOf(lk)} .engFace[data-face=${face}] .engThumb`;

    // 1 · the Overview of the RG middle (piece 3 of 3): ONE card with ONE face (the Back, with its words; ENGSMART: no empty Front slot), one dot, one Approve
    await open(k.r);
    await page.waitForFunction(() => document.querySelector('#owEng .engCard[data-state=approve] .engFace[data-face=back] .engThumb .engpv, #owEng .engCard[data-state=approve] .engFace[data-face=back] .engThumb canvas'), null, { timeout: 10000 });
    await settled(); await page.mouse.move(700, 880);
    let c = await card();
    check(!c.hidden && c.n >= 1 && c.direct && c.state === 'approve' && c.dot === 'To approve' && c.dots === 1, 'the Overview shows the piece\'s card, a direct child of the host, with ONE state dot: ' + JSON.stringify([c.n, c.state, c.dot, c.dots]));
    check(c.header === 'engDot,engName,engAct', 'one header row: the dot, the piece\'s name and the Approve button (the faces below it): ' + c.header);
    check(c.thumbs === 1 && c.front === null && c.back.label === 'Back' && !c.back.empty && c.back.text === 'KMB //\nSMH' && c.back.picture, 'only the Back is drawn (no Front slot at all), with its words and its placement picture: ' + JSON.stringify([c.front, c.back]));
    check(c.approve && c.approveText === 'Approve' && c.buttons === 1 && none(c).length === 0, 'ONE Approve button and none of the old words: ' + JSON.stringify([c.approveText, c.buttons, none(c)]));
    check(!c.settledText, 'the words "still to be settled" appear nowhere in the order window while the engraving is not settled');
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-approve.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approve.png') }); }

    // 2 · every engraved piece has its own card, whichever piece the window holds (the section never follows the piece switcher); nothing of one piece shows on another's card
    await page.evaluate(() => { const h = document.getElementById('owEng'); window.__log = []; new MutationObserver(() => __log.push({ key: OrderWin.key(), hidden: h.hidden, cards: [...h.querySelectorAll('.engCard')].map(e => e.dataset.key + ':' + e.dataset.state + ':' + ((e.querySelector('.engFace[data-face=back] .engTxt') || {}).title || '')), wait: !!h.querySelector('.owEngWait') })).observe(h, { childList: true, subtree: true, attributes: true, attributeFilter: ['hidden'] }); });
    await pick(1);   // All 3 pieces
    await page.waitForFunction(() => document.querySelectorAll('#owEng .engCard').length === 2, null, { timeout: 8000 });
    const all2 = await page.evaluate(() => [...document.querySelectorAll('#owEng .engCard')].map(e => e.dataset.key + ':' + e.dataset.state + ':' + e.querySelector('.engFace[data-face=back] .engTxt').title).sort());
    check(all2.join('|') === [`${k.g}:approved:I\ndissent`, `${k.r}:approve:KMB //\nSMH`].sort().join('|'), 'a card for each piece with an engraving (the GF middle, the RG middle), none for the chain: ' + JSON.stringify(all2));
    c = await card(k.g);
    check(c.state === 'approved' && c.dot === 'Approved' && c.back.text === 'I\ndissent' && !c.approve && c.buttons === 0 && none(c).length === 0, 'the GF middle (piece 2) shows its own approved card, with no button: ' + JSON.stringify([c.state, c.back.text, c.buttons]));
    check(c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna' && c.header === 'engDot,engName,engAct', 'with Giovanna\'s BACK ENGRAVING seal, in the header row where the button would be: ' + JSON.stringify([c.seals, c.header]));
    if (shots) await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approved.png') });
    c = await card(k.r); check(c.state === 'approve' && c.back.text === 'KMB //\nSMH' && c.seals.length === 0, 'the RG middle (piece 3) is its own: to approve, no seal');
    check(!(await page.evaluate(k => !!document.querySelector(`#owEng .engCard[data-key="${k}"]`), k.c)), 'the chain (piece 1) has no engraving: no card');
    await pick(4); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approve]`), k.r, { timeout: 8000 });
    const log = await page.evaluate(() => __log);
    const bad = log.filter(l => l.cards.some(x => { const [lk, , ...w] = x.split(':'); return w.join(':') && w.join(':') !== (WORDS[lk] || '#none#'); }));
    check(bad.length === 0 && !log.some(l => l.cards.some(x => x.startsWith(k.c + ':'))), 'at no moment did a piece show another piece\'s words or a card on the chain (' + log.length + ' changes seen)');

    // 3 · approve: the name is asked as everywhere (me() || askEmployee()), Engrave.approve gets the very button, the seal lands on it
    await page.evaluate(() => { window.__prompts = 0; window.prompt = () => { __prompts++; return null; }; B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} });
    const nameless = await page.evaluate(() => (window.CNEmployee.name() || '') === '');
    if (nameless) {
      // (the name is asked in the small name bar inside the order window, never as a browser pop-up over it; "Not now" approves nothing)
      await page.click(`${cardOf(k.r)} [data-e=approve]`); await page.waitForSelector('#orderWin .cnNameBar input', { timeout: 4000 });
      await page.click('#orderWin .cnNameBar .cnNbX'); await page.waitForTimeout(250);
      check((await calls()).approve.length === 0 && (await page.evaluate(() => __prompts)) === 0 && !(await page.$('.cnNameBar')) && (await card(k.r)).approve, 'with no name, the name bar asks for it in the window and "Not now" approves nothing (the button stays)');
      await page.click(`${cardOf(k.r)} [data-e=approve]`); await page.waitForSelector('#orderWin .cnNameBar input', { timeout: 4000 });
      await page.fill('#orderWin .cnNameBar input', 'Zed Tester'); await page.click('#orderWin .cnNameBar button[type=submit]');
    } else { console.log('  – the name could not be cleared here: the ask-for-a-name path was not run'); await page.click(`${cardOf(k.r)} [data-e=approve]`); }
    await page.waitForFunction(() => __approve.length === 1, null, { timeout: 8000 });
    const during = await page.evaluate(k => { const b = document.querySelector(`#owEng .engCard[data-key="${k}"] .egApproveButton`); return { disabled: b.disabled, busy: b.getAttribute('aria-busy'), pending: !!document.querySelector(`#owEng .engCard[data-key="${k}"] .seal.pending, #owEng .engCard[data-key="${k}"] .egButtonSeal .seal`) }; }, k.r);
    check(during.disabled && during.busy === 'true' && during.pending, 'pressed: the button waits (disabled, busy) while the seal is pressed on it: ' + JSON.stringify(during));
    await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`) && !document.querySelector('#owEng .seal.pending'), k.r, { timeout: 20000 });
    await settled();
    c = await card(k.r); const who = nameless ? 'Zed Tester' : 'Test Operator';
    check((await calls()).approve.length === 1 && (await calls()).approve[0] === `${k.r}|${who}|true`, 'Engrave.approve was called once, for this piece\'s job, with the name and the button on screen: ' + JSON.stringify((await calls()).approve));
    check(c.state === 'approved' && !c.approve && c.buttons === 0 && c.seals.length === 1 && c.seals[0] === `BACK ENGRAVING|${who}` && c.header === 'engDot,engName,engAct', 'the card is approved: the button is gone, the BACK ENGRAVING seal stands in its place, once: ' + JSON.stringify([c.state, c.seals, c.header]));
    check(!c.settledText, 'and still nowhere once it is settled');
    const after = await calls(); check(after.poke >= 1 && after.nudge >= 1, `the run and the order's timeline feed were told (RunCtl.poke ${after.poke}, OrderWin.nudge ${after.nudge})`);
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-approved.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-approved-by-me.png') }); }
    // permanent, never duplicated: away and back, and the window closed and opened again
    await pick(3); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`), k.g, { timeout: 8000 });
    await pick(4); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved] .egButtonSeal .seal`), k.r, { timeout: 8000 });
    await page.waitForTimeout(1300);
    c = await card(k.r); check(c.seals.length === 1 && c.seals[0] === `BACK ENGRAVING|${who}`, 'switching away and back: the seal is there, once');
    await page.evaluate(k => { const b = document.querySelector(`#owEng .engCard[data-key="${k}"] .egApproveButton`); b && b.click(); }, k.r);
    await page.waitForTimeout(300);
    check((await calls()).approve.length === 1 && (await card(k.r)).seals.length === 1 && !(await card(k.r)).approve, 'there is no button to press again: nothing is approved twice and no seal is added');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen() && !document.getElementById('orderWin').open, null, { timeout: 4000 });
    check(await page.evaluate(() => !document.getElementById('owEng')._orderEngraving && !document.getElementById('owEng').firstChild), 'the window closed: the cards and their timer are gone');
    await open(k.r); await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved] .egButtonSeal .seal`), k.r, { timeout: 10000 }); await page.waitForTimeout(1200);
    c = await card(k.r); check(c.seals.length === 1 && c.back.text === 'KMB //\nSMH', 'opened again: still approved, one seal');

    // 4 · the Sheet tab shows the approval made here, with its own card (kept as it is), and the timeline got it
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => document.querySelector('#owSheetPanel [data-engraving-panel] .swEng[data-state=approved]'), null, { timeout: 15000 });
    const sheetSeals = await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel [data-engraving-panel] .egButtonSeal .seal svg[data-seal-model]')].map(s => JSON.parse(s.getAttribute('data-seal-model')).action));
    check(sheetSeals.length === 1 && sheetSeals[0] === 'BACK ENGRAVING', 'the Sheet tab shows the same approval, its own card as before: ' + JSON.stringify(sheetSeals));
    await page.click('.owTabsV [data-ow-view="info"]'); await settled();
    const told = await (async () => { for (let i = 0; i < 40; i++) { if (srv.st.calls.some(c => JSON.stringify(c.body || {}).includes('engraveApproved'))) return true; await page.waitForTimeout(250); } return false; })();
    check(told, 'the order\'s timeline was told (an engraveApproved step went to the record)');

    // 5 · a press on a thumbnail opens THAT face of THAT piece in Engraving: EngraveLink when it is there, else the Sheet tab's own way
    if (await page.evaluate(() => !!(window.EngraveLink && EngraveLink.open))) {
      await page.click(thumb(k.r, 'back'));
      await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 15000 });
      const rv = await page.evaluate(() => Engrave.view());
      check(rv.tab === 'done' && rv.chosen === true && rv.list === false, 'with the real EngraveLink: the window closed and the Engraving tab is on the approved piece: ' + JSON.stringify({ tab: rv.tab, q: rv.q, chosen: rv.chosen, list: rv.list }));
      await page.waitForTimeout(600); await page.evaluate(() => CN.setMode('orders')); await open(k.r);
      await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved] .egButtonSeal .seal`), k.r, { timeout: 10000 });
    }
    await page.evaluate(() => { window.EngraveLink = { open: async t => { __links.push(t); await new Promise(r => setTimeout(r, 1500)); } }; });
    await page.click(thumb(k.r, 'back'));
    const mid = await page.evaluate(sel => { const b = document.querySelector(sel); return { busy: b.getAttribute('aria-busy'), spin: !!b.querySelector('.owSpin') }; }, thumb(k.r, 'back'));
    check(mid.busy === 'true' && mid.spin, 'while it opens the thumbnail says so, with a small spinner: ' + JSON.stringify(mid));
    await page.waitForFunction(() => __links.length === 1, null, { timeout: 4000 });
    await page.waitForFunction(sel => { const b = document.querySelector(sel); return b && b.getAttribute('aria-busy') !== 'true'; }, thumb(k.r, 'back'), { timeout: 4000 });
    const links = (await calls()).links;
    check(links.length === 1 && JSON.stringify(links[0]) === JSON.stringify({ rid: R1, key: k.r, poolId: pool(R1, 3), piece: k.r, face: 'back' }), 'it called EngraveLink.open({rid, key, poolId, piece, face}) for this piece\'s back: ' + JSON.stringify(links[0]));
    check(await page.evaluate(sel => !document.querySelector(sel), thumb(k.r, 'front')), 'there is no Front thumbnail to press on a back-only piece (ENGSMART: no empty slot)');
    await page.click(`${cardOf(k.r)} .engName`); await page.waitForFunction(() => __links.length === 2, null, { timeout: 4000 });
    check((await calls()).links[1].face === 'back' && (await calls()).links[1].key === k.r, 'a press on the piece\'s name opens the same piece on the one face it has: ' + JSON.stringify((await calls()).links[1]));
    check(await page.evaluate(() => OrderWin.isOpen()), 'and the window was left to EngraveLink (it closes it first)');
    await pick(3); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`), k.g, { timeout: 8000 });
    await page.click(thumb(k.g, 'back')); await page.waitForFunction(() => __links.length === 3, null, { timeout: 4000 });
    check(JSON.stringify((await calls()).links[2]) === JSON.stringify({ rid: R1, key: k.g, poolId: pool(R1, 2), piece: k.g, face: 'back' }), 'the other piece asks for its own: ' + JSON.stringify((await calls()).links[2]));
    // (until EngraveLink exists: the Sheet tab's way, the window closes and the Engraving tab is set to that order and piece)
    await page.evaluate(() => { delete window.EngraveLink; });
    await page.click(thumb(k.g, 'back'));
    await page.waitForFunction(() => !OrderWin.isOpen() && CN.S.mode === 'engrave', null, { timeout: 8000 });
    const v1 = await page.evaluate(() => Engrave.view());
    check(v1.tab === 'done' && v1.q === R1 && v1.chosen === true && v1.list === false && !v1.focus, 'without EngraveLink: the window closes and the Engraving tab opens on the approved piece\'s order: ' + JSON.stringify({ tab: v1.tab, q: v1.q, chosen: v1.chosen, list: v1.list }));
    await page.evaluate(() => CN.setMode('orders'));

    // 6 · live: an approval made in the Engraving tab / Sheet tab / sheet window (Engrave's own job), and one another computer made
    await open(key(R2, 1)); await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approve] [data-e=approve]`), key(R2, 1), { timeout: 10000 });
    c = await card(key(R2, 1)); check(c.state === 'approve' && c.back.text === 'Aster' && !c.settledText, 'order 2, piece 1: to approve, with no "still to be settled" text');
    const t0 = Date.now();
    await page.evaluate(k => { const j = Engrave.items().get(k), at = Date.now(); CNEngravingSeals.add(j, 'engraveApproved', 'Sam Tester', at); Object.assign(j, { state: 'approved', approvedBy: 'Sam Tester', approvedAt: at }); Object.assign(j.row.engrave, { state: 'approved', approved: true, approvedBy: 'Sam Tester', approvedAt: at }); }, key(R2, 1));
    await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`), key(R2, 1), { timeout: 5000 });
    const took = Date.now() - t0; c = await card(key(R2, 1));
    check(took <= 3000 && !c.approve && c.buttons === 0 && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Sam Tester', `approved in the Engraving tab or the sheet window: the card follows within ${took} ms, with the seal`);
    // another computer: only the timeline knows (its feed is read by the open window already)
    await page.evaluate(k => OrderWin.selectPiece(k), key(R2, 2)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approve]`), key(R2, 2), { timeout: 8000 });
    const t1 = Date.now();
    await page.evaluate(({ R2, k }) => { const at = Date.now(); window.__evs[R2] = [{ id: R2 + '~arrived', orderId: R2, type: 'arrived', at: at - 3600e3, by: 'Etsy', source: 'etsy' }, { id: `${k}_1-${at}`, orderId: R2, type: 'engraveApproved', at, by: 'Giovanna', source: 'sorter', station: 'sorter', lineKey: k, transactionId: k.split('_')[1], text: '“Leaf”', data: { text: 'Leaf', poolId: k + '_1', copy: 1 } }]; OrderWin._feed().refresh({ force: true }); }, { R2, k: key(R2, 2) });
    await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`), key(R2, 2), { timeout: 6000 });
    const took2 = Date.now() - t1; c = await card(key(R2, 2));
    check(took2 <= 3500 && !c.approve && c.back.text === 'Leaf' && c.seals.length === 1 && c.seals[0] === 'BACK ENGRAVING|Giovanna', `approved on another computer: the card shows it ${took2} ms after the timeline feed read it, nobody can approve it twice here: ` + JSON.stringify([c.state, c.back.text, c.seals]));
    check((await page.evaluate(k => Engrave.items().get(k).state, key(R2, 2))) === 'review', 'and Engrave\'s own job was not touched by showing it');
    const n0 = (await calls()).approve.length; await page.evaluate(k => { const b = document.querySelector(`#owEng .engCard[data-key="${k}"] .egApproveButton`); b && b.click(); }, key(R2, 2)); await page.waitForTimeout(200);
    check((await calls()).approve.length === n0 && !(await card(key(R2, 2))).approve, 'no second approval is possible from the card (it has no button)');

    // 7 · an order with one piece and no engraving: no card; an order still being read says what it waits for
    await open(key(R3, 1)); await page.waitForTimeout(500);
    c = await card(key(R3, 1)); check(c.hidden && !c.state && !c.wait && c.n === 0, 'an order with no engraving shows no card');
    const early = await page.evaluate(() => { OrderWin.openOrder('4179990001', { q: '4179990001', row: null }); const h = document.getElementById('owEng'); return { hidden: h.hidden, wait: h.querySelector('.owEngWait')?.textContent || '' }; });
    check(!early.hidden && /Reading/.test(early.wait), 'an order still being read: a small spinner with its words, not a stale card: ' + JSON.stringify(early));
    await page.waitForTimeout(800);
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-engraving-card: ok');
})().catch(e => { console.error(e); process.exit(1); });
