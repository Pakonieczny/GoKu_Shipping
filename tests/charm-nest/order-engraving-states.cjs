// The engraving card, every state, every width (Paul, 12 Oct 2026: "the cards are currently full of needless text ... front and back side-by-side ... keep it very simple and minimalist").
// charm-nest-engrave-card.js draws ONE card for each piece of the order in the order window's Engraving section (charm-nest-order-engraving.js keeps them alive): the piece's name, ONE state
// dot, Front and Back side by side (a titled picture of the charm with the words laid on it, the words under it) and ONE Approve button while a face waits for approval (an approved
// card has its seals in the button's place). The Sheet tab and the Sheet window draw CNEngravingSeals.panel(), unchanged: its gallery below is the same test it always was.
//   · every state (words to confirm, being prepared, to approve, approved, cut plain) is its one dot and nothing else: no status word, no reason, no "Fix in Engraving", no percentage;
//     nothing in a card is wider than the card, and the cards stay inside their grid at 390, 900 and 1440 px windows;
//   · a press on a face asks EngraveLink for that exact order, piece and face (a small spinner on the picture while it opens); Approve is the host's own approval (Engrave.approve, the name,
//     the BACK ENGRAVING seal pressed on that button) and the card then shows the seal where the button was;
//   · every piece that has an engraving has its card at once, whichever piece the window holds; a piece with none has none.
// Headless Chromium against the local fake site (bridge-server.cjs), offline; Engrave.approve is a stand-in that presses the real seal through CNEngravingSeals.press.
//   SHOTS=<dir> node tests/charm-nest/order-engraving-states.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const A = { rid: '4174601819', tids: ['41746018191', '41746018192', '41746018193'], skus: ['HUGGIE_HOOPS_RUBBER_DUCK_SHAPE', 'DUCK_38090', 'DUCK_CHARM_38091'], buyer: 'Amy Thomas' };
const B = { rid: '4175550002', tids: ['41755500021', '41755500022', '41755500023', '41755500024', '41755500025'], skus: ['ASTER_FLOWER', 'LEAF_CHARM', 'MOON_DISC', 'PLAIN_TAG', 'STAR_CHARM'], buyer: 'Ava Patel' };
const C = { rid: '4175550003', tids: ['41755500031'], skus: ['LONG_WORDS_TAG'], buyer: 'Hannah Whitford' };
const keyOf = (o, i) => `${o.rid}_${o.tids[i]}`, pool = (o, i) => keyOf(o, i) + '_1';
const line = (tid, sku) => ({ transactionId: tid, listingId: '19037' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'GF 14/20' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: '' });
const order = o => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: o.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.tids.map((t, i) => line(t, o.skus[i])) });
const LONG = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOP';

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the orders and their jobs: one of each state ──
    //   A0 an earring with a words job (no card: no earring is engraved) · A1 no back engraving · A2 words to confirm (with what is unclear)
    //   B0 to approve · B1 being prepared (working) · B2 approved (an earlier seal) · B3 cut plain · B4 being prepared (waits its turn)
    //   C0 words to confirm, a long unbroken string and three questions
    await page.evaluate(async ({ orders, LONG }) => {
      const T0 = Date.now() - 6 * 3600e3, rows = new Map();
      for (const o of orders) for (const l of o.lines) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); rows.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const art = text => { const c = document.createElement('canvas'); c.width = c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; String(text).split('\n').forEach((t, i) => x.fillText(t, 150, 130 + i * 48)); return c; };
      Engrave.renderBack = job => art(job.text || '');
      const fit = () => ({ size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' });
      const mk = (key, state, text, extra) => { const row = rows.get(key), j = Engrave.ensureJob(row); Object.assign(j, { state, text, lines: text ? text.split('\n') : [], verify: { geometry: { ok: true } } }, extra || {}); row.engrave = state === 'none' ? null : { needed: state !== 'skipped', state, approved: false, text }; return j; };
      const [a0, a1, a2, b0, b1, b2, b3, b4, c0] = [...rows.keys()];
      mk(a0, 'words', 'Lily\n& Max', { reason: 'Check the requested inscription', questions: ['Is “& Max” part of the engraving or a note for us?'], source: 'personalization', confidence: .52, requests: { font: 'script' } });
      mk(a2, 'words', 'Lily\n& Max', { reason: 'Check the requested inscription', questions: ['Is “& Max” part of the engraving or a note for us?'], source: 'personalization', confidence: .52, requests: { font: 'script' } });
      mk(a1, 'none', '');
      mk(b0, 'review', 'KMB //\nSMH', { fit: fit(), view: {}, source: 'personalization', confidence: .91 });
      mk(b1, 'ready', 'Leaf', {});
      const at = Date.now() - 2 * 3600e3;
      const j2 = mk(b2, 'approved', 'I\ndissent', { approvedBy: 'Giovanna', approvedAt: at, backs: [{ poolId: b2 + '_1', approvedAt: at, approvedBy: 'Giovanna', png: art('I\ndissent').toDataURL() }] });
      Object.assign(rows.get(b2).engrave, { approved: true, approvedBy: 'Giovanna', approvedAt: at }); CNEngravingSeals.add(j2, 'engraveApproved', 'Giovanna', at); CNEngravingSeals.keep(j2);
      const j3 = mk(b3, 'skipped', '', { approvedBy: 'Sam', decidedAt: at, decidedBy: 'Sam' }); CNEngravingSeals.add(j3, 'engravePlain', 'Sam', at); CNEngravingSeals.keep(j3);
      mk(b4, 'ready', 'Star', {});
      mk(c0, 'words', LONG, { reason: 'Enter the requested inscription', questions: ['Is the buyer’s long note really the engraving, or only part of it?', 'Which of ' + LONG + ' should go on the back?'], source: 'buyerMessage', confidence: .4, requests: { side: 'front', handwriting: true } });
      Engrave.isWorking = j => j.key === b1;
      window.__approve = []; window.__links = [];
      Engrave.approve = async (job, by, button) => {
        __approve.push({ key: job.key, by, button: !!button && button.isConnected });
        await new Promise(r => setTimeout(r, 700));
        const t = Date.now(), seal = CNEngravingSeals.add(job, 'engraveApproved', by, t);
        job.stamping = true; try { await CNEngravingSeals.press(button, seal); } finally { job.stamping = false; }
        Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: t }); Object.assign(job.row.engrave, { state: 'approved', approved: true, approvedBy: by, approvedAt: t });
      };
      Engrave.render = () => {};
      window.EngraveLink = { open: async t => { __links.push(t); await new Promise(r => setTimeout(r, 900)); return true; } };
      OrderTimeline.get = async rid => { await new Promise(r => setTimeout(r, 15)); return JSON.parse(JSON.stringify({ id: rid, events: [{ id: rid + '~arrived', orderId: rid, type: 'arrived', at: T0, by: 'Etsy', source: 'etsy' }], cancelled: null, where: null })); };
      CN.setMode('orders'); Orders.render();
    }, { orders: [order(A), order(B), order(C)], LONG });

    const open = async key => { await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !document.querySelector('#orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), key, { timeout: 15000 }); };
    const close = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 6000 }).catch(() => {}); };
    const pick = piece => page.evaluate(p => OrderWin.selectPiece(p || null), piece);   // (the "Its pieces" list's own switch; null: all pieces)
    const settled = () => page.waitForFunction(() => !document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 });
    // what a piece's card shows, and whether anything of it lies outside the card's own box (the card is found by its line key, whichever piece the window holds)
    const DEL = ['TO APPROVE', 'To approve', 'Waiting for your approval', 'WORDS ON THE BACK', 'Words on the back', 'Read from', 'sure', '%', 'ENGRAVES', 'nothing in the listing', 'The system read', 'Undo', 'Approve engraving', 'Being prepared', 'Cut plain', 'Preview not ready', 'The words need a decision'];
    const cardSel = lk => `#owEng .engCard[data-key="${lk}"]`;
    const read = lk => page.evaluate(sel => {
      const card = document.querySelector(sel); if (!card) return null;
      const r = card.getBoundingClientRect(), q = s => card.querySelector(s), out = [];
      for (const n of card.querySelectorAll('*')) {
        if (n.closest('.seal, .sealTool, .sealRing') || !n.getClientRects().length) continue;
        const b = n.getBoundingClientRect(); if (!b.width || !b.height) continue;
        if (b.right > r.right + 0.5 || b.left < r.left - 0.5) out.push(`${n.tagName.toLowerCase()}.${String(n.className).split(' ')[0]} ${Math.round(b.left - r.left)}..${Math.round(b.right - r.left)} of ${Math.round(r.width)}`);
      }
      const seals = [...card.querySelectorAll('.seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '|' + m.by; } catch (_) { return '?'; } });
      const ab = q('.egApproveButton'), face = f => { const e = q(`.engFace[data-face=${f}]`); return e ? { empty: e.dataset.empty === '1', text: (e.querySelector('.engTxt') || {}).title || '', shown: (e.querySelector('.engTxt') || {}).textContent || '', picture: !!e.querySelector('.engThumb canvas, .engThumb img, .engThumb .engpv') } : null; };
      return { state: card.dataset.state, w: Math.round(r.width), h: Math.round(r.height), scrollOver: card.scrollWidth - card.clientWidth, out, dot: q('.engDot') && q('.engDot').getAttribute('aria-label'), dots: card.querySelectorAll('.engDot').length, name: (q('.engName') || {}).textContent || '',
        front: face('front'), back: face('back'), buttons: card.querySelectorAll('button:not(.engThumb):not(.egOpen)').length, approve: ab ? { text: ab.textContent.trim(), disabled: ab.disabled, live: !!q('[data-e=approve]'), title: ab.title || '' } : null, seals, text: card.textContent.replace(/\s+/g, ' ').trim(), spinner: !!q('.engTxt .owSpin') };
    }, cardSel(lk));
    const none = c => DEL.filter(w => c.text.includes(w));

    // 1 · each state, as the Overview draws it (1440 px window): a dot, the name, Front and Back, and nothing else
    await open(keyOf(A, 2)); await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=words]`), keyOf(A, 2), { timeout: 10000 }); await settled();
    let c = await read(keyOf(A, 2));
    check(!(await page.evaluate(k => !!document.querySelector(`#owEng .engCard[data-key="${k}"]`), keyOf(A, 0))), 'an earring (its words job is still there) has no card: no earring is engraved');
    check(c.state === 'words' && c.dot === 'Words to confirm' && c.dots === 1 && c.name.startsWith('DUCK CHARM 38091'), 'words to confirm: ONE dot that says so, on the piece it belongs to: ' + JSON.stringify([c.dot, c.dots, c.name]));
    check(c.back.text === 'Lily\n& Max' && c.back.shown === 'Lily / & Max' && c.front === null && none(c).length === 0, 'the words are shown on one line under the Back, there is no Front slot at all, none of the old text is there: ' + JSON.stringify([c.back, c.front, none(c)]));
    check(!c.approve && c.buttons === 0, 'no Approve while the words are not confirmed (no button at all): ' + JSON.stringify([c.approve, c.buttons]));
    check(c.out.length === 0 && c.scrollOver <= 0, 'nothing is wider than the card at 1440: ' + JSON.stringify(c.out));
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-1440-words.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-words.png') }); }

    // 2 · an order of several: a card for each piece that has an engraving, in every state (none for a piece with none, nor for one cut plain)
    await close(); await open(keyOf(B, 0)); await page.waitForFunction(() => document.querySelectorAll('#owEng .engCard').length === 4, null, { timeout: 10000 }); await settled();   // (the plain tag, cut plain, engraves nothing: ENGSMART gives it no card)
    const list = await page.evaluate(() => [...document.querySelectorAll('#owEng .engCard')].map(e => ({ key: e.dataset.key, state: e.dataset.state, name: (e.querySelector('.engName') || {}).textContent || '', buttons: e.querySelectorAll('button:not(.engThumb):not(.egOpen)').length })).sort((a, b) => a.key.localeCompare(b.key)));
    check(list.map(x => x.key).join() === [0, 1, 2, 4].map(i => keyOf(B, i)).join() && list.map(x => x.state).join() === 'approve,busy,approved,busy', 'a card for each of the four pieces that are engraved (none for the one cut plain), each in its own state (' + list.map(x => x.state).join('/') + ')');
    check(list[0].name.startsWith('ASTER FLOWER') && list[2].name.startsWith('MOON DISC') && !list.some(x => x.name.startsWith('PLAIN TAG')) && list.map(x => x.buttons).join() === '1,0,0,0', 'each names its piece, and only the one that waits for approval has a button: ' + JSON.stringify(list.map(x => [x.name.slice(0, 14), x.buttons])));
    check(await page.evaluate(() => document.querySelectorAll('#owEng > :not(.engCard)').length === 0 && !document.querySelector('#owEng .fLabel')), 'the cards are the grid\'s direct children, with no label of their own');
    const allOut = await page.evaluate(() => { const h = document.querySelector('#owEng'), r = h.getBoundingClientRect(); return [...h.querySelectorAll('.engCard')].filter(e => { const b = e.getBoundingClientRect(); return b.right > r.right + .5 || e.scrollWidth - e.clientWidth > 0; }).length; });
    check(allOut === 0, 'none is wider than its column');
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-1440-all-pieces.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-all-pieces.png') }); }
    c = await read(keyOf(B, 0));
    check(c.state === 'approve' && c.dot === 'To approve' && c.back.text === 'KMB //\nSMH' && c.back.picture && c.name.startsWith('ASTER FLOWER') && c.approve && !c.approve.disabled && c.approve.live && c.approve.text === 'Approve' && c.buttons === 1 && none(c).length === 0 && c.out.length === 0, 'to approve: the words with their picture, ONE live Approve button: ' + JSON.stringify([c.dot, c.name, c.approve, none(c)]));
    if (shots) await page.locator(cardSel(keyOf(B, 0))).screenshot({ path: path.join(shots, 'card-1440-approve.png') });
    c = await read(keyOf(B, 1));
    check(c.state === 'busy' && c.dot === 'Preparing' && c.back.shown === 'Leaf' && c.buttons === 0 && none(c).length === 0, 'being prepared: its dot, the words, no button (a piece Engrave is still working on): ' + JSON.stringify([c.dot, c.back, c.buttons]));
    if (shots) await page.locator(cardSel(keyOf(B, 1))).screenshot({ path: path.join(shots, 'card-1440-preparing.png') });
    await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"] .egButtonSeal .seal`), keyOf(B, 2), { timeout: 5000 }); await page.mouse.move(700, 880);
    c = await read(keyOf(B, 2));
    check(c.state === 'approved' && c.dot === 'Approved' && c.back.text === 'I\ndissent' && !c.approve && c.buttons === 0 && c.seals.join() === 'BACK ENGRAVING|Giovanna' && c.back.picture, 'approved: the green dot, the words, the seal where the button would be, no button: ' + JSON.stringify([c.dot, c.approve, c.seals]));
    check(c.out.length === 0, 'with the seal inside the card: ' + JSON.stringify(c.out));
    if (shots) await page.locator(cardSel(keyOf(B, 2))).screenshot({ path: path.join(shots, 'card-1440-approved.png') });
    // a seal zooms where it stands (a click), with no tooltip and no caption of its own
    await page.evaluate(sel => document.querySelector(sel + ' .egButtonSeal .seal').scrollIntoView({ block: 'center' }), cardSel(keyOf(B, 2))); await page.waitForTimeout(500);
    await page.click(cardSel(keyOf(B, 2)) + ' .egButtonSeal .seal'); await page.waitForTimeout(800);
    const zoomed = await page.evaluate(sel => { const s = document.querySelector(sel + ' .egButtonSeal .seal'); return { cur: Seal.zoom.current === s, tip: !!(s.getAttribute('title') || s.querySelector('title')), hasTitle: s.hasAttribute('title') }; }, cardSel(keyOf(B, 2)));
    check(zoomed.cur && !zoomed.tip && !zoomed.hasTitle, 'a seal on the card zooms in place and carries no tooltip: ' + JSON.stringify(zoomed));
    await page.evaluate(() => Seal.zoom.away()); await page.waitForTimeout(400);
    c = await read(keyOf(B, 3));
    // (ENGSMART, Paul: "if there is no engraving, it should not show it": a piece a person cut plain engraves nothing, so it has no card and no face; the Engrave tab's Decided list keeps its seal)
    check(c === null, 'cut plain: no card and no face for the piece (nothing is engraved on it): ' + JSON.stringify(c && [c.state, c.front, c.back]));
    await close(); await open(keyOf(A, 1)); await page.waitForFunction(() => document.querySelector('#owEng .engCard'), null, { timeout: 8000 });
    check(await page.evaluate(() => document.querySelectorAll('#owEng .engCard').length === 1 && document.querySelector('#owEng .engCard .engName').textContent.startsWith('DUCK CHARM')), 'an order of three pieces with one engraving: its one card, none for the earring or the piece with no engraving');
    const none2 = await page.evaluate(() => { const h = document.createElement('div'); h.innerHTML = CNEngravingSeals.panel({ kind: 'none' }); return h.querySelector('.swEng').dataset.state + '|' + h.querySelector('.top b').textContent; });
    check(none2 === 'none|No back engraving', 'and the Sheet tab and Sheet window say it quietly, in their own card: ' + none2);

    // 3 · the buttons: a press on a face asks EngraveLink for exactly that order, piece and face, with a small spinner; Approve is the host's own approval
    await close(); await open(keyOf(B, 0)); await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approve]`), keyOf(B, 0), { timeout: 8000 }); await settled();
    await page.click(cardSel(keyOf(B, 0)) + ' .engFace[data-face=back] .engThumb');
    const mid = await page.evaluate(sel => { const b = document.querySelector(sel + ' .engFace[data-face=back] .engThumb'); return { busy: b.getAttribute('aria-busy'), spin: !!b.querySelector('.owSpin') }; }, cardSel(keyOf(B, 0)));
    check(mid.busy === 'true' && mid.spin, 'a press on the Back: while it opens the picture says so, with a small spinner: ' + JSON.stringify(mid));
    await page.waitForFunction(() => __links.length === 1, null, { timeout: 6000 });
    const l0 = await page.evaluate(() => __links[0]);
    check(JSON.stringify(l0) === JSON.stringify({ rid: B.rid, key: keyOf(B, 0), poolId: pool(B, 0), piece: keyOf(B, 0), face: 'back' }), 'it called EngraveLink.open({rid, key, poolId, piece, face}) for that piece\'s back: ' + JSON.stringify(l0));
    await page.waitForFunction(sel => document.querySelector(sel + ' .engFace[data-face=back] .engThumb').getAttribute('aria-busy') !== 'true', cardSel(keyOf(B, 0)), { timeout: 6000 });
    // Approve: the name is asked as everywhere, the seal is pressed on that very button, the card then shows the BACK ENGRAVING seal once
    await page.click(cardSel(keyOf(B, 0)) + ' [data-e=approve]');
    const during = await page.evaluate(sel => { const b = document.querySelector(sel + ' .egApproveButton'); return { disabled: b.disabled, text: b.textContent.trim(), spin: !!b.querySelector('.spin'), busy: b.getAttribute('aria-busy') }; }, cardSel(keyOf(B, 0)));
    check(during.disabled && during.spin && during.text === 'Approving…' && during.busy === 'true', 'Approve: the wait before the stamp is a small labelled spinner on the button: ' + JSON.stringify(during));
    await page.waitForFunction(k => document.querySelector(`#owEng .engCard[data-key="${k}"][data-state=approved]`) && !document.querySelector('#owEng .seal.pending') && !Seal.busy(), keyOf(B, 0), { timeout: 20000 });
    await settled(); c = await read(keyOf(B, 0));
    const ap = await page.evaluate(() => __approve);
    check(ap.length === 1 && ap[0].key === keyOf(B, 0) && ap[0].by === 'Test Operator' && ap[0].button, 'it ran the host\'s own approval once, for that piece, with the name and the button on screen: ' + JSON.stringify(ap));
    check(c.state === 'approved' && c.dot === 'Approved' && c.seals.join() === 'BACK ENGRAVING|Test Operator' && !c.approve && c.buttons === 0 && c.out.length === 0, 'the card now has its green dot and the BACK ENGRAVING seal, once, and no button: ' + JSON.stringify([c.state, c.seals, c.buttons]));

    // 4 · no width of the card clips: the card in every state at 280, 390 and 700 px (a gallery of the same markup, wired as the hosts wire it), 200 px too
    await close();
    const galleryFn = async w => {
        document.getElementById('__gal')?.remove();
        const g = document.createElement('div'); g.id = '__gal'; Object.assign(g.style, { position: 'fixed', left: '0', top: '0', width: (w + 40) + 'px', zIndex: 2147483000, background: 'var(--paper)', padding: '20px', display: 'grid', gap: '16px', alignContent: 'start' });
        const job = (state, extra) => Object.assign({ key: 'g-' + state, state, row: { state: 'written' } }, extra || {});
        const T = Date.now() - 3600e3, seals = n => Array.from({ length: n }, (_, i) => ({ id: 's' + i, how: 'engraveApproved', at: T + i * 60000, by: ['Giovanna', 'Seth', 'Another Operator'][i % 3] }));
        const LONGW = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOP';
        const cards = [
          ['approve', { kind: 'approve', job: job('review', { fit: {}, view: {}, verify: { geometry: { ok: true } } }), text: 'Lily\n& Max', pieceLabel: 'ASTER FLOWER', pieceMeta: 'GF 14/20' }],
          ['approve with a seal', { kind: 'approve', job: job('review', { fit: {}, view: {}, verify: { geometry: { ok: true } }, engravingSeals: seals(1) }), text: 'Lily & Max', pieceLabel: 'A VERY LONG PIECE NAME THAT KEEPS GOING ' + LONGW, pieceMeta: 'GF 14/20 · ×2' }],
          ['words', { kind: 'words', job: job('words', { reason: 'Check the requested inscription', questions: ['Is the long note really the engraving? ' + LONGW], source: 'personalization', confidence: .5, requests: { font: 'script', handwriting: true } }), text: LONGW, pieceLabel: 'HUGGIE HOOPS RUBBER DUCK SHAPE', pieceMeta: 'GF 14/20' }],
          ['words, no text', { kind: 'words', job: job('words', { reason: 'Enter the requested inscription' }), text: '' }],
          ['preparing', { kind: 'preparing', job: job('fitting'), text: 'Leaf', note: 'Fitting the words on the back…', working: true }],
          ['approved, three seals', { kind: 'approved', job: job('approved', { engravingSeals: seals(3) }), text: 'I\ndissent', by: 'Another Operator', at: T + 120000 }],
          ['cut plain', { kind: 'skipped', job: job('skipped', { engravingSeals: [{ id: 'p', how: 'engravePlain', at: T, by: 'Sam' }] }), by: 'Sam' }],
          ['none', { kind: 'none' }],
          ['compact approve', { kind: 'approve', job: job('review', { fit: {}, view: {}, verify: { geometry: { ok: true } } }), text: 'KMB // SMH', pieceLabel: 'DUCK 38090', pieceMeta: 'SS', compact: true }]
        ];
        const rows = [];
        for (const [name, e] of cards) {
          const h = document.createElement('div'); h.className = 'galOne'; h.dataset.name = name; h.style.width = w + 'px'; h.innerHTML = CNEngravingSeals.panel(e); g.appendChild(h);
        }
        document.body.appendChild(g);
        for (const h of g.children) { const e = [...cards].find(c => c[0] === h.dataset.name)[1]; CNEngravingSeals.wirePanel(h, e, { approve: async () => {}, open: async () => {}, imageUrl: u => u }); const pv = h.querySelector('[data-engraving-preview]'); if (pv) { const cv = document.createElement('canvas'); cv.width = cv.height = 300; const x = cv.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; String(e.text || '').slice(0, 14).split('\n').forEach((t, i) => x.fillText(t, 150, 140 + i * 48)); pv.replaceChildren(cv); } }
        await new Promise(r => setTimeout(r, 450)); Seal.fitGroups && Seal.fitGroups(g); await new Promise(r => setTimeout(r, 200));
        for (const h of g.children) {
          const card = h.querySelector('.swEng'), r = card.getBoundingClientRect(), bad = [];
          for (const n of card.querySelectorAll('*')) { if (n.closest('.seal') || !n.getClientRects().length) continue; const b = n.getBoundingClientRect(); if (b.width && (b.right > r.right + .5 || b.left < r.left - .5)) bad.push(n.tagName.toLowerCase() + '.' + String(n.className).split(' ')[0] + ' ' + Math.round(b.right - r.right)); }
          const seal = [...card.querySelectorAll('.seal')].filter(s => { const b = s.getBoundingClientRect(); return b.right > r.right + 1.5 || b.left < r.left - 1.5; }).length;
          const sr = [...card.querySelectorAll('.seal')].map(s => Math.round(s.getBoundingClientRect().right - r.right)), kr = card.querySelector('.egButtonSeal'), kb = kr && kr.getBoundingClientRect();
          rows.push({ name: h.dataset.name, w: Math.round(r.width), over: card.scrollWidth - card.clientWidth, bad, sealOut: seal, sealR: sr, row: kb && [Math.round(kb.left - r.left), Math.round(kb.right - r.right)], pill: (card.querySelector('.egPill') || {}).textContent || '' });
        }
        return rows;
    };
    const widths = [200, 280, 390, 700];
    for (const w of widths) {
      const rep = await page.evaluate(galleryFn, w);
      const bad = rep.filter(x => x.over > 0 || x.bad.length || x.sealOut);
      check(bad.length === 0, `at a card width of ${w} px: ${rep.length} cards (${rep.map(x => x.name).join(', ')}), nothing clipped or poking out` + (bad.length ? ' ✗ ' + JSON.stringify(bad) : ''));
    }
    await page.evaluate(() => document.getElementById('__gal')?.remove());

    // 5 · the cards in the order window at 390, 900 and 1440 px: nothing in a card pokes out of it, the card stays inside the grid
    for (const [vw, vh] of [[390, 844], [900, 800], [1440, 900]]) {
      await page.setViewportSize({ width: vw, height: vh });
      for (const [who, key, state, shot] of [[A, keyOf(A, 2), 'words', 'words'], [B, keyOf(B, 1), 'busy', 'preparing'], [C, keyOf(C, 0), 'words', 'words-long']]) {
        await close(); await open(key);
        await page.waitForFunction(({ s, key }) => document.querySelector(`#owEng .engCard[data-key="${key}"][data-state=${s}]`), { s: state, key }, { timeout: 10000 }); await settled(); await page.waitForTimeout(250);
        // (at 390 px the window's own header, with the previous/next arrows of a list of orders, is wider than the phone and the window slides sideways when focus lands on an arrow: not this card's;
        //  put back to the left edge so the card is read where a person first sees it, and said plainly)
        const slid = await page.evaluate(() => { const o = document.getElementById('orderWin'), n = o.scrollLeft; o.scrollLeft = 0; return n; });
        if (slid) console.log(`  - ${vw} px window: the window's header is wider than the screen (the window had slid ${Math.round(slid)} px sideways); not the card's: put back before reading it`);
        const geo = await page.evaluate(k => {
          const h = document.getElementById('owEng'), card = h.querySelector(`.engCard[data-key="${k}"]`), r = card.getBoundingClientRect(), cr = h.getBoundingClientRect(), out = [];
          for (const n of card.querySelectorAll('*')) { if (n.closest('.seal') || !n.getClientRects().length) continue; const b = n.getBoundingClientRect(); if (b.width && (b.right > r.right + .5 || b.left < r.left - .5)) out.push(n.tagName.toLowerCase() + '.' + String(n.className).split(' ')[0]); }
          return { card: [Math.round(r.left), Math.round(r.right)], col: [Math.round(cr.left), Math.round(cr.right)], out, inCol: r.left >= cr.left - 1 && r.right <= cr.right + 1, over: card.scrollWidth - card.clientWidth, h: Math.round(r.height) };
        }, key);
        check(geo.out.length === 0 && geo.over <= 0 && geo.inCol, `${vw} px window, ${shot}: the card (${geo.card.join('..')}) is inside its grid (${geo.col.join('..')}), nothing in it pokes out ${JSON.stringify(geo.out)}`);
        if (shots) { await page.screenshot({ path: path.join(shots, `overview-${vw}-${shot}.png`) }); await page.locator('#owEng').screenshot({ path: path.join(shots, `card-${vw}-${shot}.png`) }); }
      }
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    if (shots) {   // the same gallery, one picture per width, on a page of its own (tall enough to hold it)
      const p2 = await context.newPage(); p2.setDefaultTimeout(30000);
      await p2.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await p2.waitForFunction(() => window.CNEngravingSeals && window.Seal && window.Engrave, null, { timeout: 60000 });
      for (const w of [280, 390, 700]) { await p2.setViewportSize({ width: w + 40, height: 3300 }); await p2.evaluate(galleryFn, w); const gh = await p2.evaluate(() => document.getElementById('__gal').scrollHeight); await p2.setViewportSize({ width: w + 40, height: gh + 40 }); await p2.waitForTimeout(250); await p2.locator('#__gal').screenshot({ path: path.join(shots, `gallery-card-${w}.png`) }); for (const n of await p2.locator('#__gal .galOne').all()) { const nm = await n.getAttribute('data-name'); await n.screenshot({ path: path.join(shots, `state-${nm.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}-${w}.png`) }); } }
      await p2.close();
    }
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-engraving-states: ok');
})().catch(e => { console.error(e); process.exit(1); });
