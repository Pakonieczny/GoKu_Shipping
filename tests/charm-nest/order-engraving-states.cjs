// The back engraving card, every state, every width (Paul, 5 Oct 2026, 17:47 UTC: "The engraving UI is broken in all detail order modals. Fix this version of
// the engraving and make it look proper and fully featured showing what is actually needed with quick link buttons to the engraving panel to fix it and also to
// approve engraving."). charm-nest-engraving-seals.js (CNEngravingSeals.panel + wirePanel) is the one card; charm-nest-order-engraving.js draws it in the order
// window's Overview (one piece, or one compact card per piece under "All pieces"); the Sheet tab and the Sheet window draw the same card.
//   · every state (To approve, Words to confirm, Being prepared, Approved, Cut plain, No back engraving) says its one status and the plain reason, and shows the
//     words as read, what is unclear and the piece it belongs to; nothing in it is wider than the card, from 200 to 700 px, and nothing pokes into the next column
//     (the status pill and the buttons wrap and stack: the card in the order window's Overview at 390, 900 and 1440 px windows);
//   · Fix in Engraving calls EngraveLink.open for that exact order and piece; Approve engraving is the host's own approval (Engrave.approve, the name, the BACK
//     ENGRAVING seal pressed on that button) and is disabled, with one plain line of why, while the words are not confirmed or the back is not ready;
//   · "All pieces" shows a compact card for each piece that has a back engraving and none for one that has none; one piece picked shows that piece's card.
// Headless Chromium against the local fake site (bridge-server.cjs), offline; Engrave.approve is a stand-in that presses the real seal through CNEngravingSeals.press.
//   SHOTS=<dir> node tests/charm-nest/order-engraving-states.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const A = { rid: '4174601819', tids: ['41746018191', '41746018192'], skus: ['HUGGIE_HOOPS_RUBBER_DUCK_SHAPE', 'DUCK_38090'], buyer: 'Amy Thomas' };
const B = { rid: '4175550002', tids: ['41755500021', '41755500022', '41755500023', '41755500024', '41755500025'], skus: ['ASTER_FLOWER', 'LEAF_CHARM', 'MOON_DISC', 'PLAIN_TAG', 'STAR_STUD'], buyer: 'Ava Patel' };
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
    //   A0 words to confirm (with what is unclear) · A1 no back engraving
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
      const [a0, a1, b0, b1, b2, b3, b4, c0] = [...rows.keys()];
      mk(a0, 'words', 'Lily\n& Max', { reason: 'Check the requested inscription', questions: ['Is “& Max” part of the engraving or a note for us?'], source: 'personalization', confidence: .52, requests: { font: 'script' } });
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
    // what a card shows, and whether anything of it lies outside the card's own box
    const read = sel => page.evaluate(sel => {
      const card = document.querySelector(sel); if (!card) return null;
      const r = card.getBoundingClientRect(), q = s => card.querySelector(s), out = [];
      for (const n of card.querySelectorAll('*')) {
        if (n.closest('.seal, .sealTool, .sealRing') || !n.getClientRects().length) continue;
        const b = n.getBoundingClientRect(); if (!b.width || !b.height) continue;
        if (b.right > r.right + 0.5 || b.left < r.left - 0.5) out.push(`${n.tagName.toLowerCase()}.${String(n.className).split(' ')[0]} ${Math.round(b.left - r.left)}..${Math.round(b.right - r.left)} of ${Math.round(r.width)}`);
      }
      const seals = [...card.querySelectorAll('.seal svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '|' + m.by; } catch (_) { return '?'; } });
      const ab = q('.egApproveButton'), op = q('[data-e=engrave]');
      return { state: card.dataset.state, w: Math.round(r.width), scrollOver: card.scrollWidth - card.clientWidth, out, pill: q('.egPill')?.textContent.trim() || '', why: q('.egWhy b')?.textContent || q('.top b')?.textContent || '', sub: q('.egSub')?.textContent.trim() || '',
        piece: q('.egFor')?.textContent.trim() || '', words: q('.words')?.textContent || '', unclear: [...card.querySelectorAll('.egUnclear li')].map(li => li.textContent), meta: q('.egMeta')?.textContent || '', preview: !!q('.pv canvas, .pv img'),
        approve: ab ? { text: ab.textContent.trim(), disabled: ab.disabled, live: !!q('[data-e=approve]') } : null, offWhy: q('.egOffWhy')?.textContent || '', open: op ? op.textContent.trim() : '', openW: op ? Math.round(op.getBoundingClientRect().width) : 0, seals };
    }, sel);
    const OV = '#owEng .swEng';

    // 1 · each state, as the Overview draws it (1440 px window), with its words, what is unclear and its status
    await open(keyOf(A, 0)); await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=words]'), null, { timeout: 10000 }); await settled();
    let c = await read(OV);
    check(c.state === 'words' && c.pill === 'Words to confirm' && c.why === 'The words need a decision' && c.piece.startsWith('HUGGIE HOOPS RUBBER DUCK SHAPE'), 'Words to confirm: one quiet status and the plain reason, on the piece it belongs to: ' + JSON.stringify([c.pill, c.why, c.piece]));
    check(c.words === 'Lily\n& Max' && c.unclear.length === 3 && /Check the requested inscription/.test(c.unclear[0]) && /part of the engraving/.test(c.unclear[1]) && /font script/.test(c.unclear[2]) && /52% sure/.test(c.meta), 'it shows the words as read and what is unclear: ' + JSON.stringify([c.words, c.unclear, c.meta]));
    check(c.approve && c.approve.disabled && !c.approve.live && c.approve.text === 'Approve engraving' && /Confirm the words in Engraving first/.test(c.offWhy), 'Approve engraving is disabled, with one line of why: ' + JSON.stringify([c.approve, c.offWhy]));
    check(c.open === 'Fix in Engraving →' && !c.preview, 'Fix in Engraving is there, no preview before the words are settled: ' + c.open);
    check(c.out.length === 0 && c.scrollOver <= 0, 'nothing is wider than the card at 1440: ' + JSON.stringify(c.out));
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-1440-words.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-words.png') }); }

    // 2 · the other pieces of an order of several: each piece's own card, the piece named; "All pieces" a compact card each
    await close(); await open(keyOf(B, 0)); await page.waitForFunction(() => document.querySelector('#owEng .egCompact'), null, { timeout: 10000 }); await settled();
    const list = await page.evaluate(() => [...document.querySelectorAll('#owEng .swEng')].map(e => ({ state: e.dataset.state, compact: e.classList.contains('egCompact'), piece: (e.querySelector('.egFor') || {}).textContent || '', words: (e.querySelector('.words') || {}).textContent || '' })));
    check(list.length === 5 && list.every(x => x.compact), '"All pieces": a compact card for each piece with a back engraving (' + list.map(x => x.state).join('/') + ')');
    check(list.length === 5 && list.map(x => x.state).join() === 'approve,preparing,approved,skipped,preparing' && list[0].piece.startsWith('ASTER FLOWER') && list[2].piece.startsWith('MOON DISC') && list[3].piece.startsWith('PLAIN TAG'), 'each names its piece: ' + JSON.stringify(list.map(x => x.piece.slice(0, 16))));
    check(await page.evaluate(() => document.querySelectorAll('#owEng > .fLabel').length === 1 && document.querySelector('#owEng > .fLabel').textContent === 'Back engraving'), 'under one "Back engraving" label');
    const allOut = await page.evaluate(() => { const h = document.querySelector('#owEng'), r = h.getBoundingClientRect(); return [...h.querySelectorAll('.swEng')].filter(e => { const b = e.getBoundingClientRect(); return b.right > r.right + .5 || e.scrollWidth - e.clientWidth > 0; }).length; });
    check(allOut === 0, 'none is wider than its column');
    if (shots) { await page.screenshot({ path: path.join(shots, 'overview-1440-all-pieces.png') }); await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-all-pieces.png') }); }
    await pick(keyOf(B, 0)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approve]:not(.egCompact)'), keyOf(B, 0), { timeout: 8000 }); await settled();
    c = await read(OV);
    check(c.state === 'approve' && c.pill === 'To approve' && c.why === 'Waiting for your approval' && c.words === 'KMB //\nSMH' && c.preview && c.piece.startsWith('ASTER FLOWER') && /91% sure/.test(c.meta), 'one piece picked: its own card, To approve, Waiting for your approval, words, preview, piece named: ' + JSON.stringify([c.pill, c.why, c.piece, c.meta]));
    check(c.approve && !c.approve.disabled && c.approve.live && c.approve.text === 'Approve engraving' && c.open === 'Fix in Engraving →' && c.out.length === 0, 'Approve engraving is live, Fix in Engraving is beside it: ' + JSON.stringify([c.approve, c.open]));
    if (shots) await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-approve.png') });
    await pick(keyOf(B, 1)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=preparing]:not(.egCompact)'), keyOf(B, 1), { timeout: 8000 }); await settled();
    c = await read(OV);
    check(c.pill === 'Being prepared' && c.why === 'Preview not ready yet' && /Fitting the words on the back/.test(c.sub) && c.approve.disabled && /not ready to approve/i.test(c.offWhy) && c.open === 'Fix in Engraving →' && !!(await page.evaluate(() => document.querySelector('#owEng .egSub .owSpin'))), 'Being prepared: Preview not ready yet, a small spinner with what it does, Approve engraving disabled with why: ' + JSON.stringify([c.pill, c.why, c.sub, c.offWhy]));
    if (shots) await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-preparing.png') });
    await pick(keyOf(B, 2)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approved]:not(.egCompact)'), keyOf(B, 2), { timeout: 8000 }); await settled();
    await page.waitForFunction(() => document.querySelector('#owEng .egButtonSeal .seal'), null, { timeout: 5000 }); await page.mouse.move(700, 880);
    c = await read(OV);
    check(c.pill === 'Approved' && c.why === 'The back is approved' && c.words === 'I\ndissent' && c.approve.disabled && c.approve.text === 'Approved' && c.seals.join() === 'BACK ENGRAVING|Giovanna' && c.open === 'View in Engraving →' && c.preview, 'Approved: the green Approved button with its seal and View in Engraving: ' + JSON.stringify([c.pill, c.approve, c.seals, c.open]));
    check(c.out.length === 0, 'with the seal inside the card: ' + JSON.stringify(c.out));
    if (shots) await page.locator('#owEng').screenshot({ path: path.join(shots, 'card-1440-approved.png') });
    // a seal zooms where it stands (a click), with no tooltip and no caption of its own
    // (the seal lies below the fold of the Overview at 900 px: it is brought into view and left to settle first, because a scroll puts any zoomed seal back by design
    //  and the scroll event Playwright's own scroll-into-view causes lands a frame after the click - on a slow machine after the zoom, which then reads as no zoom at all)
    await page.evaluate(() => document.querySelector('#owEng .egButtonSeal .seal').scrollIntoView({ block: 'center' })); await page.waitForTimeout(500);
    await page.click('#owEng .egButtonSeal .seal'); await page.waitForTimeout(800);
    const zoomed = await page.evaluate(() => { const s = document.querySelector('#owEng .egButtonSeal .seal'); return { cur: Seal.zoom.current === s, tip: !!(s.getAttribute('title') || s.querySelector('title')), hasTitle: s.hasAttribute('title') }; });
    check(zoomed.cur && !zoomed.tip && !zoomed.hasTitle, 'a seal on the card zooms in place and carries no tooltip: ' + JSON.stringify(zoomed));
    await page.evaluate(() => Seal.zoom.away()); await page.waitForTimeout(400);
    await pick(keyOf(B, 3)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=skipped]'), keyOf(B, 3), { timeout: 8000 }); await settled();
    c = await read(OV);
    check(c.pill === 'Cut plain' && /nothing on the back/.test(c.why) && !c.preview && c.open === 'View in Engraving →' && !c.approve && c.out.length === 0, 'Cut plain: nothing on the back, View in Engraving: ' + JSON.stringify([c.pill, c.why, c.open, c.approve]));
    await close(); await open(keyOf(A, 1)); await page.waitForFunction(() => document.querySelector('#owEng .swEng'), null, { timeout: 8000 });
    check(await page.evaluate(() => document.querySelectorAll('#owEng .swEng').length === 1 && document.querySelector('#owEng .swEng .egFor').textContent.startsWith('HUGGIE')), '"All 2 pieces" of an order with one back engraving: its one compact card, none for the piece with no back');
    await pick(keyOf(A, 1)); await page.waitForFunction(k => OrderWin.key() === k && document.getElementById('owEng').hidden, keyOf(A, 1), { timeout: 8000 });
    check(true, 'the piece with no back engraving picked: no card in the Overview (No back engraving)');
    const none = await page.evaluate(() => { const h = document.createElement('div'); h.innerHTML = CNEngravingSeals.panel({ kind: 'none' }); return h.querySelector('.swEng').dataset.state + '|' + h.querySelector('.top b').textContent; });
    check(none === 'none|No back engraving', 'and the Sheet tab and Sheet window say it quietly: ' + none);

    // 3 · the buttons: Fix in Engraving asks EngraveLink for exactly that order and piece, with a small labelled spinner
    await close(); await open(keyOf(B, 0)); await pick(keyOf(B, 0)); await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owEng .swEng[data-state=approve]:not(.egCompact)'), keyOf(B, 0), { timeout: 8000 }); await settled();
    await page.click('#owEng [data-e=engrave]');
    const mid = await page.evaluate(() => { const b = document.querySelector('#owEng [data-e=engrave]'); return { disabled: b.disabled, text: b.textContent.trim(), spin: !!b.querySelector('.spin') }; });
    check(mid.disabled && mid.spin && mid.text === 'Opening Engraving…', 'Fix in Engraving: while it opens the button says so, with a small spinner: ' + JSON.stringify(mid));
    await page.waitForFunction(() => __links.length === 1 && !document.querySelector('#owEng [data-e=engrave]').disabled, null, { timeout: 6000 });
    const l0 = await page.evaluate(() => __links[0]);
    check(JSON.stringify(l0) === JSON.stringify({ rid: B.rid, key: keyOf(B, 0), poolId: pool(B, 0), piece: keyOf(B, 0) }), 'it called EngraveLink.open({rid, key, poolId, piece}) for that piece: ' + JSON.stringify(l0));
    // Approve engraving: the name is asked as everywhere, the seal is pressed on that very button, the card reads Approved with the BACK ENGRAVING seal once
    await page.click('#owEng [data-e=approve]');
    const during = await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); return { disabled: b.disabled, text: b.textContent.trim(), spin: !!b.querySelector('.spin'), busy: b.getAttribute('aria-busy') }; });
    check(during.disabled && during.spin && during.text === 'Approving…' && during.busy === 'true', 'Approve engraving: the wait before the stamp is a small labelled spinner on the button: ' + JSON.stringify(during));
    await page.waitForFunction(() => document.querySelector('#owEng .swEng[data-state=approved]') && !document.querySelector('#owEng .seal.pending') && !Seal.busy(), null, { timeout: 20000 });
    await settled(); c = await read(OV);
    const ap = await page.evaluate(() => __approve);
    check(ap.length === 1 && ap[0].key === keyOf(B, 0) && ap[0].by === 'Test Operator' && ap[0].button, 'it ran the host\'s own approval once, for that piece, with the name and the button on screen: ' + JSON.stringify(ap));
    check(c.state === 'approved' && c.pill === 'Approved' && c.seals.join() === 'BACK ENGRAVING|Test Operator' && c.open === 'View in Engraving →' && c.out.length === 0, 'the card now reads Approved with the BACK ENGRAVING seal, once: ' + JSON.stringify([c.state, c.seals, c.open]));
    await page.evaluate(() => { const b = document.querySelector('#owEng .egApproveButton'); b.disabled = false; b.click(); }); await page.waitForTimeout(300);
    check((await page.evaluate(() => __approve.length)) === 1, 'pressing it again approves nothing');

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

    // 5 · the card in the order window at 390, 900 and 1440 px: the status and the buttons stay inside the card, the card inside its column
    for (const [vw, vh] of [[390, 844], [900, 800], [1440, 900]]) {
      await page.setViewportSize({ width: vw, height: vh });
      for (const [who, key, state, shot] of [[A, keyOf(A, 0), 'words', 'words'], [B, keyOf(B, 1), 'preparing', 'preparing'], [C, keyOf(C, 0), 'words', 'words-long']]) {
        await close(); await open(key);
        if (who === B) { await pick(key); }
        await page.waitForFunction(({ s, key }) => OrderWin.key() === key && document.querySelector(`#owEng .swEng[data-state=${s}]`), { s: state, key }, { timeout: 10000 }); await settled(); await page.waitForTimeout(250);
        // (at 390 px the window's own header, with the previous/next arrows of a list of orders, is wider than the phone and the window slides sideways when focus lands on an arrow: not this card's;
        //  put back to the left edge so the card is read where a person first sees it, and said plainly)
        const slid = await page.evaluate(() => { const o = document.getElementById('orderWin'), n = o.scrollLeft; o.scrollLeft = 0; return n; });
        if (slid) console.log(`  - ${vw} px window: the window's header is wider than the screen (the window had slid ${Math.round(slid)} px sideways); not the card's: put back before reading it`);
        const geo = await page.evaluate(() => {
          const h = document.getElementById('owEng'), card = h.querySelector('.swEng'), col = h.parentElement, r = card.getBoundingClientRect(), cr = col.getBoundingClientRect(), out = [];
          for (const n of card.querySelectorAll('*')) { if (n.closest('.seal') || !n.getClientRects().length) continue; const b = n.getBoundingClientRect(); if (b.width && (b.right > r.right + .5 || b.left < r.left - .5)) out.push(n.tagName.toLowerCase() + '.' + String(n.className).split(' ')[0]); }
          const vwid = document.documentElement.clientWidth, win = document.getElementById('orderWin').getBoundingClientRect();
          return { card: [Math.round(r.left), Math.round(r.right)], col: [Math.round(cr.left), Math.round(cr.right)], out, inCol: r.left >= cr.left - 1 && r.right <= cr.right + 1, over: card.scrollWidth - card.clientWidth, winW: Math.round(win.width), vw: vwid };
        });
        check(geo.out.length === 0 && geo.over <= 0 && geo.inCol, `${vw} px window, ${shot}: the card (${geo.card.join('..')}) is inside its column (${geo.col.join('..')}), nothing in it pokes out ${JSON.stringify(geo.out)}`);
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
