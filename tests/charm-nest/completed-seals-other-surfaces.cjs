// A completed piece, sheet or order shows its seal wherever the app says "completed" (Paul, 5 Oct 2026: "The 'Complete' orders/pieces are missing
// their associated stamp/seal. Please review all modals and fix."). This test covers the surfaces outside the order window's Overview, Review and
// Orders (those have their own tests): the sheet window's piece list and its header, the order window's Sheet tab (its piece list, and the
// "Completed by hand" place where a sheet would be) and the order search card. Each shows the ONE shared small seal (Seal.compact: the press that
// completed it, "+N" for the others), drawn once, never cut off at 900 and 390 px; a piece or sheet with nothing recorded shows none; and two
// mutants (the seal dropped, the seal drawn twice) are caught by the very same checks.
// Runs in headless Chromium against the local fake site (bridge-server.cjs), offline; nothing is written anywhere live.
//   SHOTS=<dir> node tests/charm-nest/completed-seals-other-surfaces.cjs [playwright-core dir]
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 9, 17) / 1000), H = 36e5;
const RID = '4171711853', RID2 = '4171711999', RID3 = '4171711777';
const TX = { A: '41717118531', B: '41717118532', C: '41717118533', D: '41717119991', E: '41717117771' };
const KEY = { A: `${RID}_${TX.A}`, B: `${RID}_${TX.B}`, C: `${RID}_${TX.C}`, D: `${RID2}_${TX.D}`, E: `${RID3}_${TX.E}` };
const line = (tx, sku, title, qty) => ({ transactionId: tx, listingId: '18000' + tx.slice(-4), sku, title, quantity: qty || 2, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold' }, { name: 'Length', value: '17 Inches' }], metalKey: '', metalLabel: '', personalization: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 6 * DAY, updateTs: SHIP - 6 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  order(RID, 'Carolyn Schmidt', [line(TX.A, 'SPORTS_10_FIGURE_SKATE', 'SPORTS 10 - FIGURE SKATE'), line(TX.B, 'SNAKE_5', 'SNAKE 5'), line(TX.C, 'FOOTBALL', 'FOOTBALL')]),
  order(RID2, 'Dana Lopez', [line(TX.D, 'CHAIN_8941', 'CHAIN REPLACEMENT', 1)]),
  order(RID3, 'Erin Walsh', [line(TX.E, 'HEART_3', 'HEART 3', 1)])];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] }), { st } = srv;
  const T1 = Date.now() - 6 * H, T2 = T1 + 2 * H, T3 = T1 + 1.5 * H, TD = T1 + 3 * H, TL = Date.now() - 5 * H;
  // A: Complete Order press, then a QR label print (stamps)   C: completed by hand, an older record with no stamps   D: one press (its own order)
  st.put('Charm_Custom_Orders', KEY.A, { key: KEY.A, receiptId: RID, transactionId: TX.A, sku: 'SPORTS_10_FIGURE_SKATE', title: 'SPORTS 10 - FIGURE SKATE', state: 'completed', how: 'button', completedAt: T1, completedBy: 'Paul', printedAt: T2, printedBy: 'Paul', lastPrintedAt: T2, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, updatedAtMs: T2, stamps: [{ how: 'button', at: T1, by: 'Paul' }, { how: 'print', at: T2, by: 'Paul' }] }, false);
  st.put('Charm_Custom_Orders', KEY.C, { key: KEY.C, receiptId: RID, transactionId: TX.C, sku: 'FOOTBALL', title: 'FOOTBALL', state: 'completed', how: 'button', completedAt: T3, completedBy: 'Paul', hasLabel: false, updatedAtMs: T3 }, false);
  st.put('Charm_Custom_Orders', KEY.D, { key: KEY.D, receiptId: RID2, transactionId: TX.D, sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', state: 'completed', how: 'button', completedAt: TD, completedBy: 'Paul', hasLabel: false, updatedAtMs: TD, stamps: [{ how: 'button', at: TD, by: 'Paul' }] }, false);
  const pid = (k, n) => `${k}_${n}`;
  // sheet 1: piece B, laser cut done (a sheet that was cut)   sheet 2: order 3's piece, not cut (nothing recorded: no seal)
  st.put('Charm_Nest_Sheets', 'rg-sheet-1', { id: 'rg-sheet-1', metal: 'rose', metalLabel: 'RG', sheetIndex: 1, charms: [1, 2].map(n => ({ id: 'c' + n, poolId: pid(KEY.B, n), order: RID })), placements: [1, 2].map(n => ({ id: 'c' + n, cxPt: 30 + n * 40, cyPt: 40, angle: 0 })), poolIds: [pid(KEY.B, 1), pid(KEY.B, 2)], orders: [RID], status: 'complete', runId: 'run-x', laserDoneAt: TL, laserDoneBy: 'Paul' });
  st.put('Charm_Nest_Sheets', 'rg-sheet-2', { id: 'rg-sheet-2', metal: 'rose', metalLabel: 'RG', sheetIndex: 2, charms: [{ id: 'e1', poolId: pid(KEY.E, 1), order: RID3 }], placements: [{ id: 'e1', cxPt: 50, cyPt: 40, angle: 0 }], poolIds: [pid(KEY.E, 1)], orders: [RID3], status: 'complete', runId: 'run-x' });
  for (const n of [1, 2]) st.put('Charm_Pool', pid(KEY.B, n), { poolId: pid(KEY.B, n), orderId: RID, sheetId: 'rg-sheet-1', state: 'placed', material: 'rose' });
  st.put('Charm_Pool', pid(KEY.E, 1), { poolId: pid(KEY.E, 1), orderId: RID3, sheetId: 'rg-sheet-2', state: 'placed', material: 'rose' });
  const T0 = Date.now() - 6 * 24 * H, ev = (type, at, x) => Object.assign({ id: `${RID}~${type}~${at}`, orderId: RID, type, at, by: 'Paul', source: 'sorter' }, x || {});
  const evs = Timeline.chronology([ev('arrived', T0, { source: 'etsy', by: 'Etsy' }),
    ev('sealCompleted', T1, { lineKey: KEY.A, data: { how: 'button' }, text: 'Custom order completed' }), ev('sealPrinted', T2, { lineKey: KEY.A, data: { how: 'print', prints: 1 }, text: 'Custom QR label printed' }),
    ev('sealCompleted', T3, { lineKey: KEY.C, data: { how: 'button' }, text: 'Custom order completed' })]).sort(Timeline.byTime);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 900, height: 820 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && window.SheetWin && window.OrderSearch && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, evs, where, KEY, rids }) => {
      const placed = new Set([KEY.B, KEY.E]);
      for (const o of orders) for (const ln of o.lines) {
        const key = CharmNestOrders.lineKey(o, ln), on = placed.has(key);
        const row = { key, order: o, line: ln, arrivedAt: Date.now(), spec: null, problems: [], state: on ? 'placed' : 'pulled', reason: null, claimedBy: null, poolIds: on ? [1, 2].slice(0, ln.quantity).map(n => `${key}_${n}`) : [], engrave: null, material: 'rose' };
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      }
      for (const [k, sheet, n] of [[KEY.B, 'rg-sheet-1', 1], [KEY.B, 'rg-sheet-1', 2], [KEY.E, 'rg-sheet-2', 1]]) B.pool.rows.set(`${k}_${n}`, { poolId: `${k}_${n}`, orderId: k.split('_')[0], sheetId: sheet, state: 'placed', material: 'rose' });
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const wait = window.setTimeout.bind(window);
      window.__evs = evs; OrderTimeline.get = async rid => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify({ id: String(rid), events: String(rid) === rids[0] ? window.__evs : [], cancelled: null, where: String(rid) === rids[0] ? where : null })); };
    }, { orders: ORDERS, evs, where: Timeline.whereOf(evs, null, { record: true }), KEY, rids: [RID] });

    const settle = ms => page.evaluate(m => new Promise(r => setTimeout(r, m)), ms);
    const closeAll = async () => {
      await page.evaluate(() => { try { OrderSearch.close(); } catch (_) {} try { SheetWin.close(true); } catch (_) {} try { OrderWin.close(); } catch (_) {} });
      await page.waitForFunction(() => !SheetWin.isOpen() && !document.getElementById('orderWin').open && !OrderSearch.isOpen(), null, { timeout: 8000 });
    };
    /** the sheet window opened on sheet 1 (piece B chosen): the trail's hand rows, and the header beside "Completed" */
    const sheetWin = async id => {
      await page.evaluate(({ i, pool }) => SheetWin.open(i, { select: pool }), { i: id, pool: id === 'rg-sheet-1' ? pid(KEY.B, 1) : pid(KEY.E, 1) });
      await page.waitForFunction(() => document.querySelector('.swTrail li') && document.querySelector('.swState'), null, { timeout: 20000 });
      await settle(2500);
      return page.evaluate(() => {
        const sealsOf = root => [...root.querySelectorAll('.seal')].filter(s => s.offsetParent).map(s => { const m = s.querySelector('svg[data-seal-model]'); let say = ''; try { const j = JSON.parse(m.getAttribute('data-seal-model')); say = j.action + ' · ' + j.date + ' ' + j.time; } catch (_) {} const r = s.getBoundingClientRect(); return { say, kind: [...s.classList].find(c => /^seal-/.test(c)) || '', at: +s.dataset.at || 0, l: r.left, r: r.right, t: r.top, b: r.bottom, w: r.width }; });
        const rows = [...document.querySelectorAll('.swTrail li')].map(li => { const r = li.getBoundingClientRect(), more = li.querySelector('.sealPlus'); return { text: li.textContent.replace(/\s+/g, ' ').trim().slice(0, 70), hand: /completed by hand/.test(li.textContent), seals: sealsOf(li), more: more ? more.textContent.trim() : '', l: r.left, r: r.right, t: r.top, b: r.bottom, over: li.scrollWidth - li.clientWidth }; });
        const hd = document.querySelector('.swStateSeal'), head = hd && hd.closest('header,.swHead,.swHd,.swTop') || (hd && hd.parentElement), hr = head && head.getBoundingClientRect();
        const dlg = document.querySelector('dialog[open]') || document.body;
        return { rows, header: hd ? sealsOf(hd) : [], headerShown: !!(hd && hd.offsetParent), state: (document.querySelector('.swState') || {}).textContent || '', headBox: hr ? { l: hr.left, r: hr.right, over: head.scrollWidth - head.clientWidth } : null, page: { sw: document.documentElement.scrollWidth, iw: innerWidth }, dlg: (() => { const r = dlg.getBoundingClientRect(); return { l: r.left, r: r.right }; })() };
      });
    };
    const verdictSheet = (m, w, label, expect) => {
      const out = [];
      const hand = m.rows.filter(r => r.hand);
      if (expect.handRows != null && hand.length !== expect.handRows) out.push(`${label}: ${hand.length} rows say completed by hand, expected ${expect.handRows}`);
      for (const r of hand) {
        if (r.seals.length !== 1) out.push(`${label}: a completed-by-hand row shows ${r.seals.length} seals (one, the completion): ${r.text}`);
      }
      const A = hand.filter(r => /SPORTS/.test(r.text)), C = hand.filter(r => /FOOTBALL/.test(r.text));
      if (expect.handRows) {
        if (!A.every(r => r.seals.length === 1 && r.seals[0].kind === 'seal-button' && Math.abs(r.seals[0].at - T1) < 1500 && r.more === '+1')) out.push(`${label}: the SPORTS rows show their ORDER COMPLETE seal (the press that completed them) and +1 for the QR label: ${JSON.stringify(A.map(r => [r.seals.map(s => s.kind + '@' + s.at), r.more]))}`);
        if (!C.every(r => r.seals.length === 1 && Math.abs(r.seals[0].at - T3) < 1500 && r.more === '')) out.push(`${label}: the FOOTBALL rows (an older record, no stamps) show their one completion seal: ${JSON.stringify(C.map(r => [r.seals.map(s => s.kind + '@' + s.at), r.more]))}`);
      }
      for (const r of m.rows.filter(r => !r.hand)) if (r.seals.length) out.push(`${label}: a piece that was not completed by hand shows a seal: ${r.text}`);
      for (const r of hand) for (const s of r.seals) { if (s.w < 16 || s.w > 34) out.push(`${label}: a trail seal is ${Math.round(s.w)}px wide (small: 18 to 30)`); if (s.l < r.l - 1 || s.r > r.r + 1 || s.t < r.t - 1 || s.b > r.b + 1) out.push(`${label}: a trail seal leaves its row`); }
      for (const r of m.rows) if (r.over > 1) out.push(`${label}: a trail row scrolls sideways by ${r.over}px at ${w}px`);
      if (m.page.sw > m.page.iw + 1) out.push(`${label}: the page scrolls sideways at ${w}px`);
      if (expect.header === 1) {
        if (m.header.length !== 1 || !/LASER CUT/i.test(m.header[0].say)) out.push(`${label}: the header of a cut sheet shows its one LASER CUT seal beside Completed: ${JSON.stringify(m.header)}`);
        else if (m.headBox && (m.header[0].r > m.headBox.r + 1 || m.header[0].l < m.headBox.l - 1 || m.headBox.over > 1)) out.push(`${label}: the header seal does not fit its bar at ${w}px: ${JSON.stringify({ seal: [m.header[0].l, m.header[0].r], head: m.headBox })}`);
        else if (m.header[0].r > w + 1 || m.header[0].l < 0) out.push(`${label}: the header seal is off the screen at ${w}px`);
      } else if (m.header.length || m.headerShown) out.push(`${label}: a sheet that was not cut shows a seal in its header (nothing recorded, none made up): ${JSON.stringify(m.header)}`);
      return out;
    };

    /** the order window's Sheet tab on piece B: its piece list (the hand rows' seals) */
    const orderWinSheet = async () => {
      await page.evaluate(k => OrderWin.open(k), KEY.B);
      await page.waitForFunction(() => document.querySelector('#orderWin [data-ow-view="sheet"]'), null, { timeout: 15000 });
      await settle(2500);
      await page.evaluate(() => document.querySelector('#orderWin [data-ow-view="sheet"]').click());
      await page.waitForFunction(() => document.querySelectorAll('#orderWin .owPieces li').length >= 6, null, { timeout: 15000 });
      await settle(1500);
      return page.evaluate(() => {
        const sealsOf = root => [...root.querySelectorAll('.seal')].filter(s => s.offsetParent).map(s => { const m = s.querySelector('svg[data-seal-model]'); let say = ''; try { const j = JSON.parse(m.getAttribute('data-seal-model')); say = j.action + ' · ' + j.date + ' ' + j.time; } catch (_) {} const r = s.getBoundingClientRect(); return { say, kind: [...s.classList].find(c => /^seal-/.test(c)) || '', at: +s.dataset.at || 0, l: r.left, r: r.right, t: r.top, b: r.bottom, w: r.width }; });
        const ul = document.querySelector('#orderWin .owPieces'), ur = ul.getBoundingClientRect();
        const rows = [...ul.querySelectorAll('li[data-i]')].map(li => { const r = li.getBoundingClientRect(), more = li.querySelector('.sealPlus'); return { text: li.textContent.replace(/\s+/g, ' ').trim().slice(0, 70), hand: /completed by hand/.test(li.textContent), seals: sealsOf(li), more: more ? more.textContent.trim() : '', l: r.left, r: r.right, t: r.top, b: r.bottom, over: li.scrollWidth - li.clientWidth }; });
        return { rows, list: { l: ur.left, r: ur.right }, page: { sw: document.documentElement.scrollWidth, iw: innerWidth } };
      });
    };

    /** the place where a sheet would be, for a piece completed by hand (order 2: no sheet at all), reached from the Sheet tab of another order */
    const placeholder = async () => {
      // the Sheet view stays open as the person steps on; the place for a piece with no sheet draws the Completed by hand word
      let found = null;
      for (let i = 0; i < 6 && !found; i++) {
        await page.evaluate(() => document.getElementById('owNext') && !document.getElementById('owNext').disabled && document.getElementById('owNext').click());
        await settle(1800);
        found = await page.evaluate(() => { const n = document.querySelector('#orderWin .owPlateNone[data-none]'); return n && /Completed by hand/.test(n.textContent) ? true : null; });
      }
      if (!found) return null;
      return page.evaluate(() => {
        const sealsOf = root => [...root.querySelectorAll('.seal')].filter(s => s.offsetParent).map(s => { const m = s.querySelector('svg[data-seal-model]'); let say = ''; try { const j = JSON.parse(m.getAttribute('data-seal-model')); say = j.action + ' · ' + j.date + ' ' + j.time; } catch (_) {} const r = s.getBoundingClientRect(); return { say, kind: [...s.classList].find(c => /^seal-/.test(c)) || '', at: +s.dataset.at || 0, l: r.left, r: r.right, t: r.top, b: r.bottom, w: r.width }; });
        const n = document.querySelector('#orderWin .owPlateNone[data-none]'), r = n.getBoundingClientRect(), wrap = document.querySelector('#orderWin #owPlateWrap').getBoundingClientRect();
        return { seals: sealsOf(n), box: { l: r.left, r: r.right, t: r.top, b: r.bottom }, wrap: { l: wrap.left, r: wrap.right }, page: { sw: document.documentElement.scrollWidth, iw: innerWidth } };
      });
    };

    /** the order search card of order 2 (completed by hand: Completed by hand · Paul), and of order 1 (a decision waits: nothing completed, no seal) */
    const search = async () => {
      const one = async rid => {
        await page.evaluate(r => OrderSearch.open(r), rid);
        await page.waitForFunction(() => document.querySelector('.cnsCard'), null, { timeout: 15000 });
        await settle(1200);
        return page.evaluate(() => {
          const sealsOf = root => [...root.querySelectorAll('.seal')].filter(s => s.offsetParent).map(s => { const m = s.querySelector('svg[data-seal-model]'); let say = ''; try { const j = JSON.parse(m.getAttribute('data-seal-model')); say = j.action + ' · ' + j.date + ' ' + j.time; } catch (_) {} const r = s.getBoundingClientRect(); return { say, kind: [...s.classList].find(c => /^seal-/.test(c)) || '', at: +s.dataset.at || 0, l: r.left, r: r.right, t: r.top, b: r.bottom, w: r.width }; });
          const c = document.querySelector('.cnsCard'), meta = c.querySelector('.cnsMeta'), cr = c.getBoundingClientRect();
          return { now: (c.querySelector('.cnsNow') || {}).textContent || '', seals: sealsOf(c), card: { l: cr.left, r: cr.right, t: cr.top, b: cr.bottom }, over: meta.scrollWidth - meta.clientWidth, page: { sw: document.documentElement.scrollWidth, iw: innerWidth } };
        });
      };
      const done = await one(RID2); await closeAll();
      const open = await one(RID); await closeAll();
      return { done, open };
    };
    const verdictOne = (list, w, label, expect) => {   // a list of rows with `hand`, `seals`
      const out = [];
      const hand = list.rows.filter(r => r.hand);
      if (hand.length !== expect.handRows) out.push(`${label}: ${hand.length} rows say completed by hand, expected ${expect.handRows}`);
      for (const r of hand) {
        if (r.seals.length !== 1) out.push(`${label}: a completed-by-hand row shows ${r.seals.length} seals (one, the completion): ${r.text}`);
        for (const s of r.seals) { if (s.w < 16 || s.w > 34) out.push(`${label}: a seal is ${Math.round(s.w)}px wide (small: 18 to 30)`); if (s.l < r.l - 1 || s.r > r.r + 1 || s.t < r.t - 1 || s.b > r.b + 1) out.push(`${label}: a seal leaves its row`); }
      }
      const A = hand.filter(r => /SPORTS/.test(r.text)), C = hand.filter(r => /FOOTBALL/.test(r.text));
      if (!A.every(r => r.seals.length === 1 && r.seals[0].kind === 'seal-button' && Math.abs(r.seals[0].at - T1) < 1500 && r.more === '+1')) out.push(`${label}: the SPORTS rows show their ORDER COMPLETE seal (the press that completed them) and +1 for the QR label`);
      if (!C.every(r => r.seals.length === 1 && Math.abs(r.seals[0].at - T3) < 1500 && r.more === '')) out.push(`${label}: the FOOTBALL rows show their one completion seal`);
      for (const r of list.rows.filter(r => !r.hand)) if (r.seals.length) out.push(`${label}: a piece that was not completed by hand shows a seal: ${r.text}`);
      for (const r of list.rows) if (r.over > 1) out.push(`${label}: a row scrolls sideways by ${r.over}px at ${w}px`);
      if (list.page.sw > list.page.iw + 1) out.push(`${label}: the page scrolls sideways at ${w}px`);
      return out;
    };
    const verdictPlace = (m, w, label) => {
      const out = [];
      if (!m) return [`${label}: the place for a piece completed by hand was not reached`];
      if (m.seals.length !== 1 || !/ORDER COMPLETE/i.test(m.seals[0].say)) out.push(`${label}: "Completed by hand" shows its one ORDER COMPLETE seal: ${JSON.stringify(m.seals.map(s => s.say))}`);
      else {
        const s = m.seals[0];
        if (s.w < 16 || s.w > 34) out.push(`${label}: its seal is ${Math.round(s.w)}px wide (small)`);
        // (the order window itself is wider than a 390px screen without any seal, and sits scrolled: the seal is held to its own place, not to the screen)
        if (s.l < m.box.l - 1 || s.r > m.box.r + 1) out.push(`${label}: its seal does not fit at ${w}px: ${JSON.stringify({ seal: [s.l, s.r], box: m.box, wrap: m.wrap })}`);
      }
      if (m.page.sw > m.page.iw + 1) out.push(`${label}: the page scrolls sideways at ${w}px`);
      return out;
    };
    const verdictSearch = (m, w, label) => {
      const out = [];
      if (!/Completed by hand/.test(m.done.now)) out.push(`${label}: the card of the order completed by hand does not say so: "${m.done.now}"`);
      if (m.done.seals.length !== 1 || !/ORDER COMPLETE/i.test(m.done.seals[0].say) || Math.abs(m.done.seals[0].at - TD) > 1500) out.push(`${label}: the card of an order completed by hand shows its one completion seal: ${JSON.stringify(m.done.seals.map(s => s.say))}`);
      else { const s = m.done.seals[0]; if (s.w < 16 || s.w > 34) out.push(`${label}: its seal is ${Math.round(s.w)}px wide (small)`); if (s.l < m.done.card.l || s.r > m.done.card.r + 1 || s.r > w + 1) out.push(`${label}: its seal does not fit the card at ${w}px`); }
      if (m.done.over > 1) out.push(`${label}: the card's line scrolls sideways at ${w}px`);
      if (m.open.seals.length) out.push(`${label}: an order still waiting for a decision shows a seal (it is not completed): ${JSON.stringify(m.open.seals.map(s => s.say))}`);
      if (m.done.page.sw > m.done.page.iw + 1) out.push(`${label}: the page scrolls sideways at ${w}px`);
      return out;
    };

    // everything at once: each surface's measure and verdict, for one width
    const everything = async (w, tag) => {
      const out = [], shot = n => shots && page.screenshot({ path: path.join(shots, `${tag}-${n}.png`) });
      await closeAll();
      let m = await sheetWin('rg-sheet-1'); await shot('sheet-window');
      out.push(...verdictSheet(m, w, 'sheet window (cut sheet)', { handRows: 4, header: 1 }));
      await closeAll();
      m = await sheetWin('rg-sheet-2'); await shot('sheet-window-uncut');
      out.push(...verdictSheet(m, w, 'sheet window (sheet not cut)', { handRows: 0, header: 0 }));
      await closeAll();
      m = await orderWinSheet(); await shot('order-sheet-tab');
      out.push(...verdictOne(m, w, 'order window, Sheet tab', { handRows: 4 }));
      const ph = await placeholder(); await shot('order-sheet-none');
      out.push(...verdictPlace(ph, w, 'order window, Sheet tab, completed by hand'));
      await closeAll();
      const s = await search(); await page.evaluate(r => OrderSearch.open(r), RID2); await page.waitForFunction(() => document.querySelector('.cnsCard')); await settle(900); await shot('search'); await closeAll();
      out.push(...verdictSearch(s, w, 'order search card'));
      await closeAll();
      return out;
    };

    for (const w of [900, 390].filter(w => !process.env.ONLY || +process.env.ONLY === w)) {
      await page.setViewportSize({ width: w, height: w === 900 ? 820 : 800 });
      await settle(400);
      const bad = await everything(w, `w${w}`);
      check(bad.length === 0, `every completed piece, sheet and order shows its seal, once, small and whole at ${w}px` + (bad.length ? ': ' + bad.join(' | ') : ''));
    }

    // the press itself, in the sheet window (the page's own LibraryDone.mark answers as the server does, nothing is sent): Mark completed brings the header seal,
    // Move back to current takes the status back and the seal stays (a seal is permanent)
    await closeAll();
    await page.evaluate(pool => {
      window.__marks = 0; LibraryDone.canComplete = () => true;   // (a sheet the Library lets be completed)
      LibraryDone.mark = async (kind, id, done) => { __marks++; const at = window.__markAt = window.__markAt || Date.now(); return { kind, id, done, at, process: [{ kind: 'sheet', id, patch: { processSeals: [{ id: 'sealed-1', how: 'laserDone', at, by: 'Paul' }], processReady: true, laserDoneAt: done ? at : null, laserDoneBy: done ? 'Paul' : null } }] }; };
      SheetWin.open('rg-sheet-2', { select: pool });
    }, pid(KEY.E, 1));
    await page.waitForFunction(() => document.querySelector('.swTrail li') && document.querySelector('[data-r=done]') && !document.querySelector('[data-r=done]').hidden, null, { timeout: 20000 });
    await settle(2000);
    const hdr = () => page.evaluate(() => ({ seals: [...document.querySelectorAll('.swStateSeal .seal')].filter(x => x.offsetParent).length, state: (document.querySelector('.swState') || {}).textContent.trim(), btn: document.querySelector('[data-r=done]').textContent.trim() }));
    const h0 = await hdr();
    check(h0.seals === 0 && /Mark completed/i.test(h0.btn), 'a sheet not yet cut: no seal, and the button says Mark completed: ' + JSON.stringify(h0));
    await page.evaluate(() => document.querySelector('[data-r=done]').click());
    await page.waitForFunction(() => document.querySelectorAll('.swStateSeal .seal').length === 1, null, { timeout: 8000 }).catch(() => {});
    await settle(900);
    const h1 = await hdr();
    check(h1.seals === 1 && /completed/i.test(h1.state), 'Mark completed: the header shows the sheet\'s one LASER CUT seal beside Completed: ' + JSON.stringify(h1));
    if (shots) await page.screenshot({ path: path.join(shots, 'w900-sheet-window-marked.png') });
    await page.waitForFunction(() => !document.querySelector('[data-r=done]').disabled, null, { timeout: 8000 }).catch(() => {});
    await page.evaluate(() => document.querySelector('[data-r=done]').click());
    await page.waitForFunction(() => window.__marks >= 2 && !/completed$/i.test((document.querySelector('.swState') || {}).textContent.trim()), null, { timeout: 8000 }).catch(() => {});
    await settle(900);
    const h2 = await hdr();
    check(h2.seals === 1 && !/^completed$/i.test(h2.state), 'Move back to current: the status goes back and the seal stays, one (a seal is permanent): ' + JSON.stringify(h2));
    await closeAll();

    // mutants, at 900px: the same checks must fail on each surface
    await page.setViewportSize({ width: 900, height: 820 }); await settle(300);
    const mutate = (name, fn) => page.evaluate(`(() => { const S = window.Seal; if (!S.__orig) S.__orig = { compact: S.compact, compactRow: S.compactRow }; S.compact = S.__orig.compact; S.compactRow = S.__orig.compactRow; ${fn} })()`).then(() => name);
    const surfacesOf = bad => ['sheet window (cut sheet)', 'sheet window (sheet not cut)', 'order window, Sheet tab,', 'order window, Sheet tab, completed by hand', 'order search card'].filter(p => bad.some(b => b.startsWith(p)));
    const drop = await mutate('seal dropped', 'S.compact = () => ""; S.compactRow = () => "";');
    let bad = await everything(900, 'mutant-drop');
    const caughtDrop = surfacesOf(bad);
    check(bad.length > 0 && ['sheet window (cut sheet)', 'order window, Sheet tab,', 'order window, Sheet tab, completed by hand', 'order search card'].every(p => caughtDrop.includes(p)), `mutant caught (${drop}): the checks fail on the sheet window, the Sheet tab list, its completed-by-hand place and the search card (${caughtDrop.length} surfaces: ${caughtDrop.join(' ; ')})`);
    const twice = await mutate('seal drawn twice', 'const c = S.compact, r = S.compactRow; S.compact = function () { const h = c.apply(this, arguments); return h + h; }; S.compactRow = function () { const h = r.apply(this, arguments); return h + h; };');
    bad = await everything(900, 'mutant-twice');
    const caughtTwice = surfacesOf(bad);
    check(bad.length > 0 && ['sheet window (cut sheet)', 'order window, Sheet tab,', 'order window, Sheet tab, completed by hand', 'order search card'].every(p => caughtTwice.includes(p)), `mutant caught (${twice}): drawn twice is seen on every surface (${caughtTwice.length} surfaces)`);
    await mutate('restored', '');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('completed-seals-other-surfaces: ok');
})().catch(e => { console.error(e); process.exit(1); });
