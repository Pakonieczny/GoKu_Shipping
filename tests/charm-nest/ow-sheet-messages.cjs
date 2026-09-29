// The order view's Sheet tab shows the order's messages (Paul, 29 Sep 01:04: "I don't see the message history for the
// emails or their internal messages"): under the Sheet panel, the Overview's own Team / Customer column — the same
// threads and composers, moved, never copied. Both histories are there for the order, what was typed stays across
// Overview ↔ Sheet, nothing is read twice, the plate keeps its size, and the Timeline keeps its full width.
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted, and
// the customer's inbox link is answered here (nothing is sent).
//   node tests/charm-nest/ow-sheet-messages.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOT=<png>)
const path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tid: '41762001721', sku: 'LEAF_CHARM' };
const poolOf = o => `${o.rid}_${o.tid}_1`, SHEET = 'sheet-om-1';
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  srv.st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', sheetIndex: 1, day: '2026-09-28', status: 'written', density: 0.42, stock: { wPt: 283.46, hPt: 141.73 }, orders: [A.rid, '4179999999'],
    placements: [box('pa', 60, 50), box('px', 200, 70)],
    charms: [{ id: 'pa', name: `${A.rid} · ${A.sku}`, poolId: poolOf(A), order: A.rid, sku: A.sku }, { id: 'px', name: '4179999999 · OTHER', poolId: '4179999999_41799999991_1', order: '4179999999', sku: 'OTHER' }] });
  srv.st.put('Charm_Pool', poolOf(A), { poolId: poolOf(A), orderId: A.rid, transactionId: A.tid, lineKey: `${A.rid}_${A.tid}`, sku: A.sku, material: 'gold', copy: 1, quantity: 1, state: 'placed', sheetId: SHEET, sheetName: 'GF_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = []; const check = (ok, msg) => { console.log((ok ? '  ✓ ' : '  ✗ ') + msg); if (!ok) fails.push(msg); };
  const reads = { team: [], cust: [] };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    // the customer's side, answered here: one question with its email history (nothing is ever sent from this test)
    const now = Date.now();
    const eng = { id: 'eng-om-1', receiptId: A.rid, sandbox: false, v: 1, status: 'open', unread: 0, createdAtMs: now - 3 * 3600e3, startedAtMs: now - 3 * 3600e3, customer: { name: 'Hannah Whitford' },
      messages: [{ id: 'c1', side: 'us', who: 'Paul', atMs: now - 3 * 3600e3, text: 'CUST-Q: which initial would you like on the back?', status: 'sent', delivered: true },
        { id: 'c2', side: 'customer', who: 'Hannah Whitford', atMs: now - 2 * 3600e3, text: 'CUST-A: an H please, thank you!' }] };
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => {
      let op = ''; try { op = JSON.parse(r.request().postData() || '{}').op || ''; } catch (_) {}
      reads.cust.push(op);
      const body = op === 'order' ? { ok: true, n: 1, engagements: [eng], active: eng, conversation: { customer: eng.customer } }
        : op === 'health' ? { ok: true, level: 'ok' } : op === 'history_info' ? { ok: true, total: 2, threads: [] } : { ok: true, n: 1, changes: [], v: 1 };
      return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    });
    // the team's thread (internal): two messages on A
    await context.route(/\/\.netlify\/functions\/firebaseOrders\?messagesFor=/, r => {
      const ids = decodeURIComponent(/messagesFor=([^&]*)/.exec(r.request().url())[1]).split(','); reads.team.push(ids.join(','));
      const byOrder = {}; for (const id of ids) byOrder[id] = id === A.rid ? [{ id: 'mA1', senderName: 'Welding', text: 'TEAM-1: chain swapped to 18 in', at: now - 90 * 60000 }, { id: 'mA2', senderName: 'Packing', text: 'TEAM-2: gift box added', at: now - 30 * 60000 }] : [];
      return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, byOrder }) });
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-om-test')); localStorage.setItem('cn.mail.tab', JSON.stringify('team')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.CustomerMail && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, pools }) => {
      await Orders.loadMaps(true);
      for (const [order, poolId] of orders.map((o, i) => [o, pools[i]])) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: poolId ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: poolId ? [poolId] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [order(A, 'Hannah Whitford'), order(B2, 'Ava Patel')], pools: [poolOf(A), null] });
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 5000 });
    const view = v => page.click(`.owTabsV [data-ow-view="${v}"]`).then(settled);
    const custTab = () => page.click('#owTabCust'), teamTab = () => page.click('#owTabTeam');

    // 1 · the order opens on the Overview, its two histories read once
    await page.click(`#ordItems [data-key="${A.rid}_${A.tid}"]`);
    await settled();
    await page.waitForFunction(() => /TEAM-2/.test(document.getElementById('owThread').textContent));
    await custTab(); await page.waitForFunction(() => /CUST-A/.test(document.querySelector('#owPaneCust .cmThread').textContent)); await teamTab();
    await page.waitForTimeout(600);
    const read0 = { team: reads.team.length, cust: reads.cust.filter(op => /^(order|thread|history|history_info)$/.test(op)).length };
    check(read0.team >= 1 && read0.cust >= 1, `the Overview read both histories: ${JSON.stringify(read0)}`);

    // 2 · the Sheet tab: the plate, its panel, and the same column under it, with both histories
    await view('sheet');
    await page.waitForFunction(() => OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces') && document.getElementById('owPlateWait').hidden, null, { timeout: 15000 });
    const s1 = await page.evaluate(() => {
      const side = document.getElementById('owSheetSide'), chat = document.querySelector('#orderWin .owChat'), acts = document.querySelector('#owSheetPanel .acts');
      const r = el => el.getBoundingClientRect(), sr = r(side), cr = r(chat), ar = acts ? r(acts) : null, cv = r(document.getElementById('owSheetCv')), area = r(document.querySelector('.owPlateArea'));
      return { inSide: chat.parentNode === side, chats: document.querySelectorAll('#orderWin .owChat').length, tabs: [...chat.querySelectorAll('[data-ow-tab]')].map(b => [...b.children].map(c => c.textContent.trim()).filter(Boolean).join(' ')),
        team: document.getElementById('owThread').textContent, below: ar ? cr.top >= ar.bottom : null, fills: Math.abs(cr.bottom - sr.bottom) < 2 || side.scrollHeight > side.clientHeight + 1,
        sideW: Math.round(sr.width), areaW: Math.round(area.width), plate: [Math.round(cv.width), Math.round(cv.height)], viewScroll: document.querySelector('.owVSheet').scrollHeight - document.querySelector('.owVSheet').clientHeight,
        threadScrolls: getComputedStyle(document.getElementById('owThread')).overflowY };
    });
    check(s1.inSide && s1.chats === 1, 'the Overview\'s own column sits under the Sheet panel (moved, one of it)');
    check(JSON.stringify(s1.tabs) === JSON.stringify(['Team internal', 'Customer on Etsy']), 'its two tabs, apart: ' + JSON.stringify(s1.tabs));
    check(/TEAM-1/.test(s1.team) && /TEAM-2/.test(s1.team), 'the Team (internal) history of the order');
    check(s1.below === true && s1.fills, 'under the buttons, filling the space left');
    check(s1.sideW === 400 && s1.areaW === 1440 - 400 && s1.plate[0] > 600, `the plate keeps its size (side ${s1.sideW}, plate area ${s1.areaW}, plate ${s1.plate})`);
    check(s1.viewScroll <= 1 && s1.threadScrolls === 'auto', 'it scrolls inside the panel, never the view');
    await custTab();
    const c1 = await page.evaluate(() => ({ text: document.querySelector('#owSheetSide #owPaneCust .cmThread').textContent, shown: !document.getElementById('owPaneCust').hidden && document.getElementById('owPaneTeam').hidden }));
    check(/CUST-Q/.test(c1.text) && /CUST-A/.test(c1.text) && c1.shown, 'the Customer (email / Etsy) history, in its own tab');
    if (process.env.CN_SHOT) await page.screenshot({ path: process.env.CN_SHOT });

    // 3 · what is typed stays across Overview ↔ Sheet (both composers), and nothing is read again
    await page.fill('#owPaneCust textarea[data-cm="input"]', 'typed-for-the-customer');
    await teamTab(); await page.fill('#owInput', 'typed-for-the-team');
    await view('info');
    const b1 = await page.evaluate(() => ({ home: document.querySelector('#orderWin .owChat').parentNode.classList.contains('owVInfo'), team: document.getElementById('owInput').value, cust: document.querySelector('#owPaneCust textarea[data-cm="input"]').value }));
    check(b1.home && b1.team === 'typed-for-the-team' && b1.cust === 'typed-for-the-customer', 'back on the Overview: the column is there, with both drafts ' + JSON.stringify(b1));
    await view('sheet');
    const b2 = await page.evaluate(() => ({ team: document.getElementById('owInput').value, cust: document.querySelector('#owPaneCust textarea[data-cm="input"]').value, send: document.getElementById('owSend').disabled }));
    check(b2.team === 'typed-for-the-team' && b2.cust === 'typed-for-the-customer' && b2.send === false, 'and on the Sheet again ' + JSON.stringify(b2));
    await page.waitForTimeout(500);
    const read1 = { team: reads.team.length, cust: reads.cust.filter(op => /^(order|thread|history|history_info)$/.test(op)).length };
    check(read1.team === read0.team && read1.cust === read0.cust, `nothing is read a second time: ${JSON.stringify(read0)} → ${JSON.stringify(read1)}`);

    // 4 · the Timeline keeps its full width (no room for the column there)
    await view('timeline');
    const t1 = await page.evaluate(() => ({ w: Math.round(document.querySelector('.owVTime').getBoundingClientRect().width), chat: !!document.querySelector('.owVTime .owChat') }));
    check(t1.w === 1440 && !t1.chat, 'the Timeline is left as it is, full width');
    await view('info');
    check(await page.evaluate(() => document.getElementById('owInput').value === 'typed-for-the-team'), 'the draft is still there after the Timeline');
    check(!errors.length, 'no page errors' + (errors.length ? ': ' + errors.join(' | ') : ''));
  } finally { await browser.close(); await srv.close(); }
  if (fails.length) { console.error(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nok');
}
main().catch(e => { console.error(e); process.exit(1); });
