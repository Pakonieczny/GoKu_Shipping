// Adversarial design pass on the order view (plans/design/order-view-spec.md), at 1280 × 800, the narrowest width the
// spec lays out: nothing runs past its column or the screen, the chrome is one header and one tab row, and a cancelled
// order says so without covering the header's step names.
//   - the team composer (paperclip, box, send, hint) stays inside its 380px column; the send button is not cut off
//   - the Timeline's filters sit in the tab row and its own Now + rail strip is not drawn under the header's rail
//   - a day column's head ("10:05 AM – 1:12 PM · 3") never runs into the next day
//   - the header rail's step names are 8.5px, and a cancelled order's big stamp is not drawn over them
//   - "Where it is now" shows the station's icon in its badge disc; a line of a 2-line order reads "line 1 of 2"
//   - the field grid does not wrap a date; "Not on a sheet yet" sits in the middle of the plate area
// Loopback only (the fake site, bridge-server.cjs); OrderTimeline.get answers from a fixture.
//   node tests/charm-nest/adv-design.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const out = process.env.CN_SHOTS || null, widths = [1280];
const { start } = require('./bridge-server.cjs');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const only = null;

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' };
const D = { rid: '4176200172', tid: '41762001721', sku: 'LEAF_CHARM', tid2: '41762001722', sku2: 'ASTER_FLOWER' };
const CX = { rid: '4175011873', tid: '41750118731', sku: 'MONSTERA_LEAF' };
const CU = { rid: '4174476673', tid: '41744766731', sku: 'CUSTOM_6673' };
const poolOf = (rid, tid) => `${rid}_${tid}_1`;
const line = (tid, sku, title, extra) => Object.assign({ transactionId: tid, listingId: '1800000' + tid.slice(-3), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }, extra || {});
const order = (rid, buyer, lines, extra) => Object.assign({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }, extra || {});

function fixture({ MAIN, CX, D }) {
  const day0 = new Date(); day0.setHours(0, 0, 0, 0); day0.setDate(day0.getDate() - 5);
  const T = (d, hm) => { const [h, m] = hm.split(':').map(Number); const t = new Date(day0); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return +t; };
  let n = 0;
  const ev = (orderId, d, hm, type, by, extra) => Object.assign({ id: `${orderId}~${type}~e${++n}`, orderId, at: T(d, hm), type, by, source: 'sorter', station: '', device: '', text: '' }, extra || {});
  const mk = id => { const M = (d, hm, type, by, extra) => ev(id, d, hm, type, by, extra); return [
    M(0, '14:39', 'arrived', 'Etsy', { source: 'etsy', text: 'Order arrived from Etsy' }),
    M(0, '14:40', 'pulled', '', { source: 'system', text: 'Pulled into the day’s run' }),
    M(0, '14:40', 'interpreted', '', { source: 'system', text: 'Read: 14K Tiny Initial Tag, back engraving' }),
    M(0, '14:41', 'placed', '', { source: 'system', sheet: 'GF Sheet 2', sheetId: 'sheet-ov-1', text: 'Tiny Initial Tag placed on GF Sheet 2' }),
    M(0, '15:02', 'engraveNeeded', '', { source: 'system', text: 'Back engraving: Love, Mom · 2026' }),
    M(0, '16:05', 'engraveApproved', 'Giovanna C.', { text: 'Back engraving approved', data: { words: 'Love, Mom · 2026', height: '1.1 mm' } }),
    M(0, '16:07', 'held', 'Giovanna C.', { text: 'Held — waiting on the buyer', data: { reason: 'The note could read H or K.' } }),
    M(0, '16:08', 'teamMessage', 'Giovanna C.', { text: 'Asked the buyer: is the initial H?' }),
    M(1, '08:52', 'customerMessage', 'Hannah W.', { source: 'etsy', text: '“Yes, H please! Thank you.”' }),
    M(1, '09:10', 'released', 'Giovanna C.', { text: 'Released — initial H confirmed' }),
    M(1, '09:22', 'decided', 'Paul', { text: '14K Solid Gold approved' }),
    M(1, '13:15', 'roseLine', 'Paul', { sheet: 'RG Sheet 7', sheetId: 'sh-rg7', text: 'RG green line approved' }),
    M(1, '15:40', 'included', '', { source: 'system', setId: 'set-212', text: 'Included in Set 212' }),
    M(1, '15:42', 'qrLabel', 'Paul', { setId: 'set-212', text: 'QR label made for Set 212' }),
    M(1, '15:44', 'setCommitted', 'Paul', { setId: 'set-212', text: 'Set 212 committed' }),
    M(2, '10:05', 'laserDone', 'Marco R.', { station: 'laser', sheet: 'GF Sheet 2', sheetId: 'sheet-ov-1', text: 'GF Sheet 2 laser cut' }),
    M(2, '13:10', 'scan', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Scanned at Sorting' }),
    M(2, '13:12', 'sorted', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Both pieces sorted to the order' }),
    M(3, '09:40', 'scan', 'Marco R.', { source: 'station', station: 'welding', device: 'weld-1', text: 'Scanned at Welding' }),
    M(3, '10:15', 'welded', 'Marco R.', { source: 'station', station: 'welding', device: 'weld-1', text: 'Jump rings closed' }),
    M(3, '14:05', 'scan', 'Luisa T.', { source: 'station', station: 'assembly', device: 'assembly-2', text: 'Scanned at Assembly' }),
    M(3, '14:48', 'assembled', 'Luisa T.', { source: 'station', station: 'assembly', device: 'assembly-2', text: 'Assembled on 18″ cable chain' }),
    M(5, '09:05', 'scan', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'Scanned at Shipping' }),
    M(5, '09:20', 'packed', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'Packed in a gift box' }),
    M(5, '09:22', 'labelPrinted', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'USPS label printed', data: { tracking: '9400 1112 0206 5512 3345 67' } })
  ]; };
  const C = (d, hm, type, by, extra) => ev(CX, d, hm, type, by, extra);
  const cx = [
    C(0, '10:12', 'arrived', 'Etsy', { source: 'etsy', text: 'Order arrived from Etsy' }),
    C(0, '10:13', 'placed', '', { source: 'system', sheet: 'GF Sheet 2', sheetId: 'sheet-ov-1', text: 'Placed on GF Sheet 2' }),
    C(0, '15:30', 'engraveApproved', 'Giovanna C.', { text: 'Back engraving approved' }),
    C(1, '09:05', 'teamMessage', 'Giovanna C.', { text: 'Buyer asked for a gift note.' }),
    C(2, '11:40', 'etsyCancelled', 'Etsy', { source: 'etsy', text: 'Cancelled on Etsy', data: { reason: 'Buyer requested', refund: '$42.00' } }),
    C(2, '11:40', 'removed', '', { source: 'system', sheet: 'GF Sheet 2', sheetId: 'sheet-ov-1', text: 'Taken off GF Sheet 2', data: { reason: 'Cancelled on Etsy — the sheet was not cut yet' } }),
    C(3, '13:02', 'cancelAlert', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Scanned at Sorting — alert shown, Understood pressed' })
  ];
  return { [MAIN]: { events: mk(MAIN), cancelled: null, where: null }, [D]: { events: mk(D).slice(0, 17), cancelled: null, where: null }, [CX]: { events: cx, cancelled: { at: cx[4].at, by: 'Etsy', why: 'Buyer requested', source: 'etsy' }, where: null } };
}

(async () => {
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const others = []; for (let i = 0; i < 24; i++) others.push(box('o' + i, 30 + (i % 8) * 32, 25 + Math.floor(i / 8) * 40));
  const oc = others.map((b, i) => ({ id: b.id, name: `41799999${String(i).padStart(2, '0')} · OTHER`, poolId: `41799999${String(i).padStart(2, '0')}_1_1`, order: `41799999${String(i).padStart(2, '0')}`, sku: 'OTHER' }));
  const sheet = (id, idx, mine) => srv.st.put('Charm_Nest_Sheets', id, { id, metal: 'gold', sheetIndex: idx, day: '2026-09-27', status: 'written', density: 0.42, stock: { wPt: 283.46, hPt: 141.73 }, orders: mine.map(m => m.order).concat(oc.map(o => o.order)),
    placements: mine.map(m => box(m.id, m.x, m.y)).concat(others.filter((_, i) => i % 3)), charms: mine.map(m => ({ id: m.id, name: `${m.order} · ${m.sku}`, poolId: m.poolId, order: m.order, sku: m.sku })).concat(oc.filter((_, i) => i % 3)),
    backPool: mine.filter(m => m.back).map(m => ({ poolId: m.poolId, order: m.order, sku: m.sku, copy: 1, text: m.back, approvedAt: Date.now() - DAY * 1000, approvedBy: 'Giovanna C.' })) });
  sheet('sheet-ov-1', 2, [{ id: 'pa', x: 60, y: 130, order: A.rid, sku: A.sku, poolId: poolOf(A.rid, A.tid), back: 'Love, Mom · 2026' }, { id: 'pd1', x: 150, y: 130, order: D.rid, sku: D.sku, poolId: poolOf(D.rid, D.tid) }, { id: 'pcx', x: 230, y: 130, order: CX.rid, sku: CX.sku, poolId: poolOf(CX.rid, CX.tid) }]);
  sheet('sheet-ov-2', 3, [{ id: 'pd2', x: 120, y: 125, order: D.rid, sku: D.sku2, poolId: poolOf(D.rid, D.tid2), back: 'R.N.' }]);
  const pool = (rid, tid, sku, sheetId, sheetName) => srv.st.put('Charm_Pool', poolOf(rid, tid), { poolId: poolOf(rid, tid), orderId: rid, transactionId: tid, lineKey: `${rid}_${tid}`, sku, material: 'gold', copy: 1, quantity: 1, state: 'placed', sheetId, sheetName, runId: null, updatedAt: Date.now() });
  pool(A.rid, A.tid, A.sku, 'sheet-ov-1', 'GF_Sheet-2'); pool(D.rid, D.tid, D.sku, 'sheet-ov-1', 'GF_Sheet-2'); pool(D.rid, D.tid2, D.sku2, 'sheet-ov-2', 'GF_Sheet-3');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = [];
  try {
    for (const W of widths) {
      const H = W >= 1920 ? 1080 : W >= 1440 ? 900 : 800;
      const context = await browser.newContext({ viewport: { width: W, height: H } });
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); return r.abort(); });
      await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
      const page = await context.newPage();
      const errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true && window.OrderTimeline, null, { timeout: 60000 });
      await page.evaluate(({ fx, ids }) => {
        window.__fx = (new Function('return (' + fx + ')'))()(ids);
        const realGet = OrderTimeline.get;
        OrderTimeline.get = async id => { if (!window.__fx[id]) return realGet(id); await new Promise(r => setTimeout(r, 60)); return JSON.parse(JSON.stringify(window.__fx[id])); };
      }, { fx: fixture.toString(), ids: { MAIN: A.rid, CX: CX.rid, D: D.rid } });
      const fx = await page.evaluate(() => window.__fx), where = {};
      for (const [id, o] of Object.entries(fx)) where[id] = Timeline.whereOf(o.events, o.cancelled);
      await page.evaluate(w => { for (const id in w) window.__fx[id].where = w[id]; }, where);
      const orders = [
        [order(A.rid, 'Hannah Whitford', [line(A.tid, A.sku, 'Tiny Initial Tag necklace', { personalization: ['Initial: H', 'Back engraving: Love, Mom · 2026'] })], { buyerMessage: 'Please make sure it is an H, not a K. Thank you!' }), [poolOf(A.rid, A.tid)]],
        [order(D.rid, 'Ava Patel', [line(D.tid, D.sku, 'Monstera leaf charm necklace'), line(D.tid2, D.sku2, 'Aster birth flower necklace', { personalization: ['September'] })]), [poolOf(D.rid, D.tid), poolOf(D.rid, D.tid2)]],
        [order(CX.rid, 'Rachel Nguyen', [line(CX.tid, CX.sku, 'Monstera leaf necklace', { personalization: ['R.N.'] })]), [poolOf(CX.rid, CX.tid)]],
        [order(CU.rid, 'Buyer 6673', [line(CU.tid, CU.sku, 'CUSTOM CHARM', { variations: [{ name: 'Price', value: '28' }], metalKey: '', metalLabel: '', personalization: [] })]), []]
      ];
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const pid = pools[i]; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pid ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pid ? [pid] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
        Orders.interpretAll(); CN.setMode('orders'); Orders.render();
      }, orders);
      const settled = async () => { await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(500); };
      const snap = async name => { if (!out) return; const f = path.join(out, `adv-design-${name}-${W}.png`); await page.screenshot({ path: f }); shots.push(f); };
      const box = s => page.evaluate(s => { const e = document.querySelector(s); if (!e) return null; const b = e.getBoundingClientRect(); return { l: b.left, r: b.right, t: b.top, b: b.bottom, w: b.width, h: b.height, sw: e.scrollWidth, cw: e.clientWidth }; }, s);
      const openKey = async (o, tid) => { const sel = `#ordItems [data-key="${o.rid}_${tid}"]`; await page.waitForSelector(sel); await page.click(sel); await settled(); };
      const view = async v => { await page.click(`.owTabsV [data-ow-view="${v}"]`); await settled(); if (v === 'sheet') await page.waitForFunction(() => document.getElementById('owPlateWait').hidden, null, { timeout: 15000 }).catch(() => {}); await page.waitForTimeout(700); };
      const close = async () => { await page.click('#owClose'); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 }).catch(() => {}); await page.waitForTimeout(300); };
      // 1 · a normal order: the composer, the header rail, the station badge, the field grid
      await openKey(A, A.tid); await page.waitForTimeout(600); await snap('normal-overview');
      const chat = await box("#orderWin .owChat"), send = await box("#owSend"), cline = await box("#orderWin .owCompLine"), comp = await box('#orderWin .owComp');
      assert(send.r <= chat.r - 4 && cline.r <= chat.r, `the send button stays inside its column: send ${send.l}-${send.r}, column ends ${chat.r}`);
      assert(comp.sw <= comp.cw + 1, `the composer does not run wider than its column: ${comp.sw} > ${comp.cw}`);
      const rail = await page.evaluate(() => ({ fs: getComputedStyle(document.querySelector('#owRail .tlStop>span')).fontSize, clipped: [...document.querySelectorAll('#owRail .tlStop>span')].filter(s => s.scrollWidth > s.clientWidth + 1).map(s => s.textContent) }));
      assert.equal(rail.fs, '8.5px', 'the header rail reads at 8.5px'); assert.deepEqual(rail.clipped, [], 'no step name is cut at 1280');
      await page.waitForFunction(() => !!document.querySelector('#owNowCard .tlNowSeal, #owNowCard .who i svg'), null, { timeout: 5000 });
      assert(await page.evaluate(() => !(document.querySelector('#owNowCard .tlNowSeal') && document.querySelector('#owNowCard .who'))), 'with a seal, no who · time line beside it (the seal says it)');
      const purchased = await page.evaluate(() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => /Purchased/i.test(x.querySelector('i').textContent)); const s = m.querySelector('span'); return s.getClientRects().length === 1 ? s.getBoundingClientRect().height : 99; });
      assert(purchased < 22, 'the purchase date reads on one line: ' + purchased);
      // 2 · its Timeline: the one quiet chip in the tab row, no second Now and rail strip, day heads inside their columns
      await view('timeline'); await snap('normal-timeline');
      const tl = await page.evaluate(() => {
        const tools = document.getElementById('owTlTools'), top = document.querySelector('#owTimeline .tlTop'), grid = document.querySelector('#owTimeline .tlGrid');
        const heads = [...document.querySelectorAll('#owTimeline .tlDay:not(.idle)')].map(d => { const r = d.getBoundingClientRect(), s = d.querySelector('.dh small'); return { day: d.querySelector('.dh').firstChild.textContent, right: r.right, textRight: s.getBoundingClientRect().left + s.scrollWidth, cut: s.scrollWidth > s.clientWidth + 1 }; });
        const chip = tools.querySelector('.tlChip'), cs = chip && getComputedStyle(chip);
        return { chips: tools.querySelectorAll('.tlChip').length, chipText: chip && chip.textContent, chipFs: cs && cs.fontSize, top: top ? getComputedStyle(top).display : 'none', gridTop: grid.getBoundingClientRect().top, heads };
      });
      assert.equal(tl.chips, 1, 'one quiet chip in the tab row: the filters are gone'); assert.equal(tl.chipText, 'Stamps');
      assert.equal(tl.top, 'none', 'no second Now + rail strip under the header');
      assert(tl.gridTop <= 92, 'the lanes start right under the tab row: ' + tl.gridTop);
      assert.equal(tl.chipFs, '11px', 'the chip at 11px');
      for (const h of tl.heads) assert(!h.cut && h.textRight <= h.right - 4, `${h.day}: its head stays inside its column (${Math.round(h.textRight)} > ${Math.round(h.right)})`);
      // the Stamps chip still opens the legend of the seals from the tab row
      await page.click('#owTlTools .tlChip[data-legend]');
      await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlLegend figure').length > 0 && !!document.querySelector('#owTlTools .tlChip.on'));
      await page.click('#owTlTools .tlChip[data-legend]');
      await close();
      assert.equal(await page.evaluate(() => document.querySelectorAll('#owTlTools .tlBar').length), 0, 'closing takes the bar out of the tab row');

      // 3 · a 2-line order: no "line 1 of 2" in the header any more (Paul, 28 Sep: multi-piece orders were confusing);
      //     the piece switch says "All 2 pieces" instead
      await openKey(D, D.tid); await snap('twosheets-overview');
      assert.equal(await page.evaluate(() => !!document.querySelector('#owTitle .owPc')), false, 'no "line 1 of 2" after the number');
      await page.waitForFunction(() => { const sw = document.getElementById('owPieceSw'); return sw && !sw.hidden && /^All 2 pieces/.test(sw.textContent.trim()); }, null, { timeout: 10000 });
      await view('timeline'); await view('info');
      assert.equal(await page.evaluate(() => document.querySelectorAll('#owTlTools .tlBar').length), 1, 'one bar, however often the tab is shown');
      await close();

      // 4 · a cancelled order: the ✕ and struck steps on the header rail, no stamp over them
      await openKey(CX, CX.tid);
      await page.waitForFunction(() => document.getElementById('orderWin').classList.contains('owCancelled') && document.querySelector('#owRail .tlStop.x'), null, { timeout: 8000 });
      await page.waitForTimeout(900); await snap('cancelled-overview');
      const cx = await page.evaluate(() => { const s = document.querySelector('#owRail .tlCxStamp'); return { stamp: s ? getComputedStyle(s).display : 'none', seal: !!document.querySelector('#owNowCard .tlNowSeal.cx') }; });
      assert.equal(cx.stamp, 'none', 'no big stamp over the header\'s step names'); assert(cx.seal, 'the Overview keeps its CANCELLED ORDER seal');
      await close();

      // 5 · a custom order on no sheet: the message in the middle of the plate area
      //    (its Sheet tab is greyed and inert, Paul 5 Oct: a piece on no sheet is never sent to another's; the plate is reached as a search or a
      //    Review card reaches it, by opening the order on its Sheet view)
      await openKey(CU, CU.tid);
      assert.equal(await page.evaluate(() => document.querySelector('.owTabsV [data-ow-view="sheet"]').getAttribute('aria-disabled')), 'true', 'the Sheet tab of a piece on no sheet is greyed');
      await page.evaluate(() => OrderWin.setView('sheet')); await settled(); await page.waitForFunction(() => document.getElementById('owPlateWait').hidden, null, { timeout: 15000 }).catch(() => {}); await page.waitForTimeout(700);
      await page.waitForSelector('#owPlateWrap .owPlateNone'); await snap('custom-sheet');
      const wrap = await box('#owPlateWrap'), none = await box('#owPlateWrap .owPlateNone');
      assert(Math.abs((none.t + none.b) / 2 - (wrap.t + wrap.b) / 2) < 40, `"Not on a sheet yet" is centred: ${none.t}–${none.b} in ${wrap.t}–${wrap.b}`);
      await close();
      assert.deepEqual(errors, [], 'no page errors');
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  if (shots.length) console.log('  shots: ' + shots.join(', '));
  console.log('  ✓ composer inside its column, filters in the tab row, day heads inside their columns, rail names at 8.5px, cancelled without a stamp over them, badge icon, no "line 1 of 2" (the piece switch instead), one-line dates, centred empty plate');
})().then(() => console.log('Order view design OK')).catch(e => { console.error(e); process.exit(1); });
