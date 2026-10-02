// The order window's Sheet tab (Paul, 2 Oct 21:02: "I should be able to click all of the orders on the sheet and just have
// the page adjust to that order. You don't want to block people from clicking orders and charm designs on the sheet."):
// ONE click anywhere on a charm of another order moves the window to that order — no "Open order" step, no chip over the
// neighbouring charms — and it stays on the Sheet tab, on the same sheet, with the clicked charm chosen. A second click
// right after, while the plate is still being drawn for the first, is taken too; so is one on the charm just above the
// first (where the old chip sat), and one made with the pointer fresh from a seal in the header. A charm of the order that
// is open is only chosen; Previous / Next keep walking the Orders list from the new order; the plate does not move.
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted.
//   node tests/charm-nest/ow-sheet-click-any.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), SH = 'sheet-click-any';
const mk = (n, sku) => ({ rid: '41780000' + n, tid: '41780000' + n + '1', sku });
const S = mk('01', 'TINY_TAG'), A = mk('02', 'LEAF_CHARM'), B = mk('03', 'MOON_DROP'), C = mk('04', 'STAR_STUD');
const ALL = [[S, 20, 'Mia Lund'], [A, 21, 'Ada Byrne'], [B, 6, 'Noor Haddad'], [C, 40, 'Lea Fischer']];   // [order, its place on the grid, buyer]
const pool = o => `${o.rid}_${o.tid}_1`;
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '18000' + o.tid.slice(-5), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });

function seed(st) {
  // 60 charms on a 15-wide grid: B's charm sits directly above A's, which is where A's "Open order" chip used to be drawn
  const pl = [], ch = [];
  for (let i = 0; i < 60; i++) {
    const hit = ALL.find(([, at]) => at === i), o = hit && hit[0], rid = o ? o.rid : String(4179000000 + i);
    pl.push({ id: 'b' + i, cxPt: 12 + (i % 15) * 22, cyPt: 12 + Math.floor(i / 15) * 22, angle: 0, wPt: 18, hPt: 18 });
    ch.push({ id: 'b' + i, name: `${rid} · SKU${i}`, poolId: o ? pool(o) : `${rid}_${rid}1_1`, order: rid, sku: o ? o.sku : 'SKU' + i });
  }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 2, day: '2026-09-27', status: 'written', stock: { wPt: 340, hPt: 110 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch });
  for (const [o] of ALL) st.put('Charm_Pool', pool(o), { poolId: pool(o), orderId: o.rid, transactionId: o.tid, lineKey: `${o.rid}_${o.tid}`, sku: o.sku, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: '2026-09-27_GF_Set-1_Sheet-2', updatedAt: Date.now() });
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = process.env.SHOTS || '';
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-click-any')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // every order's timeline has a few steps, so the header strip shows seals to rest the pointer on
    const T0 = Date.now() - 4 * 24 * 36e5, ev = (rid, type, h) => ({ id: `${rid}~${type}~${h}`, orderId: rid, type, at: T0 + h * 36e5, by: 'Paul', source: 'sorter' });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pool] of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pool], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: ALL.map(([o, , buyer]) => [order(o, buyer), pool(o)]) });
    const evs = rid => Timeline.chronology([ev(rid, 'arrived', 0), ev(rid, 'placed', 5), ev(rid, 'engraveApproved', 20), ev(rid, 'laserDone', 30)].map(e => Object.assign(e, e.type === 'arrived' ? { source: 'etsy', by: 'Etsy' } : { sheetId: SH, sheet: 'GF Sheet 2' }))).sort(Timeline.byTime);
    await page.evaluate(({ all }) => { const wait = window.setTimeout.bind(window); OrderTimeline.get = async rid => { await new Promise(r => wait(r, 20)); const events = all[String(rid)] || []; return JSON.parse(JSON.stringify({ id: String(rid), events, cancelled: null, where: null })); }; },
      { all: Object.fromEntries(ALL.map(([o]) => [o.rid, evs(o.rid)])) });
    await page.waitForTimeout(400);

    // a picture that is still loading (the charm's, in the panel beside the plate) must not lie over the plate: the moment a
    // loading box is drawn, what is on top of the plate's middle is looked at (it was a white box over the whole Sheet view)
    await page.evaluate(() => {
      const V = window.__veil = { seen: 0, over: [] };
      new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) {
        if (n.nodeType !== 1 || !(n.matches('.thumbLoading') || n.querySelector('.thumbLoading'))) continue;
        const cv = document.getElementById('owSheetCv'), r = cv && cv.getBoundingClientRect(); if (!r || !r.width || cv.offsetParent === null) continue;
        V.seen++; const top = document.elementsFromPoint(r.left + r.width / 2, r.top + r.height / 2)[0];
        if (top && top !== cv && !top.contains(cv) && top.id !== 'owPlateWrap') V.over.push(String(top.className || top.id || top.tagName));
      } }).observe(document.body, { childList: true, subtree: true });
    });
    const title = () => page.evaluate(() => document.getElementById('owTitle').textContent);
    const drawn = (rid, pid) => page.waitForFunction(({ rid, pid }) => { const i = OrderWin._sheet(); return !!(i && i.sheet && i.sheet.id === 'sheet-click-any' && i.pointOf(pid) && i.mine.length && String(i.mine[0].rid) === rid) && document.getElementById('owSheetCv').style.visibility !== 'hidden' && !document.getElementById('owPlateWait').offsetParent; }, { rid, pid }, { timeout: 15000 });
    // where each order's charm is on the screen (read once: the plate does not move, and is not always "drawn" while the window switches)
    const PT = {}, at = pid => PT[pid];
    const tab = () => page.evaluate(() => (document.querySelector('#orderWin .owTabsV [aria-selected=true]') || {}).textContent || '');
    const chosen = () => page.evaluate(() => (document.querySelector('#owSheetPanel .owCharm b') || {}).textContent || '');
    const plateBox = () => page.evaluate(() => { const r = document.getElementById('owSheetCv').getBoundingClientRect(); return [r.left, r.top, r.width, r.height].map(Math.round).join(','); });
    // what the pointer would meet at a point: nothing but the plate may be there (the hover label has no pointer events)
    const meets = (x, y) => page.evaluate(([x, y]) => document.elementsFromPoint(x, y).slice(0, 3).map(e => e.id || e.className || e.tagName), [x, y]);
    const dialogs = () => page.evaluate(() => document.querySelectorAll('dialog[open]').length);
    // one click on a charm: the pointer comes to it, rests a moment (the hover label shows), the plate is the only thing under it, and ONE click is made
    const clickCharm = async (o, label) => {
      const pt = at(pool(o));
      await page.mouse.move(pt.x - 14, pt.y - 6, { steps: 4 }); await page.waitForTimeout(70);
      const under = await meets(pt.x, pt.y);
      assert.equal(under[0], 'owSheetCv', `${label}: only the plate lies under the charm: ${under.join(' / ')}`);
      // (the hover label names the order and may stay; it takes no clicks, so it can never lie in the way of a neighbour)
      // (in (b) the plate is being drawn again, which puts the label away until the pointer moves: only its manner is checked)
      assert(await page.evaluate(([shown]) => { const t = document.getElementById('owPlateTip'); return getComputedStyle(t).pointerEvents === 'none' && (!shown || (!t.hidden && t.textContent.length > 4)); }, [label !== '(b)']), `${label}: the hover label shows and takes no clicks`);
      const t0 = Date.now(); await page.mouse.click(pt.x, pt.y);
      await page.waitForFunction(rid => document.getElementById('owTitle').textContent.includes(rid), o.rid, { timeout: 2500 });
      return Date.now() - t0;
    };

    // ── S's Sheet tab, opened from the Orders list (so Previous / Next walk it) ──
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${S.rid}_${S.tid}`);
    await drawn(S.rid, pool(S)); await page.waitForTimeout(600);
    assert(/Sheet/.test(await tab()) && (await title()).includes(S.rid), 'S is open on its Sheet tab');
    const pos0 = await page.evaluate(() => document.getElementById('owPos').textContent);
    assert(/^\d+ of \d+$/.test(pos0), 'Previous / Next walk the Orders list: ' + pos0);
    const box0 = await plateBox();
    for (const [o] of ALL) PT[pool(o)] = await page.evaluate(p => OrderWin._sheet().pointOf(p), pool(o));
    // the pointer rests on a seal of the header strip until it has grown
    const seal = '#owRail .tlStop .tlSeal:not(.pending)';
    await page.waitForFunction(s => document.querySelectorAll(s).length >= 2, seal, { timeout: 8000 });
    const sp = await page.evaluate(s => { const r = document.querySelectorAll(s)[1].getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, seal);
    await page.mouse.move(sp.x, sp.y, { steps: 3 });
    await page.waitForFunction(s => !!document.querySelectorAll(s)[1].dataset.sealZoom, seal, { timeout: 4000 });
    if (shots) await page.screenshot({ path: path.join(shots, 'a-seal-rest.png') });

    // (a) a click on A's charm, straight from the seal: the window is A's, on the Sheet tab, with no step in between
    const ms = await clickCharm(A, '(a)');
    assert(/Sheet/.test(await tab()), '(a) still on the Sheet tab');
    assert.equal(await dialogs(), 1, '(a) one window: never a view over the view');
    assert(await page.evaluate(() => { const b = document.getElementById('owPlatePick'); return !b || b.hidden || !b.offsetParent; }), '(a) no "Open order" label on the plate');
    const pos1 = await page.evaluate(() => document.getElementById('owPos').textContent);
    assert(/^\d+ of \d+$/.test(pos1) && pos1 !== pos0, `(a) the counter follows to A (${pos0} -> ${pos1})`);
    console.log(`  (a) A opened by one click in ${ms} ms, counter ${pos0} -> ${pos1}`);

    // (b) straight on to C's charm while A's plate is still being drawn: taken, not ignored
    console.log(`  (b) the plate is still being drawn for A when C is clicked: ${await page.evaluate(() => !OrderWin._sheet())}`);
    await clickCharm(C, '(b)');
    assert(/Sheet/.test(await tab()), '(b) still on the Sheet tab');
    assert.equal(await dialogs(), 1, '(b) one window');
    await drawn(C.rid, pool(C)); await page.waitForTimeout(450);
    assert.equal(await chosen(), C.sku, "(b) C's charm is the one chosen");
    assert.equal(await plateBox(), box0, '(b) the plate has not moved or changed size');
    if (shots) await page.screenshot({ path: path.join(shots, 'b-after-two-clicks.png') });

    // (c) B's charm lies right above A's, where A's offer used to hang: first go back to A (one click), then click B
    await clickCharm(A, '(c0)'); await drawn(A.rid, pool(A)); await page.waitForTimeout(350);
    const pa = at(pool(A)), pb = at(pool(B));
    assert(Math.abs(pb.x - pa.x) < 4 && pa.y - pb.y > 20 && pa.y - pb.y < 80, 'B lies just above A on the plate');
    await page.mouse.move(pa.x, pa.y, { steps: 3 }); await page.waitForTimeout(120);   // (A's own charm: the hover label shows)
    await clickCharm(B, '(c)');
    assert(/Sheet/.test(await tab()), '(c) still on the Sheet tab');
    await drawn(B.rid, pool(B)); await page.waitForTimeout(350);
    assert.equal(await chosen(), B.sku, "(c) B's charm is the one chosen");

    // (d) a charm of the open order is only chosen: same window, nothing opens, nothing is hidden
    const pbb = at(pool(B)); await page.mouse.click(pbb.x, pbb.y); await page.waitForTimeout(150);
    assert((await title()).includes(B.rid) && /Sheet/.test(await tab()) && await chosen() === B.sku, "(d) B's own charm: chosen, same order");

    // (e) the keyboard: Enter in "Order # on sheet" opens the first order found
    await page.fill('#owSheetOrderFind', S.rid);
    await page.waitForFunction(() => document.querySelector('#owSheetMatches [data-order-rid]'));
    await page.press('#owSheetOrderFind', 'Enter');
    await page.waitForFunction(rid => document.getElementById('owTitle').textContent.includes(rid), S.rid, { timeout: 2500 });
    assert(/Sheet/.test(await tab()), '(e) Enter on the order number opens it, still on the Sheet tab');

    // Back to the order it came from, and Esc
    await drawn(S.rid, pool(S)); await page.waitForTimeout(350);
    assert(await page.evaluate(() => !!document.querySelector('#owSheetPanel [data-ow-back]')), 'Back to the order it came from is offered');
    await page.keyboard.press('Escape'); await page.waitForTimeout(700);
    assert(!(await page.evaluate(() => OrderWin.isOpen())), 'Esc closes the window');
    { const V = await page.evaluate(() => window.__veil); assert.equal(V.over.length, 0, 'no loading box was ever drawn over the plate: ' + V.over.join(', ')); console.log(`  loading boxes checked over the plate: ${V.seen}`); }
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ one click on any charm opens its order: from a seal, twice in a row, under the old offer, own charm, keyboard');
    await context.close();
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('ow-sheet-click-any: ok'), e => { console.error(e); process.exit(1); });
