// Order search (Paul, 28 Sep 19:21: B1-B3): "/" or Ctrl/Cmd+K opens one box over every order the sorter holds (the pull
// with its holds, the pool, the Library's sheets, Cancelled, Review) and a whole Etsy number that is nowhere here is looked
// up in the cloud once the typing pauses, cancelled by the next key. A result opens the order's view out of its card.
// The sorter runs in headless Chromium against the local fake site (bridge-server.cjs); every request that is not to the
// loopback is aborted, and the cloud's answers for the numbers looked up are this file's own (page.route).
//   node tests/charm-nest/order-search.cjs [playwright-core dir]   (CN_SHOTS=dir: where search-*.png go)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, buyer, lines, extra) => Object.assign({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }, extra || {});
const line = (tid, sku, title, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }, extra || {});
const ORDERS = [
  order(4176208841, 'Hannah Whitford', [line(41762088411, 'BLOOMING_20239', 'Blooming Flower Charm Necklace')], { createTs: SHIP - 2 * DAY }),
  order(4176200172, 'Ava Patel', [line(41762001721, 'BUNNY5', 'Bunny Charm with Gift Box')], { createTs: SHIP - 3 * DAY }),
  order(4176229486, 'Zoe Adams', [line(41762294861, 'SEA_TURTLE', 'Sea Turtle Charm')], { createTs: SHIP - 4 * DAY }),
  order(4176245466, 'Stephanie Lopez', [line(41762454661, 'MOON_12', 'Crescent Moon Charm')], { createTs: SHIP - 6 * DAY }),
  order(4175000001, 'Maria Alvarez', [line(41750000011, 'STAR_3', 'Tiny Star Charm'), line(41750000012, 'HEART_9', 'Heart Charm')], { createTs: SHIP - 7 * DAY }),
  order(4174476673, 'Olivia Chen', [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM')], { createTs: SHIP - 8 * DAY })
];
const CLOUD = '4170000123', SLOW = '4170000555', NOWHERE = '41700005559';

async function main() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || process.env.SCRATCH || null;
  const srv = await start({ receipts: [] });
  // a cancelled order in the cloud's Cancelled list (the sorter reads it when the search first opens)
  srv.st.put('Charm_Nest_Cancelled', '4173299999', { orderId: '4173299999', by: 'Etsy', why: 'buyer asked', at: Date.now() - 3600e3, buyer: 'Cara Quinn', lines: [{ transactionId: '1', sku: 'LEAF_2', title: 'Leaf Charm', quantity: 1 }], sheets: [] });
  // Zoe's line sits on a written sheet: the CLOUD holds that (the sheet's record lists her pool id, her pool row names the sheet). The search says where a piece is
  // from the cloud's one answer (PiecePlacement), so a sheet that only this page's own rows named would be a stale hint, not a fact (consistency: audit.md row 8)
  const ZOE_POOL = '4176229486_41762294861_1', NOW = Date.now();
  srv.st.put('Charm_Nest_Sheets', 'sh2', { id: 'sh2', metal: 'gold', sheetIndex: 2, status: 'complete', fileBase: '2026-09-28_GF_Sheet_2', folder: '2026-09-28_GF_Sheet_2', poolIds: [ZOE_POOL], orders: ['4176229486'], charms: [{ id: 'sh2-c0', poolId: ZOE_POOL, order: '4176229486', name: '4176229486 · SEA_TURTLE' }], placements: [{ id: 'sh2-c0', cxPt: 40, cyPt: 40, angle: 0, wPt: 28, hPt: 28 }], placedCount: 1, charmCount: 1, density: .1, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, verification: { ok: true }, outputs: {}, createdAt: NOW - 3600e3, updatedAt: NOW - 600e3 });
  srv.st.put('Charm_Pool', ZOE_POOL, { poolId: ZOE_POOL, orderId: '4176229486', sku: 'SEA_TURTLE', state: 'written', sheetId: 'sh2', sheetName: '2026-09-28_GF_Sheet_2', createdAt: NOW - 3600e3, updatedAt: NOW - 600e3 });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    // the cloud's answers for the numbers this test looks up; every other call goes to the fake site
    const asked = [], failed = [];
    page.on('requestfailed', r => { if (/charmNestLibrary|designArchive/.test(r.url())) failed.push({ url: r.url(), body: r.postData() || '', why: r.failure() && r.failure().errorText }); });
    await page.route(/\/\.netlify\/functions\/(charmNestLibrary|designArchive)/, async r => {
      const req = r.request(), url = req.url(); let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (_) {}
      const id = String(body.orderId || body.q || new URL(url).searchParams.get('id') || '');
      const op = body.op || new URL(url).searchParams.get('op');
      if (![CLOUD, SLOW].includes(id) || !['timelineGet', 'poolList', 'findSheets', 'get'].includes(op)) return r.fallback();
      asked.push({ op, id, t: Date.now() });
      if (id === SLOW) { await new Promise(res => setTimeout(res, 1500)); try { return await r.fulfill({ status: 200, contentType: 'application/json', body: '{}' }); } catch (_) { return; } }
      const out = op === 'timelineGet' ? { orderId: CLOUD, events: [{ type: 'arrived', at: Date.now() - 20 * DAY * 1000, by: 'Etsy' }, { type: 'placed', at: Date.now() - 19 * DAY * 1000, by: 'Ann' }, { type: 'laserDone', at: Date.now() - 18 * DAY * 1000, by: 'Ben' }, { type: 'shipped', at: Date.now() - 16 * DAY * 1000, by: 'Dana K.', station: 'shipping' }], cancelled: null }
        : op === 'poolList' ? { pools: [{ poolId: `${CLOUD}_9_1`, orderId: CLOUD, sku: 'OLD_ROSE', sheetId: 'sheetOld', sheetName: '2026-09-10_GF_Sheet_4' }] }
        : op === 'findSheets' ? { q: CLOUD, sheets: [], rows: [], sets: [], setRows: [], matches: { order: 0, listing: 0 } }
        : { success: true, row: { receiptId: CLOUD, orderNumber: CLOUD, buyer: { name: 'Olive Archer' }, items: [{ sku: 'OLD_ROSE', title: 'Old Rose Charm', listingId: 1811112222 }], shipments: [{ carrier: 'usps' }], completedAtMs: Date.now() - 17 * DAY * 1000 } };
      return r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(out) });
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderSearch && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems();
      // Ava's order is on hold; Zoe's line sits on a written sheet (its pool row names it)
      const ava = B.orders.rows.find(r => r.order.receiptId === '4176200172'); ava.hold = 'held by Test Operator'; ava.reason = ava.hold; ava.state = 'held';
      const zoe = B.orders.rows.find(r => r.order.receiptId === '4176229486'); zoe.state = 'written'; zoe.poolIds = ['4176229486_41762294861_1'];
      B.pool.rows.set('4176229486_41762294861_1', { poolId: '4176229486_41762294861_1', orderId: '4176229486', sku: 'SEA_TURTLE', sheetId: 'sh2', sheetName: '2026-09-28_GF_Sheet_2', state: 'written' });
    }, ORDERS);
    const etsyCalls = () => srv.st.calls.filter(c => /etsy|listOpenOrders|Receipt/i.test(c.name)).length;
    const etsyBefore = etsyCalls();

    /* ── the entry: a field in the thin top bar that adds no height ── */
    const bar = await page.evaluate(() => {
      const b = document.getElementById('cnsFind'), top = document.querySelector('.topbar'), h1 = top.getBoundingClientRect().height;
      b.style.display = 'none'; const h0 = top.getBoundingClientRect().height; b.style.display = '';
      const r = b.getBoundingClientRect(), t = top.getBoundingClientRect();
      return { h0, h1, inside: r.top >= t.top && r.bottom <= t.bottom, h: r.height, text: b.textContent };
    });
    assert.equal(bar.h1, bar.h0, 'the search field adds no height to the top bar: ' + JSON.stringify(bar));
    assert(bar.inside && bar.h <= 32, 'it sits on the bar\'s one line: ' + JSON.stringify(bar));
    console.log('  ✓ top bar entry, one line, no added height');

    /* ── "/" opens it (not while typing elsewhere), Ctrl+K too ── */
    await page.evaluate(() => { const i = document.createElement('input'); i.id = '__other'; document.body.appendChild(i); i.focus(); });
    await page.keyboard.press('/');
    assert.equal(await page.evaluate(() => OrderSearch.isOpen()), false, '"/" typed into another field is text');
    await page.evaluate(() => { document.getElementById('__other').remove(); document.activeElement.blur(); });
    await page.keyboard.press('Control+k');
    await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement && document.activeElement.id === 'cnsQ');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => document.getElementById('cnSearch').hidden);
    await page.keyboard.press('/');
    await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement.id === 'cnsQ', null, { timeout: 5000 }).catch(async e => { console.error(await page.evaluate(() => JSON.stringify({ open: OrderSearch.isOpen(), active: document.activeElement && (document.activeElement.id || document.activeElement.tagName), dialogs: [...document.querySelectorAll('dialog[open]')].map(d => d.id || d.className) }))); throw e; });
    const first = await page.evaluate(() => ({ cards: document.querySelectorAll('#cnsList .cnsCard').length, head: document.querySelector('.cnsHead')?.textContent, count: document.getElementById('cnsCount').textContent }));
    assert(first.cards >= 5 && first.head === 'Latest orders', 'opened empty: the latest orders: ' + JSON.stringify(first));
    console.log('  ✓ "/" and Ctrl+K open it; "/" in another field does not');

    /* ── every digit filters at once, the typed part highlighted ── */
    const cards = () => page.evaluate(() => [...document.querySelectorAll('#cnsList .cnsCard')].map(n => ({ rid: n.dataset.rid, mark: n.querySelector('.cnsNum mark')?.textContent || '', sel: n.classList.contains('sel'), one: n.classList.contains('one'), enter: !!n.querySelector('.cnsEnter'), pill: n.querySelector('.cnsPill').textContent, now: n.querySelector('.cnsNow').textContent, sheet: n.querySelector('.cnsSheet')?.textContent || '', rail: n.querySelectorAll('.cnsRail i.d').length, cloud: !!n.querySelector('.cnsTag.cloud'), who: n.querySelector('.cnsWho').textContent })));
    const seen = [];
    for (const ch of '41762') { await page.keyboard.type(ch); seen.push((await cards()).map(c => c.rid)); }   // read right after each key: no waiting
    assert.deepEqual(seen[4].slice().sort(), ['4176200172', '4176208841', '4176229486', '4176245466'], 'four orders start with 41762: ' + JSON.stringify(seen[4]));
    assert(seen[2].length > seen[3].length && seen[3].length >= seen[4].length, 'each digit narrows the list: ' + JSON.stringify(seen.map(s => s.length)));
    let list = await cards();
    assert(list.every(c => c.mark === '41762'), 'the typed digits are highlighted in each number: ' + JSON.stringify(list));
    assert.equal(list[0].sel, true, 'the first card carries the selection');
    assert.match(await page.textContent('#cnsCount'), /matches ·/, 'the count says how many');
    const zoe = list.find(c => c.rid === '4176229486'), ava = list.find(c => c.rid === '4176200172');
    assert.equal(zoe.sheet, 'GF Sheet 2', 'the sheet it is on: ' + JSON.stringify(zoe)); assert(zoe.rail >= 2, 'its rail has passed On sheet');
    assert.equal(ava.pill, 'On hold'); assert.match(ava.now, /held by Test Operator/);
    await page.waitForTimeout(700);                                                   // the cards have settled
    if (shots) await page.screenshot({ path: path.join(shots, 'search-results.png') });
    await page.keyboard.type('08');
    list = await cards();
    assert.deepEqual(list.map(c => c.rid), ['4176208841'], 'one left'); assert(list[0].one && list[0].enter, 'the last match glows with ↵ open');
    const times = await page.evaluate(() => OrderSearch.stats());
    assert(times.lastMs < 5, 'the keystroke\'s pass took ' + times.lastMs + ' ms');
    console.log('  ✓ digits filter on every key, prefix highlighted, one match glows');

    /* ── words: a buyer, a SKU; Cancelled from the cloud's list ── */
    await page.fill('#cnsQ', 'ava pat');
    list = await cards(); assert.deepEqual(list.map(c => c.rid), ['4176200172'], 'a buyer by name');
    await page.fill('#cnsQ', 'sea_tur');
    list = await cards(); assert.deepEqual(list.map(c => c.rid), ['4176229486'], 'a SKU');
    await page.waitForFunction(() => OrderSearch.find('4173299999'), null, { timeout: 10000 });
    await page.fill('#cnsQ', '4173299');
    list = await cards(); const cx = list.find(c => c.rid === '4173299999');
    assert(cx && cx.pill === 'Cancelled' && /Cancelled by Etsy/.test(cx.now), 'a cancelled order reads cancelled: ' + JSON.stringify(list));
    assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('#cnsList .cnsCard[data-rid="4173299999"] .cnsNow')).color), 'rgb(176, 86, 63)', 'in red (clay)');
    console.log('  ✓ buyer, SKU and the Cancelled list');

    /* ── arrows move the glow, Enter opens the order out of its card ── */
    const hadOpenOrder = await page.evaluate(() => typeof OrderWin.openOrder === 'function');
    await page.evaluate(() => {
      const real = OrderWin.openOrder; window.__opened = [];
      OrderWin.openOrder = (rid, opts) => { const r = opts && opts.from && opts.from.getBoundingClientRect(); window.__opened.push({ rid, highlight: opts && opts.highlight, from: !!(opts && opts.from && opts.from.isConnected), w: r ? r.width : 0, h: r ? r.height : 0 }); return real ? real(rid, opts) : Promise.resolve(); };
    });
    await page.fill('#cnsQ', '41762');
    await page.keyboard.press('ArrowDown');
    list = await cards(); assert.equal(list.findIndex(c => c.sel), 1, 'ArrowDown moves the selection');
    const halo = await page.evaluate(() => { const h = document.querySelector('.cnsHalo'), c = document.querySelector('#cnsList .cnsCard.sel'); return { on: h.classList.contains('on'), t: h.style.transform, top: c.offsetTop + document.getElementById('cnsList').offsetTop }; });
    assert(halo.on && halo.t === `translateY(${halo.top}px)`, 'the glow slides to it: ' + JSON.stringify(halo));
    await page.keyboard.press('ArrowUp'); await page.keyboard.press('ArrowDown');
    const want = list[1].rid;
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => window.__opened.length === 1, null, { timeout: 3000 });
    const opened = await page.evaluate(() => window.__opened[0]);
    assert.equal(opened.rid, want, 'Enter opens the selected order'); assert.equal(opened.highlight, true);
    assert(opened.from && opened.w > 300 && opened.h > 40, 'from its card, where it was: ' + JSON.stringify(opened));
    await page.waitForFunction(() => !OrderSearch.isOpen(), null, { timeout: 3000 });
    if (hadOpenOrder) await page.evaluate(() => { for (const d of document.querySelectorAll('dialog[open]')) d.close(); });
    console.log(`  ✓ arrows, Enter → OrderWin.openOrder(rid, { highlight, from: card })${hadOpenOrder ? '' : ' (stub)'}`);

    /* ── while openOrder is not in: the pull's order window, grown out of the card, number highlighted ── */
    if (!hadOpenOrder) {
      await page.evaluate(() => { delete OrderWin.openOrder; });
      await page.keyboard.press('/');
      await page.waitForFunction(() => OrderSearch.isOpen());
      await page.keyboard.type('4176208841');
      await page.keyboard.press('Enter');
      await page.waitForFunction(() => OrderWin.isOpen() && !OrderSearch.isOpen(), null, { timeout: 5000 });
      const ow = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, found: document.getElementById('owTitle').classList.contains('cnsFound'), dialogs: document.querySelectorAll('dialog[open]').length, search: document.getElementById('cnSearch').hidden }));
      assert.match(ow.title, /4176208841/); assert.equal(ow.found, true, 'the number is highlighted'); assert.equal(ow.dialogs, 1, 'one window: no pop-up on a pop-up'); assert.equal(ow.search, true);
      // Ctrl+K from the order window: it gives way to the search
      await page.keyboard.press('Control+k');
      await page.waitForFunction(() => OrderSearch.isOpen() && !OrderWin.isOpen());
      // an order outside the pull: what is known opens in its card
      await page.fill('#cnsQ', '4173299999'); await page.keyboard.press('Enter');
      await page.waitForSelector('#cnsList .cnsCard[data-rid="4173299999"] .cnsDetail', { timeout: 3000 });
      await page.keyboard.press('Escape'); await page.keyboard.press('Escape');
      await page.waitForFunction(() => document.getElementById('cnSearch').hidden);
      console.log('  ✓ fallback: OrderWin.open grows from the card; outside the pull, the card opens in place');
    }

    /* ── a whole number that is nowhere here: the cloud, after the pause, once ── */
    await page.keyboard.press('/');
    await page.waitForFunction(() => OrderSearch.isOpen());
    await page.fill('#cnsQ', '');
    await page.keyboard.type(CLOUD, { delay: 40 });
    await page.waitForSelector(`#cnsList .cnsCard[data-rid="${CLOUD}"]`, { timeout: 8000 });
    list = await cards(); const old = list.find(c => c.rid === CLOUD);
    assert.equal(old.who, 'Olive Archer'); assert.equal(old.cloud, true, 'marked as found in the cloud'); assert.equal(old.pill, 'Shipped'); assert.equal(old.sheet, 'GF Sheet 4'); assert.equal(old.mark, CLOUD);
    // the order view's own steps: a charm (no stud) has no Welded, so seven dots, every one passed (Approved and Completed are gone)
    const dots = await page.evaluate(id => [...document.querySelectorAll(`#cnsList .cnsCard[data-rid="${id}"] .cnsRail i`)].map(i => i.title), CLOUD);
    assert.deepEqual(dots, ['Order in', 'Nested', 'Engraved', 'Laser cut', 'Sorted', 'Assembled', 'Shipped'], 'its steps: ' + dots);
    assert.equal(old.rail, 7, 'shipped: every step passed: ' + old.rail);
    const ops = asked.filter(a => a.id === CLOUD).map(a => a.op).sort();
    assert.deepEqual(ops, ['findSheets', 'get', 'poolList', 'timelineGet'], 'each read once, for the whole number only: ' + JSON.stringify(asked));
    assert.equal(await page.evaluate(() => document.querySelectorAll('.cnsLift').length), 0, 'no card is left lifted over the box');
    await page.waitForTimeout(500);
    if (shots) await page.screenshot({ path: path.join(shots, 'search-cloud.png') });
    // found once, it stays found: typed again, it answers from memory
    await page.fill('#cnsQ', ''); await page.keyboard.type(CLOUD.slice(0, 7));
    assert((await cards()).some(c => c.rid === CLOUD), 'a cloud order is in the index now');
    assert.equal(asked.filter(a => a.id === CLOUD).length, 4, 'and is not asked again');
    console.log('  ✓ unknown number → the cloud after the pause (timeline, pool, sheets, archive), shown and kept');

    /* ── the next key cancels a lookup in flight; nothing anywhere → a friendly empty state ── */
    await page.evaluate(() => { const f = window.fetch; window.__aborts = []; window.fetch = function (u, o) { const p = f.apply(this, arguments); p.catch(e => { if (e && e.name === 'AbortError') window.__aborts.push(String(u) + ' ' + String((o && o.body) || '')); }); return p; }; });
    await page.fill('#cnsQ', ''); await page.keyboard.type(SLOW);
    await page.waitForFunction(() => /Looking in the cloud for/.test(document.getElementById('cnsMsg').textContent), null, { timeout: 3000 });
    for (let i = 0; i < 40 && asked.filter(a => a.id === SLOW).length < 4; i++) await page.waitForTimeout(50);   // all four reads in flight
    assert.equal(asked.filter(a => a.id === SLOW).length, 4, 'the lookup started after the pause');
    await page.keyboard.type('9');
    await page.waitForFunction(n => /No order has/.test(document.getElementById('cnsMsg').textContent) && /in the cloud/.test(document.getElementById('cnsMsg').textContent), NOWHERE, { timeout: 10000 });
    const aborted = (await page.evaluate(() => window.__aborts)).filter(a => a.includes(SLOW));
    assert.equal(aborted.length, 4, 'the next key cancelled the lookup in flight: ' + JSON.stringify(aborted));
    console.log('  ✓ the next key aborts the lookup in flight; nothing found reads as a friendly empty state');
    assert.equal(etsyCalls(), etsyBefore, 'no Etsy call');
    await page.keyboard.press('Escape'); await page.keyboard.press('Escape');

    /* ── 5,000 orders: the index under 50 ms, a keystroke under 5 ms ── */
    const perf = await page.evaluate(() => {
      const base = B.orders.rows.length, names = ['Hannah', 'Ava', 'Zoe', 'Stephanie', 'Maria', 'Olivia', 'Grace', 'Chloe', 'Lily', 'Nora'];
      for (let i = 0; i < 5000; i++) {
        const rid = String(4180000000 + i * 7), order = { receiptId: rid, orderNumber: rid, createTs: 1790000000 + i, shipBy: 1790500000, buyer: { name: `${names[i % 10]} Buyer${i}` }, lines: [] };
        const line = { transactionId: rid + '1', listingId: String(1800000000 + i), sku: 'SKU_' + (i % 900), title: `Charm number ${i % 700} necklace`, variations: [] };
        order.lines.push(line);
        const row = { key: rid + '_' + line.transactionId, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', poolIds: [], engrave: null };
        B.orders.rows.push(row); B.orders.byKey.set(row.key, row);
      }
      const builds = []; for (let k = 0; k < 5; k++) builds.push(OrderSearch.rebuild());
      // each keystroke three times, its median kept (a collection of the garbage the five builds left is not the search's)
      const keys = [], raw = [], typeOf = s => { for (let i = 1; i <= s.length; i++) { const t = [0, 1, 2].map(() => OrderSearch.query(s.slice(0, i)).ms).sort((a, b) => a - b); keys.push(t[1]); raw.push(t[2]); } };
      OrderSearch.query('4');                                                         // warm
      typeOf('4180012345'); typeOf('grace buyer12'); typeOf('sku_42'); typeOf('41762'); typeOf('1800004');
      // and a whole keystroke as the box does it (the pass, the cards, the count), for the record
      OrderSearch.open(''); const q = document.getElementById('cnsQ'), full = [];
      for (const s of ['4', '41', '418', '4180', '41800', '418001']) { q.value = s; const t0 = performance.now(); q.dispatchEvent(new Event('input')); full.push(performance.now() - t0); }
      OrderSearch.close(true);
      return { orders: OrderSearch.stats().orders, rows: B.orders.rows.length - base, builds, keys, raw, full };
    });
    const med = a => a.slice().sort((x, y) => x - y)[a.length >> 1];
    console.log(`    index of ${perf.orders} orders: ${perf.builds.map(x => x.toFixed(1)).join(', ')} ms (median ${med(perf.builds).toFixed(1)}) · keystroke pass max ${Math.max(...perf.keys).toFixed(2)} ms, median ${med(perf.keys).toFixed(2)} ms (slowest single run ${Math.max(...perf.raw).toFixed(2)} ms) · with the cards drawn: max ${Math.max(...perf.full).toFixed(1)} ms`);
    assert(perf.orders >= 5000, 'five thousand orders indexed');
    assert(med(perf.builds) < 50, 'indexing 5,000 orders takes under 50 ms: ' + perf.builds.join(', '));
    assert(Math.max(...perf.keys) < 5, 'every keystroke\'s pass takes under 5 ms: ' + perf.keys.map(x => x.toFixed(2)).join(', '));
    console.log('  ✓ 5,000 orders: index < 50 ms, keystroke < 5 ms');

    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('order-search: ok'), e => { console.error(e); process.exit(1); });
